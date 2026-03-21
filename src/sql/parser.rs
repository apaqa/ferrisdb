// =============================================================================
// sql/parser.rs -- SQL Parser
// =============================================================================
//
// Parser 會把 lexer 產生的 token 串轉成 AST。
// 這裡採用 hand-written recursive descent parser，讓 SQL 語法的擴充
// 可以直接透過函式呼叫順序表達優先級與結構。

use crate::error::{FerrisDbError, Result};

use super::ast::{
    AggregateFunc, Assignment, ColumnDef, DataType, GroupByClause, JoinClause, JoinType, Operator,
    OrderByClause, OrderDirection, SelectColumns, SelectItem, Statement, Value, WhereExpr,
};
use super::lexer::{Keyword, Token};

pub struct Parser {
    tokens: Vec<Token>,
    pos: usize,
}

impl Parser {
    pub fn new(tokens: Vec<Token>) -> Parser {
        Parser { tokens, pos: 0 }
    }

    // 中文註解：解析整條 SQL，並檢查結尾是否還殘留未消耗的 token。
    pub fn parse(&mut self) -> Result<Statement> {
        if self.tokens.is_empty() {
            return Err(FerrisDbError::InvalidCommand(
                "empty SQL statement".to_string(),
            ));
        }

        let stmt = match self.peek() {
            Some(Token::Keyword(Keyword::Explain)) => self.parse_explain()?,
            Some(Token::Keyword(Keyword::Alter)) => self.parse_alter_table()?,
            Some(Token::Keyword(Keyword::Create)) => self.parse_create_statement()?,
            Some(Token::Keyword(Keyword::Drop)) => self.parse_drop_statement()?,
            Some(Token::Keyword(Keyword::Insert)) => self.parse_insert()?,
            Some(Token::Keyword(Keyword::Select)) => self.parse_select()?,
            Some(Token::Keyword(Keyword::Update)) => self.parse_update()?,
            Some(Token::Keyword(Keyword::Delete)) => self.parse_delete()?,
            other => {
                return Err(FerrisDbError::InvalidCommand(format!(
                    "unsupported SQL statement starting with {:?}",
                    other
                )));
            }
        };

        if matches!(self.peek(), Some(Token::Semicolon)) {
            self.bump();
        }

        if self.peek().is_some() {
            return Err(FerrisDbError::InvalidCommand(
                "unexpected trailing tokens in SQL".to_string(),
            ));
        }

        Ok(stmt)
    }

    fn parse_create_statement(&mut self) -> Result<Statement> {
        self.expect_keyword(Keyword::Create)?;
        match self.peek() {
            Some(Token::Keyword(Keyword::Table)) => self.parse_create_table_after_create(),
            Some(Token::Keyword(Keyword::Index)) => self.parse_create_index_after_create(),
            other => Err(FerrisDbError::InvalidCommand(format!(
                "expected TABLE or INDEX after CREATE, got {:?}",
                other
            ))),
        }
    }

    fn parse_create_table_after_create(&mut self) -> Result<Statement> {
        self.expect_keyword(Keyword::Table)?;
        let if_not_exists = self.parse_optional_if_not_exists()?;
        let table_name = self.expect_ident()?;
        self.expect_token(Token::LParen)?;

        let mut columns = Vec::new();
        loop {
            let name = self.expect_ident()?;
            let data_type = self.parse_data_type()?;
            columns.push(ColumnDef { name, data_type });

            if matches!(self.peek(), Some(Token::Comma)) {
                self.bump();
                continue;
            }
            break;
        }

        self.expect_token(Token::RParen)?;
        Ok(Statement::CreateTable {
            table_name,
            if_not_exists,
            columns,
        })
    }

    fn parse_create_index_after_create(&mut self) -> Result<Statement> {
        self.expect_keyword(Keyword::Index)?;
        self.expect_keyword(Keyword::On)?;
        let table_name = self.expect_ident()?;
        self.expect_token(Token::LParen)?;
        let column_name = self.expect_ident()?;
        self.expect_token(Token::RParen)?;
        Ok(Statement::CreateIndex {
            table_name,
            column_name,
        })
    }

    fn parse_drop_statement(&mut self) -> Result<Statement> {
        self.expect_keyword(Keyword::Drop)?;
        match self.peek() {
            Some(Token::Keyword(Keyword::Index)) => {
                self.expect_keyword(Keyword::Index)?;
                self.expect_keyword(Keyword::On)?;
                let table_name = self.expect_ident()?;
                self.expect_token(Token::LParen)?;
                let column_name = self.expect_ident()?;
                self.expect_token(Token::RParen)?;
                Ok(Statement::DropIndex {
                    table_name,
                    column_name,
                })
            }
            Some(Token::Keyword(Keyword::Table)) => {
                self.expect_keyword(Keyword::Table)?;
                let if_exists = self.parse_optional_if_exists()?;
                let table_name = self.expect_ident()?;
                Ok(Statement::DropTable {
                    table_name,
                    if_exists,
                })
            }
            other => Err(FerrisDbError::InvalidCommand(format!(
                "expected INDEX or TABLE after DROP, got {:?}",
                other
            ))),
        }
    }

    fn parse_alter_table(&mut self) -> Result<Statement> {
        self.expect_keyword(Keyword::Alter)?;
        self.expect_keyword(Keyword::Table)?;
        let table_name = self.expect_ident()?;
        match self.peek() {
            Some(Token::Keyword(Keyword::Add)) => {
                self.expect_keyword(Keyword::Add)?;
                self.expect_keyword(Keyword::Column)?;
                let name = self.expect_ident()?;
                let data_type = self.parse_data_type()?;
                Ok(Statement::AlterTableAdd {
                    table_name,
                    column: ColumnDef { name, data_type },
                })
            }
            Some(Token::Keyword(Keyword::Drop)) => {
                self.expect_keyword(Keyword::Drop)?;
                self.expect_keyword(Keyword::Column)?;
                let column_name = self.expect_ident()?;
                Ok(Statement::AlterTableDropColumn {
                    table_name,
                    column_name,
                })
            }
            other => Err(FerrisDbError::InvalidCommand(format!(
                "expected ADD or DROP after ALTER TABLE, got {:?}",
                other
            ))),
        }
    }

    fn parse_explain(&mut self) -> Result<Statement> {
        self.expect_keyword(Keyword::Explain)?;
        let statement = match self.peek() {
            Some(Token::Keyword(Keyword::Select)) => self.parse_select()?,
            other => {
                return Err(FerrisDbError::InvalidCommand(format!(
                    "EXPLAIN currently only supports SELECT, got {:?}",
                    other
                )));
            }
        };

        Ok(Statement::Explain {
            statement: Box::new(statement),
        })
    }

    fn parse_insert(&mut self) -> Result<Statement> {
        self.expect_keyword(Keyword::Insert)?;
        self.expect_keyword(Keyword::Into)?;
        let table_name = self.expect_ident()?;
        self.expect_keyword(Keyword::Values)?;

        let mut rows = Vec::new();
        loop {
            self.expect_token(Token::LParen)?;
            let mut values = Vec::new();
            loop {
                values.push(self.parse_value()?);
                if matches!(self.peek(), Some(Token::Comma)) {
                    self.bump();
                    continue;
                }
                break;
            }
            self.expect_token(Token::RParen)?;
            rows.push(values);

            if matches!(self.peek(), Some(Token::Comma)) {
                self.bump();
                continue;
            }
            break;
        }

        Ok(Statement::Insert {
            table_name,
            values: rows,
        })
    }

    // 中文註解：解析 SELECT 主體，包含 JOIN、WHERE、GROUP BY、HAVING、ORDER BY、LIMIT。
    fn parse_select(&mut self) -> Result<Statement> {
        self.expect_keyword(Keyword::Select)?;
        let columns = if matches!(self.peek(), Some(Token::Star)) {
            self.bump();
            SelectColumns::All
        } else {
            let mut items = Vec::new();
            let mut has_aggregate = false;
            loop {
                let item = self.parse_select_item()?;
                has_aggregate |= matches!(item, SelectItem::Aggregate { .. });
                items.push(item);
                if matches!(self.peek(), Some(Token::Comma)) {
                    self.bump();
                    continue;
                }
                break;
            }
            if has_aggregate {
                SelectColumns::Aggregate(items)
            } else {
                SelectColumns::Named(
                    items.into_iter()
                        .map(|item| match item {
                            SelectItem::Column(name) => name,
                            SelectItem::Aggregate { .. } => unreachable!("aggregate filtered"),
                        })
                        .collect(),
                )
            }
        };

        self.expect_keyword(Keyword::From)?;
        let table_name = self.parse_identifier_path()?;
        let join = self.parse_optional_join()?;
        let where_clause = self.parse_optional_where()?;
        let group_by = self.parse_optional_group_by()?;
        let having = self.parse_optional_having()?;
        let order_by = self.parse_optional_order_by()?;
        let limit = self.parse_optional_limit()?;

        Ok(Statement::Select {
            table_name,
            columns,
            join,
            where_clause,
            group_by,
            having,
            order_by,
            limit,
        })
    }

    fn parse_update(&mut self) -> Result<Statement> {
        self.expect_keyword(Keyword::Update)?;
        let table_name = self.expect_ident()?;
        self.expect_keyword(Keyword::Set)?;

        let mut assignments = Vec::new();
        loop {
            let column = self.expect_ident()?;
            self.expect_token(Token::Eq)?;
            let value = self.parse_value()?;
            assignments.push(Assignment { column, value });

            if matches!(self.peek(), Some(Token::Comma)) {
                self.bump();
                continue;
            }
            break;
        }

        let where_clause = self.parse_optional_where()?;
        Ok(Statement::Update {
            table_name,
            assignments,
            where_clause,
        })
    }

    fn parse_delete(&mut self) -> Result<Statement> {
        self.expect_keyword(Keyword::Delete)?;
        self.expect_keyword(Keyword::From)?;
        let table_name = self.expect_ident()?;
        let where_clause = self.parse_optional_where()?;
        Ok(Statement::Delete {
            table_name,
            where_clause,
        })
    }

    // 中文註解：WHERE 使用布林運算式樹狀結構，因此這裡只負責判斷有無 WHERE，實際遞迴解析交給 parse_where_expr。
    fn parse_optional_where(&mut self) -> Result<Option<WhereExpr>> {
        if !matches!(self.peek(), Some(Token::Keyword(Keyword::Where))) {
            return Ok(None);
        }

        self.bump();
        Ok(Some(self.parse_where_expr()?))
    }

    // 中文註解：HAVING 也沿用 WhereExpr，讓聚合後條件與一般 WHERE 共用同一套布林語法。
    fn parse_optional_having(&mut self) -> Result<Option<WhereExpr>> {
        if !matches!(self.peek(), Some(Token::Keyword(Keyword::Having))) {
            return Ok(None);
        }

        self.bump();
        Ok(Some(self.parse_where_expr()?))
    }

    // 中文註解：WHERE/HAVING 的入口，最低優先級是 OR。
    fn parse_where_expr(&mut self) -> Result<WhereExpr> {
        self.parse_or_expr()
    }

    // 中文註解：OR 是最低優先級，所以先吃完整個 AND 鏈再組成 OR 節點。
    fn parse_or_expr(&mut self) -> Result<WhereExpr> {
        let mut expr = self.parse_and_expr()?;
        while matches!(self.peek(), Some(Token::Keyword(Keyword::Or))) {
            self.bump();
            let rhs = self.parse_and_expr()?;
            expr = WhereExpr::Or(Box::new(expr), Box::new(rhs));
        }
        Ok(expr)
    }

    // 中文註解：AND 優先級高於 OR，因此在這一層連續吃掉多個 NOT/primary 子句。
    fn parse_and_expr(&mut self) -> Result<WhereExpr> {
        let mut expr = self.parse_not_expr()?;
        while matches!(self.peek(), Some(Token::Keyword(Keyword::And))) {
            self.bump();
            let rhs = self.parse_not_expr()?;
            expr = WhereExpr::And(Box::new(expr), Box::new(rhs));
        }
        Ok(expr)
    }

    // 中文註解：NOT 只套用到右邊的單一運算式，並支援連續巢狀 NOT。
    fn parse_not_expr(&mut self) -> Result<WhereExpr> {
        if matches!(self.peek(), Some(Token::Keyword(Keyword::Not))) {
            self.bump();
            return Ok(WhereExpr::Not(Box::new(self.parse_not_expr()?)));
        }
        self.parse_where_primary()
    }

    // 中文註解：primary 可以是括號包起來的子運算式，或一個實際比較/IN predicate。
    fn parse_where_primary(&mut self) -> Result<WhereExpr> {
        if matches!(self.peek(), Some(Token::LParen)) {
            self.bump();
            let expr = self.parse_where_expr()?;
            self.expect_token(Token::RParen)?;
            return Ok(expr);
        }

        self.parse_predicate_expr()
    }

    // 中文註解：解析最底層條件，支援 `column op value` 與 `column IN (SELECT ...)`。
    fn parse_predicate_expr(&mut self) -> Result<WhereExpr> {
        let column = self.parse_condition_column()?;
        if matches!(self.peek(), Some(Token::Keyword(Keyword::In))) {
            self.bump();
            self.expect_token(Token::LParen)?;
            let subquery = self.parse_select()?;
            self.expect_token(Token::RParen)?;
            return Ok(WhereExpr::InSubquery {
                column,
                subquery: Box::new(subquery),
            });
        }

        let operator = self.parse_operator()?;
        let value = self.parse_value()?;
        Ok(WhereExpr::Comparison {
            column,
            operator,
            value,
        })
    }

    // 中文註解：條件左側除了普通欄位，也允許 HAVING 使用聚合函式結果，例如 `COUNT(*) > 2`。
    fn parse_condition_column(&mut self) -> Result<String> {
        match self.peek() {
            Some(Token::Keyword(Keyword::Count))
            | Some(Token::Keyword(Keyword::Sum))
            | Some(Token::Keyword(Keyword::Min))
            | Some(Token::Keyword(Keyword::Max)) => {
                let item = self.parse_aggregate_item()?;
                Ok(render_condition_item(&item))
            }
            _ => self.parse_identifier_path(),
        }
    }

    // 中文註解：JOIN 目前支援 `JOIN`/`INNER JOIN` 與 `LEFT JOIN`。
    fn parse_optional_join(&mut self) -> Result<Option<JoinClause>> {
        let join_type = match self.peek() {
            Some(Token::Keyword(Keyword::Inner)) => {
                self.bump();
                JoinType::Inner
            }
            Some(Token::Keyword(Keyword::Left)) => {
                self.bump();
                JoinType::Left
            }
            Some(Token::Keyword(Keyword::Join)) => JoinType::Inner,
            _ => return Ok(None),
        };

        self.expect_keyword(Keyword::Join)?;
        let right_table = self.parse_identifier_path()?;
        self.expect_keyword(Keyword::On)?;
        let left_column = self.parse_identifier_path()?;
        self.expect_token(Token::Eq)?;
        let right_column = self.parse_identifier_path()?;

        Ok(Some(JoinClause {
            join_type,
            right_table,
            left_column,
            right_column,
        }))
    }

    fn parse_optional_order_by(&mut self) -> Result<Option<OrderByClause>> {
        if !matches!(self.peek(), Some(Token::Keyword(Keyword::Order))) {
            return Ok(None);
        }

        self.expect_keyword(Keyword::Order)?;
        self.expect_keyword(Keyword::By)?;
        let column = self.parse_identifier_path()?;
        let direction = match self.peek() {
            Some(Token::Keyword(Keyword::Asc)) => {
                self.bump();
                OrderDirection::Asc
            }
            Some(Token::Keyword(Keyword::Desc)) => {
                self.bump();
                OrderDirection::Desc
            }
            _ => OrderDirection::Asc,
        };

        Ok(Some(OrderByClause { column, direction }))
    }

    // 中文註解：GROUP BY 目前仍維持單欄位群組，但可以搭配 HAVING 做聚合後過濾。
    fn parse_optional_group_by(&mut self) -> Result<Option<GroupByClause>> {
        if !matches!(self.peek(), Some(Token::Keyword(Keyword::Group))) {
            return Ok(None);
        }

        self.expect_keyword(Keyword::Group)?;
        self.expect_keyword(Keyword::By)?;
        Ok(Some(GroupByClause {
            column: self.parse_identifier_path()?,
        }))
    }

    fn parse_optional_limit(&mut self) -> Result<Option<usize>> {
        if !matches!(self.peek(), Some(Token::Keyword(Keyword::Limit))) {
            return Ok(None);
        }

        self.expect_keyword(Keyword::Limit)?;
        match self.bump() {
            Some(Token::IntLit(value)) if value >= 0 => Ok(Some(value as usize)),
            other => Err(FerrisDbError::InvalidCommand(format!(
                "expected non-negative LIMIT value, got {:?}",
                other
            ))),
        }
    }

    fn parse_operator(&mut self) -> Result<Operator> {
        match self.bump() {
            Some(Token::Eq) => Ok(Operator::Eq),
            Some(Token::Ne) => Ok(Operator::Ne),
            Some(Token::Lt) => Ok(Operator::Lt),
            Some(Token::Gt) => Ok(Operator::Gt),
            Some(Token::Le) => Ok(Operator::Le),
            Some(Token::Ge) => Ok(Operator::Ge),
            other => Err(FerrisDbError::InvalidCommand(format!(
                "expected SQL operator, got {:?}",
                other
            ))),
        }
    }

    fn parse_data_type(&mut self) -> Result<DataType> {
        match self.bump() {
            Some(Token::Keyword(Keyword::Int)) => Ok(DataType::Int),
            Some(Token::Keyword(Keyword::Text)) => Ok(DataType::Text),
            Some(Token::Keyword(Keyword::Bool)) => Ok(DataType::Bool),
            other => Err(FerrisDbError::InvalidCommand(format!(
                "expected SQL data type, got {:?}",
                other
            ))),
        }
    }

    fn parse_value(&mut self) -> Result<Value> {
        match self.bump() {
            Some(Token::IntLit(v)) => Ok(Value::Int(v)),
            Some(Token::StringLit(v)) => Ok(Value::Text(v)),
            Some(Token::Keyword(Keyword::True)) => Ok(Value::Bool(true)),
            Some(Token::Keyword(Keyword::False)) => Ok(Value::Bool(false)),
            Some(Token::Keyword(Keyword::Null)) => Ok(Value::Null),
            other => Err(FerrisDbError::InvalidCommand(format!(
                "expected SQL value, got {:?}",
                other
            ))),
        }
    }

    fn parse_select_item(&mut self) -> Result<SelectItem> {
        match self.peek() {
            Some(Token::Keyword(Keyword::Count))
            | Some(Token::Keyword(Keyword::Sum))
            | Some(Token::Keyword(Keyword::Min))
            | Some(Token::Keyword(Keyword::Max)) => self.parse_aggregate_item(),
            _ => Ok(SelectItem::Column(self.parse_identifier_path()?)),
        }
    }

    fn parse_aggregate_item(&mut self) -> Result<SelectItem> {
        let func = match self.bump() {
            Some(Token::Keyword(Keyword::Count)) => AggregateFunc::Count,
            Some(Token::Keyword(Keyword::Sum)) => AggregateFunc::Sum,
            Some(Token::Keyword(Keyword::Min)) => AggregateFunc::Min,
            Some(Token::Keyword(Keyword::Max)) => AggregateFunc::Max,
            other => {
                return Err(FerrisDbError::InvalidCommand(format!(
                    "expected aggregate function, got {:?}",
                    other
                )));
            }
        };

        self.expect_token(Token::LParen)?;
        let column = if matches!(self.peek(), Some(Token::Star)) {
            self.bump();
            None
        } else {
            Some(self.parse_identifier_path()?)
        };
        self.expect_token(Token::RParen)?;

        if !matches!(func, AggregateFunc::Count) && column.is_none() {
            return Err(FerrisDbError::InvalidCommand(
                "only COUNT supports '*'".to_string(),
            ));
        }

        Ok(SelectItem::Aggregate { func, column })
    }

    fn parse_optional_if_exists(&mut self) -> Result<bool> {
        if !matches!(self.peek(), Some(Token::Keyword(Keyword::If))) {
            return Ok(false);
        }
        self.expect_keyword(Keyword::If)?;
        self.expect_keyword(Keyword::Exists)?;
        Ok(true)
    }

    fn parse_optional_if_not_exists(&mut self) -> Result<bool> {
        if !matches!(self.peek(), Some(Token::Keyword(Keyword::If))) {
            return Ok(false);
        }
        self.expect_keyword(Keyword::If)?;
        self.expect_keyword(Keyword::Not)?;
        self.expect_keyword(Keyword::Exists)?;
        Ok(true)
    }

    fn expect_ident(&mut self) -> Result<String> {
        match self.bump() {
            Some(Token::Ident(name)) => Ok(name),
            other => Err(FerrisDbError::InvalidCommand(format!(
                "expected identifier, got {:?}",
                other
            ))),
        }
    }

    fn expect_keyword(&mut self, expected: Keyword) -> Result<()> {
        match self.bump() {
            Some(Token::Keyword(actual)) if actual == expected => Ok(()),
            other => Err(FerrisDbError::InvalidCommand(format!(
                "expected keyword {:?}, got {:?}",
                expected, other
            ))),
        }
    }

    fn expect_token(&mut self, expected: Token) -> Result<()> {
        match self.bump() {
            Some(actual) if actual == expected => Ok(()),
            other => Err(FerrisDbError::InvalidCommand(format!(
                "expected token {:?}, got {:?}",
                expected, other
            ))),
        }
    }

    fn peek(&self) -> Option<&Token> {
        self.tokens.get(self.pos)
    }

    fn parse_identifier_path(&mut self) -> Result<String> {
        let mut parts = vec![self.expect_ident()?];
        while matches!(self.peek(), Some(Token::Dot)) {
            self.bump();
            parts.push(self.expect_ident()?);
        }
        Ok(parts.join("."))
    }

    fn bump(&mut self) -> Option<Token> {
        let token = self.tokens.get(self.pos).cloned();
        if token.is_some() {
            self.pos += 1;
        }
        token
    }
}

// 中文註解：把聚合函式 token 重新轉回固定字串，讓 HAVING 可以用欄位名稱比對聚合結果。
fn render_condition_item(item: &SelectItem) -> String {
    match item {
        SelectItem::Column(name) => name.clone(),
        SelectItem::Aggregate { func, column } => match (func, column.as_deref()) {
            (AggregateFunc::Count, None) => "COUNT(*)".to_string(),
            (AggregateFunc::Count, Some(column)) => format!("COUNT({})", column),
            (AggregateFunc::Sum, Some(column)) => format!("SUM({})", column),
            (AggregateFunc::Min, Some(column)) => format!("MIN({})", column),
            (AggregateFunc::Max, Some(column)) => format!("MAX({})", column),
            (_, None) => "INVALID_AGGREGATE".to_string(),
        },
    }
}

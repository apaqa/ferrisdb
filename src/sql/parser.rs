// =============================================================================
// sql/parser.rs -- SQL Parser
// =============================================================================
use crate::error::{FerrisDbError, Result};

use super::ast::{
    AggregateFunc, Assignment, CTE, ColumnDef, DataType, Expr, GroupByClause, InsertSource,
    JoinClause, JoinType, Operator, OrderByClause, OrderDirection, SelectColumns, SelectItem,
    Statement, Value, WhereExpr, WindowFunc,
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

    // 中文註解：多語句 parser 會先按分號切開，再逐段做 lexer/parser，空語句直接略過。
    pub fn parse_multiple(input: &str) -> Result<Vec<Statement>> {
        let mut statements = Vec::new();
        for sql in split_sql_statements(input) {
            let mut lexer = super::lexer::Lexer::new(&sql);
            let tokens = lexer.tokenize()?;
            let mut parser = Parser::new(tokens);
            statements.push(parser.parse()?);
        }
        Ok(statements)
    }

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
            Some(Token::Keyword(Keyword::With)) => self.parse_query_expression()?,
            Some(Token::Keyword(Keyword::Select)) => self.parse_query_expression()?,
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
            Some(Token::Keyword(Keyword::View)) => self.parse_create_view_after_create(),
            Some(Token::Keyword(Keyword::Table)) => self.parse_create_table_after_create(),
            Some(Token::Keyword(Keyword::Index)) => self.parse_create_index_after_create(),
            other => Err(FerrisDbError::InvalidCommand(format!(
                "expected VIEW, TABLE or INDEX after CREATE, got {:?}",
                other
            ))),
        }
    }

    fn parse_create_view_after_create(&mut self) -> Result<Statement> {
        self.expect_keyword(Keyword::View)?;
        let view_name = self.expect_ident()?;
        self.expect_keyword(Keyword::As)?;

        // 中文註解：View 要保留定義 SQL 字串，executor 之後才能重新展開並執行。
        let mut query_tokens = self.tokens[self.pos..].to_vec();
        if matches!(query_tokens.last(), Some(Token::Semicolon)) {
            query_tokens.pop();
        }
        let query_sql = tokens_to_sql(&query_tokens);
        let mut query_parser = Parser::new(query_tokens);
        let query = query_parser.parse_query_expression()?;
        self.pos = self.tokens.len();

        Ok(Statement::CreateView {
            view_name,
            query_sql,
            query: Box::new(query),
        })
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
        let column_names = self.parse_identifier_list()?;
        self.expect_token(Token::RParen)?;
        Ok(Statement::CreateIndex {
            table_name,
            column_names,
        })
    }

    fn parse_drop_statement(&mut self) -> Result<Statement> {
        self.expect_keyword(Keyword::Drop)?;
        match self.peek() {
            Some(Token::Keyword(Keyword::View)) => {
                self.expect_keyword(Keyword::View)?;
                let if_exists = self.parse_optional_if_exists()?;
                let view_name = self.expect_ident()?;
                Ok(Statement::DropView {
                    view_name,
                    if_exists,
                })
            }
            Some(Token::Keyword(Keyword::Index)) => {
                self.expect_keyword(Keyword::Index)?;
                self.expect_keyword(Keyword::On)?;
                let table_name = self.expect_ident()?;
                self.expect_token(Token::LParen)?;
                let column_names = self.parse_identifier_list()?;
                self.expect_token(Token::RParen)?;
                Ok(Statement::DropIndex {
                    table_name,
                    column_names,
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
                "expected VIEW, INDEX or TABLE after DROP, got {:?}",
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
            Some(Token::Keyword(Keyword::With)) => self.parse_query_expression()?,
            Some(Token::Keyword(Keyword::Select)) => self.parse_query_expression()?,
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

        let source = match self.peek() {
            Some(Token::Keyword(Keyword::Values)) => {
                self.bump();
                InsertSource::Values(self.parse_insert_values()?)
            }
            Some(Token::Keyword(Keyword::Select)) => {
                InsertSource::Select(Box::new(self.parse_query_expression()?))
            }
            other => {
                return Err(FerrisDbError::InvalidCommand(format!(
                    "expected VALUES or SELECT after INSERT INTO, got {:?}",
                    other
                )));
            }
        };

        Ok(Statement::Insert { table_name, source })
    }

    fn parse_insert_values(&mut self) -> Result<Vec<Vec<Value>>> {
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
        Ok(rows)
    }

    fn parse_optional_ctes(&mut self) -> Result<Vec<CTE>> {
        if !matches!(self.peek(), Some(Token::Keyword(Keyword::With))) {
            return Ok(Vec::new());
        }

        self.expect_keyword(Keyword::With)?;
        if matches!(self.peek(), Some(Token::Keyword(Keyword::Recursive))) {
            return Err(FerrisDbError::InvalidCommand(
                "WITH RECURSIVE is not supported yet".to_string(),
            ));
        }

        let mut ctes = Vec::new();
        loop {
            let name = self.expect_ident()?;
            self.expect_keyword(Keyword::As)?;
            self.expect_token(Token::LParen)?;
            let inner_tokens = self.collect_parenthesized_query_tokens()?;
            let mut parser = Parser::new(inner_tokens);
            let query = parser.parse_query_expression()?;
            ctes.push(CTE {
                name,
                query: Box::new(query),
            });

            if matches!(self.peek(), Some(Token::Comma)) {
                self.bump();
                continue;
            }
            break;
        }

        Ok(ctes)
    }

    // 中文註解：查詢表達式允許 `SELECT ... UNION [ALL] SELECT ...` 的鏈式結構。
    fn parse_query_expression(&mut self) -> Result<Statement> {
        let ctes = self.parse_optional_ctes()?;
        let mut statement = self.parse_select_with_ctes(ctes)?;
        while matches!(self.peek(), Some(Token::Keyword(Keyword::Union))) {
            self.bump();
            let all = if matches!(self.peek(), Some(Token::Keyword(Keyword::All))) {
                self.bump();
                true
            } else {
                false
            };
            let right = self.parse_select_with_ctes(Vec::new())?;
            statement = Statement::Union {
                left: Box::new(statement),
                right: Box::new(right),
                all,
            };
        }
        Ok(statement)
    }

    fn parse_select_with_ctes(&mut self, ctes: Vec<CTE>) -> Result<Statement> {
        self.expect_keyword(Keyword::Select)?;
        let distinct = if matches!(self.peek(), Some(Token::Keyword(Keyword::Distinct))) {
            self.bump();
            true
        } else {
            false
        };
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
                SelectColumns::Named(items)
            }
        };

        self.expect_keyword(Keyword::From)?;
        let table_name = self.parse_identifier_path()?;
        let table_alias = self.parse_optional_table_alias()?;
        let join = self.parse_optional_join()?;
        let where_clause = self.parse_optional_where()?;
        let group_by = self.parse_optional_group_by()?;
        let having = self.parse_optional_having()?;
        let order_by = self.parse_optional_order_by()?;
        let limit = self.parse_optional_limit()?;

        Ok(Statement::Select {
            ctes,
            distinct,
            table_name,
            table_alias,
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

        let mut from_table = None;
        let mut join_condition = None;
        if matches!(self.peek(), Some(Token::Keyword(Keyword::From))) {
            self.bump();
            from_table = Some(self.expect_ident()?);
        }
        let where_clause = self.parse_optional_where()?;
        if from_table.is_some() {
            join_condition = where_clause.clone();
        }
        Ok(Statement::Update {
            table_name,
            assignments,
            from_table,
            join_condition,
            where_clause,
        })
    }

    fn parse_delete(&mut self) -> Result<Statement> {
        self.expect_keyword(Keyword::Delete)?;
        self.expect_keyword(Keyword::From)?;
        let table_name = self.expect_ident()?;
        let mut using_table = None;
        let mut join_condition = None;
        if matches!(self.peek(), Some(Token::Keyword(Keyword::Using))) {
            self.bump();
            using_table = Some(self.expect_ident()?);
        }
        let where_clause = self.parse_optional_where()?;
        if using_table.is_some() {
            join_condition = where_clause.clone();
        }
        Ok(Statement::Delete {
            table_name,
            using_table,
            join_condition,
            where_clause,
        })
    }

    fn parse_optional_where(&mut self) -> Result<Option<WhereExpr>> {
        if !matches!(self.peek(), Some(Token::Keyword(Keyword::Where))) {
            return Ok(None);
        }

        self.bump();
        Ok(Some(self.parse_where_expr()?))
    }

    fn parse_optional_having(&mut self) -> Result<Option<WhereExpr>> {
        if !matches!(self.peek(), Some(Token::Keyword(Keyword::Having))) {
            return Ok(None);
        }

        self.bump();
        Ok(Some(self.parse_where_expr()?))
    }

    fn parse_where_expr(&mut self) -> Result<WhereExpr> {
        self.parse_or_expr()
    }

    fn parse_or_expr(&mut self) -> Result<WhereExpr> {
        let mut expr = self.parse_and_expr()?;
        while matches!(self.peek(), Some(Token::Keyword(Keyword::Or))) {
            self.bump();
            let rhs = self.parse_and_expr()?;
            expr = WhereExpr::Or(Box::new(expr), Box::new(rhs));
        }
        Ok(expr)
    }

    fn parse_and_expr(&mut self) -> Result<WhereExpr> {
        let mut expr = self.parse_not_expr()?;
        while matches!(self.peek(), Some(Token::Keyword(Keyword::And))) {
            self.bump();
            let rhs = self.parse_not_expr()?;
            expr = WhereExpr::And(Box::new(expr), Box::new(rhs));
        }
        Ok(expr)
    }

    fn parse_not_expr(&mut self) -> Result<WhereExpr> {
        if matches!(self.peek(), Some(Token::Keyword(Keyword::Not))) {
            self.bump();
            return Ok(WhereExpr::Not(Box::new(self.parse_not_expr()?)));
        }
        self.parse_where_primary()
    }

    fn parse_where_primary(&mut self) -> Result<WhereExpr> {
        if matches!(self.peek(), Some(Token::LParen)) {
            self.bump();
            let expr = self.parse_where_expr()?;
            self.expect_token(Token::RParen)?;
            return Ok(expr);
        }

        self.parse_predicate_expr()
    }

    fn parse_predicate_expr(&mut self) -> Result<WhereExpr> {
        let column = self.parse_condition_column()?;
        if matches!(self.peek(), Some(Token::Keyword(Keyword::Between))) {
            self.bump();
            let low = self.parse_value()?;
            self.expect_keyword(Keyword::And)?;
            let high = self.parse_value()?;
            return Ok(WhereExpr::Between { column, low, high });
        }
        if matches!(self.peek(), Some(Token::Keyword(Keyword::Like))) {
            self.bump();
            let pattern = match self.parse_value()? {
                Value::Text(pattern) => pattern,
                other => {
                    return Err(FerrisDbError::InvalidCommand(format!(
                        "LIKE expects string pattern, got {:?}",
                        other
                    )));
                }
            };
            return Ok(WhereExpr::Like { column, pattern });
        }
        if matches!(self.peek(), Some(Token::Keyword(Keyword::Is))) {
            self.bump();
            let negated = if matches!(self.peek(), Some(Token::Keyword(Keyword::Not))) {
                self.bump();
                true
            } else {
                false
            };
            self.expect_keyword(Keyword::Null)?;
            return Ok(WhereExpr::IsNull { column, negated });
        }
        if matches!(self.peek(), Some(Token::Keyword(Keyword::In))) {
            self.bump();
            self.expect_token(Token::LParen)?;
            let subquery = self.parse_query_expression()?;
            self.expect_token(Token::RParen)?;
            return Ok(WhereExpr::InSubquery {
                column,
                subquery: Box::new(subquery),
            });
        }

        let operator = self.parse_operator()?;
        match self.peek() {
            Some(Token::Ident(_)) => Ok(WhereExpr::ColumnComparison {
                left: column,
                operator,
                right: self.parse_identifier_path()?,
            }),
            _ => {
                let value = self.parse_value()?;
                Ok(WhereExpr::Comparison {
                    column,
                    operator,
                    value,
                })
            }
        }
    }

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
        let right_alias = self.parse_optional_table_alias()?;
        self.expect_keyword(Keyword::On)?;
        let left_column = self.parse_identifier_path()?;
        self.expect_token(Token::Eq)?;
        let right_column = self.parse_identifier_path()?;

        Ok(Some(JoinClause {
            join_type,
            right_table,
            right_alias,
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
        let (column, expr) = if matches!(self.peek(), Some(Token::Keyword(Keyword::Case))) {
            (
                "__case_when__".to_string(),
                Some(self.parse_case_when_expr()?),
            )
        } else {
            (self.parse_identifier_path()?, None)
        };
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

        Ok(Some(OrderByClause {
            column,
            expr,
            direction,
        }))
    }

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
            Some(Token::Keyword(Keyword::Case)) => {
                let expr = self.parse_case_when_expr()?;
                let alias = self.parse_optional_alias()?;
                Ok(SelectItem::Expression { expr, alias })
            }
            Some(Token::Keyword(Keyword::RowNumber)) | Some(Token::Keyword(Keyword::Rank)) => {
                let expr = self.parse_window_function_expr()?;
                let alias = self.parse_optional_alias()?;
                Ok(SelectItem::Expression { expr, alias })
            }
            Some(Token::Keyword(Keyword::Count))
            | Some(Token::Keyword(Keyword::Sum))
            | Some(Token::Keyword(Keyword::Min))
            | Some(Token::Keyword(Keyword::Max)) => {
                if self.is_window_aggregate_start() {
                    let expr = self.parse_window_function_expr()?;
                    let alias = self.parse_optional_alias()?;
                    Ok(SelectItem::Expression { expr, alias })
                } else {
                    self.parse_aggregate_item()
                }
            }
            _ => {
                let name = self.parse_identifier_path()?;
                let alias = self.parse_optional_alias()?;
                Ok(SelectItem::Column { name, alias })
            }
        }
    }

    fn parse_case_when_expr(&mut self) -> Result<Expr> {
        self.expect_keyword(Keyword::Case)?;
        let mut conditions = Vec::new();
        while matches!(self.peek(), Some(Token::Keyword(Keyword::When))) {
            self.bump();
            let condition = self.parse_where_expr()?;
            self.expect_keyword(Keyword::Then)?;
            let result = self.parse_case_result_expr()?;
            conditions.push((condition, result));
        }
        let else_result = if matches!(self.peek(), Some(Token::Keyword(Keyword::Else))) {
            self.bump();
            Some(Box::new(self.parse_case_result_expr()?))
        } else {
            None
        };
        self.expect_keyword(Keyword::End)?;
        Ok(Expr::CaseWhen {
            conditions,
            else_result,
        })
    }

    fn parse_case_result_expr(&mut self) -> Result<Expr> {
        match self.peek() {
            Some(Token::Keyword(Keyword::Case)) => self.parse_case_when_expr(),
            Some(Token::Ident(_)) => Ok(Expr::Column(self.parse_identifier_path()?)),
            _ => Ok(Expr::Value(self.parse_value()?)),
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

        let alias = self.parse_optional_alias()?;
        Ok(SelectItem::Aggregate {
            func,
            column,
            alias,
        })
    }

    fn parse_window_function_expr(&mut self) -> Result<Expr> {
        let (func, target_column) = match self.bump() {
            Some(Token::Keyword(Keyword::RowNumber)) => {
                self.expect_token(Token::LParen)?;
                self.expect_token(Token::RParen)?;
                (WindowFunc::RowNumber, None)
            }
            Some(Token::Keyword(Keyword::Rank)) => {
                self.expect_token(Token::LParen)?;
                self.expect_token(Token::RParen)?;
                (WindowFunc::Rank, None)
            }
            Some(Token::Keyword(Keyword::Count)) => {
                self.expect_token(Token::LParen)?;
                let target = if matches!(self.peek(), Some(Token::Star)) {
                    self.bump();
                    None
                } else {
                    Some(self.parse_identifier_path()?)
                };
                self.expect_token(Token::RParen)?;
                (WindowFunc::WinCount, target)
            }
            Some(Token::Keyword(Keyword::Sum)) => {
                self.expect_token(Token::LParen)?;
                let target = Some(self.parse_identifier_path()?);
                self.expect_token(Token::RParen)?;
                (WindowFunc::WinSum, target)
            }
            other => {
                return Err(FerrisDbError::InvalidCommand(format!(
                    "expected window function, got {:?}",
                    other
                )));
            }
        };
        self.expect_keyword(Keyword::Over)?;
        self.expect_token(Token::LParen)?;
        let partition_by = if matches!(self.peek(), Some(Token::Keyword(Keyword::Partition))) {
            self.bump();
            self.expect_keyword(Keyword::By)?;
            Some(self.parse_identifier_path()?)
        } else {
            None
        };
        let order_by = if matches!(self.peek(), Some(Token::Keyword(Keyword::Order))) {
            self.bump();
            self.expect_keyword(Keyword::By)?;
            let column = self.parse_identifier_path()?;
            let asc = !matches!(self.peek(), Some(Token::Keyword(Keyword::Desc)));
            if matches!(
                self.peek(),
                Some(Token::Keyword(Keyword::Asc)) | Some(Token::Keyword(Keyword::Desc))
            ) {
                self.bump();
            }
            Some((column, asc))
        } else {
            None
        };
        self.expect_token(Token::RParen)?;

        Ok(Expr::WindowFunction {
            func,
            target_column,
            partition_by,
            order_by,
        })
    }

    fn parse_optional_alias(&mut self) -> Result<Option<String>> {
        if matches!(self.peek(), Some(Token::Keyword(Keyword::As))) {
            self.bump();
            return Ok(Some(self.expect_ident()?));
        }
        Ok(None)
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

    fn parse_identifier_list(&mut self) -> Result<Vec<String>> {
        let mut columns = Vec::new();
        loop {
            columns.push(self.expect_ident()?);
            if matches!(self.peek(), Some(Token::Comma)) {
                self.bump();
                continue;
            }
            break;
        }
        Ok(columns)
    }

    fn collect_parenthesized_query_tokens(&mut self) -> Result<Vec<Token>> {
        let mut depth = 1_i32;
        let mut tokens = Vec::new();

        while let Some(token) = self.bump() {
            match token {
                Token::LParen => {
                    depth += 1;
                    tokens.push(Token::LParen);
                }
                Token::RParen => {
                    depth -= 1;
                    if depth == 0 {
                        return Ok(tokens);
                    }
                    tokens.push(Token::RParen);
                }
                other => tokens.push(other),
            }
        }

        Err(FerrisDbError::InvalidCommand(
            "unterminated CTE subquery".to_string(),
        ))
    }

    fn parse_optional_table_alias(&mut self) -> Result<Option<String>> {
        if matches!(self.peek(), Some(Token::Keyword(Keyword::As))) {
            self.bump();
            return Ok(Some(self.expect_ident()?));
        }
        Ok(None)
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

    fn peek_n(&self, offset: usize) -> Option<&Token> {
        self.tokens.get(self.pos + offset)
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

    fn is_window_aggregate_start(&self) -> bool {
        let mut depth = 0;
        let mut index = self.pos + 1;
        if !matches!(self.peek_n(1), Some(Token::LParen)) {
            return false;
        }
        while let Some(token) = self.tokens.get(index) {
            match token {
                Token::LParen => depth += 1,
                Token::RParen => {
                    depth -= 1;
                    if depth == 0 {
                        return matches!(
                            self.tokens.get(index + 1),
                            Some(Token::Keyword(Keyword::Over))
                        );
                    }
                }
                _ => {}
            }
            index += 1;
        }
        false
    }
}

fn render_condition_item(item: &SelectItem) -> String {
    match item {
        SelectItem::Column { name, .. } => name.clone(),
        SelectItem::Expression { .. } => "EXPR".to_string(),
        SelectItem::Aggregate { func, column, .. } => match (func, column.as_deref()) {
            (AggregateFunc::Count, None) => "COUNT(*)".to_string(),
            (AggregateFunc::Count, Some(column)) => format!("COUNT({})", column),
            (AggregateFunc::Sum, Some(column)) => format!("SUM({})", column),
            (AggregateFunc::Min, Some(column)) => format!("MIN({})", column),
            (AggregateFunc::Max, Some(column)) => format!("MAX({})", column),
            (_, None) => "INVALID_AGGREGATE".to_string(),
        },
    }
}

fn split_sql_statements(input: &str) -> Vec<String> {
    let mut statements = Vec::new();
    let mut current = String::new();
    let mut in_string = false;
    let mut chars = input.chars().peekable();

    while let Some(ch) = chars.next() {
        match ch {
            '\'' => {
                current.push(ch);
                if in_string && matches!(chars.peek(), Some('\'')) {
                    current.push(chars.next().expect("escaped quote"));
                } else {
                    in_string = !in_string;
                }
            }
            ';' if !in_string => {
                let trimmed = current.trim();
                if !trimmed.is_empty() {
                    statements.push(trimmed.to_string());
                }
                current.clear();
            }
            _ => current.push(ch),
        }
    }

    let trimmed = current.trim();
    if !trimmed.is_empty() {
        statements.push(trimmed.to_string());
    }

    statements
}

fn tokens_to_sql(tokens: &[Token]) -> String {
    let mut sql = String::new();
    for (index, token) in tokens.iter().enumerate() {
        let piece = token_to_sql(token);
        if should_insert_space(sql.chars().last(), token, index > 0) {
            sql.push(' ');
        }
        sql.push_str(&piece);
    }
    sql
}

fn token_to_sql(token: &Token) -> String {
    match token {
        Token::Keyword(keyword) => keyword_to_sql(keyword).to_string(),
        Token::Ident(value) => value.clone(),
        Token::IntLit(value) => value.to_string(),
        Token::StringLit(value) => format!("'{}'", value.replace('\'', "''")),
        Token::Star => "*".to_string(),
        Token::Comma => ",".to_string(),
        Token::LParen => "(".to_string(),
        Token::RParen => ")".to_string(),
        Token::Eq => "=".to_string(),
        Token::Dot => ".".to_string(),
        Token::Ne => "!=".to_string(),
        Token::Lt => "<".to_string(),
        Token::Gt => ">".to_string(),
        Token::Le => "<=".to_string(),
        Token::Ge => ">=".to_string(),
        Token::Semicolon => ";".to_string(),
    }
}

fn keyword_to_sql(keyword: &Keyword) -> &'static str {
    match keyword {
        Keyword::Explain => "EXPLAIN",
        Keyword::Alter => "ALTER",
        Keyword::Select => "SELECT",
        Keyword::From => "FROM",
        Keyword::Where => "WHERE",
        Keyword::With => "WITH",
        Keyword::Recursive => "RECURSIVE",
        Keyword::Case => "CASE",
        Keyword::When => "WHEN",
        Keyword::Then => "THEN",
        Keyword::Else => "ELSE",
        Keyword::End => "END",
        Keyword::Count => "COUNT",
        Keyword::Sum => "SUM",
        Keyword::Min => "MIN",
        Keyword::Max => "MAX",
        Keyword::Group => "GROUP",
        Keyword::Having => "HAVING",
        Keyword::Order => "ORDER",
        Keyword::By => "BY",
        Keyword::Asc => "ASC",
        Keyword::As => "AS",
        Keyword::Desc => "DESC",
        Keyword::Distinct => "DISTINCT",
        Keyword::Limit => "LIMIT",
        Keyword::Insert => "INSERT",
        Keyword::Into => "INTO",
        Keyword::Values => "VALUES",
        Keyword::Create => "CREATE",
        Keyword::View => "VIEW",
        Keyword::Index => "INDEX",
        Keyword::Table => "TABLE",
        Keyword::Add => "ADD",
        Keyword::Column => "COLUMN",
        Keyword::Drop => "DROP",
        Keyword::If => "IF",
        Keyword::Exists => "EXISTS",
        Keyword::Not => "NOT",
        Keyword::Is => "IS",
        Keyword::In => "IN",
        Keyword::Between => "BETWEEN",
        Keyword::Like => "LIKE",
        Keyword::Update => "UPDATE",
        Keyword::Set => "SET",
        Keyword::Delete => "DELETE",
        Keyword::Using => "USING",
        Keyword::Over => "OVER",
        Keyword::Partition => "PARTITION",
        Keyword::RowNumber => "ROW_NUMBER",
        Keyword::Rank => "RANK",
        Keyword::Join => "JOIN",
        Keyword::On => "ON",
        Keyword::Union => "UNION",
        Keyword::Inner => "INNER",
        Keyword::Left => "LEFT",
        Keyword::And => "AND",
        Keyword::Or => "OR",
        Keyword::Int => "INT",
        Keyword::Text => "TEXT",
        Keyword::Bool => "BOOL",
        Keyword::Null => "NULL",
        Keyword::True => "TRUE",
        Keyword::False => "FALSE",
        Keyword::All => "ALL",
    }
}

fn should_insert_space(previous: Option<char>, token: &Token, has_previous: bool) -> bool {
    if !has_previous {
        return false;
    }

    let Some(previous) = previous else {
        return false;
    };

    if matches!(token, Token::Comma | Token::RParen | Token::Dot) {
        return false;
    }
    if matches!(token, Token::LParen) && previous.is_ascii_alphanumeric() {
        return false;
    }
    !matches!(previous, '(' | '.' | ' ')
}

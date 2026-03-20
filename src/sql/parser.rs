// =============================================================================
// sql/parser.rs — SQL Parser
// =============================================================================
//
// Parser 接收 lexer 產生的 token 串，並依照 SQL 語法規則組成 AST。
//
// 例子：
// - tokens: SELECT, name, FROM, users, WHERE, id, =, 1
// - AST: Statement::Select { ... }
//
// 這裡採用簡單的 hand-written recursive descent parser，
// 好處是易讀、容易擴充，也很適合這種子集 SQL。

use crate::error::{FerrisDbError, Result};

use super::ast::{
    AggregateFunc, Assignment, ColumnDef, DataType, GroupByClause, JoinClause, Operator,
    OrderByClause, OrderDirection, SelectColumns, SelectItem, Statement, Value, WhereClause,
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

    pub fn parse(&mut self) -> Result<Statement> {
        if self.tokens.is_empty() {
            return Err(FerrisDbError::InvalidCommand(
                "empty SQL statement".to_string(),
            ));
        }

        let stmt = match self.peek() {
            Some(Token::Keyword(Keyword::Explain)) => self.parse_explain()?,
            Some(Token::Keyword(Keyword::Create)) => self.parse_create_table()?,
            Some(Token::Keyword(Keyword::Insert)) => self.parse_insert()?,
            Some(Token::Keyword(Keyword::Select)) => self.parse_select()?,
            Some(Token::Keyword(Keyword::Update)) => self.parse_update()?,
            Some(Token::Keyword(Keyword::Delete)) => self.parse_delete()?,
            other => {
                return Err(FerrisDbError::InvalidCommand(format!(
                    "unsupported SQL statement starting with {:?}",
                    other
                )))
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

    fn parse_create_table(&mut self) -> Result<Statement> {
        self.expect_keyword(Keyword::Create)?;
        self.expect_keyword(Keyword::Table)?;
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
        Ok(Statement::CreateTable { table_name, columns })
    }

    fn parse_explain(&mut self) -> Result<Statement> {
        self.expect_keyword(Keyword::Explain)?;
        let statement = match self.peek() {
            Some(Token::Keyword(Keyword::Select)) => self.parse_select()?,
            other => {
                return Err(FerrisDbError::InvalidCommand(format!(
                    "EXPLAIN currently only supports SELECT, got {:?}",
                    other
                )))
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
        let order_by = self.parse_optional_order_by()?;
        let limit = self.parse_optional_limit()?;

        Ok(Statement::Select {
            table_name,
            columns,
            join,
            where_clause,
            group_by,
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

    fn parse_optional_where(&mut self) -> Result<Option<WhereClause>> {
        if !matches!(self.peek(), Some(Token::Keyword(Keyword::Where))) {
            return Ok(None);
        }

        self.bump();
        let column = self.parse_identifier_path()?;
        let operator = self.parse_operator()?;
        let value = self.parse_value()?;

        Ok(Some(WhereClause {
            column,
            operator,
            value,
        }))
    }

    fn parse_optional_join(&mut self) -> Result<Option<JoinClause>> {
        if !matches!(
            self.peek(),
            Some(Token::Keyword(Keyword::Inner)) | Some(Token::Keyword(Keyword::Join))
        ) {
            return Ok(None);
        }

        if matches!(self.peek(), Some(Token::Keyword(Keyword::Inner))) {
            self.bump();
        }
        self.expect_keyword(Keyword::Join)?;
        let right_table = self.parse_identifier_path()?;
        self.expect_keyword(Keyword::On)?;
        let left_column = self.parse_identifier_path()?;
        self.expect_token(Token::Eq)?;
        let right_column = self.parse_identifier_path()?;

        Ok(Some(JoinClause {
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
                )))
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

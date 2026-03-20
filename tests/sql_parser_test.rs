// =============================================================================
// tests/sql_parser_test.rs — SQL Lexer / Parser 測試
// =============================================================================
//
// 這組測試驗證：
// - lexer 是否能正確把 SQL 切成 token
// - parser 是否能正確組出 AST
// - 不合法 SQL 是否有合理錯誤
// - 大小寫不敏感與多餘空白情況

use ferrisdb::sql::ast::{
    AggregateFunc, Assignment, ColumnDef, DataType, GroupByClause, JoinClause, Operator,
    OrderByClause, OrderDirection, SelectColumns, SelectItem, Statement, Value, WhereClause,
};
use ferrisdb::sql::lexer::{Keyword, Lexer, Token};
use ferrisdb::sql::parser::Parser;

#[test]
fn test_lexer_tokenizes_basic_sql() {
    let mut lexer = Lexer::new("SELECT name, age FROM users WHERE id = 1;");
    let tokens = lexer.tokenize().expect("tokenize");

    assert_eq!(
        tokens,
        vec![
            Token::Keyword(Keyword::Select),
            Token::Ident("name".to_string()),
            Token::Comma,
            Token::Ident("age".to_string()),
            Token::Keyword(Keyword::From),
            Token::Ident("users".to_string()),
            Token::Keyword(Keyword::Where),
            Token::Ident("id".to_string()),
            Token::Eq,
            Token::IntLit(1),
            Token::Semicolon,
        ]
    );
}

#[test]
fn test_parse_create_table() {
    let stmt = parse_sql("CREATE TABLE users (id INT, name TEXT, active BOOL);");
    assert_eq!(
        stmt,
        Statement::CreateTable {
            table_name: "users".to_string(),
            columns: vec![
                ColumnDef {
                    name: "id".to_string(),
                    data_type: DataType::Int,
                },
                ColumnDef {
                    name: "name".to_string(),
                    data_type: DataType::Text,
                },
                ColumnDef {
                    name: "active".to_string(),
                    data_type: DataType::Bool,
                },
            ],
        }
    );
}

#[test]
fn test_parse_insert_single_and_multi_rows() {
    let stmt = parse_sql("INSERT INTO users VALUES (1, 'Alice', true), (2, 'Bob', false);");
    assert_eq!(
        stmt,
        Statement::Insert {
            table_name: "users".to_string(),
            values: vec![
                vec![
                    Value::Int(1),
                    Value::Text("Alice".to_string()),
                    Value::Bool(true),
                ],
                vec![
                    Value::Int(2),
                    Value::Text("Bob".to_string()),
                    Value::Bool(false),
                ],
            ],
        }
    );
}

#[test]
fn test_parse_select() {
    let stmt = parse_sql("SELECT name, age FROM users WHERE id = 1;");
    assert_eq!(
        stmt,
        Statement::Select {
            table_name: "users".to_string(),
            columns: SelectColumns::Named(vec!["name".to_string(), "age".to_string()]),
            join: None,
            where_clause: Some(WhereClause {
                column: "id".to_string(),
                operator: Operator::Eq,
                value: Value::Int(1),
            }),
            group_by: None,
            order_by: None,
            limit: None,
        }
    );
}

#[test]
fn test_parse_select_all_and_comparison_operator() {
    let stmt = parse_sql("SELECT * FROM users WHERE age > 25;");
    assert_eq!(
        stmt,
        Statement::Select {
            table_name: "users".to_string(),
            columns: SelectColumns::All,
            join: None,
            where_clause: Some(WhereClause {
                column: "age".to_string(),
                operator: Operator::Gt,
                value: Value::Int(25),
            }),
            group_by: None,
            order_by: None,
            limit: None,
        }
    );
}

#[test]
fn test_parse_update() {
    let stmt = parse_sql("UPDATE users SET name = 'Bob' WHERE id = 1;");
    assert_eq!(
        stmt,
        Statement::Update {
            table_name: "users".to_string(),
            assignments: vec![Assignment {
                column: "name".to_string(),
                value: Value::Text("Bob".to_string()),
            }],
            where_clause: Some(WhereClause {
                column: "id".to_string(),
                operator: Operator::Eq,
                value: Value::Int(1),
            }),
        }
    );
}

#[test]
fn test_parse_delete() {
    let stmt = parse_sql("DELETE FROM users WHERE id = 1;");
    assert_eq!(
        stmt,
        Statement::Delete {
            table_name: "users".to_string(),
            where_clause: Some(WhereClause {
                column: "id".to_string(),
                operator: Operator::Eq,
                value: Value::Int(1),
            }),
        }
    );
}

#[test]
fn test_invalid_sql_returns_meaningful_error() {
    let err = parse_sql_result("SELECT FROM users;").expect_err("should fail");
    assert!(
        format!("{}", err).contains("identifier")
            || format!("{}", err).contains("SQL")
            || format!("{}", err).contains("token"),
        "unexpected error: {}",
        err
    );
}

#[test]
fn test_empty_sql_returns_error() {
    let err = parse_sql_result("   ").expect_err("empty sql should fail");
    assert!(format!("{}", err).contains("empty"));
}

#[test]
fn test_case_insensitive_and_extra_whitespace() {
    let stmt = parse_sql("  sElEcT   *   FrOm   users   ; ");
    assert_eq!(
        stmt,
        Statement::Select {
            table_name: "users".to_string(),
            columns: SelectColumns::All,
            join: None,
            where_clause: None,
            group_by: None,
            order_by: None,
            limit: None,
        }
    );
}

#[test]
fn test_parse_select_with_inner_join() {
    let stmt = parse_sql(
        "SELECT * FROM users INNER JOIN orders ON users.id = orders.user_id WHERE users.id = 1;",
    );
    assert_eq!(
        stmt,
        Statement::Select {
            table_name: "users".to_string(),
            columns: SelectColumns::All,
            join: Some(JoinClause {
                right_table: "orders".to_string(),
                left_column: "users.id".to_string(),
                right_column: "orders.user_id".to_string(),
            }),
            where_clause: Some(WhereClause {
                column: "users.id".to_string(),
                operator: Operator::Eq,
                value: Value::Int(1),
            }),
            group_by: None,
            order_by: None,
            limit: None,
        }
    );
}

#[test]
fn test_parse_explain_select() {
    let stmt = parse_sql("EXPLAIN SELECT * FROM users WHERE id = 1;");
    assert_eq!(
        stmt,
        Statement::Explain {
            statement: Box::new(Statement::Select {
                table_name: "users".to_string(),
                columns: SelectColumns::All,
                join: None,
                where_clause: Some(WhereClause {
                    column: "id".to_string(),
                    operator: Operator::Eq,
                    value: Value::Int(1),
                }),
                group_by: None,
                order_by: None,
                limit: None,
            }),
        }
    );
}

#[test]
fn test_parse_select_with_order_by_and_limit() {
    let stmt = parse_sql(
        "SELECT * FROM users WHERE age > 20 ORDER BY age DESC LIMIT 5;",
    );
    assert_eq!(
        stmt,
        Statement::Select {
            table_name: "users".to_string(),
            columns: SelectColumns::All,
            join: None,
            where_clause: Some(WhereClause {
                column: "age".to_string(),
                operator: Operator::Gt,
                value: Value::Int(20),
            }),
            group_by: None,
            order_by: Some(OrderByClause {
                column: "age".to_string(),
                direction: OrderDirection::Desc,
            }),
            limit: Some(5),
        }
    );
}

#[test]
fn test_parse_select_with_count_and_group_by() {
    let stmt = parse_sql(
        "SELECT age, COUNT(*) FROM users WHERE age > 25 GROUP BY age ORDER BY age DESC LIMIT 3;",
    );
    assert_eq!(
        stmt,
        Statement::Select {
            table_name: "users".to_string(),
            columns: SelectColumns::Aggregate(vec![
                SelectItem::Column("age".to_string()),
                SelectItem::Aggregate {
                    func: AggregateFunc::Count,
                    column: None,
                },
            ]),
            join: None,
            where_clause: Some(WhereClause {
                column: "age".to_string(),
                operator: Operator::Gt,
                value: Value::Int(25),
            }),
            group_by: Some(GroupByClause {
                column: "age".to_string(),
            }),
            order_by: Some(OrderByClause {
                column: "age".to_string(),
                direction: OrderDirection::Desc,
            }),
            limit: Some(3),
        }
    );
}

fn parse_sql(sql: &str) -> Statement {
    parse_sql_result(sql).expect("parse sql")
}

fn parse_sql_result(sql: &str) -> Result<Statement, ferrisdb::error::FerrisDbError> {
    let mut lexer = Lexer::new(sql);
    let tokens = lexer.tokenize()?;
    let mut parser = Parser::new(tokens);
    parser.parse()
}

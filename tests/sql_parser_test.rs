// =============================================================================
// tests/sql_parser_test.rs -- SQL Lexer / Parser 測試
// =============================================================================
//
// 這裡主要驗證：
// - lexer 是否能正確切出 SQL token
// - parser 是否能產生新版 AST
// - WHERE 布林運算、LEFT JOIN、HAVING 等語法是否正確建模

use ferrisdb::sql::ast::{
    AggregateFunc, Assignment, ColumnDef, DataType, GroupByClause, JoinClause, JoinType, Operator,
    OrderByClause, OrderDirection, SelectColumns, SelectItem, Statement, Value, WhereExpr,
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
            if_not_exists: false,
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
fn test_parse_create_and_drop_index() {
    assert_eq!(
        parse_sql("CREATE INDEX ON users(age);"),
        Statement::CreateIndex {
            table_name: "users".to_string(),
            column_name: "age".to_string(),
        }
    );

    assert_eq!(
        parse_sql("DROP INDEX ON users(age);"),
        Statement::DropIndex {
            table_name: "users".to_string(),
            column_name: "age".to_string(),
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
            where_clause: Some(WhereExpr::Comparison {
                column: "id".to_string(),
                operator: Operator::Eq,
                value: Value::Int(1),
            }),
            group_by: None,
            having: None,
            order_by: None,
            limit: None,
        }
    );
}

#[test]
fn test_parse_where_and_or_not() {
    let stmt = parse_sql(
        "SELECT * FROM users WHERE id = 1 AND (age > 20 OR NOT active = false);",
    );
    assert_eq!(
        stmt,
        Statement::Select {
            table_name: "users".to_string(),
            columns: SelectColumns::All,
            join: None,
            where_clause: Some(WhereExpr::And(
                Box::new(WhereExpr::Comparison {
                    column: "id".to_string(),
                    operator: Operator::Eq,
                    value: Value::Int(1),
                }),
                Box::new(WhereExpr::Or(
                    Box::new(WhereExpr::Comparison {
                        column: "age".to_string(),
                        operator: Operator::Gt,
                        value: Value::Int(20),
                    }),
                    Box::new(WhereExpr::Not(Box::new(WhereExpr::Comparison {
                        column: "active".to_string(),
                        operator: Operator::Eq,
                        value: Value::Bool(false),
                    }))),
                )),
            )),
            group_by: None,
            having: None,
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
            where_clause: Some(WhereExpr::Comparison {
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
            where_clause: Some(WhereExpr::Comparison {
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
            having: None,
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
                join_type: JoinType::Inner,
                right_table: "orders".to_string(),
                left_column: "users.id".to_string(),
                right_column: "orders.user_id".to_string(),
            }),
            where_clause: Some(WhereExpr::Comparison {
                column: "users.id".to_string(),
                operator: Operator::Eq,
                value: Value::Int(1),
            }),
            group_by: None,
            having: None,
            order_by: None,
            limit: None,
        }
    );
}

#[test]
fn test_parse_left_join() {
    let stmt = parse_sql("SELECT * FROM users LEFT JOIN orders ON users.id = orders.user_id;");
    assert_eq!(
        stmt,
        Statement::Select {
            table_name: "users".to_string(),
            columns: SelectColumns::All,
            join: Some(JoinClause {
                join_type: JoinType::Left,
                right_table: "orders".to_string(),
                left_column: "users.id".to_string(),
                right_column: "orders.user_id".to_string(),
            }),
            where_clause: None,
            group_by: None,
            having: None,
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
                where_clause: Some(WhereExpr::Comparison {
                    column: "id".to_string(),
                    operator: Operator::Eq,
                    value: Value::Int(1),
                }),
                group_by: None,
                having: None,
                order_by: None,
                limit: None,
            }),
        }
    );
}

#[test]
fn test_parse_select_with_order_by_and_limit() {
    let stmt = parse_sql("SELECT * FROM users WHERE age > 20 ORDER BY age DESC LIMIT 5;");
    assert_eq!(
        stmt,
        Statement::Select {
            table_name: "users".to_string(),
            columns: SelectColumns::All,
            join: None,
            where_clause: Some(WhereExpr::Comparison {
                column: "age".to_string(),
                operator: Operator::Gt,
                value: Value::Int(20),
            }),
            group_by: None,
            having: None,
            order_by: Some(OrderByClause {
                column: "age".to_string(),
                direction: OrderDirection::Desc,
            }),
            limit: Some(5),
        }
    );
}

#[test]
fn test_parse_select_with_count_group_by_and_having() {
    let stmt = parse_sql(
        "SELECT age, COUNT(*) FROM users WHERE age > 25 GROUP BY age HAVING COUNT(*) > 1 ORDER BY age DESC LIMIT 3;",
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
            where_clause: Some(WhereExpr::Comparison {
                column: "age".to_string(),
                operator: Operator::Gt,
                value: Value::Int(25),
            }),
            group_by: Some(GroupByClause {
                column: "age".to_string(),
            }),
            having: Some(WhereExpr::Comparison {
                column: "COUNT(*)".to_string(),
                operator: Operator::Gt,
                value: Value::Int(1),
            }),
            order_by: Some(OrderByClause {
                column: "age".to_string(),
                direction: OrderDirection::Desc,
            }),
            limit: Some(3),
        }
    );
}

#[test]
fn test_parse_create_if_not_exists_alter_drop_table_and_subquery() {
    assert_eq!(
        parse_sql("CREATE TABLE IF NOT EXISTS users (id INT, name TEXT);"),
        Statement::CreateTable {
            table_name: "users".to_string(),
            if_not_exists: true,
            columns: vec![
                ColumnDef {
                    name: "id".to_string(),
                    data_type: DataType::Int,
                },
                ColumnDef {
                    name: "name".to_string(),
                    data_type: DataType::Text,
                },
            ],
        }
    );

    assert_eq!(
        parse_sql("ALTER TABLE users ADD COLUMN email TEXT;"),
        Statement::AlterTableAdd {
            table_name: "users".to_string(),
            column: ColumnDef {
                name: "email".to_string(),
                data_type: DataType::Text,
            },
        }
    );

    assert_eq!(
        parse_sql("ALTER TABLE users DROP COLUMN age;"),
        Statement::AlterTableDropColumn {
            table_name: "users".to_string(),
            column_name: "age".to_string(),
        }
    );

    assert_eq!(
        parse_sql("DROP TABLE IF EXISTS users;"),
        Statement::DropTable {
            table_name: "users".to_string(),
            if_exists: true,
        }
    );

    assert_eq!(
        parse_sql("SELECT * FROM users WHERE id IN (SELECT user_id FROM orders);"),
        Statement::Select {
            table_name: "users".to_string(),
            columns: SelectColumns::All,
            join: None,
            where_clause: Some(WhereExpr::InSubquery {
                column: "id".to_string(),
                subquery: Box::new(Statement::Select {
                    table_name: "orders".to_string(),
                    columns: SelectColumns::Named(vec!["user_id".to_string()]),
                    join: None,
                    where_clause: None,
                    group_by: None,
                    having: None,
                    order_by: None,
                    limit: None,
                }),
            }),
            group_by: None,
            having: None,
            order_by: None,
            limit: None,
        }
    );
}

// 中文註解：測試內統一走 lexer + parser，避免每個案例重複樣板。
fn parse_sql(sql: &str) -> Statement {
    parse_sql_result(sql).expect("parse sql")
}

// 中文註解：保留 Result 版 helper，方便直接測錯誤訊息。
fn parse_sql_result(sql: &str) -> Result<Statement, ferrisdb::error::FerrisDbError> {
    let mut lexer = Lexer::new(sql);
    let tokens = lexer.tokenize()?;
    let mut parser = Parser::new(tokens);
    parser.parse()
}

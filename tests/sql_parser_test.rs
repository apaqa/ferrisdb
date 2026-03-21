// =============================================================================
// tests/sql_parser_test.rs -- SQL Lexer / Parser 皜祈岫
// =============================================================================
//
// ?ㄐ銝餉?撽?嚗?// - lexer ?臬?賣迤蝣箏???SQL token
// - parser ?臬?賜???AST
// - WHERE 撣????EFT JOIN?AVING 蝑?瘜?行迤蝣箏遣璅?
use ferrisdb::sql::ast::{
    AggregateFunc, Assignment, CTE, ColumnDef, DataType, ForeignKey, GroupByClause, InsertSource,
    IsolationLevel, JoinClause, JoinType, Operator, OrderByClause, OrderDirection,
    ProcedureParam, SelectColumns, SelectItem, Statement, Value, WhereExpr,
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
            foreign_keys: vec![],
        }
    );
}

#[test]
fn test_parse_create_and_drop_index() {
    assert_eq!(
        parse_sql("CREATE INDEX ON users(age);"),
        Statement::CreateIndex {
            table_name: "users".to_string(),
            column_names: vec!["age".to_string()],
        }
    );

    assert_eq!(
        parse_sql("DROP INDEX ON users(age);"),
        Statement::DropIndex {
            table_name: "users".to_string(),
            column_names: vec!["age".to_string()],
        }
    );
}

#[test]
fn test_parse_create_composite_index() {
    assert_eq!(
        parse_sql("CREATE INDEX ON employees(department, salary);"),
        Statement::CreateIndex {
            table_name: "employees".to_string(),
            column_names: vec!["department".to_string(), "salary".to_string()],
        }
    );
}

#[test]
fn test_parse_analyze_table() {
    assert_eq!(
        parse_sql("ANALYZE TABLE employees;"),
        Statement::AnalyzeTable {
            table_name: "employees".to_string(),
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
            source: InsertSource::Values(vec![
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
            ]),
        }
    );
}

#[test]
fn test_parse_select() {
    let stmt = parse_sql("SELECT name, age FROM users WHERE id = 1;");
    assert_eq!(
        stmt,
        Statement::Select {
            ctes: vec![],
            distinct: false,
            table_name: "users".to_string(),
            table_alias: None,
            columns: SelectColumns::Named(vec![
                SelectItem::Column {
                    name: "name".to_string(),
                    alias: None,
                },
                SelectItem::Column {
                    name: "age".to_string(),
                    alias: None,
                },
            ]),
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
    let stmt = parse_sql("SELECT * FROM users WHERE id = 1 AND (age > 20 OR NOT active = false);");
    assert_eq!(
        stmt,
        Statement::Select {
            ctes: vec![],
            distinct: false,
            table_name: "users".to_string(),
            table_alias: None,
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
            from_table: None,
            join_condition: None,
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
            using_table: None,
            join_condition: None,
            where_clause: Some(WhereExpr::Comparison {
                column: "id".to_string(),
                operator: Operator::Eq,
                value: Value::Int(1),
            }),
        }
    );
}

#[test]
fn test_parse_select_with_ctes() {
    assert_eq!(
        parse_sql(
            "WITH a AS (SELECT * FROM users), b AS (SELECT * FROM admins) SELECT * FROM a;"
        ),
        Statement::Select {
            ctes: vec![
                CTE {
                    name: "a".to_string(),
                    query: Box::new(Statement::Select {
                        ctes: vec![],
                        distinct: false,
                        table_name: "users".to_string(),
                        table_alias: None,
                        columns: SelectColumns::All,
                        join: None,
                        where_clause: None,
                        group_by: None,
                        having: None,
                        order_by: None,
                        limit: None,
                    }),
                },
                CTE {
                    name: "b".to_string(),
                    query: Box::new(Statement::Select {
                        ctes: vec![],
                        distinct: false,
                        table_name: "admins".to_string(),
                        table_alias: None,
                        columns: SelectColumns::All,
                        join: None,
                        where_clause: None,
                        group_by: None,
                        having: None,
                        order_by: None,
                        limit: None,
                    }),
                },
            ],
            distinct: false,
            table_name: "a".to_string(),
            table_alias: None,
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
fn test_parse_update_from_and_delete_using() {
    assert_eq!(
        parse_sql(
            "UPDATE employees SET salary = 0 FROM departments WHERE employees.department = departments.dept_name AND departments.location = 'Remote';"
        ),
        Statement::Update {
            table_name: "employees".to_string(),
            assignments: vec![Assignment {
                column: "salary".to_string(),
                value: Value::Int(0),
            }],
            from_table: Some("departments".to_string()),
            join_condition: Some(WhereExpr::And(
                Box::new(WhereExpr::ColumnComparison {
                    left: "employees.department".to_string(),
                    operator: Operator::Eq,
                    right: "departments.dept_name".to_string(),
                }),
                Box::new(WhereExpr::Comparison {
                    column: "departments.location".to_string(),
                    operator: Operator::Eq,
                    value: Value::Text("Remote".to_string()),
                }),
            )),
            where_clause: Some(WhereExpr::And(
                Box::new(WhereExpr::ColumnComparison {
                    left: "employees.department".to_string(),
                    operator: Operator::Eq,
                    right: "departments.dept_name".to_string(),
                }),
                Box::new(WhereExpr::Comparison {
                    column: "departments.location".to_string(),
                    operator: Operator::Eq,
                    value: Value::Text("Remote".to_string()),
                }),
            )),
        }
    );

    assert_eq!(
        parse_sql(
            "DELETE FROM employees USING departments WHERE employees.department = departments.dept_name AND departments.location = 'Remote';"
        ),
        Statement::Delete {
            table_name: "employees".to_string(),
            using_table: Some("departments".to_string()),
            join_condition: Some(WhereExpr::And(
                Box::new(WhereExpr::ColumnComparison {
                    left: "employees.department".to_string(),
                    operator: Operator::Eq,
                    right: "departments.dept_name".to_string(),
                }),
                Box::new(WhereExpr::Comparison {
                    column: "departments.location".to_string(),
                    operator: Operator::Eq,
                    value: Value::Text("Remote".to_string()),
                }),
            )),
            where_clause: Some(WhereExpr::And(
                Box::new(WhereExpr::ColumnComparison {
                    left: "employees.department".to_string(),
                    operator: Operator::Eq,
                    right: "departments.dept_name".to_string(),
                }),
                Box::new(WhereExpr::Comparison {
                    column: "departments.location".to_string(),
                    operator: Operator::Eq,
                    value: Value::Text("Remote".to_string()),
                }),
            )),
        }
    );
}

#[test]
fn test_invalid_sql_returns_meaningful_error() {
    let err = parse_sql_result("SELECT FROM users;").expect_err("should fail");
    assert!(
        format!("{}", err).contains("identifier")
            || format!("{}", err).contains("SQL")
            || format!("{}", err).contains("token")
            || format!("{}", err).contains("keyword"),
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
            ctes: vec![],
            distinct: false,
            table_name: "users".to_string(),
            table_alias: None,
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
            ctes: vec![],
            distinct: false,
            table_name: "users".to_string(),
            table_alias: None,
            columns: SelectColumns::All,
            join: Some(JoinClause {
                join_type: JoinType::Inner,
                right_table: "orders".to_string(),
                right_alias: None,
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
            ctes: vec![],
            distinct: false,
            table_name: "users".to_string(),
            table_alias: None,
            columns: SelectColumns::All,
            join: Some(JoinClause {
                join_type: JoinType::Left,
                right_table: "orders".to_string(),
                right_alias: None,
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
            ctes: vec![],
                distinct: false,
                table_name: "users".to_string(),
                table_alias: None,
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
            ctes: vec![],
            distinct: false,
            table_name: "users".to_string(),
            table_alias: None,
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
                expr: None,
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
            ctes: vec![],
            distinct: false,
            table_name: "users".to_string(),
            table_alias: None,
            columns: SelectColumns::Aggregate(vec![
                SelectItem::Column {
                    name: "age".to_string(),
                    alias: None,
                },
                SelectItem::Aggregate {
                    func: AggregateFunc::Count,
                    column: None,
                    alias: None,
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
                expr: None,
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
            foreign_keys: vec![],
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
            ctes: vec![],
            distinct: false,
            table_name: "users".to_string(),
            table_alias: None,
            columns: SelectColumns::All,
            join: None,
            where_clause: Some(WhereExpr::InSubquery {
                column: "id".to_string(),
                subquery: Box::new(Statement::Select {
            ctes: vec![],
                    distinct: false,
                    table_name: "orders".to_string(),
                    table_alias: None,
                    columns: SelectColumns::Named(vec![SelectItem::Column {
                        name: "user_id".to_string(),
                        alias: None,
                    }]),
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

#[test]
fn test_parse_select_distinct_and_alias() {
    let stmt = parse_sql("SELECT DISTINCT name AS employee_name FROM employees AS e;");
    assert_eq!(
        stmt,
        Statement::Select {
            ctes: vec![],
            distinct: true,
            table_name: "employees".to_string(),
            table_alias: Some("e".to_string()),
            columns: SelectColumns::Named(vec![SelectItem::Column {
                name: "name".to_string(),
                alias: Some("employee_name".to_string()),
            }]),
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
fn test_parse_aggregate_alias_and_predicates() {
    let stmt = parse_sql(
        "SELECT COUNT(*) AS total FROM employees WHERE salary BETWEEN 10 AND 20 AND name LIKE 'A%' AND department IS NOT NULL;",
    );
    assert_eq!(
        stmt,
        Statement::Select {
            ctes: vec![],
            distinct: false,
            table_name: "employees".to_string(),
            table_alias: None,
            columns: SelectColumns::Aggregate(vec![SelectItem::Aggregate {
                func: AggregateFunc::Count,
                column: None,
                alias: Some("total".to_string()),
            }]),
            join: None,
            where_clause: Some(WhereExpr::And(
                Box::new(WhereExpr::And(
                    Box::new(WhereExpr::Between {
                        column: "salary".to_string(),
                        low: Value::Int(10),
                        high: Value::Int(20),
                    }),
                    Box::new(WhereExpr::Like {
                        column: "name".to_string(),
                        pattern: "A%".to_string(),
                    }),
                )),
                Box::new(WhereExpr::IsNull {
                    column: "department".to_string(),
                    negated: true,
                }),
            )),
            group_by: None,
            having: None,
            order_by: None,
            limit: None,
        }
    );
}

#[test]
fn test_parse_multiple_skips_empty_statements() {
    let statements = Parser::parse_multiple(
        " ; SELECT DISTINCT name FROM users;;SELECT COUNT(*) AS total FROM users; ; ",
    )
    .expect("parse multiple");

    assert_eq!(statements.len(), 2);
}

#[test]
fn test_parse_create_drop_view_insert_select_and_union() {
    assert_eq!(
        parse_sql("CREATE VIEW high_earners AS SELECT * FROM employees WHERE salary > 80000;"),
        Statement::CreateView {
            view_name: "high_earners".to_string(),
            query_sql: "SELECT * FROM employees WHERE salary > 80000".to_string(),
            query: Box::new(Statement::Select {
            ctes: vec![],
                distinct: false,
                table_name: "employees".to_string(),
                table_alias: None,
                columns: SelectColumns::All,
                join: None,
                where_clause: Some(WhereExpr::Comparison {
                    column: "salary".to_string(),
                    operator: Operator::Gt,
                    value: Value::Int(80000),
                }),
                group_by: None,
                having: None,
                order_by: None,
                limit: None,
            }),
        }
    );

    assert_eq!(
        parse_sql(
            "CREATE MATERIALIZED VIEW high_earners_cache AS SELECT * FROM employees WHERE salary > 80000;"
        ),
        Statement::CreateMaterializedView {
            view_name: "high_earners_cache".to_string(),
            query_sql: "SELECT * FROM employees WHERE salary > 80000".to_string(),
            query: Box::new(Statement::Select {
            ctes: vec![],
                distinct: false,
                table_name: "employees".to_string(),
                table_alias: None,
                columns: SelectColumns::All,
                join: None,
                where_clause: Some(WhereExpr::Comparison {
                    column: "salary".to_string(),
                    operator: Operator::Gt,
                    value: Value::Int(80000),
                }),
                group_by: None,
                having: None,
                order_by: None,
                limit: None,
            }),
        }
    );

    assert_eq!(
        parse_sql("REFRESH MATERIALIZED VIEW high_earners_cache;"),
        Statement::RefreshMaterializedView {
            view_name: "high_earners_cache".to_string(),
        }
    );

    assert_eq!(
        parse_sql("DROP MATERIALIZED VIEW IF EXISTS high_earners_cache;"),
        Statement::DropMaterializedView {
            view_name: "high_earners_cache".to_string(),
            if_exists: true,
        }
    );

    assert_eq!(
        parse_sql("DROP VIEW IF EXISTS high_earners;"),
        Statement::DropView {
            view_name: "high_earners".to_string(),
            if_exists: true,
        }
    );

    assert_eq!(
        parse_sql("INSERT INTO archived_users SELECT name, age FROM users WHERE age > 25;"),
        Statement::Insert {
            table_name: "archived_users".to_string(),
            source: InsertSource::Select(Box::new(Statement::Select {
            ctes: vec![],
                distinct: false,
                table_name: "users".to_string(),
                table_alias: None,
                columns: SelectColumns::Named(vec![
                    SelectItem::Column {
                        name: "name".to_string(),
                        alias: None,
                    },
                    SelectItem::Column {
                        name: "age".to_string(),
                        alias: None,
                    },
                ]),
                join: None,
                where_clause: Some(WhereExpr::Comparison {
                    column: "age".to_string(),
                    operator: Operator::Gt,
                    value: Value::Int(25),
                }),
                group_by: None,
                having: None,
                order_by: None,
                limit: None,
            })),
        }
    );

    assert_eq!(
        parse_sql("SELECT name FROM employees UNION ALL SELECT dept_name FROM departments;"),
        Statement::Union {
            left: Box::new(Statement::Select {
            ctes: vec![],
                distinct: false,
                table_name: "employees".to_string(),
                table_alias: None,
                columns: SelectColumns::Named(vec![SelectItem::Column {
                    name: "name".to_string(),
                    alias: None,
                }]),
                join: None,
                where_clause: None,
                group_by: None,
                having: None,
                order_by: None,
                limit: None,
            }),
            right: Box::new(Statement::Select {
            ctes: vec![],
                distinct: false,
                table_name: "departments".to_string(),
                table_alias: None,
                columns: SelectColumns::Named(vec![SelectItem::Column {
                    name: "dept_name".to_string(),
                    alias: None,
                }]),
                join: None,
                where_clause: None,
                group_by: None,
                having: None,
                order_by: None,
                limit: None,
            }),
            all: true,
        }
    );
}

#[test]
fn test_parse_create_table_with_foreign_key() {
    assert_eq!(
        parse_sql(
            "CREATE TABLE orders (id INT, customer_id INT, FOREIGN KEY (customer_id) REFERENCES customers(id));"
        ),
        Statement::CreateTable {
            table_name: "orders".to_string(),
            if_not_exists: false,
            columns: vec![
                ColumnDef {
                    name: "id".to_string(),
                    data_type: DataType::Int,
                },
                ColumnDef {
                    name: "customer_id".to_string(),
                    data_type: DataType::Int,
                },
            ],
            foreign_keys: vec![ForeignKey {
                columns: vec!["customer_id".to_string()],
                ref_table: "customers".to_string(),
                ref_columns: vec!["id".to_string()],
            }],
        }
    );
}

#[test]
fn test_parse_prepare_execute_deallocate_and_isolation_level() {
    assert_eq!(
        parse_sql(
            "PREPARE dept_stmt AS SELECT * FROM employees WHERE department = $1 AND salary > $2;"
        ),
        Statement::Prepare {
            name: "dept_stmt".to_string(),
            params: vec!["$1".to_string(), "$2".to_string()],
            body: Box::new(Statement::Select {
                ctes: vec![],
                distinct: false,
                table_name: "employees".to_string(),
                table_alias: None,
                columns: SelectColumns::All,
                join: None,
                where_clause: Some(WhereExpr::And(
                    Box::new(WhereExpr::PlaceholderComparison {
                        column: "department".to_string(),
                        operator: Operator::Eq,
                        placeholder: 1,
                    }),
                    Box::new(WhereExpr::PlaceholderComparison {
                        column: "salary".to_string(),
                        operator: Operator::Gt,
                        placeholder: 2,
                    }),
                )),
                group_by: None,
                having: None,
                order_by: None,
                limit: None,
            }),
        }
    );

    assert_eq!(
        parse_sql("EXECUTE dept_stmt('Engineering', 80000);"),
        Statement::Execute {
            name: "dept_stmt".to_string(),
            args: vec![Value::Text("Engineering".to_string()), Value::Int(80000)],
        }
    );

    assert_eq!(
        parse_sql("DEALLOCATE dept_stmt;"),
        Statement::Deallocate {
            name: "dept_stmt".to_string(),
        }
    );

    assert_eq!(
        parse_sql("SET TRANSACTION ISOLATION LEVEL READ COMMITTED;"),
        Statement::SetIsolationLevel {
            level: IsolationLevel::ReadCommitted,
        }
    );
    assert_eq!(
        parse_sql("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ;"),
        Statement::SetIsolationLevel {
            level: IsolationLevel::RepeatableRead,
        }
    );
    assert_eq!(
        parse_sql("SET TRANSACTION ISOLATION LEVEL SERIALIZABLE;"),
        Statement::SetIsolationLevel {
            level: IsolationLevel::Serializable,
        }
    );
}

#[test]
fn test_parse_create_call_and_drop_procedure() {
    assert_eq!(
        parse_sql(
            "CREATE PROCEDURE add_user(user_id INT, user_name TEXT) BEGIN DECLARE i INT; SET i = user_id; INSERT INTO users VALUES (i, user_name); END;"
        ),
        Statement::CreateProcedure {
            name: "add_user".to_string(),
            params: vec![
                ProcedureParam {
                    name: "user_id".to_string(),
                    data_type: DataType::Int,
                },
                ProcedureParam {
                    name: "user_name".to_string(),
                    data_type: DataType::Text,
                },
            ],
            body: vec![
                Statement::DeclareVariable {
                    name: "i".to_string(),
                    data_type: DataType::Int,
                },
                Statement::SetVariable {
                    name: "i".to_string(),
                    value: ferrisdb::sql::ast::Expr::Variable("user_id".to_string()),
                },
                Statement::Insert {
                    table_name: "users".to_string(),
                    source: InsertSource::Values(vec![vec![
                        Value::Variable("i".to_string()),
                        Value::Variable("user_name".to_string()),
                    ]]),
                },
            ],
        }
    );

    assert_eq!(
        parse_sql("CALL add_user(1, 'Alice');"),
        Statement::CallProcedure {
            name: "add_user".to_string(),
            args: vec![Value::Int(1), Value::Text("Alice".to_string())],
        }
    );

    assert_eq!(
        parse_sql("DROP PROCEDURE add_user;"),
        Statement::DropProcedure {
            name: "add_user".to_string(),
        }
    );
}

#[test]
fn test_parse_if_while_and_cursor_statements() {
    assert_eq!(
        parse_sql(
            "IF counter > 0 THEN SET counter = counter; ELSE SET counter = 0; END IF;"
        ),
        Statement::IfThenElse {
            condition: WhereExpr::Comparison {
                column: "counter".to_string(),
                operator: Operator::Gt,
                value: Value::Int(0),
            },
            then_body: vec![Statement::SetVariable {
                name: "counter".to_string(),
                value: ferrisdb::sql::ast::Expr::Variable("counter".to_string()),
            }],
            else_body: vec![Statement::SetVariable {
                name: "counter".to_string(),
                value: ferrisdb::sql::ast::Expr::Value(Value::Int(0)),
            }],
        }
    );

    assert_eq!(
        parse_sql("WHILE counter < 3 DO SET counter = counter; END WHILE;"),
        Statement::WhileDo {
            condition: WhereExpr::Comparison {
                column: "counter".to_string(),
                operator: Operator::Lt,
                value: Value::Int(3),
            },
            body: vec![Statement::SetVariable {
                name: "counter".to_string(),
                value: ferrisdb::sql::ast::Expr::Variable("counter".to_string()),
            }],
        }
    );

    assert_eq!(
        parse_sql("DECLARE user_cursor CURSOR FOR SELECT id, name FROM users;"),
        Statement::DeclareCursor {
            name: "user_cursor".to_string(),
            query: Box::new(Statement::Select {
                ctes: vec![],
                distinct: false,
                table_name: "users".to_string(),
                table_alias: None,
                columns: SelectColumns::Named(vec![
                    SelectItem::Column {
                        name: "id".to_string(),
                        alias: None,
                    },
                    SelectItem::Column {
                        name: "name".to_string(),
                        alias: None,
                    },
                ]),
                join: None,
                where_clause: None,
                group_by: None,
                having: None,
                order_by: None,
                limit: None,
            }),
        }
    );

    assert_eq!(
        parse_sql("FETCH NEXT FROM user_cursor INTO id_var, name_var;"),
        Statement::FetchCursor {
            name: "user_cursor".to_string(),
            variables: vec!["id_var".to_string(), "name_var".to_string()],
        }
    );
}

// 中文註解：把 lexer + parser 封裝成 helper，讓測試更容易閱讀。
fn parse_sql(sql: &str) -> Statement {
    parse_sql_result(sql).expect("parse sql")
}

// 中文註解：保留 Result 版本，讓錯誤訊息測試可以直接斷言 parser 回傳的錯誤。
fn parse_sql_result(sql: &str) -> Result<Statement, ferrisdb::error::FerrisDbError> {
    let mut lexer = Lexer::new(sql);
    let tokens = lexer.tokenize()?;
    let mut parser = Parser::new(tokens);
    parser.parse()
}




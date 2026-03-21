// =============================================================================
// tests/sql_executor_test.rs -- SQL Executor Integration Tests
// =============================================================================
//
// 這些測試驗證 SQL Executor 是否真的把 AST 轉成底層 KV 操作：
// - schema 是否能正確存進 catalog
// - row 是否能正確寫入 / 讀出
// - WHERE / UPDATE / DELETE 是否能依條件作用
// - engine 重啟後 schema 與 row 是否仍然存在

use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use ferrisdb::sql::ast::Value;
use ferrisdb::sql::executor::{ExecuteResult, SqlExecutor};
use ferrisdb::sql::lexer::Lexer;
use ferrisdb::sql::parser::Parser;
use ferrisdb::storage::lsm::LsmEngine;
use ferrisdb::transaction::mvcc::MvccEngine;

fn temp_dir(name: &str) -> std::path::PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time")
        .as_nanos();
    std::env::temp_dir().join(format!("ferrisdb-sql-executor-{}-{}", name, nanos))
}

fn open_executor(name: &str) -> (std::path::PathBuf, Arc<MvccEngine>, SqlExecutor) {
    let dir = temp_dir(name);
    let lsm = LsmEngine::open(&dir, 4096).expect("open lsm");
    let engine = Arc::new(MvccEngine::new(lsm));
    let executor = SqlExecutor::new(Arc::clone(&engine));
    (dir, engine, executor)
}

fn exec(executor: &SqlExecutor, sql: &str) -> ExecuteResult {
    let mut lexer = Lexer::new(sql);
    let tokens = lexer.tokenize().expect("tokenize sql");
    let mut parser = Parser::new(tokens);
    let stmt = parser.parse().expect("parse sql");
    executor.execute(stmt).expect("execute sql")
}

fn rows_only(result: ExecuteResult) -> (Vec<String>, Vec<Vec<Value>>) {
    match result {
        ExecuteResult::Selected { columns, rows } => (columns, rows),
        other => panic!("expected selected result, got {:?}", other),
    }
}

// 中文註解：把 EXPLAIN 結果抽成字串，方便直接驗證是否走到 IndexScan/Filter。
fn explain_only(result: ExecuteResult) -> String {
    match result {
        ExecuteResult::Explain { plan, .. } => plan,
        other => panic!("expected explain result, got {:?}", other),
    }
}

#[test]
fn test_create_insert_select_flow() {
    let (dir, _engine, executor) = open_executor("create-insert-select");

    assert_eq!(
        exec(
            &executor,
            "CREATE TABLE users (id INT, name TEXT, age INT, active BOOL);"
        ),
        ExecuteResult::Created {
            table_name: "users".to_string()
        }
    );

    assert_eq!(
        exec(
            &executor,
            "INSERT INTO users VALUES (1, 'Alice', 30, true), (2, 'Bob', 25, false);"
        ),
        ExecuteResult::Inserted { count: 2 }
    );

    let (columns, rows) = rows_only(exec(&executor, "SELECT * FROM users;"));
    assert_eq!(columns, vec!["id", "name", "age", "active"]);
    assert_eq!(
        rows,
        vec![
            vec![
                Value::Int(1),
                Value::Text("Alice".to_string()),
                Value::Int(30),
                Value::Bool(true),
            ],
            vec![
                Value::Int(2),
                Value::Text("Bob".to_string()),
                Value::Int(25),
                Value::Bool(false),
            ],
        ]
    );

    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn test_select_named_columns_and_where_operators() {
    let (dir, _engine, executor) = open_executor("select-where");

    exec(
        &executor,
        "CREATE TABLE users (id INT, name TEXT, age INT, active BOOL);",
    );
    exec(
        &executor,
        "INSERT INTO users VALUES (1, 'Alice', 30, true), (2, 'Bob', 25, false), (3, 'Cara', 35, true);",
    );

    let (columns_eq, rows_eq) = rows_only(exec(
        &executor,
        "SELECT name, age FROM users WHERE id = 1;",
    ));
    assert_eq!(columns_eq, vec!["name", "age"]);
    assert_eq!(
        rows_eq,
        vec![vec![Value::Text("Alice".to_string()), Value::Int(30)]]
    );

    let (_, rows_ne) = rows_only(exec(
        &executor,
        "SELECT name FROM users WHERE name != 'Bob';",
    ));
    assert_eq!(rows_ne.len(), 2);

    let (_, rows_lt) = rows_only(exec(
        &executor,
        "SELECT name FROM users WHERE age < 30;",
    ));
    assert_eq!(rows_lt, vec![vec![Value::Text("Bob".to_string())]]);

    let (_, rows_gt) = rows_only(exec(
        &executor,
        "SELECT name FROM users WHERE age > 30;",
    ));
    assert_eq!(rows_gt, vec![vec![Value::Text("Cara".to_string())]]);

    let (_, rows_le) = rows_only(exec(
        &executor,
        "SELECT name FROM users WHERE age <= 25;",
    ));
    assert_eq!(rows_le, vec![vec![Value::Text("Bob".to_string())]]);

    let (_, rows_ge) = rows_only(exec(
        &executor,
        "SELECT name FROM users WHERE age >= 30;",
    ));
    assert_eq!(
        rows_ge,
        vec![
            vec![Value::Text("Alice".to_string())],
            vec![Value::Text("Cara".to_string())],
        ]
    );

    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn test_update_and_delete() {
    let (dir, _engine, executor) = open_executor("update-delete");

    exec(
        &executor,
        "CREATE TABLE users (id INT, name TEXT, age INT, active BOOL);",
    );
    exec(
        &executor,
        "INSERT INTO users VALUES (1, 'Alice', 30, true), (2, 'Bob', 25, false);",
    );

    assert_eq!(
        exec(&executor, "UPDATE users SET name = 'Bobby' WHERE id = 2;"),
        ExecuteResult::Updated { count: 1 }
    );
    let (_, rows_after_update) =
        rows_only(exec(&executor, "SELECT name FROM users WHERE id = 2;"));
    assert_eq!(
        rows_after_update,
        vec![vec![Value::Text("Bobby".to_string())]]
    );

    assert_eq!(
        exec(&executor, "DELETE FROM users WHERE id = 1;"),
        ExecuteResult::Deleted { count: 1 }
    );
    let (_, rows_after_delete) = rows_only(exec(&executor, "SELECT * FROM users;"));
    assert_eq!(
        rows_after_delete,
        vec![vec![
            Value::Int(2),
            Value::Text("Bobby".to_string()),
            Value::Int(25),
            Value::Bool(false),
        ]]
    );

    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn test_sql_errors_are_reported() {
    let (dir, _engine, executor) = open_executor("errors");

    assert_eq!(
        exec(
            &executor,
            "INSERT INTO missing VALUES (1, 'Alice', 30, true);"
        ),
        ExecuteResult::Error {
            message: "table 'missing' does not exist".to_string(),
        }
    );

    exec(
        &executor,
        "CREATE TABLE users (id INT, name TEXT, age INT, active BOOL);",
    );

    assert_eq!(
        exec(
            &executor,
            "CREATE TABLE users (id INT, name TEXT, age INT, active BOOL);"
        ),
        ExecuteResult::Error {
            message: "table 'users' already exists".to_string(),
        }
    );

    assert_eq!(
        exec(&executor, "INSERT INTO users VALUES (1, 'Alice');"),
        ExecuteResult::Error {
            message: "INSERT expected 4 values for table 'users', got 2".to_string(),
        }
    );

    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn test_schema_and_rows_survive_reopen() {
    let dir = temp_dir("reopen");

    {
        let lsm = LsmEngine::open(&dir, 4096).expect("open lsm first");
        let engine = Arc::new(MvccEngine::new(lsm));
        let executor = SqlExecutor::new(Arc::clone(&engine));

        exec(
            &executor,
            "CREATE TABLE users (id INT, name TEXT, age INT, active BOOL);",
        );
        exec(
            &executor,
            "INSERT INTO users VALUES (1, 'Alice', 30, true), (2, 'Bob', 25, false);",
        );

        engine.shutdown().expect("shutdown engine");
    }

    {
        let lsm = LsmEngine::open(&dir, 4096).expect("reopen lsm");
        let engine = Arc::new(MvccEngine::new(lsm));
        let executor = SqlExecutor::new(Arc::clone(&engine));

        let (columns, rows) = rows_only(exec(&executor, "SELECT * FROM users;"));
        assert_eq!(columns, vec!["id", "name", "age", "active"]);
        assert_eq!(rows.len(), 2);
    }

    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn test_inner_join_between_two_tables() {
    let (dir, _engine, executor) = open_executor("join-basic");

    exec(
        &executor,
        "CREATE TABLE users (id INT, name TEXT, active BOOL);",
    );
    exec(
        &executor,
        "CREATE TABLE orders (id INT, user_id INT, item TEXT);",
    );
    exec(
        &executor,
        "INSERT INTO users VALUES (1, 'Alice', true), (2, 'Bob', false);",
    );
    exec(
        &executor,
        "INSERT INTO orders VALUES (10, 1, 'Book'), (11, 1, 'Pen'), (12, 2, 'Cup');",
    );

    let (columns, rows) = rows_only(exec(
        &executor,
        "SELECT * FROM users INNER JOIN orders ON users.id = orders.user_id;",
    ));
    assert_eq!(
        columns,
        vec![
            "users.id",
            "users.name",
            "users.active",
            "orders.id",
            "orders.user_id",
            "orders.item",
        ]
    );
    assert_eq!(rows.len(), 3);

    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn test_where_with_join_filters_rows() {
    let (dir, _engine, executor) = open_executor("join-where");

    exec(
        &executor,
        "CREATE TABLE users (id INT, name TEXT, active BOOL);",
    );
    exec(
        &executor,
        "CREATE TABLE orders (id INT, user_id INT, item TEXT);",
    );
    exec(
        &executor,
        "INSERT INTO users VALUES (1, 'Alice', true), (2, 'Bob', false);",
    );
    exec(
        &executor,
        "INSERT INTO orders VALUES (10, 1, 'Book'), (11, 2, 'Cup');",
    );

    let (columns, rows) = rows_only(exec(
        &executor,
        "SELECT users.name, orders.item FROM users INNER JOIN orders ON users.id = orders.user_id WHERE users.id = 1;",
    ));
    assert_eq!(columns, vec!["users.name", "orders.item"]);
    assert_eq!(
        rows,
        vec![vec![
            Value::Text("Alice".to_string()),
            Value::Text("Book".to_string()),
        ]]
    );

    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn test_join_with_no_matching_rows_returns_empty_result() {
    let (dir, _engine, executor) = open_executor("join-empty");

    exec(
        &executor,
        "CREATE TABLE users (id INT, name TEXT, active BOOL);",
    );
    exec(
        &executor,
        "CREATE TABLE orders (id INT, user_id INT, item TEXT);",
    );
    exec(
        &executor,
        "INSERT INTO users VALUES (1, 'Alice', true);",
    );
    exec(
        &executor,
        "INSERT INTO orders VALUES (10, 99, 'Book');",
    );

    let (columns, rows) = rows_only(exec(
        &executor,
        "SELECT users.name, orders.item FROM users INNER JOIN orders ON users.id = orders.user_id;",
    ));
    assert_eq!(columns, vec!["users.name", "orders.item"]);
    assert!(rows.is_empty());

    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn test_explain_select_returns_plan() {
    let (dir, _engine, executor) = open_executor("explain");

    exec(
        &executor,
        "CREATE TABLE users (id INT, name TEXT, active BOOL);",
    );
    exec(
        &executor,
        "INSERT INTO users VALUES (1, 'Alice', true), (2, 'Bob', false), (3, 'Cara', true);",
    );

    let result = exec(&executor, "EXPLAIN SELECT * FROM users WHERE id = 1;");
    match result {
        ExecuteResult::Explain { plan, .. } => {
            assert!(plan.contains("SeqScan"));
            assert!(plan.contains("Filter"));
            assert!(plan.contains("Project"));
            assert!(plan.contains("rows="));
            assert!(plan.contains("cost="));
        }
        other => panic!("expected explain result, got {:?}", other),
    }

    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn test_order_by_asc_and_desc_on_int_and_text() {
    let (dir, _engine, executor) = open_executor("order-basic");

    exec(
        &executor,
        "CREATE TABLE users (id INT, name TEXT, age INT, active BOOL);",
    );
    exec(
        &executor,
        "INSERT INTO users VALUES (1, 'Cara', 35, true), (2, 'Alice', 30, true), (3, 'Bob', 25, false);",
    );

    let (_, rows_age_asc) = rows_only(exec(&executor, "SELECT name, age FROM users ORDER BY age ASC;"));
    assert_eq!(
        rows_age_asc,
        vec![
            vec![Value::Text("Bob".to_string()), Value::Int(25)],
            vec![Value::Text("Alice".to_string()), Value::Int(30)],
            vec![Value::Text("Cara".to_string()), Value::Int(35)],
        ]
    );

    let (_, rows_name_desc) =
        rows_only(exec(&executor, "SELECT name FROM users ORDER BY name DESC;"));
    assert_eq!(
        rows_name_desc,
        vec![
            vec![Value::Text("Cara".to_string())],
            vec![Value::Text("Bob".to_string())],
            vec![Value::Text("Alice".to_string())],
        ]
    );

    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn test_limit_only_returns_first_n_rows() {
    let (dir, _engine, executor) = open_executor("limit");

    exec(
        &executor,
        "CREATE TABLE users (id INT, name TEXT, age INT, active BOOL);",
    );
    exec(
        &executor,
        "INSERT INTO users VALUES (1, 'Alice', 30, true), (2, 'Bob', 25, false), (3, 'Cara', 35, true);",
    );

    let (_, rows) = rows_only(exec(&executor, "SELECT * FROM users LIMIT 2;"));
    assert_eq!(rows.len(), 2);

    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn test_order_by_and_limit_combination() {
    let (dir, _engine, executor) = open_executor("order-limit");

    exec(
        &executor,
        "CREATE TABLE users (id INT, name TEXT, age INT, active BOOL);",
    );
    exec(
        &executor,
        "INSERT INTO users VALUES (1, 'Alice', 30, true), (2, 'Bob', 25, false), (3, 'Cara', 35, true), (4, 'Dora', 40, true);",
    );

    let (_, rows) = rows_only(exec(
        &executor,
        "SELECT name, age FROM users ORDER BY age DESC LIMIT 2;",
    ));
    assert_eq!(
        rows,
        vec![
            vec![Value::Text("Dora".to_string()), Value::Int(40)],
            vec![Value::Text("Cara".to_string()), Value::Int(35)],
        ]
    );

    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn test_order_by_with_where_combination() {
    let (dir, _engine, executor) = open_executor("order-where");

    exec(
        &executor,
        "CREATE TABLE users (id INT, name TEXT, age INT, active BOOL);",
    );
    exec(
        &executor,
        "INSERT INTO users VALUES (1, 'Alice', 30, true), (2, 'Bob', 25, false), (3, 'Cara', 35, true), (4, 'Dora', 20, true);",
    );

    let (_, rows) = rows_only(exec(
        &executor,
        "SELECT name, age FROM users WHERE age > 20 ORDER BY age DESC;",
    ));
    assert_eq!(
        rows,
        vec![
            vec![Value::Text("Cara".to_string()), Value::Int(35)],
            vec![Value::Text("Alice".to_string()), Value::Int(30)],
            vec![Value::Text("Bob".to_string()), Value::Int(25)],
        ]
    );

    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn test_count_without_group_by_and_empty_table() {
    let (dir, _engine, executor) = open_executor("agg-count-empty");

    exec(
        &executor,
        "CREATE TABLE users (id INT, name TEXT, age INT, active BOOL);",
    );

    let (columns_empty, rows_empty) = rows_only(exec(&executor, "SELECT COUNT(*) FROM users;"));
    assert_eq!(columns_empty, vec!["COUNT(*)"]);
    assert_eq!(rows_empty, vec![vec![Value::Int(0)]]);

    exec(
        &executor,
        "INSERT INTO users VALUES (1, 'Alice', 30, true), (2, 'Bob', 25, false), (3, 'Cara', 35, true);",
    );

    let (_, rows_count) = rows_only(exec(&executor, "SELECT COUNT(*) FROM users;"));
    assert_eq!(rows_count, vec![vec![Value::Int(3)]]);

    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn test_count_with_where_clause() {
    let (dir, _engine, executor) = open_executor("agg-count-where");

    exec(
        &executor,
        "CREATE TABLE users (id INT, name TEXT, age INT, active BOOL);",
    );
    exec(
        &executor,
        "INSERT INTO users VALUES (1, 'Alice', 30, true), (2, 'Bob', 25, false), (3, 'Cara', 35, true);",
    );

    let (_, rows) = rows_only(exec(
        &executor,
        "SELECT COUNT(*) FROM users WHERE age > 25;",
    ));
    assert_eq!(rows, vec![vec![Value::Int(2)]]);

    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn test_group_by_with_count() {
    let (dir, _engine, executor) = open_executor("agg-group-count");

    exec(
        &executor,
        "CREATE TABLE users (id INT, name TEXT, age INT, active BOOL);",
    );
    exec(
        &executor,
        "INSERT INTO users VALUES (1, 'Alice', 30, true), (2, 'Bob', 25, false), (3, 'Cara', 30, true), (4, 'Dora', 25, true);",
    );

    let (columns, rows) = rows_only(exec(
        &executor,
        "SELECT age, COUNT(*) FROM users GROUP BY age ORDER BY age ASC;",
    ));
    assert_eq!(columns, vec!["age", "COUNT(*)"]);
    assert_eq!(
        rows,
        vec![
            vec![Value::Int(25), Value::Int(2)],
            vec![Value::Int(30), Value::Int(2)],
        ]
    );

    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn test_group_by_with_sum_min_and_max() {
    let (dir, _engine, executor) = open_executor("agg-group-sum");

    exec(
        &executor,
        "CREATE TABLE employees (id INT, department TEXT, salary INT, active BOOL);",
    );
    exec(
        &executor,
        "INSERT INTO employees VALUES (1, 'Eng', 100, true), (2, 'Eng', 150, true), (3, 'HR', 80, true), (4, 'HR', 120, false);",
    );

    let (columns, rows) = rows_only(exec(
        &executor,
        "SELECT department, SUM(salary), MIN(salary), MAX(salary) FROM employees GROUP BY department ORDER BY department ASC;",
    ));
    assert_eq!(
        columns,
        vec![
            "department",
            "SUM(salary)",
            "MIN(salary)",
            "MAX(salary)",
        ]
    );
    assert_eq!(
        rows,
        vec![
            vec![
                Value::Text("Eng".to_string()),
                Value::Int(250),
                Value::Int(100),
                Value::Int(150),
            ],
            vec![
                Value::Text("HR".to_string()),
                Value::Int(200),
                Value::Int(80),
                Value::Int(120),
            ],
        ]
    );

    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn test_group_by_with_order_by_and_limit() {
    let (dir, _engine, executor) = open_executor("agg-group-order-limit");

    exec(
        &executor,
        "CREATE TABLE employees (id INT, department TEXT, salary INT, active BOOL);",
    );
    exec(
        &executor,
        "INSERT INTO employees VALUES (1, 'Eng', 100, true), (2, 'Eng', 150, true), (3, 'HR', 80, true), (4, 'Sales', 200, true), (5, 'Sales', 50, false);",
    );

    let (_, rows) = rows_only(exec(
        &executor,
        "SELECT department, SUM(salary) FROM employees WHERE salary > 60 GROUP BY department ORDER BY department DESC LIMIT 2;",
    ));
    assert_eq!(
        rows,
        vec![
            vec![Value::Text("Sales".to_string()), Value::Int(200)],
            vec![Value::Text("HR".to_string()), Value::Int(80)],
        ]
    );

    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn test_where_and_or_not_expressions() {
    let (dir, _engine, executor) = open_executor("where-bool");

    exec(
        &executor,
        "CREATE TABLE flags (id INT, a INT, b INT, c INT);",
    );
    exec(
        &executor,
        "INSERT INTO flags VALUES (1, 1, 2, 9), (2, 1, 0, 3), (3, 0, 2, 3), (4, 0, 0, 0);",
    );

    let (_, and_rows) = rows_only(exec(
        &executor,
        "SELECT id FROM flags WHERE a = 1 AND b = 2 ORDER BY id ASC;",
    ));
    assert_eq!(and_rows, vec![vec![Value::Int(1)]]);

    let (_, or_rows) = rows_only(exec(
        &executor,
        "SELECT id FROM flags WHERE a = 1 OR b = 2 ORDER BY id ASC;",
    ));
    assert_eq!(
        or_rows,
        vec![
            vec![Value::Int(1)],
            vec![Value::Int(2)],
            vec![Value::Int(3)],
        ]
    );

    let (_, nested_rows) = rows_only(exec(
        &executor,
        "SELECT id FROM flags WHERE a = 1 AND (b = 2 OR c = 3) ORDER BY id ASC;",
    ));
    assert_eq!(
        nested_rows,
        vec![vec![Value::Int(1)], vec![Value::Int(2)]]
    );

    let (_, not_rows) = rows_only(exec(
        &executor,
        "SELECT id FROM flags WHERE NOT a = 1 ORDER BY id ASC;",
    ));
    assert_eq!(not_rows, vec![vec![Value::Int(3)], vec![Value::Int(4)]]);

    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn test_and_where_can_still_use_index_scan() {
    let (dir, _engine, executor) = open_executor("where-index-and");

    exec(
        &executor,
        "CREATE TABLE users (id INT, age INT, active BOOL);",
    );
    exec(
        &executor,
        "INSERT INTO users VALUES (1, 30, true), (2, 30, false), (3, 25, true);",
    );
    exec(&executor, "CREATE INDEX ON users(age);");

    let plan = explain_only(exec(
        &executor,
        "EXPLAIN SELECT * FROM users WHERE age = 30 AND active = true;",
    ));
    assert!(plan.contains("IndexScan"));
    assert!(plan.contains("active = true"));

    let (_, rows) = rows_only(exec(
        &executor,
        "SELECT id FROM users WHERE age = 30 AND active = true;",
    ));
    assert_eq!(rows, vec![vec![Value::Int(1)]]);

    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn test_left_join_with_matches_and_missing_rows() {
    let (dir, _engine, executor) = open_executor("left-join");

    exec(
        &executor,
        "CREATE TABLE users (id INT, name TEXT, active BOOL);",
    );
    exec(
        &executor,
        "CREATE TABLE orders (id INT, user_id INT, item TEXT);",
    );
    exec(
        &executor,
        "INSERT INTO users VALUES (1, 'Alice', true), (2, 'Bob', false), (3, 'Cara', true);",
    );
    exec(
        &executor,
        "INSERT INTO orders VALUES (10, 1, 'Book'), (11, 1, 'Pen'), (12, 2, 'Cup');",
    );

    let (columns, rows) = rows_only(exec(
        &executor,
        "SELECT users.name, orders.item FROM users LEFT JOIN orders ON users.id = orders.user_id ORDER BY users.name ASC;",
    ));
    assert_eq!(columns, vec!["users.name", "orders.item"]);
    assert_eq!(
        rows,
        vec![
            vec![
                Value::Text("Alice".to_string()),
                Value::Text("Book".to_string()),
            ],
            vec![
                Value::Text("Alice".to_string()),
                Value::Text("Pen".to_string()),
            ],
            vec![
                Value::Text("Bob".to_string()),
                Value::Text("Cup".to_string()),
            ],
            vec![Value::Text("Cara".to_string()), Value::Null],
        ]
    );

    let (_, filtered_rows) = rows_only(exec(
        &executor,
        "SELECT users.name, orders.item FROM users LEFT JOIN orders ON users.id = orders.user_id WHERE users.id = 3;",
    ));
    assert_eq!(
        filtered_rows,
        vec![vec![Value::Text("Cara".to_string()), Value::Null]]
    );

    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn test_having_filters_aggregate_rows() {
    let (dir, _engine, executor) = open_executor("having");

    exec(
        &executor,
        "CREATE TABLE employees (id INT, department TEXT, salary INT, active BOOL);",
    );
    exec(
        &executor,
        "INSERT INTO employees VALUES (1, 'Eng', 100, true), (2, 'Eng', 120, true), (3, 'HR', 90, true), (4, 'Sales', 70, false), (5, 'Sales', 60, true);",
    );

    let (columns, rows) = rows_only(exec(
        &executor,
        "SELECT department, COUNT(*) FROM employees GROUP BY department HAVING COUNT(*) > 1 ORDER BY department ASC;",
    ));
    assert_eq!(columns, vec!["department", "COUNT(*)"]);
    assert_eq!(
        rows,
        vec![
            vec![Value::Text("Eng".to_string()), Value::Int(2)],
            vec![Value::Text("Sales".to_string()), Value::Int(2)],
        ]
    );

    let (_, combo_rows) = rows_only(exec(
        &executor,
        "SELECT department, SUM(salary) FROM employees WHERE salary >= 60 GROUP BY department HAVING SUM(salary) >= 130 ORDER BY department DESC LIMIT 2;",
    ));
    assert_eq!(
        combo_rows,
        vec![
            vec![Value::Text("Sales".to_string()), Value::Int(130)],
            vec![Value::Text("Eng".to_string()), Value::Int(220)],
        ]
    );

    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn test_cte_select_variants() {
    let (dir, _engine, executor) = open_executor("cte-select");

    exec(
        &executor,
        "CREATE TABLE employees (id INT, name TEXT, department TEXT, salary INT);",
    );
    exec(
        &executor,
        "CREATE TABLE departments (id INT, dept_name TEXT, location TEXT);",
    );
    exec(
        &executor,
        "INSERT INTO employees VALUES (1, 'Alice', 'Engineering', 90000), (2, 'Bob', 'HR', 70000), (3, 'Cara', 'Engineering', 95000);",
    );
    exec(
        &executor,
        "INSERT INTO departments VALUES (1, 'Engineering', 'Remote'), (2, 'HR', 'Onsite');",
    );

    let (_, single_rows) = rows_only(exec(
        &executor,
        "WITH high_earners AS (SELECT * FROM employees WHERE salary > 80000) SELECT name FROM high_earners ORDER BY name ASC;",
    ));
    assert_eq!(
        single_rows,
        vec![
            vec![Value::Text("Alice".to_string())],
            vec![Value::Text("Cara".to_string())],
        ]
    );

    let (_, multi_rows) = rows_only(exec(
        &executor,
        "WITH eng AS (SELECT * FROM employees WHERE department = 'Engineering'), hr AS (SELECT * FROM employees WHERE department = 'HR') SELECT name FROM hr ORDER BY name ASC;",
    ));
    assert_eq!(multi_rows, vec![vec![Value::Text("Bob".to_string())]]);

    let (_, join_rows) = rows_only(exec(
        &executor,
        "WITH high_earners AS (SELECT * FROM employees WHERE salary > 80000) SELECT high_earners.name, departments.location FROM high_earners INNER JOIN departments ON high_earners.department = departments.dept_name ORDER BY high_earners.name ASC;",
    ));
    assert_eq!(
        join_rows,
        vec![
            vec![
                Value::Text("Alice".to_string()),
                Value::Text("Remote".to_string()),
            ],
            vec![
                Value::Text("Cara".to_string()),
                Value::Text("Remote".to_string()),
            ],
        ]
    );

    let (_, filtered_rows) = rows_only(exec(
        &executor,
        "WITH high_earners AS (SELECT * FROM employees WHERE salary > 80000) SELECT name, salary FROM high_earners WHERE salary >= 90000 ORDER BY salary DESC;",
    ));
    assert_eq!(
        filtered_rows,
        vec![
            vec![Value::Text("Cara".to_string()), Value::Int(95000)],
            vec![Value::Text("Alice".to_string()), Value::Int(90000)],
        ]
    );

    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn test_update_from_and_delete_using() {
    let (dir, _engine, executor) = open_executor("update-delete-join");

    exec(
        &executor,
        "CREATE TABLE employees (id INT, name TEXT, department TEXT, salary INT);",
    );
    exec(
        &executor,
        "CREATE TABLE departments (id INT, dept_name TEXT, location TEXT);",
    );
    exec(
        &executor,
        "INSERT INTO employees VALUES (1, 'Alice', 'Engineering', 100), (2, 'Bob', 'HR', 80), (3, 'Cara', 'Engineering', 110);",
    );
    exec(
        &executor,
        "INSERT INTO departments VALUES (1, 'Engineering', 'Remote'), (2, 'HR', 'Onsite');",
    );

    assert_eq!(
        exec(
            &executor,
            "UPDATE employees SET salary = 0 FROM departments WHERE employees.department = departments.dept_name AND departments.location = 'Remote';",
        ),
        ExecuteResult::Updated { count: 2 }
    );
    let (_, updated_rows) = rows_only(exec(
        &executor,
        "SELECT name, salary FROM employees ORDER BY name ASC;",
    ));
    assert_eq!(
        updated_rows,
        vec![
            vec![Value::Text("Alice".to_string()), Value::Int(0)],
            vec![Value::Text("Bob".to_string()), Value::Int(80)],
            vec![Value::Text("Cara".to_string()), Value::Int(0)],
        ]
    );

    assert_eq!(
        exec(
            &executor,
            "DELETE FROM employees USING departments WHERE employees.department = departments.dept_name AND departments.location = 'Remote';",
        ),
        ExecuteResult::Deleted { count: 2 }
    );
    let (_, remaining_rows) = rows_only(exec(
        &executor,
        "SELECT name FROM employees ORDER BY name ASC;",
    ));
    assert_eq!(remaining_rows, vec![vec![Value::Text("Bob".to_string())]]);

    let _ = std::fs::remove_dir_all(dir);
}

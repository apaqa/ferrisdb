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
    std::env::temp_dir().join(format!("ferrisdb-matview-test-{}-{}", name, nanos))
}

fn open_executor(name: &str) -> (std::path::PathBuf, SqlExecutor) {
    let dir = temp_dir(name);
    let lsm = LsmEngine::open(&dir, 4096).expect("open lsm");
    let engine = Arc::new(MvccEngine::new(lsm));
    let executor = SqlExecutor::new(engine);
    (dir, executor)
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

#[test]
fn test_create_materialized_view_and_select_cached_rows() {
    let (dir, executor) = open_executor("create-select");
    exec(
        &executor,
        "CREATE TABLE employees (id INT, name TEXT, salary INT);",
    );
    exec(
        &executor,
        "INSERT INTO employees VALUES (1, 'Alice', 90000), (2, 'Bob', 70000), (3, 'Cara', 95000);",
    );

    assert_eq!(
        exec(
            &executor,
            "CREATE MATERIALIZED VIEW high_earners_cache AS SELECT id, name FROM employees WHERE salary > 80000;"
        ),
        ExecuteResult::Created {
            table_name: "high_earners_cache".to_string(),
        }
    );

    exec(
        &executor,
        "INSERT INTO employees VALUES (4, 'Dora', 99000);",
    );

    let (columns, rows) = rows_only(exec(
        &executor,
        "SELECT * FROM high_earners_cache ORDER BY id ASC;",
    ));
    assert_eq!(columns, vec!["id", "name"]);
    assert_eq!(
        rows,
        vec![
            vec![Value::Int(1), Value::Text("Alice".to_string())],
            vec![Value::Int(3), Value::Text("Cara".to_string())],
        ]
    );

    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn test_refresh_materialized_view_updates_cached_rows() {
    let (dir, executor) = open_executor("refresh");
    exec(
        &executor,
        "CREATE TABLE employees (id INT, name TEXT, salary INT);",
    );
    exec(
        &executor,
        "INSERT INTO employees VALUES (1, 'Alice', 90000), (2, 'Bob', 70000);",
    );
    exec(
        &executor,
        "CREATE MATERIALIZED VIEW high_earners_cache AS SELECT id, name FROM employees WHERE salary > 80000;",
    );
    exec(
        &executor,
        "INSERT INTO employees VALUES (3, 'Cara', 95000);",
    );

    assert_eq!(
        exec(&executor, "REFRESH MATERIALIZED VIEW high_earners_cache;"),
        ExecuteResult::Created {
            table_name: "high_earners_cache".to_string(),
        }
    );

    let (_, rows) = rows_only(exec(
        &executor,
        "SELECT * FROM high_earners_cache ORDER BY id ASC;",
    ));
    assert_eq!(
        rows,
        vec![
            vec![Value::Int(1), Value::Text("Alice".to_string())],
            vec![Value::Int(3), Value::Text("Cara".to_string())],
        ]
    );

    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn test_drop_materialized_view() {
    let (dir, executor) = open_executor("drop");
    exec(
        &executor,
        "CREATE TABLE employees (id INT, name TEXT, salary INT);",
    );
    exec(
        &executor,
        "CREATE MATERIALIZED VIEW high_earners_cache AS SELECT id, name FROM employees WHERE salary > 80000;",
    );

    assert_eq!(
        exec(&executor, "DROP MATERIALIZED VIEW high_earners_cache;"),
        ExecuteResult::Dropped {
            table_name: "high_earners_cache".to_string(),
        }
    );

    match exec(&executor, "SELECT * FROM high_earners_cache;") {
        ExecuteResult::Error { message } => assert!(message.contains("high_earners_cache")),
        other => panic!("expected missing-materialized-view error, got {:?}", other),
    }

    let _ = std::fs::remove_dir_all(dir);
}

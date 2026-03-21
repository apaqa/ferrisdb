use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use ferrisdb::sql::ast::Value;
use ferrisdb::sql::executor::{ExecuteResult, SqlExecutor};
use ferrisdb::sql::lexer::Lexer;
use ferrisdb::sql::parser::Parser;
use ferrisdb::storage::lsm::LsmEngine;
use ferrisdb::transaction::mvcc::MvccEngine;

// 中文註解：temporary table 測試需要隔離 session 狀態，所以每次都建立新 executor。
fn temp_dir(name: &str) -> std::path::PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time")
        .as_nanos();
    std::env::temp_dir().join(format!("ferrisdb-temp-table-{}-{}", name, nanos))
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
fn test_create_temporary_table_insert_and_select() {
    let (dir, executor) = open_executor("basic");

    exec(&executor, "CREATE TEMPORARY TABLE temp_users (id INT, name TEXT);");
    exec(
        &executor,
        "INSERT INTO temp_users VALUES (1, 'Alice'), (2, 'Bob');",
    );

    let (columns, rows) = rows_only(exec(
        &executor,
        "SELECT * FROM temp_users ORDER BY id ASC;",
    ));
    assert_eq!(columns, vec!["id", "name"]);
    assert_eq!(
        rows,
        vec![
            vec![Value::Int(1), Value::Text("Alice".to_string())],
            vec![Value::Int(2), Value::Text("Bob".to_string())],
        ]
    );

    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn test_temp_table_does_not_affect_persistent_table() {
    let (dir, executor) = open_executor("shadow");

    exec(&executor, "CREATE TABLE users (id INT, name TEXT);");
    exec(&executor, "INSERT INTO users VALUES (1, 'persistent');");
    exec(&executor, "CREATE TEMPORARY TABLE users (id INT, name TEXT);");
    exec(&executor, "INSERT INTO users VALUES (2, 'temp');");

    let (_, temp_rows) = rows_only(exec(&executor, "SELECT * FROM users ORDER BY id ASC;"));
    assert_eq!(temp_rows, vec![vec![Value::Int(2), Value::Text("temp".to_string())]]);

    assert_eq!(
        exec(&executor, "DROP TEMPORARY TABLE users;"),
        ExecuteResult::Dropped {
            table_name: "users".to_string(),
        }
    );

    let (_, persistent_rows) = rows_only(exec(&executor, "SELECT * FROM users ORDER BY id ASC;"));
    assert_eq!(
        persistent_rows,
        vec![vec![
            Value::Int(1),
            Value::Text("persistent".to_string()),
        ]]
    );

    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn test_drop_temporary_table() {
    let (dir, executor) = open_executor("drop");

    exec(&executor, "CREATE TEMPORARY TABLE scratch (id INT, name TEXT);");
    exec(&executor, "INSERT INTO scratch VALUES (1, 'row');");

    assert_eq!(
        exec(&executor, "DROP TEMPORARY TABLE scratch;"),
        ExecuteResult::Dropped {
            table_name: "scratch".to_string(),
        }
    );

    assert_eq!(
        exec(&executor, "SELECT * FROM scratch;"),
        ExecuteResult::Error {
            message: "table 'scratch' does not exist".to_string(),
        }
    );

    let _ = std::fs::remove_dir_all(dir);
}

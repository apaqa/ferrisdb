use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use ferrisdb::sql::ast::Value;
use ferrisdb::sql::executor::{ExecuteResult, SqlExecutor};
use ferrisdb::sql::lexer::Lexer;
use ferrisdb::sql::parser::Parser;
use ferrisdb::storage::lsm::LsmEngine;
use ferrisdb::transaction::mvcc::MvccEngine;

// 中文註解：cursor 測試需要跨多個 SQL 指令共享 executor session。
fn temp_dir(name: &str) -> std::path::PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time")
        .as_nanos();
    std::env::temp_dir().join(format!("ferrisdb-cursor-{}-{}", name, nanos))
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
fn test_declare_open_fetch_and_close_cursor() {
    let (dir, executor) = open_executor("basic");
    exec(&executor, "CREATE TABLE users (id INT, name TEXT);");
    exec(&executor, "INSERT INTO users VALUES (1, 'Alice'), (2, 'Bob');");

    exec(
        &executor,
        "DECLARE user_cursor CURSOR FOR SELECT id, name FROM users ORDER BY id ASC;",
    );
    exec(&executor, "OPEN user_cursor;");

    let (columns, rows) = rows_only(exec(
        &executor,
        "FETCH NEXT FROM user_cursor INTO current_id, current_name;",
    ));
    assert_eq!(columns, vec!["current_id", "current_name"]);
    assert_eq!(
        rows,
        vec![vec![Value::Int(1), Value::Text("Alice".to_string())]]
    );

    assert_eq!(
        exec(&executor, "CLOSE user_cursor;"),
        ExecuteResult::Selected {
            columns: vec![],
            rows: vec![],
        }
    );

    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn test_fetch_returns_empty_after_last_row() {
    let (dir, executor) = open_executor("empty-after-last");
    exec(&executor, "CREATE TABLE users (id INT, name TEXT);");
    exec(&executor, "INSERT INTO users VALUES (1, 'Alice');");

    exec(
        &executor,
        "DECLARE user_cursor CURSOR FOR SELECT id, name FROM users ORDER BY id ASC;",
    );
    exec(&executor, "OPEN user_cursor;");
    let _ = exec(
        &executor,
        "FETCH NEXT FROM user_cursor INTO current_id, current_name;",
    );

    let (columns, rows) = rows_only(exec(
        &executor,
        "FETCH NEXT FROM user_cursor INTO current_id, current_name;",
    ));
    assert_eq!(columns, vec!["current_id", "current_name"]);
    assert!(rows.is_empty());

    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn test_fetch_after_close_returns_error() {
    let (dir, executor) = open_executor("close-error");
    exec(&executor, "CREATE TABLE users (id INT, name TEXT);");
    exec(&executor, "INSERT INTO users VALUES (1, 'Alice');");

    exec(
        &executor,
        "DECLARE user_cursor CURSOR FOR SELECT id, name FROM users;",
    );
    exec(&executor, "OPEN user_cursor;");
    exec(&executor, "CLOSE user_cursor;");

    assert_eq!(
        exec(
            &executor,
            "FETCH NEXT FROM user_cursor INTO current_id, current_name;",
        ),
        ExecuteResult::Error {
            message: "cursor 'user_cursor' is not open".to_string(),
        }
    );

    let _ = std::fs::remove_dir_all(dir);
}

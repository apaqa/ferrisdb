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
    std::env::temp_dir().join(format!("ferrisdb-json-test-{}-{}", name, nanos))
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
fn test_insert_json_and_select_raw_value() {
    let (dir, executor) = open_executor("insert-select");
    exec(&executor, "CREATE TABLE users (id INT, data JSON);");
    exec(
        &executor,
        "INSERT INTO users VALUES (1, '{\"name\":\"Alice\",\"profile\":{\"city\":\"Taipei\"}}');",
    );

    let (_, rows) = rows_only(exec(&executor, "SELECT data FROM users;"));
    assert_eq!(
        rows,
        vec![vec![Value::Text(
            "{\"name\":\"Alice\",\"profile\":{\"city\":\"Taipei\"}}".to_string()
        )]]
    );

    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn test_json_extract_nested_value() {
    let (dir, executor) = open_executor("extract");
    exec(&executor, "CREATE TABLE users (id INT, data JSON);");
    exec(
        &executor,
        "INSERT INTO users VALUES (1, '{\"profile\":{\"name\":\"Alice\"}}');",
    );

    let (_, rows) = rows_only(exec(
        &executor,
        "SELECT JSON_EXTRACT(data, '$.profile.name') AS name FROM users;",
    ));
    assert_eq!(rows, vec![vec![Value::Text("Alice".to_string())]]);

    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn test_where_json_extract_filters_rows() {
    let (dir, executor) = open_executor("where");
    exec(&executor, "CREATE TABLE users (id INT, data JSON);");
    exec(
        &executor,
        "INSERT INTO users VALUES (1, '{\"name\":\"Alice\"}'), (2, '{\"name\":\"Bob\"}');",
    );

    let (_, rows) = rows_only(exec(
        &executor,
        "SELECT id FROM users WHERE JSON_EXTRACT(data, '$.name') = 'Alice';",
    ));
    assert_eq!(rows, vec![vec![Value::Int(1)]]);

    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn test_insert_invalid_json_is_rejected() {
    let (dir, executor) = open_executor("invalid");
    exec(&executor, "CREATE TABLE users (id INT, data JSON);");

    match exec(&executor, "INSERT INTO users VALUES (1, '{invalid json}');") {
        ExecuteResult::Error { message } => assert!(message.contains("invalid JSON")),
        other => panic!("expected invalid JSON error, got {:?}", other),
    }

    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn test_json_set_returns_updated_json() {
    let (dir, executor) = open_executor("set");
    exec(&executor, "CREATE TABLE users (id INT, data JSON);");
    exec(
        &executor,
        "INSERT INTO users VALUES (1, '{\"name\":\"Alice\",\"profile\":{}}');",
    );

    let (_, rows) = rows_only(exec(
        &executor,
        "SELECT JSON_SET(data, '$.profile.city', 'Taipei') AS updated FROM users;",
    ));
    assert_eq!(
        rows,
        vec![vec![Value::Text(
            "{\"name\":\"Alice\",\"profile\":{\"city\":\"Taipei\"}}".to_string()
        )]]
    );

    let _ = std::fs::remove_dir_all(dir);
}

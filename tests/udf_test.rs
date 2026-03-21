use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use ferrisdb::sql::ast::Value;
use ferrisdb::sql::executor::{ExecuteResult, SqlExecutor};
use ferrisdb::sql::lexer::Lexer;
use ferrisdb::sql::parser::Parser;
use ferrisdb::storage::lsm::LsmEngine;
use ferrisdb::transaction::mvcc::MvccEngine;

// 中文註解：每個測試使用獨立資料夾，避免 UDF metadata 互相污染。
fn temp_dir(name: &str) -> std::path::PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time")
        .as_nanos();
    std::env::temp_dir().join(format!("ferrisdb-udf-{}-{}", name, nanos))
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
fn test_create_function_and_select_call() {
    let (dir, executor) = open_executor("basic-call");

    exec(
        &executor,
        "CREATE FUNCTION greet(flag BOOL) RETURNS TEXT BEGIN IF flag = true THEN RETURN 'hello'; END IF; RETURN 'bye'; END;",
    );

    let (columns, rows) = rows_only(exec(&executor, "SELECT greet(true) AS msg;"));
    assert_eq!(columns, vec!["msg"]);
    assert_eq!(rows, vec![vec![Value::Text("hello".to_string())]]);

    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn test_udf_with_multiple_parameters() {
    let (dir, executor) = open_executor("multi-params");

    exec(
        &executor,
        "CREATE FUNCTION choose_text(num INT, label TEXT) RETURNS TEXT BEGIN IF num > 0 THEN RETURN label; END IF; RETURN 'fallback'; END;",
    );

    let (_, rows) = rows_only(exec(
        &executor,
        "SELECT choose_text(1, 'hello') AS chosen;",
    ));
    assert_eq!(rows, vec![vec![Value::Text("hello".to_string())]]);

    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn test_udf_in_where_clause() {
    let (dir, executor) = open_executor("where");
    exec(&executor, "CREATE TABLE users (id INT, name TEXT);");
    exec(
        &executor,
        "INSERT INTO users VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Cara');",
    );
    exec(
        &executor,
        "CREATE FUNCTION bigger_than_one(num INT) RETURNS BOOL BEGIN IF num > 1 THEN RETURN true; END IF; RETURN false; END;",
    );

    let (_, rows) = rows_only(exec(
        &executor,
        "SELECT name FROM users WHERE bigger_than_one(id) = true ORDER BY name ASC;",
    ));
    assert_eq!(
        rows,
        vec![
            vec![Value::Text("Bob".to_string())],
            vec![Value::Text("Cara".to_string())],
        ]
    );

    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn test_drop_function_then_call_returns_error() {
    let (dir, executor) = open_executor("drop");

    exec(
        &executor,
        "CREATE FUNCTION greet(flag BOOL) RETURNS TEXT BEGIN RETURN 'hello'; END;",
    );
    assert_eq!(
        exec(&executor, "DROP FUNCTION greet;"),
        ExecuteResult::Dropped {
            table_name: "greet".to_string(),
        }
    );
    match exec(&executor, "SELECT greet(true);") {
        ExecuteResult::Error { message } => {
            assert!(message.contains("function 'greet' does not exist"));
        }
        other => panic!("expected error after drop function, got {:?}", other),
    }

    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn test_udf_body_with_if_and_while() {
    let (dir, executor) = open_executor("control-flow");

    exec(
        &executor,
        "CREATE FUNCTION spin_once(enabled BOOL) RETURNS TEXT BEGIN DECLARE keep_running BOOL; SET keep_running = enabled; WHILE keep_running = true DO SET keep_running = false; END WHILE; IF enabled = true THEN RETURN 'done'; END IF; RETURN 'skip'; END;",
    );

    let (_, rows) = rows_only(exec(&executor, "SELECT spin_once(true) AS status;"));
    assert_eq!(rows, vec![vec![Value::Text("done".to_string())]]);

    let _ = std::fs::remove_dir_all(dir);
}

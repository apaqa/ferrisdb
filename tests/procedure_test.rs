use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use ferrisdb::sql::ast::Value;
use ferrisdb::sql::executor::{ExecuteResult, SqlExecutor};
use ferrisdb::sql::lexer::Lexer;
use ferrisdb::sql::parser::Parser;
use ferrisdb::storage::lsm::LsmEngine;
use ferrisdb::transaction::mvcc::MvccEngine;

// 中文註解：每個測試使用獨立資料夾，避免 procedure metadata 彼此污染。
fn temp_dir(name: &str) -> std::path::PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time")
        .as_nanos();
    std::env::temp_dir().join(format!("ferrisdb-procedure-{}-{}", name, nanos))
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
fn test_create_procedure_and_call_basic_usage() {
    let (dir, executor) = open_executor("basic");
    exec(&executor, "CREATE TABLE users (id INT, name TEXT);");

    assert_eq!(
        exec(
            &executor,
            "CREATE PROCEDURE add_alice() BEGIN INSERT INTO users VALUES (1, 'Alice'); END;"
        ),
        ExecuteResult::ProcedureCreated {
            name: "add_alice".to_string(),
        }
    );

    assert_eq!(
        exec(&executor, "CALL add_alice();"),
        ExecuteResult::Inserted { count: 1 }
    );

    let (_, rows) = rows_only(exec(&executor, "SELECT * FROM users;"));
    assert_eq!(
        rows,
        vec![vec![Value::Int(1), Value::Text("Alice".to_string())]]
    );

    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn test_procedure_with_parameters_and_local_variables() {
    let (dir, executor) = open_executor("params");
    exec(&executor, "CREATE TABLE users (id INT, name TEXT);");

    exec(
        &executor,
        "CREATE PROCEDURE add_user(user_id INT, user_name TEXT) BEGIN DECLARE local_id INT; SET local_id = user_id; INSERT INTO users VALUES (local_id, user_name); END;",
    );

    assert_eq!(
        exec(&executor, "CALL add_user(2, 'Bob');"),
        ExecuteResult::Inserted { count: 1 }
    );

    let (_, rows) = rows_only(exec(&executor, "SELECT * FROM users;"));
    assert_eq!(
        rows,
        vec![vec![Value::Int(2), Value::Text("Bob".to_string())]]
    );

    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn test_procedure_if_and_while_control_flow() {
    let (dir, executor) = open_executor("control-flow");
    exec(&executor, "CREATE TABLE audit_log (msg TEXT);");

    exec(
        &executor,
        "CREATE PROCEDURE control_flow(enabled BOOL) BEGIN DECLARE keep_running BOOL; SET keep_running = enabled; IF enabled = true THEN INSERT INTO audit_log VALUES ('if-branch'); ELSE INSERT INTO audit_log VALUES ('else-branch'); END IF; WHILE keep_running = true DO INSERT INTO audit_log VALUES ('while-branch'); SET keep_running = false; END WHILE; END;",
    );

    assert_eq!(
        exec(&executor, "CALL control_flow(true);"),
        ExecuteResult::Selected {
            columns: vec![],
            rows: vec![],
        }
    );

    let (_, rows) = rows_only(exec(&executor, "SELECT msg FROM audit_log ORDER BY msg ASC;"));
    assert_eq!(
        rows,
        vec![
            vec![Value::Text("if-branch".to_string())],
            vec![Value::Text("while-branch".to_string())],
        ]
    );

    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn test_procedure_can_run_insert_and_select() {
    let (dir, executor) = open_executor("insert-select");
    exec(&executor, "CREATE TABLE items (id INT, name TEXT);");

    exec(
        &executor,
        "CREATE PROCEDURE seed_items() BEGIN INSERT INTO items VALUES (1, 'Pen'); SELECT * FROM items; END;",
    );

    let (columns, rows) = rows_only(exec(&executor, "CALL seed_items();"));
    assert_eq!(columns, vec!["id", "name"]);
    assert_eq!(
        rows,
        vec![vec![Value::Int(1), Value::Text("Pen".to_string())]]
    );

    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn test_drop_procedure_and_missing_procedure_error() {
    let (dir, executor) = open_executor("drop");

    exec(
        &executor,
        "CREATE PROCEDURE noop() BEGIN DECLARE x INT; SET x = 1; END;",
    );

    assert_eq!(
        exec(&executor, "DROP PROCEDURE noop;"),
        ExecuteResult::ProcedureDropped {
            name: "noop".to_string(),
        }
    );

    assert_eq!(
        exec(&executor, "CALL missing_proc();"),
        ExecuteResult::Error {
            message: "procedure 'missing_proc' does not exist".to_string(),
        }
    );

    let _ = std::fs::remove_dir_all(dir);
}

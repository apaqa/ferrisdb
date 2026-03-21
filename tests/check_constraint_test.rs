use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

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
    std::env::temp_dir().join(format!("ferrisdb-check-test-{}-{}", name, nanos))
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

#[test]
fn test_insert_rejects_check_constraint_violation() {
    let (dir, executor) = open_executor("insert-invalid");
    exec(
        &executor,
        "CREATE TABLE employees (id INT, salary INT, CHECK (salary > 0));",
    );

    match exec(&executor, "INSERT INTO employees VALUES (1, 0);") {
        ExecuteResult::Error { message } => assert!(message.contains("check constraint")),
        other => panic!("expected CHECK error, got {:?}", other),
    }

    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn test_insert_accepts_valid_check_constraint() {
    let (dir, executor) = open_executor("insert-valid");
    exec(
        &executor,
        "CREATE TABLE employees (id INT, salary INT, CHECK (salary > 0));",
    );

    assert_eq!(
        exec(&executor, "INSERT INTO employees VALUES (1, 100);"),
        ExecuteResult::Inserted { count: 1 }
    );

    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn test_update_rejects_check_constraint_violation() {
    let (dir, executor) = open_executor("update-invalid");
    exec(
        &executor,
        "CREATE TABLE employees (id INT, salary INT, CHECK (salary > 0));",
    );
    exec(&executor, "INSERT INTO employees VALUES (1, 100);");

    match exec(&executor, "UPDATE employees SET salary = -1 WHERE id = 1;") {
        ExecuteResult::Error { message } => assert!(message.contains("check constraint")),
        other => panic!("expected CHECK error, got {:?}", other),
    }

    let _ = std::fs::remove_dir_all(dir);
}

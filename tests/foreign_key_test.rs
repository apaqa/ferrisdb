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
    std::env::temp_dir().join(format!("ferrisdb-fk-test-{}-{}", name, nanos))
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
fn test_insert_rejects_foreign_key_violation() {
    let (dir, executor) = open_executor("insert-violation");
    exec(&executor, "CREATE TABLE customers (id INT, name TEXT);");
    exec(
        &executor,
        "CREATE TABLE orders (id INT, customer_id INT, FOREIGN KEY (customer_id) REFERENCES customers(id));",
    );

    match exec(&executor, "INSERT INTO orders VALUES (1, 999);") {
        ExecuteResult::Error { message } => assert!(message.contains("foreign key")),
        other => panic!("expected FK error, got {:?}", other),
    }

    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn test_insert_accepts_valid_foreign_key() {
    let (dir, executor) = open_executor("insert-valid");
    exec(&executor, "CREATE TABLE customers (id INT, name TEXT);");
    exec(
        &executor,
        "CREATE TABLE orders (id INT, customer_id INT, FOREIGN KEY (customer_id) REFERENCES customers(id));",
    );
    exec(&executor, "INSERT INTO customers VALUES (1, 'Alice');");

    assert_eq!(
        exec(&executor, "INSERT INTO orders VALUES (1, 1);"),
        ExecuteResult::Inserted { count: 1 }
    );

    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn test_delete_rejects_referenced_row_without_cascade() {
    let (dir, executor) = open_executor("delete-referenced");
    exec(&executor, "CREATE TABLE customers (id INT, name TEXT);");
    exec(
        &executor,
        "CREATE TABLE orders (id INT, customer_id INT, FOREIGN KEY (customer_id) REFERENCES customers(id));",
    );
    exec(&executor, "INSERT INTO customers VALUES (1, 'Alice');");
    exec(&executor, "INSERT INTO orders VALUES (1, 1);");

    match exec(&executor, "DELETE FROM customers WHERE id = 1;") {
        ExecuteResult::Error { message } => assert!(message.contains("referenced")),
        other => panic!("expected FK delete error, got {:?}", other),
    }

    let _ = std::fs::remove_dir_all(dir);
}

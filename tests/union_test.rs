// =============================================================================
// tests/union_test.rs -- SQL UNION 整合測試
// =============================================================================

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
    std::env::temp_dir().join(format!("ferrisdb-union-test-{}-{}", name, nanos))
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

fn selected(result: ExecuteResult) -> (Vec<String>, Vec<Vec<Value>>) {
    match result {
        ExecuteResult::Selected { columns, rows } => (columns, rows),
        other => panic!("expected selected result, got {:?}", other),
    }
}

#[test]
fn test_union_deduplicates_by_default() {
    let (dir, executor) = open_executor("dedup");
    exec(&executor, "CREATE TABLE employees (id INT, name TEXT);");
    exec(
        &executor,
        "CREATE TABLE departments (id INT, dept_name TEXT);",
    );
    exec(
        &executor,
        "INSERT INTO employees VALUES (1, 'Alice'), (2, 'Bob');",
    );
    exec(
        &executor,
        "INSERT INTO departments VALUES (10, 'Bob'), (20, 'Sales');",
    );

    let (columns, rows) = selected(exec(
        &executor,
        "SELECT name FROM employees UNION SELECT dept_name FROM departments;",
    ));
    assert_eq!(columns, vec!["name"]);
    assert_eq!(
        rows,
        vec![
            vec![Value::Text("Alice".to_string())],
            vec![Value::Text("Bob".to_string())],
            vec![Value::Text("Sales".to_string())],
        ]
    );

    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn test_union_all_keeps_duplicates() {
    let (dir, executor) = open_executor("all");
    exec(&executor, "CREATE TABLE employees (id INT, name TEXT);");
    exec(
        &executor,
        "CREATE TABLE departments (id INT, dept_name TEXT);",
    );
    exec(
        &executor,
        "INSERT INTO employees VALUES (1, 'Alice'), (2, 'Bob');",
    );
    exec(
        &executor,
        "INSERT INTO departments VALUES (10, 'Bob'), (20, 'Sales');",
    );

    let (_, rows) = selected(exec(
        &executor,
        "SELECT name FROM employees UNION ALL SELECT dept_name FROM departments;",
    ));
    assert_eq!(
        rows,
        vec![
            vec![Value::Text("Alice".to_string())],
            vec![Value::Text("Bob".to_string())],
            vec![Value::Text("Bob".to_string())],
            vec![Value::Text("Sales".to_string())],
        ]
    );

    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn test_union_reports_column_count_mismatch() {
    let (dir, executor) = open_executor("mismatch");
    exec(&executor, "CREATE TABLE employees (id INT, name TEXT);");
    exec(
        &executor,
        "CREATE TABLE departments (id INT, dept_name TEXT);",
    );
    exec(&executor, "INSERT INTO employees VALUES (1, 'Alice');");
    exec(&executor, "INSERT INTO departments VALUES (10, 'Sales');");

    match exec(
        &executor,
        "SELECT id, name FROM employees UNION SELECT dept_name FROM departments;",
    ) {
        ExecuteResult::Error { message } => assert!(message.contains("expected 2 columns")),
        other => panic!("expected mismatch error, got {:?}", other),
    }

    let _ = std::fs::remove_dir_all(dir);
}

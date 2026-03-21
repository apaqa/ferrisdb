// =============================================================================
// tests/case_when_test.rs -- CASE WHEN 整合測試
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
    std::env::temp_dir().join(format!("ferrisdb-case-test-{}-{}", name, nanos))
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

fn seed(executor: &SqlExecutor) {
    exec(
        executor,
        "CREATE TABLE employees (id INT, name TEXT, salary INT);",
    );
    exec(
        executor,
        "INSERT INTO employees VALUES (1, 'Alice', 95000), (2, 'Bob', 70000), (3, 'Cara', 50000);",
    );
}

#[test]
fn test_case_when_basic_and_multiple_conditions() {
    let (dir, executor) = open_executor("basic");
    seed(&executor);

    let (columns, rows) = selected(exec(
        &executor,
        "SELECT name, CASE WHEN salary > 90000 THEN 'high' WHEN salary > 60000 THEN 'mid' ELSE 'low' END AS level FROM employees ORDER BY name ASC;",
    ));
    assert_eq!(columns, vec!["name", "level"]);
    assert_eq!(
        rows,
        vec![
            vec![
                Value::Text("Alice".to_string()),
                Value::Text("high".to_string()),
            ],
            vec![
                Value::Text("Bob".to_string()),
                Value::Text("mid".to_string()),
            ],
            vec![
                Value::Text("Cara".to_string()),
                Value::Text("low".to_string()),
            ],
        ]
    );

    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn test_case_when_without_else_returns_null_and_supports_order_by() {
    let (dir, executor) = open_executor("order");
    seed(&executor);

    let (_, rows) = selected(exec(
        &executor,
        "SELECT name, CASE WHEN salary > 90000 THEN 'priority' END AS bucket FROM employees ORDER BY CASE WHEN salary > 90000 THEN 0 ELSE 1 END ASC;",
    ));
    assert_eq!(
        rows,
        vec![
            vec![
                Value::Text("Alice".to_string()),
                Value::Text("priority".to_string()),
            ],
            vec![Value::Text("Bob".to_string()), Value::Null],
            vec![Value::Text("Cara".to_string()), Value::Null],
        ]
    );

    let _ = std::fs::remove_dir_all(dir);
}

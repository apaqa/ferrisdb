// =============================================================================
// tests/insert_select_test.rs -- INSERT INTO SELECT 整合測試
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
    std::env::temp_dir().join(format!("ferrisdb-insert-select-{}-{}", name, nanos))
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

fn rows_only(result: ExecuteResult) -> Vec<Vec<Value>> {
    match result {
        ExecuteResult::Selected { rows, .. } => rows,
        other => panic!("expected selected result, got {:?}", other),
    }
}

#[test]
fn test_insert_into_select_basic_and_where() {
    let (dir, executor) = open_executor("basic");
    exec(
        &executor,
        "CREATE TABLE source_users (id INT, name TEXT, age INT);",
    );
    exec(
        &executor,
        "CREATE TABLE target_users (id INT, name TEXT, age INT);",
    );
    exec(
        &executor,
        "INSERT INTO source_users VALUES (1, 'Alice', 30), (2, 'Bob', 20), (3, 'Cara', 40);",
    );

    assert_eq!(
        exec(
            &executor,
            "INSERT INTO target_users SELECT * FROM source_users;"
        ),
        ExecuteResult::Inserted { count: 3 }
    );

    let rows = rows_only(exec(
        &executor,
        "SELECT * FROM target_users ORDER BY id ASC;",
    ));
    assert_eq!(rows.len(), 3);

    exec(
        &executor,
        "CREATE TABLE adult_users (id INT, name TEXT, age INT);",
    );
    assert_eq!(
        exec(
            &executor,
            "INSERT INTO adult_users SELECT id, name, age FROM source_users WHERE age > 25;"
        ),
        ExecuteResult::Inserted { count: 2 }
    );
    assert_eq!(
        rows_only(exec(
            &executor,
            "SELECT * FROM adult_users ORDER BY id ASC;"
        )),
        vec![
            vec![
                Value::Int(1),
                Value::Text("Alice".to_string()),
                Value::Int(30),
            ],
            vec![
                Value::Int(3),
                Value::Text("Cara".to_string()),
                Value::Int(40),
            ],
        ]
    );

    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn test_insert_into_select_reports_column_mismatch() {
    let (dir, executor) = open_executor("mismatch");
    exec(&executor, "CREATE TABLE source_users (id INT, name TEXT);");
    exec(
        &executor,
        "CREATE TABLE target_names (id INT, name TEXT, age INT);",
    );
    exec(
        &executor,
        "INSERT INTO source_users VALUES (1, 'Alice'), (2, 'Bob');",
    );

    match exec(
        &executor,
        "INSERT INTO target_names SELECT id, name FROM source_users;",
    ) {
        ExecuteResult::Error { message } => assert!(message.contains("expected 3 values")),
        other => panic!("expected mismatch error, got {:?}", other),
    }

    let _ = std::fs::remove_dir_all(dir);
}

// =============================================================================
// tests/alter_drop_test.rs -- ALTER TABLE / DROP TABLE 整合測試
// =============================================================================
//
// 這組測試驗證 schema 變更是否會正確反映到：
// - schema metadata
// - 現有 row 的 JSON 結構
// - table drop 後的查詢行為
//
// 真實資料庫裡，schema migration 是很常見的操作。
// 如果 ALTER TABLE 只改 schema、不改既有 row，之後查詢就會出現欄位不一致。
// 如果 DROP TABLE 沒清乾淨 row 或 index metadata，之後也可能留下髒資料。

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
    std::env::temp_dir().join(format!("ferrisdb-alter-drop-{}-{}", name, nanos))
}

fn open_executor(name: &str) -> (std::path::PathBuf, SqlExecutor) {
    let dir = temp_dir(name);
    let lsm = LsmEngine::open(&dir, 4096).expect("open lsm");
    let engine = Arc::new(MvccEngine::new(lsm));
    let executor = SqlExecutor::new(Arc::clone(&engine));
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
fn test_alter_table_add_column_sets_existing_rows_to_null() {
    let (dir, executor) = open_executor("add-column");

    exec(&executor, "CREATE TABLE users (id INT, name TEXT);");
    exec(&executor, "INSERT INTO users VALUES (1, 'Alice'), (2, 'Bob');");
    assert_eq!(
        exec(&executor, "ALTER TABLE users ADD COLUMN email TEXT;"),
        ExecuteResult::Altered {
            table_name: "users".to_string(),
        }
    );

    let (columns, rows) = selected(exec(&executor, "SELECT * FROM users ORDER BY id ASC;"));
    assert_eq!(columns, vec!["id", "name", "email"]);
    assert_eq!(
        rows,
        vec![
            vec![
                Value::Int(1),
                Value::Text("Alice".to_string()),
                Value::Null,
            ],
            vec![Value::Int(2), Value::Text("Bob".to_string()), Value::Null],
        ]
    );

    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn test_alter_table_drop_column_removes_column_from_results() {
    let (dir, executor) = open_executor("drop-column");

    exec(&executor, "CREATE TABLE users (id INT, name TEXT, age INT);");
    exec(
        &executor,
        "INSERT INTO users VALUES (1, 'Alice', 30), (2, 'Bob', 25);",
    );
    assert_eq!(
        exec(&executor, "ALTER TABLE users DROP COLUMN age;"),
        ExecuteResult::Altered {
            table_name: "users".to_string(),
        }
    );

    let (columns, rows) = selected(exec(&executor, "SELECT * FROM users ORDER BY id ASC;"));
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
fn test_drop_table_then_select_returns_error() {
    let (dir, executor) = open_executor("drop-table");

    exec(&executor, "CREATE TABLE users (id INT, name TEXT);");
    exec(&executor, "DROP TABLE users;");

    assert_eq!(
        exec(&executor, "SELECT * FROM users;"),
        ExecuteResult::Error {
            message: "table 'users' does not exist".to_string(),
        }
    );

    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn test_drop_table_if_exists_is_noop_for_missing_table() {
    let (dir, executor) = open_executor("drop-if-exists");

    assert_eq!(
        exec(&executor, "DROP TABLE IF EXISTS users;"),
        ExecuteResult::Dropped {
            table_name: "users".to_string(),
        }
    );

    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn test_create_table_if_not_exists_does_not_error() {
    let (dir, executor) = open_executor("create-if-not-exists");

    exec(&executor, "CREATE TABLE users (id INT, name TEXT);");
    assert_eq!(
        exec(
            &executor,
            "CREATE TABLE IF NOT EXISTS users (id INT, name TEXT);",
        ),
        ExecuteResult::Created {
            table_name: "users".to_string(),
        }
    );

    let (columns, rows) = selected(exec(&executor, "SELECT * FROM users;"));
    assert_eq!(columns, vec!["id", "name"]);
    assert!(rows.is_empty());

    let _ = std::fs::remove_dir_all(dir);
}

// =============================================================================
// tests/subquery_test.rs -- SQL 子查詢整合測試
// =============================================================================
//
// 這組測試模擬常見的 WHERE ... IN (SELECT ...) 查詢。
// 真實世界中，子查詢常用來把一張表的篩選結果帶進另一張表。
//
// 正確的執行器需要：
// 1. 先執行內層 SELECT
// 2. 取回結果集
// 3. 再拿這些值去過濾外層查詢

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
    std::env::temp_dir().join(format!("ferrisdb-subquery-{}-{}", name, nanos))
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

fn rows_only(result: ExecuteResult) -> (Vec<String>, Vec<Vec<Value>>) {
    match result {
        ExecuteResult::Selected { columns, rows } => (columns, rows),
        other => panic!("expected selected result, got {:?}", other),
    }
}

#[test]
fn test_where_in_subquery_basic() {
    let (dir, executor) = open_executor("basic");

    exec(&executor, "CREATE TABLE users (id INT, name TEXT);");
    exec(&executor, "CREATE TABLE orders (id INT, user_id INT);");
    exec(
        &executor,
        "INSERT INTO users VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Cara');",
    );
    exec(
        &executor,
        "INSERT INTO orders VALUES (10, 1), (11, 3);",
    );

    let (_, rows) = rows_only(exec(
        &executor,
        "SELECT name FROM users WHERE id IN (SELECT user_id FROM orders) ORDER BY name ASC;",
    ));
    assert_eq!(
        rows,
        vec![
            vec![Value::Text("Alice".to_string())],
            vec![Value::Text("Cara".to_string())],
        ]
    );

    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn test_subquery_with_empty_result_returns_no_rows() {
    let (dir, executor) = open_executor("empty");

    exec(&executor, "CREATE TABLE users (id INT, name TEXT);");
    exec(&executor, "CREATE TABLE orders (id INT, user_id INT);");
    exec(
        &executor,
        "INSERT INTO users VALUES (1, 'Alice'), (2, 'Bob');",
    );

    let (_, rows) = rows_only(exec(
        &executor,
        "SELECT * FROM users WHERE id IN (SELECT user_id FROM orders);",
    ));
    assert!(rows.is_empty());

    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn test_subquery_with_join() {
    let (dir, executor) = open_executor("join");

    exec(&executor, "CREATE TABLE users (id INT, name TEXT);");
    exec(&executor, "CREATE TABLE profiles (id INT, user_id INT, city TEXT);");
    exec(&executor, "CREATE TABLE orders (id INT, user_id INT);");
    exec(
        &executor,
        "INSERT INTO users VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Cara');",
    );
    exec(
        &executor,
        "INSERT INTO profiles VALUES (100, 1, 'Taipei'), (101, 2, 'Tokyo'), (102, 3, 'Paris');",
    );
    exec(
        &executor,
        "INSERT INTO orders VALUES (10, 1), (11, 3);",
    );

    let (_, rows) = rows_only(exec(
        &executor,
        "SELECT users.name, profiles.city FROM users INNER JOIN profiles ON users.id = profiles.user_id WHERE users.id IN (SELECT user_id FROM orders) ORDER BY users.name ASC;",
    ));
    assert_eq!(
        rows,
        vec![
            vec![
                Value::Text("Alice".to_string()),
                Value::Text("Taipei".to_string()),
            ],
            vec![
                Value::Text("Cara".to_string()),
                Value::Text("Paris".to_string()),
            ],
        ]
    );

    let _ = std::fs::remove_dir_all(dir);
}

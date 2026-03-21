// =============================================================================
// tests/privilege_test.rs -- GRANT/REVOKE 權限控制整合測試
// =============================================================================
//
// 測試三種權限情境：
// 1. GRANT SELECT 後使用者可以查詢，但不能 INSERT
// 2. REVOKE 之後使用者無法查詢
// 3. ALL PRIVILEGES：GRANT ALL 讓使用者可執行 SELECT/INSERT/UPDATE/DELETE

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
    std::env::temp_dir().join(format!("ferrisdb-privilege-test-{}-{}", name, nanos))
}

fn open_executor(name: &str) -> (std::path::PathBuf, SqlExecutor) {
    let dir = temp_dir(name);
    let lsm = LsmEngine::open(&dir, 4096).expect("open lsm");
    let engine = Arc::new(MvccEngine::new(lsm));
    let executor = SqlExecutor::new(engine);
    (dir, executor)
}

// 中文註解：執行單一 SQL 語句的輔助函式
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
        other => panic!("expected Selected result, got {:?}", other),
    }
}

fn expect_permission_denied(result: ExecuteResult) {
    match result {
        ExecuteResult::Error { message } => {
            assert!(
                message.contains("permission denied"),
                "expected permission denied error, got: {}",
                message
            );
        }
        other => panic!("expected permission denied error, got {:?}", other),
    }
}

// 中文註解：測試 GRANT SELECT 後可查詢，但 INSERT 被拒絕
#[test]
fn test_grant_select_allows_query() {
    let (dir, executor) = open_executor("grant-select");

    // 管理員建立表格並插入資料（current_user = None）
    exec(
        &executor,
        "CREATE TABLE employees (id INT, name TEXT, salary INT);",
    );
    exec(
        &executor,
        "INSERT INTO employees VALUES (1, 'Alice', 90000), (2, 'Bob', 70000);",
    );

    // 管理員授予 user1 對 employees 的 SELECT 權限
    assert_eq!(
        exec(&executor, "GRANT SELECT ON employees TO user1;"),
        ExecuteResult::Granted {
            user: "user1".to_string(),
            table_name: "employees".to_string(),
        }
    );

    // 切換為 user1
    executor.set_current_user(Some("user1".to_string()));

    // user1 可以執行 SELECT
    let (columns, rows) = rows_only(exec(
        &executor,
        "SELECT id, name FROM employees ORDER BY id ASC;",
    ));
    assert_eq!(columns, vec!["id", "name"]);
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0][1], Value::Text("Alice".to_string()));

    // user1 不能執行 INSERT（無 INSERT 權限）
    let insert_result = exec(
        &executor,
        "INSERT INTO employees VALUES (3, 'Charlie', 60000);",
    );
    expect_permission_denied(insert_result);

    let _ = std::fs::remove_dir_all(dir);
}

// 中文註解：測試 REVOKE 之後使用者無法再查詢
#[test]
fn test_revoke_deny_query() {
    let (dir, executor) = open_executor("revoke-deny");

    // 管理員建立表格
    exec(&executor, "CREATE TABLE orders (id INT, amount INT);");
    exec(&executor, "INSERT INTO orders VALUES (1, 100), (2, 200);");

    // 授予 user2 SELECT 權限
    exec(&executor, "GRANT SELECT ON orders TO user2;");

    // 切換為 user2，可以查詢
    executor.set_current_user(Some("user2".to_string()));
    let (_, rows) = rows_only(exec(&executor, "SELECT * FROM orders;"));
    assert_eq!(rows.len(), 2);

    // 切回管理員，撤銷 SELECT 權限
    executor.set_current_user(None);
    assert_eq!(
        exec(&executor, "REVOKE SELECT ON orders FROM user2;"),
        ExecuteResult::Revoked {
            user: "user2".to_string(),
            table_name: "orders".to_string(),
        }
    );

    // 切換回 user2，此時 SELECT 應被拒絕
    executor.set_current_user(Some("user2".to_string()));
    expect_permission_denied(exec(&executor, "SELECT * FROM orders;"));

    let _ = std::fs::remove_dir_all(dir);
}

// 中文註解：測試 GRANT ALL PRIVILEGES 讓使用者可以執行所有資料操作
#[test]
fn test_all_privileges() {
    let (dir, executor) = open_executor("all-privileges");

    // 管理員建立表格
    exec(&executor, "CREATE TABLE items (id INT, label TEXT);");

    // 授予 superuser ALL PRIVILEGES
    assert_eq!(
        exec(&executor, "GRANT ALL PRIVILEGES ON items TO superuser;"),
        ExecuteResult::Granted {
            user: "superuser".to_string(),
            table_name: "items".to_string(),
        }
    );

    // 切換為 superuser
    executor.set_current_user(Some("superuser".to_string()));

    // 可以 INSERT
    assert_eq!(
        exec(&executor, "INSERT INTO items VALUES (1, 'alpha');"),
        ExecuteResult::Inserted { count: 1 }
    );

    // 可以 SELECT
    let (_, rows) = rows_only(exec(&executor, "SELECT * FROM items;"));
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][1], Value::Text("alpha".to_string()));

    // 可以 UPDATE
    assert_eq!(
        exec(&executor, "UPDATE items SET label = 'beta' WHERE id = 1;"),
        ExecuteResult::Updated { count: 1 }
    );

    // 可以 DELETE
    assert_eq!(
        exec(&executor, "DELETE FROM items WHERE id = 1;"),
        ExecuteResult::Deleted { count: 1 }
    );

    // 切回管理員，撤銷 ALL 之後 superuser 不能再操作
    executor.set_current_user(None);
    exec(&executor, "REVOKE ALL ON items FROM superuser;");

    executor.set_current_user(Some("superuser".to_string()));
    expect_permission_denied(exec(&executor, "SELECT * FROM items;"));

    let _ = std::fs::remove_dir_all(dir);
}

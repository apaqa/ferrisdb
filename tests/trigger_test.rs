// =============================================================================
// tests/trigger_test.rs -- SQL Trigger 整合測試
// =============================================================================
//
// 測試三種觸發器情境：
// 1. BEFORE INSERT trigger 修改即將寫入的值
// 2. AFTER DELETE trigger 寫入 audit log
// 3. DROP TRIGGER 之後觸發器不再生效

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
    std::env::temp_dir().join(format!("ferrisdb-trigger-test-{}-{}", name, nanos))
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

// 中文註解：測試 BEFORE INSERT trigger 能夠在插入前修改欄位值
#[test]
fn test_before_insert_trigger_modify_value() {
    let (dir, executor) = open_executor("before-insert");

    // 建立員工表
    exec(
        &executor,
        "CREATE TABLE employees (id INT, name TEXT, salary INT);",
    );

    // 建立 BEFORE INSERT trigger：把 salary 強制設成 100
    assert_eq!(
        exec(
            &executor,
            "CREATE TRIGGER cap_salary BEFORE INSERT ON employees \
             FOR EACH ROW BEGIN SET NEW.salary = 100 END"
        ),
        ExecuteResult::TriggerCreated {
            trigger_name: "cap_salary".to_string(),
        }
    );

    // 插入 salary = 999 的資料，觸發器應該把它改成 100
    assert_eq!(
        exec(
            &executor,
            "INSERT INTO employees VALUES (1, 'Alice', 999);"
        ),
        ExecuteResult::Inserted { count: 1 }
    );

    // 驗證 salary 確實被改成 100
    let (columns, rows) = rows_only(exec(
        &executor,
        "SELECT * FROM employees WHERE id = 1;",
    ));
    assert_eq!(columns, vec!["id", "name", "salary"]);
    assert_eq!(
        rows,
        vec![vec![
            Value::Int(1),
            Value::Text("Alice".to_string()),
            Value::Int(100),  // 觸發器把 999 改成了 100
        ]]
    );

    let _ = std::fs::remove_dir_all(dir);
}

// 中文註解：測試 AFTER DELETE trigger 在刪除後自動寫入 audit log
#[test]
fn test_after_delete_trigger_audit_log() {
    let (dir, executor) = open_executor("after-delete");

    // 建立員工表與稽核記錄表
    exec(
        &executor,
        "CREATE TABLE employees (id INT, name TEXT);",
    );
    exec(
        &executor,
        "CREATE TABLE audit_log (log_id INT, action TEXT);",
    );

    // 預先插入員工資料
    exec(
        &executor,
        "INSERT INTO employees VALUES (1, 'Alice'), (2, 'Bob');",
    );

    // 建立 AFTER DELETE trigger：刪除後往 audit_log 插入一筆記錄
    assert_eq!(
        exec(
            &executor,
            "CREATE TRIGGER log_delete AFTER DELETE ON employees \
             FOR EACH ROW BEGIN INSERT INTO audit_log VALUES (1, 'deleted') END"
        ),
        ExecuteResult::TriggerCreated {
            trigger_name: "log_delete".to_string(),
        }
    );

    // 刪除一筆員工資料
    assert_eq!(
        exec(&executor, "DELETE FROM employees WHERE id = 1;"),
        ExecuteResult::Deleted { count: 1 }
    );

    // 驗證 audit_log 有寫入記錄
    let (columns, rows) = rows_only(exec(&executor, "SELECT * FROM audit_log;"));
    assert_eq!(columns, vec!["log_id", "action"]);
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][1], Value::Text("deleted".to_string()));

    let _ = std::fs::remove_dir_all(dir);
}

// 中文註解：測試 DROP TRIGGER 之後觸發器不再修改插入值
#[test]
fn test_drop_trigger() {
    let (dir, executor) = open_executor("drop-trigger");

    exec(
        &executor,
        "CREATE TABLE products (id INT, price INT);",
    );

    // 建立 BEFORE INSERT trigger
    exec(
        &executor,
        "CREATE TRIGGER fix_price BEFORE INSERT ON products \
         FOR EACH ROW BEGIN SET NEW.price = 0 END",
    );

    // 觸發器存在時插入 price = 500，應被改成 0
    exec(&executor, "INSERT INTO products VALUES (1, 500);");
    let (_, rows) = rows_only(exec(&executor, "SELECT * FROM products WHERE id = 1;"));
    assert_eq!(rows[0][1], Value::Int(0));  // 觸發器生效，price = 0

    // 刪除觸發器
    assert_eq!(
        exec(&executor, "DROP TRIGGER fix_price;"),
        ExecuteResult::TriggerDropped {
            trigger_name: "fix_price".to_string(),
        }
    );

    // 觸發器已被刪除，再插入 price = 500 應該保留原始值
    exec(&executor, "INSERT INTO products VALUES (2, 500);");
    let (_, rows) = rows_only(exec(&executor, "SELECT * FROM products WHERE id = 2;"));
    assert_eq!(rows[0][1], Value::Int(500));  // 觸發器已移除，price 保持 500

    let _ = std::fs::remove_dir_all(dir);
}

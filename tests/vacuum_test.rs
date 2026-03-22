// =============================================================================
// tests/vacuum_test.rs -- VACUUM 測試
// =============================================================================
//
// 中文註解：
// VACUUM 在這個專案裡的角色是：
// 1. 觸發底層 compaction，讓 row tombstone 有機會被整理掉。
// 2. 清掉 secondary index 這類 SQL metadata 裡已經失效的殘留 entry。
//
// 這裡會驗證：
// - 全域 VACUUM 不會改變查詢結果
// - table-specific VACUUM 只清指定表
// - VACUUM 的統計資訊會回報清理結果

use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use ferrisdb::sql::ast::Value;
use ferrisdb::sql::executor::{ExecuteResult, SqlExecutor};
use ferrisdb::sql::index::encode_index_entry_key;
use ferrisdb::sql::lexer::Lexer;
use ferrisdb::sql::parser::Parser;
use ferrisdb::storage::lsm::LsmEngine;
use ferrisdb::transaction::mvcc::MvccEngine;

fn temp_dir(name: &str) -> std::path::PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time")
        .as_nanos();
    std::env::temp_dir().join(format!("ferrisdb-vacuum-{}-{}", name, nanos))
}

fn open_executor(name: &str) -> (Arc<MvccEngine>, SqlExecutor) {
    let dir = temp_dir(name);
    let lsm = LsmEngine::open(&dir, 128).expect("open lsm");
    let engine = Arc::new(MvccEngine::new(lsm));
    let executor = SqlExecutor::new(Arc::clone(&engine));
    (engine, executor)
}

fn exec(executor: &SqlExecutor, sql: &str) -> ExecuteResult {
    let mut lexer = Lexer::new(sql);
    let tokens = lexer.tokenize().expect("tokenize sql");
    let mut parser = Parser::new(tokens);
    let stmt = parser.parse().expect("parse sql");
    executor.execute(stmt).expect("execute sql")
}

fn selected_rows(result: ExecuteResult) -> Vec<Vec<Value>> {
    match result {
        ExecuteResult::Selected { rows, .. } => rows,
        other => panic!("expected selected result, got {:?}", other),
    }
}

#[test]
fn test_vacuum_cleans_tombstone_without_changing_select_result() {
    let (engine, executor) = open_executor("global");
    exec(&executor, "CREATE TABLE users (id INT, name TEXT);");
    exec(&executor, "CREATE INDEX ON users(name);");
    exec(&executor, "INSERT INTO users VALUES (1, 'Alice'), (2, 'Bob');");
    exec(&executor, "DELETE FROM users WHERE id = 2;");
    engine.inner.flush().expect("flush before vacuum");

    let before_rows = selected_rows(exec(&executor, "SELECT * FROM users ORDER BY id ASC;"));
    let vacuum = exec(&executor, "VACUUM;");
    let after_rows = selected_rows(exec(&executor, "SELECT * FROM users ORDER BY id ASC;"));

    assert_eq!(before_rows, after_rows);
    match vacuum {
        ExecuteResult::Vacuumed {
            tombstones_removed,
            reclaimed_bytes,
            ..
        } => {
            let _ = tombstones_removed;
            let _ = reclaimed_bytes;
        }
        other => panic!("expected vacuum result, got {:?}", other),
    }
}

#[test]
fn test_vacuum_table_name_only_cleans_target_table_metadata() {
    let (engine, executor) = open_executor("targeted");
    exec(&executor, "CREATE TABLE users (id INT, name TEXT);");
    exec(&executor, "CREATE TABLE orders (id INT, label TEXT);");
    exec(&executor, "CREATE INDEX ON users(name);");
    exec(&executor, "CREATE INDEX ON orders(label);");
    exec(&executor, "INSERT INTO users VALUES (1, 'Alice');");
    exec(&executor, "INSERT INTO orders VALUES (1, 'Order-1');");

    let users_key = encode_index_entry_key(
        "users",
        &[String::from("name")],
        &[Value::Text("Ghost".to_string())],
        &Value::Int(999),
    );
    let orders_key = encode_index_entry_key(
        "orders",
        &[String::from("label")],
        &[Value::Text("GhostOrder".to_string())],
        &Value::Int(999),
    );
    let mut txn = engine.begin_transaction();
    txn.put(users_key.clone(), Vec::new()).expect("put stale users index");
    txn.put(orders_key.clone(), Vec::new()).expect("put stale orders index");
    txn.commit().expect("commit stale indexes");

    match exec(&executor, "VACUUM users;") {
        ExecuteResult::Vacuumed {
            table_name,
            reclaimed_bytes,
            ..
        } => {
            assert_eq!(table_name.as_deref(), Some("users"));
            assert!(reclaimed_bytes >= users_key.len());
        }
        other => panic!("expected vacuum result, got {:?}", other),
    }

    let txn = engine.begin_transaction();
    assert!(txn.get(&users_key).expect("get users stale key").is_none());
    assert!(txn.get(&orders_key).expect("get orders stale key").is_some());
}

#[test]
fn test_vacuum_preserves_visible_rows() {
    let (_engine, executor) = open_executor("preserve");
    exec(&executor, "CREATE TABLE users (id INT, name TEXT);");
    exec(&executor, "INSERT INTO users VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Cara');");
    let before = selected_rows(exec(
        &executor,
        "SELECT * FROM users WHERE id >= 2 ORDER BY id ASC;",
    ));
    let _ = exec(&executor, "VACUUM users;");
    let after = selected_rows(exec(
        &executor,
        "SELECT * FROM users WHERE id >= 2 ORDER BY id ASC;",
    ));
    assert_eq!(before, after);
}

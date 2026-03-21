use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use ferrisdb::sql::ast::IsolationLevel;
use ferrisdb::storage::lsm::LsmEngine;
use ferrisdb::transaction::mvcc::MvccEngine;

fn temp_dir(name: &str) -> std::path::PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time")
        .as_nanos();
    std::env::temp_dir().join(format!("ferrisdb-isolation-{}-{}", name, nanos))
}

fn open_engine(name: &str) -> Arc<MvccEngine> {
    let dir = temp_dir(name);
    let lsm = LsmEngine::open(&dir, 4096).expect("open lsm");
    Arc::new(MvccEngine::new(lsm))
}

#[test]
fn test_default_is_repeatable_read() {
    let engine = open_engine("default");
    assert_eq!(engine.isolation_level(), IsolationLevel::RepeatableRead);
}

#[test]
fn test_read_committed_sees_latest_commit_per_statement() {
    let engine = open_engine("read-committed");
    engine.set_isolation_level(IsolationLevel::ReadCommitted);

    let txn1 = engine.begin_transaction();
    assert_eq!(txn1.get(b"k").expect("initial get"), None);

    let mut txn2 = engine.begin_transaction();
    txn2.put(b"k".to_vec(), b"v1".to_vec()).expect("put");
    txn2.commit().expect("commit");

    assert_eq!(txn1.get(b"k").expect("fresh snapshot get"), Some(b"v1".to_vec()));
}

#[test]
fn test_repeatable_read_keeps_same_snapshot() {
    let engine = open_engine("repeatable-read");
    engine.set_isolation_level(IsolationLevel::RepeatableRead);

    let txn1 = engine.begin_transaction();
    assert_eq!(txn1.get(b"k").expect("initial get"), None);

    let mut txn2 = engine.begin_transaction();
    txn2.put(b"k".to_vec(), b"v1".to_vec()).expect("put");
    txn2.commit().expect("commit");

    assert_eq!(txn1.get(b"k").expect("stable snapshot get"), None);
    let fresh = engine.begin_transaction();
    assert_eq!(fresh.get(b"k").expect("fresh get"), Some(b"v1".to_vec()));
}

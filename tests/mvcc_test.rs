// =============================================================================
// tests/mvcc_test.rs — MVCC 測試
// =============================================================================
//
// 這組測試驗證：
// - transaction 基本 CRUD
// - snapshot isolation
// - commit / rollback
// - key 編碼與解碼
// - scan 只回傳每個 key 的最新可見版本

use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use ferrisdb::storage::lsm::LsmEngine;
use ferrisdb::transaction::keyutil::{
    decode_key, encode_key, encode_key_prefix_end, encode_key_prefix_start,
};
use ferrisdb::transaction::mvcc::MvccEngine;

fn temp_dir(name: &str) -> std::path::PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time")
        .as_nanos();
    std::env::temp_dir().join(format!("ferrisdb-mvcc-{}-{}", name, nanos))
}

fn open_engine(name: &str) -> Arc<MvccEngine> {
    let dir = temp_dir(name);
    let lsm = LsmEngine::open(dir, 4096).expect("open lsm");
    Arc::new(MvccEngine::new(lsm))
}

#[test]
fn test_basic_crud_through_transaction() {
    let engine = open_engine("crud");

    let mut txn = engine.begin_transaction();
    txn.put(b"user:1".to_vec(), b"Alice".to_vec()).expect("put");
    txn.commit().expect("commit");

    let txn = engine.begin_transaction();
    assert_eq!(txn.get(b"user:1").expect("get"), Some(b"Alice".to_vec()));
}

#[test]
fn test_snapshot_isolation() {
    let engine = open_engine("snapshot");

    let txn1 = engine.begin_transaction();

    let mut txn2 = engine.begin_transaction();
    txn2.put(b"k".to_vec(), b"v2".to_vec()).expect("put");
    txn2.commit().expect("commit");

    assert_eq!(txn1.get(b"k").expect("txn1 get"), None);
}

#[test]
fn test_commit_visible_to_new_transaction() {
    let engine = open_engine("commit-visible");

    let mut txn = engine.begin_transaction();
    txn.put(b"k".to_vec(), b"v".to_vec()).expect("put");
    txn.commit().expect("commit");

    let txn_new = engine.begin_transaction();
    assert_eq!(txn_new.get(b"k").expect("get"), Some(b"v".to_vec()));
}

#[test]
fn test_rollback_discards_uncommitted_writes() {
    let engine = open_engine("rollback");

    {
        let mut txn = engine.begin_transaction();
        txn.put(b"k".to_vec(), b"temp".to_vec()).expect("put");
        txn.rollback();
    }

    let txn = engine.begin_transaction();
    assert_eq!(txn.get(b"k").expect("get"), None);
}

#[test]
fn test_key_encoding_roundtrip() {
    let encoded = encode_key(b"user:1", 42);
    let (user_key, ts) = decode_key(&encoded);

    assert_eq!(user_key, b"user:1");
    assert_eq!(ts, 42);
    assert_eq!(encode_key_prefix_start(b"user:1").len(), b"user:1".len() + 8);
    assert_eq!(encode_key_prefix_end(b"user:1").len(), b"user:1".len() + 8);
}

#[test]
fn test_scan_returns_latest_visible_version_per_key() {
    let engine = open_engine("scan");

    let mut txn1 = engine.begin_transaction();
    txn1.put(b"a".to_vec(), b"1".to_vec()).expect("put a1");
    txn1.put(b"b".to_vec(), b"1".to_vec()).expect("put b1");
    txn1.commit().expect("commit txn1");

    let snapshot = engine.begin_transaction();

    let mut txn2 = engine.begin_transaction();
    txn2.put(b"a".to_vec(), b"2".to_vec()).expect("put a2");
    txn2.put(b"c".to_vec(), b"3".to_vec()).expect("put c3");
    txn2.commit().expect("commit txn2");

    let rows_snapshot = snapshot.scan(b"a", b"z").expect("snapshot scan");
    assert_eq!(
        rows_snapshot,
        vec![
            (b"a".to_vec(), b"1".to_vec()),
            (b"b".to_vec(), b"1".to_vec()),
        ]
    );

    let rows_latest = engine.begin_transaction().scan(b"a", b"z").expect("latest scan");
    assert_eq!(
        rows_latest,
        vec![
            (b"a".to_vec(), b"2".to_vec()),
            (b"b".to_vec(), b"1".to_vec()),
            (b"c".to_vec(), b"3".to_vec()),
        ]
    );
}

#[test]
fn test_delete_visible_as_none_for_new_transaction() {
    let engine = open_engine("delete");

    let mut txn1 = engine.begin_transaction();
    txn1.put(b"k".to_vec(), b"v".to_vec()).expect("put");
    txn1.commit().expect("commit");

    let mut txn2 = engine.begin_transaction();
    txn2.delete(b"k").expect("delete");
    txn2.commit().expect("commit delete");

    let txn3 = engine.begin_transaction();
    assert_eq!(txn3.get(b"k").expect("get"), None);
}

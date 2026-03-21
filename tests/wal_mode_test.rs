use std::mem;
use std::time::{SystemTime, UNIX_EPOCH};

use ferrisdb::config::WalMode;
use ferrisdb::storage::lsm::{LsmEngine, DEFAULT_MEMTABLE_SIZE_THRESHOLD};
use ferrisdb::storage::traits::StorageEngine;

fn temp_dir(name: &str) -> std::path::PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time")
        .as_nanos();
    std::env::temp_dir().join(format!("ferrisdb-wal-mode-{}-{}", name, nanos))
}

#[test]
fn test_wal_mode_recovers_after_crash() {
    let dir = temp_dir("wal");
    let mut engine = LsmEngine::open_with_options(
        &dir,
        DEFAULT_MEMTABLE_SIZE_THRESHOLD,
        4,
        WalMode::Wal,
    )
    .expect("open wal mode");
    engine.put(b"k".to_vec(), b"v".to_vec()).expect("put");
    mem::forget(engine);

    let reopened = LsmEngine::open_with_options(
        &dir,
        DEFAULT_MEMTABLE_SIZE_THRESHOLD,
        4,
        WalMode::Wal,
    )
    .expect("reopen wal mode");
    assert_eq!(reopened.get(b"k").expect("get"), Some(b"v".to_vec()));

    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn test_sync_mode_recovers_after_crash() {
    let dir = temp_dir("sync");
    let mut engine = LsmEngine::open_with_options(
        &dir,
        DEFAULT_MEMTABLE_SIZE_THRESHOLD,
        4,
        WalMode::Sync,
    )
    .expect("open sync mode");
    engine.put(b"k".to_vec(), b"v".to_vec()).expect("put");
    mem::forget(engine);

    let reopened = LsmEngine::open_with_options(
        &dir,
        DEFAULT_MEMTABLE_SIZE_THRESHOLD,
        4,
        WalMode::Sync,
    )
    .expect("reopen sync mode");
    assert_eq!(reopened.get(b"k").expect("get"), Some(b"v".to_vec()));

    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn test_wal_disabled_loses_unflushed_data_after_crash() {
    let dir = temp_dir("disabled");
    let mut engine = LsmEngine::open_with_options(
        &dir,
        DEFAULT_MEMTABLE_SIZE_THRESHOLD,
        4,
        WalMode::WalDisabled,
    )
    .expect("open disabled mode");
    engine.put(b"k".to_vec(), b"v".to_vec()).expect("put");
    mem::forget(engine);

    let reopened = LsmEngine::open_with_options(
        &dir,
        DEFAULT_MEMTABLE_SIZE_THRESHOLD,
        4,
        WalMode::WalDisabled,
    )
    .expect("reopen disabled mode");
    assert_eq!(reopened.get(b"k").expect("get"), None);

    let _ = std::fs::remove_dir_all(dir);
}

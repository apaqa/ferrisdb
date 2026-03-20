// =============================================================================
// tests/wal_test.rs — WAL 與 crash recovery 測試
// =============================================================================
//
// 這組測試專注於：
// - WAL record 的寫入/讀取
// - WAL -> MemTable recovery
// - checksum 壞掉時的檢測
// - LsmEngine 透過 WAL 做 crash recovery
// - flush 後舊 WAL 被清除，並建立新的空 WAL

use std::fs;
use std::mem;
use std::path::PathBuf;
use std::time::{SystemTime, UNIX_EPOCH};

use ferrisdb::storage::lsm::{LsmEngine, DEFAULT_MEMTABLE_SIZE_THRESHOLD};
use ferrisdb::storage::traits::StorageEngine;
use ferrisdb::storage::wal::{WalReader, WalRecord, WalWriter};

fn temp_dir(name: &str) -> PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time")
        .as_nanos();
    std::env::temp_dir().join(format!("ferrisdb-wal-test-{}-{}", name, nanos))
}

#[test]
fn test_wal_write_and_read_records() {
    let dir = temp_dir("records");
    fs::create_dir_all(&dir).expect("create temp dir");
    let path = dir.join("wal.log");

    let mut writer = WalWriter::new(&path).expect("create wal");
    writer.append_put(b"user:1", b"Alice").expect("put");
    writer.append_delete(b"user:2").expect("delete");

    let reader = WalReader::open(&path).expect("open wal");
    let records: Vec<WalRecord> = reader
        .iter()
        .expect("iter")
        .map(|item| item.expect("record"))
        .collect();

    assert_eq!(
        records,
        vec![
            WalRecord::Put {
                key: b"user:1".to_vec(),
                value: b"Alice".to_vec(),
            },
            WalRecord::Delete {
                key: b"user:2".to_vec(),
            },
        ]
    );

    let _ = fs::remove_dir_all(dir);
}

#[test]
fn test_wal_recover_to_memtable() {
    let dir = temp_dir("recover");
    fs::create_dir_all(&dir).expect("create temp dir");
    let path = dir.join("wal.log");

    let mut writer = WalWriter::new(&path).expect("create wal");
    writer.append_put(b"a", b"1").expect("put a");
    writer.append_put(b"b", b"2").expect("put b");
    writer.append_delete(b"a").expect("delete a");

    let reader = WalReader::open(&path).expect("open wal");
    let memtable = reader.recover_to_memtable().expect("recover to memtable");

    assert_eq!(
        memtable.get(b"a").expect("get a"),
        Some(b"__TOMBSTONE__".to_vec())
    );
    assert_eq!(memtable.get(b"b").expect("get b"), Some(b"2".to_vec()));

    let _ = fs::remove_dir_all(dir);
}

#[test]
fn test_wal_checksum_corruption_detected() {
    let dir = temp_dir("checksum");
    fs::create_dir_all(&dir).expect("create temp dir");
    let path = dir.join("wal.log");

    let mut writer = WalWriter::new(&path).expect("create wal");
    writer.append_put(b"k", b"v").expect("append put");

    let mut bytes = fs::read(&path).expect("read wal bytes");
    // 改掉 payload 內的一個位元組，讓 checksum 不一致。
    let last = bytes.len() - 1;
    bytes[last] ^= 0xFF;
    fs::write(&path, bytes).expect("write corrupted wal");

    let reader = WalReader::open(&path).expect("open wal");
    let mut iter = reader.iter().expect("iter");
    let err = iter.next().expect("first record").expect_err("should fail");
    assert!(format!("{}", err).contains("checksum"));

    let _ = fs::remove_dir_all(dir);
}

#[test]
fn test_lsm_crash_recovery_via_wal() {
    let dir = temp_dir("crash-recovery");

    let mut engine = LsmEngine::open(&dir, DEFAULT_MEMTABLE_SIZE_THRESHOLD).expect("open lsm");
    engine.put(b"crash:key".to_vec(), b"crash:value".to_vec()).expect("put");
    engine
        .put(b"crash:key:2".to_vec(), b"another".to_vec())
        .expect("put second");

    // 用 forget 模擬程式直接崩潰，不觸發 Drop，也就不會自動 flush/shutdown。
    mem::forget(engine);

    let reopened = LsmEngine::open(&dir, DEFAULT_MEMTABLE_SIZE_THRESHOLD).expect("reopen lsm");
    assert_eq!(
        reopened.get(b"crash:key").expect("get crash:key"),
        Some(b"crash:value".to_vec())
    );
    assert_eq!(
        reopened.get(b"crash:key:2").expect("get crash:key:2"),
        Some(b"another".to_vec())
    );

    // 這個測試故意不強制清理目錄，避免 forget 後的檔案 handle 在某些平台造成刪除失敗。
}

#[test]
fn test_wal_cleared_after_flush() {
    let dir = temp_dir("clear-after-flush");
    let mut engine = LsmEngine::open(&dir, 64).expect("open lsm");

    // 這裡用單筆超過閾值的 value，讓這次 put 結束時剛好觸發 flush，
    // 而且 flush 之後不再有後續寫入，如此 wal.log 應該是新建的空檔。
    engine
        .put(b"flush:key".to_vec(), b"xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx".to_vec())
        .expect("put oversized entry");

    let wal_path = dir.join("wal.log");
    assert!(wal_path.exists(), "wal.log should exist after flush");
    let metadata = fs::metadata(&wal_path).expect("wal metadata");
    assert_eq!(metadata.len(), 0, "wal should be recreated as empty after flush");

    let _ = fs::remove_dir_all(dir);
}

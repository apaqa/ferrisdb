// =============================================================================
// tests/lsm_test.rs — LSM-Tree 整合測試
// =============================================================================
//
// 這組測試驗證 LsmEngine 的核心讀寫流程：
// - 寫入先進 active MemTable
// - 超過閾值自動 flush 成 SSTable
// - 讀取時 memtable 優先，找不到再往舊層查
// - delete 以 tombstone 表示，會遮蔽舊資料
// - 重新 open 同資料夾後資料仍存在

use std::fs;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use ferrisdb::storage::lsm::{LsmEngine, DEFAULT_MEMTABLE_SIZE_THRESHOLD};
use ferrisdb::storage::traits::StorageEngine;

fn temp_dir(name: &str) -> PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time")
        .as_nanos();
    std::env::temp_dir().join(format!("ferrisdb-lsm-{}-{}", name, nanos))
}

fn count_sst_files(dir: &Path) -> usize {
    fs::read_dir(dir)
        .expect("read dir")
        .filter_map(|entry| entry.ok().map(|e| e.path()))
        .filter(|p| p.extension().and_then(|e| e.to_str()) == Some("sst"))
        .count()
}

#[test]
fn test_lsm_basic_crud() {
    let dir = temp_dir("basic-crud");
    let mut engine = LsmEngine::open(&dir, DEFAULT_MEMTABLE_SIZE_THRESHOLD).expect("open lsm");

    engine
        .put(b"user:1".to_vec(), b"Alice".to_vec())
        .expect("put user:1");
    assert_eq!(engine.get(b"user:1").expect("get user:1"), Some(b"Alice".to_vec()));

    engine
        .put(b"user:1".to_vec(), b"Alice Updated".to_vec())
        .expect("update user:1");
    assert_eq!(
        engine.get(b"user:1").expect("get updated user:1"),
        Some(b"Alice Updated".to_vec())
    );

    engine.delete(b"user:1").expect("delete user:1");
    assert_eq!(engine.get(b"user:1").expect("get deleted"), None);

    let _ = fs::remove_dir_all(dir);
}

#[test]
fn test_auto_flush_creates_sstable_file() {
    let dir = temp_dir("auto-flush");
    let mut engine = LsmEngine::open(&dir, 64).expect("open lsm");

    // 用較長 value 快速超過閾值，觸發自動 flush。
    for i in 0..20_u32 {
        let key = format!("k{:03}", i);
        let value = format!("v{:03}-xxxxxxxxxxxxxxxxxxxx", i);
        engine
            .put(key.into_bytes(), value.into_bytes())
            .expect("put for flush");
    }

    assert!(
        count_sst_files(&dir) > 0,
        "expected at least one .sst file after flush"
    );

    let _ = fs::remove_dir_all(dir);
}

#[test]
fn test_read_after_flush_still_works() {
    let dir = temp_dir("read-after-flush");
    let mut engine = LsmEngine::open(&dir, 64).expect("open lsm");

    for i in 0..30_u32 {
        let key = format!("user:{:03}", i);
        let value = format!("name-{}-yyyyyyyyyyyyyyyyyyyy", i);
        engine
            .put(key.clone().into_bytes(), value.clone().into_bytes())
            .expect("put batch");
    }

    assert_eq!(
        engine.get(b"user:005").expect("get user:005"),
        Some(b"name-5-yyyyyyyyyyyyyyyyyyyy".to_vec())
    );
    assert_eq!(
        engine.get(b"user:020").expect("get user:020"),
        Some(b"name-20-yyyyyyyyyyyyyyyyyyyy".to_vec())
    );

    let _ = fs::remove_dir_all(dir);
}

#[test]
fn test_delete_returns_none_with_tombstone() {
    let dir = temp_dir("tombstone");
    let mut engine = LsmEngine::open(&dir, 64).expect("open lsm");

    engine.put(b"a".to_vec(), b"1".to_vec()).expect("put a");
    // 逼一次 flush，確保值落在 sstable，再用 delete 寫 tombstone 遮蔽它。
    for i in 0..20_u32 {
        let key = format!("f{:03}", i);
        let value = "zzzzzzzzzzzzzzzzzzzz".as_bytes().to_vec();
        engine.put(key.into_bytes(), value).expect("put filler");
    }

    engine.delete(b"a").expect("delete a");
    assert_eq!(engine.get(b"a").expect("get a after delete"), None);

    let _ = fs::remove_dir_all(dir);
}

#[test]
fn test_reopen_keeps_data() {
    let dir = temp_dir("reopen");

    {
        let mut engine = LsmEngine::open(&dir, 64).expect("open lsm first");
        engine.put(b"k1".to_vec(), b"v1".to_vec()).expect("put k1");
        engine.put(b"k2".to_vec(), b"v2".to_vec()).expect("put k2");

        // 再放一些 filler 保證 flush 發生，資料才會持久化到檔案。
        for i in 0..20_u32 {
            let key = format!("g{:03}", i);
            let value = "persist-persist-persist".as_bytes().to_vec();
            engine.put(key.into_bytes(), value).expect("put filler");
        }
    }

    let engine = LsmEngine::open(&dir, 64).expect("reopen lsm");
    assert_eq!(engine.get(b"k1").expect("get k1"), Some(b"v1".to_vec()));
    assert_eq!(engine.get(b"k2").expect("get k2"), Some(b"v2".to_vec()));

    let _ = fs::remove_dir_all(dir);
}

#[test]
fn test_scan_merges_memtable_and_sstable() {
    let dir = temp_dir("scan-merge");
    let mut engine = LsmEngine::open(&dir, 80).expect("open lsm");

    // 第一批資料：盡量觸發 flush，進入 sstable。
    engine.put(b"a".to_vec(), b"1".to_vec()).expect("put a");
    engine.put(b"b".to_vec(), b"1".to_vec()).expect("put b");
    engine.put(b"c".to_vec(), b"1".to_vec()).expect("put c");
    for i in 0..15_u32 {
        let key = format!("x{:03}", i);
        let value = "xxxxxxxxxxxxxxxxxxxx".as_bytes().to_vec();
        engine.put(key.into_bytes(), value).expect("put filler");
    }

    // 第二批資料在 memtable：覆蓋 b、刪除 c、新增 d。
    engine.put(b"b".to_vec(), b"2".to_vec()).expect("update b");
    engine.delete(b"c").expect("delete c");
    engine.put(b"d".to_vec(), b"4".to_vec()).expect("put d");

    let rows = engine.scan(b"a", b"z").expect("scan a..z");
    let map: std::collections::BTreeMap<Vec<u8>, Vec<u8>> = rows.into_iter().collect();

    assert_eq!(map.get(b"a".as_slice()), Some(&b"1".to_vec()));
    assert_eq!(map.get(b"b".as_slice()), Some(&b"2".to_vec()));
    assert_eq!(map.get(b"c".as_slice()), None);
    assert_eq!(map.get(b"d".as_slice()), Some(&b"4".to_vec()));

    let _ = fs::remove_dir_all(dir);
}

#[test]
fn test_manual_shutdown_persists_small_memtable() {
    let dir = temp_dir("manual-shutdown");

    {
        let mut engine = LsmEngine::open(&dir, DEFAULT_MEMTABLE_SIZE_THRESHOLD).expect("open lsm");
        engine
            .put(b"small:key".to_vec(), b"small:value".to_vec())
            .expect("put small entry");
        engine
            .put(b"small:key:2".to_vec(), b"another".to_vec())
            .expect("put second small entry");

        // 這裡資料量故意不超過閾值，必須靠手動 shutdown 才能持久化。
        engine.shutdown().expect("manual shutdown");
    }

    let reopened = LsmEngine::open(&dir, DEFAULT_MEMTABLE_SIZE_THRESHOLD).expect("reopen lsm");
    assert_eq!(
        reopened.get(b"small:key").expect("get small:key"),
        Some(b"small:value".to_vec())
    );
    assert_eq!(
        reopened.get(b"small:key:2").expect("get small:key:2"),
        Some(b"another".to_vec())
    );

    let _ = fs::remove_dir_all(dir);
}

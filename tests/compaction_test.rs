// =============================================================================
// tests/compaction_test.rs — Compaction 測試
// =============================================================================

use std::fs;
use std::path::PathBuf;
use std::time::{SystemTime, UNIX_EPOCH};

use ferrisdb::storage::compaction;
use ferrisdb::storage::lsm::{LsmEngine, TOMBSTONE};
use ferrisdb::storage::sstable::{SSTableReader, SSTableWriter};
use ferrisdb::storage::traits::StorageEngine;

fn temp_dir(name: &str) -> PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time")
        .as_nanos();
    std::env::temp_dir().join(format!("ferrisdb-compaction-{}-{}", name, nanos))
}

fn write_sstable(path: &PathBuf, entries: &[(&[u8], &[u8])]) {
    let mut writer = SSTableWriter::new(path).expect("create writer");
    for (key, value) in entries {
        writer.write_entry(key, value).expect("write entry");
    }
    writer.finish().expect("finish sstable");
}

#[test]
fn test_compact_merges_multiple_sstables() {
    let dir = temp_dir("merge");
    fs::create_dir_all(&dir).expect("create dir");

    let newest = dir.join("000003.sst");
    let older = dir.join("000002.sst");
    let oldest = dir.join("000001.sst");
    let output = dir.join("000004.sst");

    write_sstable(&oldest, &[(b"a", b"1"), (b"b", b"1")]);
    write_sstable(&older, &[(b"b", b"2"), (b"c", b"2")]);
    write_sstable(&newest, &[(b"c", b"3"), (b"d", b"4")]);

    compaction::compact(&[newest.clone(), older.clone(), oldest.clone()], &output)
        .expect("compact");

    let reader = SSTableReader::open(&output).expect("open output");
    assert_eq!(reader.get(b"a").expect("get a"), Some(b"1".to_vec()));
    assert_eq!(reader.get(b"b").expect("get b"), Some(b"2".to_vec()));
    assert_eq!(reader.get(b"c").expect("get c"), Some(b"3".to_vec()));
    assert_eq!(reader.get(b"d").expect("get d"), Some(b"4".to_vec()));

    let _ = fs::remove_dir_all(dir);
}

#[test]
fn test_compact_drops_tombstones() {
    let dir = temp_dir("tombstone");
    fs::create_dir_all(&dir).expect("create dir");

    let newest = dir.join("000002.sst");
    let oldest = dir.join("000001.sst");
    let output = dir.join("000003.sst");

    write_sstable(&oldest, &[(b"a", b"1"), (b"b", b"2")]);
    write_sstable(&newest, &[(b"a", TOMBSTONE)]);

    compaction::compact(&[newest.clone(), oldest.clone()], &output).expect("compact");
    let reader = SSTableReader::open(&output).expect("open output");

    assert_eq!(reader.get(b"a").expect("get a"), None);
    assert_eq!(reader.get(b"b").expect("get b"), Some(b"2".to_vec()));

    let items: Vec<(Vec<u8>, Vec<u8>)> = reader
        .iter()
        .expect("iter")
        .map(|item| item.expect("entry"))
        .collect();
    assert_eq!(items.len(), 1);

    let _ = fs::remove_dir_all(dir);
}

#[test]
fn test_lsm_level_compaction_keeps_reads_correct() {
    let dir = temp_dir("lsm");
    let mut engine = LsmEngine::open(&dir, 64).expect("open lsm");

    for i in 0..60_u32 {
        let key = format!("key:{:03}", i);
        let value = format!("value:{}-xxxxxxxxxxxxxxxx", i);
        engine.put(key.into_bytes(), value.into_bytes()).expect("put");
    }

    engine.put(b"key:010".to_vec(), b"latest-10".to_vec()).expect("update 10");
    engine.put(b"key:020".to_vec(), b"latest-20".to_vec()).expect("update 20");
    engine.delete(b"key:030").expect("delete 30");
    engine.compact().expect("manual compact");

    assert_eq!(engine.get(b"key:010").expect("get 10"), Some(b"latest-10".to_vec()));
    assert_eq!(engine.get(b"key:020").expect("get 20"), Some(b"latest-20".to_vec()));
    assert_eq!(engine.get(b"key:030").expect("get 30"), None);
    assert_eq!(
        engine.get(b"key:040").expect("get 40"),
        Some(b"value:40-xxxxxxxxxxxxxxxx".to_vec())
    );

    let _ = fs::remove_dir_all(dir);
}

#[test]
fn test_compact_removes_old_sstable_files() {
    let dir = temp_dir("remove-old");
    let mut engine = LsmEngine::open(&dir, 64).expect("open lsm");

    // 先建立 4 個 sstable，避免超過自動 compaction 閾值（> 4）而被提前合併。
    for batch in 0..4_u32 {
        let key = format!("batch:{:03}", batch);
        let value = format!("payload:{}-yyyyyyyyyyyyyyyyyyyy", batch);
        engine.put(key.into_bytes(), value.into_bytes()).expect("put batch key");
        engine
            .put(
                format!("filler:{:03}", batch).into_bytes(),
                b"zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz".to_vec(),
            )
            .expect("put filler");
    }

    let before: Vec<PathBuf> = fs::read_dir(&dir)
        .expect("read dir before")
        .filter_map(|entry| entry.ok().map(|e| e.path()))
        .filter(|path| path.extension().and_then(|e| e.to_str()) == Some("sst"))
        .collect();
    assert!(before.len() > 1, "expected multiple sstables before compact");

    engine.compact().expect("compact");

    let after: Vec<PathBuf> = fs::read_dir(&dir)
        .expect("read dir after")
        .filter_map(|entry| entry.ok().map(|e| e.path()))
        .filter(|path| path.extension().and_then(|e| e.to_str()) == Some("sst"))
        .collect();
    assert_eq!(after.len(), 1, "expected only one sstable after compact");

    for old in before {
        if old != after[0] {
            assert!(!old.exists(), "old sstable should be removed: {:?}", old);
        }
    }

    let _ = fs::remove_dir_all(dir);
}

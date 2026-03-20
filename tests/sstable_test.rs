// =============================================================================
// tests/sstable_test.rs — SSTable 整合測試
// =============================================================================
//
// 這組測試會走完整流程（writer -> reader），確保檔案格式與查詢邏輯正確。
// 涵蓋情境：
// 1. 基本寫入與讀取
// 2. 大量資料 + 隨機抽查
// 3. 空表
// 4. key 不存在
// 5. iter 順序

use std::fs;
use std::path::PathBuf;
use std::time::{SystemTime, UNIX_EPOCH};

use ferrisdb::storage::sstable::{SSTableReader, SSTableWriter};

fn temp_file(name: &str) -> PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time")
        .as_nanos();
    std::env::temp_dir().join(format!("ferrisdb-sstable-{}-{}.sst", name, nanos))
}

#[test]
fn test_write_then_read_verify() {
    let path = temp_file("basic");

    let mut writer = SSTableWriter::new(&path).expect("create writer");
    writer.write_entry(b"user:1", b"Alice").expect("write user:1");
    writer.write_entry(b"user:2", b"Bob").expect("write user:2");
    writer.finish().expect("finish");

    let reader = SSTableReader::open(&path).expect("open reader");
    assert_eq!(
        reader.get(b"user:1").expect("get user:1"),
        Some(b"Alice".to_vec())
    );
    assert_eq!(
        reader.get(b"user:2").expect("get user:2"),
        Some(b"Bob".to_vec())
    );

    let _ = fs::remove_file(path);
}

#[test]
fn test_large_write_and_random_reads() {
    let path = temp_file("large-random");
    let total = 5_000_u32;

    let mut writer = SSTableWriter::new(&path).expect("create writer");
    for i in 0..total {
        let key = format!("key:{:05}", i);
        let value = format!("value:{:05}", i);
        writer
            .write_entry(key.as_bytes(), value.as_bytes())
            .expect("write bulk entry");
    }
    writer.finish().expect("finish bulk table");

    let reader = SSTableReader::open(&path).expect("open reader");

    // 不引入 rand crate，使用簡單 LCG 做可重現「隨機」抽樣。
    let mut seed: u64 = 0x1234_5678_abcd_ef01;
    for _ in 0..300 {
        seed = seed
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1);
        let idx = (seed % total as u64) as u32;

        let key = format!("key:{:05}", idx);
        let expected = format!("value:{:05}", idx).into_bytes();
        assert_eq!(
            reader.get(key.as_bytes()).expect("random get"),
            Some(expected)
        );
    }

    let _ = fs::remove_file(path);
}

#[test]
fn test_empty_sstable() {
    let path = temp_file("empty");

    let mut writer = SSTableWriter::new(&path).expect("create writer");
    writer.finish().expect("finish empty table");

    let reader = SSTableReader::open(&path).expect("open reader");
    assert_eq!(reader.get(b"anything").expect("get on empty"), None);

    let all: Vec<(Vec<u8>, Vec<u8>)> = reader
        .iter()
        .expect("create iterator")
        .map(|entry| entry.expect("iter entry"))
        .collect();
    assert!(all.is_empty());

    let _ = fs::remove_file(path);
}

#[test]
fn test_key_not_found() {
    let path = temp_file("not-found");

    let mut writer = SSTableWriter::new(&path).expect("create writer");
    writer.write_entry(b"a", b"1").expect("write a");
    writer.write_entry(b"b", b"2").expect("write b");
    writer.write_entry(b"c", b"3").expect("write c");
    writer.finish().expect("finish");

    let reader = SSTableReader::open(&path).expect("open reader");
    assert_eq!(reader.get(b"d").expect("get d"), None);
    assert_eq!(reader.get(b"aa").expect("get aa"), None);

    let _ = fs::remove_file(path);
}

#[test]
fn test_iter_order() {
    let path = temp_file("iter-order");

    let mut writer = SSTableWriter::new(&path).expect("create writer");
    writer.write_entry(b"apple", b"1").expect("write apple");
    writer.write_entry(b"banana", b"2").expect("write banana");
    writer.write_entry(b"cherry", b"3").expect("write cherry");
    writer.finish().expect("finish");

    let reader = SSTableReader::open(&path).expect("open reader");
    let collected: Vec<(Vec<u8>, Vec<u8>)> = reader
        .iter()
        .expect("create iterator")
        .map(|entry| entry.expect("iter entry"))
        .collect();

    assert_eq!(
        collected,
        vec![
            (b"apple".to_vec(), b"1".to_vec()),
            (b"banana".to_vec(), b"2".to_vec()),
            (b"cherry".to_vec(), b"3".to_vec()),
        ]
    );

    let _ = fs::remove_file(path);
}

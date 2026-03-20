// =============================================================================
// tests/failure_injection_test.rs -- Failure Injection Tests
// =============================================================================
//
// 這些測試模擬資料庫在真實世界常見的故障場景：
// - 斷電導致 WAL 只寫了一半
// - 磁碟位元翻轉導致 SSTable footer / data 損壞
// - MANIFEST 被截斷或損壞
// - compaction 做到一半留下臨時檔
// - 長時間混合操作 + restart 壓力下是否仍與參考模型一致
// - 多執行緒並發寫入是否有遺失資料或 panic
//
// 正確的資料庫不一定要「自動修好一切」，
// 但至少應該做到：
// - 不 panic
// - 對可恢復的部分能恢復
// - 對不可恢復的部分能回報錯誤或安全跳過

use std::collections::BTreeMap;
use std::fs;
use std::io::{Seek, SeekFrom, Write};
use std::mem;
use std::path::PathBuf;
use std::sync::Arc;
use std::thread;
use std::time::{SystemTime, UNIX_EPOCH};

use ferrisdb::storage::lsm::LsmEngine;
use ferrisdb::storage::manifest::MANIFEST_FILENAME;
use ferrisdb::storage::traits::StorageEngine;
use ferrisdb::transaction::mvcc::MvccEngine;

fn temp_dir(name: &str) -> PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time")
        .as_nanos();
    std::env::temp_dir().join(format!("ferrisdb-failure-{}-{}", name, nanos))
}

fn first_sstable_path(dir: &PathBuf) -> PathBuf {
    fs::read_dir(dir)
        .expect("read dir")
        .filter_map(|entry| entry.ok().map(|e| e.path()))
        .find(|path| path.extension().and_then(|ext| ext.to_str()) == Some("sst"))
        .expect("sstable should exist")
}

/// 這個測試模擬「WAL 最後一筆 record 還沒寫完就突然斷電」。
/// 真實世界常見於作業系統還沒把完整資料刷到磁碟時就掉電。
/// 正確行為是：前面完整的 record 能恢復，最後不完整的那筆被安全忽略。
#[test]
fn test_truncated_wal_recovers_complete_records_only() {
    let dir = temp_dir("truncated-wal");
    let mut engine = LsmEngine::open(&dir, 4096).expect("open lsm");
    engine.put(b"a".to_vec(), b"1".to_vec()).expect("put a");
    engine.put(b"b".to_vec(), b"2".to_vec()).expect("put b");
    engine.put(b"c".to_vec(), b"3".to_vec()).expect("put c");

    let wal_path = dir.join("wal.log");
    mem::forget(engine);

    let metadata = fs::metadata(&wal_path).expect("wal metadata");
    assert!(metadata.len() > 8);
    let truncated_len = metadata.len() - 5;
    let file = fs::OpenOptions::new()
        .write(true)
        .open(&wal_path)
        .expect("open wal for truncate");
    file.set_len(truncated_len).expect("truncate wal");

    let reopened = LsmEngine::open(&dir, 4096).expect("reopen after truncated wal");
    assert_eq!(reopened.get(b"a").expect("get a"), Some(b"1".to_vec()));
    assert_eq!(reopened.get(b"b").expect("get b"), Some(b"2".to_vec()));
    assert_eq!(reopened.get(b"c").expect("get c"), None);

    let _ = fs::remove_dir_all(dir);
}

/// 這個測試模擬 SSTable footer 被磁碟損壞。
/// 真實世界可能來自位元翻轉、檔案系統錯誤、或未完整寫入 footer。
/// 正確行為是偵測出格式錯誤並回傳錯誤，而不是 panic。
#[test]
fn test_sstable_footer_corruption_is_detected() {
    let dir = temp_dir("sstable-footer");
    let mut engine = LsmEngine::open(&dir, 32).expect("open lsm");
    for i in 0..10_u32 {
        engine
            .put(
                format!("k{:02}", i).into_bytes(),
                b"xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx".to_vec(),
            )
            .expect("put");
    }
    engine.shutdown().expect("shutdown");

    let path = first_sstable_path(&dir);
    let mut file = fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(&path)
        .expect("open sstable");
    let len = file.metadata().expect("metadata").len();
    file.seek(SeekFrom::Start(len - 24)).expect("seek footer");
    file.write_all(&[0xFF; 8]).expect("corrupt footer");
    file.sync_all().expect("sync");

    let reopened = LsmEngine::open(&dir, 32);
    assert!(reopened.is_err(), "corrupted footer should be detected");

    let _ = fs::remove_dir_all(dir);
}

/// 這個測試模擬 SSTable data section 中間某個 byte 壞掉。
/// 真實世界常見於壞磁區或儲存裝置 silent corruption。
/// 第一版沒有 entry checksum，所以至少要能觀察到資料被改壞，或回傳錯誤。
#[test]
fn test_sstable_data_corruption_is_observable() {
    let dir = temp_dir("sstable-data");
    let mut engine = LsmEngine::open(&dir, 32).expect("open lsm");
    engine
        .put(
            b"victim".to_vec(),
            b"original-value-xxxxxxxxxxxxxxxx".to_vec(),
        )
        .expect("put victim");
    for i in 0..8_u32 {
        engine
            .put(
                format!("f{:02}", i).into_bytes(),
                b"yyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyy".to_vec(),
            )
            .expect("put filler");
    }
    engine.shutdown().expect("shutdown");

    let path = first_sstable_path(&dir);
    let mut bytes = fs::read(&path).expect("read sstable");
    let needle = b"original-value-xxxxxxxxxxxxxxxx";
    let start = bytes
        .windows(needle.len())
        .position(|window| window == needle)
        .expect("victim value should exist in sstable bytes");
    bytes[start + 5] ^= 0x5A;
    fs::write(&path, bytes).expect("write corrupted sstable");

    let reopened = LsmEngine::open(&dir, 32).expect("reopen");
    let result = reopened.get(b"victim");
    match result {
        Ok(value) => assert_ne!(value, Some(b"original-value-xxxxxxxxxxxxxxxx".to_vec())),
        Err(_) => {}
    }

    let _ = fs::remove_dir_all(dir);
}

/// 這個測試模擬 MANIFEST 被截斷。
/// 真實世界可能發生在 append metadata 到一半時掉電。
/// 正確行為可以是降級到掃描資料夾模式，或回傳有意義的錯誤；不能 panic。
#[test]
fn test_truncated_manifest_can_recover_or_fail_cleanly() {
    let dir = temp_dir("manifest-truncate");
    let mut engine = LsmEngine::open(&dir, 32).expect("open lsm");
    for i in 0..10_u32 {
        engine
            .put(
                format!("k{:02}", i).into_bytes(),
                b"zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz".to_vec(),
            )
            .expect("put");
    }
    engine.shutdown().expect("shutdown");

    let path = dir.join(MANIFEST_FILENAME);
    let file = fs::OpenOptions::new()
        .write(true)
        .open(&path)
        .expect("open manifest");
    let len = file.metadata().expect("metadata").len();
    file.set_len(len / 2).expect("truncate manifest");

    match LsmEngine::open(&dir, 32) {
        Ok(reopened) => {
            assert!(reopened.get(b"k00").expect("get after fallback").is_some());
        }
        Err(err) => {
            let message = format!("{}", err);
            assert!(
                message.contains("manifest") || message.contains("checksum") || message.contains("record"),
                "unexpected error: {}",
                message
            );
        }
    }

    let _ = fs::remove_dir_all(dir);
}

/// 這個測試模擬 compaction 途中留下臨時檔。
/// 真實世界常見於 merge 完成前程序被 kill，磁碟上殘留半成品。
/// 正確行為是重啟時清掉明顯的臨時檔，並維持既有資料可讀。
#[test]
fn test_compaction_temp_file_is_cleaned_on_reopen() {
    let dir = temp_dir("compaction-temp");
    let mut engine = LsmEngine::open(&dir, 64).expect("open lsm");
    for i in 0..20_u32 {
        engine
            .put(
                format!("k{:02}", i).into_bytes(),
                b"payload-payload-payload-payload".to_vec(),
            )
            .expect("put");
    }
    engine.shutdown().expect("shutdown");

    let temp_path = dir.join("000999.sst.compacting");
    fs::write(&temp_path, b"partial compaction").expect("write temp file");
    assert!(temp_path.exists());

    let reopened = LsmEngine::open(&dir, 64).expect("reopen");
    assert!(!temp_path.exists(), "temporary compaction file should be removed");
    assert_eq!(reopened.get(b"k00").expect("get k00").is_some(), true);

    let _ = fs::remove_dir_all(dir);
}

/// 這個測試是故障注入的總壓力測試。
/// 真實世界的資料庫會遇到各種操作交錯：寫入、刪除、scan、flush、compact、restart。
/// 正確行為是：即使反覆 reopen，資料仍與參考模型一致，而且不會 panic。
#[test]
fn test_stress_with_reopen_matches_reference_model() {
    let dir = temp_dir("stress");
    let mut engine = LsmEngine::open(&dir, 128).expect("open lsm");
    let mut model = BTreeMap::<Vec<u8>, Vec<u8>>::new();
    let mut rng = DeterministicRng::new(0xDEADBEEFCAFEBABE);

    for _ in 0..1000 {
        match rng.next_usize(6) {
            0 => {
                let key = random_key(&mut rng);
                let value = random_value(&mut rng);
                engine.put(key.clone(), value.clone()).expect("put");
                model.insert(key, value);
            }
            1 => {
                let key = random_key(&mut rng);
                engine.delete(&key).expect("delete");
                model.remove(&key);
            }
            2 => {
                let start = b"k010".to_vec();
                let end = b"k070".to_vec();
                let actual: BTreeMap<Vec<u8>, Vec<u8>> =
                    engine.scan(&start, &end).expect("scan").into_iter().collect();
                let expected: BTreeMap<Vec<u8>, Vec<u8>> = model
                    .iter()
                    .filter(|(key, _)| key.as_slice() >= start.as_slice() && key.as_slice() <= end.as_slice())
                    .map(|(k, v)| (k.clone(), v.clone()))
                    .collect();
                assert_eq!(actual, expected);
            }
            3 => {
                let _ = engine.flush();
            }
            4 => {
                let _ = engine.compact();
            }
            5 => {
                engine.shutdown().expect("shutdown");
                engine = LsmEngine::open(&dir, 128).expect("reopen");
                let actual: BTreeMap<Vec<u8>, Vec<u8>> =
                    engine.list_all().expect("list_all").into_iter().collect();
                assert_eq!(actual, model);
            }
            _ => unreachable!(),
        }
    }

    let final_actual: BTreeMap<Vec<u8>, Vec<u8>> =
        engine.list_all().expect("final list_all").into_iter().collect();
    assert_eq!(final_actual, model);

    let _ = fs::remove_dir_all(dir);
}

/// 這個測試模擬多個 client / thread 同時寫入。
/// 真實世界中 server 會同時處理很多連線，若同步或 MVCC 有 bug，容易遺失資料。
/// 正確行為是所有 thread 完成後資料完整存在，且過程中沒有 panic。
#[test]
fn test_concurrent_writes_preserve_all_rows() {
    let dir = temp_dir("concurrent");
    let engine = Arc::new(MvccEngine::new(
        LsmEngine::open(&dir, 256).expect("open lsm"),
    ));

    let mut handles = Vec::new();
    for thread_id in 0..10_u32 {
        let shared = Arc::clone(&engine);
        handles.push(thread::spawn(move || {
            for i in 0..100_u32 {
                let mut txn = shared.begin_transaction();
                txn.put(
                    format!("t{}:{:03}", thread_id, i).into_bytes(),
                    format!("value-{}-{}", thread_id, i).into_bytes(),
                )
                .expect("put");
                txn.commit().expect("commit");
            }
        }));
    }

    for handle in handles {
        handle.join().expect("thread join");
    }

    let txn = engine.begin_transaction();
    let rows = txn.scan(&[], &[0xFF]).expect("scan all");
    assert_eq!(rows.len(), 1000);
    assert_eq!(
        txn.get(b"t0:000").expect("get sample"),
        Some(b"value-0-0".to_vec())
    );
    assert_eq!(
        txn.get(b"t9:099").expect("get sample"),
        Some(b"value-9-99".to_vec())
    );

    let _ = fs::remove_dir_all(dir);
}

#[derive(Debug, Clone)]
struct DeterministicRng {
    state: u64,
}

impl DeterministicRng {
    fn new(seed: u64) -> Self {
        Self { state: seed }
    }

    fn next_u64(&mut self) -> u64 {
        self.state = self
            .state
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1);
        self.state
    }

    fn next_usize(&mut self, modulo: usize) -> usize {
        (self.next_u64() % modulo as u64) as usize
    }
}

fn random_key(rng: &mut DeterministicRng) -> Vec<u8> {
    format!("k{:03}", rng.next_usize(100)).into_bytes()
}

fn random_value(rng: &mut DeterministicRng) -> Vec<u8> {
    format!("v{:06}", rng.next_u64() % 1_000_000).into_bytes()
}

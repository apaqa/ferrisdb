// =============================================================================
// tests/background_compaction_test.rs -- Background Compaction Tests
// =============================================================================
//
// 中文註解：驗證背景 compaction worker 會自動執行，且資料在 compact 前後仍正確。

use std::fs;
use std::path::PathBuf;
use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use ferrisdb::storage::lsm::LsmEngine;
use ferrisdb::storage::traits::StorageEngine;

fn temp_dir(name: &str) -> PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time")
        .as_nanos();
    std::env::temp_dir().join(format!("ferrisdb-bg-compact-{}-{}", name, nanos))
}

#[test]
fn test_background_compaction_runs_and_preserves_data() {
    let dir = temp_dir("auto");
    let mut engine = LsmEngine::open(&dir, 64).expect("open lsm");

    for i in 0..120_u32 {
        let key = format!("user:{:03}", i);
        let value = format!("value-{}-xxxxxxxxxxxxxxxxxxxx", i);
        engine
            .put(key.clone().into_bytes(), value.clone().into_bytes())
            .expect("put entry");
    }

    let sstable_count_before = engine.sstable_infos().expect("sstable infos before").len();
    assert!(
        sstable_count_before > 1,
        "expected multiple sstables before compaction"
    );

    thread::sleep(Duration::from_secs(7));

    let sstable_count_after = engine.sstable_infos().expect("sstable infos after").len();
    let (_, _, total_compactions) = engine.compaction_status();
    assert!(
        total_compactions >= 1,
        "background compaction should have run"
    );
    assert!(
        sstable_count_after <= sstable_count_before,
        "background compaction should not increase sstable count"
    );

    for i in 0..120_u32 {
        let key = format!("user:{:03}", i);
        let value = format!("value-{}-xxxxxxxxxxxxxxxxxxxx", i);
        assert_eq!(
            engine.get(key.as_bytes()).expect("get after compaction"),
            Some(value.into_bytes())
        );
    }

    engine.shutdown().expect("shutdown");
    let (enabled, _, total_compactions_after_shutdown) = engine.compaction_status();
    assert!(!enabled, "worker should be stopped after shutdown");
    assert!(total_compactions_after_shutdown >= total_compactions);

    let _ = fs::remove_dir_all(dir);
}

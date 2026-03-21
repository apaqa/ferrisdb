// =============================================================================
// tests/manifest_test.rs -- MANIFEST Tests
// =============================================================================
//
// 這些測試驗證：
// - MANIFEST record replay 是否能恢復正確狀態
// - compaction metadata 是否會更新有效 SSTable 列表
// - snapshot record 之後 reopen 是否仍能恢復
// - checksum 損壞是否能被偵測
// - LsmEngine 是否改為依 MANIFEST 恢復，而不是只掃描資料夾

use std::fs;
use std::fs::OpenOptions;
use std::io::{Seek, SeekFrom, Write};
use std::path::PathBuf;
use std::time::{SystemTime, UNIX_EPOCH};

use ferrisdb::storage::lsm::LsmEngine;
use ferrisdb::storage::manifest::{Manifest, ManifestRecord, MANIFEST_FILENAME};
use ferrisdb::storage::sstable::SSTableWriter;
use ferrisdb::storage::traits::StorageEngine;

fn temp_dir(name: &str) -> PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time")
        .as_nanos();
    std::env::temp_dir().join(format!("ferrisdb-manifest-{}-{}", name, nanos))
}

fn write_sstable(dir: &PathBuf, id: u64, entries: &[(&[u8], &[u8])]) -> PathBuf {
    let path = dir.join(format!("{:06}.sst", id));
    let mut writer = SSTableWriter::new(&path).expect("create writer");
    for (key, value) in entries {
        writer.write_entry(key, value).expect("write entry");
    }
    writer.finish().expect("finish sstable");
    path
}

#[test]
fn test_manifest_replay_add_and_remove_records() {
    let dir = temp_dir("replay");
    fs::create_dir_all(&dir).expect("create dir");
    let path = dir.join(MANIFEST_FILENAME);

    let mut manifest = Manifest::create(&path).expect("create manifest");
    manifest
        .append_record(ManifestRecord::AddSstable {
            filename: "000001.sst".to_string(),
        })
        .expect("add 1");
    manifest
        .append_record(ManifestRecord::AddSstable {
            filename: "000002.sst".to_string(),
        })
        .expect("add 2");
    manifest
        .append_record(ManifestRecord::RemoveSstable {
            filename: "000001.sst".to_string(),
        })
        .expect("remove 1");
    drop(manifest);

    let reopened = Manifest::open(&path).expect("reopen manifest");
    assert_eq!(reopened.current_sstables(), &["000002.sst"]);
    assert_eq!(reopened.next_sstable_id, 3);

    let _ = fs::remove_dir_all(dir);
}

#[test]
fn test_manifest_compaction_record_updates_sstable_list() {
    let dir = temp_dir("compaction");
    fs::create_dir_all(&dir).expect("create dir");
    let path = dir.join(MANIFEST_FILENAME);

    let mut manifest = Manifest::create(&path).expect("create manifest");
    manifest
        .append_record(ManifestRecord::Snapshot {
            sstable_files: vec!["000003.sst".to_string(), "000002.sst".to_string()],
            next_sstable_id: 4,
        })
        .expect("snapshot");
    manifest
        .append_record(ManifestRecord::Compaction {
            added: vec!["000004.sst".to_string()],
            removed: vec!["000003.sst".to_string(), "000002.sst".to_string()],
        })
        .expect("compaction");
    drop(manifest);

    let reopened = Manifest::open(&path).expect("reopen manifest");
    assert_eq!(reopened.current_sstables(), &["000004.sst"]);
    assert_eq!(reopened.next_sstable_id, 5);
    assert!(reopened.last_compaction_ts > 0);

    let _ = fs::remove_dir_all(dir);
}

#[test]
fn test_manifest_snapshot_reopen_restores_state() {
    let dir = temp_dir("snapshot");
    fs::create_dir_all(&dir).expect("create dir");
    let path = dir.join(MANIFEST_FILENAME);

    let mut manifest = Manifest::create(&path).expect("create manifest");
    manifest
        .append_record(ManifestRecord::AddSstable {
            filename: "000001.sst".to_string(),
        })
        .expect("add");
    manifest.snapshot().expect("snapshot");
    drop(manifest);

    let reopened = Manifest::open(&path).expect("reopen manifest");
    assert_eq!(reopened.current_sstables(), &["000001.sst"]);
    assert_eq!(reopened.next_sstable_id, 2);

    let _ = fs::remove_dir_all(dir);
}

#[test]
fn test_manifest_checksum_corruption_detected() {
    let dir = temp_dir("checksum");
    fs::create_dir_all(&dir).expect("create dir");
    let path = dir.join(MANIFEST_FILENAME);

    let mut manifest = Manifest::create(&path).expect("create manifest");
    manifest
        .append_record(ManifestRecord::AddSstable {
            filename: "000001.sst".to_string(),
        })
        .expect("add");
    drop(manifest);

    let mut file = OpenOptions::new()
        .read(true)
        .write(true)
        .open(&path)
        .expect("open for corruption");
    file.seek(SeekFrom::Start(10)).expect("seek");
    file.write_all(&[0x42]).expect("overwrite byte");
    file.sync_all().expect("sync");

    let err = Manifest::open(&path).expect_err("checksum should fail");
    assert!(format!("{}", err).contains("checksum"));

    let _ = fs::remove_dir_all(dir);
}

#[test]
fn test_lsm_manifest_stays_consistent_after_flush_compact_restart() {
    let dir = temp_dir("lsm-restart");
    let mut engine = LsmEngine::open(&dir, 64).expect("open lsm");

    for batch in 0..5_u32 {
        let key = format!("k{:03}", batch);
        let value = format!("payload-{}-xxxxxxxxxxxxxxxxxxxxxxxx", batch);
        engine
            .put(key.into_bytes(), value.into_bytes())
            .expect("put");
        engine
            .put(
                format!("filler:{:03}", batch).into_bytes(),
                b"yyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyy".to_vec(),
            )
            .expect("put filler");
    }

    engine.compact().expect("compact");
    engine.shutdown().expect("shutdown");
    drop(engine);

    let reopened = LsmEngine::open(&dir, 64).expect("reopen lsm");
    let manifest_state = reopened.manifest_state();
    assert_eq!(
        manifest_state.sstable_files.len(),
        reopened.sstable_infos().expect("sstable infos").len()
    );
    for filename in &manifest_state.sstable_files {
        assert!(
            dir.join(filename).exists(),
            "manifest file should exist: {}",
            filename
        );
    }

    let disk_files: Vec<String> = fs::read_dir(&dir)
        .expect("read dir")
        .filter_map(|entry| entry.ok().map(|e| e.path()))
        .filter(|path| path.extension().and_then(|ext| ext.to_str()) == Some("sst"))
        .filter_map(|path| {
            path.file_name()
                .and_then(|name| name.to_str())
                .map(|s| s.to_string())
        })
        .collect();
    assert_eq!(disk_files.len(), manifest_state.sstable_files.len());

    let _ = fs::remove_dir_all(dir);
}

#[test]
fn test_lsm_without_manifest_can_upgrade_from_directory_scan() {
    let dir = temp_dir("upgrade");
    fs::create_dir_all(&dir).expect("create dir");

    write_sstable(&dir, 1, &[(b"a", b"1")]);
    write_sstable(&dir, 2, &[(b"b", b"2")]);
    assert!(!dir.join(MANIFEST_FILENAME).exists());

    let engine = LsmEngine::open(&dir, 4096).expect("open lsm");
    let manifest_state = engine.manifest_state();

    assert!(dir.join(MANIFEST_FILENAME).exists());
    assert_eq!(
        manifest_state.sstable_files,
        vec!["000002.sst".to_string(), "000001.sst".to_string()]
    );
    assert_eq!(engine.get(b"a").expect("get a"), Some(b"1".to_vec()));
    assert_eq!(engine.get(b"b").expect("get b"), Some(b"2".to_vec()));

    let _ = fs::remove_dir_all(dir);
}

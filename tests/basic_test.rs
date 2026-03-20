use std::fs;
use std::path::PathBuf;
use std::time::{SystemTime, UNIX_EPOCH};

use ferrisdb::cli::repl::{dump_to_file, load_from_file};
use ferrisdb::storage::memory::MemTable;
use ferrisdb::storage::traits::StorageEngine;

#[test]
fn test_full_crud_cycle() {
    let mut db = MemTable::new();

    db.put(b"user:1".to_vec(), b"Alice".to_vec()).unwrap();
    db.put(b"user:2".to_vec(), b"Bob".to_vec()).unwrap();
    db.put(b"user:3".to_vec(), b"Charlie".to_vec()).unwrap();

    assert_eq!(db.get(b"user:1").unwrap(), Some(b"Alice".to_vec()));
    assert_eq!(db.count(), 3);

    db.put(b"user:1".to_vec(), b"Alice Updated".to_vec()).unwrap();
    assert_eq!(
        db.get(b"user:1").unwrap(),
        Some(b"Alice Updated".to_vec())
    );
    assert_eq!(db.count(), 3);

    db.delete(b"user:2").unwrap();
    assert_eq!(db.get(b"user:2").unwrap(), None);
    assert_eq!(db.count(), 2);
}

#[test]
fn test_scan_with_prefix() {
    let mut db = MemTable::new();

    db.put(b"post:001".to_vec(), b"Hello World".to_vec()).unwrap();
    db.put(b"post:002".to_vec(), b"Rust is cool".to_vec()).unwrap();
    db.put(b"post:003".to_vec(), b"FerrisDB rocks".to_vec()).unwrap();
    db.put(b"user:001".to_vec(), b"Alice".to_vec()).unwrap();
    db.put(b"user:002".to_vec(), b"Bob".to_vec()).unwrap();

    let posts = db.scan(b"post:001", b"post:003").unwrap();
    assert_eq!(posts.len(), 3);

    let users = db.scan(b"user:001", b"user:002").unwrap();
    assert_eq!(users.len(), 2);
}

#[test]
fn test_empty_database() {
    let db = MemTable::new();

    assert_eq!(db.count(), 0);
    assert_eq!(db.get(b"anything").unwrap(), None);
    assert_eq!(db.list_all().unwrap().len(), 0);
    assert_eq!(db.scan(b"a", b"z").unwrap().len(), 0);
}

#[test]
fn test_list_all_sorted() {
    let mut db = MemTable::new();

    db.put(b"cherry".to_vec(), b"3".to_vec()).unwrap();
    db.put(b"apple".to_vec(), b"1".to_vec()).unwrap();
    db.put(b"banana".to_vec(), b"2".to_vec()).unwrap();

    let all = db.list_all().unwrap();
    assert_eq!(all[0].0, b"apple".to_vec());
    assert_eq!(all[1].0, b"banana".to_vec());
    assert_eq!(all[2].0, b"cherry".to_vec());
}

#[test]
fn test_bulk_insert() {
    let mut db = MemTable::new();

    for i in 0..1000 {
        let key = format!("key:{:04}", i);
        let value = format!("value:{}", i);
        db.put(key.into_bytes(), value.into_bytes()).unwrap();
    }

    assert_eq!(db.count(), 1000);

    let val = db.get(b"key:0500").unwrap();
    assert_eq!(val, Some(b"value:500".to_vec()));

    let range = db.scan(b"key:0100", b"key:0199").unwrap();
    assert_eq!(range.len(), 100);
}

fn unique_temp_file(name: &str) -> PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    std::env::temp_dir().join(format!("ferrisdb-{}-{}.json", name, nanos))
}

#[test]
fn test_dump_writes_json_file() {
    let mut db = MemTable::new();
    db.put(b"user:1".to_vec(), b"Alice".to_vec()).unwrap();
    db.put(b"user:2".to_vec(), b"Bob".to_vec()).unwrap();

    let file_path = unique_temp_file("dump");
    let file_name = file_path.to_string_lossy().into_owned();

    dump_to_file(&db, &file_name).unwrap();

    let content = fs::read_to_string(&file_path).unwrap();
    let json: serde_json::Value = serde_json::from_str(&content).unwrap();

    assert_eq!(json["user:1"], "Alice");
    assert_eq!(json["user:2"], "Bob");

    fs::remove_file(file_path).unwrap();
}

#[test]
fn test_load_merges_and_overwrites_existing_keys() {
    let mut source = MemTable::new();
    source
        .put(b"user:1".to_vec(), b"Alice Updated".to_vec())
        .unwrap();
    source.put(b"user:2".to_vec(), b"Bob".to_vec()).unwrap();

    let file_path = unique_temp_file("load");
    let file_name = file_path.to_string_lossy().into_owned();
    dump_to_file(&source, &file_name).unwrap();

    let mut target = MemTable::new();
    target.put(b"user:1".to_vec(), b"Alice".to_vec()).unwrap();
    target.put(b"user:3".to_vec(), b"Charlie".to_vec()).unwrap();

    load_from_file(&mut target, &file_name).unwrap();

    assert_eq!(
        target.get(b"user:1").unwrap(),
        Some(b"Alice Updated".to_vec())
    );
    assert_eq!(target.get(b"user:2").unwrap(), Some(b"Bob".to_vec()));
    assert_eq!(target.get(b"user:3").unwrap(), Some(b"Charlie".to_vec()));
    assert_eq!(target.count(), 3);

    fs::remove_file(file_path).unwrap();
}

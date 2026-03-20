// =============================================================================
// tests/basic_test.rs — 整合測試
// =============================================================================
//
// 什麼是整合測試？
// ----------------
// Rust 有兩種測試：
// - 單元測試：寫在各個模組裡（用 #[cfg(test)] mod tests），測試單一模組
// - 整合測試：寫在 tests/ 資料夾裡，模擬「外部使用者」的角度來測試
//
// 整合測試會把你的 crate（專案）當成一個外部函式庫來使用，
// 只能存取 pub（公開的）API。這更能確保你的 API 設計是合理的。
//
// 為什麼我們之前在 memory.rs 裡面已經寫了測試，這裡還要寫？
// 因為 memory.rs 裡面的測試是單元測試,只測它自己裡面的function,
// 這裡的整合測試確保不同模組之間、從外部使用時也沒有問題。

use ferrisdb::storage::memory::MemTable;
use ferrisdb::storage::traits::StorageEngine;

/// 測試完整的 CRUD 流程（Create, Read, Update, Delete）
#[test]
fn test_full_crud_cycle() {
    let mut db = MemTable::new();

    // Create：寫入資料
    db.put(b"user:1".to_vec(), b"Alice".to_vec()).unwrap();
    db.put(b"user:2".to_vec(), b"Bob".to_vec()).unwrap();
    db.put(b"user:3".to_vec(), b"Charlie".to_vec()).unwrap();

    // Read：讀取資料
    assert_eq!(
        db.get(b"user:1").unwrap(),
        Some(b"Alice".to_vec())
    );
    assert_eq!(db.count(), 3);

    // Update：覆蓋已存在的 key
    db.put(b"user:1".to_vec(), b"Alice Updated".to_vec()).unwrap();
    assert_eq!(
        db.get(b"user:1").unwrap(),
        Some(b"Alice Updated".to_vec())
    );
    // count 不變，因為是覆蓋不是新增
    assert_eq!(db.count(), 3);

    // Delete：刪除資料
    db.delete(b"user:2").unwrap();
    assert_eq!(db.get(b"user:2").unwrap(), None);
    assert_eq!(db.count(), 2);
}

/// 測試 scan 在有多筆資料時的行為
#[test]
fn test_scan_with_prefix() {
    let mut db = MemTable::new();

    // 模擬真實場景：用前綴來分類資料
    db.put(b"post:001".to_vec(), b"Hello World".to_vec()).unwrap();
    db.put(b"post:002".to_vec(), b"Rust is cool".to_vec()).unwrap();
    db.put(b"post:003".to_vec(), b"FerrisDB rocks".to_vec()).unwrap();
    db.put(b"user:001".to_vec(), b"Alice".to_vec()).unwrap();
    db.put(b"user:002".to_vec(), b"Bob".to_vec()).unwrap();

    // 只查 post 開頭的
    let posts = db.scan(b"post:001", b"post:003").unwrap();
    assert_eq!(posts.len(), 3);

    // 只查 user 開頭的
    let users = db.scan(b"user:001", b"user:002").unwrap();
    assert_eq!(users.len(), 2);
}

/// 測試空資料庫的行為
#[test]
fn test_empty_database() {
    let db = MemTable::new();

    assert_eq!(db.count(), 0);
    assert_eq!(db.get(b"anything").unwrap(), None);
    assert_eq!(db.list_all().unwrap().len(), 0);
    assert_eq!(db.scan(b"a", b"z").unwrap().len(), 0);
}

/// 測試 list_all 的排序
#[test]
fn test_list_all_sorted() {
    let mut db = MemTable::new();

    // 故意亂序插入
    db.put(b"cherry".to_vec(), b"3".to_vec()).unwrap();
    db.put(b"apple".to_vec(), b"1".to_vec()).unwrap();
    db.put(b"banana".to_vec(), b"2".to_vec()).unwrap();

    let all = db.list_all().unwrap();

    // BTreeMap 保證排序，所以結果應該是 apple, banana, cherry
    assert_eq!(all[0].0, b"apple".to_vec());
    assert_eq!(all[1].0, b"banana".to_vec());
    assert_eq!(all[2].0, b"cherry".to_vec());
}

/// 測試大量資料寫入
#[test]
fn test_bulk_insert() {
    let mut db = MemTable::new();

    // 寫入 1000 筆
    for i in 0..1000 {
        let key = format!("key:{:04}", i);  // key:0000, key:0001, ...
        let value = format!("value:{}", i);
        db.put(key.into_bytes(), value.into_bytes()).unwrap();
    }

    assert_eq!(db.count(), 1000);

    // 確認第 500 筆能正確讀取
    let val = db.get(b"key:0500").unwrap();
    assert_eq!(val, Some(b"value:500".to_vec()));

    // scan 一個子範圍
    let range = db.scan(b"key:0100", b"key:0199").unwrap();
    assert_eq!(range.len(), 100);
}

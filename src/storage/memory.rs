// =============================================================================
// storage/memory.rs — MemTable：記憶體中的 KV 儲存
// =============================================================================
//
// 這是什麼？
// ----------
// MemTable 是整個資料庫最底層的元件之一。
// 它把 key-value pairs 存在記憶體裡的 BTreeMap 中。
// 所有的寫入都先進 MemTable，之後（Phase 2）滿了再 flush 到磁碟。
//
// 為什麼用 BTreeMap 不用 HashMap？
// --------------------------------
// - BTreeMap：key 會自動排序。這對 scan（範圍查詢）非常重要。
//   例如你要查 "user:001" 到 "user:100"，BTreeMap 能直接給你這個範圍。
// - HashMap：key 沒有順序，你沒辦法有效率地做範圍查詢。
//
// 在真實資料庫裡（LevelDB、RocksDB），MemTable 通常用 SkipList，
// 但 BTreeMap 對我們來說功能一樣，而且是 Rust 標準庫自帶的。

use std::collections::BTreeMap;
use crate::error::Result;
use crate::storage::traits::StorageEngine;

/// MemTable — 基於 BTreeMap 的記憶體儲存引擎
///
/// #[derive(Debug, Default)] 是什麼？
/// - derive(Debug)：讓這個 struct 可以用 {:?} 印出來
/// - derive(Default)：自動產生一個 default() 方法，建立空的 MemTable
///
/// pub struct 是什麼？
/// struct 是 Rust 的「結構體」，類似其他語言的 class（但沒有繼承）。
/// pub 表示其他模組可以使用這個 struct。
#[derive(Debug, Default)]
pub struct MemTable {
    /// 實際儲存資料的地方
    /// BTreeMap<Vec<u8>, Vec<u8>> 表示：key 是 bytes，value 也是 bytes
    /// 這裡不用 String 是因為資料庫的 key/value 不一定是文字
    data: BTreeMap<Vec<u8>, Vec<u8>>,
}

/// 為 MemTable 實作一些自己的方法
impl MemTable {
    /// 建立一個新的空 MemTable
    ///
    /// Self 是什麼？
    /// Self 就是指「目前這個 struct 的型別」，也就是 MemTable。
    /// Self { data: BTreeMap::new() } 就等於 MemTable { data: BTreeMap::new() }
    pub fn new() -> Self {
        Self {
            data: BTreeMap::new(),
        }
    }

    /// 估算目前佔用的記憶體大小（bytes）
    /// 這是粗略估算，把所有 key 和 value 的長度加起來
    /// 之後 Phase 2 會用這個值來決定什麼時候把 MemTable flush 到磁碟
    pub fn approximate_size(&self) -> usize {
        self.data
            .iter()  // iter() 會給你 BTreeMap 裡每一對 (key, value) 的借用
            .map(|(k, v)| k.len() + v.len())  // 對每一對計算 key + value 的長度
            .sum()  // 全部加起來
    }
}

/// 為 MemTable 實作 StorageEngine trait
///
/// impl StorageEngine for MemTable 是什麼意思？
/// 表示「MemTable 這個 struct 承諾會提供 StorageEngine 要求的所有方法」。
/// 這樣任何接受 StorageEngine 的程式碼，都可以用 MemTable。
impl StorageEngine for MemTable {
    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        // self.data.get(key) 從 BTreeMap 查找
        // 回傳 Option<&Vec<u8>>：找到就是 Some(&value)，沒找到就是 None
        //
        // .cloned() 是什麼？
        // BTreeMap.get() 回傳的是「借用」(&Vec<u8>)，但我們要回傳「擁有的」(Vec<u8>)
        // .cloned() 會複製一份出來，讓呼叫者擁有那份資料
        //
        // Ok() 是什麼？
        // Result 有兩種值：Ok(成功的值) 和 Err(錯誤)
        // 這裡操作一定成功（記憶體操作不會失敗），所以直接包 Ok
        Ok(self.data.get(key).cloned())
    }

    fn put(&mut self, key: Vec<u8>, value: Vec<u8>) -> Result<()> {
        // insert 會把 key-value 放進 BTreeMap
        // 如果 key 已經存在，舊的 value 會被覆蓋
        self.data.insert(key, value);
        Ok(())
    }

    fn delete(&mut self, key: &[u8]) -> Result<()> {
        // remove 會把指定 key 從 BTreeMap 移除
        // 如果 key 不存在，remove 什麼都不做（不會報錯）
        self.data.remove(key);
        Ok(())
    }

    fn scan(&self, start: &[u8], end: &[u8]) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        // range 是 BTreeMap 最強大的功能之一
        // 它回傳 start 到 end 之間（包含 start 和 end）所有的 key-value pairs
        // 這就是為什麼我們選 BTreeMap 而不是 HashMap
        //
        // ..= 是什麼？
        // 這是 Rust 的「包含結尾的範圍」語法
        // start..=end 表示從 start 到 end，兩端都包含
        //
        // .map(|(k, v)| (k.clone(), v.clone())) 是什麼？
        // range 回傳的是借用 (&key, &value)，我們需要擁有的版本
        // 所以對每一對都 clone 一份
        //
        // .collect() 是什麼？
        // 把 iterator（迭代器）收集成一個 Vec（向量/陣列）
        let result: Vec<(Vec<u8>, Vec<u8>)> = self
            .data
            .range(start.to_vec()..=end.to_vec())
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();
        Ok(result)
    }

    fn list_all(&self) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        // iter() 遍歷所有 key-value pairs，clone 後收集成 Vec
        let result: Vec<(Vec<u8>, Vec<u8>)> = self
            .data
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();
        Ok(result)
    }

    fn count(&self) -> usize {
        // len() 回傳 BTreeMap 裡有幾筆資料
        self.data.len()
    }
}

// =============================================================================
// 單元測試
// =============================================================================
//
// #[cfg(test)] 是什麼？
// 這個 attribute 告訴編譯器：底下的模組只在跑 cargo test 時才編譯。
// 正式 build 時不會包含這些測試程式碼。
#[cfg(test)]
mod tests {
    use super::*;  // 匯入上面所有的東西（MemTable, StorageEngine, etc.）

    /// 測試基本的 put 和 get
    #[test]
    fn test_put_and_get() {
        let mut table = MemTable::new();

        // 寫入一筆資料
        // b"name" 是什麼？它是 byte string literal，型別是 &[u8; 4]
        // .to_vec() 把它轉成 Vec<u8>，因為 put 需要 Vec<u8>（擁有所有權）
        table.put(b"name".to_vec(), b"Alice".to_vec()).unwrap();

        // 讀取並驗證
        // .unwrap() 是什麼？
        // 它把 Result 或 Option 裡的值取出來。如果是 Err 或 None，程式會 panic（崩潰）。
        // 在測試裡用 unwrap 很正常，因為失敗了就代表測試沒過。
        let value = table.get(b"name").unwrap();
        assert_eq!(value, Some(b"Alice".to_vec()));
    }

    /// 測試 get 不存在的 key
    #[test]
    fn test_get_nonexistent() {
        let table = MemTable::new();
        let value = table.get(b"nothing").unwrap();
        assert_eq!(value, None);
    }

    /// 測試 put 覆蓋已存在的 key
    #[test]
    fn test_put_overwrite() {
        let mut table = MemTable::new();
        table.put(b"name".to_vec(), b"Alice".to_vec()).unwrap();
        table.put(b"name".to_vec(), b"Bob".to_vec()).unwrap();

        let value = table.get(b"name").unwrap();
        assert_eq!(value, Some(b"Bob".to_vec()));
    }

    /// 測試 delete
    #[test]
    fn test_delete() {
        let mut table = MemTable::new();
        table.put(b"name".to_vec(), b"Alice".to_vec()).unwrap();
        table.delete(b"name").unwrap();

        let value = table.get(b"name").unwrap();
        assert_eq!(value, None);
    }

    /// 測試 delete 不存在的 key（應該不會報錯）
    #[test]
    fn test_delete_nonexistent() {
        let mut table = MemTable::new();
        // 不應該報錯
        table.delete(b"nothing").unwrap();
    }

    /// 測試 scan 範圍查詢
    #[test]
    fn test_scan() {
        let mut table = MemTable::new();
        table.put(b"a".to_vec(), b"1".to_vec()).unwrap();
        table.put(b"b".to_vec(), b"2".to_vec()).unwrap();
        table.put(b"c".to_vec(), b"3".to_vec()).unwrap();
        table.put(b"d".to_vec(), b"4".to_vec()).unwrap();

        // scan a 到 c，應該回傳 a, b, c（包含兩端）
        let result = table.scan(b"a", b"c").unwrap();
        assert_eq!(result.len(), 3);
        assert_eq!(result[0], (b"a".to_vec(), b"1".to_vec()));
        assert_eq!(result[1], (b"b".to_vec(), b"2".to_vec()));
        assert_eq!(result[2], (b"c".to_vec(), b"3".to_vec()));
    }

    /// 測試 list_all
    #[test]
    fn test_list_all() {
        let mut table = MemTable::new();
        table.put(b"b".to_vec(), b"2".to_vec()).unwrap();
        table.put(b"a".to_vec(), b"1".to_vec()).unwrap();

        let result = table.list_all().unwrap();
        // BTreeMap 會自動排序，所以 a 在前面
        assert_eq!(result.len(), 2);
        assert_eq!(result[0].0, b"a".to_vec());
        assert_eq!(result[1].0, b"b".to_vec());
    }

    /// 測試 count
    #[test]
    fn test_count() {
        let mut table = MemTable::new();
        assert_eq!(table.count(), 0);

        table.put(b"a".to_vec(), b"1".to_vec()).unwrap();
        assert_eq!(table.count(), 1);

        table.put(b"b".to_vec(), b"2".to_vec()).unwrap();
        assert_eq!(table.count(), 2);

        table.delete(b"a").unwrap();
        assert_eq!(table.count(), 1);
    }

    /// 測試 approximate_size
    #[test]
    fn test_approximate_size() {
        let mut table = MemTable::new();
        assert_eq!(table.approximate_size(), 0);

        // "name" = 4 bytes, "Alice" = 5 bytes, total = 9
        table.put(b"name".to_vec(), b"Alice".to_vec()).unwrap();
        assert_eq!(table.approximate_size(), 9);
    }
}

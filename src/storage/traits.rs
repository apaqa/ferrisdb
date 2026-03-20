// =============================================================================
// storage/traits.rs — 儲存引擎的介面定義
// =============================================================================
//
// 為什麼需要 trait？
// -----------------
// trait 是 Rust 的「介面」，類似其他語言的 interface。
// 我們定義 StorageEngine trait，規定「任何儲存引擎都必須能做 get/put/delete/scan」。
//
// 為什麼不直接寫一個 struct？
// 因為之後我們會有多種儲存引擎：
// - MemTable（記憶體版，現在要做的）
// - SSTable（磁碟版，Phase 2）
// - LSM-Tree（結合兩者，Phase 2-3）
//
// 用 trait 的好處是：上層程式碼（CLI、Server）只認 trait，不管底層是記憶體還是磁碟。
// 之後換引擎不用改上層程式碼。

use crate::error::Result;

/// StorageEngine — 所有儲存引擎都必須實作的介面
///
/// 什麼是 pub trait？
/// - pub：公開的，其他模組可以使用
/// - trait：定義一組方法簽名，任何 struct 都可以 impl（實作）這個 trait
///
/// 方法裡的 &self 和 &mut self 是什麼？
/// - &self：唯讀借用，只是看資料，不改資料（用在 get、scan）
/// - &mut self：可變借用，會修改資料（用在 put、delete）
/// 這是 Rust 的所有權系統，確保不會有兩個地方同時修改同一份資料
pub trait StorageEngine {
    /// 根據 key 取得 value
    ///
    /// 回傳 Result<Option<Vec<u8>>>，為什麼要三層包裝？
    /// - Result：操作可能失敗（例如之後磁碟 IO 錯誤）
    /// - Option：key 可能不存在，不存在時回傳 None
    /// - Vec<u8>：value 的內容，用 byte 陣列表示（之後可以存任何東西）
    ///
    /// &[u8] 是什麼？
    /// 它是 byte slice（位元組切片），是對一段記憶體的「唯讀借用」。
    /// 我們用 &[u8] 而不是 String，因為資料庫的 key 不一定是文字，可以是任何 bytes。
    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>>;

    /// 寫入一對 key-value
    ///
    /// 如果 key 已經存在，會覆蓋舊的 value。
    /// Vec<u8> 是「擁有所有權的 byte 陣列」，跟 &[u8]（借用）不同。
    /// 這裡用 Vec<u8> 是因為 MemTable 需要「擁有」這份資料，存進 BTreeMap 裡。
    fn put(&mut self, key: Vec<u8>, value: Vec<u8>) -> Result<()>;

    /// 刪除指定的 key
    ///
    /// 回傳 Result<()>，() 是空的 tuple，表示「成功但沒有回傳值」
    fn delete(&mut self, key: &[u8]) -> Result<()>;

    /// 範圍查詢：回傳 start 到 end 之間所有的 key-value pairs
    ///
    /// 為什麼需要 scan？
    /// 資料庫不只需要「查一筆」，還常常需要「查一個範圍」。
    /// 例如：找出所有 user:001 到 user:100 之間的使用者。
    /// 這也是為什麼我們用 BTreeMap（有序）而不是 HashMap（無序）。
    ///
    /// 回傳 Vec<(Vec<u8>, Vec<u8>)>：一個 vector，裡面每個元素是 (key, value) tuple
    fn scan(&self, start: &[u8], end: &[u8]) -> Result<Vec<(Vec<u8>, Vec<u8>)>>;

    /// 列出所有 key-value pairs
    ///
    /// 這是方便除錯用的，之後正式版可能會拿掉或加上分頁
    fn list_all(&self) -> Result<Vec<(Vec<u8>, Vec<u8>)>>;

    /// 回傳目前儲存了多少筆資料
    fn count(&self) -> usize;
}

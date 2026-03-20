// =============================================================================
// storage/traits.rs — 儲存引擎抽象介面
// =============================================================================
//
// StorageEngine 是 ferrisdb 所有儲存實作共用的操作介面。
// 不管底層是純記憶體、LSM-Tree、還是未來的其他引擎，
// 上層 REPL / TCP server 都透過這個 trait 來讀寫資料。

use crate::error::{FerrisDbError, Result};

pub trait StorageEngine {
    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>>;
    fn put(&mut self, key: Vec<u8>, value: Vec<u8>) -> Result<()>;
    fn delete(&mut self, key: &[u8]) -> Result<()>;
    fn scan(&self, start: &[u8], end: &[u8]) -> Result<Vec<(Vec<u8>, Vec<u8>)>>;
    fn list_all(&self) -> Result<Vec<(Vec<u8>, Vec<u8>)>>;
    fn count(&self) -> usize;

    /// 手動觸發 compaction。
    ///
    /// 不是所有引擎都支援 compaction，因此 trait 提供預設實作：
    /// 若底層沒有覆寫，就回傳「不支援」。
    fn compact(&mut self) -> Result<()> {
        Err(FerrisDbError::InvalidCommand(
            "compact is not supported by this storage engine".to_string(),
        ))
    }
}

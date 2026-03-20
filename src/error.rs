// =============================================================================
// error.rs — 統一錯誤型別
// =============================================================================
//
// 為什麼需要這個檔案？
// --------------------
// 在 Rust 裡，每個函式如果可能失敗，就要回傳 Result<成功型別, 錯誤型別>。
// 我們定義一個統一的 FerrisDbError，這樣整個專案的錯誤處理方式一致，
// 之後不管是記憶體操作、磁碟 IO、還是 SQL 解析，都用同一套錯誤系統。
//
// 什麼是 thiserror？
// ------------------
// thiserror 是一個 derive macro 函式庫，讓你用 #[derive(Error)] 就能自動
// 幫你實作 Display 和 Error trait，不用自己手寫一大堆 boilerplate。

use thiserror::Error;

/// FerrisDbError — ferrisdb 的統一錯誤型別
///
/// #[derive(Debug, Error)] 是什麼意思？
/// - derive(Debug)：讓這個 enum 可以用 {:?} 格式印出來，方便除錯
/// - derive(Error)：來自 thiserror，自動幫你實作 std::error::Error trait
///
/// 什麼是 enum？
/// enum 是 Rust 的「列舉」型別，表示一個值可以是其中任何一個變體。
/// 這裡 FerrisDbError 可以是 KeyNotFound、InvalidCommand 等等。
#[derive(Debug, Error)]
pub enum FerrisDbError {
    /// 查詢的 key 不存在
    /// #[error("...")] 定義了這個錯誤印出來時顯示的訊息
    /// {0} 代表這個變體裡的第一個欄位（就是那個 String）
    #[error("Key not found: {0}")]
    KeyNotFound(String),

    /// 使用者輸入了無法辨識的指令
    #[error("Invalid command: {0}")]
    InvalidCommand(String),

    /// IO 錯誤（之後讀寫檔案時會用到）
    /// #[from] 表示可以自動從 std::io::Error 轉換成 FerrisDbError::Io
    /// 這樣你在程式裡用 ? 運算子時，io 錯誤會自動變成我們的錯誤型別
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),
}

/// 自定義 Result 型別
/// 這是一個 type alias（型別別名），讓我們不用每次都寫 Result<T, FerrisDbError>
/// 只要寫 Result<T> 就好，更簡潔
pub type Result<T> = std::result::Result<T, FerrisDbError>;

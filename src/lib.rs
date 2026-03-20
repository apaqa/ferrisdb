// =============================================================================
// lib.rs — ferrisdb 對外公開 API
// =============================================================================
//
// 這個檔案負責宣告 crate 對外可用的模組。
// main.rs 主要是執行入口；lib.rs 讓其他程式可以 `use ferrisdb::...`。

/// 錯誤型別模組
pub mod error;

/// 儲存引擎模組（目前是 in-memory 的 MemTable）
pub mod storage;

/// CLI REPL 模組
pub mod cli;

/// TCP server 模組
pub mod server;

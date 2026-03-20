// =============================================================================
// lib.rs — ferrisdb 對外公開 API
// =============================================================================
//
// 這個檔案負責宣告 crate 對外可見的模組。
// 目前包含：
// - error：錯誤型別
// - storage：MemTable / WAL / SSTable / LSM-Tree
// - transaction：MVCC transaction 層
// - cli：REPL
// - server：TCP server

pub mod error;
pub mod config;
pub mod bench;
pub mod storage;
pub mod transaction;
pub mod sql;
pub mod cli;
pub mod server;

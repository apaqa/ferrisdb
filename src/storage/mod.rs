// =============================================================================
// storage/mod.rs — storage 模組的入口
// =============================================================================
//
// 什麼是 mod.rs？
// ---------------
// 在 Rust 裡，一個資料夾要變成一個模組，必須有一個 mod.rs 檔案。
// 這個檔案的作用是「告訴 Rust 這個資料夾裡有哪些子模組」。
//
// pub mod 是什麼？
// pub mod traits; 表示「把 traits.rs 這個檔案當成一個公開的子模組」。
// 外面的程式碼就可以用 use ferrisdb::storage::traits::StorageEngine 來使用它。

// =============================================================================
// storage/mod.rs — 儲存層模組入口
// =============================================================================
//
// ferrisdb 的儲存層目前包含：
// 1. memory：in-memory 的 MemTable
// 2. sstable：落盤的 Sorted String Table（LSM Tree 核心元件）
// 3. traits：抽象介面 StorageEngine

pub mod traits;
pub mod memory;
pub mod sstable;

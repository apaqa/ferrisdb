// =============================================================================
// transaction/mod.rs — transaction 模組入口
// =============================================================================
//
// 這個資料夾實作 MVCC（Multi-Version Concurrency Control）。
// 它建立在底層 LsmEngine 之上，透過版本化 key 達成 snapshot isolation。

pub mod keyutil;
pub mod mvcc;

// =============================================================================
// storage/mod.rs — 儲存層模組入口
// =============================================================================
//
// ferrisdb 目前的儲存元件：
// - traits：抽象介面 StorageEngine
// - memory：in-memory 的 MemTable
// - wal：Write-Ahead Log，提供 crash recovery
// - sstable：不可變的排序磁碟檔案
// - lsm：把 MemTable / WAL / SSTable 串起來的 LSM-Tree 引擎

pub mod traits;
pub mod memory;
pub mod bloom;
pub mod wal;
pub mod sstable;
pub mod compaction;
pub mod lsm;

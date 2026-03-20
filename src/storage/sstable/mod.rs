// =============================================================================
// storage/sstable/mod.rs — SSTable 模組入口
// =============================================================================
//
// SSTable（Sorted String Table）是 LSM-Tree 的核心元件之一：
// - 「Sorted」代表 key 依序排列，便於二分搜尋與 range query。
// - 「String Table」這裡泛指 key/value 的位元組資料表。
// - 一旦寫完通常是 immutable（不可變），讀取效率高，適合批次 flush。
//
// 這個資料夾包含：
// - format.rs：SSTable 檔案格式常數與編解碼輔助
// - writer.rs：把排序好的 key-value 寫成 sstable 檔案
// - reader.rs：從 sstable 讀資料，並用 index 做二分搜尋

pub mod format;
pub mod reader;
pub mod writer;

pub use reader::{SSTableIterator, SSTableReader};
pub use writer::SSTableWriter;

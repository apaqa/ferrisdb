// =============================================================================
// storage/lsm.rs — LSM-Tree 第一版儲存引擎
// =============================================================================
//
// LSM-Tree 的核心想法：
// 1. 寫入先進記憶體（MemTable），寫很快。
// 2. 記憶體累積到一定大小後，批次 flush 成磁碟上的 SSTable（不可變、排序好）。
// 3. 讀取時先查最新資料（MemTable），再查較舊的 SSTable。
//
// 這樣可以把大量隨機寫入轉成順序落盤，對儲存系統更友善。
//
// Tombstone（墓碑）：
// - delete 不直接刪舊檔，而是寫一筆特殊值代表「這個 key 已刪除」。
// - 讀取時看到 tombstone 要回傳 None，並遮蔽更舊層的資料。

use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};

use crate::error::Result;
use crate::storage::memory::MemTable;
use crate::storage::sstable::{SSTableReader, SSTableWriter};
use crate::storage::traits::StorageEngine;

/// Tombstone 的內容（簡化版先用固定字串）。
pub const TOMBSTONE: &[u8] = b"__TOMBSTONE__";

/// 預設 MemTable flush 閾值（4KB）。
pub const DEFAULT_MEMTABLE_SIZE_THRESHOLD: usize = 4096;

#[derive(Debug)]
pub struct LsmEngine {
    /// 目前正在接受寫入的 active memtable。
    pub active_memtable: MemTable,
    /// 已落盤的 sstable，順序為「新 -> 舊」。
    pub sstables: Vec<SSTableReader>,
    /// sstable 檔案存放目錄。
    pub data_dir: PathBuf,
    /// active memtable 超過此大小就觸發 flush。
    pub memtable_size_threshold: usize,
    /// 下一個 sstable 檔案 id（遞增）。
    pub next_sstable_id: u64,
}

impl LsmEngine {
    /// 開啟（或建立）LSM 儲存引擎。
    ///
    /// - `data_dir`：sstable 存放路徑
    /// - `threshold`：flush 閾值；若傳 0 則使用預設 4096
    pub fn open(data_dir: impl AsRef<Path>, threshold: usize) -> Result<Self> {
        let data_dir = data_dir.as_ref().to_path_buf();
        fs::create_dir_all(&data_dir)?;

        let mut files: Vec<(u64, PathBuf)> = Vec::new();
        for entry in fs::read_dir(&data_dir)? {
            let entry = entry?;
            let path = entry.path();

            if path.extension().and_then(|e| e.to_str()) != Some("sst") {
                continue;
            }

            let Some(stem) = path.file_stem().and_then(|s| s.to_str()) else {
                continue;
            };
            let Ok(id) = stem.parse::<u64>() else {
                continue;
            };
            files.push((id, path));
        }

        // 先按 id 由小到大排序，再組成「新 -> 舊」的 reader 向量。
        files.sort_by_key(|(id, _)| *id);
        let mut sstables = Vec::with_capacity(files.len());
        for (_, path) in files.iter().rev() {
            sstables.push(SSTableReader::open(path)?);
        }

        let next_sstable_id = files.last().map(|(id, _)| id + 1).unwrap_or(1);
        let memtable_size_threshold = if threshold == 0 {
            DEFAULT_MEMTABLE_SIZE_THRESHOLD
        } else {
            threshold
        };

        Ok(Self {
            active_memtable: MemTable::new(),
            sstables,
            data_dir,
            memtable_size_threshold,
            next_sstable_id,
        })
    }

    /// 判斷 value 是否 tombstone。
    fn is_tombstone(value: &[u8]) -> bool {
        value == TOMBSTONE
    }

    /// 判斷 key 是否在 scan 範圍內（inclusive）。
    fn in_range(key: &[u8], start: &[u8], end: &[u8]) -> bool {
        key >= start && key <= end
    }

    /// 需要時觸發 flush。
    fn maybe_flush(&mut self) -> Result<()> {
        if self.active_memtable.approximate_size() > self.memtable_size_threshold {
            self.flush_active_memtable()?;
        }
        Ok(())
    }

    /// 把 active memtable 落盤成一個新的 sstable 檔案。
    ///
    /// 流程：
    /// 1. 以遞增編號建立檔名（例如 000001.sst）
    /// 2. 將 memtable 所有資料依序寫入 writer
    /// 3. finish 後 reopen 成 reader
    /// 4. 放到 sstables 最前面（最新）
    /// 5. 清空 active memtable
    /// 6. next_sstable_id += 1
    fn flush_active_memtable(&mut self) -> Result<()> {
        if self.active_memtable.count() == 0 {
            return Ok(());
        }

        let filename = format!("{:06}.sst", self.next_sstable_id);
        let path = self.data_dir.join(filename);

        let mut writer = SSTableWriter::new(&path)?;
        for (key, value) in self.active_memtable.list_all()? {
            writer.write_entry(&key, &value)?;
        }
        writer.finish()?;

        let reader = SSTableReader::open(&path)?;
        self.sstables.insert(0, reader);

        self.active_memtable = MemTable::new();
        self.next_sstable_id += 1;
        Ok(())
    }

    /// 合併資料來源（old -> new），最後得到最新視圖。
    ///
    /// 為什麼 old -> new？
    /// - 舊資料先放進 map，新資料再覆蓋，才能自然得到「新覆蓋舊」。
    fn merged_view_with_range(
        &self,
        range: Option<(&[u8], &[u8])>,
    ) -> Result<BTreeMap<Vec<u8>, Vec<u8>>> {
        let mut merged = BTreeMap::<Vec<u8>, Vec<u8>>::new();

        // 先套用最舊到最新的 sstable。
        for reader in self.sstables.iter().rev() {
            let iter = reader.iter()?;
            for entry in iter {
                let (key, value) = entry?;
                if let Some((start, end)) = range {
                    if !Self::in_range(&key, start, end) {
                        continue;
                    }
                }

                if Self::is_tombstone(&value) {
                    merged.remove(&key);
                } else {
                    merged.insert(key, value);
                }
            }
        }

        // 最後套用最新的 active memtable（優先權最高）。
        let mem_entries = match range {
            Some((start, end)) => self.active_memtable.scan(start, end)?,
            None => self.active_memtable.list_all()?,
        };
        for (key, value) in mem_entries {
            if Self::is_tombstone(&value) {
                merged.remove(&key);
            } else {
                merged.insert(key, value);
            }
        }

        Ok(merged)
    }
}

impl StorageEngine for LsmEngine {
    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        // 1) 先查 active memtable（最新）
        if let Some(value) = self.active_memtable.get(key)? {
            if Self::is_tombstone(&value) {
                return Ok(None);
            }
            return Ok(Some(value));
        }

        // 2) 再由新到舊查 sstable
        for reader in &self.sstables {
            if let Some(value) = reader.get(key)? {
                if Self::is_tombstone(&value) {
                    return Ok(None);
                }
                return Ok(Some(value));
            }
        }

        Ok(None)
    }

    fn put(&mut self, key: Vec<u8>, value: Vec<u8>) -> Result<()> {
        self.active_memtable.put(key, value)?;
        self.maybe_flush()?;
        Ok(())
    }

    fn delete(&mut self, key: &[u8]) -> Result<()> {
        // delete 轉成寫入 tombstone。
        self.put(key.to_vec(), TOMBSTONE.to_vec())
    }

    fn scan(&self, start: &[u8], end: &[u8]) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        let merged = self.merged_view_with_range(Some((start, end)))?;
        Ok(merged.into_iter().collect())
    }

    fn list_all(&self) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        let merged = self.merged_view_with_range(None)?;
        Ok(merged.into_iter().collect())
    }

    fn count(&self) -> usize {
        // trait 介面固定回傳 usize，無法傳遞錯誤；這裡退化為 0。
        self.list_all().map(|v| v.len()).unwrap_or(0)
    }
}

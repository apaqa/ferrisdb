// =============================================================================
// storage/lsm.rs — LSM-Tree 儲存引擎
// =============================================================================
//
// LSM-Tree 的讀寫流程：
// 1. 寫入時：
//    - 先 append 到 WAL，確保崩潰時有恢復依據
//    - 再寫入 active MemTable
//    - MemTable 超過閾值後 flush 成 SSTable
//
// 2. 讀取時：
//    - 先查 active MemTable（最新）
//    - 再由新到舊查所有 SSTable
//
// 3. 崩潰恢復：
//    - 如果程式在 flush 前崩潰，未落盤資料仍保留在 wal.log
//    - 下次 open 時先重放 WAL，重建出 active MemTable
//
// 4. 刪除時：
//    - 不直接把舊資料抹掉，而是寫入 tombstone
//    - tombstone 會遮蔽更舊層的同名 key

use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};

use crate::error::Result;
use crate::storage::memory::MemTable;
use crate::storage::sstable::{SSTableReader, SSTableWriter};
use crate::storage::traits::StorageEngine;
use crate::storage::wal::{WalReader, WalWriter};

pub const TOMBSTONE: &[u8] = b"__TOMBSTONE__";
pub const DEFAULT_MEMTABLE_SIZE_THRESHOLD: usize = 4096;
const WAL_FILENAME: &str = "wal.log";

#[derive(Debug)]
pub struct LsmEngine {
    pub active_memtable: MemTable,
    pub active_wal: Option<WalWriter>,
    pub sstables: Vec<SSTableReader>,
    pub data_dir: PathBuf,
    pub memtable_size_threshold: usize,
    pub next_sstable_id: u64,
}

impl LsmEngine {
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

        // 若 wal.log 殘留，代表上次可能在 flush 前崩潰，需先重放。
        let wal_path = data_dir.join(WAL_FILENAME);
        let active_memtable = if wal_path.exists() && wal_path.metadata()?.len() > 0 {
            WalReader::open(&wal_path)?.recover_to_memtable()?
        } else {
            MemTable::new()
        };

        let active_wal = Some(WalWriter::new(&wal_path)?);

        Ok(Self {
            active_memtable,
            active_wal,
            sstables,
            data_dir,
            memtable_size_threshold,
            next_sstable_id,
        })
    }

    pub fn shutdown(&mut self) -> Result<()> {
        if self.active_memtable.count() > 0 {
            self.flush_active_memtable(false)?;
        }

        self.active_wal = None;
        Ok(())
    }

    pub fn wal_path(&self) -> PathBuf {
        self.data_dir.join(WAL_FILENAME)
    }

    fn is_tombstone(value: &[u8]) -> bool {
        value == TOMBSTONE
    }

    fn in_range(key: &[u8], start: &[u8], end: &[u8]) -> bool {
        key >= start && key <= end
    }

    fn maybe_flush(&mut self) -> Result<()> {
        if self.active_memtable.approximate_size() > self.memtable_size_threshold {
            self.flush_active_memtable(true)?;
        }
        Ok(())
    }

    /// 把 active memtable 落盤成一個新的 sstable。
    ///
    /// `recreate_wal = true`：
    /// - 表示 flush 完後刪除舊 WAL，並建立新的空 WAL 繼續接收寫入
    ///
    /// `recreate_wal = false`：
    /// - 表示用於 shutdown，flush 後直接關閉 WAL
    fn flush_active_memtable(&mut self, recreate_wal: bool) -> Result<()> {
        if self.active_memtable.count() == 0 {
            if !recreate_wal {
                self.active_wal = None;
            }
            return Ok(());
        }

        let filename = format!("{:06}.sst", self.next_sstable_id);
        let sstable_path = self.data_dir.join(filename);

        let mut writer = SSTableWriter::new(&sstable_path)?;
        for (key, value) in self.active_memtable.list_all()? {
            writer.write_entry(&key, &value)?;
        }
        writer.finish()?;

        let reader = SSTableReader::open(&sstable_path)?;
        self.sstables.insert(0, reader);

        // 到這一步代表新 sstable 已成功寫好，舊 WAL 可安全刪除。
        let wal_path = self.wal_path();
        let old_wal = self.active_wal.take();
        drop(old_wal);
        if wal_path.exists() {
            fs::remove_file(&wal_path)?;
        }

        self.active_memtable = MemTable::new();
        self.next_sstable_id += 1;

        if recreate_wal {
            self.active_wal = Some(WalWriter::new(&wal_path)?);
        }

        Ok(())
    }

    fn merged_view_with_range(
        &self,
        range: Option<(&[u8], &[u8])>,
    ) -> Result<BTreeMap<Vec<u8>, Vec<u8>>> {
        let mut merged = BTreeMap::<Vec<u8>, Vec<u8>>::new();

        // 先套用最舊到最新的 sstable，再套用 active memtable，
        // 讓較新的版本自然覆蓋較舊版本。
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
        if let Some(value) = self.active_memtable.get(key)? {
            if Self::is_tombstone(&value) {
                return Ok(None);
            }
            return Ok(Some(value));
        }

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
        if let Some(wal) = self.active_wal.as_mut() {
            wal.append_put(&key, &value)?;
        }

        self.active_memtable.put(key, value)?;
        self.maybe_flush()?;
        Ok(())
    }

    fn delete(&mut self, key: &[u8]) -> Result<()> {
        if let Some(wal) = self.active_wal.as_mut() {
            wal.append_delete(key)?;
        }

        self.active_memtable.put(key.to_vec(), TOMBSTONE.to_vec())?;
        self.maybe_flush()?;
        Ok(())
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
        self.list_all().map(|v| v.len()).unwrap_or(0)
    }
}

impl Drop for LsmEngine {
    fn drop(&mut self) {
        let _ = self.shutdown();
    }
}

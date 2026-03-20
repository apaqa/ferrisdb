// =============================================================================
// storage/lsm.rs — LSM-Tree 儲存引擎
// =============================================================================
//
// LSM-Tree 的完整讀寫流程：
// 1. put/delete：
//    - 先寫 WAL，確保崩潰時可恢復
//    - 再更新 active MemTable
//    - 若 MemTable 太大，flush 成 SSTable
//
// 2. get：
//    - 先查 active MemTable
//    - 若沒有，再由新到舊查 SSTable
//    - 其中 SSTable 可先用 Bloom Filter 快速判斷「一定不存在」
//
// 3. compaction：
//    - 當 SSTable 太多時，合併成較少的檔案
//    - 把舊版本與 tombstone 清理掉
//
// 4. crash recovery：
//    - open() 時若發現 wal.log 殘留，就重放回 active MemTable

use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};

use crate::error::Result;
use crate::storage::compaction;
use crate::storage::memory::MemTable;
use crate::storage::sstable::{SSTableReader, SSTableWriter};
use crate::storage::traits::StorageEngine;
use crate::storage::wal::{WalReader, WalWriter};

pub const TOMBSTONE: &[u8] = b"__TOMBSTONE__";
pub const DEFAULT_MEMTABLE_SIZE_THRESHOLD: usize = 4096;
const WAL_FILENAME: &str = "wal.log";
const AUTO_COMPACTION_SSTABLE_LIMIT: usize = 4;

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

    pub fn compact(&mut self) -> Result<()> {
        // 手動 compact 前，先把記憶體資料 flush，確保新資料也被納入合併。
        if self.active_memtable.count() > 0 {
            self.flush_active_memtable(true)?;
        }

        if self.sstables.len() <= 1 {
            return Ok(());
        }

        let source_paths: Vec<PathBuf> = self
            .sstables
            .iter()
            .map(|reader| reader.path().to_path_buf())
            .collect();

        let output_path = self
            .data_dir
            .join(format!("{:06}.sst", self.next_sstable_id));
        compaction::compact(&source_paths, &output_path)?;
        let new_reader = SSTableReader::open(&output_path)?;

        let old_readers = std::mem::take(&mut self.sstables);
        drop(old_readers);
        for path in &source_paths {
            if path.exists() {
                fs::remove_file(path)?;
            }
        }

        self.sstables = vec![new_reader];
        self.next_sstable_id += 1;
        Ok(())
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
            self.maybe_auto_compact()?;
        }
        Ok(())
    }

    fn maybe_auto_compact(&mut self) -> Result<()> {
        if self.sstables.len() > AUTO_COMPACTION_SSTABLE_LIMIT {
            self.compact()?;
        }
        Ok(())
    }

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
        self.merged_view_with_range_internal(range, false)
    }

    pub fn raw_scan(&self, start: &[u8], end: &[u8]) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        let merged = self.merged_view_with_range_internal(Some((start, end)), true)?;
        Ok(merged.into_iter().collect())
    }

    pub fn raw_list_all(&self) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        let merged = self.merged_view_with_range_internal(None, true)?;
        Ok(merged.into_iter().collect())
    }

    fn merged_view_with_range_internal(
        &self,
        range: Option<(&[u8], &[u8])>,
        include_tombstones: bool,
    ) -> Result<BTreeMap<Vec<u8>, Vec<u8>>> {
        let mut merged = BTreeMap::<Vec<u8>, Vec<u8>>::new();

        for reader in self.sstables.iter().rev() {
            let iter = reader.iter()?;
            for entry in iter {
                let (key, value) = entry?;
                if let Some((start, end)) = range {
                    if !Self::in_range(&key, start, end) {
                        continue;
                    }
                }

                if Self::is_tombstone(&value) && !include_tombstones {
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
            if Self::is_tombstone(&value) && !include_tombstones {
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

    fn compact(&mut self) -> Result<()> {
        LsmEngine::compact(self)
    }
}

impl Drop for LsmEngine {
    fn drop(&mut self) {
        let _ = self.shutdown();
    }
}

// =============================================================================
// storage/lsm.rs -- LSM-Tree Storage Engine
// =============================================================================
//
// LSM-Tree 的核心讀寫流程：
//
// 1. 寫入路徑
//    - 先寫 WAL，保證 crash recovery
//    - 再寫 active MemTable
//    - MemTable 超過閾值後 flush 成新的 SSTable
//    - flush / compaction 的 metadata 另外寫入 MANIFEST
//
// 2. 讀取路徑
//    - 先查 active MemTable
//    - 再從最新到最舊依序查 SSTable
//    - 每個 SSTable 內部會先用 Bloom Filter 快速排除不存在的 key
//
// 3. 持久化狀態恢復
//    - WAL 負責恢復尚未 flush 的 MemTable
//    - MANIFEST 負責恢復「哪些 SSTable 目前有效」
//
// 為什麼需要 MANIFEST：
// - 如果只掃描資料夾裡的 `.sst`，遇到 compaction 或 crash 中斷時，
//   很難分辨哪些檔案是舊版本、哪些是目前有效集合。
// - MANIFEST 讓 restart 時有一份權威 metadata 可重放。

use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};

use serde::Serialize;

use crate::error::Result;
use crate::storage::compaction;
use crate::storage::manifest::{Manifest, ManifestRecord, ManifestState, MANIFEST_FILENAME};
use crate::storage::memory::MemTable;
use crate::storage::sstable::{SSTableReader, SSTableWriter};
use crate::storage::traits::StorageEngine;
use crate::storage::wal::{WalReader, WalWriter};

pub const TOMBSTONE: &[u8] = b"__TOMBSTONE__";
pub const DEFAULT_MEMTABLE_SIZE_THRESHOLD: usize = 4096;
const WAL_FILENAME: &str = "wal.log";
const AUTO_COMPACTION_SSTABLE_LIMIT: usize = 4;

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct SstableInfo {
    pub filename: String,
    pub size_bytes: u64,
    pub key_count: usize,
}

#[derive(Debug, Clone, PartialEq)]
pub struct EngineStats {
    pub entries: usize,
    pub sstable_count: usize,
    pub disk_usage_bytes: u64,
    pub bloom_filter_hit_rate: f64,
}

#[derive(Debug)]
pub struct LsmEngine {
    pub active_memtable: MemTable,
    pub active_wal: Option<WalWriter>,
    pub sstables: Vec<SSTableReader>,
    pub manifest: Manifest,
    pub data_dir: PathBuf,
    pub memtable_size_threshold: usize,
    pub compaction_threshold: usize,
    pub wal_sync_on_write: bool,
    pub next_sstable_id: u64,
}

impl LsmEngine {
    pub fn open(data_dir: impl AsRef<Path>, threshold: usize) -> Result<Self> {
        Self::open_with_options(
            data_dir,
            threshold,
            AUTO_COMPACTION_SSTABLE_LIMIT,
            true,
        )
    }

    pub fn open_with_options(
        data_dir: impl AsRef<Path>,
        threshold: usize,
        compaction_threshold: usize,
        wal_sync_on_write: bool,
    ) -> Result<Self> {
        let data_dir = data_dir.as_ref().to_path_buf();
        fs::create_dir_all(&data_dir)?;
        cleanup_temporary_files(&data_dir)?;

        let manifest_path = data_dir.join(MANIFEST_FILENAME);
        let mut manifest = if manifest_path.exists() {
            match Manifest::open(&manifest_path) {
                Ok(manifest) => manifest,
                Err(_) => {
                    let state = scan_sstable_state(&data_dir)?;
                    let mut manifest = Manifest::create(&manifest_path)?;
                    manifest.set_state(state);
                    manifest.snapshot()?;
                    manifest
                }
            }
        } else {
            let state = scan_sstable_state(&data_dir)?;
            let mut manifest = Manifest::create(&manifest_path)?;
            manifest.set_state(state);
            manifest.snapshot()?;
            manifest
        };

        let mut sstables = Vec::with_capacity(manifest.current_sstables().len());
        for filename in manifest.current_sstables() {
            let path = data_dir.join(filename);
            sstables.push(SSTableReader::open(path)?);
        }

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
        let active_wal = Some(WalWriter::new_with_options(&wal_path, wal_sync_on_write)?);

        manifest.set_state(manifest.state());
        let next_sstable_id = manifest.next_sstable_id;

        Ok(Self {
            active_memtable,
            active_wal,
            sstables,
            manifest,
            data_dir,
            memtable_size_threshold,
            compaction_threshold: compaction_threshold.max(1),
            wal_sync_on_write,
            next_sstable_id,
        })
    }

    pub fn shutdown(&mut self) -> Result<()> {
        if self.active_memtable.count() > 0 {
            self.flush_active_memtable(false)?;
        }
        self.manifest.snapshot()?;
        self.next_sstable_id = self.manifest.next_sstable_id;
        self.active_wal = None;
        Ok(())
    }

    pub fn wal_path(&self) -> PathBuf {
        self.data_dir.join(WAL_FILENAME)
    }

    pub fn manifest_state(&self) -> ManifestState {
        self.manifest.state()
    }

    pub fn flush(&mut self) -> Result<()> {
        self.flush_active_memtable(true)
    }

    pub fn compact(&mut self) -> Result<()> {
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
        let removed: Vec<String> = source_paths
            .iter()
            .filter_map(|path| path.file_name().and_then(|name| name.to_str()).map(|s| s.to_string()))
            .collect();

        let next_id = self.manifest.next_id();
        self.next_sstable_id = self.manifest.next_sstable_id;
        let output_filename = format!("{:06}.sst", next_id);
        let output_path = self.data_dir.join(&output_filename);

        compaction::compact(&source_paths, &output_path)?;
        self.manifest.append_record(ManifestRecord::Compaction {
            added: vec![output_filename.clone()],
            removed,
        })?;

        let new_reader = SSTableReader::open(&output_path)?;
        let old_readers = std::mem::take(&mut self.sstables);
        drop(old_readers);
        for path in &source_paths {
            if path.exists() {
                fs::remove_file(path)?;
            }
        }

        self.sstables = vec![new_reader];
        self.next_sstable_id = self.manifest.next_sstable_id;
        Ok(())
    }

    pub fn sstable_infos(&self) -> Result<Vec<SstableInfo>> {
        self.sstables
            .iter()
            .map(|reader| {
                Ok(SstableInfo {
                    filename: reader
                        .path()
                        .file_name()
                        .and_then(|name| name.to_str())
                        .unwrap_or("<invalid>")
                        .to_string(),
                    size_bytes: reader.file_size()?,
                    key_count: reader.entry_count(),
                })
            })
            .collect()
    }

    pub fn wal_info(&self) -> Result<(u64, usize)> {
        let path = self.wal_path();
        let size = if path.exists() { fs::metadata(&path)?.len() } else { 0 };
        let count = if path.exists() && size > 0 {
            WalReader::open(&path)?.record_count()?
        } else {
            0
        };
        Ok((size, count))
    }

    pub fn disk_usage_bytes(&self) -> Result<u64> {
        let sstable_bytes: u64 = self
            .sstable_infos()?
            .into_iter()
            .map(|info| info.size_bytes)
            .sum();
        let wal_bytes = if self.wal_path().exists() {
            fs::metadata(self.wal_path())?.len()
        } else {
            0
        };
        let manifest_bytes = if self.manifest.path().exists() {
            fs::metadata(self.manifest.path())?.len()
        } else {
            0
        };
        Ok(sstable_bytes + wal_bytes + manifest_bytes)
    }

    pub fn bloom_filter_hit_rate(&self) -> f64 {
        if self.sstables.is_empty() {
            return 0.0;
        }
        let total: f64 = self
            .sstables
            .iter()
            .map(|reader| reader.bloom_filter_hit_rate())
            .sum();
        total / self.sstables.len() as f64
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
        if self.sstables.len() > self.compaction_threshold {
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

        let sstable_id = self.manifest.next_id();
        self.next_sstable_id = self.manifest.next_sstable_id;
        let filename = format!("{:06}.sst", sstable_id);
        let sstable_path = self.data_dir.join(&filename);

        let mut writer = SSTableWriter::new(&sstable_path)?;
        for (key, value) in self.active_memtable.list_all()? {
            writer.write_entry(&key, &value)?;
        }
        writer.finish()?;

        self.manifest
            .append_record(ManifestRecord::AddSstable { filename: filename.clone() })?;
        let reader = SSTableReader::open(&sstable_path)?;
        self.sstables.insert(0, reader);

        let wal_path = self.wal_path();
        let old_wal = self.active_wal.take();
        drop(old_wal);
        if wal_path.exists() {
            fs::remove_file(&wal_path)?;
        }

        self.active_memtable = MemTable::new();
        self.next_sstable_id = self.manifest.next_sstable_id;

        if recreate_wal {
            self.active_wal = Some(WalWriter::new_with_options(
                &wal_path,
                self.wal_sync_on_write,
            )?);
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

fn scan_sstable_state(data_dir: &Path) -> Result<ManifestState> {
    let mut files: Vec<(u64, String)> = Vec::new();
    for entry in fs::read_dir(data_dir)? {
        let entry = entry?;
        let path = entry.path();
        if path.extension().and_then(|ext| ext.to_str()) != Some("sst") {
            continue;
        }

        let Some(filename) = path.file_name().and_then(|name| name.to_str()) else {
            continue;
        };
        let Some(stem) = path.file_stem().and_then(|stem| stem.to_str()) else {
            continue;
        };
        let Ok(id) = stem.parse::<u64>() else {
            continue;
        };
        files.push((id, filename.to_string()));
    }

    files.sort_by_key(|(id, _)| *id);
    files.reverse();

    let next_sstable_id = files.first().map(|(id, _)| id + 1).unwrap_or(1);
    Ok(ManifestState {
        sstable_files: files.into_iter().map(|(_, filename)| filename).collect(),
        next_sstable_id,
        last_compaction_ts: 0,
    })
}

fn cleanup_temporary_files(data_dir: &Path) -> Result<()> {
    for entry in fs::read_dir(data_dir)? {
        let entry = entry?;
        let path = entry.path();
        let extension = path.extension().and_then(|ext| ext.to_str());
        if matches!(extension, Some("tmp") | Some("compacting")) {
            let _ = fs::remove_file(path);
        }
    }
    Ok(())
}

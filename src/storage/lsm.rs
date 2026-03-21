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
use std::sync::{Arc, Condvar, Mutex};
use std::thread::{self, JoinHandle};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use serde::Serialize;

use crate::config::WalMode;
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
    pub bloom_filter_hit_rate: u64,
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
    state: Arc<Mutex<LsmState>>,
    compaction_runtime: Arc<(Mutex<CompactionRuntime>, Condvar)>,
    background_worker: Mutex<Option<JoinHandle<()>>>,
}

#[derive(Debug)]
struct LsmState {
    active_memtable: MemTable,
    active_wal: Option<WalWriter>,
    sstables: Vec<SSTableReader>,
    manifest: Manifest,
    data_dir: PathBuf,
    memtable_size_threshold: usize,
    compaction_threshold: usize,
    wal_mode: WalMode,
    next_sstable_id: u64,
}

#[derive(Debug)]
struct CompactionRuntime {
    enabled: bool,
    stop_requested: bool,
    last_compaction_ts: u64,
    total_compactions: u64,
}

impl LsmEngine {
    pub fn open(data_dir: impl AsRef<Path>, threshold: usize) -> Result<Self> {
        Self::open_with_options(
            data_dir,
            threshold,
            AUTO_COMPACTION_SSTABLE_LIMIT,
            WalMode::Wal,
        )
    }

    pub fn open_with_options(
        data_dir: impl AsRef<Path>,
        threshold: usize,
        compaction_threshold: usize,
        wal_mode: WalMode,
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
        let active_memtable = if !matches!(wal_mode, WalMode::WalDisabled)
            && wal_path.exists()
            && wal_path.metadata()?.len() > 0
        {
            WalReader::open(&wal_path)?.recover_to_memtable()?
        } else {
            MemTable::new()
        };
        let active_wal = open_wal_writer(&wal_path, &wal_mode)?;

        manifest.set_state(manifest.state());
        let next_sstable_id = manifest.next_sstable_id;

        let state = Arc::new(Mutex::new(LsmState {
            active_memtable,
            active_wal,
            sstables,
            manifest,
            data_dir,
            memtable_size_threshold,
            compaction_threshold: compaction_threshold.max(1),
            wal_mode,
            next_sstable_id,
        }));
        let compaction_runtime = Arc::new((
            Mutex::new(CompactionRuntime {
                enabled: true,
                stop_requested: false,
                last_compaction_ts: 0,
                total_compactions: 0,
            }),
            Condvar::new(),
        ));
        let background_worker =
            spawn_background_compaction_worker(Arc::clone(&state), Arc::clone(&compaction_runtime));

        Ok(Self {
            state,
            compaction_runtime,
            background_worker: Mutex::new(Some(background_worker)),
        })
    }

    pub fn shutdown(&self) -> Result<()> {
        {
            let (runtime_lock, condvar) = &*self.compaction_runtime;
            let mut runtime = runtime_lock
                .lock()
                .expect("compaction runtime mutex poisoned");
            runtime.stop_requested = true;
            condvar.notify_all();
        }
        if let Some(worker) = self
            .background_worker
            .lock()
            .expect("background worker mutex poisoned")
            .take()
        {
            let _ = worker.join();
        }

        let mut state = self.state.lock().expect("lsm state mutex poisoned");
        if state.active_memtable.count() > 0 {
            Self::flush_active_memtable_locked(&mut state, false)?;
        }
        state.manifest.snapshot()?;
        state.next_sstable_id = state.manifest.next_sstable_id;
        state.active_wal = None;
        Ok(())
    }

    pub fn wal_path(&self) -> PathBuf {
        let state = self.state.lock().expect("lsm state mutex poisoned");
        state.data_dir.join(WAL_FILENAME)
    }

    pub fn manifest_state(&self) -> ManifestState {
        let state = self.state.lock().expect("lsm state mutex poisoned");
        state.manifest.state()
    }

    pub fn flush(&self) -> Result<()> {
        let mut state = self.state.lock().expect("lsm state mutex poisoned");
        Self::flush_active_memtable_locked(&mut state, true)
    }

    // 中文註解：提供 MVCC 層用 `&self` 寫入，避免外層還要再包一層 mutex。
    pub fn put_entry(&self, key: Vec<u8>, value: Vec<u8>) -> Result<()> {
        let mut state = self.state.lock().expect("lsm state mutex poisoned");
        if let Some(wal) = state.active_wal.as_mut() {
            wal.append_put(&key, &value)?;
        }
        state.active_memtable.put(key, value)?;
        self.maybe_flush(&mut state)
    }

    // 中文註解：提供 MVCC 層用 `&self` 刪除資料。
    pub fn delete_entry(&self, key: &[u8]) -> Result<()> {
        let mut state = self.state.lock().expect("lsm state mutex poisoned");
        if let Some(wal) = state.active_wal.as_mut() {
            wal.append_delete(key)?;
        }
        state
            .active_memtable
            .put(key.to_vec(), TOMBSTONE.to_vec())?;
        self.maybe_flush(&mut state)
    }

    pub fn compact(&self) -> Result<()> {
        let mut state = self.state.lock().expect("lsm state mutex poisoned");
        let compacted = Self::compact_locked(&mut state)?;
        if compacted {
            self.mark_compaction_finished();
        }
        Ok(())
    }

    pub fn sstable_infos(&self) -> Result<Vec<SstableInfo>> {
        let state = self.state.lock().expect("lsm state mutex poisoned");
        state
            .sstables
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
                    // 中文註解：為了避免 JSON 浮點比較在測試中太脆弱，這裡以萬分比整數輸出。
                    bloom_filter_hit_rate: (reader.bloom_filter_hit_rate() * 10_000.0) as u64,
                })
            })
            .collect()
    }

    pub fn wal_info(&self) -> Result<(u64, usize)> {
        let state = self.state.lock().expect("lsm state mutex poisoned");
        let path = state.data_dir.join(WAL_FILENAME);
        let size = if path.exists() {
            fs::metadata(&path)?.len()
        } else {
            0
        };
        let count = if path.exists() && size > 0 {
            WalReader::open(&path)?.record_count()?
        } else {
            0
        };
        Ok((size, count))
    }

    pub fn disk_usage_bytes(&self) -> Result<u64> {
        let state = self.state.lock().expect("lsm state mutex poisoned");
        let sstable_bytes: u64 = state
            .sstables
            .iter()
            .map(|reader| reader.file_size())
            .collect::<Result<Vec<_>>>()?
            .into_iter()
            .sum();
        let wal_path = state.data_dir.join(WAL_FILENAME);
        let wal_bytes = if wal_path.exists() {
            fs::metadata(&wal_path)?.len()
        } else {
            0
        };
        let manifest_bytes = if state.manifest.path().exists() {
            fs::metadata(state.manifest.path())?.len()
        } else {
            0
        };
        Ok(sstable_bytes + wal_bytes + manifest_bytes)
    }

    pub fn bloom_filter_hit_rate(&self) -> f64 {
        let state = self.state.lock().expect("lsm state mutex poisoned");
        if state.sstables.is_empty() {
            return 0.0;
        }
        let total: f64 = state
            .sstables
            .iter()
            .map(|reader| reader.bloom_filter_hit_rate())
            .sum();
        total / state.sstables.len() as f64
    }

    // 中文註解：提供 REPL `show compaction` 顯示背景 compact 狀態。
    pub fn compaction_status(&self) -> (bool, u64, u64) {
        let (runtime_lock, _) = &*self.compaction_runtime;
        let runtime = runtime_lock
            .lock()
            .expect("compaction runtime mutex poisoned");
        (
            runtime.enabled && !runtime.stop_requested,
            runtime.last_compaction_ts,
            runtime.total_compactions,
        )
    }

    // 中文註解：提供 REPL debug 使用的 memtable 快照，避免直接暴露內部鎖。
    pub fn active_memtable_entries(&self) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        let state = self.state.lock().expect("lsm state mutex poisoned");
        state.active_memtable.list_all()
    }

    // 中文註解：提供 REPL debug 使用的 SSTable 快照。
    pub fn sstable_debug_snapshots(&self) -> Result<Vec<(String, Vec<(Vec<u8>, Vec<u8>)>)>> {
        let state = self.state.lock().expect("lsm state mutex poisoned");
        state
            .sstables
            .iter()
            .map(|reader| {
                let filename = reader
                    .path()
                    .file_name()
                    .and_then(|file| file.to_str())
                    .unwrap_or("<invalid>")
                    .to_string();
                let entries = reader.iter()?.collect::<Result<Vec<_>>>()?;
                Ok((filename, entries))
            })
            .collect()
    }

    pub fn wal_mode(&self) -> WalMode {
        self.state
            .lock()
            .expect("lsm state mutex poisoned")
            .wal_mode
            .clone()
    }

    pub fn set_wal_mode(&self, wal_mode: WalMode) -> Result<()> {
        let mut state = self.state.lock().expect("lsm state mutex poisoned");
        state.wal_mode = wal_mode.clone();
        let wal_path = state.data_dir.join(WAL_FILENAME);
        let old_wal = state.active_wal.take();
        drop(old_wal);
        if matches!(wal_mode, WalMode::WalDisabled) {
            if wal_path.exists() {
                fs::remove_file(&wal_path)?;
            }
        } else {
            state.active_wal = open_wal_writer(&wal_path, &wal_mode)?;
        }
        Ok(())
    }

    fn mark_compaction_finished(&self) {
        let (runtime_lock, _) = &*self.compaction_runtime;
        let mut runtime = runtime_lock
            .lock()
            .expect("compaction runtime mutex poisoned");
        runtime.last_compaction_ts = now_unix_ts();
        runtime.total_compactions += 1;
    }

    fn compact_locked(state: &mut LsmState) -> Result<bool> {
        if state.active_memtable.count() > 0 {
            Self::flush_active_memtable_locked(state, true)?;
        }

        if state.sstables.len() <= 1 {
            return Ok(false);
        }

        let source_paths: Vec<PathBuf> = state
            .sstables
            .iter()
            .map(|reader| reader.path().to_path_buf())
            .collect();
        let removed: Vec<String> = source_paths
            .iter()
            .filter_map(|path| {
                path.file_name()
                    .and_then(|name| name.to_str())
                    .map(|s| s.to_string())
            })
            .collect();

        let next_id = state.manifest.next_id();
        state.next_sstable_id = state.manifest.next_sstable_id;
        let output_filename = format!("{:06}.sst", next_id);
        let output_path = state.data_dir.join(&output_filename);

        compaction::compact(&source_paths, &output_path)?;
        state.manifest.append_record(ManifestRecord::Compaction {
            added: vec![output_filename.clone()],
            removed,
        })?;

        let new_reader = SSTableReader::open(&output_path)?;
        let old_readers = std::mem::take(&mut state.sstables);
        drop(old_readers);
        for path in &source_paths {
            if path.exists() {
                fs::remove_file(path)?;
            }
        }

        state.sstables = vec![new_reader];
        state.next_sstable_id = state.manifest.next_sstable_id;
        Ok(true)
    }

    fn is_tombstone(value: &[u8]) -> bool {
        value == TOMBSTONE
    }

    fn in_range(key: &[u8], start: &[u8], end: &[u8]) -> bool {
        key >= start && key <= end
    }

    fn maybe_flush(&self, state: &mut LsmState) -> Result<()> {
        if state.active_memtable.approximate_size() > state.memtable_size_threshold {
            Self::flush_active_memtable_locked(state, true)?;
            let (_, condvar) = &*self.compaction_runtime;
            condvar.notify_all();
        }
        Ok(())
    }

    fn flush_active_memtable_locked(state: &mut LsmState, recreate_wal: bool) -> Result<()> {
        if state.active_memtable.count() == 0 {
            if !recreate_wal {
                state.active_wal = None;
            }
            return Ok(());
        }

        let sstable_id = state.manifest.next_id();
        state.next_sstable_id = state.manifest.next_sstable_id;
        let filename = format!("{:06}.sst", sstable_id);
        let sstable_path = state.data_dir.join(&filename);

        let mut writer = SSTableWriter::new(&sstable_path)?;
        for (key, value) in state.active_memtable.list_all()? {
            writer.write_entry(&key, &value)?;
        }
        writer.finish()?;

        state.manifest.append_record(ManifestRecord::AddSstable {
            filename: filename.clone(),
        })?;
        let reader = SSTableReader::open(&sstable_path)?;
        state.sstables.insert(0, reader);

        let wal_path = state.data_dir.join(WAL_FILENAME);
        let old_wal = state.active_wal.take();
        drop(old_wal);
        if wal_path.exists() {
            fs::remove_file(&wal_path)?;
        }

        state.active_memtable = MemTable::new();
        state.next_sstable_id = state.manifest.next_sstable_id;

        if recreate_wal {
            state.active_wal = open_wal_writer(&wal_path, &state.wal_mode)?;
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
        let state = self.state.lock().expect("lsm state mutex poisoned");
        let mut merged = BTreeMap::<Vec<u8>, Vec<u8>>::new();

        for reader in state.sstables.iter().rev() {
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
            Some((start, end)) => state.active_memtable.scan(start, end)?,
            None => state.active_memtable.list_all()?,
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
        let state = self.state.lock().expect("lsm state mutex poisoned");
        if let Some(value) = state.active_memtable.get(key)? {
            if Self::is_tombstone(&value) {
                return Ok(None);
            }
            return Ok(Some(value));
        }

        for reader in &state.sstables {
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
        self.put_entry(key, value)
    }

    fn delete(&mut self, key: &[u8]) -> Result<()> {
        self.delete_entry(key)
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

// 中文註解：背景 worker 每 5 秒醒來一次，超過 threshold 就自動 compact。
fn spawn_background_compaction_worker(
    state: Arc<Mutex<LsmState>>,
    runtime: Arc<(Mutex<CompactionRuntime>, Condvar)>,
) -> JoinHandle<()> {
    thread::spawn(move || loop {
        let should_stop = {
            let (runtime_lock, condvar) = &*runtime;
            let runtime_guard = runtime_lock
                .lock()
                .expect("compaction runtime mutex poisoned");
            let (runtime_guard, _) = condvar
                .wait_timeout(runtime_guard, Duration::from_secs(5))
                .expect("compaction condvar wait poisoned");
            runtime_guard.stop_requested
        };
        if should_stop {
            break;
        }

        let should_compact = {
            let state_guard = state.lock().expect("lsm state mutex poisoned");
            state_guard.sstables.len() > state_guard.compaction_threshold
        };
        if !should_compact {
            continue;
        }

        let compacted = {
            let mut state_guard = state.lock().expect("lsm state mutex poisoned");
            LsmEngine::compact_locked(&mut state_guard)
        };

        if matches!(compacted, Ok(true)) {
            let (runtime_lock, _) = &*runtime;
            let mut runtime_guard = runtime_lock
                .lock()
                .expect("compaction runtime mutex poisoned");
            runtime_guard.last_compaction_ts = now_unix_ts();
            runtime_guard.total_compactions += 1;
        }
    })
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

fn open_wal_writer(path: &Path, wal_mode: &WalMode) -> Result<Option<WalWriter>> {
    match wal_mode {
        WalMode::Wal => Ok(Some(WalWriter::new_with_options(path, false)?)),
        WalMode::WalDisabled => Ok(None),
        WalMode::Sync => Ok(Some(WalWriter::new_with_options(path, true)?)),
    }
}

fn now_unix_ts() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_secs())
        .unwrap_or(0)
}

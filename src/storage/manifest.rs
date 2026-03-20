// =============================================================================
// storage/manifest.rs -- MANIFEST Metadata Log
// =============================================================================
//
// MANIFEST 是 LSM-Tree 的 metadata log，用來記錄「目前有哪些 SSTable 有效」。
// 如果只靠掃描資料夾裡的 `.sst` 檔案來恢復狀態，會有幾個問題：
// - compact 過程中可能同時存在舊檔與新檔，光靠掃描無法判斷哪些才是有效集合
// - crash 發生在 rename / remove 中間時，磁碟檔案與邏輯狀態可能暫時不一致
// - 之後若要支援更複雜的 level / compaction metadata，也需要一份權威來源
//
// 因此這裡引入 MANIFEST：
// - 每次 flush / compaction 都先把 metadata 追加寫入 MANIFEST
// - 重啟時重放 MANIFEST records，恢復目前有效的 SSTable 列表
//
// 檔案格式（每筆 record）：
// - record_len: u32 LE
// - checksum: u32 LE (CRC32)
// - record_data: JSON 序列化的 ManifestRecord

use std::fs::{File, OpenOptions};
use std::io::{BufReader, Read, Write};
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use crc32fast::Hasher;
use serde::{Deserialize, Serialize};

use crate::error::{FerrisDbError, Result};

pub const MANIFEST_FILENAME: &str = "MANIFEST";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ManifestState {
    pub sstable_files: Vec<String>,
    pub next_sstable_id: u64,
    pub last_compaction_ts: u64,
}

#[derive(Debug)]
pub struct Manifest {
    pub sstable_files: Vec<String>,
    pub next_sstable_id: u64,
    pub last_compaction_ts: u64,
    path: PathBuf,
    file: File,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ManifestRecord {
    AddSstable { filename: String },
    RemoveSstable { filename: String },
    Compaction { added: Vec<String>, removed: Vec<String> },
    Snapshot {
        sstable_files: Vec<String>,
        next_sstable_id: u64,
    },
}

impl Manifest {
    pub fn create(path: &Path) -> Result<Manifest> {
        let path = path.to_path_buf();
        let file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .truncate(true)
            .open(&path)?;

        Ok(Manifest {
            sstable_files: Vec::new(),
            next_sstable_id: 1,
            last_compaction_ts: 0,
            path,
            file,
        })
    }

    pub fn open(path: &Path) -> Result<Manifest> {
        let path = path.to_path_buf();
        let read_file = File::open(&path)?;
        let mut manifest = Manifest {
            sstable_files: Vec::new(),
            next_sstable_id: 1,
            last_compaction_ts: 0,
            path: path.clone(),
            file: OpenOptions::new().create(true).read(true).append(true).open(&path)?,
        };

        let mut reader = BufReader::new(read_file);
        loop {
            let Some(record) = read_record(&mut reader)? else {
                break;
            };
            manifest.apply_record(record);
        }

        manifest.ensure_next_id_from_files();
        Ok(manifest)
    }

    pub fn append_record(&mut self, record: ManifestRecord) -> Result<()> {
        let encoded = encode_record(&record)?;
        self.file.write_all(&encoded)?;
        self.file.sync_all()?;
        self.apply_record(record);
        Ok(())
    }

    pub fn snapshot(&mut self) -> Result<()> {
        let record = ManifestRecord::Snapshot {
            sstable_files: self.sstable_files.clone(),
            next_sstable_id: self.next_sstable_id,
        };
        self.append_record(record)
    }

    pub fn current_sstables(&self) -> &[String] {
        &self.sstable_files
    }

    pub fn next_id(&mut self) -> u64 {
        let id = self.next_sstable_id;
        self.next_sstable_id += 1;
        id
    }

    pub fn state(&self) -> ManifestState {
        ManifestState {
            sstable_files: self.sstable_files.clone(),
            next_sstable_id: self.next_sstable_id,
            last_compaction_ts: self.last_compaction_ts,
        }
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn set_state(&mut self, state: ManifestState) {
        self.sstable_files = state.sstable_files;
        self.next_sstable_id = state.next_sstable_id;
        self.last_compaction_ts = state.last_compaction_ts;
        self.ensure_next_id_from_files();
    }

    fn apply_record(&mut self, record: ManifestRecord) {
        match record {
            ManifestRecord::AddSstable { filename } => {
                self.sstable_files.retain(|existing| existing != &filename);
                self.sstable_files.insert(0, filename.clone());
                self.bump_next_id_from_filename(&filename);
            }
            ManifestRecord::RemoveSstable { filename } => {
                self.sstable_files.retain(|existing| existing != &filename);
            }
            ManifestRecord::Compaction { added, removed } => {
                self.sstable_files
                    .retain(|existing| !removed.iter().any(|removed_name| removed_name == existing));
                for filename in added.iter().rev() {
                    self.sstable_files.retain(|existing| existing != filename);
                    self.sstable_files.insert(0, filename.clone());
                    self.bump_next_id_from_filename(filename);
                }
                self.last_compaction_ts = now_unix_ts();
            }
            ManifestRecord::Snapshot {
                sstable_files,
                next_sstable_id,
            } => {
                self.sstable_files = sstable_files;
                self.next_sstable_id = next_sstable_id.max(1);
            }
        }

        self.ensure_next_id_from_files();
    }

    fn ensure_next_id_from_files(&mut self) {
        let max_from_files = self
            .sstable_files
            .iter()
            .filter_map(|filename| parse_sstable_id(filename))
            .max()
            .map(|id| id + 1)
            .unwrap_or(1);
        self.next_sstable_id = self.next_sstable_id.max(max_from_files);
    }

    fn bump_next_id_from_filename(&mut self, filename: &str) {
        if let Some(id) = parse_sstable_id(filename) {
            self.next_sstable_id = self.next_sstable_id.max(id + 1);
        }
    }
}

fn encode_record(record: &ManifestRecord) -> Result<Vec<u8>> {
    let data = serde_json::to_vec(record)?;
    let checksum = crc32(&data);
    let record_len = u32::try_from(4 + data.len()).map_err(|_| {
        FerrisDbError::InvalidCommand("manifest record too large".to_string())
    })?;

    let mut out = Vec::with_capacity(4 + record_len as usize);
    out.extend_from_slice(&record_len.to_le_bytes());
    out.extend_from_slice(&checksum.to_le_bytes());
    out.extend_from_slice(&data);
    Ok(out)
}

fn read_record<R: Read>(reader: &mut R) -> Result<Option<ManifestRecord>> {
    let mut len_buf = [0_u8; 4];
    match reader.read_exact(&mut len_buf) {
        Ok(()) => {}
        Err(err) if err.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(None),
        Err(err) => return Err(err.into()),
    }

    let record_len = u32::from_le_bytes(len_buf) as usize;
    if record_len < 4 {
        return Err(FerrisDbError::InvalidCommand(
            "manifest record length too small".to_string(),
        ));
    }

    let mut body = vec![0_u8; record_len];
    reader.read_exact(&mut body)?;

    let stored_checksum =
        u32::from_le_bytes(body[0..4].try_into().map_err(|_| {
            FerrisDbError::InvalidCommand("invalid manifest checksum bytes".to_string())
        })?);
    let data = &body[4..];
    let actual_checksum = crc32(data);
    if stored_checksum != actual_checksum {
        return Err(FerrisDbError::InvalidCommand(
            "manifest checksum mismatch".to_string(),
        ));
    }

    Ok(Some(serde_json::from_slice(data)?))
}

fn crc32(bytes: &[u8]) -> u32 {
    let mut hasher = Hasher::new();
    hasher.update(bytes);
    hasher.finalize()
}

fn parse_sstable_id(filename: &str) -> Option<u64> {
    let stem = filename.strip_suffix(".sst").unwrap_or(filename);
    stem.parse::<u64>().ok()
}

fn now_unix_ts() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_secs())
        .unwrap_or(0)
}

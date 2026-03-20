// =============================================================================
// storage/wal.rs — Write-Ahead Log
// =============================================================================
//
// WAL（Write-Ahead Log）的用途：
// - 每次修改資料之前，先把操作 append 到磁碟日誌。
// - 這樣就算程式在 MemTable flush 前崩潰，重啟後仍可從 WAL 重放操作，
//   把未落盤的資料重新恢復到 MemTable。
//
// Crash recovery 流程：
// 1. put/delete 時，先寫 WAL 並 fsync。
// 2. 再更新記憶體中的 MemTable。
// 3. 如果程式在 flush 前崩潰，磁碟上仍保有 WAL。
// 4. 下次 open 時讀 WAL，依序重放所有操作，重建出當時的 MemTable 狀態。
// 5. 當 MemTable 成功 flush 成 SSTable 後，對應的 WAL 就可以刪除。

use std::fs::{File, OpenOptions};
use std::io::{BufReader, Read, Write};
use std::path::{Path, PathBuf};

use crc32fast::Hasher;

use crate::error::{FerrisDbError, Result};
use crate::storage::lsm::TOMBSTONE;
use crate::storage::memory::MemTable;
use crate::storage::traits::StorageEngine;

const OP_PUT: u8 = 1;
const OP_DELETE: u8 = 2;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WalRecord {
    Put { key: Vec<u8>, value: Vec<u8> },
    Delete { key: Vec<u8> },
}

#[derive(Debug)]
pub struct WalWriter {
    path: PathBuf,
    file: File,
}

impl WalWriter {
    /// 建立或開啟 WAL 檔案，採 append 模式。
    ///
    /// 之所以不用 truncate，是因為 LSM 重啟後若沿用既有 WAL，
    /// 我們要能繼續把後續操作接在檔尾。
    pub fn new(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref().to_path_buf();
        let file = OpenOptions::new()
            .create(true)
            .read(true)
            .append(true)
            .open(&path)?;

        Ok(Self { path, file })
    }

    pub fn append_put(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
        self.append_record(WalRecord::Put {
            key: key.to_vec(),
            value: value.to_vec(),
        })
    }

    pub fn append_delete(&mut self, key: &[u8]) -> Result<()> {
        self.append_record(WalRecord::Delete { key: key.to_vec() })
    }

    pub fn sync(&self) -> Result<()> {
        self.file.sync_all()?;
        Ok(())
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    fn append_record(&mut self, record: WalRecord) -> Result<()> {
        let encoded = encode_record(&record)?;
        self.file.write_all(&encoded)?;
        self.file.sync_all()?;
        Ok(())
    }
}

#[derive(Debug)]
pub struct WalReader {
    path: PathBuf,
}

impl WalReader {
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref().to_path_buf();
        let _ = File::open(&path)?;
        Ok(Self { path })
    }

    pub fn iter(&self) -> Result<WalIterator> {
        let file = File::open(&self.path)?;
        Ok(WalIterator {
            reader: BufReader::new(file),
        })
    }

    /// 把 WAL 內所有操作重放成一個 MemTable。
    ///
    /// 注意 Delete 在 LSM 中代表 tombstone，因此這裡不是直接 remove，
    /// 而是把 tombstone 值放回 MemTable，確保它能遮蔽更舊層的資料。
    pub fn recover_to_memtable(&self) -> Result<MemTable> {
        let mut memtable = MemTable::new();
        let iter = self.iter()?;

        for item in iter {
            match item? {
                WalRecord::Put { key, value } => memtable.put(key, value)?,
                WalRecord::Delete { key } => memtable.put(key, TOMBSTONE.to_vec())?,
            }
        }

        Ok(memtable)
    }
}

pub struct WalIterator {
    reader: BufReader<File>,
}

impl Iterator for WalIterator {
    type Item = Result<WalRecord>;

    fn next(&mut self) -> Option<Self::Item> {
        let mut len_buf = [0_u8; 4];
        match self.reader.read_exact(&mut len_buf) {
            Ok(()) => {}
            Err(err) if err.kind() == std::io::ErrorKind::UnexpectedEof => return None,
            Err(err) => return Some(Err(err.into())),
        }

        let record_len = u32::from_le_bytes(len_buf) as usize;
        if record_len < 4 + 1 + 4 + 4 {
            return Some(Err(FerrisDbError::InvalidCommand(
                "wal record length too small".to_string(),
            )));
        }

        let mut body = vec![0_u8; record_len];
        if let Err(err) = self.reader.read_exact(&mut body) {
            return Some(Err(err.into()));
        }

        Some(decode_record(&body))
    }
}

fn encode_record(record: &WalRecord) -> Result<Vec<u8>> {
    let (op, key, value): (u8, &[u8], &[u8]) = match record {
        WalRecord::Put { key, value } => (OP_PUT, key, value),
        WalRecord::Delete { key } => (OP_DELETE, key, &[]),
    };

    let key_len = u32::try_from(key.len()).map_err(|_| {
        FerrisDbError::InvalidCommand("wal key length exceeds u32".to_string())
    })?;
    let value_len = u32::try_from(value.len()).map_err(|_| {
        FerrisDbError::InvalidCommand("wal value length exceeds u32".to_string())
    })?;

    let mut payload = Vec::with_capacity(1 + 4 + 4 + key.len() + value.len());
    payload.push(op);
    payload.extend_from_slice(&key_len.to_le_bytes());
    payload.extend_from_slice(&value_len.to_le_bytes());
    payload.extend_from_slice(key);
    payload.extend_from_slice(value);

    let checksum = crc32(&payload);
    let record_len = u32::try_from(4 + payload.len()).map_err(|_| {
        FerrisDbError::InvalidCommand("wal record length exceeds u32".to_string())
    })?;

    let mut out = Vec::with_capacity(4 + record_len as usize);
    out.extend_from_slice(&record_len.to_le_bytes());
    out.extend_from_slice(&checksum.to_le_bytes());
    out.extend_from_slice(&payload);
    Ok(out)
}

fn decode_record(body: &[u8]) -> Result<WalRecord> {
    if body.len() < 4 + 1 + 4 + 4 {
        return Err(FerrisDbError::InvalidCommand(
            "wal record body too small".to_string(),
        ));
    }

    let stored_checksum = u32::from_le_bytes(body[0..4].try_into().map_err(|_| {
        FerrisDbError::InvalidCommand("invalid wal checksum bytes".to_string())
    })?);
    let payload = &body[4..];
    let actual_checksum = crc32(payload);
    if stored_checksum != actual_checksum {
        return Err(FerrisDbError::InvalidCommand(
            "wal checksum mismatch".to_string(),
        ));
    }

    let op = payload[0];
    let key_len = u32::from_le_bytes(payload[1..5].try_into().map_err(|_| {
        FerrisDbError::InvalidCommand("invalid wal key_len bytes".to_string())
    })?) as usize;
    let value_len = u32::from_le_bytes(payload[5..9].try_into().map_err(|_| {
        FerrisDbError::InvalidCommand("invalid wal value_len bytes".to_string())
    })?) as usize;

    let expected_len = 1 + 4 + 4 + key_len + value_len;
    if payload.len() != expected_len {
        return Err(FerrisDbError::InvalidCommand(
            "wal record length does not match payload".to_string(),
        ));
    }

    let key_start = 9;
    let key_end = key_start + key_len;
    let value_end = key_end + value_len;

    let key = payload[key_start..key_end].to_vec();
    let value = payload[key_end..value_end].to_vec();

    match op {
        OP_PUT => Ok(WalRecord::Put { key, value }),
        OP_DELETE => {
            if !value.is_empty() {
                return Err(FerrisDbError::InvalidCommand(
                    "delete wal record must not contain value".to_string(),
                ));
            }
            Ok(WalRecord::Delete { key })
        }
        _ => Err(FerrisDbError::InvalidCommand(format!(
            "unknown wal operation {}",
            op
        ))),
    }
}

fn crc32(bytes: &[u8]) -> u32 {
    let mut hasher = Hasher::new();
    hasher.update(bytes);
    hasher.finalize()
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::fs::OpenOptions;
    use std::io::{Seek, SeekFrom, Write};
    use std::path::PathBuf;
    use std::time::{SystemTime, UNIX_EPOCH};

    use crate::storage::lsm::TOMBSTONE;
    use crate::storage::traits::StorageEngine;

    use super::{WalReader, WalRecord, WalWriter};

    fn temp_file(name: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system time")
            .as_nanos();
        std::env::temp_dir().join(format!("ferrisdb-wal-{}-{}.log", name, nanos))
    }

    #[test]
    fn test_wal_roundtrip_records() {
        let path = temp_file("roundtrip");
        let mut writer = WalWriter::new(&path).expect("create writer");
        writer.append_put(b"a", b"1").expect("append put");
        writer.append_delete(b"b").expect("append delete");

        let reader = WalReader::open(&path).expect("open reader");
        let records: Vec<WalRecord> = reader
            .iter()
            .expect("iter")
            .map(|item| item.expect("record"))
            .collect();

        assert_eq!(
            records,
            vec![
                WalRecord::Put {
                    key: b"a".to_vec(),
                    value: b"1".to_vec(),
                },
                WalRecord::Delete { key: b"b".to_vec() },
            ]
        );

        let _ = fs::remove_file(path);
    }

    #[test]
    fn test_wal_recover_memtable() {
        let path = temp_file("recover");
        let mut writer = WalWriter::new(&path).expect("create writer");
        writer.append_put(b"a", b"1").expect("put a");
        writer.append_put(b"b", b"2").expect("put b");
        writer.append_delete(b"a").expect("delete a");

        let reader = WalReader::open(&path).expect("open reader");
        let memtable = reader.recover_to_memtable().expect("recover");

        assert_eq!(
            memtable.get(b"a").expect("get a"),
            Some(TOMBSTONE.to_vec())
        );
        assert_eq!(memtable.get(b"b").expect("get b"), Some(b"2".to_vec()));

        let _ = fs::remove_file(path);
    }

    #[test]
    fn test_wal_checksum_corruption_detected() {
        let path = temp_file("corrupt");
        let mut writer = WalWriter::new(&path).expect("create writer");
        writer.append_put(b"hello", b"world").expect("append put");

        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&path)
            .expect("open for corruption");
        file.seek(SeekFrom::Start(12)).expect("seek");
        file.write_all(&[0x7F]).expect("overwrite byte");
        file.sync_all().expect("sync corruption");

        let reader = WalReader::open(&path).expect("open reader");
        let mut iter = reader.iter().expect("iter");
        let err = iter.next().expect("first item").expect_err("should fail checksum");
        assert!(format!("{}", err).contains("checksum"));

        let _ = fs::remove_file(path);
    }
}

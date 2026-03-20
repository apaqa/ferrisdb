// =============================================================================
// storage/sstable/writer.rs — SSTableWriter
// =============================================================================
//
// SSTableWriter 的責任：
// 1. 接收「已排序」的 key-value 資料（呼叫端要保證 key 遞增）
// 2. 把資料寫到 data section
// 3. 同時累積 (key -> offset) 索引
// 4. finish() 時把 index section + footer 一次寫完
//
// 為什麼要索引？
// - data section 是連續資料，若沒有索引就只能線性掃描。
// - 有了 index（排序 key + offset）就能先二分搜尋 key，再直接 seek 到資料位置。

use std::fs::File;
use std::io::{BufWriter, Seek, Write};
use std::path::{Path, PathBuf};

use crate::error::{FerrisDbError, Result};

use super::format::{self, Footer};

#[derive(Debug)]
pub struct SSTableWriter {
    path: PathBuf,
    file: BufWriter<File>,
    index: Vec<(Vec<u8>, u64)>,
    last_key: Option<Vec<u8>>,
    finished: bool,
}

impl SSTableWriter {
    /// 建立新 sstable 檔案並寫入 header。
    pub fn new(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref().to_path_buf();
        let file = File::create(&path)?;
        let mut file = BufWriter::new(file);
        format::write_header(&mut file)?;

        Ok(Self {
            path,
            file,
            index: Vec::new(),
            last_key: None,
            finished: false,
        })
    }

    /// 寫入一筆資料，必須按 key 排序（遞增或相等）。
    pub fn write_entry(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
        if self.finished {
            return Err(FerrisDbError::InvalidCommand(
                "cannot write entry after finish".to_string(),
            ));
        }

        // SSTable 必須保持 key 排序，這樣 reader 才能做二分搜尋。
        if let Some(last) = &self.last_key {
            if key < last.as_slice() {
                return Err(FerrisDbError::InvalidCommand(
                    "keys must be written in sorted order".to_string(),
                ));
            }
        }

        let offset = self.file.stream_position()?;
        format::write_entry(&mut self.file, key, value)?;

        self.index.push((key.to_vec(), offset));
        self.last_key = Some(key.to_vec());
        Ok(())
    }

    /// 寫入 index + footer，並 flush/sync 檔案。
    pub fn finish(&mut self) -> Result<()> {
        if self.finished {
            return Ok(());
        }

        let index_offset = self.file.stream_position()?;
        for (key, offset) in &self.index {
            let value = offset.to_le_bytes();
            format::write_entry(&mut self.file, key, &value)?;
        }

        let footer = Footer {
            index_offset,
            index_count: self.index.len() as u64,
        };
        format::write_footer(&mut self.file, footer)?;

        self.file.flush()?;
        self.file.get_ref().sync_all()?;
        self.finished = true;
        Ok(())
    }

    /// 目前 writer 對應的檔案路徑（測試與除錯方便）。
    pub fn path(&self) -> &Path {
        &self.path
    }
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::path::PathBuf;
    use std::time::{SystemTime, UNIX_EPOCH};

    use super::SSTableWriter;

    fn temp_file(name: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system time")
            .as_nanos();
        std::env::temp_dir().join(format!("ferrisdb-sstable-{}-{}.sst", name, nanos))
    }

    #[test]
    fn test_writer_rejects_unsorted_keys() {
        let path = temp_file("writer-unsorted");
        let mut writer = SSTableWriter::new(&path).expect("create writer");

        writer
            .write_entry(b"key:2", b"value:2")
            .expect("write first entry");
        let err = writer
            .write_entry(b"key:1", b"value:1")
            .expect_err("should reject out-of-order key");

        assert!(format!("{}", err).contains("sorted order"));
        let _ = writer.finish();
        let _ = fs::remove_file(path);
    }

    #[test]
    fn test_writer_can_finish_empty_table() {
        let path = temp_file("writer-empty");
        let mut writer = SSTableWriter::new(&path).expect("create writer");
        writer.finish().expect("finish empty table");

        let metadata = fs::metadata(&path).expect("metadata");
        // 最少應包含 header + footer
        assert!(metadata.len() >= super::format::HEADER_SIZE + super::format::FOOTER_SIZE);

        let _ = fs::remove_file(path);
    }
}

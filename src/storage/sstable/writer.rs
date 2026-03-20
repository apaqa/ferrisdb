// =============================================================================
// storage/sstable/writer.rs — SSTableWriter
// =============================================================================
//
// Writer 的責任：
// 1. 接收排序好的 key/value
// 2. 寫入 data section
// 3. 建立 bloom filter 與 index
// 4. finish() 時把 bloom section、index section、footer 一次寫好

use std::fs::File;
use std::io::{BufWriter, Seek, Write};
use std::path::{Path, PathBuf};

use crate::error::{FerrisDbError, Result};
use crate::storage::bloom::BloomFilter;

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

    pub fn write_entry(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
        if self.finished {
            return Err(FerrisDbError::InvalidCommand(
                "cannot write entry after finish".to_string(),
            ));
        }

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

    pub fn finish(&mut self) -> Result<()> {
        if self.finished {
            return Ok(());
        }

        // 先依最終 key 數量建立 bloom filter，再把所有 key 塞進去。
        let mut bloom = BloomFilter::new(self.index.len().max(1), 0.01);
        for (key, _) in &self.index {
            bloom.insert(key);
        }
        let bloom_bytes = bloom.to_bytes();

        let bloom_offset = self.file.stream_position()?;
        self.file.write_all(&bloom_bytes)?;

        let index_offset = self.file.stream_position()?;
        for (key, offset) in &self.index {
            let value = offset.to_le_bytes();
            format::write_entry(&mut self.file, key, &value)?;
        }

        let footer = Footer {
            bloom_offset,
            bloom_len: bloom_bytes.len() as u64,
            index_offset,
            index_count: self.index.len() as u64,
        };
        format::write_footer(&mut self.file, footer)?;

        self.file.flush()?;
        self.file.get_ref().sync_all()?;
        self.finished = true;
        Ok(())
    }

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

        writer.write_entry(b"key:2", b"value:2").expect("write first");
        let err = writer
            .write_entry(b"key:1", b"value:1")
            .expect_err("should reject unsorted keys");

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
        assert!(metadata.len() >= super::format::HEADER_SIZE + super::format::FOOTER_SIZE);

        let _ = fs::remove_file(path);
    }
}

// =============================================================================
// storage/sstable/reader.rs — SSTableReader
// =============================================================================
//
// Reader 的核心流程：
// 1. open()：
//    - 讀 header 驗證 magic/version
//    - 從尾端讀 footer，取得 index_offset/index_count
//    - 載入 index 到記憶體（Vec<(key, offset)>）
//
// 2. get(key)：
//    - 在 index 向量上做「二分搜尋」
//    - 找到後取出 offset，直接 seek 到 data section 對應 entry 讀值
//
// 二分搜尋為什麼可行？
// - index 依 key 排序（writer 保證按序寫入）
// - 每次比較中間元素，將搜尋範圍減半，時間複雜度 O(log n)

use std::fs::File;
use std::io::{BufReader, Seek, SeekFrom};
use std::path::{Path, PathBuf};

use crate::error::{FerrisDbError, Result};

use super::format::{self, Footer};

#[derive(Debug, Clone)]
pub struct SSTableReader {
    path: PathBuf,
    index: Vec<(Vec<u8>, u64)>,
    index_offset: u64,
}

impl SSTableReader {
    /// 開啟既有 sstable 檔案並載入索引。
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref().to_path_buf();
        let mut file = File::open(&path)?;
        let file_len = file.metadata()?.len();

        // 檔案長度至少要容納 header + footer
        if file_len < format::HEADER_SIZE + format::FOOTER_SIZE {
            return Err(FerrisDbError::InvalidCommand(
                "sstable file is too small".to_string(),
            ));
        }

        format::read_and_validate_header(&mut file)?;

        let footer = Self::read_footer(&mut file)?;
        Self::validate_footer_positions(file_len, footer)?;
        let index = Self::load_index(&mut file, footer)?;

        Ok(Self {
            path,
            index,
            index_offset: footer.index_offset,
        })
    }

    /// 以 index 二分搜尋 key，若存在則讀回 value。
    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let found = self
            .index
            .binary_search_by(|(idx_key, _)| idx_key.as_slice().cmp(key));

        let pos = match found {
            Ok(pos) => pos,
            Err(_) => return Ok(None),
        };

        let offset = self.index[pos].1;
        let (actual_key, value) = self.read_data_entry_at(offset)?;
        if actual_key != key {
            return Err(FerrisDbError::InvalidCommand(
                "sstable index points to mismatched key".to_string(),
            ));
        }
        Ok(Some(value))
    }

    /// 取得資料區域迭代器（依 key 排序順序）。
    pub fn iter(&self) -> Result<SSTableIterator> {
        let mut file = File::open(&self.path)?;
        file.seek(SeekFrom::Start(format::HEADER_SIZE))?;
        Ok(SSTableIterator {
            reader: BufReader::new(file),
            data_end_offset: self.index_offset,
        })
    }

    fn read_footer(file: &mut File) -> Result<Footer> {
        file.seek(SeekFrom::End(-(format::FOOTER_SIZE as i64)))?;
        Ok(format::read_and_validate_footer(file)?)
    }

    fn validate_footer_positions(file_len: u64, footer: Footer) -> Result<()> {
        let data_end = file_len - format::FOOTER_SIZE;
        if footer.index_offset < format::HEADER_SIZE || footer.index_offset > data_end {
            return Err(FerrisDbError::InvalidCommand(
                "sstable footer has invalid index_offset".to_string(),
            ));
        }
        Ok(())
    }

    fn load_index(file: &mut File, footer: Footer) -> Result<Vec<(Vec<u8>, u64)>> {
        file.seek(SeekFrom::Start(footer.index_offset))?;

        let mut index = Vec::with_capacity(footer.index_count as usize);
        for _ in 0..footer.index_count {
            let (key, value) = format::read_entry(&mut *file)?;
            if value.len() != format::INDEX_VALUE_SIZE as usize {
                return Err(FerrisDbError::InvalidCommand(
                    "sstable index value length must be 8 bytes".to_string(),
                ));
            }

            let mut buf = [0_u8; 8];
            buf.copy_from_slice(&value);
            index.push((key, u64::from_le_bytes(buf)));
        }

        Ok(index)
    }

    fn read_data_entry_at(&self, offset: u64) -> Result<(Vec<u8>, Vec<u8>)> {
        let mut file = File::open(&self.path)?;
        file.seek(SeekFrom::Start(offset))?;
        Ok(format::read_entry(&mut file)?)
    }
}

/// SSTable 的資料區塊迭代器。
///
/// 注意：這個迭代器只掃 data section，不會碰 index/footer。
pub struct SSTableIterator {
    reader: BufReader<File>,
    data_end_offset: u64,
}

impl Iterator for SSTableIterator {
    type Item = Result<(Vec<u8>, Vec<u8>)>;

    fn next(&mut self) -> Option<Self::Item> {
        let pos = match self.reader.stream_position() {
            Ok(pos) => pos,
            Err(err) => return Some(Err(err.into())),
        };

        if pos >= self.data_end_offset {
            return None;
        }

        match format::read_entry(&mut self.reader) {
            Ok((k, v)) => Some(Ok((k, v))),
            Err(err) => Some(Err(err.into())),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::path::PathBuf;
    use std::time::{SystemTime, UNIX_EPOCH};

    use crate::storage::sstable::writer::SSTableWriter;

    use super::SSTableReader;

    fn temp_file(name: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system time")
            .as_nanos();
        std::env::temp_dir().join(format!("ferrisdb-sstable-{}-{}.sst", name, nanos))
    }

    #[test]
    fn test_reader_get_basic() {
        let path = temp_file("reader-basic");

        let mut writer = SSTableWriter::new(&path).expect("create writer");
        writer.write_entry(b"a", b"1").expect("write a");
        writer.write_entry(b"b", b"2").expect("write b");
        writer.finish().expect("finish");

        let reader = SSTableReader::open(&path).expect("open reader");
        assert_eq!(reader.get(b"a").expect("get a"), Some(b"1".to_vec()));
        assert_eq!(reader.get(b"b").expect("get b"), Some(b"2".to_vec()));
        assert_eq!(reader.get(b"c").expect("get c"), None);

        let _ = fs::remove_file(path);
    }
}

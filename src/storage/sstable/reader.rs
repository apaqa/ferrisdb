// =============================================================================
// storage/sstable/reader.rs — SSTableReader
// =============================================================================
//
// Reader 的關鍵步驟：
// 1. open() 時讀 header/footer，載入 bloom filter 與 index
// 2. get() 時先查 bloom filter
//    - 如果 may_contain == false，代表 key 一定不在這個 SSTable
//    - 可以直接略過，不必做二分搜尋
// 3. 若 bloom 表示「可能存在」，才進一步在 index 上二分搜尋
//
// index 二分搜尋原理：
// - index 中的 key 本身已排序
// - 每次比較中間 key，將搜尋範圍縮小一半
// - 找到 offset 後再 seek 到 data section 讀取真正 entry

use std::fs::File;
use std::io::{BufReader, Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};

use crate::error::{FerrisDbError, Result};
use crate::storage::bloom::BloomFilter;

use super::format::{self, Footer};

#[derive(Debug, Clone)]
pub struct SSTableReader {
    path: PathBuf,
    bloom_filter: BloomFilter,
    index: Vec<(Vec<u8>, u64)>,
    data_end_offset: u64,
}

impl SSTableReader {
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref().to_path_buf();
        let mut file = File::open(&path)?;
        let file_len = file.metadata()?.len();

        if file_len < format::HEADER_SIZE + format::FOOTER_SIZE {
            return Err(FerrisDbError::InvalidCommand(
                "sstable file is too small".to_string(),
            ));
        }

        format::read_and_validate_header(&mut file)?;
        let footer = Self::read_footer(&mut file)?;
        Self::validate_footer_positions(file_len, footer)?;

        let bloom_filter = Self::load_bloom_filter(&mut file, footer)?;
        let index = Self::load_index(&mut file, footer)?;

        Ok(Self {
            path,
            bloom_filter,
            index,
            data_end_offset: footer.bloom_offset,
        })
    }

    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        if !self.bloom_filter.may_contain(key) {
            return Ok(None);
        }

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

    pub fn iter(&self) -> Result<SSTableIterator> {
        let mut file = File::open(&self.path)?;
        file.seek(SeekFrom::Start(format::HEADER_SIZE))?;
        Ok(SSTableIterator {
            reader: BufReader::new(file),
            data_end_offset: self.data_end_offset,
        })
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    fn read_footer(file: &mut File) -> Result<Footer> {
        file.seek(SeekFrom::End(-(format::FOOTER_SIZE as i64)))?;
        Ok(format::read_and_validate_footer(file)?)
    }

    fn validate_footer_positions(file_len: u64, footer: Footer) -> Result<()> {
        let footer_offset = file_len - format::FOOTER_SIZE;
        if footer.bloom_offset < format::HEADER_SIZE
            || footer.bloom_offset > footer_offset
            || footer.bloom_offset + footer.bloom_len > footer.index_offset
            || footer.index_offset > footer_offset
        {
            return Err(FerrisDbError::InvalidCommand(
                "sstable footer has invalid section offsets".to_string(),
            ));
        }
        Ok(())
    }

    fn load_bloom_filter(file: &mut File, footer: Footer) -> Result<BloomFilter> {
        file.seek(SeekFrom::Start(footer.bloom_offset))?;
        let mut data = vec![0_u8; footer.bloom_len as usize];
        file.read_exact(&mut data)?;
        BloomFilter::from_bytes(&data)
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

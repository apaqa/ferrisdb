// =============================================================================
// storage/bloom.rs — Bloom Filter
// =============================================================================
//
// Bloom Filter 是一種節省空間的 probabilistic data structure。
// 它很適合回答這個問題：
// - 「某個 key 一定不存在嗎？」
//
// 特性：
// - 若 `may_contain(key) == false`，那麼 key 一定不存在。
// - 若 `may_contain(key) == true`，key 可能存在，也可能只是 false positive。
//
// 在 SSTable 中，Bloom Filter 的用途是：
// - 在真正做 index 二分搜尋之前，先快速判斷 key 幾乎不可能在這個檔案裡。
// - 這能讓 LSM 讀取時跳過大量不相關的 SSTable。

use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

use crate::error::{FerrisDbError, Result};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BloomFilter {
    bit_len: usize,
    num_hashes: u32,
    bits: Vec<u8>,
}

impl BloomFilter {
    pub fn new(expected_items: usize, false_positive_rate: f64) -> BloomFilter {
        let expected_items = expected_items.max(1);
        let false_positive_rate = false_positive_rate.clamp(0.000_001, 0.999_999);

        let m = (-(expected_items as f64) * false_positive_rate.ln() / (2.0_f64.ln().powi(2)))
            .ceil()
            .max(8.0) as usize;
        let k = ((m as f64 / expected_items as f64) * 2.0_f64.ln())
            .ceil()
            .max(1.0) as u32;

        let byte_len = m.div_ceil(8);
        BloomFilter {
            bit_len: m,
            num_hashes: k,
            bits: vec![0_u8; byte_len],
        }
    }

    pub fn insert(&mut self, key: &[u8]) {
        for bit_index in self.bit_indexes(key) {
            self.set_bit(bit_index);
        }
    }

    pub fn may_contain(&self, key: &[u8]) -> bool {
        self.bit_indexes(key).into_iter().all(|idx| self.get_bit(idx))
    }

    pub fn to_bytes(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(8 + 4 + 8 + self.bits.len());
        out.extend_from_slice(&(self.bit_len as u64).to_le_bytes());
        out.extend_from_slice(&self.num_hashes.to_le_bytes());
        out.extend_from_slice(&(self.bits.len() as u64).to_le_bytes());
        out.extend_from_slice(&self.bits);
        out
    }

    pub fn from_bytes(data: &[u8]) -> Result<BloomFilter> {
        if data.len() < 20 {
            return Err(FerrisDbError::InvalidCommand(
                "bloom filter bytes too short".to_string(),
            ));
        }

        let bit_len = u64::from_le_bytes(
            data[0..8]
                .try_into()
                .map_err(|_| FerrisDbError::InvalidCommand("invalid bloom bit_len".to_string()))?,
        ) as usize;
        let num_hashes = u32::from_le_bytes(
            data[8..12]
                .try_into()
                .map_err(|_| FerrisDbError::InvalidCommand("invalid bloom num_hashes".to_string()))?,
        );
        let bytes_len = u64::from_le_bytes(
            data[12..20]
                .try_into()
                .map_err(|_| FerrisDbError::InvalidCommand("invalid bloom bytes_len".to_string()))?,
        ) as usize;

        if data.len() != 20 + bytes_len {
            return Err(FerrisDbError::InvalidCommand(
                "bloom filter bytes length mismatch".to_string(),
            ));
        }
        if bit_len == 0 || num_hashes == 0 {
            return Err(FerrisDbError::InvalidCommand(
                "bloom filter parameters must be non-zero".to_string(),
            ));
        }

        Ok(BloomFilter {
            bit_len,
            num_hashes,
            bits: data[20..].to_vec(),
        })
    }

    fn bit_indexes(&self, key: &[u8]) -> Vec<usize> {
        let h1 = self.hash_with_seed(key, 0x9e37_79b9_7f4a_7c15);
        let mut h2 = self.hash_with_seed(key, 0xc2b2_ae35_87b7_3a4d);
        if h2 == 0 {
            h2 = 1;
        }

        (0..self.num_hashes)
            .map(|i| h1.wrapping_add((i as u64).wrapping_mul(h2)) % self.bit_len as u64)
            .map(|idx| idx as usize)
            .collect()
    }

    fn hash_with_seed(&self, key: &[u8], seed: u64) -> u64 {
        let mut hasher = DefaultHasher::new();
        seed.hash(&mut hasher);
        key.hash(&mut hasher);
        hasher.finish()
    }

    fn set_bit(&mut self, bit_index: usize) {
        let byte_index = bit_index / 8;
        let bit_offset = bit_index % 8;
        self.bits[byte_index] |= 1 << bit_offset;
    }

    fn get_bit(&self, bit_index: usize) -> bool {
        let byte_index = bit_index / 8;
        let bit_offset = bit_index % 8;
        (self.bits[byte_index] & (1 << bit_offset)) != 0
    }
}

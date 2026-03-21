// =============================================================================
// transaction/mvcc.rs — MVCC 交易層
// =============================================================================
//
// MVCC（Multi-Version Concurrency Control）核心觀念：
// - 每次寫入都帶一個 timestamp（版本號）
// - 讀 transaction 在開始時拿到一個 read_ts（快照時間點）
// - 之後它只能看到 <= read_ts 的版本
//
// 這樣可以達成 snapshot isolation：
// - reader 看到的是一致快照
// - writer 寫入新版本時，不會直接覆蓋舊版本
// - reader 和 writer 彼此不需要阻擋
//
// 在這個簡化版本中：
// - Transaction 的寫入先存在本地 writes buffer
// - commit 時才一次寫入到底層 LsmEngine
// - rollback 不需要真的做什麼，只要不 commit 即可

use std::collections::{BTreeMap, BTreeSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use crate::error::Result;
use crate::storage::lsm::{LsmEngine, TOMBSTONE};

use super::keyutil::{decode_key, encode_key, encode_key_prefix_end};

#[derive(Debug)]
pub struct MvccEngine {
    pub inner: Arc<LsmEngine>,
    pub next_ts: AtomicU64,
}

#[derive(Debug, Clone)]
pub struct Transaction {
    pub engine: Arc<MvccEngine>,
    pub read_ts: u64,
    pub writes: Vec<(Vec<u8>, Option<Vec<u8>>)>,
    pub committed: bool,
}

impl MvccEngine {
    pub fn new(lsm: LsmEngine) -> MvccEngine {
        let next_ts = infer_next_timestamp(&lsm);
        MvccEngine {
            inner: Arc::new(lsm),
            next_ts: AtomicU64::new(next_ts),
        }
    }

    pub fn begin_transaction(self: &Arc<Self>) -> Transaction {
        // read_ts 取目前「已提交」的最新時間點。
        // next_ts 指向下一個可分配的 write timestamp，因此要減 1。
        let read_ts = self.next_ts.load(Ordering::SeqCst).saturating_sub(1);
        Transaction {
            engine: Arc::clone(self),
            read_ts,
            writes: Vec::new(),
            committed: false,
        }
    }

    pub fn next_timestamp(&self) -> u64 {
        self.next_ts.fetch_add(1, Ordering::SeqCst)
    }

    pub fn shutdown(&self) -> Result<()> {
        self.inner.shutdown()
    }

    pub fn compact(&self) -> Result<()> {
        self.inner.compact()
    }
}

impl Transaction {
    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        // 先看本 transaction 內是否有尚未 commit 的覆蓋值。
        for (write_key, write_value) in self.writes.iter().rev() {
            if write_key.as_slice() == key {
                return Ok(write_value.clone());
            }
        }

        let encoded_start = encode_key(key, self.read_ts);
        let encoded_end = encode_key_prefix_end(key);

        let rows = self.engine.inner.raw_scan(&encoded_start, &encoded_end)?;
        for (encoded_key, value) in rows {
            let (user_key, ts) = decode_key(&encoded_key);
            if user_key == key && ts <= self.read_ts {
                if value == TOMBSTONE {
                    return Ok(None);
                }
                return Ok(Some(value));
            }
        }

        Ok(None)
    }

    pub fn put(&mut self, key: Vec<u8>, value: Vec<u8>) -> Result<()> {
        self.writes.push((key, Some(value)));
        Ok(())
    }

    pub fn delete(&mut self, key: &[u8]) -> Result<()> {
        self.writes.push((key.to_vec(), None));
        Ok(())
    }

    pub fn commit(&mut self) -> Result<()> {
        if self.committed {
            return Ok(());
        }

        let write_ts = self.engine.next_timestamp();
        for (key, value_opt) in &self.writes {
            let encoded_key = encode_key(key, write_ts);
            let value = value_opt.clone().unwrap_or_else(|| TOMBSTONE.to_vec());
            self.engine.inner.put_entry(encoded_key, value)?;
        }

        self.committed = true;
        Ok(())
    }

    pub fn scan(&self, start: &[u8], end: &[u8]) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        let mut visible = BTreeMap::<Vec<u8>, Vec<u8>>::new();
        let mut seen = BTreeSet::<Vec<u8>>::new();

        {
            let rows = self.engine.inner.raw_list_all()?;
            for (encoded_key, value) in rows {
                let (user_key, ts) = decode_key(&encoded_key);
                if user_key < start || user_key > end || ts > self.read_ts {
                    continue;
                }

                // 因為 encoded key 會讓新版本排前面，所以每個 user_key 第一筆
                // 符合 read_ts 的版本就是這個 transaction 可見的最新版本。
                if seen.contains(user_key) {
                    continue;
                }
                seen.insert(user_key.to_vec());
                if value != TOMBSTONE {
                    visible.insert(user_key.to_vec(), value);
                }
            }
        }

        // 最後疊上本 transaction 自己的未提交寫入，優先權最高。
        for (key, value_opt) in &self.writes {
            if key.as_slice() < start || key.as_slice() > end {
                continue;
            }
            match value_opt {
                Some(value) => {
                    visible.insert(key.clone(), value.clone());
                }
                None => {
                    visible.remove(key);
                }
            }
        }

        Ok(visible.into_iter().collect())
    }

    pub fn rollback(&mut self) {
        self.writes.clear();
        self.committed = false;
    }
}

fn infer_next_timestamp(lsm: &LsmEngine) -> u64 {
    let mut max_ts = 0_u64;
    if let Ok(rows) = lsm.raw_list_all() {
        for (encoded_key, _) in rows {
            if encoded_key.len() < 8 {
                continue;
            }
            let (_, ts) = decode_key(&encoded_key);
            if ts > max_ts {
                max_ts = ts;
            }
        }
    }
    max_ts + 1
}

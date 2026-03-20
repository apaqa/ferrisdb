// =============================================================================
// storage/compaction.rs — SSTable Compaction
// =============================================================================
//
// Compaction 的目標：
// - 把多個 SSTable 合併成較少甚至單一的 SSTable
// - 同一個 key 只保留最新版本
// - 把 tombstone 真正清掉，不再往新檔寫入
//
// 為什麼需要 compaction？
// - LSM 寫入很快，但久了會累積很多 SSTable
// - 讀取時就得查很多層
// - tombstone 也會一直存在
// - compaction 可以整理這些歷史資料，降低讀取成本

use std::cmp::Ordering;
use std::collections::BinaryHeap;
use std::path::{Path, PathBuf};

use crate::error::Result;
use crate::storage::lsm::TOMBSTONE;
use crate::storage::sstable::{SSTableReader, SSTableWriter};

#[derive(Debug, Eq, PartialEq)]
struct HeapItem {
    key: Vec<u8>,
    value: Vec<u8>,
    source_index: usize,
}

impl Ord for HeapItem {
    fn cmp(&self, other: &Self) -> Ordering {
        // BinaryHeap 是 max-heap，這裡反轉 key 比較以模擬 min-heap。
        match other.key.cmp(&self.key) {
            Ordering::Equal => other.source_index.cmp(&self.source_index),
            order => order,
        }
    }
}

impl PartialOrd for HeapItem {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

pub fn compact(sstables: &[PathBuf], output_path: &Path) -> Result<()> {
    if sstables.is_empty() {
        let mut writer = SSTableWriter::new(output_path)?;
        writer.finish()?;
        return Ok(());
    }

    // 呼叫端約定 sstables 順序是「新 -> 舊」。
    // 因此 source_index 越小，版本越新。
    let readers: Vec<SSTableReader> = sstables
        .iter()
        .map(SSTableReader::open)
        .collect::<Result<Vec<_>>>()?;

    let mut iterators = readers
        .iter()
        .map(|reader| reader.iter())
        .collect::<Result<Vec<_>>>()?;

    let mut heap = BinaryHeap::new();
    for (source_index, iter) in iterators.iter_mut().enumerate() {
        if let Some(entry) = iter.next() {
            let (key, value) = entry?;
            heap.push(HeapItem {
                key,
                value,
                source_index,
            });
        }
    }

    let mut writer = SSTableWriter::new(output_path)?;

    while let Some(first) = heap.pop() {
        let current_key = first.key.clone();
        let mut candidates = vec![first];

        // 把所有同 key 的項目都拿出來，一次決定要保留哪個版本。
        loop {
            let should_take = heap
                .peek()
                .map(|item| item.key == current_key)
                .unwrap_or(false);
            if !should_take {
                break;
            }
            candidates.push(heap.pop().expect("heap peeked item must exist"));
        }

        // 最新版本是 source_index 最小的那個。
        candidates.sort_by_key(|item| item.source_index);
        let winner = &candidates[0];
        if winner.value != TOMBSTONE {
            writer.write_entry(&winner.key, &winner.value)?;
        }

        // 把剛才消耗掉的 iterator 往前推一格。
        for item in candidates {
            if let Some(entry) = iterators[item.source_index].next() {
                let (key, value) = entry?;
                heap.push(HeapItem {
                    key,
                    value,
                    source_index: item.source_index,
                });
            }
        }
    }

    writer.finish()?;
    Ok(())
}

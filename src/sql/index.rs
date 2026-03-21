// =============================================================================
// sql/index.rs -- SQL Secondary Index 管理
// =============================================================================
//
// Secondary Index 的目的是避免每次 SELECT ... WHERE col = value 都做全表掃描。
// 這個版本採用最直接的做法：把 index entry 也存在同一個 KV store 裡。
//
// 1. Index metadata
//    - key: "__meta:index:{table}:{column}"
//    - value: JSON 序列化的 IndexDefinition
//    - 用來記錄某個 table 的哪個欄位目前已建立 index
//
// 2. Index entry
//    - key: "__idx:{table}:{column}:{value_hex}:{pk_hex}"
//    - value: 空 bytes
//    - 一筆 row 對應一個 secondary index entry，方便用 prefix scan 找出符合條件的主鍵
//
// 這個設計雖然簡單，但很適合教學與作品集展示：
// - 建 index 不需要額外檔案格式
// - index entry 同樣走 MVCC / WAL / LSM，重啟後自動存在
// - lookup 時只要掃描 index prefix，就能拿到所有匹配的 primary key

use std::sync::Arc;

use serde::{Deserialize, Serialize};

use crate::error::Result;
use crate::transaction::mvcc::{MvccEngine, Transaction};

use super::ast::{Operator, Value, WhereExpr};
use super::catalog::{Catalog, TableSchema};
use super::row::{
    decode_row_key, encode_row_prefix_end, encode_row_prefix_start, primary_key_to_string, Row,
};

pub const INDEX_META_PREFIX: &str = "__meta:index:";
pub const INDEX_ENTRY_PREFIX: &str = "__idx:";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct IndexDefinition {
    pub table_name: String,
    pub column_name: String,
}

#[derive(Debug, Clone)]
pub struct IndexManager {
    engine: Arc<MvccEngine>,
    catalog: Catalog,
}

impl IndexManager {
    pub fn new(engine: Arc<MvccEngine>) -> Self {
        let catalog = Catalog::new(Arc::clone(&engine));
        Self { engine, catalog }
    }

    pub fn create_index(&self, table: &str, column: &str) -> Result<()> {
        let mut txn = self.engine.begin_transaction();
        self.create_index_in_txn(&mut txn, table, column)?;
        txn.commit()
    }

    pub fn create_index_in_txn(
        &self,
        txn: &mut Transaction,
        table: &str,
        column: &str,
    ) -> Result<bool> {
        let Some(schema) = self.catalog.get_table(txn, table)? else {
            return Ok(false);
        };
        if !schema.columns.iter().any(|col| col.name == column) {
            return Ok(false);
        }

        let meta_key = encode_index_meta_key(table, column);
        if txn.get(&meta_key)?.is_some() {
            return Ok(false);
        }

        let definition = IndexDefinition {
            table_name: table.to_string(),
            column_name: column.to_string(),
        };
        txn.put(meta_key, serde_json::to_vec(&definition)?)?;

        for (_, row) in self.scan_rows(txn, &schema)? {
            if let (Some(index_value), Some(pk_value)) =
                (row.get(column), row.get(&schema.columns[0].name))
            {
                let key = encode_index_entry_key(table, column, index_value, pk_value);
                txn.put(key, Vec::new())?;
            }
        }

        Ok(true)
    }

    pub fn drop_index(&self, table: &str, column: &str) -> Result<()> {
        let mut txn = self.engine.begin_transaction();
        self.drop_index_in_txn(&mut txn, table, column)?;
        txn.commit()
    }

    pub fn drop_index_in_txn(
        &self,
        txn: &mut Transaction,
        table: &str,
        column: &str,
    ) -> Result<bool> {
        let meta_key = encode_index_meta_key(table, column);
        if txn.get(&meta_key)?.is_none() {
            return Ok(false);
        }

        txn.delete(&meta_key)?;
        let start = encode_index_entry_prefix(table, column);
        let end = prefix_end(&start);
        for (key, _) in txn.scan(&start, &end)? {
            txn.delete(&key)?;
        }
        Ok(true)
    }

    pub fn insert_index_entry(
        &self,
        txn: &mut Transaction,
        table: &str,
        column: &str,
        value: &Value,
        pk: &Value,
    ) -> Result<()> {
        let key = encode_index_entry_key(table, column, value, pk);
        txn.put(key, Vec::new())
    }

    pub fn delete_index_entry(
        &self,
        txn: &mut Transaction,
        table: &str,
        column: &str,
        value: &Value,
        pk: &Value,
    ) -> Result<()> {
        let key = encode_index_entry_key(table, column, value, pk);
        txn.delete(&key)
    }

    pub fn lookup(
        &self,
        txn: &Transaction,
        table: &str,
        column: &str,
        value: &Value,
    ) -> Result<Vec<Value>> {
        let start = encode_index_lookup_prefix(table, column, value);
        let end = prefix_end(&start);
        let mut pks = Vec::new();
        for (key, _) in txn.scan(&start, &end)? {
            if let Some(pk) = decode_index_pk(&key) {
                pks.push(pk);
            }
        }
        Ok(pks)
    }

    pub fn has_index(&self, txn: &Transaction, table: &str, column: &str) -> Result<bool> {
        Ok(txn.get(&encode_index_meta_key(table, column))?.is_some())
    }

    pub fn list_indexes(&self, txn: &Transaction, table: &str) -> Result<Vec<String>> {
        let start = encode_index_meta_prefix(table);
        let end = prefix_end(&start);
        let mut columns = Vec::new();
        for (_, value) in txn.scan(&start, &end)? {
            let definition: IndexDefinition = serde_json::from_slice(&value)?;
            columns.push(definition.column_name);
        }
        columns.sort();
        columns.dedup();
        Ok(columns)
    }

    // 中文註解：從 WhereExpr 中找出最適合先走 index scan 的等值條件。
    pub fn find_indexable_comparison<'a>(
        &self,
        txn: &Transaction,
        table: &str,
        where_expr: Option<&'a WhereExpr>,
    ) -> Result<Option<(&'a str, &'a Value)>> {
        let Some(where_expr) = where_expr else {
            return Ok(None);
        };
        self.find_indexable_comparison_in_expr(txn, table, where_expr)
    }

    fn scan_rows(&self, txn: &Transaction, schema: &TableSchema) -> Result<Vec<(Vec<u8>, Row)>> {
        let start = encode_row_prefix_start(&schema.table_name);
        let end = encode_row_prefix_end(&schema.table_name);
        let mut rows = Vec::new();
        for (key, value) in txn.scan(&start, &end)? {
            let Some((table_name, _pk)) = decode_row_key(&key) else {
                continue;
            };
            if table_name != schema.table_name {
                continue;
            }

            let row: Row = serde_json::from_slice(&value)?;
            rows.push((key, row));
        }
        Ok(rows)
    }

    // 中文註解：只有單一等值比較或 AND 中的其中一支等值比較可以直接轉成 index lookup。
    fn find_indexable_comparison_in_expr<'a>(
        &self,
        txn: &Transaction,
        table: &str,
        where_expr: &'a WhereExpr,
    ) -> Result<Option<(&'a str, &'a Value)>> {
        match where_expr {
            WhereExpr::Comparison {
                column,
                operator: Operator::Eq,
                value,
            } => {
                if self.has_index(txn, table, column)? {
                    Ok(Some((column.as_str(), value)))
                } else {
                    Ok(None)
                }
            }
            WhereExpr::And(left, right) => Ok(self
                .find_indexable_comparison_in_expr(txn, table, left)?
                .or(self.find_indexable_comparison_in_expr(txn, table, right)?)),
            _ => Ok(None),
        }
    }
}

pub fn encode_index_meta_key(table: &str, column: &str) -> Vec<u8> {
    format!("{}{}:{}", INDEX_META_PREFIX, table, column).into_bytes()
}

pub fn encode_index_meta_prefix(table: &str) -> Vec<u8> {
    format!("{}{}:", INDEX_META_PREFIX, table).into_bytes()
}

pub fn encode_index_entry_key(table: &str, column: &str, value: &Value, pk: &Value) -> Vec<u8> {
    format!(
        "{}{}:{}:{}:{}",
        INDEX_ENTRY_PREFIX,
        table,
        column,
        encode_value_component(value),
        encode_value_component(pk)
    )
    .into_bytes()
}

pub fn encode_index_entry_prefix(table: &str, column: &str) -> Vec<u8> {
    format!("{}{}:{}:", INDEX_ENTRY_PREFIX, table, column).into_bytes()
}

pub fn encode_index_lookup_prefix(table: &str, column: &str, value: &Value) -> Vec<u8> {
    format!(
        "{}{}:{}:{}:",
        INDEX_ENTRY_PREFIX,
        table,
        column,
        encode_value_component(value)
    )
    .into_bytes()
}

pub fn encode_value_component(value: &Value) -> String {
    let raw = serde_json::to_vec(value).unwrap_or_else(|_| primary_key_to_string(value).into_bytes());
    hex_encode(&raw)
}

pub fn decode_value_component(component: &str) -> Option<Value> {
    let bytes = hex_decode(component)?;
    serde_json::from_slice(&bytes).ok()
}

fn decode_index_pk(key: &[u8]) -> Option<Value> {
    let key = std::str::from_utf8(key).ok()?;
    let (_, pk_hex) = key.rsplit_once(':')?;
    decode_value_component(pk_hex)
}

fn prefix_end(prefix: &[u8]) -> Vec<u8> {
    let mut end = prefix.to_vec();
    end.push(0xFF);
    end
}

fn hex_encode(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        out.push(HEX[(byte >> 4) as usize] as char);
        out.push(HEX[(byte & 0x0f) as usize] as char);
    }
    out
}

fn hex_decode(input: &str) -> Option<Vec<u8>> {
    if !input.len().is_multiple_of(2) {
        return None;
    }

    let mut out = Vec::with_capacity(input.len() / 2);
    let bytes = input.as_bytes();
    for chunk in bytes.chunks(2) {
        let high = hex_nibble(chunk[0])?;
        let low = hex_nibble(chunk[1])?;
        out.push((high << 4) | low);
    }
    Some(out)
}

fn hex_nibble(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        b'A'..=b'F' => Some(byte - b'A' + 10),
        _ => None,
    }
}

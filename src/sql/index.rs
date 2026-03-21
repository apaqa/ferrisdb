// =============================================================================
// sql/index.rs -- SQL Secondary / Composite Index
// =============================================================================
use std::collections::BTreeMap;
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
    // 中文註解：欄位順序保留 composite index 的 prefix 匹配語意。
    pub table_name: String,
    pub column_names: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IndexLookupPlan {
    // 中文註解：scan_columns 用於鎖定哪一個索引，prefix_values 表示已匹配到的前綴欄位值。
    pub scan_columns: Vec<String>,
    pub prefix_values: Vec<Value>,
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

    pub fn create_index_in_txn(
        &self,
        txn: &mut Transaction,
        table: &str,
        columns: &[String],
    ) -> Result<bool> {
        let Some(schema) = self.catalog.get_table(txn, table)? else {
            return Ok(false);
        };
        if columns.is_empty()
            || columns
                .iter()
                .any(|column| !schema.columns.iter().any(|item| item.name == *column))
        {
            return Ok(false);
        }

        let meta_key = encode_index_meta_key(table, columns);
        if txn.get(&meta_key)?.is_some() {
            return Ok(false);
        }

        let definition = IndexDefinition {
            table_name: table.to_string(),
            column_names: columns.to_vec(),
        };
        txn.put(meta_key, serde_json::to_vec(&definition)?)?;

        for (_, row) in self.scan_rows(txn, &schema)? {
            let pk = row
                .get(&schema.columns[0].name)
                .cloned()
                .unwrap_or(Value::Null);
            self.insert_index_entry(txn, table, columns, &row, &pk)?;
        }
        Ok(true)
    }

    pub fn drop_index_in_txn(
        &self,
        txn: &mut Transaction,
        table: &str,
        columns: &[String],
    ) -> Result<bool> {
        let meta_key = encode_index_meta_key(table, columns);
        if txn.get(&meta_key)?.is_none() {
            return Ok(false);
        }

        txn.delete(&meta_key)?;
        let start = encode_index_entry_prefix(table, columns);
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
        columns: &[String],
        row: &Row,
        pk: &Value,
    ) -> Result<()> {
        let key = encode_index_entry_key(table, columns, &collect_index_values(row, columns), pk);
        txn.put(key, Vec::new())
    }

    pub fn delete_index_entry(
        &self,
        txn: &mut Transaction,
        table: &str,
        columns: &[String],
        row: &Row,
        pk: &Value,
    ) -> Result<()> {
        let key = encode_index_entry_key(table, columns, &collect_index_values(row, columns), pk);
        txn.delete(&key)
    }

    pub fn list_indexes(&self, txn: &Transaction, table: &str) -> Result<Vec<Vec<String>>> {
        let start = encode_index_meta_prefix(table);
        let end = prefix_end(&start);
        let mut indexes = Vec::new();
        for (_, value) in txn.scan(&start, &end)? {
            let definition: IndexDefinition = serde_json::from_slice(&value)?;
            indexes.push(definition.column_names);
        }
        indexes.sort();
        indexes.dedup();
        Ok(indexes)
    }

    pub fn find_best_index(
        &self,
        txn: &Transaction,
        table: &str,
        where_expr: Option<&WhereExpr>,
    ) -> Result<Option<IndexLookupPlan>> {
        let Some(where_expr) = where_expr else {
            return Ok(None);
        };

        let equality_map = collect_equality_map(where_expr);
        let mut best_plan = None;
        let mut best_score = 0usize;
        for columns in self.list_indexes(txn, table)? {
            let mut prefix_values = Vec::new();
            for column in &columns {
                let Some(value) = equality_map.get(column) else {
                    break;
                };
                prefix_values.push((*value).clone());
            }

            if prefix_values.is_empty() {
                continue;
            }

            let score = prefix_values.len();
            if score > best_score {
                best_score = score;
                best_plan = Some(IndexLookupPlan {
                    scan_columns: columns,
                    prefix_values,
                });
            }
        }

        Ok(best_plan)
    }

    pub fn lookup_prefix(
        &self,
        txn: &Transaction,
        table: &str,
        columns: &[String],
        prefix_values: &[Value],
    ) -> Result<Vec<Value>> {
        let start = encode_index_lookup_prefix(table, columns, prefix_values);
        let end = prefix_end(&start);
        let mut pks = Vec::new();
        for (key, _) in txn.scan(&start, &end)? {
            if let Some(pk) = decode_index_pk(&key) {
                pks.push(pk);
            }
        }
        Ok(pks)
    }

    fn scan_rows(&self, txn: &Transaction, schema: &TableSchema) -> Result<Vec<(Vec<u8>, Row)>> {
        let start = encode_row_prefix_start(&schema.table_name);
        let end = encode_row_prefix_end(&schema.table_name);
        let mut rows = Vec::new();
        for (key, value) in txn.scan(&start, &end)? {
            let Some((table_name, _)) = decode_row_key(&key) else {
                continue;
            };
            if table_name != schema.table_name {
                continue;
            }
            rows.push((key, serde_json::from_slice(&value)?));
        }
        Ok(rows)
    }
}

fn collect_index_values(row: &Row, columns: &[String]) -> Vec<Value> {
    columns
        .iter()
        .map(|column| row.get(column).cloned().unwrap_or(Value::Null))
        .collect()
}

fn collect_equality_map<'a>(where_expr: &'a WhereExpr) -> BTreeMap<String, &'a Value> {
    let mut map = BTreeMap::new();
    collect_equality_map_inner(where_expr, &mut map);
    map
}

fn collect_equality_map_inner<'a>(
    where_expr: &'a WhereExpr,
    map: &mut BTreeMap<String, &'a Value>,
) {
    match where_expr {
        WhereExpr::Comparison {
            column,
            operator: Operator::Eq,
            value,
        } => {
            map.insert(column.clone(), value);
        }
        WhereExpr::And(left, right) => {
            collect_equality_map_inner(left, map);
            collect_equality_map_inner(right, map);
        }
        _ => {}
    }
}

pub fn encode_index_meta_key(table: &str, columns: &[String]) -> Vec<u8> {
    format!("{}{}:{}", INDEX_META_PREFIX, table, columns.join(",")).into_bytes()
}

pub fn encode_index_meta_prefix(table: &str) -> Vec<u8> {
    format!("{}{}:", INDEX_META_PREFIX, table).into_bytes()
}

pub fn encode_index_entry_key(
    table: &str,
    columns: &[String],
    values: &[Value],
    pk: &Value,
) -> Vec<u8> {
    format!(
        "{}{}:{}:{}:{}",
        INDEX_ENTRY_PREFIX,
        table,
        columns.join(","),
        encode_value_components(values),
        encode_value_component(pk)
    )
    .into_bytes()
}

pub fn encode_index_entry_prefix(table: &str, columns: &[String]) -> Vec<u8> {
    format!("{}{}:{}:", INDEX_ENTRY_PREFIX, table, columns.join(",")).into_bytes()
}

pub fn encode_index_lookup_prefix(table: &str, columns: &[String], values: &[Value]) -> Vec<u8> {
    if values.is_empty() {
        return encode_index_entry_prefix(table, columns);
    }
    format!(
        "{}{}:{}:{}",
        INDEX_ENTRY_PREFIX,
        table,
        columns.join(","),
        encode_value_components(values)
    )
    .into_bytes()
}

fn encode_value_components(values: &[Value]) -> String {
    let mut out = values
        .iter()
        .map(encode_value_component)
        .collect::<Vec<_>>()
        .join("|");
    out.push('|');
    out
}

pub fn encode_value_component(value: &Value) -> String {
    let raw =
        serde_json::to_vec(value).unwrap_or_else(|_| primary_key_to_string(value).into_bytes());
    hex_encode(&raw)
}

fn decode_index_pk(key: &[u8]) -> Option<Value> {
    let key = std::str::from_utf8(key).ok()?;
    let (_, pk_hex) = key.rsplit_once(':')?;
    let bytes = hex_decode(pk_hex)?;
    serde_json::from_slice(&bytes).ok()
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
    for chunk in input.as_bytes().chunks(2) {
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

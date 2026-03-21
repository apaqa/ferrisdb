use std::collections::BTreeSet;
use std::sync::Arc;

use serde::{Deserialize, Serialize};

use crate::error::Result;
use crate::transaction::mvcc::{MvccEngine, Transaction};

use super::ast::Value;
use super::catalog::{Catalog, TableSchema};
use super::row::{decode_row_key, encode_row_prefix_end, encode_row_prefix_start, Row};

pub const STATS_TABLE_PREFIX: &str = "__stats:table:";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TableStatistics {
    pub table_name: String,
    pub row_count: usize,
    pub stale: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ColumnStatistics {
    pub table_name: String,
    pub column_name: String,
    pub ndv: usize,
    pub min: Option<Value>,
    pub max: Option<Value>,
}

#[derive(Debug, Clone)]
pub struct StatisticsManager {
    catalog: Catalog,
}

impl StatisticsManager {
    pub fn new(engine: Arc<MvccEngine>) -> Self {
        Self {
            catalog: Catalog::new(engine),
        }
    }

    pub fn analyze_table(&self, txn: &mut Transaction, table_name: &str) -> Result<Option<TableStatistics>> {
        let Some(schema) = self.catalog.get_table(txn, table_name)? else {
            return Ok(None);
        };
        let rows = scan_rows(txn, &schema)?;
        let table_stats = TableStatistics {
            table_name: table_name.to_string(),
            row_count: rows.len(),
            stale: false,
        };
        txn.put(encode_table_stats_key(table_name), serde_json::to_vec(&table_stats)?)?;

        for column in &schema.columns {
            let mut distinct = BTreeSet::new();
            let mut min = None;
            let mut max = None;
            for (_, row) in &rows {
                let value = row.get(&column.name).cloned().unwrap_or(Value::Null);
                distinct.insert(format!("{:?}", value));
                min = compare_min(min, Some(value.clone()));
                max = compare_max(max, Some(value));
            }
            let column_stats = ColumnStatistics {
                table_name: table_name.to_string(),
                column_name: column.name.clone(),
                ndv: distinct.len(),
                min,
                max,
            };
            txn.put(
                encode_column_stats_key(table_name, &column.name),
                serde_json::to_vec(&column_stats)?,
            )?;
        }

        Ok(Some(table_stats))
    }

    pub fn get_table_stats(&self, txn: &Transaction, table_name: &str) -> Result<Option<TableStatistics>> {
        let Some(raw) = txn.get(&encode_table_stats_key(table_name))? else {
            return Ok(None);
        };
        Ok(Some(serde_json::from_slice(&raw)?))
    }

    pub fn get_column_stats(
        &self,
        txn: &Transaction,
        table_name: &str,
        column_name: &str,
    ) -> Result<Option<ColumnStatistics>> {
        let Some(raw) = txn.get(&encode_column_stats_key(table_name, column_name))? else {
            return Ok(None);
        };
        Ok(Some(serde_json::from_slice(&raw)?))
    }

    pub fn mark_stale(&self, txn: &mut Transaction, table_name: &str) -> Result<()> {
        let stats = self
            .get_table_stats(txn, table_name)?
            .unwrap_or(TableStatistics {
                table_name: table_name.to_string(),
                row_count: 0,
                stale: true,
            });
        txn.put(
            encode_table_stats_key(table_name),
            serde_json::to_vec(&TableStatistics {
                stale: true,
                ..stats
            })?,
        )?;
        Ok(())
    }
}

pub fn encode_table_stats_key(table_name: &str) -> Vec<u8> {
    format!("{}{}:summary", STATS_TABLE_PREFIX, table_name).into_bytes()
}

pub fn encode_column_stats_key(table_name: &str, column_name: &str) -> Vec<u8> {
    format!("{}{}:column:{}", STATS_TABLE_PREFIX, table_name, column_name).into_bytes()
}

fn scan_rows(txn: &Transaction, schema: &TableSchema) -> Result<Vec<(Vec<u8>, Row)>> {
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

fn compare_min(current: Option<Value>, incoming: Option<Value>) -> Option<Value> {
    match (current, incoming) {
        (None, value) => value,
        (value, None) => value,
        (Some(left), Some(right)) => {
            if compare_values(&left, &right).is_gt() {
                Some(right)
            } else {
                Some(left)
            }
        }
    }
}

fn compare_max(current: Option<Value>, incoming: Option<Value>) -> Option<Value> {
    match (current, incoming) {
        (None, value) => value,
        (value, None) => value,
        (Some(left), Some(right)) => {
            if compare_values(&left, &right).is_lt() {
                Some(right)
            } else {
                Some(left)
            }
        }
    }
}

fn compare_values(left: &Value, right: &Value) -> std::cmp::Ordering {
    match (left, right) {
        (Value::Int(a), Value::Int(b)) => a.cmp(b),
        (Value::Text(a), Value::Text(b)) => a.cmp(b),
        (Value::Bool(a), Value::Bool(b)) => a.cmp(b),
        _ => std::cmp::Ordering::Equal,
    }
}

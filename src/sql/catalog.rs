// =============================================================================
// sql/catalog.rs -- SQL Catalog / Schema Metadata
// =============================================================================
//
// Catalog 是 SQL 層的系統目錄，負責管理 table schema。
// 在這個簡化版設計中，我們不另外做獨立的 metadata 檔案，而是直接把 schema
// 存在既有的 KV storage 裡，這樣 schema 也能沿用 MVCC / WAL / LSM 的持久化能力。
//
// 目前規則：
// - schema key: "__meta:table:{table_name}"
// - schema value: JSON 序列化後的 TableSchema
//
// 這種做法很適合第一版 SQL 引擎：
// - 優點是簡單、容易除錯，而且 schema 會跟資料一起持久化
// - 缺點是之後若要支援大量 metadata、索引、權限等，需要更完整的 system catalog

use std::sync::Arc;

use serde::{Deserialize, Serialize};

use crate::error::Result;
use crate::transaction::mvcc::{MvccEngine, Transaction};

use super::ast::ColumnDef;

pub const TABLE_META_PREFIX: &str = "__meta:table:";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TableSchema {
    pub table_name: String,
    pub columns: Vec<ColumnDef>,
}

#[derive(Debug, Clone)]
pub struct Catalog {
    engine: Arc<MvccEngine>,
}

impl Catalog {
    pub fn new(engine: Arc<MvccEngine>) -> Self {
        Self { engine }
    }

    pub fn create_table(&self, txn: &mut Transaction, schema: &TableSchema) -> Result<bool> {
        let key = encode_schema_key(&schema.table_name);
        if txn.get(&key)?.is_some() {
            return Ok(false);
        }

        let value = serde_json::to_vec(schema)?;
        txn.put(key, value)?;
        Ok(true)
    }

    pub fn get_table(&self, txn: &Transaction, table_name: &str) -> Result<Option<TableSchema>> {
        let key = encode_schema_key(table_name);
        let Some(raw) = txn.get(&key)? else {
            return Ok(None);
        };
        Ok(Some(serde_json::from_slice(&raw)?))
    }

    pub fn list_tables(&self, txn: &Transaction) -> Result<Vec<TableSchema>> {
        let start = TABLE_META_PREFIX.as_bytes().to_vec();
        let mut end = TABLE_META_PREFIX.as_bytes().to_vec();
        end.push(0xFF);

        let mut tables = Vec::new();
        for (_, value) in txn.scan(&start, &end)? {
            tables.push(serde_json::from_slice(&value)?);
        }
        Ok(tables)
    }

    pub fn drop_table(&self, txn: &mut Transaction, table_name: &str) -> Result<bool> {
        let key = encode_schema_key(table_name);
        if txn.get(&key)?.is_none() {
            return Ok(false);
        }

        txn.delete(&key)?;
        Ok(true)
    }

    pub fn engine(&self) -> &Arc<MvccEngine> {
        &self.engine
    }
}

pub fn encode_schema_key(table_name: &str) -> Vec<u8> {
    format!("{}{}", TABLE_META_PREFIX, table_name).into_bytes()
}

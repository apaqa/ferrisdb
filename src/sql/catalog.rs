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

use super::ast::{CheckConstraint, ColumnDef, ForeignKey};

pub const TABLE_META_PREFIX: &str = "__meta:table:";
pub const VIEW_META_PREFIX: &str = "__meta:view:";
pub const MATERIALIZED_VIEW_META_PREFIX: &str = "__meta:matview:";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TableSchema {
    pub table_name: String,
    pub columns: Vec<ColumnDef>,
    // 中文註解：foreign_keys 會在 INSERT / UPDATE / DELETE 時供 executor 做參照完整性檢查。
    pub foreign_keys: Vec<ForeignKey>,
    // 中文註解：check_constraints 會在 INSERT / UPDATE 時驗證資料列是否符合條件。
    pub check_constraints: Vec<CheckConstraint>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ViewDefinition {
    pub view_name: String,
    pub query_sql: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MaterializedViewDefinition {
    pub view_name: String,
    pub query_sql: String,
    pub schema: TableSchema,
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

    // 中文註解：View 定義會和 table schema 一樣放在系統 metadata 區，方便重啟後直接讀回。
    pub fn create_view(&self, txn: &mut Transaction, view: &ViewDefinition) -> Result<bool> {
        let key = encode_view_key(&view.view_name);
        if txn.get(&key)?.is_some() {
            return Ok(false);
        }

        txn.put(key, serde_json::to_vec(view)?)?;
        Ok(true)
    }

    pub fn get_view(&self, txn: &Transaction, view_name: &str) -> Result<Option<ViewDefinition>> {
        let key = encode_view_key(view_name);
        let Some(raw) = txn.get(&key)? else {
            return Ok(None);
        };
        Ok(Some(serde_json::from_slice(&raw)?))
    }

    pub fn drop_view(&self, txn: &mut Transaction, view_name: &str) -> Result<bool> {
        let key = encode_view_key(view_name);
        if txn.get(&key)?.is_none() {
            return Ok(false);
        }

        txn.delete(&key)?;
        Ok(true)
    }

    pub fn create_materialized_view(
        &self,
        txn: &mut Transaction,
        view: &MaterializedViewDefinition,
    ) -> Result<bool> {
        let key = encode_materialized_view_key(&view.view_name);
        if txn.get(&key)?.is_some() {
            return Ok(false);
        }

        txn.put(key, serde_json::to_vec(view)?)?;
        Ok(true)
    }

    pub fn get_materialized_view(
        &self,
        txn: &Transaction,
        view_name: &str,
    ) -> Result<Option<MaterializedViewDefinition>> {
        let key = encode_materialized_view_key(view_name);
        let Some(raw) = txn.get(&key)? else {
            return Ok(None);
        };
        Ok(Some(serde_json::from_slice(&raw)?))
    }

    pub fn put_materialized_view(
        &self,
        txn: &mut Transaction,
        view: &MaterializedViewDefinition,
    ) -> Result<()> {
        txn.put(
            encode_materialized_view_key(&view.view_name),
            serde_json::to_vec(view)?,
        )?;
        Ok(())
    }

    pub fn drop_materialized_view(&self, txn: &mut Transaction, view_name: &str) -> Result<bool> {
        let key = encode_materialized_view_key(view_name);
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

pub fn encode_view_key(view_name: &str) -> Vec<u8> {
    format!("{}{}", VIEW_META_PREFIX, view_name).into_bytes()
}

pub fn encode_materialized_view_key(view_name: &str) -> Vec<u8> {
    format!("{}{}", MATERIALIZED_VIEW_META_PREFIX, view_name).into_bytes()
}

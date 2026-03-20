// =============================================================================
// sql/row.rs -- SQL Row Encoding
// =============================================================================
//
// SQL Executor 需要把一列 row 落到底層 KV store。
// 這一版的設計非常直接：
// - row key: "__row:{table_name}:{primary_key}"
// - row value: JSON 序列化的 Row
//
// 這裡的 primary key 先簡化為「第一個 column 的值」。
// 因此只要知道 table name 和第一欄的值，我們就能定位一筆 row。
//
// decode_row_key 的用途主要有兩個：
// 1. 做 table prefix scan 之後，確認這筆 key 確實屬於某個 table
// 2. 在 debug / 測試時把 row key 拆回可讀的 table / primary key 字串

use serde::{Deserialize, Serialize};

use super::ast::Value;

pub const ROW_KEY_PREFIX: &str = "__row:";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Row {
    pub columns: Vec<(String, Value)>,
}

impl Row {
    pub fn new(columns: Vec<(String, Value)>) -> Self {
        Self { columns }
    }

    pub fn get(&self, column: &str) -> Option<&Value> {
        self.columns
            .iter()
            .find(|(name, _)| name == column)
            .map(|(_, value)| value)
    }

    pub fn set(&mut self, column: &str, value: Value) -> bool {
        if let Some((_, current)) = self.columns.iter_mut().find(|(name, _)| name == column) {
            *current = value;
            true
        } else {
            false
        }
    }

    pub fn push(&mut self, column: String, value: Value) {
        self.columns.push((column, value));
    }

    pub fn remove(&mut self, column: &str) -> Option<Value> {
        let index = self.columns.iter().position(|(name, _)| name == column)?;
        Some(self.columns.remove(index).1)
    }
}

pub fn encode_row_key(table_name: &str, pk_value: &Value) -> Vec<u8> {
    format!(
        "{}{}:{}",
        ROW_KEY_PREFIX,
        table_name,
        primary_key_to_string(pk_value)
    )
    .into_bytes()
}

pub fn decode_row_key(key: &[u8]) -> Option<(String, String)> {
    let key = std::str::from_utf8(key).ok()?;
    let rest = key.strip_prefix(ROW_KEY_PREFIX)?;
    let (table_name, pk_value) = rest.split_once(':')?;
    Some((table_name.to_string(), pk_value.to_string()))
}

pub fn encode_row_prefix_start(table_name: &str) -> Vec<u8> {
    format!("{}{}:", ROW_KEY_PREFIX, table_name).into_bytes()
}

pub fn encode_row_prefix_end(table_name: &str) -> Vec<u8> {
    let mut end = encode_row_prefix_start(table_name);
    end.push(0xFF);
    end
}

pub fn primary_key_to_string(value: &Value) -> String {
    match value {
        Value::Int(v) => v.to_string(),
        Value::Text(v) => v.clone(),
        Value::Bool(v) => v.to_string(),
        Value::Null => "null".to_string(),
    }
}

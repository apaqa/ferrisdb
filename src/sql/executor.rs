// =============================================================================
// sql/executor.rs -- SQL Executor
// =============================================================================
//
// Parser 的工作是把 SQL 字串轉成 AST；Executor 的工作則是把 AST 轉成真正的資料操作。
//
// 這一版的 SQL 執行流程：
// 1. CREATE TABLE
//    - schema 存進 "__meta:table:{table_name}"
//
// 2. INSERT / UPDATE / DELETE
//    - 透過 MVCC transaction 做 auto-commit
//    - row 存在 "__row:{table_name}:{primary_key}"
//    - row value 為 JSON
//
// 3. SELECT
//    - 先掃描該 table 的所有 row
//    - 再依 WHERE 條件過濾
//    - 最後投影成使用者要求的欄位
//
// 因為這是第一版 SQL 引擎，所以刻意保持簡單：
// - 不做 query optimizer
// - 不做 secondary index
// - 直接用 prefix scan + row filter
//
// 之後若要繼續演進，可以把 WHERE 下推、索引查找、型別檢查等能力逐步補上。

use std::sync::Arc;

use crate::error::Result;
use crate::transaction::mvcc::{MvccEngine, Transaction};

use super::ast::{
    Assignment, Operator, SelectColumns, Statement, Value, WhereClause,
};
use super::catalog::{Catalog, TableSchema};
use super::row::{
    decode_row_key, encode_row_key, encode_row_prefix_end, encode_row_prefix_start, Row,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ExecuteResult {
    Created { table_name: String },
    Inserted { count: usize },
    Selected { columns: Vec<String>, rows: Vec<Vec<Value>> },
    Updated { count: usize },
    Deleted { count: usize },
    Error { message: String },
}

#[derive(Debug, Clone)]
pub struct SqlExecutor {
    engine: Arc<MvccEngine>,
    catalog: Catalog,
}

impl SqlExecutor {
    pub fn new(engine: Arc<MvccEngine>) -> Self {
        let catalog = Catalog::new(Arc::clone(&engine));
        Self { engine, catalog }
    }

    pub fn execute(&self, stmt: Statement) -> Result<ExecuteResult> {
        match stmt {
            Statement::CreateTable {
                table_name,
                columns,
            } => self.execute_create_table(table_name, columns),
            Statement::Insert { table_name, values } => self.execute_insert(table_name, values),
            Statement::Select {
                table_name,
                columns,
                where_clause,
            } => self.execute_select(table_name, columns, where_clause),
            Statement::Update {
                table_name,
                assignments,
                where_clause,
            } => self.execute_update(table_name, assignments, where_clause),
            Statement::Delete {
                table_name,
                where_clause,
            } => self.execute_delete(table_name, where_clause),
        }
    }

    fn execute_create_table(
        &self,
        table_name: String,
        columns: Vec<super::ast::ColumnDef>,
    ) -> Result<ExecuteResult> {
        if columns.is_empty() {
            return Ok(ExecuteResult::Error {
                message: "CREATE TABLE requires at least one column".to_string(),
            });
        }

        let mut txn = self.engine.begin_transaction();
        let schema = TableSchema {
            table_name: table_name.clone(),
            columns,
        };

        if !self.catalog.create_table(&mut txn, &schema)? {
            return Ok(ExecuteResult::Error {
                message: format!("table '{}' already exists", table_name),
            });
        }

        txn.commit()?;
        Ok(ExecuteResult::Created { table_name })
    }

    fn execute_insert(&self, table_name: String, values: Vec<Vec<Value>>) -> Result<ExecuteResult> {
        let mut txn = self.engine.begin_transaction();
        let Some(schema) = self.catalog.get_table(&txn, &table_name)? else {
            return Ok(ExecuteResult::Error {
                message: format!("table '{}' does not exist", table_name),
            });
        };

        let mut inserted = 0;
        for row_values in values {
            if row_values.len() != schema.columns.len() {
                return Ok(ExecuteResult::Error {
                    message: format!(
                        "INSERT expected {} values for table '{}', got {}",
                        schema.columns.len(),
                        table_name,
                        row_values.len()
                    ),
                });
            }

            let Some(pk_value) = row_values.first().cloned() else {
                return Ok(ExecuteResult::Error {
                    message: "INSERT row cannot be empty".to_string(),
                });
            };
            if matches!(pk_value, Value::Null) {
                return Ok(ExecuteResult::Error {
                    message: "primary key cannot be NULL".to_string(),
                });
            }

            let row = Row::new(
                schema
                    .columns
                    .iter()
                    .map(|column| column.name.clone())
                    .zip(row_values.into_iter())
                    .collect(),
            );
            let key = encode_row_key(&table_name, &pk_value);
            let value = serde_json::to_vec(&row)?;
            txn.put(key, value)?;
            inserted += 1;
        }

        txn.commit()?;
        Ok(ExecuteResult::Inserted { count: inserted })
    }

    fn execute_select(
        &self,
        table_name: String,
        columns: SelectColumns,
        where_clause: Option<WhereClause>,
    ) -> Result<ExecuteResult> {
        let txn = self.engine.begin_transaction();
        let Some(schema) = self.catalog.get_table(&txn, &table_name)? else {
            return Ok(ExecuteResult::Error {
                message: format!("table '{}' does not exist", table_name),
            });
        };

        let projection = match self.resolve_projection(&schema, &columns) {
            Ok(columns) => columns,
            Err(message) => return Ok(ExecuteResult::Error { message }),
        };

        let rows = self.scan_rows(&txn, &schema)?;
        let selected = rows
            .into_iter()
            .filter(|(_, row)| matches_where(row, where_clause.as_ref()))
            .map(|(_, row)| {
                projection
                    .iter()
                    .map(|column| row.get(column).cloned().unwrap_or(Value::Null))
                    .collect::<Vec<_>>()
            })
            .collect();

        Ok(ExecuteResult::Selected {
            columns: projection,
            rows: selected,
        })
    }

    fn execute_update(
        &self,
        table_name: String,
        assignments: Vec<Assignment>,
        where_clause: Option<WhereClause>,
    ) -> Result<ExecuteResult> {
        let mut txn = self.engine.begin_transaction();
        let Some(schema) = self.catalog.get_table(&txn, &table_name)? else {
            return Ok(ExecuteResult::Error {
                message: format!("table '{}' does not exist", table_name),
            });
        };

        if assignments
            .iter()
            .any(|assignment| assignment.column == schema.columns[0].name)
        {
            return Ok(ExecuteResult::Error {
                message: "updating the primary key column is not supported".to_string(),
            });
        }

        for assignment in &assignments {
            if !schema.columns.iter().any(|column| column.name == assignment.column) {
                return Ok(ExecuteResult::Error {
                    message: format!("unknown column '{}'", assignment.column),
                });
            }
        }

        let rows = self.scan_rows(&txn, &schema)?;
        let mut updated = 0;
        for (row_key, mut row) in rows {
            if !matches_where(&row, where_clause.as_ref()) {
                continue;
            }

            for assignment in &assignments {
                row.set(&assignment.column, assignment.value.clone());
            }

            txn.put(row_key, serde_json::to_vec(&row)?)?;
            updated += 1;
        }

        txn.commit()?;
        Ok(ExecuteResult::Updated { count: updated })
    }

    fn execute_delete(
        &self,
        table_name: String,
        where_clause: Option<WhereClause>,
    ) -> Result<ExecuteResult> {
        let mut txn = self.engine.begin_transaction();
        let Some(schema) = self.catalog.get_table(&txn, &table_name)? else {
            return Ok(ExecuteResult::Error {
                message: format!("table '{}' does not exist", table_name),
            });
        };

        let rows = self.scan_rows(&txn, &schema)?;
        let mut deleted = 0;
        for (row_key, row) in rows {
            if !matches_where(&row, where_clause.as_ref()) {
                continue;
            }
            txn.delete(&row_key)?;
            deleted += 1;
        }

        txn.commit()?;
        Ok(ExecuteResult::Deleted { count: deleted })
    }

    fn resolve_projection(
        &self,
        schema: &TableSchema,
        columns: &SelectColumns,
    ) -> std::result::Result<Vec<String>, String> {
        match columns {
            SelectColumns::All => Ok(schema.columns.iter().map(|column| column.name.clone()).collect()),
            SelectColumns::Named(names) => {
                for name in names {
                    if !schema.columns.iter().any(|column| &column.name == name) {
                        return Err(format!("unknown column '{}'", name));
                    }
                }
                Ok(names.clone())
            }
        }
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
}

pub fn format_execute_result(result: &ExecuteResult) -> String {
    match result {
        ExecuteResult::Created { table_name } => format!("Table '{}' created", table_name),
        ExecuteResult::Inserted { count } => format!("Inserted {} row(s)", count),
        ExecuteResult::Selected { columns, rows } => format_selected(columns, rows),
        ExecuteResult::Updated { count } => format!("Updated {} row(s)", count),
        ExecuteResult::Deleted { count } => format!("Deleted {} row(s)", count),
        ExecuteResult::Error { message } => format!("Error: {}", message),
    }
}

fn format_selected(columns: &[String], rows: &[Vec<Value>]) -> String {
    if columns.is_empty() {
        return "(no columns)".to_string();
    }

    let rendered_rows: Vec<Vec<String>> = rows
        .iter()
        .map(|row| row.iter().map(render_value).collect())
        .collect();

    let mut widths: Vec<usize> = columns.iter().map(|column| column.len()).collect();
    for row in &rendered_rows {
        for (idx, value) in row.iter().enumerate() {
            widths[idx] = widths[idx].max(value.len());
        }
    }

    let header = columns
        .iter()
        .enumerate()
        .map(|(idx, column)| format!("{:width$}", column, width = widths[idx]))
        .collect::<Vec<_>>()
        .join(" | ");
    let separator = widths
        .iter()
        .map(|width| "-".repeat(*width))
        .collect::<Vec<_>>()
        .join("-+-");

    if rendered_rows.is_empty() {
        return format!("{}\n{}\n(0 rows)", header, separator);
    }

    let body = rendered_rows
        .iter()
        .map(|row| {
            row.iter()
                .enumerate()
                .map(|(idx, value)| format!("{:width$}", value, width = widths[idx]))
                .collect::<Vec<_>>()
                .join(" | ")
        })
        .collect::<Vec<_>>()
        .join("\n");

    format!("{}\n{}\n{}\n({} rows)", header, separator, body, rows.len())
}

fn matches_where(row: &Row, where_clause: Option<&WhereClause>) -> bool {
    let Some(where_clause) = where_clause else {
        return true;
    };

    let Some(left) = row.get(&where_clause.column) else {
        return false;
    };

    compare_values(left, &where_clause.value, where_clause.operator.clone())
}

fn compare_values(left: &Value, right: &Value, operator: Operator) -> bool {
    match operator {
        Operator::Eq => left == right,
        Operator::Ne => left != right,
        Operator::Lt => compare_order(left, right).is_some_and(|ord| ord.is_lt()),
        Operator::Gt => compare_order(left, right).is_some_and(|ord| ord.is_gt()),
        Operator::Le => compare_order(left, right).is_some_and(|ord| ord.is_le()),
        Operator::Ge => compare_order(left, right).is_some_and(|ord| ord.is_ge()),
    }
}

fn compare_order(left: &Value, right: &Value) -> Option<std::cmp::Ordering> {
    match (left, right) {
        (Value::Int(a), Value::Int(b)) => Some(a.cmp(b)),
        (Value::Text(a), Value::Text(b)) => Some(a.cmp(b)),
        (Value::Bool(a), Value::Bool(b)) => Some(a.cmp(b)),
        _ => None,
    }
}

fn render_value(value: &Value) -> String {
    match value {
        Value::Int(v) => v.to_string(),
        Value::Text(v) => v.clone(),
        Value::Bool(v) => v.to_string(),
        Value::Null => "NULL".to_string(),
    }
}

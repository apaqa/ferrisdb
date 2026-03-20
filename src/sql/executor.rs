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

use std::collections::BTreeMap;
use std::sync::Arc;

use crate::error::Result;
use crate::transaction::mvcc::{MvccEngine, Transaction};

use super::ast::{
    AggregateFunc, Assignment, GroupByClause, JoinClause, Operator, OrderByClause,
    OrderDirection, SelectColumns, SelectItem, Statement, Value, WhereClause,
};
use super::catalog::{Catalog, TableSchema};
use super::row::{
    decode_row_key, encode_row_key, encode_row_prefix_end, encode_row_prefix_start, Row,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ExecuteResult {
    Explain { plan: String },
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
            Statement::Explain { statement } => self.execute_explain(*statement),
            Statement::CreateTable {
                table_name,
                columns,
            } => self.execute_create_table(table_name, columns),
            Statement::Insert { table_name, values } => self.execute_insert(table_name, values),
            Statement::Select {
                table_name,
                columns,
                join,
                where_clause,
                group_by,
                order_by,
                limit,
            } => self.execute_select(
                table_name,
                columns,
                join,
                where_clause,
                group_by,
                order_by,
                limit,
            ),
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

    fn execute_explain(&self, statement: Statement) -> Result<ExecuteResult> {
        match statement {
            Statement::Select {
                table_name,
                columns,
                join,
                where_clause,
                group_by,
                order_by,
                limit,
            } => {
                let txn = self.engine.begin_transaction();
                let Some(schema) = self.catalog.get_table(&txn, &table_name)? else {
                    return Ok(ExecuteResult::Error {
                        message: format!("table '{}' does not exist", table_name),
                    });
                };

                if join.is_some() {
                    return Ok(ExecuteResult::Error {
                        message: "EXPLAIN for JOIN is not implemented yet".to_string(),
                    });
                }

                let total_rows = self.scan_rows(&txn, &schema)?.len();
                let scan_cost = total_rows as f64;
                let has_filter = where_clause.is_some();
                let filtered_rows = estimate_filtered_rows(total_rows, where_clause.as_ref());
                let filter_cost = if has_filter {
                    (total_rows as f64) * 0.25
                } else {
                    0.0
                };
                let project_cost = estimate_project_cost(filtered_rows, &columns);

                let mut lines = Vec::new();
                lines.push(format!(
                    "SeqScan(table={}, rows={}, cost={:.2})",
                    table_name, total_rows, scan_cost
                ));
                if let Some(where_clause) = where_clause {
                    lines.push(format!(
                        "  -> Filter(predicate=\"{} {} {}\", rows={}, cost={:.2})",
                        where_clause.column,
                        operator_to_str(&where_clause.operator),
                        render_value(&where_clause.value),
                        filtered_rows,
                        filter_cost
                    ));
                }
                if let Some(group_by) = group_by {
                    lines.push(format!(
                        "  -> GroupBy(column={}, rows={}, cost={:.2})",
                        group_by.column,
                        filtered_rows.max(1),
                        (filtered_rows.max(1)) as f64 * 0.35
                    ));
                }
                if let Some(order_by) = order_by {
                    lines.push(format!(
                        "  -> OrderBy(column={}, direction={}, rows={}, cost={:.2})",
                        order_by.column,
                        order_direction_to_str(&order_by.direction),
                        if has_filter { filtered_rows } else { total_rows },
                        (if has_filter { filtered_rows } else { total_rows }) as f64 * 0.2
                    ));
                }
                if let Some(limit) = limit {
                    lines.push(format!(
                        "  -> Limit(limit={}, rows={}, cost={:.2})",
                        limit,
                        limit.min(if has_filter { filtered_rows } else { total_rows }),
                        0.05
                    ));
                }

                let project_rows = if has_filter {
                    filtered_rows
                } else {
                    total_rows
                };
                let project_rows = limit.map(|value| value.min(project_rows)).unwrap_or(project_rows);
                let project_desc = match columns {
                    SelectColumns::All => "*".to_string(),
                    SelectColumns::Named(names) => names.join(", "),
                    SelectColumns::Aggregate(items) => items
                        .iter()
                        .map(render_select_item)
                        .collect::<Vec<_>>()
                        .join(", "),
                };
                lines.push(format!(
                    "  -> Project(columns=[{}], rows={}, cost={:.2})",
                    project_desc, project_rows, project_cost
                ));

                Ok(ExecuteResult::Explain {
                    plan: lines.join("\n"),
                })
            }
            other => Ok(ExecuteResult::Error {
                message: format!("EXPLAIN does not support {:?}", other),
            }),
        }
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
        join: Option<JoinClause>,
        where_clause: Option<WhereClause>,
        group_by: Option<GroupByClause>,
        order_by: Option<OrderByClause>,
        limit: Option<usize>,
    ) -> Result<ExecuteResult> {
        let txn = self.engine.begin_transaction();
        let Some(schema) = self.catalog.get_table(&txn, &table_name)? else {
            return Ok(ExecuteResult::Error {
                message: format!("table '{}' does not exist", table_name),
            });
        };

        if let Some(join_clause) = join {
            if matches!(columns, SelectColumns::Aggregate(_)) || group_by.is_some() {
                return Ok(ExecuteResult::Error {
                    message: "JOIN with aggregate queries is not implemented yet".to_string(),
                });
            }
            return self.execute_join_select(
                &txn,
                &schema,
                columns,
                join_clause,
                where_clause,
                order_by,
                limit,
            );
        }

        if matches!(columns, SelectColumns::Aggregate(_)) || group_by.is_some() {
            return self.execute_aggregate_select(
                &txn,
                &schema,
                columns,
                where_clause,
                group_by,
                order_by,
                limit,
            );
        }

        let projection = match self.resolve_projection(&schema, &columns) {
            Ok(columns) => columns,
            Err(message) => return Ok(ExecuteResult::Error { message }),
        };

        let mut rows: Vec<Row> = self
            .scan_rows(&txn, &schema)?
            .into_iter()
            .filter(|(_, row)| matches_where(row, where_clause.as_ref()))
            .map(|(_, row)| row)
            .collect();
        if let Some(order_by) = order_by.as_ref() {
            if let Err(message) = sort_rows_by_order(&mut rows, order_by) {
                return Ok(ExecuteResult::Error { message });
            }
        }
        if let Some(limit) = limit {
            rows.truncate(limit);
        }
        let selected = rows
            .into_iter()
            .map(|row| {
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

    fn execute_aggregate_select(
        &self,
        txn: &Transaction,
        schema: &TableSchema,
        columns: SelectColumns,
        where_clause: Option<WhereClause>,
        group_by: Option<GroupByClause>,
        order_by: Option<OrderByClause>,
        limit: Option<usize>,
    ) -> Result<ExecuteResult> {
        let SelectColumns::Aggregate(items) = columns else {
            return Ok(ExecuteResult::Error {
                message: "GROUP BY requires aggregate expressions in SELECT".to_string(),
            });
        };

        let output_columns = match self.resolve_aggregate_projection(schema, &items, group_by.as_ref()) {
            Ok(columns) => columns,
            Err(message) => return Ok(ExecuteResult::Error { message }),
        };

        let rows: Vec<Row> = self
            .scan_rows(txn, schema)?
            .into_iter()
            .filter(|(_, row)| matches_where(row, where_clause.as_ref()))
            .map(|(_, row)| row)
            .collect();

        let grouped_rows = group_rows(rows, group_by.as_ref());
        let mut selected = Vec::new();
        for (_group_key, group_rows) in grouped_rows {
            let mut output_row = Vec::with_capacity(items.len());
            for item in &items {
                match evaluate_select_item(item, &group_rows) {
                    Ok(value) => output_row.push(value),
                    Err(message) => return Ok(ExecuteResult::Error { message }),
                }
            }
            selected.push(output_row);
        }

        if let Some(order_by) = order_by.as_ref() {
            if let Err(message) = sort_result_rows_by_order(&mut selected, &output_columns, order_by) {
                return Ok(ExecuteResult::Error { message });
            }
        }
        if let Some(limit) = limit {
            selected.truncate(limit);
        }

        Ok(ExecuteResult::Selected {
            columns: output_columns,
            rows: selected,
        })
    }

    fn execute_join_select(
        &self,
        txn: &Transaction,
        left_schema: &TableSchema,
        columns: SelectColumns,
        join: JoinClause,
        where_clause: Option<WhereClause>,
        order_by: Option<OrderByClause>,
        limit: Option<usize>,
    ) -> Result<ExecuteResult> {
        let Some(right_schema) = self.catalog.get_table(txn, &join.right_table)? else {
            return Ok(ExecuteResult::Error {
                message: format!("table '{}' does not exist", join.right_table),
            });
        };

        let left_rows = self.scan_rows(txn, left_schema)?;
        let right_rows = self.scan_rows(txn, &right_schema)?;
        let projection = match self.resolve_join_projection(left_schema, &right_schema, &columns) {
            Ok(columns) => columns,
            Err(message) => return Ok(ExecuteResult::Error { message }),
        };

        let mut matched_rows = Vec::new();
        for (_, left_row) in &left_rows {
            let Some(left_value) = resolve_join_value(left_row, &left_schema.table_name, &join.left_column) else {
                return Ok(ExecuteResult::Error {
                    message: format!("unknown join column '{}'", join.left_column),
                });
            };

            for (_, right_row) in &right_rows {
                let Some(right_value) =
                    resolve_join_value(right_row, &right_schema.table_name, &join.right_column)
                else {
                    return Ok(ExecuteResult::Error {
                        message: format!("unknown join column '{}'", join.right_column),
                    });
                };

                if left_value != right_value {
                    continue;
                }

                let joined = JoinedRow::new(left_schema, left_row, &right_schema, right_row);
                if !matches_join_where(&joined, where_clause.as_ref()) {
                    continue;
                }
                matched_rows.push(joined);
            }
        }
        if let Some(order_by) = order_by.as_ref() {
            if let Err(message) = sort_joined_rows_by_order(&mut matched_rows, order_by) {
                return Ok(ExecuteResult::Error { message });
            }
        }
        if let Some(limit) = limit {
            matched_rows.truncate(limit);
        }
        let mut selected = Vec::with_capacity(matched_rows.len());
        for joined in matched_rows {
            let mut row = Vec::with_capacity(projection.len());
            for column in &projection {
                let Some(value) = joined.get(column) else {
                    return Ok(ExecuteResult::Error {
                        message: format!("unknown column '{}'", column),
                    });
                };
                row.push(value.clone());
            }
            selected.push(row);
        }

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
            SelectColumns::Aggregate(_) => Err("aggregate columns require aggregate execution path".to_string()),
        }
    }

    fn resolve_aggregate_projection(
        &self,
        schema: &TableSchema,
        items: &[SelectItem],
        group_by: Option<&GroupByClause>,
    ) -> std::result::Result<Vec<String>, String> {
        let mut columns = Vec::with_capacity(items.len());
        for item in items {
            match item {
                SelectItem::Column(name) => {
                    if !schema.columns.iter().any(|column| &column.name == name) {
                        return Err(format!("unknown column '{}'", name));
                    }
                    if group_by.as_ref().map(|group| &group.column) != Some(name) {
                        return Err(format!(
                            "column '{}' must appear in GROUP BY when aggregate functions are used",
                            name
                        ));
                    }
                    columns.push(name.clone());
                }
                SelectItem::Aggregate { func, column } => {
                    if let Some(column_name) = column {
                        if !schema.columns.iter().any(|col| &col.name == column_name) {
                            return Err(format!("unknown column '{}'", column_name));
                        }
                    }
                    columns.push(render_select_item(item));
                    if matches!(func, AggregateFunc::Count) {
                        continue;
                    }
                }
            }
        }
        Ok(columns)
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

    fn resolve_join_projection(
        &self,
        left_schema: &TableSchema,
        right_schema: &TableSchema,
        columns: &SelectColumns,
    ) -> std::result::Result<Vec<String>, String> {
        let all_columns = join_output_columns(left_schema, right_schema);
        match columns {
            SelectColumns::All => Ok(all_columns),
            SelectColumns::Named(names) => {
                for name in names {
                    if !all_columns.iter().any(|column| column == name) {
                        let left_matches = left_schema
                            .columns
                            .iter()
                            .filter(|column| column.name == *name)
                            .count();
                        let right_matches = right_schema
                            .columns
                            .iter()
                            .filter(|column| column.name == *name)
                            .count();
                        if left_matches + right_matches != 1 {
                            return Err(format!("unknown or ambiguous column '{}'", name));
                        }
                    }
                }
                Ok(names.clone())
            }
            SelectColumns::Aggregate(_) => {
                Err("aggregate projection is not supported for JOIN queries".to_string())
            }
        }
    }
}

pub fn format_execute_result(result: &ExecuteResult) -> String {
    match result {
        ExecuteResult::Explain { plan } => plan.clone(),
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

fn estimate_filtered_rows(total_rows: usize, where_clause: Option<&WhereClause>) -> usize {
    if where_clause.is_none() {
        return total_rows;
    }

    if total_rows == 0 {
        return 0;
    }

    total_rows.div_ceil(4)
}

fn estimate_project_cost(rows: usize, columns: &SelectColumns) -> f64 {
    let column_factor = match columns {
        SelectColumns::All => 1.0,
        SelectColumns::Named(names) => (names.len().max(1)) as f64 / 4.0,
        SelectColumns::Aggregate(items) => (items.len().max(1)) as f64 / 3.0,
    };
    (rows as f64) * 0.1 * column_factor.max(0.25)
}

fn operator_to_str(operator: &Operator) -> &'static str {
    match operator {
        Operator::Eq => "=",
        Operator::Ne => "!=",
        Operator::Lt => "<",
        Operator::Gt => ">",
        Operator::Le => "<=",
        Operator::Ge => ">=",
    }
}

fn order_direction_to_str(direction: &OrderDirection) -> &'static str {
    match direction {
        OrderDirection::Asc => "ASC",
        OrderDirection::Desc => "DESC",
    }
}

fn render_select_item(item: &SelectItem) -> String {
    match item {
        SelectItem::Column(name) => name.clone(),
        SelectItem::Aggregate { func, column } => match (func, column.as_deref()) {
            (AggregateFunc::Count, None) => "COUNT(*)".to_string(),
            (AggregateFunc::Count, Some(column)) => format!("COUNT({})", column),
            (AggregateFunc::Sum, Some(column)) => format!("SUM({})", column),
            (AggregateFunc::Min, Some(column)) => format!("MIN({})", column),
            (AggregateFunc::Max, Some(column)) => format!("MAX({})", column),
            (_, None) => "INVALID_AGGREGATE".to_string(),
        },
    }
}

fn sort_rows_by_order(rows: &mut [Row], order_by: &OrderByClause) -> std::result::Result<(), String> {
    if !rows.is_empty() && rows[0].get(&order_by.column).is_none() {
        return Err(format!("unknown column '{}'", order_by.column));
    }

    rows.sort_by(|left, right| {
        let left_value = left.get(&order_by.column).unwrap_or(&Value::Null);
        let right_value = right.get(&order_by.column).unwrap_or(&Value::Null);
        let order = compare_order(left_value, right_value).unwrap_or(std::cmp::Ordering::Equal);
        match order_by.direction {
            OrderDirection::Asc => order,
            OrderDirection::Desc => order.reverse(),
        }
    });
    Ok(())
}

fn sort_result_rows_by_order(
    rows: &mut [Vec<Value>],
    columns: &[String],
    order_by: &OrderByClause,
) -> std::result::Result<(), String> {
    let Some(index) = columns.iter().position(|column| column == &order_by.column) else {
        return Err(format!("unknown column '{}'", order_by.column));
    };

    rows.sort_by(|left, right| {
        let left_value = left.get(index).unwrap_or(&Value::Null);
        let right_value = right.get(index).unwrap_or(&Value::Null);
        let order = compare_order(left_value, right_value).unwrap_or(std::cmp::Ordering::Equal);
        match order_by.direction {
            OrderDirection::Asc => order,
            OrderDirection::Desc => order.reverse(),
        }
    });
    Ok(())
}

fn sort_joined_rows_by_order(
    rows: &mut [JoinedRow],
    order_by: &OrderByClause,
) -> std::result::Result<(), String> {
    if !rows.is_empty() && rows[0].get(&order_by.column).is_none() {
        return Err(format!("unknown or ambiguous column '{}'", order_by.column));
    }

    rows.sort_by(|left, right| {
        let left_value = left.get(&order_by.column).unwrap_or(&Value::Null);
        let right_value = right.get(&order_by.column).unwrap_or(&Value::Null);
        let order = compare_order(left_value, right_value).unwrap_or(std::cmp::Ordering::Equal);
        match order_by.direction {
            OrderDirection::Asc => order,
            OrderDirection::Desc => order.reverse(),
        }
    });
    Ok(())
}

fn resolve_join_value<'a>(row: &'a Row, table_name: &str, column: &str) -> Option<&'a Value> {
    if let Some((qualifier, bare)) = column.split_once('.') {
        if qualifier == table_name {
            return row.get(bare);
        }
        return None;
    }

    row.get(column)
}

fn join_output_columns(left_schema: &TableSchema, right_schema: &TableSchema) -> Vec<String> {
    let mut columns = Vec::new();
    for column in &left_schema.columns {
        columns.push(format!("{}.{}", left_schema.table_name, column.name));
    }
    for column in &right_schema.columns {
        columns.push(format!("{}.{}", right_schema.table_name, column.name));
    }
    columns
}

fn matches_join_where(joined: &JoinedRow, where_clause: Option<&WhereClause>) -> bool {
    let Some(where_clause) = where_clause else {
        return true;
    };

    let Some(left) = joined.get(&where_clause.column) else {
        return false;
    };

    compare_values(left, &where_clause.value, where_clause.operator.clone())
}

fn group_rows(
    rows: Vec<Row>,
    group_by: Option<&GroupByClause>,
) -> Vec<(Option<Value>, Vec<Row>)> {
    if let Some(group_by) = group_by {
        let mut groups: BTreeMap<String, (Option<Value>, Vec<Row>)> = BTreeMap::new();
        for row in rows {
            let group_value = row.get(&group_by.column).cloned().unwrap_or(Value::Null);
            let group_key = value_group_key(&group_value);
            groups
                .entry(group_key)
                .or_insert_with(|| (Some(group_value.clone()), Vec::new()))
                .1
                .push(row);
        }
        groups.into_values().collect()
    } else {
        vec![(None, rows)]
    }
}

fn value_group_key(value: &Value) -> String {
    match value {
        Value::Int(value) => format!("i:{}", value),
        Value::Text(value) => format!("t:{}", value),
        Value::Bool(value) => format!("b:{}", value),
        Value::Null => "n:null".to_string(),
    }
}

fn evaluate_select_item(item: &SelectItem, rows: &[Row]) -> std::result::Result<Value, String> {
    match item {
        SelectItem::Column(name) => Ok(rows
            .first()
            .and_then(|row| row.get(name))
            .cloned()
            .unwrap_or(Value::Null)),
        SelectItem::Aggregate { func, column } => evaluate_aggregate(func, column.as_deref(), rows),
    }
}

fn evaluate_aggregate(
    func: &AggregateFunc,
    column: Option<&str>,
    rows: &[Row],
) -> std::result::Result<Value, String> {
    match func {
        AggregateFunc::Count => Ok(Value::Int(rows.len() as i64)),
        AggregateFunc::Sum => {
            let Some(column) = column else {
                return Err("SUM requires a column".to_string());
            };
            let mut sum = 0_i64;
            for row in rows {
                match row.get(column).unwrap_or(&Value::Null) {
                    Value::Int(value) => sum += value,
                    Value::Null => {}
                    _ => return Err(format!("SUM only supports INT column '{}'", column)),
                }
            }
            Ok(Value::Int(sum))
        }
        AggregateFunc::Min => evaluate_min_max(column, rows, true),
        AggregateFunc::Max => evaluate_min_max(column, rows, false),
    }
}

fn evaluate_min_max(
    column: Option<&str>,
    rows: &[Row],
    find_min: bool,
) -> std::result::Result<Value, String> {
    let Some(column) = column else {
        return Err("MIN/MAX requires a column".to_string());
    };

    let mut best: Option<Value> = None;
    for row in rows {
        let value = row.get(column).cloned().unwrap_or(Value::Null);
        if matches!(value, Value::Null) {
            continue;
        }

        best = match best {
            None => Some(value),
            Some(current) => {
                let ord = compare_order(&value, &current)
                    .ok_or_else(|| format!("MIN/MAX cannot compare column '{}'", column))?;
                if (find_min && ord.is_lt()) || (!find_min && ord.is_gt()) {
                    Some(value)
                } else {
                    Some(current)
                }
            }
        };
    }

    Ok(best.unwrap_or(Value::Null))
}

#[derive(Debug, Clone)]
struct JoinedRow {
    columns: Vec<(String, Value)>,
}

impl JoinedRow {
    fn new(left_schema: &TableSchema, left_row: &Row, right_schema: &TableSchema, right_row: &Row) -> Self {
        let mut columns = Vec::new();
        for column in &left_schema.columns {
            if let Some(value) = left_row.get(&column.name) {
                columns.push((format!("{}.{}", left_schema.table_name, column.name), value.clone()));
            }
        }
        for column in &right_schema.columns {
            if let Some(value) = right_row.get(&column.name) {
                columns.push((format!("{}.{}", right_schema.table_name, column.name), value.clone()));
            }
        }
        Self { columns }
    }

    fn get(&self, column: &str) -> Option<&Value> {
        if column.contains('.') {
            return self
                .columns
                .iter()
                .find(|(name, _)| name == column)
                .map(|(_, value)| value);
        }

        let mut matches = self
            .columns
            .iter()
            .filter(|(name, _)| name.rsplit('.').next() == Some(column));
        let first = matches.next()?;
        if matches.next().is_some() {
            return None;
        }
        Some(&first.1)
    }
}

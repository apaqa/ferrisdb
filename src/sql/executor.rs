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
    AggregateFunc, Assignment, ColumnDef, DataType, GroupByClause, InsertSource, JoinClause,
    JoinType, Operator, OrderByClause, OrderDirection, SelectColumns, SelectItem, Statement, Value,
    WhereExpr,
};
use super::catalog::{Catalog, TableSchema, ViewDefinition};
use super::index::IndexManager;
use super::parser::Parser;
use super::row::{
    decode_row_key, encode_row_key, encode_row_prefix_end, encode_row_prefix_start, Row,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ExecuteResult {
    Explain {
        plan: String,
    },
    Created {
        table_name: String,
    },
    Altered {
        table_name: String,
    },
    Dropped {
        table_name: String,
    },
    IndexCreated {
        table_name: String,
        column_name: String,
    },
    IndexDropped {
        table_name: String,
        column_name: String,
    },
    Inserted {
        count: usize,
    },
    Selected {
        columns: Vec<String>,
        rows: Vec<Vec<Value>>,
    },
    Updated {
        count: usize,
    },
    Deleted {
        count: usize,
    },
    Error {
        message: String,
    },
}

#[derive(Debug, Clone)]
pub struct SqlExecutor {
    engine: Arc<MvccEngine>,
    catalog: Catalog,
    index_manager: IndexManager,
}

impl SqlExecutor {
    pub fn new(engine: Arc<MvccEngine>) -> Self {
        let catalog = Catalog::new(Arc::clone(&engine));
        let index_manager = IndexManager::new(Arc::clone(&engine));
        Self {
            engine,
            catalog,
            index_manager,
        }
    }

    pub fn execute(&self, stmt: Statement) -> Result<ExecuteResult> {
        match stmt {
            Statement::Explain { statement } => self.execute_explain(*statement),
            Statement::CreateView {
                view_name,
                query_sql,
                query: _,
            } => self.execute_create_view(view_name, query_sql),
            Statement::CreateTable {
                table_name,
                if_not_exists,
                columns,
            } => self.execute_create_table(table_name, if_not_exists, columns),
            Statement::AlterTableAdd { table_name, column } => {
                self.execute_alter_table_add(table_name, column)
            }
            Statement::AlterTableDropColumn {
                table_name,
                column_name,
            } => self.execute_alter_table_drop_column(table_name, column_name),
            Statement::DropTable {
                table_name,
                if_exists,
            } => self.execute_drop_table(table_name, if_exists),
            Statement::DropView {
                view_name,
                if_exists,
            } => self.execute_drop_view(view_name, if_exists),
            Statement::CreateIndex {
                table_name,
                column_name,
            } => self.execute_create_index(table_name, column_name),
            Statement::DropIndex {
                table_name,
                column_name,
            } => self.execute_drop_index(table_name, column_name),
            Statement::Insert { table_name, source } => self.execute_insert(table_name, source),
            Statement::Select {
                distinct,
                table_name,
                table_alias,
                columns,
                join,
                where_clause,
                group_by,
                having,
                order_by,
                limit,
            } => self.execute_select(
                distinct,
                table_name,
                table_alias,
                columns,
                join,
                where_clause,
                group_by,
                having,
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
            Statement::Union { left, right, all } => self.execute_union(*left, *right, all),
        }
    }

    fn execute_create_table(
        &self,
        table_name: String,
        if_not_exists: bool,
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
            if if_not_exists {
                return Ok(ExecuteResult::Created { table_name });
            }
            return Ok(ExecuteResult::Error {
                message: format!("table '{}' already exists", table_name),
            });
        }

        txn.commit()?;
        Ok(ExecuteResult::Created { table_name })
    }

    fn execute_create_view(&self, view_name: String, query_sql: String) -> Result<ExecuteResult> {
        let mut txn = self.engine.begin_transaction();
        let view = ViewDefinition {
            view_name: view_name.clone(),
            query_sql,
        };

        if !self.catalog.create_view(&mut txn, &view)? {
            return Ok(ExecuteResult::Error {
                message: format!("view '{}' already exists", view_name),
            });
        }

        txn.commit()?;
        Ok(ExecuteResult::Created {
            table_name: view_name,
        })
    }

    fn execute_alter_table_add(
        &self,
        table_name: String,
        column: ColumnDef,
    ) -> Result<ExecuteResult> {
        let mut txn = self.engine.begin_transaction();
        let Some(mut schema) = self.catalog.get_table(&txn, &table_name)? else {
            return Ok(ExecuteResult::Error {
                message: format!("table '{}' does not exist", table_name),
            });
        };
        if schema
            .columns
            .iter()
            .any(|existing| existing.name == column.name)
        {
            return Ok(ExecuteResult::Error {
                message: format!("column '{}' already exists", column.name),
            });
        }

        schema.columns.push(column.clone());
        txn.put(
            crate::sql::catalog::encode_schema_key(&table_name),
            serde_json::to_vec(&schema)?,
        )?;

        let rows = self.scan_rows(&txn, &schema_with_removed_column(&schema, &column.name))?;
        for (row_key, mut row) in rows {
            row.push(column.name.clone(), Value::Null);
            txn.put(row_key, serde_json::to_vec(&row)?)?;
        }

        txn.commit()?;
        Ok(ExecuteResult::Altered { table_name })
    }

    fn execute_alter_table_drop_column(
        &self,
        table_name: String,
        column_name: String,
    ) -> Result<ExecuteResult> {
        let mut txn = self.engine.begin_transaction();
        let Some(mut schema) = self.catalog.get_table(&txn, &table_name)? else {
            return Ok(ExecuteResult::Error {
                message: format!("table '{}' does not exist", table_name),
            });
        };
        if schema
            .columns
            .first()
            .is_some_and(|column| column.name == column_name)
        {
            return Ok(ExecuteResult::Error {
                message: "dropping the primary key column is not supported".to_string(),
            });
        }
        if !schema
            .columns
            .iter()
            .any(|column| column.name == column_name)
        {
            return Ok(ExecuteResult::Error {
                message: format!("unknown column '{}'", column_name),
            });
        }

        let rows = self.scan_rows(&txn, &schema)?;
        for (row_key, mut row) in rows {
            row.remove(&column_name);
            txn.put(row_key, serde_json::to_vec(&row)?)?;
        }

        if self
            .index_manager
            .has_index(&txn, &table_name, &column_name)?
        {
            self.index_manager
                .drop_index_in_txn(&mut txn, &table_name, &column_name)?;
        }

        schema.columns.retain(|column| column.name != column_name);
        txn.put(
            crate::sql::catalog::encode_schema_key(&table_name),
            serde_json::to_vec(&schema)?,
        )?;

        txn.commit()?;
        Ok(ExecuteResult::Altered { table_name })
    }

    fn execute_drop_table(&self, table_name: String, if_exists: bool) -> Result<ExecuteResult> {
        let mut txn = self.engine.begin_transaction();
        let Some(schema) = self.catalog.get_table(&txn, &table_name)? else {
            if if_exists {
                return Ok(ExecuteResult::Dropped { table_name });
            }
            return Ok(ExecuteResult::Error {
                message: format!("table '{}' does not exist", table_name),
            });
        };

        for index_column in self.index_manager.list_indexes(&txn, &table_name)? {
            self.index_manager
                .drop_index_in_txn(&mut txn, &table_name, &index_column)?;
        }

        for (row_key, _) in self.scan_rows(&txn, &schema)? {
            txn.delete(&row_key)?;
        }
        self.catalog.drop_table(&mut txn, &table_name)?;

        txn.commit()?;
        Ok(ExecuteResult::Dropped { table_name })
    }

    fn execute_drop_view(&self, view_name: String, if_exists: bool) -> Result<ExecuteResult> {
        let mut txn = self.engine.begin_transaction();
        if !self.catalog.drop_view(&mut txn, &view_name)? {
            if if_exists {
                return Ok(ExecuteResult::Dropped {
                    table_name: view_name,
                });
            }
            return Ok(ExecuteResult::Error {
                message: format!("view '{}' does not exist", view_name),
            });
        }

        txn.commit()?;
        Ok(ExecuteResult::Dropped {
            table_name: view_name,
        })
    }

    fn execute_create_index(
        &self,
        table_name: String,
        column_name: String,
    ) -> Result<ExecuteResult> {
        let mut txn = self.engine.begin_transaction();
        let Some(schema) = self.catalog.get_table(&txn, &table_name)? else {
            return Ok(ExecuteResult::Error {
                message: format!("table '{}' does not exist", table_name),
            });
        };
        if !schema
            .columns
            .iter()
            .any(|column| column.name == column_name)
        {
            return Ok(ExecuteResult::Error {
                message: format!("unknown column '{}'", column_name),
            });
        }
        if !self
            .index_manager
            .create_index_in_txn(&mut txn, &table_name, &column_name)?
        {
            return Ok(ExecuteResult::Error {
                message: format!("index on '{}.{}' already exists", table_name, column_name),
            });
        }

        txn.commit()?;
        Ok(ExecuteResult::IndexCreated {
            table_name,
            column_name,
        })
    }

    fn execute_drop_index(&self, table_name: String, column_name: String) -> Result<ExecuteResult> {
        let mut txn = self.engine.begin_transaction();
        if !self
            .index_manager
            .drop_index_in_txn(&mut txn, &table_name, &column_name)?
        {
            return Ok(ExecuteResult::Error {
                message: format!("index on '{}.{}' does not exist", table_name, column_name),
            });
        }

        txn.commit()?;
        Ok(ExecuteResult::IndexDropped {
            table_name,
            column_name,
        })
    }

    fn execute_explain(&self, statement: Statement) -> Result<ExecuteResult> {
        match statement {
            Statement::Select {
                distinct: _,
                table_name,
                table_alias: _,
                columns,
                join,
                where_clause,
                group_by,
                having,
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

                let use_index = self.can_use_index(&txn, &table_name, where_clause.as_ref())?;
                let index_lookup = if use_index {
                    self.index_manager.find_indexable_comparison(
                        &txn,
                        &table_name,
                        where_clause.as_ref(),
                    )?
                } else {
                    None
                };
                let total_rows = self.scan_rows(&txn, &schema)?.len();
                let scan_rows = if use_index {
                    estimate_filtered_rows(total_rows, where_clause.as_ref())
                } else {
                    total_rows
                };
                let scan_cost = if use_index {
                    (scan_rows.max(1)) as f64 * 0.35
                } else {
                    total_rows as f64
                };
                let has_filter = where_clause.is_some();
                let filtered_rows = estimate_filtered_rows(total_rows, where_clause.as_ref());
                let filter_cost = if has_filter {
                    (total_rows as f64) * 0.25
                } else {
                    0.0
                };
                let project_cost = estimate_project_cost(filtered_rows, &columns);

                let mut lines = Vec::new();
                if let Some((column, _)) = index_lookup {
                    lines.push(format!(
                        "IndexScan(table={}, column={}, rows={}, cost={:.2})",
                        table_name, column, scan_rows, scan_cost
                    ));
                } else {
                    lines.push(format!(
                        "SeqScan(table={}, rows={}, cost={:.2})",
                        table_name, total_rows, scan_cost
                    ));
                }
                if let Some(where_clause) = where_clause {
                    if use_index && where_eq_comparison_parts(&where_clause).is_some() {
                        let (column, value) =
                            where_eq_comparison_parts(&where_clause).expect("comparison");
                        lines.push(format!(
                            "  -> IndexFilter(predicate=\"{} = {}\", rows={}, cost={:.2})",
                            column,
                            render_value(value),
                            filtered_rows,
                            filter_cost * 0.25
                        ));
                    } else {
                        lines.push(format!(
                            "  -> Filter(predicate=\"{}\", rows={}, cost={:.2})",
                            render_where_expr(&where_clause),
                            filtered_rows,
                            filter_cost
                        ));
                    }
                }
                if let Some(group_by) = group_by {
                    lines.push(format!(
                        "  -> GroupBy(column={}, rows={}, cost={:.2})",
                        group_by.column,
                        filtered_rows.max(1),
                        (filtered_rows.max(1)) as f64 * 0.35
                    ));
                }
                if let Some(having) = having {
                    lines.push(format!(
                        "  -> Having(predicate=\"{}\", rows={}, cost={:.2})",
                        render_where_expr(&having),
                        filtered_rows.max(1),
                        (filtered_rows.max(1)) as f64 * 0.15
                    ));
                }
                if let Some(order_by) = order_by {
                    lines.push(format!(
                        "  -> OrderBy(column={}, direction={}, rows={}, cost={:.2})",
                        order_by.column,
                        order_direction_to_str(&order_by.direction),
                        if has_filter {
                            filtered_rows
                        } else {
                            total_rows
                        },
                        (if has_filter {
                            filtered_rows
                        } else {
                            total_rows
                        }) as f64
                            * 0.2
                    ));
                }
                if let Some(limit) = limit {
                    lines.push(format!(
                        "  -> Limit(limit={}, rows={}, cost={:.2})",
                        limit,
                        limit.min(if has_filter {
                            filtered_rows
                        } else {
                            total_rows
                        }),
                        0.05
                    ));
                }

                let project_rows = if has_filter {
                    filtered_rows
                } else {
                    total_rows
                };
                let project_rows = limit
                    .map(|value| value.min(project_rows))
                    .unwrap_or(project_rows);
                let project_desc = match columns {
                    SelectColumns::All => "*".to_string(),
                    SelectColumns::Named(items) | SelectColumns::Aggregate(items) => items
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

    fn execute_insert(&self, table_name: String, source: InsertSource) -> Result<ExecuteResult> {
        let mut txn = self.engine.begin_transaction();
        let Some(schema) = self.catalog.get_table(&txn, &table_name)? else {
            return Ok(ExecuteResult::Error {
                message: format!("table '{}' does not exist", table_name),
            });
        };

        let rows = match source {
            InsertSource::Values(rows) => rows,
            InsertSource::Select(statement) => {
                let result = self.execute(*statement)?;
                let ExecuteResult::Selected { rows, .. } = result else {
                    return Ok(ExecuteResult::Error {
                        message: "INSERT INTO SELECT requires a SELECT-compatible source"
                            .to_string(),
                    });
                };
                rows
            }
        };

        let inserted = match self.insert_rows_into_table(&mut txn, &schema, &table_name, rows)? {
            Ok(count) => count,
            Err(message) => return Ok(ExecuteResult::Error { message }),
        };

        txn.commit()?;
        Ok(ExecuteResult::Inserted { count: inserted })
    }

    fn execute_union(&self, left: Statement, right: Statement, all: bool) -> Result<ExecuteResult> {
        let left_result = self.execute(left)?;
        let right_result = self.execute(right)?;
        let ExecuteResult::Selected {
            columns: left_columns,
            mut rows,
        } = left_result
        else {
            return Ok(ExecuteResult::Error {
                message: "UNION only supports SELECT-compatible queries".to_string(),
            });
        };
        let ExecuteResult::Selected {
            columns: right_columns,
            rows: right_rows,
        } = right_result
        else {
            return Ok(ExecuteResult::Error {
                message: "UNION only supports SELECT-compatible queries".to_string(),
            });
        };

        if left_columns.len() != right_columns.len() {
            return Ok(ExecuteResult::Error {
                message: format!(
                    "UNION expected {} columns on right side, got {}",
                    left_columns.len(),
                    right_columns.len()
                ),
            });
        }

        rows.extend(right_rows);
        if !all {
            dedup_selected_rows(&mut rows);
        }

        Ok(ExecuteResult::Selected {
            columns: left_columns,
            rows,
        })
    }

    #[allow(clippy::too_many_arguments)]
    fn execute_select_from_view(
        &self,
        txn: &Transaction,
        view: ViewDefinition,
        distinct: bool,
        view_name: String,
        table_alias: Option<String>,
        columns: SelectColumns,
        join: Option<JoinClause>,
        where_clause: Option<WhereExpr>,
        group_by: Option<GroupByClause>,
        having: Option<WhereExpr>,
        order_by: Option<OrderByClause>,
        limit: Option<usize>,
    ) -> Result<ExecuteResult> {
        let statement = parse_sql_to_statement(&view.query_sql)?;
        let ExecuteResult::Selected {
            columns: source_columns,
            rows: source_rows,
        } = self.execute(statement)?
        else {
            return Ok(ExecuteResult::Error {
                message: format!("view '{}' does not produce tabular rows", view.view_name),
            });
        };

        let schema = materialized_schema(&view_name, &source_columns, &source_rows);
        let rows = materialized_rows(&source_columns, source_rows);
        self.execute_select_from_materialized_rows(
            txn,
            schema,
            rows,
            distinct,
            table_alias,
            columns,
            join,
            where_clause,
            group_by,
            having,
            order_by,
            limit,
        )
    }

    fn execute_select(
        &self,
        distinct: bool,
        table_name: String,
        table_alias: Option<String>,
        columns: SelectColumns,
        join: Option<JoinClause>,
        where_clause: Option<WhereExpr>,
        group_by: Option<GroupByClause>,
        having: Option<WhereExpr>,
        order_by: Option<OrderByClause>,
        limit: Option<usize>,
    ) -> Result<ExecuteResult> {
        let txn = self.engine.begin_transaction();
        if self.catalog.get_table(&txn, &table_name)?.is_none() {
            if let Some(view) = self.catalog.get_view(&txn, &table_name)? {
                return self.execute_select_from_view(
                    &txn,
                    view,
                    distinct,
                    table_name,
                    table_alias,
                    columns,
                    join,
                    where_clause,
                    group_by,
                    having,
                    order_by,
                    limit,
                );
            }
        }
        let Some(schema) = self.catalog.get_table(&txn, &table_name)? else {
            return Ok(ExecuteResult::Error {
                message: format!("table '{}' does not exist", table_name),
            });
        };
        let columns = normalize_select_columns(columns, table_alias.as_deref(), &table_name);
        let where_clause = where_clause
            .map(|expr| normalize_where_expr(expr, table_alias.as_deref(), &table_name));
        let group_by = group_by.map(|group| GroupByClause {
            column: normalize_column_reference(group.column, table_alias.as_deref(), &table_name),
        });
        let having =
            having.map(|expr| normalize_where_expr(expr, table_alias.as_deref(), &table_name));
        let order_by = order_by.map(|order| OrderByClause {
            column: normalize_column_reference(order.column, table_alias.as_deref(), &table_name),
            direction: order.direction,
        });

        if let Some(join_clause) = join {
            if matches!(columns, SelectColumns::Aggregate(_))
                || group_by.is_some()
                || having.is_some()
            {
                return Ok(ExecuteResult::Error {
                    message: "JOIN with aggregate queries is not implemented yet".to_string(),
                });
            }
            return self.execute_join_select(
                &txn,
                &schema,
                distinct,
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
                distinct,
                columns,
                where_clause,
                group_by,
                having,
                order_by,
                limit,
            );
        }

        let projection = match self.resolve_projection(&schema, &columns) {
            Ok(columns) => columns,
            Err(message) => return Ok(ExecuteResult::Error { message }),
        };

        let rows: Vec<Row> = self
            .fetch_rows_for_select(&txn, &schema, where_clause.as_ref())?
            .into_iter()
            .map(|(_, row)| row)
            .collect();
        let output_columns = projection
            .iter()
            .map(|item| item.header.clone())
            .collect::<Vec<_>>();
        let mut selected = rows
            .into_iter()
            .map(|row| {
                projection
                    .iter()
                    .map(|column| row.get(&column.lookup).cloned().unwrap_or(Value::Null))
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();
        if distinct {
            dedup_selected_rows(&mut selected);
        }
        if let Some(order_by) = order_by.as_ref() {
            if let Err(message) =
                sort_result_rows_by_order(&mut selected, &output_columns, order_by)
            {
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

    // 中文註解：聚合查詢會先做 WHERE 過濾，再做 GROUP BY，最後套用 HAVING。
    fn execute_aggregate_select(
        &self,
        txn: &Transaction,
        schema: &TableSchema,
        distinct: bool,
        columns: SelectColumns,
        where_clause: Option<WhereExpr>,
        group_by: Option<GroupByClause>,
        having: Option<WhereExpr>,
        order_by: Option<OrderByClause>,
        limit: Option<usize>,
    ) -> Result<ExecuteResult> {
        let SelectColumns::Aggregate(items) = columns else {
            return Ok(ExecuteResult::Error {
                message: "GROUP BY requires aggregate expressions in SELECT".to_string(),
            });
        };

        let output_columns =
            match self.resolve_aggregate_projection(schema, &items, group_by.as_ref()) {
                Ok(columns) => columns,
                Err(message) => return Ok(ExecuteResult::Error { message }),
            };
        let resolved_having = self.resolve_where_expr(having.as_ref())?;

        let rows: Vec<Row> = self
            .fetch_rows_for_select(txn, schema, where_clause.as_ref())?
            .into_iter()
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
            let projected_row = ProjectedRow::new(&output_columns, &output_row);
            if !eval_where_expr(&projected_row, resolved_having.as_ref()) {
                continue;
            }
            selected.push(output_row);
        }

        if let Some(order_by) = order_by.as_ref() {
            if let Err(message) =
                sort_result_rows_by_order(&mut selected, &output_columns, order_by)
            {
                return Ok(ExecuteResult::Error { message });
            }
        }
        if let Some(limit) = limit {
            selected.truncate(limit);
        }
        if distinct {
            dedup_selected_rows(&mut selected);
        }

        Ok(ExecuteResult::Selected {
            columns: output_columns,
            rows: selected,
        })
    }

    // 中文註解：JOIN 查詢目前支援 INNER JOIN 與 LEFT JOIN，WHERE 會在 join 結果上再過濾。
    fn execute_join_select(
        &self,
        txn: &Transaction,
        left_schema: &TableSchema,
        distinct: bool,
        columns: SelectColumns,
        join: JoinClause,
        where_clause: Option<WhereExpr>,
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
        let resolved_where = self.resolve_where_expr(where_clause.as_ref())?;
        let projection = match self.resolve_join_projection(left_schema, &right_schema, &columns) {
            Ok(columns) => columns,
            Err(message) => return Ok(ExecuteResult::Error { message }),
        };

        let mut matched_rows = Vec::new();
        for (_, left_row) in &left_rows {
            let Some(left_value) =
                resolve_join_value(left_row, &left_schema.table_name, &join.left_column)
            else {
                return Ok(ExecuteResult::Error {
                    message: format!("unknown join column '{}'", join.left_column),
                });
            };

            let mut found_match = false;
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

                found_match = true;
                let joined = JoinedRow::new(left_schema, left_row, &right_schema, Some(right_row));
                if !eval_where_expr(&joined, resolved_where.as_ref()) {
                    continue;
                }
                matched_rows.push(joined);
            }

            if !found_match && matches!(join.join_type, JoinType::Left) {
                let joined = JoinedRow::new(left_schema, left_row, &right_schema, None);
                if eval_where_expr(&joined, resolved_where.as_ref()) {
                    matched_rows.push(joined);
                }
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
                let Some(value) = joined.get(&column.lookup) else {
                    return Ok(ExecuteResult::Error {
                        message: format!("unknown column '{}'", column.lookup),
                    });
                };
                row.push(value.clone());
            }
            selected.push(row);
        }
        if distinct {
            dedup_selected_rows(&mut selected);
        }

        Ok(ExecuteResult::Selected {
            columns: projection.into_iter().map(|item| item.header).collect(),
            rows: selected,
        })
    }

    #[allow(clippy::too_many_arguments)]
    fn execute_select_from_materialized_rows(
        &self,
        txn: &Transaction,
        schema: TableSchema,
        source_rows: Vec<Row>,
        distinct: bool,
        table_alias: Option<String>,
        columns: SelectColumns,
        join: Option<JoinClause>,
        where_clause: Option<WhereExpr>,
        group_by: Option<GroupByClause>,
        having: Option<WhereExpr>,
        order_by: Option<OrderByClause>,
        limit: Option<usize>,
    ) -> Result<ExecuteResult> {
        let table_name = schema.table_name.clone();
        let columns = normalize_select_columns(columns, table_alias.as_deref(), &table_name);
        let where_clause = where_clause
            .map(|expr| normalize_where_expr(expr, table_alias.as_deref(), &table_name));
        let group_by = group_by.map(|group| GroupByClause {
            column: normalize_column_reference(group.column, table_alias.as_deref(), &table_name),
        });
        let having =
            having.map(|expr| normalize_where_expr(expr, table_alias.as_deref(), &table_name));
        let order_by = order_by.map(|order| OrderByClause {
            column: normalize_column_reference(order.column, table_alias.as_deref(), &table_name),
            direction: order.direction,
        });
        let resolved_where = self.resolve_where_expr(where_clause.as_ref())?;

        if let Some(join_clause) = join {
            return self.execute_join_select_from_rows(
                txn,
                &schema,
                &source_rows,
                distinct,
                columns,
                join_clause,
                where_clause,
                order_by,
                limit,
            );
        }

        if matches!(columns, SelectColumns::Aggregate(_)) || group_by.is_some() {
            let resolved_having = self.resolve_where_expr(having.as_ref())?;
            let SelectColumns::Aggregate(items) = columns else {
                return Ok(ExecuteResult::Error {
                    message: "GROUP BY requires aggregate expressions in SELECT".to_string(),
                });
            };
            let output_columns =
                match self.resolve_aggregate_projection(&schema, &items, group_by.as_ref()) {
                    Ok(columns) => columns,
                    Err(message) => return Ok(ExecuteResult::Error { message }),
                };
            let mut selected = Vec::new();
            for (_, group_rows) in group_rows(
                filtered_rows(source_rows, resolved_where.as_ref()),
                group_by.as_ref(),
            ) {
                let mut output_row = Vec::with_capacity(items.len());
                for item in &items {
                    match evaluate_select_item(item, &group_rows) {
                        Ok(value) => output_row.push(value),
                        Err(message) => return Ok(ExecuteResult::Error { message }),
                    }
                }
                let projected_row = ProjectedRow::new(&output_columns, &output_row);
                if !eval_where_expr(&projected_row, resolved_having.as_ref()) {
                    continue;
                }
                selected.push(output_row);
            }
            if let Some(order_by) = order_by.as_ref() {
                if let Err(message) =
                    sort_result_rows_by_order(&mut selected, &output_columns, order_by)
                {
                    return Ok(ExecuteResult::Error { message });
                }
            }
            if let Some(limit) = limit {
                selected.truncate(limit);
            }
            if distinct {
                dedup_selected_rows(&mut selected);
            }
            return Ok(ExecuteResult::Selected {
                columns: output_columns,
                rows: selected,
            });
        }

        let projection = match self.resolve_projection(&schema, &columns) {
            Ok(columns) => columns,
            Err(message) => return Ok(ExecuteResult::Error { message }),
        };
        let output_columns = projection
            .iter()
            .map(|item| item.header.clone())
            .collect::<Vec<_>>();
        let mut selected = filtered_rows(source_rows, resolved_where.as_ref())
            .into_iter()
            .map(|row| {
                projection
                    .iter()
                    .map(|column| row.get(&column.lookup).cloned().unwrap_or(Value::Null))
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();
        if distinct {
            dedup_selected_rows(&mut selected);
        }
        if let Some(order_by) = order_by.as_ref() {
            if let Err(message) =
                sort_result_rows_by_order(&mut selected, &output_columns, order_by)
            {
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

    fn execute_join_select_from_rows(
        &self,
        txn: &Transaction,
        left_schema: &TableSchema,
        left_rows: &[Row],
        distinct: bool,
        columns: SelectColumns,
        join: JoinClause,
        where_clause: Option<WhereExpr>,
        order_by: Option<OrderByClause>,
        limit: Option<usize>,
    ) -> Result<ExecuteResult> {
        let Some(right_schema) = self.catalog.get_table(txn, &join.right_table)? else {
            return Ok(ExecuteResult::Error {
                message: format!("table '{}' does not exist", join.right_table),
            });
        };

        let right_rows = self.scan_rows(txn, &right_schema)?;
        let resolved_where = self.resolve_where_expr(where_clause.as_ref())?;
        let projection = match self.resolve_join_projection(left_schema, &right_schema, &columns) {
            Ok(columns) => columns,
            Err(message) => return Ok(ExecuteResult::Error { message }),
        };

        let mut matched_rows = Vec::new();
        for left_row in left_rows {
            let Some(left_value) =
                resolve_join_value(left_row, &left_schema.table_name, &join.left_column)
            else {
                return Ok(ExecuteResult::Error {
                    message: format!("unknown join column '{}'", join.left_column),
                });
            };

            let mut found_match = false;
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

                found_match = true;
                let joined = JoinedRow::new(left_schema, left_row, &right_schema, Some(right_row));
                if eval_where_expr(&joined, resolved_where.as_ref()) {
                    matched_rows.push(joined);
                }
            }

            if !found_match && matches!(join.join_type, JoinType::Left) {
                let joined = JoinedRow::new(left_schema, left_row, &right_schema, None);
                if eval_where_expr(&joined, resolved_where.as_ref()) {
                    matched_rows.push(joined);
                }
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
        let mut selected = Vec::new();
        for joined in matched_rows {
            let mut row = Vec::new();
            for column in &projection {
                let Some(value) = joined.get(&column.lookup) else {
                    return Ok(ExecuteResult::Error {
                        message: format!("unknown column '{}'", column.lookup),
                    });
                };
                row.push(value.clone());
            }
            selected.push(row);
        }
        if distinct {
            dedup_selected_rows(&mut selected);
        }

        Ok(ExecuteResult::Selected {
            columns: projection.into_iter().map(|item| item.header).collect(),
            rows: selected,
        })
    }

    // 中文註解：UPDATE 會對每一列套用遞迴 WHERE 判斷，再同步維護索引內容。
    fn execute_update(
        &self,
        table_name: String,
        assignments: Vec<Assignment>,
        where_clause: Option<WhereExpr>,
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
            if !schema
                .columns
                .iter()
                .any(|column| column.name == assignment.column)
            {
                return Ok(ExecuteResult::Error {
                    message: format!("unknown column '{}'", assignment.column),
                });
            }
        }

        let rows = self.scan_rows(&txn, &schema)?;
        let resolved_where = self.resolve_where_expr(where_clause.as_ref())?;
        let mut updated = 0;
        for (row_key, mut row) in rows {
            if !eval_where_expr(&row, resolved_where.as_ref()) {
                continue;
            }

            let old_row = row.clone();
            let pk_value = old_row
                .get(&schema.columns[0].name)
                .cloned()
                .unwrap_or(Value::Null);

            for assignment in &assignments {
                row.set(&assignment.column, assignment.value.clone());
            }

            txn.put(row_key, serde_json::to_vec(&row)?)?;
            for indexed_column in self.index_manager.list_indexes(&txn, &table_name)? {
                let old_value = old_row.get(&indexed_column).cloned().unwrap_or(Value::Null);
                let new_value = row.get(&indexed_column).cloned().unwrap_or(Value::Null);
                if old_value != new_value {
                    self.index_manager.delete_index_entry(
                        &mut txn,
                        &table_name,
                        &indexed_column,
                        &old_value,
                        &pk_value,
                    )?;
                    self.index_manager.insert_index_entry(
                        &mut txn,
                        &table_name,
                        &indexed_column,
                        &new_value,
                        &pk_value,
                    )?;
                }
            }
            updated += 1;
        }

        txn.commit()?;
        Ok(ExecuteResult::Updated { count: updated })
    }

    // 中文註解：DELETE 與 UPDATE 共用同一套布林 WHERE evaluator，避免條件語意不一致。
    fn execute_delete(
        &self,
        table_name: String,
        where_clause: Option<WhereExpr>,
    ) -> Result<ExecuteResult> {
        let mut txn = self.engine.begin_transaction();
        let Some(schema) = self.catalog.get_table(&txn, &table_name)? else {
            return Ok(ExecuteResult::Error {
                message: format!("table '{}' does not exist", table_name),
            });
        };

        let rows = self.scan_rows(&txn, &schema)?;
        let resolved_where = self.resolve_where_expr(where_clause.as_ref())?;
        let mut deleted = 0;
        for (row_key, row) in rows {
            if !eval_where_expr(&row, resolved_where.as_ref()) {
                continue;
            }
            let pk_value = row
                .get(&schema.columns[0].name)
                .cloned()
                .unwrap_or(Value::Null);
            for indexed_column in self.index_manager.list_indexes(&txn, &table_name)? {
                if let Some(index_value) = row.get(&indexed_column) {
                    self.index_manager.delete_index_entry(
                        &mut txn,
                        &table_name,
                        &indexed_column,
                        index_value,
                        &pk_value,
                    )?;
                }
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
    ) -> std::result::Result<Vec<ProjectionColumn>, String> {
        match columns {
            SelectColumns::All => Ok(schema
                .columns
                .iter()
                .map(|column| ProjectionColumn {
                    lookup: column.name.clone(),
                    header: column.name.clone(),
                })
                .collect()),
            SelectColumns::Named(items) => {
                let mut projection = Vec::with_capacity(items.len());
                for item in items {
                    let SelectItem::Column { name, alias } = item else {
                        return Err(
                            "aggregate columns require aggregate execution path".to_string()
                        );
                    };
                    if !schema.columns.iter().any(|column| column.name == *name) {
                        return Err(format!("unknown column '{}'", name));
                    }
                    projection.push(ProjectionColumn {
                        lookup: name.clone(),
                        header: alias.clone().unwrap_or_else(|| name.clone()),
                    });
                }
                Ok(projection)
            }
            SelectColumns::Aggregate(_) => {
                Err("aggregate columns require aggregate execution path".to_string())
            }
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
                SelectItem::Column { name, alias } => {
                    if !schema.columns.iter().any(|column| &column.name == name) {
                        return Err(format!("unknown column '{}'", name));
                    }
                    if group_by.as_ref().map(|group| &group.column) != Some(name) {
                        return Err(format!(
                            "column '{}' must appear in GROUP BY when aggregate functions are used",
                            name
                        ));
                    }
                    columns.push(alias.clone().unwrap_or_else(|| name.clone()));
                }
                SelectItem::Aggregate {
                    func,
                    column,
                    alias,
                } => {
                    if let Some(column_name) = column {
                        if !schema.columns.iter().any(|col| &col.name == column_name) {
                            return Err(format!("unknown column '{}'", column_name));
                        }
                    }
                    columns.push(alias.clone().unwrap_or_else(|| render_select_item(item)));
                    if matches!(func, AggregateFunc::Count) {
                        continue;
                    }
                }
            }
        }
        Ok(columns)
    }

    fn insert_rows_into_table(
        &self,
        txn: &mut Transaction,
        schema: &TableSchema,
        table_name: &str,
        rows: Vec<Vec<Value>>,
    ) -> Result<std::result::Result<usize, String>> {
        let mut inserted = 0;
        for row_values in rows {
            if row_values.len() != schema.columns.len() {
                return Ok(Err(format!(
                    "INSERT expected {} values for table '{}', got {}",
                    schema.columns.len(),
                    table_name,
                    row_values.len()
                )));
            }

            let Some(pk_value) = row_values.first().cloned() else {
                return Ok(Err("INSERT row cannot be empty".to_string()));
            };
            if matches!(pk_value, Value::Null) {
                return Ok(Err("primary key cannot be NULL".to_string()));
            }

            let row = Row::new(
                schema
                    .columns
                    .iter()
                    .map(|column| column.name.clone())
                    .zip(row_values.into_iter())
                    .collect(),
            );
            txn.put(
                encode_row_key(table_name, &pk_value),
                serde_json::to_vec(&row)?,
            )?;
            for indexed_column in self.index_manager.list_indexes(txn, table_name)? {
                if let Some(index_value) = row.get(&indexed_column) {
                    self.index_manager.insert_index_entry(
                        txn,
                        table_name,
                        &indexed_column,
                        index_value,
                        &pk_value,
                    )?;
                }
            }
            inserted += 1;
        }

        Ok(Ok(inserted))
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

    // 中文註解：SELECT 會優先嘗試使用 indexable 的等值條件縮小候選列，再用完整條件做二次過濾。
    fn fetch_rows_for_select(
        &self,
        txn: &Transaction,
        schema: &TableSchema,
        where_clause: Option<&WhereExpr>,
    ) -> Result<Vec<(Vec<u8>, Row)>> {
        let resolved_where = self.resolve_where_expr(where_clause)?;
        if let Some((column, value)) =
            self.index_manager
                .find_indexable_comparison(txn, &schema.table_name, where_clause)?
        {
            let pks = self
                .index_manager
                .lookup(txn, &schema.table_name, column, value)?;
            let mut rows = Vec::new();
            for pk in pks {
                let row_key = encode_row_key(&schema.table_name, &pk);
                if let Some(raw) = txn.get(&row_key)? {
                    let row: Row = serde_json::from_slice(&raw)?;
                    if eval_where_expr(&row, resolved_where.as_ref()) {
                        rows.push((row_key, row));
                    }
                }
            }
            return Ok(rows);
        }

        let mut rows = self.scan_rows(txn, schema)?;
        rows.retain(|(_, row)| eval_where_expr(row, resolved_where.as_ref()));
        Ok(rows)
    }

    // 中文註解：只要 WHERE 樹中存在可索引的等值比較，就允許 explain/scan 走 index scan 路徑。
    fn can_use_index(
        &self,
        txn: &Transaction,
        table_name: &str,
        where_clause: Option<&WhereExpr>,
    ) -> Result<bool> {
        Ok(self
            .index_manager
            .find_indexable_comparison(txn, table_name, where_clause)?
            .is_some())
    }

    // 中文註解：先把子查詢解析成值集合，之後 Row / JoinedRow / 聚合列都能共用同一套 evaluator。
    fn resolve_where_expr(
        &self,
        where_clause: Option<&WhereExpr>,
    ) -> Result<Option<ResolvedWhereExpr>> {
        let Some(where_clause) = where_clause else {
            return Ok(None);
        };

        Ok(Some(self.resolve_where_expr_inner(where_clause)?))
    }

    // 中文註解：遞迴把 WhereExpr 轉成可直接求值的樹，特別是把 IN 子查詢預先展開。
    fn resolve_where_expr_inner(&self, where_clause: &WhereExpr) -> Result<ResolvedWhereExpr> {
        match where_clause {
            WhereExpr::Comparison {
                column,
                operator,
                value,
            } => Ok(ResolvedWhereExpr::Comparison {
                column: column.clone(),
                operator: operator.clone(),
                value: value.clone(),
            }),
            WhereExpr::Between { column, low, high } => Ok(ResolvedWhereExpr::Between {
                column: column.clone(),
                low: low.clone(),
                high: high.clone(),
            }),
            WhereExpr::Like { column, pattern } => Ok(ResolvedWhereExpr::Like {
                column: column.clone(),
                pattern: pattern.clone(),
            }),
            WhereExpr::IsNull { column, negated } => Ok(ResolvedWhereExpr::IsNull {
                column: column.clone(),
                negated: *negated,
            }),
            WhereExpr::InSubquery { column, subquery } => {
                let result = self.execute((**subquery).clone())?;
                let ExecuteResult::Selected { rows, .. } = result else {
                    return Ok(ResolvedWhereExpr::InValues {
                        column: column.clone(),
                        values: Vec::new(),
                    });
                };

                let mut values = Vec::new();
                for row in rows {
                    if let Some(value) = row.first() {
                        values.push(value.clone());
                    }
                }
                Ok(ResolvedWhereExpr::InValues {
                    column: column.clone(),
                    values,
                })
            }
            WhereExpr::And(left, right) => Ok(ResolvedWhereExpr::And(
                Box::new(self.resolve_where_expr_inner(left)?),
                Box::new(self.resolve_where_expr_inner(right)?),
            )),
            WhereExpr::Or(left, right) => Ok(ResolvedWhereExpr::Or(
                Box::new(self.resolve_where_expr_inner(left)?),
                Box::new(self.resolve_where_expr_inner(right)?),
            )),
            WhereExpr::Not(expr) => Ok(ResolvedWhereExpr::Not(Box::new(
                self.resolve_where_expr_inner(expr)?,
            ))),
        }
    }

    fn resolve_join_projection(
        &self,
        left_schema: &TableSchema,
        right_schema: &TableSchema,
        columns: &SelectColumns,
    ) -> std::result::Result<Vec<ProjectionColumn>, String> {
        let all_columns = join_output_columns(left_schema, right_schema);
        match columns {
            SelectColumns::All => Ok(all_columns
                .into_iter()
                .map(|column| ProjectionColumn {
                    lookup: column.clone(),
                    header: column,
                })
                .collect()),
            SelectColumns::Named(items) => {
                let mut projection = Vec::with_capacity(items.len());
                for item in items {
                    let SelectItem::Column { name, alias } = item else {
                        return Err(
                            "aggregate projection is not supported for JOIN queries".to_string()
                        );
                    };
                    if !all_columns.iter().any(|column| column == name) {
                        let left_matches = left_schema
                            .columns
                            .iter()
                            .filter(|column| {
                                column.name == *name
                                    || format!("{}.{}", left_schema.table_name, column.name)
                                        == *name
                            })
                            .count();
                        let right_matches = right_schema
                            .columns
                            .iter()
                            .filter(|column| {
                                column.name == *name
                                    || format!("{}.{}", right_schema.table_name, column.name)
                                        == *name
                            })
                            .count();
                        if left_matches + right_matches != 1 {
                            return Err(format!("unknown or ambiguous column '{}'", name));
                        }
                    }
                    projection.push(ProjectionColumn {
                        lookup: name.clone(),
                        header: alias.clone().unwrap_or_else(|| name.clone()),
                    });
                }
                Ok(projection)
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
        ExecuteResult::Altered { table_name } => format!("Table '{}' altered", table_name),
        ExecuteResult::Dropped { table_name } => format!("Table '{}' dropped", table_name),
        ExecuteResult::IndexCreated {
            table_name,
            column_name,
        } => format!("Index on '{}.{}' created", table_name, column_name),
        ExecuteResult::IndexDropped {
            table_name,
            column_name,
        } => format!("Index on '{}.{}' dropped", table_name, column_name),
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

#[derive(Debug, Clone)]
enum ResolvedWhereExpr {
    Comparison {
        column: String,
        operator: Operator,
        value: Value,
    },
    Between {
        column: String,
        low: Value,
        high: Value,
    },
    Like {
        column: String,
        pattern: String,
    },
    IsNull {
        column: String,
        negated: bool,
    },
    InValues {
        column: String,
        values: Vec<Value>,
    },
    And(Box<ResolvedWhereExpr>, Box<ResolvedWhereExpr>),
    Or(Box<ResolvedWhereExpr>, Box<ResolvedWhereExpr>),
    Not(Box<ResolvedWhereExpr>),
}

trait ValueLookup {
    fn lookup(&self, column: &str) -> Option<&Value>;
}

#[derive(Debug, Clone)]
struct ProjectionColumn {
    lookup: String,
    header: String,
}

fn where_eq_comparison_parts(where_clause: &WhereExpr) -> Option<(&str, &Value)> {
    match where_clause {
        WhereExpr::Comparison {
            column,
            operator: Operator::Eq,
            value,
        } => Some((column, value)),
        _ => None,
    }
}

fn render_where_expr(where_clause: &WhereExpr) -> String {
    match where_clause {
        WhereExpr::Comparison {
            column,
            operator,
            value,
        } => format!(
            "{} {} {}",
            column,
            operator_to_str(operator),
            render_value(value)
        ),
        WhereExpr::Between { column, low, high } => {
            format!(
                "{} BETWEEN {} AND {}",
                column,
                render_value(low),
                render_value(high)
            )
        }
        WhereExpr::Like { column, pattern } => {
            format!("{} LIKE {}", column, pattern)
        }
        WhereExpr::IsNull { column, negated } => {
            if *negated {
                format!("{} IS NOT NULL", column)
            } else {
                format!("{} IS NULL", column)
            }
        }
        WhereExpr::InSubquery { column, .. } => {
            format!("{} IN (subquery)", column)
        }
        WhereExpr::And(left, right) => format!(
            "({} AND {})",
            render_where_expr(left),
            render_where_expr(right)
        ),
        WhereExpr::Or(left, right) => format!(
            "({} OR {})",
            render_where_expr(left),
            render_where_expr(right)
        ),
        WhereExpr::Not(expr) => format!("(NOT {})", render_where_expr(expr)),
    }
}

fn normalize_select_columns(
    columns: SelectColumns,
    table_alias: Option<&str>,
    table_name: &str,
) -> SelectColumns {
    match columns {
        SelectColumns::All => SelectColumns::All,
        SelectColumns::Named(items) => SelectColumns::Named(
            items
                .into_iter()
                .map(|item| normalize_select_item(item, table_alias, table_name))
                .collect(),
        ),
        SelectColumns::Aggregate(items) => SelectColumns::Aggregate(
            items
                .into_iter()
                .map(|item| normalize_select_item(item, table_alias, table_name))
                .collect(),
        ),
    }
}

fn normalize_select_item(
    item: SelectItem,
    table_alias: Option<&str>,
    table_name: &str,
) -> SelectItem {
    match item {
        SelectItem::Column { name, alias } => SelectItem::Column {
            name: normalize_column_reference(name, table_alias, table_name),
            alias,
        },
        SelectItem::Aggregate {
            func,
            column,
            alias,
        } => SelectItem::Aggregate {
            func,
            column: column
                .map(|column| normalize_column_reference(column, table_alias, table_name)),
            alias,
        },
    }
}

fn normalize_where_expr(expr: WhereExpr, table_alias: Option<&str>, table_name: &str) -> WhereExpr {
    match expr {
        WhereExpr::Comparison {
            column,
            operator,
            value,
        } => WhereExpr::Comparison {
            column: normalize_column_reference(column, table_alias, table_name),
            operator,
            value,
        },
        WhereExpr::Between { column, low, high } => WhereExpr::Between {
            column: normalize_column_reference(column, table_alias, table_name),
            low,
            high,
        },
        WhereExpr::Like { column, pattern } => WhereExpr::Like {
            column: normalize_column_reference(column, table_alias, table_name),
            pattern,
        },
        WhereExpr::IsNull { column, negated } => WhereExpr::IsNull {
            column: normalize_column_reference(column, table_alias, table_name),
            negated,
        },
        WhereExpr::InSubquery { column, subquery } => WhereExpr::InSubquery {
            column: normalize_column_reference(column, table_alias, table_name),
            subquery,
        },
        WhereExpr::And(left, right) => WhereExpr::And(
            Box::new(normalize_where_expr(*left, table_alias, table_name)),
            Box::new(normalize_where_expr(*right, table_alias, table_name)),
        ),
        WhereExpr::Or(left, right) => WhereExpr::Or(
            Box::new(normalize_where_expr(*left, table_alias, table_name)),
            Box::new(normalize_where_expr(*right, table_alias, table_name)),
        ),
        WhereExpr::Not(expr) => WhereExpr::Not(Box::new(normalize_where_expr(
            *expr,
            table_alias,
            table_name,
        ))),
    }
}

fn normalize_column_reference(
    column: String,
    table_alias: Option<&str>,
    table_name: &str,
) -> String {
    let Some(alias) = table_alias else {
        return column;
    };
    let prefix = format!("{}.", alias);
    if let Some(stripped) = column.strip_prefix(&prefix) {
        return stripped.to_string();
    }
    let table_prefix = format!("{}.", table_name);
    if let Some(stripped) = column.strip_prefix(&table_prefix) {
        return stripped.to_string();
    }
    column
}

fn dedup_selected_rows(rows: &mut Vec<Vec<Value>>) {
    let mut seen = BTreeMap::<String, ()>::new();
    rows.retain(|row| {
        let key = format!("{:?}", row);
        seen.insert(key, ()).is_none()
    });
}

fn filtered_rows(rows: Vec<Row>, where_clause: Option<&ResolvedWhereExpr>) -> Vec<Row> {
    rows.into_iter()
        .filter(|row| eval_where_expr(row, where_clause))
        .collect()
}

fn parse_sql_to_statement(sql: &str) -> Result<Statement> {
    let mut statements = Parser::parse_multiple(sql)?;
    Ok(statements.remove(0))
}

fn materialized_schema(table_name: &str, columns: &[String], rows: &[Vec<Value>]) -> TableSchema {
    TableSchema {
        table_name: table_name.to_string(),
        columns: columns
            .iter()
            .enumerate()
            .map(|(index, name)| ColumnDef {
                name: name.clone(),
                data_type: infer_materialized_type(rows, index),
            })
            .collect(),
    }
}

fn infer_materialized_type(rows: &[Vec<Value>], index: usize) -> DataType {
    for row in rows {
        match row.get(index) {
            Some(Value::Int(_)) => return DataType::Int,
            Some(Value::Text(_)) => return DataType::Text,
            Some(Value::Bool(_)) => return DataType::Bool,
            Some(Value::Null) | None => {}
        }
    }
    DataType::Text
}

fn materialized_rows(columns: &[String], rows: Vec<Vec<Value>>) -> Vec<Row> {
    rows.into_iter()
        .map(|values| Row::new(columns.iter().cloned().zip(values).collect()))
        .collect()
}

// 中文註解：遞迴執行布林 WHERE/HAVING 條件，讓 Row、JOIN 結果與聚合列共用同一套邏輯。
fn eval_where_expr<T: ValueLookup>(row: &T, where_clause: Option<&ResolvedWhereExpr>) -> bool {
    let Some(where_clause) = where_clause else {
        return true;
    };

    match where_clause {
        ResolvedWhereExpr::Comparison {
            column,
            operator,
            value,
        } => row
            .lookup(column)
            .is_some_and(|left| compare_values(left, value, operator.clone())),
        ResolvedWhereExpr::Between { column, low, high } => {
            row.lookup(column).is_some_and(|value| {
                compare_values(value, low, Operator::Ge)
                    && compare_values(value, high, Operator::Le)
            })
        }
        ResolvedWhereExpr::Like { column, pattern } => row
            .lookup(column)
            .is_some_and(|value| matches_like_pattern(value, pattern)),
        ResolvedWhereExpr::IsNull { column, negated } => {
            let is_null = row
                .lookup(column)
                .is_none_or(|value| matches!(value, Value::Null));
            if *negated {
                !is_null
            } else {
                is_null
            }
        }
        ResolvedWhereExpr::InValues { column, values } => {
            row.lookup(column).is_some_and(|left| values.contains(left))
        }
        ResolvedWhereExpr::And(left, right) => {
            eval_where_expr(row, Some(left)) && eval_where_expr(row, Some(right))
        }
        ResolvedWhereExpr::Or(left, right) => {
            eval_where_expr(row, Some(left)) || eval_where_expr(row, Some(right))
        }
        ResolvedWhereExpr::Not(expr) => !eval_where_expr(row, Some(expr)),
    }
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

fn estimate_filtered_rows(total_rows: usize, where_clause: Option<&WhereExpr>) -> usize {
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
        SelectItem::Column { name, alias } => alias.clone().unwrap_or_else(|| name.clone()),
        SelectItem::Aggregate {
            func,
            column,
            alias,
        } => alias
            .clone()
            .unwrap_or_else(|| match (func, column.as_deref()) {
                (AggregateFunc::Count, None) => "COUNT(*)".to_string(),
                (AggregateFunc::Count, Some(column)) => format!("COUNT({})", column),
                (AggregateFunc::Sum, Some(column)) => format!("SUM({})", column),
                (AggregateFunc::Min, Some(column)) => format!("MIN({})", column),
                (AggregateFunc::Max, Some(column)) => format!("MAX({})", column),
                (_, None) => "INVALID_AGGREGATE".to_string(),
            }),
    }
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
        let order = compare_sort_order(left_value, right_value);
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
        let order = compare_sort_order(left_value, right_value);
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

impl ValueLookup for Row {
    fn lookup(&self, column: &str) -> Option<&Value> {
        self.get(column)
    }
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

fn group_rows(rows: Vec<Row>, group_by: Option<&GroupByClause>) -> Vec<(Option<Value>, Vec<Row>)> {
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

fn schema_with_removed_column(schema: &TableSchema, removed_column: &str) -> TableSchema {
    let mut old_schema = schema.clone();
    old_schema
        .columns
        .retain(|column| column.name != removed_column);
    old_schema
}

fn evaluate_select_item(item: &SelectItem, rows: &[Row]) -> std::result::Result<Value, String> {
    match item {
        SelectItem::Column { name, .. } => Ok(rows
            .first()
            .and_then(|row| row.get(name))
            .cloned()
            .unwrap_or(Value::Null)),
        SelectItem::Aggregate { func, column, .. } => {
            evaluate_aggregate(func, column.as_deref(), rows)
        }
    }
}

fn evaluate_aggregate(
    func: &AggregateFunc,
    column: Option<&str>,
    rows: &[Row],
) -> std::result::Result<Value, String> {
    match func {
        // 中文註解：COUNT(*) 計入所有列，COUNT(column) 只計算非 NULL 值。
        AggregateFunc::Count => {
            let count = match column {
                Some(column) => rows
                    .iter()
                    .filter(|row| !matches!(row.get(column).unwrap_or(&Value::Null), Value::Null))
                    .count(),
                None => rows.len(),
            };
            Ok(Value::Int(count as i64))
        }
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

fn matches_like_pattern(value: &Value, pattern: &str) -> bool {
    let Value::Text(text) = value else {
        return false;
    };

    // 中文註解：目前 LIKE 只支援 `%foo`、`foo%`、`%foo%` 與完全相等四種簡單樣式。
    if pattern.starts_with('%') && pattern.ends_with('%') && pattern.len() >= 2 {
        return text.contains(&pattern[1..pattern.len() - 1]);
    }
    if let Some(suffix) = pattern.strip_prefix('%') {
        return text.ends_with(suffix);
    }
    if let Some(prefix) = pattern.strip_suffix('%') {
        return text.starts_with(prefix);
    }
    text == pattern
}

fn compare_sort_order(left: &Value, right: &Value) -> std::cmp::Ordering {
    // 中文註解：排序時固定把 NULL 放到最後，避免 ASC/DESC 時被混進正常值前面。
    match (left, right) {
        (Value::Null, Value::Null) => std::cmp::Ordering::Equal,
        (Value::Null, _) => std::cmp::Ordering::Greater,
        (_, Value::Null) => std::cmp::Ordering::Less,
        _ => compare_order(left, right).unwrap_or(std::cmp::Ordering::Equal),
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
    // 中文註解：JOIN 結果會把左右表欄位都展平成一列；LEFT JOIN 沒命中時右表欄位補 NULL。
    fn new(
        left_schema: &TableSchema,
        left_row: &Row,
        right_schema: &TableSchema,
        right_row: Option<&Row>,
    ) -> Self {
        let mut columns = Vec::new();
        for column in &left_schema.columns {
            if let Some(value) = left_row.get(&column.name) {
                columns.push((
                    format!("{}.{}", left_schema.table_name, column.name),
                    value.clone(),
                ));
            }
        }
        for column in &right_schema.columns {
            let value = right_row
                .and_then(|row| row.get(&column.name))
                .cloned()
                .unwrap_or(Value::Null);
            columns.push((
                format!("{}.{}", right_schema.table_name, column.name),
                value,
            ));
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

impl ValueLookup for JoinedRow {
    fn lookup(&self, column: &str) -> Option<&Value> {
        self.get(column)
    }
}

#[derive(Debug, Clone)]
struct ProjectedRow {
    columns: Vec<(String, Value)>,
}

impl ProjectedRow {
    // 中文註解：把聚合輸出欄位與值組成一列，讓 HAVING 可以像一般 WHERE 一樣查值。
    fn new(columns: &[String], values: &[Value]) -> Self {
        Self {
            columns: columns
                .iter()
                .cloned()
                .zip(values.iter().cloned())
                .collect(),
        }
    }
}

impl ValueLookup for ProjectedRow {
    fn lookup(&self, column: &str) -> Option<&Value> {
        self.columns
            .iter()
            .find(|(name, _)| name == column)
            .map(|(_, value)| value)
    }
}

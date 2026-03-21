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

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::{Arc, Mutex};

use crate::error::Result;
use crate::transaction::mvcc::{MvccEngine, PreparedStatement, Transaction};

use super::ast::{
    AggregateFunc, Assignment, CTE, ColumnDef, DataType, Expr, GroupByClause, InsertSource,
    IsolationLevel, JoinClause, JoinType, Operator, OrderByClause, OrderDirection,
    SelectColumns, SelectItem, Statement, Value, WhereExpr, WindowFunc,
};
use super::catalog::{Catalog, TableSchema, ViewDefinition};
use super::index::IndexManager;
use super::optimizer::{Optimizer, Plan, QueryPlanNode};
use super::parser::Parser;
use super::plan_cache::{PlanCache, PlanCacheStats};
use super::row::{
    decode_row_key, encode_row_key, encode_row_prefix_end, encode_row_prefix_start, Row,
};
use super::statistics::StatisticsManager;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ExecuteResult {
    Explain {
        plan: String,
    },
    Prepared {
        name: String,
    },
    Deallocated {
        name: String,
    },
    IsolationLevelSet {
        level: IsolationLevel,
    },
    Analyzed {
        table_name: String,
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
        column_names: Vec<String>,
    },
    IndexDropped {
        table_name: String,
        column_names: Vec<String>,
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

#[derive(Debug)]
pub struct SqlExecutor {
    engine: Arc<MvccEngine>,
    catalog: Catalog,
    index_manager: IndexManager,
    statistics: StatisticsManager,
    optimizer: Optimizer,
    plan_cache: Mutex<PlanCache>,
}

impl SqlExecutor {
    pub fn new(engine: Arc<MvccEngine>) -> Self {
        let catalog = Catalog::new(Arc::clone(&engine));
        let index_manager = IndexManager::new(Arc::clone(&engine));
        let statistics = StatisticsManager::new(Arc::clone(&engine));
        let optimizer = Optimizer::new(catalog.clone(), statistics.clone(), index_manager.clone());
        Self {
            engine,
            catalog,
            index_manager,
            statistics,
            optimizer,
            plan_cache: Mutex::new(PlanCache::new(100)),
        }
    }

    pub fn plan_cache_stats(&self) -> PlanCacheStats {
        self.plan_cache
            .lock()
            .expect("plan cache lock")
            .stats()
    }

    pub fn execute(&self, stmt: Statement) -> Result<ExecuteResult> {
        match stmt {
            Statement::Explain { statement } => self.execute_explain(*statement),
            Statement::Prepare { name, params, body } => self.execute_prepare(name, params, *body),
            Statement::Execute { name, args } => self.execute_prepared(name, args),
            Statement::Deallocate { name } => self.execute_deallocate(name),
            Statement::SetIsolationLevel { level } => self.execute_set_isolation_level(level),
            Statement::AnalyzeTable { table_name } => self.execute_analyze_table(table_name),
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
                column_names,
            } => self.execute_create_index(table_name, column_names),
            Statement::DropIndex {
                table_name,
                column_names,
            } => self.execute_drop_index(table_name, column_names),
            Statement::Insert { table_name, source } => self.execute_insert(table_name, source),
            Statement::Select {
                ctes,
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
                ctes,
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
                from_table,
                join_condition,
                where_clause,
            } => self.execute_update(
                table_name,
                assignments,
                from_table,
                join_condition,
                where_clause,
            ),
            Statement::Delete {
                table_name,
                using_table,
                join_condition,
                where_clause,
            } => self.execute_delete(table_name, using_table, join_condition, where_clause),
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
        self.invalidate_plan_cache(&table_name);
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
        self.invalidate_plan_cache(&table_name);
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

        for index_columns in self.index_manager.list_indexes(&txn, &table_name)? {
            if index_columns.iter().any(|column| column == &column_name) {
                self.index_manager
                    .drop_index_in_txn(&mut txn, &table_name, &index_columns)?;
            }
        }

        schema.columns.retain(|column| column.name != column_name);
        txn.put(
            crate::sql::catalog::encode_schema_key(&table_name),
            serde_json::to_vec(&schema)?,
        )?;

        txn.commit()?;
        self.invalidate_plan_cache(&table_name);
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
        self.invalidate_plan_cache(&table_name);
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
        column_names: Vec<String>,
    ) -> Result<ExecuteResult> {
        let mut txn = self.engine.begin_transaction();
        let Some(schema) = self.catalog.get_table(&txn, &table_name)? else {
            return Ok(ExecuteResult::Error {
                message: format!("table '{}' does not exist", table_name),
            });
        };
        if column_names.is_empty() {
            return Ok(ExecuteResult::Error {
                message: "CREATE INDEX requires at least one column".to_string(),
            });
        }
        for column_name in &column_names {
            if !schema.columns.iter().any(|column| column.name == *column_name) {
                return Ok(ExecuteResult::Error {
                    message: format!("unknown column '{}'", column_name),
                });
            }
        }
        if !self
            .index_manager
            .create_index_in_txn(&mut txn, &table_name, &column_names)?
        {
            return Ok(ExecuteResult::Error {
                message: format!(
                    "index on '{}.{}' already exists",
                    table_name,
                    column_names.join(",")
                ),
            });
        }

        txn.commit()?;
        self.invalidate_plan_cache(&table_name);
        Ok(ExecuteResult::IndexCreated {
            table_name,
            column_names,
        })
    }

    fn execute_drop_index(
        &self,
        table_name: String,
        column_names: Vec<String>,
    ) -> Result<ExecuteResult> {
        let mut txn = self.engine.begin_transaction();
        if !self
            .index_manager
            .drop_index_in_txn(&mut txn, &table_name, &column_names)?
        {
            return Ok(ExecuteResult::Error {
                message: format!(
                    "index on '{}.{}' does not exist",
                    table_name,
                    column_names.join(",")
                ),
            });
        }

        txn.commit()?;
        self.invalidate_plan_cache(&table_name);
        Ok(ExecuteResult::IndexDropped {
            table_name,
            column_names,
        })
    }

    fn execute_analyze_table(&self, table_name: String) -> Result<ExecuteResult> {
        let mut txn = self.engine.begin_transaction();
        let analyzed = self.statistics.analyze_table(&mut txn, &table_name)?;
        if analyzed.is_none() {
            return Ok(ExecuteResult::Error {
                message: format!("table '{}' does not exist", table_name),
            });
        }
        txn.commit()?;
        self.invalidate_plan_cache(&table_name);
        self.engine.invalidate_prepared_statement_plans();
        Ok(ExecuteResult::Analyzed { table_name })
    }

    fn invalidate_plan_cache(&self, table_name: &str) {
        self.plan_cache
            .lock()
            .expect("plan cache lock")
            .invalidate_table(table_name);
        self.engine.invalidate_prepared_statement_plans();
    }

    fn get_or_optimize_plan(&self, statement: &Statement) -> Result<QueryPlanNode> {
        let key_sql = serde_json::to_string(statement)?;
        let key = PlanCache::compute_key(&key_sql);
        if let Some(plan) = self
            .plan_cache
            .lock()
            .expect("plan cache lock")
            .get(key)
        {
            return Ok(plan);
        }

        let txn = self.engine.begin_transaction();
        let plan = self.optimizer.optimize_select(&txn, statement)?;
        let tables = tables_in_statement(statement);
        self.plan_cache
            .lock()
            .expect("plan cache lock")
            .put(key, tables, plan.clone());
        Ok(plan)
    }

    fn execute_explain(&self, statement: Statement) -> Result<ExecuteResult> {
        match statement {
            Statement::Select { .. } => Ok(ExecuteResult::Explain {
                plan: Optimizer::format_plan_tree(&self.get_or_optimize_plan(&statement)?),
            }),
            other => Ok(ExecuteResult::Error {
                message: format!("EXPLAIN does not support {:?}", other),
            }),
        }
    }

    fn execute_prepare(
        &self,
        name: String,
        params: Vec<String>,
        body: Statement,
    ) -> Result<ExecuteResult> {
        let prepared = PreparedStatement {
            ast: body,
            param_count: params.len(),
            cached_plan: None,
        };
        self.engine.store_prepared_statement(name.clone(), prepared);
        Ok(ExecuteResult::Prepared { name })
    }

    fn execute_prepared(&self, name: String, args: Vec<Value>) -> Result<ExecuteResult> {
        let Some(prepared) = self.engine.get_prepared_statement(&name) else {
            return Ok(ExecuteResult::Error {
                message: format!("prepared statement '{}' does not exist", name),
            });
        };
        if args.len() != prepared.param_count {
            return Ok(ExecuteResult::Error {
                message: format!(
                    "prepared statement '{}' expects {} parameter(s), got {}",
                    name,
                    prepared.param_count,
                    args.len()
                ),
            });
        }

        let mut params = HashMap::new();
        for (index, value) in args.into_iter().enumerate() {
            params.insert(index + 1, value);
        }
        let substituted = substitute_statement_placeholders(&prepared.ast, &params)?;

        let result = self.execute(substituted.clone())?;
        if let Statement::Select { .. } = substituted {
            let plan = self.get_or_optimize_plan(&substituted)?;
            self.engine
                .update_prepared_statement_plan(&name, Some(plan));
        }
        Ok(result)
    }

    fn execute_deallocate(&self, name: String) -> Result<ExecuteResult> {
        if !self.engine.remove_prepared_statement(&name) {
            return Ok(ExecuteResult::Error {
                message: format!("prepared statement '{}' does not exist", name),
            });
        }
        Ok(ExecuteResult::Deallocated { name })
    }

    fn execute_set_isolation_level(&self, level: IsolationLevel) -> Result<ExecuteResult> {
        self.engine.set_isolation_level(level.clone());
        Ok(ExecuteResult::IsolationLevelSet { level })
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

        self.statistics.mark_stale(&mut txn, &table_name)?;
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
        ctes: Vec<CTE>,
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
        let statement_for_plan = Statement::Select {
            ctes: ctes.clone(),
            distinct,
            table_name: table_name.clone(),
            table_alias: table_alias.clone(),
            columns: columns.clone(),
            join: join.clone(),
            where_clause: where_clause.clone(),
            group_by: group_by.clone(),
            having: having.clone(),
            order_by: order_by.clone(),
            limit,
        };
        let txn = self.engine.begin_transaction();
        let cte_scope = self.materialize_ctes(&ctes)?;
        if let Some(relation) = cte_scope.get(&table_name) {
            return self.execute_select_from_materialized_rows(
                &txn,
                relation.schema.clone(),
                relation.rows.clone(),
                distinct,
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
        let optimized_plan = if ctes.is_empty() {
            Some(self.get_or_optimize_plan(&statement_for_plan)?)
        } else {
            None
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
            expr: order
                .expr
                .map(|expr| normalize_expr(expr, table_alias.as_deref(), &table_name)),
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
                optimized_plan.as_ref(),
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

        let mut rows: Vec<Row> = self
            .fetch_rows_for_select(&txn, &schema, where_clause.as_ref(), optimized_plan.as_ref())?
            .into_iter()
            .map(|(_, row)| row)
            .collect();
        if let Some(order_by) = order_by.as_ref() {
            if order_by.expr.is_some() {
                sort_plain_rows_by_order_expr(&mut rows, order_by);
            }
        }
        let output_columns = projection
            .iter()
            .map(|item| item.header.clone())
            .collect::<Vec<_>>();
        let window_values = build_window_projection_values(&rows, &projection)?;
        let mut selected = rows
            .iter()
            .enumerate()
            .map(|(row_index, row)| {
                projection
                    .iter()
                    .enumerate()
                    .map(|(projection_index, column)| {
                        evaluate_projection_value(
                            row,
                            column,
                            window_values.get(projection_index),
                            row_index,
                        )
                    })
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();
        if distinct {
            dedup_selected_rows(&mut selected);
        }
        if let Some(order_by) = order_by.as_ref() {
            if order_by.expr.is_none() {
                if let Err(message) =
                    sort_result_rows_by_order(&mut selected, &output_columns, order_by)
                {
                    return Ok(ExecuteResult::Error { message });
                }
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
            .fetch_rows_for_select(txn, schema, where_clause.as_ref(), None)?
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
        optimized_plan: Option<&QueryPlanNode>,
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
        let use_hash_join = optimized_plan.is_some_and(plan_uses_hash_join);
        if use_hash_join {
            let hash_rows = match build_hash_join_rows(
                left_schema,
                &left_rows,
                &right_schema,
                &right_rows,
                &join,
            ) {
                Ok(rows) => rows,
                Err(message) => return Ok(ExecuteResult::Error { message }),
            };
            for joined in hash_rows {
                if eval_where_expr(&joined, resolved_where.as_ref()) {
                    matched_rows.push(joined);
                }
            }
        } else {
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
                match &column.kind {
                    ProjectionKind::Column(name) => {
                        let Some(value) = joined.get(name) else {
                            return Ok(ExecuteResult::Error {
                                message: format!("unknown column '{}'", name),
                            });
                        };
                        row.push(value.clone());
                    }
                    ProjectionKind::Expression(_) => {
                        return Ok(ExecuteResult::Error {
                            message: "expression projection is not supported for JOIN queries"
                                .to_string(),
                        });
                    }
                }
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
            expr: order
                .expr
                .map(|expr| normalize_expr(expr, table_alias.as_deref(), &table_name)),
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
        let mut filtered = filtered_rows(source_rows, resolved_where.as_ref());
        if let Some(order_by) = order_by.as_ref() {
            if order_by.expr.is_some() {
                sort_plain_rows_by_order_expr(&mut filtered, order_by);
            }
        }
        let window_values = build_window_projection_values(&filtered, &projection)?;
        let mut selected = filtered
            .iter()
            .enumerate()
            .map(|(row_index, row)| {
                projection
                    .iter()
                    .enumerate()
                    .map(|(projection_index, column)| {
                        evaluate_projection_value(
                            row,
                            column,
                            window_values.get(projection_index),
                            row_index,
                        )
                    })
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();
        if distinct {
            dedup_selected_rows(&mut selected);
        }
        if let Some(order_by) = order_by.as_ref() {
            if order_by.expr.is_none() {
                if let Err(message) =
                    sort_result_rows_by_order(&mut selected, &output_columns, order_by)
                {
                    return Ok(ExecuteResult::Error { message });
                }
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
                match &column.kind {
                    ProjectionKind::Column(name) => {
                        let Some(value) = joined.get(name) else {
                            return Ok(ExecuteResult::Error {
                                message: format!("unknown column '{}'", name),
                            });
                        };
                        row.push(value.clone());
                    }
                    ProjectionKind::Expression(_) => {
                        return Ok(ExecuteResult::Error {
                            message: "expression projection is not supported for JOIN queries"
                                .to_string(),
                        });
                    }
                }
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
        from_table: Option<String>,
        join_condition: Option<WhereExpr>,
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
        let target_keys = if let Some(from_table) = from_table {
            Some(self.resolve_target_rows_via_join(
                &txn,
                &schema,
                &rows,
                &from_table,
                join_condition.as_ref().or(where_clause.as_ref()),
            )?)
        } else {
            None
        };
        let mut updated = 0;
        for (row_key, mut row) in rows {
            let matches_row = if let Some(target_keys) = target_keys.as_ref() {
                target_keys.contains(&row_key)
            } else {
                eval_where_expr(&row, resolved_where.as_ref())
            };
            if !matches_row {
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
            for indexed_columns in self.index_manager.list_indexes(&txn, &table_name)? {
                let old_values = indexed_columns
                    .iter()
                    .map(|column| old_row.get(column).cloned().unwrap_or(Value::Null))
                    .collect::<Vec<_>>();
                let new_values = indexed_columns
                    .iter()
                    .map(|column| row.get(column).cloned().unwrap_or(Value::Null))
                    .collect::<Vec<_>>();
                if old_values != new_values {
                    self.index_manager.delete_index_entry(
                        &mut txn,
                        &table_name,
                        &indexed_columns,
                        &old_row,
                        &pk_value,
                    )?;
                    self.index_manager.insert_index_entry(
                        &mut txn,
                        &table_name,
                        &indexed_columns,
                        &row,
                        &pk_value,
                    )?;
                }
            }
            updated += 1;
        }

        self.statistics.mark_stale(&mut txn, &table_name)?;
        txn.commit()?;
        Ok(ExecuteResult::Updated { count: updated })
    }

    // 中文註解：DELETE 與 UPDATE 共用同一套布林 WHERE evaluator，避免條件語意不一致。
    fn execute_delete(
        &self,
        table_name: String,
        using_table: Option<String>,
        join_condition: Option<WhereExpr>,
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
        let target_keys = if let Some(using_table) = using_table {
            Some(self.resolve_target_rows_via_join(
                &txn,
                &schema,
                &rows,
                &using_table,
                join_condition.as_ref().or(where_clause.as_ref()),
            )?)
        } else {
            None
        };
        let mut deleted = 0;
        for (row_key, row) in rows {
            let matches_row = if let Some(target_keys) = target_keys.as_ref() {
                target_keys.contains(&row_key)
            } else {
                eval_where_expr(&row, resolved_where.as_ref())
            };
            if !matches_row {
                continue;
            }
            let pk_value = row
                .get(&schema.columns[0].name)
                .cloned()
                .unwrap_or(Value::Null);
            for indexed_columns in self.index_manager.list_indexes(&txn, &table_name)? {
                self.index_manager.delete_index_entry(
                    &mut txn,
                    &table_name,
                    &indexed_columns,
                    &row,
                    &pk_value,
                )?;
            }
            txn.delete(&row_key)?;
            deleted += 1;
        }

        self.statistics.mark_stale(&mut txn, &table_name)?;
        txn.commit()?;
        Ok(ExecuteResult::Deleted { count: deleted })
    }

    fn materialize_ctes(&self, ctes: &[CTE]) -> Result<BTreeMap<String, MaterializedRelation>> {
        let mut scope = BTreeMap::new();
        for cte in ctes {
            let result = self.execute((*cte.query).clone())?;
            let ExecuteResult::Selected { columns, rows } = result else {
                continue;
            };
            scope.insert(
                cte.name.clone(),
                MaterializedRelation {
                    schema: materialized_schema(&cte.name, &columns, &rows),
                    rows: materialized_rows(&columns, rows),
                },
            );
        }
        Ok(scope)
    }

    fn resolve_target_rows_via_join(
        &self,
        txn: &Transaction,
        left_schema: &TableSchema,
        left_rows: &[(Vec<u8>, Row)],
        right_table: &str,
        predicate: Option<&WhereExpr>,
    ) -> Result<HashSet<Vec<u8>>> {
        let Some(right_schema) = self.catalog.get_table(txn, right_table)? else {
            return Ok(HashSet::new());
        };
        let right_rows = self.scan_rows(txn, &right_schema)?;
        let resolved_where = self.resolve_where_expr(predicate)?;
        let mut matched = HashSet::new();
        for (left_key, left_row) in left_rows {
            for (_, right_row) in &right_rows {
                let joined = JoinedRow::new(left_schema, left_row, &right_schema, Some(right_row));
                if eval_where_expr(&joined, resolved_where.as_ref()) {
                    matched.insert(left_key.clone());
                    break;
                }
            }
        }
        Ok(matched)
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
                    kind: ProjectionKind::Column(column.name.clone()),
                    header: column.name.clone(),
                })
                .collect()),
            SelectColumns::Named(items) => {
                let mut projection = Vec::with_capacity(items.len());
                for item in items {
                    match item {
                        SelectItem::Column { name, alias } => {
                            if !schema.columns.iter().any(|column| column.name == *name) {
                                return Err(format!("unknown column '{}'", name));
                            }
                            projection.push(ProjectionColumn {
                                kind: ProjectionKind::Column(name.clone()),
                                header: alias.clone().unwrap_or_else(|| name.clone()),
                            });
                        }
                        SelectItem::Expression { expr, alias } => {
                            validate_expr_columns(expr, schema)?;
                            projection.push(ProjectionColumn {
                                kind: ProjectionKind::Expression(expr.clone()),
                                header: alias.clone().unwrap_or_else(|| render_expr(expr)),
                            });
                        }
                        SelectItem::Aggregate { .. } => {
                            return Err(
                                "aggregate columns require aggregate execution path".to_string()
                            );
                        }
                    }
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
                SelectItem::Expression { .. } => {
                    return Err("expressions are not supported in aggregate SELECT yet".to_string());
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
            for indexed_columns in self.index_manager.list_indexes(txn, table_name)? {
                self.index_manager
                    .insert_index_entry(txn, table_name, &indexed_columns, &row, &pk_value)?;
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
        optimized_plan: Option<&QueryPlanNode>,
    ) -> Result<Vec<(Vec<u8>, Row)>> {
        let resolved_where = self.resolve_where_expr(where_clause)?;
        let planned_lookup = optimized_plan
            .and_then(find_scan_lookup)
            .filter(|(table, _, _)| table == &schema.table_name)
            .map(|(_, columns, values)| (columns, values));
        let fallback_lookup = self
            .index_manager
            .find_best_index(txn, &schema.table_name, where_clause)?
            .map(|plan| (plan.scan_columns, plan.prefix_values));
        if let Some((scan_columns, prefix_values)) = planned_lookup.or(fallback_lookup) {
            let pks = self
                .index_manager
                .lookup_prefix(txn, &schema.table_name, &scan_columns, &prefix_values)?;
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
            WhereExpr::PlaceholderComparison {
                column,
                operator,
                placeholder,
            } => Err(crate::error::FerrisDbError::InvalidCommand(format!(
                "placeholder ${} in column '{}' was not bound before execution ({})",
                placeholder, column, operator_to_str(operator)
            ))),
            WhereExpr::ColumnComparison {
                left,
                operator,
                right,
            } => Ok(ResolvedWhereExpr::ColumnComparison {
                left: left.clone(),
                operator: operator.clone(),
                right: right.clone(),
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
                    kind: ProjectionKind::Column(column.clone()),
                    header: column,
                })
                .collect()),
            SelectColumns::Named(items) => {
                let mut projection = Vec::with_capacity(items.len());
                for item in items {
                    match item {
                        SelectItem::Column { name, alias } => {
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
                                            || format!(
                                                "{}.{}",
                                                right_schema.table_name, column.name
                                            ) == *name
                                    })
                                    .count();
                                if left_matches + right_matches != 1 {
                                    return Err(format!("unknown or ambiguous column '{}'", name));
                                }
                            }
                            projection.push(ProjectionColumn {
                                kind: ProjectionKind::Column(name.clone()),
                                header: alias.clone().unwrap_or_else(|| name.clone()),
                            });
                        }
                        _ => {
                            return Err("expression projection is not supported for JOIN queries"
                                .to_string());
                        }
                    }
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
        ExecuteResult::Prepared { name } => format!("Prepared statement '{}' created", name),
        ExecuteResult::Deallocated { name } => {
            format!("Prepared statement '{}' deallocated", name)
        }
        ExecuteResult::IsolationLevelSet { level } => {
            format!("Transaction isolation level set to {}", isolation_level_name(level))
        }
        ExecuteResult::Analyzed { table_name } => format!("Table '{}' analyzed", table_name),
        ExecuteResult::Created { table_name } => format!("Table '{}' created", table_name),
        ExecuteResult::Altered { table_name } => format!("Table '{}' altered", table_name),
        ExecuteResult::Dropped { table_name } => format!("Table '{}' dropped", table_name),
        ExecuteResult::IndexCreated {
            table_name,
            column_names,
        } => format!("Index on '{}.{}' created", table_name, column_names.join(",")),
        ExecuteResult::IndexDropped {
            table_name,
            column_names,
        } => format!("Index on '{}.{}' dropped", table_name, column_names.join(",")),
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
    ColumnComparison {
        left: String,
        operator: Operator,
        right: String,
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
    kind: ProjectionKind,
    header: String,
}

#[derive(Debug, Clone)]
enum ProjectionKind {
    Column(String),
    Expression(Expr),
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
        WhereExpr::PlaceholderComparison {
            column,
            operator,
            placeholder,
        } => format!("{} {} ${}", column, operator_to_str(operator), placeholder),
        WhereExpr::ColumnComparison {
            left,
            operator,
            right,
        } => format!("{} {} {}", left, operator_to_str(operator), right),
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
        SelectItem::Expression { expr, alias } => SelectItem::Expression {
            expr: normalize_expr(expr, table_alias, table_name),
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

fn normalize_expr(expr: Expr, table_alias: Option<&str>, table_name: &str) -> Expr {
    match expr {
        Expr::Value(value) => Expr::Value(value),
        Expr::Column(column) => {
            Expr::Column(normalize_column_reference(column, table_alias, table_name))
        }
        Expr::Placeholder(index) => Expr::Placeholder(index),
        Expr::CaseWhen {
            conditions,
            else_result,
        } => Expr::CaseWhen {
            conditions: conditions
                .into_iter()
                .map(|(condition, result)| {
                    (
                        normalize_where_expr(condition, table_alias, table_name),
                        normalize_expr(result, table_alias, table_name),
                    )
                })
                .collect(),
            else_result: else_result
                .map(|expr| Box::new(normalize_expr(*expr, table_alias, table_name))),
        },
        Expr::WindowFunction {
            func,
            target_column,
            partition_by,
            order_by,
        } => Expr::WindowFunction {
            func,
            target_column: target_column
                .map(|column| normalize_column_reference(column, table_alias, table_name)),
            partition_by: partition_by
                .map(|column| normalize_column_reference(column, table_alias, table_name)),
            order_by: order_by.map(|(column, asc)| {
                (
                    normalize_column_reference(column, table_alias, table_name),
                    asc,
                )
            }),
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
        WhereExpr::PlaceholderComparison {
            column,
            operator,
            placeholder,
        } => WhereExpr::PlaceholderComparison {
            column: normalize_column_reference(column, table_alias, table_name),
            operator,
            placeholder,
        },
        WhereExpr::ColumnComparison {
            left,
            operator,
            right,
        } => WhereExpr::ColumnComparison {
            left: normalize_column_reference(left, table_alias, table_name),
            operator,
            right: normalize_column_reference(right, table_alias, table_name),
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

fn tables_in_statement(statement: &Statement) -> Vec<String> {
    match statement {
        Statement::Select {
            table_name,
            join,
            ctes: _,
            ..
        } => {
            let mut tables = vec![table_name.clone()];
            if let Some(join) = join {
                tables.push(join.right_table.clone());
            }
            tables
        }
        _ => Vec::new(),
    }
}

fn plan_uses_hash_join(plan: &QueryPlanNode) -> bool {
    match &plan.plan {
        Plan::HashJoin { .. } => true,
        Plan::NestedLoopJoin { left, right, .. } => plan_uses_hash_join(left) || plan_uses_hash_join(right),
        Plan::Sort { input, .. }
        | Plan::Limit { input, .. }
        | Plan::Project { input, .. }
        | Plan::Aggregate { input, .. } => plan_uses_hash_join(input),
        Plan::SeqScan { .. } | Plan::IndexScan { .. } | Plan::CompositeIndexScan { .. } => false,
    }
}

fn find_scan_lookup(plan: &QueryPlanNode) -> Option<(String, Vec<String>, Vec<Value>)> {
    match &plan.plan {
        Plan::IndexScan {
            table,
            index_columns,
            lookup_value,
            ..
        } => Some((
            table.clone(),
            index_columns.clone(),
            lookup_value.iter().cloned().collect::<Vec<_>>(),
        )),
        Plan::CompositeIndexScan {
            table,
            index_columns,
            prefix_values,
            ..
        } => Some((table.clone(), index_columns.clone(), prefix_values.clone())),
        Plan::Sort { input, .. }
        | Plan::Limit { input, .. }
        | Plan::Project { input, .. }
        | Plan::Aggregate { input, .. } => find_scan_lookup(input),
        Plan::NestedLoopJoin { left, .. } | Plan::HashJoin { left, .. } => find_scan_lookup(left),
        Plan::SeqScan { .. } => None,
    }
}

fn build_hash_join_rows(
    left_schema: &TableSchema,
    left_rows: &[(Vec<u8>, Row)],
    right_schema: &TableSchema,
    right_rows: &[(Vec<u8>, Row)],
    join: &JoinClause,
) -> std::result::Result<Vec<JoinedRow>, String> {
    let mut hash = BTreeMap::<String, Vec<Row>>::new();
    for (_, right_row) in right_rows {
        let Some(value) = resolve_join_value(right_row, &right_schema.table_name, &join.right_column)
        else {
            return Err(format!("unknown join column '{}'", join.right_column));
        };
        hash.entry(format!("{:?}", value))
            .or_default()
            .push(right_row.clone());
    }

    let mut joined_rows = Vec::new();
    for (_, left_row) in left_rows {
        let Some(value) = resolve_join_value(left_row, &left_schema.table_name, &join.left_column)
        else {
            return Err(format!("unknown join column '{}'", join.left_column));
        };
        if let Some(matches) = hash.get(&format!("{:?}", value)) {
            for right_row in matches {
                joined_rows.push(JoinedRow::new(
                    left_schema,
                    left_row,
                    right_schema,
                    Some(right_row),
                ));
            }
        } else if matches!(join.join_type, JoinType::Left) {
            joined_rows.push(JoinedRow::new(left_schema, left_row, right_schema, None));
        }
    }
    Ok(joined_rows)
}

#[derive(Debug, Clone)]
struct MaterializedRelation {
    schema: TableSchema,
    rows: Vec<Row>,
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

fn substitute_statement_placeholders(
    statement: &Statement,
    params: &HashMap<usize, Value>,
) -> Result<Statement> {
    Ok(match statement {
        Statement::Select {
            ctes,
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
        } => Statement::Select {
            ctes: ctes
                .iter()
                .map(|cte| {
                    Ok(CTE {
                        name: cte.name.clone(),
                        query: Box::new(substitute_statement_placeholders(&cte.query, params)?),
                    })
                })
                .collect::<Result<Vec<_>>>()?,
            distinct: *distinct,
            table_name: table_name.clone(),
            table_alias: table_alias.clone(),
            columns: substitute_select_columns(columns, params)?,
            join: join.clone(),
            where_clause: where_clause
                .as_ref()
                .map(|expr| substitute_where_expr(expr, params))
                .transpose()?,
            group_by: group_by.clone(),
            having: having
                .as_ref()
                .map(|expr| substitute_where_expr(expr, params))
                .transpose()?,
            order_by: order_by
                .as_ref()
                .map(|order| -> Result<OrderByClause> {
                    Ok(OrderByClause {
                        column: order.column.clone(),
                        expr: order
                            .expr
                            .as_ref()
                            .map(|expr| substitute_expr(expr, params))
                            .transpose()?,
                        direction: order.direction.clone(),
                    })
                })
                .transpose()?,
            limit: *limit,
        },
        Statement::Update {
            table_name,
            assignments,
            from_table,
            join_condition,
            where_clause,
        } => Statement::Update {
            table_name: table_name.clone(),
            assignments: assignments.clone(),
            from_table: from_table.clone(),
            join_condition: join_condition
                .as_ref()
                .map(|expr| substitute_where_expr(expr, params))
                .transpose()?,
            where_clause: where_clause
                .as_ref()
                .map(|expr| substitute_where_expr(expr, params))
                .transpose()?,
        },
        Statement::Delete {
            table_name,
            using_table,
            join_condition,
            where_clause,
        } => Statement::Delete {
            table_name: table_name.clone(),
            using_table: using_table.clone(),
            join_condition: join_condition
                .as_ref()
                .map(|expr| substitute_where_expr(expr, params))
                .transpose()?,
            where_clause: where_clause
                .as_ref()
                .map(|expr| substitute_where_expr(expr, params))
                .transpose()?,
        },
        Statement::Union { left, right, all } => Statement::Union {
            left: Box::new(substitute_statement_placeholders(left, params)?),
            right: Box::new(substitute_statement_placeholders(right, params)?),
            all: *all,
        },
        other => other.clone(),
    })
}

fn substitute_select_columns(columns: &SelectColumns, params: &HashMap<usize, Value>) -> Result<SelectColumns> {
    Ok(match columns {
        SelectColumns::All => SelectColumns::All,
        SelectColumns::Named(items) => SelectColumns::Named(
            items.iter()
                .map(|item| substitute_select_item(item, params))
                .collect::<Result<Vec<_>>>()?,
        ),
        SelectColumns::Aggregate(items) => SelectColumns::Aggregate(
            items.iter()
                .map(|item| substitute_select_item(item, params))
                .collect::<Result<Vec<_>>>()?,
        ),
    })
}

fn substitute_select_item(item: &SelectItem, params: &HashMap<usize, Value>) -> Result<SelectItem> {
    Ok(match item {
        SelectItem::Column { name, alias } => SelectItem::Column {
            name: name.clone(),
            alias: alias.clone(),
        },
        SelectItem::Expression { expr, alias } => SelectItem::Expression {
            expr: substitute_expr(expr, params)?,
            alias: alias.clone(),
        },
        SelectItem::Aggregate {
            func,
            column,
            alias,
        } => SelectItem::Aggregate {
            func: func.clone(),
            column: column.clone(),
            alias: alias.clone(),
        },
    })
}

fn substitute_expr(expr: &Expr, params: &HashMap<usize, Value>) -> Result<Expr> {
    Ok(match expr {
        Expr::Value(value) => Expr::Value(value.clone()),
        Expr::Column(column) => Expr::Column(column.clone()),
        Expr::Placeholder(index) => Expr::Value(resolve_placeholder(*index, params)?),
        Expr::CaseWhen {
            conditions,
            else_result,
        } => Expr::CaseWhen {
            conditions: conditions
                .iter()
                .map(|(condition, result)| {
                    Ok((
                        substitute_where_expr(condition, params)?,
                        substitute_expr(result, params)?,
                    ))
                })
                .collect::<Result<Vec<_>>>()?,
            else_result: else_result
                .as_ref()
                .map(|expr| substitute_expr(expr, params).map(Box::new))
                .transpose()?,
        },
        Expr::WindowFunction {
            func,
            target_column,
            partition_by,
            order_by,
        } => Expr::WindowFunction {
            func: func.clone(),
            target_column: target_column.clone(),
            partition_by: partition_by.clone(),
            order_by: order_by.clone(),
        },
    })
}

fn substitute_where_expr(expr: &WhereExpr, params: &HashMap<usize, Value>) -> Result<WhereExpr> {
    Ok(match expr {
        WhereExpr::Comparison {
            column,
            operator,
            value,
        } => WhereExpr::Comparison {
            column: column.clone(),
            operator: operator.clone(),
            value: value.clone(),
        },
        WhereExpr::PlaceholderComparison {
            column,
            operator,
            placeholder,
        } => WhereExpr::Comparison {
            column: column.clone(),
            operator: operator.clone(),
            value: resolve_placeholder(*placeholder, params)?,
        },
        WhereExpr::ColumnComparison {
            left,
            operator,
            right,
        } => WhereExpr::ColumnComparison {
            left: left.clone(),
            operator: operator.clone(),
            right: right.clone(),
        },
        WhereExpr::Between { column, low, high } => WhereExpr::Between {
            column: column.clone(),
            low: low.clone(),
            high: high.clone(),
        },
        WhereExpr::Like { column, pattern } => WhereExpr::Like {
            column: column.clone(),
            pattern: pattern.clone(),
        },
        WhereExpr::IsNull { column, negated } => WhereExpr::IsNull {
            column: column.clone(),
            negated: *negated,
        },
        WhereExpr::InSubquery { column, subquery } => WhereExpr::InSubquery {
            column: column.clone(),
            subquery: Box::new(substitute_statement_placeholders(subquery, params)?),
        },
        WhereExpr::And(left, right) => WhereExpr::And(
            Box::new(substitute_where_expr(left, params)?),
            Box::new(substitute_where_expr(right, params)?),
        ),
        WhereExpr::Or(left, right) => WhereExpr::Or(
            Box::new(substitute_where_expr(left, params)?),
            Box::new(substitute_where_expr(right, params)?),
        ),
        WhereExpr::Not(inner) => {
            WhereExpr::Not(Box::new(substitute_where_expr(inner, params)?))
        }
    })
}

fn resolve_placeholder(index: usize, params: &HashMap<usize, Value>) -> Result<Value> {
    params.get(&index).cloned().ok_or_else(|| {
        crate::error::FerrisDbError::InvalidCommand(format!(
            "missing value for placeholder ${}",
            index
        ))
    })
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

fn validate_expr_columns(expr: &Expr, schema: &TableSchema) -> std::result::Result<(), String> {
    match expr {
        Expr::Value(_) | Expr::Placeholder(_) => Ok(()),
        Expr::Column(column) => {
            if schema.columns.iter().any(|item| item.name == *column) {
                Ok(())
            } else {
                Err(format!("unknown column '{}'", column))
            }
        }
        Expr::CaseWhen {
            conditions,
            else_result,
        } => {
            for (condition, result) in conditions {
                validate_where_expr_columns(condition, schema)?;
                validate_expr_columns(result, schema)?;
            }
            if let Some(result) = else_result {
                validate_expr_columns(result, schema)?;
            }
            Ok(())
        }
        Expr::WindowFunction {
            func: _,
            target_column,
            partition_by,
            order_by,
        } => {
            if let Some(column) = target_column {
                validate_expr_columns(&Expr::Column(column.clone()), schema)?;
            }
            if let Some(column) = partition_by {
                validate_expr_columns(&Expr::Column(column.clone()), schema)?;
            }
            if let Some((column, _)) = order_by {
                validate_expr_columns(&Expr::Column(column.clone()), schema)?;
            }
            Ok(())
        }
    }
}

fn validate_where_expr_columns(
    expr: &WhereExpr,
    schema: &TableSchema,
) -> std::result::Result<(), String> {
    match expr {
        WhereExpr::Comparison { column, .. }
        | WhereExpr::PlaceholderComparison { column, .. }
        | WhereExpr::Between { column, .. }
        | WhereExpr::Like { column, .. }
        | WhereExpr::IsNull { column, .. }
        | WhereExpr::InSubquery { column, .. } => {
            validate_expr_columns(&Expr::Column(column.clone()), schema)
        }
        WhereExpr::ColumnComparison { left, right, .. } => {
            validate_expr_columns(&Expr::Column(left.clone()), schema)?;
            validate_expr_columns(&Expr::Column(right.clone()), schema)
        }
        WhereExpr::And(left, right) | WhereExpr::Or(left, right) => {
            validate_where_expr_columns(left, schema)?;
            validate_where_expr_columns(right, schema)
        }
        WhereExpr::Not(inner) => validate_where_expr_columns(inner, schema),
    }
}

fn render_expr(expr: &Expr) -> String {
    match expr {
        Expr::Value(value) => render_value(value),
        Expr::Column(column) => column.clone(),
        Expr::Placeholder(index) => format!("${}", index),
        Expr::CaseWhen {
            conditions,
            else_result,
        } => {
            let mut parts = vec!["CASE".to_string()];
            for (condition, result) in conditions {
                parts.push(format!(
                    "WHEN {} THEN {}",
                    render_where_expr(condition),
                    render_expr(result)
                ));
            }
            if let Some(result) = else_result {
                parts.push(format!("ELSE {}", render_expr(result)));
            }
            parts.push("END".to_string());
            parts.join(" ")
        }
        Expr::WindowFunction {
            func,
            target_column,
            partition_by,
            order_by,
        } => {
            let func_str = match func {
                WindowFunc::RowNumber => "ROW_NUMBER()".to_string(),
                WindowFunc::Rank => "RANK()".to_string(),
                WindowFunc::WinCount => format!(
                    "COUNT({})",
                    target_column.clone().unwrap_or_else(|| "*".to_string())
                ),
                WindowFunc::WinSum => format!(
                    "SUM({})",
                    target_column.clone().unwrap_or_else(|| "?".to_string())
                ),
            };
            let mut over_parts = Vec::new();
            if let Some(column) = partition_by {
                over_parts.push(format!("PARTITION BY {}", column));
            }
            if let Some((column, asc)) = order_by {
                over_parts.push(format!(
                    "ORDER BY {} {}",
                    column,
                    if *asc { "ASC" } else { "DESC" }
                ));
            }
            format!("{} OVER ({})", func_str, over_parts.join(" "))
        }
    }
}

fn evaluate_projection_value(
    row: &Row,
    projection: &ProjectionColumn,
    window_values: Option<&Vec<Value>>,
    row_index: usize,
) -> Value {
    match &projection.kind {
        ProjectionKind::Column(name) => row.get(name).cloned().unwrap_or(Value::Null),
        ProjectionKind::Expression(expr) => match expr {
            Expr::WindowFunction { .. } => window_values
                .and_then(|values| values.get(row_index).cloned())
                .unwrap_or(Value::Null),
            _ => eval_expr_on_row(row, expr),
        },
    }
}

fn eval_expr_on_row(row: &Row, expr: &Expr) -> Value {
    match expr {
        Expr::Value(value) => value.clone(),
        Expr::Column(column) => row.get(column).cloned().unwrap_or(Value::Null),
        Expr::Placeholder(_) => Value::Null,
        Expr::CaseWhen {
            conditions,
            else_result,
        } => {
            for (condition, result) in conditions {
                if eval_where_expr_unresolved(row, condition) {
                    return eval_expr_on_row(row, result);
                }
            }
            else_result
                .as_ref()
                .map(|result| eval_expr_on_row(row, result))
                .unwrap_or(Value::Null)
        }
        Expr::WindowFunction { .. } => Value::Null,
    }
}

fn eval_where_expr_unresolved<T: ValueLookup>(row: &T, expr: &WhereExpr) -> bool {
    match expr {
        WhereExpr::Comparison {
            column,
            operator,
            value,
        } => row
            .lookup(column)
            .is_some_and(|left| compare_values(left, value, operator.clone())),
        WhereExpr::PlaceholderComparison { .. } => false,
        WhereExpr::ColumnComparison {
            left,
            operator,
            right,
        } => row
            .lookup(left)
            .zip(row.lookup(right))
            .is_some_and(|(lhs, rhs)| compare_values(lhs, rhs, operator.clone())),
        WhereExpr::Between { column, low, high } => row.lookup(column).is_some_and(|value| {
            compare_values(value, low, Operator::Ge) && compare_values(value, high, Operator::Le)
        }),
        WhereExpr::Like { column, pattern } => row
            .lookup(column)
            .is_some_and(|value| matches_like_pattern(value, pattern)),
        WhereExpr::IsNull { column, negated } => {
            let is_null = row
                .lookup(column)
                .is_none_or(|value| matches!(value, Value::Null));
            if *negated {
                !is_null
            } else {
                is_null
            }
        }
        WhereExpr::InSubquery { .. } => false,
        WhereExpr::And(left, right) => {
            eval_where_expr_unresolved(row, left) && eval_where_expr_unresolved(row, right)
        }
        WhereExpr::Or(left, right) => {
            eval_where_expr_unresolved(row, left) || eval_where_expr_unresolved(row, right)
        }
        WhereExpr::Not(inner) => !eval_where_expr_unresolved(row, inner),
    }
}

fn build_window_projection_values(
    rows: &[Row],
    projection: &[ProjectionColumn],
) -> Result<Vec<Vec<Value>>> {
    let mut values = Vec::with_capacity(projection.len());
    for column in projection {
        match &column.kind {
            ProjectionKind::Column(_) => values.push(Vec::new()),
            ProjectionKind::Expression(expr) => {
                values.push(compute_window_expr_values(rows, expr)?);
            }
        }
    }
    Ok(values)
}

fn compute_window_expr_values(rows: &[Row], expr: &Expr) -> Result<Vec<Value>> {
    let Expr::WindowFunction {
        func,
        target_column,
        partition_by,
        order_by,
    } = expr
    else {
        return Ok(Vec::new());
    };

    let mut result = vec![Value::Null; rows.len()];
    let mut partitions: BTreeMap<String, Vec<usize>> = BTreeMap::new();
    for (index, row) in rows.iter().enumerate() {
        let key = partition_by
            .as_ref()
            .and_then(|column| row.get(column))
            .map(value_group_key)
            .unwrap_or_else(|| "__all__".to_string());
        partitions.entry(key).or_default().push(index);
    }

    for indices in partitions.values() {
        let mut ordered = indices.clone();
        if let Some((column, asc)) = order_by {
            ordered.sort_by(|left, right| {
                let left_value = rows[*left].get(column).unwrap_or(&Value::Null);
                let right_value = rows[*right].get(column).unwrap_or(&Value::Null);
                let order = compare_sort_order(left_value, right_value);
                if *asc {
                    order
                } else {
                    order.reverse()
                }
            });
        }

        match func {
            WindowFunc::RowNumber => {
                for (position, row_index) in ordered.iter().enumerate() {
                    result[*row_index] = Value::Int((position + 1) as i64);
                }
            }
            WindowFunc::Rank => {
                let mut last_value: Option<Value> = None;
                let mut current_rank = 1_i64;
                for (position, row_index) in ordered.iter().enumerate() {
                    let current_value = order_by
                        .as_ref()
                        .and_then(|(column, _)| rows[*row_index].get(column))
                        .cloned()
                        .unwrap_or(Value::Null);
                    if position > 0 && Some(current_value.clone()) != last_value {
                        current_rank = (position + 1) as i64;
                    }
                    result[*row_index] = Value::Int(current_rank);
                    last_value = Some(current_value);
                }
            }
            WindowFunc::WinCount => {
                let count = match target_column {
                    Some(column) => indices
                        .iter()
                        .filter(|index| {
                            !matches!(
                                rows[**index].get(column).unwrap_or(&Value::Null),
                                Value::Null
                            )
                        })
                        .count() as i64,
                    None => indices.len() as i64,
                };
                for row_index in indices {
                    result[*row_index] = Value::Int(count);
                }
            }
            WindowFunc::WinSum => {
                let Some(column) = target_column else {
                    return Ok(result);
                };
                let mut sum = 0_i64;
                for row_index in indices {
                    match rows[*row_index].get(column).unwrap_or(&Value::Null) {
                        Value::Int(value) => sum += value,
                        Value::Null => {}
                        _ => return Ok(result),
                    }
                }
                for row_index in indices {
                    result[*row_index] = Value::Int(sum);
                }
            }
        }
    }

    Ok(result)
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
        ResolvedWhereExpr::ColumnComparison {
            left,
            operator,
            right,
        } => row
            .lookup(left)
            .zip(row.lookup(right))
            .is_some_and(|(lhs, rhs)| compare_values(lhs, rhs, operator.clone())),
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

fn isolation_level_name(level: &IsolationLevel) -> &'static str {
    match level {
        IsolationLevel::ReadCommitted => "READ COMMITTED",
        IsolationLevel::RepeatableRead => "REPEATABLE READ",
        IsolationLevel::Serializable => "SERIALIZABLE",
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

fn render_select_item(item: &SelectItem) -> String {
    match item {
        SelectItem::Column { name, alias } => alias.clone().unwrap_or_else(|| name.clone()),
        SelectItem::Expression { expr, alias } => {
            alias.clone().unwrap_or_else(|| render_expr(expr))
        }
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

fn sort_plain_rows_by_order_expr(rows: &mut [Row], order_by: &OrderByClause) {
    let Some(expr) = order_by.expr.as_ref() else {
        return;
    };
    rows.sort_by(|left, right| {
        let left_value = eval_expr_on_row(left, expr);
        let right_value = eval_expr_on_row(right, expr);
        let order = compare_sort_order(&left_value, &right_value);
        match order_by.direction {
            OrderDirection::Asc => order,
            OrderDirection::Desc => order.reverse(),
        }
    });
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
        SelectItem::Expression { .. } => {
            Err("expressions are not supported in aggregate SELECT yet".to_string())
        }
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

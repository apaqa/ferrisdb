use crate::error::Result;
use crate::transaction::mvcc::Transaction;

use super::ast::{GroupByClause, JoinClause, OrderByClause, SelectColumns, Statement, Value, WhereExpr};
use super::catalog::Catalog;
use super::index::{IndexLookupPlan, IndexManager};
use super::statistics::{ColumnStatistics, StatisticsManager, TableStatistics};

#[derive(Debug, Clone, PartialEq)]
pub struct QueryPlanNode {
    pub plan: Plan,
    pub estimated_rows: usize,
    pub estimated_cost: f64,
    pub reason: String,
}

#[derive(Debug, Clone, PartialEq)]
pub enum Plan {
    SeqScan {
        table: String,
        filter: Option<WhereExpr>,
    },
    IndexScan {
        table: String,
        index_columns: Vec<String>,
        lookup_value: Option<Value>,
        filter: Option<WhereExpr>,
    },
    CompositeIndexScan {
        table: String,
        index_columns: Vec<String>,
        prefix_values: Vec<Value>,
        filter: Option<WhereExpr>,
    },
    NestedLoopJoin {
        left: Box<QueryPlanNode>,
        right: Box<QueryPlanNode>,
        condition: JoinClause,
    },
    HashJoin {
        left: Box<QueryPlanNode>,
        right: Box<QueryPlanNode>,
        left_key: String,
        right_key: String,
    },
    Sort {
        input: Box<QueryPlanNode>,
        order_by: OrderByClause,
    },
    Limit {
        input: Box<QueryPlanNode>,
        count: usize,
    },
    Project {
        input: Box<QueryPlanNode>,
        columns: SelectColumns,
    },
    Aggregate {
        input: Box<QueryPlanNode>,
        group_by: Option<GroupByClause>,
        aggregates: SelectColumns,
    },
}

#[derive(Debug, Clone)]
pub struct Optimizer {
    catalog: Catalog,
    statistics: StatisticsManager,
    index_manager: IndexManager,
}

impl Optimizer {
    pub fn new(catalog: Catalog, statistics: StatisticsManager, index_manager: IndexManager) -> Self {
        Self {
            catalog,
            statistics,
            index_manager,
        }
    }

    pub fn optimize_select(&self, txn: &Transaction, statement: &Statement) -> Result<QueryPlanNode> {
        let Statement::Select {
            ctes: _,
            distinct: _,
            table_name,
            table_alias: _,
            columns,
            join,
            where_clause,
            group_by,
            having: _,
            order_by,
            limit,
        } = statement
        else {
            return Ok(QueryPlanNode {
                plan: Plan::SeqScan {
                    table: "__unsupported__".to_string(),
                    filter: None,
                },
                estimated_rows: 0,
                estimated_cost: 0.0,
                reason: "optimizer currently only supports SELECT".to_string(),
            });
        };

        let mut root = self.choose_scan_plan(txn, table_name, where_clause.clone())?;

        if let Some(join_clause) = join.clone() {
            let right = self.choose_scan_plan(txn, &join_clause.right_table, None)?;
            let right_stats = self.table_stats(txn, &join_clause.right_table)?;
            root = if right_stats.row_count < 1000 {
                QueryPlanNode {
                    estimated_rows: root.estimated_rows.max(1).min(right.estimated_rows.max(1)),
                    estimated_cost: root.estimated_cost + right.estimated_cost + (right_stats.row_count as f64),
                    reason: format!(
                        "NestedLoopJoin because right table '{}' is small ({} rows)",
                        join_clause.right_table, right_stats.row_count
                    ),
                    plan: Plan::NestedLoopJoin {
                        left: Box::new(root),
                        right: Box::new(right),
                        condition: join_clause,
                    },
                }
            } else {
                QueryPlanNode {
                    estimated_rows: root.estimated_rows.max(1).min(right.estimated_rows.max(1)),
                    estimated_cost: root.estimated_cost + right.estimated_cost,
                    reason: format!(
                        "HashJoin because right table '{}' is large ({} rows)",
                        join_clause.right_table, right_stats.row_count
                    ),
                    plan: Plan::HashJoin {
                        left: Box::new(root),
                        right: Box::new(right),
                        left_key: join_clause.left_column,
                        right_key: join_clause.right_column,
                    },
                }
            };
        }

        if matches!(columns, SelectColumns::Aggregate(_)) || group_by.is_some() {
            root = QueryPlanNode {
                estimated_rows: root.estimated_rows.max(1),
                estimated_cost: root.estimated_cost + root.estimated_rows as f64 * 0.2,
                reason: "Aggregate because SELECT contains GROUP BY or aggregate columns".to_string(),
                plan: Plan::Aggregate {
                    input: Box::new(root),
                    group_by: group_by.clone(),
                    aggregates: columns.clone(),
                },
            };
        }

        if let Some(order_by) = order_by.clone() {
            root = QueryPlanNode {
                estimated_rows: root.estimated_rows,
                estimated_cost: root.estimated_cost + root.estimated_rows.max(1) as f64 * 0.1,
                reason: "Sort because query contains ORDER BY".to_string(),
                plan: Plan::Sort {
                    input: Box::new(root),
                    order_by,
                },
            };
        }

        if let Some(limit_count) = limit {
            root = QueryPlanNode {
                estimated_rows: (*limit_count).min(root.estimated_rows.max(1)),
                estimated_cost: root.estimated_cost + 0.05,
                reason: "Limit because query contains LIMIT".to_string(),
                plan: Plan::Limit {
                    input: Box::new(root),
                    count: *limit_count,
                },
            };
        }

        Ok(QueryPlanNode {
            estimated_rows: root.estimated_rows,
            estimated_cost: root.estimated_cost + root.estimated_rows as f64 * 0.05,
            reason: "Project because final result needs SELECT columns".to_string(),
            plan: Plan::Project {
                input: Box::new(root),
                columns: columns.clone(),
            },
        })
    }

    pub fn format_plan_tree(plan: &QueryPlanNode) -> String {
        let mut lines = Vec::new();
        render_plan(plan, 0, &mut lines);
        lines.join("\n")
    }

    fn choose_scan_plan(
        &self,
        txn: &Transaction,
        table_name: &str,
        filter: Option<WhereExpr>,
    ) -> Result<QueryPlanNode> {
        let row_stats = self.table_stats(txn, table_name)?;
        if let Some(index_plan) = self.index_manager.find_best_index(txn, table_name, filter.as_ref())? {
            return Ok(self.index_scan_plan(table_name, filter, row_stats, index_plan, txn)?);
        }

        Ok(QueryPlanNode {
            plan: Plan::SeqScan {
                table: table_name.to_string(),
                filter,
            },
            estimated_rows: row_stats.row_count.max(1),
            estimated_cost: row_stats.row_count.max(1) as f64,
            reason: format!("SeqScan because no usable index was found for '{}'", table_name),
        })
    }

    fn index_scan_plan(
        &self,
        table_name: &str,
        filter: Option<WhereExpr>,
        row_stats: TableStatistics,
        index_plan: IndexLookupPlan,
        txn: &Transaction,
    ) -> Result<QueryPlanNode> {
        let first_column = index_plan
            .scan_columns
            .first()
            .cloned()
            .unwrap_or_default();
        let column_stats = self
            .statistics
            .get_column_stats(txn, table_name, &first_column)?
            .unwrap_or(ColumnStatistics {
                table_name: table_name.to_string(),
                column_name: first_column.clone(),
                ndv: row_stats.row_count.max(1),
                min: None,
                max: None,
            });
        let estimated_rows = (row_stats.row_count.max(1) / column_stats.ndv.max(1)).max(1);
        let estimated_cost = estimated_rows as f64;
        if index_plan.scan_columns.len() > 1 {
            Ok(QueryPlanNode {
                plan: Plan::CompositeIndexScan {
                    table: table_name.to_string(),
                    index_columns: index_plan.scan_columns.clone(),
                    prefix_values: index_plan.prefix_values.clone(),
                    filter,
                },
                estimated_rows,
                estimated_cost,
                reason: format!(
                    "CompositeIndexScan because composite index ({}) matches the WHERE prefix",
                    index_plan.scan_columns.join(",")
                ),
            })
        } else {
            Ok(QueryPlanNode {
                plan: Plan::IndexScan {
                    table: table_name.to_string(),
                    index_columns: index_plan.scan_columns.clone(),
                    lookup_value: index_plan.prefix_values.first().cloned(),
                    filter,
                },
                estimated_rows,
                estimated_cost,
                reason: format!(
                    "IndexScan because column '{}' has an equality predicate and index",
                    first_column
                ),
            })
        }
    }

    fn table_stats(&self, txn: &Transaction, table_name: &str) -> Result<TableStatistics> {
        if let Some(stats) = self.statistics.get_table_stats(txn, table_name)? {
            if !stats.stale {
                return Ok(stats);
            }
        }

        let row_count = self
            .catalog
            .get_table(txn, table_name)?
            .map(|schema| {
                let start = super::row::encode_row_prefix_start(&schema.table_name);
                let end = super::row::encode_row_prefix_end(&schema.table_name);
                txn.scan(&start, &end).map(|rows| rows.len()).unwrap_or(0)
            })
            .unwrap_or(0);

        Ok(TableStatistics {
            table_name: table_name.to_string(),
            row_count,
            stale: true,
        })
    }
}

fn render_plan(plan: &QueryPlanNode, depth: usize, lines: &mut Vec<String>) {
    let indent = "  ".repeat(depth);
    let filter = match &plan.plan {
        Plan::SeqScan { filter, .. }
        | Plan::IndexScan { filter, .. }
        | Plan::CompositeIndexScan { filter, .. } => filter.as_ref(),
        _ => None,
    };
    let name = match &plan.plan {
        Plan::SeqScan { table, .. } => format!("SeqScan(table={})", table),
        Plan::IndexScan { table, index_columns, .. } => {
            format!("IndexScan(table={}, index={})", table, index_columns.join(","))
        }
        Plan::CompositeIndexScan {
            table,
            index_columns,
            ..
        } => format!(
            "CompositeIndexScan(table={}, index={})",
            table,
            index_columns.join(",")
        ),
        Plan::NestedLoopJoin { condition, .. } => format!(
            "NestedLoopJoin(condition={}={})",
            condition.left_column, condition.right_column
        ),
        Plan::HashJoin {
            left_key,
            right_key,
            ..
        } => format!("HashJoin(keys={}={})", left_key, right_key),
        Plan::Sort { order_by, .. } => format!("Sort(order_by={})", order_by.column),
        Plan::Limit { count, .. } => format!("Limit(count={})", count),
        Plan::Project { .. } => "Project".to_string(),
        Plan::Aggregate { group_by, .. } => format!(
            "Aggregate(group_by={})",
            group_by
                .as_ref()
                .map(|group| group.column.clone())
                .unwrap_or_else(|| "none".to_string())
        ),
    };
    lines.push(format!(
        "{}{} rows={} cost={:.2} reason={}",
        indent, name, plan.estimated_rows, plan.estimated_cost, plan.reason
    ));
    if let Some(filter) = filter {
        lines.push(format!(
            "{}  Filter(predicate=\"{}\")",
            indent,
            render_where_expr(filter)
        ));
    }

    match &plan.plan {
        Plan::NestedLoopJoin { left, right, .. } => {
            render_plan(left, depth + 1, lines);
            render_plan(right, depth + 1, lines);
        }
        Plan::HashJoin { left, right, .. } => {
            render_plan(left, depth + 1, lines);
            render_plan(right, depth + 1, lines);
        }
        Plan::Sort { input, .. }
        | Plan::Limit { input, .. }
        | Plan::Project { input, .. }
        | Plan::Aggregate { input, .. } => render_plan(input, depth + 1, lines),
        Plan::SeqScan { .. } | Plan::IndexScan { .. } | Plan::CompositeIndexScan { .. } => {}
    }
}

fn render_where_expr(where_clause: &WhereExpr) -> String {
    match where_clause {
        WhereExpr::Comparison {
            column,
            operator,
            value,
        } => format!("{} {} {}", column, operator_to_str(operator), render_value(value)),
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
        WhereExpr::Between { column, low, high } => format!(
            "{} BETWEEN {} AND {}",
            column,
            render_value(low),
            render_value(high)
        ),
        WhereExpr::Like { column, pattern } => format!("{} LIKE {}", column, pattern),
        WhereExpr::IsNull { column, negated } => {
            if *negated {
                format!("{} IS NOT NULL", column)
            } else {
                format!("{} IS NULL", column)
            }
        }
        WhereExpr::ExprComparison {
            left,
            operator,
            right,
        } => format!(
            "{} {} {}",
            render_expr(left),
            operator_to_str(operator),
            render_expr(right)
        ),
        WhereExpr::InSubquery { column, .. } => format!("{} IN (subquery)", column),
        WhereExpr::And(left, right) => {
            format!("({} AND {})", render_where_expr(left), render_where_expr(right))
        }
        WhereExpr::Or(left, right) => {
            format!("({} OR {})", render_where_expr(left), render_where_expr(right))
        }
        WhereExpr::Not(inner) => format!("(NOT {})", render_where_expr(inner)),
    }
}

fn render_expr(expr: &super::ast::Expr) -> String {
    match expr {
        super::ast::Expr::Value(value) => render_value(value),
        super::ast::Expr::Column(column) => column.clone(),
        super::ast::Expr::Placeholder(index) => format!("${}", index),
        super::ast::Expr::Variable(name) => name.clone(),
        super::ast::Expr::JsonExtract { column, path } => {
            format!("JSON_EXTRACT({}, '{}')", column, path)
        }
        super::ast::Expr::JsonSet { column, path, value } => {
            format!("JSON_SET({}, '{}', {})", column, path, render_expr(value))
        }
        super::ast::Expr::CaseWhen { .. } => "CASE".to_string(),
        super::ast::Expr::WindowFunction { .. } => "WINDOW".to_string(),
    }
}

fn operator_to_str(operator: &super::ast::Operator) -> &'static str {
    match operator {
        super::ast::Operator::Eq => "=",
        super::ast::Operator::Ne => "!=",
        super::ast::Operator::Lt => "<",
        super::ast::Operator::Gt => ">",
        super::ast::Operator::Le => "<=",
        super::ast::Operator::Ge => ">=",
    }
}

fn render_value(value: &Value) -> String {
    match value {
        Value::Int(v) => v.to_string(),
        Value::Text(v) => v.clone(),
        Value::Bool(v) => v.to_string(),
        Value::Null => "NULL".to_string(),
        Value::Variable(name) => name.clone(),
    }
}

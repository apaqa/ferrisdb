// =============================================================================
// sql/ast.rs — SQL 抽象語法樹（AST）
// =============================================================================
//
// AST（Abstract Syntax Tree）是 parser 的輸出。
// 它把 SQL 的字串形式轉成結構化資料，讓後續執行器不需要再處理字串細節。
//
// 例如：
//   SELECT name FROM users WHERE id = 1;
//
// 解析後會變成一個 Statement::Select，
// 其中 table_name / columns / where_clause 都是明確欄位。

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Statement {
    Explain {
        statement: Box<Statement>,
    },
    CreateTable {
        table_name: String,
        if_not_exists: bool,
        columns: Vec<ColumnDef>,
    },
    AlterTableAdd {
        table_name: String,
        column: ColumnDef,
    },
    AlterTableDropColumn {
        table_name: String,
        column_name: String,
    },
    DropTable {
        table_name: String,
        if_exists: bool,
    },
    CreateIndex {
        table_name: String,
        column_name: String,
    },
    DropIndex {
        table_name: String,
        column_name: String,
    },
    Insert {
        table_name: String,
        values: Vec<Vec<Value>>,
    },
    Select {
        table_name: String,
        columns: SelectColumns,
        join: Option<JoinClause>,
        where_clause: Option<WhereClause>,
        group_by: Option<GroupByClause>,
        order_by: Option<OrderByClause>,
        limit: Option<usize>,
    },
    Update {
        table_name: String,
        assignments: Vec<Assignment>,
        where_clause: Option<WhereClause>,
    },
    Delete {
        table_name: String,
        where_clause: Option<WhereClause>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ColumnDef {
    pub name: String,
    pub data_type: DataType,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum DataType {
    Int,
    Text,
    Bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Value {
    Int(i64),
    Text(String),
    Bool(bool),
    Null,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum SelectColumns {
    All,
    Named(Vec<String>),
    Aggregate(Vec<SelectItem>),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum SelectItem {
    Column(String),
    Aggregate {
        func: AggregateFunc,
        column: Option<String>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum AggregateFunc {
    Count,
    Sum,
    Min,
    Max,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum WhereClause {
    Comparison {
        column: String,
        operator: Operator,
        value: Value,
    },
    Subquery(SubqueryCondition),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SubqueryCondition {
    pub column: String,
    pub subquery: Box<Statement>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct JoinClause {
    pub right_table: String,
    pub left_column: String,
    pub right_column: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GroupByClause {
    pub column: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OrderByClause {
    pub column: String,
    pub direction: OrderDirection,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum OrderDirection {
    Asc,
    Desc,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Operator {
    Eq,
    Ne,
    Lt,
    Gt,
    Le,
    Ge,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Assignment {
    pub column: String,
    pub value: Value,
}

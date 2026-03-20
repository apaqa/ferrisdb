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
    CreateTable {
        table_name: String,
        columns: Vec<ColumnDef>,
    },
    Insert {
        table_name: String,
        values: Vec<Vec<Value>>,
    },
    Select {
        table_name: String,
        columns: SelectColumns,
        where_clause: Option<WhereClause>,
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
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WhereClause {
    pub column: String,
    pub operator: Operator,
    pub value: Value,
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

// =============================================================================
// sql/ast.rs -- SQL AST Definitions
// =============================================================================
//
// AST（Abstract Syntax Tree）是 parser 輸出的結構化 SQL 表示。
// 這一層不直接執行查詢，只負責把 SQL 的語意明確描述出來，
// 讓 executor 後續可以依照欄位、條件、JOIN、GROUP BY、HAVING 等資訊執行。

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
    },
    Update {
        table_name: String,
        assignments: Vec<Assignment>,
        where_clause: Option<WhereExpr>,
    },
    Delete {
        table_name: String,
        where_clause: Option<WhereExpr>,
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
    Named(Vec<SelectItem>),
    Aggregate(Vec<SelectItem>),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum SelectItem {
    Column {
        name: String,
        alias: Option<String>,
    },
    Aggregate {
        func: AggregateFunc,
        column: Option<String>,
        alias: Option<String>,
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
pub enum WhereExpr {
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
    InSubquery {
        column: String,
        subquery: Box<Statement>,
    },
    And(Box<WhereExpr>, Box<WhereExpr>),
    Or(Box<WhereExpr>, Box<WhereExpr>),
    Not(Box<WhereExpr>),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct JoinClause {
    pub join_type: JoinType,
    pub right_table: String,
    pub right_alias: Option<String>,
    pub left_column: String,
    pub right_column: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum JoinType {
    Inner,
    Left,
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

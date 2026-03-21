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
    AnalyzeTable {
        table_name: String,
    },
    Prepare {
        name: String,
        params: Vec<String>,
        body: Box<Statement>,
    },
    Execute {
        name: String,
        args: Vec<Value>,
    },
    Deallocate {
        name: String,
    },
    SetIsolationLevel {
        level: IsolationLevel,
    },
    CreateView {
        view_name: String,
        query_sql: String,
        query: Box<Statement>,
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
    DropView {
        view_name: String,
        if_exists: bool,
    },
    CreateIndex {
        table_name: String,
        // 中文註解：索引現在可同時覆蓋多個欄位，保留欄位順序供前綴匹配使用。
        column_names: Vec<String>,
    },
    DropIndex {
        table_name: String,
        // 中文註解：DROP INDEX 也改用多欄位簽名來唯一識別 composite index。
        column_names: Vec<String>,
    },
    Insert {
        table_name: String,
        source: InsertSource,
    },
    Select {
        // 中文註解：WITH 產生的 CTE 只在當前 SELECT / 查詢表達式內有效。
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
    },
    Update {
        table_name: String,
        assignments: Vec<Assignment>,
        // 中文註解：UPDATE ... FROM 會先把來源表與目標表配對，再依 join_condition 判斷是否更新。
        from_table: Option<String>,
        join_condition: Option<WhereExpr>,
        where_clause: Option<WhereExpr>,
    },
    Delete {
        table_name: String,
        // 中文註解：DELETE ... USING 會先建立目標表與來源表的 JOIN 視圖，再套用條件找刪除目標。
        using_table: Option<String>,
        join_condition: Option<WhereExpr>,
        where_clause: Option<WhereExpr>,
    },
    Union {
        left: Box<Statement>,
        right: Box<Statement>,
        all: bool,
    },
    // 中文註解：CREATE TRIGGER 定義觸發器，含 BEFORE/AFTER、INSERT/UPDATE/DELETE 以及 BEGIN...END 主體
    CreateTrigger {
        trigger_name: String,
        timing: TriggerTiming,
        event: TriggerEvent,
        table_name: String,
        body: Vec<Statement>,
    },
    // 中文註解：DROP TRIGGER 移除已存在的觸發器
    DropTrigger {
        trigger_name: String,
    },
    // 中文註解：觸發器主體內使用 SET NEW.col = val 修改即將寫入的欄位值
    TriggerSetNew {
        column: String,
        value: Value,
    },
    // 中文註解：GRANT 賦予特定使用者對某張表的操作權限
    Grant {
        privileges: Vec<Privilege>,
        table_name: String,
        user: String,
    },
    // 中文註解：REVOKE 撤銷特定使用者對某張表的操作權限
    Revoke {
        privileges: Vec<Privilege>,
        table_name: String,
        user: String,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CTE {
    pub name: String,
    pub query: Box<Statement>,
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
pub enum InsertSource {
    Values(Vec<Vec<Value>>),
    Select(Box<Statement>),
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
    Expression {
        expr: Expr,
        alias: Option<String>,
    },
    Aggregate {
        func: AggregateFunc,
        column: Option<String>,
        alias: Option<String>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Expr {
    Value(Value),
    Column(String),
    Placeholder(usize),
    CaseWhen {
        conditions: Vec<(WhereExpr, Expr)>,
        else_result: Option<Box<Expr>>,
    },
    WindowFunction {
        func: WindowFunc,
        target_column: Option<String>,
        partition_by: Option<String>,
        order_by: Option<(String, bool)>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum WindowFunc {
    RowNumber,
    Rank,
    WinCount,
    WinSum,
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
    PlaceholderComparison {
        column: String,
        operator: Operator,
        placeholder: usize,
    },
    // 中文註解：欄位對欄位比較供 UPDATE/DELETE JOIN 與進階條件共用。
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
    InSubquery {
        column: String,
        subquery: Box<Statement>,
    },
    And(Box<WhereExpr>, Box<WhereExpr>),
    Or(Box<WhereExpr>, Box<WhereExpr>),
    Not(Box<WhereExpr>),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum IsolationLevel {
    ReadCommitted,
    RepeatableRead,
    Serializable,
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
    pub expr: Option<Expr>,
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

// 中文註解：TriggerTiming 決定觸發器在 DML 操作前或後執行
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum TriggerTiming {
    Before,
    After,
}

// 中文註解：TriggerEvent 決定觸發器對哪種 DML 操作反應
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum TriggerEvent {
    Insert,
    Update,
    Delete,
}

// 中文註解：Privilege 代表資料庫操作權限類型
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Privilege {
    Select,
    Insert,
    Update,
    Delete,
    All,
}

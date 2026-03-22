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
    Vacuum {
        table_name: Option<String>,
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
    // 中文註解：CREATE MATERIALIZED VIEW 會同時保存原始查詢 SQL 與解析後查詢，供 refresh 重新計算。
    CreateMaterializedView {
        view_name: String,
        query_sql: String,
        query: Box<Statement>,
    },
    // 中文註解：REFRESH MATERIALIZED VIEW 會重新執行保存的查詢並覆寫快取資料。
    RefreshMaterializedView {
        view_name: String,
    },
    CreateProcedure {
        name: String,
        params: Vec<ProcedureParam>,
        body: Vec<Statement>,
    },
    // 中文註解：UDF 會保存參數型別、回傳型別與函式 body，供 executor 之後執行。
    CreateFunction {
        name: String,
        params: Vec<ProcedureParam>,
        return_type: DataType,
        body: Vec<Statement>,
    },
    CreateTable {
        table_name: String,
        // 中文註解：temporary=true 代表這張表只存在 executor 記憶體中，不落盤。
        temporary: bool,
        if_not_exists: bool,
        columns: Vec<ColumnDef>,
        foreign_keys: Vec<ForeignKey>,
        check_constraints: Vec<CheckConstraint>,
        // 中文註解：UNIQUE 約束用欄位名稱陣列表示，單欄 UNIQUE 會是只有一個欄位的陣列。
        unique_constraints: Vec<Vec<String>>,
        // 中文註解：partition_by 保存 RANGE 分區鍵，None 代表這不是分區表。
        partition_by: Option<String>,
        // 中文註解：partitions 依序描述每個 RANGE 分區的邊界。
        partitions: Vec<PartitionDef>,
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
        temporary: bool,
        if_exists: bool,
    },
    DropView {
        view_name: String,
        if_exists: bool,
    },
    // 中文註解：DROP MATERIALIZED VIEW 會刪除 metadata 與已快取的實體 rows。
    DropMaterializedView {
        view_name: String,
        if_exists: bool,
    },
    DropProcedure {
        name: String,
    },
    DropFunction {
        name: String,
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
    CallProcedure {
        name: String,
        args: Vec<Value>,
    },
    DeclareVariable {
        name: String,
        data_type: DataType,
    },
    DeclareCursor {
        name: String,
        query: Box<Statement>,
    },
    SetVariable {
        name: String,
        value: Expr,
    },
    // 中文註解：RETURN 只在 function body 內有意義，用來結束 UDF 並回傳值。
    Return {
        expr: Expr,
    },
    IfThenElse {
        condition: WhereExpr,
        then_body: Vec<Statement>,
        else_body: Vec<Statement>,
    },
    WhileDo {
        condition: WhereExpr,
        body: Vec<Statement>,
    },
    OpenCursor {
        name: String,
    },
    FetchCursor {
        name: String,
        variables: Vec<String>,
    },
    CloseCursor {
        name: String,
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
    pub recursive: bool,
    pub query: Box<Statement>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ColumnDef {
    pub name: String,
    pub data_type: DataType,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PartitionDef {
    pub name: String,
    pub less_than: Option<i64>,
    pub is_maxvalue: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ForeignKey {
    pub columns: Vec<String>,
    pub ref_table: String,
    pub ref_columns: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CheckConstraint {
    pub expr: WhereExpr,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProcedureParam {
    pub name: String,
    pub data_type: DataType,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum DataType {
    Int,
    Text,
    Bool,
    Json,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Value {
    Int(i64),
    Text(String),
    Bool(bool),
    Null,
    Variable(String),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum InsertSource {
    // 中文註解：INSERT ... VALUES 現在允許 expression，才能在 VALUES 裡呼叫 UDF。
    Values(Vec<Vec<Expr>>),
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
    Variable(String),
    // 中文註解：JSON_EXTRACT 會從 JSON 字串欄位依 $.a.b 路徑取出值。
    JsonExtract {
        column: String,
        path: String,
    },
    // 中文註解：JSON_SET 會回傳更新指定路徑後的 JSON 字串結果。
    JsonSet {
        column: String,
        path: String,
        value: Box<Expr>,
    },
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
    // 中文註解：FunctionCall 同時承載 UDF 與未來可擴充的 scalar function 呼叫。
    FunctionCall {
        name: String,
        args: Vec<Expr>,
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
    // 中文註解：ExprComparison 讓 WHERE / CHECK 可以比較 JSON_EXTRACT 這類函式結果。
    ExprComparison {
        left: Expr,
        operator: Operator,
        right: Expr,
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

// =============================================================================
// tests/sql_advanced_test.rs -- SQL 進階功能整合測試
// =============================================================================
//
// 這組測試聚焦在新補上的 SQL 能力：
// - DISTINCT 去重
// - AS 欄位/聚合/資料表別名
// - BETWEEN / LIKE / IS NULL / IS NOT NULL
// - NULL 排序與 COUNT(NULL) 行為

use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use ferrisdb::sql::ast::Value;
use ferrisdb::sql::executor::{ExecuteResult, SqlExecutor};
use ferrisdb::sql::lexer::Lexer;
use ferrisdb::sql::parser::Parser;
use ferrisdb::storage::lsm::LsmEngine;
use ferrisdb::transaction::mvcc::MvccEngine;

fn temp_dir(name: &str) -> std::path::PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time")
        .as_nanos();
    std::env::temp_dir().join(format!("ferrisdb-sql-advanced-{}-{}", name, nanos))
}

fn open_executor(name: &str) -> (std::path::PathBuf, SqlExecutor) {
    let dir = temp_dir(name);
    let lsm = LsmEngine::open(&dir, 4096).expect("open lsm");
    let engine = Arc::new(MvccEngine::new(lsm));
    let executor = SqlExecutor::new(Arc::clone(&engine));
    (dir, executor)
}

fn exec(executor: &SqlExecutor, sql: &str) -> ExecuteResult {
    let mut lexer = Lexer::new(sql);
    let tokens = lexer.tokenize().expect("tokenize sql");
    let mut parser = Parser::new(tokens);
    let stmt = parser.parse().expect("parse sql");
    executor.execute(stmt).expect("execute sql")
}

fn rows_only(result: ExecuteResult) -> (Vec<String>, Vec<Vec<Value>>) {
    match result {
        ExecuteResult::Selected { columns, rows } => (columns, rows),
        other => panic!("expected selected result, got {:?}", other),
    }
}

fn seed_employees(executor: &SqlExecutor) {
    exec(
        executor,
        "CREATE TABLE employees (id INT, name TEXT, department TEXT, salary INT);",
    );
    exec(
        executor,
        "INSERT INTO employees VALUES
        (1, 'Alice', 'Eng', 90000),
        (2, 'Aaron', 'Eng', 90000),
        (3, 'Bob', 'HR', 60000),
        (4, 'Anderson', NULL, 50000),
        (5, 'Jason', 'Sales', 70000),
        (6, 'Susan', NULL, 65000),
        (7, 'Andy', 'Eng', 90000),
        (8, 'Brandon', 'Sales', 70000);",
    );
}

#[test]
fn test_distinct_basic_and_where_and_order_by() {
    let (dir, executor) = open_executor("distinct");
    seed_employees(&executor);

    let (columns_basic, rows_basic) = rows_only(exec(
        &executor,
        "SELECT DISTINCT department FROM employees ORDER BY department ASC;",
    ));
    assert_eq!(columns_basic, vec!["department"]);
    assert_eq!(
        rows_basic,
        vec![
            vec![Value::Text("Eng".to_string())],
            vec![Value::Text("HR".to_string())],
            vec![Value::Text("Sales".to_string())],
            vec![Value::Null],
        ]
    );

    let (_, rows_where) = rows_only(exec(
        &executor,
        "SELECT DISTINCT department FROM employees WHERE salary >= 70000 ORDER BY department ASC;",
    ));
    assert_eq!(
        rows_where,
        vec![
            vec![Value::Text("Eng".to_string())],
            vec![Value::Text("Sales".to_string())],
        ]
    );

    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn test_as_alias_for_column_aggregate_and_table() {
    let (dir, executor) = open_executor("alias");
    seed_employees(&executor);

    let (columns_alias, rows_alias) = rows_only(exec(
        &executor,
        "SELECT name AS employee_name FROM employees ORDER BY employee_name ASC LIMIT 2;",
    ));
    assert_eq!(columns_alias, vec!["employee_name"]);
    assert_eq!(
        rows_alias,
        vec![
            vec![Value::Text("Aaron".to_string())],
            vec![Value::Text("Alice".to_string())],
        ]
    );

    let (columns_count, rows_count) = rows_only(exec(
        &executor,
        "SELECT COUNT(department) AS total_non_null, COUNT(*) AS total_rows FROM employees;",
    ));
    assert_eq!(columns_count, vec!["total_non_null", "total_rows"]);
    assert_eq!(rows_count, vec![vec![Value::Int(6), Value::Int(8)]]);

    let (columns_table_alias, rows_table_alias) = rows_only(exec(
        &executor,
        "SELECT e.name FROM employees AS e WHERE e.department = 'Eng' ORDER BY name ASC LIMIT 2;",
    ));
    assert_eq!(columns_table_alias, vec!["name"]);
    assert_eq!(
        rows_table_alias,
        vec![
            vec![Value::Text("Aaron".to_string())],
            vec![Value::Text("Alice".to_string())],
        ]
    );

    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn test_between_and_like_patterns() {
    let (dir, executor) = open_executor("between-like");
    seed_employees(&executor);

    let (_, rows_between) = rows_only(exec(
        &executor,
        "SELECT name FROM employees WHERE salary BETWEEN 60000 AND 70000 ORDER BY name ASC;",
    ));
    assert_eq!(
        rows_between,
        vec![
            vec![Value::Text("Bob".to_string())],
            vec![Value::Text("Brandon".to_string())],
            vec![Value::Text("Jason".to_string())],
            vec![Value::Text("Susan".to_string())],
        ]
    );

    let (_, rows_prefix) = rows_only(exec(
        &executor,
        "SELECT name FROM employees WHERE name LIKE 'A%' ORDER BY name ASC;",
    ));
    assert_eq!(
        rows_prefix,
        vec![
            vec![Value::Text("Aaron".to_string())],
            vec![Value::Text("Alice".to_string())],
            vec![Value::Text("Anderson".to_string())],
            vec![Value::Text("Andy".to_string())],
        ]
    );

    let (_, rows_suffix) = rows_only(exec(
        &executor,
        "SELECT name FROM employees WHERE name LIKE '%son' ORDER BY name ASC;",
    ));
    assert_eq!(
        rows_suffix,
        vec![
            vec![Value::Text("Anderson".to_string())],
            vec![Value::Text("Jason".to_string())],
        ]
    );

    let (_, rows_contains) = rows_only(exec(
        &executor,
        "SELECT name FROM employees WHERE name LIKE '%an%' ORDER BY name ASC;",
    ));
    assert_eq!(
        rows_contains,
        vec![
            vec![Value::Text("Brandon".to_string())],
            vec![Value::Text("Susan".to_string())],
        ]
    );

    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn test_is_null_null_sort_and_count_behavior() {
    let (dir, executor) = open_executor("null");
    seed_employees(&executor);

    let (_, rows_is_null) = rows_only(exec(
        &executor,
        "SELECT name FROM employees WHERE department IS NULL ORDER BY name ASC;",
    ));
    assert_eq!(
        rows_is_null,
        vec![
            vec![Value::Text("Anderson".to_string())],
            vec![Value::Text("Susan".to_string())],
        ]
    );

    let (_, rows_is_not_null) = rows_only(exec(
        &executor,
        "SELECT name FROM employees WHERE department IS NOT NULL ORDER BY name ASC LIMIT 3;",
    ));
    assert_eq!(
        rows_is_not_null,
        vec![
            vec![Value::Text("Aaron".to_string())],
            vec![Value::Text("Alice".to_string())],
            vec![Value::Text("Andy".to_string())],
        ]
    );

    let (_, rows_null_last) = rows_only(exec(
        &executor,
        "SELECT name, department FROM employees ORDER BY department ASC;",
    ));
    assert_eq!(
        rows_null_last.last(),
        Some(&vec![Value::Text("Susan".to_string()), Value::Null,])
    );
    assert_eq!(
        rows_null_last.get(rows_null_last.len() - 2),
        Some(&vec![Value::Text("Anderson".to_string()), Value::Null,])
    );

    let (_, rows_count) = rows_only(exec(
        &executor,
        "SELECT COUNT(*) AS total_rows, COUNT(department) AS non_null_departments FROM employees;",
    ));
    assert_eq!(rows_count, vec![vec![Value::Int(8), Value::Int(6)]]);

    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn test_distinct_order_by_limit_combination() {
    let (dir, executor) = open_executor("distinct-order-limit");
    seed_employees(&executor);

    let (columns, rows) = rows_only(exec(
        &executor,
        "SELECT DISTINCT salary FROM employees ORDER BY salary DESC LIMIT 2;",
    ));
    assert_eq!(columns, vec!["salary"]);
    assert_eq!(rows, vec![vec![Value::Int(90000)], vec![Value::Int(70000)]]);

    let _ = std::fs::remove_dir_all(dir);
}

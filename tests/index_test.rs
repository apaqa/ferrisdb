// =============================================================================
// tests/index_test.rs -- Secondary Index 整合測試
// =============================================================================
//
// 這組測試驗證 SQL secondary index 的三個重點：
// 1. CREATE INDEX 之後，等值 WHERE 可以改走 IndexScan
// 2. INSERT / UPDATE / DELETE 會同步維護 index entry
// 3. DROP INDEX 後，查詢計畫會回到 SeqScan
//
// 真實世界中，secondary index 是 OLTP 資料庫查詢效能的核心之一。
// 如果只靠全表掃描，資料量一大就會明顯變慢；而 index 維護錯誤又會導致查詢結果不正確。
// 所以這裡同時測「結果正確」與「計畫確實使用 index」兩個層面。

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
    std::env::temp_dir().join(format!("ferrisdb-index-test-{}-{}", name, nanos))
}

fn open_executor(name: &str) -> (std::path::PathBuf, Arc<MvccEngine>, SqlExecutor) {
    let dir = temp_dir(name);
    let lsm = LsmEngine::open(&dir, 4096).expect("open lsm");
    let engine = Arc::new(MvccEngine::new(lsm));
    let executor = SqlExecutor::new(Arc::clone(&engine));
    (dir, engine, executor)
}

fn exec(executor: &SqlExecutor, sql: &str) -> ExecuteResult {
    let mut lexer = Lexer::new(sql);
    let tokens = lexer.tokenize().expect("tokenize sql");
    let mut parser = Parser::new(tokens);
    let stmt = parser.parse().expect("parse sql");
    executor.execute(stmt).expect("execute sql")
}

fn selected_rows(result: ExecuteResult) -> (Vec<String>, Vec<Vec<Value>>) {
    match result {
        ExecuteResult::Selected { columns, rows } => (columns, rows),
        other => panic!("expected selected result, got {:?}", other),
    }
}

fn explain_plan(result: ExecuteResult) -> String {
    match result {
        ExecuteResult::Explain { plan } => plan,
        other => panic!("expected explain result, got {:?}", other),
    }
}

#[test]
fn test_create_index_and_select_where_uses_index_scan() {
    let (dir, _engine, executor) = open_executor("create-index");

    exec(
        &executor,
        "CREATE TABLE users (id INT, name TEXT, age INT, active BOOL);",
    );
    exec(
        &executor,
        "INSERT INTO users VALUES (1, 'Alice', 30, true), (2, 'Bob', 25, false), (3, 'Cara', 30, true);",
    );
    assert_eq!(
        exec(&executor, "CREATE INDEX ON users(age);"),
        ExecuteResult::IndexCreated {
            table_name: "users".to_string(),
            column_names: vec!["age".to_string()],
        }
    );

    let (_, rows) = selected_rows(exec(
        &executor,
        "SELECT name FROM users WHERE age = 30 ORDER BY name ASC;",
    ));
    assert_eq!(
        rows,
        vec![
            vec![Value::Text("Alice".to_string())],
            vec![Value::Text("Cara".to_string())],
        ]
    );

    let plan = explain_plan(exec(
        &executor,
        "EXPLAIN SELECT name FROM users WHERE age = 30;",
    ));
    assert!(plan.contains("IndexScan"));

    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn test_insert_updates_index_automatically() {
    let (dir, _engine, executor) = open_executor("insert-index");

    exec(
        &executor,
        "CREATE TABLE users (id INT, name TEXT, age INT, active BOOL);",
    );
    exec(&executor, "CREATE INDEX ON users(age);");
    exec(
        &executor,
        "INSERT INTO users VALUES (1, 'Alice', 30, true), (2, 'Bob', 30, false);",
    );

    let (_, rows) = selected_rows(exec(
        &executor,
        "SELECT name FROM users WHERE age = 30 ORDER BY name ASC;",
    ));
    assert_eq!(
        rows,
        vec![
            vec![Value::Text("Alice".to_string())],
            vec![Value::Text("Bob".to_string())],
        ]
    );

    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn test_delete_cleans_up_index_entries() {
    let (dir, _engine, executor) = open_executor("delete-index");

    exec(
        &executor,
        "CREATE TABLE users (id INT, name TEXT, age INT, active BOOL);",
    );
    exec(
        &executor,
        "INSERT INTO users VALUES (1, 'Alice', 30, true), (2, 'Bob', 30, false);",
    );
    exec(&executor, "CREATE INDEX ON users(age);");
    exec(&executor, "DELETE FROM users WHERE id = 1;");

    let (_, rows) = selected_rows(exec(
        &executor,
        "SELECT name FROM users WHERE age = 30;",
    ));
    assert_eq!(rows, vec![vec![Value::Text("Bob".to_string())]]);

    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn test_update_refreshes_index_entries() {
    let (dir, _engine, executor) = open_executor("update-index");

    exec(
        &executor,
        "CREATE TABLE users (id INT, name TEXT, age INT, active BOOL);",
    );
    exec(
        &executor,
        "INSERT INTO users VALUES (1, 'Alice', 30, true), (2, 'Bob', 25, false);",
    );
    exec(&executor, "CREATE INDEX ON users(age);");
    exec(&executor, "UPDATE users SET age = 35 WHERE id = 2;");

    let (_, old_rows) = selected_rows(exec(
        &executor,
        "SELECT name FROM users WHERE age = 25;",
    ));
    assert!(old_rows.is_empty());

    let (_, new_rows) = selected_rows(exec(
        &executor,
        "SELECT name FROM users WHERE age = 35;",
    ));
    assert_eq!(new_rows, vec![vec![Value::Text("Bob".to_string())]]);

    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn test_no_index_uses_seq_scan_and_drop_index_restores_seq_scan() {
    let (dir, _engine, executor) = open_executor("drop-index");

    exec(
        &executor,
        "CREATE TABLE users (id INT, name TEXT, age INT, active BOOL);",
    );
    exec(
        &executor,
        "INSERT INTO users VALUES (1, 'Alice', 30, true), (2, 'Bob', 25, false);",
    );

    let seq_plan = explain_plan(exec(
        &executor,
        "EXPLAIN SELECT * FROM users WHERE age = 30;",
    ));
    assert!(seq_plan.contains("SeqScan"));
    assert!(!seq_plan.contains("IndexScan"));

    exec(&executor, "CREATE INDEX ON users(age);");
    let index_plan = explain_plan(exec(
        &executor,
        "EXPLAIN SELECT * FROM users WHERE age = 30;",
    ));
    assert!(index_plan.contains("IndexScan"));

    assert_eq!(
        exec(&executor, "DROP INDEX ON users(age);"),
        ExecuteResult::IndexDropped {
            table_name: "users".to_string(),
            column_names: vec!["age".to_string()],
        }
    );

    let seq_plan_again = explain_plan(exec(
        &executor,
        "EXPLAIN SELECT * FROM users WHERE age = 30;",
    ));
    assert!(seq_plan_again.contains("SeqScan"));
    assert!(!seq_plan_again.contains("IndexScan"));

    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn test_composite_index_supports_prefix_and_full_match() {
    let (dir, _engine, executor) = open_executor("composite-index");

    exec(
        &executor,
        "CREATE TABLE employees (id INT, department TEXT, salary INT, active BOOL);",
    );
    exec(
        &executor,
        "INSERT INTO employees VALUES (1, 'Engineering', 90000, true), (2, 'Engineering', 85000, true), (3, 'HR', 70000, false);",
    );
    assert_eq!(
        exec(&executor, "CREATE INDEX ON employees(department, salary);"),
        ExecuteResult::IndexCreated {
            table_name: "employees".to_string(),
            column_names: vec!["department".to_string(), "salary".to_string()],
        }
    );

    let prefix_plan = explain_plan(exec(
        &executor,
        "EXPLAIN SELECT * FROM employees WHERE department = 'Engineering';",
    ));
    assert!(prefix_plan.contains("CompositeIndexScan"));

    let full_plan = explain_plan(exec(
        &executor,
        "EXPLAIN SELECT * FROM employees WHERE department = 'Engineering' AND salary = 90000;",
    ));
    assert!(full_plan.contains("CompositeIndexScan"));

    let seq_plan = explain_plan(exec(
        &executor,
        "EXPLAIN SELECT * FROM employees WHERE salary = 90000;",
    ));
    assert!(seq_plan.contains("SeqScan"));

    let (_, rows) = selected_rows(exec(
        &executor,
        "SELECT id FROM employees WHERE department = 'Engineering' AND salary = 90000;",
    ));
    assert_eq!(rows, vec![vec![Value::Int(1)]]);

    let _ = std::fs::remove_dir_all(dir);
}

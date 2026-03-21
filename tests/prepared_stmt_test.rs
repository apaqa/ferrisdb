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
    std::env::temp_dir().join(format!("ferrisdb-prepared-{}-{}", name, nanos))
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
    let tokens = lexer.tokenize().expect("tokenize");
    let mut parser = Parser::new(tokens);
    let stmt = parser.parse().expect("parse");
    executor.execute(stmt).expect("execute")
}

fn rows_only(result: ExecuteResult) -> Vec<Vec<Value>> {
    match result {
        ExecuteResult::Selected { rows, .. } => rows,
        other => panic!("expected rows, got {:?}", other),
    }
}

#[test]
fn test_prepare_and_execute_basic_usage() {
    let (dir, _engine, executor) = open_executor("basic");
    exec(
        &executor,
        "CREATE TABLE employees (id INT, department TEXT, salary INT);",
    );
    exec(
        &executor,
        "INSERT INTO employees VALUES (1, 'Engineering', 95000), (2, 'HR', 70000);",
    );

    assert_eq!(
        exec(
            &executor,
            "PREPARE dept_stmt AS SELECT * FROM employees WHERE department = $1;"
        ),
        ExecuteResult::Prepared {
            name: "dept_stmt".to_string(),
        }
    );

    let rows = rows_only(exec(&executor, "EXECUTE dept_stmt('Engineering');"));
    assert_eq!(
        rows,
        vec![vec![
            Value::Int(1),
            Value::Text("Engineering".to_string()),
            Value::Int(95000),
        ]]
    );

    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn test_prepare_with_multiple_parameters_and_cached_plan() {
    let (dir, engine, executor) = open_executor("multi-params");
    exec(
        &executor,
        "CREATE TABLE employees (id INT, department TEXT, salary INT);",
    );
    exec(
        &executor,
        "INSERT INTO employees VALUES (1, 'Engineering', 95000), (2, 'Engineering', 75000), (3, 'HR', 85000);",
    );
    exec(&executor, "CREATE INDEX ON employees(department);");

    exec(
        &executor,
        "PREPARE rich_stmt AS SELECT id FROM employees WHERE department = $1 AND salary > $2;",
    );

    let first_rows = rows_only(exec(&executor, "EXECUTE rich_stmt('Engineering', 80000);"));
    assert_eq!(first_rows, vec![vec![Value::Int(1)]]);
    assert!(
        engine
            .get_prepared_statement("rich_stmt")
            .expect("prepared")
            .cached_plan
            .is_some()
    );

    let second_rows = rows_only(exec(&executor, "EXECUTE rich_stmt('HR', 80000);"));
    assert_eq!(second_rows, vec![vec![Value::Int(3)]]);
    assert!(
        engine
            .get_prepared_statement("rich_stmt")
            .expect("prepared")
            .cached_plan
            .is_some()
    );

    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn test_deallocate_then_execute_returns_error() {
    let (dir, _engine, executor) = open_executor("deallocate");
    exec(
        &executor,
        "CREATE TABLE employees (id INT, department TEXT, salary INT);",
    );
    exec(
        &executor,
        "PREPARE dept_stmt AS SELECT * FROM employees WHERE department = $1;",
    );
    assert_eq!(
        exec(&executor, "DEALLOCATE dept_stmt;"),
        ExecuteResult::Deallocated {
            name: "dept_stmt".to_string(),
        }
    );
    assert_eq!(
        exec(&executor, "EXECUTE dept_stmt('Engineering');"),
        ExecuteResult::Error {
            message: "prepared statement 'dept_stmt' does not exist".to_string(),
        }
    );

    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn test_execute_with_wrong_parameter_count_returns_error() {
    let (dir, _engine, executor) = open_executor("arg-mismatch");
    exec(
        &executor,
        "CREATE TABLE employees (id INT, department TEXT, salary INT);",
    );
    exec(
        &executor,
        "PREPARE dept_stmt AS SELECT * FROM employees WHERE department = $1 AND salary > $2;",
    );

    assert_eq!(
        exec(&executor, "EXECUTE dept_stmt('Engineering');"),
        ExecuteResult::Error {
            message: "prepared statement 'dept_stmt' expects 2 parameter(s), got 1".to_string(),
        }
    );

    let _ = std::fs::remove_dir_all(dir);
}

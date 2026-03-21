// =============================================================================
// tests/view_test.rs -- SQL VIEW 整合測試
// =============================================================================

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
    std::env::temp_dir().join(format!("ferrisdb-view-test-{}-{}", name, nanos))
}

fn open_executor(name: &str) -> (std::path::PathBuf, SqlExecutor) {
    let dir = temp_dir(name);
    let lsm = LsmEngine::open(&dir, 4096).expect("open lsm");
    let engine = Arc::new(MvccEngine::new(lsm));
    let executor = SqlExecutor::new(engine);
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

#[test]
fn test_create_view_and_select_from_view() {
    let (dir, executor) = open_executor("basic");
    exec(
        &executor,
        "CREATE TABLE employees (id INT, name TEXT, salary INT);",
    );
    exec(
        &executor,
        "INSERT INTO employees VALUES (1, 'Alice', 90000), (2, 'Bob', 70000), (3, 'Cara', 95000);",
    );

    assert_eq!(
        exec(
            &executor,
            "CREATE VIEW high_earners AS SELECT * FROM employees WHERE salary > 80000;"
        ),
        ExecuteResult::Created {
            table_name: "high_earners".to_string(),
        }
    );

    let (columns, rows) = rows_only(exec(
        &executor,
        "SELECT * FROM high_earners ORDER BY salary ASC;",
    ));
    assert_eq!(columns, vec!["id", "name", "salary"]);
    assert_eq!(
        rows,
        vec![
            vec![
                Value::Int(1),
                Value::Text("Alice".to_string()),
                Value::Int(90000),
            ],
            vec![
                Value::Int(3),
                Value::Text("Cara".to_string()),
                Value::Int(95000),
            ],
        ]
    );

    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn test_view_with_outer_where_and_join() {
    let (dir, executor) = open_executor("where-join");
    exec(
        &executor,
        "CREATE TABLE employees (id INT, name TEXT, department_id INT, salary INT);",
    );
    exec(
        &executor,
        "CREATE TABLE departments (id INT, dept_name TEXT, location TEXT);",
    );
    exec(
        &executor,
        "INSERT INTO employees VALUES (1, 'Alice', 10, 90000), (2, 'Bob', 20, 70000), (3, 'Cara', 10, 95000);",
    );
    exec(
        &executor,
        "INSERT INTO departments VALUES (10, 'Engineering', 'Taipei'), (20, 'HR', 'Tokyo');",
    );
    exec(
        &executor,
        "CREATE VIEW high_earners AS SELECT id, name, department_id FROM employees WHERE salary > 80000;",
    );

    let (_, filtered_rows) = rows_only(exec(
        &executor,
        "SELECT name FROM high_earners WHERE id = 3;",
    ));
    assert_eq!(filtered_rows, vec![vec![Value::Text("Cara".to_string())]]);

    let (join_columns, join_rows) = rows_only(exec(
        &executor,
        "SELECT high_earners.name, departments.dept_name FROM high_earners INNER JOIN departments ON high_earners.department_id = departments.id ORDER BY high_earners.name ASC;",
    ));
    assert_eq!(
        join_columns,
        vec!["high_earners.name", "departments.dept_name"]
    );
    assert_eq!(
        join_rows,
        vec![
            vec![
                Value::Text("Alice".to_string()),
                Value::Text("Engineering".to_string()),
            ],
            vec![
                Value::Text("Cara".to_string()),
                Value::Text("Engineering".to_string()),
            ],
        ]
    );

    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn test_drop_view_and_missing_view_behaviour() {
    let (dir, executor) = open_executor("drop");
    exec(
        &executor,
        "CREATE TABLE employees (id INT, name TEXT, salary INT);",
    );
    exec(
        &executor,
        "CREATE VIEW high_earners AS SELECT * FROM employees WHERE salary > 80000;",
    );

    assert_eq!(
        exec(&executor, "DROP VIEW high_earners;"),
        ExecuteResult::Dropped {
            table_name: "high_earners".to_string(),
        }
    );

    assert_eq!(
        exec(&executor, "DROP VIEW IF EXISTS high_earners;"),
        ExecuteResult::Dropped {
            table_name: "high_earners".to_string(),
        }
    );

    match exec(&executor, "SELECT * FROM high_earners;") {
        ExecuteResult::Error { message } => assert!(message.contains("high_earners")),
        other => panic!("expected missing-view error, got {:?}", other),
    }

    let _ = std::fs::remove_dir_all(dir);
}

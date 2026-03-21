// =============================================================================
// tests/window_function_test.rs -- Window Function 整合測試
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
    std::env::temp_dir().join(format!("ferrisdb-window-test-{}-{}", name, nanos))
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

fn selected(result: ExecuteResult) -> (Vec<String>, Vec<Vec<Value>>) {
    match result {
        ExecuteResult::Selected { columns, rows } => (columns, rows),
        other => panic!("expected selected result, got {:?}", other),
    }
}

fn seed(executor: &SqlExecutor) {
    exec(
        executor,
        "CREATE TABLE employees (id INT, name TEXT, department TEXT, salary INT);",
    );
    exec(
        executor,
        "INSERT INTO employees VALUES
        (1, 'Alice', 'Eng', 100),
        (2, 'Bob', 'Eng', 100),
        (3, 'Cara', 'Eng', 90),
        (4, 'Dora', 'HR', 80),
        (5, 'Evan', 'HR', 70);",
    );
}

#[test]
fn test_row_number_and_rank() {
    let (dir, executor) = open_executor("row-rank");
    seed(&executor);

    let (_, rows) = selected(exec(
        &executor,
        "SELECT name, salary, ROW_NUMBER() OVER (ORDER BY salary DESC) AS rn, RANK() OVER (ORDER BY salary DESC) AS rk FROM employees ORDER BY name ASC;",
    ));
    assert_eq!(
        rows,
        vec![
            vec![
                Value::Text("Alice".to_string()),
                Value::Int(100),
                Value::Int(1),
                Value::Int(1),
            ],
            vec![
                Value::Text("Bob".to_string()),
                Value::Int(100),
                Value::Int(2),
                Value::Int(1),
            ],
            vec![
                Value::Text("Cara".to_string()),
                Value::Int(90),
                Value::Int(3),
                Value::Int(3),
            ],
            vec![
                Value::Text("Dora".to_string()),
                Value::Int(80),
                Value::Int(4),
                Value::Int(4),
            ],
            vec![
                Value::Text("Evan".to_string()),
                Value::Int(70),
                Value::Int(5),
                Value::Int(5),
            ],
        ]
    );

    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn test_partitioned_window_sum_count_and_row_number() {
    let (dir, executor) = open_executor("partition");
    seed(&executor);

    let (_, rows) = selected(exec(
        &executor,
        "SELECT name, department, ROW_NUMBER() OVER (PARTITION BY department ORDER BY salary DESC) AS dept_row, SUM(salary) OVER (PARTITION BY department) AS dept_total, COUNT(*) OVER (PARTITION BY department) AS dept_count FROM employees WHERE salary >= 70 ORDER BY name ASC;",
    ));
    assert_eq!(
        rows,
        vec![
            vec![
                Value::Text("Alice".to_string()),
                Value::Text("Eng".to_string()),
                Value::Int(1),
                Value::Int(290),
                Value::Int(3),
            ],
            vec![
                Value::Text("Bob".to_string()),
                Value::Text("Eng".to_string()),
                Value::Int(2),
                Value::Int(290),
                Value::Int(3),
            ],
            vec![
                Value::Text("Cara".to_string()),
                Value::Text("Eng".to_string()),
                Value::Int(3),
                Value::Int(290),
                Value::Int(3),
            ],
            vec![
                Value::Text("Dora".to_string()),
                Value::Text("HR".to_string()),
                Value::Int(1),
                Value::Int(150),
                Value::Int(2),
            ],
            vec![
                Value::Text("Evan".to_string()),
                Value::Text("HR".to_string()),
                Value::Int(2),
                Value::Int(150),
                Value::Int(2),
            ],
        ]
    );

    let _ = std::fs::remove_dir_all(dir);
}

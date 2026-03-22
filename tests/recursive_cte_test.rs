// =============================================================================
// tests/recursive_cte_test.rs -- WITH RECURSIVE 整合測試
// =============================================================================
//
// 這組測試驗證 recursive CTE 的 fixpoint 執行流程：
// 1. 先跑 base query
// 2. 用上一輪結果當作 recursive query 的輸入
// 3. 持續迭代直到沒有新 rows
// 4. 若超過最大深度 100，必須報錯而不是卡死

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
    std::env::temp_dir().join(format!("ferrisdb-recursive-{}-{}", name, nanos))
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

fn exec_result(
    executor: &SqlExecutor,
    sql: &str,
) -> Result<ExecuteResult, ferrisdb::error::FerrisDbError> {
    let mut lexer = Lexer::new(sql);
    let tokens = lexer.tokenize()?;
    let mut parser = Parser::new(tokens);
    let stmt = parser.parse()?;
    executor.execute(stmt)
}

fn rows_only(result: ExecuteResult) -> (Vec<String>, Vec<Vec<Value>>) {
    match result {
        ExecuteResult::Selected { columns, rows } => (columns, rows),
        other => panic!("expected selected result, got {:?}", other),
    }
}

#[test]
fn test_recursive_sequence_one_to_ten() {
    let (dir, executor) = open_executor("sequence");

    exec(&executor, "CREATE TABLE edges (parent INT, child INT);");
    exec(
        &executor,
        "INSERT INTO edges VALUES (0, 1), (1, 2), (2, 3), (3, 4), (4, 5), (5, 6), (6, 7), (7, 8), (8, 9), (9, 10);",
    );

    let (_, rows) = rows_only(exec(
        &executor,
        "WITH RECURSIVE nums AS (SELECT child AS n FROM edges WHERE parent = 0 UNION ALL SELECT edges.child AS n FROM edges INNER JOIN nums ON edges.parent = nums.n) SELECT n FROM nums WHERE n <= 10 ORDER BY n ASC;",
    ));
    let expected = (1..=10)
        .map(|n| vec![Value::Int(n)])
        .collect::<Vec<_>>();
    assert_eq!(rows, expected);

    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn test_recursive_employee_hierarchy() {
    let (dir, executor) = open_executor("hierarchy");

    exec(&executor, "CREATE TABLE employees (id INT, manager_id INT, name TEXT);");
    exec(
        &executor,
        "INSERT INTO employees VALUES (1, 0, 'CEO'), (2, 1, 'CTO'), (3, 1, 'CFO'), (4, 2, 'Engineer1'), (5, 2, 'Engineer2'), (6, 3, 'Analyst');",
    );

    let (_, rows) = rows_only(exec(
        &executor,
        "WITH RECURSIVE team AS (SELECT id, manager_id, name FROM employees WHERE id = 1 UNION ALL SELECT employees.id, employees.manager_id, employees.name FROM employees INNER JOIN team ON employees.manager_id = team.id) SELECT name FROM team ORDER BY name ASC;",
    ));
    assert_eq!(
        rows,
        vec![
            vec![Value::Text("Analyst".to_string())],
            vec![Value::Text("CEO".to_string())],
            vec![Value::Text("CFO".to_string())],
            vec![Value::Text("CTO".to_string())],
            vec![Value::Text("Engineer1".to_string())],
            vec![Value::Text("Engineer2".to_string())],
        ]
    );

    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn test_recursive_cte_exceeds_max_depth() {
    let (dir, executor) = open_executor("depth");

    exec(&executor, "CREATE TABLE edges (parent INT, child INT);");
    let mut inserts = Vec::new();
    for n in 0..105 {
        inserts.push(format!("({}, {})", n, n + 1));
    }
    let sql = format!("INSERT INTO edges VALUES {};", inserts.join(", "));
    exec(&executor, &sql);

    let err = exec_result(
        &executor,
        "WITH RECURSIVE nums AS (SELECT child AS n FROM edges WHERE parent = 0 UNION ALL SELECT edges.child AS n FROM edges INNER JOIN nums ON edges.parent = nums.n) SELECT n FROM nums;",
    )
    .expect_err("should exceed recursive depth");
    assert!(err.to_string().contains("max recursion depth 100"));

    let _ = std::fs::remove_dir_all(dir);
}

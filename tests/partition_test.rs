// =============================================================================
// tests/partition_test.rs -- RANGE 分區整合測試
// =============================================================================
//
// 中文註解：
// 這組測試驗證 RANGE 分區表的三件事：
// 1. INSERT 會依分區鍵把 row 寫到對應的 partition prefix。
// 2. SELECT 在帶有分區鍵條件時，至少在結果上只會碰到相關分區資料。
// 3. MAXVALUE 分區可以接住所有大於前面邊界的資料。

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
    std::env::temp_dir().join(format!("ferrisdb-partition-{}-{}", name, nanos))
}

fn open_executor(name: &str) -> SqlExecutor {
    let dir = temp_dir(name);
    let lsm = LsmEngine::open(&dir, 4096).expect("open lsm");
    let engine = Arc::new(MvccEngine::new(lsm));
    SqlExecutor::new(Arc::clone(&engine))
}

fn exec(executor: &SqlExecutor, sql: &str) -> ExecuteResult {
    let mut lexer = Lexer::new(sql);
    let tokens = lexer.tokenize().expect("tokenize sql");
    let mut parser = Parser::new(tokens);
    let stmt = parser.parse().expect("parse sql");
    executor.execute(stmt).expect("execute sql")
}

fn selected_rows(result: ExecuteResult) -> Vec<Vec<Value>> {
    match result {
        ExecuteResult::Selected { rows, .. } => rows,
        other => panic!("expected selected result, got {:?}", other),
    }
}

fn create_partitioned_logs(executor: &SqlExecutor) {
    assert_eq!(
        exec(
            executor,
            "CREATE TABLE logs (id INT, created_date INT, message TEXT) PARTITION BY RANGE (created_date) (PARTITION p1 VALUES LESS THAN (100), PARTITION p2 VALUES LESS THAN (200), PARTITION p3 VALUES LESS THAN MAXVALUE);",
        ),
        ExecuteResult::Created {
            table_name: "logs".to_string()
        }
    );
}

#[test]
fn test_partitioned_table_insert_and_select() {
    let executor = open_executor("insert-select");
    create_partitioned_logs(&executor);
    assert_eq!(
        exec(
            &executor,
            "INSERT INTO logs VALUES (1, 10, 'early'), (2, 150, 'mid'), (3, 250, 'late');",
        ),
        ExecuteResult::Inserted { count: 3 }
    );

    let rows = selected_rows(exec(
        &executor,
        "SELECT * FROM logs ORDER BY created_date ASC;",
    ));
    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0], vec![Value::Int(1), Value::Int(10), Value::Text("early".to_string())]);
    assert_eq!(rows[1], vec![Value::Int(2), Value::Int(150), Value::Text("mid".to_string())]);
    assert_eq!(rows[2], vec![Value::Int(3), Value::Int(250), Value::Text("late".to_string())]);
}

#[test]
fn test_partition_pruning_query_returns_only_matching_range() {
    let executor = open_executor("partition-pruning");
    create_partitioned_logs(&executor);
    exec(
        &executor,
        "INSERT INTO logs VALUES (1, 10, 'early'), (2, 20, 'also-early'), (3, 150, 'mid'), (4, 250, 'late');",
    );

    // 中文註解：這裡用分區鍵 created_date 做 WHERE，理論上 executor 可以只掃描 p1。
    let rows = selected_rows(exec(
        &executor,
        "SELECT * FROM logs WHERE created_date < 100 ORDER BY created_date ASC;",
    ));
    assert_eq!(
        rows,
        vec![
            vec![Value::Int(1), Value::Int(10), Value::Text("early".to_string())],
            vec![Value::Int(2), Value::Int(20), Value::Text("also-early".to_string())],
        ]
    );
}

#[test]
fn test_maxvalue_partition_accepts_large_values() {
    let executor = open_executor("maxvalue");
    create_partitioned_logs(&executor);
    assert_eq!(
        exec(&executor, "INSERT INTO logs VALUES (9, 999, 'huge');"),
        ExecuteResult::Inserted { count: 1 }
    );

    let rows = selected_rows(exec(
        &executor,
        "SELECT * FROM logs WHERE created_date >= 900;",
    ));
    assert_eq!(
        rows,
        vec![vec![
            Value::Int(9),
            Value::Int(999),
            Value::Text("huge".to_string())
        ]]
    );
}

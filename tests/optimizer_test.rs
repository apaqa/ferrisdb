use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use ferrisdb::sql::executor::{ExecuteResult, SqlExecutor};
use ferrisdb::sql::lexer::Lexer;
use ferrisdb::sql::parser::Parser;
use ferrisdb::sql::statistics::StatisticsManager;
use ferrisdb::storage::lsm::LsmEngine;
use ferrisdb::transaction::mvcc::MvccEngine;

fn temp_dir(name: &str) -> std::path::PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time")
        .as_nanos();
    std::env::temp_dir().join(format!("ferrisdb-optimizer-test-{}-{}", name, nanos))
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

fn explain(result: ExecuteResult) -> String {
    match result {
        ExecuteResult::Explain { plan, .. } => plan,
        other => panic!("expected explain result, got {:?}", other),
    }
}

#[test]
fn test_analyze_collects_stats_and_marks_stale_after_write() {
    let (dir, engine, executor) = open_executor("analyze-stats");
    exec(
        &executor,
        "CREATE TABLE employees (id INT, department TEXT, salary INT, active BOOL);",
    );
    exec(
        &executor,
        "INSERT INTO employees VALUES (1, 'Engineering', 100, true), (2, 'HR', 80, false), (3, 'Engineering', 120, true);",
    );
    assert_eq!(
        exec(&executor, "ANALYZE TABLE employees;"),
        ExecuteResult::Analyzed {
            table_name: "employees".to_string(),
        }
    );

    let stats = StatisticsManager::new(Arc::clone(&engine));
    let txn = engine.begin_transaction();
    let table_stats = stats
        .get_table_stats(&txn, "employees")
        .expect("get table stats")
        .expect("table stats");
    assert_eq!(table_stats.row_count, 3);
    assert!(!table_stats.stale);

    let column_stats = stats
        .get_column_stats(&txn, "employees", "department")
        .expect("get column stats")
        .expect("column stats");
    assert_eq!(column_stats.ndv, 2);

    exec(
        &executor,
        "INSERT INTO employees VALUES (4, 'Sales', 90, true);",
    );
    let txn = engine.begin_transaction();
    let table_stats = stats
        .get_table_stats(&txn, "employees")
        .expect("get table stats")
        .expect("table stats");
    assert!(table_stats.stale);

    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn test_optimizer_scan_and_join_choices_and_plan_cache() {
    let (dir, _engine, executor) = open_executor("optimizer-choices");
    exec(&executor, "CREATE TABLE users (id INT, age INT, active BOOL);");
    exec(&executor, "CREATE TABLE orders (id INT, user_id INT, item TEXT);");
    exec(
        &executor,
        "INSERT INTO users VALUES (1, 30, true), (2, 25, false), (3, 30, true);",
    );
    exec(
        &executor,
        "INSERT INTO orders VALUES (10, 1, 'Book'), (11, 1, 'Pen'), (12, 2, 'Cup');",
    );
    exec(&executor, "ANALYZE TABLE users;");
    exec(&executor, "ANALYZE TABLE orders;");

    let seq_plan = explain(exec(
        &executor,
        "EXPLAIN SELECT * FROM users WHERE age = 30;",
    ));
    assert!(seq_plan.contains("SeqScan") || seq_plan.contains("Project"));

    exec(&executor, "CREATE INDEX ON users(age);");
    exec(&executor, "ANALYZE TABLE users;");
    let index_plan = explain(exec(
        &executor,
        "EXPLAIN SELECT * FROM users WHERE age = 30;",
    ));
    assert!(index_plan.contains("IndexScan"));
    assert!(index_plan.contains("rows="));
    assert!(index_plan.contains("cost="));

    let join_plan = explain(exec(
        &executor,
        "EXPLAIN SELECT users.id, orders.item FROM users INNER JOIN orders ON users.id = orders.user_id;",
    ));
    assert!(join_plan.contains("NestedLoopJoin"));

    let before = executor.plan_cache_stats();
    let _ = exec(&executor, "SELECT * FROM users WHERE age = 30;");
    let middle = executor.plan_cache_stats();
    let _ = exec(&executor, "SELECT * FROM users WHERE age = 30;");
    let after = executor.plan_cache_stats();
    assert!(middle.inserts >= before.inserts);
    assert!(after.hits >= middle.hits + 1);

    exec(&executor, "ALTER TABLE users ADD COLUMN score INT;");
    let invalidated_before = executor.plan_cache_stats();
    let _ = exec(&executor, "SELECT * FROM users WHERE age = 30;");
    let invalidated_after = executor.plan_cache_stats();
    assert!(invalidated_after.inserts >= invalidated_before.inserts + 1);

    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn test_hash_join_result_is_correct() {
    let (dir, _engine, executor) = open_executor("hash-join");
    exec(&executor, "CREATE TABLE users (id INT, name TEXT, active BOOL);");
    exec(&executor, "CREATE TABLE orders (id INT, user_id INT, item TEXT);");

    for user_id in 1..=3 {
        exec(
            &executor,
            &format!(
                "INSERT INTO users VALUES ({}, 'user{}', true);",
                user_id, user_id
            ),
        );
    }

    for order_id in 1..=1200 {
        let user_id = ((order_id - 1) % 3) + 1;
        exec(
            &executor,
            &format!(
                "INSERT INTO orders VALUES ({}, {}, 'item{}');",
                order_id, user_id, order_id
            ),
        );
    }

    exec(&executor, "ANALYZE TABLE users;");
    exec(&executor, "ANALYZE TABLE orders;");

    let plan = explain(exec(
        &executor,
        "EXPLAIN SELECT users.name, orders.item FROM users INNER JOIN orders ON users.id = orders.user_id;",
    ));
    assert!(plan.contains("HashJoin"));

    let result = exec(
        &executor,
        "SELECT users.name, orders.item FROM users INNER JOIN orders ON users.id = orders.user_id;",
    );
    match result {
        ExecuteResult::Selected { rows, .. } => assert_eq!(rows.len(), 1200),
        other => panic!("expected selected result, got {:?}", other),
    }

    let _ = std::fs::remove_dir_all(dir);
}

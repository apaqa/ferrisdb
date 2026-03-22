// =============================================================================
// tests/proptest_sql.rs -- SQL Property-Based Testing
// =============================================================================
//
// 這裡用 property-based testing 驗證 SQL 層與簡單參照模型的一致性。
// 核心不變式：
// 1. 任意 INSERT 完成後，SELECT COUNT(*) 必須等於插入的唯一主鍵數量
// 2. 任意 INSERT + DELETE 完成後，SELECT * 的結果必須跟參照 BTreeMap 一致

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use ferrisdb::sql::ast::Value;
use ferrisdb::sql::executor::{ExecuteResult, SqlExecutor};
use ferrisdb::sql::lexer::Lexer;
use ferrisdb::sql::parser::Parser;
use ferrisdb::storage::lsm::LsmEngine;
use ferrisdb::transaction::mvcc::MvccEngine;
use proptest::prelude::*;

fn temp_dir(name: &str) -> std::path::PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time")
        .as_nanos();
    std::env::temp_dir().join(format!("ferrisdb-proptest-sql-{}-{}", name, nanos))
}

fn with_executor<T>(name: &str, f: impl FnOnce(&SqlExecutor) -> T) -> T {
    let dir = temp_dir(name);
    let lsm = LsmEngine::open(&dir, 4096).expect("open lsm");
    let engine = Arc::new(MvccEngine::new(lsm));
    let executor = SqlExecutor::new(Arc::clone(&engine));
    let out = f(&executor);
    let _ = engine.shutdown();
    let _ = std::fs::remove_dir_all(dir);
    out
}

fn exec(executor: &SqlExecutor, sql: &str) -> ExecuteResult {
    let mut lexer = Lexer::new(sql);
    let tokens = lexer.tokenize().expect("tokenize sql");
    let mut parser = Parser::new(tokens);
    let stmt = parser.parse().expect("parse sql");
    executor.execute(stmt).expect("execute sql")
}

proptest! {
    #[test]
    fn prop_insert_then_count_is_correct(
        rows in prop::collection::vec((0_i64..40_i64, "[a-zA-Z]{1,8}"), 1..30)
    ) {
        let result: Result<(), TestCaseError> = with_executor("count", |executor| {
            exec(executor, "CREATE TABLE users (id INT, name TEXT);");
            let mut model = BTreeMap::<i64, String>::new();
            for (id, name) in rows {
                let sql = format!("INSERT INTO users VALUES ({}, '{}');", id, name);
                exec(executor, &sql);
                model.insert(id, name);
            }

            match exec(executor, "SELECT COUNT(*) FROM users;") {
                ExecuteResult::Selected { rows, .. } => {
                    prop_assert_eq!(rows, vec![vec![Value::Int(model.len() as i64)]]);
                }
                other => prop_assert!(false, "unexpected result: {:?}", other),
            }
            Ok(())
        });
        result?;
    }

    #[test]
    fn prop_insert_delete_matches_reference_model(
        inserts in prop::collection::vec((0_i64..30_i64, "[a-zA-Z]{1,8}"), 1..25),
        deletes in prop::collection::vec(0_i64..30_i64, 0..20)
    ) {
        let result: Result<(), TestCaseError> = with_executor("delete", |executor| {
            exec(executor, "CREATE TABLE users (id INT, name TEXT);");
            let mut model = BTreeMap::<i64, String>::new();

            for (id, name) in inserts {
                let sql = format!("INSERT INTO users VALUES ({}, '{}');", id, name);
                exec(executor, &sql);
                model.insert(id, name);
            }

            for id in deletes {
                let sql = format!("DELETE FROM users WHERE id = {};", id);
                exec(executor, &sql);
                model.remove(&id);
            }

            match exec(executor, "SELECT * FROM users ORDER BY id ASC;") {
                ExecuteResult::Selected { rows, .. } => {
                    let expected = model
                        .into_iter()
                        .map(|(id, name)| vec![Value::Int(id), Value::Text(name)])
                        .collect::<Vec<_>>();
                    prop_assert_eq!(rows, expected);
                }
                other => prop_assert!(false, "unexpected result: {:?}", other),
            }
            Ok(())
        });
        result?;
    }
}

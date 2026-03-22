// =============================================================================
// tests/unique_constraint_test.rs -- UNIQUE 約束整合測試
// =============================================================================
//
// 中文註解：
// 這組測試驗證 SQL executor 在 INSERT / UPDATE 前，會確實檢查 schema 上宣告的 UNIQUE 約束。
// 單欄 UNIQUE 與複合 UNIQUE 都要能拒絕重複值，避免資料表出現邏輯上不允許的重複資料。

use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

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
    std::env::temp_dir().join(format!("ferrisdb-unique-{}-{}", name, nanos))
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

#[test]
fn test_insert_rejects_single_column_unique_violation() {
    let executor = open_executor("single-unique");
    assert_eq!(
        exec(
            &executor,
            "CREATE TABLE users (id INT, email TEXT UNIQUE, name TEXT);",
        ),
        ExecuteResult::Created {
            table_name: "users".to_string()
        }
    );
    assert_eq!(
        exec(
            &executor,
            "INSERT INTO users VALUES (1, 'alice@example.com', 'Alice');",
        ),
        ExecuteResult::Inserted { count: 1 }
    );

    match exec(
        &executor,
        "INSERT INTO users VALUES (2, 'alice@example.com', 'Another Alice');",
    ) {
        ExecuteResult::Error { message } => {
            assert!(message.contains("unique constraint violation"));
        }
        other => panic!("expected unique violation, got {:?}", other),
    }
}

#[test]
fn test_insert_accepts_distinct_unique_values() {
    let executor = open_executor("distinct-unique");
    exec(
        &executor,
        "CREATE TABLE users (id INT, email TEXT UNIQUE, name TEXT);",
    );

    assert_eq!(
        exec(
            &executor,
            "INSERT INTO users VALUES (1, 'alice@example.com', 'Alice'), (2, 'bob@example.com', 'Bob');",
        ),
        ExecuteResult::Inserted { count: 2 }
    );
}

#[test]
fn test_composite_unique_constraint_rejects_duplicate_pairs() {
    let executor = open_executor("composite-unique");
    exec(
        &executor,
        "CREATE TABLE memberships (id INT, tenant_id INT, email TEXT, UNIQUE (tenant_id, email));",
    );
    assert_eq!(
        exec(
            &executor,
            "INSERT INTO memberships VALUES (1, 10, 'alice@example.com');",
        ),
        ExecuteResult::Inserted { count: 1 }
    );
    assert_eq!(
        exec(
            &executor,
            "INSERT INTO memberships VALUES (2, 11, 'alice@example.com');",
        ),
        ExecuteResult::Inserted { count: 1 }
    );

    match exec(
        &executor,
        "INSERT INTO memberships VALUES (3, 10, 'alice@example.com');",
    ) {
        ExecuteResult::Error { message } => {
            assert!(message.contains("unique constraint violation"));
        }
        other => panic!("expected unique violation, got {:?}", other),
    }
}

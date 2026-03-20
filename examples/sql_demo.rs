// =============================================================================
// examples/sql_demo.rs -- FerrisDB SQL 功能完整展示
// =============================================================================
//
// 這個 example 會建立一個暫時的資料目錄，然後依序執行一串 SQL：
// - schema 建立 / 刪除
// - INSERT / SELECT / UPDATE
// - ORDER BY / LIMIT
// - GROUP BY / aggregate
// - INNER JOIN
// - CREATE INDEX / EXPLAIN
// - ALTER TABLE
// - WHERE IN (subquery)
//
// 目標是讓使用者執行：
// cargo run --release --example sql_demo
// 就能快速看到 FerrisDB 目前 SQL 層的能力與輸出樣貌。

use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use ferrisdb::sql::executor::{format_execute_result, SqlExecutor};
use ferrisdb::sql::lexer::Lexer;
use ferrisdb::sql::parser::Parser;
use ferrisdb::storage::lsm::LsmEngine;
use ferrisdb::transaction::mvcc::MvccEngine;

fn main() {
    if let Err(err) = run_demo() {
        eprintln!("sql_demo failed: {}", err);
        std::process::exit(1);
    }
}

fn run_demo() -> ferrisdb::error::Result<()> {
    let data_dir = temp_dir();
    let lsm = LsmEngine::open(&data_dir, 4096)?;
    let engine = Arc::new(MvccEngine::new(lsm));
    let executor = SqlExecutor::new(Arc::clone(&engine));

    println!("FerrisDB SQL Demo");
    println!("Data dir: {}\n", data_dir.display());

    let steps = [
        "CREATE TABLE employees (id INT, name TEXT, department TEXT, salary INT);",
        "INSERT INTO employees VALUES (1, 'Alice', 'Engineering', 95000), (2, 'Bob', 'Engineering', 88000), (3, 'Cara', 'Design', 72000), (4, 'Dylan', 'Sales', 68000), (5, 'Eva', 'Engineering', 105000), (6, 'Finn', 'HR', 54000), (7, 'Gina', 'Sales', 61000), (8, 'Hank', 'Design', 79000), (9, 'Iris', 'HR', 57000), (10, 'Jake', 'Engineering', 99000), (11, 'Kara', 'Support', 50000), (12, 'Liam', 'Support', 52000);",
        "SELECT * FROM employees;",
        "SELECT * FROM employees WHERE salary > 50000 ORDER BY salary DESC;",
        "SELECT department, COUNT(*), SUM(salary), MIN(salary), MAX(salary) FROM employees GROUP BY department ORDER BY department ASC;",
        "CREATE TABLE departments (id INT, dept_name TEXT, location TEXT);",
        "INSERT INTO departments VALUES (1, 'Engineering', 'NYC'), (2, 'Design', 'SF'), (3, 'Sales', 'NYC'), (4, 'HR', 'Remote'), (5, 'Support', 'NYC');",
        "SELECT * FROM employees INNER JOIN departments ON employees.department = departments.dept_name ORDER BY employees.id ASC;",
        "CREATE INDEX ON employees(department);",
        "EXPLAIN SELECT * FROM employees WHERE department = 'Engineering';",
        "ALTER TABLE employees ADD COLUMN bonus INT;",
        "UPDATE employees SET bonus = 5000 WHERE department = 'Engineering';",
        "SELECT * FROM employees WHERE department IN (SELECT dept_name FROM departments WHERE location = 'NYC') ORDER BY id ASC;",
        "SELECT * FROM employees ORDER BY salary DESC LIMIT 3;",
        "DROP TABLE IF EXISTS departments;",
    ];

    for (idx, sql) in steps.iter().enumerate() {
        println!("Step {}:", idx + 1);
        println!("SQL> {}", sql);
        let output = execute_sql(&executor, sql)?;
        println!("{}\n", output);
    }

    engine.shutdown()?;
    let _ = std::fs::remove_dir_all(&data_dir);
    Ok(())
}

fn execute_sql(executor: &SqlExecutor, sql: &str) -> ferrisdb::error::Result<String> {
    let mut lexer = Lexer::new(sql);
    let tokens = lexer.tokenize()?;
    let mut parser = Parser::new(tokens);
    let statement = parser.parse()?;
    let result = executor.execute(statement)?;
    Ok(format_execute_result(&result))
}

fn temp_dir() -> std::path::PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time")
        .as_nanos();
    std::env::temp_dir().join(format!("ferrisdb-sql-demo-{}", nanos))
}

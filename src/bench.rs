// =============================================================================
// bench.rs -- FerrisDB Benchmark Helpers
// =============================================================================
//
// 這個模組提供簡單的 benchmark helper，讓：
// - `examples/bench.rs`
// - `examples/bench_sql.rs`
// - REPL 的 `bench` 指令
//
// 可以共用同一套量測與輸出資料格式。

use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use crate::config::FerrisDbConfig;
use crate::error::Result;
use crate::sql::executor::SqlExecutor;
use crate::sql::lexer::Lexer;
use crate::sql::parser::Parser;
use crate::storage::lsm::LsmEngine;
use crate::transaction::mvcc::MvccEngine;

#[derive(Debug, Clone)]
pub struct BenchResult {
    pub name: String,
    pub operations: usize,
    pub elapsed: Duration,
}

impl BenchResult {
    pub fn ops_per_sec(&self) -> f64 {
        let secs = self.elapsed.as_secs_f64();
        if secs == 0.0 {
            return self.operations as f64;
        }
        self.operations as f64 / secs
    }

    pub fn average_latency(&self) -> Duration {
        if self.operations == 0 {
            return Duration::ZERO;
        }
        Duration::from_secs_f64(self.elapsed.as_secs_f64() / self.operations as f64)
    }
}

pub fn run_basic_kv_benchmark(engine: &Arc<MvccEngine>, count: usize) -> Result<Vec<BenchResult>> {
    let write_start = Instant::now();
    for i in 0..count {
        let mut txn = engine.begin_transaction();
        txn.put(
            format!("bench:key:{:06}", i).into_bytes(),
            format!("value:{:06}", i).into_bytes(),
        )?;
        txn.commit()?;
    }
    let writes = BenchResult {
        name: "REPL sequential write".to_string(),
        operations: count,
        elapsed: write_start.elapsed(),
    };

    let read_start = Instant::now();
    for i in 0..count {
        let txn = engine.begin_transaction();
        let _ = txn.get(format!("bench:key:{:06}", i).as_bytes())?;
    }
    let reads = BenchResult {
        name: "REPL sequential read".to_string(),
        operations: count,
        elapsed: read_start.elapsed(),
    };

    Ok(vec![writes, reads])
}

pub fn run_full_kv_benchmark(config: &FerrisDbConfig) -> Result<Vec<BenchResult>> {
    let data_dir = temp_bench_dir("kv");
    let engine = open_engine_with_temp_dir(config, &data_dir)?;
    let mut results = Vec::new();

    results.push(measure_sequential_write(&engine, 10_000)?);
    results.push(measure_random_read(&engine, 10_000)?);
    results.push(measure_mixed_workload(&engine, 10_000)?);
    results.push(measure_scan(&engine, 100)?);

    let shutdown_start = Instant::now();
    engine.shutdown()?;
    let shutdown_elapsed = shutdown_start.elapsed();

    let reopen_start = Instant::now();
    let reopened = Arc::new(MvccEngine::new(LsmEngine::open_with_options(
        &data_dir,
        config.memtable_size_threshold,
        config.compaction_threshold,
        config.wal_sync_on_write,
    )?));
    let reopen_elapsed = reopen_start.elapsed();
    results.push(BenchResult {
        name: "Restart recovery".to_string(),
        operations: 1,
        elapsed: shutdown_elapsed + reopen_elapsed,
    });

    let compaction_start = Instant::now();
    reopened.compact()?;
    results.push(BenchResult {
        name: "Compaction".to_string(),
        operations: 1,
        elapsed: compaction_start.elapsed(),
    });

    reopened.shutdown()?;
    let _ = std::fs::remove_dir_all(&data_dir);
    Ok(results)
}

pub fn run_sql_benchmark(config: &FerrisDbConfig) -> Result<Vec<BenchResult>> {
    let data_dir = temp_bench_dir("sql");
    let engine = open_engine_with_temp_dir(config, &data_dir)?;
    let executor = SqlExecutor::new(Arc::clone(&engine));
    let mut results = Vec::new();

    execute_sql(&executor, "CREATE TABLE bench (id INT, name TEXT, score INT, active BOOL);")?;

    let insert_start = Instant::now();
    for i in 0..10_000 {
        execute_sql(
            &executor,
            &format!(
                "INSERT INTO bench VALUES ({}, 'user{}', {}, true);",
                i, i, i
            ),
        )?;
    }
    results.push(BenchResult {
        name: "SQL INSERT".to_string(),
        operations: 10_000,
        elapsed: insert_start.elapsed(),
    });

    let select_all_start = Instant::now();
    execute_sql(&executor, "SELECT * FROM bench;")?;
    results.push(BenchResult {
        name: "SQL SELECT *".to_string(),
        operations: 1,
        elapsed: select_all_start.elapsed(),
    });

    let select_where_start = Instant::now();
    for i in 0..1_000 {
        execute_sql(&executor, &format!("SELECT * FROM bench WHERE id = {};", i))?;
    }
    results.push(BenchResult {
        name: "SQL SELECT WHERE".to_string(),
        operations: 1_000,
        elapsed: select_where_start.elapsed(),
    });

    let update_start = Instant::now();
    for i in 0..1_000 {
        execute_sql(
            &executor,
            &format!("UPDATE bench SET score = {} WHERE id = {};", i + 1000, i),
        )?;
    }
    results.push(BenchResult {
        name: "SQL UPDATE".to_string(),
        operations: 1_000,
        elapsed: update_start.elapsed(),
    });

    let delete_start = Instant::now();
    for i in 0..1_000 {
        execute_sql(&executor, &format!("DELETE FROM bench WHERE id = {};", i))?;
    }
    results.push(BenchResult {
        name: "SQL DELETE".to_string(),
        operations: 1_000,
        elapsed: delete_start.elapsed(),
    });

    engine.shutdown()?;
    let _ = std::fs::remove_dir_all(&data_dir);
    Ok(results)
}

pub fn format_bench_results(results: &[BenchResult]) -> String {
    let header = format!(
        "{:<22} | {:>10} | {:>12} | {:>12} | {:>14}",
        "Operation", "Ops", "Elapsed(ms)", "Ops/sec", "Avg latency"
    );
    let separator = "-".repeat(header.len());

    let mut lines = vec![header, separator];
    for result in results {
        lines.push(format!(
            "{:<22} | {:>10} | {:>12.2} | {:>12.2} | {:>14.6} ms",
            result.name,
            result.operations,
            result.elapsed.as_secs_f64() * 1000.0,
            result.ops_per_sec(),
            result.average_latency().as_secs_f64() * 1000.0
        ));
    }
    lines.join("\n")
}

fn measure_sequential_write(engine: &Arc<MvccEngine>, count: usize) -> Result<BenchResult> {
    let start = Instant::now();
    for i in 0..count {
        let mut txn = engine.begin_transaction();
        txn.put(
            format!("seq:{:06}", i).into_bytes(),
            format!("value:{:06}", i).into_bytes(),
        )?;
        txn.commit()?;
    }
    Ok(BenchResult {
        name: "Sequential write".to_string(),
        operations: count,
        elapsed: start.elapsed(),
    })
}

fn measure_random_read(engine: &Arc<MvccEngine>, count: usize) -> Result<BenchResult> {
    let start = Instant::now();
    for i in 0..count {
        let key = (i * 7919) % count;
        let txn = engine.begin_transaction();
        let _ = txn.get(format!("seq:{:06}", key).as_bytes())?;
    }
    Ok(BenchResult {
        name: "Random read".to_string(),
        operations: count,
        elapsed: start.elapsed(),
    })
}

fn measure_mixed_workload(engine: &Arc<MvccEngine>, count: usize) -> Result<BenchResult> {
    let start = Instant::now();
    for i in 0..count {
        if i % 2 == 0 {
            let txn = engine.begin_transaction();
            let _ = txn.get(format!("seq:{:06}", i % 5000).as_bytes())?;
        } else {
            let mut txn = engine.begin_transaction();
            txn.put(
                format!("mixed:{:06}", i).into_bytes(),
                format!("value:{:06}", i).into_bytes(),
            )?;
            txn.commit()?;
        }
    }
    Ok(BenchResult {
        name: "Mixed workload".to_string(),
        operations: count,
        elapsed: start.elapsed(),
    })
}

fn measure_scan(engine: &Arc<MvccEngine>, count: usize) -> Result<BenchResult> {
    let start = Instant::now();
    for _ in 0..count {
        let txn = engine.begin_transaction();
        let _ = txn.scan(&[], &[0xFF])?;
    }
    Ok(BenchResult {
        name: "Scan".to_string(),
        operations: count,
        elapsed: start.elapsed(),
    })
}

fn execute_sql(executor: &SqlExecutor, sql: &str) -> Result<()> {
    let mut lexer = Lexer::new(sql);
    let tokens = lexer.tokenize()?;
    let mut parser = Parser::new(tokens);
    let stmt = parser.parse()?;
    let _ = executor.execute(stmt)?;
    Ok(())
}

fn open_engine_with_temp_dir(config: &FerrisDbConfig, data_dir: &std::path::Path) -> Result<Arc<MvccEngine>> {
    let lsm = LsmEngine::open_with_options(
        data_dir,
        config.memtable_size_threshold,
        config.compaction_threshold,
        config.wal_sync_on_write,
    )?;
    Ok(Arc::new(MvccEngine::new(lsm)))
}

fn temp_bench_dir(prefix: &str) -> std::path::PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time")
        .as_nanos();
    std::env::temp_dir().join(format!("ferrisdb-bench-{}-{}", prefix, nanos))
}

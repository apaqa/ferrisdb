// =============================================================================
// examples/bench_sql.rs -- SQL Benchmark Example
// =============================================================================
//
// 執行方式：
//   cargo run --release --example bench_sql
//
// 這個範例會量測 SQL 層常見操作的成本。

use ferrisdb::bench::{format_bench_results, run_sql_benchmark};
use ferrisdb::config::FerrisDbConfig;

fn main() -> ferrisdb::error::Result<()> {
    let config = FerrisDbConfig::default();
    let results = run_sql_benchmark(&config)?;

    println!("FerrisDB SQL benchmark");
    println!("{}", format_bench_results(&results));
    Ok(())
}

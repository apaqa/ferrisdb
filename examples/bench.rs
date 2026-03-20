// =============================================================================
// examples/bench.rs -- KV Benchmark Example
// =============================================================================
//
// 執行方式：
//   cargo run --release --example bench
//
// 這個範例會建立暫時資料目錄，跑一組 KV benchmark，最後印出總結表格。

use ferrisdb::bench::{format_bench_results, run_full_kv_benchmark};
use ferrisdb::config::FerrisDbConfig;

fn main() -> ferrisdb::error::Result<()> {
    let config = FerrisDbConfig::default();
    let results = run_full_kv_benchmark(&config)?;

    println!("FerrisDB KV benchmark");
    println!("{}", format_bench_results(&results));
    Ok(())
}

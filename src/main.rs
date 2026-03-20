// =============================================================================
// main.rs — ferrisdb 執行入口（REPL / TCP server）
// =============================================================================
//
// 這版 main 使用 LsmEngine 作為底層儲存引擎。
// 因此：
// - REPL 模式的資料不再只留在記憶體
// - TCP server 模式也會把資料持久化到磁碟
//
// 使用方式：
// - cargo run
// - cargo run -- --server
// - cargo run -- --server --port 7777

use std::sync::{Arc, Mutex};

use ferrisdb::cli::repl;
use ferrisdb::server::tcp::{self, DEFAULT_PORT};
use ferrisdb::storage::lsm::{LsmEngine, DEFAULT_MEMTABLE_SIZE_THRESHOLD};

const DEFAULT_DATA_DIR: &str = "./ferrisdb-data";

fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();

    if args.is_empty() {
        run_repl_mode();
        return;
    }

    match parse_server_args(&args) {
        Ok(Some(port)) => run_server_mode(port),
        Ok(None) => {
            eprintln!("Unknown args: {}", args.join(" "));
            eprintln!("Usage:");
            eprintln!("  cargo run");
            eprintln!("  cargo run -- --server");
            eprintln!("  cargo run -- --server --port <port>");
            std::process::exit(2);
        }
        Err(msg) => {
            eprintln!("Argument error: {}", msg);
            std::process::exit(2);
        }
    }
}

fn run_repl_mode() {
    let mut engine = match LsmEngine::open(DEFAULT_DATA_DIR, DEFAULT_MEMTABLE_SIZE_THRESHOLD) {
        Ok(engine) => engine,
        Err(err) => {
            eprintln!("Failed to open LSM engine: {}", err);
            std::process::exit(1);
        }
    };

    if let Err(err) = repl::run(&mut engine) {
        eprintln!("Fatal error: {}", err);
        std::process::exit(1);
    }

    // REPL 結束時主動 shutdown，確保就算資料量沒超過閾值也會被 flush。
    if let Err(err) = engine.shutdown() {
        eprintln!("Failed to shutdown LSM engine cleanly: {}", err);
        std::process::exit(1);
    }
}

fn run_server_mode(port: u16) {
    let engine = match LsmEngine::open(DEFAULT_DATA_DIR, DEFAULT_MEMTABLE_SIZE_THRESHOLD) {
        Ok(engine) => engine,
        Err(err) => {
            eprintln!("Failed to open LSM engine: {}", err);
            std::process::exit(1);
        }
    };

    let shared_engine = Arc::new(Mutex::new(engine));
    if let Err(err) = tcp::run_server_with_engine(port, shared_engine) {
        eprintln!("Fatal server error: {}", err);
        std::process::exit(1);
    }
}

/// 解析是否為 server 模式。
/// 成功時回傳 `Some(port)`；若不是 server 模式則回傳 `None`。
fn parse_server_args(args: &[String]) -> Result<Option<u16>, String> {
    if args.is_empty() || args[0] != "--server" {
        return Ok(None);
    }

    if args.len() == 1 {
        return Ok(Some(DEFAULT_PORT));
    }

    if args.len() == 3 && args[1] == "--port" {
        let port: u16 = args[2]
            .parse()
            .map_err(|_| format!("invalid port '{}'", args[2]))?;
        return Ok(Some(port));
    }

    Err("expected '--server' or '--server --port <port>'".to_string())
}

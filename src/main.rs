// =============================================================================
// main.rs — ferrisdb 執行入口（MVCC REPL / MVCC TCP server）
// =============================================================================
//
// 這一版 main 使用：
// - 底層儲存：LsmEngine
// - 交易層：MvccEngine
//
// 因此 REPL 與 TCP server 都會透過 MVCC 存取資料。

use std::sync::Arc;

use ferrisdb::cli::repl;
use ferrisdb::server::tcp::{self, DEFAULT_PORT};
use ferrisdb::storage::lsm::{LsmEngine, DEFAULT_MEMTABLE_SIZE_THRESHOLD};
use ferrisdb::transaction::mvcc::MvccEngine;

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
    let lsm = match LsmEngine::open(DEFAULT_DATA_DIR, DEFAULT_MEMTABLE_SIZE_THRESHOLD) {
        Ok(engine) => engine,
        Err(err) => {
            eprintln!("Failed to open LSM engine: {}", err);
            std::process::exit(1);
        }
    };

    let engine = Arc::new(MvccEngine::new(lsm));
    if let Err(err) = repl::run(Arc::clone(&engine)) {
        eprintln!("Fatal error: {}", err);
        std::process::exit(1);
    }

    if let Err(err) = engine.shutdown() {
        eprintln!("Failed to shutdown MVCC engine cleanly: {}", err);
        std::process::exit(1);
    }
}

fn run_server_mode(port: u16) {
    let lsm = match LsmEngine::open(DEFAULT_DATA_DIR, DEFAULT_MEMTABLE_SIZE_THRESHOLD) {
        Ok(engine) => engine,
        Err(err) => {
            eprintln!("Failed to open LSM engine: {}", err);
            std::process::exit(1);
        }
    };

    let engine = Arc::new(MvccEngine::new(lsm));
    if let Err(err) = tcp::run_server_with_engine(port, engine) {
        eprintln!("Fatal server error: {}", err);
        std::process::exit(1);
    }
}

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

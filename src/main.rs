// =============================================================================
// main.rs -- FerrisDB Startup Entry
// =============================================================================
//
// 啟動流程：
// 1. 先載入 `ferrisdb.toml`，若不存在則用預設設定
// 2. 再用 CLI 參數覆蓋 config
// 3. 依模式啟動：
//    - 預設：REPL
//    - `--server`：TCP server
//
// 目前支援的 CLI 覆蓋：
// - `--data-dir <path>`
// - `--port <port>`
// - `--memtable-threshold <bytes>`

use std::path::Path;
use std::sync::Arc;

use ferrisdb::cli::repl;
use ferrisdb::config::FerrisDbConfig;
use ferrisdb::server::{http, tcp};
use ferrisdb::storage::lsm::LsmEngine;
use ferrisdb::transaction::mvcc::MvccEngine;

const CONFIG_PATH: &str = "ferrisdb.toml";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Mode {
    Repl,
    Server,
    Http { port: u16 },
}

fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();

    let mut config = load_config_or_default(CONFIG_PATH);
    if let Err(err) = config.merge_cli_args(&args) {
        eprintln!("Argument error: {}", err);
        std::process::exit(2);
    }

    let mode = match parse_mode(&args) {
        Ok(mode) => mode,
        Err(err) => {
            eprintln!("Argument error: {}", err);
            print_usage();
            std::process::exit(2);
        }
    };

    match mode {
        Mode::Repl => run_repl_mode(&config),
        Mode::Server => run_server_mode(&config),
        Mode::Http { port } => run_http_mode(&config, port),
    }
}

fn load_config_or_default(path: &str) -> FerrisDbConfig {
    if Path::new(path).exists() {
        match FerrisDbConfig::from_file(path) {
            Ok(config) => config,
            Err(err) => {
                eprintln!("Failed to load config '{}': {}", path, err);
                std::process::exit(2);
            }
        }
    } else {
        FerrisDbConfig::default()
    }
}

fn build_engine(config: &FerrisDbConfig) -> Arc<MvccEngine> {
    let lsm = match LsmEngine::open_with_options(
        &config.data_dir,
        config.memtable_size_threshold,
        config.compaction_threshold,
        config.wal_sync_on_write,
    ) {
        Ok(engine) => engine,
        Err(err) => {
            eprintln!("Failed to open LSM engine: {}", err);
            std::process::exit(1);
        }
    };

    Arc::new(MvccEngine::new(lsm))
}

fn run_repl_mode(config: &FerrisDbConfig) {
    let engine = build_engine(config);
    if let Err(err) = repl::run(Arc::clone(&engine)) {
        eprintln!("Fatal error: {}", err);
        std::process::exit(1);
    }

    if let Err(err) = engine.shutdown() {
        eprintln!("Failed to shutdown MVCC engine cleanly: {}", err);
        std::process::exit(1);
    }
}

fn run_server_mode(config: &FerrisDbConfig) {
    let engine = build_engine(config);
    if let Err(err) = tcp::run_server_at(&config.server_host, config.server_port, engine) {
        eprintln!("Fatal server error: {}", err);
        std::process::exit(1);
    }
}

fn run_http_mode(config: &FerrisDbConfig, port: u16) {
    let engine = build_engine(config);
    if let Err(err) = http::run_http_at(&config.server_host, port, engine) {
        eprintln!("Fatal HTTP server error: {}", err);
        std::process::exit(1);
    }
}

fn parse_mode(args: &[String]) -> Result<Mode, String> {
    if let Some(port) = parse_http_port(args)? {
        return Ok(Mode::Http { port });
    }

    if args.iter().any(|arg| arg == "--server") {
        return Ok(Mode::Server);
    }

    for arg in args {
        if arg.starts_with("--")
            && arg != "--data-dir"
            && arg != "--port"
            && arg != "--memtable-threshold"
            && arg != "--http-port"
        {
            return Err(format!("unknown arg '{}'", arg));
        }
    }

    Ok(Mode::Repl)
}

fn parse_http_port(args: &[String]) -> Result<Option<u16>, String> {
    let Some(idx) = args.iter().position(|arg| arg == "--http-port") else {
        return Ok(None);
    };

    let value = args
        .get(idx + 1)
        .ok_or_else(|| "missing value for --http-port".to_string())?;
    let port = value
        .parse::<u16>()
        .map_err(|_| format!("invalid http port '{}'", value))?;
    Ok(Some(port))
}

fn print_usage() {
    eprintln!("Usage:");
    eprintln!("  cargo run");
    eprintln!("  cargo run -- --server");
    eprintln!("  cargo run -- --server --port <port>");
    eprintln!("  cargo run -- --http-port <port>");
    eprintln!("  cargo run -- --data-dir <path>");
    eprintln!("  cargo run -- --memtable-threshold <bytes>");
}

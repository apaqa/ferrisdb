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
use ferrisdb::sql::catalog::Catalog;
use ferrisdb::sql::executor::SqlExecutor;
use ferrisdb::sql::lexer::Lexer;
use ferrisdb::sql::parser::Parser;
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
        config.wal_mode.clone(),
    ) {
        Ok(engine) => engine,
        Err(err) => {
            eprintln!("Failed to open LSM engine: {}", err);
            std::process::exit(1);
        }
    };

    let engine = Arc::new(MvccEngine::new(lsm));
    if let Err(err) = seed_demo_data_if_empty(&engine) {
        eprintln!("Failed to seed demo data: {}", err);
        std::process::exit(1);
    }
    engine
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
    println!("FerrisDB Studio available at http://127.0.0.1:{}/", port);
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

// 中文註解：只有在全新資料庫完全沒有資料表時，才自動建立 Studio 示範資料。
fn seed_demo_data_if_empty(engine: &Arc<MvccEngine>) -> ferrisdb::error::Result<()> {
    let catalog = Catalog::new(Arc::clone(engine));
    let txn = engine.begin_transaction();
    if !catalog.list_tables(&txn)?.is_empty() {
        return Ok(());
    }

    let demo_sql = [
        "CREATE TABLE IF NOT EXISTS employees (id INT, name TEXT, department TEXT, salary INT);",
        "INSERT INTO employees VALUES (1, 'Alice', 'Engineering', 95000), (2, 'Bob', 'Engineering', 88000), (3, 'Cara', 'Design', 72000), (4, 'Dylan', 'Sales', 68000), (5, 'Eva', 'Engineering', 105000), (6, 'Finn', 'HR', 54000), (7, 'Gina', 'Sales', 61000), (8, 'Hank', 'Design', 79000), (9, 'Iris', 'HR', 57000), (10, 'Jake', 'Engineering', 99000), (11, 'Kara', 'Support', 50000), (12, 'Liam', 'Support', 52000);",
        "CREATE TABLE IF NOT EXISTS departments (id INT, dept_name TEXT, location TEXT);",
        "INSERT INTO departments VALUES (1, 'Engineering', 'NYC'), (2, 'Design', 'SF'), (3, 'Sales', 'NYC'), (4, 'HR', 'Remote'), (5, 'Support', 'NYC');",
        "CREATE INDEX ON employees(department);",
    ];

    let executor = SqlExecutor::new(Arc::clone(engine));
    for sql in demo_sql {
        run_startup_sql(&executor, sql)?;
    }

    Ok(())
}

// 中文註解：啟動時直接重用 SQL parser 與 executor，避免維護兩套建立資料的邏輯。
fn run_startup_sql(executor: &SqlExecutor, sql: &str) -> ferrisdb::error::Result<()> {
    let mut lexer = Lexer::new(sql);
    let tokens = lexer.tokenize()?;
    let mut parser = Parser::new(tokens);
    let statement = parser.parse()?;
    let _ = executor.execute(statement)?;
    Ok(())
}

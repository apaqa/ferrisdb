// =============================================================================
// main.rs — ferrisdb 執行入口
// =============================================================================
//
// 模式說明：
// - `cargo run`：啟動 REPL
// - `cargo run -- --server`：啟動 TCP server（127.0.0.1:6379）
// - `cargo run -- --server --port 7777`：啟動 TCP server（自訂 port）

use ferrisdb::cli::repl;
use ferrisdb::server::tcp::{self, DEFAULT_PORT};
use ferrisdb::storage::memory::MemTable;

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
    let mut engine = MemTable::new();
    if let Err(err) = repl::run(&mut engine) {
        eprintln!("Fatal error: {}", err);
        std::process::exit(1);
    }
}

fn run_server_mode(port: u16) {
    if let Err(err) = tcp::run_server(port) {
        eprintln!("Fatal server error: {}", err);
        std::process::exit(1);
    }
}

/// 解析是否為 server 模式，成功回傳 `Some(port)`。
/// 若不是 server 模式，回傳 `None`。
fn parse_server_args(args: &[String]) -> Result<Option<u16>, String> {
    if args.is_empty() || args[0] != "--server" {
        return Ok(None);
    }

    // 僅 `--server`
    if args.len() == 1 {
        return Ok(Some(DEFAULT_PORT));
    }

    // `--server --port <port>`
    if args.len() == 3 && args[1] == "--port" {
        let port: u16 = args[2]
            .parse()
            .map_err(|_| format!("invalid port '{}'", args[2]))?;
        return Ok(Some(port));
    }

    Err("expected '--server' or '--server --port <port>'".to_string())
}

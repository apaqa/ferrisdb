// =============================================================================
// server/tcp.rs — MVCC TCP server
// =============================================================================
//
// TCP server 現在建立在 MvccEngine 之上。
// 每個指令都自動包在一個 transaction 中（auto-commit 模式）：
// - get / scan / list / stats：只讀 transaction
// - set / delete：寫 transaction，最後自動 commit
//
// 這樣多個 client 即使並行操作，也能透過 MVCC 拿到自己的快照視圖。

use std::io::{BufRead, BufReader, BufWriter, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::Arc;
use std::thread;

use crate::error::Result;
use crate::sql::executor::{format_execute_result, SqlExecutor};
use crate::sql::lexer::Lexer;
use crate::sql::parser::Parser;
use crate::transaction::mvcc::MvccEngine;

pub const DEFAULT_HOST: &str = "127.0.0.1";
pub const DEFAULT_PORT: u16 = 6379;

pub fn run_on_listener(listener: TcpListener, engine: Arc<MvccEngine>) -> Result<()> {
    let local_addr = listener.local_addr()?;
    println!("FerrisDB TCP server listening on {}", local_addr);

    for incoming in listener.incoming() {
        match incoming {
            Ok(stream) => {
                let shared = Arc::clone(&engine);
                thread::spawn(move || {
                    if let Err(err) = handle_client(stream, shared) {
                        eprintln!("Client connection error: {}", err);
                    }
                });
            }
            Err(err) => eprintln!("Accept error: {}", err),
        }
    }

    Ok(())
}

pub fn run_server_with_engine(port: u16, engine: Arc<MvccEngine>) -> Result<()> {
    run_server_at(DEFAULT_HOST, port, engine)
}

pub fn run_server_at(host: &str, port: u16, engine: Arc<MvccEngine>) -> Result<()> {
    let addr = format!("{}:{}", host, port);
    let listener = TcpListener::bind(&addr)?;
    run_on_listener(listener, engine)
}

fn handle_client(stream: TcpStream, engine: Arc<MvccEngine>) -> Result<()> {
    let reader_stream = stream.try_clone()?;
    let mut reader = BufReader::new(reader_stream);
    let mut writer = BufWriter::new(stream);
    let mut line = String::new();
    let mut sql_mode = false;

    loop {
        line.clear();
        let bytes = reader.read_line(&mut line)?;
        if bytes == 0 {
            break;
        }

        let cmd = line.trim();
        if cmd.is_empty() {
            continue;
        }

        let response = execute_command(cmd, &engine, &mut sql_mode);
        writer.write_all(response.as_bytes())?;
        writer.write_all(b"\n")?;
        writer.flush()?;
    }

    Ok(())
}

fn execute_command(line: &str, engine: &Arc<MvccEngine>, sql_mode: &mut bool) -> String {
    if line.eq_ignore_ascii_case("sql") {
        *sql_mode = true;
        return "Switched to SQL mode".to_string();
    }

    if line.eq_ignore_ascii_case("kv") {
        *sql_mode = false;
        return "Switched to KV mode".to_string();
    }

    if *sql_mode {
        return execute_sql(line, engine);
    }

    let parts: Vec<&str> = line.split_whitespace().collect();
    if parts.is_empty() {
        return "Error: empty command".to_string();
    }

    match parts[0].to_lowercase().as_str() {
        "set" => cmd_set(&parts, engine),
        "get" => cmd_get(&parts, engine),
        "delete" | "del" => cmd_delete(&parts, engine),
        "list" | "ls" => cmd_list(engine),
        "scan" => cmd_scan(&parts, engine),
        "stats" => cmd_stats(engine),
        _ => format!("Error: unknown command '{}'", parts[0]),
    }
}

fn execute_sql(line: &str, engine: &Arc<MvccEngine>) -> String {
    let mut lexer = Lexer::new(line);
    let tokens = match lexer.tokenize() {
        Ok(tokens) => tokens,
        Err(err) => return format!("SQL lexer error: {}", err),
    };

    let mut parser = Parser::new(tokens);
    let statement = match parser.parse() {
        Ok(statement) => statement,
        Err(err) => return format!("SQL parser error: {}", err),
    };

    let executor = SqlExecutor::new(Arc::clone(engine));
    match executor.execute(statement) {
        Ok(result) => format_execute_result(&result),
        Err(err) => format!("SQL execution error: {}", err),
    }
}

fn cmd_set(parts: &[&str], engine: &Arc<MvccEngine>) -> String {
    if parts.len() < 3 {
        return "Usage: set <key> <value>".to_string();
    }

    let mut txn = engine.begin_transaction();
    let result = txn
        .put(
            parts[1].as_bytes().to_vec(),
            parts[2..].join(" ").into_bytes(),
        )
        .and_then(|_| txn.commit());

    match result {
        Ok(()) => "OK".to_string(),
        Err(err) => format!("Error: {}", err),
    }
}

fn cmd_get(parts: &[&str], engine: &Arc<MvccEngine>) -> String {
    if parts.len() != 2 {
        return "Usage: get <key>".to_string();
    }

    let txn = engine.begin_transaction();
    match txn.get(parts[1].as_bytes()) {
        Ok(Some(value)) => String::from_utf8_lossy(&value).into_owned(),
        Ok(None) => "(not found)".to_string(),
        Err(err) => format!("Error: {}", err),
    }
}

fn cmd_delete(parts: &[&str], engine: &Arc<MvccEngine>) -> String {
    if parts.len() != 2 {
        return "Usage: delete <key>".to_string();
    }

    let mut txn = engine.begin_transaction();
    let result = txn.delete(parts[1].as_bytes()).and_then(|_| txn.commit());
    match result {
        Ok(()) => "OK".to_string(),
        Err(err) => format!("Error: {}", err),
    }
}

fn cmd_list(engine: &Arc<MvccEngine>) -> String {
    let txn = engine.begin_transaction();
    match txn.scan(&[], &[0xFF]) {
        Ok(pairs) => format_pairs_or_empty(&pairs),
        Err(err) => format!("Error: {}", err),
    }
}

fn cmd_scan(parts: &[&str], engine: &Arc<MvccEngine>) -> String {
    if parts.len() != 3 {
        return "Usage: scan <start_key> <end_key>".to_string();
    }

    let txn = engine.begin_transaction();
    match txn.scan(parts[1].as_bytes(), parts[2].as_bytes()) {
        Ok(pairs) => {
            if pairs.is_empty() {
                format!("(no results in range {} .. {})", parts[1], parts[2])
            } else {
                format_pairs_or_empty(&pairs)
            }
        }
        Err(err) => format!("Error: {}", err),
    }
}

fn cmd_stats(engine: &Arc<MvccEngine>) -> String {
    let txn = engine.begin_transaction();
    match txn.scan(&[], &[0xFF]) {
        Ok(pairs) => {
            let total_bytes: usize = pairs.iter().map(|(k, v)| k.len() + v.len()).sum();
            format!("Entries: {}, Data size: {} bytes", pairs.len(), total_bytes)
        }
        Err(err) => format!("Error: {}", err),
    }
}

fn format_pairs_or_empty(pairs: &[(Vec<u8>, Vec<u8>)]) -> String {
    if pairs.is_empty() {
        return "(empty)".to_string();
    }

    let items: Vec<String> = pairs
        .iter()
        .map(|(k, v)| {
            format!(
                "{} -> {}",
                String::from_utf8_lossy(k),
                String::from_utf8_lossy(v)
            )
        })
        .collect();

    format!("{} ({} entries)", items.join(" | "), pairs.len())
}

// =============================================================================
// server/tcp.rs — 使用 std::net 的簡易 TCP KV server
// =============================================================================
//
// 協議規則：
// - 一行一個指令，格式與 REPL 相同（例如：set user:1 Alice）
// - 每個指令回傳一行文字結果
// - 每個 client 連線由一個 thread 處理

use std::io::{BufRead, BufReader, BufWriter, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::{Arc, Mutex};
use std::thread;

use crate::error::Result;
use crate::storage::memory::MemTable;
use crate::storage::traits::StorageEngine;

/// 預設監聽位址
pub const DEFAULT_HOST: &str = "127.0.0.1";

/// 預設監聽 Port
pub const DEFAULT_PORT: u16 = 6379;

/// 對外啟動入口：建立 listener 後開始服務
pub fn run_server(port: u16) -> Result<()> {
    let addr = format!("{}:{}", DEFAULT_HOST, port);
    let listener = TcpListener::bind(&addr)?;
    let engine = Arc::new(Mutex::new(MemTable::new()));
    run_on_listener(listener, engine)
}

/// 使用既有 listener 啟動 server（方便測試時先綁定隨機 port）
pub fn run_on_listener(listener: TcpListener, engine: Arc<Mutex<MemTable>>) -> Result<()> {
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
            Err(err) => {
                // 監聽期間如果 accept 失敗，記錄後繼續服務其他連線
                eprintln!("Accept error: {}", err);
            }
        }
    }

    Ok(())
}

/// 處理單一 client 連線
fn handle_client(stream: TcpStream, engine: Arc<Mutex<MemTable>>) -> Result<()> {
    let peer = stream.peer_addr()?;
    let reader_stream = stream.try_clone()?;
    let mut reader = BufReader::new(reader_stream);
    let mut writer = BufWriter::new(stream);
    let mut line = String::new();

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

        let response = execute_command(cmd, &engine);
        writer.write_all(response.as_bytes())?;
        writer.write_all(b"\n")?;
        writer.flush()?;
    }

    println!("Client disconnected: {}", peer);
    Ok(())
}

/// 解析與執行一行指令，回傳單行文字結果
fn execute_command(line: &str, engine: &Arc<Mutex<MemTable>>) -> String {
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

fn cmd_set(parts: &[&str], engine: &Arc<Mutex<MemTable>>) -> String {
    if parts.len() < 3 {
        return "Usage: set <key> <value>".to_string();
    }

    let key = parts[1];
    let value = parts[2..].join(" ");

    match engine.lock() {
        Ok(mut db) => match db.put(key.as_bytes().to_vec(), value.as_bytes().to_vec()) {
            Ok(()) => "OK".to_string(),
            Err(err) => format!("Error: {}", err),
        },
        Err(_) => "Error: storage lock poisoned".to_string(),
    }
}

fn cmd_get(parts: &[&str], engine: &Arc<Mutex<MemTable>>) -> String {
    if parts.len() != 2 {
        return "Usage: get <key>".to_string();
    }

    let key = parts[1];
    match engine.lock() {
        Ok(db) => match db.get(key.as_bytes()) {
            Ok(Some(value)) => String::from_utf8_lossy(&value).into_owned(),
            Ok(None) => "(not found)".to_string(),
            Err(err) => format!("Error: {}", err),
        },
        Err(_) => "Error: storage lock poisoned".to_string(),
    }
}

fn cmd_delete(parts: &[&str], engine: &Arc<Mutex<MemTable>>) -> String {
    if parts.len() != 2 {
        return "Usage: delete <key>".to_string();
    }

    let key = parts[1];
    match engine.lock() {
        Ok(mut db) => match db.delete(key.as_bytes()) {
            Ok(()) => "OK".to_string(),
            Err(err) => format!("Error: {}", err),
        },
        Err(_) => "Error: storage lock poisoned".to_string(),
    }
}

fn cmd_list(engine: &Arc<Mutex<MemTable>>) -> String {
    match engine.lock() {
        Ok(db) => match db.list_all() {
            Ok(pairs) => format_pairs_or_empty(&pairs),
            Err(err) => format!("Error: {}", err),
        },
        Err(_) => "Error: storage lock poisoned".to_string(),
    }
}

fn cmd_scan(parts: &[&str], engine: &Arc<Mutex<MemTable>>) -> String {
    if parts.len() != 3 {
        return "Usage: scan <start_key> <end_key>".to_string();
    }

    let start = parts[1];
    let end = parts[2];

    match engine.lock() {
        Ok(db) => match db.scan(start.as_bytes(), end.as_bytes()) {
            Ok(pairs) => {
                if pairs.is_empty() {
                    format!("(no results in range {} .. {})", start, end)
                } else {
                    format_pairs_or_empty(&pairs)
                }
            }
            Err(err) => format!("Error: {}", err),
        },
        Err(_) => "Error: storage lock poisoned".to_string(),
    }
}

fn cmd_stats(engine: &Arc<Mutex<MemTable>>) -> String {
    match engine.lock() {
        Ok(db) => match db.list_all() {
            Ok(pairs) => {
                let count = db.count();
                let total_bytes: usize = pairs.iter().map(|(k, v)| k.len() + v.len()).sum();
                format!("Entries: {}, Data size: {} bytes", count, total_bytes)
            }
            Err(err) => format!("Error: {}", err),
        },
        Err(_) => "Error: storage lock poisoned".to_string(),
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

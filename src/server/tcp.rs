// =============================================================================
// server/tcp.rs — 使用 std::net 的簡易 TCP KV server
// =============================================================================
//
// 這個 server 採「每個連線一個 thread」模型，先以簡潔可讀為主。
//
// 協議：
// - client 每送一行文字指令，server 回一行文字結果。
// - 指令格式與 REPL 一致：set/get/delete/list/scan/stats。
//
// 與 LSM-Tree 的關係：
// - server 本身不綁定特定儲存實作，透過 StorageEngine trait 操作。
// - 因此 main 可以把 LsmEngine 傳進來，達成重啟持久化。

use std::io::{BufRead, BufReader, BufWriter, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::{Arc, Mutex};
use std::thread;

use crate::error::Result;
use crate::storage::traits::StorageEngine;

/// 預設監聽位址
pub const DEFAULT_HOST: &str = "127.0.0.1";

/// 預設監聽 Port
pub const DEFAULT_PORT: u16 = 6379;

/// 用既有 listener + 引擎啟動 server（測試很方便）。
pub fn run_on_listener<E>(listener: TcpListener, engine: Arc<Mutex<E>>) -> Result<()>
where
    E: StorageEngine + Send + 'static,
{
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
                eprintln!("Accept error: {}", err);
            }
        }
    }

    Ok(())
}

/// 由 port 直接啟動 server（實際執行時使用）。
pub fn run_server_with_engine<E>(port: u16, engine: Arc<Mutex<E>>) -> Result<()>
where
    E: StorageEngine + Send + 'static,
{
    let addr = format!("{}:{}", DEFAULT_HOST, port);
    let listener = TcpListener::bind(&addr)?;
    run_on_listener(listener, engine)
}

fn handle_client<E>(stream: TcpStream, engine: Arc<Mutex<E>>) -> Result<()>
where
    E: StorageEngine + Send + 'static,
{
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

    Ok(())
}

fn execute_command<E>(line: &str, engine: &Arc<Mutex<E>>) -> String
where
    E: StorageEngine + Send + 'static,
{
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

fn cmd_set<E>(parts: &[&str], engine: &Arc<Mutex<E>>) -> String
where
    E: StorageEngine + Send + 'static,
{
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

fn cmd_get<E>(parts: &[&str], engine: &Arc<Mutex<E>>) -> String
where
    E: StorageEngine + Send + 'static,
{
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

fn cmd_delete<E>(parts: &[&str], engine: &Arc<Mutex<E>>) -> String
where
    E: StorageEngine + Send + 'static,
{
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

fn cmd_list<E>(engine: &Arc<Mutex<E>>) -> String
where
    E: StorageEngine + Send + 'static,
{
    match engine.lock() {
        Ok(db) => match db.list_all() {
            Ok(pairs) => format_pairs_or_empty(&pairs),
            Err(err) => format!("Error: {}", err),
        },
        Err(_) => "Error: storage lock poisoned".to_string(),
    }
}

fn cmd_scan<E>(parts: &[&str], engine: &Arc<Mutex<E>>) -> String
where
    E: StorageEngine + Send + 'static,
{
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

fn cmd_stats<E>(engine: &Arc<Mutex<E>>) -> String
where
    E: StorageEngine + Send + 'static,
{
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

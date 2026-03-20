// =============================================================================
// server/http.rs -- HTTP Admin API
// =============================================================================
//
// 這個模組提供一個非常小型的 HTTP admin API，直接用 std::net 手動解析 HTTP。
// 目標不是做完整的 web server，而是提供資料庫觀察與操作介面：
// - GET /health
// - GET /stats
// - GET /sstables
// - POST /compact
// - POST /flush
//
// 設計重點：
// - 不依賴外部 web framework
// - 每個連線一個 thread，保持和 TCP server 一致的簡單模型
// - 回傳 JSON，方便之後拿 curl / script / dashboard 整合

use std::io::{BufRead, BufReader, BufWriter, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::Arc;
use std::thread;

use serde::Serialize;

use crate::error::Result;
use crate::transaction::mvcc::MvccEngine;

pub const DEFAULT_HTTP_PORT: u16 = 8080;

pub fn run_http_at(host: &str, port: u16, engine: Arc<MvccEngine>) -> Result<()> {
    let addr = format!("{}:{}", host, port);
    let listener = TcpListener::bind(&addr)?;
    run_on_listener(listener, engine)
}

pub fn run_on_listener(listener: TcpListener, engine: Arc<MvccEngine>) -> Result<()> {
    let local_addr = listener.local_addr()?;
    println!("FerrisDB HTTP admin API listening on {}", local_addr);

    for incoming in listener.incoming() {
        match incoming {
            Ok(stream) => {
                let shared = Arc::clone(&engine);
                thread::spawn(move || {
                    if let Err(err) = handle_client(stream, shared) {
                        eprintln!("HTTP client error: {}", err);
                    }
                });
            }
            Err(err) => eprintln!("HTTP accept error: {}", err),
        }
    }

    Ok(())
}

fn handle_client(stream: TcpStream, engine: Arc<MvccEngine>) -> Result<()> {
    let reader_stream = stream.try_clone()?;
    let mut reader = BufReader::new(reader_stream);
    let mut writer = BufWriter::new(stream);

    let Some(request) = read_http_request(&mut reader)? else {
        return Ok(());
    };

    let response = route_request(&request.method, &request.path, &engine);
    write_http_response(&mut writer, response)?;
    writer.flush()?;
    Ok(())
}

fn route_request(method: &str, path: &str, engine: &Arc<MvccEngine>) -> HttpResponse {
    match (method, path) {
        ("GET", "/health") => json_response(
            200,
            &SimpleMessage {
                status: "ok",
                message: "ferrisdb is healthy",
            },
        ),
        ("GET", "/stats") => handle_stats(engine),
        ("GET", "/sstables") => handle_sstables(engine),
        ("POST", "/compact") => handle_compact(engine),
        ("POST", "/flush") => handle_flush(engine),
        _ => json_response(
            404,
            &ErrorMessage {
                error: "not_found".to_string(),
                message: format!("unknown route {} {}", method, path),
            },
        ),
    }
}

fn handle_stats(engine: &Arc<MvccEngine>) -> HttpResponse {
    let entries = {
        let txn = engine.begin_transaction();
        match txn.scan(&[], &[0xFF]) {
            Ok(rows) => rows.len(),
            Err(err) => {
                return json_response(
                    500,
                    &ErrorMessage {
                        error: "scan_failed".to_string(),
                        message: err.to_string(),
                    },
                )
            }
        }
    };

    let inner = engine.inner.lock().expect("mvcc engine mutex poisoned");
    let disk_usage_bytes = match inner.disk_usage_bytes() {
        Ok(bytes) => bytes,
        Err(err) => {
            return json_response(
                500,
                &ErrorMessage {
                    error: "stats_failed".to_string(),
                    message: err.to_string(),
                },
            )
        }
    };

    json_response(
        200,
        &StatsResponse {
            status: "ok",
            entries,
            sstable_count: inner.manifest_state().sstable_files.len(),
            disk_usage_bytes,
            bloom_filter_hit_rate: inner.bloom_filter_hit_rate(),
        },
    )
}

fn handle_sstables(engine: &Arc<MvccEngine>) -> HttpResponse {
    let inner = engine.inner.lock().expect("mvcc engine mutex poisoned");
    match inner.sstable_infos() {
        Ok(sstables) => json_response(
            200,
            &SstablesResponse {
                status: "ok",
                sstables,
            },
        ),
        Err(err) => json_response(
            500,
            &ErrorMessage {
                error: "sstables_failed".to_string(),
                message: err.to_string(),
            },
        ),
    }
}

fn handle_compact(engine: &Arc<MvccEngine>) -> HttpResponse {
    match engine.compact() {
        Ok(()) => json_response(
            200,
            &SimpleMessage {
                status: "ok",
                message: "compaction completed",
            },
        ),
        Err(err) => json_response(
            500,
            &ErrorMessage {
                error: "compact_failed".to_string(),
                message: err.to_string(),
            },
        ),
    }
}

fn handle_flush(engine: &Arc<MvccEngine>) -> HttpResponse {
    let mut inner = engine.inner.lock().expect("mvcc engine mutex poisoned");
    match inner.flush() {
        Ok(()) => json_response(
            200,
            &SimpleMessage {
                status: "ok",
                message: "flush completed",
            },
        ),
        Err(err) => json_response(
            500,
            &ErrorMessage {
                error: "flush_failed".to_string(),
                message: err.to_string(),
            },
        ),
    }
}

fn read_http_request(reader: &mut BufReader<TcpStream>) -> Result<Option<HttpRequest>> {
    let mut request_line = String::new();
    let bytes = reader.read_line(&mut request_line)?;
    if bytes == 0 {
        return Ok(None);
    }

    let request_line = request_line.trim_end();
    let mut parts = request_line.split_whitespace();
    let method = parts.next().unwrap_or("").to_string();
    let path = parts.next().unwrap_or("").to_string();
    let _version = parts.next().unwrap_or("").to_string();

    loop {
        let mut header = String::new();
        let bytes = reader.read_line(&mut header)?;
        if bytes == 0 || header == "\r\n" {
            break;
        }
    }
    Ok(Some(HttpRequest { method, path }))
}

fn write_http_response(writer: &mut BufWriter<TcpStream>, response: HttpResponse) -> Result<()> {
    let status_text = match response.status_code {
        200 => "OK",
        404 => "Not Found",
        500 => "Internal Server Error",
        _ => "OK",
    };

    write!(
        writer,
        "HTTP/1.1 {} {}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        response.status_code,
        status_text,
        response.body.len()
    )?;
    writer.write_all(response.body.as_bytes())?;
    Ok(())
}

fn json_response<T: Serialize>(status_code: u16, body: &T) -> HttpResponse {
    let body = serde_json::to_string(body).unwrap_or_else(|_| {
        "{\"status\":\"error\",\"message\":\"failed to serialize response\"}".to_string()
    });
    HttpResponse { status_code, body }
}

struct HttpRequest {
    method: String,
    path: String,
}

struct HttpResponse {
    status_code: u16,
    body: String,
}

#[derive(Serialize)]
struct SimpleMessage<'a> {
    status: &'a str,
    message: &'a str,
}

#[derive(Serialize)]
struct ErrorMessage {
    error: String,
    message: String,
}

#[derive(Serialize)]
struct StatsResponse<'a> {
    status: &'a str,
    entries: usize,
    sstable_count: usize,
    disk_usage_bytes: u64,
    bloom_filter_hit_rate: f64,
}

#[derive(Serialize)]
struct SstablesResponse<'a> {
    status: &'a str,
    sstables: Vec<crate::storage::lsm::SstableInfo>,
}

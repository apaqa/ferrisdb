// =============================================================================
// tests/http_test.rs -- HTTP API Tests
// =============================================================================
//
// 這裡驗證 FerrisDB 的 HTTP 介面是否能讓前端或腳本正常操作資料庫。
// 測試重點包含：
// - 既有管理 API（health / stats / flush / sstables / compact）
// - 新增的 SQL API 與 table API
// - CORS 與 OPTIONS preflight
// - 首頁是否提供中文化說明

use std::collections::HashMap;
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use ferrisdb::server::http;
use ferrisdb::storage::lsm::LsmEngine;
use ferrisdb::transaction::mvcc::MvccEngine;
use serde_json::{json, Value};

fn temp_dir(name: &str) -> std::path::PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time")
        .as_nanos();
    std::env::temp_dir().join(format!("ferrisdb-http-test-{}-{}", name, nanos))
}

#[test]
fn test_http_admin_api_end_to_end() {
    let dir = temp_dir("admin");
    let lsm = LsmEngine::open(&dir, 64).expect("open lsm");
    let engine = Arc::new(MvccEngine::new(lsm));

    {
        let mut txn = engine.begin_transaction();
        txn.put(b"k1".to_vec(), b"v1".to_vec()).expect("put k1");
        txn.put(b"k2".to_vec(), b"v2".to_vec()).expect("put k2");
        txn.commit().expect("commit");
    }

    let listener = TcpListener::bind("127.0.0.1:0").expect("bind listener");
    let addr = listener.local_addr().expect("local addr");
    let shared = Arc::clone(&engine);
    thread::spawn(move || {
        let _ = http::run_on_listener(listener, shared);
    });
    thread::sleep(Duration::from_millis(100));

    let homepage = http_request(addr, "GET", "/", &[], "");
    assert!(homepage.status_line.starts_with("HTTP/1.1 200"));
    assert!(homepage.body.contains("FerrisDB HTTP 服務"));
    assert!(homepage.body.contains("前端"));

    let health = http_request(addr, "GET", "/health", &[], "");
    assert!(health.status_line.starts_with("HTTP/1.1 200"));
    assert_eq!(
        header(&health, "access-control-allow-origin"),
        Some("*")
    );
    let health_json = parse_json_body(&health);
    assert_eq!(health_json["status"], "ok");

    let stats = http_request(addr, "GET", "/stats", &[], "");
    assert!(stats.status_line.starts_with("HTTP/1.1 200"));
    let stats_json = parse_json_body(&stats);
    assert_eq!(stats_json["status"], "ok");
    assert_eq!(stats_json["entries"], 2);

    let flush = http_request(addr, "POST", "/flush", &[], "");
    assert!(flush.status_line.starts_with("HTTP/1.1 200"));
    let flush_json = parse_json_body(&flush);
    assert_eq!(flush_json["status"], "ok");

    let sstables = http_request(addr, "GET", "/sstables", &[], "");
    assert!(sstables.status_line.starts_with("HTTP/1.1 200"));
    let sstables_json = parse_json_body(&sstables);
    assert_eq!(sstables_json["status"], "ok");
    assert!(
        sstables_json["sstables"]
            .as_array()
            .expect("sstables array")
            .len()
            >= 1
    );

    let compact = http_request(addr, "POST", "/compact", &[], "");
    assert!(compact.status_line.starts_with("HTTP/1.1 200"));
    let compact_json = parse_json_body(&compact);
    assert_eq!(compact_json["status"], "ok");

    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn test_http_sql_and_table_api_end_to_end() {
    let dir = temp_dir("sql-api");
    let lsm = LsmEngine::open(&dir, 4096).expect("open lsm");
    let engine = Arc::new(MvccEngine::new(lsm));

    let listener = TcpListener::bind("127.0.0.1:0").expect("bind listener");
    let addr = listener.local_addr().expect("local addr");
    let shared = Arc::clone(&engine);
    thread::spawn(move || {
        let _ = http::run_on_listener(listener, shared);
    });
    thread::sleep(Duration::from_millis(100));

    let options = http_request(addr, "OPTIONS", "/api/sql", &[], "");
    assert!(options.status_line.starts_with("HTTP/1.1 204"));
    assert_eq!(
        header(&options, "access-control-allow-methods"),
        Some("GET, POST, OPTIONS")
    );
    assert_eq!(
        header(&options, "access-control-allow-headers"),
        Some("Content-Type")
    );

    let create = http_request(
        addr,
        "POST",
        "/api/sql",
        &[("Content-Type", "text/plain; charset=utf-8")],
        "CREATE TABLE users (id INT, name TEXT, age INT);",
    );
    assert!(create.status_line.starts_with("HTTP/1.1 200"));
    assert_eq!(
        header(&create, "access-control-allow-origin"),
        Some("*")
    );
    let create_json = parse_json_body(&create);
    assert_eq!(create_json["success"], true);
    assert_eq!(create_json["type"], "created");
    assert_eq!(create_json["message"], "Table 'users' created");

    let insert = http_request(
        addr,
        "POST",
        "/api/sql",
        &[("Content-Type", "text/plain")],
        "INSERT INTO users VALUES (1, 'Alice', 30), (2, 'Bob', 25);",
    );
    let insert_json = parse_json_body(&insert);
    assert_eq!(insert_json["success"], true);
    assert_eq!(insert_json["type"], "inserted");
    assert_eq!(insert_json["row_count"], 2);
    assert_eq!(insert_json["columns"], json!([]));
    assert_eq!(insert_json["rows"], json!([]));

    let select = http_request(
        addr,
        "POST",
        "/api/sql",
        &[("Content-Type", "text/plain")],
        "SELECT * FROM users;",
    );
    let select_json = parse_json_body(&select);
    assert_eq!(select_json["success"], true);
    assert_eq!(select_json["type"], "select");
    assert_eq!(select_json["columns"], json!(["id", "name", "age"]));
    assert_eq!(select_json["rows"], json!([[1, "Alice", 30], [2, "Bob", 25]]));
    assert_eq!(select_json["row_count"], 2);

    let tables = http_request(addr, "GET", "/api/tables", &[], "");
    let tables_json = parse_json_body(&tables);
    assert_eq!(tables_json, json!({ "tables": ["users"] }));

    let schema = http_request(addr, "GET", "/api/tables/users/schema", &[], "");
    let schema_json = parse_json_body(&schema);
    assert_eq!(
        schema_json,
        json!({
            "table": "users",
            "columns": [
                { "name": "id", "type": "INT" },
                { "name": "name", "type": "TEXT" },
                { "name": "age", "type": "INT" }
            ]
        })
    );

    let rows = http_request(addr, "GET", "/api/tables/users/rows?limit=1", &[], "");
    let rows_json = parse_json_body(&rows);
    assert_eq!(
        rows_json,
        json!({
            "columns": ["id", "name", "age"],
            "rows": [[1, "Alice", 30]],
            "row_count": 1
        })
    );

    let _ = std::fs::remove_dir_all(dir);
}

// 中文註解：用最接近真實瀏覽器/HTTP client 的方式送出請求並解析回應。
fn http_request(
    addr: std::net::SocketAddr,
    method: &str,
    path: &str,
    headers: &[(&str, &str)],
    body: &str,
) -> TestHttpResponse {
    let mut stream = TcpStream::connect(addr).expect("connect http");
    write!(
        stream,
        "{} {} HTTP/1.1\r\nHost: {}\r\nConnection: close\r\n",
        method, path, addr
    )
    .expect("write request line");

    for (name, value) in headers {
        write!(stream, "{}: {}\r\n", name, value).expect("write header");
    }

    if !body.is_empty() {
        write!(stream, "Content-Length: {}\r\n", body.len()).expect("write content length");
    }

    write!(stream, "\r\n").expect("write header terminator");
    if !body.is_empty() {
        stream
            .write_all(body.as_bytes())
            .expect("write request body");
    }
    stream.flush().expect("flush request");

    let mut raw_response = String::new();
    stream
        .read_to_string(&mut raw_response)
        .expect("read response");
    parse_http_response(&raw_response)
}

// 中文註解：把原始 HTTP 回應拆成 status line、headers 與 body，讓斷言更直觀。
fn parse_http_response(raw_response: &str) -> TestHttpResponse {
    let mut sections = raw_response.splitn(2, "\r\n\r\n");
    let header_block = sections.next().unwrap_or("");
    let body = sections.next().unwrap_or("").to_string();

    let mut lines = header_block.lines();
    let status_line = lines.next().unwrap_or("").to_string();
    let mut headers = HashMap::new();
    for line in lines {
        if let Some((name, value)) = line.split_once(':') {
            headers.insert(name.trim().to_ascii_lowercase(), value.trim().to_string());
        }
    }

    TestHttpResponse {
        status_line,
        headers,
        body,
    }
}

// 中文註解：讀取指定 response header，避免測試中重複處理大小寫。
fn header<'a>(response: &'a TestHttpResponse, name: &str) -> Option<&'a str> {
    response.headers.get(name).map(String::as_str)
}

// 中文註解：把回應 body 解析成 JSON，讓測試可以直接比對欄位內容。
fn parse_json_body(response: &TestHttpResponse) -> Value {
    serde_json::from_str(&response.body).expect("body should be valid json")
}

struct TestHttpResponse {
    status_line: String,
    headers: HashMap<String, String>,
    body: String,
}

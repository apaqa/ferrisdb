// =============================================================================
// tests/http_test.rs -- HTTP API Tests
// =============================================================================
//
// 中文註解：
// 這裡驗證 FerrisDB Studio 依賴的 HTTP 路由都能正常工作，
// 包含首頁 HTML、SQL API、table API、storage API 與 admin API。

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

fn spawn_http_server(engine: Arc<MvccEngine>) -> std::net::SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind listener");
    let addr = listener.local_addr().expect("local addr");
    thread::spawn(move || {
        let _ = http::run_on_listener(listener, engine);
    });
    thread::sleep(Duration::from_millis(100));
    addr
}

#[test]
fn test_http_homepage_and_admin_api_end_to_end() {
    let dir = temp_dir("admin");
    let lsm = LsmEngine::open(&dir, 64).expect("open lsm");
    let engine = Arc::new(MvccEngine::new(lsm));

    {
        let mut txn = engine.begin_transaction();
        txn.put(b"k1".to_vec(), b"v1".to_vec()).expect("put k1");
        txn.put(b"k2".to_vec(), b"v2".to_vec()).expect("put k2");
        txn.commit().expect("commit");
    }

    let addr = spawn_http_server(Arc::clone(&engine));

    let homepage = http_request(addr, "GET", "/", &[], "");
    assert!(homepage.status_line.starts_with("HTTP/1.1 200"));
    assert_eq!(
        header(&homepage, "content-type"),
        Some("text/html; charset=utf-8")
    );
    assert!(homepage.body.contains("FerrisDB Studio v0.1.0"));
    assert!(homepage.body.contains("Dashboard"));
    assert!(homepage.body.contains("connected to"));

    let health = http_request(addr, "GET", "/health", &[], "");
    assert!(health.status_line.starts_with("HTTP/1.1 200"));
    assert_eq!(header(&health, "access-control-allow-origin"), Some("*"));
    let health_json = parse_json_body(&health);
    assert_eq!(health_json["status"], "ok");

    let stats = http_request(addr, "GET", "/stats", &[], "");
    assert!(stats.status_line.starts_with("HTTP/1.1 200"));
    let stats_json = parse_json_body(&stats);
    assert_eq!(stats_json["status"], "ok");
    assert_eq!(stats_json["entries"], 2);
    assert_eq!(stats_json["table_count"], 0);
    assert_eq!(stats_json["total_rows"], 0);
    assert!(stats_json["manifest_status"]["summary"].is_string());
    assert!(stats_json["wal_status"]["path"].is_string());

    let flush = http_request(addr, "POST", "/api/admin/flush", &[], "");
    assert!(flush.status_line.starts_with("HTTP/1.1 200"));
    let flush_json = parse_json_body(&flush);
    assert_eq!(flush_json["status"], "ok");

    let sstables = http_request(addr, "GET", "/sstables", &[], "");
    assert!(sstables.status_line.starts_with("HTTP/1.1 200"));
    let sstables_json = parse_json_body(&sstables);
    assert_eq!(sstables_json["status"], "ok");
    assert!(sstables_json["manifest"]["summary"].is_string());
    assert!(sstables_json["wal"]["record_count"].is_number());
    assert!(
        sstables_json["sstables"]
            .as_array()
            .expect("sstables array")
            .len()
            >= 1
    );

    let compact = http_request(addr, "POST", "/api/admin/compact", &[], "");
    assert!(compact.status_line.starts_with("HTTP/1.1 200"));
    let compact_json = parse_json_body(&compact);
    assert_eq!(compact_json["status"], "ok");

    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn test_http_sql_table_and_storage_routes_used_by_studio() {
    let dir = temp_dir("studio");
    let lsm = LsmEngine::open(&dir, 4096).expect("open lsm");
    let engine = Arc::new(MvccEngine::new(lsm));
    let addr = spawn_http_server(Arc::clone(&engine));

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
    let create_json = parse_json_body(&create);
    assert_eq!(create_json["success"], true);
    assert_eq!(create_json["type"], "created");

    let create_index = http_request(
        addr,
        "POST",
        "/api/sql",
        &[("Content-Type", "text/plain")],
        "CREATE INDEX ON users(name);",
    );
    let create_index_json = parse_json_body(&create_index);
    assert_eq!(create_index_json["success"], true);

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
    assert_eq!(
        select_json["rows"],
        json!([[1, "Alice", 30], [2, "Bob", 25]])
    );

    let explain = http_request(
        addr,
        "POST",
        "/api/sql",
        &[("Content-Type", "text/plain")],
        "EXPLAIN SELECT * FROM users WHERE name = 'Alice';",
    );
    let explain_json = parse_json_body(&explain);
    assert_eq!(explain_json["success"], true);
    assert_eq!(explain_json["type"], "explained");

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
                { "name": "id", "type": "INT", "indexed": false },
                { "name": "name", "type": "TEXT", "indexed": true },
                { "name": "age", "type": "INT", "indexed": false }
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

    let stats = http_request(addr, "GET", "/stats", &[], "");
    let stats_json = parse_json_body(&stats);
    assert_eq!(stats_json["table_count"], 1);
    assert_eq!(stats_json["total_rows"], 2);
    assert_eq!(stats_json["sstable_count"], 0);

    let sstables = http_request(addr, "GET", "/sstables", &[], "");
    let sstables_json = parse_json_body(&sstables);
    assert_eq!(sstables_json["status"], "ok");
    assert!(sstables_json["sstables"].is_array());

    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn test_http_prepared_statement_endpoints() {
    let dir = temp_dir("prepared-http");
    let lsm = LsmEngine::open(&dir, 4096).expect("open lsm");
    let engine = Arc::new(MvccEngine::new(lsm));
    let addr = spawn_http_server(Arc::clone(&engine));

    let _ = http_request(
        addr,
        "POST",
        "/api/sql",
        &[("Content-Type", "text/plain")],
        "CREATE TABLE employees (id INT, department TEXT, salary INT);",
    );
    let _ = http_request(
        addr,
        "POST",
        "/api/sql",
        &[("Content-Type", "text/plain")],
        "INSERT INTO employees VALUES (1, 'Engineering', 95000), (2, 'HR', 70000);",
    );

    let prepare = http_request(
        addr,
        "POST",
        "/api/sql/prepare",
        &[("Content-Type", "application/json")],
        r#"{"name":"dept_stmt","sql":"SELECT id FROM employees WHERE department = $1"}"#,
    );
    let prepare_json = parse_json_body(&prepare);
    assert_eq!(prepare_json["success"], true);
    assert_eq!(prepare_json["type"], "prepared");

    let execute = http_request(
        addr,
        "POST",
        "/api/sql/execute",
        &[("Content-Type", "application/json")],
        r#"{"name":"dept_stmt","params":["Engineering"]}"#,
    );
    let execute_json = parse_json_body(&execute);
    assert_eq!(execute_json["success"], true);
    assert_eq!(execute_json["type"], "select");
    assert_eq!(execute_json["rows"], json!([[1]]));

    let deallocate = http_request(
        addr,
        "POST",
        "/api/sql/deallocate",
        &[("Content-Type", "application/json")],
        r#"{"name":"dept_stmt"}"#,
    );
    let deallocate_json = parse_json_body(&deallocate);
    assert_eq!(deallocate_json["success"], true);
    assert_eq!(deallocate_json["type"], "deallocated");

    let _ = std::fs::remove_dir_all(dir);
}

// 中文註解：最小化 HTTP client，直接用 TCP 發 raw request 驗證 server 行為。
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

// 中文註解：把原始 HTTP response 切成 status line、headers 與 body。
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

fn header<'a>(response: &'a TestHttpResponse, name: &str) -> Option<&'a str> {
    response.headers.get(name).map(String::as_str)
}

fn parse_json_body(response: &TestHttpResponse) -> Value {
    serde_json::from_str(&response.body).expect("body should be valid json")
}

struct TestHttpResponse {
    status_line: String,
    headers: HashMap<String, String>,
    body: String,
}

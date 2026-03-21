// =============================================================================
// tests/multi_statement_test.rs -- Multi-statement SQL Tests
// =============================================================================
//
// 中文註解：驗證 parser 的多語句切分，以及 HTTP SQL API 的多語句執行行為。

use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use ferrisdb::server::http;
use ferrisdb::sql::parser::Parser;
use ferrisdb::storage::lsm::LsmEngine;
use ferrisdb::transaction::mvcc::MvccEngine;
use serde_json::{json, Value};

fn temp_dir(name: &str) -> std::path::PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time")
        .as_nanos();
    std::env::temp_dir().join(format!("ferrisdb-multi-sql-{}-{}", name, nanos))
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
fn test_parse_multiple_skips_empty_statements() {
    let statements = Parser::parse_multiple(
        " ; ; CREATE TABLE users (id INT, name TEXT); ; INSERT INTO users VALUES (1, 'Alice'); ; ",
    )
    .expect("parse multiple");
    assert_eq!(statements.len(), 2);
}

#[test]
fn test_http_multi_statement_create_insert_select() {
    let dir = temp_dir("success");
    let engine = Arc::new(MvccEngine::new(
        LsmEngine::open(&dir, 4096).expect("open lsm"),
    ));
    let addr = spawn_http_server(engine);

    let response = http_request(
        addr,
        "POST",
        "/api/sql",
        &[("Content-Type", "text/plain; charset=utf-8")],
        "CREATE TABLE users (id INT, name TEXT); INSERT INTO users VALUES (1, 'Alice'), (2, 'Bob'); SELECT * FROM users;",
    );
    assert!(response.status_line.starts_with("HTTP/1.1 200"));
    let json = parse_json_body(&response);

    assert_eq!(json["success"], true);
    assert_eq!(json["executed_count"], 3);
    assert_eq!(json["type"], "select");
    assert_eq!(json["rows"], json!([[1, "Alice"], [2, "Bob"]]));
    assert_eq!(
        json["statement_results"]
            .as_array()
            .expect("statement results")
            .len(),
        3
    );

    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn test_http_multi_statement_stops_on_error() {
    let dir = temp_dir("error");
    let engine = Arc::new(MvccEngine::new(
        LsmEngine::open(&dir, 4096).expect("open lsm"),
    ));
    let addr = spawn_http_server(engine);

    let response = http_request(
        addr,
        "POST",
        "/api/sql",
        &[("Content-Type", "text/plain; charset=utf-8")],
        "CREATE TABLE users (id INT, name TEXT); INSERT INTO users VALUES (1, 'Alice'); INSERT INTO missing VALUES (2, 'Bob'); SELECT * FROM users;",
    );
    assert!(response.status_line.starts_with("HTTP/1.1 200"));
    let json = parse_json_body(&response);

    assert_eq!(json["success"], false);
    assert_eq!(json["executed_count"], 2);
    assert!(json["message"]
        .as_str()
        .expect("message")
        .contains("executed 2"));
    assert_eq!(
        json["statement_results"]
            .as_array()
            .expect("statement results")
            .len(),
        3
    );
    assert_eq!(json["statement_results"][2]["success"], false);

    let _ = std::fs::remove_dir_all(dir);
}

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

fn parse_http_response(raw_response: &str) -> TestHttpResponse {
    let mut sections = raw_response.splitn(2, "\r\n\r\n");
    let header_block = sections.next().unwrap_or("");
    let body = sections.next().unwrap_or("").to_string();

    let mut lines = header_block.lines();
    let status_line = lines.next().unwrap_or("").to_string();
    for line in lines {
        let _ = line;
    }

    TestHttpResponse { status_line, body }
}

fn parse_json_body(response: &TestHttpResponse) -> Value {
    serde_json::from_str(&response.body).expect("body should be valid json")
}

struct TestHttpResponse {
    status_line: String,
    body: String,
}

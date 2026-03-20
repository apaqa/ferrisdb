// =============================================================================
// tests/http_test.rs -- HTTP Admin API Tests
// =============================================================================
//
// 這些測試驗證手寫的 HTTP admin API 是否能正確：
// - 回應 health / stats / sstables
// - 接受 flush / compact 管理操作
// - 回傳 JSON，而不是純文字或 panic

use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use ferrisdb::server::http;
use ferrisdb::storage::lsm::LsmEngine;
use ferrisdb::transaction::mvcc::MvccEngine;
use serde_json::Value;

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

    let health = http_request(addr, "GET", "/health");
    assert!(health.starts_with("HTTP/1.1 200"));
    let health_json = parse_json_body(&health);
    assert_eq!(health_json["status"], "ok");

    let stats = http_request(addr, "GET", "/stats");
    assert!(stats.starts_with("HTTP/1.1 200"));
    let stats_json = parse_json_body(&stats);
    assert_eq!(stats_json["status"], "ok");
    assert_eq!(stats_json["entries"], 2);

    let flush = http_request(addr, "POST", "/flush");
    assert!(flush.starts_with("HTTP/1.1 200"));
    let flush_json = parse_json_body(&flush);
    assert_eq!(flush_json["status"], "ok");

    let sstables = http_request(addr, "GET", "/sstables");
    assert!(sstables.starts_with("HTTP/1.1 200"));
    let sstables_json = parse_json_body(&sstables);
    assert_eq!(sstables_json["status"], "ok");
    assert!(
        sstables_json["sstables"]
            .as_array()
            .expect("sstables array")
            .len()
            >= 1
    );

    let compact = http_request(addr, "POST", "/compact");
    assert!(compact.starts_with("HTTP/1.1 200"));
    let compact_json = parse_json_body(&compact);
    assert_eq!(compact_json["status"], "ok");

    let _ = std::fs::remove_dir_all(dir);
}

fn http_request(addr: std::net::SocketAddr, method: &str, path: &str) -> String {
    let mut stream = TcpStream::connect(addr).expect("connect http");
    write!(
        stream,
        "{} {} HTTP/1.1\r\nHost: {}\r\nConnection: close\r\n\r\n",
        method, path, addr
    )
    .expect("write request");
    stream.flush().expect("flush request");

    let mut response = String::new();
    stream.read_to_string(&mut response).expect("read response");
    response
}

fn parse_json_body(response: &str) -> Value {
    let body = response
        .split("\r\n\r\n")
        .nth(1)
        .expect("http body should exist");
    serde_json::from_str(body).expect("body should be valid json")
}

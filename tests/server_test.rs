// =============================================================================
// tests/server_test.rs — TCP server 整合測試
// =============================================================================
//
// server 現在跑在 MvccEngine 上，但對外仍然是 auto-commit 指令模式。
// 這個測試驗證：
// - client 可透過 TCP 做基本 CRUD
// - list / scan / stats 回應正確

use std::io::{BufRead, BufReader, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::Arc;
use std::thread;
use std::time::{SystemTime, UNIX_EPOCH, Duration};

use ferrisdb::server::tcp;
use ferrisdb::storage::lsm::LsmEngine;
use ferrisdb::transaction::mvcc::MvccEngine;

fn temp_dir(name: &str) -> std::path::PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time")
        .as_nanos();
    std::env::temp_dir().join(format!("ferrisdb-server-{}-{}", name, nanos))
}

#[test]
fn test_tcp_server_end_to_end() {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind test listener");
    let port = listener.local_addr().expect("read local addr").port();

    let dir = temp_dir("mvcc");
    let lsm = LsmEngine::open(&dir, 4096).expect("open lsm");
    let engine = Arc::new(MvccEngine::new(lsm));

    let server_engine = Arc::clone(&engine);
    thread::spawn(move || {
        let _ = tcp::run_on_listener(listener, server_engine);
    });

    let mut stream = connect_with_retry(port, 20, Duration::from_millis(50));
    let reader_stream = stream.try_clone().expect("clone stream");
    let mut reader = BufReader::new(reader_stream);

    assert_eq!(send_and_read(&mut stream, &mut reader, "set user:1 Alice"), "OK");
    assert_eq!(send_and_read(&mut stream, &mut reader, "set user:2 Bob"), "OK");
    assert_eq!(send_and_read(&mut stream, &mut reader, "get user:1"), "Alice");
    assert_eq!(send_and_read(&mut stream, &mut reader, "delete user:2"), "OK");
    assert_eq!(
        send_and_read(&mut stream, &mut reader, "get user:2"),
        "(not found)"
    );

    assert_eq!(
        send_and_read(&mut stream, &mut reader, "set user:2 Bob Updated"),
        "OK"
    );

    let list_resp = send_and_read(&mut stream, &mut reader, "list");
    assert!(list_resp.contains("user:1 -> Alice"));
    assert!(list_resp.contains("user:2 -> Bob Updated"));
    assert!(list_resp.contains("(2 entries)"));

    let scan_resp = send_and_read(&mut stream, &mut reader, "scan user:1 user:2");
    assert!(scan_resp.contains("user:1 -> Alice"));
    assert!(scan_resp.contains("user:2 -> Bob Updated"));

    let stats_resp = send_and_read(&mut stream, &mut reader, "stats");
    assert!(stats_resp.contains("Entries: 2"));
    assert!(stats_resp.contains("Data size:"));
}

fn connect_with_retry(port: u16, max_retry: usize, interval: Duration) -> TcpStream {
    let addr = format!("127.0.0.1:{}", port);

    for _ in 0..max_retry {
        if let Ok(stream) = TcpStream::connect(&addr) {
            return stream;
        }
        thread::sleep(interval);
    }

    panic!("failed to connect to test server at {}", addr);
}

fn send_and_read(
    stream: &mut TcpStream,
    reader: &mut BufReader<TcpStream>,
    cmd: &str,
) -> String {
    stream
        .write_all(format!("{}\n", cmd).as_bytes())
        .expect("write command");
    stream.flush().expect("flush command");

    let mut resp = String::new();
    reader.read_line(&mut resp).expect("read response");
    resp.trim_end().to_string()
}

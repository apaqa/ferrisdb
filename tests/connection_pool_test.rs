// =============================================================================
// tests/connection_pool_test.rs -- Connection Pool 測試
// =============================================================================
//
// 中文註解：
// 這裡驗證 connection pool 的同步行為，而不是 HTTP 協定本身。
// 真實資料庫常見的需求是：
// - 同時進來很多請求時，不要無限制建立 session
// - 超過上限的請求要等待
// - 有人 release 後要能繼續往前執行

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{mpsc, Arc, Barrier};
use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use ferrisdb::server::connection_pool::ConnectionPool;
use ferrisdb::storage::lsm::LsmEngine;
use ferrisdb::transaction::mvcc::MvccEngine;

fn temp_dir(name: &str) -> std::path::PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time")
        .as_nanos();
    std::env::temp_dir().join(format!("ferrisdb-pool-{}-{}", name, nanos))
}

fn open_pool(name: &str, max_connections: usize) -> ConnectionPool {
    let dir = temp_dir(name);
    let lsm = LsmEngine::open(&dir, 4096).expect("open lsm");
    let engine = Arc::new(MvccEngine::new(lsm));
    ConnectionPool::new(engine, max_connections)
}

#[test]
fn test_concurrent_connections_do_not_exceed_max() {
    let pool = open_pool("max", 3);
    let barrier = Arc::new(Barrier::new(10));
    let current = Arc::new(AtomicUsize::new(0));
    let max_seen = Arc::new(AtomicUsize::new(0));
    let mut handles = Vec::new();

    for _ in 0..10 {
        let pool = pool.clone();
        let barrier = Arc::clone(&barrier);
        let current = Arc::clone(&current);
        let max_seen = Arc::clone(&max_seen);
        handles.push(thread::spawn(move || {
            barrier.wait();
            let _connection = pool.acquire();
            let active = current.fetch_add(1, Ordering::SeqCst) + 1;
            loop {
                let observed = max_seen.load(Ordering::SeqCst);
                if active <= observed {
                    break;
                }
                if max_seen
                    .compare_exchange(observed, active, Ordering::SeqCst, Ordering::SeqCst)
                    .is_ok()
                {
                    break;
                }
            }
            thread::sleep(Duration::from_millis(50));
            current.fetch_sub(1, Ordering::SeqCst);
        }));
    }

    for handle in handles {
        handle.join().expect("join worker");
    }

    assert!(max_seen.load(Ordering::SeqCst) <= 3);
}

#[test]
fn test_waiters_resume_after_release() {
    let pool = open_pool("wait", 1);
    let first = pool.acquire();
    let (tx, rx) = mpsc::channel();
    let pool_for_thread = pool.clone();

    let handle = thread::spawn(move || {
        let _second = pool_for_thread.acquire();
        tx.send(()).expect("notify acquired");
    });

    thread::sleep(Duration::from_millis(100));
    assert!(rx.try_recv().is_err(), "second acquire should still be waiting");
    assert_eq!(pool.stats().waiting_queue, 1);

    pool.release(first);
    rx.recv_timeout(Duration::from_secs(2))
        .expect("second acquire should continue after release");
    handle.join().expect("join waiter");
}

#[test]
fn test_pool_stats_are_updated_correctly() {
    let pool = open_pool("stats", 2);
    let first = pool.acquire();
    let second = pool.acquire();
    let stats = pool.stats();
    assert_eq!(stats.max_connections, 2);
    assert_eq!(stats.active_connections, 2);
    assert_eq!(stats.waiting_queue, 0);

    let pool_for_thread = pool.clone();
    let (tx, rx) = mpsc::channel();
    let handle = thread::spawn(move || {
        let conn = pool_for_thread.acquire();
        tx.send(()).expect("notify acquire");
        drop(conn);
    });

    thread::sleep(Duration::from_millis(100));
    let waiting_stats = pool.stats();
    assert_eq!(waiting_stats.active_connections, 2);
    assert_eq!(waiting_stats.waiting_queue, 1);

    pool.release(first);
    rx.recv_timeout(Duration::from_secs(2))
        .expect("waiter should acquire after release");
    thread::sleep(Duration::from_millis(50));
    let final_stats = pool.stats();
    assert!(final_stats.active_connections <= 2);
    assert_eq!(final_stats.waiting_queue, 0);

    drop(second);
    handle.join().expect("join stats waiter");
}

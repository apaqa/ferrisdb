// =============================================================================
// server/connection_pool.rs -- Connection Pool 模擬
// =============================================================================
//
// 中文註解：
// 這個模組用 std::sync::Mutex + Condvar 模擬資料庫常見的 connection pool。
// HTTP SQL 請求在進 executor 前，會先向 pool 取得一條「邏輯連線」：
// - 如果目前 active_connections 還沒滿，就立刻拿到一個 session。
// - 如果已達上限，就進入等待佇列，直到別的請求 release。
//
// 這裡的 Connection 不是真的 TCP socket，而是「一個帶有 SqlExecutor session 的工作單位」。
// 這樣我們可以測試：
// 1. 同時最多只有 max_connections 個 SQL session 在執行
// 2. 超過上限的請求會等待
// 3. release 後會喚醒等待中的請求

use std::sync::{Arc, Condvar, Mutex};

use crate::sql::executor::SqlExecutor;
use crate::transaction::mvcc::MvccEngine;

#[derive(Debug)]
struct PoolState {
    active_connections: usize,
    waiting_queue: usize,
}

#[derive(Debug)]
struct ConnectionPoolInner {
    engine: Arc<MvccEngine>,
    max_connections: usize,
    state: Mutex<PoolState>,
    condvar: Condvar,
}

#[derive(Debug, Clone)]
pub struct ConnectionPool {
    inner: Arc<ConnectionPoolInner>,
}

#[derive(Debug)]
pub struct Connection {
    inner: Option<Arc<ConnectionPoolInner>>,
    executor: SqlExecutor,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ConnectionPoolStats {
    pub max_connections: usize,
    pub active_connections: usize,
    pub waiting_queue: usize,
}

impl ConnectionPool {
    pub fn new(engine: Arc<MvccEngine>, max_connections: usize) -> Self {
        Self {
            inner: Arc::new(ConnectionPoolInner {
                engine,
                max_connections: max_connections.max(1),
                state: Mutex::new(PoolState {
                    active_connections: 0,
                    waiting_queue: 0,
                }),
                condvar: Condvar::new(),
            }),
        }
    }

    pub fn acquire(&self) -> Connection {
        let mut state = self.inner.state.lock().expect("connection pool mutex poisoned");
        while state.active_connections >= self.inner.max_connections {
            state.waiting_queue += 1;
            state = self
                .inner
                .condvar
                .wait(state)
                .expect("connection pool condvar poisoned");
            state.waiting_queue = state.waiting_queue.saturating_sub(1);
        }
        state.active_connections += 1;
        drop(state);

        Connection {
            inner: Some(Arc::clone(&self.inner)),
            executor: SqlExecutor::new(Arc::clone(&self.inner.engine)),
        }
    }

    pub fn release(&self, connection: Connection) {
        drop(connection);
    }

    pub fn stats(&self) -> ConnectionPoolStats {
        let state = self.inner.state.lock().expect("connection pool mutex poisoned");
        ConnectionPoolStats {
            max_connections: self.inner.max_connections,
            active_connections: state.active_connections,
            waiting_queue: state.waiting_queue,
        }
    }
}

impl Connection {
    pub fn executor(&self) -> &SqlExecutor {
        &self.executor
    }

    fn release_inner(&mut self) {
        let Some(inner) = self.inner.take() else {
            return;
        };
        let mut state = inner.state.lock().expect("connection pool mutex poisoned");
        state.active_connections = state.active_connections.saturating_sub(1);
        inner.condvar.notify_one();
    }
}

impl Drop for Connection {
    fn drop(&mut self) {
        self.release_inner();
    }
}

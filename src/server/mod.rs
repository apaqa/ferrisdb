// =============================================================================
// server/mod.rs — server 模組入口
// =============================================================================
//
// 這裡只負責匯出子模組，讓外部可以用 `ferrisdb::server::tcp::...`。

pub mod connection_pool;
pub mod http;
pub mod static_assets;
pub mod tcp;

// =============================================================================
// config.rs -- FerrisDB Configuration
// =============================================================================
//
// 這個模組負責把原本寫死在程式裡的參數集中管理。
// 第一版採用 TOML 檔案 + CLI 覆蓋的方式：
// - ferrisdb.toml 提供專案或部署環境的預設設定
// - CLI 參數提供臨時覆蓋，方便測試與 benchmark
//
// 載入順序：
// 1. 先用內建預設值建立 FerrisDbConfig
// 2. 如果 ferrisdb.toml 存在，讀進來覆蓋預設
// 3. 再用 CLI 參數覆蓋

use std::fs;

use serde::Deserialize;

use crate::error::{FerrisDbError, Result};

fn default_data_dir() -> String {
    "./ferrisdb-data".to_string()
}

fn default_memtable_size_threshold() -> usize {
    4096
}

fn default_compaction_threshold() -> usize {
    4
}

fn default_server_host() -> String {
    "127.0.0.1".to_string()
}

fn default_server_port() -> u16 {
    6379
}

fn default_max_connections() -> usize {
    4
}

fn default_wal_mode() -> WalMode {
    WalMode::Wal
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WalMode {
    Wal,
    WalDisabled,
    Sync,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(default)]
pub struct FerrisDbConfig {
    pub data_dir: String,
    pub memtable_size_threshold: usize,
    pub compaction_threshold: usize,
    pub server_host: String,
    pub server_port: u16,
    pub max_connections: usize,
    pub wal_mode: WalMode,
}

impl Default for FerrisDbConfig {
    fn default() -> Self {
        Self {
            data_dir: default_data_dir(),
            memtable_size_threshold: default_memtable_size_threshold(),
            compaction_threshold: default_compaction_threshold(),
            server_host: default_server_host(),
            server_port: default_server_port(),
            max_connections: default_max_connections(),
            wal_mode: default_wal_mode(),
        }
    }
}

impl FerrisDbConfig {
    pub fn from_file(path: &str) -> Result<Self> {
        let content = fs::read_to_string(path)?;
        toml::from_str(&content).map_err(|err| {
            FerrisDbError::InvalidCommand(format!("failed to parse config '{}': {}", path, err))
        })
    }

    pub fn merge_cli_args(&mut self, args: &[String]) -> Result<()> {
        let mut idx = 0;
        while idx < args.len() {
            match args[idx].as_str() {
                "--data-dir" => {
                    let value = args.get(idx + 1).ok_or_else(|| {
                        FerrisDbError::InvalidCommand(
                            "missing value for --data-dir".to_string(),
                        )
                    })?;
                    self.data_dir = value.clone();
                    idx += 2;
                }
                "--port" => {
                    let value = args.get(idx + 1).ok_or_else(|| {
                        FerrisDbError::InvalidCommand("missing value for --port".to_string())
                    })?;
                    self.server_port = value.parse().map_err(|_| {
                        FerrisDbError::InvalidCommand(format!("invalid port '{}'", value))
                    })?;
                    idx += 2;
                }
                "--memtable-threshold" => {
                    let value = args.get(idx + 1).ok_or_else(|| {
                        FerrisDbError::InvalidCommand(
                            "missing value for --memtable-threshold".to_string(),
                        )
                    })?;
                    self.memtable_size_threshold = value.parse().map_err(|_| {
                        FerrisDbError::InvalidCommand(format!(
                            "invalid memtable threshold '{}'",
                            value
                        ))
                    })?;
                    idx += 2;
                }
                "--server" => {
                    idx += 1;
                }
                _ => {
                    idx += 1;
                }
            }
        }

        Ok(())
    }
}

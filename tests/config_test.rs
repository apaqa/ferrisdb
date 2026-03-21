// =============================================================================
// tests/config_test.rs -- Config Tests
// =============================================================================
//
// 這些測試驗證設定系統：
// - 預設值是否正確
// - TOML 解析是否會把缺少欄位補成預設值
// - 不合法 TOML 是否會回傳錯誤

use std::fs;
use std::time::{SystemTime, UNIX_EPOCH};

use ferrisdb::config::{FerrisDbConfig, WalMode};

fn temp_file(name: &str) -> std::path::PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time")
        .as_nanos();
    std::env::temp_dir().join(format!("ferrisdb-config-{}-{}.toml", name, nanos))
}

#[test]
fn test_default_values_are_correct() {
    let config = FerrisDbConfig::default();
    assert_eq!(config.data_dir, "./ferrisdb-data");
    assert_eq!(config.memtable_size_threshold, 4096);
    assert_eq!(config.compaction_threshold, 4);
    assert_eq!(config.server_host, "127.0.0.1");
    assert_eq!(config.server_port, 6379);
    assert_eq!(config.wal_mode, WalMode::Wal);
}

#[test]
fn test_from_toml_file_parses_values() {
    let path = temp_file("parse");
    fs::write(
        &path,
        r#"
data_dir = "./custom-data"
memtable_size_threshold = 8192
compaction_threshold = 8
server_host = "0.0.0.0"
server_port = 7000
wal_mode = "wal_disabled"
"#,
    )
    .expect("write config");

    let config = FerrisDbConfig::from_file(path.to_str().expect("utf8 path")).expect("parse");
    assert_eq!(config.data_dir, "./custom-data");
    assert_eq!(config.memtable_size_threshold, 8192);
    assert_eq!(config.compaction_threshold, 8);
    assert_eq!(config.server_host, "0.0.0.0");
    assert_eq!(config.server_port, 7000);
    assert_eq!(config.wal_mode, WalMode::WalDisabled);

    let _ = fs::remove_file(path);
}

#[test]
fn test_missing_fields_fall_back_to_defaults() {
    let path = temp_file("defaults");
    fs::write(
        &path,
        r#"
server_port = 7777
"#,
    )
    .expect("write config");

    let config = FerrisDbConfig::from_file(path.to_str().expect("utf8 path")).expect("parse");
    assert_eq!(config.server_port, 7777);
    assert_eq!(config.data_dir, "./ferrisdb-data");
    assert_eq!(config.memtable_size_threshold, 4096);
    assert_eq!(config.compaction_threshold, 4);
    assert_eq!(config.server_host, "127.0.0.1");
    assert_eq!(config.wal_mode, WalMode::Wal);

    let _ = fs::remove_file(path);
}

#[test]
fn test_invalid_toml_returns_error() {
    let path = temp_file("invalid");
    fs::write(&path, "server_port = not-a-number").expect("write invalid config");

    let err = FerrisDbConfig::from_file(path.to_str().expect("utf8 path"))
        .expect_err("invalid toml should fail");
    assert!(format!("{}", err).contains("failed to parse config"));

    let _ = fs::remove_file(path);
}

#[test]
fn test_merge_cli_args_overrides_config_values() {
    let mut config = FerrisDbConfig::default();
    let args = vec![
        "--data-dir".to_string(),
        "./bench-data".to_string(),
        "--port".to_string(),
        "7777".to_string(),
        "--memtable-threshold".to_string(),
        "8192".to_string(),
    ];

    config.merge_cli_args(&args).expect("merge cli args");

    assert_eq!(config.data_dir, "./bench-data");
    assert_eq!(config.server_port, 7777);
    assert_eq!(config.memtable_size_threshold, 8192);
}

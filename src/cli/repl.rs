// =============================================================================
// cli/repl.rs — MVCC REPL
// =============================================================================
//
// 這個 REPL 建立在 MvccEngine 之上。
//
// 模式：
// 1. Auto-commit（預設）
//    - 每個指令都包在一個 transaction 裡，自動 commit
//
// 2. Manual transaction
//    - `begin` 開始一個 transaction
//    - `commit` 提交
//    - `rollback` 放棄
//
// Snapshot isolation：
// - 一個 transaction 在 begin 時拿到 read_ts
// - 後續同一個 transaction 內的讀取，都只看到那個時間點以前的資料

use std::collections::BTreeMap;
use std::fs::File;
use std::io::{self, BufReader, Write};
use std::sync::Arc;

use serde::{Deserialize, Serialize};

use crate::bench::{format_bench_results, run_basic_kv_benchmark};
use crate::error::Result;
use crate::sql::executor::{format_execute_result, SqlExecutor};
use crate::sql::parser::Parser;
use crate::storage::lsm::TOMBSTONE;
use crate::storage::traits::StorageEngine;
use crate::transaction::keyutil::decode_key;
use crate::transaction::mvcc::{MvccEngine, Transaction};

#[derive(Debug, Serialize, Deserialize)]
struct JsonKvDump(BTreeMap<String, String>);

pub fn dump_to_file(engine: &dyn StorageEngine, filename: &str) -> Result<()> {
    let pairs = engine.list_all()?;
    dump_pairs_to_file(&pairs, filename)
}

pub fn load_from_file(engine: &mut dyn StorageEngine, filename: &str) -> Result<()> {
    let pairs = read_pairs_from_file(filename)?;
    for (key, value) in pairs {
        engine.put(key, value)?;
    }
    Ok(())
}

pub fn run(engine: Arc<MvccEngine>) -> Result<()> {
    println!("=== FerrisDB v0.1.0 ===");
    println!("Type 'help' for available commands.\n");

    let mut input = String::new();
    let mut active_txn: Option<Transaction> = None;
    let mut sql_mode = false;

    loop {
        print!("ferrisdb> ");
        io::stdout().flush()?;

        input.clear();
        let bytes_read = io::stdin().read_line(&mut input)?;
        if bytes_read == 0 {
            println!("\nBye!");
            break;
        }

        let line = input.trim();
        if line.is_empty() {
            continue;
        }

        if line.eq_ignore_ascii_case("sql") {
            sql_mode = true;
            println!("Switched to SQL mode");
            continue;
        }

        if line.eq_ignore_ascii_case("kv") {
            sql_mode = false;
            println!("Switched to KV mode");
            continue;
        }

        if sql_mode {
            handle_sql_line(line, &engine, active_txn.is_some());
            continue;
        }

        let parts: Vec<&str> = line.split_whitespace().collect();
        match parts[0].to_lowercase().as_str() {
            "begin" => handle_begin(&engine, &mut active_txn),
            "commit" => handle_commit(&mut active_txn),
            "rollback" => handle_rollback(&mut active_txn),
            "set" => handle_set(&engine, &mut active_txn, &parts),
            "get" => handle_get(&engine, &mut active_txn, &parts),
            "delete" | "del" => handle_delete(&engine, &mut active_txn, &parts),
            "dump" => handle_dump(&engine, &mut active_txn, &parts),
            "load" => handle_load(&engine, &mut active_txn, &parts),
            "compact" => handle_compact(&engine, &active_txn),
            "bench" => handle_bench(&engine, &active_txn),
            "flush" => handle_flush(&engine, &active_txn),
            "show" => handle_show(&engine, &active_txn, &parts),
            "debug" => handle_debug(&engine, &active_txn, &parts),
            "list" | "ls" => handle_list(&engine, &mut active_txn),
            "scan" => handle_scan(&engine, &mut active_txn, &parts),
            "stats" => handle_stats(&engine, &mut active_txn),
            "help" | "h" => handle_help(),
            "exit" | "quit" | "q" => {
                println!("Bye!");
                break;
            }
            _ => println!("Unknown command: '{}'. Type 'help' for usage.", parts[0]),
        }
    }

    Ok(())
}

fn handle_begin(engine: &Arc<MvccEngine>, active_txn: &mut Option<Transaction>) {
    if active_txn.is_some() {
        println!("Error: transaction already active");
        return;
    }

    *active_txn = Some(engine.begin_transaction());
    println!("OK");
}

fn handle_commit(active_txn: &mut Option<Transaction>) {
    let Some(txn) = active_txn.as_mut() else {
        println!("Error: no active transaction");
        return;
    };

    match txn.commit() {
        Ok(()) => {
            *active_txn = None;
            println!("OK");
        }
        Err(err) => println!("Error: {}", err),
    }
}

fn handle_rollback(active_txn: &mut Option<Transaction>) {
    let Some(txn) = active_txn.as_mut() else {
        println!("Error: no active transaction");
        return;
    };

    txn.rollback();
    *active_txn = None;
    println!("OK");
}

fn handle_set(engine: &Arc<MvccEngine>, active_txn: &mut Option<Transaction>, parts: &[&str]) {
    if parts.len() < 3 {
        println!("Usage: set <key> <value>");
        return;
    }

    let key = parts[1].as_bytes().to_vec();
    let value = parts[2..].join(" ").into_bytes();

    if let Some(txn) = active_txn.as_mut() {
        match txn.put(key, value) {
            Ok(()) => println!("OK"),
            Err(err) => println!("Error: {}", err),
        }
        return;
    }

    let mut txn = engine.begin_transaction();
    match txn.put(key, value).and_then(|_| txn.commit()) {
        Ok(()) => println!("OK"),
        Err(err) => println!("Error: {}", err),
    }
}

fn handle_get(engine: &Arc<MvccEngine>, active_txn: &mut Option<Transaction>, parts: &[&str]) {
    if parts.len() != 2 {
        println!("Usage: get <key>");
        return;
    }

    let result = if let Some(txn) = active_txn.as_ref() {
        txn.get(parts[1].as_bytes())
    } else {
        let txn = engine.begin_transaction();
        txn.get(parts[1].as_bytes())
    };

    match result {
        Ok(Some(value)) => println!("{}", String::from_utf8_lossy(&value)),
        Ok(None) => println!("(not found)"),
        Err(err) => println!("Error: {}", err),
    }
}

fn handle_delete(engine: &Arc<MvccEngine>, active_txn: &mut Option<Transaction>, parts: &[&str]) {
    if parts.len() != 2 {
        println!("Usage: delete <key>");
        return;
    }

    if let Some(txn) = active_txn.as_mut() {
        match txn.delete(parts[1].as_bytes()) {
            Ok(()) => println!("OK"),
            Err(err) => println!("Error: {}", err),
        }
        return;
    }

    let mut txn = engine.begin_transaction();
    match txn.delete(parts[1].as_bytes()).and_then(|_| txn.commit()) {
        Ok(()) => println!("OK"),
        Err(err) => println!("Error: {}", err),
    }
}

fn handle_dump(engine: &Arc<MvccEngine>, active_txn: &mut Option<Transaction>, parts: &[&str]) {
    if parts.len() != 2 {
        println!("Usage: dump <filename>");
        return;
    }

    let pairs = if let Some(txn) = active_txn.as_ref() {
        txn.scan(&[], &[0xFF])
    } else {
        let txn = engine.begin_transaction();
        txn.scan(&[], &[0xFF])
    };

    match pairs.and_then(|pairs| dump_pairs_to_file(&pairs, parts[1])) {
        Ok(()) => println!("OK"),
        Err(err) => println!("Error: {}", err),
    }
}

fn handle_load(engine: &Arc<MvccEngine>, active_txn: &mut Option<Transaction>, parts: &[&str]) {
    if parts.len() != 2 {
        println!("Usage: load <filename>");
        return;
    }

    let load = read_pairs_from_file(parts[1]);
    let Ok(pairs) = load else {
        println!("Error: {}", load.expect_err("load should fail"));
        return;
    };

    if let Some(txn) = active_txn.as_mut() {
        for (key, value) in pairs {
            if let Err(err) = txn.put(key, value) {
                println!("Error: {}", err);
                return;
            }
        }
        println!("OK");
        return;
    }

    let mut txn = engine.begin_transaction();
    let result = (|| -> Result<()> {
        for (key, value) in pairs {
            txn.put(key, value)?;
        }
        txn.commit()
    })();

    match result {
        Ok(()) => println!("OK"),
        Err(err) => println!("Error: {}", err),
    }
}

fn handle_compact(engine: &Arc<MvccEngine>, active_txn: &Option<Transaction>) {
    if active_txn.is_some() {
        println!("Error: cannot compact while a transaction is active");
        return;
    }

    match engine.compact() {
        Ok(()) => println!("OK"),
        Err(err) => println!("Error: {}", err),
    }
}

fn handle_bench(engine: &Arc<MvccEngine>, active_txn: &Option<Transaction>) {
    if active_txn.is_some() {
        println!("Error: cannot run bench while a transaction is active");
        return;
    }

    match run_basic_kv_benchmark(engine, 1000) {
        Ok(results) => println!("{}", format_bench_results(&results)),
        Err(err) => println!("Error: {}", err),
    }
}

fn handle_flush(engine: &Arc<MvccEngine>, active_txn: &Option<Transaction>) {
    if active_txn.is_some() {
        println!("Error: cannot flush while a transaction is active");
        return;
    }

    match engine.inner.flush() {
        Ok(()) => println!("OK"),
        Err(err) => println!("Error: {}", err),
    }
}

fn handle_show(engine: &Arc<MvccEngine>, _active_txn: &Option<Transaction>, parts: &[&str]) {
    if parts.len() != 2 {
        println!("Usage: show <sstables|manifest|wal|stats|compaction>");
        return;
    }

    match parts[1].to_lowercase().as_str() {
        "sstables" => show_sstables(engine),
        "manifest" => show_manifest(engine),
        "wal" => show_wal(engine),
        "stats" => show_full_stats(engine),
        "compaction" => show_compaction(engine),
        _ => println!("Unknown show target: '{}'", parts[1]),
    }
}

fn handle_debug(engine: &Arc<MvccEngine>, _active_txn: &Option<Transaction>, parts: &[&str]) {
    if parts.len() != 3 || parts[1].to_lowercase() != "key" {
        println!("Usage: debug key <key>");
        return;
    }

    debug_key(engine, parts[2].as_bytes());
}

fn handle_list(engine: &Arc<MvccEngine>, active_txn: &mut Option<Transaction>) {
    let result = if let Some(txn) = active_txn.as_ref() {
        txn.scan(&[], &[0xFF])
    } else {
        let txn = engine.begin_transaction();
        txn.scan(&[], &[0xFF])
    };

    match result {
        Ok(pairs) => {
            if pairs.is_empty() {
                println!("(empty)");
                return;
            }
            for (key, value) in &pairs {
                println!(
                    "{} -> {}",
                    String::from_utf8_lossy(key),
                    String::from_utf8_lossy(value)
                );
            }
            println!("({} entries)", pairs.len());
        }
        Err(err) => println!("Error: {}", err),
    }
}

fn handle_scan(engine: &Arc<MvccEngine>, active_txn: &mut Option<Transaction>, parts: &[&str]) {
    if parts.len() != 3 {
        println!("Usage: scan <start_key> <end_key>");
        return;
    }

    let result = if let Some(txn) = active_txn.as_ref() {
        txn.scan(parts[1].as_bytes(), parts[2].as_bytes())
    } else {
        let txn = engine.begin_transaction();
        txn.scan(parts[1].as_bytes(), parts[2].as_bytes())
    };

    match result {
        Ok(pairs) => {
            if pairs.is_empty() {
                println!("(no results in range {} .. {})", parts[1], parts[2]);
                return;
            }
            for (key, value) in &pairs {
                println!(
                    "{} -> {}",
                    String::from_utf8_lossy(key),
                    String::from_utf8_lossy(value)
                );
            }
            println!("({} entries)", pairs.len());
        }
        Err(err) => println!("Error: {}", err),
    }
}

fn handle_stats(engine: &Arc<MvccEngine>, active_txn: &mut Option<Transaction>) {
    let result = if let Some(txn) = active_txn.as_ref() {
        txn.scan(&[], &[0xFF])
    } else {
        let txn = engine.begin_transaction();
        txn.scan(&[], &[0xFF])
    };

    match result {
        Ok(pairs) => {
            let total_bytes: usize = pairs.iter().map(|(k, v)| k.len() + v.len()).sum();
            println!("Entries:    {}", pairs.len());
            println!("Data size:  {} bytes", total_bytes);
        }
        Err(err) => println!("Error: {}", err),
    }
}

fn handle_help() {
    println!("Available commands:");
    println!("  sql                     Switch to SQL mode");
    println!("  kv                      Switch back to KV mode");
    println!("  begin                   Begin a transaction");
    println!("  commit                  Commit the active transaction");
    println!("  rollback                Roll back the active transaction");
    println!("  set <key> <value>       Set a key-value pair");
    println!("  get <key>               Get value by key");
    println!("  delete <key>            Delete a key (alias: del)");
    println!("  dump <filename>         Dump visible key-value pairs to a JSON file");
    println!("  load <filename>         Load key-value pairs from a JSON file");
    println!("  compact                 Compact SSTables");
    println!("  bench                   Run a simple built-in benchmark");
    println!("  flush                   Force flush the active MemTable");
    println!("  show sstables           Show active SSTables");
    println!("  show manifest           Show MANIFEST state");
    println!("  show wal                Show WAL size and record count");
    println!("  show stats              Show storage statistics");
    println!("  show compaction         Show background compaction status");
    println!("  debug key <key>         Show where a key exists in memtable / sstables");
    println!("  list                    List visible key-value pairs (alias: ls)");
    println!("  scan <start> <end>      Range scan");
    println!("  stats                   Show visible statistics");
    println!("  help                    Show this help");
    println!("  exit                    Exit the REPL");
}

fn handle_sql_line(line: &str, engine: &Arc<MvccEngine>, has_active_txn: bool) {
    if has_active_txn {
        println!("SQL error: finish or rollback the active KV transaction first");
        return;
    }

    match Parser::parse_multiple(line) {
        Ok(statements) => {
            if statements.is_empty() {
                println!("SQL parser error: empty SQL statement");
                return;
            }
            let executor = SqlExecutor::new(Arc::clone(engine));
            for (idx, stmt) in statements.into_iter().enumerate() {
                match executor.execute(stmt) {
                    Ok(result) => {
                        println!("-- statement {} --", idx + 1);
                        println!("{}", format_execute_result(&result));
                    }
                    Err(err) => {
                        println!("SQL execution error after {} statement(s): {}", idx, err);
                        break;
                    }
                }
            }
        }
        Err(err) => println!("SQL parser error: {}", err),
    }
}

fn dump_pairs_to_file(pairs: &[(Vec<u8>, Vec<u8>)], filename: &str) -> Result<()> {
    let json_map: BTreeMap<String, String> = pairs
        .iter()
        .map(|(key, value)| {
            (
                String::from_utf8_lossy(key).into_owned(),
                String::from_utf8_lossy(value).into_owned(),
            )
        })
        .collect();

    let file = File::create(filename)?;
    serde_json::to_writer_pretty(file, &JsonKvDump(json_map))?;
    Ok(())
}

fn read_pairs_from_file(filename: &str) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
    let file = File::open(filename)?;
    let reader = BufReader::new(file);
    let JsonKvDump(json_map) = serde_json::from_reader(reader)?;

    Ok(json_map
        .into_iter()
        .map(|(key, value)| (key.into_bytes(), value.into_bytes()))
        .collect())
}

fn show_sstables(engine: &Arc<MvccEngine>) {
    match engine.inner.sstable_infos() {
        Ok(infos) => {
            if infos.is_empty() {
                println!("(no sstables)");
                return;
            }

            for info in infos {
                println!(
                    "{}  size={} bytes  keys={}",
                    info.filename, info.size_bytes, info.key_count
                );
            }
        }
        Err(err) => println!("Error: {}", err),
    }
}

fn show_manifest(engine: &Arc<MvccEngine>) {
    let state = engine.inner.manifest_state();

    println!("MANIFEST:");
    println!("  next_sstable_id: {}", state.next_sstable_id);
    println!("  last_compaction_ts: {}", state.last_compaction_ts);
    if state.sstable_files.is_empty() {
        println!("  sstable_files: (empty)");
    } else {
        println!("  sstable_files:");
        for filename in state.sstable_files {
            println!("    {}", filename);
        }
    }
}

fn show_wal(engine: &Arc<MvccEngine>) {
    match engine.inner.wal_info() {
        Ok((size, records)) => {
            println!("WAL path: {}", engine.inner.wal_path().display());
            println!("WAL size: {} bytes", size);
            println!("WAL records: {}", records);
        }
        Err(err) => println!("Error: {}", err),
    }
}

fn show_full_stats(engine: &Arc<MvccEngine>) {
    let entries = {
        let txn = engine.begin_transaction();
        match txn.scan(&[], &[0xFF]) {
            Ok(pairs) => pairs.len(),
            Err(err) => {
                println!("Error: {}", err);
                return;
            }
        }
    };

    match engine.inner.disk_usage_bytes() {
        Ok(disk_usage) => {
            println!("Entries: {}", entries);
            println!(
                "SSTables: {}",
                engine.inner.manifest_state().sstable_files.len()
            );
            println!("Disk usage: {} bytes", disk_usage);
            println!(
                "Bloom filter hit rate: {:.2}%",
                engine.inner.bloom_filter_hit_rate() * 100.0
            );
        }
        Err(err) => println!("Error: {}", err),
    }
}

fn show_compaction(engine: &Arc<MvccEngine>) {
    let (enabled, last_compaction_ts, total_compactions) = engine.inner.compaction_status();
    println!("Background compaction enabled: {}", enabled);
    println!("Last compaction time: {}", last_compaction_ts);
    println!("Total compactions: {}", total_compactions);
}

fn debug_key(engine: &Arc<MvccEngine>, key: &[u8]) {
    let visible = {
        let txn = engine.begin_transaction();
        txn.get(key)
    };

    match visible {
        Ok(Some(value)) => println!("Visible value: {}", String::from_utf8_lossy(&value)),
        Ok(None) => println!("Visible value: (not found)"),
        Err(err) => {
            println!("Error: {}", err);
            return;
        }
    }

    match engine.inner.active_memtable_entries() {
        Ok(entries) => {
            let mem_matches: Vec<_> = entries
                .into_iter()
                .filter_map(|(encoded_key, value)| {
                    match decode_mvcc_entry(&encoded_key, &value, key) {
                        Some(line) => Some(line),
                        None => None,
                    }
                })
                .collect();

            if mem_matches.is_empty() {
                println!("MemTable: (no versions)");
            } else {
                println!("MemTable:");
                for line in mem_matches {
                    println!("  {}", line);
                }
            }
        }
        Err(err) => {
            println!("Error: {}", err);
            return;
        }
    }

    let snapshots = match engine.inner.sstable_debug_snapshots() {
        Ok(snapshots) => snapshots,
        Err(err) => {
            println!("Error: {}", err);
            return;
        }
    };
    for (name, entries) in snapshots {
        let mut matched = Vec::new();
        for (encoded_key, value) in entries {
            if let Some(line) = decode_mvcc_entry(&encoded_key, &value, key) {
                matched.push(line);
            }
        }
        if matched.is_empty() {
            println!("{}: (no versions)", name);
        } else {
            println!("{}:", name);
            for line in matched {
                println!("  {}", line);
            }
        }
    }
}

fn decode_mvcc_entry(encoded_key: &[u8], value: &[u8], user_key: &[u8]) -> Option<String> {
    if encoded_key.len() < 8 {
        return None;
    }

    let (decoded_user_key, ts) = decode_key(encoded_key);
    if decoded_user_key != user_key {
        return None;
    }

    let value_repr = if value == TOMBSTONE {
        "__TOMBSTONE__".to_string()
    } else {
        String::from_utf8_lossy(value).into_owned()
    };
    Some(format!("ts={} value={}", ts, value_repr))
}

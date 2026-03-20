// =============================================================================
// cli/repl.rs — 命令列互動介面
// =============================================================================
//
// 這個 REPL 讓我們可以直接操作 StorageEngine。
// 除了基本的 CRUD / scan / stats，也支援：
// - dump / load：JSON 匯出匯入
// - compact：對支援 compaction 的引擎手動觸發 compact

use std::collections::BTreeMap;
use std::fs::File;
use std::io::{self, BufReader, Write};

use serde::{Deserialize, Serialize};

use crate::error::Result;
use crate::storage::traits::StorageEngine;

#[derive(Debug, Serialize, Deserialize)]
struct JsonKvDump(BTreeMap<String, String>);

pub fn run(engine: &mut dyn StorageEngine) -> Result<()> {
    println!("=== FerrisDB v0.1.0 ===");
    println!("Type 'help' for available commands.\n");

    let mut input = String::new();

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

        let parts: Vec<&str> = line.split_whitespace().collect();
        match parts[0].to_lowercase().as_str() {
            "set" => handle_set(engine, &parts),
            "get" => handle_get(engine, &parts),
            "delete" | "del" => handle_delete(engine, &parts),
            "dump" => handle_dump(engine, &parts),
            "load" => handle_load(engine, &parts),
            "compact" => handle_compact(engine),
            "list" | "ls" => handle_list(engine),
            "scan" => handle_scan(engine, &parts),
            "stats" => handle_stats(engine),
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

pub fn dump_to_file(engine: &dyn StorageEngine, filename: &str) -> Result<()> {
    let pairs = engine.list_all()?;
    let json_map: BTreeMap<String, String> = pairs
        .into_iter()
        .map(|(key, value)| {
            (
                String::from_utf8_lossy(&key).into_owned(),
                String::from_utf8_lossy(&value).into_owned(),
            )
        })
        .collect();

    let file = File::create(filename)?;
    serde_json::to_writer_pretty(file, &JsonKvDump(json_map))?;
    Ok(())
}

pub fn load_from_file(engine: &mut dyn StorageEngine, filename: &str) -> Result<()> {
    let file = File::open(filename)?;
    let reader = BufReader::new(file);
    let JsonKvDump(json_map) = serde_json::from_reader(reader)?;

    for (key, value) in json_map {
        engine.put(key.into_bytes(), value.into_bytes())?;
    }
    Ok(())
}

fn handle_set(engine: &mut dyn StorageEngine, parts: &[&str]) {
    if parts.len() < 3 {
        println!("Usage: set <key> <value>");
        return;
    }

    let key = parts[1];
    let value = parts[2..].join(" ");
    match engine.put(key.as_bytes().to_vec(), value.as_bytes().to_vec()) {
        Ok(()) => println!("OK"),
        Err(err) => println!("Error: {}", err),
    }
}

fn handle_get(engine: &mut dyn StorageEngine, parts: &[&str]) {
    if parts.len() < 2 {
        println!("Usage: get <key>");
        return;
    }

    match engine.get(parts[1].as_bytes()) {
        Ok(Some(value)) => println!("{}", String::from_utf8_lossy(&value)),
        Ok(None) => println!("(not found)"),
        Err(err) => println!("Error: {}", err),
    }
}

fn handle_delete(engine: &mut dyn StorageEngine, parts: &[&str]) {
    if parts.len() < 2 {
        println!("Usage: delete <key>");
        return;
    }

    match engine.delete(parts[1].as_bytes()) {
        Ok(()) => println!("OK"),
        Err(err) => println!("Error: {}", err),
    }
}

fn handle_dump(engine: &dyn StorageEngine, parts: &[&str]) {
    if parts.len() != 2 {
        println!("Usage: dump <filename>");
        return;
    }

    match dump_to_file(engine, parts[1]) {
        Ok(()) => println!("OK"),
        Err(err) => println!("Error: {}", err),
    }
}

fn handle_load(engine: &mut dyn StorageEngine, parts: &[&str]) {
    if parts.len() != 2 {
        println!("Usage: load <filename>");
        return;
    }

    match load_from_file(engine, parts[1]) {
        Ok(()) => println!("OK"),
        Err(err) => println!("Error: {}", err),
    }
}

fn handle_compact(engine: &mut dyn StorageEngine) {
    match engine.compact() {
        Ok(()) => println!("OK"),
        Err(err) => println!("Error: {}", err),
    }
}

fn handle_list(engine: &dyn StorageEngine) {
    match engine.list_all() {
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

fn handle_scan(engine: &dyn StorageEngine, parts: &[&str]) {
    if parts.len() < 3 {
        println!("Usage: scan <start_key> <end_key>");
        return;
    }

    match engine.scan(parts[1].as_bytes(), parts[2].as_bytes()) {
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

fn handle_stats(engine: &dyn StorageEngine) {
    let count = engine.count();
    match engine.list_all() {
        Ok(pairs) => {
            let total_bytes: usize = pairs.iter().map(|(k, v)| k.len() + v.len()).sum();
            println!("Entries:    {}", count);
            println!("Data size:  {} bytes", total_bytes);
        }
        Err(err) => println!("Error: {}", err),
    }
}

fn handle_help() {
    println!("Available commands:");
    println!("  set <key> <value>       Set a key-value pair");
    println!("  get <key>               Get value by key");
    println!("  delete <key>            Delete a key (alias: del)");
    println!("  dump <filename>         Dump all key-value pairs to a JSON file");
    println!("  load <filename>         Load key-value pairs from a JSON file");
    println!("  compact                 Compact SSTables");
    println!("  list                    List all key-value pairs (alias: ls)");
    println!("  scan <start> <end>      Range scan from start to end (inclusive)");
    println!("  stats                   Show database statistics");
    println!("  help                    Show this help (alias: h)");
    println!("  exit                    Exit the REPL (alias: quit, q)");
}

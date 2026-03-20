// =============================================================================
// cli/repl.rs — REPL 互動介面
// =============================================================================
//
// 什麼是 REPL？
// -------------
// REPL = Read-Eval-Print Loop（讀取-執行-印出-循環）
// 就是你在終端機裡打指令，它執行完把結果印出來，然後等你打下一個。
// Redis 的 redis-cli、Python 的互動模式，都是 REPL。
//
// 這個檔案做什麼？
// ----------------
// 1. 顯示提示符號 ferrisdb>
// 2. 讀取使用者輸入
// 3. 解析指令（set / get / delete / list / scan / stats / help / exit）
// 4. 呼叫 StorageEngine 執行
// 5. 把結果印出來
// 6. 回到第 1 步

use std::io::{self, Write};
use crate::error::{FerrisDbError, Result};
use crate::storage::traits::StorageEngine;

/// 啟動 REPL，傳入一個實作了 StorageEngine 的儲存引擎
///
/// 為什麼參數是 &mut dyn StorageEngine？
/// - &mut：可變借用，因為 put 和 delete 需要修改資料
/// - dyn StorageEngine：動態分派（dynamic dispatch）
///   表示「任何實作了 StorageEngine trait 的東西都可以傳進來」
///   這樣 REPL 不需要知道底層是 MemTable 還是其他引擎
///
/// 什麼是動態分派？
/// Rust 有兩種多型：
/// - 靜態分派（泛型 <T: StorageEngine>）：編譯時決定用哪個型別，效能好但會產生多份程式碼
/// - 動態分派（dyn StorageEngine）：執行時才決定，比較彈性但有一點點效能開銷
/// 對 REPL 來說，動態分派完全夠用。
pub fn run(engine: &mut dyn StorageEngine) -> Result<()> {
    println!("=== FerrisDB v0.1.0 ===");
    println!("Type 'help' for available commands.\n");

    // 建立一個可重複使用的 String buffer 來讀取輸入
    let mut input = String::new();

    loop {
        // 印出提示符號，flush 確保它立刻顯示
        // 為什麼要 flush？
        // print! 不像 println! 會自動換行並 flush，
        // 如果不 flush，提示符號可能不會立刻出現在螢幕上
        print!("ferrisdb> ");
        io::stdout().flush()?;  // ? 運算子：如果出錯就直接回傳 Err

        // 清空上一次的輸入
        input.clear();

        // 從 stdin 讀一行
        // read_line 會把整行（包含換行符號）讀進 input
        let bytes_read = io::stdin().read_line(&mut input)?;

        // 如果讀到 0 bytes，表示 stdin 關閉了（例如使用者按 Ctrl+D）
        if bytes_read == 0 {
            println!("\nBye!");
            break;
        }

        // trim() 去掉前後空白和換行符號
        let line = input.trim();

        // 空行就跳過
        if line.is_empty() {
            continue;
        }

        // 把輸入拆成單詞
        // split_whitespace() 會忽略多餘的空白
        // collect::<Vec<&str>>() 把結果收集成字串切片的陣列
        let parts: Vec<&str> = line.split_whitespace().collect();

        // 用 match 配對第一個單詞（指令名稱）
        // match 是 Rust 的模式匹配，類似 switch 但更強大
        match parts[0].to_lowercase().as_str() {
            "set" => handle_set(engine, &parts),
            "get" => handle_get(engine, &parts),
            "delete" | "del" => handle_delete(engine, &parts),
            "list" | "ls" => handle_list(engine),
            "scan" => handle_scan(engine, &parts),
            "stats" => handle_stats(engine),
            "help" | "h" => handle_help(),
            "exit" | "quit" | "q" => {
                println!("Bye!");
                break;
            }
            _ => {
                // _ 是萬用配對，表示「其他所有情況」
                println!("Unknown command: '{}'. Type 'help' for usage.", parts[0]);
            }
        }
    }

    Ok(())
}

/// 處理 set 指令：set <key> <value>
fn handle_set(engine: &mut dyn StorageEngine, parts: &[&str]) {
    if parts.len() < 3 {
        println!("Usage: set <key> <value>");
        return;
    }

    let key = parts[1];
    // 把第 3 個單詞之後的所有東西合併成 value
    // 這樣 set greeting hello world 的 value 會是 "hello world"
    let value = parts[2..].join(" ");

    match engine.put(key.as_bytes().to_vec(), value.as_bytes().to_vec()) {
        Ok(()) => println!("OK"),
        Err(e) => println!("Error: {}", e),
    }
}

/// 處理 get 指令：get <key>
fn handle_get(engine: &mut dyn StorageEngine, parts: &[&str]) {
    if parts.len() < 2 {
        println!("Usage: get <key>");
        return;
    }

    let key = parts[1];
    match engine.get(key.as_bytes()) {
        Ok(Some(value)) => {
            // 把 bytes 轉回字串來顯示
            // String::from_utf8_lossy：如果不是合法 UTF-8，會用 ? 替代
            println!("{}", String::from_utf8_lossy(&value));
        }
        Ok(None) => println!("(not found)"),
        Err(e) => println!("Error: {}", e),
    }
}

/// 處理 delete 指令：delete <key>
fn handle_delete(engine: &mut dyn StorageEngine, parts: &[&str]) {
    if parts.len() < 2 {
        println!("Usage: delete <key>");
        return;
    }

    let key = parts[1];
    match engine.delete(key.as_bytes()) {
        Ok(()) => println!("OK"),
        Err(e) => println!("Error: {}", e),
    }
}

/// 處理 list 指令：列出所有 key-value pairs
fn handle_list(engine: &mut dyn StorageEngine) {
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
        Err(e) => println!("Error: {}", e),
    }
}

/// 處理 scan 指令：scan <start_key> <end_key>
fn handle_scan(engine: &mut dyn StorageEngine, parts: &[&str]) {
    if parts.len() < 3 {
        println!("Usage: scan <start_key> <end_key>");
        return;
    }

    let start = parts[1];
    let end = parts[2];
    match engine.scan(start.as_bytes(), end.as_bytes()) {
        Ok(pairs) => {
            if pairs.is_empty() {
                println!("(no results in range {} .. {})", start, end);
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
        Err(e) => println!("Error: {}", e),
    }
}

/// 處理 stats 指令：顯示統計資訊
fn handle_stats(engine: &mut dyn StorageEngine) {
    let count = engine.count();
    // 我們需要拿到 list_all 才能算 approximate size
    // 這裡簡單處理，之後可以優化
    match engine.list_all() {
        Ok(pairs) => {
            let total_bytes: usize = pairs
                .iter()
                .map(|(k, v)| k.len() + v.len())
                .sum();
            println!("Entries:    {}", count);
            println!("Data size:  {} bytes", total_bytes);
        }
        Err(e) => println!("Error: {}", e),
    }
}

/// 顯示使用說明
fn handle_help() {
    println!("Available commands:");
    println!("  set <key> <value>       Set a key-value pair");
    println!("  get <key>               Get value by key");
    println!("  delete <key>            Delete a key (alias: del)");
    println!("  list                    List all key-value pairs (alias: ls)");
    println!("  scan <start> <end>      Range scan from start to end (inclusive)");
    println!("  stats                   Show database statistics");
    println!("  help                    Show this help (alias: h)");
    println!("  exit                    Exit the REPL (alias: quit, q)");
}

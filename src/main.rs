// =============================================================================
// main.rs — ferrisdb 程式入口
// =============================================================================
//
// 這個檔案做什麼？
// ----------------
// 1. 建立一個 MemTable（記憶體儲存引擎）
// 2. 啟動 REPL（互動式命令列）
// 3. 使用者就可以開始用 set / get / delete / list 操作資料
//
// 之後的擴充方向：
// - 加上命令列參數（選擇引擎類型、設定 port 等）
// - 加上 TCP server 模式
// - 加上設定檔支援

use ferrisdb::cli::repl;
use ferrisdb::storage::memory::MemTable;

/// 程式入口
///
/// fn main() 就像其他語言的 main 函式，是程式開始執行的地方。
///
/// 為什麼 main 不回傳 Result？
/// 其實 Rust 的 main 可以回傳 Result，但為了簡單起見，
/// 我們在這裡用 if let Err 手動處理錯誤，並印出訊息。
fn main() {
    // 建立記憶體儲存引擎
    // mut 表示這個變數是可變的，因為 put/delete 需要修改它
    let mut engine = MemTable::new();

    // 啟動 REPL，把 engine 的可變借用傳進去
    // &mut engine 是什麼？
    // & 表示「借用」（不是給出所有權），mut 表示「允許修改」
    // 這樣 REPL 可以用 engine，但 engine 的所有權還是在 main 裡
    if let Err(e) = repl::run(&mut engine) {
        // 如果 REPL 回傳錯誤，印出來然後結束
        // eprintln! 跟 println! 一樣，但是印到 stderr（標準錯誤輸出）
        eprintln!("Fatal error: {}", e);
        // 用非零 exit code 結束，表示程式異常退出
        std::process::exit(1);
    }
}

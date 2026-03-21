// =============================================================================
// server/static_assets.rs -- 嵌入式前端靜態資產
// =============================================================================
//
// 中文註解：
// 這個模組使用 include_str! 在編譯期把前端檔案直接打包進 Rust 二進位。
// 目前 FerrisDB Studio 只需要單一個 index.html，因此不需要額外的前端建置流程。

const INDEX_HTML: &str = include_str!("../../static/index.html");

/// 中文註解：依照路徑回傳對應的 content-type 與檔案內容。
pub fn get_asset(path: &str) -> Option<(&'static str, &'static str)> {
    match path {
        "/" | "/index.html" | "/static/index.html" => {
            Some(("text/html; charset=utf-8", INDEX_HTML))
        }
        _ => None,
    }
}

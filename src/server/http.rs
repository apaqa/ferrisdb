// =============================================================================
// server/http.rs -- HTTP Admin API
// =============================================================================
//
// 這個模組不追求完整的 web framework，而是用 std::net 直接提供一個
// 輕量的 HTTP 介面，讓前端頁面、curl、或其他腳本可以透過 HTTP 操作
// FerrisDB。
//
// 目前支援：
// - GET /
// - GET /health
// - GET /stats
// - GET /sstables
// - POST /compact
// - POST /flush
// - POST /api/sql
// - GET /api/tables
// - GET /api/tables/{name}/schema
// - GET /api/tables/{name}/rows?limit=100

use std::collections::HashMap;
use std::io::{BufRead, BufReader, BufWriter, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::Arc;
use std::thread;
use std::time::Instant;

use serde::Serialize;
use serde_json::Value as JsonValue;

use crate::error::{FerrisDbError, Result};
use crate::sql::ast::{DataType, Value as SqlValue};
use crate::sql::catalog::Catalog;
use crate::sql::executor::{ExecuteResult, SqlExecutor};
use crate::sql::lexer::Lexer;
use crate::sql::parser::Parser;
use crate::sql::row::{encode_row_prefix_end, encode_row_prefix_start, Row};
use crate::transaction::mvcc::MvccEngine;

pub const DEFAULT_HTTP_PORT: u16 = 8080;

pub fn run_http_at(host: &str, port: u16, engine: Arc<MvccEngine>) -> Result<()> {
    let addr = format!("{}:{}", host, port);
    let listener = TcpListener::bind(&addr)?;
    run_on_listener(listener, engine)
}

pub fn run_on_listener(listener: TcpListener, engine: Arc<MvccEngine>) -> Result<()> {
    let local_addr = listener.local_addr()?;
    println!("FerrisDB HTTP API listening on {}", local_addr);

    for incoming in listener.incoming() {
        match incoming {
            Ok(stream) => {
                let shared = Arc::clone(&engine);
                thread::spawn(move || {
                    if let Err(err) = handle_client(stream, shared) {
                        eprintln!("HTTP client error: {}", err);
                    }
                });
            }
            Err(err) => eprintln!("HTTP accept error: {}", err),
        }
    }

    Ok(())
}

fn handle_client(stream: TcpStream, engine: Arc<MvccEngine>) -> Result<()> {
    let reader_stream = stream.try_clone()?;
    let mut reader = BufReader::new(reader_stream);
    let mut writer = BufWriter::new(stream);

    let request = match read_http_request(&mut reader) {
        Ok(Some(request)) => request,
        Ok(None) => return Ok(()),
        Err(err) => {
            let response = json_error_response(400, "bad_request", err.to_string());
            write_http_response(&mut writer, response)?;
            writer.flush()?;
            return Ok(());
        }
    };

    let response = route_request(&request, &engine);
    write_http_response(&mut writer, response)?;
    writer.flush()?;
    Ok(())
}

fn route_request(request: &HttpRequest, engine: &Arc<MvccEngine>) -> HttpResponse {
    if request.method == "OPTIONS" {
        return cors_preflight_response();
    }

    match request.path.as_str() {
        "/" => {
            if request.method == "GET" {
                handle_homepage()
            } else {
                method_not_allowed(&request.method, &request.path)
            }
        }
        "/health" => {
            if request.method == "GET" {
                json_response(
                    200,
                    &SimpleMessage {
                        status: "ok",
                        message: "ferrisdb is healthy",
                    },
                )
            } else {
                method_not_allowed(&request.method, &request.path)
            }
        }
        "/stats" => {
            if request.method == "GET" {
                handle_stats(engine)
            } else {
                method_not_allowed(&request.method, &request.path)
            }
        }
        "/sstables" => {
            if request.method == "GET" {
                handle_sstables(engine)
            } else {
                method_not_allowed(&request.method, &request.path)
            }
        }
        "/compact" => {
            if request.method == "POST" {
                handle_compact(engine)
            } else {
                method_not_allowed(&request.method, &request.path)
            }
        }
        "/flush" => {
            if request.method == "POST" {
                handle_flush(engine)
            } else {
                method_not_allowed(&request.method, &request.path)
            }
        }
        "/api/sql" => {
            if request.method == "POST" {
                handle_sql_api(request, engine)
            } else {
                method_not_allowed(&request.method, &request.path)
            }
        }
        "/api/tables" => {
            if request.method == "GET" {
                handle_tables_api(engine)
            } else {
                method_not_allowed(&request.method, &request.path)
            }
        }
        _ => {
            if let Some(table_name) = extract_table_name(&request.path, "/schema") {
                return if request.method == "GET" {
                    handle_table_schema_api(engine, &table_name)
                } else {
                    method_not_allowed(&request.method, &request.path)
                };
            }

            if let Some(table_name) = extract_table_name(&request.path, "/rows") {
                return if request.method == "GET" {
                    handle_table_rows_api(request, engine, &table_name)
                } else {
                    method_not_allowed(&request.method, &request.path)
                };
            }

            json_error_response(
                404,
                "not_found",
                format!("unknown route {} {}", request.method, request.path),
            )
        }
    }
}

fn handle_stats(engine: &Arc<MvccEngine>) -> HttpResponse {
    let entries = {
        let txn = engine.begin_transaction();
        match txn.scan(&[], &[0xFF]) {
            Ok(rows) => rows.len(),
            Err(err) => return json_error_response(500, "scan_failed", err.to_string()),
        }
    };

    let inner = engine.inner.lock().expect("mvcc engine mutex poisoned");
    let disk_usage_bytes = match inner.disk_usage_bytes() {
        Ok(bytes) => bytes,
        Err(err) => return json_error_response(500, "stats_failed", err.to_string()),
    };

    json_response(
        200,
        &StatsResponse {
            status: "ok",
            entries,
            sstable_count: inner.manifest_state().sstable_files.len(),
            disk_usage_bytes,
            bloom_filter_hit_rate: inner.bloom_filter_hit_rate(),
        },
    )
}

fn handle_sstables(engine: &Arc<MvccEngine>) -> HttpResponse {
    let inner = engine.inner.lock().expect("mvcc engine mutex poisoned");
    match inner.sstable_infos() {
        Ok(sstables) => json_response(
            200,
            &SstablesResponse {
                status: "ok",
                sstables,
            },
        ),
        Err(err) => json_error_response(500, "sstables_failed", err.to_string()),
    }
}

fn handle_compact(engine: &Arc<MvccEngine>) -> HttpResponse {
    match engine.compact() {
        Ok(()) => json_response(
            200,
            &SimpleMessage {
                status: "ok",
                message: "compaction completed",
            },
        ),
        Err(err) => json_error_response(500, "compact_failed", err.to_string()),
    }
}

fn handle_flush(engine: &Arc<MvccEngine>) -> HttpResponse {
    let mut inner = engine.inner.lock().expect("mvcc engine mutex poisoned");
    match inner.flush() {
        Ok(()) => json_response(
            200,
            &SimpleMessage {
                status: "ok",
                message: "flush completed",
            },
        ),
        Err(err) => json_error_response(500, "flush_failed", err.to_string()),
    }
}

// 中文註解：回傳一個中文化首頁，方便直接用瀏覽器打開 HTTP 服務時看到 API 說明。
fn handle_homepage() -> HttpResponse {
    let body = r#"<!DOCTYPE html>
<html lang="zh-Hant">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>FerrisDB HTTP 服務</title>
  <style>
    :root {
      color-scheme: light;
      --bg: #f6f1e8;
      --card: #fffaf2;
      --ink: #1f2933;
      --accent: #b85c38;
      --line: #e4d3bf;
    }
    body {
      margin: 0;
      font-family: "Noto Sans TC", "Microsoft JhengHei", sans-serif;
      background: radial-gradient(circle at top, #fff8ef, var(--bg));
      color: var(--ink);
    }
    main {
      max-width: 960px;
      margin: 0 auto;
      padding: 40px 20px 64px;
    }
    .hero, .card {
      background: var(--card);
      border: 1px solid var(--line);
      border-radius: 18px;
      box-shadow: 0 16px 40px rgba(31, 41, 51, 0.08);
    }
    .hero {
      padding: 28px;
      margin-bottom: 24px;
    }
    .hero h1 {
      margin: 0 0 12px;
      font-size: 34px;
    }
    .hero p {
      margin: 0;
      line-height: 1.7;
    }
    .grid {
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(260px, 1fr));
      gap: 16px;
    }
    .card {
      padding: 20px;
    }
    .card h2 {
      margin-top: 0;
      font-size: 20px;
    }
    code, pre {
      font-family: "Consolas", "Courier New", monospace;
    }
    pre {
      background: #2c2f36;
      color: #f8f8f2;
      padding: 16px;
      border-radius: 12px;
      overflow-x: auto;
      margin: 0;
      line-height: 1.5;
    }
    ul {
      padding-left: 20px;
      line-height: 1.8;
    }
    .accent {
      color: var(--accent);
      font-weight: 700;
    }
  </style>
</head>
<body>
  <main>
    <section class="hero">
      <h1>FerrisDB HTTP 服務</h1>
      <p>這個介面讓前端網頁、管理工具或腳本可以直接透過 <span class="accent">HTTP + SQL</span> 操作資料庫。所有 API 都支援 CORS，瀏覽器前端可直接呼叫。</p>
    </section>
    <section class="grid">
      <article class="card">
        <h2>可用 API</h2>
        <ul>
          <li><code>POST /api/sql</code>：送出純文字 SQL，直接執行查詢或寫入。</li>
          <li><code>GET /api/tables</code>：列出目前所有資料表。</li>
          <li><code>GET /api/tables/{name}/schema</code>：查看欄位名稱與型別。</li>
          <li><code>GET /api/tables/{name}/rows?limit=100</code>：讀取前 N 筆資料。</li>
          <li><code>GET /health</code>、<code>/stats</code>、<code>/sstables</code>：查看系統狀態。</li>
        </ul>
      </article>
      <article class="card">
        <h2>前端串接範例</h2>
        <pre>fetch("/api/sql", {
  method: "POST",
  headers: { "Content-Type": "text/plain" },
  body: "SELECT * FROM users LIMIT 10;"
}).then((res) => res.json());</pre>
      </article>
      <article class="card">
        <h2>回應格式</h2>
        <p>SQL API 會回傳成功狀態、查詢型別、欄位、資料列、影響筆數與耗時毫秒數，方便前端直接渲染表格或顯示錯誤訊息。</p>
      </article>
    </section>
  </main>
</body>
</html>"#
        .to_string();

    html_response(200, body)
}

// 中文註解：執行純文字 SQL，並把執行結果轉成前端容易使用的 JSON 格式。
fn handle_sql_api(request: &HttpRequest, engine: &Arc<MvccEngine>) -> HttpResponse {
    if let Some(content_type) = request.headers.get("content-type") {
        if !content_type.starts_with("text/plain") {
            let response = SqlApiResponse::error(
                "request Content-Type must be text/plain".to_string(),
                0,
            );
            return json_response(415, &response);
        }
    }

    let sql = match String::from_utf8(request.body.clone()) {
        Ok(sql) => sql,
        Err(_) => {
            let response = SqlApiResponse::error(
                "SQL request body must be valid UTF-8 text".to_string(),
                0,
            );
            return json_response(400, &response);
        }
    };

    let response = execute_sql_text(engine, &sql);
    json_response(200, &response)
}

// 中文註解：列出目前 catalog 中所有資料表名稱，方便前端做側邊欄或下拉選單。
fn handle_tables_api(engine: &Arc<MvccEngine>) -> HttpResponse {
    let catalog = Catalog::new(Arc::clone(engine));
    let txn = engine.begin_transaction();
    match catalog.list_tables(&txn) {
        Ok(tables) => json_response(
            200,
            &TablesResponse {
                tables: tables.into_iter().map(|table| table.table_name).collect(),
            },
        ),
        Err(err) => json_error_response(500, "tables_failed", err.to_string()),
    }
}

// 中文註解：回傳指定資料表的 schema，讓前端知道每個欄位名稱與型別。
fn handle_table_schema_api(engine: &Arc<MvccEngine>, table_name: &str) -> HttpResponse {
    let catalog = Catalog::new(Arc::clone(engine));
    let txn = engine.begin_transaction();

    match catalog.get_table(&txn, table_name) {
        Ok(Some(schema)) => json_response(
            200,
            &TableSchemaResponse {
                table: schema.table_name,
                columns: schema
                    .columns
                    .into_iter()
                    .map(|column| TableColumnResponse {
                        name: column.name,
                        column_type: data_type_name(&column.data_type).to_string(),
                    })
                    .collect(),
            },
        ),
        Ok(None) => json_error_response(
            404,
            "table_not_found",
            format!("table '{}' does not exist", table_name),
        ),
        Err(err) => json_error_response(500, "schema_failed", err.to_string()),
    }
}

// 中文註解：讀取指定資料表前 N 筆資料，並依照 schema 欄位順序輸出成 JSON。
fn handle_table_rows_api(
    request: &HttpRequest,
    engine: &Arc<MvccEngine>,
    table_name: &str,
) -> HttpResponse {
    let limit = match parse_limit(&request.query) {
        Ok(limit) => limit,
        Err(message) => return json_error_response(400, "invalid_limit", message),
    };

    let catalog = Catalog::new(Arc::clone(engine));
    let txn = engine.begin_transaction();
    let Some(schema) = (match catalog.get_table(&txn, table_name) {
        Ok(schema) => schema,
        Err(err) => return json_error_response(500, "rows_failed", err.to_string()),
    }) else {
        return json_error_response(
            404,
            "table_not_found",
            format!("table '{}' does not exist", table_name),
        );
    };

    let columns: Vec<String> = schema
        .columns
        .iter()
        .map(|column| column.name.clone())
        .collect();

    let row_pairs = match txn.scan(
        &encode_row_prefix_start(table_name),
        &encode_row_prefix_end(table_name),
    ) {
        Ok(rows) => rows,
        Err(err) => return json_error_response(500, "rows_failed", err.to_string()),
    };

    let mut rows = Vec::new();
    for (_, raw_row) in row_pairs.into_iter().take(limit) {
        let row: Row = match serde_json::from_slice(&raw_row) {
            Ok(row) => row,
            Err(err) => return json_error_response(500, "rows_failed", err.to_string()),
        };
        rows.push(project_row_to_json(&row, &columns));
    }

    json_response(
        200,
        &TableRowsResponse {
            columns,
            row_count: rows.len(),
            rows,
        },
    )
}

fn read_http_request(reader: &mut BufReader<TcpStream>) -> Result<Option<HttpRequest>> {
    let mut request_line = String::new();
    let bytes = reader.read_line(&mut request_line)?;
    if bytes == 0 {
        return Ok(None);
    }

    let request_line = request_line.trim_end();
    let mut parts = request_line.split_whitespace();
    let method = parts
        .next()
        .ok_or_else(|| FerrisDbError::InvalidCommand("missing HTTP method".to_string()))?
        .to_ascii_uppercase();
    let target = parts
        .next()
        .ok_or_else(|| FerrisDbError::InvalidCommand("missing HTTP target".to_string()))?;
    let _version = parts
        .next()
        .ok_or_else(|| FerrisDbError::InvalidCommand("missing HTTP version".to_string()))?;

    let (path, query) = split_request_target(target);
    let mut headers = HashMap::new();

    loop {
        let mut header_line = String::new();
        let bytes = reader.read_line(&mut header_line)?;
        if bytes == 0 || header_line == "\r\n" {
            break;
        }

        let header_line = header_line.trim_end();
        let Some((name, value)) = header_line.split_once(':') else {
            return Err(FerrisDbError::InvalidCommand(format!(
                "invalid HTTP header '{}'",
                header_line
            )));
        };
        headers.insert(name.trim().to_ascii_lowercase(), value.trim().to_string());
    }

    let content_length = match headers.get("content-length") {
        Some(value) => value.parse::<usize>().map_err(|_| {
            FerrisDbError::InvalidCommand(format!("invalid Content-Length '{}'", value))
        })?,
        None => 0,
    };

    let mut body = vec![0_u8; content_length];
    if content_length > 0 {
        reader.read_exact(&mut body)?;
    }

    Ok(Some(HttpRequest {
        method,
        path,
        query,
        headers,
        body,
    }))
}

fn write_http_response(writer: &mut BufWriter<TcpStream>, response: HttpResponse) -> Result<()> {
    let body_bytes = response.body.as_bytes();

    write!(
        writer,
        "HTTP/1.1 {} {}\r\n",
        response.status_code,
        http_status_text(response.status_code)
    )?;
    write!(writer, "Content-Type: {}\r\n", response.content_type)?;
    write!(writer, "Content-Length: {}\r\n", body_bytes.len())?;
    write!(writer, "Connection: close\r\n")?;
    write!(writer, "Access-Control-Allow-Origin: *\r\n")?;
    write!(writer, "Access-Control-Allow-Methods: GET, POST, OPTIONS\r\n")?;
    write!(writer, "Access-Control-Allow-Headers: Content-Type\r\n")?;
    write!(writer, "\r\n")?;
    writer.write_all(body_bytes)?;
    Ok(())
}

// 中文註解：把 `POST /api/sql` 的純文字 SQL 解析、執行，並統一轉成 API 回應格式。
fn execute_sql_text(engine: &Arc<MvccEngine>, sql: &str) -> SqlApiResponse {
    let start = Instant::now();

    let mut lexer = Lexer::new(sql);
    let tokens = match lexer.tokenize() {
        Ok(tokens) => tokens,
        Err(err) => return SqlApiResponse::error(err.to_string(), elapsed_ms(start)),
    };

    let mut parser = Parser::new(tokens);
    let statement = match parser.parse() {
        Ok(statement) => statement,
        Err(err) => return SqlApiResponse::error(err.to_string(), elapsed_ms(start)),
    };

    let executor = SqlExecutor::new(Arc::clone(engine));
    let result = match executor.execute(statement) {
        Ok(result) => result,
        Err(err) => return SqlApiResponse::error(err.to_string(), elapsed_ms(start)),
    };

    SqlApiResponse::from_execute_result(result, elapsed_ms(start))
}

// 中文註解：把 SQL 的內部值型別轉成一般 JSON 值，方便前端直接顯示。
fn sql_value_to_json(value: SqlValue) -> JsonValue {
    match value {
        SqlValue::Int(value) => JsonValue::from(value),
        SqlValue::Text(value) => JsonValue::from(value),
        SqlValue::Bool(value) => JsonValue::from(value),
        SqlValue::Null => JsonValue::Null,
    }
}

// 中文註解：依照欄位順序把 row 投影成 JSON 陣列，缺少欄位時補上 null。
fn project_row_to_json(row: &Row, columns: &[String]) -> Vec<JsonValue> {
    columns
        .iter()
        .map(|column| {
            row.get(column)
                .cloned()
                .map(sql_value_to_json)
                .unwrap_or(JsonValue::Null)
        })
        .collect()
}

// 中文註解：從 `/api/tables/{name}/...` 這類路徑中抽出 table 名稱。
fn extract_table_name(path: &str, suffix: &str) -> Option<String> {
    let prefix = "/api/tables/";
    let rest = path.strip_prefix(prefix)?;
    let table_name = rest.strip_suffix(suffix)?;
    if table_name.is_empty() || table_name.contains('/') {
        return None;
    }
    Some(table_name.to_string())
}

// 中文註解：解析 query string 的 `limit` 參數，未提供時預設回傳 100 筆。
fn parse_limit(query: &HashMap<String, String>) -> std::result::Result<usize, String> {
    match query.get("limit") {
        Some(value) => value
            .parse::<usize>()
            .map_err(|_| format!("invalid limit '{}'", value)),
        None => Ok(100),
    }
}

// 中文註解：把 request target 分離成純路徑與 query map，供後續路由與參數解析使用。
fn split_request_target(target: &str) -> (String, HashMap<String, String>) {
    let Some((path, raw_query)) = target.split_once('?') else {
        return (target.to_string(), HashMap::new());
    };

    let mut query = HashMap::new();
    for pair in raw_query.split('&').filter(|pair| !pair.is_empty()) {
        let (key, value) = match pair.split_once('=') {
            Some((key, value)) => (key, value),
            None => (pair, ""),
        };
        query.insert(key.to_string(), value.to_string());
    }

    (path.to_string(), query)
}

// 中文註解：把 DataType 轉成前端較容易理解的 SQL 型別字串。
fn data_type_name(data_type: &DataType) -> &'static str {
    match data_type {
        DataType::Int => "INT",
        DataType::Text => "TEXT",
        DataType::Bool => "BOOL",
    }
}

// 中文註解：建立通用的 JSON 錯誤回應，讓 API 失敗時有一致格式。
fn json_error_response(status_code: u16, error: &str, message: String) -> HttpResponse {
    json_response(
        status_code,
        &ErrorMessage {
            error: error.to_string(),
            message,
        },
    )
}

// 中文註解：為已知路徑但錯誤 HTTP method 的情況回傳 405，方便前端排查。
fn method_not_allowed(method: &str, path: &str) -> HttpResponse {
    json_error_response(
        405,
        "method_not_allowed",
        format!("method {} is not allowed for {}", method, path),
    )
}

// 中文註解：回傳 CORS preflight 成功，讓瀏覽器可先完成 OPTIONS 驗證。
fn cors_preflight_response() -> HttpResponse {
    HttpResponse {
        status_code: 204,
        content_type: "text/plain; charset=utf-8",
        body: String::new(),
    }
}

fn html_response(status_code: u16, body: String) -> HttpResponse {
    HttpResponse {
        status_code,
        content_type: "text/html; charset=utf-8",
        body,
    }
}

fn json_response<T: Serialize>(status_code: u16, body: &T) -> HttpResponse {
    let body = serde_json::to_string(body).unwrap_or_else(|_| {
        "{\"status\":\"error\",\"message\":\"failed to serialize response\"}".to_string()
    });
    HttpResponse {
        status_code,
        content_type: "application/json; charset=utf-8",
        body,
    }
}

fn http_status_text(status_code: u16) -> &'static str {
    match status_code {
        200 => "OK",
        204 => "No Content",
        400 => "Bad Request",
        404 => "Not Found",
        405 => "Method Not Allowed",
        415 => "Unsupported Media Type",
        500 => "Internal Server Error",
        _ => "OK",
    }
}

// 中文註解：把耗時統一轉成毫秒，避免不同回應格式重複處理。
fn elapsed_ms(start: Instant) -> u64 {
    start.elapsed().as_millis().min(u128::from(u64::MAX)) as u64
}

struct HttpRequest {
    method: String,
    path: String,
    query: HashMap<String, String>,
    headers: HashMap<String, String>,
    body: Vec<u8>,
}

struct HttpResponse {
    status_code: u16,
    content_type: &'static str,
    body: String,
}

#[derive(Serialize)]
struct SimpleMessage<'a> {
    status: &'a str,
    message: &'a str,
}

#[derive(Serialize)]
struct ErrorMessage {
    error: String,
    message: String,
}

#[derive(Serialize)]
struct StatsResponse<'a> {
    status: &'a str,
    entries: usize,
    sstable_count: usize,
    disk_usage_bytes: u64,
    bloom_filter_hit_rate: f64,
}

#[derive(Serialize)]
struct SstablesResponse<'a> {
    status: &'a str,
    sstables: Vec<crate::storage::lsm::SstableInfo>,
}

#[derive(Serialize)]
struct TablesResponse {
    tables: Vec<String>,
}

#[derive(Serialize)]
struct TableSchemaResponse {
    table: String,
    columns: Vec<TableColumnResponse>,
}

#[derive(Serialize)]
struct TableColumnResponse {
    name: String,
    #[serde(rename = "type")]
    column_type: String,
}

#[derive(Serialize)]
struct TableRowsResponse {
    columns: Vec<String>,
    rows: Vec<Vec<JsonValue>>,
    row_count: usize,
}

#[derive(Serialize)]
struct SqlApiResponse {
    success: bool,
    #[serde(rename = "type")]
    kind: String,
    columns: Vec<String>,
    rows: Vec<Vec<JsonValue>>,
    row_count: usize,
    elapsed_ms: u64,
    message: String,
}

impl SqlApiResponse {
    // 中文註解：把 SQL executor 的結果對應到 HTTP API 所需的固定 JSON 欄位。
    fn from_execute_result(result: ExecuteResult, elapsed_ms: u64) -> Self {
        match result {
            ExecuteResult::Explain { plan } => Self {
                success: true,
                kind: "explained".to_string(),
                columns: vec!["plan".to_string()],
                rows: vec![vec![JsonValue::from(plan)]],
                row_count: 1,
                elapsed_ms,
                message: String::new(),
            },
            ExecuteResult::Created { table_name } => Self {
                success: true,
                kind: "created".to_string(),
                columns: Vec::new(),
                rows: Vec::new(),
                row_count: 0,
                elapsed_ms,
                message: format!("Table '{}' created", table_name),
            },
            ExecuteResult::Altered { table_name } => Self {
                success: true,
                kind: "updated".to_string(),
                columns: Vec::new(),
                rows: Vec::new(),
                row_count: 0,
                elapsed_ms,
                message: format!("Table '{}' altered", table_name),
            },
            ExecuteResult::Dropped { table_name } => Self {
                success: true,
                kind: "deleted".to_string(),
                columns: Vec::new(),
                rows: Vec::new(),
                row_count: 0,
                elapsed_ms,
                message: format!("Table '{}' dropped", table_name),
            },
            ExecuteResult::IndexCreated {
                table_name,
                column_name,
            } => Self {
                success: true,
                kind: "created".to_string(),
                columns: Vec::new(),
                rows: Vec::new(),
                row_count: 0,
                elapsed_ms,
                message: format!("Index on '{}.{}' created", table_name, column_name),
            },
            ExecuteResult::IndexDropped {
                table_name,
                column_name,
            } => Self {
                success: true,
                kind: "deleted".to_string(),
                columns: Vec::new(),
                rows: Vec::new(),
                row_count: 0,
                elapsed_ms,
                message: format!("Index on '{}.{}' dropped", table_name, column_name),
            },
            ExecuteResult::Inserted { count } => Self {
                success: true,
                kind: "inserted".to_string(),
                columns: Vec::new(),
                rows: Vec::new(),
                row_count: count,
                elapsed_ms,
                message: String::new(),
            },
            ExecuteResult::Selected { columns, rows } => Self {
                success: true,
                kind: "select".to_string(),
                row_count: rows.len(),
                elapsed_ms,
                message: String::new(),
                columns,
                rows: rows
                    .into_iter()
                    .map(|row| row.into_iter().map(sql_value_to_json).collect())
                    .collect(),
            },
            ExecuteResult::Updated { count } => Self {
                success: true,
                kind: "updated".to_string(),
                columns: Vec::new(),
                rows: Vec::new(),
                row_count: count,
                elapsed_ms,
                message: String::new(),
            },
            ExecuteResult::Deleted { count } => Self {
                success: true,
                kind: "deleted".to_string(),
                columns: Vec::new(),
                rows: Vec::new(),
                row_count: count,
                elapsed_ms,
                message: String::new(),
            },
            ExecuteResult::Error { message } => Self::error(message, elapsed_ms),
        }
    }

    // 中文註解：建立 SQL API 的錯誤回應，固定使用 `type = error`。
    fn error(message: String, elapsed_ms: u64) -> Self {
        Self {
            success: false,
            kind: "error".to_string(),
            columns: Vec::new(),
            rows: Vec::new(),
            row_count: 0,
            elapsed_ms,
            message,
        }
    }
}

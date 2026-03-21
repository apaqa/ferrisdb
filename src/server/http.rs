// =============================================================================
// server/http.rs -- HTTP Admin API
// =============================================================================
//
// ?芋蝯?餈賣?摰??web framework嚗??std::net ?湔??銝??// 頛???HTTP 隞嚗??垢??url???嗡??單?臭誑?? HTTP ??
// FerrisDB??//
// ?桀??舀嚗?// - GET /
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

use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;

use crate::error::{FerrisDbError, Result};
use crate::server::static_assets;
use crate::sql::ast::{DataType, Value as SqlValue};
use crate::sql::catalog::Catalog;
use crate::sql::executor::{ExecuteResult, SqlExecutor};
use crate::sql::index::IndexManager;
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
        "/" | "/index.html" | "/static/index.html" => {
            if request.method == "GET" {
                handle_homepage(&request.path)
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
        "/compact" | "/api/admin/compact" => {
            if request.method == "POST" {
                handle_compact(engine)
            } else {
                method_not_allowed(&request.method, &request.path)
            }
        }
        "/flush" | "/api/admin/flush" => {
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
        "/api/sql/prepare" => {
            if request.method == "POST" {
                handle_sql_prepare_api(request, engine)
            } else {
                method_not_allowed(&request.method, &request.path)
            }
        }
        "/api/sql/execute" => {
            if request.method == "POST" {
                handle_sql_execute_api(request, engine)
            } else {
                method_not_allowed(&request.method, &request.path)
            }
        }
        "/api/sql/deallocate" => {
            if request.method == "POST" {
                handle_sql_deallocate_api(request, engine)
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
    let txn = engine.begin_transaction();
    let entries = match txn.scan(&[], &[0xFF]) {
        Ok(rows) => rows.len(),
        Err(err) => return json_error_response(500, "scan_failed", err.to_string()),
    };
    let catalog = Catalog::new(Arc::clone(engine));
    let tables = match catalog.list_tables(&txn) {
        Ok(tables) => tables,
        Err(err) => return json_error_response(500, "stats_failed", err.to_string()),
    };
    // 中文註解：逐表掃描 row prefix，統計 Studio 儀表板需要的總筆數。
    let mut total_rows = 0;
    for schema in &tables {
        let rows = match txn.scan(
            &encode_row_prefix_start(&schema.table_name),
            &encode_row_prefix_end(&schema.table_name),
        ) {
            Ok(rows) => rows,
            Err(err) => return json_error_response(500, "stats_failed", err.to_string()),
        };
        total_rows += rows.len();
    }

    let disk_usage_bytes = match engine.inner.disk_usage_bytes() {
        Ok(bytes) => bytes,
        Err(err) => return json_error_response(500, "stats_failed", err.to_string()),
    };
    let manifest_state = engine.inner.manifest_state();
    let (wal_size_bytes, wal_record_count) = match engine.inner.wal_info() {
        Ok(info) => info,
        Err(err) => return json_error_response(500, "stats_failed", err.to_string()),
    };

    json_response(
        200,
        &StatsResponse {
            status: "ok",
            entries,
            table_count: tables.len(),
            total_rows,
            sstable_count: manifest_state.sstable_files.len(),
            disk_usage_bytes,
            wal_size_bytes,
            wal_record_count,
            bloom_filter_hit_rate: engine.inner.bloom_filter_hit_rate(),
            next_sstable_id: manifest_state.next_sstable_id,
            manifest_status: ManifestStatusResponse {
                summary: format!("{} SSTables tracked", manifest_state.sstable_files.len()),
                last_compaction_ts: manifest_state.last_compaction_ts,
            },
            wal_status: WalStatusResponse {
                path: engine.inner.wal_path().display().to_string(),
                size_bytes: wal_size_bytes,
                record_count: wal_record_count,
            },
        },
    )
}

fn handle_sstables(engine: &Arc<MvccEngine>) -> HttpResponse {
    let manifest_state = engine.inner.manifest_state();
    let wal_info = match engine.inner.wal_info() {
        Ok(info) => info,
        Err(err) => return json_error_response(500, "sstables_failed", err.to_string()),
    };
    match engine.inner.sstable_infos() {
        Ok(sstables) => json_response(
            200,
            &SstablesResponse {
                status: "ok",
                sstables,
                manifest: ManifestStatusResponse {
                    summary: format!("{} SSTables tracked", manifest_state.sstable_files.len()),
                    last_compaction_ts: manifest_state.last_compaction_ts,
                },
                wal: WalStatusResponse {
                    path: engine.inner.wal_path().display().to_string(),
                    size_bytes: wal_info.0,
                    record_count: wal_info.1,
                },
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
    match engine.inner.flush() {
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

// 中文註解：首頁直接回傳嵌入式 Studio 單頁應用。
fn handle_homepage(path: &str) -> HttpResponse {
    match static_assets::get_asset(path) {
        Some((content_type, content)) => text_response(200, content_type, content.to_string()),
        None => json_error_response(404, "not_found", format!("asset '{}' not found", path)),
    }
}

// 中文註解：SQL API 直接接收純文字 SQL，並回傳統一格式的 JSON。
fn handle_sql_api(request: &HttpRequest, engine: &Arc<MvccEngine>) -> HttpResponse {
    if let Some(content_type) = request.headers.get("content-type") {
        if !content_type.starts_with("text/plain") {
            let response = SqlApiResponse::error(
                "request Content-Type must be text/plain".to_string(),
                0,
                0,
                Vec::new(),
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
                0,
                Vec::new(),
            );
            return json_response(400, &response);
        }
    };

    let response = execute_sql_text(engine, &sql);
    json_response(200, &response)
}

fn handle_sql_prepare_api(request: &HttpRequest, engine: &Arc<MvccEngine>) -> HttpResponse {
    let payload: PrepareApiRequest = match parse_json_request(request) {
        Ok(payload) => payload,
        Err(response) => return response,
    };
    let sql = format!("PREPARE {} AS {}", payload.name, payload.sql.trim());
    json_response(200, &execute_sql_text(engine, &sql))
}

fn handle_sql_execute_api(request: &HttpRequest, engine: &Arc<MvccEngine>) -> HttpResponse {
    let payload: ExecutePreparedApiRequest = match parse_json_request(request) {
        Ok(payload) => payload,
        Err(response) => return response,
    };
    let executor = SqlExecutor::new(Arc::clone(engine));
    let args = match payload
        .params
        .into_iter()
        .map(json_to_sql_value)
        .collect::<std::result::Result<Vec<_>, String>>()
    {
        Ok(args) => args,
        Err(message) => {
            return json_response(
                400,
                &SqlApiResponse::error(message, 0, 0, Vec::new()),
            )
        }
    };
    match executor.execute(crate::sql::ast::Statement::Execute {
        name: payload.name,
        args,
    }) {
        Ok(result) => json_response(
            200,
            &SqlApiResponse::from_execute_result(result, 0, 1, Vec::new()),
        ),
        Err(err) => json_response(
            200,
            &SqlApiResponse::error(err.to_string(), 0, 0, Vec::new()),
        ),
    }
}

fn handle_sql_deallocate_api(request: &HttpRequest, engine: &Arc<MvccEngine>) -> HttpResponse {
    let payload: DeallocateApiRequest = match parse_json_request(request) {
        Ok(payload) => payload,
        Err(response) => return response,
    };
    let executor = SqlExecutor::new(Arc::clone(engine));
    match executor.execute(crate::sql::ast::Statement::Deallocate { name: payload.name }) {
        Ok(result) => json_response(
            200,
            &SqlApiResponse::from_execute_result(result, 0, 1, Vec::new()),
        ),
        Err(err) => json_response(
            200,
            &SqlApiResponse::error(err.to_string(), 0, 0, Vec::new()),
        ),
    }
}

// 中文註解：列出目前資料庫內所有資料表，供 Studio 左側導覽使用。
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

// 中文註解：回傳欄位型別與 index 狀態，供 Studio schema 區塊顯示。
fn handle_table_schema_api(engine: &Arc<MvccEngine>, table_name: &str) -> HttpResponse {
    let catalog = Catalog::new(Arc::clone(engine));
    let index_manager = IndexManager::new(Arc::clone(engine));
    let txn = engine.begin_transaction();

    match catalog.get_table(&txn, table_name) {
        Ok(Some(schema)) => {
            let indexed_columns: Vec<String> = match index_manager.list_indexes(&txn, table_name) {
                Ok(columns) => columns.into_iter().flatten().collect(),
                Err(err) => return json_error_response(500, "schema_failed", err.to_string()),
            };
            json_response(
                200,
                &TableSchemaResponse {
                    table: schema.table_name,
                    columns: schema
                        .columns
                        .into_iter()
                        .map(|column| TableColumnResponse {
                            indexed: indexed_columns.iter().any(|item| item == &column.name),
                            name: column.name,
                            column_type: data_type_name(&column.data_type).to_string(),
                        })
                        .collect(),
                },
            )
        }
        Ok(None) => json_error_response(
            404,
            "table_not_found",
            format!("table '{}' does not exist", table_name),
        ),
        Err(err) => json_error_response(500, "schema_failed", err.to_string()),
    }
}

// 中文註解：回傳指定資料表的前 N 筆資料，供 Studio 右側表格使用。
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

fn parse_json_request<T: for<'de> Deserialize<'de>>(
    request: &HttpRequest,
) -> std::result::Result<T, HttpResponse> {
    if let Some(content_type) = request.headers.get("content-type") {
        if !content_type.starts_with("application/json") {
            return Err(json_error_response(
                415,
                "unsupported_media_type",
                "request Content-Type must be application/json".to_string(),
            ));
        }
    }

    serde_json::from_slice(&request.body).map_err(|err| {
        json_error_response(400, "bad_json", format!("invalid JSON body: {}", err))
    })
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
    write!(
        writer,
        "Access-Control-Allow-Methods: GET, POST, OPTIONS\r\n"
    )?;
    write!(writer, "Access-Control-Allow-Headers: Content-Type\r\n")?;
    write!(writer, "\r\n")?;
    writer.write_all(body_bytes)?;
    Ok(())
}

// 中文註解：重用 parser 與 executor，把 SQL 純文字轉成 API 回應格式。
fn execute_sql_text(engine: &Arc<MvccEngine>, sql: &str) -> SqlApiResponse {
    let start = Instant::now();
    let executor = SqlExecutor::new(Arc::clone(engine));
    let statements = match Parser::parse_multiple(sql) {
        Ok(statements) if !statements.is_empty() => statements,
        Ok(_) => {
            return SqlApiResponse::error(
                "empty SQL statement".to_string(),
                elapsed_ms(start),
                0,
                Vec::new(),
            )
        }
        Err(err) => {
            return SqlApiResponse::error(err.to_string(), elapsed_ms(start), 0, Vec::new())
        }
    };

    let sql_texts = Parser::parse_multiple(sql)
        .ok()
        .and(Some(split_sql_texts_for_response(sql)))
        .unwrap_or_else(|| vec![sql.trim().to_string()]);

    let mut executed_count = 0;
    let mut statement_results = Vec::new();
    let mut last_result = ExecuteResult::Error {
        message: "empty SQL statement".to_string(),
    };

    for (idx, statement) in statements.into_iter().enumerate() {
        let statement_start = Instant::now();
        let result = match executor.execute(statement) {
            Ok(result) => result,
            Err(err) => {
                statement_results.push(SqlStatementResult::from_execute_result(
                    sql_texts.get(idx).cloned().unwrap_or_default(),
                    ExecuteResult::Error {
                        message: err.to_string(),
                    },
                    elapsed_ms(statement_start),
                ));
                return SqlApiResponse::error(
                    err.to_string(),
                    elapsed_ms(start),
                    executed_count,
                    statement_results,
                );
            }
        };

        if let ExecuteResult::Error { message } = &result {
            statement_results.push(SqlStatementResult::from_execute_result(
                sql_texts.get(idx).cloned().unwrap_or_default(),
                ExecuteResult::Error {
                    message: message.clone(),
                },
                elapsed_ms(statement_start),
            ));
            return SqlApiResponse::error(
                message.clone(),
                elapsed_ms(start),
                executed_count,
                statement_results,
            );
        }

        executed_count += 1;
        statement_results.push(SqlStatementResult::from_execute_result(
            sql_texts.get(idx).cloned().unwrap_or_default(),
            result.clone(),
            elapsed_ms(statement_start),
        ));
        last_result = result;
    }

    SqlApiResponse::from_execute_result(
        last_result,
        elapsed_ms(start),
        executed_count,
        statement_results,
    )
}

// 中文註解：前端結果區需要保留原始語句文字，這裡沿用 parser 的切句規則。
fn split_sql_texts_for_response(sql: &str) -> Vec<String> {
    let mut statements = Vec::new();
    let mut current = String::new();
    let mut in_string = false;
    let mut chars = sql.chars().peekable();

    while let Some(ch) = chars.next() {
        match ch {
            '\'' => {
                current.push(ch);
                if in_string && matches!(chars.peek(), Some('\'')) {
                    current.push(chars.next().expect("escaped quote"));
                } else {
                    in_string = !in_string;
                }
            }
            ';' if !in_string => {
                let trimmed = current.trim();
                if !trimmed.is_empty() {
                    statements.push(trimmed.to_string());
                }
                current.clear();
            }
            _ => current.push(ch),
        }
    }

    let trimmed = current.trim();
    if !trimmed.is_empty() {
        statements.push(trimmed.to_string());
    }

    statements
}

// 中文註解：把 SQL Value 轉成 JSON，方便前端直接渲染。
fn sql_value_to_json(value: SqlValue) -> JsonValue {
    match value {
        SqlValue::Int(value) => JsonValue::from(value),
        SqlValue::Text(value) => JsonValue::from(value),
        SqlValue::Bool(value) => JsonValue::from(value),
        SqlValue::Null => JsonValue::Null,
        SqlValue::Variable(name) => JsonValue::from(name),
    }
}

fn json_to_sql_value(value: JsonValue) -> std::result::Result<SqlValue, String> {
    match value {
        JsonValue::Null => Ok(SqlValue::Null),
        JsonValue::Bool(value) => Ok(SqlValue::Bool(value)),
        JsonValue::Number(number) => number
            .as_i64()
            .map(SqlValue::Int)
            .ok_or_else(|| "only i64 JSON numbers are supported".to_string()),
        JsonValue::String(value) => Ok(SqlValue::Text(value)),
        _ => Err("only scalar JSON values are supported as SQL parameters".to_string()),
    }
}

// 中文註解：依照 schema 欄位順序把 row 投影成 JSON 陣列。
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

// 中文註解：從 `/api/tables/{name}/...` 路徑中安全解析資料表名稱。
fn extract_table_name(path: &str, suffix: &str) -> Option<String> {
    let prefix = "/api/tables/";
    let rest = path.strip_prefix(prefix)?;
    let table_name = rest.strip_suffix(suffix)?;
    if table_name.is_empty() || table_name.contains('/') {
        return None;
    }
    Some(table_name.to_string())
}

// 中文註解：解析 rows API 的 limit 參數，預設值為 100。
fn parse_limit(query: &HashMap<String, String>) -> std::result::Result<usize, String> {
    match query.get("limit") {
        Some(value) => value
            .parse::<usize>()
            .map_err(|_| format!("invalid limit '{}'", value)),
        None => Ok(100),
    }
}

// 中文註解：切開 path 與 query string，提供後續 route 與參數解析使用。
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

// 中文註解：把內部 DataType 轉成前端與 API 使用的字串名稱。
fn data_type_name(data_type: &DataType) -> &'static str {
    match data_type {
        DataType::Int => "INT",
        DataType::Text => "TEXT",
        DataType::Bool => "BOOL",
    }
}

// 中文註解：建立統一格式的 JSON 錯誤回應。
fn json_error_response(status_code: u16, error: &str, message: String) -> HttpResponse {
    json_response(
        status_code,
        &ErrorMessage {
            error: error.to_string(),
            message,
        },
    )
}

// 中文註解：回傳 405，提示當前路徑不支援該 HTTP method。
fn method_not_allowed(method: &str, path: &str) -> HttpResponse {
    json_error_response(
        405,
        "method_not_allowed",
        format!("method {} is not allowed for {}", method, path),
    )
}

// 中文註解：處理前端 fetch 發出的 CORS preflight 請求。
fn cors_preflight_response() -> HttpResponse {
    HttpResponse {
        status_code: 204,
        content_type: "text/plain; charset=utf-8",
        body: String::new(),
    }
}

fn text_response(status_code: u16, content_type: &'static str, body: String) -> HttpResponse {
    HttpResponse {
        status_code,
        content_type,
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

// 中文註解：把執行耗時轉成毫秒，提供 SQL tab 顯示。
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
    table_count: usize,
    total_rows: usize,
    sstable_count: usize,
    disk_usage_bytes: u64,
    wal_size_bytes: u64,
    wal_record_count: usize,
    bloom_filter_hit_rate: f64,
    next_sstable_id: u64,
    manifest_status: ManifestStatusResponse,
    wal_status: WalStatusResponse,
}

#[derive(Serialize)]
struct SstablesResponse<'a> {
    status: &'a str,
    sstables: Vec<crate::storage::lsm::SstableInfo>,
    manifest: ManifestStatusResponse,
    wal: WalStatusResponse,
}

#[derive(Serialize)]
struct ManifestStatusResponse {
    summary: String,
    last_compaction_ts: u64,
}

#[derive(Serialize)]
struct WalStatusResponse {
    path: String,
    size_bytes: u64,
    record_count: usize,
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
    indexed: bool,
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
    executed_count: usize,
    message: String,
    statement_results: Vec<SqlStatementResult>,
    // 中文註解：EXPLAIN 時包含結構化查詢計劃樹，供前端視覺化顯示
    #[serde(skip_serializing_if = "Option::is_none")]
    plan_tree: Option<JsonValue>,
}

#[derive(Serialize)]
struct SqlStatementResult {
    sql: String,
    success: bool,
    #[serde(rename = "type")]
    kind: String,
    columns: Vec<String>,
    rows: Vec<Vec<JsonValue>>,
    row_count: usize,
    elapsed_ms: u64,
    message: String,
    // 中文註解：EXPLAIN 時包含結構化查詢計劃樹，供前端視覺化顯示
    #[serde(skip_serializing_if = "Option::is_none")]
    plan_tree: Option<JsonValue>,
}

impl SqlApiResponse {
    // 中文註解：把 executor 回傳結果轉成前端固定吃的 JSON 格式。
    fn from_execute_result(
        result: ExecuteResult,
        elapsed_ms: u64,
        executed_count: usize,
        statement_results: Vec<SqlStatementResult>,
    ) -> Self {
        match result {
            ExecuteResult::Explain { plan, plan_tree_json } => Self {
                success: true,
                kind: "explained".to_string(),
                columns: vec!["plan".to_string()],
                rows: vec![vec![JsonValue::from(plan)]],
                row_count: 1,
                elapsed_ms,
                executed_count,
                message: String::new(),
                statement_results,
                // 中文註解：反序列化 JSON 字串成為結構化物件，讓前端可以直接使用
                plan_tree: serde_json::from_str(&plan_tree_json).ok(),
            },
            ExecuteResult::Prepared { name } => Self {
                success: true,
                kind: "prepared".to_string(),
                columns: Vec::new(),
                rows: Vec::new(),
                row_count: 0,
                elapsed_ms,
                executed_count,
                message: format!("Prepared statement '{}' created", name),
                statement_results,
                plan_tree: None,
            },
            ExecuteResult::Deallocated { name } => Self {
                success: true,
                kind: "deallocated".to_string(),
                columns: Vec::new(),
                rows: Vec::new(),
                row_count: 0,
                elapsed_ms,
                executed_count,
                message: format!("Prepared statement '{}' deallocated", name),
                statement_results,
                plan_tree: None,
            },
            ExecuteResult::IsolationLevelSet { level } => Self {
                success: true,
                kind: "updated".to_string(),
                columns: Vec::new(),
                rows: Vec::new(),
                row_count: 0,
                elapsed_ms,
                executed_count,
                message: format!("Isolation level set to {}", isolation_level_label(&level)),
                statement_results,
                plan_tree: None,
            },
            ExecuteResult::Created { table_name } => Self {
                success: true,
                kind: "created".to_string(),
                columns: Vec::new(),
                rows: Vec::new(),
                row_count: 0,
                elapsed_ms,
                executed_count,
                message: format!("Table '{}' created", table_name),
                statement_results,
                plan_tree: None,
            },
            ExecuteResult::Analyzed { table_name } => Self {
                success: true,
                kind: "analyzed".to_string(),
                columns: Vec::new(),
                rows: Vec::new(),
                row_count: 0,
                elapsed_ms,
                executed_count,
                message: format!("Table '{}' analyzed", table_name),
                statement_results,
                plan_tree: None,
            },
            ExecuteResult::Altered { table_name } => Self {
                success: true,
                kind: "updated".to_string(),
                columns: Vec::new(),
                rows: Vec::new(),
                row_count: 0,
                elapsed_ms,
                executed_count,
                message: format!("Table '{}' altered", table_name),
                statement_results,
                plan_tree: None,
            },
            ExecuteResult::Dropped { table_name } => Self {
                success: true,
                kind: "deleted".to_string(),
                columns: Vec::new(),
                rows: Vec::new(),
                row_count: 0,
                elapsed_ms,
                executed_count,
                message: format!("Table '{}' dropped", table_name),
                statement_results,
                plan_tree: None,
            },
            ExecuteResult::IndexCreated {
                table_name,
                column_names,
            } => Self {
                success: true,
                kind: "created".to_string(),
                columns: Vec::new(),
                rows: Vec::new(),
                row_count: 0,
                elapsed_ms,
                executed_count,
                message: format!("Index on '{}.{}' created", table_name, column_names.join(",")),
                statement_results,
                plan_tree: None,
            },
            ExecuteResult::IndexDropped {
                table_name,
                column_names,
            } => Self {
                success: true,
                kind: "deleted".to_string(),
                columns: Vec::new(),
                rows: Vec::new(),
                row_count: 0,
                elapsed_ms,
                executed_count,
                message: format!("Index on '{}.{}' dropped", table_name, column_names.join(",")),
                statement_results,
                plan_tree: None,
            },
            ExecuteResult::Inserted { count } => Self {
                success: true,
                kind: "inserted".to_string(),
                columns: Vec::new(),
                rows: Vec::new(),
                row_count: count,
                elapsed_ms,
                executed_count,
                message: String::new(),
                statement_results,
                plan_tree: None,
            },
            ExecuteResult::Selected { columns, rows } => Self {
                success: true,
                kind: "select".to_string(),
                row_count: rows.len(),
                elapsed_ms,
                executed_count,
                message: String::new(),
                statement_results,
                columns,
                rows: rows
                    .into_iter()
                    .map(|row| row.into_iter().map(sql_value_to_json).collect())
                    .collect(),
                plan_tree: None,
            },
            ExecuteResult::Updated { count } => Self {
                success: true,
                kind: "updated".to_string(),
                columns: Vec::new(),
                rows: Vec::new(),
                row_count: count,
                elapsed_ms,
                executed_count,
                message: String::new(),
                statement_results,
                plan_tree: None,
            },
            ExecuteResult::Deleted { count } => Self {
                success: true,
                kind: "deleted".to_string(),
                columns: Vec::new(),
                rows: Vec::new(),
                row_count: count,
                elapsed_ms,
                executed_count,
                message: String::new(),
                statement_results,
                plan_tree: None,
            },
            ExecuteResult::Error { message } => {
                Self::error(message, elapsed_ms, executed_count, statement_results)
            }
            // 中文註解：觸發器與權限操作結果
            ExecuteResult::TriggerCreated { trigger_name }
            | ExecuteResult::TriggerDropped { trigger_name } => Self {
                success: true,
                kind: "trigger".to_string(),
                columns: Vec::new(),
                rows: Vec::new(),
                row_count: 0,
                elapsed_ms,
                executed_count,
                message: trigger_name,
                statement_results,
                plan_tree: None,
            },
            ExecuteResult::Granted { user, table_name }
            | ExecuteResult::Revoked { user, table_name } => Self {
                success: true,
                kind: "privilege".to_string(),
                columns: Vec::new(),
                rows: Vec::new(),
                row_count: 0,
                elapsed_ms,
                executed_count,
                message: format!("{} on {}", user, table_name),
                statement_results,
                plan_tree: None,
            },
            // 中文註解：預存程序操作結果
            ExecuteResult::ProcedureCreated { name } => Self {
                success: true,
                kind: "created".to_string(),
                columns: Vec::new(),
                rows: Vec::new(),
                row_count: 0,
                elapsed_ms,
                executed_count,
                message: format!("Procedure '{}' created", name),
                statement_results,
                plan_tree: None,
            },
            ExecuteResult::ProcedureCalled { name } => Self {
                success: true,
                kind: "procedure".to_string(),
                columns: Vec::new(),
                rows: Vec::new(),
                row_count: 0,
                elapsed_ms,
                executed_count,
                message: format!("Procedure '{}' called", name),
                statement_results,
                plan_tree: None,
            },
            ExecuteResult::ProcedureDropped { name } => Self {
                success: true,
                kind: "deleted".to_string(),
                columns: Vec::new(),
                rows: Vec::new(),
                row_count: 0,
                elapsed_ms,
                executed_count,
                message: format!("Procedure '{}' dropped", name),
                statement_results,
                plan_tree: None,
            },
        }
    }

    // 中文註解：統一產生 SQL API 的錯誤 payload。
    fn error(
        message: String,
        elapsed_ms: u64,
        executed_count: usize,
        statement_results: Vec<SqlStatementResult>,
    ) -> Self {
        Self {
            success: false,
            kind: "error".to_string(),
            columns: Vec::new(),
            rows: Vec::new(),
            row_count: 0,
            elapsed_ms,
            executed_count,
            message: format!(
                "{} (executed {} statement(s) before failure)",
                message, executed_count
            ),
            statement_results,
            plan_tree: None,
        }
    }
}

impl SqlStatementResult {
    // 中文註解：每一條語句都保留自己的結果，前端才能完整顯示多語句輸出。
    fn from_execute_result(sql: String, result: ExecuteResult, elapsed_ms: u64) -> Self {
        match result {
            ExecuteResult::Explain { plan, plan_tree_json } => Self {
                sql,
                success: true,
                kind: "explained".to_string(),
                columns: vec!["plan".to_string()],
                rows: vec![vec![JsonValue::from(plan)]],
                row_count: 1,
                elapsed_ms,
                message: String::new(),
                // 中文註解：反序列化 JSON 字串成為結構化物件，讓前端可以直接使用
                plan_tree: serde_json::from_str(&plan_tree_json).ok(),
            },
            ExecuteResult::Prepared { name } => Self {
                sql,
                success: true,
                kind: "prepared".to_string(),
                columns: Vec::new(),
                rows: Vec::new(),
                row_count: 0,
                elapsed_ms,
                message: format!("Prepared statement '{}' created", name),
                plan_tree: None,
            },
            ExecuteResult::Deallocated { name } => Self {
                sql,
                success: true,
                kind: "deallocated".to_string(),
                columns: Vec::new(),
                rows: Vec::new(),
                row_count: 0,
                elapsed_ms,
                message: format!("Prepared statement '{}' deallocated", name),
                plan_tree: None,
            },
            ExecuteResult::IsolationLevelSet { level } => Self {
                sql,
                success: true,
                kind: "updated".to_string(),
                columns: Vec::new(),
                rows: Vec::new(),
                row_count: 0,
                elapsed_ms,
                message: format!("Isolation level set to {}", isolation_level_label(&level)),
                plan_tree: None,
            },
            ExecuteResult::Created { table_name } => Self {
                sql,
                success: true,
                kind: "created".to_string(),
                columns: Vec::new(),
                rows: Vec::new(),
                row_count: 0,
                elapsed_ms,
                message: format!("Table '{}' created", table_name),
                plan_tree: None,
            },
            ExecuteResult::Analyzed { table_name } => Self {
                sql,
                success: true,
                kind: "analyzed".to_string(),
                columns: Vec::new(),
                rows: Vec::new(),
                row_count: 0,
                elapsed_ms,
                message: format!("Table '{}' analyzed", table_name),
                plan_tree: None,
            },
            ExecuteResult::Altered { table_name } => Self {
                sql,
                success: true,
                kind: "updated".to_string(),
                columns: Vec::new(),
                rows: Vec::new(),
                row_count: 0,
                elapsed_ms,
                message: format!("Table '{}' altered", table_name),
                plan_tree: None,
            },
            ExecuteResult::Dropped { table_name } => Self {
                sql,
                success: true,
                kind: "deleted".to_string(),
                columns: Vec::new(),
                rows: Vec::new(),
                row_count: 0,
                elapsed_ms,
                message: format!("Table '{}' dropped", table_name),
                plan_tree: None,
            },
            ExecuteResult::IndexCreated {
                table_name,
                column_names,
            } => Self {
                sql,
                success: true,
                kind: "created".to_string(),
                columns: Vec::new(),
                rows: Vec::new(),
                row_count: 0,
                elapsed_ms,
                message: format!("Index on '{}.{}' created", table_name, column_names.join(",")),
                plan_tree: None,
            },
            ExecuteResult::IndexDropped {
                table_name,
                column_names,
            } => Self {
                sql,
                success: true,
                kind: "deleted".to_string(),
                columns: Vec::new(),
                rows: Vec::new(),
                row_count: 0,
                elapsed_ms,
                message: format!("Index on '{}.{}' dropped", table_name, column_names.join(",")),
                plan_tree: None,
            },
            ExecuteResult::Inserted { count } => Self {
                sql,
                success: true,
                kind: "inserted".to_string(),
                columns: Vec::new(),
                rows: Vec::new(),
                row_count: count,
                elapsed_ms,
                message: String::new(),
                plan_tree: None,
            },
            ExecuteResult::Selected { columns, rows } => Self {
                row_count: rows.len(),
                sql,
                success: true,
                kind: "select".to_string(),
                columns,
                rows: rows
                    .into_iter()
                    .map(|row| row.into_iter().map(sql_value_to_json).collect())
                    .collect(),
                elapsed_ms,
                message: String::new(),
                plan_tree: None,
            },
            ExecuteResult::Updated { count } => Self {
                sql,
                success: true,
                kind: "updated".to_string(),
                columns: Vec::new(),
                rows: Vec::new(),
                row_count: count,
                elapsed_ms,
                message: String::new(),
                plan_tree: None,
            },
            ExecuteResult::Deleted { count } => Self {
                sql,
                success: true,
                kind: "deleted".to_string(),
                columns: Vec::new(),
                rows: Vec::new(),
                row_count: count,
                elapsed_ms,
                message: String::new(),
                plan_tree: None,
            },
            ExecuteResult::Error { message } => Self {
                sql,
                success: false,
                kind: "error".to_string(),
                columns: Vec::new(),
                rows: Vec::new(),
                row_count: 0,
                elapsed_ms,
                message,
                plan_tree: None,
            },
            // 中文註解：觸發器與權限操作結果
            ExecuteResult::TriggerCreated { trigger_name }
            | ExecuteResult::TriggerDropped { trigger_name } => Self {
                sql,
                success: true,
                kind: "trigger".to_string(),
                columns: Vec::new(),
                rows: Vec::new(),
                row_count: 0,
                elapsed_ms,
                message: trigger_name,
                plan_tree: None,
            },
            ExecuteResult::Granted { user, table_name }
            | ExecuteResult::Revoked { user, table_name } => Self {
                sql,
                success: true,
                kind: "privilege".to_string(),
                columns: Vec::new(),
                rows: Vec::new(),
                row_count: 0,
                elapsed_ms,
                message: format!("{} on {}", user, table_name),
                plan_tree: None,
            },
            // 中文註解：預存程序操作結果
            ExecuteResult::ProcedureCreated { name } => Self {
                sql,
                success: true,
                kind: "created".to_string(),
                columns: Vec::new(),
                rows: Vec::new(),
                row_count: 0,
                elapsed_ms,
                message: format!("Procedure '{}' created", name),
                plan_tree: None,
            },
            ExecuteResult::ProcedureCalled { name } => Self {
                sql,
                success: true,
                kind: "procedure".to_string(),
                columns: Vec::new(),
                rows: Vec::new(),
                row_count: 0,
                elapsed_ms,
                message: format!("Procedure '{}' called", name),
                plan_tree: None,
            },
            ExecuteResult::ProcedureDropped { name } => Self {
                sql,
                success: true,
                kind: "deleted".to_string(),
                columns: Vec::new(),
                rows: Vec::new(),
                row_count: 0,
                elapsed_ms,
                message: format!("Procedure '{}' dropped", name),
                plan_tree: None,
            },
        }
    }
}

fn isolation_level_label(level: &crate::sql::ast::IsolationLevel) -> &'static str {
    match level {
        crate::sql::ast::IsolationLevel::ReadCommitted => "READ COMMITTED",
        crate::sql::ast::IsolationLevel::RepeatableRead => "REPEATABLE READ",
        crate::sql::ast::IsolationLevel::Serializable => "SERIALIZABLE",
    }
}

#[derive(Deserialize)]
struct PrepareApiRequest {
    name: String,
    sql: String,
}

#[derive(Deserialize)]
struct ExecutePreparedApiRequest {
    name: String,
    params: Vec<JsonValue>,
}

#[derive(Deserialize)]
struct DeallocateApiRequest {
    name: String,
}

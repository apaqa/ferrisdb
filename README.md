# FerrisDB

**A database engine built from scratch in Rust.**

FerrisDB is a learning-oriented but feature-rich database project that combines an LSM-tree storage engine, WAL-based durability, MVCC concurrency control, a SQL layer, and network-facing admin/server interfaces in a single Rust codebase.

![Tests](https://img.shields.io/badge/tests-240%20passing-brightgreen)
![Language](https://img.shields.io/badge/language-Rust-orange)

## Architecture

```text
                         +---------------------------+
                         |   Client / Applications   |
                         | REPL | HTTP API | TCP API |
                         +-------------+-------------+
                                       |
                                       v
                         +---------------------------+
                         |        SQL Engine         |
                         | Lexer -> Parser -> AST    |
                         | Executor -> Result sets   |
                         +-------------+-------------+
                                       |
                                       v
                         +---------------------------+
                         |     Query Optimizer       |
                         | Plan cache | EXPLAIN      |
                         | SeqScan / IndexScan / Join|
                         +-------------+-------------+
                                       |
                                       v
                         +---------------------------+
                         |            MVCC           |
                         | Transactions | Snapshots  |
                         | Versioned keys | Isolation|
                         +-------------+-------------+
                                       |
                                       v
                         +---------------------------+
                         |         LSM-Tree          |
                         | MemTable | Compaction     |
                         | MANIFEST | Bloom Filter   |
                         +------+--------------+-----+
                                |              |
                                v              v
                      +----------------+  +----------------+
                      |      WAL       |  |    SSTables    |
                      | CRC32 + replay |  | sorted files   |
                      +----------------+  +----------------+
                                \              /
                                 \            /
                                  v          v
                                   +--------+
                                   |  Disk  |
                                   +--------+
```

## Features

### Storage and durability

FerrisDB uses an LSM-tree storage engine with an in-memory MemTable, immutable SSTables, Bloom filters for negative lookup acceleration, compaction, and MANIFEST-based metadata recovery. Durability comes from a Write-Ahead Log with CRC32 checksums, crash recovery, configurable WAL behavior, and maintenance commands such as `VACUUM`.

### Transactions and concurrency

The engine supports MVCC with timestamped versions, snapshot reads, and multiple isolation levels including `READ COMMITTED`, `REPEATABLE READ`, and `SERIALIZABLE`. Readers do not block writers, and the HTTP SQL path includes a simulated connection pool built with `Arc<Mutex<_>>` and `Condvar`.

### SQL layer

The SQL stack includes a handwritten lexer, parser, AST, and executor. Supported functionality includes `CREATE TABLE`, `INSERT`, `SELECT`, `UPDATE`, `DELETE`, `ALTER TABLE`, `DROP TABLE`, `CREATE INDEX`, `DROP INDEX`, `EXPLAIN`, `VACUUM`, temporary tables, views, materialized views, prepared statements, triggers, stored procedures, cursors, UDFs, JSON functions, recursive CTEs, partitioning, constraints, and subqueries.

### Query processing

FerrisDB supports filters, projections, `INNER JOIN`, `LEFT JOIN`, `GROUP BY`, aggregates (`COUNT`, `SUM`, `MIN`, `MAX`), `ORDER BY`, `LIMIT`, `DISTINCT`, `HAVING`, `BETWEEN`, `LIKE`, `IS NULL`, `IN (subquery)`, and equality-driven index scans. It also includes a small cost-based optimizer, LRU plan cache, and `EXPLAIN` output for plan inspection.

### Interfaces and tooling

The project exposes an interactive REPL with KV and SQL modes, a multi-threaded TCP server, an HTTP admin API, benchmark examples, a SQL demo script, and FerrisDB Studio-oriented HTTP endpoints. Configuration is loaded from `ferrisdb.toml`, and the project includes extensive integration, stress, failure-injection, and property-based tests.

## Quick Start

### Run the REPL

```bash
cargo run
```

Example session:

```text
ferrisdb> sql
Switched to SQL mode
ferrisdb> CREATE TABLE users (id INT, name TEXT, active BOOL);
Table 'users' created
ferrisdb> INSERT INTO users VALUES (1, 'Alice', true), (2, 'Bob', false);
Inserted 2 row(s)
ferrisdb> SELECT * FROM users ORDER BY id ASC;
id | name  | active
---+-------+-------
1  | Alice | true
2  | Bob   | false
```

### Run the TCP server

```bash
cargo run -- --server
```

### Run the HTTP admin API

```bash
cargo run -- --http-port 8080
curl http://127.0.0.1:8080/health
curl http://127.0.0.1:8080/stats
curl -X POST http://127.0.0.1:8080/compact
```

### FerrisDB Studio

Add a screenshot or product UI image here when you have one:

```text
[ Studio screenshot placeholder ]
```

## SQL Feature Matrix

| Feature | Example | Status |
| --- | --- | --- |
| Create / insert / select | `CREATE TABLE users (...); INSERT INTO users VALUES (...); SELECT * FROM users;` | Implemented |
| Update / delete | `UPDATE users SET name = 'Bob' WHERE id = 1;` | Implemented |
| Filtering | `SELECT * FROM users WHERE age >= 18;` | Implemented |
| Ordering / limit | `SELECT * FROM users ORDER BY age DESC LIMIT 5;` | Implemented |
| Aggregates | `SELECT COUNT(*), SUM(salary) FROM employees;` | Implemented |
| Group by / having | `SELECT dept, COUNT(*) FROM employees GROUP BY dept HAVING COUNT(*) > 1;` | Implemented |
| Inner join | `SELECT * FROM t1 INNER JOIN t2 ON t1.id = t2.user_id;` | Implemented |
| Left join | `SELECT * FROM users LEFT JOIN teams ON ...;` | Implemented |
| Subquery | `SELECT * FROM users WHERE id IN (SELECT user_id FROM orders);` | Implemented |
| Recursive CTE | `WITH RECURSIVE seq AS (...) SELECT * FROM seq;` | Implemented |
| Explain | `EXPLAIN SELECT * FROM users WHERE id = 1;` | Implemented |
| Vacuum | `VACUUM;` / `VACUUM users;` | Implemented |
| Secondary index | `CREATE INDEX ON users(email);` | Implemented |
| Temporary tables | `CREATE TEMPORARY TABLE temp_ids (...);` | Implemented |
| Views / materialized views | `CREATE VIEW active_users AS ...;` | Implemented |
| JSON functions | `SELECT JSON_EXTRACT(profile, '$.city') FROM users;` | Implemented |
| Procedures / triggers / cursors | `CREATE PROCEDURE ...`, `CREATE TRIGGER ...`, `DECLARE CURSOR ...` | Implemented |
| Constraints | `UNIQUE`, `CHECK`, `FOREIGN KEY` | Implemented |
| Partitioning | `PARTITION BY RANGE (...)` | Implemented |

## Storage Engine

FerrisDB writes first to a MemTable and a WAL, then flushes sorted data into SSTables on disk. Reads check the MemTable first, then consult SSTables from newest to oldest, using Bloom filters and indexes to avoid unnecessary work. Compaction merges SSTables, drops obsolete versions and tombstones, and keeps read amplification under control.

On restart, the engine uses WAL replay, MANIFEST records, and SSTable metadata to rebuild state safely. MVCC is layered on top by storing versioned keys with timestamps so multiple readers and writers can coexist without coarse-grained locking.

## Testing

FerrisDB currently includes **240 automated tests across 46 test files**.

Run everything with:

```bash
cargo test
```

The suite includes unit tests, integration tests, HTTP/TCP end-to-end tests, failure-injection scenarios, randomized stress tests, and property-based tests powered by `proptest`.

Property-based testing currently covers:

- KV invariants such as put/get round-trips and operation sequences against a reference map model.
- SQL invariants such as insert/count consistency and insert/delete behavior against a reference model.
- Reproducible randomized inputs to catch edge cases that are easy to miss in example-driven tests.

## Configuration

FerrisDB loads configuration from `ferrisdb.toml` when present. Example:

```toml
data_dir = "./ferrisdb-data"
memtable_size_threshold = 4096
compaction_threshold = 4
server_host = "127.0.0.1"
server_port = 6379
max_connections = 4
wal_mode = "wal"
```

## Project Structure

```text
src/
  bench.rs                 Shared benchmark helpers and reporting
  config.rs                TOML configuration loading and CLI overrides
  error.rs                 Shared error type used across the project
  lib.rs                   Library module exports
  main.rs                  Entry point for REPL, TCP server, and HTTP API
  cli/
    repl.rs                Interactive KV/SQL shell
  server/
    tcp.rs                 Line-based TCP server
    http.rs                HTTP admin and SQL API built on std::net
    connection_pool.rs     Connection pool simulation for HTTP SQL sessions
  sql/
    ast.rs                 SQL AST definitions
    lexer.rs               Handwritten SQL tokenizer
    parser.rs              Recursive-descent SQL parser
    executor.rs            SQL execution engine and result formatting
    catalog.rs             Table schema metadata and catalog operations
    row.rs                 Row encoding and JSON serialization helpers
    index.rs               Secondary index storage and lookup logic
  transaction/
    mvcc.rs                MVCC engine, transactions, and isolation logic
    keyutil.rs             Versioned key encoding helpers
  storage/
    memory.rs              In-memory MemTable implementation
    wal.rs                 Write-Ahead Log with recovery
    manifest.rs            MANIFEST metadata log and replay
    bloom.rs               Bloom filter implementation
    compaction.rs          SSTable merge/compaction logic
    lsm.rs                 LSM engine orchestration and persistence flow
    sstable/
      format.rs            SSTable binary format definitions
      writer.rs            SSTable writer
      reader.rs            SSTable reader and index lookup
tests/
  *_test.rs                Unit, integration, stress, recovery, and property tests
examples/
  client.rs                TCP client example
  bench.rs                 KV benchmark runner
  bench_sql.rs             SQL benchmark runner
  sql_demo.rs              End-to-end SQL feature demonstration
```

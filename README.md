# FerrisDB — A database engine built from scratch in Rust

FerrisDB is a database engine implemented from scratch in Rust, featuring LSM-Tree storage, configurable WAL durability, MVCC concurrency control, and a small SQL layer.

## Features

- LSM-Tree storage engine (MemTable + SSTable + Compaction)
- Write-Ahead Log with CRC32 checksum, crash recovery, and runtime mode switching
- Bloom Filter for read optimization
- MVCC with `READ COMMITTED`, `REPEATABLE READ`, and `SERIALIZABLE`
- SQL support (`CREATE TABLE`, `INSERT`, `SELECT`, `UPDATE`, `DELETE`)
- SQL Common Table Expressions (`WITH`)
- SQL prepared statements (`PREPARE`, `EXECUTE`, `DEALLOCATE`)
- SQL INNER JOIN with nested loop
- SQL `UPDATE ... FROM` and `DELETE ... USING`
- SQL `ANALYZE TABLE` statistics collection
- SQL aggregate functions: COUNT, SUM, MIN, MAX
- SQL GROUP BY
- SQL DISTINCT
- SQL AS aliases for columns, aggregates, and tables
- SQL ORDER BY ASC/DESC
- SQL LIMIT
- SQL EXPLAIN with query plan and visual tree diagram in FerrisDB Studio
- Cost-Based Query Optimizer with scan / join strategy selection
- Query Plan Cache (LRU)
- SQL `WHERE` with comparison operators (`=`, `!=`, `<`, `>`, `<=`, `>=`)
- SQL `BETWEEN`, `LIKE`, `IS NULL`, `IS NOT NULL`
- SQL NULL-aware behavior for sorting and aggregates
- Secondary / composite indexes with `CREATE INDEX ON table(col1, col2, ...)`
- Index Scan optimization for equality-prefix queries
- ALTER TABLE (`ADD COLUMN` / `DROP COLUMN`)
- DROP TABLE / DROP TABLE IF EXISTS
- CREATE TABLE IF NOT EXISTS
- WHERE IN (subquery)
- SQL Triggers (`CREATE TRIGGER ... BEFORE/AFTER INSERT/UPDATE/DELETE ON table FOR EACH ROW BEGIN ... END`)
- BEFORE trigger modifies row values via `SET NEW.col = value`
- AFTER trigger executes arbitrary SQL after DML (e.g., writing audit logs)
- `DROP TRIGGER`
- GRANT/REVOKE access control (`GRANT SELECT, INSERT ON table TO user`, `REVOKE ALL ON table FROM user`)
- Per-session user context with privilege enforcement for SELECT/INSERT/UPDATE/DELETE
- TCP server with multi-threaded connections
- HTTP Admin API (`/health`, `/stats`, `/sstables`, `/compact`, `/flush`, `/api/sql/prepare`, `/api/sql/execute`, `/api/sql/deallocate`) with `plan_tree` JSON for EXPLAIN
- Interactive REPL with KV and SQL modes
- MANIFEST metadata management
- Configurable via `ferrisdb.toml`
- Built-in benchmark framework
- Background compaction worker
- Failure injection and stress tests
- 179+ automated tests

## Architecture

FerrisDB is structured as a layered database engine. Both the KV interface and the SQL interface eventually flow into the same MVCC and storage stack.

```text
Client (REPL / TCP)
  |
  v
SQL Layer (Lexer -> Parser -> Executor)
  |
  v
MVCC (Transaction, Snapshot Isolation)
  |
  v
LSM Engine
  |-- MemTable (BTreeMap, in-memory)
  |-- WAL (Write-Ahead Log)
  |-- SSTable (sorted on-disk files)
  |    \-- Bloom Filter
  |-- Compaction
  \-- MANIFEST (metadata)
  |
  v
Disk
```

## Quick Start

Run the interactive REPL:

```bash
cargo run
```

Run the TCP server:

```bash
cargo run -- --server
```

Run the test suite:

```bash
cargo test
```

Run the benchmark example:

```bash
cargo run --release --example bench
```

## REPL Examples

### KV Mode

```text
ferrisdb> set user:1 Alice
OK
ferrisdb> get user:1
Alice
ferrisdb> list
user:1 -> Alice
(1 entries)
```

### SQL Mode

```text
ferrisdb> sql
Switched to SQL mode
ferrisdb> CREATE TABLE users (id INT, name TEXT, active BOOL);
Table 'users' created
ferrisdb> INSERT INTO users VALUES (1, 'Alice', true), (2, 'Bob', false);
Inserted 2 row(s)
ferrisdb> SELECT * FROM users;
id | name  | active
---+-------+-------
1  | Alice | true
2  | Bob   | false
(2 rows)
```

## Benchmark Results

Representative benchmark results from the current implementation:

### KV

- Sequential write: 2,100 ops/sec
- Random read: 312 ops/sec
- Mixed workload: 419 ops/sec
- Scan: 63 ops/sec
- Restart recovery: 11 ops/sec
- Compaction: 5.87 ops/sec

### SQL

- INSERT: 188 ops/sec
- SELECT *: 52 ops/sec
- SELECT WHERE: 46 ops/sec
- UPDATE: 39 ops/sec
- DELETE: 38 ops/sec

## Technical Highlights

### LSM-Tree Storage

FerrisDB uses an LSM-style write path: updates land in a MemTable first and are later flushed into sorted SSTables on disk. This makes writes simple and fast while keeping the on-disk layout compact and sequential.

### WAL + Recovery

Every write can be appended to a Write-Ahead Log before it reaches the MemTable. FerrisDB now supports three WAL modes: `wal` (default batched durability), `sync` (fsync every write), and `wal_disabled` (fastest but crash-unsafe). The WAL format includes CRC32 checksums so corruption can be detected instead of silently accepted.

### MVCC and Isolation Levels

The MVCC layer assigns timestamps to versions and supports `READ COMMITTED`, `REPEATABLE READ`, and `SERIALIZABLE`. Readers do not block writers, and `REPEATABLE READ` / `SERIALIZABLE` transactions see a stable snapshot while `READ COMMITTED` refreshes visibility per statement.

### SSTables + Bloom Filters

SSTables are immutable sorted files with an in-memory index and Bloom filter. The Bloom filter makes negative lookups much cheaper by avoiding unnecessary disk reads for tables that definitely do not contain the key.

### MANIFEST Metadata

Instead of reconstructing state purely by scanning the data directory, FerrisDB records SSTable metadata in a MANIFEST log. This makes restarts more reliable and is a stepping stone toward more realistic storage-engine metadata management.

### Failure Injection

The project includes failure-injection tests for truncated WALs, corrupted SSTables, damaged MANIFEST files, interrupted compaction, and randomized stress/reopen sequences. This is especially valuable for database code, where correctness under failure matters more than happy-path functionality.

## Project Structure

```text
src/
  bench.rs              # Shared benchmark helpers
  config.rs             # TOML-based configuration loading and CLI overrides
  error.rs              # Shared error type
  main.rs               # Startup entry point for REPL / server
  cli/
    repl.rs             # Interactive REPL for KV and SQL
  server/
    tcp.rs              # Multi-threaded TCP server
  sql/
    ast.rs              # SQL AST definitions
    lexer.rs            # SQL tokenizer
    parser.rs           # SQL parser
    catalog.rs          # Table schema metadata
    index.rs            # Secondary index metadata and lookup
    row.rs              # Row encoding and storage mapping
    executor.rs         # SQL execution engine
  transaction/
    keyutil.rs          # MVCC key encoding helpers
    mvcc.rs             # MVCC engine and transactions
  storage/
    memory.rs           # MemTable
    wal.rs              # Write-Ahead Log
    manifest.rs         # MANIFEST metadata log
    bloom.rs            # Bloom filter implementation
    compaction.rs       # SSTable compaction
    lsm.rs              # LSM engine orchestration
    sstable/
      format.rs         # SSTable file format
      writer.rs         # SSTable writer
      reader.rs         # SSTable reader
tests/
  *_test.rs             # Unit, integration, recovery, and stress tests
examples/
  client.rs             # TCP client example
  bench.rs              # KV benchmark
  bench_sql.rs          # SQL benchmark
  sql_demo.rs           # End-to-end SQL feature demo
```

## What I Learned

- How LSM-Tree storage engines combine MemTables, SSTables, compaction, and metadata tracking.
- Why WAL durability and recovery logic are central to correctness, not just performance.
- How MVCC and snapshot isolation can be built with versioned keys and timestamp ordering.
- How a small SQL layer maps onto a lower-level KV engine.
- Why failure handling, corruption detection, and restart behavior are essential parts of database design.
- How to structure a non-trivial Rust codebase with layered modules, tests, and recoverability concerns.

## Roadmap

- More advanced SQL planning and cost-based optimization
- Multi-column and range indexes
- JOIN algorithm improvements (hash join / merge join)
- Query optimizer basics
- Richer HTTP / admin APIs
- Replication and distributed coordination experiments
- Background compaction scheduling
- More robust on-disk checksums and validation
- Range tombstones and better delete handling
- Better benchmark dashboards and profiling

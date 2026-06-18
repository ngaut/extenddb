# High-Level Design

## Overview

ExtendDB is a standalone async Rust application that speaks the DynamoDB wire
protocol over HTTPS. It authenticates requests with SigV4 and builtin IAM,
executes DynamoDB operation logic in the engine crate, and persists state in
TiDB through the storage trait boundary.

TiDB is the only supported storage implementation.

## Workspace

```text
extenddb/
├── crates/core          # pure sync types, validation, expressions, errors
├── crates/engine        # DynamoDB operation handlers
├── crates/storage       # storage traits and shared helpers
├── crates/storage-tidb  # TiDB implementation
├── crates/auth          # SigV4 verification and IAM policy engine
├── crates/server        # HTTP server, management API, console
└── crates/bin           # CLI, config, daemon lifecycle
```

`core` has no async runtime, I/O, HTTP, or database dependencies. `engine` owns
DynamoDB behavior and calls storage traits. `server` owns wire protocol and web
surfaces. `bin` wires TiDB storage into the server.

## Request Lifecycle

1. HTTP request arrives at `POST /`.
2. Server validates DynamoDB headers, content type, checksum, and target action.
3. SigV4 authentication resolves credentials through the TiDB-backed credential
   store.
4. IAM authorization evaluates identity, group, role, session policy,
   permissions boundary, resource ARN, and condition context.
5. Engine handler validates input, parses expressions, and calls storage traits.
6. TiDB executes reads/writes, transactions, stream capture, TTL metadata, or
   control-plane DDL reconciliation.
7. Engine formats DynamoDB-compatible JSON, consumed capacity, and errors.

## Storage Model

ExtendDB uses a catalog/data topology inside one TiDB cluster:

- Catalog tables store accounts, IAM objects, access keys, table metadata,
  index metadata, stream metadata, settings, metrics, and backup metadata.
- Data tables store items once, with generated columns and native secondary
  indexes for GSI/LSI access.
- Streams use TiDB storage with transaction TSO values plus an in-transaction
  ordinal for sequence ordering.
- User-table TTL uses an ExtendDB expiry worker backed by TiDB lookup artifacts; fixed-retention internal tables use TiDB native TTL.
- Backup/restore delegates physical data to TiDB BR.

The catalog and data databases must stay in the same TiDB cluster so snapshot
timestamps, online DDL, TTL, and BR share one timeline.

## Consistency And Transactions

Condition expressions are evaluated inside the storage transaction that performs
the write. This prevents time-of-check/time-of-use races for conditional writes
and transactions.

Read routing:

- Strong reads and writes use the strong data pool.
- Default reads use TiDB follower-read routing.
- Optional bounded stale reads are configured on the default-read pool.
- `TransactGetItems` always uses a strong transaction.

TiDB native secondary indexes make base-row and index state transactional.

## Operational Model

Multiple ExtendDB frontends can share one TiDB deployment. TiDB owns
replication, online DDL scheduling, Resource Control, TTL jobs, and BR. ExtendDB
should avoid local coordination mechanisms when TiDB already provides the
distributed primitive.

Required verification gates for architecture changes:

```bash
cargo check -j12 --workspace
cargo test -j12 --workspace
cargo clippy -j12 --workspace --all-targets -- -D warnings
```

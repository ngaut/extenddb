# Storage Component

## Scope

ExtendDB has one supported storage implementation: TiDB, in
`crates/storage-tidb`. The `crates/storage` crate remains a boundary between
the DynamoDB engine and persistence code, but it is not a promise that multiple
backends are shipped or tested.

The storage layer owns:

- Catalog metadata for accounts, tables, indexes, streams, TTL, tags, settings,
  IAM objects, metrics, rate limits, and backup metadata.
- User item storage in TiDB data tables.
- Native TiDB DDL for table creation, GSI/LSI materialization, generated
  columns, TTL, online schema changes, follower reads, Resource Control, and BR
  backup/restore integration.

## Crates

| Crate | Responsibility |
|---|---|
| `extenddb-storage` | Trait definitions, shared storage data types, and backend-agnostic helpers. |
| `extenddb-storage-tidb` | The only concrete storage implementation. |
| `extenddb-engine` | DynamoDB operation handlers that call storage traits. |
| `extenddb-server` | HTTP, authz context, management API, console, health, and metrics. |
| `extenddb` | CLI/config wiring that creates TiDB stores and passes trait objects to the server. |

## Trait Boundary

The storage traits keep engine and server code free of SQL driver details:

- `TableEngine`: table lifecycle and table/index metadata.
- `DataEngine`: item CRUD, query, scan, batch, and transactions.
- `MetadataEngine`: TTL configuration and tags.
- `StreamEngine`: stream metadata, iterators, and stream records.
- `WorkerStore`: backend-owned runtime work.
- `BackupEngine`: backup and restore metadata plus TiDB BR coordination.
- `ManagementStore`, `AdminStore`, `SettingsStore`, `MetricsStore`,
  `RateLimitStore`, and `AuthorizationStore`: IAM, settings, metrics, lockout,
  and authorization persistence.
- `Bootstrapper`: init, destroy, migrate, verify, and catalog checks.

Traits use explicit `BoxFuture` return types where object safety is required.
The concrete runtime is TiDB-only; code should not add conditional branches for
removed backends.

## TiDB Catalog And Data Layout

ExtendDB uses a catalog database and account/data databases in the same TiDB
cluster. Keeping them in one cluster is required so online DDL, snapshot reads,
native TTL, generated columns, and BR all refer to one TiDB timeline.

User tables are stored as TiDB tables with:

- Binary collations for DynamoDB-sensitive strings.
- Generated columns for DynamoDB key and index attributes.
- Native secondary indexes for GSIs and LSIs.
- Native table TTL where TiDB can own expiration.
- Partitioning/pre-split Regions for hot shared and user data tables.
- `merge_option=deny` where empty split Regions must be preserved.

GSI and LSI writes are not replayed through companion tables. TiDB maintains
native secondary indexes transactionally from the base table row. Query and Scan
still enforce DynamoDB projection and pagination semantics above the physical
layout.

## Control Plane

Catalog intent is durable before physical DDL is submitted. Reconciliation is
idempotent: if a frontend observes an existing table, generated column, index,
or in-flight TiDB DDL job, it repairs or defers based on TiDB state and retries
from catalog intent.

Table and index transitions should avoid process-local locks. TiDB already owns
distributed online DDL scheduling and backfill. ExtendDB should only persist the
desired state, submit idempotent DDL, and publish the catalog transition after
the physical state is ready.

## Data Plane

Condition expressions are evaluated inside the storage transaction that performs
the write. This preserves DynamoDB's atomic check-and-write behavior for
PutItem, UpdateItem, DeleteItem, BatchWriteItem, and TransactWriteItems.

Read consistency is a storage routing decision:

- Strong reads and writes use the strong data pool.
- Default reads use the TiDB default-read pool configured for
  `tidb_replica_read = 'closest-adaptive'`.
- Optional bounded stale reads use `storage.tidb.default_read_staleness_seconds`
  on the default-read pool.

Pagination tokens are built from DynamoDB key attributes and the TiDB native
index order. Index reads use TiDB's secondary-index order plus the clustered
primary-key handle as the deterministic tie breaker.

## Streams, TTL, Metrics, And Rate Limits

Stream records live in TiDB and use MVCC commit timestamps plus an in-transaction
ordinal for ordering. Retention is TiDB-native where possible.

TTL deletes are TiDB-native. ExtendDB does not run an application sweep worker
or synthesize TTL service REMOVE stream records.

Metrics and rate-limit state live in TiDB tables so multiple frontends see one
consistent view. Avoid process-local capacity limiters; use TiDB Resource
Control/resource groups for distributed capacity governance.

## Backup, Restore, And Export

TiDB BR is the physical backup data plane. ExtendDB stores backup metadata and
delegates snapshot data to BR. Point-in-time export uses TiDB historical reads
with `AS OF TIMESTAMP`; BR PITR remains a cluster recovery path rather than an
ExtendDB table-copy implementation.

## Verification

Storage changes should pass:

```bash
cargo check -j12 --workspace
cargo test -j12 --workspace
cargo clippy -j12 --workspace --all-targets -- -D warnings
```

When a change affects physical TiDB behavior, also run the TiDB-backed Python
integration tests through `devtools/run-tests --extenddb --all`.

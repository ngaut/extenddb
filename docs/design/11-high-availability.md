# Scaling And High Availability

## Scope

ExtendDB scaling is TiDB-first. The frontend is stateless enough to run multiple
instances, while TiDB owns durable data, replication, online DDL, follower reads,
Resource Control, TTL, and physical backup/restore.

The design goal is to keep coordination in TiDB whenever TiDB already provides
the distributed primitive. Do not add process-local locks, per-node schema
workers, custom TTL sweepers, or local capacity limiters unless a live TiDB
limitation has been verified and documented.

## Runtime Topology

Each ExtendDB frontend opens TiDB pools for:

- Catalog metadata and control-plane work.
- Catalog-store/authz work.
- Strong data reads and writes.
- Default-read data traffic.

Operators should size TiDB SQL nodes for roughly:

```text
frontends * (2 * catalog_pool_size + 2 * pool_size)
```

plus any operational clients. Pool sizing belongs in `[storage.tidb]`.

## Read Routing

The engine passes DynamoDB's consistency flag to storage:

- `ConsistentRead=true` and all writes use the strong data pool.
- Default reads use the default-read pool with TiDB
  `tidb_replica_read = 'closest-adaptive'`.
- Optional bounded stale reads use
  `storage.tidb.default_read_staleness_seconds`.
- `TransactGetItems` always uses strong transactional reads.

This keeps consistency choices at the storage boundary instead of scattering
conditionals through operation handlers.

## Schema Changes

Catalog intent is the source of truth. Frontends reconcile desired table and
index state through idempotent TiDB online DDL:

- Create tables with generated columns and native secondary indexes.
- Add GSI generated columns before adding the native index.
- Drop GSI indexes and generated columns with native DDL when safe.
- Defer while TiDB already has a queued/running DDL job for the same table.
- Retry write conflicts, schema-version races, lock waits, and deadlocks from
  the durable intent.

TiDB's DDL owner and online schema-job scheduler are the distributed
coordination mechanism. ExtendDB should not add a per-cluster schema leader.

## Capacity

Use TiDB Resource Control/resource groups for distributed capacity governance.
Process-local token buckets do not produce correct quotas when multiple
frontends serve the same catalog.

When `storage.tidb.resource_group` is configured, runtime pools bind their
sessions to that resource group before serving traffic.

## Failure Model

| Failure | Expected behavior | Operator action |
|---|---|---|
| One ExtendDB frontend exits | Other frontends continue serving through TiDB. | Restart the frontend. |
| TiDB SQL endpoint unavailable | ExtendDB health/readiness fails and data requests fail with storage unavailable. | Repair TiDB SQL access or fail over according to TiDB operations. |
| TiKV/PD disruption | TiDB determines availability based on quorum and placement rules. | Use TiDB diagnostics and recovery procedures. |
| DDL job stalls | Table/index transition remains pending or updating. | Inspect TiDB DDL jobs and resolve the TiDB-side blocker. |
| BR backup/restore failure | Backup metadata records failure; data remains in TiDB. | Inspect BR logs and object storage credentials. |

## Health Checks

`/health` is readiness-oriented. It should verify that the configured TiDB pools
can acquire sessions and that catalog metadata is readable. Deeper checks for
DDL queues, TTL jobs, BR state, slow queries, and Resource Control belong in
operator diagnostics, not in the hot health endpoint.

## Data And Index Atomicity

TiDB native secondary indexes are part of the base table's transactional state.
ExtendDB does not maintain separate index companion tables. A successful write
updates the base row, projected index keys, and stream capture state in the same
storage transaction.

## Verification

For scaling or HA changes, run the normal Rust gates plus TiDB-backed integration
tests:

```bash
cargo test -j12 --workspace
devtools/run-tests --extenddb --all
```

Changes that alter read routing, DDL reconciliation, Resource Control, TTL, or
BR behavior need targeted TiDB verification against a live cluster.

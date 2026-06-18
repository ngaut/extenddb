# Differences from DynamoDB

This document lists all known behavioral differences between ExtendDB and real
Amazon DynamoDB. Use it to understand what works identically and what requires
adaptation when switching between ExtendDB and the real service.

## Storage and Infrastructure

| Area | DynamoDB | ExtendDB |
|------|----------|------|
| Storage technology | Proprietary distributed storage | TiDB |
| Global Tables | CreateGlobalTable, replication | Not implemented (returns UnknownOperationException) |
| DAX (Accelerator) | In-memory caching layer | Not applicable |
| PartiQL | ExecuteStatement, BatchExecuteStatement | Not implemented (returns UnknownOperationException) |

## Authentication and Authorization (AWS IAM/STS auth surface used by DynamoDB)

| Area | DynamoDB | ExtendDB |
|------|----------|------|
| Credential management | AWS IAM console/API | `extenddb manage` CLI and `/management` REST API |
| Access key prefixes | `AKIA` (long-term), `ASIA` (session) AWS-wide IAM/STS conventions | `AKIAEXTENDDB` (long-term), `ASIAEXTENDDB` (session) |
| Federated roles | AssumeRoleWithSAML, AssumeRoleWithWebIdentity | Not implemented |
| Role chaining | Supported | Not implemented |
| SourceIdentity, TransitiveTagKeys | Supported | Not implemented |
| Resource policies | Supported | Not implemented (deferred) |

## Import and Export

| Area | DynamoDB | ExtendDB |
|------|----------|------|
| Import source | S3BucketSource (S3 bucket) | FileSource (local filesystem path) |
| Export destination | S3 bucket | Local filesystem path |
| Import formats | CSV, DYNAMODB_JSON, ION | CSV, DYNAMODB_JSON, ION |
| Export formats | DYNAMODB_JSON, ION | DYNAMODB_JSON, ION |
| Import execution | Asynchronous (background job) | Synchronous (completes before returning) |
| Export execution | Point-in-time snapshot | Synchronous storage-owned snapshot. ExtendDB honors `ExportTime` with TiDB native `AS OF TIMESTAMP` |

## Control Plane

| Area | DynamoDB | ExtendDB |
|------|----------|------|
| Table creation delay | Returns `CREATING` immediately; transitions to `ACTIVE` typically within seconds. Same behavior for on-demand and provisioned | ExtendDB reconciles through TiDB native online DDL and does not expose a frontend delay simulator |
| DeletionProtectionEnabled | Enforced | Enforced (accepted and stored, DeleteTable rejects when enabled) |

## Time to Live (TTL)

| Area | DynamoDB | ExtendDB |
|------|----------|------|
| TTL attribute name | Any UTF-8 string (1–255 bytes) | Same 1–255 UTF-8 byte bound for ordinary names, including spaces, quotes, punctuation, and non-ASCII names. ExtendDB rejects the null character because TiDB SQL metadata cannot represent it reliably. |
| TTL deletion | Background process, items deleted within 48 hours of expiry | ExtendDB background worker deletes expired user items through TiDB transactions |
| TTL transition states | `ENABLING`, `ENABLED`, `DISABLING`, `DISABLED` | Same API states. TiDB stores these states explicitly in the catalog so distributed startup repair can complete TTL lookup-artifact DDL and catalog transitions after a crash. |
| TTL stream records | REMOVE events with `userIdentity: {type: "Service", principalId: "dynamodb.amazonaws.com"}` | Same. TTL expiry flows through ExtendDB's transactional delete path so streams see service-owned REMOVE records. |
| TTL modification cooldown | Enforces a cooldown period between enable/disable changes ("Time to live has been modified multiple times within a fixed interval") | No cooldown — TTL can be enabled and disabled immediately. Intentional divergence for faster local development. |

## Tagging

| Area | DynamoDB | ExtendDB |
|------|----------|------|
| TagResource / UntagResource | Validates resource ARN exists, returns `ResourceNotFoundException` for missing tables | Matches DynamoDB — validates resource ARN and returns `ResourceNotFoundException` for missing tables. |

## Secondary Indexes

| Area | DynamoDB | ExtendDB |
|------|----------|------|
| GSI update propagation | Eventually consistent (milliseconds to seconds) | TiDB native secondary indexes are maintained from the base table write |
| Multi-part base table keys | Not supported | Preview extension. Standard single/composite keys work identically. TiDB accepts multi-HASH shapes that fit its raw 2048-byte hash slot, and rejects multi-RANGE shapes because native TiDB indexes must stay within the 3072-byte key limit. |

## Capacity and Throttling

| Area | DynamoDB | ExtendDB |
|------|----------|------|
| Provisioned throughput | Token bucket per table/partition | ExtendDB delegates distributed capacity governance to TiDB Resource Control/resource groups; `storage.tidb.resource_group` can bind runtime sessions to the selected group |
| On-demand capacity | Automatic scaling | ExtendDB delegates cluster capacity and scheduling to TiDB |
| Throttling | Always on; throttles requests that exceed provisioned/burst capacity. No setting to disable | ExtendDB does not implement process-local throughput admission or DynamoDB throughput-error translation; use TiDB native Resource Control/resource groups |
| `ConsumedCapacity=INDEXES` | Includes table and per-index capacity maps | ExtendDB returns aggregate/table capacity only; TiDB native secondary indexes do not expose per-index request-unit attribution |

## Operations Not Implemented

The following operations return `UnknownOperationException`:

- CreateGlobalTable, DescribeGlobalTable, ListGlobalTables, UpdateGlobalTable
- DescribeGlobalTableSettings, UpdateGlobalTableSettings
- ExecuteStatement, BatchExecuteStatement, ExecuteTransaction
- DescribeContributorInsights, UpdateContributorInsights
- DescribeKinesisStreamingDestination, EnableKinesisStreamingDestination, DisableKinesisStreamingDestination
- DescribeTableReplicaAutoScaling, UpdateTableReplicaAutoScaling

## Runtime Configuration

ExtendDB exposes runtime settings that have no DynamoDB equivalent:

| Setting | Default | Description |
|---------|---------|-------------|
| `log_level` | `info` | Runtime log level (trace, debug, info, warn, error) |
| `sqlx_log_level` | `warn` | Separate log level for sqlx query traces |
| `allow_credential_import` | `true` | Allow importing credentials via the management API |
| `ttl_expiry_interval_ms` | `1000` | Delay between user-table TTL expiry worker polling cycles |
| `ttl_expiry_batch_size` | `1000` | Maximum expired user items deleted per cleanup batch |
| `ttl_expiry_table_scan_limit` | `1024` | Maximum TTL-enabled table candidates scanned per cleanup batch; the worker advances a table-id cursor and wraps around |
| `ttl_expiry_drain_batches` | `8` | Maximum back-to-back cleanup batches per poll when each batch fills, allowing aggressive backlog drain under high write throughput |

## TiDB Native Backup/Restore

TiDB storage uses TiDB BR for backup data instead of copying items into
ExtendDB catalog tables. `CreateBackup` requires `[storage.tidb.backup]`
configuration (`pd_endpoint` and `storage_uri`). Because the API call waits for
BR to complete, ExtendDB publishes TiDB backup catalog metadata only after BR
has produced the snapshot, and the backup appears as `AVAILABLE`.
The catalog and data databases must be in the same TiDB cluster: ExtendDB takes
one TiDB TSO inside a transaction, reads catalog metadata at that snapshot, and
passes the same timestamp to BR as `--backupts`.

BR restores physical TiDB tables to their recorded database/table identity.
That means TiDB `RestoreTableFromBackup` is available only when the target TiDB
cluster is empty or conflict-free for the backed physical table. ExtendDB does
not emulate unsupported BR restore shapes by replaying item rows. The TiDB
storage layer publishes the target table catalog only after BR restore, physical table
rename, and restored-table normalization complete; an interrupted restore does
not leave a durable `CREATING` table entry.

TiDB table-level `RestoreTableToPointInTime` is not exposed. TiDB has native
historical reads, but they are read-only for this live-target restore shape:
the target table is current and therefore invisible under a historical session
snapshot, while `INSERT ... SELECT ... AS OF TIMESTAMP` mixes a current write
target with a stale-read source. ExtendDB does not add a frontend row-replay
implementation because that would duplicate TiDB's data plane and change the
operational profile. Use native BR for table backups, or TiDB cluster-level PITR
into an empty or conflict-free recovery cluster when full-cluster recovery is
required.

Restored tables do not inherit TTL or stream settings. When BR restores a TiDB
table that previously used native TTL, ExtendDB strips the restored physical TTL
artifacts before publishing the target table as `ACTIVE`.

`DeleteBackup` removes ExtendDB's catalog reference to the TiDB BR backup.
TiDB BR snapshot files remain under the configured backup storage URI and are
managed by the operator, TiDB Operator clean policy, or object-store lifecycle
rules. ExtendDB does not run a frontend-side file deleter for BR data.

## Web Console

ExtendDB includes a built-in web management console at `/console` for credential
and account management. DynamoDB uses the AWS Management Console.

---

## License

Copyright 2026 ExtendDB contributors. Licensed under the Apache License, Version 2.0.
See [LICENSE](../LICENSE) for the full text.

This software is provided "as is" without warranty of any kind. ExtendDB is not
affiliated with, endorsed by, or sponsored by Amazon Web Services. "DynamoDB" is a trademark
of Amazon.com, Inc.

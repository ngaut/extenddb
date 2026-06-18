# DynamoDB Limits Enforcement Status

Last updated: 2026-06-03

Source: [AWS DynamoDB Service Quotas](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/ServiceQuotas.html)

## Legend

- **Enforced**: ExtendDB validates and rejects requests that exceed this limit
- **Partial**: ExtendDB enforces the limit in some but not all code paths
- **Not enforced**: ExtendDB accepts requests that would exceed this limit on real DynamoDB
- **N/A**: Limit does not apply to ExtendDB (e.g., provisioned throughput billing, global tables)

## Read/Write Throughput

| Limit | DynamoDB Value | Status | Notes |
|-------|---------------|--------|-------|
| Per-table RCU (provisioned) | 40,000 | TiDB-native | Use TiDB Resource Control/resource groups for distributed enforcement; DynamoDB throughput errors are not synthesized |
| Per-table WCU (provisioned) | 40,000 | TiDB-native | Use TiDB Resource Control/resource groups for distributed enforcement; DynamoDB throughput errors are not synthesized |
| Per-table read request units (on-demand) | 40,000 | TiDB-native | ExtendDB delegates capacity to TiDB Resource Control |
| Per-table write request units (on-demand) | 40,000 | TiDB-native | ExtendDB delegates capacity to TiDB Resource Control |
| Per-account RCU (provisioned) | 80,000 | TiDB-native | Use TiDB Resource Control/resource groups for distributed enforcement; DynamoDB throughput errors are not synthesized |
| Per-account WCU (provisioned) | 80,000 | TiDB-native | Use TiDB Resource Control/resource groups for distributed enforcement; DynamoDB throughput errors are not synthesized |
| Minimum throughput per table/GSI | 1 RCU / 1 WCU | Enforced | `validate_provisioned_throughput` rejects < 1 |
| Provisioned capacity decrease limit | 27 per day (4 + 1/hour) | Enforced | `apply_provisioned_throughput_update` tracks table decreases under catalog row locks and returns `NumberOfDecreasesToday` |
| Reserved capacity per account | 1,000,000 units | N/A | ExtendDB has no reserved capacity concept |

## Tables

| Limit | DynamoDB Value | Status | Notes |
|-------|---------------|--------|-------|
| Maximum tables per account per region | 2,500 (adjustable to 10,000) | Enforced | `LimitsConfig::max_tables_per_account`, configurable |
| Table size | No practical limit | Enforced | ExtendDB has no table size limit |
| Table name length | 3–255 characters | Enforced | `validate_table_name`, `LimitsConfig` |
| Table name character set | `[a-zA-Z0-9_.-]` | Enforced | `validate_table_name_chars` |

## Items

| Limit | DynamoDB Value | Status | Notes                                                                         |
|-------|---------------|--------|-------------------------------------------------------------------------------|
| Maximum item size | 400 KB (409,600 bytes) | Enforced | `LimitsConfig::max_item_size_bytes`, validated on PutItem and post-UpdateItem |
| Partition key size | 1–2,048 bytes | Enforced | `validate_key_sizes`, `LimitsConfig::max_partition_key_size_bytes`            |
| Sort key size | 1–1,024 bytes | Enforced | `validate_key_sizes`, `LimitsConfig::max_sort_key_size_bytes`                 |
| Attribute name size | 1–64 KB (65,535 bytes) | Enforced | `validate_attribute_name_sizes`, `LimitsConfig::max_attribute_name_bytes`     |
| Attribute nesting depth | 32 levels | Enforced | `validate_item_nesting_depth`, applied on PutItem, UpdateItem, BatchWriteItem.PutRequest, TransactWriteItems.Put, ImportTable |
| Number of attributes per item | No practical limit | Enforced | ExtendDB has no per-item attribute count limit                                |

## Secondary Indexes

| Limit | DynamoDB Value | Status | Notes |
|-------|---------------|--------|-------|
| GSIs per table | 20 | Enforced | `validate_gsi_count`, `LimitsConfig::max_gsis_per_table`, configurable |
| LSIs per table | 5 | Enforced | `validate_lsi_count`, `LimitsConfig::max_lsis_per_table`, configurable |
| Projected attributes across all indexes | 100 | Enforced | `validate_projected_attribute_count` counts INCLUDE `NonKeyAttributes` across GSIs/LSIs before CreateTable and TiDB GSI create |
| Index name length | 3–255 characters | Enforced | `validate_index_name` |
| Index name character set | `[a-zA-Z0-9_.-]` | Enforced | `validate_index_name` |
| LSI item collection size | 10 GB | Enforced for TiDB | LSI tables store exact partition-key totals in a table-local `_collections` ledger; TiDB write transactions update old/new size deltas and reject positive deltas above `LimitsConfig::max_lsi_item_collection_size_bytes` |

## Query and Scan

| Limit | DynamoDB Value | Status | Notes |
|-------|---------------|--------|-------|
| Response size per page | 1 MB (1,048,576 bytes) | Enforced | `read_helpers.rs` enforces 1 MB page limit |
| `Limit` parameter (max items evaluated) | No maximum | Enforced | Honored in query/scan |
| Filter expression size | 4 KB | Enforced | `LimitsConfig::max_expression_bytes` rejects oversized expression strings before tokenization |
| Projection expression size | 4 KB | Enforced | `LimitsConfig::max_expression_bytes` rejects oversized expression strings before tokenization |
| Condition expression size | 4 KB | Enforced | `LimitsConfig::max_expression_bytes` rejects oversized expression strings before tokenization |
| Expression attribute names | 2 MB total | Enforced | `LimitsConfig::max_expression_attribute_names_bytes` rejects oversized substitution maps before parsing/storage |
| Expression attribute values | 2 MB total | Enforced | `LimitsConfig::max_expression_attribute_values_bytes` rejects oversized substitution maps before parsing/storage |

## Batch Operations

| Limit | DynamoDB Value | Status | Notes |
|-------|---------------|--------|-------|
| BatchGetItem: max keys | 100 | Enforced | `MAX_BATCH_GET_KEYS` in `batch_get_item.rs` |
| BatchGetItem: response size | 16 MB | Enforced | `max_batch_get_response_bytes` defers overflow keys into `UnprocessedKeys` |
| BatchWriteItem: max operations | 25 | Enforced | `MAX_BATCH_WRITE_ITEMS` in `batch_write_item.rs` |
| BatchWriteItem: max item size | 400 KB | Enforced | Item size validated per item |
| BatchWriteItem: max request size | 16 MB | Enforced | `max_batch_write_request_bytes` validates canonical request bytes before storage |

## Transactions

| Limit | DynamoDB Value | Status | Notes |
|-------|---------------|--------|-------|
| TransactWriteItems: max items | 100 | Enforced | `MAX_TRANSACT_WRITE_ITEMS` in `transact_write_items.rs` |
| TransactGetItems: max items | 100 | Enforced | `MAX_TRANSACT_GET_ITEMS` in `transact_get_items.rs` |
| Transaction request size | 4 MB | Enforced | `max_transaction_request_bytes` validates request bytes plus aggregate transaction item/response bytes |
| Items per transaction across tables | No limit on table count | Enforced | ExtendDB supports cross-table transactions |

## DynamoDB Streams

| Limit | DynamoDB Value | Status | Notes |
|-------|---------------|--------|-------|
| Simultaneous shard readers | 2 (1 for global tables) | Enforced | Storage-backed stream reader leases admit at most two active reader ids per shard; ExtendDB has no global tables, so the global-table value is N/A |
| Max write capacity with streams (provisioned) | 40,000 WCU | Enforced | Same as table WCU limit |
| GetRecords: max records per call | 1,000 | Enforced | `GetRecords` defaults and caps `Limit` at 1,000 before storage reads |
| Shard iterator lifetime | 15 minutes | Enforced | Iterator tokens carry creation time and `GetRecords` rejects expired iterators |

## API-Level Limits

| Limit | DynamoDB Value | Status | Notes                                    |
|-------|---------------|--------|------------------------------------------|
| ListTables: max per page | 100 | Enforced | `LimitsConfig::list_tables_max_per_page` |
| DescribeTable: request rate | No specific limit | N/A | ExtendDB does not rate-limit             |
| TagResource: max tags per resource | 50 | Enforced | `validate_tags` and storage-side merged-count validation use `LimitsConfig::max_tags_per_resource` |
| Tag key length | 1–128 characters | Enforced | `validate_tag_key`, configurable via `LimitsConfig::max_tag_key_length` |
| Tag value length | 0–256 characters | Enforced | `validate_tag_value`, configurable via `LimitsConfig::max_tag_value_length` |

## Import from Amazon S3

| Limit | DynamoDB Value | Status | Notes |
|-------|---------------|--------|-------|
| Concurrent import jobs | 50 | N/A | ExtendDB import is synchronous, single-threaded |
| Max S3 objects per import | 50,000 | N/A | ExtendDB imports from local files |
| Total import size | 15 TB (us-east-1/us-west-2/eu-west-1), 1 TB (other) | N/A | No size limit on local import |

## Table Export to Amazon S3

| Limit | DynamoDB Value | Status | Notes |
|-------|---------------|--------|-------|
| Concurrent export tasks | 300 | N/A | ExtendDB export is synchronous, single-threaded |
| Total in-flight export size | 100 TB | N/A | No size limit on local export |
| Incremental export window | 15 min – 24 hours | N/A | ExtendDB does not support incremental export |

## Backup and Restore

| Limit | DynamoDB Value | Status | Notes |
|-------|---------------|--------|-------|
| Concurrent restores | 50 | TiDB-native | Backup/restore delegates physical data to TiDB BR. ExtendDB does not enforce DynamoDB's concurrent restore quota. |

## Global Tables

| Limit | DynamoDB Value | Status | Notes |
|-------|---------------|--------|-------|
| MRSC global tables | 400 | N/A | ExtendDB does not support global tables |
| Backfill data per account/region/day | 10 TB | N/A | ExtendDB does not support global tables |

## Contributor Insights

| Limit | DynamoDB Value | Status | Notes |
|-------|---------------|--------|-------|
| All Contributor Insights quotas | Various | N/A | ExtendDB does not support Contributor Insights |

## Summary

### Enforcement Coverage

| Category | Enforced | Partial | Not Enforced | N/A |
|----------|----------|---------|--------------|-----|
| Throughput | 6 | 0 | 0 | 2 |
| Tables | 4 | 0 | 0 | 0 |
| Items | 6 | 0 | 0 | 0 |
| Secondary Indexes | 6 | 0 | 0 | 0 |
| Query/Scan | 7 | 0 | 0 | 0 |
| Batch Operations | 5 | 0 | 0 | 0 |
| Transactions | 4 | 0 | 0 | 0 |
| Streams | 4 | 0 | 0 | 0 |
| API-Level | 4 | 0 | 0 | 1 |
| Import/Export/Backup | 0 | 0 | 0 | 8 |
| Global Tables | 0 | 0 | 0 | 2 |
| Contributor Insights | 0 | 0 | 0 | 1 |
| **Total** | **46** | **0** | **0** | **14** |

---

## License

Copyright 2026 ExtendDB contributors. Licensed under the Apache License, Version 2.0.
See [LICENSE](../LICENSE) for the full text.

This software is provided "as is" without warranty of any kind. ExtendDB is not
affiliated with, endorsed by, or sponsored by Amazon Web Services. "DynamoDB" is a trademark
of Amazon.com, Inc.

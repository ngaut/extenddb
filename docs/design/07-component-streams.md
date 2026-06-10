# extenddb - Component Design: DynamoDB Streams

**Version:** 2.0
**Date:** 2026-06-07
**Status:** Implemented design

## 1. Purpose

DynamoDB Streams provides change data capture for table writes. When streams
are enabled, `PutItem`, `UpdateItem`, `DeleteItem`, `BatchWriteItem`, and
`TransactWriteItems` generate stream records with keys and the configured
old/new image shape. TTL deletes are delegated to TiDB native TTL, so ExtendDB
does not synthesize TTL service stream records.

ExtendDB serves DynamoDB and DynamoDB Streams on the same HTTPS endpoint. The
wire target prefix distinguishes operations:

| Prefix | Operations |
|--------|------------|
| `DynamoDB_20120810` | Table and item APIs |
| `DynamoDBStreams_20120810` | `ListStreams`, `DescribeStream`, `GetShardIterator`, `GetRecords` |

Both paths use mandatory SigV4 auth and the same account isolation model.

## 2. Storage Model

Streams are backend-owned storage state, not frontend-local state.

- Stream records are written in the same storage transaction as the data
  mutation when the backend can do so.
- TiDB stages native stream records inside write transactions and finalizes
  them through backend-native append tables and stream generations.
- Stream metadata is tied to table metadata through stream labels, so disabled
  stream generations remain addressable during their retention window.

The storage trait exposes stream methods for record writes, shard validation,
stream description/listing, iterator reads, latest sequence lookup, and reader
lease claiming.

## 3. Shards And Iterators

ExtendDB uses fixed hash-based shards per stream generation. Shard IDs encode
the shard index, stream label, and table identity so stale iterator tokens
cannot silently drift to a new table generation.

`GetShardIterator` validates the stream and shard, claims a bounded reader
lease, resolves the requested iterator type to an internal
`AFTER_SEQUENCE_NUMBER` position, and returns an opaque base64 token containing:

- shard ID
- normalized iterator mode
- sequence position
- creation timestamp
- stream ARN
- reader ID

`GetRecords` decodes the token, rejects expired iterators after 15 minutes,
revalidates the shard against the authenticated account and original stream
ARN, enforces the reader lease limit, reads records, and returns a fresh next
iterator.

## 4. Retention And Lifecycle

DynamoDB-compatible stream retention is 24 hours by default. TiDB uses native
generation metadata and backend cleanup paths so disabled generations and their
records age out without frontend replay logic.

Enabling or disabling streams updates table metadata and stream-generation
metadata atomically with the backend's catalog path. The data plane reads the
cached table key info to decide whether a write should capture stream data.

## 5. Compatibility Boundaries

Implemented:

- `ListStreams`
- `DescribeStream`
- `GetShardIterator`
- `GetRecords`
- fixed shard descriptions
- stream view types: `KEYS_ONLY`, `NEW_IMAGE`, `OLD_IMAGE`, `NEW_AND_OLD_IMAGES`
- 15-minute shard iterator expiry
- bounded simultaneous readers per shard
- stream ARN and account rebinding for iterator tokens

Not implemented:

- dynamic shard splitting
- Kinesis adapter compatibility
- multi-region stream replication

---

## License

Copyright 2026 ExtendDB contributors. Licensed under the Apache License,
Version 2.0. See [LICENSE](../../LICENSE) for the full text.

This software is provided "as is" without warranty of any kind. ExtendDB is not
affiliated with, endorsed by, or sponsored by Amazon Web Services. "DynamoDB" is
a trademark of Amazon.com, Inc.

# Runtime Symptoms

This file holds verbatim Cause and Fix entries for runtime performance and eventual-consistency symptoms that appear while the extenddb server is running. Entries are copied from `docs/troubleshooting.md` and the "Source" line at the end of each entry records the section and last sync date for drift detection.

## Connection pool exhausted

### HTTP 503 on all requests under heavy load

<a name="connection-pool-exhausted"></a>

**Error text:**
```
HTTP 503 on all requests under heavy load
```

**Cause:** The storage backend connection pool is exhausted. All connections are in use and new requests cannot acquire a connection within the timeout. extenddb maps typed backend unavailability, SQL pool-acquire timeouts, and closed-pool acquisition errors to `ServiceUnavailable` / HTTP 503 instead of exposing internal storage details.

**Fix:** Increase the pool size in `extenddb.toml`:
```toml
[storage.tidb]
pool_size = 50
catalog_pool_size = 50
```

Each frontend opens strong-data, default-read-data, engine-catalog, and catalog-store/auth pools. If the problem persists, inspect TiDB sessions, slow queries, DDL jobs, and Resource Control throttling with TiDB's cluster diagnostics.

**Source:** `docs/troubleshooting.md`, section "Connection Pool Exhaustion", last synced 2026-06-06.

## Streams capture delay

### `Stream capture: failed to assign shard for <table>: <error>`

<a name="streams-capture-delay"></a>

**Error text:**
```
Stream capture: failed to assign shard for <table>: <error>
```

**Cause:** After a successful write (PutItem, DeleteItem, UpdateItem), extenddb tried to capture a stream record but could not determine which shard to assign it to. The data write succeeded — only the stream record is missing.

**Fix:** Check TiDB connectivity and stream metadata. TiDB stores stream records in the shared data database table and uses native TTL for retention.

**Source:** `docs/troubleshooting.md`, section "DynamoDB Streams", last synced 2026-06-06.

### `Stream capture: failed to write record for <table>: <error>`

**Error text:**
```
Stream capture: failed to write record for <table>: <error>
```

**Cause:** A stream record was constructed but could not be persisted to the `stream_records` table. The data write succeeded — only the stream record is missing.

**Fix:** Check storage backend connectivity and disk space. TiDB uses MVCC commit timestamps plus a per-transaction ordinal for stream sequence numbers, so duplicate stream sequence errors indicate a storage failure rather than same-millisecond application writes or multiple writes in one transaction.

**Source:** `docs/troubleshooting.md`, section "DynamoDB Streams", last synced 2026-06-06.

### `Stream capture: failed to get sequence number: <error>`

**Error text:**
```
Stream capture: failed to get sequence number: <error>
```

**Cause:** extenddb could not generate a sequence number for a stream record. The data write succeeded — only the stream record is missing.

**Fix:** Check storage backend connectivity.

**Source:** `docs/troubleshooting.md`, section "DynamoDB Streams", last synced 2026-06-06.

### `Stream cleanup worker: <error>`

**Error text:**
```
Stream cleanup worker: <error>
```

**Cause:** On backends without native stream-record TTL, the background worker that deletes stream records older than 24 hours encountered a database error. Expired records will accumulate until the worker succeeds.

**Fix:** Check storage backend connectivity. The worker retries every hour automatically. TiDB does not run this worker; it uses native table TTL on `stream_records`, so TiDB stream-retention failures should be investigated through TiDB TTL job state and table DDL instead.

**Source:** `docs/troubleshooting.md`, section "DynamoDB Streams", last synced 2026-06-06.

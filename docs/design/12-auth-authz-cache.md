# Auth And Authorization Cache

## Scope

ExtendDB uses builtin SigV4 authentication and IAM-style authorization. The
server caches credential and policy lookups above the TiDB-backed stores to
reduce request latency without changing the storage contract. Table metadata is
prefetched per request from TiDB and is not cached across requests by the server.

The cache is a wrapper around trait objects. It is not embedded in the TiDB
store and it does not introduce a second persistence path.

## Cached Data

| Data | Source trait | Invalidated by |
|---|---|---|
| Access key/session credential lookup | `CredentialStore` | access-key, user, role, or session changes |
| IAM policy and principal metadata | `AuthorizationStore` | management API mutations |

All cached entries must fail closed. Missing, malformed, or stale authorization
state should deny access rather than grant it.

## Invalidation

Management APIs emit explicit invalidation scopes after successful storage
commits. The frontend that performs the mutation invalidates its local cache and
bumps the TiDB-backed `auth_cache_epoch` settings row. Other frontends poll that
epoch and flush local auth caches when it changes. If the epoch bump or poller
fails, bounded TTLs still force refresh from TiDB.

The cache does not rely on process-local notification channels. TiDB remains the
durable source of truth and the epoch is only a coarse invalidation signal.

## Request Flow

1. The server verifies the SigV4 request shape.
2. Credential lookup is served from cache or TiDB.
3. IAM policy context is resolved from cache or TiDB.
4. The authorization engine evaluates identity, group, role, session policy,
   permissions boundary, resource, and condition context.
5. The auth path prefetches table metadata from TiDB for single-table reads and
   writes; operation handlers reuse it through `OperationContext`.
6. Storage operations execute against TiDB.

## Metrics

Cache implementations should expose hit, miss, eviction, and invalidation
counts. These metrics are diagnostic only; authorization correctness must not
depend on the cache being warm.

## Verification

Auth/cache changes should include tests for:

- Access key creation, deletion, and rotation.
- User/group/role policy mutation invalidation.
- Permissions boundary changes.
- Distributed epoch bump and local flush behavior.
- Request-local table metadata prefetch for single-table reads and writes.
- Immediate table/index visibility through TiDB metadata reads.
- Fail-closed behavior for malformed stored policies.

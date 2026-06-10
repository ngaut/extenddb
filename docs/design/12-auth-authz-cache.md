# Auth And Authorization Cache

## Scope

ExtendDB uses builtin SigV4 authentication and IAM-style authorization. The
server caches credential, policy, and table-key lookups above the TiDB-backed
stores to reduce request latency without changing the storage contract.

The cache is a wrapper around trait objects. It is not embedded in the TiDB
store and it does not introduce a second persistence path.

## Cached Data

| Data | Source trait | Invalidated by |
|---|---|---|
| Access key/session credential lookup | `CredentialStore` | access-key, user, role, or session changes |
| IAM policy and principal metadata | `AuthorizationStore` | management API mutations |
| Table key metadata | `TableEngine` | CreateTable, UpdateTable, DeleteTable, stream/index changes |

All cached entries must fail closed. Missing, malformed, or stale authorization
state should deny access rather than grant it.

## Invalidation

Management APIs emit explicit invalidation scopes after successful storage
commits. Data-plane table metadata is invalidated after table and index catalog
state changes. A frontend that misses an invalidation still has bounded TTLs and
will refresh from TiDB.

The cache does not rely on backend-specific notification channels. TiDB remains
the durable source of truth.

## Request Flow

1. The server verifies the SigV4 request shape.
2. Credential lookup is served from cache or TiDB.
3. IAM policy context is resolved from cache or TiDB.
4. The authorization engine evaluates identity, group, role, session policy,
   permissions boundary, resource, and condition context.
5. The operation handler fetches table key metadata from cache or TiDB.
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
- Table key metadata invalidation after table/index updates.
- Fail-closed behavior for malformed stored policies.

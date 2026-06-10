# ExtendDB Component Design: Authentication And Authorization

**Status:** Implemented
**Crate:** `extenddb-auth`

## Purpose

`extenddb-auth` owns request authentication primitives and IAM-style policy
evaluation. It has no storage or HTTP-server dependency beyond `HeaderMap`, so
the server can compose it with any storage backend.

The shipped runtime supports exactly one authentication provider:
`auth.provider = "builtin"`. Startup rejects `auth.provider = "none"` and any
unknown provider. Future providers can implement `AuthProvider`, but they are a
new design effort rather than part of the current architecture.

## Module Structure

```text
crates/auth/src/
├── lib.rs                  # AuthProvider, AuthIdentity, CredentialStore, BuiltinAuthProvider
├── cache_registry.rs       # Cross-cache invalidation traits
├── credential_cache.rs     # Stale-while-revalidate credential cache wrapper
├── sigv4/
│   ├── canonical.rs        # Canonical request and string-to-sign construction
│   ├── parse.rs            # Authorization header parsing
│   ├── signing_key.rs      # AWS4-HMAC-SHA256 signing key derivation
│   └── verify.rs           # Signature verification and timestamp checks
└── policy/
    ├── condition.rs        # IAM condition operators
    ├── context.rs          # Request/assume-role condition contexts
    ├── document.rs         # Policy document parser
    ├── evaluator.rs        # Explicit deny, boundary, session, allow evaluation
    └── matcher.rs          # Action, resource, wildcard, and ARN matching
```

## Authentication Model

### Identity

Authentication returns an `AuthIdentity`:

- `User`: long-lived access key owned by an IAM user.
- `RoleSession`: temporary access key created by `AssumeRole`; the access key id
  is part of the identity because session names are not unique.

There is no anonymous identity in the current runtime.

### AuthProvider

`AuthProvider` is intentionally small:

```rust
#[async_trait::async_trait]
pub trait AuthProvider: Send + Sync {
    async fn authenticate(
        &self,
        headers: &HeaderMap,
        body: &[u8],
    ) -> Result<AuthIdentity, DynamoDbError>;
}
```

The provider authenticates only. Authorization is a server-layer operation that
uses storage-owned authorization aggregates and the policy evaluator.

### CredentialStore

The auth crate defines `CredentialStore` for access-key lookup. The server
implements it against the selected storage backend, which keeps the auth crate
free of `sqlx`, TiDB, and PostgreSQL dependencies.

`StoredCredential` zeroizes secret material on drop. Long-lived keys and
temporary session keys use the same lookup interface; session credentials carry
`session_token` and `expires_at`, and expiration is checked even on cache hits.

## Built-In SigV4 Provider

`BuiltinAuthProvider` performs the DynamoDB SigV4 authentication path:

1. Read and parse the `Authorization` header.
2. Look up the access key id through `CredentialStore`.
3. Reject missing, inactive, expired, or token-mismatched credentials with the
   DynamoDB-compatible auth error shape.
4. Validate the request timestamp against the configured clock-skew window.
5. Rebuild the canonical request and string to sign.
6. Derive the signing key from the stored secret and compare the signature in
   constant time.
7. Return `AuthIdentity::User` or `AuthIdentity::RoleSession`.

The provider accepts the borrowed `HeaderMap` directly so request authentication
does not allocate a per-request header map.

## Authorization Model

The server maps each DynamoDB operation to an IAM action and resource ARN, then
loads all authorization metadata through the server-side authorization cache:

- identity policies
- group policies
- permissions boundary
- session policy and session tags
- principal tags
- resource tags

The cache fetches these inputs through split storage lookup methods and keeps
parsed policy documents hot. This keeps the storage contract narrow while
letting each backend keep native indexes on the underlying lookup tables.

The policy evaluator applies the IAM decision order:

1. Explicit deny across identity, group, boundary, and session policies.
2. Permissions boundary must allow the action when present.
3. Session policy must allow the action when present.
4. At least one identity or group policy must allow the action.
5. Otherwise the result is implicit deny.

## Policy Documents

Policy documents use the AWS IAM JSON shape:

- `Effect` is `Allow` or `Deny`.
- `Action` and `NotAction` are mutually exclusive.
- `Resource` and `NotResource` are mutually exclusive.
- `Principal` and `NotPrincipal` are parsed for trust policies.
- `Condition` supports the implemented string, numeric, date, ARN, bool, null,
  set, and `IfExists` operators.

Policy variables such as `${aws:PrincipalTag/team}` are expanded in condition
values. Resource ARN policy variables are not expanded today.

## Condition Context

The server builds a request context from the authenticated identity, resource
metadata, and operation parameters. Condition keys include:

- `aws:PrincipalTag/*`
- `aws:ResourceTag/*`
- `dynamodb:LeadingKeys`
- `dynamodb:Attributes`
- `dynamodb:Select`
- `dynamodb:ReturnValues`
- `dynamodb:ReturnConsumedCapacity`
- `dynamodb:FullTableScan`

Multi-value context fields distinguish an absent key from a present empty set,
which is required for correct `Null`, `ForAllValues`, and `ForAnyValue`
semantics.

## AssumeRole

`POST /management/assume-role` creates temporary credentials backed by the
storage management store:

1. Authenticate the caller through the management API.
2. Load the target role and its trust policy.
3. Evaluate the trust policy with principal ARN, principal tags, request tags,
   and optional `sts:ExternalId`.
4. Persist a temporary session credential and session metadata.
5. Return an `ASIA...` access key, secret key, session token, and expiration.

Authorization for role sessions fetches session policy and tags by access key id
instead of by role/session name, because session names are caller-controlled and
not unique.

## Caching

Credential and authorization caches use stale-while-revalidate wrappers with a
hard TTL, soft TTL, negative TTL, and explicit invalidation hooks. The cache
layer is optional; pass-through mode preserves the same interfaces while making
every lookup hit storage directly.

Self-induced changes from management APIs and the console invalidate affected
entries immediately. Off-instance changes are bounded by `auth.cache.ttl_seconds`
unless a future multi-instance invalidation channel is enabled.

## Server Integration

At startup, `extenddb serve` accepts only `auth.provider = "builtin"`. It builds:

- a backend-backed `CredentialStore`
- a `CachedCredentialStore`
- `BuiltinAuthProvider`
- a backend-backed `AuthorizationStore`
- `CachedAuthzStore`
- `CachedTableKeyInfoStore`

The request path is:

1. Authenticate with `BuiltinAuthProvider`.
2. Decode the DynamoDB operation.
3. Load authorization metadata from storage/cache.
4. Build `RequestContext`.
5. Evaluate IAM policies.
6. Execute the engine operation if allowed.

## Deferred Provider Boundary

External identity providers such as AWS IAM, OIDC, or Azure AD are not part of
the current runtime. Adding one requires a new design covering token format,
credential replay protection, policy retrieval, cache invalidation, error
mapping, and operator configuration. Until that work exists, startup rejection
of unknown providers is intentional architecture, not a missing branch.

---

## License

Copyright 2026 ExtendDB contributors. Licensed under the Apache License,
Version 2.0. See [LICENSE](../../LICENSE) for the full text.

This software is provided "as is" without warranty of any kind. ExtendDB is not
affiliated with, endorsed by, or sponsored by Amazon Web Services. "DynamoDB" is
a trademark of Amazon.com, Inc.

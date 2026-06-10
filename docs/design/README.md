# extenddb — Architecture & Design

This directory contains the authoritative design documents for ExtendDB (extenddb). All implementation work references these docs.

## Document Index

| #  | Document | Scope |
|----|----------|-------|
| 01 | [Requirements](01-requirements.md) | Wire protocol, operations, limits, data types, non-functional requirements |
| 02 | [High-Level Design](02-high-level-design.md) | Architecture overview, crate structure, request lifecycle, key design decisions, technology choices |
| 03 | [Core](03-component-core.md) | Types, expression engine, validation, capacity calculation, errors (`extenddb-core` crate) |
| 04 | [Storage](04-component-storage.md) | StorageEngine traits, TiDB schema design, pagination, native indexes, TTL, and backup/restore (`extenddb-storage`, `extenddb-storage-tidb` crates) |
| 05 | [Auth](05-component-auth.md) | AuthProvider trait, SigV4 validation, IAM policy engine, credential encryption (`extenddb-auth` crate) |
| 06 | [Server](06-component-server.md) | HTTP server, routing, middleware pipeline, response formatting, TLS, rate limiting, throughput tracking (`extenddb-server` crate) |
| 07 | [Streams](07-component-streams.md) | Implemented DynamoDB Streams design, shard iterators, retention, and compatibility boundaries |
| 08 | [Configuration](08-component-config.md) | TOML config, env vars, CLI, logging, metrics, health checks, deployment (VM, Kubernetes, Docker) |
| 09 | [Testing](09-testing.md) | Current test surfaces, external suite registry, TiDB acceptance gate, and required verification gates |
| 10 | [Licenses](10-dependency-licenses.md) | Licensing summary for third-party dependencies |
| 11 | [Scaling](11-high-availability.md) | DRAFT document on scaling ExtendDB |
| 12 | [Auth / Authz Cache](12-auth-authz-cache.md) | Implemented IAM and table-key cache design, invalidation rules, metrics, and deferred multi-instance fanout |

## How to Use These Docs

- **Before implementing a feature:** read the relevant component doc and the requirements doc for the operation.
- **Requirement IDs** (e.g., `REQ-WIRE-001`, `REQ-DATA-005`) are stable references — use them in code comments and commit messages.
- **Deferred items** are explicitly marked. Do not implement deferred features without updating the design doc first.

---

## License

Copyright 2026 ExtendDB contributors. Licensed under the Apache License, Version 2.0.
See [LICENSE](../../LICENSE) for the full text.

This software is provided "as is" without warranty of any kind. ExtendDB is not
affiliated with, endorsed by, or sponsored by Amazon Web Services. "DynamoDB" is a trademark
of Amazon.com, Inc.

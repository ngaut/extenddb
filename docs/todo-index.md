# TODO Index

Regenerated: v0.0.113 (P112)

## TODO(fidelity)

- `crates/storage-postgres/src/config.rs` — Add an optional PostgreSQL default-read topology if PostgreSQL deployments need a separate physical read path.
- `crates/engine/src/batch_write_item.rs:166` — DynamoDB charges WCU based on old item size for deletes. Backlog classifies this as a minor native-boundary follow-up; implement only through storage-owned write outcomes, not engine pre-reads.
- `crates/engine/src/transact_write_helpers.rs:178` — DynamoDB charges WCU based on old item size for deletes and max(old, new) for updates. Backlog classifies this as a minor native-boundary follow-up; implement only through storage-owned write outcomes, not engine update replay.

## TODO(architecture)

- `crates/storage-postgres/src/stream_engine.rs:367` — Shard list per table requires an extra SQL round-trip.

---

## License

Copyright 2026 ExtendDB contributors. Licensed under the Apache License, Version 2.0.
See [LICENSE](../LICENSE) for the full text.

This software is provided "as is" without warranty of any kind. ExtendDB is not
affiliated with, endorsed by, or sponsored by Amazon Web Services. "DynamoDB" is a trademark
of Amazon.com, Inc.

# Backlog

This file tracks open work only. Completed work lives in Git history, release
notes, and the PR ledger.

## Fidelity

- No archive-blocking TiDB/DynamoDB-client fidelity bugs are currently
  identified. Minor capacity-reporting boundaries are tracked below.

## Test Gaps

- CLI lifecycle tests require `EXTENDDB_TEST_PG_CONNECTION_STRING` and are not
  part of the standard pytest runner.
- Cross-restart metrics coverage is missing: the metrics suite does not yet
  verify that persisted metrics survive a server restart.

## Code Quality

- Split large modules when the next change naturally touches them:
  `validation/mod.rs`, `policy/condition.rs`, `key_condition.rs`,
  `policy/evaluator.rs`, `backup_engine.rs`, `throttle.rs`,
  `update_evaluator.rs`, and `policy/document.rs`.
- Consolidate repetitive HTTP handler boilerplate.
- Consider an expression AST cache if profiling shows repeated parse cost on
  hot request paths.
- Add a benchmark gate for the TiDB-backed customer smoke path.
- Preserve the original request path in the HTTP-to-HTTPS redirect helper.
- Derive the console docs page category order from the docs manifest instead of
  a hardcoded list.
- PostgreSQL stream shard listing still uses an extra SQL round trip per table.

## Feature Backlog

- Ion import parser: `InputFormat::Ion` currently uses the DynamoDB JSON reader.
- PostgreSQL single-frontend-per-catalog enforcement needs an HA-safe worker
  coordination design before shared-catalog multi-frontend deployment.
- PostgreSQL default-read topology can be added if deployments need a separate
  physical read path.
- C/C++ SDK test coverage remains unconfirmed; Python and Rust suites are
  active in this repo, and JVM coverage can be attached through the external
  suite registry.

## Native Backend Boundaries

- Batch/transact delete/update `ConsumedCapacity` reports a key-size lower-bound
  WCU when the old item is not already available in the engine. Keep this out of
  the engine hot path; if customer evidence makes exact reporting critical,
  implement it as a storage-owned write outcome/metering contract.
- ExtendDB does not emulate AWS tag API TPS quotas with a frontend token bucket.
  Prefer documenting the AWS quota difference unless customer evidence justifies
  a storage-owned distributed counter.
- TiDB table-level `RestoreTableToPointInTime` is an explicit native-backend
  boundary. TiDB BR PITR restores into a recovery cluster rather than replaying
  historical rows into a live table.

## Decisions Needed

- License review for Unicode-3.0, CDLA-Permissive-2.0, and MPL-2.0
  dependencies.

---

## License

Copyright 2026 ExtendDB contributors. Licensed under the Apache License,
Version 2.0. See [LICENSE](../LICENSE) for the full text.

This software is provided "as is" without warranty of any kind. ExtendDB is not
affiliated with, endorsed by, or sponsored by Amazon Web Services. "DynamoDB" is
a trademark of Amazon.com, Inc.

# ExtendDB Test Strategy

**Version:** 2.0
**Date:** 2026-06-06
**Status:** Implemented

## 1. Purpose

This document describes the current ExtendDB test architecture: which suites
exist in the repository, which external suites can be registered, and which
acceptance gates prove a change is ready. The test suite treats DynamoDB
compatibility as product behavior, not as an implementation detail.

## 2. Principles

1. **Real DynamoDB remains the oracle.** When expected behavior is uncertain,
   validate against real DynamoDB and then encode the behavior in a clean-room
   ExtendDB test.
2. **One test, two targets.** Integration tests run against ExtendDB when
   `EXTENDDB_TEST_ENDPOINT` is set and against real DynamoDB when it is unset.
3. **No target-specific assertions.** A behavioral difference is either an
   ExtendDB bug, a test bug, or an explicitly documented native-backend
   boundary.
4. **SDKs are part of the contract.** boto3 and the Rust AWS SDK both exercise
   the wire protocol through normal customer clients. Additional external
   suites can be registered without copying them into this repository.
5. **Acceptance evidence is durable.** Long-running gates write artifacts under
   `discussions/` with the current commit hash.

## 3. Repository Test Surfaces

```
tests/
├── *.py                 # Primary boto3 integration suite
├── python/              # Comprehensive clean-room compatibility suite
├── rust/                # Standalone Rust AWS SDK integration crate
├── cli/                 # Shell-level CLI and docs consistency checks
├── shared/              # Shared test notes and reusable data area
├── conftest.py          # Main pytest fixtures
└── management_helpers.py

devtools/
├── run-tests            # Unified suite runner
├── tidb-acceptance      # TiDB-focused acceptance selector/gate
├── tidb-sdk-smoke       # Live customer-path DynamoDB SDK smoke
├── tidb-native-read-smoke
├── run-external-tests   # Registry-based external suite runner
└── provision-test-credentials
```

### 3.1 Rust Unit Tests

`cargo test -j12 --workspace` is the default in-repo correctness gate. These
tests cover pure types, expression parsing/evaluation, operation handlers,
auth, server helpers, storage metadata, and backend SQL generation.

Crate-level tests should stay close to the code they protect. Prefer focused
tests for parser, validator, and query-shape changes, then run the workspace
gate before commit.

### 3.2 Primary Python Integration Suite

Top-level `tests/*.py` files exercise ExtendDB through boto3 and requests.
They cover auth, IAM, cross-account behavior, item/table operations, query/scan,
batch operations, transactions, streams, TTL, metrics, console integration, and
import/export.

The suite is target-selectable:

- Set `EXTENDDB_TEST_ENDPOINT` plus test credentials to run against ExtendDB.
- Leave `EXTENDDB_TEST_ENDPOINT` unset to validate behavior against real
  DynamoDB where the test is intended to be portable.

All tests must clean up resources they create. Fixture-level cleanup is
preferred so failures do not leave tables behind.

### 3.3 Comprehensive Python Suite

`tests/python/` is the broader clean-room compatibility suite. It keeps
operation-focused coverage separate from the main pytest files and is run via:

```bash
devtools/run-tests --extenddb --comprehensive
```

This suite should remain backend-agnostic and use the same endpoint/credential
model as the primary Python suite.

### 3.4 Rust AWS SDK Suite

`tests/rust/` is a standalone Rust SDK integration crate. It is not part of the
workspace build; the runner compiles or runs it when the selected gate includes
Rust SDK coverage.

Use it for SDK serialization, SigV4, and customer-path scenarios that are more
valuable through the real Rust AWS SDK than through internal unit tests.

### 3.5 CLI and Documentation Checks

`tests/cli/` covers shell-level lifecycle and documentation consistency checks.
CLI lifecycle tests run against the TiDB-backed runtime.

Documentation builds are part of the test architecture because manuals and
embedded console docs are generated artifacts. Use:

```bash
.venv/bin/python docs/build-docs.py
```

when the project virtualenv is available.

## 4. External Suite Registry

External suites are referenced by configuration, not copied into the repo.
`external-suites.sample.toml` documents the registry shape; copy it to
`external-suites.toml` and point entries at local or organization-specific test
suites.

Supported runner types are:

- `maven`
- `gradle`
- `pytest`
- `cargo`

Run registered suites with:

```bash
devtools/run-tests --extenddb --external
```

or directly:

```bash
devtools/run-external-tests --verbose
```

The external runner writes structured and text artifacts under `discussions/`.

## 5. Unified Runner

`devtools/run-tests` owns the normal integration workflow. It provisions test
credentials, performs a health check, configures runtime settings for fast
tests, sets import/export paths, prepares JVM trust material when a registered
external suite needs it, and writes per-suite artifacts.

Typical commands:

```bash
devtools/run-tests --extenddb --rust
devtools/run-tests --extenddb --pytest
devtools/run-tests --extenddb --comprehensive
devtools/run-tests --extenddb --rust-integration
devtools/run-tests --extenddb --external
devtools/run-tests --extenddb --all
```

Use `--filter` for focused iteration and `--release` when validating the
release binary path.

## 6. TiDB Acceptance Gate

TiDB is the default backend, so TiDB changes use `devtools/tidb-acceptance`.
The gate maps changed files to the smallest useful check set, while still
offering full and archive modes for final proof.

```bash
devtools/tidb-acceptance --changed --dry-run
devtools/tidb-acceptance --changed
devtools/tidb-acceptance --changed --with-playground
devtools/tidb-acceptance --full
devtools/tidb-acceptance --archive
```

The selector can run shell checks, whitespace checks, TiDB native SQL smoke,
`storage-tidb` tests and clippy, `extenddb --features tidb` tests and clippy,
Rust SDK compile checks, live customer SDK smoke, and docs builds.

Use `--archive` for final branch evidence. It runs the full developer gate plus
the live customer SDK smoke and fails fast when endpoint or credential
prerequisites are missing.

## 7. Adding Coverage

Add the narrowest test that proves the behavior:

- Pure parsing, validation, conversion, and error mapping: Rust unit test in
  the owning crate.
- Operation behavior over the DynamoDB API: top-level pytest.
- Broad compatibility scenario: `tests/python/`.
- SDK serialization or customer-path proof: `tests/rust/` or a registered
  external suite.
- CLI lifecycle behavior: `tests/cli/`.
- TiDB-native behavior: `storage-tidb` unit test plus `tidb-acceptance` when
  the behavior depends on live TiDB.

For bug fixes, encode the failing case directly. Do not add broad conditional
checks where a better data shape can eliminate the edge case.

## 8. Required Gates by Change Type

| Change type | Minimum local gate |
|-------------|--------------------|
| Rust source | `cargo fmt --all -- --check`, focused test, `cargo test -j12 --workspace`, `cargo clippy -j12 --all-targets -- -D warnings` |
| Docs only | `.venv/bin/python docs/build-docs.py`, `git diff --check` |
| TiDB backend | `devtools/tidb-acceptance --changed`; add `--with-playground` or `--full` when live TiDB behavior changed |
| SDK/customer path | `devtools/tidb-acceptance --sdk-smoke` or a registered external suite |
| Final TiDB archive proof | `devtools/tidb-acceptance --archive` |

The acceptance gate may run more than the minimum when the touched files imply
broader risk.

## 9. License

Copyright 2026 ExtendDB contributors. Licensed under the Apache License,
Version 2.0. See [LICENSE](../../LICENSE) for the full text.

This software is provided "as is" without warranty of any kind. ExtendDB is not
affiliated with, endorsed by, or sponsored by Amazon Web Services. "DynamoDB" is
a trademark of Amazon.com, Inc.

# Troubleshooting

> See [NOTICE](NOTICE.md) for important disclaimers.

## TiDB Cannot Be Reached

**Symptoms**

- `extenddb init` or `extenddb serve` fails while opening the storage
  connection.
- `/health` reports storage unavailable.

**Fix**

Verify the TiDB SQL endpoint and credentials:

```bash
mysql -h 127.0.0.1 -P 4000 -uroot -e "SELECT VERSION();"
```

Then check `[storage.tidb].connection_string` in `extenddb.toml` or the
`EXTENDDB__STORAGE__TIDB__CONNECTION_STRING` override.

## Catalog Version Mismatch

**Error**

```text
Catalog version mismatch: expected X, found Y. Run 'extenddb migrate' to update.
```

**Cause**

The catalog was initialized by a different ExtendDB binary than the one now
running.

**Fix**

Back up the deployment, then run:

```bash
extenddb migrate --config extenddb.toml
```

## Catalog Not Initialized

**Error**

```text
Catalog not initialized. Run 'extenddb init' to set up the catalog.
```

**Fix**

Run:

```bash
extenddb init --config extenddb.toml
```

## Database Already Exists

**Error**

```text
Database '<name>' already exists. Run 'extenddb destroy --config <config>' first, then re-run 'extenddb init'.
```

**Cause**

`extenddb init` refuses to overwrite existing catalog or data databases.

**Fix**

If you want a fresh local deployment, destroy first:

```bash
extenddb destroy --config extenddb.toml
extenddb init --config extenddb.toml
```

Use `extenddb migrate` instead when preserving existing data.

## Address Already In Use

**Error**

```text
Failed to bind <addr>: Address already in use
```

**Fix**

Stop the process already using the configured port, or change
`[server].port`/`EXTENDDB__SERVER__PORT`.

## TLS Certificate Errors

**Symptoms**

- Server startup fails loading TLS files.
- AWS CLI/SDK rejects the endpoint certificate.

**Fix**

Ensure the configured certificate and key paths exist and are readable. For the
default self-signed certificate, export:

```bash
export AWS_CA_BUNDLE=~/.extenddb/tls/cert.pem
```

## Authentication Errors

### InvalidSignatureException

The secret key used by the client does not match the stored access key. Create a
new access key if the secret is lost.

### UnrecognizedClientException

The access key ID does not exist. Re-run:

```bash
eval $(python3 devtools/provision-test-credentials)
```

or create credentials through `extenddb manage`.

### AccessDeniedException

The SigV4 identity is valid but IAM policy evaluation denied the action. Check
user, group, role, session policy, permissions boundary, resource ARN, and
condition context.

## Connection Pool Exhaustion

**Symptom**

```text
HTTP 503 on requests under load
```

**Fix**

Increase TiDB pool sizes:

```toml
[storage.tidb]
pool_size = 50
catalog_pool_size = 50
```

Then inspect TiDB sessions, slow queries, DDL jobs, and Resource Control
throttling.

## Table Stays In CREATING Or UPDATING

TiDB owns online DDL scheduling and backfill. Check TiDB DDL jobs for the
physical table. ExtendDB publishes catalog transitions after the required TiDB
state is ready.

## Streams Capture Errors

If a write succeeds but stream capture reports an error, inspect TiDB
connectivity and the shared stream-record table. TiDB uses MVCC commit
timestamps plus a per-transaction ordinal for stream sequence numbers.

## Logs

Linux:

```bash
journalctl -t extenddb -n 100
```

macOS:

```bash
log show --predicate 'processImagePath ENDSWITH "extenddb"' --last 10m
```

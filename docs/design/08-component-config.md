# Configuration Component

## Scope

Configuration is loaded by the `extenddb` binary from TOML, environment
variables, and CLI flags. The supported storage backend is TiDB.

Precedence:

```text
CLI flags > EXTENDDB__ environment variables > config file > defaults
```

## Static Configuration

### `[server]`

| Key | Purpose |
|---|---|
| `bind_addr` | Interface to bind. |
| `port` | HTTPS port. |
| `region` | Region used for ARNs and SigV4 scope. |
| `docs_dir` | Optional rendered documentation directory. |

TLS is mandatory and configured under `[server.tls]`.

### `[storage]`

```toml
[storage]
backend = "tidb"
```

`tidb` is the only supported value.

### `[storage.tidb]`

| Key | Purpose |
|---|---|
| `connection_string` | TiDB MySQL-compatible catalog connection string. |
| `pool_size` | Strong/default-read data pool size. |
| `catalog_pool_size` | Catalog metadata, management, and authz pool size. |
| `resource_group` | Optional TiDB Resource Control group for runtime sessions. |
| `default_read_staleness_seconds` | Optional bounded-staleness window for default reads. |

### `[storage.tidb.backup]`

| Key | Purpose |
|---|---|
| `pd_endpoint` | TiDB PD endpoint used by BR. |
| `storage_uri` | BR storage URI. |
| `send_credentials_to_tikv` | Whether BR sends object-store credentials to TiKV. |

## Environment Variables

Environment variables use `EXTENDDB__` and `__` as separators:

```bash
EXTENDDB__SERVER__PORT=9000
EXTENDDB__STORAGE__TIDB__CONNECTION_STRING="mysql://root@127.0.0.1:4000/extenddb_catalog"
EXTENDDB__AUTH__PROVIDER=builtin
```

## Runtime Settings

Runtime settings live in the catalog and can be changed without restart:

| Setting | Purpose |
|---|---|
| `log_level` | Runtime log verbosity. |
| `sqlx_log_level` | SQL query trace verbosity. |
| `allow_credential_import` | Enables/disables credential import through management APIs. |

TiDB storage behavior is configured through TiDB and `[storage.tidb]`, not
through runtime compatibility toggles.

## Validation

Config validation should reject unsupported backends and invalid TiDB settings
early, before daemonizing. Avoid silently accepting removed or ignored keys.

## Startup Flow

1. Load config file.
2. Apply environment and CLI overrides.
3. Validate server, auth, TLS, and TiDB storage settings.
4. Create TiDB backend stores and runtime hooks.
5. Build server state from trait objects.
6. Start the HTTPS server and background tasks.

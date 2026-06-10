# Getting Started

> See [NOTICE](NOTICE.md) for important disclaimers.

ExtendDB is a DynamoDB-compatible server backed by TiDB. Any AWS SDK or CLI that
can target a custom DynamoDB endpoint can talk to ExtendDB unchanged.

## Prerequisites

- Rust 1.88+
- TiDB 8.5.4+
- Python 3.10+ for integration tests and documentation tooling

Create the Python environment used by tests and docs:

```bash
python3 -m venv ~/venvs/extenddb-venv
source ~/venvs/extenddb-venv/bin/activate
pip install -r requirements.txt
```

## Build

```bash
cargo build -j12 --release
```

The binary is written to `target/release/extenddb`.

## Configure TiDB

`extenddb init` reads TiDB settings from config or environment. The sample file
documents every supported key:

```bash
cp extenddb.sample.toml extenddb.toml
```

The supported storage backend is TiDB:

```toml
[storage]
backend = "tidb"

[storage.tidb]
connection_string = "mysql://root@127.0.0.1:4000/extenddb_catalog"
pool_size = 20
catalog_pool_size = 20
```

Use `EXTENDDB__` environment variables for overrides, for example:

```bash
export EXTENDDB__STORAGE__TIDB__CONNECTION_STRING="mysql://root@127.0.0.1:4000/extenddb_catalog"
```

## Initialize

```bash
./target/release/extenddb init --config extenddb.toml
```

Initialization creates the catalog/data databases, the admin user, and a
self-signed TLS certificate. Save the generated admin password.

## Start

```bash
./target/release/extenddb serve --config extenddb.toml
```

The default endpoint is:

```text
https://127.0.0.1:8000
```

TLS is mandatory. For the self-signed certificate:

```bash
export AWS_CA_BUNDLE=~/.extenddb/tls/cert.pem
```

## Create Test Credentials

For local tests, provision an access key through the helper:

```bash
export EXTENDDB_TEST_ENDPOINT=https://127.0.0.1:8000
export EXTENDDB_ADMIN_USER=admin
export EXTENDDB_ADMIN_PASSWORD=<password-from-init>
eval $(python3 devtools/provision-test-credentials)
```

## First CRUD

```bash
aws dynamodb create-table \
  --endpoint-url "$EXTENDDB_TEST_ENDPOINT" \
  --region us-east-1 \
  --table-name demo \
  --attribute-definitions AttributeName=pk,AttributeType=S \
  --key-schema AttributeName=pk,KeyType=HASH \
  --billing-mode PAY_PER_REQUEST

aws dynamodb put-item \
  --endpoint-url "$EXTENDDB_TEST_ENDPOINT" \
  --region us-east-1 \
  --table-name demo \
  --item '{"pk":{"S":"hello"},"message":{"S":"world"}}'

aws dynamodb get-item \
  --endpoint-url "$EXTENDDB_TEST_ENDPOINT" \
  --region us-east-1 \
  --table-name demo \
  --key '{"pk":{"S":"hello"}}'
```

## Run Tests

```bash
cargo test -j12 --workspace
devtools/run-tests --extenddb --all
```

Run Python-only integration tests with:

```bash
pytest tests/ -v
```

## Runtime Settings

Runtime settings can be changed without restart:

```bash
extenddb settings --config extenddb.toml set log_level debug
extenddb settings --config extenddb.toml list
```

TiDB owns storage-level scheduling and capacity. Use TiDB Resource Control for
distributed capacity governance, TiDB native TTL for expiration, and TiDB BR for
physical backup/restore.

## Lifecycle Commands

```bash
./target/release/extenddb status --config extenddb.toml
./target/release/extenddb verify --config extenddb.toml
./target/release/extenddb stop --config extenddb.toml
```

Destroy is destructive:

```bash
./target/release/extenddb destroy --config extenddb.toml
```

## More Docs

- [Architecture Guide](manuals/01-architecture-guide.md)
- [Admin Guide](manuals/05-admin-guide.md)
- [Troubleshooting](troubleshooting.md)
- [Differences from DynamoDB](differences-from-dynamodb.md)

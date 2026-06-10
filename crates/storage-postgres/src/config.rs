// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! PostgreSQL connection configuration.

use serde::Deserialize;
use std::borrow::Cow;

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PostgresStorageConfig {
    #[serde(default = "default_connection_string")]
    pub connection_string: String,
    #[serde(default = "default_pool_size")]
    pub pool_size: u32,
    /// Maximum connections for the management/catalog pool (authz, IAM, console).
    /// Defaults to `pool_size` if not set.
    #[serde(default)]
    pub catalog_pool_size: Option<u32>,
}

impl Default for PostgresStorageConfig {
    fn default() -> Self {
        Self {
            connection_string: default_connection_string(),
            pool_size: default_pool_size(),
            catalog_pool_size: None,
        }
    }
}

fn default_connection_string() -> String {
    "postgresql://extenddb:extenddb-local-dev@localhost:5432/extenddb_catalog".to_owned()
}

fn default_pool_size() -> u32 {
    20
}

/// Parsed components of a `PostgreSQL` connection string.
pub struct ConnParts {
    pub user: String,
    pub password: String,
    pub host: String,
    pub port: u16,
    pub database: String,
}

fn decode_url_component(value: &str, label: &str) -> anyhow::Result<String> {
    urlencoding::decode(value)
        .map(Cow::into_owned)
        .map_err(|e| anyhow::anyhow!("Invalid percent-encoding in {label}: {e}"))
}

/// Parse host, port, user, password, and database from a `PostgreSQL` connection string.
///
/// Handles the standard `postgresql://user:pass@host:port/db` format.
///
/// # Errors
///
/// Returns an error if the connection string doesn't match the expected format.
pub fn parse_connection_string(conn: &str) -> anyhow::Result<ConnParts> {
    let rest = conn
        .strip_prefix("postgresql://")
        .or_else(|| conn.strip_prefix("postgres://"))
        .ok_or_else(|| {
            anyhow::anyhow!("Connection string must start with postgresql:// or postgres://")
        })?;

    let at = rest
        .rfind('@')
        .ok_or_else(|| anyhow::anyhow!("Connection string missing '@' separator"))?;
    let (userpass, hostdb) = rest.split_at(at);
    let hostdb = &hostdb[1..];

    let (user, password) = userpass
        .split_once(':')
        .map_or((userpass, ""), |(u, p)| (u, p));

    let (hostport, database) = hostdb
        .split_once('/')
        .ok_or_else(|| anyhow::anyhow!("Connection string missing /database"))?;

    let (host, port_str) = hostport
        .rsplit_once(':')
        .ok_or_else(|| anyhow::anyhow!("Connection string missing :port"))?;

    let port: u16 = port_str
        .parse()
        .map_err(|_| anyhow::anyhow!("Invalid port: {port_str}"))?;

    Ok(ConnParts {
        user: decode_url_component(user, "username")?,
        password: decode_url_component(password, "password")?,
        host: decode_url_component(host, "host")?,
        port,
        database: decode_url_component(database, "database")?,
    })
}

// ── StorageConfig trait implementation ────────────────────────────────

impl extenddb_storage::config::StorageConfig for PostgresStorageConfig {
    fn connection_config(&self) -> &str {
        &self.connection_string
    }

    fn max_connections(&self) -> u32 {
        self.pool_size
    }

    fn max_catalog_connections(&self) -> u32 {
        self.catalog_pool_size.unwrap_or(self.pool_size)
    }

    fn clone_box(&self) -> Box<dyn extenddb_storage::config::StorageConfig> {
        Box::new(self.clone())
    }
}

#[cfg(test)]
mod tests {
    use super::parse_connection_string;

    #[test]
    fn parses_percent_encoded_components() {
        let parts = parse_connection_string(
            "postgresql://extend%40db:p%40ss%3Aword@%2Fvar%2Frun%2Fpostgresql:5432/db%2D1",
        )
        .expect("encoded connection string should parse");

        assert_eq!(parts.user, "extend@db");
        assert_eq!(parts.password, "p@ss:word");
        assert_eq!(parts.host, "/var/run/postgresql");
        assert_eq!(parts.port, 5432);
        assert_eq!(parts.database, "db-1");
    }

    #[test]
    fn splits_at_last_userinfo_separator() {
        let parts = parse_connection_string("postgresql://extenddb:raw@pass@localhost:5432/db")
            .expect("raw at sign in password should parse");

        assert_eq!(parts.user, "extenddb");
        assert_eq!(parts.password, "raw@pass");
        assert_eq!(parts.host, "localhost");
        assert_eq!(parts.database, "db");
    }
}

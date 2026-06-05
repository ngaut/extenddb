// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! PostgreSQL schema migration helpers for catalog and data databases.

use extenddb_storage::management_store::{OpError, OpResult};
use sqlx::PgPool;

/// Embedded catalog migration files, applied in order.
pub(crate) const CATALOG_MIGRATIONS: &[(&str, &str)] = &[
    (
        "001_schema.sql",
        include_str!("../../storage-postgres/migrations/001_schema.sql"),
    ),
    (
        "002_drop_continuous_backups.sql",
        include_str!("../../storage-postgres/migrations/002_drop_continuous_backups.sql"),
    ),
    (
        "003_drop_role_permissions_boundary_column.sql",
        include_str!(
            "../../storage-postgres/migrations/003_drop_role_permissions_boundary_column.sql"
        ),
    ),
];

/// Run catalog migrations, skipping already-applied ones.
pub(crate) async fn run_catalog_migrations(pool: &PgPool) -> OpResult<()> {
    println!("--- Running catalog migrations...");
    for (filename, sql) in CATALOG_MIGRATIONS {
        if is_migration_applied(pool, filename).await? {
            println!("    {filename} — already applied, skipping.");
            continue;
        }
        println!("    Applying {filename}...");
        sqlx::raw_sql(sql)
            .execute(pool)
            .await
            .map_err(|e| OpError::Internal(format!("Migration {filename} failed: {e}")))?;
        record_migration(pool, filename).await?;
    }
    println!("    Migrations applied.");
    Ok(())
}

/// Run data database migrations.
pub(crate) async fn run_data_migrations(pool: &PgPool) -> OpResult<()> {
    let sql = include_str!("../../storage-postgres/data_migrations/001_data_schema.sql");

    println!("--- Initializing data database schema...");
    let initialized: bool = sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM information_schema.tables \
         WHERE table_name = 'stream_shards' AND table_schema = 'public')",
    )
    .fetch_one(pool)
    .await
    .map_err(|e| OpError::Internal(format!("Check data schema: {e}")))?;

    if initialized {
        println!("    Data schema already initialized.");
    } else {
        sqlx::raw_sql(sql)
            .execute(pool)
            .await
            .map_err(|e| OpError::Internal(format!("Data migration failed: {e}")))?;
        println!("    Data schema initialized.");
    }
    ensure_stream_reader_leases(pool).await?;
    Ok(())
}

async fn ensure_stream_reader_leases(pool: &PgPool) -> OpResult<()> {
    sqlx::raw_sql(
        r"
        CREATE TABLE IF NOT EXISTS stream_reader_leases (
            shard_id TEXT NOT NULL,
            reader_slot SMALLINT NOT NULL,
            reader_id TEXT NOT NULL,
            expires_at TIMESTAMPTZ NOT NULL,
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            PRIMARY KEY (shard_id, reader_slot),
            UNIQUE (shard_id, reader_id)
        );

        CREATE INDEX IF NOT EXISTS idx_stream_reader_leases_expires
            ON stream_reader_leases (expires_at);
        ",
    )
    .execute(pool)
    .await
    .map_err(|e| OpError::Internal(format!("Ensure stream reader leases: {e}")))?;
    Ok(())
}

/// Check if a table exists in the public schema.
pub(crate) async fn table_exists(pool: &PgPool, name: &str) -> OpResult<bool> {
    let exists: bool = sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM information_schema.tables \
         WHERE table_name = $1 AND table_schema = 'public')",
    )
    .bind(name)
    .fetch_one(pool)
    .await
    .map_err(|e| OpError::Internal(format!("Check table exists: {e}")))?;
    Ok(exists)
}

/// Check if a migration has already been applied.
async fn is_migration_applied(pool: &PgPool, filename: &str) -> OpResult<bool> {
    if table_exists(pool, "schema_history").await? {
        let applied: (bool,) =
            sqlx::query_as("SELECT EXISTS(SELECT 1 FROM schema_history WHERE filename = $1)")
                .bind(filename)
                .fetch_one(pool)
                .await
                .map_err(|e| OpError::Internal(format!("Check migration: {e}")))?;
        return Ok(applied.0);
    }
    Ok(false)
}

/// Record a migration in the `schema_history` table.
async fn record_migration(pool: &PgPool, filename: &str) -> OpResult<()> {
    if !table_exists(pool, "schema_history").await? {
        return Ok(());
    }
    sqlx::query(
        "INSERT INTO schema_history (filename) VALUES ($1) ON CONFLICT (filename) DO NOTHING",
    )
    .bind(filename)
    .execute(pool)
    .await
    .map_err(|e| OpError::Internal(format!("Record migration: {e}")))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::CATALOG_MIGRATIONS;
    use crate::CATALOG_VERSION;

    #[test]
    fn catalog_migration_drops_unsupported_pitr_state() {
        let (filename, sql) = CATALOG_MIGRATIONS
            .iter()
            .find(|(filename, _)| *filename == "002_drop_continuous_backups.sql")
            .expect("PITR state migration");

        assert_eq!(*filename, "002_drop_continuous_backups.sql");
        assert!(sql.contains("DROP TABLE IF EXISTS continuous_backups"));
        assert!(sql.contains("0.0.3"));
    }

    #[test]
    fn latest_catalog_migration_drops_role_permissions_boundary_column() {
        let (filename, sql) = CATALOG_MIGRATIONS.last().expect("latest migration");

        assert_eq!(*filename, "003_drop_role_permissions_boundary_column.sql");
        assert!(
            sql.contains("ALTER TABLE iam_roles DROP COLUMN IF EXISTS permissions_boundary_arn")
        );
        assert!(sql.contains("0.0.4"));
    }

    #[test]
    fn compiled_catalog_version_matches_latest_migration() {
        let (_filename, sql) = CATALOG_MIGRATIONS.last().expect("latest migration");

        assert!(sql.contains(&format!(
            "UPDATE settings SET value = '{}' WHERE key = 'catalog_version'",
            CATALOG_VERSION
        )));
    }

    #[test]
    fn fresh_catalog_schema_omits_unsupported_pitr_state() {
        let (filename, sql) = CATALOG_MIGRATIONS.first().expect("fresh catalog migration");

        assert_eq!(*filename, "001_schema.sql");
        assert!(!sql.contains("CREATE TABLE IF NOT EXISTS continuous_backups"));
        assert!(!sql.contains("permissions_boundary_arn"));
        assert!(sql.contains(&format!(
            "INSERT INTO settings (key, value) VALUES ('catalog_version', '{}')",
            CATALOG_VERSION
        )));
    }
}

// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! `MetadataEngine` trait implementation for `TidbEngine`.

use extenddb_core::types::{Item, Tag, TimeToLiveDescription, TimeToLiveStatus};
use extenddb_storage::MetadataEngine;
use extenddb_storage::error::StorageError;
use futures::future::BoxFuture;

use crate::TidbEngine;
use crate::data;
use crate::worker_store::CONTROL_PLANE_LEASE_SECONDS;

const TTL_EXPIRES_AT_COLUMN: &str = "_edb_ttl_expires_at";
const TTL_EXPIRES_AT_INDEX: &str = "_edb_ttl_expires_at_idx";
const LEGACY_TTL_EPOCH_COLUMN: &str = "_edb_ttl_epoch";
const LEGACY_TTL_EPOCH_INDEX: &str = "_edb_ttl_epoch_idx";

#[derive(sqlx::FromRow)]
struct TtlArtifactRow {
    table_id: String,
    table_status: String,
    ttl_attribute: Option<String>,
    ttl_pending_action: Option<String>,
    ttl_index_ready: bool,
    control_plane_token: Option<String>,
}

fn ttl_json_path(ttl_attribute: &str) -> String {
    format!(
        "$.\"{}\".N",
        ttl_attribute.replace('\\', "\\\\").replace('"', "\\\"")
    )
}

fn sql_string_literal(value: &str) -> String {
    format!("'{}'", value.replace('\\', "\\\\").replace('\'', "''"))
}

fn ttl_json_value_expr(ttl_attribute: &str) -> String {
    let ttl_path = sql_string_literal(&ttl_json_path(ttl_attribute));
    format!("JSON_UNQUOTE(JSON_EXTRACT(item_data, {ttl_path}))")
}

fn ttl_expires_at_expr(ttl_attribute: &str) -> String {
    let ttl_value = ttl_json_value_expr(ttl_attribute);
    format!(
        "CASE \
             WHEN {ttl_value} REGEXP '^[0-9]+$' \
                  AND CAST({ttl_value} AS UNSIGNED) > 0 \
             THEN FROM_UNIXTIME(CAST({ttl_value} AS UNSIGNED)) \
             ELSE NULL \
         END"
    )
}

async fn data_table_has_native_ttl(
    pool: &sqlx::MySqlPool,
    data_table: &str,
) -> Result<bool, StorageError> {
    let (_table_name, create_table): (String, String) =
        sqlx::query_as(&format!("SHOW CREATE TABLE {data_table}"))
            .fetch_one(pool)
            .await
            .map_err(|e| StorageError::Internal(e.to_string()))?;
    let create_table = create_table.to_ascii_uppercase();
    Ok(create_table.contains(" TTL =") || create_table.contains("/*T![TTL] TTL ="))
}

pub(crate) async fn drop_ttl_artifacts(
    pool: &sqlx::MySqlPool,
    table_id: &str,
) -> Result<(), StorageError> {
    let data_table = data::data_table_name(table_id);

    if data_table_has_native_ttl(pool, &data_table).await? {
        let sql = format!("ALTER TABLE {data_table} REMOVE TTL");
        sqlx::query(&sql)
            .execute(pool)
            .await
            .map_err(|e| StorageError::Internal(e.to_string()))?;
    }

    for index_name in [TTL_EXPIRES_AT_INDEX, LEGACY_TTL_EPOCH_INDEX] {
        let sql = format!("DROP INDEX IF EXISTS `{index_name}` ON {data_table}");
        sqlx::query(&sql)
            .execute(pool)
            .await
            .map_err(|e| StorageError::Internal(e.to_string()))?;
    }

    for column_name in [TTL_EXPIRES_AT_COLUMN, LEGACY_TTL_EPOCH_COLUMN] {
        let sql = format!("ALTER TABLE {data_table} DROP COLUMN IF EXISTS `{column_name}`");
        sqlx::query(&sql)
            .execute(pool)
            .await
            .map_err(|e| StorageError::Internal(e.to_string()))?;
    }

    Ok(())
}

async fn add_ttl_generated_column(
    pool: &sqlx::MySqlPool,
    table_id: &str,
    ttl_attribute: &str,
) -> Result<(), StorageError> {
    let data_table = data::data_table_name(table_id);
    let ttl_expr = ttl_expires_at_expr(ttl_attribute);
    let add_column = format!(
        "ALTER TABLE {data_table} ADD COLUMN `{TTL_EXPIRES_AT_COLUMN}` DATETIME \
         AS ({ttl_expr}) VIRTUAL"
    );
    sqlx::query(&add_column)
        .execute(pool)
        .await
        .map_err(|e| StorageError::Internal(e.to_string()))?;

    Ok(())
}

async fn configure_native_ttl(
    pool: &sqlx::MySqlPool,
    table_id: &str,
    ttl_attribute: &str,
) -> Result<(), StorageError> {
    drop_ttl_artifacts(pool, table_id).await?;
    add_ttl_generated_column(pool, table_id, ttl_attribute).await?;

    let data_table = data::data_table_name(table_id);
    let enable_ttl = format!(
        "ALTER TABLE {data_table} \
         TTL = `{TTL_EXPIRES_AT_COLUMN}` + INTERVAL 0 SECOND \
         TTL_JOB_INTERVAL = '1h'"
    );
    sqlx::query(&enable_ttl)
        .execute(pool)
        .await
        .map_err(|e| StorageError::Internal(e.to_string()))?;

    Ok(())
}

async fn configure_ttl_artifacts(
    pool: &sqlx::MySqlPool,
    table_id: &str,
    ttl_attribute: &str,
) -> Result<(), StorageError> {
    configure_native_ttl(pool, table_id, ttl_attribute).await
}

fn lost_ttl_ownership() -> StorageError {
    StorageError::Internal("lost TiDB TTL control-plane lease".to_owned())
}

impl TidbEngine {
    pub(crate) async fn create_ttl_artifacts_owned(
        &self,
        table_id: &str,
        token: &str,
        ttl_attribute: &str,
    ) -> Result<(), StorageError> {
        configure_ttl_artifacts(&self.data_pool, table_id, ttl_attribute).await?;

        let result = sqlx::query(
            "UPDATE tables SET ttl_attribute = ?, ttl_index_ready = TRUE, \
                 ttl_native_enabled = TRUE, ttl_pending_action = NULL \
             WHERE table_id = ? AND table_status = 'UPDATING' \
               AND control_plane_token = ?",
        )
        .bind(ttl_attribute)
        .bind(table_id)
        .bind(token)
        .execute(&self.pool)
        .await
        .map_err(|e| StorageError::Internal(e.to_string()))?;

        if result.rows_affected() == 0 {
            return Err(lost_ttl_ownership());
        }
        Ok(())
    }

    pub(crate) async fn drop_ttl_artifacts_owned(
        &self,
        table_id: &str,
        token: &str,
    ) -> Result<(), StorageError> {
        drop_ttl_artifacts(&self.data_pool, table_id).await?;

        let result = sqlx::query(
            "UPDATE tables SET ttl_attribute = NULL, ttl_index_ready = FALSE, \
                 ttl_native_enabled = FALSE, ttl_pending_action = NULL \
             WHERE table_id = ? AND table_status = 'UPDATING' \
               AND control_plane_token = ?",
        )
        .bind(table_id)
        .bind(token)
        .execute(&self.pool)
        .await
        .map_err(|e| StorageError::Internal(e.to_string()))?;

        if result.rows_affected() == 0 {
            return Err(lost_ttl_ownership());
        }
        Ok(())
    }

    async fn finish_ttl_update_owned(
        &self,
        table_id: &str,
        token: &str,
    ) -> Result<(), StorageError> {
        let result = sqlx::query(
            "UPDATE tables SET table_status = 'ACTIVE', status_transition_at = NULL, \
                 control_plane_token = NULL, control_plane_lease_until = NULL \
             WHERE table_id = ? AND table_status = 'UPDATING' \
               AND control_plane_token = ?",
        )
        .bind(table_id)
        .bind(token)
        .execute(&self.pool)
        .await
        .map_err(|e| StorageError::Internal(e.to_string()))?;

        if result.rows_affected() == 0 {
            return Err(lost_ttl_ownership());
        }
        Ok(())
    }
}

impl MetadataEngine for TidbEngine {
    fn describe_ttl(
        &self,
        account_id: &str,
        table_name: &str,
    ) -> BoxFuture<'_, Result<TimeToLiveDescription, StorageError>> {
        let account_id = account_id.to_string();
        let table_name = table_name.to_string();
        Box::pin(async move {
            let row: Option<(Option<String>,)> = sqlx::query_as(
                "SELECT ttl_attribute FROM tables WHERE account_id = ? AND table_name = ?",
            )
            .bind(&account_id)
            .bind(&table_name)
            .fetch_optional(&self.pool)
            .await
            .map_err(|e| StorageError::Internal(e.to_string()))?;

            let (ttl_attr,) = row.ok_or_else(|| StorageError::TableNotFound(table_name.clone()))?;

            Ok(match ttl_attr {
                Some(attr) => TimeToLiveDescription {
                    time_to_live_status: TimeToLiveStatus::Enabled,
                    attribute_name: Some(attr),
                },
                None => TimeToLiveDescription {
                    time_to_live_status: TimeToLiveStatus::Disabled,
                    attribute_name: None,
                },
            })
        })
    }

    fn update_ttl(
        &self,
        account_id: &str,
        table_name: &str,
        attribute_name: &str,
        enabled: bool,
    ) -> BoxFuture<'_, Result<(), StorageError>> {
        let account_id = account_id.to_string();
        let table_name = table_name.to_string();
        let attribute_name = attribute_name.to_string();
        Box::pin(async move {
            Self::validate_account_id(&account_id)?;
            let mut tx = self
                .pool
                .begin()
                .await
                .map_err(|e| StorageError::Internal(e.to_string()))?;
            let row: Option<(String, String, Option<String>, Option<String>)> = sqlx::query_as(
                "SELECT table_id, table_status, ttl_attribute, ttl_pending_action \
                 FROM tables WHERE account_id = ? AND table_name = ? FOR UPDATE",
            )
            .bind(&account_id)
            .bind(&table_name)
            .fetch_optional(&mut *tx)
            .await
            .map_err(|e| StorageError::Internal(e.to_string()))?;

            let Some((table_id, status, current_ttl_attribute, pending_action)) = row else {
                return Err(StorageError::TableNotFound(table_name));
            };
            if status != "ACTIVE" {
                return Err(StorageError::TableNotActive(table_name));
            }
            if !enabled && current_ttl_attribute.is_none() && pending_action.is_none() {
                tx.commit()
                    .await
                    .map_err(|e| StorageError::Internal(e.to_string()))?;
                return Ok(());
            }
            if self.has_active_data_ddl_job(&table_id).await? {
                return Err(StorageError::TableNotActive(table_name));
            }

            let token = uuid::Uuid::new_v4().to_string();

            if enabled {
                sqlx::query(
                    "UPDATE tables SET ttl_attribute = ?, ttl_pending_action = 'ENABLE', \
                         ttl_index_ready = FALSE, ttl_native_enabled = FALSE, \
                         table_status = 'UPDATING', status_transition_at = CURRENT_TIMESTAMP(6), \
                         control_plane_token = ?, \
                         control_plane_lease_until = DATE_ADD(CURRENT_TIMESTAMP(6), INTERVAL ? SECOND) \
                     WHERE account_id = ? AND table_name = ? AND table_status = 'ACTIVE'",
                )
                .bind(&attribute_name)
                .bind(&token)
                .bind(CONTROL_PLANE_LEASE_SECONDS)
                .bind(&account_id)
                .bind(&table_name)
                .execute(&mut *tx)
                .await
                .map_err(|e| StorageError::Internal(e.to_string()))?;
            } else {
                sqlx::query(
                    "UPDATE tables SET ttl_pending_action = 'DISABLE', \
                         table_status = 'UPDATING', status_transition_at = CURRENT_TIMESTAMP(6), \
                         control_plane_token = ?, \
                         control_plane_lease_until = DATE_ADD(CURRENT_TIMESTAMP(6), INTERVAL ? SECOND) \
                     WHERE account_id = ? AND table_name = ? AND table_status = 'ACTIVE'",
                )
                .bind(&token)
                .bind(CONTROL_PLANE_LEASE_SECONDS)
                .bind(&account_id)
                .bind(&table_name)
                .execute(&mut *tx)
                .await
                .map_err(|e| StorageError::Internal(e.to_string()))?;
            }
            tx.commit()
                .await
                .map_err(|e| StorageError::Internal(e.to_string()))?;

            let _lease_heartbeat = self.start_control_plane_lease_heartbeat(&table_id, &token);
            if enabled {
                self.create_ttl_artifacts_owned(&table_id, &token, &attribute_name)
                    .await?;
            } else {
                self.drop_ttl_artifacts_owned(&table_id, &token).await?;
            }
            self.finish_ttl_update_owned(&table_id, &token).await?;

            Ok(())
        })
    }

    fn tag_resource(&self, arn: &str, tags: &[Tag]) -> BoxFuture<'_, Result<(), StorageError>> {
        let arn = arn.to_string();
        let tags = tags.to_vec();
        Box::pin(async move {
            for tag in &tags {
                sqlx::query(
                    "INSERT INTO tags (resource_arn, tag_key, tag_value) VALUES (?, ?, ?) \
                     ON DUPLICATE KEY UPDATE tag_value = VALUES(tag_value)",
                )
                .bind(&arn)
                .bind(&tag.key)
                .bind(&tag.value)
                .execute(&self.pool)
                .await
                .map_err(|e| StorageError::Internal(e.to_string()))?;
            }
            Ok(())
        })
    }

    fn untag_resource(
        &self,
        arn: &str,
        tag_keys: &[String],
    ) -> BoxFuture<'_, Result<(), StorageError>> {
        let arn = arn.to_string();
        let tag_keys = tag_keys.to_vec();
        Box::pin(async move {
            for key in &tag_keys {
                sqlx::query("DELETE FROM tags WHERE resource_arn = ? AND tag_key = ?")
                    .bind(&arn)
                    .bind(key)
                    .execute(&self.pool)
                    .await
                    .map_err(|e| StorageError::Internal(e.to_string()))?;
            }
            Ok(())
        })
    }

    fn list_tags(&self, arn: &str) -> BoxFuture<'_, Result<Vec<Tag>, StorageError>> {
        let arn = arn.to_string();
        Box::pin(async move {
            let rows: Vec<(String, String)> = sqlx::query_as(
                "SELECT tag_key, tag_value FROM tags WHERE resource_arn = ? ORDER BY tag_key",
            )
            .bind(&arn)
            .fetch_all(&self.pool)
            .await
            .map_err(|e| StorageError::Internal(e.to_string()))?;

            Ok(rows
                .into_iter()
                .map(|(key, value)| Tag { key, value })
                .collect())
        })
    }

    fn tables_with_ttl(
        &self,
        account_id: &str,
    ) -> BoxFuture<'_, Result<Vec<(String, String)>, StorageError>> {
        let account_id = account_id.to_string();
        Box::pin(async move {
            let rows: Vec<(String, String)> = sqlx::query_as(
                "SELECT table_name, ttl_attribute FROM tables \
                 WHERE account_id = ? AND ttl_attribute IS NOT NULL AND table_status = 'ACTIVE'",
            )
            .bind(&account_id)
            .fetch_all(&self.pool)
            .await
            .map_err(|e| StorageError::Internal(e.to_string()))?;

            Ok(rows)
        })
    }

    fn refresh_table_size(
        &self,
        account_id: &str,
        table_name: &str,
    ) -> BoxFuture<'_, Result<(), StorageError>> {
        let account_id = account_id.to_string();
        let table_name = table_name.to_string();
        Box::pin(async move {
            Self::validate_account_id(&account_id)?;
            let (table_id,): (String,) = sqlx::query_as(
                "SELECT table_id FROM tables WHERE account_id = ? AND table_name = ?",
            )
            .bind(&account_id)
            .bind(&table_name)
            .fetch_one(&self.pool)
            .await
            .map_err(|e| StorageError::Internal(e.to_string()))?;

            let data_table = data::data_table_name(&table_id);
            let raw_table = data_table.trim_matches('`');
            let (item_count, table_size): (i64, i64) = sqlx::query_as(
                "SELECT COALESCE(TABLE_ROWS, 0), COALESCE(DATA_LENGTH, 0) \
                 FROM information_schema.tables \
                 WHERE table_schema = DATABASE() AND table_name = ?",
            )
            .bind(raw_table)
            .fetch_optional(&self.data_pool)
            .await
            .map_err(|e| StorageError::Internal(e.to_string()))?
            .unwrap_or((0, 0));

            sqlx::query(
                "UPDATE tables SET item_count = ?, table_size_bytes = ? \
                 WHERE account_id = ? AND table_name = ? AND table_status = 'ACTIVE'",
            )
            .bind(item_count)
            .bind(table_size)
            .bind(&account_id)
            .bind(&table_name)
            .execute(&self.pool)
            .await
            .map_err(|e| StorageError::Internal(e.to_string()))?;

            Ok(())
        })
    }

    fn list_active_table_names(
        &self,
        account_id: &str,
    ) -> BoxFuture<'_, Result<Vec<String>, StorageError>> {
        let account_id = account_id.to_string();
        Box::pin(async move {
            let rows: Vec<(String,)> = sqlx::query_as(
                "SELECT table_name FROM tables WHERE account_id = ? AND table_status = 'ACTIVE' ORDER BY table_name",
            )
            .bind(&account_id)
            .fetch_all(&self.pool)
            .await
            .map_err(|e| StorageError::Internal(e.to_string()))?;

            Ok(rows.into_iter().map(|(n,)| n).collect())
        })
    }

    fn all_tables_with_ttl(
        &self,
    ) -> BoxFuture<'_, Result<Vec<(String, String, String)>, StorageError>> {
        Box::pin(async move {
            let rows: Vec<(String, String, String)> = sqlx::query_as(
                "SELECT account_id, table_name, ttl_attribute FROM tables \
                 WHERE ttl_attribute IS NOT NULL AND table_status = 'ACTIVE'",
            )
            .fetch_all(&self.pool)
            .await
            .map_err(|e| StorageError::Internal(e.to_string()))?;

            Ok(rows)
        })
    }

    fn all_tables_with_ttl_index_ready(
        &self,
    ) -> BoxFuture<'_, Result<Vec<(String, String, String)>, StorageError>> {
        Box::pin(async move {
            let rows: Vec<(String, String, String)> = sqlx::query_as(
                "SELECT account_id, table_name, ttl_attribute FROM tables \
                 WHERE ttl_attribute IS NOT NULL AND ttl_index_ready = TRUE AND table_status = 'ACTIVE'",
            )
            .fetch_all(&self.pool)
            .await
            .map_err(|e| StorageError::Internal(e.to_string()))?;

            Ok(rows)
        })
    }

    fn create_ttl_index(
        &self,
        account_id: &str,
        table_name: &str,
        ttl_attribute: &str,
    ) -> BoxFuture<'_, Result<(), StorageError>> {
        let account_id = account_id.to_string();
        let table_name = table_name.to_string();
        let ttl_attribute = ttl_attribute.to_string();
        Box::pin(async move {
            Self::validate_account_id(&account_id)?;
            let row: Option<TtlArtifactRow> = sqlx::query_as(
                "SELECT table_id, table_status, ttl_attribute, ttl_pending_action, \
                    ttl_index_ready, control_plane_token \
                 FROM tables WHERE account_id = ? AND table_name = ?",
            )
            .bind(&account_id)
            .bind(&table_name)
            .fetch_optional(&self.pool)
            .await
            .map_err(|e| StorageError::Internal(e.to_string()))?;

            let row = row.ok_or_else(|| StorageError::TableNotFound(table_name.clone()))?;
            if row.ttl_index_ready
                && row.ttl_attribute.as_deref() == Some(ttl_attribute.as_str())
                && row.ttl_pending_action.is_none()
            {
                return Ok(());
            }
            if row.table_status != "ACTIVE" || row.control_plane_token.is_some() {
                return Err(StorageError::TableNotActive(table_name));
            }
            if self.has_active_data_ddl_job(&row.table_id).await? {
                return Err(StorageError::TableNotActive(table_name));
            }

            let token = uuid::Uuid::new_v4().to_string();
            let result = sqlx::query(
                "UPDATE tables SET ttl_attribute = ?, ttl_pending_action = 'ENABLE', \
                     ttl_index_ready = FALSE, ttl_native_enabled = FALSE, \
                     table_status = 'UPDATING', status_transition_at = CURRENT_TIMESTAMP(6), \
                     control_plane_token = ?, \
                     control_plane_lease_until = DATE_ADD(CURRENT_TIMESTAMP(6), INTERVAL ? SECOND) \
                 WHERE table_id = ? AND table_status = 'ACTIVE' \
                   AND control_plane_token IS NULL",
            )
            .bind(&ttl_attribute)
            .bind(&token)
            .bind(CONTROL_PLANE_LEASE_SECONDS)
            .bind(&row.table_id)
            .execute(&self.pool)
            .await
            .map_err(|e| StorageError::Internal(e.to_string()))?;

            if result.rows_affected() == 0 {
                return Err(StorageError::TableNotActive(table_name));
            }

            let _lease_heartbeat = self.start_control_plane_lease_heartbeat(&row.table_id, &token);
            self.create_ttl_artifacts_owned(&row.table_id, &token, &ttl_attribute)
                .await?;
            self.finish_ttl_update_owned(&row.table_id, &token).await?;

            Ok(())
        })
    }

    fn drop_ttl_index(
        &self,
        account_id: &str,
        table_name: &str,
    ) -> BoxFuture<'_, Result<(), StorageError>> {
        let account_id = account_id.to_string();
        let table_name = table_name.to_string();
        Box::pin(async move {
            Self::validate_account_id(&account_id)?;
            let mut tx = self
                .pool
                .begin()
                .await
                .map_err(|e| StorageError::Internal(e.to_string()))?;
            let row: Option<(String, String, Option<String>)> = sqlx::query_as(
                "SELECT table_id, table_status, ttl_attribute \
                 FROM tables WHERE account_id = ? AND table_name = ? FOR UPDATE",
            )
            .bind(&account_id)
            .bind(&table_name)
            .fetch_optional(&mut *tx)
            .await
            .map_err(|e| StorageError::Internal(e.to_string()))?;

            let (table_id, status, ttl_attribute) =
                row.ok_or_else(|| StorageError::TableNotFound(table_name.clone()))?;
            if ttl_attribute.is_none() {
                tx.commit()
                    .await
                    .map_err(|e| StorageError::Internal(e.to_string()))?;
                return Ok(());
            }
            if status != "ACTIVE" {
                return Err(StorageError::TableNotActive(table_name));
            }
            if self.has_active_data_ddl_job(&table_id).await? {
                return Err(StorageError::TableNotActive(table_name));
            }

            let token = uuid::Uuid::new_v4().to_string();
            sqlx::query(
                "UPDATE tables SET ttl_pending_action = 'DISABLE', \
                     table_status = 'UPDATING', status_transition_at = CURRENT_TIMESTAMP(6), \
                     control_plane_token = ?, \
                     control_plane_lease_until = DATE_ADD(CURRENT_TIMESTAMP(6), INTERVAL ? SECOND) \
                 WHERE account_id = ? AND table_name = ? AND table_status = 'ACTIVE'",
            )
            .bind(&token)
            .bind(CONTROL_PLANE_LEASE_SECONDS)
            .bind(&account_id)
            .bind(&table_name)
            .execute(&mut *tx)
            .await
            .map_err(|e| StorageError::Internal(e.to_string()))?;
            tx.commit()
                .await
                .map_err(|e| StorageError::Internal(e.to_string()))?;

            let _lease_heartbeat = self.start_control_plane_lease_heartbeat(&table_id, &token);
            self.drop_ttl_artifacts_owned(&table_id, &token).await?;
            self.finish_ttl_update_owned(&table_id, &token).await?;

            Ok(())
        })
    }

    fn find_expired_items_indexed(
        &self,
        account_id: &str,
        table_name: &str,
        _ttl_attribute: &str,
        limit: usize,
    ) -> BoxFuture<'_, Result<Vec<Item>, StorageError>> {
        let account_id = account_id.to_string();
        let table_name = table_name.to_string();
        Box::pin(async move {
            Self::validate_account_id(&account_id)?;
            let (table_id,): (String,) = sqlx::query_as(
                "SELECT table_id FROM tables WHERE account_id = ? AND table_name = ?",
            )
            .bind(account_id)
            .bind(table_name)
            .fetch_one(&self.pool)
            .await
            .map_err(|e| StorageError::Internal(e.to_string()))?;

            let data_table = data::data_table_name(&table_id);

            let now_epoch = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs();

            let limit_i64 = i64::try_from(limit).unwrap_or(i64::MAX);
            let now_i64 = i64::try_from(now_epoch).unwrap_or(i64::MAX);
            let sql = format!(
                "SELECT item_data FROM {data_table} \
                 WHERE `{TTL_EXPIRES_AT_COLUMN}` IS NOT NULL \
                   AND `{TTL_EXPIRES_AT_COLUMN}` <= FROM_UNIXTIME(?) \
                 ORDER BY `{TTL_EXPIRES_AT_COLUMN}` \
                 LIMIT ?"
            );
            let rows: Vec<(serde_json::Value,)> = sqlx::query_as(&sql)
                .bind(now_i64)
                .bind(limit_i64)
                .fetch_all(&self.data_pool)
                .await
                .map_err(|e| StorageError::Internal(e.to_string()))?;

            rows.into_iter().map(|(v,)| data::json_to_item(v)).collect()
        })
    }

    fn all_active_tables(&self) -> BoxFuture<'_, Result<Vec<(String, String)>, StorageError>> {
        Box::pin(async move {
            let rows: Vec<(String, String)> = sqlx::query_as(
                "SELECT account_id, table_name FROM tables \
                 WHERE table_status = 'ACTIVE' ORDER BY account_id, table_name",
            )
            .fetch_all(&self.pool)
            .await
            .map_err(|e| StorageError::Internal(e.to_string()))?;

            Ok(rows)
        })
    }
}

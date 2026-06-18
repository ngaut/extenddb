// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! `WorkerStore` trait implementation and storage maintenance processing.

use std::sync::Arc;

use futures::{StreamExt, future::BoxFuture, stream};

use extenddb_core::types::{
    AttributeDefinition, Item, KeySchemaElement, StreamSpecification, TableKeyInfo, UserIdentity,
};
use extenddb_storage::error::StorageError;
use extenddb_storage::{StreamCapture, WorkerStore};

use crate::TidbEngine;
use crate::data::item_collections::apply_lsi_item_collection_delta_in_tx;
use crate::data::tx_helpers::{
    StreamSequenceAllocator, delete_item_without_old_item_in_tx,
    finalize_stream_records_best_effort, write_stream_record_in_tx,
};
use crate::data::{data_table_name, json_to_item, physical_data_table_name, physical_pk_bytes};
use crate::tidb_util::{
    defer_if_table_has_active_ddl_job, is_table_not_found_tidb_storage_error,
    retry_tidb_idempotent_operation,
};

type CreatingTableRow = (
    String,
    serde_json::Value,
    serde_json::Value,
    Option<serde_json::Value>,
);
type CreateIndexRow = (String, String, serde_json::Value);
type UpdatingTableRow = (
    String,
    serde_json::Value,
    Option<serde_json::Value>,
    Option<String>,
    String,
);
type PendingIndexRow = (String, String, String, serde_json::Value);

const TTL_EXPIRES_AT_COLUMN: &str = "_edb_ttl_expires_at";
const TTL_EXPIRES_AT_INDEX: &str = "_edb_ttl_expires_at_idx";
const TTL_EXPIRY_TABLE_SCAN_LIMIT: i64 = 1_024;
const CONTROL_PLANE_TRANSITION_CONCURRENCY: usize = 16;
const CONTROL_PLANE_TRANSITION_SCAN_LIMIT: i64 = 256;
const CONTROL_PLANE_TRANSITION_CANDIDATES_SQL: &str = r"SELECT table_status, table_name, table_id, table_arn
               FROM tables
              WHERE table_status IN ('CREATING', 'UPDATING', 'DELETING')
                AND status_transition_at <= CURRENT_TIMESTAMP(6)
              ORDER BY status_transition_at, table_name
              LIMIT ?";

struct CreateReconcilePlan {
    table_name: String,
    key_schema: Vec<KeySchemaElement>,
    attr_defs: Vec<AttributeDefinition>,
    stream_enabled: bool,
    has_lsi: bool,
    indexes: Vec<(String, Vec<KeySchemaElement>)>,
}

struct UpdateReconcilePlan {
    table_name: String,
    base_attr_defs: Vec<AttributeDefinition>,
    stream_enabled: bool,
    ttl_attribute: Option<String>,
    ttl_status: String,
    pending_indexes: Vec<PendingIndexPlan>,
}

struct PendingIndexPlan {
    index_id: String,
    index_name: String,
    index_status: String,
    key_schema: Vec<KeySchemaElement>,
}

#[derive(Clone)]
struct DeleteReconcilePlan {
    table_name: String,
    table_arn: String,
    table_id: String,
}

struct ControlPlaneTransitionRow {
    table_status: String,
    table_name: String,
    table_id: String,
    table_arn: String,
}

impl<'r> sqlx::FromRow<'r, sqlx::mysql::MySqlRow> for ControlPlaneTransitionRow {
    fn from_row(row: &'r sqlx::mysql::MySqlRow) -> Result<Self, sqlx::Error> {
        Ok(Self {
            table_status: sqlx::Row::try_get(row, "table_status")?,
            table_name: sqlx::Row::try_get(row, "table_name")?,
            table_id: sqlx::Row::try_get(row, "table_id")?,
            table_arn: sqlx::Row::try_get(row, "table_arn")?,
        })
    }
}

struct TtlTableCandidate {
    account_id: String,
    table_name: String,
    table_id: String,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub(crate) struct TtlExpiryStats {
    pub(crate) expired_items: usize,
    pub(crate) candidate_tables: usize,
    pub(crate) scanned_tables: usize,
    pub(crate) oldest_expired_age_seconds: Option<i64>,
    pub(crate) next_table_scan_cursor: Option<String>,
}

impl TtlExpiryStats {
    fn merge_table(&mut self, table: TtlTableExpiryStats) {
        self.expired_items = self.expired_items.saturating_add(table.expired_items);
        self.oldest_expired_age_seconds = max_optional_i64(
            self.oldest_expired_age_seconds,
            table.oldest_expired_age_seconds,
        );
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
struct TtlTableExpiryStats {
    expired_items: usize,
    oldest_expired_age_seconds: Option<i64>,
}

struct ExpiredTtlItem {
    item: Item,
    expired_age_seconds: i64,
}

impl<'r> sqlx::FromRow<'r, sqlx::mysql::MySqlRow> for TtlTableCandidate {
    fn from_row(row: &'r sqlx::mysql::MySqlRow) -> Result<Self, sqlx::Error> {
        Ok(Self {
            account_id: sqlx::Row::try_get(row, "account_id")?,
            table_name: sqlx::Row::try_get(row, "table_name")?,
            table_id: sqlx::Row::try_get(row, "table_id")?,
        })
    }
}

#[derive(Clone)]
enum ControlPlaneReconcilePlan {
    Create { table_id: String },
    Update { table_id: String },
    Delete(DeleteReconcilePlan),
}

impl ControlPlaneReconcilePlan {
    fn from_row(row: ControlPlaneTransitionRow) -> Result<Self, StorageError> {
        match row.table_status.as_str() {
            "CREATING" => Ok(Self::Create {
                table_id: row.table_id,
            }),
            "UPDATING" => Ok(Self::Update {
                table_id: row.table_id,
            }),
            "DELETING" => Ok(Self::Delete(DeleteReconcilePlan {
                table_name: row.table_name,
                table_arn: row.table_arn,
                table_id: row.table_id,
            })),
            other => Err(StorageError::Internal(format!(
                "unknown TiDB control-plane table status: {other}"
            ))),
        }
    }
}

fn parse_json<T: serde::de::DeserializeOwned>(
    value: serde_json::Value,
    label: &str,
) -> Result<T, StorageError> {
    serde_json::from_value(value)
        .map_err(|e| StorageError::Internal(format!("invalid {label}: {e}")))
}

fn index_id_placeholders(count: usize) -> String {
    std::iter::repeat_n("?", count)
        .collect::<Vec<_>>()
        .join(", ")
}

fn mark_creating_indexes_active_sql(count: usize) -> String {
    format!(
        "UPDATE indexes SET index_status = 'ACTIVE' \
         WHERE table_id = ? AND index_status = 'CREATING' \
           AND index_id IN ({}) \
           AND EXISTS ( \
               SELECT 1 FROM tables \
               WHERE tables.table_id = indexes.table_id \
                 AND tables.table_status = 'UPDATING' \
           )",
        index_id_placeholders(count)
    )
}

fn delete_pending_indexes_sql(count: usize) -> String {
    format!(
        "DELETE FROM indexes \
         WHERE table_id = ? AND index_status = 'DELETING' \
           AND index_id IN ({}) \
           AND EXISTS ( \
               SELECT 1 FROM tables \
               WHERE tables.table_id = indexes.table_id \
                 AND tables.table_status = 'UPDATING' \
           )",
        index_id_placeholders(count)
    )
}

impl WorkerStore for TidbEngine {
    fn process_control_plane_transitions(
        &self,
    ) -> BoxFuture<'_, Result<Vec<(String, &'static str)>, StorageError>> {
        Box::pin(async move { Self::process_control_plane_transitions(self).await })
    }

    fn expire_ttl_items(&self, limit: i64) -> BoxFuture<'_, Result<usize, StorageError>> {
        Box::pin(async move { Self::expire_ttl_items(self, limit).await })
    }
}

impl TidbEngine {
    pub(crate) async fn expire_ttl_items(&self, limit: i64) -> Result<usize, StorageError> {
        Ok(self
            .expire_ttl_items_with_options(limit, TTL_EXPIRY_TABLE_SCAN_LIMIT, None)
            .await?
            .expired_items)
    }

    pub(crate) async fn expire_ttl_items_with_options(
        &self,
        limit: i64,
        table_scan_limit: i64,
        start_after_table_id: Option<&str>,
    ) -> Result<TtlExpiryStats, StorageError> {
        let mut remaining = usize::try_from(limit.max(0)).unwrap_or(usize::MAX);
        let mut stats = TtlExpiryStats::default();
        let table_scan_limit = table_scan_limit.max(0);
        if remaining == 0 || table_scan_limit == 0 {
            return Ok(stats);
        }

        let candidates = self
            .ttl_table_candidates(table_scan_limit, start_after_table_id)
            .await?;
        stats.candidate_tables = candidates.len();

        for candidate in candidates {
            if remaining == 0 {
                break;
            }
            stats.scanned_tables = stats.scanned_tables.saturating_add(1);
            stats.next_table_scan_cursor = Some(candidate.table_id.clone());

            let key_info = match self
                .fetch_table_key_info(&candidate.account_id, &candidate.table_name)
                .await
            {
                Ok(info) => info,
                Err(StorageError::TableNotFound(_) | StorageError::TableNotActive(_)) => {
                    continue;
                }
                Err(error) => return Err(error),
            };

            let batch_limit = i64::try_from(remaining).unwrap_or(i64::MAX);
            let table_stats = match self
                .expire_ttl_items_for_table(&key_info, batch_limit)
                .await
            {
                Ok(stats) => stats,
                Err(error) if is_table_not_found_tidb_storage_error(&error) => {
                    if self.table_is_deleting_or_absent(&key_info.table_id).await? {
                        TtlTableExpiryStats::default()
                    } else {
                        return Err(error);
                    }
                }
                Err(error) => return Err(error),
            };
            remaining = remaining.saturating_sub(table_stats.expired_items);
            stats.merge_table(table_stats);
        }

        Ok(stats)
    }

    async fn expire_ttl_items_for_table(
        &self,
        key_info: &TableKeyInfo,
        limit: i64,
    ) -> Result<TtlTableExpiryStats, StorageError> {
        if limit <= 0 {
            return Ok(TtlTableExpiryStats::default());
        }

        let mut tx = self
            .data_pool
            .begin()
            .await
            .map_err(|e| StorageError::Internal(e.to_string()))?;

        let items = lock_expired_ttl_items(&mut tx, key_info, limit).await?;
        if items.is_empty() {
            tx.commit()
                .await
                .map_err(|e| StorageError::Internal(e.to_string()))?;
            return Ok(TtlTableExpiryStats::default());
        }

        let stream = self.ttl_stream_capture(key_info);
        let mut sequence_allocator = StreamSequenceAllocator::default();
        let oldest_expired_age_seconds = items
            .iter()
            .map(|expired| expired.expired_age_seconds)
            .max();
        for expired in &items {
            let item = &expired.item;
            let pk = physical_pk_bytes(item, &key_info.key_schema)?;
            apply_lsi_item_collection_delta_in_tx(
                &mut tx,
                key_info,
                &pk,
                Some(item),
                None,
                self.limits.max_lsi_item_collection_size_bytes,
            )
            .await?;

            let removed = delete_item_without_old_item_in_tx(&mut tx, key_info, item).await?;
            if !removed {
                return Err(StorageError::Internal(format!(
                    "locked TTL row for table {} was not deleted",
                    key_info.table_name
                )));
            }

            if let Some(capture) = stream.as_ref() {
                write_stream_record_in_tx(
                    &mut tx,
                    &mut sequence_allocator,
                    key_info,
                    capture,
                    Some(item),
                    None,
                )
                .await?;
            }
        }

        tx.commit()
            .await
            .map_err(|e| StorageError::Internal(e.to_string()))?;
        finalize_stream_records_best_effort(
            &self.data_pool,
            "ttl_expiry",
            sequence_allocator.pending_records(),
        )
        .await;
        Ok(TtlTableExpiryStats {
            expired_items: items.len(),
            oldest_expired_age_seconds,
        })
    }

    fn ttl_stream_capture(&self, key_info: &TableKeyInfo) -> Option<StreamCapture> {
        let spec = key_info.stream_specification.as_ref()?;
        if !spec.stream_enabled {
            return None;
        }
        Some(StreamCapture {
            view_type: spec.stream_view_type?,
            user_identity: Some(UserIdentity {
                identity_type: "Service".to_owned(),
                principal_id: "dynamodb.amazonaws.com".to_owned(),
            }),
            region: Arc::from(self.region.as_str()),
        })
    }

    async fn drop_table_data_artifacts(&self, table_id: &str) -> Result<(), StorageError> {
        // `stream_records` is a shared TiDB TTL table keyed by stream
        // generation shard ids. Deleting a table must not issue a large
        // foreground delete over stream history; native TTL owns retention, and
        // stream_generations keeps disabled/deleted streams readable for the
        // DynamoDB Streams retention window.
        Self::drop_data_table(&self.data_pool, table_id).await?;
        Ok(())
    }

    async fn table_has_active_native_ddl_job(
        &self,
        table_id: &str,
        operation: &'static str,
    ) -> Result<bool, StorageError> {
        let physical_table = physical_data_table_name(table_id);
        defer_if_table_has_active_ddl_job(&self.data_pool, operation, &physical_table).await
    }

    async fn drop_create_artifacts_if_table_was_deleted(
        &self,
        table_id: &str,
    ) -> Result<(), StorageError> {
        let status: Option<String> =
            sqlx::query_scalar("SELECT table_status FROM tables WHERE table_id = ?")
                .bind(table_id)
                .fetch_optional(&self.pool)
                .await
                .map_err(|e| StorageError::Internal(e.to_string()))?;

        if matches!(status.as_deref(), None | Some("DELETING")) {
            self.drop_table_data_artifacts(table_id).await?;
        }

        Ok(())
    }

    async fn ensure_stream_label_for_table_id(&self, table_id: &str) -> Result<(), StorageError> {
        let label = Self::new_stream_label();
        sqlx::query(
            "UPDATE tables SET stream_label = COALESCE(stream_label, ?) \
             WHERE table_id = ? AND table_status IN ('CREATING', 'UPDATING', 'ACTIVE')",
        )
        .bind(&label)
        .bind(table_id)
        .execute(&self.pool)
        .await
        .map_err(|e| StorageError::Internal(e.to_string()))?;

        sqlx::query(
            "INSERT INTO stream_generations \
             (account_id, table_name, table_id, stream_label, key_schema, \
              stream_specification, stream_status, disabled_at, expires_at) \
             SELECT account_id, table_name, table_id, stream_label, key_schema, \
                    stream_specification, 'ENABLED', NULL, NULL \
             FROM tables \
             WHERE table_id = ? \
               AND stream_label IS NOT NULL \
               AND JSON_UNQUOTE(JSON_EXTRACT(stream_specification, '$.StreamEnabled')) = 'true' \
             ON DUPLICATE KEY UPDATE \
              table_id = VALUES(table_id), \
              key_schema = VALUES(key_schema), \
              stream_specification = VALUES(stream_specification), \
              stream_status = 'ENABLED', \
              disabled_at = NULL, \
              expires_at = NULL",
        )
        .bind(table_id)
        .execute(&self.pool)
        .await
        .map_err(|e| StorageError::Internal(e.to_string()))?;

        Ok(())
    }

    /// Reconcile a CREATING table by replaying the durable catalog row as
    /// idempotent TiDB online DDL, then publishing ACTIVE when the catalog row
    /// still represents the same pending transition.
    pub(crate) async fn reconcile_table_create(
        &self,
        table_id: &str,
    ) -> Result<Option<String>, StorageError> {
        let row: Option<CreatingTableRow> = sqlx::query_as(
            "SELECT table_name, key_schema, attribute_definitions, stream_specification \
             FROM tables \
             WHERE table_id = ? AND table_status = 'CREATING' \
               AND status_transition_at <= CURRENT_TIMESTAMP(6)",
        )
        .bind(table_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| StorageError::Internal(e.to_string()))?;

        let Some((table_name, key_schema_json, attr_defs_json, stream_json)) = row else {
            return Ok(None);
        };

        let key_schema: Vec<KeySchemaElement> = parse_json(key_schema_json, "table key schema")?;
        let attr_defs: Vec<AttributeDefinition> =
            parse_json(attr_defs_json, "table attribute definitions")?;
        let stream_spec: Option<StreamSpecification> = stream_json
            .map(|v| parse_json(v, "stream specification"))
            .transpose()?;
        let stream_enabled = stream_spec.as_ref().is_some_and(|spec| spec.stream_enabled);

        let index_rows: Vec<CreateIndexRow> = sqlx::query_as(
            "SELECT index_id, index_type, key_schema FROM indexes WHERE table_id = ?",
        )
        .bind(table_id)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| StorageError::Internal(e.to_string()))?;
        let has_lsi = index_rows
            .iter()
            .any(|(_, index_type, _)| index_type == "LSI");
        let indexes = index_rows
            .into_iter()
            .map(|(index_id, _, index_key_schema_json)| {
                parse_json(index_key_schema_json, "index key schema")
                    .map(|index_key_schema| (index_id, index_key_schema))
            })
            .collect::<Result<Vec<_>, _>>()?;

        let plan = CreateReconcilePlan {
            table_name,
            key_schema,
            attr_defs,
            stream_enabled,
            has_lsi,
            indexes,
        };

        if self
            .table_has_active_native_ddl_job(table_id, "reconcile_table_create")
            .await?
        {
            return Ok(None);
        }

        let indexes = plan
            .indexes
            .iter()
            .map(|(index_id, key_schema)| (index_id.as_str(), key_schema.as_slice()))
            .collect::<Vec<_>>();
        Self::create_data_table(
            &self.data_pool,
            table_id,
            &plan.key_schema,
            &plan.attr_defs,
            &indexes,
            plan.has_lsi,
        )
        .await?;

        if plan.stream_enabled {
            self.ensure_stream_label_for_table_id(table_id).await?;
        }

        let result = sqlx::query(
            "UPDATE tables \
             SET table_status = 'ACTIVE', status_transition_at = NULL \
             WHERE table_id = ? AND table_status = 'CREATING' \
               AND status_transition_at <= CURRENT_TIMESTAMP(6)",
        )
        .bind(table_id)
        .execute(&self.pool)
        .await
        .map_err(|e| StorageError::Internal(e.to_string()))?;

        if result.rows_affected() > 0 {
            Ok(Some(plan.table_name))
        } else {
            self.drop_create_artifacts_if_table_was_deleted(table_id)
                .await?;
            Ok(None)
        }
    }

    /// Reconcile an UPDATING table. Pending GSI creates/deletes are retried
    /// from catalog metadata until complete. TiDB stream shards are fixed and
    /// derived from the table id, so stream metadata does not require data-side
    /// shard rows.
    async fn reconcile_table_update(&self, table_id: &str) -> Result<Option<String>, StorageError> {
        let row: Option<UpdatingTableRow> = sqlx::query_as(
            "SELECT table_name, attribute_definitions, \
                    stream_specification, ttl_attribute, ttl_status \
             FROM tables \
             WHERE table_id = ? AND table_status = 'UPDATING'",
        )
        .bind(table_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| StorageError::Internal(e.to_string()))?;

        let Some((table_name, attr_defs_json, stream_json, ttl_attribute, ttl_status)) = row else {
            return Ok(None);
        };

        let base_attr_defs: Vec<AttributeDefinition> =
            parse_json(attr_defs_json, "table attribute definitions")?;
        let stream_spec: Option<StreamSpecification> = stream_json
            .map(|v| parse_json(v, "stream specification"))
            .transpose()?;
        let stream_enabled = stream_spec.as_ref().is_some_and(|spec| spec.stream_enabled);

        let pending_indexes: Vec<PendingIndexRow> = sqlx::query_as(
            "SELECT index_id, index_name, index_status, key_schema \
             FROM indexes \
             WHERE table_id = ? AND index_type = 'GSI' \
               AND index_status IN ('CREATING', 'DELETING') \
             ORDER BY index_name",
        )
        .bind(table_id)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| StorageError::Internal(e.to_string()))?;

        let pending_indexes = pending_indexes
            .into_iter()
            .map(|(index_id, index_name, index_status, key_schema_json)| {
                let key_schema = parse_json(key_schema_json, "index key schema")?;
                Ok(PendingIndexPlan {
                    index_id,
                    index_name,
                    index_status,
                    key_schema,
                })
            })
            .collect::<Result<Vec<_>, StorageError>>()?;

        let plan = UpdateReconcilePlan {
            table_name,
            base_attr_defs,
            stream_enabled,
            ttl_attribute,
            ttl_status,
            pending_indexes,
        };

        if self
            .table_has_active_native_ddl_job(table_id, "reconcile_table_update")
            .await?
        {
            return Ok(None);
        }

        if plan.stream_enabled {
            self.ensure_stream_label_for_table_id(table_id).await?;
        }

        self.reconcile_user_ttl_transition(
            table_id,
            plan.ttl_attribute.as_deref(),
            &plan.ttl_status,
        )
        .await?;

        self.reconcile_pending_indexes(table_id, plan.pending_indexes, &plan.base_attr_defs)
            .await?;

        let result = sqlx::query(
            "UPDATE tables \
             SET table_status = 'ACTIVE', status_transition_at = NULL \
             WHERE table_id = ? AND table_status = 'UPDATING' \
               AND ttl_status NOT IN ('ENABLING', 'DISABLING') \
               AND NOT EXISTS ( \
                   SELECT 1 FROM indexes \
                   WHERE indexes.table_id = tables.table_id \
                     AND index_status IN ('CREATING', 'DELETING') \
               )",
        )
        .bind(table_id)
        .execute(&self.pool)
        .await
        .map_err(|e| StorageError::Internal(e.to_string()))?;

        if result.rows_affected() > 0 {
            Ok(Some(plan.table_name))
        } else {
            Ok(None)
        }
    }

    async fn reconcile_pending_indexes(
        &self,
        table_id: &str,
        pending_indexes: Vec<PendingIndexPlan>,
        base_attr_defs: &[AttributeDefinition],
    ) -> Result<(), StorageError> {
        let mut creating = Vec::new();
        let mut deleting = Vec::new();
        for pending in pending_indexes {
            match pending.index_status.as_str() {
                "CREATING" => creating.push(pending),
                "DELETING" => deleting.push(pending),
                other => {
                    return Err(StorageError::Internal(format!(
                        "unknown pending GSI status for {}: {other}",
                        pending.index_name
                    )));
                }
            }
        }

        self.reconcile_creating_indexes(table_id, &creating, base_attr_defs)
            .await?;
        self.reconcile_deleting_indexes(table_id, &deleting, base_attr_defs)
            .await?;
        Ok(())
    }

    async fn reconcile_creating_indexes(
        &self,
        table_id: &str,
        pending_indexes: &[PendingIndexPlan],
        base_attr_defs: &[AttributeDefinition],
    ) -> Result<(), StorageError> {
        if pending_indexes.is_empty() || !self.table_status_matches(table_id, "UPDATING").await? {
            return Ok(());
        }

        let indexes = pending_indexes
            .iter()
            .map(|pending| (pending.index_id.as_str(), pending.key_schema.as_slice()))
            .collect::<Vec<_>>();
        self.create_index_artifacts_batch_for_pending_create(table_id, &indexes, base_attr_defs)
            .await?;

        let sql = mark_creating_indexes_active_sql(pending_indexes.len());
        let mut query = sqlx::query(&sql).bind(table_id);
        for pending in pending_indexes {
            query = query.bind(&pending.index_id);
        }
        query
            .execute(&self.pool)
            .await
            .map_err(|e| StorageError::Internal(e.to_string()))?;

        Ok(())
    }

    async fn reconcile_deleting_indexes(
        &self,
        table_id: &str,
        pending_indexes: &[PendingIndexPlan],
        base_attr_defs: &[AttributeDefinition],
    ) -> Result<(), StorageError> {
        if pending_indexes.is_empty() || !self.table_status_matches(table_id, "UPDATING").await? {
            return Ok(());
        }

        let indexes = pending_indexes
            .iter()
            .map(|pending| (pending.index_id.as_str(), pending.key_schema.as_slice()))
            .collect::<Vec<_>>();
        self.drop_index_artifacts_batch_for_pending_removal(table_id, &indexes, base_attr_defs)
            .await?;

        let sql = delete_pending_indexes_sql(pending_indexes.len());
        let mut query = sqlx::query(&sql).bind(table_id);
        for pending in pending_indexes {
            query = query.bind(&pending.index_id);
        }
        query
            .execute(&self.pool)
            .await
            .map_err(|e| StorageError::Internal(e.to_string()))?;
        Ok(())
    }

    async fn fetch_table_status(&self, table_id: &str) -> Result<Option<String>, StorageError> {
        sqlx::query_scalar("SELECT table_status FROM tables WHERE table_id = ?")
            .bind(table_id)
            .fetch_optional(&self.pool)
            .await
            .map_err(|e| StorageError::Internal(e.to_string()))
    }

    async fn table_status_matches(
        &self,
        table_id: &str,
        expected_status: &str,
    ) -> Result<bool, StorageError> {
        let status = self.fetch_table_status(table_id).await?;

        Ok(status.as_deref() == Some(expected_status))
    }

    async fn table_is_deleting_or_absent(&self, table_id: &str) -> Result<bool, StorageError> {
        let status = self.fetch_table_status(table_id).await?;
        Ok(matches!(status.as_deref(), None | Some("DELETING")))
    }

    async fn create_index_artifacts_batch_for_pending_create(
        &self,
        table_id: &str,
        indexes: &[(&str, &[KeySchemaElement])],
        attr_defs: &[AttributeDefinition],
    ) -> Result<(), StorageError> {
        match Self::create_index_artifacts_batch(&self.data_pool, table_id, indexes, attr_defs)
            .await
        {
            Ok(()) => Ok(()),
            Err(err) if is_table_not_found_tidb_storage_error(&err) => {
                if self.table_is_deleting_or_absent(table_id).await? {
                    Ok(())
                } else {
                    Err(err)
                }
            }
            Err(err) => Err(err),
        }
    }

    async fn drop_index_artifacts_batch_for_pending_removal(
        &self,
        table_id: &str,
        indexes: &[(&str, &[KeySchemaElement])],
        attr_defs: &[AttributeDefinition],
    ) -> Result<(), StorageError> {
        match Self::drop_index_artifacts_batch(&self.data_pool, table_id, indexes, attr_defs).await
        {
            Ok(()) => Ok(()),
            Err(err) if is_table_not_found_tidb_storage_error(&err) => {
                if self.table_is_deleting_or_absent(table_id).await? {
                    Ok(())
                } else {
                    Err(err)
                }
            }
            Err(err) => Err(err),
        }
    }

    async fn reconcile_control_plane_plan(
        &self,
        plan: ControlPlaneReconcilePlan,
    ) -> Result<Option<(String, &'static str)>, StorageError> {
        match plan {
            ControlPlaneReconcilePlan::Create { table_id } => self
                .reconcile_table_create(&table_id)
                .await
                .map(|table_name| table_name.map(|name| (name, "CREATING → active"))),
            ControlPlaneReconcilePlan::Update { table_id } => self
                .reconcile_table_update(&table_id)
                .await
                .map(|table_name| table_name.map(|name| (name, "UPDATING → active"))),
            ControlPlaneReconcilePlan::Delete(plan) => {
                if self
                    .table_has_active_native_ddl_job(&plan.table_id, "reconcile_table_delete")
                    .await?
                {
                    return Ok(None);
                }

                self.drop_table_data_artifacts(&plan.table_id).await?;

                let mut finalize = self
                    .pool
                    .begin()
                    .await
                    .map_err(|e| StorageError::Internal(e.to_string()))?;

                let result = sqlx::query(
                    "DELETE FROM tables \
                     WHERE table_id = ? AND table_status = 'DELETING'",
                )
                .bind(&plan.table_id)
                .execute(&mut *finalize)
                .await
                .map_err(|e| StorageError::Internal(e.to_string()))?;

                let deleted = result.rows_affected() > 0;
                if deleted {
                    sqlx::query("DELETE FROM tags WHERE resource_arn = ?")
                        .bind(&plan.table_arn)
                        .execute(&mut *finalize)
                        .await
                        .map_err(|e| StorageError::Internal(e.to_string()))?;
                }

                finalize
                    .commit()
                    .await
                    .map_err(|e| StorageError::Internal(e.to_string()))?;

                Ok(deleted.then_some((plan.table_name, "DELETING → deleted")))
            }
        }
    }

    /// Process pending control plane transitions.
    ///
    /// TiDB owns distributed online DDL ordering and backfill. ExtendDB keeps
    /// the catalog as durable desired state and lets every frontend replay the
    /// same idempotent transition work. Concurrent workers may race, but they
    /// converge through TiDB `IF [NOT] EXISTS` DDL and conditional catalog
    /// publication instead of an ExtendDB-specific ownership lease.
    ///
    /// # Errors
    ///
    /// Returns [`StorageError`] if the database is unreachable or a query fails.
    pub async fn process_control_plane_transitions(
        &self,
    ) -> Result<Vec<(String, &'static str)>, StorageError> {
        let candidates: Vec<ControlPlaneTransitionRow> =
            sqlx::query_as(CONTROL_PLANE_TRANSITION_CANDIDATES_SQL)
                .bind(CONTROL_PLANE_TRANSITION_SCAN_LIMIT)
                .fetch_all(&self.pool)
                .await
                .map_err(|e| StorageError::Internal(e.to_string()))?;

        let plans = candidates
            .into_iter()
            .map(ControlPlaneReconcilePlan::from_row)
            .collect::<Result<Vec<_>, _>>()?;

        let results = stream::iter(plans)
            .map(|plan| async move {
                retry_tidb_idempotent_operation("reconcile_control_plane_plan", || {
                    let plan = plan.clone();
                    async move { self.reconcile_control_plane_plan(plan).await }
                })
                .await
            })
            .buffer_unordered(CONTROL_PLANE_TRANSITION_CONCURRENCY)
            .collect::<Vec<_>>()
            .await;

        let mut transitions = Vec::new();
        for result in results {
            if let Some(transition) = result? {
                transitions.push(transition);
            }
        }
        Ok(transitions)
    }

    async fn ttl_table_candidates(
        &self,
        limit: i64,
        start_after_table_id: Option<&str>,
    ) -> Result<Vec<TtlTableCandidate>, StorageError> {
        let limit_usize = usize::try_from(limit.max(0)).unwrap_or(usize::MAX);
        if limit_usize == 0 {
            return Ok(Vec::new());
        }

        let mut candidates = if let Some(cursor) = start_after_table_id {
            fetch_ttl_table_candidates_after(&self.pool, cursor, limit).await?
        } else {
            fetch_ttl_table_candidates_from_start(&self.pool, limit).await?
        };

        if let Some(cursor) = start_after_table_id {
            let remaining = limit_usize.saturating_sub(candidates.len());
            if remaining > 0 {
                candidates.extend(
                    fetch_ttl_table_candidates_until(
                        &self.pool,
                        cursor,
                        i64::try_from(remaining).unwrap_or(i64::MAX),
                    )
                    .await?,
                );
            }
        }

        Ok(candidates)
    }
}

fn ttl_table_candidates_from_start_sql() -> &'static str {
    "SELECT account_id, table_name, table_id \
     FROM tables USE INDEX (idx_tables_ttl_work) \
     WHERE ttl_status = 'ENABLED' \
       AND ttl_attribute IS NOT NULL \
       AND table_status IN ('ACTIVE', 'UPDATING') \
     ORDER BY table_id \
     LIMIT ?"
}

fn ttl_table_candidates_after_sql() -> &'static str {
    "SELECT account_id, table_name, table_id \
     FROM tables USE INDEX (idx_tables_ttl_work) \
     WHERE ttl_status = 'ENABLED' \
       AND ttl_attribute IS NOT NULL \
       AND table_status IN ('ACTIVE', 'UPDATING') \
       AND table_id > ? \
     ORDER BY table_id \
     LIMIT ?"
}

fn ttl_table_candidates_until_sql() -> &'static str {
    "SELECT account_id, table_name, table_id \
     FROM tables USE INDEX (idx_tables_ttl_work) \
     WHERE ttl_status = 'ENABLED' \
       AND ttl_attribute IS NOT NULL \
       AND table_status IN ('ACTIVE', 'UPDATING') \
       AND table_id <= ? \
     ORDER BY table_id \
     LIMIT ?"
}

async fn fetch_ttl_table_candidates_from_start(
    pool: &sqlx::MySqlPool,
    limit: i64,
) -> Result<Vec<TtlTableCandidate>, StorageError> {
    sqlx::query_as(ttl_table_candidates_from_start_sql())
        .bind(limit)
        .fetch_all(pool)
        .await
        .map_err(|e| StorageError::Internal(e.to_string()))
}

async fn fetch_ttl_table_candidates_after(
    pool: &sqlx::MySqlPool,
    cursor: &str,
    limit: i64,
) -> Result<Vec<TtlTableCandidate>, StorageError> {
    sqlx::query_as(ttl_table_candidates_after_sql())
        .bind(cursor)
        .bind(limit)
        .fetch_all(pool)
        .await
        .map_err(|e| StorageError::Internal(e.to_string()))
}

async fn fetch_ttl_table_candidates_until(
    pool: &sqlx::MySqlPool,
    cursor: &str,
    limit: i64,
) -> Result<Vec<TtlTableCandidate>, StorageError> {
    sqlx::query_as(ttl_table_candidates_until_sql())
        .bind(cursor)
        .bind(limit)
        .fetch_all(pool)
        .await
        .map_err(|e| StorageError::Internal(e.to_string()))
}

fn expired_ttl_items_sql(table_id: &str) -> String {
    let table = data_table_name(table_id);
    format!(
        "SELECT item_data, \
                TIMESTAMPDIFF(SECOND, `{TTL_EXPIRES_AT_COLUMN}`, CURRENT_TIMESTAMP(6)) \
                  AS expired_age_seconds \
         FROM {table} FORCE INDEX (`{TTL_EXPIRES_AT_INDEX}`) \
         WHERE `{TTL_EXPIRES_AT_COLUMN}` <= CURRENT_TIMESTAMP(6) \
         ORDER BY `{TTL_EXPIRES_AT_COLUMN}`, pk \
         LIMIT ? FOR UPDATE SKIP LOCKED"
    )
}

async fn lock_expired_ttl_items(
    tx: &mut sqlx::Transaction<'_, sqlx::MySql>,
    key_info: &TableKeyInfo,
    limit: i64,
) -> Result<Vec<ExpiredTtlItem>, StorageError> {
    let sql = expired_ttl_items_sql(&key_info.table_id);
    let rows: Vec<(serde_json::Value, i64)> = sqlx::query_as(&sql)
        .bind(limit)
        .fetch_all(&mut **tx)
        .await
        .map_err(|e| StorageError::Internal(e.to_string()))?;

    rows.into_iter()
        .map(|(json, expired_age_seconds)| {
            Ok(ExpiredTtlItem {
                item: json_to_item(json)?,
                expired_age_seconds: expired_age_seconds.max(0),
            })
        })
        .collect()
}

fn max_optional_i64(left: Option<i64>, right: Option<i64>) -> Option<i64> {
    match (left, right) {
        (Some(left), Some(right)) => Some(left.max(right)),
        (Some(value), None) | (None, Some(value)) => Some(value),
        (None, None) => None,
    }
}

#[cfg(test)]
mod tests {
    use super::{
        CONTROL_PLANE_TRANSITION_CANDIDATES_SQL, ControlPlaneReconcilePlan,
        ControlPlaneTransitionRow, delete_pending_indexes_sql, expired_ttl_items_sql,
        mark_creating_indexes_active_sql, ttl_table_candidates_after_sql,
        ttl_table_candidates_from_start_sql, ttl_table_candidates_until_sql,
    };
    use crate::tidb_util::retry_tidb_idempotent_operation;
    use extenddb_storage::error::StorageError;

    fn row(status: &str) -> ControlPlaneTransitionRow {
        ControlPlaneTransitionRow {
            table_status: status.to_owned(),
            table_name: "orders".to_owned(),
            table_id: "table-1".to_owned(),
            table_arn: "arn:aws:dynamodb:us-east-1:000000000000:table/orders".to_owned(),
        }
    }

    #[test]
    fn transition_rows_map_to_replayable_reconcile_plans() {
        assert!(matches!(
            ControlPlaneReconcilePlan::from_row(row("CREATING")).expect("create"),
            ControlPlaneReconcilePlan::Create { .. }
        ));
        assert!(matches!(
            ControlPlaneReconcilePlan::from_row(row("UPDATING")).expect("update"),
            ControlPlaneReconcilePlan::Update { .. }
        ));
        assert!(matches!(
            ControlPlaneReconcilePlan::from_row(row("DELETING")).expect("delete"),
            ControlPlaneReconcilePlan::Delete(_)
        ));
    }

    #[test]
    fn transition_rows_reject_unknown_status() {
        let Err(error) = ControlPlaneReconcilePlan::from_row(row("ARCHIVING")) else {
            panic!("unknown status should be rejected");
        };
        assert!(error.to_string().contains("unknown TiDB control-plane"));
    }

    #[test]
    fn pending_index_publication_uses_one_set_based_catalog_statement() {
        let sql = mark_creating_indexes_active_sql(2);

        assert!(sql.contains("SET index_status = 'ACTIVE'"));
        assert!(sql.contains("index_status = 'CREATING'"));
        assert!(sql.contains("index_id IN (?, ?)"));
        assert!(sql.contains("tables.table_status = 'UPDATING'"));
    }

    #[test]
    fn pending_index_delete_uses_one_set_based_catalog_statement() {
        let sql = delete_pending_indexes_sql(3);

        assert!(sql.starts_with("DELETE FROM indexes"));
        assert!(sql.contains("index_status = 'DELETING'"));
        assert!(sql.contains("index_id IN (?, ?, ?)"));
        assert!(sql.contains("tables.table_status = 'UPDATING'"));
    }

    #[test]
    fn control_plane_transition_scan_uses_due_time_queue() {
        let sql = CONTROL_PLANE_TRANSITION_CANDIDATES_SQL;

        assert!(sql.contains("table_status IN ('CREATING', 'UPDATING', 'DELETING')"));
        assert!(sql.contains("status_transition_at <= CURRENT_TIMESTAMP(6)"));
        assert!(sql.contains("ORDER BY status_transition_at, table_name"));
        assert!(!sql.contains("status_transition_at IS NULL"));
        assert!(!sql.contains(" OR "));
    }

    #[test]
    fn ttl_candidate_scan_only_selects_enabled_user_ttl_tables() {
        let sql = ttl_table_candidates_from_start_sql();

        assert!(sql.contains("SELECT account_id, table_name, table_id"));
        assert!(sql.contains("USE INDEX (idx_tables_ttl_work)"));
        assert!(sql.contains("ttl_status = 'ENABLED'"));
        assert!(sql.contains("ttl_attribute IS NOT NULL"));
        assert!(sql.contains("table_status IN ('ACTIVE', 'UPDATING')"));
        assert!(sql.contains("ORDER BY table_id"));
        assert!(sql.contains("LIMIT ?"));
    }

    #[test]
    fn ttl_candidate_scan_can_resume_and_wrap_by_table_id() {
        let after = ttl_table_candidates_after_sql();
        let until = ttl_table_candidates_until_sql();

        assert!(after.contains("table_id > ?"));
        assert!(after.contains("ORDER BY table_id"));
        assert!(until.contains("table_id <= ?"));
        assert!(until.contains("ORDER BY table_id"));
    }

    #[test]
    fn ttl_expiry_scan_uses_lookup_index_and_row_locks() {
        let sql = expired_ttl_items_sql("table-1");

        assert!(sql.contains("FORCE INDEX (`_edb_ttl_expires_at_idx`)"));
        assert!(sql.contains("TIMESTAMPDIFF(SECOND, `_edb_ttl_expires_at`"));
        assert!(sql.contains("`_edb_ttl_expires_at` <= CURRENT_TIMESTAMP(6)"));
        assert!(sql.contains("ORDER BY `_edb_ttl_expires_at`, pk"));
        assert!(sql.ends_with("LIMIT ? FOR UPDATE SKIP LOCKED"));
    }

    #[tokio::test]
    async fn control_plane_reconcile_retry_replays_the_whole_plan() {
        let plan = ControlPlaneReconcilePlan::from_row(row("CREATING")).expect("create plan");
        let mut attempts = 0;

        let result = retry_tidb_idempotent_operation("test_control_plane_retry", || {
            let plan = plan.clone();
            attempts += 1;
            let attempt = attempts;
            async move {
                match plan {
                    ControlPlaneReconcilePlan::Create { table_id } => {
                        assert_eq!(table_id, "table-1");
                    }
                    _ => panic!("expected create plan"),
                }

                if attempt == 1 {
                    Err(StorageError::Internal(
                        "ERROR 9007 (HY000): Write conflict".to_owned(),
                    ))
                } else {
                    Ok(attempt)
                }
            }
        })
        .await;

        assert_eq!(result.expect("retry succeeds"), 2);
        assert_eq!(attempts, 2);
    }
}

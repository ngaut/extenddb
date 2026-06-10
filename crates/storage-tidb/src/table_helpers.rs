// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Helper types and methods for `TableEngine` operations.

use extenddb_core::provisioning::{
    provisioned_throughput_description_from_value, zero_provisioned_throughput_description,
};
use extenddb_core::types::{
    BillingMode, BillingModeSummary, GsiDescription, LsiDescription, StreamSpecification,
    TableDescription, TableStatus,
};
use extenddb_storage::error::StorageError;
use extenddb_storage::util::{index_arn, stream_arn};

use crate::TidbEngine;
use crate::data::physical_data_table_name;

/// Row type for table metadata queries.
pub(crate) struct TableRow {
    pub table_name: String,
    pub key_schema: serde_json::Value,
    pub attribute_definitions: serde_json::Value,
    pub billing_mode: String,
    pub provisioned_throughput: Option<serde_json::Value>,
    pub stream_specification: Option<serde_json::Value>,
    pub table_status: String,
    pub creation_epoch: Option<f64>,
    pub table_arn: String,
    pub table_id: String,
    pub deletion_protection_enabled: bool,
    pub stream_label: Option<String>,
}

impl<'r> sqlx::FromRow<'r, sqlx::mysql::MySqlRow> for TableRow {
    fn from_row(row: &'r sqlx::mysql::MySqlRow) -> Result<Self, sqlx::Error> {
        Ok(Self {
            table_name: sqlx::Row::try_get(row, "table_name")?,
            key_schema: sqlx::Row::try_get(row, "key_schema")?,
            attribute_definitions: sqlx::Row::try_get(row, "attribute_definitions")?,
            billing_mode: sqlx::Row::try_get(row, "billing_mode")?,
            provisioned_throughput: sqlx::Row::try_get(row, "provisioned_throughput")?,
            stream_specification: sqlx::Row::try_get(row, "stream_specification")?,
            table_status: sqlx::Row::try_get(row, "table_status")?,
            creation_epoch: sqlx::Row::try_get(row, "creation_epoch")?,
            table_arn: sqlx::Row::try_get(row, "table_arn")?,
            table_id: sqlx::Row::try_get(row, "table_id")?,
            deletion_protection_enabled: sqlx::Row::try_get(row, "deletion_protection_enabled")?,
            stream_label: sqlx::Row::try_get(row, "stream_label")?,
        })
    }
}

/// TiDB table statistics used for DynamoDB table descriptions.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct TableStats {
    pub table_size_bytes: i64,
    pub item_count: i64,
}

/// Row type for index metadata queries.
pub(crate) struct IndexRow {
    pub index_name: String,
    pub index_type: String,
    pub key_schema: serde_json::Value,
    pub projection: serde_json::Value,
    pub index_status: String,
    pub provisioned_throughput: Option<serde_json::Value>,
}

impl<'r> sqlx::FromRow<'r, sqlx::mysql::MySqlRow> for IndexRow {
    fn from_row(row: &'r sqlx::mysql::MySqlRow) -> Result<Self, sqlx::Error> {
        Ok(Self {
            index_name: sqlx::Row::try_get(row, "index_name")?,
            index_type: sqlx::Row::try_get(row, "index_type")?,
            key_schema: sqlx::Row::try_get(row, "key_schema")?,
            projection: sqlx::Row::try_get(row, "projection")?,
            index_status: sqlx::Row::try_get(row, "index_status")?,
            provisioned_throughput: sqlx::Row::try_get(row, "provisioned_throughput")?,
        })
    }
}

fn current_table_stats_sql() -> &'static str {
    "SELECT CAST(COALESCE(MAX(DATA_LENGTH), 0) AS SIGNED), \
            CAST(COALESCE(MAX(TABLE_ROWS), 0) AS SIGNED) \
     FROM information_schema.tables \
     WHERE table_schema = DATABASE() AND table_name = ?"
}

impl TidbEngine {
    pub(crate) async fn current_table_stats(
        &self,
        table_id: &str,
    ) -> Result<TableStats, StorageError> {
        let physical_table = physical_data_table_name(table_id);
        let (table_size_bytes, item_count): (i64, i64) = sqlx::query_as(current_table_stats_sql())
            .bind(&physical_table)
            .fetch_one(&self.data_pool)
            .await
            .map_err(|e| StorageError::Internal(e.to_string()))?;

        Ok(TableStats {
            table_size_bytes,
            item_count,
        })
    }

    pub(crate) async fn build_table_description(
        &self,
        account_id: &str,
        table_name: &str,
    ) -> Result<TableDescription, StorageError> {
        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|e| StorageError::Internal(e.to_string()))?;

        let row: Option<TableRow> = sqlx::query_as(
            r"SELECT table_name, key_schema, attribute_definitions, billing_mode,
                      provisioned_throughput, stream_specification, table_status,
                      CAST(UNIX_TIMESTAMP(creation_date_time) AS DOUBLE) as creation_epoch,
                      table_arn, table_id, deletion_protection_enabled, stream_label
               FROM tables WHERE account_id = ? AND table_name = ?",
        )
        .bind(account_id)
        .bind(table_name)
        .fetch_optional(&mut *tx)
        .await
        .map_err(|e| StorageError::Internal(e.to_string()))?;

        let row = row.ok_or_else(|| StorageError::TableNotFound(table_name.to_owned()))?;

        let index_rows: Vec<IndexRow> = sqlx::query_as(
            r"SELECT index_name, index_type, key_schema, projection,
                      index_status, provisioned_throughput
               FROM indexes WHERE table_id = ?",
        )
        .bind(&row.table_id)
        .fetch_all(&mut *tx)
        .await
        .map_err(|e| StorageError::Internal(e.to_string()))?;

        tx.commit()
            .await
            .map_err(|e| StorageError::Internal(e.to_string()))?;

        let stats = self.current_table_stats(&row.table_id).await?;

        self.build_table_description_from_row(account_id, row, index_rows, stats)
    }

    pub(crate) fn build_table_description_from_row(
        &self,
        account_id: &str,
        row: TableRow,
        index_rows: Vec<IndexRow>,
        stats: TableStats,
    ) -> Result<TableDescription, StorageError> {
        let mut gsis: Vec<GsiDescription> = Vec::new();
        let mut lsis: Vec<LsiDescription> = Vec::new();

        for idx in index_rows {
            let ks = serde_json::from_value(idx.key_schema)
                .map_err(|e| StorageError::Internal(e.to_string()))?;
            let proj = serde_json::from_value(idx.projection)
                .map_err(|e| StorageError::Internal(e.to_string()))?;

            match idx.index_type.as_str() {
                "GSI" => {
                    let provisioned_throughput = idx
                        .provisioned_throughput
                        .map(provisioned_throughput_description_from_value)
                        .transpose()
                        .map_err(|e| StorageError::Internal(e.to_string()))?
                        .unwrap_or_else(zero_provisioned_throughput_description);

                    gsis.push(GsiDescription {
                        index_name: idx.index_name.clone(),
                        key_schema: ks,
                        projection: proj,
                        index_status: idx.index_status,
                        provisioned_throughput: Some(provisioned_throughput),
                        index_size_bytes: 0,
                        item_count: 0,
                        index_arn: index_arn(
                            &self.region,
                            account_id,
                            &row.table_name,
                            &idx.index_name,
                        ),
                    });
                }
                "LSI" => {
                    lsis.push(LsiDescription {
                        index_name: idx.index_name.clone(),
                        key_schema: ks,
                        projection: proj,
                        index_size_bytes: 0,
                        item_count: 0,
                        index_arn: index_arn(
                            &self.region,
                            account_id,
                            &row.table_name,
                            &idx.index_name,
                        ),
                    });
                }
                other => {
                    return Err(StorageError::Internal(format!(
                        "unknown index type in database: {other}"
                    )));
                }
            }
        }

        let key_schema = serde_json::from_value(row.key_schema)
            .map_err(|e| StorageError::Internal(e.to_string()))?;
        let attr_defs = serde_json::from_value(row.attribute_definitions)
            .map_err(|e| StorageError::Internal(e.to_string()))?;
        let stream_spec: Option<StreamSpecification> = row
            .stream_specification
            .map(serde_json::from_value)
            .transpose()
            .map_err(|e| StorageError::Internal(e.to_string()))?;
        let active_stream_label = if stream_spec.as_ref().is_some_and(|spec| spec.stream_enabled) {
            row.stream_label.clone()
        } else {
            None
        };

        let provisioned_throughput = row
            .provisioned_throughput
            .map(provisioned_throughput_description_from_value)
            .transpose()
            .map_err(|e| StorageError::Internal(e.to_string()))?
            .unwrap_or_else(zero_provisioned_throughput_description);

        let table_status = match row.table_status.as_str() {
            "ACTIVE" => TableStatus::Active,
            "CREATING" => TableStatus::Creating,
            "DELETING" => TableStatus::Deleting,
            "UPDATING" => TableStatus::Updating,
            other => {
                return Err(StorageError::Internal(format!(
                    "unknown table status in database: {other}"
                )));
            }
        };

        let creation_epoch = row.creation_epoch.unwrap_or(0.0);

        let billing_mode_summary = if row.billing_mode == "PAY_PER_REQUEST" {
            Some(BillingModeSummary {
                billing_mode: BillingMode::PayPerRequest,
                last_update_to_pay_per_request_date_time: Some(creation_epoch),
            })
        } else {
            None
        };

        let latest_stream_arn = active_stream_label
            .as_ref()
            .map(|label| stream_arn(&self.region, account_id, &row.table_name, label));

        Ok(TableDescription {
            table_name: row.table_name,
            key_schema,
            attribute_definitions: attr_defs,
            table_status,
            creation_date_time: creation_epoch,
            table_size_bytes: stats.table_size_bytes,
            item_count: stats.item_count,
            table_arn: row.table_arn,
            table_id: row.table_id,
            provisioned_throughput,
            billing_mode_summary,
            global_secondary_indexes: if gsis.is_empty() { None } else { Some(gsis) },
            local_secondary_indexes: if lsis.is_empty() { None } else { Some(lsis) },
            stream_specification: stream_spec,
            latest_stream_arn,
            latest_stream_label: active_stream_label,
            deletion_protection_enabled: row.deletion_protection_enabled,
            sse_description: None,
            table_class_summary: None,
            on_demand_throughput: None,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::current_table_stats_sql;

    #[test]
    fn table_description_stats_use_app_user_safe_tidb_table_metadata() {
        let stats_sql = current_table_stats_sql();

        assert!(stats_sql.contains("information_schema.tables"));
        assert!(stats_sql.contains("DATA_LENGTH"));
        assert!(stats_sql.contains("TABLE_ROWS"));
        assert!(stats_sql.contains("COALESCE(MAX(DATA_LENGTH), 0)"));
        assert!(stats_sql.contains("COALESCE(MAX(TABLE_ROWS), 0)"));
        assert!(!stats_sql.contains("SHOW STATS_META"));
        assert!(!stats_sql.contains("information_schema.table_storage_stats"));
        assert!(!stats_sql.contains("TABLE_SIZE"));
    }
}

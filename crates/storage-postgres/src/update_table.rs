// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! `update_table` implementation for `PostgresEngine`.

use extenddb_core::provisioning::{
    apply_provisioned_throughput_update, current_unix_epoch_seconds,
    provisioned_throughput_description, provisioned_throughput_description_from_value,
};
use extenddb_core::types::{
    AttributeDefinition, BillingMode, KeySchemaElement, ProvisionedThroughput, TableDescription,
    UpdateTableInput,
};
use extenddb_storage::error::StorageError;

use crate::PostgresEngine;

type UpdateTableCatalogRow = (
    String,
    String,
    serde_json::Value,
    serde_json::Value,
    Option<String>,
    Option<serde_json::Value>,
);

fn apply_gsi_provisioned_throughput_update_from_catalog(
    index_name: &str,
    current: Option<serde_json::Value>,
    requested: &ProvisionedThroughput,
    now_epoch_seconds: f64,
) -> Result<serde_json::Value, StorageError> {
    let current = current
        .map(provisioned_throughput_description_from_value)
        .transpose()
        .map_err(|e| StorageError::Internal(e.to_string()))?;

    if current.as_ref().is_some_and(|current| {
        current.read_capacity_units == requested.read_capacity_units
            && current.write_capacity_units == requested.write_capacity_units
    }) {
        return Err(StorageError::NoOpUpdate(format!(
            "The provisioned throughput for global secondary index {index_name} will not change. \
             The requested value equals the current value."
        )));
    }

    let next = apply_provisioned_throughput_update(current.as_ref(), requested, now_epoch_seconds)
        .map_err(|err| StorageError::Validation(err.to_string()))?;
    serde_json::to_value(&next).map_err(|e| StorageError::Internal(e.to_string()))
}

impl PostgresEngine {
    /// Core implementation of `update_table` (REQ-CTRL-003).
    pub(crate) async fn update_table_impl(
        &self,
        account_id: &str,
        input: UpdateTableInput,
    ) -> Result<TableDescription, StorageError> {
        Self::validate_account_id(account_id)?;
        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|e| StorageError::Internal(e.to_string()))?;

        // Lock the row and fetch table metadata plus throughput state.
        let row: Option<UpdateTableCatalogRow> = sqlx::query_as(
            "SELECT table_status, table_id, key_schema, attribute_definitions, \
                    billing_mode, provisioned_throughput \
             FROM tables WHERE account_id = $1 AND table_name = $2 FOR UPDATE",
        )
        .bind(account_id)
        .bind(&input.table_name)
        .fetch_optional(&mut *tx)
        .await
        .map_err(|e| StorageError::Internal(e.to_string()))?;

        let (status, table_id, ks_json, ad_json, current_billing_mode, current_pt_json) =
            row.ok_or_else(|| StorageError::TableNotFound(input.table_name.clone()))?;
        if status != "ACTIVE" {
            return Err(StorageError::TableNotActive(input.table_name.clone()));
        }
        let current_is_provisioned =
            current_billing_mode.as_deref().unwrap_or("PROVISIONED") == "PROVISIONED";
        let has_gsi_throughput_update = input
            .global_secondary_index_updates
            .as_deref()
            .is_some_and(|updates| updates.iter().any(|update| update.update.is_some()));
        let current_pt_description = if current_is_provisioned {
            current_pt_json
                .as_ref()
                .map(|value| provisioned_throughput_description_from_value(value.clone()))
                .transpose()
                .map_err(|e| StorageError::Internal(e.to_string()))?
        } else {
            None
        };
        let effective_billing_mode = match input.billing_mode.as_ref() {
            Some(BillingMode::Provisioned) => "PROVISIONED",
            Some(BillingMode::PayPerRequest) => "PAY_PER_REQUEST",
            None => current_billing_mode.as_deref().unwrap_or("PROVISIONED"),
        };
        if has_gsi_throughput_update && effective_billing_mode == "PAY_PER_REQUEST" {
            return Err(StorageError::Validation(
                "One or more parameter values were invalid: ProvisionedThroughput cannot be specified for a global secondary index when BillingMode is PAY_PER_REQUEST".to_owned(),
            ));
        }

        // No-op rejection: setting same billing mode to PROVISIONED with same
        // throughput values is rejected by DynamoDB. This check runs under the
        // FOR UPDATE lock to eliminate the TOCTOU race that existed when the
        // check was in the engine layer.
        if matches!(input.billing_mode, Some(BillingMode::Provisioned))
            && let Some(ref pt) = input.provisioned_throughput
        {
            let (current_rcu, current_wcu) =
                current_pt_description.as_ref().map_or((0, 0), |current| {
                    (current.read_capacity_units, current.write_capacity_units)
                });

            if current_is_provisioned
                && current_rcu == pt.read_capacity_units
                && current_wcu == pt.write_capacity_units
            {
                return Err(StorageError::NoOpUpdate(format!(
                    "The provisioned throughput for the table will not change. \
                         The requested value equals the current value. \
                         Current ReadCapacityUnits provisioned for the table: {}. \
                         Requested ReadCapacityUnits: {}. \
                         Current WriteCapacityUnits provisioned for the table: {}. \
                         Requested WriteCapacityUnits: {}.",
                    current_rcu, pt.read_capacity_units, current_wcu, pt.write_capacity_units
                )));
            }
        }

        // Apply billing mode change.
        if let Some(bm) = &input.billing_mode {
            let bm_str = match bm {
                BillingMode::Provisioned => "PROVISIONED",
                BillingMode::PayPerRequest => "PAY_PER_REQUEST",
            };
            sqlx::query(
                "UPDATE tables SET billing_mode = $1 WHERE account_id = $2 AND table_name = $3",
            )
            .bind(bm_str)
            .bind(account_id)
            .bind(&input.table_name)
            .execute(&mut *tx)
            .await
            .map_err(|e| StorageError::Internal(e.to_string()))?;
        }

        // Apply provisioned throughput change.
        if let Some(pt) = &input.provisioned_throughput {
            let pt_description = apply_provisioned_throughput_update(
                current_pt_description.as_ref(),
                pt,
                current_unix_epoch_seconds(),
            )
            .map_err(|err| StorageError::Validation(err.to_string()))?;
            let pt_json = serde_json::to_value(&pt_description)
                .map_err(|e| StorageError::Internal(e.to_string()))?;
            sqlx::query("UPDATE tables SET provisioned_throughput = $1 WHERE account_id = $2 AND table_name = $3")
                .bind(&pt_json)
                .bind(account_id)
                .bind(&input.table_name)
                .execute(&mut *tx)
                .await
                .map_err(|e| StorageError::Internal(e.to_string()))?;
        }

        // Apply deletion protection change.
        if let Some(dp) = input.deletion_protection_enabled {
            sqlx::query("UPDATE tables SET deletion_protection_enabled = $1 WHERE account_id = $2 AND table_name = $3")
                .bind(dp)
                .bind(account_id)
                .bind(&input.table_name)
                .execute(&mut *tx)
                .await
                .map_err(|e| StorageError::Internal(e.to_string()))?;
        }

        // Apply stream specification change (enable/disable streams).
        if let Some(spec) = &input.stream_specification {
            let spec_json =
                serde_json::to_value(spec).map_err(|e| StorageError::Internal(e.to_string()))?;
            sqlx::query(
                "UPDATE tables SET stream_specification = $1 \
                 WHERE account_id = $2 AND table_name = $3",
            )
            .bind(&spec_json)
            .bind(account_id)
            .bind(&input.table_name)
            .execute(&mut *tx)
            .await
            .map_err(|e| StorageError::Internal(e.to_string()))?;

            if spec.stream_enabled {
                // Check if shards already exist (re-enabling streams on a table
                // that previously had them). Query the data pool since
                // stream_shards lives in the data database.
                let existing: Option<(String,)> = sqlx::query_as(
                    "SELECT shard_id FROM stream_shards \
                     WHERE table_id = $1 \
                     LIMIT 1",
                )
                .bind(&table_id)
                .fetch_optional(&self.data_pool)
                .await
                .map_err(|e| StorageError::Internal(e.to_string()))?;

                if existing.is_none() {
                    Self::init_stream_shards(
                        &mut tx,
                        &self.data_pool,
                        account_id,
                        &input.table_name,
                        &table_id,
                    )
                    .await?;
                } else {
                    // Shards exist but stream_label may be NULL if streams were
                    // previously disabled and the disable path cleared the label.
                    // This is a defensive check — init_stream_shards sets the
                    // label on first enable, but re-enable after disable needs
                    // to restore it.
                    let current_label: Option<String> = sqlx::query_scalar(
                        "SELECT stream_label FROM tables \
                         WHERE account_id = $1 AND table_name = $2",
                    )
                    .bind(account_id)
                    .bind(&input.table_name)
                    .fetch_one(&mut *tx)
                    .await
                    .map_err(|e| StorageError::Internal(e.to_string()))?;

                    if current_label.is_none() {
                        sqlx::query(
                            "UPDATE tables SET stream_label = \
                             to_char(NOW(), 'YYYY-MM-DD\"T\"HH24:MI:SS') \
                             WHERE account_id = $1 AND table_name = $2",
                        )
                        .bind(account_id)
                        .bind(&input.table_name)
                        .execute(&mut *tx)
                        .await
                        .map_err(|e| StorageError::Internal(e.to_string()))?;
                    }
                }
            }
        }

        // Apply GSI updates (create/delete).
        let mut created_index_ids: Vec<String> = Vec::new();
        let mut deleted_index_ids: Vec<String> = Vec::new();
        if let Some(updates) = &input.global_secondary_index_updates {
            for update in updates {
                if let Some(create) = &update.create {
                    // Check for duplicate index name.
                    let existing: Option<(String,)> = sqlx::query_as(
                        "SELECT index_name FROM indexes WHERE table_id = $1 AND index_name = $2",
                    )
                    .bind(&table_id)
                    .bind(&create.index_name)
                    .fetch_optional(&mut *tx)
                    .await
                    .map_err(|e| StorageError::Internal(e.to_string()))?;

                    if existing.is_some() {
                        return Err(StorageError::IndexAlreadyExists(create.index_name.clone()));
                    }

                    let gsi_ks = serde_json::to_value(&create.key_schema)
                        .map_err(|e| StorageError::Internal(e.to_string()))?;
                    let gsi_proj = serde_json::to_value(&create.projection)
                        .map_err(|e| StorageError::Internal(e.to_string()))?;
                    let gsi_pt = create
                        .provisioned_throughput
                        .as_ref()
                        .map(|throughput| {
                            serde_json::to_value(provisioned_throughput_description(throughput))
                        })
                        .transpose()
                        .map_err(|e| StorageError::Internal(e.to_string()))?;

                    let index_id = uuid::Uuid::new_v4().to_string();
                    sqlx::query(
                        r"INSERT INTO indexes
                           (table_id, index_name, index_id, index_type, key_schema, projection,
                            index_status, provisioned_throughput)
                           VALUES ($1, $2, $3, 'GSI', $4, $5, 'ACTIVE', $6)",
                    )
                    .bind(&table_id)
                    .bind(&create.index_name)
                    .bind(&index_id)
                    .bind(&gsi_ks)
                    .bind(&gsi_proj)
                    .bind(&gsi_pt)
                    .execute(&mut *tx)
                    .await
                    .map_err(|e| StorageError::Internal(e.to_string()))?;
                    created_index_ids.push(index_id);

                    // Create the index data table on the data pool.
                    // Catalog metadata is committed first; data DDL follows.
                }

                if let Some(delete) = &update.delete {
                    // Verify the index exists and fetch its index_id.
                    let existing: Option<(String, String)> = sqlx::query_as(
                        "SELECT index_name, index_id FROM indexes WHERE table_id = $1 AND index_name = $2",
                    )
                    .bind(&table_id)
                    .bind(&delete.index_name)
                    .fetch_optional(&mut *tx)
                    .await
                    .map_err(|e| StorageError::Internal(e.to_string()))?;

                    let (_, del_index_id) = existing
                        .ok_or_else(|| StorageError::IndexNotFound(delete.index_name.clone()))?;

                    // Delete the index metadata.
                    sqlx::query("DELETE FROM indexes WHERE table_id = $1 AND index_name = $2")
                        .bind(&table_id)
                        .bind(&delete.index_name)
                        .execute(&mut *tx)
                        .await
                        .map_err(|e| StorageError::Internal(e.to_string()))?;
                    deleted_index_ids.push(del_index_id);

                    // Drop the index data table on the data pool after catalog commit.
                }

                if let Some(update) = &update.update {
                    let existing: Option<(Option<serde_json::Value>,)> = sqlx::query_as(
                        "SELECT provisioned_throughput FROM indexes \
                         WHERE table_id = $1 AND index_name = $2 AND index_type = 'GSI' \
                         FOR UPDATE",
                    )
                    .bind(&table_id)
                    .bind(&update.index_name)
                    .fetch_optional(&mut *tx)
                    .await
                    .map_err(|e| StorageError::Internal(e.to_string()))?;

                    let (current_throughput,) = existing
                        .ok_or_else(|| StorageError::IndexNotFound(update.index_name.clone()))?;
                    let requested = update.provisioned_throughput.as_ref().ok_or_else(|| {
                        StorageError::Validation(
                            "One or more parameter values were invalid: ProvisionedThroughput must be specified for GlobalSecondaryIndexUpdate Update".to_owned(),
                        )
                    })?;
                    let next_throughput = apply_gsi_provisioned_throughput_update_from_catalog(
                        &update.index_name,
                        current_throughput,
                        requested,
                        current_unix_epoch_seconds(),
                    )?;

                    sqlx::query(
                        "UPDATE indexes SET provisioned_throughput = $1 \
                         WHERE table_id = $2 AND index_name = $3 AND index_type = 'GSI'",
                    )
                    .bind(&next_throughput)
                    .bind(&table_id)
                    .bind(&update.index_name)
                    .execute(&mut *tx)
                    .await
                    .map_err(|e| StorageError::Internal(e.to_string()))?;
                }
            }

            // Update attribute_definitions on the table if new ones were provided.
            if let Some(new_attr_defs) = &input.attribute_definitions {
                let ad_json = serde_json::to_value(new_attr_defs)
                    .map_err(|e| StorageError::Internal(e.to_string()))?;
                sqlx::query("UPDATE tables SET attribute_definitions = $1 WHERE account_id = $2 AND table_name = $3")
                    .bind(&ad_json)
                    .bind(account_id)
                    .bind(&input.table_name)
                    .execute(&mut *tx)
                    .await
                    .map_err(|e| StorageError::Internal(e.to_string()))?;
            }
        }

        tx.commit()
            .await
            .map_err(|e| StorageError::Internal(e.to_string()))?;

        // Execute data DDL on the data pool after catalog commit.
        if let Some(updates) = &input.global_secondary_index_updates {
            let base_key_schema: Vec<KeySchemaElement> = serde_json::from_value(ks_json.clone())
                .map_err(|e| StorageError::Internal(e.to_string()))?;
            let base_attr_defs: Vec<AttributeDefinition> = serde_json::from_value(ad_json.clone())
                .map_err(|e| StorageError::Internal(e.to_string()))?;
            let effective_attr_defs = input
                .attribute_definitions
                .as_deref()
                .unwrap_or(&base_attr_defs);

            let mut create_idx = 0usize;
            let mut delete_idx = 0usize;
            for update in updates {
                if let Some(create) = &update.create {
                    let idx_id = &created_index_ids[create_idx];
                    create_idx += 1;
                    let data_result = async {
                        let mut data_tx = self
                            .data_pool
                            .begin()
                            .await
                            .map_err(|e| StorageError::Internal(e.to_string()))?;

                        Self::create_index_data_table(
                            &mut data_tx,
                            idx_id,
                            &create.key_schema,
                            effective_attr_defs,
                            &base_key_schema,
                            &base_attr_defs,
                        )
                        .await?;

                        Self::backfill_gsi(
                            &mut data_tx,
                            &table_id,
                            idx_id,
                            &create.key_schema,
                            effective_attr_defs,
                            &base_key_schema,
                            &base_attr_defs,
                            &create.projection,
                        )
                        .await?;

                        data_tx
                            .commit()
                            .await
                            .map_err(|e| StorageError::Internal(e.to_string()))?;
                        Ok::<(), StorageError>(())
                    }
                    .await;

                    if let Err(e) = data_result {
                        tracing::error!(
                            "Failed to create data table for GSI '{}' on '{}', \
                             cleaning up catalog: {e}",
                            create.index_name,
                            input.table_name,
                        );
                        let _ = sqlx::query(
                            "DELETE FROM indexes WHERE table_id = $1 AND index_name = $2",
                        )
                        .bind(&table_id)
                        .bind(&create.index_name)
                        .execute(&self.pool)
                        .await;
                        return Err(e);
                    }
                }

                if update.delete.is_some() {
                    let idx_id = &deleted_index_ids[delete_idx];
                    delete_idx += 1;
                    let idx_table = Self::index_table_name_static(idx_id);
                    if let Err(e) = sqlx::query(&format!("DROP TABLE IF EXISTS {idx_table}"))
                        .execute(&self.data_pool)
                        .await
                    {
                        tracing::warn!(
                            "Failed to drop data table for deleted GSI on '{}': {e}",
                            input.table_name,
                        );
                    }
                }
            }
        }

        self.build_table_description(account_id, &input.table_name)
            .await
    }
}

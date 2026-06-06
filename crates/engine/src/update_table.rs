// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! `UpdateTable` operation handler.

use extenddb_core::error::DynamoDbError;
use extenddb_core::types::{BillingMode, GlobalSecondaryIndexUpdate, UpdateTableInput};
use serde_json::Value;

use crate::OperationContext;
use crate::serialize_output;

fn gsi_update_action_count(update: &GlobalSecondaryIndexUpdate) -> usize {
    usize::from(update.create.is_some())
        + usize::from(update.update.is_some())
        + usize::from(update.delete.is_some())
}

fn validate_gsi_updates(input: &UpdateTableInput) -> Result<(), DynamoDbError> {
    let Some(updates) = &input.global_secondary_index_updates else {
        return Ok(());
    };

    if updates.len() > 1 {
        return Err(DynamoDbError::ValidationException(
            "One or more parameter values were invalid: Only one GlobalSecondaryIndexUpdate can be specified per UpdateTable operation".to_owned(),
        ));
    }

    for update in updates {
        match gsi_update_action_count(update) {
            0 => {
                return Err(DynamoDbError::ValidationException(
                    "One or more parameter values were invalid: GlobalSecondaryIndexUpdate must contain Create, Update, or Delete".to_owned(),
                ));
            }
            1 => {}
            _ => {
                return Err(DynamoDbError::ValidationException(
                    "One or more parameter values were invalid: Only one of Create, Update, or Delete can be specified per GlobalSecondaryIndexUpdate".to_owned(),
                ));
            }
        }

        if let Some(upd) = &update.update {
            extenddb_core::validation::validate_index_name(&upd.index_name)?;
            let Some(throughput) = &upd.provisioned_throughput else {
                return Err(DynamoDbError::ValidationException(
                    "One or more parameter values were invalid: ProvisionedThroughput must be specified for GlobalSecondaryIndexUpdate Update".to_owned(),
                ));
            };
            if throughput.read_capacity_units < 1 || throughput.write_capacity_units < 1 {
                return Err(DynamoDbError::ValidationException(
                    "One or more parameter values were invalid: ReadCapacityUnits and WriteCapacityUnits must each be at least 1".to_owned(),
                ));
            }
        }

        // M3: Validate index names the same way CreateTable does.
        if let Some(create) = &update.create {
            extenddb_core::validation::validate_index_name(&create.index_name)?;
            if create.key_schema.is_empty() {
                return Err(DynamoDbError::ValidationException(
                    "One or more parameter values were invalid: KeySchema must not be empty for GSI creation".to_owned(),
                ));
            }
            // Validate that all key attributes are defined in AttributeDefinitions
            let attr_defs = input.attribute_definitions.as_deref().unwrap_or(&[]);
            for ks in &create.key_schema {
                if !attr_defs
                    .iter()
                    .any(|ad| ad.attribute_name == ks.attribute_name)
                {
                    return Err(DynamoDbError::ValidationException(format!(
                        "One or more parameter values were invalid: Some index key attributes are not defined in AttributeDefinitions. \
                         Keys: [{}], AttributeDefinitions: [{}]",
                        ks.attribute_name,
                        attr_defs
                            .iter()
                            .map(|ad| ad.attribute_name.as_str())
                            .collect::<Vec<_>>()
                            .join(", ")
                    )));
                }
            }
        }
        if let Some(delete) = &update.delete {
            extenddb_core::validation::validate_index_name(&delete.index_name)?;
        }
    }

    Ok(())
}

/// Handle `UpdateTable` — modify billing mode, throughput, deletion protection,
/// or GSI configuration.
///
/// REQ-CTRL-003: `UpdateTable` must support changing billing mode, provisioned
/// throughput, and GSI create/delete.
///
/// # Errors
///
/// Returns `ValidationException` if no fields are specified, or if switching to
/// `PROVISIONED` without providing throughput values.
/// Returns `ResourceNotFoundException` if the table does not exist.
/// Returns `ResourceInUseException` if the table is not ACTIVE.
/// Returns `InternalServerError` on storage failures.
pub async fn handle_update_table(
    body: Value,
    ctx: &OperationContext,
) -> Result<Value, DynamoDbError> {
    let input: UpdateTableInput = serde_json::from_value(body).map_err(crate::deserialize_error)?;

    if input.table_name.is_empty() {
        return Err(DynamoDbError::ValidationException(
            "TableName must not be empty".to_owned(),
        ));
    }

    let has_gsi_updates = input
        .global_secondary_index_updates
        .as_ref()
        .is_some_and(|u| !u.is_empty());

    // Validate: at least one field must be specified.
    if input.billing_mode.is_none()
        && input.provisioned_throughput.is_none()
        && input.deletion_protection_enabled.is_none()
        && input.stream_specification.is_none()
        && !has_gsi_updates
    {
        return Err(DynamoDbError::ValidationException(
            "At least one of BillingMode, ProvisionedThroughput, DeletionProtectionEnabled, StreamSpecification, or GlobalSecondaryIndexUpdates must be specified".to_owned(),
        ));
    }

    // Validate: enabling streams requires a view type.
    if let Some(spec) = &input.stream_specification
        && spec.stream_enabled
        && spec.stream_view_type.is_none()
    {
        return Err(DynamoDbError::ValidationException(
            "StreamViewType must be specified when StreamEnabled is true".to_owned(),
        ));
    }

    // Switching to PROVISIONED requires explicit throughput values.
    if matches!(input.billing_mode, Some(BillingMode::Provisioned))
        && input.provisioned_throughput.is_none()
    {
        return Err(DynamoDbError::ValidationException(
            "One or more parameter values were invalid: ProvisionedThroughput must be specified when changing BillingMode to PROVISIONED".to_owned(),
        ));
    }

    // PAY_PER_REQUEST with ProvisionedThroughput is invalid.
    if matches!(input.billing_mode, Some(BillingMode::PayPerRequest))
        && input.provisioned_throughput.is_some()
    {
        return Err(DynamoDbError::ValidationException(
            "One or more parameter values were invalid: Neither ReadCapacityUnits nor WriteCapacityUnits can be specified when BillingMode is PAY_PER_REQUEST".to_owned(),
        ));
    }

    // Validate throughput values (must be > 0).
    if let Some(ref tp) = input.provisioned_throughput
        && (tp.read_capacity_units < 1 || tp.write_capacity_units < 1)
    {
        return Err(DynamoDbError::ValidationException(
                "One or more parameter values were invalid: ReadCapacityUnits and WriteCapacityUnits must each be at least 1".to_owned(),
            ));
    }

    // Validate GSI updates: each entry must have exactly one of Create, Update, or Delete.
    validate_gsi_updates(&input)?;

    let table_name = input.table_name.clone();
    let desc = ctx
        .storage
        .update_table(&ctx.account_id, input)
        .await
        .map_err(|e| match e {
            extenddb_storage::error::StorageError::TableNotFound(_name) => {
                DynamoDbError::ResourceNotFoundException("Requested resource not found".to_string())
            }
            extenddb_storage::error::StorageError::TableNotActive(name) => {
                DynamoDbError::ResourceInUseException(format!(
                    "Table {name} is not in ACTIVE state"
                ))
            }
            extenddb_storage::error::StorageError::IndexAlreadyExists(name) => {
                DynamoDbError::ValidationException(format!(
                    "One or more parameter values were invalid: Index already exists: {name}"
                ))
            }
            extenddb_storage::error::StorageError::IndexNotFound(name) => {
                DynamoDbError::ResourceNotFoundException(format!(
                    "Requested resource not found: Index {name} for table {}",
                    table_name
                ))
            }
            extenddb_storage::error::StorageError::NoOpUpdate(msg) => {
                DynamoDbError::ValidationException(msg)
            }
            extenddb_storage::error::StorageError::Validation(msg) => {
                DynamoDbError::ValidationException(msg)
            }
            other => crate::storage_other_to_dynamo(other, "update table storage error"),
        })?;

    // Drop the cached TableKeyInfo: index changes, stream-spec changes, and
    // throughput changes all alter what the cached value contains.
    //
    // NOTE: UpdateTable does NOT currently accept Tags. If that ever
    // changes, also invalidate `resource_tags` for the table ARN here —
    // the request itself populates resource_tags during authorize_request,
    // so a stale empty entry would otherwise hide the new tags. See
    // handle_create_table for the same pattern.
    ctx.auth_cache
        .invalidate_table_key_info(&ctx.account_id, &table_name)
        .await;

    let output = extenddb_core::types::UpdateTableOutput {
        table_description: desc,
    };
    serialize_output(&output)
}

#[cfg(test)]
mod tests {
    use extenddb_core::error::DynamoDbError;
    use extenddb_core::types::{
        DeleteGsiAction, GlobalSecondaryIndexUpdate, ProvisionedThroughput, UpdateGsiAction,
        UpdateTableInput,
    };

    use super::validate_gsi_updates;

    fn provisioned(read: i64, write: i64) -> ProvisionedThroughput {
        ProvisionedThroughput {
            read_capacity_units: read,
            write_capacity_units: write,
        }
    }

    fn input(update: GlobalSecondaryIndexUpdate) -> UpdateTableInput {
        UpdateTableInput {
            table_name: "table".to_owned(),
            billing_mode: None,
            provisioned_throughput: None,
            deletion_protection_enabled: None,
            global_secondary_index_updates: Some(vec![update]),
            attribute_definitions: None,
            stream_specification: None,
        }
    }

    #[test]
    fn update_global_secondary_index_throughput_is_supported() {
        validate_gsi_updates(&input(GlobalSecondaryIndexUpdate {
            create: None,
            update: Some(UpdateGsiAction {
                index_name: "by_customer".to_owned(),
                provisioned_throughput: Some(provisioned(7, 9)),
            }),
            delete: None,
        }))
        .expect("throughput update should validate");
    }

    #[test]
    fn update_global_secondary_index_requires_throughput() {
        let err = validate_gsi_updates(&input(GlobalSecondaryIndexUpdate {
            create: None,
            update: Some(UpdateGsiAction {
                index_name: "by_customer".to_owned(),
                provisioned_throughput: None,
            }),
            delete: None,
        }))
        .unwrap_err();

        assert!(matches!(err, DynamoDbError::ValidationException(_)));
        assert!(err.to_string().contains("ProvisionedThroughput"));
    }

    #[test]
    fn update_global_secondary_index_rejects_zero_throughput() {
        let err = validate_gsi_updates(&input(GlobalSecondaryIndexUpdate {
            create: None,
            update: Some(UpdateGsiAction {
                index_name: "by_customer".to_owned(),
                provisioned_throughput: Some(provisioned(0, 1)),
            }),
            delete: None,
        }))
        .unwrap_err();

        assert!(matches!(err, DynamoDbError::ValidationException(_)));
        assert!(err.to_string().contains("at least 1"));
    }

    #[test]
    fn global_secondary_index_update_rejects_multiple_actions() {
        let err = validate_gsi_updates(&input(GlobalSecondaryIndexUpdate {
            create: None,
            update: Some(UpdateGsiAction {
                index_name: "by_customer".to_owned(),
                provisioned_throughput: Some(provisioned(7, 9)),
            }),
            delete: Some(DeleteGsiAction {
                index_name: "by_customer".to_owned(),
            }),
        }))
        .unwrap_err();

        assert!(matches!(err, DynamoDbError::ValidationException(_)));
        assert!(
            err.to_string()
                .contains("Only one of Create, Update, or Delete")
        );
    }
}

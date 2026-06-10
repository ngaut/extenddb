// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! `TagResource`, `UntagResource`, and `ListTagsOfResource` operation handlers.

use extenddb_core::error::DynamoDbError;
use extenddb_core::types::{
    ListTagsOfResourceInput, ListTagsOfResourceOutput, TagResourceInput, UntagResourceInput,
};
use extenddb_core::validation::{validate_tag_keys, validate_tags};
use extenddb_storage::util::table_arn;
use serde_json::Value;

use crate::OperationContext;
use crate::create_table::storage_err_to_dynamo;
use crate::sanitize_storage_error;
use crate::serialize_output;

/// Extract the table name from a `DynamoDB` table ARN.
///
/// Expected format: `arn:aws:dynamodb:{region}:{account}:table/{name}[/...]`
struct TableResourceArn<'a> {
    region: &'a str,
    account_id: &'a str,
    table_name: &'a str,
}

fn parse_table_resource_arn(arn: &str) -> Option<TableResourceArn<'_>> {
    let mut parts = arn.splitn(6, ':');
    let (
        Some("arn"),
        Some("aws"),
        Some("dynamodb"),
        Some(region),
        Some(account_id),
        Some(resource),
    ) = (
        parts.next(),
        parts.next(),
        parts.next(),
        parts.next(),
        parts.next(),
        parts.next(),
    )
    else {
        return None;
    };
    if region.is_empty() || account_id.is_empty() {
        return None;
    }
    let table_resource = resource.strip_prefix("table/")?;
    let table_name = table_resource.split('/').next().unwrap_or(table_resource);
    if table_name.is_empty() {
        return None;
    }
    Some(TableResourceArn {
        region,
        account_id,
        table_name,
    })
}

/// Validate that the ARN refers to an existing table and return its canonical table ARN.
///
/// Returns `ResourceNotFoundException` if the table does not exist.
async fn canonical_resource_arn(
    arn: &str,
    ctx: &OperationContext,
) -> Result<String, DynamoDbError> {
    let parsed = parse_table_resource_arn(arn).ok_or_else(|| {
        DynamoDbError::ValidationException(format!(
            "1 validation error detected: Value '{arn}' at 'resourceArn' failed to satisfy constraint: \
             Member must satisfy regular expression pattern: arn:aws:dynamodb:.+"
        ))
    })?;

    // Check the ARN's account matches the caller's account.
    if parsed.account_id != ctx.account_id.as_ref() {
        return Err(DynamoDbError::AccessDeniedException(
            "Access is denied".to_owned(),
        ));
    }

    // Verify the table exists via table_key_info (lightweight check).
    ctx.table_key_info(parsed.table_name)
        .await
        .map_err(|e| match e {
            extenddb_storage::error::StorageError::TableNotFound(_) => {
                DynamoDbError::ResourceNotFoundException(format!(
                    "Requested resource not found: {arn}"
                ))
            }
            other => sanitize_storage_error(other),
        })?;

    Ok(table_arn(
        parsed.region,
        parsed.account_id,
        parsed.table_name,
    ))
}

/// Handle `TagResource` — add or overwrite tags on a resource.
///
/// # Errors
///
/// Returns `ResourceNotFoundException` if the resource does not exist.
/// Returns `ValidationException` if the resource ARN is empty.
/// Returns `InternalServerError` on storage failures.
pub async fn handle_tag_resource(
    body: Value,
    ctx: &OperationContext,
) -> Result<Value, DynamoDbError> {
    let input: TagResourceInput = serde_json::from_value(body).map_err(crate::deserialize_error)?;

    if input.resource_arn.is_empty() {
        return Err(DynamoDbError::ValidationException(
            "ResourceArn must not be empty".to_owned(),
        ));
    }
    validate_tags(&input.tags, &ctx.limits)?;

    let resource_arn = canonical_resource_arn(&input.resource_arn, ctx).await?;

    ctx.storage
        .tag_resource(&resource_arn, &input.tags)
        .await
        .map_err(storage_err_to_dynamo)?;

    // Drop any cached resource-tag entry so the new tags are visible to
    // ABAC policy evaluation immediately.
    ctx.auth_cache.invalidate_resource_tags(&resource_arn).await;

    // TagResource returns an empty body on success.
    Ok(Value::Object(serde_json::Map::new()))
}

/// Handle `UntagResource` — remove tags by key from a resource.
///
/// # Errors
///
/// Returns `ResourceNotFoundException` if the resource does not exist.
/// Returns `ValidationException` if the resource ARN is empty.
/// Returns `InternalServerError` on storage failures.
pub async fn handle_untag_resource(
    body: Value,
    ctx: &OperationContext,
) -> Result<Value, DynamoDbError> {
    let input: UntagResourceInput =
        serde_json::from_value(body).map_err(crate::deserialize_error)?;

    if input.resource_arn.is_empty() {
        return Err(DynamoDbError::ValidationException(
            "ResourceArn must not be empty".to_owned(),
        ));
    }
    validate_tag_keys(&input.tag_keys, &ctx.limits)?;

    let resource_arn = canonical_resource_arn(&input.resource_arn, ctx).await?;

    ctx.storage
        .untag_resource(&resource_arn, &input.tag_keys)
        .await
        .map_err(storage_err_to_dynamo)?;

    ctx.auth_cache.invalidate_resource_tags(&resource_arn).await;

    // UntagResource returns an empty body on success.
    Ok(Value::Object(serde_json::Map::new()))
}

/// Handle `ListTagsOfResource` — list all tags for a resource.
///
/// # Errors
///
/// Returns `ResourceNotFoundException` if the resource does not exist.
/// Returns `ValidationException` if the resource ARN is empty.
/// Returns `InternalServerError` on storage failures.
pub async fn handle_list_tags_of_resource(
    body: Value,
    ctx: &OperationContext,
) -> Result<Value, DynamoDbError> {
    let input: ListTagsOfResourceInput =
        serde_json::from_value(body).map_err(crate::deserialize_error)?;

    if input.resource_arn.is_empty() {
        return Err(DynamoDbError::ValidationException(
            "ResourceArn must not be empty".to_owned(),
        ));
    }

    let resource_arn = canonical_resource_arn(&input.resource_arn, ctx).await?;

    let tags = ctx
        .storage
        .list_tags(&resource_arn)
        .await
        .map_err(storage_err_to_dynamo)?;

    let output = ListTagsOfResourceOutput {
        tags,
        next_token: None, // All tags returned in one page.
    };
    serialize_output(&output)
}

#[cfg(test)]
mod tests {
    use super::parse_table_resource_arn;
    use extenddb_storage::util::table_arn;

    #[test]
    fn parses_table_and_subresource_arns_to_owning_table() {
        let table = parse_table_resource_arn(
            "arn:aws:dynamodb:us-west-2:123456789012:table/Orders/index/ByDate",
        )
        .expect("index ARN should parse");

        assert_eq!(table.region, "us-west-2");
        assert_eq!(table.account_id, "123456789012");
        assert_eq!(table.table_name, "Orders");
        assert_eq!(
            table_arn(table.region, table.account_id, table.table_name),
            "arn:aws:dynamodb:us-west-2:123456789012:table/Orders"
        );
    }

    #[test]
    fn rejects_non_table_resource_arns() {
        assert!(
            parse_table_resource_arn("arn:aws:dynamodb:us-west-2:123456789012:backup/x").is_none()
        );
        assert!(
            parse_table_resource_arn("arn:aws:s3:us-west-2:123456789012:table/Orders").is_none()
        );
        assert!(
            parse_table_resource_arn("arn:aws:dynamodb:us-west-2:123456789012:table/").is_none()
        );
    }
}

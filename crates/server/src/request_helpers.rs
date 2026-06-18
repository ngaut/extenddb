// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Request parsing and authorization helpers for the `DynamoDB` wire protocol.

use axum::http::HeaderMap;
use extenddb_core::error::DynamoDbError;
use extenddb_core::types::{TableKeyInfo, TableReadInfo};
use serde_json::Value;
use std::sync::Arc;

use crate::AppState;
use crate::authorization;
use crate::authz_request_context::{
    DynamoDbResource, authorization_resources_for_operation, build_nested_authorization_targets,
    extract_attributes, extract_leading_keys, extract_select, optional_auth_metadata,
    request_params,
};

/// Extract operation name from X-Amz-Target header.
/// Accepts both `DynamoDB_20120810` and `DynamoDBStreams_20120810` wire-format prefixes.
pub(crate) fn extract_operation(headers: &HeaderMap) -> Result<String, DynamoDbError> {
    let target = headers
        .get("x-amz-target")
        .and_then(|v| v.to_str().ok())
        .ok_or_else(|| {
            // Real DynamoDB returns MissingAuthenticationToken only when auth headers
            // are also absent. When auth headers are present but X-Amz-Target is
            // missing, it returns UnknownOperationException.
            if headers.contains_key("authorization") {
                DynamoDbError::UnknownOperationException(String::new())
            } else {
                DynamoDbError::MissingAuthenticationToken("Missing Authentication Token".to_owned())
            }
        })?;

    target
        .strip_prefix("DynamoDB_20120810.")
        .or_else(|| target.strip_prefix("DynamoDBStreams_20120810."))
        .map(std::borrow::ToOwned::to_owned)
        .ok_or_else(|| DynamoDbError::UnknownOperationException(String::new()))
}

/// Extract the top-level `TableName` from a `DynamoDB` request body.
///
/// This is used for request metrics. Authorization uses typed operation
/// resources from `authz_request_context`.
pub(crate) fn extract_table_name(input: &Value) -> Option<String> {
    input
        .get("TableName")
        .and_then(|v| v.as_str())
        .map(ToOwned::to_owned)
}

pub(crate) struct AuthorizedOperationMetadata {
    pub account_id: Arc<str>,
    pub read_info: Option<TableReadInfo>,
    pub write_info: Option<TableKeyInfo>,
}

struct AuthorizationPrefetch {
    read_info: Option<TableReadInfo>,
    write_info: Option<TableKeyInfo>,
}

pub(crate) async fn authorize_operation_metadata(
    state: &AppState,
    identity: &extenddb_auth::AuthIdentity,
    input: &Value,
    operation: &str,
) -> Result<AuthorizedOperationMetadata, DynamoDbError> {
    if state.catalog_store.is_none() {
        tracing::error!("Authorization required but catalog_store is not configured");
        return Err(DynamoDbError::AccessDeniedException(
            "User: is not authorized to perform this operation".to_owned(),
        ));
    }

    let account_id: Arc<str> = match identity {
        extenddb_auth::AuthIdentity::User { account_id, .. }
        | extenddb_auth::AuthIdentity::RoleSession { account_id, .. } => {
            Arc::from(account_id.as_str())
        }
    };
    let prefetch = authorize_request(state, identity, input, operation, &account_id).await?;

    Ok(AuthorizedOperationMetadata {
        account_id,
        read_info: prefetch.read_info,
        write_info: prefetch.write_info,
    })
}

/// Evaluate IAM policies for an authenticated identity.
///
/// Returns pre-fetched table metadata for single-table item-level operations.
/// The caller passes this into `OperationContext` to avoid redundant catalog
/// roundtrips in the engine layer. Read operations fetch lightweight read
/// metadata; write operations that can change secondary-index keys fetch
/// explicit write metadata.
///
/// All authorization data is fetched via `state.authz_cache`, which sits on
/// top of the underlying `AuthorizationStore` and serves cached, pre-parsed
/// `PolicyDocument`s.
async fn authorize_request(
    state: &AppState,
    identity: &extenddb_auth::AuthIdentity,
    input: &Value,
    operation: &str,
    account_id: &str,
) -> Result<AuthorizationPrefetch, DynamoDbError> {
    let resources = authorization_resources_for_operation(input, operation, account_id)?;
    let primary_resource = resources
        .first()
        .cloned()
        .unwrap_or(DynamoDbResource::TableWildcard);
    let table_name = primary_resource.table_name().map(ToOwned::to_owned);
    let index_name = primary_resource.index_name().map(ToOwned::to_owned);

    // Fetch table metadata for item-level operations. The result is both used
    // for LeadingKeys extraction here and returned to the caller to avoid a
    // redundant fetch in the engine layer. Metadata comes directly from TiDB so
    // distributed frontends do not need cross-node table-metadata invalidation.
    let (read_info, write_info) = match operation {
        "PutItem" | "UpdateItem" | "DeleteItem" => {
            let write_info = if let Some(ref tn) = table_name {
                optional_auth_metadata(state.storage.table_write_info(account_id, tn).await)?
            } else {
                None
            };
            (None, write_info)
        }
        "GetItem" => {
            if let Some(ref tn) = table_name {
                let read_info =
                    optional_auth_metadata(state.storage.table_key_info(account_id, tn).await)?
                        .map(|table| TableReadInfo { table, index: None });
                (read_info, None)
            } else {
                (None, None)
            }
        }
        "Query" | "Scan" => {
            if let Some(ref tn) = table_name {
                let read_info = if index_name.is_some() {
                    optional_auth_metadata(
                        state
                            .storage
                            .table_read_info(account_id, tn, index_name.as_deref())
                            .await,
                    )?
                } else {
                    optional_auth_metadata(state.storage.table_key_info(account_id, tn).await)?
                        .map(|table| TableReadInfo { table, index: None })
                };
                (read_info, None)
            } else {
                (None, None)
            }
        }
        _ => (None, None),
    };

    let prepared_authorization =
        authorization::prepare_authorization(state.authz_cache.as_ref(), identity).await?;

    if let Some(targets) =
        build_nested_authorization_targets(state, input, operation, account_id).await?
    {
        for target in targets {
            let target_operation = target.operation;
            let params = request_params(
                input,
                &target_operation,
                target.leading_keys,
                target.attributes,
                target.select,
                target.enclosing_operation,
            );
            let resource_arn = target.resource.policy_arn(&state.region, account_id);
            let tag_resource_arn = target.resource.tag_arn(&state.region, account_id);
            authorization::check_prepared_authorization(
                state.authz_cache.as_ref(),
                &target_operation,
                &prepared_authorization,
                authorization::AuthorizationResource {
                    policy_arn: &resource_arn,
                    tag_arn: &tag_resource_arn,
                },
                false,
                params,
            )
            .await?;
        }

        return Ok(AuthorizationPrefetch {
            read_info,
            write_info,
        });
    }

    let key_info = read_info
        .as_ref()
        .map(|info| &info.table)
        .or(write_info.as_ref());

    let leading_keys = extract_leading_keys(
        input,
        operation,
        key_info,
        read_info.as_ref(),
        state.limits.as_ref(),
    );
    let attributes = extract_attributes(input, operation, state.limits.as_ref())?;
    let select = extract_select(input, operation, read_info.as_ref());

    for resource in resources {
        let params = request_params(
            input,
            operation,
            leading_keys.clone(),
            attributes.clone(),
            select.clone(),
            None,
        );
        let resource_arn = resource.policy_arn(&state.region, account_id);
        let tag_resource_arn = resource.tag_arn(&state.region, account_id);
        authorization::check_prepared_authorization(
            state.authz_cache.as_ref(),
            operation,
            &prepared_authorization,
            authorization::AuthorizationResource {
                policy_arn: &resource_arn,
                tag_arn: &tag_resource_arn,
            },
            operation == "Scan",
            params,
        )
        .await?;
    }

    Ok(AuthorizationPrefetch {
        read_info,
        write_info,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn missing_target_no_auth_returns_missing_auth_token() {
        // No Authorization header + no X-Amz-Target → MissingAuthenticationToken.
        let headers = HeaderMap::new();
        let err = extract_operation(&headers).unwrap_err();
        assert!(
            matches!(err, DynamoDbError::MissingAuthenticationToken(_)),
            "Expected MissingAuthenticationToken, got: {err:?}"
        );
    }

    #[test]
    fn missing_target_with_auth_returns_unknown_operation() {
        // Authorization header present but no X-Amz-Target → UnknownOperationException.
        use axum::http::HeaderValue;
        let mut headers = HeaderMap::new();
        headers.insert("authorization", HeaderValue::from_static("AWS4-HMAC-SHA256 Credential=AKID/20260415/us-east-1/dynamodb/aws4_request, SignedHeaders=host, Signature=abc"));
        let err = extract_operation(&headers).unwrap_err();
        assert!(
            matches!(err, DynamoDbError::UnknownOperationException(_)),
            "Expected UnknownOperationException, got: {err:?}"
        );
    }
}

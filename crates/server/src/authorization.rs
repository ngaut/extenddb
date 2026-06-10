// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Authorization layer for DynamoDB requests.
//!
//! After authentication resolves an `AuthIdentity`, this module fetches the
//! applicable IAM policies, permissions boundary, and session policy via
//! [`CachedAuthzStore`] (which sits on top of the storage [`AuthorizationStore`]
//! trait), builds a `RequestContext`, and evaluates authorization using the
//! policy engine from `extenddb-auth`.
//!
//! All policy documents are pre-parsed by the cache, so this layer never
//! invokes `PolicyDocument::from_json` on the request hot path.
//!
//! See `docs/design/12-auth-authz-cache.md`.

use std::collections::HashMap;
use std::sync::Arc;

use extenddb_auth::AuthIdentity;
use extenddb_auth::policy::context::{RequestContext, RequestParams};
use extenddb_auth::policy::document::PolicyDocument;
use extenddb_auth::policy::evaluator::{AuthzDecision, evaluate_policies_arc};
use extenddb_core::error::DynamoDbError;
use extenddb_storage::management_store::OpError;

use crate::authz_cache::{CachedAuthzStore, PolicyList, TagMap};

#[derive(Clone, Copy)]
pub(crate) struct AuthorizationResource<'a> {
    pub(crate) policy_arn: &'a str,
    pub(crate) tag_arn: &'a str,
}

pub(crate) struct PreparedAuthorization {
    principal_arn: String,
    identity_policies: Vec<Arc<PolicyDocument>>,
    boundary: Option<Arc<PolicyDocument>>,
    session_policy: Option<Arc<PolicyDocument>>,
    principal_tags: HashMap<String, String>,
}

/// Fetch all principal-scoped authorization inputs once for a DynamoDB request.
///
/// Resource tags still depend on the target resource and are fetched per
/// resource check. The identity policies, permissions boundary, session policy,
/// and principal tags are invariant across every nested target in a batch or
/// transaction request, so preparing them once avoids repeated cache/catalog
/// work and repeated policy-list assembly.
pub(crate) async fn prepare_authorization(
    cache: &CachedAuthzStore,
    identity: &AuthIdentity,
) -> Result<PreparedAuthorization, DynamoDbError> {
    match identity {
        AuthIdentity::User {
            account_id,
            user_name,
        } => {
            let (user_policies, group_policies, boundary, principal_tags) = tokio::try_join!(
                wrap_policies(cache.fetch_user_policies(account_id, user_name)),
                wrap_policies(cache.fetch_user_group_policies(account_id, user_name)),
                wrap_boundary(cache.fetch_user_boundary(account_id, user_name)),
                wrap_tags(cache.fetch_user_tags(account_id, user_name)),
            )?;

            let mut identity_policies =
                Vec::with_capacity(user_policies.len() + group_policies.len());
            identity_policies.extend(user_policies.iter().cloned());
            identity_policies.extend(group_policies.iter().cloned());

            Ok(PreparedAuthorization {
                principal_arn: format!("arn:aws:iam::{account_id}:user/{user_name}"),
                identity_policies,
                boundary,
                session_policy: None,
                principal_tags: (*principal_tags).clone(),
            })
        }
        AuthIdentity::RoleSession {
            account_id,
            role_name,
            session_name,
            access_key_id,
        } => {
            let (identity_policies, boundary, (session_policy, principal_tags)) = tokio::try_join!(
                wrap_policies(cache.fetch_role_policies(account_id, role_name)),
                wrap_boundary(cache.fetch_role_boundary(account_id, role_name)),
                fetch_session_data_and_tags(
                    cache,
                    account_id,
                    role_name,
                    session_name,
                    access_key_id
                ),
            )?;

            Ok(PreparedAuthorization {
                principal_arn: format!(
                    "arn:aws:iam::{account_id}:assumed-role/{role_name}/{session_name}"
                ),
                identity_policies: identity_policies.iter().cloned().collect(),
                boundary,
                session_policy,
                principal_tags,
            })
        }
    }
}

/// Evaluate a target resource against request-scoped authorization inputs.
pub(crate) async fn check_prepared_authorization(
    cache: &CachedAuthzStore,
    operation: &str,
    prepared: &PreparedAuthorization,
    resource: AuthorizationResource<'_>,
    is_scan: bool,
    params: RequestParams,
) -> Result<(), DynamoDbError> {
    let action = format!("dynamodb:{operation}");

    let resource_tags = wrap_tags(cache.fetch_resource_tags(resource.tag_arn)).await?;

    let context = RequestContext::build(
        prepared.principal_tags.clone(),
        (*resource_tags).clone(),
        is_scan,
        params,
    );

    let decision = evaluate_policies_arc(
        &prepared.identity_policies,
        prepared.boundary.as_deref(),
        prepared.session_policy.as_deref(),
        &action,
        resource.policy_arn,
        &context,
    );

    if decision == AuthzDecision::Allow {
        Ok(())
    } else {
        tracing::warn!(
            principal = prepared.principal_arn.as_str(),
            action = action,
            resource = resource.policy_arn,
            "Authorization denied"
        );
        Err(DynamoDbError::AccessDeniedException(format!(
            "User: {} is not authorized \
             to perform: {action} on resource: {}",
            prepared.principal_arn, resource.policy_arn
        )))
    }
}

fn op_err_to_dynamo(e: OpError) -> DynamoDbError {
    match &e {
        OpError::Internal(msg) if msg.starts_with("policy parse failed") => {
            tracing::error!("Authorization: {msg}");
            DynamoDbError::AccessDeniedException(
                "Not authorized to perform this action (policy evaluation error)".to_owned(),
            )
        }
        _ => {
            tracing::error!("Authorization: cache load failed: {e:?}");
            DynamoDbError::InternalServerError("Internal error during authorization".to_owned())
        }
    }
}

async fn wrap_policies(
    fut: impl std::future::Future<Output = extenddb_storage::management_store::OpResult<PolicyList>>,
) -> Result<PolicyList, DynamoDbError> {
    fut.await.map_err(op_err_to_dynamo)
}

async fn wrap_boundary(
    fut: impl std::future::Future<
        Output = extenddb_storage::management_store::OpResult<
            Option<std::sync::Arc<extenddb_auth::policy::document::PolicyDocument>>,
        >,
    >,
) -> Result<Option<std::sync::Arc<extenddb_auth::policy::document::PolicyDocument>>, DynamoDbError>
{
    fut.await.map_err(op_err_to_dynamo)
}

async fn wrap_tags(
    fut: impl std::future::Future<Output = extenddb_storage::management_store::OpResult<TagMap>>,
) -> Result<TagMap, DynamoDbError> {
    fut.await.map_err(op_err_to_dynamo)
}

async fn fetch_session_data_and_tags(
    cache: &CachedAuthzStore,
    account_id: &str,
    role_name: &str,
    session_name: &str,
    access_key_id: &str,
) -> Result<
    (
        Option<std::sync::Arc<extenddb_auth::policy::document::PolicyDocument>>,
        HashMap<String, String>,
    ),
    DynamoDbError,
> {
    let (role_tags, session_data) = tokio::try_join!(
        wrap_tags(cache.fetch_role_tags(account_id, role_name)),
        async {
            cache
                .fetch_session_data(account_id, role_name, session_name, access_key_id)
                .await
                .map_err(op_err_to_dynamo)
        },
    )?;

    let mut tags: HashMap<String, String> = (*role_tags).clone();
    let mut session_policy = None;
    if let Some(data) = session_data {
        session_policy = data.session_policy.clone();
        for (k, v) in &data.session_tags {
            tags.insert(k.clone(), v.clone());
        }
    }
    Ok((session_policy, tags))
}

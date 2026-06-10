// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! DynamoDB request context extraction for IAM condition keys.

use std::collections::hash_map::Entry;
use std::collections::{HashMap, HashSet};

use base64::Engine;
use base64::engine::general_purpose::STANDARD as BASE64;
use extenddb_auth::policy::context::RequestParams;
use extenddb_core::error::DynamoDbError;
use extenddb_core::expression::{
    Expr, ExpressionKind, ExpressionMaps, PathElement, UpdateAction, parse_condition,
    parse_key_condition, parse_projection, parse_update_from, tokenize_for_with_limits,
    validate_no_reserved_words,
};
use extenddb_core::limits::LimitsConfig;
use extenddb_core::types::{
    AttributeValue, AttributeValueUpdate, BatchGetItemInput, BatchWriteItemInput, Condition,
    DeleteItemInput, ExpectedAttributeValue, GetItemInput, Item, KeySchemaElement, KeyType,
    PutItemInput, QueryInput, ScanInput, TableKeyInfo, TableReadInfo, TransactGetItemsInput,
    TransactWriteItem, TransactWriteItemsInput, UpdateItemInput,
};
use extenddb_storage::error::StorageError;
use futures::future::join_all;
use serde::Deserialize;
use serde_json::Value;

use crate::AppState;

pub(crate) struct AuthorizationTarget {
    pub(crate) operation: String,
    pub(crate) resource: DynamoDbResource,
    pub(crate) leading_keys: Option<Vec<String>>,
    pub(crate) attributes: Option<Vec<String>>,
    pub(crate) select: Option<String>,
    pub(crate) enclosing_operation: Option<String>,
}

impl AuthorizationTarget {
    fn wildcard(operation: &str) -> Self {
        Self {
            operation: operation.to_owned(),
            resource: DynamoDbResource::TableWildcard,
            leading_keys: None,
            attributes: None,
            select: None,
            enclosing_operation: None,
        }
    }
}

struct NestedTargetBuilder {
    operation: String,
    table_name: String,
    key_info: Option<TableKeyInfo>,
    leading_keys: Vec<String>,
    leading_keys_known: bool,
    attributes: Option<Vec<String>>,
    select: Option<String>,
    enclosing_operation: Option<String>,
}

impl NestedTargetBuilder {
    fn new(
        operation: String,
        table_name: String,
        key_info: Option<TableKeyInfo>,
        enclosing_operation: Option<String>,
    ) -> Self {
        let leading_keys_known = key_info.is_some();
        Self {
            operation,
            table_name,
            key_info,
            leading_keys: Vec::new(),
            leading_keys_known,
            attributes: None,
            select: None,
            enclosing_operation,
        }
    }

    fn add_leading_keys(&mut self, values: Option<Vec<String>>) {
        match values {
            Some(values) if self.leading_keys_known => self.leading_keys.extend(values),
            _ => self.leading_keys_known = false,
        }
    }

    fn add_attributes(&mut self, attributes: Option<Vec<String>>) {
        let Some(attributes) = attributes else {
            return;
        };
        self.attributes
            .get_or_insert_with(Vec::new)
            .extend(attributes);
    }

    fn add_select(&mut self, select: Option<String>) {
        self.select = merge_select(self.select.take(), select);
    }

    fn build(self) -> AuthorizationTarget {
        AuthorizationTarget {
            operation: self.operation,
            resource: DynamoDbResource::Table(self.table_name),
            leading_keys: self.leading_keys_known.then_some(self.leading_keys),
            attributes: self.attributes.map(dedup_preserve_order),
            select: self.select,
            enclosing_operation: self.enclosing_operation,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum DynamoDbResource {
    Table(String),
    Index {
        table_name: String,
        index_name: String,
    },
    ExactArn {
        policy_arn: String,
        tag_arn: String,
    },
    TableWildcard,
}

impl DynamoDbResource {
    pub(crate) fn table_name(&self) -> Option<&str> {
        match self {
            Self::Table(table_name) | Self::Index { table_name, .. } => Some(table_name),
            Self::ExactArn { .. } | Self::TableWildcard => None,
        }
    }

    pub(crate) fn index_name(&self) -> Option<&str> {
        match self {
            Self::Index { index_name, .. } => Some(index_name),
            Self::Table(_) | Self::ExactArn { .. } | Self::TableWildcard => None,
        }
    }

    pub(crate) fn policy_arn(&self, region: &str, account_id: &str) -> String {
        match self {
            Self::Table(table_name) => table_arn(region, account_id, table_name),
            Self::Index {
                table_name,
                index_name,
            } => format!("{}{}", table_arn(region, account_id, table_name), "/index/") + index_name,
            Self::ExactArn { policy_arn, .. } => policy_arn.clone(),
            Self::TableWildcard => format!("arn:aws:dynamodb:{region}:{account_id}:table/*"),
        }
    }

    pub(crate) fn tag_arn(&self, region: &str, account_id: &str) -> String {
        match self {
            Self::Table(table_name) | Self::Index { table_name, .. } => {
                table_arn(region, account_id, table_name)
            }
            Self::ExactArn { tag_arn, .. } => tag_arn.clone(),
            Self::TableWildcard => format!("arn:aws:dynamodb:{region}:{account_id}:table/*"),
        }
    }
}

pub(crate) fn authorization_resources_for_operation(
    input: &Value,
    operation: &str,
    account_id: &str,
) -> Result<Vec<DynamoDbResource>, DynamoDbError> {
    let resources = match operation {
        "CreateTable" | "DeleteTable" | "DescribeTable" | "UpdateTable" | "DescribeTimeToLive"
        | "UpdateTimeToLive" | "PutItem" | "GetItem" | "DeleteItem" | "UpdateItem" => {
            table_name_resource(input)
        }
        "Query" | "Scan" => table_index_resource(input),
        "TagResource" | "UntagResource" | "ListTagsOfResource" => {
            exact_arn_from_field(input, account_id, "resource", "ResourceArn")?
        }
        "DescribeStream" | "GetShardIterator" => {
            exact_arn_from_field(input, account_id, "stream", "StreamArn")?
        }
        "ListStreams" => optional_table_name_resource(input),
        "GetRecords" => {
            if let Some(shard_iterator) = string_field(input, "ShardIterator") {
                if let Some(stream_arn) = stream_arn_from_shard_iterator(&shard_iterator) {
                    exact_arn_resource(&stream_arn, account_id, "stream")?
                } else {
                    table_wildcard()
                }
            } else {
                table_wildcard()
            }
        }
        "ImportTable" => table_from::<ImportTableAuthInput>(input, |input| {
            input.table_creation_parameters.table_name
        }),
        "ExportTableToPointInTime" => exact_arn_from_field(input, account_id, "table", "TableArn")?,
        "CreateBackup" => table_name_resource(input),
        "DescribeBackup" | "DeleteBackup" => {
            exact_arn_from_field(input, account_id, "backup", "BackupArn")?
        }
        "ListBackups" => optional_table_name_resource(input),
        "RestoreTableFromBackup" => {
            if let Ok(input) =
                serde_json::from_value::<RestoreTableFromBackupAuthInput>(input.clone())
            {
                let mut resources = exact_arn_resource(&input.backup_arn, account_id, "backup")?;
                resources.push(DynamoDbResource::Table(input.target_table_name));
                resources
            } else {
                table_wildcard()
            }
        }
        "DescribeContinuousBackups" | "UpdateContinuousBackups" => table_name_resource(input),
        "RestoreTableToPointInTime" => {
            if let Ok(input) =
                serde_json::from_value::<RestoreTableToPointInTimeAuthInput>(input.clone())
            {
                restore_to_point_in_time_resources(input, account_id)?
            } else {
                table_wildcard()
            }
        }
        _ => table_wildcard(),
    };
    Ok(resources)
}

pub(crate) fn request_params(
    input: &Value,
    operation: &str,
    leading_keys: Option<Vec<String>>,
    attributes: Option<Vec<String>>,
    select: Option<String>,
    enclosing_operation: Option<String>,
) -> RequestParams {
    RequestParams {
        leading_keys,
        attributes,
        select,
        return_values: extract_return_values(input, operation),
        return_consumed_capacity: extract_return_consumed_capacity(input, operation),
        enclosing_operation,
    }
}

fn table_from<T>(input: &Value, table_name: impl FnOnce(T) -> String) -> Vec<DynamoDbResource>
where
    T: serde::de::DeserializeOwned,
{
    serde_json::from_value::<T>(input.clone()).map_or_else(
        |_| table_wildcard(),
        |input| vec![DynamoDbResource::Table(table_name(input))],
    )
}

fn string_field(input: &Value, field: &str) -> Option<String> {
    input
        .get(field)
        .and_then(Value::as_str)
        .map(ToOwned::to_owned)
}

fn table_name_resource(input: &Value) -> Vec<DynamoDbResource> {
    string_field(input, "TableName").map_or_else(table_wildcard, |table_name| {
        vec![DynamoDbResource::Table(table_name)]
    })
}

fn optional_table_name_resource(input: &Value) -> Vec<DynamoDbResource> {
    table_name_resource(input)
}

fn table_index_resource(input: &Value) -> Vec<DynamoDbResource> {
    let Some(table_name) = string_field(input, "TableName") else {
        return table_wildcard();
    };
    match string_field(input, "IndexName") {
        Some(index_name) => vec![DynamoDbResource::Index {
            table_name,
            index_name,
        }],
        None => vec![DynamoDbResource::Table(table_name)],
    }
}

fn exact_arn_from_field(
    input: &Value,
    account_id: &str,
    kind: &str,
    field: &str,
) -> Result<Vec<DynamoDbResource>, DynamoDbError> {
    string_field(input, field).map_or_else(
        || Ok(table_wildcard()),
        |arn| exact_arn_resource(&arn, account_id, kind),
    )
}

fn exact_arn_resource(
    arn: &str,
    account_id: &str,
    kind: &str,
) -> Result<Vec<DynamoDbResource>, DynamoDbError> {
    let parsed = parse_dynamodb_arn(arn, kind)?;
    if parsed.account_id != account_id {
        return Err(DynamoDbError::AccessDeniedException(
            "Access is denied".to_owned(),
        ));
    }
    let tag_arn = table_tag_arn(parsed.region, parsed.account_id, parsed.resource)
        .unwrap_or_else(|| arn.to_owned());
    Ok(vec![DynamoDbResource::ExactArn {
        policy_arn: arn.to_owned(),
        tag_arn,
    }])
}

fn table_wildcard() -> Vec<DynamoDbResource> {
    vec![DynamoDbResource::TableWildcard]
}

fn table_arn(region: &str, account_id: &str, table_name: &str) -> String {
    format!("arn:aws:dynamodb:{region}:{account_id}:table/{table_name}")
}

fn table_tag_arn(region: &str, account_id: &str, resource: &str) -> Option<String> {
    let mut parts = resource.split('/');
    match (parts.next(), parts.next()) {
        (Some("table"), Some(table_name)) if !table_name.is_empty() => {
            Some(table_arn(region, account_id, table_name))
        }
        _ => None,
    }
}

struct ParsedDynamoDbArn<'a> {
    region: &'a str,
    account_id: &'a str,
    resource: &'a str,
}

fn parse_dynamodb_arn<'a>(
    arn: &'a str,
    kind: &str,
) -> Result<ParsedDynamoDbArn<'a>, DynamoDbError> {
    let mut parts = arn.splitn(6, ':');
    match (
        parts.next(),
        parts.next(),
        parts.next(),
        parts.next(),
        parts.next(),
        parts.next(),
    ) {
        (
            Some("arn"),
            Some(partition),
            Some("dynamodb"),
            Some(region),
            Some(account_id),
            Some(resource),
        ) if !partition.is_empty()
            && !region.is_empty()
            && !account_id.is_empty()
            && !resource.is_empty() =>
        {
            Ok(ParsedDynamoDbArn {
                region,
                account_id,
                resource,
            })
        }
        _ => Err(DynamoDbError::ValidationException(format!(
            "Invalid {kind} ARN: {arn}"
        ))),
    }
}

fn restore_to_point_in_time_resources(
    input: RestoreTableToPointInTimeAuthInput,
    account_id: &str,
) -> Result<Vec<DynamoDbResource>, DynamoDbError> {
    let mut resources = Vec::with_capacity(2);
    match (input.source_table_name, input.source_table_arn) {
        (Some(name), None) => resources.push(DynamoDbResource::Table(name)),
        (None, Some(arn)) => {
            resources.extend(exact_arn_resource(&arn, account_id, "source table")?)
        }
        (Some(name), Some(arn)) => {
            resources.extend(exact_arn_resource(&arn, account_id, "source table")?);
            resources.push(DynamoDbResource::Table(name));
        }
        (None, None) => return Ok(table_wildcard()),
    }
    resources.push(DynamoDbResource::Table(input.target_table_name));
    Ok(resources)
}

fn stream_arn_from_shard_iterator(iterator: &str) -> Option<String> {
    let decoded = BASE64.decode(iterator).ok()?;
    let token = String::from_utf8(decoded).ok()?;
    let parts: Vec<&str> = token.splitn(6, '|').collect();
    if parts.len() == 6 && parts[1] == "AFTER_SEQUENCE_NUMBER" && !parts[4].is_empty() {
        Some(parts[4].to_owned())
    } else {
        None
    }
}

fn extract_return_values(input: &Value, operation: &str) -> Option<String> {
    if !matches!(operation, "PutItem" | "DeleteItem" | "UpdateItem") {
        return None;
    }
    let value = input.get("ReturnValues")?.as_str()?;
    match value {
        "NONE" | "ALL_OLD" | "ALL_NEW" | "UPDATED_OLD" | "UPDATED_NEW" => Some(value.to_owned()),
        _ => None,
    }
}

fn extract_return_consumed_capacity(input: &Value, operation: &str) -> Option<String> {
    if !matches!(
        operation,
        "PutItem"
            | "GetItem"
            | "DeleteItem"
            | "UpdateItem"
            | "ConditionCheckItem"
            | "Query"
            | "Scan"
            | "BatchGetItem"
            | "BatchWriteItem"
            | "TransactGetItems"
            | "TransactWriteItems"
    ) {
        return None;
    }
    let value = input.get("ReturnConsumedCapacity")?.as_str()?;
    match value {
        "NONE" | "TOTAL" | "INDEXES" => Some(value.to_owned()),
        _ => None,
    }
}

#[derive(Debug, Deserialize)]
struct ImportTableAuthInput {
    #[serde(rename = "TableCreationParameters")]
    table_creation_parameters: ImportTableCreationAuthInput,
}

#[derive(Debug, Deserialize)]
struct ImportTableCreationAuthInput {
    #[serde(rename = "TableName")]
    table_name: String,
}

#[derive(Debug, Deserialize)]
struct RestoreTableFromBackupAuthInput {
    #[serde(rename = "TargetTableName")]
    target_table_name: String,
    #[serde(rename = "BackupArn")]
    backup_arn: String,
}

#[derive(Debug, Deserialize)]
struct RestoreTableToPointInTimeAuthInput {
    #[serde(rename = "TargetTableName")]
    target_table_name: String,
    #[serde(rename = "SourceTableName")]
    source_table_name: Option<String>,
    #[serde(rename = "SourceTableArn")]
    source_table_arn: Option<String>,
}

pub(crate) fn optional_auth_metadata<T>(
    result: Result<T, StorageError>,
) -> Result<Option<T>, DynamoDbError> {
    match result {
        Ok(value) => Ok(Some(value)),
        Err(
            StorageError::TableNotFound(_)
            | StorageError::TableNotActive(_)
            | StorageError::IndexNotFound(_),
        ) => Ok(None),
        Err(error) => Err(auth_metadata_error_to_dynamo(error)),
    }
}

fn auth_metadata_error_to_dynamo(error: StorageError) -> DynamoDbError {
    match error {
        StorageError::Connection(message) | StorageError::Unavailable(message) => {
            tracing::error!(
                internal_error = %message,
                context = "authorization metadata lookup",
                "storage backend unavailable"
            );
            DynamoDbError::ServiceUnavailable("Service is temporarily unavailable".to_owned())
        }
        StorageError::Internal(message)
            if message.contains("pool timed out while waiting for an open connection")
                || message.contains("attempted to acquire a connection on a closed pool") =>
        {
            tracing::error!(
                internal_error = %message,
                context = "authorization metadata lookup",
                "storage backend unavailable"
            );
            DynamoDbError::ServiceUnavailable("Service is temporarily unavailable".to_owned())
        }
        other => {
            tracing::error!(
                internal_error = %other,
                context = "authorization metadata lookup",
                "storage metadata lookup failed during authorization"
            );
            DynamoDbError::InternalServerError("Internal server error".to_owned())
        }
    }
}

pub(crate) async fn build_nested_authorization_targets(
    state: &AppState,
    input: &Value,
    operation: &str,
    account_id: &str,
) -> Result<Option<Vec<AuthorizationTarget>>, DynamoDbError> {
    match operation {
        "BatchGetItem" => Ok(Some(
            build_batch_get_authorization_targets(state, input, account_id).await?,
        )),
        "BatchWriteItem" => Ok(Some(
            build_batch_write_authorization_targets(state, input, account_id).await?,
        )),
        "TransactGetItems" => Ok(Some(
            build_transact_get_authorization_targets(state, input, account_id).await?,
        )),
        "TransactWriteItems" => Ok(Some(
            build_transact_write_authorization_targets(state, input, account_id).await?,
        )),
        _ => Ok(None),
    }
}

async fn key_infos_for_tables(
    state: &AppState,
    account_id: &str,
    table_names: impl IntoIterator<Item = String>,
) -> Result<HashMap<String, Option<TableKeyInfo>>, DynamoDbError> {
    let mut table_names = table_names.into_iter().collect::<Vec<_>>();
    table_names.sort();
    table_names.dedup();

    let results = join_all(table_names.into_iter().map(|table_name| async move {
        let key_info = optional_auth_metadata(
            state
                .table_key_info_cache
                .get(account_id, &table_name)
                .await,
        )?;
        Ok::<_, DynamoDbError>((table_name, key_info))
    }))
    .await;

    let mut key_infos = HashMap::with_capacity(results.len());
    for result in results {
        let (table_name, key_info) = result?;
        key_infos.insert(table_name, key_info);
    }
    Ok(key_infos)
}

fn builder_for_table(
    builders: &mut HashMap<(String, String), NestedTargetBuilder>,
    key_infos: &HashMap<String, Option<TableKeyInfo>>,
    table_name: &str,
    operation: &str,
    enclosing_operation: Option<&str>,
) {
    let builder_key = (table_name.to_owned(), operation.to_owned());
    if let Entry::Vacant(entry) = builders.entry(builder_key) {
        let key_info = key_infos.get(table_name).cloned().flatten();
        entry.insert(NestedTargetBuilder::new(
            operation.to_owned(),
            table_name.to_owned(),
            key_info,
            enclosing_operation.map(ToOwned::to_owned),
        ));
    }
}

async fn build_batch_get_authorization_targets(
    state: &AppState,
    input: &Value,
    account_id: &str,
) -> Result<Vec<AuthorizationTarget>, DynamoDbError> {
    let Ok(input) = serde_json::from_value::<BatchGetItemInput>(input.clone()) else {
        return Ok(vec![AuthorizationTarget::wildcard("BatchGetItem")]);
    };
    let key_infos =
        key_infos_for_tables(state, account_id, input.request_items.keys().cloned()).await?;
    let mut builders = HashMap::new();
    for (table_name, request) in input.request_items {
        builder_for_table(&mut builders, &key_infos, &table_name, "BatchGetItem", None);
        if let Some(builder) = builders.get_mut(&(table_name.clone(), "BatchGetItem".to_owned())) {
            let key_info = builder.key_info.clone();
            for key in &request.keys {
                builder.add_attributes(item_attribute_names(key));
            }
            if let Some(ref key_info) = key_info {
                for key in &request.keys {
                    builder.add_leading_keys(extract_hash_key_values_from_item(key, key_info));
                }
            } else {
                builder.add_leading_keys(None);
            }
            builder.add_attributes(extract_attributes_from_parts(
                request.projection_expression.as_deref(),
                request.expression_attribute_names.as_ref(),
                request.attributes_to_get.as_ref(),
                state.limits.as_ref(),
            )?);
            builder.add_select(Some(read_select_from_parts(
                None,
                request.projection_expression.as_deref(),
                request.attributes_to_get.as_ref(),
                false,
            )));
        }
    }
    Ok(build_nested_targets(builders, "BatchGetItem"))
}

async fn build_batch_write_authorization_targets(
    state: &AppState,
    input: &Value,
    account_id: &str,
) -> Result<Vec<AuthorizationTarget>, DynamoDbError> {
    let Ok(input) = serde_json::from_value::<BatchWriteItemInput>(input.clone()) else {
        return Ok(vec![AuthorizationTarget::wildcard("BatchWriteItem")]);
    };
    let key_infos =
        key_infos_for_tables(state, account_id, input.request_items.keys().cloned()).await?;
    let mut builders = HashMap::new();
    for (table_name, requests) in input.request_items {
        builder_for_table(
            &mut builders,
            &key_infos,
            &table_name,
            "BatchWriteItem",
            None,
        );
        if let Some(builder) = builders.get_mut(&(table_name.clone(), "BatchWriteItem".to_owned()))
        {
            let key_info = builder.key_info.clone();
            if let Some(ref key_info) = key_info {
                for request in &requests {
                    if let Some(put) = &request.put_request {
                        builder.add_leading_keys(extract_hash_key_values_from_item(
                            &put.item, key_info,
                        ));
                        builder.add_attributes(item_attribute_names(&put.item));
                    }
                    if let Some(delete) = &request.delete_request {
                        builder.add_leading_keys(extract_hash_key_values_from_item(
                            &delete.key,
                            key_info,
                        ));
                        builder.add_attributes(item_attribute_names(&delete.key));
                    }
                }
            } else {
                builder.add_leading_keys(None);
                for request in &requests {
                    if let Some(put) = &request.put_request {
                        builder.add_attributes(item_attribute_names(&put.item));
                    }
                    if let Some(delete) = &request.delete_request {
                        builder.add_attributes(item_attribute_names(&delete.key));
                    }
                }
            }
        }
    }
    Ok(build_nested_targets(builders, "BatchWriteItem"))
}

async fn build_transact_get_authorization_targets(
    state: &AppState,
    input: &Value,
    account_id: &str,
) -> Result<Vec<AuthorizationTarget>, DynamoDbError> {
    let Ok(input) = serde_json::from_value::<TransactGetItemsInput>(input.clone()) else {
        return Ok(vec![AuthorizationTarget::wildcard("TransactGetItems")]);
    };
    let key_infos = key_infos_for_tables(
        state,
        account_id,
        input
            .transact_items
            .iter()
            .map(|item| item.get.table_name.clone()),
    )
    .await?;
    let mut builders = HashMap::new();
    for item in input.transact_items {
        let table_name = item.get.table_name;
        builder_for_table(
            &mut builders,
            &key_infos,
            &table_name,
            "GetItem",
            Some("TransactGetItems"),
        );
        if let Some(builder) = builders.get_mut(&(table_name.clone(), "GetItem".to_owned())) {
            let key_info = builder.key_info.clone();
            if let Some(ref key_info) = key_info {
                builder
                    .add_leading_keys(extract_hash_key_values_from_item(&item.get.key, key_info));
            } else {
                builder.add_leading_keys(None);
            }
            builder.add_attributes(item_attribute_names(&item.get.key));
            builder.add_attributes(extract_attributes_from_parts(
                item.get.projection_expression.as_deref(),
                item.get.expression_attribute_names.as_ref(),
                None,
                state.limits.as_ref(),
            )?);
            builder.add_select(Some(read_select_from_parts(
                None,
                item.get.projection_expression.as_deref(),
                None,
                false,
            )));
        }
    }
    Ok(build_nested_targets(builders, "TransactGetItems"))
}

async fn build_transact_write_authorization_targets(
    state: &AppState,
    input: &Value,
    account_id: &str,
) -> Result<Vec<AuthorizationTarget>, DynamoDbError> {
    let Ok(input) = serde_json::from_value::<TransactWriteItemsInput>(input.clone()) else {
        return Ok(vec![AuthorizationTarget::wildcard("TransactWriteItems")]);
    };
    let key_infos = key_infos_for_tables(
        state,
        account_id,
        input
            .transact_items
            .iter()
            .filter_map(transact_write_item_table_name),
    )
    .await?;
    let mut builders = HashMap::new();
    for item in input.transact_items {
        if let Some(condition_check) = item.condition_check {
            let attributes = combine_attributes([
                item_attribute_names(&condition_check.key),
                condition_expression_attribute_names(
                    Some(condition_check.condition_expression.as_str()),
                    condition_check.expression_attribute_names.as_ref(),
                    condition_check.expression_attribute_values.as_ref(),
                    state.limits.as_ref(),
                )?,
            ]);
            add_transact_write_item(
                &mut builders,
                &key_infos,
                &condition_check.table_name,
                "ConditionCheckItem",
                &condition_check.key,
                attributes,
            );
        }
        if let Some(put) = item.put {
            let attributes = combine_attributes([
                item_attribute_names(&put.item),
                condition_expression_attribute_names(
                    put.condition_expression.as_deref(),
                    put.expression_attribute_names.as_ref(),
                    put.expression_attribute_values.as_ref(),
                    state.limits.as_ref(),
                )?,
            ]);
            add_transact_write_item(
                &mut builders,
                &key_infos,
                &put.table_name,
                "PutItem",
                &put.item,
                attributes,
            );
        }
        if let Some(delete) = item.delete {
            let attributes = combine_attributes([
                item_attribute_names(&delete.key),
                condition_expression_attribute_names(
                    delete.condition_expression.as_deref(),
                    delete.expression_attribute_names.as_ref(),
                    delete.expression_attribute_values.as_ref(),
                    state.limits.as_ref(),
                )?,
            ]);
            add_transact_write_item(
                &mut builders,
                &key_infos,
                &delete.table_name,
                "DeleteItem",
                &delete.key,
                attributes,
            );
        }
        if let Some(update) = item.update {
            let attributes = combine_attributes([
                item_attribute_names(&update.key),
                update_expression_attribute_names(
                    Some(update.update_expression.as_str()),
                    update.expression_attribute_names.as_ref(),
                    update.expression_attribute_values.as_ref(),
                    state.limits.as_ref(),
                )?,
                condition_expression_attribute_names(
                    update.condition_expression.as_deref(),
                    update.expression_attribute_names.as_ref(),
                    update.expression_attribute_values.as_ref(),
                    state.limits.as_ref(),
                )?,
            ]);
            add_transact_write_item(
                &mut builders,
                &key_infos,
                &update.table_name,
                "UpdateItem",
                &update.key,
                attributes,
            );
        }
    }
    Ok(build_nested_targets(builders, "TransactWriteItems"))
}

fn transact_write_item_table_name(item: &TransactWriteItem) -> Option<String> {
    item.condition_check
        .as_ref()
        .map(|op| op.table_name.clone())
        .or_else(|| item.put.as_ref().map(|op| op.table_name.clone()))
        .or_else(|| item.delete.as_ref().map(|op| op.table_name.clone()))
        .or_else(|| item.update.as_ref().map(|op| op.table_name.clone()))
}

fn add_transact_write_item(
    builders: &mut HashMap<(String, String), NestedTargetBuilder>,
    key_infos: &HashMap<String, Option<TableKeyInfo>>,
    table_name: &str,
    operation: &str,
    item: &Item,
    attributes: Option<Vec<String>>,
) {
    builder_for_table(
        builders,
        key_infos,
        table_name,
        operation,
        Some("TransactWriteItems"),
    );
    if let Some(builder) = builders.get_mut(&(table_name.to_owned(), operation.to_owned())) {
        let key_info = builder.key_info.clone();
        if let Some(ref key_info) = key_info {
            builder.add_leading_keys(extract_hash_key_values_from_item(item, key_info));
        } else {
            builder.add_leading_keys(None);
        }
        builder.add_attributes(attributes);
    }
}

fn build_nested_targets(
    builders: HashMap<(String, String), NestedTargetBuilder>,
    fallback_operation: &str,
) -> Vec<AuthorizationTarget> {
    let targets: Vec<AuthorizationTarget> = builders
        .into_values()
        .map(NestedTargetBuilder::build)
        .collect();
    if targets.is_empty() {
        vec![AuthorizationTarget::wildcard(fallback_operation)]
    } else {
        targets
    }
}

pub(crate) fn extract_leading_keys(
    input: &Value,
    operation: &str,
    key_info: Option<&TableKeyInfo>,
    read_info: Option<&TableReadInfo>,
    limits: &LimitsConfig,
) -> Option<Vec<String>> {
    match operation {
        "GetItem" | "DeleteItem" | "UpdateItem" => {
            extract_hash_key_values_from_raw_value(input.get("Key")?, key_info?)
        }
        "PutItem" => extract_hash_key_values_from_raw_value(input.get("Item")?, key_info?),
        "Query" => extract_query_leading_keys(input, read_info?, limits),
        _ => None,
    }
}

pub(crate) fn extract_select(
    input: &Value,
    operation: &str,
    read_info: Option<&TableReadInfo>,
) -> Option<String> {
    match operation {
        "GetItem" => Some(read_select_from_parts(
            None,
            input.get("ProjectionExpression").and_then(|v| v.as_str()),
            input
                .get("AttributesToGet")
                .and_then(|v| serde_json::from_value::<Vec<String>>(v.clone()).ok())
                .as_ref(),
            false,
        )),
        "Query" | "Scan" => Some(read_select_from_raw(
            input,
            input.get("IndexName").is_some()
                || read_info.and_then(|info| info.index.as_ref()).is_some(),
        )),
        _ => None,
    }
}

fn read_select_from_raw(input: &Value, index_read: bool) -> String {
    read_select_from_parts(
        input.get("Select").and_then(|v| v.as_str()),
        input.get("ProjectionExpression").and_then(|v| v.as_str()),
        input
            .get("AttributesToGet")
            .and_then(|v| serde_json::from_value::<Vec<String>>(v.clone()).ok())
            .as_ref(),
        index_read,
    )
}

fn read_select_from_parts(
    explicit_select: Option<&str>,
    projection_expression: Option<&str>,
    attributes_to_get: Option<&Vec<String>>,
    index_read: bool,
) -> String {
    if let Some(select) = explicit_select {
        return select.to_owned();
    }
    if projection_expression.is_some() || attributes_to_get.is_some_and(|attrs| !attrs.is_empty()) {
        return "SPECIFIC_ATTRIBUTES".to_owned();
    }
    if index_read {
        "ALL_PROJECTED_ATTRIBUTES".to_owned()
    } else {
        "ALL_ATTRIBUTES".to_owned()
    }
}

fn merge_select(current: Option<String>, next: Option<String>) -> Option<String> {
    match (current, next) {
        (None, next) => next,
        (current, None) => current,
        (Some(current), Some(next)) if select_rank(&next) > select_rank(&current) => Some(next),
        (Some(current), Some(_)) => Some(current),
    }
}

fn select_rank(select: &str) -> u8 {
    match select {
        "ALL_ATTRIBUTES" => 4,
        "ALL_PROJECTED_ATTRIBUTES" => 3,
        "SPECIFIC_ATTRIBUTES" => 2,
        "COUNT" => 1,
        _ => 0,
    }
}

fn extract_query_leading_keys(
    input: &Value,
    read_info: &TableReadInfo,
    limits: &LimitsConfig,
) -> Option<Vec<String>> {
    let query: QueryInput = serde_json::from_value(input.clone()).ok()?;
    let key_schema = read_info
        .index
        .as_ref()
        .map_or(&read_info.table.key_schema, |idx| &idx.key_schema);
    let hash_keys = hash_key_elements(key_schema);
    let first_hash = hash_keys.first()?;

    if let Some(ref key_conditions) = query.key_conditions {
        return extract_legacy_query_leading_keys(key_conditions, &hash_keys);
    }

    let kce = query.key_condition_expression.as_deref()?;
    let maps = build_expression_maps(
        query.expression_attribute_names.as_ref(),
        query.expression_attribute_values.as_ref(),
    );
    let tokens = tokenize_for_with_limits(
        kce,
        limits.max_expression_tokens,
        limits.max_expression_bytes,
        ExpressionKind::KeyCondition,
    )
    .ok()?;
    if limits.enforce_reserved_keywords && validate_no_reserved_words(&tokens).is_err() {
        return None;
    }

    let mut key_condition = parse_key_condition(&tokens).ok()?;
    key_condition
        .resolve_pk_sk(&first_hash.attribute_name, &maps.names)
        .ok()?;
    if hash_keys.len() > 1 {
        let hash_names: Vec<&str> = hash_keys
            .iter()
            .map(|element| element.attribute_name.as_str())
            .collect();
        key_condition
            .resolve_multipart(&hash_names, &maps.names)
            .ok()?;
    }

    let mut values = Vec::with_capacity(hash_keys.len());
    values.push(value_for_hash_path(
        &key_condition.pk_path,
        &key_condition.pk_value,
        &maps,
        &first_hash.attribute_name,
    )?);
    for hash_key in hash_keys.iter().skip(1) {
        let value = key_condition
            .extra_pk_conditions
            .iter()
            .find_map(|(path, expr)| {
                value_for_hash_path(path, expr, &maps, &hash_key.attribute_name)
            })?;
        values.push(value);
    }
    Some(values)
}

fn extract_hash_key_values_from_raw_value(
    map: &Value,
    key_info: &TableKeyInfo,
) -> Option<Vec<String>> {
    let obj = map.as_object()?;
    let hash_keys = hash_key_elements(&key_info.key_schema);
    if hash_keys.is_empty() {
        return None;
    }
    let mut values = Vec::new();
    for hash_key in hash_keys {
        let value: AttributeValue =
            serde_json::from_value(obj.get(&hash_key.attribute_name)?.clone()).ok()?;
        values.push(attribute_value_to_leading_key(&value)?);
    }
    Some(values)
}

fn extract_hash_key_values_from_item(item: &Item, key_info: &TableKeyInfo) -> Option<Vec<String>> {
    let hash_keys = hash_key_elements(&key_info.key_schema);
    if hash_keys.is_empty() {
        return None;
    }
    let mut values = Vec::new();
    for hash_key in hash_keys {
        values.push(attribute_value_to_leading_key(
            item.get(&hash_key.attribute_name)?,
        )?);
    }
    Some(values)
}

fn hash_key_elements(key_schema: &[KeySchemaElement]) -> Vec<&KeySchemaElement> {
    key_schema
        .iter()
        .filter(|element| element.key_type == KeyType::Hash)
        .collect()
}

fn build_expression_maps(
    names: Option<&HashMap<String, String>>,
    values: Option<&HashMap<String, AttributeValue>>,
) -> ExpressionMaps {
    ExpressionMaps::new(
        names
            .map(|m| {
                m.iter()
                    .map(|(k, v)| (k.strip_prefix('#').unwrap_or(k).to_owned(), v.clone()))
                    .collect()
            })
            .unwrap_or_default(),
        values
            .map(|m| {
                m.iter()
                    .map(|(k, v)| (k.strip_prefix(':').unwrap_or(k).to_owned(), v.clone()))
                    .collect()
            })
            .unwrap_or_default(),
    )
}

fn extract_legacy_query_leading_keys(
    key_conditions: &HashMap<String, extenddb_core::types::Condition>,
    hash_keys: &[&KeySchemaElement],
) -> Option<Vec<String>> {
    let mut values = Vec::with_capacity(hash_keys.len());
    for hash_key in hash_keys {
        let condition = key_conditions.get(&hash_key.attribute_name)?;
        if condition.comparison_operator != "EQ" || condition.attribute_value_list.len() != 1 {
            return None;
        }
        values.push(attribute_value_to_leading_key(
            &condition.attribute_value_list[0],
        )?);
    }
    Some(values)
}

fn value_for_hash_path(
    path: &[PathElement],
    expr: &Expr,
    maps: &ExpressionMaps,
    hash_attr: &str,
) -> Option<String> {
    if resolve_path_attr_name(path, &maps.names).as_deref() != Some(hash_attr) {
        return None;
    }
    let Expr::Placeholder(name) = expr else {
        return None;
    };
    maps.resolve_value(name)
        .ok()
        .and_then(attribute_value_to_leading_key)
}

fn resolve_path_attr_name(path: &[PathElement], names: &HashMap<String, String>) -> Option<String> {
    let PathElement::Attribute(name) = path.first()? else {
        return None;
    };
    if let Some(ref_name) = name.strip_prefix('#') {
        names.get(ref_name).cloned()
    } else {
        Some(name.clone())
    }
}

fn attribute_value_to_leading_key(value: &AttributeValue) -> Option<String> {
    match value {
        AttributeValue::S(value) | AttributeValue::N(value) => Some(value.clone()),
        AttributeValue::B(value) => Some(BASE64.encode(value)),
        _ => None,
    }
}

pub(crate) fn extract_attributes(
    input: &Value,
    operation: &str,
    limits: &LimitsConfig,
) -> Result<Option<Vec<String>>, DynamoDbError> {
    let attributes = match operation {
        "GetItem" => {
            let Ok(input) = serde_json::from_value::<GetItemInput>(input.clone()) else {
                return Ok(None);
            };
            combine_attributes([
                item_attribute_names(&input.key),
                extract_attributes_from_parts(
                    input.projection_expression.as_deref(),
                    input.expression_attribute_names.as_ref(),
                    input.attributes_to_get.as_ref(),
                    limits,
                )?,
            ])
        }
        "Query" => {
            let Ok(input) = serde_json::from_value::<QueryInput>(input.clone()) else {
                return Ok(None);
            };
            combine_attributes([
                expression_attribute_names_from_optional_condition(
                    input.key_condition_expression.as_deref(),
                    input.expression_attribute_names.as_ref(),
                    input.expression_attribute_values.as_ref(),
                    limits,
                )?,
                legacy_condition_attribute_names(input.key_conditions.as_ref()),
                expression_attribute_names_from_optional_condition(
                    input.filter_expression.as_deref(),
                    input.expression_attribute_names.as_ref(),
                    input.expression_attribute_values.as_ref(),
                    limits,
                )?,
                legacy_condition_attribute_names(input.query_filter.as_ref()),
                extract_attributes_from_parts(
                    input.projection_expression.as_deref(),
                    input.expression_attribute_names.as_ref(),
                    input.attributes_to_get.as_ref(),
                    limits,
                )?,
            ])
        }
        "Scan" => {
            let Ok(input) = serde_json::from_value::<ScanInput>(input.clone()) else {
                return Ok(None);
            };
            combine_attributes([
                expression_attribute_names_from_optional_condition(
                    input.filter_expression.as_deref(),
                    input.expression_attribute_names.as_ref(),
                    input.expression_attribute_values.as_ref(),
                    limits,
                )?,
                legacy_condition_attribute_names(input.scan_filter.as_ref()),
                extract_attributes_from_parts(
                    input.projection_expression.as_deref(),
                    input.expression_attribute_names.as_ref(),
                    input.attributes_to_get.as_ref(),
                    limits,
                )?,
            ])
        }
        "PutItem" => {
            let Ok(input) = serde_json::from_value::<PutItemInput>(input.clone()) else {
                return Ok(None);
            };
            combine_attributes([
                item_attribute_names(&input.item),
                condition_expression_attribute_names(
                    input.condition_expression.as_deref(),
                    input.expression_attribute_names.as_ref(),
                    input.expression_attribute_values.as_ref(),
                    limits,
                )?,
                expected_attribute_names(input.expected.as_ref()),
            ])
        }
        "DeleteItem" => {
            let Ok(input) = serde_json::from_value::<DeleteItemInput>(input.clone()) else {
                return Ok(None);
            };
            combine_attributes([
                item_attribute_names(&input.key),
                condition_expression_attribute_names(
                    input.condition_expression.as_deref(),
                    input.expression_attribute_names.as_ref(),
                    input.expression_attribute_values.as_ref(),
                    limits,
                )?,
                expected_attribute_names(input.expected.as_ref()),
            ])
        }
        "UpdateItem" => {
            let Ok(input) = serde_json::from_value::<UpdateItemInput>(input.clone()) else {
                return Ok(None);
            };
            combine_attributes([
                item_attribute_names(&input.key),
                update_expression_attribute_names(
                    input.update_expression.as_deref(),
                    input.expression_attribute_names.as_ref(),
                    input.expression_attribute_values.as_ref(),
                    limits,
                )?,
                attribute_update_names(input.attribute_updates.as_ref()),
                condition_expression_attribute_names(
                    input.condition_expression.as_deref(),
                    input.expression_attribute_names.as_ref(),
                    input.expression_attribute_values.as_ref(),
                    limits,
                )?,
                expected_attribute_names(input.expected.as_ref()),
            ])
        }
        _ => extract_attributes_from_raw_projection(input, limits)?,
    };
    Ok(attributes)
}

fn extract_attributes_from_raw_projection(
    input: &Value,
    limits: &LimitsConfig,
) -> Result<Option<Vec<String>>, DynamoDbError> {
    let Some(proj) = input.get("ProjectionExpression").and_then(|v| v.as_str()) else {
        return Ok(None);
    };
    let ean = input
        .get("ExpressionAttributeNames")
        .and_then(|v| v.as_object())
        .map(|m| {
            m.iter()
                .filter_map(|(k, v)| v.as_str().map(|s| (k.clone(), s.to_owned())))
                .collect::<HashMap<_, _>>()
        });
    extract_attributes_from_parts(Some(proj), ean.as_ref(), None, limits)
}

fn extract_attributes_from_parts(
    projection_expression: Option<&str>,
    expression_attribute_names: Option<&HashMap<String, String>>,
    attributes_to_get: Option<&Vec<String>>,
    limits: &LimitsConfig,
) -> Result<Option<Vec<String>>, DynamoDbError> {
    if let Some(attributes_to_get) = attributes_to_get
        && !attributes_to_get.is_empty()
    {
        return Ok(Some(dedup_preserve_order(attributes_to_get.clone())));
    }
    let Some(proj) = projection_expression else {
        return Ok(None);
    };
    let maps = build_expression_maps(expression_attribute_names, None);
    let tokens = tokenize_authorization_expression(proj, ExpressionKind::Projection, limits)?;
    let paths = parse_projection(&tokens)?;
    let mut attributes = Vec::with_capacity(paths.len());
    for path in paths {
        attributes.push(top_level_attribute_name(&path, &maps)?);
    }
    Ok(finish_attributes(attributes))
}

fn item_attribute_names(item: &Item) -> Option<Vec<String>> {
    finish_attributes(item.keys().cloned().collect())
}

fn expected_attribute_names(
    expected: Option<&HashMap<String, ExpectedAttributeValue>>,
) -> Option<Vec<String>> {
    finish_attributes(expected?.keys().cloned().collect())
}

fn attribute_update_names(
    updates: Option<&HashMap<String, AttributeValueUpdate>>,
) -> Option<Vec<String>> {
    finish_attributes(updates?.keys().cloned().collect())
}

fn legacy_condition_attribute_names(
    conditions: Option<&HashMap<String, Condition>>,
) -> Option<Vec<String>> {
    finish_attributes(conditions?.keys().cloned().collect())
}

fn condition_expression_attribute_names(
    expression: Option<&str>,
    names: Option<&HashMap<String, String>>,
    values: Option<&HashMap<String, AttributeValue>>,
    limits: &LimitsConfig,
) -> Result<Option<Vec<String>>, DynamoDbError> {
    expression_attribute_names_from_optional_condition(expression, names, values, limits)
}

fn expression_attribute_names_from_optional_condition(
    expression: Option<&str>,
    names: Option<&HashMap<String, String>>,
    values: Option<&HashMap<String, AttributeValue>>,
    limits: &LimitsConfig,
) -> Result<Option<Vec<String>>, DynamoDbError> {
    let Some(expression) = expression else {
        return Ok(None);
    };
    let maps = build_expression_maps(names, values);
    let tokens = tokenize_authorization_expression(expression, ExpressionKind::Condition, limits)?;
    let expr = parse_condition(&tokens)?;
    let mut attributes = Vec::new();
    collect_expr_attribute_names(&expr, &maps, &mut attributes)?;
    Ok(finish_attributes(attributes))
}

fn update_expression_attribute_names(
    expression: Option<&str>,
    names: Option<&HashMap<String, String>>,
    values: Option<&HashMap<String, AttributeValue>>,
    limits: &LimitsConfig,
) -> Result<Option<Vec<String>>, DynamoDbError> {
    let Some(expression) = expression else {
        return Ok(None);
    };
    let maps = build_expression_maps(names, values);
    let tokens = tokenize_authorization_expression(expression, ExpressionKind::Update, limits)?;
    let actions = parse_update_from(&tokens, expression)?;
    let mut attributes = Vec::new();
    for action in actions {
        collect_update_action_attribute_names(&action, &maps, &mut attributes)?;
    }
    Ok(finish_attributes(attributes))
}

fn tokenize_authorization_expression(
    expression: &str,
    kind: ExpressionKind,
    limits: &LimitsConfig,
) -> Result<Vec<extenddb_core::expression::Token>, DynamoDbError> {
    let tokens = tokenize_for_with_limits(
        expression,
        limits.max_expression_tokens,
        limits.max_expression_bytes,
        kind,
    )?;
    if limits.enforce_reserved_keywords {
        validate_no_reserved_words(&tokens)?;
    }
    Ok(tokens)
}

fn collect_update_action_attribute_names(
    action: &UpdateAction,
    maps: &ExpressionMaps,
    attributes: &mut Vec<String>,
) -> Result<(), DynamoDbError> {
    match action {
        UpdateAction::Set { path, value } => {
            attributes.push(top_level_attribute_name(path, maps)?);
            collect_expr_attribute_names(value, maps, attributes)?;
        }
        UpdateAction::Remove { path } => {
            attributes.push(top_level_attribute_name(path, maps)?);
        }
        UpdateAction::Add { path, value } | UpdateAction::Delete { path, value } => {
            attributes.push(top_level_attribute_name(path, maps)?);
            collect_expr_attribute_names(value, maps, attributes)?;
        }
    }
    Ok(())
}

fn collect_expr_attribute_names(
    expr: &Expr,
    maps: &ExpressionMaps,
    attributes: &mut Vec<String>,
) -> Result<(), DynamoDbError> {
    match expr {
        Expr::Path(path) => attributes.push(top_level_attribute_name(path, maps)?),
        Expr::Placeholder(_) => {}
        Expr::Compare { left, right, .. } | Expr::Arithmetic { left, right, .. } => {
            collect_expr_attribute_names(left, maps, attributes)?;
            collect_expr_attribute_names(right, maps, attributes)?;
        }
        Expr::And(left, right) | Expr::Or(left, right) => {
            collect_expr_attribute_names(left, maps, attributes)?;
            collect_expr_attribute_names(right, maps, attributes)?;
        }
        Expr::Not(inner) => collect_expr_attribute_names(inner, maps, attributes)?,
        Expr::Function { args, .. } => {
            for arg in args {
                collect_expr_attribute_names(arg, maps, attributes)?;
            }
        }
        Expr::Between { operand, low, high } => {
            collect_expr_attribute_names(operand, maps, attributes)?;
            collect_expr_attribute_names(low, maps, attributes)?;
            collect_expr_attribute_names(high, maps, attributes)?;
        }
        Expr::In { operand, list } => {
            collect_expr_attribute_names(operand, maps, attributes)?;
            for item in list {
                collect_expr_attribute_names(item, maps, attributes)?;
            }
        }
    }
    Ok(())
}

fn top_level_attribute_name(
    path: &[PathElement],
    maps: &ExpressionMaps,
) -> Result<String, DynamoDbError> {
    let Some(PathElement::Attribute(name)) = path.first() else {
        return Err(DynamoDbError::ValidationException(
            "Invalid expression: path cannot start with an index".to_owned(),
        ));
    };
    if let Some(ref_name) = name.strip_prefix('#') {
        Ok(maps.resolve_name(ref_name)?.to_owned())
    } else {
        Ok(name.clone())
    }
}

fn combine_attributes<const N: usize>(attributes: [Option<Vec<String>>; N]) -> Option<Vec<String>> {
    let mut combined = Vec::new();
    for values in attributes.into_iter().flatten() {
        combined.extend(values);
    }
    finish_attributes(combined)
}

fn finish_attributes(attributes: Vec<String>) -> Option<Vec<String>> {
    let attributes = dedup_preserve_order(attributes);
    if attributes.is_empty() {
        None
    } else {
        Some(attributes)
    }
}

fn dedup_preserve_order(values: Vec<String>) -> Vec<String> {
    let mut seen = HashSet::new();
    let mut out = Vec::with_capacity(values.len());
    for value in values {
        if seen.insert(value.clone()) {
            out.push(value);
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use extenddb_core::limits::LimitsConfig;
    use extenddb_core::types::{
        AttributeDefinition, KeySchemaElement, KeyType, Projection, ProjectionType,
        ScalarAttributeType, TableKeyInfo, TableReadInfo,
    };
    use serde_json::json;

    fn key(name: &str, key_type: KeyType) -> KeySchemaElement {
        KeySchemaElement {
            attribute_name: name.to_owned(),
            key_type,
        }
    }

    fn attr(name: &str) -> AttributeDefinition {
        AttributeDefinition {
            attribute_name: name.to_owned(),
            attribute_type: ScalarAttributeType::S,
        }
    }

    fn table_info(key_schema: Vec<KeySchemaElement>) -> TableReadInfo {
        TableReadInfo {
            table: TableKeyInfo {
                table_name: "t".to_owned(),
                account_id: "123456789012".to_owned(),
                table_id: "table-1".to_owned(),
                attribute_definitions: key_schema
                    .iter()
                    .map(|key| attr(&key.attribute_name))
                    .collect(),
                key_schema: key_schema.clone(),
                base_key_schema: key_schema,
                secondary_index_key_schemas: Vec::new(),
                has_lsi: false,
                stream_specification: None,
                stream_label: None,
            },
            index: None,
        }
    }

    fn first_resource(input: Value, operation: &str) -> DynamoDbResource {
        authorization_resources_for_operation(&input, operation, "123456789012")
            .expect("resources")
            .into_iter()
            .next()
            .expect("resource")
    }

    #[test]
    fn get_item_ignores_smuggled_index_name_for_resource_auth() {
        let resource = first_resource(
            json!({
                "TableName": "Orders",
                "Key": {"pk": {"S": "order-1"}},
                "IndexName": "ByDate"
            }),
            "GetItem",
        );

        assert_eq!(resource, DynamoDbResource::Table("Orders".to_owned()));
        assert_eq!(
            resource.policy_arn("us-east-1", "123456789012"),
            "arn:aws:dynamodb:us-east-1:123456789012:table/Orders"
        );
    }

    #[test]
    fn get_item_resource_uses_table_name_without_key() {
        let resource = first_resource(json!({"TableName": "Orders"}), "GetItem");

        assert_eq!(resource, DynamoDbResource::Table("Orders".to_owned()));
    }

    #[test]
    fn create_table_resource_uses_table_name_without_schema() {
        let resource = first_resource(json!({"TableName": "Orders"}), "CreateTable");

        assert_eq!(resource, DynamoDbResource::Table("Orders".to_owned()));
    }

    #[test]
    fn query_owns_index_resource_auth() {
        let resource = first_resource(
            json!({
                "TableName": "Orders",
                "IndexName": "ByDate"
            }),
            "Query",
        );

        assert_eq!(
            resource,
            DynamoDbResource::Index {
                table_name: "Orders".to_owned(),
                index_name: "ByDate".to_owned(),
            }
        );
        assert_eq!(
            resource.policy_arn("us-east-1", "123456789012"),
            "arn:aws:dynamodb:us-east-1:123456789012:table/Orders/index/ByDate"
        );
        assert_eq!(
            resource.tag_arn("us-east-1", "123456789012"),
            "arn:aws:dynamodb:us-east-1:123456789012:table/Orders"
        );
    }

    #[test]
    fn unsupported_return_values_do_not_enter_auth_context() {
        let input = json!({
            "TableName": "Orders",
            "Key": {"pk": {"S": "order-1"}},
            "ReturnValues": "ALL_OLD",
            "ReturnConsumedCapacity": "TOTAL"
        });
        let params = request_params(&input, "GetItem", None, None, None, None);

        assert_eq!(params.return_values, None);
        assert_eq!(params.return_consumed_capacity, Some("TOTAL".to_owned()));
    }

    #[test]
    fn condition_check_consumed_capacity_enters_auth_context() {
        let input = json!({"ReturnConsumedCapacity": "TOTAL"});
        let params = request_params(&input, "ConditionCheckItem", None, None, None, None);

        assert_eq!(params.return_consumed_capacity, Some("TOTAL".to_owned()));
    }

    #[test]
    fn supported_return_values_enter_auth_context() {
        let input = json!({
            "TableName": "Orders",
            "Key": {"pk": {"S": "order-1"}},
            "ReturnValues": "ALL_OLD"
        });
        let params = request_params(&input, "DeleteItem", None, None, None, None);

        assert_eq!(params.return_values, Some("ALL_OLD".to_owned()));
    }

    #[test]
    fn resource_arn_operations_authorize_exact_resource() {
        let arn = "arn:aws:dynamodb:us-east-1:123456789012:table/Orders/index/ByDate";
        let resource = first_resource(
            json!({
                "ResourceArn": arn
            }),
            "TagResource",
        );

        assert_eq!(
            resource,
            DynamoDbResource::ExactArn {
                policy_arn: arn.to_owned(),
                tag_arn: "arn:aws:dynamodb:us-east-1:123456789012:table/Orders".to_owned(),
            }
        );
    }

    #[test]
    fn cross_account_resource_arn_fails_before_policy_evaluation() {
        let err = authorization_resources_for_operation(
            &json!({
                "ResourceArn": "arn:aws:dynamodb:us-east-1:999999999999:table/Orders"
            }),
            "TagResource",
            "123456789012",
        )
        .expect_err("cross-account ARN must fail");

        assert!(matches!(err, DynamoDbError::AccessDeniedException(_)));
    }

    #[test]
    fn restore_from_backup_authorizes_backup_and_target_table() {
        let resources = authorization_resources_for_operation(
            &json!({
                "BackupArn": "arn:aws:dynamodb:us-east-1:123456789012:table/Orders/backup/42",
                "TargetTableName": "RestoredOrders"
            }),
            "RestoreTableFromBackup",
            "123456789012",
        )
        .expect("resources");

        assert_eq!(resources.len(), 2);
        assert_eq!(
            resources[0].policy_arn("us-east-1", "123456789012"),
            "arn:aws:dynamodb:us-east-1:123456789012:table/Orders/backup/42"
        );
        assert_eq!(
            resources[1],
            DynamoDbResource::Table("RestoredOrders".to_owned())
        );
    }

    #[test]
    fn get_records_authorizes_stream_from_iterator_token() {
        let stream_arn =
            "arn:aws:dynamodb:us-east-1:123456789012:table/Orders/stream/2026-06-07T00:00:00";
        let iterator = BASE64.encode(format!(
            "shardId-000000000001|AFTER_SEQUENCE_NUMBER|42|123|{stream_arn}|reader-1"
        ));
        let resource = first_resource(json!({"ShardIterator": iterator}), "GetRecords");

        assert_eq!(resource.policy_arn("us-east-1", "123456789012"), stream_arn);
        assert_eq!(
            resource.tag_arn("us-east-1", "123456789012"),
            "arn:aws:dynamodb:us-east-1:123456789012:table/Orders"
        );
    }

    #[test]
    fn extract_attributes_resolves_expression_attribute_names() {
        let input = json!({
            "ProjectionExpression": "#n, #v",
            "ExpressionAttributeNames": {
                "#n": "name",
                "#v": "value"
            }
        });
        let result = extract_attributes(&input, "Unknown", &LimitsConfig::default()).unwrap();
        assert_eq!(result, Some(vec!["name".to_owned(), "value".to_owned()]));
    }

    #[test]
    fn extract_attributes_mixed_placeholders_and_literals() {
        let input = json!({
            "ProjectionExpression": "#n, age",
            "ExpressionAttributeNames": {
                "#n": "name"
            }
        });
        let result = extract_attributes(&input, "Unknown", &LimitsConfig::default()).unwrap();
        assert_eq!(result, Some(vec!["name".to_owned(), "age".to_owned()]));
    }

    #[test]
    fn extract_attributes_no_expression_attribute_names() {
        let input = json!({
            "ProjectionExpression": "display_name, age"
        });
        let result = extract_attributes(&input, "Unknown", &LimitsConfig::default()).unwrap();
        assert_eq!(
            result,
            Some(vec!["display_name".to_owned(), "age".to_owned()])
        );
    }

    #[test]
    fn extract_attributes_no_projection() {
        let input = json!({"TableName": "test"});
        assert_eq!(
            extract_attributes(&input, "Unknown", &LimitsConfig::default()).unwrap(),
            None
        );
    }

    #[test]
    fn extract_select_defaults_get_without_projection_to_all_attributes() {
        let input = json!({
            "TableName": "t",
            "Key": {"pk": {"S": "tenant-a"}},
            "Select": "SPECIFIC_ATTRIBUTES"
        });
        assert_eq!(
            extract_select(&input, "GetItem", None),
            Some("ALL_ATTRIBUTES".to_owned())
        );
    }

    #[test]
    fn extract_select_uses_specific_attributes_for_projection() {
        let input = json!({
            "TableName": "t",
            "Key": {"pk": {"S": "tenant-a"}},
            "ProjectionExpression": "pk, allowed"
        });
        assert_eq!(
            extract_select(&input, "GetItem", None),
            Some("SPECIFIC_ATTRIBUTES".to_owned())
        );
    }

    #[test]
    fn extract_select_defaults_index_reads_to_all_projected_attributes() {
        let input = json!({
            "TableName": "t",
            "IndexName": "gsi",
            "KeyConditionExpression": "gpk = :gpk",
            "ExpressionAttributeValues": {":gpk": {"S": "tenant-index"}}
        });
        let mut read_info = table_info(vec![key("pk", KeyType::Hash)]);
        read_info.index = Some(extenddb_core::types::IndexInfo {
            index_name: "gsi".to_owned(),
            index_id: "gsi-1".to_owned(),
            index_type: extenddb_core::types::IndexType::Gsi,
            key_schema: vec![key("gpk", KeyType::Hash)],
            projection: Projection {
                projection_type: ProjectionType::KeysOnly,
                non_key_attributes: None,
            },
        });
        assert_eq!(
            extract_select(&input, "Query", Some(&read_info)),
            Some("ALL_PROJECTED_ATTRIBUTES".to_owned())
        );
    }

    #[test]
    fn merge_select_keeps_full_item_access_when_nested_reads_are_mixed() {
        assert_eq!(
            merge_select(
                Some("SPECIFIC_ATTRIBUTES".to_owned()),
                Some("ALL_ATTRIBUTES".to_owned())
            ),
            Some("ALL_ATTRIBUTES".to_owned())
        );
    }

    #[test]
    fn extract_attributes_from_put_item_includes_written_attrs() {
        let input = json!({
            "TableName": "t",
            "Item": {
                "pk": {"S": "tenant-a"},
                "allowed": {"S": "ok"},
                "secret": {"S": "nope"}
            }
        });
        let result = extract_attributes(&input, "PutItem", &LimitsConfig::default()).unwrap();
        assert_eq!(
            result,
            Some(vec![
                "allowed".to_owned(),
                "pk".to_owned(),
                "secret".to_owned()
            ])
        );
    }

    #[test]
    fn extract_attributes_from_update_expression_resolves_aliases() {
        let input = json!({
            "TableName": "t",
            "Key": {"pk": {"S": "tenant-a"}},
            "UpdateExpression": "SET #a = :v REMOVE stale",
            "ExpressionAttributeNames": {"#a": "allowed"},
            "ExpressionAttributeValues": {":v": {"S": "ok"}}
        });
        let result = extract_attributes(&input, "UpdateItem", &LimitsConfig::default()).unwrap();
        assert_eq!(
            result,
            Some(vec![
                "pk".to_owned(),
                "allowed".to_owned(),
                "stale".to_owned()
            ])
        );
    }

    #[test]
    fn extract_attributes_from_condition_expression_paths() {
        let input = json!({
            "TableName": "t",
            "Key": {"pk": {"S": "tenant-a"}},
            "ConditionExpression": "attribute_exists(#a.nested) AND version = :v",
            "ExpressionAttributeNames": {"#a": "allowed"},
            "ExpressionAttributeValues": {":v": {"N": "1"}}
        });
        let result = extract_attributes(&input, "DeleteItem", &LimitsConfig::default()).unwrap();
        assert_eq!(
            result,
            Some(vec![
                "pk".to_owned(),
                "allowed".to_owned(),
                "version".to_owned()
            ])
        );
    }

    #[test]
    fn extract_item_leading_keys_from_composite_hash_key() {
        let input = json!({
            "Key": {
                "tenant": {"S": "tenant-a"},
                "bucket": {"N": "7"},
                "sk": {"S": "row-1"}
            }
        });
        let read_info = table_info(vec![
            key("tenant", KeyType::Hash),
            key("bucket", KeyType::Hash),
            key("sk", KeyType::Range),
        ]);
        assert_eq!(
            extract_leading_keys(
                &input,
                "GetItem",
                Some(&read_info.table),
                None,
                &LimitsConfig::default(),
            ),
            Some(vec!["tenant-a".to_owned(), "7".to_owned()])
        );
    }

    #[test]
    fn extract_query_leading_keys_from_key_condition_expression() {
        let input = json!({
            "TableName": "t",
            "KeyConditionExpression": "pk = :pk",
            "ExpressionAttributeValues": {
                ":pk": {"S": "tenant-a"}
            }
        });
        let read_info = table_info(vec![key("pk", KeyType::Hash)]);
        assert_eq!(
            extract_leading_keys(
                &input,
                "Query",
                Some(&read_info.table),
                Some(&read_info),
                &LimitsConfig::default(),
            ),
            Some(vec!["tenant-a".to_owned()])
        );
    }

    #[test]
    fn extract_query_leading_keys_resolves_aliases_and_reversed_conditions() {
        let input = json!({
            "TableName": "t",
            "KeyConditionExpression": "#s = :sk AND #p = :pk",
            "ExpressionAttributeNames": {
                "#p": "pk",
                "#s": "sk"
            },
            "ExpressionAttributeValues": {
                ":pk": {"S": "tenant-a"},
                ":sk": {"S": "row-1"}
            }
        });
        let read_info = table_info(vec![key("pk", KeyType::Hash), key("sk", KeyType::Range)]);
        assert_eq!(
            extract_leading_keys(
                &input,
                "Query",
                Some(&read_info.table),
                Some(&read_info),
                &LimitsConfig::default(),
            ),
            Some(vec!["tenant-a".to_owned()])
        );
    }

    #[test]
    fn extract_query_leading_keys_uses_index_hash_key() {
        let input = json!({
            "TableName": "t",
            "IndexName": "gsi",
            "KeyConditionExpression": "gpk = :gpk",
            "ExpressionAttributeValues": {
                ":gpk": {"S": "tenant-index"}
            }
        });
        let mut read_info = table_info(vec![key("pk", KeyType::Hash)]);
        read_info.index = Some(extenddb_core::types::IndexInfo {
            index_name: "gsi".to_owned(),
            index_id: "gsi-1".to_owned(),
            index_type: extenddb_core::types::IndexType::Gsi,
            key_schema: vec![key("gpk", KeyType::Hash)],
            projection: Projection {
                projection_type: ProjectionType::All,
                non_key_attributes: None,
            },
        });
        assert_eq!(
            extract_leading_keys(
                &input,
                "Query",
                Some(&read_info.table),
                Some(&read_info),
                &LimitsConfig::default(),
            ),
            Some(vec!["tenant-index".to_owned()])
        );
    }

    #[test]
    fn extract_query_leading_keys_from_legacy_key_conditions() {
        let input = json!({
            "TableName": "t",
            "KeyConditions": {
                "pk": {
                    "ComparisonOperator": "EQ",
                    "AttributeValueList": [{"S": "tenant-a"}]
                }
            }
        });
        let read_info = table_info(vec![key("pk", KeyType::Hash)]);
        assert_eq!(
            extract_leading_keys(
                &input,
                "Query",
                Some(&read_info.table),
                Some(&read_info),
                &LimitsConfig::default(),
            ),
            Some(vec!["tenant-a".to_owned()])
        );
    }
}

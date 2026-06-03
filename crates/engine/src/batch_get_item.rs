// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! `BatchGetItem` operation handler.

use std::collections::{HashMap, HashSet};

use futures::future::join_all;
use serde_json::Value;

use extenddb_core::error::DynamoDbError;
use extenddb_core::expression::{ExpressionMaps, PathElement, apply_projection, parse_projection};
use extenddb_core::limits::LimitsConfig;
use extenddb_core::types::{
    BatchGetItemInput, BatchGetItemOutput, Item, KeySchemaElement, KeysAndAttributes, TableKeyInfo,
    extract_key, item_size_bytes,
};
use extenddb_core::validation::validate_batch_key_only;

use crate::OperationContext;
use crate::capacity_helpers;
use crate::create_table::storage_err_to_dynamo;
use crate::expression_helpers::build_checked_expression_maps;
use crate::serialize_output;
use crate::{DispatchMetrics, DispatchResult};

/// Maximum number of keys across all tables in a single `BatchGetItem` request.
const MAX_BATCH_GET_KEYS: usize = 100;

#[derive(Debug)]
struct BatchGetProjection {
    paths: Vec<Vec<PathElement>>,
    maps: ExpressionMaps,
}

/// Handle a `BatchGetItem` request.
///
/// Reads items from one or more tables by primary key. Each table's keys are
/// fetched through the storage backend's batch read path. `DynamoDB` limits:
/// max 100 keys total, max 16 MB response size.
///
/// # Errors
///
/// Returns `DynamoDbError` for validation failures, missing tables, or storage errors.
pub async fn handle_batch_get_item(
    body: Value,
    ctx: &OperationContext,
) -> Result<DispatchResult, DynamoDbError> {
    let input: BatchGetItemInput =
        serde_json::from_value(body).map_err(crate::deserialize_error)?;

    // Validate: RequestItems must not be empty
    if input.request_items.is_empty() {
        return Err(DynamoDbError::ValidationException(
            "The requestItems parameter is required for BatchGetItem".to_owned(),
        ));
    }

    // Validate: per-table keys <= 100
    for (table_name, ka) in &input.request_items {
        if ka.keys.len() > MAX_BATCH_GET_KEYS {
            return Err(DynamoDbError::ValidationException(format!(
                "1 validation error detected: Value at 'RequestItems.{table_name}.member.Keys' failed to satisfy constraint: \
                 Member must have length less than or equal to 100"
            )));
        }
    }

    // Validate: total keys across all tables <= 100
    let total_keys: usize = input.request_items.values().map(|ka| ka.keys.len()).sum();
    if total_keys > MAX_BATCH_GET_KEYS {
        return Err(DynamoDbError::ValidationException(
            "Too many items requested for the BatchGetItem call".to_owned(),
        ));
    }

    // Validate: each table must have at least one key
    for (table_name, ka) in &input.request_items {
        if ka.keys.is_empty() {
            return Err(DynamoDbError::ValidationException(format!(
                "1 validation error detected: Value '[]' at 'requestItems.{table_name}.member.keys' failed to satisfy constraint: Member must have length greater than or equal to 1"
            )));
        }
    }

    let mut responses: HashMap<String, Vec<Item>> = HashMap::new();
    let mut unprocessed_keys: HashMap<String, KeysAndAttributes> = HashMap::new();
    let mut total_rcu: f64 = 0.0;
    let mut total_pre_proj_bytes: usize = 0;
    let mut returned_count: u64 = 0;
    let mut per_table_rcu: HashMap<String, f64> = HashMap::new();
    let mut response_bytes: usize = 0;
    let table_infos = batch_get_table_infos(ctx, &input.request_items).await?;

    let mut table_names = input.request_items.keys().collect::<Vec<_>>();
    table_names.sort();
    for table_name in table_names {
        let ka = input.request_items.get(table_name).ok_or_else(|| {
            DynamoDbError::InternalServerError(format!(
                "missing batch request for table {table_name}"
            ))
        })?;
        let key_info = table_infos.get(table_name).ok_or_else(|| {
            DynamoDbError::InternalServerError(format!(
                "missing batch metadata for table {table_name}"
            ))
        })?;

        let batch_projection = build_batch_get_projection(ka, ctx.limits.as_ref())?;

        let mut table_items: Vec<Item> = Vec::new();
        let mut seen_keys: HashSet<Vec<u8>> = HashSet::with_capacity(ka.keys.len());
        for key in &ka.keys {
            let key_bytes = serialize_key_for_dedup(key);
            if !seen_keys.insert(key_bytes) {
                return Err(DynamoDbError::ValidationException(
                    "Provided list of item keys contains duplicates".to_owned(),
                ));
            }
            validate_batch_key_only(key, &key_info.key_schema, &key_info.attribute_definitions)?;
        }

        let strongly_consistent = ka.consistent_read == Some(true);
        let items = ctx
            .storage
            .batch_get_items(key_info, &ka.keys, strongly_consistent)
            .await
            .map_err(storage_err_to_dynamo)?;

        let limited = apply_batch_get_response_limit(
            ka,
            &key_info.key_schema,
            items,
            &mut response_bytes,
            ctx.limits.max_batch_get_response_bytes,
        );
        if !limited.unprocessed_keys.is_empty() {
            unprocessed_keys.insert(
                (*table_name).clone(),
                keys_and_attributes_with_keys(ka, limited.unprocessed_keys),
            );
        }

        for item in limited.returned_items {
            let size = item_size_bytes(&item);
            let item_rcu = capacity_helpers::read_capacity_units(size, strongly_consistent);
            total_rcu += item_rcu;
            *per_table_rcu.entry((*table_name).clone()).or_default() += item_rcu;
            total_pre_proj_bytes += size;
            returned_count += 1;
            let item = if let Some(ref projection) = batch_projection {
                apply_projection(&item, &projection.paths, &projection.maps)?
            } else {
                item
            };
            table_items.push(item);
        }
        responses.insert((*table_name).clone(), table_items);
    }

    let consumed_capacity = capacity_helpers::batch_read_capacity(
        input.return_consumed_capacity,
        per_table_rcu.iter().map(|(t, cu)| (t.as_str(), *cu)),
    );

    // Per-item RCU already accumulated above; DynamoDB rounds per item, then sums.
    let rcu = total_rcu;

    let output = BatchGetItemOutput {
        responses,
        unprocessed_keys,
        consumed_capacity,
    };
    let body = serialize_output(&output)?;
    Ok(DispatchResult {
        body,
        metrics: DispatchMetrics {
            read_capacity_units: rcu,
            returned_item_count: returned_count,
            returned_bytes: total_pre_proj_bytes as u64,
            ..Default::default()
        },
    })
}

fn serialize_key_for_dedup(key: &Item) -> Vec<u8> {
    serde_json::to_vec(key).unwrap_or_default()
}

struct BatchGetResponseLimit {
    returned_items: Vec<Item>,
    unprocessed_keys: Vec<Item>,
}

fn apply_batch_get_response_limit(
    ka: &KeysAndAttributes,
    key_schema: &[KeySchemaElement],
    items: Vec<Item>,
    response_bytes: &mut usize,
    max_response_bytes: usize,
) -> BatchGetResponseLimit {
    let mut item_by_key: HashMap<Vec<u8>, Item> = items
        .into_iter()
        .map(|item| {
            (
                serialize_key_for_dedup(&extract_key(&item, key_schema)),
                item,
            )
        })
        .collect();
    let mut returned_items = Vec::new();
    let mut unprocessed_keys = Vec::new();
    let mut limit_reached = false;

    for key in &ka.keys {
        if limit_reached {
            unprocessed_keys.push(key.clone());
            continue;
        }

        let Some(item) = item_by_key.remove(&serialize_key_for_dedup(key)) else {
            continue;
        };
        let item_bytes = item_size_bytes(&item);
        if *response_bytes + item_bytes > max_response_bytes && *response_bytes > 0 {
            limit_reached = true;
            unprocessed_keys.push(key.clone());
            continue;
        }

        *response_bytes += item_bytes;
        returned_items.push(item);
    }

    BatchGetResponseLimit {
        returned_items,
        unprocessed_keys,
    }
}

fn keys_and_attributes_with_keys(ka: &KeysAndAttributes, keys: Vec<Item>) -> KeysAndAttributes {
    KeysAndAttributes {
        keys,
        consistent_read: ka.consistent_read,
        projection_expression: ka.projection_expression.clone(),
        expression_attribute_names: ka.expression_attribute_names.clone(),
        attributes_to_get: ka.attributes_to_get.clone(),
    }
}

fn build_batch_get_projection(
    ka: &KeysAndAttributes,
    limits: &LimitsConfig,
) -> Result<Option<BatchGetProjection>, DynamoDbError> {
    if ka.projection_expression.is_some()
        && ka.attributes_to_get.as_ref().is_some_and(|a| !a.is_empty())
    {
        return Err(DynamoDbError::ValidationException(
            "Can not use both expression and non-expression parameters in the same request: \
             Non-expression parameters: {AttributesToGet} Expression parameters: {ProjectionExpression}"
                .to_owned(),
        ));
    }

    let (effective_proj_str, extra_proj_names) = if ka.projection_expression.is_some() {
        (ka.projection_expression.clone(), HashMap::new())
    } else if let Some(attrs) = &ka.attributes_to_get {
        let mut names_map = HashMap::new();
        let placeholders: Vec<String> = attrs
            .iter()
            .enumerate()
            .map(|(i, attr)| {
                let placeholder = format!("#_ag{i}");
                names_map.insert(placeholder.clone(), attr.clone());
                placeholder
            })
            .collect();
        (Some(placeholders.join(", ")), names_map)
    } else {
        (None, HashMap::new())
    };

    let Some(proj_str) = effective_proj_str else {
        return Ok(None);
    };

    let proj_tokens = crate::expression_helpers::tokenize_typed_expression(
        &proj_str,
        limits,
        "ProjectionExpression",
    )?;
    let paths = parse_projection(&proj_tokens)?;
    let maps = if extra_proj_names.is_empty() {
        build_checked_expression_maps(ka.expression_attribute_names.as_ref(), None, limits)?
    } else {
        let mut merged = ka.expression_attribute_names.clone().unwrap_or_default();
        merged.extend(extra_proj_names);
        build_checked_expression_maps(Some(&merged), None, limits)?
    };

    Ok(Some(BatchGetProjection { paths, maps }))
}

async fn batch_get_table_infos(
    ctx: &OperationContext,
    request_items: &HashMap<String, KeysAndAttributes>,
) -> Result<HashMap<String, TableKeyInfo>, DynamoDbError> {
    let mut table_names = request_items.keys().cloned().collect::<Vec<_>>();
    table_names.sort();

    let results = join_all(table_names.iter().map(|table_name| async move {
        (
            table_name.clone(),
            ctx.table_key_info(table_name)
                .await
                .map_err(storage_err_to_dynamo),
        )
    }))
    .await;

    let mut table_infos = HashMap::with_capacity(results.len());
    for (table_name, result) in results {
        table_infos.insert(table_name, result?);
    }
    Ok(table_infos)
}

#[cfg(test)]
mod tests {
    use extenddb_core::types::{AttributeValue, KeySchemaElement, KeyType};

    use super::*;

    fn keys_and_attributes() -> KeysAndAttributes {
        KeysAndAttributes {
            keys: Vec::new(),
            consistent_read: None,
            projection_expression: None,
            expression_attribute_names: None,
            attributes_to_get: None,
        }
    }

    fn key(value: &str) -> Item {
        Item::from([("pk".to_owned(), AttributeValue::S(value.to_owned()))])
    }

    fn item(value: &str, payload: &str) -> Item {
        Item::from([
            ("pk".to_owned(), AttributeValue::S(value.to_owned())),
            ("payload".to_owned(), AttributeValue::S(payload.to_owned())),
        ])
    }

    fn key_schema() -> Vec<KeySchemaElement> {
        vec![KeySchemaElement {
            attribute_name: "pk".to_owned(),
            key_type: KeyType::Hash,
        }]
    }

    #[test]
    fn attributes_to_get_is_desugared_to_projection() {
        let ka = KeysAndAttributes {
            attributes_to_get: Some(vec!["visible".to_owned(), "count".to_owned()]),
            ..keys_and_attributes()
        };
        let projection = build_batch_get_projection(&ka, &LimitsConfig::default())
            .expect("projection should parse")
            .expect("AttributesToGet should create a projection");

        let item = Item::from([
            ("visible".to_owned(), AttributeValue::S("keep".to_owned())),
            ("count".to_owned(), AttributeValue::N("3".to_owned())),
            ("hidden".to_owned(), AttributeValue::S("drop".to_owned())),
        ]);

        let projected = apply_projection(&item, &projection.paths, &projection.maps)
            .expect("projection should apply");

        assert_eq!(projected.len(), 2);
        assert_eq!(
            projected.get("visible"),
            Some(&AttributeValue::S("keep".to_owned()))
        );
        assert_eq!(
            projected.get("count"),
            Some(&AttributeValue::N("3".to_owned()))
        );
        assert!(!projected.contains_key("hidden"));
    }

    #[test]
    fn attributes_to_get_rejects_projection_expression_conflict() {
        let ka = KeysAndAttributes {
            projection_expression: Some("visible".to_owned()),
            attributes_to_get: Some(vec!["count".to_owned()]),
            ..keys_and_attributes()
        };

        let err = build_batch_get_projection(&ka, &LimitsConfig::default())
            .expect_err("legacy and expression projections cannot be mixed");

        assert!(matches!(
            err,
            DynamoDbError::ValidationException(message)
                if message.contains("AttributesToGet")
                    && message.contains("ProjectionExpression")
        ));
    }

    #[test]
    fn absent_projection_returns_none() {
        let projection =
            build_batch_get_projection(&keys_and_attributes(), &LimitsConfig::default())
                .expect("missing projection should be valid");

        assert!(projection.is_none());
    }

    #[test]
    fn response_limit_defers_remaining_batch_get_keys() {
        let first = item("a", "fits");
        let second = item("b", "too-much-for-this-response");
        let ka = KeysAndAttributes {
            keys: vec![key("a"), key("b")],
            consistent_read: Some(true),
            projection_expression: Some("payload".to_owned()),
            ..keys_and_attributes()
        };
        let mut response_bytes = 0;
        let max_response_bytes = item_size_bytes(&first) + 1;

        let limited = apply_batch_get_response_limit(
            &ka,
            &key_schema(),
            vec![second.clone(), first.clone()],
            &mut response_bytes,
            max_response_bytes,
        );

        assert_eq!(limited.returned_items, vec![first]);
        assert_eq!(limited.unprocessed_keys, vec![key("b")]);
        assert_eq!(response_bytes, item_size_bytes(&item("a", "fits")));

        let unprocessed = keys_and_attributes_with_keys(&ka, limited.unprocessed_keys);
        assert_eq!(unprocessed.consistent_read, Some(true));
        assert_eq!(
            unprocessed.projection_expression,
            Some("payload".to_owned())
        );
    }
}

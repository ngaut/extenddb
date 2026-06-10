// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Transactional read/write implementations for the `PostgreSQL` backend.

use std::collections::HashMap;

use extenddb_core::expression::{self, ExpressionMaps};
use extenddb_core::types::{
    AttributeValue, CancellationReason, Item, ReturnValuesOnConditionCheckFailure,
    ScalarAttributeType, TableKeyInfo,
};
use extenddb_core::validation;
use extenddb_storage::error::StorageError;
use extenddb_storage::util::{SortKeyValue, composite_pk_to_text, parse_sk, sk_column, sk_info};
use extenddb_storage::{IdempotencyClaim, TransactGetOp, TransactWriteOp};
use sqlx::types::BigDecimal;

use super::index::{
    IndexMeta, enqueue_async_indexes, fetch_indexes_for_write, pk_hash, sync_indexes,
};
use super::tx_helpers::{
    check_idempotency_token_in_tx, delete_item_in_tx, fetch_item_for_update, upsert_item_in_tx,
    write_stream_record_in_tx,
};
use super::{data_table_name, json_to_item};
use crate::PostgresEngine;

type TxnGetRowsQuery<'q, O> =
    sqlx::query::QueryAs<'q, sqlx::Postgres, O, sqlx::postgres::PgArguments>;

#[derive(Clone, Debug, PartialEq)]
struct TransactGetLookupKey {
    pk: String,
    sk: Option<TransactGetSortKey>,
}

#[derive(Clone, Debug, PartialEq)]
enum TransactGetSortKey {
    S(String),
    N(BigDecimal),
    B(Vec<u8>),
}

struct TransactGetEntry {
    result_index: usize,
    lookup_key: TransactGetLookupKey,
}

struct TransactGetGroup<'a> {
    key_info: &'a TableKeyInfo,
    entries: Vec<TransactGetEntry>,
}

impl PostgresEngine {
    /// Implementation of `DataEngine::transact_get_items`.
    pub(crate) async fn transact_get_items_impl(
        &self,
        ops: &[TransactGetOp<'_>],
    ) -> Result<Vec<Option<Item>>, StorageError> {
        if ops.is_empty() {
            return Ok(Vec::new());
        }

        // Validate key types inside the transaction so mismatches produce
        // TransactionCanceledException with ValidationError cancellation
        // reasons, matching real DynamoDB behavior.
        let mut reasons: Vec<CancellationReason> = Vec::with_capacity(ops.len());
        let mut any_failed = false;
        for op in ops {
            match validation::validate_key_only(
                op.key,
                &op.key_info.key_schema,
                &op.key_info.attribute_definitions,
            ) {
                Ok(()) => reasons.push(CancellationReason::none()),
                Err(e) => {
                    any_failed = true;
                    reasons.push(CancellationReason::validation_error(e.to_string()));
                }
            }
        }
        if any_failed {
            return Err(StorageError::TransactionCanceled(reasons));
        }

        let groups = transact_get_groups(ops)?;

        let mut tx = self
            .data_pool
            .begin()
            .await
            .map_err(|e| StorageError::Internal(e.to_string()))?;

        let mut results = vec![None; ops.len()];
        for group in &groups {
            fetch_transact_get_group(&mut tx, group, &mut results).await?;
        }

        tx.commit()
            .await
            .map_err(|e| StorageError::Internal(e.to_string()))?;
        Ok(results)
    }

    /// Implementation of `DataEngine::transact_write_items`.
    pub(crate) async fn transact_write_items_impl(
        &self,
        ops: &[TransactWriteOp<'_>],
        idempotency: Option<IdempotencyClaim<'_>>,
    ) -> Result<(), StorageError> {
        // Pre-fetch indexes for each unique table involved in the transaction.
        let mut table_indexes: HashMap<String, Vec<IndexMeta>> = HashMap::new();
        for op in ops {
            let name = transact_op_table_name(op);
            if !table_indexes.contains_key(name) {
                let indexes = fetch_indexes_for_write(transact_op_key_info(op), &self.pool).await?;
                table_indexes.insert(name.to_owned(), indexes);
            }
        }

        // Read system default delay from cache.
        let sys_delay = self
            .gsi_default_delay_ms
            .load(std::sync::atomic::Ordering::Relaxed);

        let mut tx = self
            .data_pool
            .begin()
            .await
            .map_err(|e| StorageError::Internal(e.to_string()))?;

        // Check idempotency token within the transaction.
        if let Some(claim) = idempotency {
            let storage_key = claim.storage_key();
            check_idempotency_token_in_tx(&mut tx, &storage_key, claim.fingerprint).await?;
        }

        let mut reasons: Vec<CancellationReason> = Vec::with_capacity(ops.len());
        // Collect old/new items from each op for async GSI enqueue after commit.
        let mut op_items: Vec<(Option<Item>, Option<Item>)> = Vec::with_capacity(ops.len());
        let mut any_failed = false;

        for op in ops {
            let indexes = &table_indexes[transact_op_table_name(op)];
            let reason = execute_transact_write_op(
                &mut tx,
                op,
                indexes,
                self.max_item_size_bytes,
                sys_delay,
            )
            .await;
            match reason {
                Ok(items) => {
                    op_items.push(items);
                    reasons.push(CancellationReason::none());
                }
                Err(TxnOpError::Cancel(r)) => {
                    op_items.push((None, None));
                    any_failed = true;
                    reasons.push(r);
                }
                Err(TxnOpError::Storage(e)) => {
                    // Infrastructure error — abort the entire transaction
                    // without leaking internal details into cancellation reasons.
                    return Err(StorageError::Internal(e.to_string()));
                }
            }
        }

        if any_failed {
            return Err(StorageError::TransactionCanceled(reasons));
        }

        // Write stream records atomically within the transaction.
        for (op, (old_item, new_item)) in ops.iter().zip(op_items.iter()) {
            let capture = match op {
                TransactWriteOp::Put { stream, .. }
                | TransactWriteOp::Delete { stream, .. }
                | TransactWriteOp::Update { stream, .. } => stream.as_ref(),
                TransactWriteOp::ConditionCheck { .. } => None,
            };
            if let Some(capture) = capture {
                write_stream_record_in_tx(
                    &mut tx,
                    match op {
                        TransactWriteOp::Put { key_info, .. }
                        | TransactWriteOp::Delete { key_info, .. }
                        | TransactWriteOp::Update { key_info, .. }
                        | TransactWriteOp::ConditionCheck { key_info, .. } => key_info,
                    },
                    capture,
                    old_item.as_ref(),
                    new_item.as_ref(),
                )
                .await?;
            }
        }

        tx.commit()
            .await
            .map_err(|e| StorageError::Internal(e.to_string()))?;

        // Enqueue async GSI updates after commit using the old/new items
        // collected during transaction execution.
        if let Some(ref q) = self.gsi_queue {
            for (op, (old_item, new_item)) in ops.iter().zip(op_items.iter()) {
                let indexes = &table_indexes[transact_op_table_name(op)];
                if indexes.is_empty() {
                    continue;
                }
                let key_info = match op {
                    TransactWriteOp::Put { key_info, .. }
                    | TransactWriteOp::Delete { key_info, .. }
                    | TransactWriteOp::Update { key_info, .. }
                    | TransactWriteOp::ConditionCheck { key_info, .. } => key_info,
                };
                // Derive pk_text from whichever item is available.
                let pk_item = new_item.as_ref().or(old_item.as_ref());
                let Some(pk_item) = pk_item else { continue }; // ConditionCheck — no index changes
                let pk_text = match composite_pk_to_text(pk_item, &key_info.key_schema) {
                    Ok(t) => t,
                    Err(_) => continue,
                };
                enqueue_async_indexes(
                    q,
                    pk_hash(pk_text.as_str()),
                    &key_info.account_id,
                    &key_info.table_name,
                    &key_info.table_id,
                    &key_info.key_schema,
                    &key_info.attribute_definitions,
                    indexes,
                    old_item.as_ref(),
                    new_item.as_ref(),
                    sys_delay,
                )
                .await;
            }
        }

        Ok(())
    }

    /// Delete expired transaction idempotency tokens for PostgreSQL's retention worker.
    pub(crate) async fn cleanup_expired_idempotency_tokens_impl(
        &self,
        max_age_seconds: i64,
    ) -> Result<u64, StorageError> {
        // Cast i64→integer for PG 15 compat; safe for realistic values (<68 years).
        // idempotency_tokens lives in the data database.
        let result = sqlx::query(
            "DELETE FROM idempotency_tokens WHERE created_at < NOW() - make_interval(secs => $1::integer)",
        )
        .bind(max_age_seconds)
        .execute(&self.data_pool)
        .await
        .map_err(|e| StorageError::Internal(e.to_string()))?;

        Ok(result.rows_affected())
    }
}

fn transact_get_groups<'a>(
    ops: &'a [TransactGetOp<'a>],
) -> Result<Vec<TransactGetGroup<'a>>, StorageError> {
    let mut groups: Vec<TransactGetGroup<'a>> = Vec::new();

    for (result_index, op) in ops.iter().enumerate() {
        let lookup_key = transact_get_lookup_key(op.key_info, op.key)?;
        if let Some(group) = groups
            .iter_mut()
            .find(|group| group.key_info.table_id == op.key_info.table_id)
        {
            group.entries.push(TransactGetEntry {
                result_index,
                lookup_key,
            });
        } else {
            groups.push(TransactGetGroup {
                key_info: op.key_info,
                entries: vec![TransactGetEntry {
                    result_index,
                    lookup_key,
                }],
            });
        }
    }

    Ok(groups)
}

fn transact_get_lookup_key(
    key_info: &TableKeyInfo,
    key: &Item,
) -> Result<TransactGetLookupKey, StorageError> {
    let pk = composite_pk_to_text(key, &key_info.key_schema)?;
    let sk = if let Some((sk_name, sk_type)) =
        sk_info(&key_info.key_schema, &key_info.attribute_definitions)
    {
        let sk_value = key.get(sk_name).ok_or_else(|| {
            StorageError::Internal(format!("missing sort key attribute {sk_name}"))
        })?;
        Some(transact_get_sort_key(parse_sk(sk_value, sk_type)?))
    } else {
        None
    };

    Ok(TransactGetLookupKey { pk, sk })
}

fn transact_get_sort_key(sk: SortKeyValue) -> TransactGetSortKey {
    match sk {
        SortKeyValue::S(s) => TransactGetSortKey::S(s),
        SortKeyValue::N(n) => TransactGetSortKey::N(n),
        SortKeyValue::B(b) => TransactGetSortKey::B(b),
    }
}

async fn fetch_transact_get_group(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    group: &TransactGetGroup<'_>,
    results: &mut [Option<Item>],
) -> Result<(), StorageError> {
    let ddb_table = data_table_name(&group.key_info.table_id);
    let fetched = if let Some((_, sk_type)) = sk_info(
        &group.key_info.key_schema,
        &group.key_info.attribute_definitions,
    ) {
        let sk_col = sk_column(sk_type);
        let sql = transact_get_pk_sk_sql(&ddb_table, sk_col, group.entries.len());
        match sk_type {
            ScalarAttributeType::S => {
                let mut query = sqlx::query_as::<_, (String, String, serde_json::Value)>(&sql);
                for entry in &group.entries {
                    query = query.bind(entry.lookup_key.pk.clone());
                    query = bind_transact_get_sort_key(
                        query,
                        entry.lookup_key.sk.as_ref().ok_or_else(|| {
                            StorageError::Internal(
                                "missing prepared transaction sort key".to_owned(),
                            )
                        })?,
                    );
                }
                let rows = query
                    .fetch_all(&mut **tx)
                    .await
                    .map_err(|e| StorageError::Internal(e.to_string()))?;
                rows.into_iter()
                    .map(|(pk, sk, json)| {
                        Ok((
                            TransactGetLookupKey {
                                pk,
                                sk: Some(TransactGetSortKey::S(sk)),
                            },
                            json_to_item(json)?,
                        ))
                    })
                    .collect::<Result<Vec<_>, StorageError>>()?
            }
            ScalarAttributeType::N => {
                let mut query = sqlx::query_as::<_, (String, BigDecimal, serde_json::Value)>(&sql);
                for entry in &group.entries {
                    query = query.bind(entry.lookup_key.pk.clone());
                    query = bind_transact_get_sort_key(
                        query,
                        entry.lookup_key.sk.as_ref().ok_or_else(|| {
                            StorageError::Internal(
                                "missing prepared transaction sort key".to_owned(),
                            )
                        })?,
                    );
                }
                let rows = query
                    .fetch_all(&mut **tx)
                    .await
                    .map_err(|e| StorageError::Internal(e.to_string()))?;
                rows.into_iter()
                    .map(|(pk, sk, json)| {
                        Ok((
                            TransactGetLookupKey {
                                pk,
                                sk: Some(TransactGetSortKey::N(sk)),
                            },
                            json_to_item(json)?,
                        ))
                    })
                    .collect::<Result<Vec<_>, StorageError>>()?
            }
            ScalarAttributeType::B => {
                let mut query = sqlx::query_as::<_, (String, Vec<u8>, serde_json::Value)>(&sql);
                for entry in &group.entries {
                    query = query.bind(entry.lookup_key.pk.clone());
                    query = bind_transact_get_sort_key(
                        query,
                        entry.lookup_key.sk.as_ref().ok_or_else(|| {
                            StorageError::Internal(
                                "missing prepared transaction sort key".to_owned(),
                            )
                        })?,
                    );
                }
                let rows = query
                    .fetch_all(&mut **tx)
                    .await
                    .map_err(|e| StorageError::Internal(e.to_string()))?;
                rows.into_iter()
                    .map(|(pk, sk, json)| {
                        Ok((
                            TransactGetLookupKey {
                                pk,
                                sk: Some(TransactGetSortKey::B(sk)),
                            },
                            json_to_item(json)?,
                        ))
                    })
                    .collect::<Result<Vec<_>, StorageError>>()?
            }
        }
    } else {
        let sql = transact_get_pk_sql(&ddb_table, group.entries.len());
        let mut query = sqlx::query_as::<_, (String, serde_json::Value)>(&sql);
        for entry in &group.entries {
            query = query.bind(entry.lookup_key.pk.clone());
        }
        let rows = query
            .fetch_all(&mut **tx)
            .await
            .map_err(|e| StorageError::Internal(e.to_string()))?;
        rows.into_iter()
            .map(|(pk, json)| Ok((TransactGetLookupKey { pk, sk: None }, json_to_item(json)?)))
            .collect::<Result<Vec<_>, StorageError>>()?
    };

    assign_transact_get_group_results(group, &fetched, results);
    Ok(())
}

fn bind_transact_get_sort_key<'q, O>(
    query: TxnGetRowsQuery<'q, O>,
    sk: &TransactGetSortKey,
) -> TxnGetRowsQuery<'q, O> {
    match sk {
        TransactGetSortKey::S(s) => query.bind(s.clone()),
        TransactGetSortKey::N(n) => query.bind(n.clone()),
        TransactGetSortKey::B(b) => query.bind(b.clone()),
    }
}

fn assign_transact_get_group_results(
    group: &TransactGetGroup<'_>,
    fetched: &[(TransactGetLookupKey, Item)],
    results: &mut [Option<Item>],
) {
    for entry in &group.entries {
        if let Some((_, item)) = fetched
            .iter()
            .find(|(lookup_key, _)| lookup_key == &entry.lookup_key)
        {
            results[entry.result_index] = Some(item.clone());
        }
    }
}

fn transact_get_pk_sql(table: &str, key_count: usize) -> String {
    format!(
        "SELECT pk, item_data FROM {table} WHERE pk IN ({})",
        postgres_tuple_placeholders(key_count, 1)
    )
}

fn transact_get_pk_sk_sql(table: &str, sk_col: &str, key_count: usize) -> String {
    format!(
        "SELECT pk, {sk_col}, item_data FROM {table} WHERE (pk, {sk_col}) IN ({})",
        postgres_tuple_placeholders(key_count, 2)
    )
}

fn postgres_tuple_placeholders(count: usize, width: usize) -> String {
    let mut next = 1usize;
    (0..count)
        .map(|_| {
            let values = (0..width)
                .map(|_| {
                    let placeholder = format!("${next}");
                    next += 1;
                    placeholder
                })
                .collect::<Vec<_>>();
            if width == 1 {
                values[0].clone()
            } else {
                format!("({})", values.join(", "))
            }
        })
        .collect::<Vec<_>>()
        .join(", ")
}

/// Extract the table name from a transactional write operation.
fn transact_op_table_name<'a>(op: &'a TransactWriteOp<'_>) -> &'a str {
    match op {
        TransactWriteOp::Put { key_info, .. }
        | TransactWriteOp::Delete { key_info, .. }
        | TransactWriteOp::Update { key_info, .. }
        | TransactWriteOp::ConditionCheck { key_info, .. } => &key_info.table_name,
    }
}

/// Extract table metadata from a transactional write operation.
fn transact_op_key_info<'a>(op: &'a TransactWriteOp<'_>) -> &'a TableKeyInfo {
    match op {
        TransactWriteOp::Put { key_info, .. }
        | TransactWriteOp::Delete { key_info, .. }
        | TransactWriteOp::Update { key_info, .. }
        | TransactWriteOp::ConditionCheck { key_info, .. } => key_info,
    }
}

/// Error type for individual transactional write operations.
///
/// Separates user-driven cancellations (condition failures, validation errors)
/// from infrastructure errors (PG connection failures, serialization errors).
/// This prevents internal error details from leaking into client-visible
/// cancellation reasons.
enum TxnOpError {
    /// User-driven failure — becomes a per-item cancellation reason.
    Cancel(CancellationReason),
    /// Infrastructure failure — bubbles up as `StorageError::Internal`.
    Storage(StorageError),
}

impl From<CancellationReason> for TxnOpError {
    fn from(r: CancellationReason) -> Self {
        Self::Cancel(r)
    }
}

/// Execute a single transactional write operation, including sync GSI/LSI updates.
///
/// Only sync indexes (delay=0) are processed here. Async indexes are enqueued
/// by the caller after the transaction commits.
///
/// Returns `(old_item, new_item)` on success for async GSI enqueue.
async fn execute_transact_write_op(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    op: &TransactWriteOp<'_>,
    indexes: &[IndexMeta],
    max_item_size_bytes: usize,
    sys_delay: u64,
) -> Result<(Option<Item>, Option<Item>), TxnOpError> {
    match op {
        TransactWriteOp::Put {
            key_info,
            item,
            condition,
            maps,
            return_values_on_ccf,
            ..
        } => {
            // Key type validation inside the transaction so mismatches produce
            // TransactionCanceledException with ValidationError cancellation
            // reasons, matching real DynamoDB behavior.
            validation::validate_item_keys(
                item,
                &key_info.key_schema,
                &key_info.attribute_definitions,
            )
            .map_err(|e| TxnOpError::Cancel(CancellationReason::validation_error(e.to_string())))?;
            let existing = fetch_item_for_update(tx, key_info, item)
                .await
                .map_err(TxnOpError::Storage)?;
            let empty = Item::new();
            eval_condition(
                *condition,
                existing.as_ref().unwrap_or(&empty),
                maps,
                *return_values_on_ccf,
                existing.as_ref(),
            )?;
            upsert_item_in_tx(tx, key_info, item)
                .await
                .map_err(TxnOpError::Storage)?;
            if !indexes.is_empty() {
                sync_indexes(
                    tx,
                    &key_info.table_id,
                    &key_info.key_schema,
                    &key_info.attribute_definitions,
                    indexes,
                    existing.as_ref(),
                    Some(item),
                    sys_delay,
                )
                .await
                .map_err(TxnOpError::Storage)?;
            }
            Ok((existing, Some((*item).clone())))
        }
        TransactWriteOp::Delete {
            key_info,
            key,
            condition,
            maps,
            return_values_on_ccf,
            ..
        } => {
            validation::validate_key_only(
                key,
                &key_info.key_schema,
                &key_info.attribute_definitions,
            )
            .map_err(|e| TxnOpError::Cancel(CancellationReason::validation_error(e.to_string())))?;
            let existing = fetch_item_for_update(tx, key_info, key)
                .await
                .map_err(TxnOpError::Storage)?;
            let empty = Item::new();
            eval_condition(
                *condition,
                existing.as_ref().unwrap_or(&empty),
                maps,
                *return_values_on_ccf,
                existing.as_ref(),
            )?;
            delete_item_in_tx(tx, key_info, key)
                .await
                .map_err(TxnOpError::Storage)?;
            if !indexes.is_empty() {
                sync_indexes(
                    tx,
                    &key_info.table_id,
                    &key_info.key_schema,
                    &key_info.attribute_definitions,
                    indexes,
                    existing.as_ref(),
                    None,
                    sys_delay,
                )
                .await
                .map_err(TxnOpError::Storage)?;
            }
            Ok((existing, None))
        }
        TransactWriteOp::Update {
            key_info,
            key,
            actions,
            condition,
            maps,
            return_values_on_ccf,
            ..
        } => {
            validation::validate_key_only(
                key,
                &key_info.key_schema,
                &key_info.attribute_definitions,
            )
            .map_err(|e| TxnOpError::Cancel(CancellationReason::validation_error(e.to_string())))?;
            let existing = fetch_item_for_update(tx, key_info, key)
                .await
                .map_err(TxnOpError::Storage)?;
            let mut item = existing.clone().unwrap_or_else(|| (*key).clone());
            // Evaluate condition against empty item if non-existent (DynamoDB semantics)
            let condition_item = if existing.is_some() {
                &item
            } else {
                &std::collections::BTreeMap::new()
            };
            eval_condition(
                *condition,
                condition_item,
                maps,
                *return_values_on_ccf,
                existing.as_ref(),
            )?;
            expression::apply_update(actions, &mut item, maps).map_err(|e| {
                TxnOpError::Cancel(CancellationReason::validation_error(e.to_string()))
            })?;
            // Validate post-update item size
            validation::validate_item_size(&item, max_item_size_bytes).map_err(|e| {
                TxnOpError::Cancel(CancellationReason::validation_error(e.to_string()))
            })?;
            upsert_item_in_tx(tx, key_info, &item)
                .await
                .map_err(TxnOpError::Storage)?;
            if !indexes.is_empty() {
                sync_indexes(
                    tx,
                    &key_info.table_id,
                    &key_info.key_schema,
                    &key_info.attribute_definitions,
                    indexes,
                    existing.as_ref(),
                    Some(&item),
                    sys_delay,
                )
                .await
                .map_err(TxnOpError::Storage)?;
            }
            Ok((existing, Some(item)))
        }
        TransactWriteOp::ConditionCheck {
            key_info,
            key,
            condition,
            maps,
            return_values_on_ccf,
        } => {
            validation::validate_key_only(
                key,
                &key_info.key_schema,
                &key_info.attribute_definitions,
            )
            .map_err(|e| TxnOpError::Cancel(CancellationReason::validation_error(e.to_string())))?;
            let existing = fetch_item_for_update(tx, key_info, key)
                .await
                .map_err(TxnOpError::Storage)?;
            let empty = Item::new();
            let check_against = existing.as_ref().unwrap_or(&empty);
            eval_condition(
                Some(condition),
                check_against,
                maps,
                *return_values_on_ccf,
                existing.as_ref(),
            )?;
            Ok((None, None))
        }
    }
}

/// Evaluate a condition expression, returning a `CancellationReason` on failure.
///
/// When `return_values_on_ccf` is `AllOld`, the existing item is included in the
/// cancellation reason so the client can see what caused the condition to fail.
fn eval_condition(
    condition: Option<&extenddb_core::expression::Expr>,
    item: &std::collections::BTreeMap<String, AttributeValue>,
    maps: &ExpressionMaps,
    return_values_on_ccf: ReturnValuesOnConditionCheckFailure,
    existing: Option<&Item>,
) -> Result<(), CancellationReason> {
    if let Some(cond) = condition {
        let passed = expression::evaluate_condition(cond, item, maps)
            .map_err(|e| CancellationReason::validation_error(e.to_string()))?;
        if !passed {
            let item_to_return =
                if return_values_on_ccf == ReturnValuesOnConditionCheckFailure::AllOld {
                    existing.cloned()
                } else {
                    None
                };
            return Err(CancellationReason::condition_check_failed_with_item(
                item_to_return,
            ));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use extenddb_core::types::{
        AttributeDefinition, AttributeValue, Item, KeySchemaElement, KeyType, ScalarAttributeType,
        TableKeyInfo,
    };

    use super::{
        TransactGetEntry, TransactGetGroup, TransactGetLookupKey, TransactGetSortKey,
        assign_transact_get_group_results, transact_get_lookup_key, transact_get_pk_sk_sql,
        transact_get_pk_sql,
    };

    fn key_info() -> TableKeyInfo {
        TableKeyInfo {
            table_name: "orders".to_owned(),
            account_id: "acct".to_owned(),
            table_id: "ordersid".to_owned(),
            key_schema: vec![KeySchemaElement {
                attribute_name: "pk".to_owned(),
                key_type: KeyType::Hash,
            }],
            attribute_definitions: vec![AttributeDefinition {
                attribute_name: "pk".to_owned(),
                attribute_type: ScalarAttributeType::S,
            }],
            secondary_index_key_schemas: Vec::new(),
            has_lsi: false,
            stream_specification: None,
            stream_label: None,
        }
    }

    fn composite_key_info() -> TableKeyInfo {
        TableKeyInfo {
            table_name: "orders".to_owned(),
            account_id: "acct".to_owned(),
            table_id: "ordersid".to_owned(),
            key_schema: vec![
                KeySchemaElement {
                    attribute_name: "tenant".to_owned(),
                    key_type: KeyType::Hash,
                },
                KeySchemaElement {
                    attribute_name: "bucket".to_owned(),
                    key_type: KeyType::Hash,
                },
                KeySchemaElement {
                    attribute_name: "ts".to_owned(),
                    key_type: KeyType::Range,
                },
            ],
            attribute_definitions: vec![
                AttributeDefinition {
                    attribute_name: "tenant".to_owned(),
                    attribute_type: ScalarAttributeType::S,
                },
                AttributeDefinition {
                    attribute_name: "bucket".to_owned(),
                    attribute_type: ScalarAttributeType::S,
                },
                AttributeDefinition {
                    attribute_name: "ts".to_owned(),
                    attribute_type: ScalarAttributeType::N,
                },
            ],
            secondary_index_key_schemas: Vec::new(),
            has_lsi: false,
            stream_specification: None,
            stream_label: None,
        }
    }

    fn item(pk: &str) -> Item {
        let mut item = Item::new();
        item.insert("pk".to_owned(), AttributeValue::S(pk.to_owned()));
        item
    }

    #[test]
    fn transaction_get_sql_uses_postgres_batch_placeholders() {
        assert_eq!(
            transact_get_pk_sql("\"_ddb_table\"", 3),
            "SELECT pk, item_data FROM \"_ddb_table\" WHERE pk IN ($1, $2, $3)"
        );
        assert_eq!(
            transact_get_pk_sk_sql("\"_ddb_table\"", "sk_n", 2),
            "SELECT pk, sk_n, item_data FROM \"_ddb_table\" WHERE (pk, sk_n) IN (($1, $2), ($3, $4))"
        );
    }

    #[test]
    fn transaction_get_assignment_restores_request_order() {
        let key_info = key_info();
        let first = TransactGetLookupKey {
            pk: "first".to_owned(),
            sk: None,
        };
        let second = TransactGetLookupKey {
            pk: "second".to_owned(),
            sk: None,
        };
        let group = TransactGetGroup {
            key_info: &key_info,
            entries: vec![
                TransactGetEntry {
                    result_index: 0,
                    lookup_key: first.clone(),
                },
                TransactGetEntry {
                    result_index: 1,
                    lookup_key: second.clone(),
                },
            ],
        };
        let fetched = vec![(second, item("second")), (first, item("first"))];
        let mut results = vec![None, None];

        assign_transact_get_group_results(&group, &fetched, &mut results);

        assert_eq!(results, vec![Some(item("first")), Some(item("second"))]);
    }

    #[test]
    fn transaction_get_lookup_key_uses_composite_partition_key_encoding() {
        let key_info = composite_key_info();
        let mut key = Item::new();
        key.insert("tenant".to_owned(), AttributeValue::S("abc".to_owned()));
        key.insert("bucket".to_owned(), AttributeValue::S("de".to_owned()));
        key.insert("ts".to_owned(), AttributeValue::N("42".to_owned()));

        let lookup = transact_get_lookup_key(&key_info, &key).expect("lookup key");

        assert_eq!(lookup.pk, "3:abc,2:de,");
        match lookup.sk {
            Some(TransactGetSortKey::N(n)) => assert_eq!(n.to_string(), "42"),
            other => panic!("expected numeric sort key, got {other:?}"),
        }
    }
}

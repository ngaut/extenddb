// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Storage-backed LSI item collection size accounting.

use extenddb_core::types::{Item, TableKeyInfo, item_size_bytes};
use extenddb_storage::error::StorageError;

use super::{data_table_name, item_collection_table_name, json_to_item};

/// Apply an old/new item size delta to the LSI item collection for `pk`.
///
/// TiDB serializes all writers to the same item collection through a per-table
/// collection row keyed by the physical partition key. The first write for a
/// partition key lazily initializes that row by scanning only that key; later
/// writes use the maintained total and do not scan the user data table.
pub(super) async fn apply_lsi_item_collection_delta_in_tx(
    tx: &mut sqlx::Transaction<'_, sqlx::MySql>,
    key_info: &TableKeyInfo,
    pk: &[u8],
    old_item: Option<&Item>,
    new_item: Option<&Item>,
    max_collection_size_bytes: usize,
) -> Result<(), StorageError> {
    if !key_info.has_lsi {
        return Ok(());
    }

    let delta = item_size_delta(old_item, new_item)?;
    if delta == 0 {
        return Ok(());
    }

    let current = lock_item_collection_size(tx, key_info, pk).await?;
    let next = next_collection_size(current, delta, max_collection_size_bytes, key_info)?;

    if next != current {
        update_item_collection_size(tx, &key_info.table_id, pk, next).await?;
    }

    Ok(())
}

fn item_size_delta(old_item: Option<&Item>, new_item: Option<&Item>) -> Result<i128, StorageError> {
    let old_size = old_item
        .map(item_size_bytes)
        .map(i128::try_from)
        .transpose()
        .map_err(|_| StorageError::Internal("old item size exceeds signed range".to_owned()))?
        .unwrap_or(0);
    let new_size = new_item
        .map(item_size_bytes)
        .map(i128::try_from)
        .transpose()
        .map_err(|_| StorageError::Internal("new item size exceeds signed range".to_owned()))?
        .unwrap_or(0);
    Ok(new_size - old_size)
}

fn next_collection_size(
    current: u64,
    delta: i128,
    max_collection_size_bytes: usize,
    key_info: &TableKeyInfo,
) -> Result<u64, StorageError> {
    let next = if delta >= 0 {
        current
            .checked_add(u64::try_from(delta).map_err(|_| {
                StorageError::Internal(
                    "item collection size delta exceeds unsigned range".to_owned(),
                )
            })?)
            .ok_or_else(|| StorageError::Internal("item collection size overflow".to_owned()))?
    } else {
        current.saturating_sub(u64::try_from(-delta).map_err(|_| {
            StorageError::Internal("item collection size delta exceeds unsigned range".to_owned())
        })?)
    };

    let limit = u64::try_from(max_collection_size_bytes).unwrap_or(u64::MAX);
    if delta > 0 && next > limit {
        return Err(StorageError::ItemCollectionSizeLimitExceeded(format!(
            "Item collection size limit exceeded for table {}; maximum is {} bytes",
            key_info.table_name, limit
        )));
    }

    Ok(next)
}

async fn lock_item_collection_size(
    tx: &mut sqlx::Transaction<'_, sqlx::MySql>,
    key_info: &TableKeyInfo,
    pk: &[u8],
) -> Result<u64, StorageError> {
    let collection_table = item_collection_table_name(&key_info.table_id);
    let result = sqlx::query(&format!(
        "INSERT IGNORE INTO {collection_table} (pk, size_bytes) VALUES (?, 0)"
    ))
    .bind(pk)
    .execute(&mut **tx)
    .await
    .map_err(|e| StorageError::Internal(e.to_string()))?;
    let inserted = result.rows_affected() > 0;

    let (size_bytes,): (u64,) = sqlx::query_as(&format!(
        "SELECT size_bytes FROM {collection_table} WHERE pk = ? FOR UPDATE"
    ))
    .bind(pk)
    .fetch_one(&mut **tx)
    .await
    .map_err(|e| StorageError::Internal(e.to_string()))?;

    if !inserted {
        return Ok(size_bytes);
    }

    let initialized_size = scan_item_collection_size_for_update(tx, key_info, pk).await?;
    update_item_collection_size(tx, &key_info.table_id, pk, initialized_size).await?;
    Ok(initialized_size)
}

async fn scan_item_collection_size_for_update(
    tx: &mut sqlx::Transaction<'_, sqlx::MySql>,
    key_info: &TableKeyInfo,
    pk: &[u8],
) -> Result<u64, StorageError> {
    let ddb_table = data_table_name(&key_info.table_id);
    let sql = format!("SELECT item_data FROM {ddb_table} WHERE pk = ? FOR UPDATE");
    let rows: Vec<(serde_json::Value,)> = sqlx::query_as(&sql)
        .bind(pk)
        .fetch_all(&mut **tx)
        .await
        .map_err(|e| StorageError::Internal(e.to_string()))?;

    let mut total = 0_u64;
    for (json,) in rows {
        let item = json_to_item(json)?;
        let size = u64::try_from(item_size_bytes(&item))
            .map_err(|_| StorageError::Internal("item size exceeds unsigned range".to_owned()))?;
        total = total
            .checked_add(size)
            .ok_or_else(|| StorageError::Internal("item collection size overflow".to_owned()))?;
    }
    Ok(total)
}

async fn update_item_collection_size(
    tx: &mut sqlx::Transaction<'_, sqlx::MySql>,
    table_id: &str,
    pk: &[u8],
    size_bytes: u64,
) -> Result<(), StorageError> {
    let collection_table = item_collection_table_name(table_id);
    sqlx::query(&format!(
        "UPDATE {collection_table} SET size_bytes = ? WHERE pk = ?"
    ))
    .bind(size_bytes)
    .bind(pk)
    .execute(&mut **tx)
    .await
    .map_err(|e| StorageError::Internal(e.to_string()))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use extenddb_core::types::{
        AttributeDefinition, KeySchemaElement, KeyType, ScalarAttributeType, TableKeyInfo,
    };

    use super::{item_collection_table_name, next_collection_size};

    fn key_info() -> TableKeyInfo {
        TableKeyInfo {
            table_name: "table".to_owned(),
            account_id: "acct".to_owned(),
            table_id: "tableid".to_owned(),
            key_schema: vec![KeySchemaElement {
                attribute_name: "pk".to_owned(),
                key_type: KeyType::Hash,
            }],
            attribute_definitions: vec![AttributeDefinition {
                attribute_name: "pk".to_owned(),
                attribute_type: ScalarAttributeType::S,
            }],
            secondary_index_key_schemas: Vec::new(),
            has_lsi: true,
            stream_specification: None,
            stream_label: None,
        }
    }

    #[test]
    fn item_collection_table_is_table_local() {
        assert_eq!(
            item_collection_table_name("tableid"),
            "`_ddb_tableid_collections`"
        );
    }

    #[test]
    fn positive_delta_over_limit_is_rejected() {
        let err = next_collection_size(90, 11, 100, &key_info()).unwrap_err();

        assert!(matches!(
            err,
            extenddb_storage::error::StorageError::ItemCollectionSizeLimitExceeded(_)
        ));
    }

    #[test]
    fn shrink_from_oversized_collection_is_allowed() {
        let next = next_collection_size(150, -25, 100, &key_info()).unwrap();

        assert_eq!(next, 125);
    }

    #[test]
    fn exact_limit_is_allowed() {
        let next = next_collection_size(90, 10, 100, &key_info()).unwrap();

        assert_eq!(next, 100);
    }
}

// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! TransactWriteItems integration tests — continued (cross-table, idempotency, mixed ops).

use crate::test_base::*;
use aws_sdk_dynamodb::types::{
    ConditionCheck, Delete, Put, ReturnValuesOnConditionCheckFailure, TransactWriteItem, Update,
};
use std::collections::HashMap;

#[tokio::test]
async fn transact_write_across_tables() {
    let c = client();
    let t = tables().await;
    let item1 = create_item(&t.simple_key_string);
    let item2 = create_item(&t.comp_key_string_number);

    c.transact_write_items()
        .transact_items(
            TransactWriteItem::builder()
                .put(
                    Put::builder()
                        .table_name(&t.simple_key_string)
                        .set_item(Some(item1.clone()))
                        .build()
                        .unwrap(),
                )
                .build(),
        )
        .transact_items(
            TransactWriteItem::builder()
                .put(
                    Put::builder()
                        .table_name(&t.comp_key_string_number)
                        .set_item(Some(item2.clone()))
                        .build()
                        .unwrap(),
                )
                .build(),
        )
        .send()
        .await
        .unwrap();

    check_item(c, &t.simple_key_string, &item1).await;
    check_item(c, &t.comp_key_string_number, &item2).await;
}

#[tokio::test]
async fn transact_write_non_existent_table() {
    let c = client();
    let item: HashMap<String, aws_sdk_dynamodb::types::AttributeValue> =
        [(HASH_KEY_S.into(), s("k"))].into();

    let err = c
        .transact_write_items()
        .transact_items(
            TransactWriteItem::builder()
                .put(
                    Put::builder()
                        .table_name("NonExistentTable_tw")
                        .set_item(Some(item))
                        .build()
                        .unwrap(),
                )
                .build(),
        )
        .send()
        .await
        .unwrap_err();

    assert_eq!(err_code(&err), Some("ResourceNotFoundException"));
}

#[tokio::test]
async fn transact_write_idempotent_token() {
    let c = client();
    let t = tables().await;
    let table = &t.simple_key_string;
    let item = create_item(table);
    let token = uuid::Uuid::new_v4().to_string();

    c.transact_write_items()
        .client_request_token(&token)
        .transact_items(
            TransactWriteItem::builder()
                .put(
                    Put::builder()
                        .table_name(table)
                        .set_item(Some(item.clone()))
                        .build()
                        .unwrap(),
                )
                .build(),
        )
        .send()
        .await
        .unwrap();

    c.transact_write_items()
        .client_request_token(&token)
        .transact_items(
            TransactWriteItem::builder()
                .put(
                    Put::builder()
                        .table_name(table)
                        .set_item(Some(item.clone()))
                        .build()
                        .unwrap(),
                )
                .build(),
        )
        .send()
        .await
        .unwrap();

    check_item(c, table, &item).await;
}

#[tokio::test]
async fn transact_write_mixed_operations() {
    let c = client();
    let t = tables().await;
    let table = &t.simple_key_string;

    let mut update_item = create_item(table);
    update_item.insert("counter".into(), n(10));
    c.put_item()
        .table_name(table)
        .set_item(Some(update_item.clone()))
        .send()
        .await
        .unwrap();

    let delete_item = create_item(table);
    c.put_item()
        .table_name(table)
        .set_item(Some(delete_item.clone()))
        .send()
        .await
        .unwrap();

    let put_item = create_item(table);

    c.transact_write_items()
        .transact_items(
            TransactWriteItem::builder()
                .put(
                    Put::builder()
                        .table_name(table)
                        .set_item(Some(put_item.clone()))
                        .build()
                        .unwrap(),
                )
                .build(),
        )
        .transact_items(
            TransactWriteItem::builder()
                .update(
                    Update::builder()
                        .table_name(table)
                        .set_key(Some(get_key(table, &update_item)))
                        .update_expression("SET #c = :v")
                        .expression_attribute_names("#c", "counter")
                        .expression_attribute_values(":v", n(99))
                        .build()
                        .unwrap(),
                )
                .build(),
        )
        .transact_items(
            TransactWriteItem::builder()
                .delete(
                    Delete::builder()
                        .table_name(table)
                        .set_key(Some(get_key(table, &delete_item)))
                        .build()
                        .unwrap(),
                )
                .build(),
        )
        .send()
        .await
        .unwrap();

    check_item(c, table, &put_item).await;

    let key = get_key(table, &update_item);
    let resp = c
        .get_item()
        .table_name(table)
        .set_key(Some(key))
        .consistent_read(true)
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.item().unwrap().get("counter").unwrap().as_n().unwrap(),
        "99"
    );

    let key = get_key(table, &delete_item);
    let resp = c
        .get_item()
        .table_name(table)
        .set_key(Some(key))
        .consistent_read(true)
        .send()
        .await
        .unwrap();
    assert!(resp.item().is_none());
}

#[tokio::test]
async fn transact_write_condition_check_return_values() {
    let c = client();
    let t = tables().await;
    let table = &t.simple_key_string;
    let mut item = create_item(table);
    item.insert("status".into(), s("active"));
    c.put_item()
        .table_name(table)
        .set_item(Some(item.clone()))
        .send()
        .await
        .unwrap();

    let err = c
        .transact_write_items()
        .transact_items(
            TransactWriteItem::builder()
                .condition_check(
                    ConditionCheck::builder()
                        .table_name(table)
                        .set_key(Some(get_key(table, &item)))
                        .condition_expression("#s = :v")
                        .expression_attribute_names("#s", "status")
                        .expression_attribute_values(":v", s("wrong"))
                        .return_values_on_condition_check_failure(
                            ReturnValuesOnConditionCheckFailure::AllOld,
                        )
                        .build()
                        .unwrap(),
                )
                .build(),
        )
        .send()
        .await
        .unwrap_err();

    assert_eq!(err_code(&err), Some("TransactionCanceledException"));
}

#[tokio::test]
async fn transact_write_duplicate_key_same_table() {
    let c = client();
    let t = tables().await;
    let table = &t.simple_key_string;
    let item = create_item(table);

    let err = c
        .transact_write_items()
        .transact_items(
            TransactWriteItem::builder()
                .put(
                    Put::builder()
                        .table_name(table)
                        .set_item(Some(item.clone()))
                        .build()
                        .unwrap(),
                )
                .build(),
        )
        .transact_items(
            TransactWriteItem::builder()
                .put(
                    Put::builder()
                        .table_name(table)
                        .set_item(Some(item.clone()))
                        .build()
                        .unwrap(),
                )
                .build(),
        )
        .send()
        .await
        .unwrap_err();

    assert_eq!(err_code(&err), Some("ValidationException"));
}

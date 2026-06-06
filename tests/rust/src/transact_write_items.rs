// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! TransactWriteItems integration tests.

use crate::test_base::*;
use aws_sdk_dynamodb::types::{ConditionCheck, Delete, Put, TransactWriteItem, Update};

#[tokio::test]
async fn transact_write_single_put() {
    let c = client();
    let t = tables().await;
    let table = &t.simple_key_string;
    let item = create_item(table);

    c.transact_write_items()
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
async fn transact_write_multiple_puts() {
    let c = client();
    let t = tables().await;
    let table = &t.simple_key_string;
    let items: Vec<_> = (0..3).map(|_| create_item(table)).collect();

    let mut req = c.transact_write_items();
    for item in &items {
        req = req.transact_items(
            TransactWriteItem::builder()
                .put(
                    Put::builder()
                        .table_name(table)
                        .set_item(Some(item.clone()))
                        .build()
                        .unwrap(),
                )
                .build(),
        );
    }
    req.send().await.unwrap();

    for item in &items {
        check_item(c, table, item).await;
    }
}

#[tokio::test]
async fn transact_write_put_and_delete() {
    let c = client();
    let t = tables().await;
    let table = &t.simple_key_string;

    let to_delete = create_item(table);
    c.put_item()
        .table_name(table)
        .set_item(Some(to_delete.clone()))
        .send()
        .await
        .unwrap();

    let to_put = create_item(table);

    c.transact_write_items()
        .transact_items(
            TransactWriteItem::builder()
                .put(
                    Put::builder()
                        .table_name(table)
                        .set_item(Some(to_put.clone()))
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
                        .set_key(Some(get_key(table, &to_delete)))
                        .build()
                        .unwrap(),
                )
                .build(),
        )
        .send()
        .await
        .unwrap();

    check_item(c, table, &to_put).await;
    let key = get_key(table, &to_delete);
    let resp = c
        .get_item()
        .table_name(table)
        .set_key(Some(key))
        .consistent_read(true)
        .send()
        .await
        .unwrap();
    assert!(resp.item().is_none(), "Deleted item should be gone");
}

#[tokio::test]
async fn transact_write_with_update() {
    let c = client();
    let t = tables().await;
    let table = &t.simple_key_string;
    let mut item = create_item(table);
    item.insert("counter".into(), n(0));
    c.put_item()
        .table_name(table)
        .set_item(Some(item.clone()))
        .send()
        .await
        .unwrap();

    let key = get_key(table, &item);
    c.transact_write_items()
        .transact_items(
            TransactWriteItem::builder()
                .update(
                    Update::builder()
                        .table_name(table)
                        .set_key(Some(key.clone()))
                        .update_expression("SET #c = #c + :inc")
                        .expression_attribute_names("#c", "counter")
                        .expression_attribute_values(":inc", n(5))
                        .build()
                        .unwrap(),
                )
                .build(),
        )
        .send()
        .await
        .unwrap();

    let resp = c
        .get_item()
        .table_name(table)
        .set_key(Some(key))
        .consistent_read(true)
        .send()
        .await
        .unwrap();
    let actual = resp.item().unwrap();
    assert_eq!(actual.get("counter").unwrap().as_n().unwrap(), "5");
}

#[tokio::test]
async fn transact_write_with_condition_check() {
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

    let new_item = create_item(table);

    c.transact_write_items()
        .transact_items(
            TransactWriteItem::builder()
                .condition_check(
                    ConditionCheck::builder()
                        .table_name(table)
                        .set_key(Some(get_key(table, &item)))
                        .condition_expression("#s = :v")
                        .expression_attribute_names("#s", "status")
                        .expression_attribute_values(":v", s("active"))
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
                        .set_item(Some(new_item.clone()))
                        .build()
                        .unwrap(),
                )
                .build(),
        )
        .send()
        .await
        .unwrap();

    check_item(c, table, &new_item).await;
}

#[tokio::test]
async fn transact_write_condition_check_fails() {
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

    let new_item = create_item(table);

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
                        .expression_attribute_values(":v", s("inactive"))
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
                        .set_item(Some(new_item.clone()))
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
async fn transact_write_put_with_condition() {
    let c = client();
    let t = tables().await;
    let table = &t.simple_key_string;
    let item = create_item(table);

    c.transact_write_items()
        .transact_items(
            TransactWriteItem::builder()
                .put(
                    Put::builder()
                        .table_name(table)
                        .set_item(Some(item.clone()))
                        .condition_expression("attribute_not_exists(#h)")
                        .expression_attribute_names("#h", HASH_KEY_S)
                        .build()
                        .unwrap(),
                )
                .build(),
        )
        .send()
        .await
        .unwrap();

    check_item(c, table, &item).await;

    let err = c
        .transact_write_items()
        .transact_items(
            TransactWriteItem::builder()
                .put(
                    Put::builder()
                        .table_name(table)
                        .set_item(Some(item.clone()))
                        .condition_expression("attribute_not_exists(#h)")
                        .expression_attribute_names("#h", HASH_KEY_S)
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
async fn transact_write_delete_with_condition() {
    let c = client();
    let t = tables().await;
    let table = &t.simple_key_string;
    let mut item = create_item(table);
    item.insert("status".into(), s("deletable"));
    c.put_item()
        .table_name(table)
        .set_item(Some(item.clone()))
        .send()
        .await
        .unwrap();

    c.transact_write_items()
        .transact_items(
            TransactWriteItem::builder()
                .delete(
                    Delete::builder()
                        .table_name(table)
                        .set_key(Some(get_key(table, &item)))
                        .condition_expression("#s = :v")
                        .expression_attribute_names("#s", "status")
                        .expression_attribute_values(":v", s("deletable"))
                        .build()
                        .unwrap(),
                )
                .build(),
        )
        .send()
        .await
        .unwrap();

    let key = get_key(table, &item);
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

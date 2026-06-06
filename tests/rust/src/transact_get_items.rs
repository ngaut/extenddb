// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! TransactGetItems integration tests.

use crate::test_base::*;
use aws_sdk_dynamodb::types::{Get, TransactGetItem};
use std::collections::HashMap;

#[tokio::test]
async fn transact_get_single_item() {
    let c = client();
    let t = tables().await;
    let table = &t.simple_key_string;
    let item = create_item(table);
    c.put_item()
        .table_name(table)
        .set_item(Some(item.clone()))
        .send()
        .await
        .unwrap();

    let resp = c
        .transact_get_items()
        .transact_items(
            TransactGetItem::builder()
                .get(
                    Get::builder()
                        .table_name(table)
                        .set_key(Some(get_key(table, &item)))
                        .build()
                        .unwrap(),
                )
                .build(),
        )
        .send()
        .await
        .unwrap();

    let responses = resp.responses();
    assert_eq!(responses.len(), 1);
    let actual = responses[0].item().unwrap();
    for (k, v) in &item {
        assert_eq!(actual.get(k).unwrap(), v, "Attribute {k} mismatch");
    }
}

#[tokio::test]
async fn transact_get_multiple_items() {
    let c = client();
    let t = tables().await;
    let table = &t.simple_key_string;
    let items: Vec<_> = (0..3).map(|_| create_item(table)).collect();
    for item in &items {
        c.put_item()
            .table_name(table)
            .set_item(Some(item.clone()))
            .send()
            .await
            .unwrap();
    }

    let mut req = c.transact_get_items();
    for item in &items {
        req = req.transact_items(
            TransactGetItem::builder()
                .get(
                    Get::builder()
                        .table_name(table)
                        .set_key(Some(get_key(table, item)))
                        .build()
                        .unwrap(),
                )
                .build(),
        );
    }
    let resp = req.send().await.unwrap();
    assert_eq!(resp.responses().len(), 3);
}

#[tokio::test]
async fn transact_get_across_tables() {
    let c = client();
    let t = tables().await;
    let item1 = create_item(&t.simple_key_string);
    let item2 = create_item(&t.comp_key_string_number);
    c.put_item()
        .table_name(&t.simple_key_string)
        .set_item(Some(item1.clone()))
        .send()
        .await
        .unwrap();
    c.put_item()
        .table_name(&t.comp_key_string_number)
        .set_item(Some(item2.clone()))
        .send()
        .await
        .unwrap();

    let resp = c
        .transact_get_items()
        .transact_items(
            TransactGetItem::builder()
                .get(
                    Get::builder()
                        .table_name(&t.simple_key_string)
                        .set_key(Some(get_key(&t.simple_key_string, &item1)))
                        .build()
                        .unwrap(),
                )
                .build(),
        )
        .transact_items(
            TransactGetItem::builder()
                .get(
                    Get::builder()
                        .table_name(&t.comp_key_string_number)
                        .set_key(Some(get_key(&t.comp_key_string_number, &item2)))
                        .build()
                        .unwrap(),
                )
                .build(),
        )
        .send()
        .await
        .unwrap();

    assert_eq!(resp.responses().len(), 2);
    assert!(resp.responses()[0].item().is_some());
    assert!(resp.responses()[1].item().is_some());
}

#[tokio::test]
async fn transact_get_non_existent_item() {
    let c = client();
    let t = tables().await;
    let table = &t.simple_key_string;
    let item = create_item(table);

    let resp = c
        .transact_get_items()
        .transact_items(
            TransactGetItem::builder()
                .get(
                    Get::builder()
                        .table_name(table)
                        .set_key(Some(get_key(table, &item)))
                        .build()
                        .unwrap(),
                )
                .build(),
        )
        .send()
        .await
        .unwrap();

    assert_eq!(resp.responses().len(), 1);
    assert!(resp.responses()[0].item().is_none());
}

#[tokio::test]
async fn transact_get_with_projection() {
    let c = client();
    let t = tables().await;
    let table = &t.simple_key_string;
    let mut item = create_item(table);
    item.insert("projected".into(), s("yes"));
    item.insert("hidden".into(), s("no"));
    c.put_item()
        .table_name(table)
        .set_item(Some(item.clone()))
        .send()
        .await
        .unwrap();

    let resp = c
        .transact_get_items()
        .transact_items(
            TransactGetItem::builder()
                .get(
                    Get::builder()
                        .table_name(table)
                        .set_key(Some(get_key(table, &item)))
                        .projection_expression("projected")
                        .build()
                        .unwrap(),
                )
                .build(),
        )
        .send()
        .await
        .unwrap();

    let actual = resp.responses()[0].item().unwrap();
    assert!(actual.contains_key("projected"));
    assert!(!actual.contains_key("hidden"));
}

#[tokio::test]
async fn transact_get_non_existent_table() {
    let c = client();
    let key: HashMap<String, aws_sdk_dynamodb::types::AttributeValue> =
        [(HASH_KEY_S.into(), s("k"))].into();

    let err = c
        .transact_get_items()
        .transact_items(
            TransactGetItem::builder()
                .get(
                    Get::builder()
                        .table_name("NonExistentTable_tg")
                        .set_key(Some(key))
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
async fn transact_get_mix_existing_and_missing() {
    let c = client();
    let t = tables().await;
    let table = &t.simple_key_string;
    let existing = create_item(table);
    c.put_item()
        .table_name(table)
        .set_item(Some(existing.clone()))
        .send()
        .await
        .unwrap();
    let missing = create_item(table);

    let resp = c
        .transact_get_items()
        .transact_items(
            TransactGetItem::builder()
                .get(
                    Get::builder()
                        .table_name(table)
                        .set_key(Some(get_key(table, &existing)))
                        .build()
                        .unwrap(),
                )
                .build(),
        )
        .transact_items(
            TransactGetItem::builder()
                .get(
                    Get::builder()
                        .table_name(table)
                        .set_key(Some(get_key(table, &missing)))
                        .build()
                        .unwrap(),
                )
                .build(),
        )
        .send()
        .await
        .unwrap();

    assert_eq!(resp.responses().len(), 2);
    assert!(resp.responses()[0].item().is_some());
    assert!(resp.responses()[1].item().is_none());
}

#[tokio::test]
async fn transact_get_composite_key() {
    let c = client();
    let t = tables().await;
    let table = &t.comp_key_string_number;
    let item = create_item(table);
    c.put_item()
        .table_name(table)
        .set_item(Some(item.clone()))
        .send()
        .await
        .unwrap();

    let resp = c
        .transact_get_items()
        .transact_items(
            TransactGetItem::builder()
                .get(
                    Get::builder()
                        .table_name(table)
                        .set_key(Some(get_key(table, &item)))
                        .build()
                        .unwrap(),
                )
                .build(),
        )
        .send()
        .await
        .unwrap();

    let actual = resp.responses()[0].item().unwrap();
    for (k, v) in &item {
        assert_eq!(actual.get(k).unwrap(), v, "Attribute {k} mismatch");
    }
}

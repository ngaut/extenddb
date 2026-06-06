// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! BatchGetItem integration tests — expands coverage beyond the basic tests in get_item.rs.

use crate::test_base::*;
use aws_sdk_dynamodb::types::KeysAndAttributes;
use std::collections::HashMap;

#[tokio::test]
async fn batch_get_single_item() {
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
        .batch_get_item()
        .request_items(
            table,
            KeysAndAttributes::builder()
                .keys(get_key(table, &item))
                .build()
                .unwrap(),
        )
        .send()
        .await
        .unwrap();

    let items = resp.responses().unwrap().get(table).unwrap();
    assert_eq!(items.len(), 1);
}

#[tokio::test]
async fn batch_get_with_projection() {
    let c = client();
    let t = tables().await;
    let table = &t.simple_key_string;
    let mut item = create_item(table);
    item.insert("visible".into(), s("yes"));
    item.insert("hidden".into(), s("no"));
    c.put_item()
        .table_name(table)
        .set_item(Some(item.clone()))
        .send()
        .await
        .unwrap();

    let resp = c
        .batch_get_item()
        .request_items(
            table,
            KeysAndAttributes::builder()
                .keys(get_key(table, &item))
                .projection_expression("visible")
                .build()
                .unwrap(),
        )
        .send()
        .await
        .unwrap();

    let items = resp.responses().unwrap().get(table).unwrap();
    assert_eq!(items.len(), 1);
    assert!(items[0].contains_key("visible"));
    assert!(!items[0].contains_key("hidden"));
}

#[tokio::test]
async fn batch_get_non_existent_keys() {
    let c = client();
    let t = tables().await;
    let table = &t.simple_key_string;
    let keys: Vec<HashMap<String, aws_sdk_dynamodb::types::AttributeValue>> = (0..3)
        .map(|_| {
            let item = create_item(table);
            get_key(table, &item)
        })
        .collect();

    let resp = c
        .batch_get_item()
        .request_items(
            table,
            KeysAndAttributes::builder()
                .set_keys(Some(keys))
                .build()
                .unwrap(),
        )
        .send()
        .await
        .unwrap();

    let items = resp.responses().unwrap().get(table).unwrap();
    assert!(items.is_empty());
}

#[tokio::test]
async fn batch_get_mix_existing_and_missing() {
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
        .batch_get_item()
        .request_items(
            table,
            KeysAndAttributes::builder()
                .keys(get_key(table, &existing))
                .keys(get_key(table, &missing))
                .build()
                .unwrap(),
        )
        .send()
        .await
        .unwrap();

    let items = resp.responses().unwrap().get(table).unwrap();
    assert_eq!(items.len(), 1);
}

#[tokio::test]
async fn batch_get_composite_key_table() {
    let c = client();
    let t = tables().await;
    let table = &t.comp_key_string_number;
    let items: Vec<_> = (0..3).map(|_| create_item(table)).collect();
    for item in &items {
        c.put_item()
            .table_name(table)
            .set_item(Some(item.clone()))
            .send()
            .await
            .unwrap();
    }

    let keys: Vec<_> = items.iter().map(|item| get_key(table, item)).collect();
    let resp = c
        .batch_get_item()
        .request_items(
            table,
            KeysAndAttributes::builder()
                .set_keys(Some(keys))
                .build()
                .unwrap(),
        )
        .send()
        .await
        .unwrap();

    let result_items = resp.responses().unwrap().get(table).unwrap();
    assert_eq!(result_items.len(), 3);
}

#[tokio::test]
async fn batch_get_consistent_read() {
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
        .batch_get_item()
        .request_items(
            table,
            KeysAndAttributes::builder()
                .keys(get_key(table, &item))
                .consistent_read(true)
                .build()
                .unwrap(),
        )
        .send()
        .await
        .unwrap();

    let items = resp.responses().unwrap().get(table).unwrap();
    assert_eq!(items.len(), 1);
}

#[tokio::test]
async fn batch_get_non_existent_table() {
    let c = client();
    let key: HashMap<String, aws_sdk_dynamodb::types::AttributeValue> =
        [(HASH_KEY_S.into(), s("k"))].into();

    let err = c
        .batch_get_item()
        .request_items(
            "NonExistentTable_bg",
            KeysAndAttributes::builder().keys(key).build().unwrap(),
        )
        .send()
        .await
        .unwrap_err();

    assert_eq!(err_code(&err), Some("ResourceNotFoundException"));
}

#[tokio::test]
async fn batch_get_expression_attribute_names() {
    let c = client();
    let t = tables().await;
    let table = &t.simple_key_string;
    let mut item = create_item(table);
    item.insert("status".into(), s("active"));
    item.insert("count".into(), n(42));
    c.put_item()
        .table_name(table)
        .set_item(Some(item.clone()))
        .send()
        .await
        .unwrap();

    let resp = c
        .batch_get_item()
        .request_items(
            table,
            KeysAndAttributes::builder()
                .keys(get_key(table, &item))
                .projection_expression("#s, #c")
                .expression_attribute_names("#s", "status")
                .expression_attribute_names("#c", "count")
                .build()
                .unwrap(),
        )
        .send()
        .await
        .unwrap();

    let items = resp.responses().unwrap().get(table).unwrap();
    assert_eq!(items.len(), 1);
    assert!(items[0].contains_key("status"));
    assert!(items[0].contains_key("count"));
    assert!(!items[0].contains_key("str"));
}

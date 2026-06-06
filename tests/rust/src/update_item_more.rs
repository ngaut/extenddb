// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! UpdateItem integration tests — conditions, composite keys, combined actions.

use crate::test_base::*;
use std::collections::HashMap;

#[tokio::test]
async fn update_item_with_condition_expression_success() {
    let c = client();
    let t = tables().await;
    let table = &t.simple_key_string;
    let mut item = create_item(table);
    item.insert("status".into(), s("pending"));
    c.put_item()
        .table_name(table)
        .set_item(Some(item.clone()))
        .send()
        .await
        .unwrap();

    let key = get_key(table, &item);
    c.update_item()
        .table_name(table)
        .set_key(Some(key.clone()))
        .update_expression("SET #s = :new")
        .condition_expression("#s = :old")
        .expression_attribute_names("#s", "status")
        .expression_attribute_values(":old", s("pending"))
        .expression_attribute_values(":new", s("done"))
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
    assert_eq!(resp.item().unwrap().get("status").unwrap(), &s("done"));
}

#[tokio::test]
async fn update_item_with_condition_expression_failure() {
    let c = client();
    let t = tables().await;
    let table = &t.simple_key_string;
    let mut item = create_item(table);
    item.insert("status".into(), s("pending"));
    c.put_item()
        .table_name(table)
        .set_item(Some(item.clone()))
        .send()
        .await
        .unwrap();

    let key = get_key(table, &item);
    let err = c
        .update_item()
        .table_name(table)
        .set_key(Some(key))
        .update_expression("SET #s = :new")
        .condition_expression("#s = :old")
        .expression_attribute_names("#s", "status")
        .expression_attribute_values(":old", s("wrong"))
        .expression_attribute_values(":new", s("done"))
        .send()
        .await
        .unwrap_err();

    assert_eq!(err_code(&err), Some("ConditionalCheckFailedException"));
}

#[tokio::test]
async fn update_item_on_non_existent_table() {
    let c = client();
    let key: HashMap<String, aws_sdk_dynamodb::types::AttributeValue> =
        [(HASH_KEY_S.into(), s("k"))].into();
    let err = c
        .update_item()
        .table_name("NonExistentTable_upd")
        .set_key(Some(key))
        .update_expression("SET x = :v")
        .expression_attribute_values(":v", n(1))
        .send()
        .await
        .unwrap_err();

    assert_eq!(err_code(&err), Some("ResourceNotFoundException"));
}

#[tokio::test]
async fn update_item_creates_item_if_not_exists() {
    let c = client();
    let t = tables().await;
    let table = &t.simple_key_string;
    let item = create_item(table);
    let key = get_key(table, &item);

    // UpdateItem on a non-existent key creates the item.
    c.update_item()
        .table_name(table)
        .set_key(Some(key.clone()))
        .update_expression("SET newAttr = :v")
        .expression_attribute_values(":v", s("created"))
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
    let got = resp.item().expect("Item should be created by UpdateItem");
    assert_eq!(got.get("newAttr").unwrap(), &s("created"));
}

#[tokio::test]
async fn update_item_multiple_set_actions() {
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

    let key = get_key(table, &item);
    c.update_item()
        .table_name(table)
        .set_key(Some(key.clone()))
        .update_expression("SET a = :a, b = :b, c = :c")
        .expression_attribute_values(":a", s("alpha"))
        .expression_attribute_values(":b", n(2))
        .expression_attribute_values(":c", bool_val(true))
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
    let got = resp.item().unwrap();
    assert_eq!(got.get("a").unwrap(), &s("alpha"));
    assert_eq!(got.get("b").unwrap(), &n(2));
    assert_eq!(got.get("c").unwrap(), &bool_val(true));
}

#[tokio::test]
async fn update_item_set_and_remove_combined() {
    let c = client();
    let t = tables().await;
    let table = &t.simple_key_string;
    let mut item = create_item(table);
    item.insert("keep".into(), s("yes"));
    item.insert("drop".into(), s("no"));
    c.put_item()
        .table_name(table)
        .set_item(Some(item.clone()))
        .send()
        .await
        .unwrap();

    let key = get_key(table, &item);
    c.update_item()
        .table_name(table)
        .set_key(Some(key.clone()))
        .update_expression("SET added = :v REMOVE #d")
        .expression_attribute_names("#d", "drop")
        .expression_attribute_values(":v", n(99))
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
    let got = resp.item().unwrap();
    assert_eq!(got.get("added").unwrap(), &n(99));
    assert!(got.get("drop").is_none(), "'drop' should be removed");
    assert_eq!(got.get("keep").unwrap(), &s("yes"));
}

#[tokio::test]
async fn update_item_if_not_exists() {
    let c = client();
    let t = tables().await;
    let table = &t.simple_key_string;
    let mut item = create_item(table);
    item.insert("counter".into(), n(5));
    c.put_item()
        .table_name(table)
        .set_item(Some(item.clone()))
        .send()
        .await
        .unwrap();

    let key = get_key(table, &item);
    // if_not_exists should keep the existing value.
    c.update_item()
        .table_name(table)
        .set_key(Some(key.clone()))
        .update_expression("SET #c = if_not_exists(#c, :default)")
        .expression_attribute_names("#c", "counter")
        .expression_attribute_values(":default", n(0))
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
    assert_eq!(resp.item().unwrap().get("counter").unwrap(), &n(5));
}

#[tokio::test]
async fn update_item_composite_key() {
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

    let key = get_key(table, &item);
    c.update_item()
        .table_name(table)
        .set_key(Some(key.clone()))
        .update_expression("SET updated = :v")
        .expression_attribute_values(":v", bool_val(true))
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
    assert_eq!(
        resp.item().unwrap().get("updated").unwrap(),
        &bool_val(true)
    );
}

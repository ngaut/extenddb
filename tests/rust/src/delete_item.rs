// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! DeleteItem integration tests.

use crate::test_base::*;

#[tokio::test]
async fn delete_existing_item() {
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
    check_item(c, table, &item).await;

    let key = get_key(table, &item);
    c.delete_item()
        .table_name(table)
        .set_key(Some(key.clone()))
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
    assert!(resp.item().is_none(), "Item should be deleted");
}

#[tokio::test]
async fn delete_non_existent_item() {
    let c = client();
    let t = tables().await;
    let table = &t.simple_key_string;
    let item = create_item(table);
    let key = get_key(table, &item);
    // Deleting a non-existent item should succeed silently.
    c.delete_item()
        .table_name(table)
        .set_key(Some(key))
        .send()
        .await
        .unwrap();
}

#[tokio::test]
async fn delete_item_from_non_existent_table() {
    let c = client();
    let key = [(HASH_KEY_S.into(), s("k"))].into();
    let err = c
        .delete_item()
        .table_name("NonExistentTable_del")
        .set_key(Some(key))
        .send()
        .await
        .unwrap_err();
    assert_eq!(err_code(&err), Some("ResourceNotFoundException"));
}

#[tokio::test]
async fn delete_item_return_values_all_old() {
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
    let resp = c
        .delete_item()
        .table_name(table)
        .set_key(Some(key))
        .return_values(aws_sdk_dynamodb::types::ReturnValue::AllOld)
        .send()
        .await
        .unwrap();

    let old = resp.attributes().expect("Should return old item");
    for (k, v) in &item {
        assert_eq!(old.get(k).unwrap(), v, "Attribute {k} mismatch");
    }
}

#[tokio::test]
async fn delete_item_return_values_none() {
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
    let resp = c
        .delete_item()
        .table_name(table)
        .set_key(Some(key))
        .return_values(aws_sdk_dynamodb::types::ReturnValue::None)
        .send()
        .await
        .unwrap();

    assert!(
        resp.attributes().is_none(),
        "NONE should not return attributes"
    );
}

#[tokio::test]
async fn delete_item_with_condition_expression_success() {
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

    let key = get_key(table, &item);
    c.delete_item()
        .table_name(table)
        .set_key(Some(key))
        .condition_expression("#s = :v")
        .expression_attribute_names("#s", "status")
        .expression_attribute_values(":v", s("active"))
        .send()
        .await
        .unwrap();

    let key2 = get_key(table, &item);
    let resp = c
        .get_item()
        .table_name(table)
        .set_key(Some(key2))
        .consistent_read(true)
        .send()
        .await
        .unwrap();
    assert!(resp.item().is_none(), "Item should be deleted");
}

#[tokio::test]
async fn delete_item_with_condition_expression_failure() {
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

    let key = get_key(table, &item);
    let err = c
        .delete_item()
        .table_name(table)
        .set_key(Some(key))
        .condition_expression("#s = :v")
        .expression_attribute_names("#s", "status")
        .expression_attribute_values(":v", s("inactive"))
        .send()
        .await
        .unwrap_err();

    assert_eq!(err_code(&err), Some("ConditionalCheckFailedException"));
}

#[tokio::test]
async fn delete_item_composite_key() {
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
    c.delete_item()
        .table_name(table)
        .set_key(Some(key.clone()))
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
    assert!(resp.item().is_none(), "Item should be deleted");
}

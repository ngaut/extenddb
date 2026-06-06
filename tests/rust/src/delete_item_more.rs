// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Additional delete item tests — mirrors remaining Java `DeleteItemTests` scenarios.

use crate::test_base::*;
use aws_sdk_dynamodb::types::{ReturnValue, ReturnValuesOnConditionCheckFailure};

#[tokio::test]
async fn delete_item_not_part_of_gsi() {
    let c = client();
    let t = tables().await;
    let item = create_item(&t.simple_key_string_gsi);
    let key = get_key(&t.simple_key_string_gsi, &item);

    c.put_item()
        .table_name(&t.simple_key_string_gsi)
        .set_item(Some(item))
        .send()
        .await
        .unwrap();

    let get = c
        .get_item()
        .table_name(&t.simple_key_string_gsi)
        .set_key(Some(key.clone()))
        .consistent_read(true)
        .send()
        .await
        .unwrap();
    assert!(get.item().is_some());

    c.delete_item()
        .table_name(&t.simple_key_string_gsi)
        .set_key(Some(key.clone()))
        .send()
        .await
        .unwrap();

    let get2 = c
        .get_item()
        .table_name(&t.simple_key_string_gsi)
        .set_key(Some(key))
        .consistent_read(true)
        .send()
        .await
        .unwrap();
    assert!(get2.item().is_none());
}

#[tokio::test]
async fn delete_item_part_of_gsi() {
    let c = client();
    let t = tables().await;
    let item = create_item_with_gsi(&t.simple_key_string_gsi);
    let key = get_key(&t.simple_key_string_gsi, &item);

    c.put_item()
        .table_name(&t.simple_key_string_gsi)
        .set_item(Some(item))
        .send()
        .await
        .unwrap();

    c.delete_item()
        .table_name(&t.simple_key_string_gsi)
        .set_key(Some(key.clone()))
        .send()
        .await
        .unwrap();

    let get = c
        .get_item()
        .table_name(&t.simple_key_string_gsi)
        .set_key(Some(key))
        .consistent_read(true)
        .send()
        .await
        .unwrap();
    assert!(get.item().is_none());
}

#[tokio::test]
async fn delete_item_with_return_value_expression() {
    let c = client();
    let t = tables().await;
    let mut item = create_item(&t.comp_key_string_number);
    item.insert("str-attr".into(), s("some-attr"));
    let key = get_key(&t.comp_key_string_number, &item);

    c.put_item()
        .table_name(&t.comp_key_string_number)
        .set_item(Some(item.clone()))
        .send()
        .await
        .unwrap();

    let resp = c
        .delete_item()
        .table_name(&t.comp_key_string_number)
        .set_key(Some(key))
        .condition_expression("attribute_exists(#pk)")
        .expression_attribute_names("#pk", HASH_KEY_S)
        .return_values(ReturnValue::AllOld)
        .send()
        .await
        .unwrap();

    let attrs = resp.attributes().unwrap();
    assert_eq!(attrs.get("str-attr").unwrap().as_s().unwrap(), "some-attr");
}

#[tokio::test]
async fn delete_item_with_invalid_table_name() {
    let c = client();
    // Use "x" (1 char) — passes SDK client-side validation but fails
    // server-side (DynamoDB requires table names ≥ 3 characters).
    let err = c
        .delete_item()
        .table_name("x")
        .key(HASH_KEY_S, s("test"))
        .send()
        .await
        .unwrap_err();
    assert!(err_code(&err).is_some());
}

#[tokio::test]
async fn delete_item_return_values_on_condition_check_failure() {
    let c = client();
    let t = tables().await;
    let mut item = create_item(&t.simple_key_string);
    item.insert("attr1".into(), s("value1"));
    let key = get_key(&t.simple_key_string, &item);

    c.put_item()
        .table_name(&t.simple_key_string)
        .set_item(Some(item))
        .send()
        .await
        .unwrap();

    let err = c
        .delete_item()
        .table_name(&t.simple_key_string)
        .set_key(Some(key))
        .condition_expression("attr1 = :val")
        .expression_attribute_values(":val", s("nonExistentValue"))
        .return_values_on_condition_check_failure(ReturnValuesOnConditionCheckFailure::AllOld)
        .send()
        .await
        .unwrap_err();
    assert_eq!(err_code(&err), Some("ConditionalCheckFailedException"));
}

#[tokio::test]
async fn delete_if_exists_condition() {
    let c = client();
    let t = tables().await;
    let item = create_item(&t.simple_key_string);
    let key = get_key(&t.simple_key_string, &item);

    c.put_item()
        .table_name(&t.simple_key_string)
        .set_item(Some(item))
        .send()
        .await
        .unwrap();

    // Delete with condition that key exists — should succeed
    c.delete_item()
        .table_name(&t.simple_key_string)
        .set_key(Some(key.clone()))
        .condition_expression("attribute_exists(#pk)")
        .expression_attribute_names("#pk", HASH_KEY_S)
        .send()
        .await
        .unwrap();

    // Try again — item is gone, condition should fail
    let err = c
        .delete_item()
        .table_name(&t.simple_key_string)
        .set_key(Some(key))
        .condition_expression("attribute_exists(#pk)")
        .expression_attribute_names("#pk", HASH_KEY_S)
        .send()
        .await
        .unwrap_err();
    assert_eq!(err_code(&err), Some("ConditionalCheckFailedException"));
}

#[tokio::test]
async fn delete_item_with_null_check() {
    let c = client();
    let t = tables().await;
    let item = create_item(&t.simple_key_string);
    let key = get_key(&t.simple_key_string, &item);

    c.put_item()
        .table_name(&t.simple_key_string)
        .set_item(Some(item))
        .send()
        .await
        .unwrap();

    // attribute_not_exists on a non-existent attribute should succeed
    c.delete_item()
        .table_name(&t.simple_key_string)
        .set_key(Some(key))
        .condition_expression("attribute_not_exists(optionalAttr)")
        .send()
        .await
        .unwrap();
}

#[tokio::test]
async fn delete_item_with_not_null_check() {
    let c = client();
    let t = tables().await;
    let mut item = create_item(&t.simple_key_string);
    item.insert("existingAttr".into(), s("exists"));
    let key = get_key(&t.simple_key_string, &item);

    c.put_item()
        .table_name(&t.simple_key_string)
        .set_item(Some(item))
        .send()
        .await
        .unwrap();

    // attribute_exists on an existing attribute should succeed
    c.delete_item()
        .table_name(&t.simple_key_string)
        .set_key(Some(key))
        .condition_expression("attribute_exists(existingAttr)")
        .send()
        .await
        .unwrap();
}

#[tokio::test]
async fn delete_item_with_numeric_comparison() {
    let c = client();
    let t = tables().await;
    let mut item = create_item(&t.simple_key_string);
    item.insert("numAttr".into(), n(50));
    let key = get_key(&t.simple_key_string, &item);

    c.put_item()
        .table_name(&t.simple_key_string)
        .set_item(Some(item))
        .send()
        .await
        .unwrap();

    // numAttr <= 49 should fail
    let err = c
        .delete_item()
        .table_name(&t.simple_key_string)
        .set_key(Some(key.clone()))
        .condition_expression("numAttr <= :val")
        .expression_attribute_values(":val", n(49))
        .send()
        .await
        .unwrap_err();
    assert_eq!(err_code(&err), Some("ConditionalCheckFailedException"));

    // numAttr <= 50 should succeed
    c.delete_item()
        .table_name(&t.simple_key_string)
        .set_key(Some(key))
        .condition_expression("numAttr <= :val")
        .expression_attribute_values(":val", n(50))
        .send()
        .await
        .unwrap();
}

#[tokio::test]
async fn delete_item_with_between_comparison() {
    let c = client();
    let t = tables().await;
    let mut item = create_item(&t.simple_key_string);
    item.insert("numAttr".into(), n(50));
    let key = get_key(&t.simple_key_string, &item);

    c.put_item()
        .table_name(&t.simple_key_string)
        .set_item(Some(item))
        .send()
        .await
        .unwrap();

    // BETWEEN 40 and 60 should succeed
    c.delete_item()
        .table_name(&t.simple_key_string)
        .set_key(Some(key))
        .condition_expression("numAttr BETWEEN :lo AND :hi")
        .expression_attribute_values(":lo", n(40))
        .expression_attribute_values(":hi", n(60))
        .send()
        .await
        .unwrap();
}

#[tokio::test]
async fn delete_item_blob_key() {
    let c = client();
    let t = tables().await;
    let item = create_item(&t.comp_key_blob_number);
    let key = get_key(&t.comp_key_blob_number, &item);

    c.put_item()
        .table_name(&t.comp_key_blob_number)
        .set_item(Some(item))
        .send()
        .await
        .unwrap();

    c.delete_item()
        .table_name(&t.comp_key_blob_number)
        .set_key(Some(key.clone()))
        .send()
        .await
        .unwrap();

    let get = c
        .get_item()
        .table_name(&t.comp_key_blob_number)
        .set_key(Some(key))
        .consistent_read(true)
        .send()
        .await
        .unwrap();
    assert!(get.item().is_none());
}

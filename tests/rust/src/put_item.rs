// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! PutItem tests — part 1: core operations.
//! Mirrors `PutItemTests.java` (first 22 tests).

use crate::test_base::*;
use aws_sdk_dynamodb::types::{ExpectedAttributeValue, ReturnValue};
use std::collections::HashMap;

#[tokio::test]
async fn vanilla_put_item() {
    let c = client();
    let t = tables().await;
    for table in [
        &t.simple_key_string,
        &t.comp_key_string_number,
        &t.comp_key_blob_number,
    ] {
        let item = create_item(table);
        c.put_item()
            .table_name(table)
            .set_item(Some(item.clone()))
            .send()
            .await
            .unwrap();
        check_item(c, table, &item).await;
    }
}

#[tokio::test]
async fn put_item_with_condition_expression() {
    let c = client();
    let t = tables().await;
    let mut item = create_item(&t.simple_key_string);
    item.insert("dummyName".into(), s("dummyValue"));
    c.put_item()
        .table_name(&t.simple_key_string)
        .set_item(Some(item.clone()))
        .send()
        .await
        .unwrap();

    let err = c
        .put_item()
        .table_name(&t.simple_key_string)
        .set_item(Some(item.clone()))
        .condition_expression("dummyName <> :value")
        .expression_attribute_values(":value", s("dummyValue"))
        .send()
        .await;
    assert!(err.is_err());
    assert_eq!(
        err_code(&err.unwrap_err()),
        Some("ConditionalCheckFailedException")
    );

    c.put_item()
        .table_name(&t.simple_key_string)
        .set_item(Some(item.clone()))
        .condition_expression("dummyName = :value")
        .expression_attribute_values(":value", s("dummyValue"))
        .send()
        .await
        .unwrap();
}

#[tokio::test]
async fn put_item_with_map_all_supported_attributes() {
    let c = client();
    let t = tables().await;
    for table in [
        &t.simple_key_string,
        &t.comp_key_string_number,
        &t.comp_key_blob_number,
    ] {
        let mut item = create_item(table);
        item.insert(
            "AllSupportedAttr".into(),
            map_val(create_map_with_all_types()),
        );
        c.put_item()
            .table_name(table)
            .set_item(Some(item.clone()))
            .send()
            .await
            .unwrap();
        check_item(c, table, &item).await;
    }
}

#[tokio::test]
async fn put_item_with_nested_empty_map() {
    let c = client();
    let t = tables().await;
    let mut item = create_item(&t.simple_key_string);
    let mut nested = HashMap::new();
    nested.insert("emptyMap".into(), map_val(HashMap::new()));
    item.insert("MapAllEmptyNestedAttr".into(), map_val(nested));
    c.put_item()
        .table_name(&t.simple_key_string)
        .set_item(Some(item.clone()))
        .send()
        .await
        .unwrap();
    check_item(c, &t.simple_key_string, &item).await;
}

#[tokio::test]
async fn put_item_with_nested_non_empty_map() {
    let c = client();
    let t = tables().await;
    let mut item = create_item(&t.simple_key_string);
    let mut inner = HashMap::new();
    inner.insert("key1".into(), s("val1"));
    inner.insert("key2".into(), n(42));
    let mut outer = HashMap::new();
    outer.insert("nested".into(), map_val(inner));
    item.insert("MapNonEmptyNestedAttr".into(), map_val(outer));
    c.put_item()
        .table_name(&t.simple_key_string)
        .set_item(Some(item.clone()))
        .send()
        .await
        .unwrap();
    check_item(c, &t.simple_key_string, &item).await;
}

#[tokio::test]
async fn put_item_with_list_all_supported_attributes() {
    let c = client();
    let t = tables().await;
    let mut item = create_item(&t.simple_key_string);
    let mut m = HashMap::new();
    m.insert("k".into(), s("v"));
    item.insert(
        "AllSupportedList".into(),
        list_val(vec![
            s("str"),
            n(123),
            b("binary"),
            bool_val(true),
            bool_val(false),
            null_val(),
            list_val(vec![s("nested")]),
            map_val(m),
        ]),
    );
    c.put_item()
        .table_name(&t.simple_key_string)
        .set_item(Some(item.clone()))
        .send()
        .await
        .unwrap();
    check_item(c, &t.simple_key_string, &item).await;
}

#[tokio::test]
async fn put_item_with_nested_empty_list() {
    let c = client();
    let t = tables().await;
    let mut item = create_item(&t.simple_key_string);
    item.insert("nestedEmptyList".into(), list_val(vec![list_val(vec![])]));
    c.put_item()
        .table_name(&t.simple_key_string)
        .set_item(Some(item.clone()))
        .send()
        .await
        .unwrap();
    check_item(c, &t.simple_key_string, &item).await;
}

#[tokio::test]
async fn put_item_with_nested_non_empty_list() {
    let c = client();
    let t = tables().await;
    let mut item = create_item(&t.simple_key_string);
    item.insert(
        "nestedList".into(),
        list_val(vec![
            list_val(vec![s("a"), n(1)]),
            list_val(vec![bool_val(true), null_val()]),
        ]),
    );
    c.put_item()
        .table_name(&t.simple_key_string)
        .set_item(Some(item.clone()))
        .send()
        .await
        .unwrap();
    check_item(c, &t.simple_key_string, &item).await;
}

#[tokio::test]
async fn put_item_with_boolean_attributes() {
    let c = client();
    let t = tables().await;
    let mut item = create_item(&t.simple_key_string);
    item.insert("boolTrue".into(), bool_val(true));
    item.insert("boolFalse".into(), bool_val(false));
    c.put_item()
        .table_name(&t.simple_key_string)
        .set_item(Some(item.clone()))
        .send()
        .await
        .unwrap();
    check_item(c, &t.simple_key_string, &item).await;
}

#[tokio::test]
async fn put_item_with_null_data_type() {
    let c = client();
    let t = tables().await;
    let mut item = create_item(&t.simple_key_string);
    item.insert("nullAttr".into(), null_val());
    c.put_item()
        .table_name(&t.simple_key_string)
        .set_item(Some(item.clone()))
        .send()
        .await
        .unwrap();
    check_item(c, &t.simple_key_string, &item).await;
}

#[tokio::test]
async fn put_items_in_multiple_tables() {
    let c = client();
    let t = tables().await;
    for table in [
        &t.simple_key_string,
        &t.comp_key_string_number,
        &t.comp_key_blob_number,
    ] {
        let item = create_item(table);
        c.put_item()
            .table_name(table)
            .set_item(Some(item.clone()))
            .send()
            .await
            .unwrap();
        check_item(c, table, &item).await;
    }
}

#[tokio::test]
async fn put_item_with_multiple_binary_sets() {
    let c = client();
    let t = tables().await;
    let mut item = create_item(&t.simple_key_string);
    item.insert("blobSet1".into(), bs(&["bin1", "bin2"]));
    item.insert("blobSet2".into(), bs(&["bin3", "bin4", "bin5"]));
    c.put_item()
        .table_name(&t.simple_key_string)
        .set_item(Some(item.clone()))
        .send()
        .await
        .unwrap();
    check_item(c, &t.simple_key_string, &item).await;
}

#[tokio::test]
async fn put_item_with_return_value_all_old() {
    let c = client();
    let t = tables().await;
    let item = create_item(&t.simple_key_string);
    c.put_item()
        .table_name(&t.simple_key_string)
        .set_item(Some(item.clone()))
        .send()
        .await
        .unwrap();

    let mut new_item = item.clone();
    new_item.insert("newAttr".into(), s("newValue"));
    let result = c
        .put_item()
        .table_name(&t.simple_key_string)
        .set_item(Some(new_item))
        .return_values(ReturnValue::AllOld)
        .send()
        .await
        .unwrap();

    let attrs = result.attributes().unwrap();
    assert!(!attrs.is_empty());
    assert_item_eq(
        &item,
        &attrs.iter().map(|(k, v)| (k.clone(), v.clone())).collect(),
    );
}

#[tokio::test]
async fn put_item_with_return_value_none() {
    let c = client();
    let t = tables().await;
    let item = create_item(&t.simple_key_string);
    c.put_item()
        .table_name(&t.simple_key_string)
        .set_item(Some(item.clone()))
        .send()
        .await
        .unwrap();

    let result = c
        .put_item()
        .table_name(&t.simple_key_string)
        .set_item(Some(item))
        .return_values(ReturnValue::None)
        .send()
        .await
        .unwrap();

    assert!(result.attributes().map_or(true, |a| a.is_empty()));
}

#[tokio::test]
async fn put_existing_item_with_expected_false() {
    let c = client();
    let t = tables().await;
    let item = create_item(&t.simple_key_string);
    c.put_item()
        .table_name(&t.simple_key_string)
        .set_item(Some(item.clone()))
        .send()
        .await
        .unwrap();

    let err = c
        .put_item()
        .table_name(&t.simple_key_string)
        .set_item(Some(item))
        .expected(
            HASH_KEY_S,
            ExpectedAttributeValue::builder().exists(false).build(),
        )
        .send()
        .await;
    assert!(err.is_err());
    assert_eq!(
        err_code(&err.unwrap_err()),
        Some("ConditionalCheckFailedException")
    );
}

#[tokio::test]
async fn put_non_existent_item_with_expected_false() {
    let c = client();
    let t = tables().await;
    let item = create_item(&t.simple_key_string);

    c.put_item()
        .table_name(&t.simple_key_string)
        .set_item(Some(item.clone()))
        .expected(
            HASH_KEY_S,
            ExpectedAttributeValue::builder().exists(false).build(),
        )
        .send()
        .await
        .unwrap();
    check_item(c, &t.simple_key_string, &item).await;
}

#[tokio::test]
async fn put_to_replace_existing_item() {
    let c = client();
    let t = tables().await;
    let item = create_item(&t.simple_key_string);
    c.put_item()
        .table_name(&t.simple_key_string)
        .set_item(Some(item.clone()))
        .send()
        .await
        .unwrap();

    let mut new_item = get_key(&t.simple_key_string, &item);
    new_item.insert("str".into(), s("replaced"));
    new_item.insert("num".into(), n(99));
    new_item.insert("extraAttr".into(), s("extra"));
    c.put_item()
        .table_name(&t.simple_key_string)
        .set_item(Some(new_item.clone()))
        .send()
        .await
        .unwrap();

    let retrieved = check_item(c, &t.simple_key_string, &new_item).await;
    assert!(!retrieved.contains_key("oldAttr"));
}

#[tokio::test]
async fn put_item_with_expected_wrong_value() {
    let c = client();
    let t = tables().await;
    let mut item = create_item(&t.simple_key_string);
    item.insert("occ".into(), s("version1"));
    c.put_item()
        .table_name(&t.simple_key_string)
        .set_item(Some(item.clone()))
        .send()
        .await
        .unwrap();

    let err = c
        .put_item()
        .table_name(&t.simple_key_string)
        .set_item(Some(item))
        .expected(
            "occ",
            ExpectedAttributeValue::builder()
                .value(s("wrongVersion"))
                .build(),
        )
        .send()
        .await;
    assert!(err.is_err());
    assert_eq!(
        err_code(&err.unwrap_err()),
        Some("ConditionalCheckFailedException")
    );
}

#[tokio::test]
async fn put_item_in_gsi_table() {
    let c = client();
    let t = tables().await;
    let item = create_item_with_gsi(&t.simple_key_string_gsi);
    c.put_item()
        .table_name(&t.simple_key_string_gsi)
        .set_item(Some(item.clone()))
        .send()
        .await
        .unwrap();
    check_item(c, &t.simple_key_string_gsi, &item).await;
}

#[tokio::test]
async fn put_item_without_gsi_key_attributes() {
    let c = client();
    let t = tables().await;
    let item = create_item(&t.simple_key_string_gsi);
    c.put_item()
        .table_name(&t.simple_key_string_gsi)
        .set_item(Some(item.clone()))
        .send()
        .await
        .unwrap();
    check_item(c, &t.simple_key_string_gsi, &item).await;
}

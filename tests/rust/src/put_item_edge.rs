// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! PutItem tests — part 2: edge cases, limits, and error paths.
//! Mirrors remaining tests from `PutItemTests.java`.

use crate::test_base::*;
use aws_sdk_dynamodb::types::{AttributeValue, ReturnValuesOnConditionCheckFailure};
use std::collections::HashMap;
use uuid::Uuid;

#[tokio::test]
async fn put_item_with_special_characters() {
    let c = client();
    let t = tables().await;
    let mut item = create_item(&t.simple_key_string);
    let mut special = HashMap::new();
    special.insert("unicode".into(), s("\u{2665} hello!"));
    special.insert("japanese".into(), s("\u{30F0}"));
    special.insert("symbols".into(), s("#@$%^"));
    special.insert("cyrillic".into(), s("яшедгвдыуйчйк"));
    special.insert("hindi".into(), s("आत्मसमर्पण"));
    item.insert("specialChars".into(), map_val(special));
    c.put_item()
        .table_name(&t.simple_key_string)
        .set_item(Some(item.clone()))
        .send()
        .await
        .unwrap();
    check_item(c, &t.simple_key_string, &item).await;
}

#[tokio::test]
async fn put_item_with_different_number_values() {
    let c = client();
    let t = tables().await;
    let mut item = create_item(&t.simple_key_string);
    item.insert("posInt".into(), n(42));
    item.insert("negInt".into(), n(-42));
    item.insert("zero".into(), n(0));
    item.insert("decimal".into(), AttributeValue::N("3.14".into()));
    item.insert("negDecimal".into(), AttributeValue::N("-3.14".into()));
    item.insert("largeNum".into(), AttributeValue::N("999999999999".into()));
    c.put_item()
        .table_name(&t.simple_key_string)
        .set_item(Some(item.clone()))
        .send()
        .await
        .unwrap();
    check_item(c, &t.simple_key_string, &item).await;
}

#[tokio::test]
async fn put_zero_as_attribute_value_in_set() {
    let c = client();
    let t = tables().await;
    let mut item = create_item(&t.simple_key_string);
    item.insert("numSet".into(), ns(&[0, 1, 2]));
    c.put_item()
        .table_name(&t.simple_key_string)
        .set_item(Some(item.clone()))
        .send()
        .await
        .unwrap();
    check_item(c, &t.simple_key_string, &item).await;
}

#[tokio::test]
async fn put_zero_to_new_attribute() {
    let c = client();
    let t = tables().await;
    let mut item = create_item(&t.simple_key_string);
    item.insert("zeroAttr".into(), n(0));
    c.put_item()
        .table_name(&t.simple_key_string)
        .set_item(Some(item.clone()))
        .send()
        .await
        .unwrap();
    check_item(c, &t.simple_key_string, &item).await;
}

#[tokio::test]
async fn put_item_exceeding_max_item_size() {
    let c = client();
    let t = tables().await;
    let mut item = create_item(&t.simple_key_string);
    item.insert("largeAttr".into(), s(&generate_string(400 * 1024 + 1)));
    let err = c
        .put_item()
        .table_name(&t.simple_key_string)
        .set_item(Some(item))
        .send()
        .await;
    assert!(err.is_err());
}

#[tokio::test]
async fn put_item_max_attribute_name_size() {
    let c = client();
    let t = tables().await;
    let mut item = create_item(&t.simple_key_string);
    item.insert(generate_string(65535 + 1), s("value"));
    let err = c
        .put_item()
        .table_name(&t.simple_key_string)
        .set_item(Some(item))
        .send()
        .await;
    assert!(err.is_err());
}

#[tokio::test]
async fn put_item_hash_key_at_limit() {
    let c = client();
    let t = tables().await;
    let mut item = HashMap::new();
    item.insert(HASH_KEY_S.into(), s(&generate_string(2048)));
    item.insert("data".into(), s("test"));
    c.put_item()
        .table_name(&t.simple_key_string)
        .set_item(Some(item.clone()))
        .send()
        .await
        .unwrap();

    let mut key = HashMap::new();
    key.insert(HASH_KEY_S.into(), item[HASH_KEY_S].clone());
    let result = c
        .get_item()
        .table_name(&t.simple_key_string)
        .set_key(Some(key))
        .consistent_read(true)
        .send()
        .await
        .unwrap();
    assert!(result.item().is_some());
}

#[tokio::test]
async fn put_item_hash_key_over_limit() {
    let c = client();
    let t = tables().await;
    let mut item = HashMap::new();
    item.insert(HASH_KEY_S.into(), s(&generate_string(2049)));
    item.insert("data".into(), s("test"));
    let err = c
        .put_item()
        .table_name(&t.simple_key_string)
        .set_item(Some(item))
        .send()
        .await;
    assert!(err.is_err());
}

#[tokio::test]
async fn put_item_range_key_at_limit() {
    let c = client();
    let t = tables().await;
    let mut item = HashMap::new();
    item.insert(HASH_KEY_S.into(), s(&Uuid::new_v4().to_string()));
    item.insert(RANGE_KEY_S.into(), s(&generate_string(1024)));
    item.insert("data".into(), s("test"));
    c.put_item()
        .table_name(&t.comp_key_string_string_gsi)
        .set_item(Some(item.clone()))
        .send()
        .await
        .unwrap();

    let mut key = HashMap::new();
    key.insert(HASH_KEY_S.into(), item[HASH_KEY_S].clone());
    key.insert(RANGE_KEY_S.into(), item[RANGE_KEY_S].clone());
    let result = c
        .get_item()
        .table_name(&t.comp_key_string_string_gsi)
        .set_key(Some(key))
        .consistent_read(true)
        .send()
        .await
        .unwrap();
    assert!(result.item().is_some());
}

#[tokio::test]
async fn put_item_range_key_over_limit() {
    let c = client();
    let t = tables().await;
    let mut item = HashMap::new();
    item.insert(HASH_KEY_S.into(), s(&Uuid::new_v4().to_string()));
    item.insert(RANGE_KEY_S.into(), s(&generate_string(1025)));
    item.insert("data".into(), s("test"));
    let err = c
        .put_item()
        .table_name(&t.comp_key_string_string_gsi)
        .set_item(Some(item))
        .send()
        .await;
    assert!(err.is_err());
}

#[tokio::test]
async fn replace_existing_item_with_list() {
    let c = client();
    let t = tables().await;
    let mut item = create_item(&t.simple_key_string);
    item.insert("attr1".into(), s("value1"));
    c.put_item()
        .table_name(&t.simple_key_string)
        .set_item(Some(item.clone()))
        .send()
        .await
        .unwrap();

    let mut new_item = get_key(&t.simple_key_string, &item);
    new_item.insert("listAttr".into(), list_val(vec![s("a"), s("b"), s("c")]));
    c.put_item()
        .table_name(&t.simple_key_string)
        .set_item(Some(new_item.clone()))
        .send()
        .await
        .unwrap();

    let retrieved = check_item(c, &t.simple_key_string, &new_item).await;
    assert!(!retrieved.contains_key("attr1"));
}

#[tokio::test]
async fn put_item_with_attribute_type_change() {
    let c = client();
    let t = tables().await;
    let mut item = create_item(&t.simple_key_string);
    item.insert("changingAttr".into(), s("stringValue"));
    c.put_item()
        .table_name(&t.simple_key_string)
        .set_item(Some(item.clone()))
        .send()
        .await
        .unwrap();

    let mut new_item = item.clone();
    new_item.insert("changingAttr".into(), n(42));
    c.put_item()
        .table_name(&t.simple_key_string)
        .set_item(Some(new_item.clone()))
        .send()
        .await
        .unwrap();
    check_item(c, &t.simple_key_string, &new_item).await;
}

#[tokio::test]
async fn put_item_with_all_old_rv_on_ccf_and_failing_condition() {
    let c = client();
    let t = tables().await;
    let mut item = create_item(&t.simple_key_string);
    item.insert("attr1".into(), s("value1"));
    c.put_item()
        .table_name(&t.simple_key_string)
        .set_item(Some(item.clone()))
        .send()
        .await
        .unwrap();

    let mut new_item = item.clone();
    new_item.insert("attr1".into(), s("newValue"));
    let err = c
        .put_item()
        .table_name(&t.simple_key_string)
        .set_item(Some(new_item))
        .condition_expression("attr1 = :val")
        .expression_attribute_values(":val", s("nonExistentValue"))
        .return_values_on_condition_check_failure(ReturnValuesOnConditionCheckFailure::AllOld)
        .send()
        .await;

    assert!(err.is_err());
    assert_eq!(
        err_code(&err.unwrap_err()),
        Some("ConditionalCheckFailedException")
    );
}

#[tokio::test]
async fn put_item_with_invalid_table_name() {
    let c = client();
    let mut item = HashMap::new();
    item.insert(HASH_KEY_S.into(), s("test"));
    let err = c
        .put_item()
        .table_name("")
        .set_item(Some(item))
        .send()
        .await;
    assert!(err.is_err());
}

#[tokio::test]
async fn put_item_to_non_existent_table() {
    let c = client();
    let mut item = HashMap::new();
    item.insert(HASH_KEY_S.into(), s("test"));
    let err = c
        .put_item()
        .table_name(format!(
            "nonExistentTable_{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis()
        ))
        .set_item(Some(item))
        .send()
        .await;
    assert!(err.is_err());
    assert_eq!(
        err_code(&err.unwrap_err()),
        Some("ResourceNotFoundException")
    );
}

#[tokio::test]
async fn complex_multi_attribute_put() {
    let c = client();
    let t = tables().await;
    let mut item = create_item(&t.simple_key_string);
    item.insert("strAttr".into(), s("hello"));
    item.insert("numAttr".into(), n(123));
    item.insert("blobAttr".into(), b("binaryData"));
    item.insert("ssAttr".into(), ss(&["a", "b", "c"]));
    item.insert("nsAttr".into(), ns(&[1, 2, 3]));
    item.insert("bsAttr".into(), bs(&["b1", "b2"]));
    item.insert("boolAttr".into(), bool_val(true));
    item.insert("nullAttr".into(), null_val());
    item.insert("listAttr".into(), list_val(vec![s("x"), n(9)]));
    let mut nested = HashMap::new();
    nested.insert("inner".into(), s("innerVal"));
    item.insert("mapAttr".into(), map_val(nested));
    c.put_item()
        .table_name(&t.simple_key_string)
        .set_item(Some(item.clone()))
        .send()
        .await
        .unwrap();
    check_item(c, &t.simple_key_string, &item).await;
}

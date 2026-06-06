// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! GetItem tests — mirrors `GetItemTests.java` (23 tests).

use crate::test_base::*;
use aws_sdk_dynamodb::types::{AttributeValue, KeysAndAttributes};
use std::collections::HashMap;
use uuid::Uuid;

// ========== BASIC GET TESTS ==========

#[tokio::test]
async fn get_single_item() {
    let c = client();
    let t = tables().await;
    let mut item = create_item(&t.simple_key_string);
    item.insert("extra".into(), s("extraVal"));
    c.put_item()
        .table_name(&t.simple_key_string)
        .set_item(Some(item.clone()))
        .send()
        .await
        .unwrap();

    let key = get_key(&t.simple_key_string, &item);
    let result = c
        .get_item()
        .table_name(&t.simple_key_string)
        .set_key(Some(key))
        .consistent_read(true)
        .send()
        .await
        .unwrap();
    assert!(result.item().is_some());
    let actual: HashMap<String, AttributeValue> = result
        .item()
        .unwrap()
        .iter()
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect();
    assert_item_eq(&item, &actual);
}

#[tokio::test]
async fn get_single_item_with_projection_expression() {
    let c = client();
    let t = tables().await;
    let mut item = create_item(&t.simple_key_string);
    item.insert("projected".into(), s("yes"));
    item.insert("notProjected".into(), s("no"));
    c.put_item()
        .table_name(&t.simple_key_string)
        .set_item(Some(item.clone()))
        .send()
        .await
        .unwrap();

    let key = get_key(&t.simple_key_string, &item);
    let result = c
        .get_item()
        .table_name(&t.simple_key_string)
        .set_key(Some(key))
        .consistent_read(true)
        .projection_expression("projected")
        .send()
        .await
        .unwrap();

    let resp_item = result.item().unwrap();
    assert!(resp_item.contains_key("projected"));
    assert!(!resp_item.contains_key("notProjected"));
}

#[tokio::test]
async fn get_item_some_attrs_do_not_exist() {
    let c = client();
    let t = tables().await;
    let mut item = create_item(&t.simple_key_string);
    item.insert("existingAttr".into(), s("exists"));
    c.put_item()
        .table_name(&t.simple_key_string)
        .set_item(Some(item.clone()))
        .send()
        .await
        .unwrap();

    let key = get_key(&t.simple_key_string, &item);
    let result = c
        .get_item()
        .table_name(&t.simple_key_string)
        .set_key(Some(key))
        .consistent_read(true)
        .attributes_to_get("existingAttr")
        .attributes_to_get("nonExistentAttr")
        .attributes_to_get(HASH_KEY_S)
        .send()
        .await
        .unwrap();

    let resp_item = result.item().unwrap();
    assert_eq!(
        resp_item.get("existingAttr").unwrap().as_s().unwrap(),
        "exists"
    );
    assert!(!resp_item.contains_key("nonExistentAttr"));
}

#[tokio::test]
async fn get_item_with_all_json_types() {
    let c = client();
    let t = tables().await;
    let mut item = create_item(&t.simple_key_string);
    item.insert("booleanTrue".into(), bool_val(true));
    item.insert("booleanFalse".into(), bool_val(false));
    item.insert("nullVal".into(), null_val());
    item.insert("list".into(), list_val(vec![s("a"), n(1)]));
    let mut m = HashMap::new();
    m.insert("k".into(), s("v"));
    item.insert("map".into(), map_val(m));
    c.put_item()
        .table_name(&t.simple_key_string)
        .set_item(Some(item.clone()))
        .send()
        .await
        .unwrap();
    check_item(c, &t.simple_key_string, &item).await;
}

// ========== GET NON-EXISTENT ==========

#[tokio::test]
async fn get_item_key_does_not_exist() {
    let c = client();
    let t = tables().await;
    let mut key = HashMap::new();
    key.insert(
        HASH_KEY_S.into(),
        s(&format!("nonExistentKey_{}", Uuid::new_v4())),
    );
    let result = c
        .get_item()
        .table_name(&t.simple_key_string)
        .set_key(Some(key))
        .consistent_read(true)
        .send()
        .await
        .unwrap();
    assert!(result.item().is_none() || result.item().unwrap().is_empty());
}

#[tokio::test]
async fn get_item_from_non_existing_table() {
    let c = client();
    let mut key = HashMap::new();
    key.insert(HASH_KEY_S.into(), s("test"));
    let err = c
        .get_item()
        .table_name(format!(
            "nonExistentTable_{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis()
        ))
        .set_key(Some(key))
        .consistent_read(true)
        .send()
        .await;
    assert!(err.is_err());
    assert_eq!(
        err_code(&err.unwrap_err()),
        Some("ResourceNotFoundException")
    );
}

// ========== CONSISTENT READ TESTS ==========

#[tokio::test]
async fn get_item_consistent_read_true() {
    let c = client();
    let t = tables().await;
    let mut item = create_item(&t.simple_key_string);
    item.insert("extra".into(), s("val"));
    c.put_item()
        .table_name(&t.simple_key_string)
        .set_item(Some(item.clone()))
        .send()
        .await
        .unwrap();

    let key = get_key(&t.simple_key_string, &item);
    let result = c
        .get_item()
        .table_name(&t.simple_key_string)
        .set_key(Some(key))
        .consistent_read(true)
        .send()
        .await
        .unwrap();
    assert!(result.item().is_some());
    let actual: HashMap<String, AttributeValue> = result
        .item()
        .unwrap()
        .iter()
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect();
    assert_item_eq(&item, &actual);
}

#[tokio::test]
async fn get_item_consistent_read_false() {
    let c = client();
    let t = tables().await;
    let item = create_item(&t.simple_key_string);
    c.put_item()
        .table_name(&t.simple_key_string)
        .set_item(Some(item.clone()))
        .send()
        .await
        .unwrap();

    let key = get_key(&t.simple_key_string, &item);
    let result = c
        .get_item()
        .table_name(&t.simple_key_string)
        .set_key(Some(key))
        .consistent_read(false)
        .send()
        .await
        .unwrap();
    assert!(result.item().is_some());
}

// ========== SPECIAL CHARACTER TESTS ==========

#[tokio::test]
async fn get_item_with_special_characters() {
    let c = client();
    let t = tables().await;
    let mut item = create_item(&t.simple_key_string);
    item.insert("specialCharString1".into(), s("\u{2665} hello!"));
    item.insert("specialCharString2".into(), s("\u{30F0}"));
    item.insert("symbols".into(), s("#@$%^"));
    item.insert("japanese".into(), s("＝このような，そのような"));
    item.insert("cyrillic".into(), s("яшедгвдыуйчйк"));
    item.insert("hindi".into(), s("आत्मसमर्पण"));
    item.insert("@@@@@".into(), s("#####"));
    item.insert("\u{30F0}".into(), s("\u{30F0}"));
    item.insert("doubleQuoteValue".into(), s("\"\""));
    c.put_item()
        .table_name(&t.simple_key_string)
        .set_item(Some(item.clone()))
        .send()
        .await
        .unwrap();
    check_item(c, &t.simple_key_string, &item).await;
}

// ========== SELECTED ATTRIBUTES TESTS ==========

#[tokio::test]
async fn get_selected_attributes() {
    let c = client();
    let t = tables().await;
    let mut item = create_item(&t.simple_key_string);
    item.insert("attr1".into(), s("val1"));
    item.insert("attr2".into(), n(42));
    item.insert("attr3".into(), bool_val(true));
    c.put_item()
        .table_name(&t.simple_key_string)
        .set_item(Some(item.clone()))
        .send()
        .await
        .unwrap();

    let key = get_key(&t.simple_key_string, &item);
    let result = c
        .get_item()
        .table_name(&t.simple_key_string)
        .set_key(Some(key))
        .consistent_read(true)
        .attributes_to_get("attr1")
        .attributes_to_get("attr2")
        .send()
        .await
        .unwrap();

    let resp_item = result.item().unwrap();
    assert_eq!(resp_item.get("attr1").unwrap().as_s().unwrap(), "val1");
    assert_eq!(resp_item.get("attr2").unwrap().as_n().unwrap(), "42");
    assert!(!resp_item.contains_key("attr3"));
}

#[tokio::test]
async fn get_item_attrs_do_not_exist() {
    let c = client();
    let t = tables().await;
    let item = create_item(&t.simple_key_string);
    c.put_item()
        .table_name(&t.simple_key_string)
        .set_item(Some(item.clone()))
        .send()
        .await
        .unwrap();

    let key = get_key(&t.simple_key_string, &item);
    let result = c
        .get_item()
        .table_name(&t.simple_key_string)
        .set_key(Some(key))
        .consistent_read(true)
        .attributes_to_get("nonExistent1")
        .attributes_to_get("nonExistent2")
        .send()
        .await
        .unwrap();

    // Item exists but requested attrs don't — SDK still returns the item (empty or with key only)
    assert!(result.item().is_some());
    assert!(!result.item().unwrap().contains_key("nonExistent1"));
}

// ========== STRING SET AND BLOB SET TESTS ==========

#[tokio::test]
async fn get_string_set_attributes() {
    let c = client();
    let t = tables().await;
    let mut item = create_item(&t.simple_key_string);
    item.insert("ssAttr".into(), ss(&["a", "b", "c"]));
    c.put_item()
        .table_name(&t.simple_key_string)
        .set_item(Some(item.clone()))
        .send()
        .await
        .unwrap();
    check_item(c, &t.simple_key_string, &item).await;
}

#[tokio::test]
async fn get_blob_set_attributes() {
    let c = client();
    let t = tables().await;
    let mut item = create_item(&t.simple_key_string);
    item.insert("bsAttr".into(), bs(&["bin1", "bin2"]));
    c.put_item()
        .table_name(&t.simple_key_string)
        .set_item(Some(item.clone()))
        .send()
        .await
        .unwrap();
    check_item(c, &t.simple_key_string, &item).await;
}

// ========== BATCH GET ITEM TESTS ==========

#[tokio::test]
async fn batch_get_multiple_items() {
    let c = client();
    let t = tables().await;
    let mut keys = Vec::new();
    for i in 0..5 {
        let mut item = create_item(&t.simple_key_string);
        item.insert("idx".into(), n(i));
        c.put_item()
            .table_name(&t.simple_key_string)
            .set_item(Some(item.clone()))
            .send()
            .await
            .unwrap();
        keys.push(get_key(&t.simple_key_string, &item));
    }

    let result = c
        .batch_get_item()
        .request_items(
            &t.simple_key_string,
            KeysAndAttributes::builder()
                .set_keys(Some(keys))
                .build()
                .unwrap(),
        )
        .send()
        .await
        .unwrap();

    let responses = result.responses().unwrap();
    let items = responses.get(&t.simple_key_string).unwrap();
    assert_eq!(items.len(), 5);
}

#[tokio::test]
async fn batch_get_item_from_multiple_tables() {
    let c = client();
    let t = tables().await;
    let item1 = create_item(&t.simple_key_string);
    c.put_item()
        .table_name(&t.simple_key_string)
        .set_item(Some(item1.clone()))
        .send()
        .await
        .unwrap();
    let item2 = create_item(&t.comp_key_string_number);
    c.put_item()
        .table_name(&t.comp_key_string_number)
        .set_item(Some(item2.clone()))
        .send()
        .await
        .unwrap();

    let result = c
        .batch_get_item()
        .request_items(
            &t.simple_key_string,
            KeysAndAttributes::builder()
                .keys(get_key(&t.simple_key_string, &item1))
                .build()
                .unwrap(),
        )
        .request_items(
            &t.comp_key_string_number,
            KeysAndAttributes::builder()
                .keys(get_key(&t.comp_key_string_number, &item2))
                .build()
                .unwrap(),
        )
        .send()
        .await
        .unwrap();

    let responses = result.responses().unwrap();
    assert_eq!(responses.get(&t.simple_key_string).unwrap().len(), 1);
    assert_eq!(responses.get(&t.comp_key_string_number).unwrap().len(), 1);
}

#[tokio::test]
async fn batch_get_item_more_than_100_keys() {
    let c = client();
    let t = tables().await;
    let keys: Vec<HashMap<String, AttributeValue>> = (0..101)
        .map(|i| {
            let mut k = HashMap::new();
            k.insert(HASH_KEY_S.into(), s(&format!("key_{i}")));
            k
        })
        .collect();

    let err = c
        .batch_get_item()
        .request_items(
            &t.simple_key_string,
            KeysAndAttributes::builder()
                .set_keys(Some(keys))
                .build()
                .unwrap(),
        )
        .send()
        .await;
    assert!(err.is_err());
}

// ========== INVALID INPUT TESTS ==========

#[tokio::test]
async fn get_item_with_invalid_table_name() {
    let c = client();
    let mut key = HashMap::new();
    key.insert(HASH_KEY_S.into(), s("test"));
    let err = c
        .get_item()
        .table_name("")
        .set_key(Some(key))
        .consistent_read(true)
        .send()
        .await;
    assert!(err.is_err());
}

// ========== NULL TYPES GET TESTS ==========

#[tokio::test]
async fn get_item_with_null_types() {
    let c = client();
    let t = tables().await;
    let mut item = create_item(&t.simple_key_string);
    item.insert("nullAttr".into(), null_val());
    item.insert("boolAttr".into(), bool_val(false));
    item.insert("emptyMap".into(), map_val(HashMap::new()));
    item.insert("emptyList".into(), list_val(vec![]));
    c.put_item()
        .table_name(&t.simple_key_string)
        .set_item(Some(item.clone()))
        .send()
        .await
        .unwrap();
    check_item(c, &t.simple_key_string, &item).await;
}

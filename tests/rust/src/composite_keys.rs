// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Composite key tests — hash+range key CRUD, query with range conditions, different key types.
//! Mirrors Java `CompositeKeyTests`.

use crate::test_base::*;

// ─── Hash + Range Key CRUD ───────────────────────────────────

#[tokio::test]
async fn put_and_get_with_composite_key() {
    let c = client();
    let t = tables().await;
    let table = &t.comp_key_string_number;
    let mut item = std::collections::HashMap::new();
    item.insert(HASH_KEY_S.into(), s("ck_user1"));
    item.insert(RANGE_KEY_N.into(), n(100));
    item.insert("data".into(), s("hello"));
    c.put_item()
        .table_name(table)
        .set_item(Some(item.clone()))
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
    let got = resp.item().expect("Item should exist");
    assert_eq!(got.get("data").unwrap(), &s("hello"));
}

#[tokio::test]
async fn query_by_hash_key() {
    let c = client();
    let t = tables().await;
    let table = &t.comp_key_string_number;
    let pk = format!("qhk_{}", ts());
    for i in 0..5 {
        let mut item = std::collections::HashMap::new();
        item.insert(HASH_KEY_S.into(), s(&pk));
        item.insert(RANGE_KEY_N.into(), n(i));
        item.insert("idx".into(), n(i));
        c.put_item()
            .table_name(table)
            .set_item(Some(item))
            .send()
            .await
            .unwrap();
    }

    let resp = c
        .query()
        .table_name(table)
        .key_condition_expression("#hk = :hk")
        .expression_attribute_names("#hk", HASH_KEY_S)
        .expression_attribute_values(":hk", s(&pk))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.count(), 5);
}

#[tokio::test]
async fn query_with_range_key_between() {
    let c = client();
    let t = tables().await;
    let table = &t.comp_key_string_number;
    let pk = format!("rqb_{}", ts());
    for i in 0..10 {
        let mut item = std::collections::HashMap::new();
        item.insert(HASH_KEY_S.into(), s(&pk));
        item.insert(RANGE_KEY_N.into(), n(i * 10));
        c.put_item()
            .table_name(table)
            .set_item(Some(item))
            .send()
            .await
            .unwrap();
    }

    let resp = c
        .query()
        .table_name(table)
        .key_condition_expression("#hk = :hk AND #rk BETWEEN :lo AND :hi")
        .expression_attribute_names("#hk", HASH_KEY_S)
        .expression_attribute_names("#rk", RANGE_KEY_N)
        .expression_attribute_values(":hk", s(&pk))
        .expression_attribute_values(":lo", n(20))
        .expression_attribute_values(":hi", n(60))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.count(), 5); // 20, 30, 40, 50, 60
}

#[tokio::test]
async fn query_scan_forward_and_reverse() {
    let c = client();
    let t = tables().await;
    let table = &t.comp_key_string_number;
    let pk = format!("ord_{}", ts());
    for i in 1..=5 {
        let mut item = std::collections::HashMap::new();
        item.insert(HASH_KEY_S.into(), s(&pk));
        item.insert(RANGE_KEY_N.into(), n(i));
        c.put_item()
            .table_name(table)
            .set_item(Some(item))
            .send()
            .await
            .unwrap();
    }

    // Forward (ASC)
    let fwd = c
        .query()
        .table_name(table)
        .key_condition_expression("#hk = :hk")
        .expression_attribute_names("#hk", HASH_KEY_S)
        .expression_attribute_values(":hk", s(&pk))
        .scan_index_forward(true)
        .send()
        .await
        .unwrap();
    assert_eq!(fwd.items()[0].get(RANGE_KEY_N).unwrap(), &n(1));
    assert_eq!(fwd.items()[4].get(RANGE_KEY_N).unwrap(), &n(5));

    // Reverse (DESC)
    let rev = c
        .query()
        .table_name(table)
        .key_condition_expression("#hk = :hk")
        .expression_attribute_names("#hk", HASH_KEY_S)
        .expression_attribute_values(":hk", s(&pk))
        .scan_index_forward(false)
        .send()
        .await
        .unwrap();
    assert_eq!(rev.items()[0].get(RANGE_KEY_N).unwrap(), &n(5));
    assert_eq!(rev.items()[4].get(RANGE_KEY_N).unwrap(), &n(1));
}

#[tokio::test]
async fn query_with_limit() {
    let c = client();
    let t = tables().await;
    let table = &t.comp_key_string_number;
    let pk = format!("qlim_{}", ts());
    for i in 0..10 {
        let mut item = std::collections::HashMap::new();
        item.insert(HASH_KEY_S.into(), s(&pk));
        item.insert(RANGE_KEY_N.into(), n(i));
        c.put_item()
            .table_name(table)
            .set_item(Some(item))
            .send()
            .await
            .unwrap();
    }

    let resp = c
        .query()
        .table_name(table)
        .key_condition_expression("#hk = :hk")
        .expression_attribute_names("#hk", HASH_KEY_S)
        .expression_attribute_values(":hk", s(&pk))
        .limit(3)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.count(), 3);
    assert!(
        resp.last_evaluated_key().is_some(),
        "Should have pagination token"
    );
}

#[tokio::test]
async fn query_pagination() {
    let c = client();
    let t = tables().await;
    let table = &t.comp_key_string_number;
    let pk = format!("qpag_{}", ts());
    for i in 0..10 {
        let mut item = std::collections::HashMap::new();
        item.insert(HASH_KEY_S.into(), s(&pk));
        item.insert(RANGE_KEY_N.into(), n(i));
        c.put_item()
            .table_name(table)
            .set_item(Some(item))
            .send()
            .await
            .unwrap();
    }

    let mut total = 0i32;
    let mut last_key: Option<
        std::collections::HashMap<String, aws_sdk_dynamodb::types::AttributeValue>,
    > = None;
    let mut pages = 0;
    loop {
        let mut req = c
            .query()
            .table_name(table)
            .key_condition_expression("#hk = :hk")
            .expression_attribute_names("#hk", HASH_KEY_S)
            .expression_attribute_values(":hk", s(&pk))
            .limit(4);
        if let Some(ref lk) = last_key {
            req = req.set_exclusive_start_key(Some(lk.clone()));
        }
        let resp = req.send().await.unwrap();
        total += resp.count();
        pages += 1;
        last_key = resp.last_evaluated_key().map(|m| {
            m.iter()
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect::<std::collections::HashMap<_, _>>()
        });
        if last_key.is_none() {
            break;
        }
    }
    assert_eq!(total, 10);
    assert!(
        pages >= 3,
        "Should take at least 3 pages with limit=4 for 10 items"
    );
}

// ─── Different Key Types ─────────────────────────────────────

#[tokio::test]
async fn blob_hash_key() {
    let c = client();
    let t = tables().await;
    let table = &t.comp_key_blob_number;
    let mut item = std::collections::HashMap::new();
    item.insert(HASH_KEY_B.into(), b("blobKey123"));
    item.insert(RANGE_KEY_N.into(), n(1));
    item.insert("data".into(), s("blob_data"));
    c.put_item()
        .table_name(table)
        .set_item(Some(item.clone()))
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
    assert_eq!(resp.item().unwrap().get("data").unwrap(), &s("blob_data"));
}

#[tokio::test]
async fn number_hash_key() {
    let c = client();
    let t = tables().await;
    let table = &t.comp_key_number_string;
    let mut item = std::collections::HashMap::new();
    item.insert(HASH_KEY_N.into(), n(12345));
    item.insert(RANGE_KEY_S.into(), s("range1"));
    item.insert("data".into(), s("num_key_data"));
    c.put_item()
        .table_name(table)
        .set_item(Some(item.clone()))
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
    assert_eq!(
        resp.item().unwrap().get("data").unwrap(),
        &s("num_key_data")
    );
}

// ─── Delete with Composite Key ───────────────────────────────

#[tokio::test]
async fn delete_with_composite_key() {
    let c = client();
    let t = tables().await;
    let table = &t.comp_key_string_number;
    let mut item = std::collections::HashMap::new();
    item.insert(HASH_KEY_S.into(), s(&format!("del_{}", ts())));
    item.insert(RANGE_KEY_N.into(), n(999));
    item.insert("data".into(), s("to_delete"));
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

#[tokio::test]
async fn delete_with_wrong_range_key_does_nothing() {
    let c = client();
    let t = tables().await;
    let table = &t.comp_key_string_number;
    let pk = format!("keep_{}", ts());
    let mut item = std::collections::HashMap::new();
    item.insert(HASH_KEY_S.into(), s(&pk));
    item.insert(RANGE_KEY_N.into(), n(1));
    item.insert("data".into(), s("keep_me"));
    c.put_item()
        .table_name(table)
        .set_item(Some(item.clone()))
        .send()
        .await
        .unwrap();

    // Delete with wrong range key.
    let mut wrong_key = std::collections::HashMap::new();
    wrong_key.insert(HASH_KEY_S.into(), s(&pk));
    wrong_key.insert(RANGE_KEY_N.into(), n(999));
    c.delete_item()
        .table_name(table)
        .set_key(Some(wrong_key))
        .send()
        .await
        .unwrap();

    // Original item should still exist.
    let key = get_key(table, &item);
    let resp = c
        .get_item()
        .table_name(table)
        .set_key(Some(key))
        .consistent_read(true)
        .send()
        .await
        .unwrap();
    assert!(resp.item().is_some(), "Original item should still exist");
}

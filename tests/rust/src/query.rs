// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Query integration tests.

use crate::test_base::*;

/// Put N items into a composite-key table sharing the same hash key.
async fn seed_items(table: &str, hash: &str, count: usize) -> Vec<i64> {
    let c = client();
    let mut range_vals = Vec::new();
    for i in 0..count {
        let rv = (i as i64) + 1;
        range_vals.push(rv);
        let mut item = std::collections::HashMap::new();
        item.insert(HASH_KEY_S.into(), s(hash));
        item.insert(RANGE_KEY_N.into(), n(rv));
        item.insert("data".into(), s(&format!("item_{rv}")));
        c.put_item()
            .table_name(table)
            .set_item(Some(item))
            .send()
            .await
            .unwrap();
    }
    range_vals
}

#[tokio::test]
async fn query_all_items_for_hash_key() {
    let c = client();
    let t = tables().await;
    let table = &t.comp_key_string_number;
    let hash = format!("qh_{}", ts());
    seed_items(table, &hash, 5).await;

    let resp = c
        .query()
        .table_name(table)
        .key_condition_expression("#h = :hv")
        .expression_attribute_names("#h", HASH_KEY_S)
        .expression_attribute_values(":hv", s(&hash))
        .send()
        .await
        .unwrap();

    assert_eq!(resp.count(), 5);
    assert_eq!(resp.items().len(), 5);
}

#[tokio::test]
async fn query_with_range_key_condition() {
    let c = client();
    let t = tables().await;
    let table = &t.comp_key_string_number;
    let hash = format!("qr_{}", ts());
    seed_items(table, &hash, 10).await;

    let resp = c
        .query()
        .table_name(table)
        .key_condition_expression("#h = :hv AND #r BETWEEN :lo AND :hi")
        .expression_attribute_names("#h", HASH_KEY_S)
        .expression_attribute_names("#r", RANGE_KEY_N)
        .expression_attribute_values(":hv", s(&hash))
        .expression_attribute_values(":lo", n(3))
        .expression_attribute_values(":hi", n(7))
        .send()
        .await
        .unwrap();

    assert_eq!(resp.count(), 5);
}

#[tokio::test]
async fn query_with_limit() {
    let c = client();
    let t = tables().await;
    let table = &t.comp_key_string_number;
    let hash = format!("ql_{}", ts());
    seed_items(table, &hash, 5).await;

    let resp = c
        .query()
        .table_name(table)
        .key_condition_expression("#h = :hv")
        .expression_attribute_names("#h", HASH_KEY_S)
        .expression_attribute_values(":hv", s(&hash))
        .limit(2)
        .send()
        .await
        .unwrap();

    assert_eq!(resp.items().len(), 2);
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
    let hash = format!("qp_{}", ts());
    seed_items(table, &hash, 5).await;

    // Page 1
    let resp1 = c
        .query()
        .table_name(table)
        .key_condition_expression("#h = :hv")
        .expression_attribute_names("#h", HASH_KEY_S)
        .expression_attribute_values(":hv", s(&hash))
        .limit(3)
        .send()
        .await
        .unwrap();

    assert_eq!(resp1.items().len(), 3);
    let lek = resp1
        .last_evaluated_key()
        .expect("Should have LEK")
        .to_owned();

    // Page 2
    let resp2 = c
        .query()
        .table_name(table)
        .key_condition_expression("#h = :hv")
        .expression_attribute_names("#h", HASH_KEY_S)
        .expression_attribute_values(":hv", s(&hash))
        .set_exclusive_start_key(Some(lek))
        .send()
        .await
        .unwrap();

    assert_eq!(resp2.items().len(), 2);
    assert!(resp2.last_evaluated_key().is_none(), "No more pages");
}

#[tokio::test]
async fn query_scan_index_forward_false() {
    let c = client();
    let t = tables().await;
    let table = &t.comp_key_string_number;
    let hash = format!("qd_{}", ts());
    seed_items(table, &hash, 5).await;

    let resp = c
        .query()
        .table_name(table)
        .key_condition_expression("#h = :hv")
        .expression_attribute_names("#h", HASH_KEY_S)
        .expression_attribute_values(":hv", s(&hash))
        .scan_index_forward(false)
        .send()
        .await
        .unwrap();

    let items = resp.items();
    assert_eq!(items.len(), 5);
    // Items should be in descending range key order.
    for i in 0..items.len() - 1 {
        let a: i64 = items[i]
            .get(RANGE_KEY_N)
            .unwrap()
            .as_n()
            .unwrap()
            .parse()
            .unwrap();
        let b: i64 = items[i + 1]
            .get(RANGE_KEY_N)
            .unwrap()
            .as_n()
            .unwrap()
            .parse()
            .unwrap();
        assert!(a > b, "Expected descending order: {a} > {b}");
    }
}

#[tokio::test]
async fn query_with_filter_expression() {
    let c = client();
    let t = tables().await;
    let table = &t.comp_key_string_number;
    let hash = format!("qf_{}", ts());

    // Seed items with alternating "even"/"odd" data.
    for i in 1..=6 {
        let mut item = std::collections::HashMap::new();
        item.insert(HASH_KEY_S.into(), s(&hash));
        item.insert(RANGE_KEY_N.into(), n(i));
        item.insert("parity".into(), s(if i % 2 == 0 { "even" } else { "odd" }));
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
        .key_condition_expression("#h = :hv")
        .filter_expression("parity = :p")
        .expression_attribute_names("#h", HASH_KEY_S)
        .expression_attribute_values(":hv", s(&hash))
        .expression_attribute_values(":p", s("even"))
        .send()
        .await
        .unwrap();

    assert_eq!(resp.count(), 3);
}

#[tokio::test]
async fn query_with_projection_expression() {
    let c = client();
    let t = tables().await;
    let table = &t.comp_key_string_number;
    let hash = format!("qproj_{}", ts());
    seed_items(table, &hash, 3).await;

    let resp = c
        .query()
        .table_name(table)
        .key_condition_expression("#h = :hv")
        .expression_attribute_names("#h", HASH_KEY_S)
        .expression_attribute_values(":hv", s(&hash))
        .projection_expression("#h, #r")
        .expression_attribute_names("#r", RANGE_KEY_N)
        .send()
        .await
        .unwrap();

    for item in resp.items() {
        assert!(item.get(HASH_KEY_S).is_some());
        assert!(item.get(RANGE_KEY_N).is_some());
        assert!(
            item.get("data").is_none(),
            "Projection should exclude 'data'"
        );
    }
}

#[tokio::test]
async fn query_non_existent_table() {
    let c = client();
    let err = c
        .query()
        .table_name("NonExistentTable_query")
        .key_condition_expression("#h = :hv")
        .expression_attribute_names("#h", HASH_KEY_S)
        .expression_attribute_values(":hv", s("x"))
        .send()
        .await
        .unwrap_err();

    assert_eq!(err_code(&err), Some("ResourceNotFoundException"));
}

#[tokio::test]
async fn query_empty_result() {
    let c = client();
    let t = tables().await;
    let table = &t.comp_key_string_number;
    let hash = format!("qempty_{}", ts());

    let resp = c
        .query()
        .table_name(table)
        .key_condition_expression("#h = :hv")
        .expression_attribute_names("#h", HASH_KEY_S)
        .expression_attribute_values(":hv", s(&hash))
        .send()
        .await
        .unwrap();

    assert_eq!(resp.count(), 0);
    assert!(resp.items().is_empty());
}

#[tokio::test]
async fn query_begins_with() {
    let c = client();
    let t = tables().await;
    let table = &t.comp_key_string_string_gsi;
    let hash = format!("qbw_{}", ts());

    for prefix in &["alpha_1", "alpha_2", "beta_1"] {
        let mut item = std::collections::HashMap::new();
        item.insert(HASH_KEY_S.into(), s(&hash));
        item.insert(RANGE_KEY_S.into(), s(prefix));
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
        .key_condition_expression("#h = :hv AND begins_with(#r, :prefix)")
        .expression_attribute_names("#h", HASH_KEY_S)
        .expression_attribute_names("#r", RANGE_KEY_S)
        .expression_attribute_values(":hv", s(&hash))
        .expression_attribute_values(":prefix", s("alpha"))
        .send()
        .await
        .unwrap();

    assert_eq!(resp.count(), 2);
}

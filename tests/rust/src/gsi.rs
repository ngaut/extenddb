// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! GSI (Global Secondary Index) query integration tests.

use crate::test_base::*;

#[tokio::test]
async fn query_gsi_basic() {
    let c = client();
    let t = tables().await;
    let table = &t.simple_key_string_gsi;
    let gsi_hash = format!("gsi_qb_{}", ts());

    let mut item = create_item(table);
    item.insert(GSI_HASH_KEY.into(), s(&gsi_hash));
    item.insert(GSI_RANGE_KEY.into(), s("r1"));
    c.put_item()
        .table_name(table)
        .set_item(Some(item.clone()))
        .send()
        .await
        .unwrap();

    tokio::time::sleep(std::time::Duration::from_secs(2)).await;

    let resp = c
        .query()
        .table_name(table)
        .index_name(GSI_NAME)
        .key_condition_expression("#h = :hv")
        .expression_attribute_names("#h", GSI_HASH_KEY)
        .expression_attribute_values(":hv", s(&gsi_hash))
        .send()
        .await
        .unwrap();

    assert_eq!(resp.count(), 1);
}

#[tokio::test]
async fn query_gsi_multiple_items() {
    let c = client();
    let t = tables().await;
    let table = &t.simple_key_string_gsi;
    let gsi_hash = format!("gsi_qm_{}", ts());

    for i in 0..5 {
        let mut item = create_item(table);
        item.insert(GSI_HASH_KEY.into(), s(&gsi_hash));
        item.insert(GSI_RANGE_KEY.into(), s(&format!("r_{i}")));
        c.put_item()
            .table_name(table)
            .set_item(Some(item))
            .send()
            .await
            .unwrap();
    }

    tokio::time::sleep(std::time::Duration::from_secs(2)).await;

    let resp = c
        .query()
        .table_name(table)
        .index_name(GSI_NAME)
        .key_condition_expression("#h = :hv")
        .expression_attribute_names("#h", GSI_HASH_KEY)
        .expression_attribute_values(":hv", s(&gsi_hash))
        .send()
        .await
        .unwrap();

    assert_eq!(resp.count(), 5);
}

#[tokio::test]
async fn query_gsi_with_range_condition() {
    let c = client();
    let t = tables().await;
    let table = &t.simple_key_string_gsi;
    let gsi_hash = format!("gsi_qr_{}", ts());

    for prefix in &["alpha_1", "alpha_2", "beta_1", "beta_2", "gamma_1"] {
        let mut item = create_item(table);
        item.insert(GSI_HASH_KEY.into(), s(&gsi_hash));
        item.insert(GSI_RANGE_KEY.into(), s(prefix));
        c.put_item()
            .table_name(table)
            .set_item(Some(item))
            .send()
            .await
            .unwrap();
    }

    tokio::time::sleep(std::time::Duration::from_secs(2)).await;

    let resp = c
        .query()
        .table_name(table)
        .index_name(GSI_NAME)
        .key_condition_expression("#h = :hv AND begins_with(#r, :prefix)")
        .expression_attribute_names("#h", GSI_HASH_KEY)
        .expression_attribute_names("#r", GSI_RANGE_KEY)
        .expression_attribute_values(":hv", s(&gsi_hash))
        .expression_attribute_values(":prefix", s("alpha"))
        .send()
        .await
        .unwrap();

    assert_eq!(resp.count(), 2);
}

#[tokio::test]
async fn query_gsi_with_filter() {
    let c = client();
    let t = tables().await;
    let table = &t.simple_key_string_gsi;
    let gsi_hash = format!("gsi_qf_{}", ts());

    for i in 0..4 {
        let mut item = create_item(table);
        item.insert(GSI_HASH_KEY.into(), s(&gsi_hash));
        item.insert(GSI_RANGE_KEY.into(), s(&format!("r_{i}")));
        item.insert("parity".into(), s(if i % 2 == 0 { "even" } else { "odd" }));
        c.put_item()
            .table_name(table)
            .set_item(Some(item))
            .send()
            .await
            .unwrap();
    }

    tokio::time::sleep(std::time::Duration::from_secs(2)).await;

    let resp = c
        .query()
        .table_name(table)
        .index_name(GSI_NAME)
        .key_condition_expression("#h = :hv")
        .filter_expression("parity = :p")
        .expression_attribute_names("#h", GSI_HASH_KEY)
        .expression_attribute_values(":hv", s(&gsi_hash))
        .expression_attribute_values(":p", s("even"))
        .send()
        .await
        .unwrap();

    assert_eq!(resp.count(), 2);
}

#[tokio::test]
async fn query_gsi_with_projection() {
    let c = client();
    let t = tables().await;
    let table = &t.simple_key_string_gsi;
    let gsi_hash = format!("gsi_qp_{}", ts());

    let mut item = create_item(table);
    item.insert(GSI_HASH_KEY.into(), s(&gsi_hash));
    item.insert(GSI_RANGE_KEY.into(), s("r1"));
    item.insert("extra".into(), s("val"));
    c.put_item()
        .table_name(table)
        .set_item(Some(item))
        .send()
        .await
        .unwrap();

    tokio::time::sleep(std::time::Duration::from_secs(2)).await;

    let resp = c
        .query()
        .table_name(table)
        .index_name(GSI_NAME)
        .key_condition_expression("#h = :hv")
        .expression_attribute_names("#h", GSI_HASH_KEY)
        .expression_attribute_values(":hv", s(&gsi_hash))
        .projection_expression("#h, extra")
        .send()
        .await
        .unwrap();

    assert_eq!(resp.count(), 1);
    let result = &resp.items()[0];
    assert!(result.contains_key(GSI_HASH_KEY));
    assert!(result.contains_key("extra"));
}

#[tokio::test]
async fn query_gsi_empty_result() {
    let c = client();
    let t = tables().await;
    let table = &t.simple_key_string_gsi;
    let gsi_hash = format!("gsi_empty_{}", ts());

    let resp = c
        .query()
        .table_name(table)
        .index_name(GSI_NAME)
        .key_condition_expression("#h = :hv")
        .expression_attribute_names("#h", GSI_HASH_KEY)
        .expression_attribute_values(":hv", s(&gsi_hash))
        .send()
        .await
        .unwrap();

    assert_eq!(resp.count(), 0);
}

#[tokio::test]
async fn query_gsi_scan_index_forward_false() {
    let c = client();
    let t = tables().await;
    let table = &t.simple_key_string_gsi;
    let gsi_hash = format!("gsi_desc_{}", ts());

    for i in 0..5 {
        let mut item = create_item(table);
        item.insert(GSI_HASH_KEY.into(), s(&gsi_hash));
        item.insert(GSI_RANGE_KEY.into(), s(&format!("r_{i:03}")));
        c.put_item()
            .table_name(table)
            .set_item(Some(item))
            .send()
            .await
            .unwrap();
    }

    tokio::time::sleep(std::time::Duration::from_secs(2)).await;

    let resp = c
        .query()
        .table_name(table)
        .index_name(GSI_NAME)
        .key_condition_expression("#h = :hv")
        .expression_attribute_names("#h", GSI_HASH_KEY)
        .expression_attribute_values(":hv", s(&gsi_hash))
        .scan_index_forward(false)
        .send()
        .await
        .unwrap();

    let items = resp.items();
    assert_eq!(items.len(), 5);
    for i in 0..items.len() - 1 {
        let a = items[i].get(GSI_RANGE_KEY).unwrap().as_s().unwrap();
        let b = items[i + 1].get(GSI_RANGE_KEY).unwrap().as_s().unwrap();
        assert!(a > b, "Expected descending order: {a} > {b}");
    }
}

#[tokio::test]
async fn query_gsi_with_limit_and_pagination() {
    let c = client();
    let t = tables().await;
    let table = &t.simple_key_string_gsi;
    let gsi_hash = format!("gsi_page_{}", ts());

    for i in 0..5 {
        let mut item = create_item(table);
        item.insert(GSI_HASH_KEY.into(), s(&gsi_hash));
        item.insert(GSI_RANGE_KEY.into(), s(&format!("r_{i:03}")));
        c.put_item()
            .table_name(table)
            .set_item(Some(item))
            .send()
            .await
            .unwrap();
    }

    tokio::time::sleep(std::time::Duration::from_secs(2)).await;

    let resp1 = c
        .query()
        .table_name(table)
        .index_name(GSI_NAME)
        .key_condition_expression("#h = :hv")
        .expression_attribute_names("#h", GSI_HASH_KEY)
        .expression_attribute_values(":hv", s(&gsi_hash))
        .limit(3)
        .send()
        .await
        .unwrap();

    assert_eq!(resp1.items().len(), 3);
    let lek = resp1
        .last_evaluated_key()
        .expect("Should have pagination token")
        .to_owned();

    let resp2 = c
        .query()
        .table_name(table)
        .index_name(GSI_NAME)
        .key_condition_expression("#h = :hv")
        .expression_attribute_names("#h", GSI_HASH_KEY)
        .expression_attribute_values(":hv", s(&gsi_hash))
        .set_exclusive_start_key(Some(lek))
        .send()
        .await
        .unwrap();

    assert_eq!(resp2.items().len(), 2);
}

#[tokio::test]
async fn query_non_existent_gsi() {
    let c = client();
    let t = tables().await;
    let table = &t.simple_key_string_gsi;

    let err = c
        .query()
        .table_name(table)
        .index_name("non_existent_index")
        .key_condition_expression("#h = :hv")
        .expression_attribute_names("#h", GSI_HASH_KEY)
        .expression_attribute_values(":hv", s("x"))
        .send()
        .await
        .unwrap_err();

    assert_eq!(err_code(&err), Some("ValidationException"));
}

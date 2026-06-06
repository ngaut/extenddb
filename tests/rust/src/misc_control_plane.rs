// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Miscellaneous control plane tests — mirrors Java `MiscControlPlaneTests`.
//! Only tests not already covered in `table_operations.rs` / `table_operations_more.rs`.

use crate::test_base::*;

#[tokio::test]
async fn describe_endpoints() {
    let c = client();
    let resp = c.describe_endpoints().send().await.unwrap();
    let endpoints = resp.endpoints();
    assert!(!endpoints.is_empty());
    assert!(!endpoints[0].address().is_empty());
    assert!(endpoints[0].cache_period_in_minutes() > 0);
}

#[tokio::test]
async fn describe_limits() {
    let c = client();
    let resp = c.describe_limits().send().await.unwrap();
    assert!(resp.table_max_read_capacity_units().unwrap_or(0) > 0);
    assert!(resp.table_max_write_capacity_units().unwrap_or(0) > 0);
    assert!(resp.account_max_read_capacity_units().unwrap_or(0) > 0);
    assert!(resp.account_max_write_capacity_units().unwrap_or(0) > 0);
}

#[tokio::test]
async fn describe_table_returns_attribute_definitions() {
    let c = client();
    let t = tables().await;
    let resp = c
        .describe_table()
        .table_name(&t.simple_key_string)
        .send()
        .await
        .unwrap();
    let attr_defs = resp.table().unwrap().attribute_definitions();
    assert!(!attr_defs.is_empty());
    let found = attr_defs
        .iter()
        .any(|a| a.attribute_name() == HASH_KEY_S && a.attribute_type().as_str() == "S");
    assert!(found, "Expected attribute definition for {HASH_KEY_S}");
}

#[tokio::test]
async fn describe_table_returns_arn() {
    let c = client();
    let t = tables().await;
    let resp = c
        .describe_table()
        .table_name(&t.simple_key_string)
        .send()
        .await
        .unwrap();
    let arn = resp.table().unwrap().table_arn().unwrap();
    assert!(arn.contains(&t.simple_key_string));
}

// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Capacity accounting tests.

use crate::test_base::*;
use aws_sdk_dynamodb::types::{
    AttributeDefinition, BillingMode, KeySchemaElement, KeyType, ReturnConsumedCapacity,
    ScalarAttributeType,
};

async fn create_on_demand_table(name: &str) {
    let c = client();
    c.create_table()
        .table_name(name)
        .key_schema(
            KeySchemaElement::builder()
                .attribute_name("pk")
                .key_type(KeyType::Hash)
                .build()
                .unwrap(),
        )
        .attribute_definitions(
            AttributeDefinition::builder()
                .attribute_name("pk")
                .attribute_type(ScalarAttributeType::S)
                .build()
                .unwrap(),
        )
        .billing_mode(BillingMode::PayPerRequest)
        .send()
        .await
        .unwrap();
    wait_for_active(c, name).await;
}

#[tokio::test]
async fn get_item_consumed_capacity_total() {
    let c = client();
    let table = format!("CapGet_{}", ts());
    create_on_demand_table(&table).await;

    c.put_item()
        .table_name(&table)
        .item("pk", s("cap_test_1"))
        .item("data", s("some data value"))
        .send()
        .await
        .unwrap();

    let resp = c
        .get_item()
        .table_name(&table)
        .key("pk", s("cap_test_1"))
        .return_consumed_capacity(ReturnConsumedCapacity::Total)
        .send()
        .await
        .unwrap();

    let cap = resp.consumed_capacity().unwrap();
    assert_eq!(cap.table_name().unwrap(), table.as_str());
    assert!(cap.capacity_units().unwrap() > 0.0);

    c.delete_table().table_name(&table).send().await.ok();
}

#[tokio::test]
async fn put_item_consumed_capacity_total() {
    let c = client();
    let table = format!("CapPut_{}", ts());
    create_on_demand_table(&table).await;

    let resp = c
        .put_item()
        .table_name(&table)
        .item("pk", s("cap_test_2"))
        .item("data", s("write data"))
        .return_consumed_capacity(ReturnConsumedCapacity::Total)
        .send()
        .await
        .unwrap();

    let cap = resp.consumed_capacity().unwrap();
    assert_eq!(cap.table_name().unwrap(), table.as_str());
    assert!(cap.capacity_units().unwrap() > 0.0);

    c.delete_table().table_name(&table).send().await.ok();
}

#[tokio::test]
async fn scan_consumed_capacity_total() {
    let c = client();
    let table = format!("CapScan_{}", ts());
    create_on_demand_table(&table).await;

    for i in 0..3 {
        c.put_item()
            .table_name(&table)
            .item("pk", s(&format!("scan_cap_{i}")))
            .send()
            .await
            .unwrap();
    }

    let resp = c
        .scan()
        .table_name(&table)
        .return_consumed_capacity(ReturnConsumedCapacity::Total)
        .send()
        .await
        .unwrap();

    let cap = resp.consumed_capacity().unwrap();
    assert!(cap.capacity_units().unwrap() > 0.0);

    c.delete_table().table_name(&table).send().await.ok();
}

#[tokio::test]
async fn no_consumed_capacity_by_default() {
    let c = client();
    let table = format!("CapNone_{}", ts());
    create_on_demand_table(&table).await;

    c.put_item()
        .table_name(&table)
        .item("pk", s("no_cap_test"))
        .send()
        .await
        .unwrap();

    let resp = c
        .get_item()
        .table_name(&table)
        .key("pk", s("no_cap_test"))
        .send()
        .await
        .unwrap();

    assert!(resp.consumed_capacity().is_none());

    c.delete_table().table_name(&table).send().await.ok();
}

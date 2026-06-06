// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Unicode tests: unicode in keys, values, attribute names, and filter expressions.
//! Mirrors Python `test_data_types.py::TestUnicode` and external Java scenarios.

use crate::test_base::*;
use aws_sdk_dynamodb::types::{
    AttributeDefinition, BillingMode, KeySchemaElement, KeyType, ScalarAttributeType,
};
use std::collections::HashMap;

async fn create_simple_table(c: &aws_sdk_dynamodb::Client) -> String {
    let name = format!("test_unicode_{}", ts());
    c.create_table()
        .table_name(&name)
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
    wait_for_active(c, &name).await;
    name
}

async fn create_composite_table(c: &aws_sdk_dynamodb::Client) -> String {
    let name = format!("test_unicode_comp_{}", ts());
    c.create_table()
        .table_name(&name)
        .key_schema(
            KeySchemaElement::builder()
                .attribute_name("pk")
                .key_type(KeyType::Hash)
                .build()
                .unwrap(),
        )
        .key_schema(
            KeySchemaElement::builder()
                .attribute_name("sk")
                .key_type(KeyType::Range)
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
        .attribute_definitions(
            AttributeDefinition::builder()
                .attribute_name("sk")
                .attribute_type(ScalarAttributeType::S)
                .build()
                .unwrap(),
        )
        .billing_mode(BillingMode::PayPerRequest)
        .send()
        .await
        .unwrap();
    wait_for_active(c, &name).await;
    name
}

#[tokio::test]
async fn unicode_string_values() {
    let c = client();
    let name = create_simple_table(c).await;
    let mut item = HashMap::new();
    item.insert("pk".into(), s("k1"));
    item.insert("val".into(), s("日本語テスト"));
    c.put_item()
        .table_name(&name)
        .set_item(Some(item))
        .send()
        .await
        .unwrap();
    let resp = c
        .get_item()
        .table_name(&name)
        .key("pk", s("k1"))
        .consistent_read(true)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.item().unwrap().get("val").unwrap(), &s("日本語テスト"));
}

#[tokio::test]
async fn emoji() {
    let c = client();
    let name = create_simple_table(c).await;
    let mut item = HashMap::new();
    item.insert("pk".into(), s("k1"));
    item.insert("val".into(), s("🎉🚀💯"));
    c.put_item()
        .table_name(&name)
        .set_item(Some(item))
        .send()
        .await
        .unwrap();
    let resp = c
        .get_item()
        .table_name(&name)
        .key("pk", s("k1"))
        .consistent_read(true)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.item().unwrap().get("val").unwrap(), &s("🎉🚀💯"));
}

#[tokio::test]
async fn unicode_in_attribute_names() {
    let c = client();
    let name = create_simple_table(c).await;
    let mut item = HashMap::new();
    item.insert("pk".into(), s("k1"));
    item.insert("名前".into(), s("太郎"));
    c.put_item()
        .table_name(&name)
        .set_item(Some(item))
        .send()
        .await
        .unwrap();
    let resp = c
        .get_item()
        .table_name(&name)
        .key("pk", s("k1"))
        .consistent_read(true)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.item().unwrap().get("名前").unwrap(), &s("太郎"));
}

#[tokio::test]
async fn special_characters_in_values() {
    let c = client();
    let name = create_simple_table(c).await;
    let val = "He said \"hello\"\nand\\then\ttabs";
    let mut item = HashMap::new();
    item.insert("pk".into(), s("k1"));
    item.insert("val".into(), s(val));
    c.put_item()
        .table_name(&name)
        .set_item(Some(item))
        .send()
        .await
        .unwrap();
    let resp = c
        .get_item()
        .table_name(&name)
        .key("pk", s("k1"))
        .consistent_read(true)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.item().unwrap().get("val").unwrap(), &s(val));
}

#[tokio::test]
async fn single_quote_in_values() {
    let c = client();
    let name = create_simple_table(c).await;
    let mut item = HashMap::new();
    item.insert("pk".into(), s("k1"));
    item.insert("val".into(), s("it's a test"));
    c.put_item()
        .table_name(&name)
        .set_item(Some(item))
        .send()
        .await
        .unwrap();
    let resp = c
        .get_item()
        .table_name(&name)
        .key("pk", s("k1"))
        .consistent_read(true)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.item().unwrap().get("val").unwrap(), &s("it's a test"));
}

#[tokio::test]
async fn long_unicode_string() {
    let c = client();
    let name = create_simple_table(c).await;
    let val = "あ".repeat(1000);
    let mut item = HashMap::new();
    item.insert("pk".into(), s("k1"));
    item.insert("val".into(), s(&val));
    c.put_item()
        .table_name(&name)
        .set_item(Some(item))
        .send()
        .await
        .unwrap();
    let resp = c
        .get_item()
        .table_name(&name)
        .key("pk", s("k1"))
        .consistent_read(true)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.item().unwrap().get("val").unwrap(), &s(&val));
}

#[tokio::test]
async fn unicode_in_hash_key() {
    let c = client();
    let name = create_simple_table(c).await;
    let key_val = "キー_テスト";
    let mut item = HashMap::new();
    item.insert("pk".into(), s(key_val));
    item.insert("val".into(), s("data"));
    c.put_item()
        .table_name(&name)
        .set_item(Some(item))
        .send()
        .await
        .unwrap();
    let resp = c
        .get_item()
        .table_name(&name)
        .key("pk", s(key_val))
        .consistent_read(true)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.item().unwrap().get("val").unwrap(), &s("data"));
}

#[tokio::test]
async fn unicode_in_query_filter() {
    let c = client();
    let name = create_composite_table(c).await;
    let mut item1 = HashMap::new();
    item1.insert("pk".into(), s("p1"));
    item1.insert("sk".into(), s("s1"));
    item1.insert("city".into(), s("東京"));
    c.put_item()
        .table_name(&name)
        .set_item(Some(item1))
        .send()
        .await
        .unwrap();

    let mut item2 = HashMap::new();
    item2.insert("pk".into(), s("p1"));
    item2.insert("sk".into(), s("s2"));
    item2.insert("city".into(), s("大阪"));
    c.put_item()
        .table_name(&name)
        .set_item(Some(item2))
        .send()
        .await
        .unwrap();

    let resp = c
        .query()
        .table_name(&name)
        .key_condition_expression("pk = :pk")
        .filter_expression("city = :city")
        .expression_attribute_values(":pk", s("p1"))
        .expression_attribute_values(":city", s("東京"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.count(), 1);
    assert_eq!(
        resp.items().first().unwrap().get("city").unwrap(),
        &s("東京")
    );
}

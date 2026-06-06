// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Error handling tests: error codes, validation errors, nonexistent resources.
//! Mirrors Python `test_error_handling.py` and external Java error scenarios.

use crate::test_base::*;
use aws_sdk_dynamodb::types::{
    AttributeDefinition, BillingMode, Get, KeySchemaElement, KeyType, Put, ScalarAttributeType,
    TransactGetItem, TransactWriteItem, WriteRequest,
};
use std::collections::HashMap;

async fn create_simple_table(c: &aws_sdk_dynamodb::Client) -> String {
    let name = format!("test_err_{}", ts());
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
    let name = format!("test_err_comp_{}", ts());
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

fn nonexistent() -> String {
    format!("nonexistent_{}", ts())
}

// ========== RESOURCE NOT FOUND ERRORS ==========

#[tokio::test]
async fn create_duplicate_table() {
    let c = client();
    let name = create_simple_table(c).await;
    let err = c
        .create_table()
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
        .await;
    assert!(err.is_err());
    assert_eq!(err_code(&err.unwrap_err()), Some("ResourceInUseException"));
}

#[tokio::test]
async fn delete_nonexistent_table() {
    let c = client();
    let err = c.delete_table().table_name(nonexistent()).send().await;
    assert!(err.is_err());
    assert_eq!(
        err_code(&err.unwrap_err()),
        Some("ResourceNotFoundException")
    );
}

#[tokio::test]
async fn get_item_nonexistent_table() {
    let c = client();
    let err = c
        .get_item()
        .table_name(nonexistent())
        .key("pk", s("k1"))
        .send()
        .await;
    assert!(err.is_err());
    assert_eq!(
        err_code(&err.unwrap_err()),
        Some("ResourceNotFoundException")
    );
}

#[tokio::test]
async fn put_item_nonexistent_table() {
    let c = client();
    let mut item = HashMap::new();
    item.insert("pk".into(), s("k1"));
    let err = c
        .put_item()
        .table_name(nonexistent())
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
async fn delete_item_nonexistent_table() {
    let c = client();
    let err = c
        .delete_item()
        .table_name(nonexistent())
        .key("pk", s("k1"))
        .send()
        .await;
    assert!(err.is_err());
    assert_eq!(
        err_code(&err.unwrap_err()),
        Some("ResourceNotFoundException")
    );
}

#[tokio::test]
async fn scan_nonexistent_table() {
    let c = client();
    let err = c.scan().table_name(nonexistent()).send().await;
    assert!(err.is_err());
    assert_eq!(
        err_code(&err.unwrap_err()),
        Some("ResourceNotFoundException")
    );
}

#[tokio::test]
async fn query_nonexistent_table() {
    let c = client();
    let err = c
        .query()
        .table_name(nonexistent())
        .key_condition_expression("pk = :pk")
        .expression_attribute_values(":pk", s("k1"))
        .send()
        .await;
    assert!(err.is_err());
    assert_eq!(
        err_code(&err.unwrap_err()),
        Some("ResourceNotFoundException")
    );
}

#[tokio::test]
async fn batch_get_nonexistent_table() {
    let c = client();
    let tbl = nonexistent();
    let mut keys = HashMap::new();
    keys.insert("pk".into(), s("k1"));
    let err = c
        .batch_get_item()
        .request_items(
            &tbl,
            aws_sdk_dynamodb::types::KeysAndAttributes::builder()
                .keys(keys)
                .build()
                .unwrap(),
        )
        .send()
        .await;
    assert!(err.is_err());
    assert_eq!(
        err_code(&err.unwrap_err()),
        Some("ResourceNotFoundException")
    );
}

#[tokio::test]
async fn batch_write_nonexistent_table() {
    let c = client();
    let tbl = nonexistent();
    let mut item = HashMap::new();
    item.insert("pk".into(), s("k1"));
    let err = c
        .batch_write_item()
        .request_items(
            &tbl,
            vec![WriteRequest::builder()
                .put_request(
                    aws_sdk_dynamodb::types::PutRequest::builder()
                        .set_item(Some(item))
                        .build()
                        .unwrap(),
                )
                .build()],
        )
        .send()
        .await;
    assert!(err.is_err());
    assert_eq!(
        err_code(&err.unwrap_err()),
        Some("ResourceNotFoundException")
    );
}

#[tokio::test]
async fn transact_get_nonexistent_table() {
    let c = client();
    let tbl = nonexistent();
    let mut key = HashMap::new();
    key.insert("pk".into(), s("k1"));
    let err = c
        .transact_get_items()
        .transact_items(
            TransactGetItem::builder()
                .get(
                    Get::builder()
                        .table_name(&tbl)
                        .set_key(Some(key))
                        .build()
                        .unwrap(),
                )
                .build(),
        )
        .send()
        .await;
    assert!(err.is_err());
    assert_eq!(
        err_code(&err.unwrap_err()),
        Some("ResourceNotFoundException")
    );
}

#[tokio::test]
async fn transact_write_nonexistent_table() {
    let c = client();
    let tbl = nonexistent();
    let mut item = HashMap::new();
    item.insert("pk".into(), s("k1"));
    let err = c
        .transact_write_items()
        .transact_items(
            TransactWriteItem::builder()
                .put(
                    Put::builder()
                        .table_name(&tbl)
                        .set_item(Some(item))
                        .build()
                        .unwrap(),
                )
                .build(),
        )
        .send()
        .await;
    assert!(err.is_err());
    assert_eq!(
        err_code(&err.unwrap_err()),
        Some("ResourceNotFoundException")
    );
}

// ========== VALIDATION ERRORS ==========

#[tokio::test]
async fn put_item_missing_range_key() {
    let c = client();
    let name = create_composite_table(c).await;
    let mut item = HashMap::new();
    item.insert("pk".into(), s("k1"));
    // Missing sk
    let err = c
        .put_item()
        .table_name(&name)
        .set_item(Some(item))
        .send()
        .await;
    assert!(err.is_err());
    assert_eq!(err_code(&err.unwrap_err()), Some("ValidationException"));
}

#[tokio::test]
async fn query_without_key_condition() {
    let c = client();
    let name = create_simple_table(c).await;
    let err = c
        .query()
        .table_name(&name)
        .filter_expression("pk = :pk")
        .expression_attribute_values(":pk", s("k1"))
        .send()
        .await;
    assert!(err.is_err());
    assert_eq!(err_code(&err.unwrap_err()), Some("ValidationException"));
}

#[tokio::test]
async fn describe_nonexistent_table() {
    let c = client();
    let err = c.describe_table().table_name(nonexistent()).send().await;
    assert!(err.is_err());
    assert_eq!(
        err_code(&err.unwrap_err()),
        Some("ResourceNotFoundException")
    );
}

#[tokio::test]
async fn update_item_nonexistent_table() {
    let c = client();
    let err = c
        .update_item()
        .table_name(nonexistent())
        .key("pk", s("k1"))
        .update_expression("SET val = :v")
        .expression_attribute_values(":v", s("x"))
        .send()
        .await;
    assert!(err.is_err());
    assert_eq!(
        err_code(&err.unwrap_err()),
        Some("ResourceNotFoundException")
    );
}

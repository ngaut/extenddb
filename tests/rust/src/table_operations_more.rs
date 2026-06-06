// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Table operations tests — part 2: delete, provisioned throughput, validation.
//! Mirrors remaining tests from `TableOperationsTests.java`.

use crate::test_base::*;
use aws_sdk_dynamodb::types::{
    AttributeDefinition, BillingMode, GlobalSecondaryIndex, KeySchemaElement, KeyType, Projection,
    ProjectionType, ProvisionedThroughput, ScalarAttributeType,
};

// ========== DELETE TABLE TESTS ==========

#[tokio::test]
async fn delete_existing_table() {
    let c = client();
    let name = format!("test_delete_{}", ts());
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

    let result = c.delete_table().table_name(&name).send().await.unwrap();
    assert!(result.table_description().is_some());

    wait_for_deleted(c, &name).await;
    let err = c.describe_table().table_name(&name).send().await;
    assert!(err.is_err());
}

#[tokio::test]
async fn delete_non_existent_table() {
    let c = client();
    let err = c
        .delete_table()
        .table_name(format!("nonExistentTable_{}", ts()))
        .send()
        .await;

    assert!(err.is_err());
    let e = err.unwrap_err();
    assert_eq!(
        err_code(&e),
        Some("ResourceNotFoundException"),
        "Expected ResourceNotFoundException, got: {e:?}"
    );
}

#[tokio::test]
async fn delete_table_twice() {
    let c = client();
    let name = format!("test_delete_twice_{}", ts());
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

    c.delete_table().table_name(&name).send().await.unwrap();
    wait_for_deleted(c, &name).await;

    let err = c.delete_table().table_name(&name).send().await;
    assert!(err.is_err());
}

// ========== PROVISIONED THROUGHPUT ==========

#[tokio::test]
async fn create_table_with_provisioned_throughput() {
    let c = client();
    let name = format!("test_provisioned_{}", ts());
    let result = c
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
        .provisioned_throughput(
            ProvisionedThroughput::builder()
                .read_capacity_units(5)
                .write_capacity_units(5)
                .build()
                .unwrap(),
        )
        .send()
        .await
        .unwrap();

    assert!(result.table_description().is_some());

    wait_for_active(c, &name).await;
    let _ = c.delete_table().table_name(&name).send().await;
}

// ========== VALIDATION TESTS ==========

#[tokio::test]
async fn create_table_missing_key_in_attribute_definitions() {
    let c = client();
    let name = format!("test_missing_attr_{}", ts());
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
        .billing_mode(BillingMode::PayPerRequest)
        .send()
        .await;

    assert!(err.is_err());
}

#[tokio::test]
async fn create_table_extra_attribute_definitions() {
    let c = client();
    let name = format!("test_extra_attr_{}", ts());
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
        .attribute_definitions(
            AttributeDefinition::builder()
                .attribute_name("unused")
                .attribute_type(ScalarAttributeType::S)
                .build()
                .unwrap(),
        )
        .billing_mode(BillingMode::PayPerRequest)
        .send()
        .await;

    assert!(err.is_err());
}

#[tokio::test]
async fn create_table_with_gsi_keys_only_projection() {
    let c = client();
    let name = format!("test_gsi_keys_{}", ts());
    let result = c
        .create_table()
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
        .attribute_definitions(
            AttributeDefinition::builder()
                .attribute_name("gsi_pk")
                .attribute_type(ScalarAttributeType::S)
                .build()
                .unwrap(),
        )
        .global_secondary_indexes(
            GlobalSecondaryIndex::builder()
                .index_name("keys_gsi")
                .key_schema(
                    KeySchemaElement::builder()
                        .attribute_name("gsi_pk")
                        .key_type(KeyType::Hash)
                        .build()
                        .unwrap(),
                )
                .projection(
                    Projection::builder()
                        .projection_type(ProjectionType::KeysOnly)
                        .build(),
                )
                .build()
                .unwrap(),
        )
        .billing_mode(BillingMode::PayPerRequest)
        .send()
        .await
        .unwrap();

    let desc = result.table_description().unwrap();
    let gsis = desc.global_secondary_indexes();
    assert_eq!(
        gsis[0].projection().unwrap().projection_type().unwrap(),
        &ProjectionType::KeysOnly
    );

    wait_for_active(c, &name).await;
    let _ = c.delete_table().table_name(&name).send().await;
}

#[tokio::test]
async fn create_table_gsi_provisioned_throughput_rejected_on_pay_per_request() {
    let c = client();
    let name = format!("test_gsi_pt_ppr_{}", ts());
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
        .attribute_definitions(
            AttributeDefinition::builder()
                .attribute_name("gsi_pk")
                .attribute_type(ScalarAttributeType::S)
                .build()
                .unwrap(),
        )
        .global_secondary_indexes(
            GlobalSecondaryIndex::builder()
                .index_name("my_gsi")
                .key_schema(
                    KeySchemaElement::builder()
                        .attribute_name("gsi_pk")
                        .key_type(KeyType::Hash)
                        .build()
                        .unwrap(),
                )
                .projection(
                    Projection::builder()
                        .projection_type(ProjectionType::All)
                        .build(),
                )
                .provisioned_throughput(
                    ProvisionedThroughput::builder()
                        .read_capacity_units(5)
                        .write_capacity_units(5)
                        .build()
                        .unwrap(),
                )
                .build()
                .unwrap(),
        )
        .billing_mode(BillingMode::PayPerRequest)
        .send()
        .await;

    assert!(err.is_err(), "Should reject GSI ProvisionedThroughput on PayPerRequest table");
    let err = err.unwrap_err();
    let code = err_code(&err);
    assert_eq!(code, Some("ValidationException"));
}

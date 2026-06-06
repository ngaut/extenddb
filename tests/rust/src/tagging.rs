// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Tagging tests: TagResource, UntagResource, ListTagsOfResource.
//! Mirrors Python `test_tagging.py` and external Java tagging scenarios.

use crate::test_base::*;
use aws_sdk_dynamodb::types::{
    AttributeDefinition, BillingMode, KeySchemaElement, KeyType, ScalarAttributeType, Tag,
};
use std::collections::HashMap;

async fn create_tagged_table(c: &aws_sdk_dynamodb::Client) -> (String, String) {
    let name = format!("test_tag_{}", ts());
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
    let desc = c.describe_table().table_name(&name).send().await.unwrap();
    let arn = desc.table().unwrap().table_arn().unwrap().to_string();
    (name, arn)
}

#[tokio::test]
async fn tag_and_list() {
    let c = client();
    let (_name, arn) = create_tagged_table(c).await;
    c.tag_resource()
        .resource_arn(&arn)
        .tags(Tag::builder().key("env").value("test").build().unwrap())
        .tags(
            Tag::builder()
                .key("team")
                .value("platform")
                .build()
                .unwrap(),
        )
        .send()
        .await
        .unwrap();

    let resp = c
        .list_tags_of_resource()
        .resource_arn(&arn)
        .send()
        .await
        .unwrap();
    let tags: HashMap<_, _> = resp
        .tags()
        .iter()
        .map(|t| (t.key().to_string(), t.value().to_string()))
        .collect();
    assert_eq!(tags.get("env").unwrap(), "test");
    assert_eq!(tags.get("team").unwrap(), "platform");
}

#[tokio::test]
async fn tag_overwrite() {
    let c = client();
    let (_name, arn) = create_tagged_table(c).await;
    c.tag_resource()
        .resource_arn(&arn)
        .tags(Tag::builder().key("env").value("dev").build().unwrap())
        .send()
        .await
        .unwrap();
    c.tag_resource()
        .resource_arn(&arn)
        .tags(Tag::builder().key("env").value("prod").build().unwrap())
        .send()
        .await
        .unwrap();

    let resp = c
        .list_tags_of_resource()
        .resource_arn(&arn)
        .send()
        .await
        .unwrap();
    let tags: HashMap<_, _> = resp
        .tags()
        .iter()
        .map(|t| (t.key().to_string(), t.value().to_string()))
        .collect();
    assert_eq!(tags.get("env").unwrap(), "prod");
}

#[tokio::test]
async fn untag() {
    let c = client();
    let (_name, arn) = create_tagged_table(c).await;
    c.tag_resource()
        .resource_arn(&arn)
        .tags(Tag::builder().key("env").value("test").build().unwrap())
        .tags(Tag::builder().key("team").value("x").build().unwrap())
        .send()
        .await
        .unwrap();
    c.untag_resource()
        .resource_arn(&arn)
        .tag_keys("env")
        .send()
        .await
        .unwrap();

    let resp = c
        .list_tags_of_resource()
        .resource_arn(&arn)
        .send()
        .await
        .unwrap();
    let keys: Vec<_> = resp.tags().iter().map(|t| t.key().to_string()).collect();
    assert!(!keys.contains(&"env".to_string()));
    assert!(keys.contains(&"team".to_string()));
}

#[tokio::test]
async fn untag_nonexistent_key() {
    let c = client();
    let (_name, arn) = create_tagged_table(c).await;
    // Should succeed silently
    c.untag_resource()
        .resource_arn(&arn)
        .tag_keys("no-such-key")
        .send()
        .await
        .unwrap();
}

#[tokio::test]
async fn list_tags_empty() {
    let c = client();
    let (_name, arn) = create_tagged_table(c).await;
    let resp = c
        .list_tags_of_resource()
        .resource_arn(&arn)
        .send()
        .await
        .unwrap();
    assert!(resp.tags().is_empty());
}

#[tokio::test]
async fn tag_nonexistent_resource() {
    let c = client();
    let fake_arn = "arn:aws:dynamodb:us-east-1:000000000000:table/nonexistent-xyz";
    let err = c
        .tag_resource()
        .resource_arn(fake_arn)
        .tags(Tag::builder().key("k").value("v").build().unwrap())
        .send()
        .await;
    assert!(err.is_err());
    // extenddb returns ResourceNotFoundException; real DynamoDB returns
    // AccessDeniedException for cross-account ARNs.
    let err = err.unwrap_err();
    let code = err_code(&err);
    assert!(
        code == Some("ResourceNotFoundException") || code == Some("AccessDeniedException"),
        "unexpected error code: {code:?}"
    );
}

#[tokio::test]
async fn list_tags_nonexistent_resource() {
    let c = client();
    let fake_arn = "arn:aws:dynamodb:us-east-1:000000000000:table/nonexistent-xyz";
    let err = c
        .list_tags_of_resource()
        .resource_arn(fake_arn)
        .send()
        .await;
    assert!(err.is_err());
    // extenddb returns ResourceNotFoundException; real DynamoDB returns
    // AccessDeniedException for cross-account ARNs.
    let err = err.unwrap_err();
    let code = err_code(&err);
    assert!(
        code == Some("ResourceNotFoundException") || code == Some("AccessDeniedException"),
        "unexpected error code: {code:?}"
    );
}

#[tokio::test]
async fn tag_multiple_then_untag_all() {
    let c = client();
    let (_name, arn) = create_tagged_table(c).await;
    c.tag_resource()
        .resource_arn(&arn)
        .tags(Tag::builder().key("a").value("1").build().unwrap())
        .tags(Tag::builder().key("b").value("2").build().unwrap())
        .tags(Tag::builder().key("c").value("3").build().unwrap())
        .send()
        .await
        .unwrap();

    c.untag_resource()
        .resource_arn(&arn)
        .tag_keys("a")
        .tag_keys("b")
        .tag_keys("c")
        .send()
        .await
        .unwrap();

    let resp = c
        .list_tags_of_resource()
        .resource_arn(&arn)
        .send()
        .await
        .unwrap();
    assert!(resp.tags().is_empty());
}

#[tokio::test]
async fn untag_nonexistent_resource() {
    let c = client();
    let fake_arn = "arn:aws:dynamodb:us-east-1:000000000000:table/nonexistent-xyz";
    let err = c
        .untag_resource()
        .resource_arn(fake_arn)
        .tag_keys("k")
        .send()
        .await;
    assert!(err.is_err());
    // extenddb returns ResourceNotFoundException; real DynamoDB returns
    // AccessDeniedException for cross-account ARNs.
    let err = err.unwrap_err();
    let code = err_code(&err);
    assert!(
        code == Some("ResourceNotFoundException") || code == Some("AccessDeniedException"),
        "unexpected error code: {code:?}"
    );
}

#[tokio::test]
async fn tag_add_incremental() {
    let c = client();
    let (_name, arn) = create_tagged_table(c).await;
    c.tag_resource()
        .resource_arn(&arn)
        .tags(Tag::builder().key("a").value("1").build().unwrap())
        .send()
        .await
        .unwrap();
    c.tag_resource()
        .resource_arn(&arn)
        .tags(Tag::builder().key("b").value("2").build().unwrap())
        .send()
        .await
        .unwrap();

    let resp = c
        .list_tags_of_resource()
        .resource_arn(&arn)
        .send()
        .await
        .unwrap();
    let tags: HashMap<_, _> = resp
        .tags()
        .iter()
        .map(|t| (t.key().to_string(), t.value().to_string()))
        .collect();
    assert_eq!(tags.len(), 2);
    assert_eq!(tags.get("a").unwrap(), "1");
    assert_eq!(tags.get("b").unwrap(), "2");
}

#[tokio::test]
async fn tag_empty_value() {
    let c = client();
    let (_name, arn) = create_tagged_table(c).await;
    c.tag_resource()
        .resource_arn(&arn)
        .tags(Tag::builder().key("k").value("").build().unwrap())
        .send()
        .await
        .unwrap();

    let resp = c
        .list_tags_of_resource()
        .resource_arn(&arn)
        .send()
        .await
        .unwrap();
    let tags: HashMap<_, _> = resp
        .tags()
        .iter()
        .map(|t| (t.key().to_string(), t.value().to_string()))
        .collect();
    assert_eq!(tags.get("k").unwrap(), "");
}

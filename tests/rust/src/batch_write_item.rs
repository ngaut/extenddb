// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! BatchWriteItem integration tests.

use crate::test_base::*;
use aws_sdk_dynamodb::types::{DeleteRequest, PutRequest, WriteRequest};
use std::collections::HashMap;

#[tokio::test]
async fn batch_write_put_items() {
    let c = client();
    let t = tables().await;
    let table = &t.simple_key_string;

    let items: Vec<_> = (0..5).map(|_| create_item(table)).collect();
    let requests: Vec<WriteRequest> = items
        .iter()
        .map(|item| {
            WriteRequest::builder()
                .put_request(
                    PutRequest::builder()
                        .set_item(Some(item.clone()))
                        .build()
                        .unwrap(),
                )
                .build()
        })
        .collect();

    c.batch_write_item()
        .request_items(table, requests)
        .send()
        .await
        .unwrap();

    // Verify all items exist.
    for item in &items {
        check_item(c, table, item).await;
    }
}

#[tokio::test]
async fn batch_write_delete_items() {
    let c = client();
    let t = tables().await;
    let table = &t.simple_key_string;

    // Put items first.
    let items: Vec<_> = (0..3).map(|_| create_item(table)).collect();
    for item in &items {
        c.put_item()
            .table_name(table)
            .set_item(Some(item.clone()))
            .send()
            .await
            .unwrap();
    }

    // Batch delete them.
    let requests: Vec<WriteRequest> = items
        .iter()
        .map(|item| {
            WriteRequest::builder()
                .delete_request(
                    DeleteRequest::builder()
                        .set_key(Some(get_key(table, item)))
                        .build()
                        .unwrap(),
                )
                .build()
        })
        .collect();

    c.batch_write_item()
        .request_items(table, requests)
        .send()
        .await
        .unwrap();

    // Verify all items are gone.
    for item in &items {
        let key = get_key(table, item);
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
}

#[tokio::test]
async fn batch_write_mixed_put_and_delete() {
    let c = client();
    let t = tables().await;
    let table = &t.simple_key_string;

    // Put items to delete later.
    let to_delete: Vec<_> = (0..2).map(|_| create_item(table)).collect();
    for item in &to_delete {
        c.put_item()
            .table_name(table)
            .set_item(Some(item.clone()))
            .send()
            .await
            .unwrap();
    }

    // New items to put.
    let to_put: Vec<_> = (0..2).map(|_| create_item(table)).collect();

    let mut requests: Vec<WriteRequest> = to_put
        .iter()
        .map(|item| {
            WriteRequest::builder()
                .put_request(
                    PutRequest::builder()
                        .set_item(Some(item.clone()))
                        .build()
                        .unwrap(),
                )
                .build()
        })
        .collect();

    requests.extend(to_delete.iter().map(|item| {
        WriteRequest::builder()
            .delete_request(
                DeleteRequest::builder()
                    .set_key(Some(get_key(table, item)))
                    .build()
                    .unwrap(),
            )
            .build()
    }));

    c.batch_write_item()
        .request_items(table, requests)
        .send()
        .await
        .unwrap();

    // Verify puts exist.
    for item in &to_put {
        check_item(c, table, item).await;
    }
    // Verify deletes are gone.
    for item in &to_delete {
        let key = get_key(table, item);
        let resp = c
            .get_item()
            .table_name(table)
            .set_key(Some(key))
            .consistent_read(true)
            .send()
            .await
            .unwrap();
        assert!(resp.item().is_none(), "Deleted item should be gone");
    }
}

#[tokio::test]
async fn batch_write_to_multiple_tables() {
    let c = client();
    let t = tables().await;
    let table1 = &t.simple_key_string;
    let table2 = &t.comp_key_string_number;

    let item1 = create_item(table1);
    let item2 = create_item(table2);

    let req1 = vec![WriteRequest::builder()
        .put_request(
            PutRequest::builder()
                .set_item(Some(item1.clone()))
                .build()
                .unwrap(),
        )
        .build()];

    let req2 = vec![WriteRequest::builder()
        .put_request(
            PutRequest::builder()
                .set_item(Some(item2.clone()))
                .build()
                .unwrap(),
        )
        .build()];

    c.batch_write_item()
        .request_items(table1, req1)
        .request_items(table2, req2)
        .send()
        .await
        .unwrap();

    check_item(c, table1, &item1).await;
    check_item(c, table2, &item2).await;
}

#[tokio::test]
async fn batch_write_25_items() {
    let c = client();
    let t = tables().await;
    let table = &t.simple_key_string;

    let items: Vec<_> = (0..25).map(|_| create_item(table)).collect();
    let requests: Vec<WriteRequest> = items
        .iter()
        .map(|item| {
            WriteRequest::builder()
                .put_request(
                    PutRequest::builder()
                        .set_item(Some(item.clone()))
                        .build()
                        .unwrap(),
                )
                .build()
        })
        .collect();

    c.batch_write_item()
        .request_items(table, requests)
        .send()
        .await
        .unwrap();

    // Spot-check a few items.
    check_item(c, table, &items[0]).await;
    check_item(c, table, &items[12]).await;
    check_item(c, table, &items[24]).await;
}

#[tokio::test]
async fn batch_write_non_existent_table() {
    let c = client();
    let item: HashMap<String, aws_sdk_dynamodb::types::AttributeValue> =
        [(HASH_KEY_S.into(), s("k"))].into();
    let requests = vec![WriteRequest::builder()
        .put_request(PutRequest::builder().set_item(Some(item)).build().unwrap())
        .build()];

    let err = c
        .batch_write_item()
        .request_items("NonExistentTable_bw", requests)
        .send()
        .await
        .unwrap_err();

    assert_eq!(err_code(&err), Some("ResourceNotFoundException"));
}

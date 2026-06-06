// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Data type tests: all DynamoDB types round-trip correctly.
//! Mirrors Python `test_data_types.py::TestAllDataTypes` and external Java scenarios.

use crate::test_base::*;
use aws_sdk_dynamodb::types::{
    AttributeDefinition, AttributeValue, BillingMode, KeySchemaElement, KeyType,
    ScalarAttributeType,
};
use std::collections::{HashMap, HashSet};

async fn create_simple_table(c: &aws_sdk_dynamodb::Client) -> String {
    let name = format!("test_dtype_{}", ts());
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

async fn put_and_get(
    c: &aws_sdk_dynamodb::Client,
    table: &str,
    key: &str,
    item: HashMap<String, AttributeValue>,
) -> HashMap<String, AttributeValue> {
    c.put_item()
        .table_name(table)
        .set_item(Some(item))
        .send()
        .await
        .unwrap();
    let resp = c
        .get_item()
        .table_name(table)
        .key("pk", s(key))
        .consistent_read(true)
        .send()
        .await
        .unwrap();
    resp.item()
        .unwrap()
        .iter()
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect()
}

#[tokio::test]
async fn string_type() {
    let c = client();
    let name = create_simple_table(c).await;
    let mut item = HashMap::new();
    item.insert("pk".into(), s("k1"));
    item.insert("val".into(), s("hello world"));
    let got = put_and_get(c, &name, "k1", item).await;
    assert_eq!(got.get("val").unwrap(), &s("hello world"));
}

#[tokio::test]
async fn number_type() {
    let c = client();
    let name = create_simple_table(c).await;
    let mut item = HashMap::new();
    item.insert("pk".into(), s("k1"));
    item.insert("val".into(), AttributeValue::N("3.14159".into()));
    let got = put_and_get(c, &name, "k1", item).await;
    assert_eq!(
        got.get("val").unwrap(),
        &AttributeValue::N("3.14159".into())
    );
}

#[tokio::test]
async fn binary_type() {
    let c = client();
    let name = create_simple_table(c).await;
    let data = vec![0xDE, 0xAD, 0xBE, 0xEF];
    let mut item = HashMap::new();
    item.insert("pk".into(), s("k1"));
    item.insert(
        "val".into(),
        AttributeValue::B(aws_smithy_types::Blob::new(data.clone())),
    );
    let got = put_and_get(c, &name, "k1", item).await;
    if let AttributeValue::B(blob) = got.get("val").unwrap() {
        assert_eq!(blob.as_ref(), &data);
    } else {
        panic!("Expected binary type");
    }
}

#[tokio::test]
async fn boolean_type() {
    let c = client();
    let name = create_simple_table(c).await;
    let mut item = HashMap::new();
    item.insert("pk".into(), s("k1"));
    item.insert("t".into(), bool_val(true));
    item.insert("f".into(), bool_val(false));
    let got = put_and_get(c, &name, "k1", item).await;
    assert_eq!(got.get("t").unwrap(), &bool_val(true));
    assert_eq!(got.get("f").unwrap(), &bool_val(false));
}

#[tokio::test]
async fn null_type() {
    let c = client();
    let name = create_simple_table(c).await;
    let mut item = HashMap::new();
    item.insert("pk".into(), s("k1"));
    item.insert("val".into(), null_val());
    let got = put_and_get(c, &name, "k1", item).await;
    assert_eq!(got.get("val").unwrap(), &null_val());
}

#[tokio::test]
async fn string_set_type() {
    let c = client();
    let name = create_simple_table(c).await;
    let mut item = HashMap::new();
    item.insert("pk".into(), s("k1"));
    item.insert("val".into(), ss(&["a", "b", "c"]));
    let got = put_and_get(c, &name, "k1", item).await;
    if let AttributeValue::Ss(vals) = got.get("val").unwrap() {
        let set: HashSet<_> = vals.iter().collect();
        assert!(set.contains(&"a".to_string()));
        assert!(set.contains(&"b".to_string()));
        assert!(set.contains(&"c".to_string()));
    } else {
        panic!("Expected string set type");
    }
}

#[tokio::test]
async fn number_set_type() {
    let c = client();
    let name = create_simple_table(c).await;
    let mut item = HashMap::new();
    item.insert("pk".into(), s("k1"));
    item.insert(
        "val".into(),
        AttributeValue::Ns(vec!["1".into(), "2.5".into(), "3".into()]),
    );
    let got = put_and_get(c, &name, "k1", item).await;
    if let AttributeValue::Ns(vals) = got.get("val").unwrap() {
        let set: HashSet<_> = vals.iter().collect();
        assert!(set.contains(&"1".to_string()));
        assert!(set.contains(&"2.5".to_string()));
        assert!(set.contains(&"3".to_string()));
    } else {
        panic!("Expected number set type");
    }
}

#[tokio::test]
async fn binary_set_type() {
    let c = client();
    let name = create_simple_table(c).await;
    let mut item = HashMap::new();
    item.insert("pk".into(), s("k1"));
    item.insert("val".into(), bs(&["bin1", "bin2"]));
    let got = put_and_get(c, &name, "k1", item).await;
    if let AttributeValue::Bs(vals) = got.get("val").unwrap() {
        assert_eq!(vals.len(), 2);
    } else {
        panic!("Expected binary set type");
    }
}

#[tokio::test]
async fn list_type() {
    let c = client();
    let name = create_simple_table(c).await;
    let mut item = HashMap::new();
    item.insert("pk".into(), s("k1"));
    item.insert("val".into(), list_val(vec![s("a"), n(1)]));
    let got = put_and_get(c, &name, "k1", item).await;
    if let AttributeValue::L(vals) = got.get("val").unwrap() {
        assert_eq!(vals.len(), 2);
        assert_eq!(vals[0], s("a"));
        assert_eq!(vals[1], n(1));
    } else {
        panic!("Expected list type");
    }
}

#[tokio::test]
async fn map_type() {
    let c = client();
    let name = create_simple_table(c).await;
    let mut inner = HashMap::new();
    inner.insert("k".into(), s("v"));
    inner.insert("n".into(), n(9));
    let mut item = HashMap::new();
    item.insert("pk".into(), s("k1"));
    item.insert("val".into(), map_val(inner));
    let got = put_and_get(c, &name, "k1", item).await;
    if let AttributeValue::M(m) = got.get("val").unwrap() {
        assert_eq!(m.get("k").unwrap(), &s("v"));
        assert_eq!(m.get("n").unwrap(), &n(9));
    } else {
        panic!("Expected map type");
    }
}

#[tokio::test]
async fn nested_map_and_list() {
    let c = client();
    let name = create_simple_table(c).await;
    let mut level2 = HashMap::new();
    level2.insert("level2".into(), s("deep"));
    let nested_list = list_val(vec![map_val(level2), list_val(vec![n(42)])]);
    let mut level1 = HashMap::new();
    level1.insert("level1".into(), nested_list);
    let mut item = HashMap::new();
    item.insert("pk".into(), s("k1"));
    item.insert("nested".into(), map_val(level1));
    let got = put_and_get(c, &name, "k1", item).await;
    if let AttributeValue::M(m) = got.get("nested").unwrap() {
        if let AttributeValue::L(l) = m.get("level1").unwrap() {
            if let AttributeValue::M(inner) = &l[0] {
                assert_eq!(inner.get("level2").unwrap(), &s("deep"));
            } else {
                panic!("Expected map at l[0]");
            }
            if let AttributeValue::L(inner) = &l[1] {
                assert_eq!(inner[0], n(42));
            } else {
                panic!("Expected list at l[1]");
            }
        } else {
            panic!("Expected list at level1");
        }
    } else {
        panic!("Expected map at nested");
    }
}

#[tokio::test]
async fn all_types_in_one_item() {
    let c = client();
    let name = create_simple_table(c).await;
    let mut inner_map = HashMap::new();
    inner_map.insert("k".into(), s("v"));
    let mut item = HashMap::new();
    item.insert("pk".into(), s("all-types"));
    item.insert("str_attr".into(), s("hello"));
    item.insert("num_attr".into(), n(42));
    item.insert("bin_attr".into(), b("\x01\x02"));
    item.insert("bool_attr".into(), bool_val(true));
    item.insert("null_attr".into(), null_val());
    item.insert("ss_attr".into(), ss(&["x", "y"]));
    item.insert("ns_attr".into(), ns(&[1, 2]));
    item.insert("bs_attr".into(), bs(&["b1", "b2"]));
    item.insert("list_attr".into(), list_val(vec![s("a"), n(1)]));
    item.insert("map_attr".into(), map_val(inner_map));
    let got = put_and_get(c, &name, "all-types", item).await;
    assert_eq!(got.get("str_attr").unwrap(), &s("hello"));
    assert_eq!(got.get("num_attr").unwrap(), &n(42));
    assert_eq!(got.get("bool_attr").unwrap(), &bool_val(true));
    assert_eq!(got.get("null_attr").unwrap(), &null_val());
}

#[tokio::test]
async fn attribute_type_change() {
    let c = client();
    let name = create_simple_table(c).await;
    let mut item1 = HashMap::new();
    item1.insert("pk".into(), s("k1"));
    item1.insert("val".into(), s("string"));
    c.put_item()
        .table_name(&name)
        .set_item(Some(item1))
        .send()
        .await
        .unwrap();

    let mut item2 = HashMap::new();
    item2.insert("pk".into(), s("k1"));
    item2.insert("val".into(), n(42));
    c.put_item()
        .table_name(&name)
        .set_item(Some(item2))
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
    let got = resp.item().unwrap();
    assert_eq!(got.get("val").unwrap(), &n(42));
}

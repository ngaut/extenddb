// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Native Rust cache-coherence integration tests.
//!
//! These replace the old Python cache-coherence suite with native Rust coverage:
//! each test creates its own account, IAM user, access key, and policies through
//! the management API, then exercises DynamoDB through the Rust AWS SDK.

use crate::test_base::*;
use aws_credential_types::provider::SharedCredentialsProvider;
use aws_credential_types::Credentials;
use aws_sdk_dynamodb::config::Region;
use aws_sdk_dynamodb::types::{
    AttributeDefinition, BillingMode, GlobalSecondaryIndex, KeySchemaElement, KeyType, Projection,
    ProjectionType, ScalarAttributeType,
};
use aws_sdk_dynamodb::Client;
use aws_smithy_http_client::tls;
use aws_smithy_types::error::metadata::ProvideErrorMetadata;
use reqwest::{Response, StatusCode};
use serde_json::{json, Value};
use std::collections::HashSet;
use std::fmt::Debug;
use std::sync::atomic::{AtomicU64, Ordering};

static ACCOUNT_COUNTER: AtomicU64 = AtomicU64::new(0);

#[derive(Clone)]
struct ManagementClient {
    base_url: String,
    admin_user: String,
    admin_password: String,
    http: reqwest::Client,
}

struct TestEnv {
    mgmt: ManagementClient,
    account_id: String,
}

impl ManagementClient {
    fn from_env() -> Option<Self> {
        let endpoint = std::env::var("EXTENDDB_TEST_ENDPOINT").ok()?;
        let admin_user = std::env::var("EXTENDDB_ADMIN_USER").unwrap_or_else(|_| "admin".into());
        let admin_password = std::env::var("EXTENDDB_ADMIN_PASSWORD")
            .expect("EXTENDDB_ADMIN_PASSWORD is required for cache_coherence tests");
        let mut builder = reqwest::Client::builder().danger_accept_invalid_certs(true);
        if let Ok(ca_path) = std::env::var("EXTENDDB_CA_CERT") {
            if let Ok(pem) = std::fs::read(ca_path) {
                if let Ok(cert) = reqwest::Certificate::from_pem(&pem) {
                    builder = builder.add_root_certificate(cert);
                }
            }
        }
        Some(Self {
            base_url: format!("{}/management", endpoint.trim_end_matches('/')),
            admin_user,
            admin_password,
            http: builder.build().expect("management HTTP client"),
        })
    }

    async fn create_account(&self, account_id: &str) {
        let resp = self
            .http
            .post(format!("{}/accounts", self.base_url))
            .basic_auth(&self.admin_user, Some(&self.admin_password))
            .json(&json!({
                "account_id": account_id,
                "account_name": format!("rust-cache-{account_id}"),
            }))
            .send()
            .await
            .expect("create account request");
        expect_status(resp, &[StatusCode::CREATED]).await;
    }

    async fn delete_account(&self, account_id: &str) {
        let _ = self
            .http
            .delete(format!("{}/accounts/{account_id}", self.base_url))
            .basic_auth(&self.admin_user, Some(&self.admin_password))
            .send()
            .await;
    }

    async fn create_user(&self, account_id: &str, user_name: &str, password: Option<&str>) {
        let mut body = json!({ "user_name": user_name });
        if let Some(password) = password {
            body["password"] = json!(password);
        }
        let resp = self
            .http
            .post(format!("{}/accounts/{account_id}/users", self.base_url))
            .basic_auth(&self.admin_user, Some(&self.admin_password))
            .json(&body)
            .send()
            .await
            .expect("create user request");
        expect_status(resp, &[StatusCode::CREATED]).await;
    }

    async fn delete_user(&self, account_id: &str, user_name: &str) {
        let _ = self
            .http
            .delete(format!(
                "{}/accounts/{account_id}/users/{user_name}",
                self.base_url
            ))
            .basic_auth(&self.admin_user, Some(&self.admin_password))
            .send()
            .await;
    }

    async fn create_access_key(&self, account_id: &str, user_name: &str) -> (String, String) {
        let resp = self
            .http
            .post(format!(
                "{}/accounts/{account_id}/users/{user_name}/access-keys",
                self.base_url
            ))
            .basic_auth(&self.admin_user, Some(&self.admin_password))
            .send()
            .await
            .expect("create access key request");
        let body = expect_status(resp, &[StatusCode::CREATED])
            .await
            .json::<Value>()
            .await
            .expect("access key JSON");
        (
            body["access_key_id"]
                .as_str()
                .expect("access_key_id")
                .to_owned(),
            body["secret_access_key"]
                .as_str()
                .expect("secret_access_key")
                .to_owned(),
        )
    }

    async fn delete_access_key(&self, account_id: &str, user_name: &str, access_key_id: &str) {
        let resp = self
            .http
            .delete(format!(
                "{}/accounts/{account_id}/users/{user_name}/access-keys/{access_key_id}",
                self.base_url
            ))
            .basic_auth(&self.admin_user, Some(&self.admin_password))
            .send()
            .await
            .expect("delete access key request");
        expect_status(resp, &[StatusCode::OK, StatusCode::NO_CONTENT]).await;
    }

    async fn put_user_policy(&self, account_id: &str, user_name: &str, action: &str, deny: bool) {
        let effect = if deny { "Deny" } else { "Allow" };
        let policy = json!({
            "Version": "2012-10-17",
            "Statement": [{
                "Effect": effect,
                "Action": action,
                "Resource": "*",
            }],
        });
        let resp = self
            .http
            .put(format!(
                "{}/accounts/{account_id}/users/{user_name}/policy/test-policy",
                self.base_url
            ))
            .basic_auth(&self.admin_user, Some(&self.admin_password))
            .json(&policy)
            .send()
            .await
            .expect("put user policy request");
        expect_status(
            resp,
            &[StatusCode::OK, StatusCode::CREATED, StatusCode::NO_CONTENT],
        )
        .await;
    }

    async fn invalidate_cache(&self, scope: &str, selectors: Value) -> Response {
        self.invalidate_cache_with_auth(
            scope,
            selectors,
            Some((&self.admin_user, &self.admin_password)),
        )
        .await
    }

    async fn invalidate_cache_with_auth(
        &self,
        scope: &str,
        selectors: Value,
        auth: Option<(&str, &str)>,
    ) -> Response {
        let request = self
            .http
            .post(format!("{}/cache/invalidate", self.base_url))
            .json(&json!({ "scope": scope, "selectors": selectors }));
        let request = if let Some((user, password)) = auth {
            request.basic_auth(user, Some(password))
        } else {
            request
        };
        request.send().await.expect("cache invalidation request")
    }

    async fn auth_cache_metrics(&self) -> Value {
        let resp = self
            .http
            .get(format!("{}/auth-cache-metrics", self.base_url))
            .basic_auth(&self.admin_user, Some(&self.admin_password))
            .send()
            .await
            .expect("auth cache metrics request");
        expect_status(resp, &[StatusCode::OK])
            .await
            .json::<Value>()
            .await
            .expect("auth cache metrics JSON")
    }
}

impl TestEnv {
    async fn new() -> Option<Self> {
        if is_real_dynamodb() {
            return None;
        }
        let mgmt = ManagementClient::from_env()?;
        let account_id = unique_account_id();
        mgmt.create_account(&account_id).await;
        Some(Self { mgmt, account_id })
    }

    async fn user(&self) -> String {
        let user_name = unique_name("cache-user");
        self.mgmt
            .create_user(&self.account_id, &user_name, None)
            .await;
        user_name
    }

    async fn cleanup(self) {
        self.mgmt.delete_account(&self.account_id).await;
    }
}

async fn expect_status(resp: Response, expected: &[StatusCode]) -> Response {
    let status = resp.status();
    if !expected.contains(&status) {
        let body = resp.text().await.unwrap_or_default();
        panic!("expected status {expected:?}, got {status}: {body}");
    }
    resp
}

fn unique_account_id() -> String {
    let millis = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system clock before epoch")
        .as_millis() as u64;
    let counter = ACCOUNT_COUNTER.fetch_add(1, Ordering::SeqCst);
    format!("{:012}", (millis.wrapping_add(counter)) % 1_000_000_000_000)
}

fn unique_name(prefix: &str) -> String {
    format!("{prefix}-{}", uuid::Uuid::new_v4().simple())
}

fn dynamodb_client(access_key: &str, secret_key: &str) -> Client {
    let endpoint =
        std::env::var("EXTENDDB_TEST_ENDPOINT").expect("EXTENDDB_TEST_ENDPOINT is required");
    let region = std::env::var("AWS_DEFAULT_REGION").unwrap_or_else(|_| "us-east-1".into());
    let creds = Credentials::new(access_key, secret_key, None, None, "cache-coherence");

    let mut trust_store = tls::TrustStore::empty().with_native_roots(true);
    if let Ok(ca_path) = std::env::var("EXTENDDB_CA_CERT") {
        if let Ok(pem) = std::fs::read(ca_path) {
            trust_store = trust_store.with_pem_certificate(pem);
        }
    }
    let tls_context = tls::TlsContext::builder()
        .with_trust_store(trust_store)
        .build()
        .expect("TLS context build failed");
    let http_client = aws_smithy_http_client::Builder::new()
        .tls_provider(tls::Provider::Rustls(
            tls::rustls_provider::CryptoMode::Ring,
        ))
        .tls_context(tls_context)
        .build_https();

    let config = aws_sdk_dynamodb::Config::builder()
        .behavior_version_latest()
        .region(Region::new(region))
        .credentials_provider(SharedCredentialsProvider::new(creds))
        .http_client(http_client)
        .endpoint_url(endpoint)
        .build();
    Client::from_conf(config)
}

fn metric_counter(metrics: &Value, path: &[&str], counter: &str) -> u64 {
    let mut node = metrics;
    for key in path {
        node = &node[*key];
    }
    node[counter]
        .as_u64()
        .unwrap_or_else(|| panic!("missing metric {path:?}.{counter}: {metrics}"))
}

fn invalidated_set(body: &Value) -> HashSet<String> {
    body["invalidated"]
        .as_array()
        .expect("invalidated array")
        .iter()
        .map(|v| v.as_str().expect("invalidated string").to_owned())
        .collect()
}

fn assert_error_contains<E, R>(
    err: &aws_smithy_runtime_api::client::result::SdkError<E, R>,
    needles: &[&str],
) where
    E: ProvideErrorMetadata + Debug,
    R: Debug,
{
    let code = err_code(err).unwrap_or_default();
    let debug = format!("{err:?}");
    assert!(
        needles
            .iter()
            .any(|needle| code.contains(needle) || debug.contains(needle)),
        "expected one of {needles:?}, code={code:?}, err={debug}"
    );
}

#[tokio::test]
async fn put_user_policy_takes_effect_immediately() {
    let Some(env) = TestEnv::new().await else {
        return;
    };
    let user = env.user().await;
    let (access_key, secret_key) = env.mgmt.create_access_key(&env.account_id, &user).await;
    let ddb = dynamodb_client(&access_key, &secret_key);

    let err = ddb.list_tables().send().await.unwrap_err();
    assert_error_contains(&err, &["AccessDenied"]);

    env.mgmt
        .put_user_policy(&env.account_id, &user, "dynamodb:ListTables", false)
        .await;
    ddb.list_tables().send().await.unwrap();
    env.cleanup().await;
}

#[tokio::test]
async fn put_deny_policy_takes_effect_immediately() {
    let Some(env) = TestEnv::new().await else {
        return;
    };
    let user = env.user().await;
    let (access_key, secret_key) = env.mgmt.create_access_key(&env.account_id, &user).await;
    let ddb = dynamodb_client(&access_key, &secret_key);

    env.mgmt
        .put_user_policy(&env.account_id, &user, "dynamodb:ListTables", false)
        .await;
    ddb.list_tables().send().await.unwrap();

    env.mgmt
        .put_user_policy(&env.account_id, &user, "dynamodb:ListTables", true)
        .await;
    let err = ddb.list_tables().send().await.unwrap_err();
    assert_error_contains(&err, &["AccessDenied"]);
    env.cleanup().await;
}

#[tokio::test]
async fn delete_access_key_takes_effect_immediately() {
    let Some(env) = TestEnv::new().await else {
        return;
    };
    let user = env.user().await;
    let (access_key, secret_key) = env.mgmt.create_access_key(&env.account_id, &user).await;
    let ddb = dynamodb_client(&access_key, &secret_key);
    env.mgmt
        .put_user_policy(&env.account_id, &user, "dynamodb:ListTables", false)
        .await;
    ddb.list_tables().send().await.unwrap();

    env.mgmt
        .delete_access_key(&env.account_id, &user, &access_key)
        .await;
    let err = ddb.list_tables().send().await.unwrap_err();
    assert_error_contains(
        &err,
        &[
            "UnrecognizedClientException",
            "InvalidSignatureException",
            "security token",
        ],
    );
    env.cleanup().await;
}

#[tokio::test]
async fn delete_user_drops_all_cached_state_for_user() {
    let Some(env) = TestEnv::new().await else {
        return;
    };
    let user = env.user().await;
    let (access_key, secret_key) = env.mgmt.create_access_key(&env.account_id, &user).await;
    let ddb = dynamodb_client(&access_key, &secret_key);
    env.mgmt
        .put_user_policy(&env.account_id, &user, "dynamodb:ListTables", false)
        .await;
    ddb.list_tables().send().await.unwrap();

    env.mgmt.delete_user(&env.account_id, &user).await;
    let err = ddb.list_tables().send().await.unwrap_err();
    assert_error_contains(
        &err,
        &[
            "UnrecognizedClientException",
            "InvalidSignatureException",
            "AccessDenied",
        ],
    );
    env.cleanup().await;
}

#[tokio::test]
async fn create_table_visible_to_authorized_caller_immediately() {
    let Some(env) = TestEnv::new().await else {
        return;
    };
    let user = env.user().await;
    let (access_key, secret_key) = env.mgmt.create_access_key(&env.account_id, &user).await;
    let ddb = dynamodb_client(&access_key, &secret_key);
    env.mgmt
        .put_user_policy(&env.account_id, &user, "dynamodb:*", false)
        .await;

    let table = unique_name("cache-test");
    let err = ddb
        .describe_table()
        .table_name(&table)
        .send()
        .await
        .unwrap_err();
    assert_error_contains(&err, &["ResourceNotFound"]);

    ddb.create_table()
        .table_name(&table)
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
    wait_for_active(&ddb, &table).await;
    assert_eq!(
        ddb.describe_table()
            .table_name(&table)
            .send()
            .await
            .unwrap()
            .table()
            .unwrap()
            .table_name()
            .unwrap(),
        table
    );
    ddb.delete_table().table_name(&table).send().await.unwrap();
    wait_for_deleted(&ddb, &table).await;
    env.cleanup().await;
}

#[tokio::test]
async fn tidb_table_metadata_serves_writes_and_reads() {
    let Some(env) = TestEnv::new().await else {
        return;
    };
    let user = env.user().await;
    let (access_key, secret_key) = env.mgmt.create_access_key(&env.account_id, &user).await;
    let ddb = dynamodb_client(&access_key, &secret_key);
    env.mgmt
        .put_user_policy(&env.account_id, &user, "dynamodb:*", false)
        .await;

    let table = unique_name("cache-meta");
    ddb.create_table()
        .table_name(&table)
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
                .attribute_name("gpk")
                .attribute_type(ScalarAttributeType::S)
                .build()
                .unwrap(),
        )
        .attribute_definitions(
            AttributeDefinition::builder()
                .attribute_name("gsk")
                .attribute_type(ScalarAttributeType::S)
                .build()
                .unwrap(),
        )
        .global_secondary_indexes(
            GlobalSecondaryIndex::builder()
                .index_name("gsi1")
                .key_schema(
                    KeySchemaElement::builder()
                        .attribute_name("gpk")
                        .key_type(KeyType::Hash)
                        .build()
                        .unwrap(),
                )
                .key_schema(
                    KeySchemaElement::builder()
                        .attribute_name("gsk")
                        .key_type(KeyType::Range)
                        .build()
                        .unwrap(),
                )
                .projection(
                    Projection::builder()
                        .projection_type(ProjectionType::All)
                        .build(),
                )
                .build()
                .unwrap(),
        )
        .billing_mode(BillingMode::PayPerRequest)
        .send()
        .await
        .unwrap();
    wait_for_active(&ddb, &table).await;

    for sk in ["001", "002"] {
        ddb.put_item()
            .table_name(&table)
            .item("pk", s("tenant-a"))
            .item("sk", s(sk))
            .item("gpk", s("by-kind"))
            .item("gsk", s(sk))
            .send()
            .await
            .unwrap();
    }

    let query = ddb
        .query()
        .table_name(&table)
        .key_condition_expression("pk = :pk")
        .expression_attribute_values(":pk", s("tenant-a"))
        .send()
        .await
        .unwrap();
    assert_eq!(query.count(), 2);

    let scan = ddb.scan().table_name(&table).send().await.unwrap();
    assert_eq!(scan.count(), 2);

    let gsi_query = ddb
        .query()
        .table_name(&table)
        .index_name("gsi1")
        .key_condition_expression("gpk = :gpk")
        .expression_attribute_values(":gpk", s("by-kind"))
        .send()
        .await
        .unwrap();
    assert_eq!(gsi_query.count(), 2);

    let gsi_scan = ddb
        .scan()
        .table_name(&table)
        .index_name("gsi1")
        .send()
        .await
        .unwrap();
    assert_eq!(gsi_scan.count(), 2);

    ddb.delete_table().table_name(&table).send().await.unwrap();
    wait_for_deleted(&ddb, &table).await;
    env.cleanup().await;
}

#[tokio::test]
async fn manual_invalidate_user_forces_refetch() {
    let Some(env) = TestEnv::new().await else {
        return;
    };
    let user = env.user().await;
    let (access_key, secret_key) = env.mgmt.create_access_key(&env.account_id, &user).await;
    let ddb = dynamodb_client(&access_key, &secret_key);
    env.mgmt
        .put_user_policy(&env.account_id, &user, "dynamodb:ListTables", false)
        .await;
    ddb.list_tables().send().await.unwrap();
    ddb.list_tables().send().await.unwrap();

    let metrics_before = env.mgmt.auth_cache_metrics().await;
    let invalidations_before = metric_counter(
        &metrics_before,
        &["authz", "user_policies"],
        "invalidations",
    );
    let misses_before = metric_counter(&metrics_before, &["authz", "user_policies"], "misses");

    let resp = env
        .mgmt
        .invalidate_cache(
            "user",
            json!({ "account_id": env.account_id, "user_name": user }),
        )
        .await;
    let body = expect_status(resp, &[StatusCode::OK])
        .await
        .json::<Value>()
        .await
        .unwrap();
    assert_eq!(body["scope"], "user");
    let invalidated = invalidated_set(&body);
    for expected in [
        "user_policies",
        "user_group_policies",
        "user_boundary",
        "user_tags",
        "principal_credentials",
    ] {
        assert!(
            invalidated.contains(expected),
            "missing {expected} in {invalidated:?}"
        );
    }

    let invalidations_after = metric_counter(
        &env.mgmt.auth_cache_metrics().await,
        &["authz", "user_policies"],
        "invalidations",
    );
    assert!(invalidations_after > invalidations_before);

    ddb.list_tables().send().await.unwrap();
    let misses_after = metric_counter(
        &env.mgmt.auth_cache_metrics().await,
        &["authz", "user_policies"],
        "misses",
    );
    assert!(misses_after > misses_before);
    env.cleanup().await;
}

#[tokio::test]
async fn manual_invalidate_account_does_not_break_subsequent_traffic() {
    let Some(env) = TestEnv::new().await else {
        return;
    };
    let user = env.user().await;
    let (access_key, secret_key) = env.mgmt.create_access_key(&env.account_id, &user).await;
    let ddb = dynamodb_client(&access_key, &secret_key);
    env.mgmt
        .put_user_policy(&env.account_id, &user, "dynamodb:ListTables", false)
        .await;
    ddb.list_tables().send().await.unwrap();

    let resp = env
        .mgmt
        .invalidate_cache("account", json!({ "account_id": env.account_id }))
        .await;
    expect_status(resp, &[StatusCode::OK]).await;
    ddb.list_tables().send().await.unwrap();
    env.cleanup().await;
}

#[tokio::test]
async fn manual_invalidate_all_requires_confirmation() {
    let Some(env) = TestEnv::new().await else {
        return;
    };
    let resp = env.mgmt.invalidate_cache("all", json!({})).await;
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    assert!(resp
        .text()
        .await
        .unwrap()
        .to_lowercase()
        .contains("confirm"));
    env.cleanup().await;
}

#[tokio::test]
async fn manual_invalidate_all_with_confirmation() {
    let Some(env) = TestEnv::new().await else {
        return;
    };
    let resp = env
        .mgmt
        .invalidate_cache("all", json!({ "confirm": true }))
        .await;
    let body = expect_status(resp, &[StatusCode::OK])
        .await
        .json::<Value>()
        .await
        .unwrap();
    assert_eq!(body["scope"], "all");
    assert_eq!(
        invalidated_set(&body),
        ["authz".to_owned(), "credentials".to_owned()]
            .into_iter()
            .collect::<HashSet<_>>()
    );
    env.cleanup().await;
}

#[tokio::test]
async fn manual_invalidate_missing_selector_returns_400() {
    let Some(env) = TestEnv::new().await else {
        return;
    };
    let resp = env
        .mgmt
        .invalidate_cache("user", json!({ "account_id": env.account_id }))
        .await;
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    assert!(resp.text().await.unwrap().contains("user_name"));
    env.cleanup().await;
}

#[tokio::test]
async fn manual_invalidate_requires_admin_auth() {
    let Some(env) = TestEnv::new().await else {
        return;
    };
    let iam_user = unique_name("cache-iam");
    let password = uuid::Uuid::new_v4().simple().to_string();
    env.mgmt
        .create_user(&env.account_id, &iam_user, Some(&password))
        .await;

    let user = format!("{}/{}", env.account_id, iam_user);
    let resp = env
        .mgmt
        .invalidate_cache_with_auth("all", json!({ "confirm": true }), Some((&user, &password)))
        .await;
    assert_eq!(resp.status(), StatusCode::FORBIDDEN);
    env.mgmt.delete_user(&env.account_id, &iam_user).await;

    let resp = env
        .mgmt
        .invalidate_cache_with_auth("all", json!({ "confirm": true }), None)
        .await;
    assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
    env.cleanup().await;
}

#[tokio::test]
async fn delete_table_visibility_comes_from_tidb_catalog() {
    let Some(env) = TestEnv::new().await else {
        return;
    };
    let user = env.user().await;
    let (access_key, secret_key) = env.mgmt.create_access_key(&env.account_id, &user).await;
    let ddb = dynamodb_client(&access_key, &secret_key);
    env.mgmt
        .put_user_policy(&env.account_id, &user, "dynamodb:*", false)
        .await;

    let table = unique_name("cache-test");
    ddb.create_table()
        .table_name(&table)
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
    wait_for_active(&ddb, &table).await;
    ddb.describe_table()
        .table_name(&table)
        .send()
        .await
        .unwrap();

    ddb.delete_table().table_name(&table).send().await.unwrap();
    wait_for_deleted(&ddb, &table).await;

    let err = ddb
        .describe_table()
        .table_name(&table)
        .send()
        .await
        .unwrap_err();
    assert_error_contains(&err, &["ResourceNotFound"]);
    env.cleanup().await;
}

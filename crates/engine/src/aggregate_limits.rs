// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Aggregate request/response size limit helpers.

use extenddb_core::error::DynamoDbError;
use extenddb_core::limits::LimitsConfig;
use serde_json::Value;

pub(crate) fn validate_batch_write_request_size(
    body: &Value,
    limits: &LimitsConfig,
) -> Result<(), DynamoDbError> {
    validate_json_body_size(
        body,
        limits.max_batch_write_request_bytes,
        "BatchWriteItem request size",
    )
}

pub(crate) fn validate_transaction_request_size(
    body: &Value,
    limits: &LimitsConfig,
) -> Result<(), DynamoDbError> {
    validate_json_body_size(
        body,
        limits.max_transaction_request_bytes,
        "Transaction request size",
    )
}

pub(crate) fn validate_transaction_payload_size(
    size: usize,
    limits: &LimitsConfig,
) -> Result<(), DynamoDbError> {
    validate_size(
        size,
        limits.max_transaction_request_bytes,
        "Transaction item size",
    )
}

fn validate_json_body_size(
    body: &Value,
    max_bytes: usize,
    label: &str,
) -> Result<(), DynamoDbError> {
    let size = serde_json::to_vec(body)
        .map_err(|e| DynamoDbError::InternalServerError(e.to_string()))?
        .len();
    validate_size(size, max_bytes, label)
}

fn validate_size(size: usize, max_bytes: usize, label: &str) -> Result<(), DynamoDbError> {
    if size > max_bytes {
        return Err(DynamoDbError::ValidationException(format!(
            "{label} has exceeded the {} limit",
            display_size(max_bytes)
        )));
    }
    Ok(())
}

fn display_size(bytes: usize) -> String {
    if bytes % (1024 * 1024) == 0 {
        format!("{} MB", bytes / (1024 * 1024))
    } else if bytes % 1024 == 0 {
        format!("{} KB", bytes / 1024)
    } else {
        format!("{bytes} bytes")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn batch_write_request_size_limit_is_enforced() {
        let limits = LimitsConfig {
            max_batch_write_request_bytes: 10,
            ..Default::default()
        };
        let err = validate_batch_write_request_size(
            &serde_json::json!({"RequestItems": {"t": [{"DeleteRequest": {"Key": {"pk": {"S": "1"}}}}]}}),
            &limits,
        )
        .unwrap_err();

        assert!(
            err.to_string().contains("BatchWriteItem request size"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn transaction_request_size_limit_is_enforced() {
        let limits = LimitsConfig {
            max_transaction_request_bytes: 10,
            ..Default::default()
        };
        let err = validate_transaction_request_size(
            &serde_json::json!({"TransactItems": [{"Get": {"TableName": "t", "Key": {"pk": {"S": "1"}}}}]}),
            &limits,
        )
        .unwrap_err();

        assert!(
            err.to_string().contains("Transaction request size"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn transaction_payload_size_limit_is_enforced() {
        let limits = LimitsConfig {
            max_transaction_request_bytes: 4,
            ..Default::default()
        };
        let err = validate_transaction_payload_size(5, &limits).unwrap_err();

        assert!(
            err.to_string().contains("Transaction item size"),
            "unexpected error: {err}"
        );
    }
}

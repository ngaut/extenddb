// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Shared TiDB connection and error helpers.

use std::future::Future;
use std::time::Duration;

use extenddb_storage::error::StorageError;
use extenddb_storage::management_store::OpError;
use sqlx::mysql::MySqlPoolOptions;

const MAX_TRANSACTION_RETRIES: usize = 3;

/// Build TiDB pool options with the session state required by this backend.
///
/// ExtendDB uses `SELECT ... FOR UPDATE` for conditional writes, control-plane
/// ownership, and shard sequence allocation. Pinning every checked-out TiDB
/// session to pessimistic transactions makes that behavior independent of the
/// cluster default and consistent across multiple ExtendDB frontends.
pub(crate) fn tidb_pool_options() -> MySqlPoolOptions {
    MySqlPoolOptions::new().after_connect(|conn, _meta| {
        Box::pin(async move {
            sqlx::query("SET SESSION tidb_txn_mode = 'pessimistic'")
                .execute(&mut *conn)
                .await?;
            sqlx::query("SET SESSION tidb_constraint_check_in_place_pessimistic = ON")
                .execute(&mut *conn)
                .await?;
            Ok(())
        })
    })
}

/// Standard runtime pool options for the TiDB backend.
pub(crate) fn sized_tidb_pool_options(
    max_connections: u32,
    min_connections: u32,
) -> MySqlPoolOptions {
    tidb_pool_options()
        .max_connections(max_connections)
        .min_connections(min_connections)
        .test_before_acquire(false)
        .max_lifetime(Duration::from_secs(1800))
}

/// Retry a whole TiDB transaction when TiDB documents the error as safe to retry.
///
/// This deliberately retries only storage-internal TiDB transaction failures,
/// not validation errors, conditional failures, connection loss, or unknown
/// commit outcomes.
pub(crate) async fn retry_tidb_transaction<T, F, Fut>(
    operation: &'static str,
    mut op: F,
) -> Result<T, StorageError>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T, StorageError>>,
{
    let mut retries = 0;
    loop {
        match op().await {
            Ok(value) => return Ok(value),
            Err(error)
                if retries < MAX_TRANSACTION_RETRIES && is_retryable_tidb_storage_error(&error) =>
            {
                retries += 1;
                let delay = transaction_retry_delay(retries);
                tracing::debug!(
                    operation,
                    retries,
                    delay_ms = delay.as_millis(),
                    "retrying TiDB transaction after retryable error: {error}"
                );
                tokio::time::sleep(delay).await;
            }
            Err(error) => return Err(error),
        }
    }
}

/// Retry a whole TiDB management transaction when TiDB reports a safe retry.
///
/// Management APIs use `OpError`, but the retry boundary is the same as the
/// data-plane storage transactions: only retry errors TiDB documents as
/// rolled-back whole transactions, never validation, uniqueness, not-found, or
/// unknown commit outcomes.
pub(crate) async fn retry_tidb_management_transaction<T, F, Fut>(
    operation: &'static str,
    mut op: F,
) -> Result<T, OpError>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T, OpError>>,
{
    let mut retries = 0;
    loop {
        match op().await {
            Ok(value) => return Ok(value),
            Err(error)
                if retries < MAX_TRANSACTION_RETRIES && is_retryable_tidb_op_error(&error) =>
            {
                retries += 1;
                let delay = transaction_retry_delay(retries);
                tracing::debug!(
                    operation,
                    retries,
                    delay_ms = delay.as_millis(),
                    "retrying TiDB management transaction after retryable error: {error:?}"
                );
                tokio::time::sleep(delay).await;
            }
            Err(error) => return Err(error),
        }
    }
}

fn transaction_retry_delay(retries: usize) -> Duration {
    let shift = u32::try_from(retries.saturating_sub(1)).unwrap_or(0);
    Duration::from_millis(10 * 2_u64.saturating_pow(shift))
}

fn is_retryable_tidb_storage_error(error: &StorageError) -> bool {
    match error {
        StorageError::Internal(message) => is_retryable_tidb_error_text(message),
        _ => false,
    }
}

fn is_retryable_tidb_op_error(error: &OpError) -> bool {
    match error {
        OpError::Internal(message) => is_retryable_tidb_error_text(message),
        _ => false,
    }
}

fn is_retryable_tidb_error_text(message: &str) -> bool {
    const RETRYABLE_CODES: &[&str] = &[
        "8002", // SELECT FOR UPDATE transaction cannot be retried internally.
        "8022", // Transaction commit failed and was rolled back.
        "8028", // Schema changed during the transaction.
        "9004", // Resolve lock timeout.
        "9007", // Write conflict.
        "1205", // MySQL-compatible lock wait timeout.
        "1213", // MySQL-compatible deadlock.
    ];

    RETRYABLE_CODES
        .iter()
        .any(|code| contains_db_error_code(message, code))
        || message.contains("Information schema is changed")
        || message.contains("Write conflict")
        || message.contains("Resolve Lock Timeout")
        || message.contains("Lock wait timeout")
        || message.contains("Deadlock")
}

fn contains_db_error_code(message: &str, code: &str) -> bool {
    message.match_indices(code).any(|(idx, _)| {
        let before_is_digit = message[..idx]
            .chars()
            .next_back()
            .is_some_and(|c| c.is_ascii_digit());
        let after_is_digit = message[idx + code.len()..]
            .chars()
            .next()
            .is_some_and(|c| c.is_ascii_digit());
        !before_is_digit && !after_is_digit
    })
}

/// Check if a sqlx error is a unique constraint violation (MySQL/TiDB code 1062).
pub(crate) fn is_unique_violation(e: &sqlx::Error) -> bool {
    if let sqlx::Error::Database(db_err) = e {
        return db_err.kind() == sqlx::error::ErrorKind::UniqueViolation;
    }
    false
}

/// Check if a sqlx error is a foreign key violation (MySQL/TiDB code 1451/1452).
pub(crate) fn is_fk_violation(e: &sqlx::Error) -> bool {
    if let sqlx::Error::Database(db_err) = e {
        return db_err.kind() == sqlx::error::ErrorKind::ForeignKeyViolation;
    }
    false
}

#[cfg(test)]
mod tests {
    use extenddb_storage::error::StorageError;
    use extenddb_storage::management_store::OpError;

    use super::{
        is_retryable_tidb_op_error, is_retryable_tidb_storage_error, transaction_retry_delay,
    };

    #[test]
    fn retry_classifier_accepts_documented_whole_transaction_errors() {
        for message in [
            "ERROR 8028 (HY000): Information schema is changed. [try again later]",
            "ERROR 9007 (HY000): Write conflict",
            "ERROR 1213 (40001): Deadlock found when trying to get lock",
            "ERROR 1205 (HY000): Lock wait timeout exceeded",
            "ERROR 9004 (HY000): Resolve Lock Timeout",
        ] {
            assert!(is_retryable_tidb_storage_error(&StorageError::Internal(
                message.to_owned()
            )));
        }
    }

    #[test]
    fn retry_classifier_rejects_non_transaction_outcomes() {
        for error in [
            StorageError::ConditionFailed(None),
            StorageError::Validation("bad request".to_owned()),
            StorageError::Connection("lost connection during commit".to_owned()),
            StorageError::Internal("Duplicate entry for key".to_owned()),
        ] {
            assert!(!is_retryable_tidb_storage_error(&error));
        }
    }

    #[test]
    fn management_retry_classifier_uses_same_tidb_error_set() {
        assert!(is_retryable_tidb_op_error(&OpError::Internal(
            "ERROR 8028 (HY000): Information schema is changed. [try again later]".to_owned(),
        )));
        assert!(!is_retryable_tidb_op_error(&OpError::AlreadyExists(
            "already exists".to_owned(),
        )));
    }

    #[test]
    fn retry_backoff_is_short_and_bounded() {
        assert_eq!(transaction_retry_delay(1).as_millis(), 10);
        assert_eq!(transaction_retry_delay(2).as_millis(), 20);
        assert_eq!(transaction_retry_delay(3).as_millis(), 40);
    }
}

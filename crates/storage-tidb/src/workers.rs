// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! TiDB-specific background workers.

use std::sync::Arc;
use std::time::Duration;

use extenddb_core::metrics::{MetricsCollector, QuerySource};
use sqlx::MySqlPool;

use crate::TidbEngine;

const CONTROL_PLANE_ACTIVE_POLL_MAX: Duration = Duration::from_secs(1);
const CONTROL_PLANE_ACTIVE_WINDOW: Duration = Duration::from_secs(5);
const TTL_EXPIRY_DEFAULT_INTERVAL: Duration = Duration::from_secs(1);
const TTL_EXPIRY_MIN_INTERVAL_MS: i64 = 100;
const TTL_EXPIRY_MAX_INTERVAL_MS: i64 = 60_000;
const TTL_EXPIRY_DEFAULT_BATCH_LIMIT: i64 = 1_000;
const TTL_EXPIRY_MAX_BATCH_LIMIT: i64 = 10_000;
const TTL_EXPIRY_DEFAULT_TABLE_SCAN_LIMIT: i64 = 1_024;
const TTL_EXPIRY_MAX_TABLE_SCAN_LIMIT: i64 = 10_000;
const TTL_EXPIRY_DEFAULT_DRAIN_BATCHES: usize = 8;
const TTL_EXPIRY_MAX_DRAIN_BATCHES: i64 = 100;
const TTL_EXPIRY_INTERVAL_MS_KEY: &str = "ttl_expiry_interval_ms";
const TTL_EXPIRY_BATCH_SIZE_KEY: &str = "ttl_expiry_batch_size";
const TTL_EXPIRY_TABLE_SCAN_LIMIT_KEY: &str = "ttl_expiry_table_scan_limit";
const TTL_EXPIRY_DRAIN_BATCHES_KEY: &str = "ttl_expiry_drain_batches";

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct TtlExpiryWorkerConfig {
    interval: Duration,
    batch_limit: i64,
    table_scan_limit: i64,
    drain_batches: usize,
}

impl Default for TtlExpiryWorkerConfig {
    fn default() -> Self {
        Self {
            interval: TTL_EXPIRY_DEFAULT_INTERVAL,
            batch_limit: TTL_EXPIRY_DEFAULT_BATCH_LIMIT,
            table_scan_limit: TTL_EXPIRY_DEFAULT_TABLE_SCAN_LIMIT,
            drain_batches: TTL_EXPIRY_DEFAULT_DRAIN_BATCHES,
        }
    }
}

pub(crate) async fn poll_control_plane_transitions(
    storage: Arc<TidbEngine>,
    notify: Arc<tokio::sync::Notify>,
) {
    const IDLE_TIMEOUT: Duration = Duration::from_secs(60);

    loop {
        // Idle: wait for a wake signal or timeout (defensive sweep)
        let _ = tokio::time::timeout(IDLE_TIMEOUT, notify.notified()).await;

        // Active: process immediately, then sleep only until the next scheduled
        // transition or the bounded defensive poll interval.
        let deadline = tokio::time::Instant::now() + CONTROL_PLANE_ACTIVE_WINDOW;
        loop {
            match storage.process_control_plane_transitions().await {
                Ok(ref t) if t.is_empty() => {}
                Ok(transitions) => {
                    for (name, transition) in &transitions {
                        tracing::info!("Table '{name}': {transition}");
                    }
                }
                Err(e) => {
                    tracing::warn!("Control plane transition poll failed: {e}");
                    break;
                }
            }
            if tokio::time::Instant::now() >= deadline {
                break;
            }

            let sleep_for = match next_control_plane_poll_delay(&storage).await {
                Ok(delay) => delay,
                Err(e) => {
                    tracing::warn!("Control plane transition schedule probe failed: {e}");
                    CONTROL_PLANE_ACTIVE_POLL_MAX
                }
            };
            if sleep_for.is_zero() {
                continue;
            }

            tokio::select! {
                () = notify.notified() => {}
                () = tokio::time::sleep(sleep_for) => {}
            }
        }
    }
}

pub(crate) async fn ttl_expiry_worker(storage: Arc<TidbEngine>, metrics: Arc<MetricsCollector>) {
    let mut table_scan_cursor: Option<String> = None;
    loop {
        let config = ttl_expiry_worker_config(&storage)
            .await
            .unwrap_or_else(|error| {
                tracing::warn!(
                    error = ?error,
                    "failed to load DynamoDB TTL worker settings; using defaults"
                );
                TtlExpiryWorkerConfig::default()
            });
        tokio::time::sleep(config.interval).await;

        for drain_batch in 0..config.drain_batches {
            let cycle_start = std::time::Instant::now();
            match storage
                .expire_ttl_items_with_options(
                    config.batch_limit,
                    config.table_scan_limit,
                    table_scan_cursor.as_deref(),
                )
                .await
            {
                Ok(stats) => {
                    if stats.candidate_tables == 0 {
                        table_scan_cursor = None;
                    } else if let Some(cursor) = stats.next_table_scan_cursor.clone() {
                        table_scan_cursor = Some(cursor);
                    }
                    #[allow(clippy::cast_precision_loss)]
                    let cycle_us = cycle_start.elapsed().as_micros() as f64;
                    metrics.record_worker_success(QuerySource::TtlExpiry, cycle_us);
                    metrics.record_ttl_expiry_cycle(
                        stats.expired_items,
                        stats.candidate_tables,
                        stats.oldest_expired_age_seconds,
                    );
                    if stats.expired_items > 0 {
                        tracing::debug!(
                            expired = stats.expired_items,
                            candidate_tables = stats.candidate_tables,
                            scanned_tables = stats.scanned_tables,
                            oldest_expired_age_seconds = stats.oldest_expired_age_seconds,
                            batch_limit = config.batch_limit,
                            table_scan_limit = config.table_scan_limit,
                            drain_batch = drain_batch + 1,
                            drain_batches = config.drain_batches,
                            "expired DynamoDB TTL items"
                        );
                    }
                    if !ttl_expiry_has_backlog(stats.expired_items, config.batch_limit) {
                        break;
                    }
                    tokio::task::yield_now().await;
                }
                Err(error) => {
                    metrics.record_worker_error(QuerySource::TtlExpiry);
                    tracing::warn!(error = ?error, "DynamoDB TTL expiry worker failed");
                    break;
                }
            }
        }
    }
}

async fn ttl_expiry_worker_config(
    storage: &TidbEngine,
) -> Result<TtlExpiryWorkerConfig, sqlx::Error> {
    let rows: Vec<(String, String)> =
        sqlx::query_as("SELECT `key`, value FROM settings WHERE `key` IN (?, ?, ?, ?)")
            .bind(TTL_EXPIRY_INTERVAL_MS_KEY)
            .bind(TTL_EXPIRY_BATCH_SIZE_KEY)
            .bind(TTL_EXPIRY_TABLE_SCAN_LIMIT_KEY)
            .bind(TTL_EXPIRY_DRAIN_BATCHES_KEY)
            .fetch_all(&storage.pool)
            .await?;

    Ok(apply_ttl_expiry_settings(
        TtlExpiryWorkerConfig::default(),
        &rows,
    ))
}

fn apply_ttl_expiry_settings(
    mut config: TtlExpiryWorkerConfig,
    rows: &[(String, String)],
) -> TtlExpiryWorkerConfig {
    for (key, value) in rows {
        match key.as_str() {
            TTL_EXPIRY_INTERVAL_MS_KEY => {
                if let Some(value) = parse_i64_setting_in_range(
                    key,
                    value,
                    TTL_EXPIRY_MIN_INTERVAL_MS,
                    TTL_EXPIRY_MAX_INTERVAL_MS,
                ) {
                    config.interval =
                        Duration::from_millis(u64::try_from(value).unwrap_or(u64::MAX));
                }
            }
            TTL_EXPIRY_BATCH_SIZE_KEY => {
                if let Some(value) =
                    parse_i64_setting_in_range(key, value, 1, TTL_EXPIRY_MAX_BATCH_LIMIT)
                {
                    config.batch_limit = value;
                }
            }
            TTL_EXPIRY_TABLE_SCAN_LIMIT_KEY => {
                if let Some(value) =
                    parse_i64_setting_in_range(key, value, 1, TTL_EXPIRY_MAX_TABLE_SCAN_LIMIT)
                {
                    config.table_scan_limit = value;
                }
            }
            TTL_EXPIRY_DRAIN_BATCHES_KEY => {
                if let Some(value) =
                    parse_i64_setting_in_range(key, value, 1, TTL_EXPIRY_MAX_DRAIN_BATCHES)
                {
                    config.drain_batches = usize::try_from(value).unwrap_or(usize::MAX);
                }
            }
            _ => {}
        }
    }
    config
}

fn parse_i64_setting_in_range(key: &str, value: &str, min: i64, max: i64) -> Option<i64> {
    match value.parse::<i64>() {
        Ok(value) if (min..=max).contains(&value) => Some(value),
        _ => {
            tracing::warn!(
                key,
                value,
                "ignoring invalid DynamoDB TTL worker runtime setting"
            );
            None
        }
    }
}

fn ttl_expiry_has_backlog(expired_items: usize, batch_limit: i64) -> bool {
    if batch_limit <= 0 {
        return false;
    }
    let expired_items = i64::try_from(expired_items).unwrap_or(i64::MAX);
    expired_items >= batch_limit
}

async fn next_control_plane_poll_delay(storage: &TidbEngine) -> Result<Duration, sqlx::Error> {
    let next_due_micros: Option<i64> = sqlx::query_scalar(
        "SELECT TIMESTAMPDIFF(MICROSECOND, CURRENT_TIMESTAMP(6), MIN(status_transition_at)) \
         FROM tables \
         WHERE table_status IN ('CREATING', 'UPDATING', 'DELETING') \
           AND status_transition_at IS NOT NULL",
    )
    .fetch_one(&storage.pool)
    .await?;

    Ok(transition_poll_delay(
        next_due_micros,
        CONTROL_PLANE_ACTIVE_POLL_MAX,
    ))
}

fn transition_poll_delay(next_due_micros: Option<i64>, max_poll: Duration) -> Duration {
    match next_due_micros {
        Some(micros) if micros <= 0 => Duration::ZERO,
        Some(micros) => std::cmp::min(Duration::from_micros(micros as u64), max_poll),
        None => max_poll,
    }
}

pub(crate) async fn pool_metrics_worker(pools: Vec<MySqlPool>, metrics: Arc<MetricsCollector>) {
    const SAMPLE_INTERVAL: Duration = Duration::from_secs(5);

    loop {
        tokio::time::sleep(SAMPLE_INTERVAL).await;

        let snapshots = pools
            .iter()
            .map(|pool| PoolSnapshot {
                size: pool.size() as usize,
                idle: pool.num_idle(),
            })
            .collect::<Vec<_>>();
        let (total_active, total_idle) = pool_metric_totals(&snapshots);

        #[allow(clippy::cast_possible_truncation)]
        metrics.record_pool_state(total_active as u32, total_idle as u32);
    }
}

#[derive(Clone, Copy)]
struct PoolSnapshot {
    size: usize,
    idle: usize,
}

fn pool_metric_totals(pools: &[PoolSnapshot]) -> (usize, usize) {
    pools.iter().fold((0, 0), |(active, idle), pool| {
        (
            active + pool.size.saturating_sub(pool.idle),
            idle + pool.idle,
        )
    })
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::{
        PoolSnapshot, TTL_EXPIRY_BATCH_SIZE_KEY, TTL_EXPIRY_DRAIN_BATCHES_KEY,
        TTL_EXPIRY_INTERVAL_MS_KEY, TTL_EXPIRY_TABLE_SCAN_LIMIT_KEY, TtlExpiryWorkerConfig,
        apply_ttl_expiry_settings, parse_i64_setting_in_range, pool_metric_totals,
        transition_poll_delay, ttl_expiry_has_backlog,
    };

    #[test]
    fn transition_poll_delay_tracks_near_due_transitions() {
        let max_poll = Duration::from_secs(1);

        assert_eq!(transition_poll_delay(Some(-1), max_poll), Duration::ZERO);
        assert_eq!(transition_poll_delay(Some(0), max_poll), Duration::ZERO);
        assert_eq!(
            transition_poll_delay(Some(250_000), max_poll),
            Duration::from_millis(250)
        );
        assert_eq!(transition_poll_delay(Some(2_000_000), max_poll), max_poll);
        assert_eq!(transition_poll_delay(None, max_poll), max_poll);
    }

    #[test]
    fn pool_metric_totals_include_every_tidb_pool() {
        let pools = [
            PoolSnapshot { size: 10, idle: 7 },
            PoolSnapshot { size: 10, idle: 8 },
            PoolSnapshot { size: 10, idle: 10 },
            PoolSnapshot { size: 10, idle: 6 },
        ];

        assert_eq!(pool_metric_totals(&pools), (9, 31));
    }

    #[test]
    fn ttl_expiry_settings_override_defaults() {
        let rows = vec![
            (TTL_EXPIRY_INTERVAL_MS_KEY.to_owned(), "250".to_owned()),
            (TTL_EXPIRY_BATCH_SIZE_KEY.to_owned(), "500".to_owned()),
            (TTL_EXPIRY_TABLE_SCAN_LIMIT_KEY.to_owned(), "64".to_owned()),
            (TTL_EXPIRY_DRAIN_BATCHES_KEY.to_owned(), "4".to_owned()),
        ];

        let config = apply_ttl_expiry_settings(TtlExpiryWorkerConfig::default(), &rows);

        assert_eq!(config.interval, Duration::from_millis(250));
        assert_eq!(config.batch_limit, 500);
        assert_eq!(config.table_scan_limit, 64);
        assert_eq!(config.drain_batches, 4);
    }

    #[test]
    fn ttl_expiry_settings_ignore_invalid_direct_db_values() {
        let rows = vec![
            (TTL_EXPIRY_INTERVAL_MS_KEY.to_owned(), "0".to_owned()),
            (TTL_EXPIRY_BATCH_SIZE_KEY.to_owned(), "-1".to_owned()),
            (
                TTL_EXPIRY_TABLE_SCAN_LIMIT_KEY.to_owned(),
                "not-a-number".to_owned(),
            ),
            (TTL_EXPIRY_DRAIN_BATCHES_KEY.to_owned(), "101".to_owned()),
        ];

        let config = apply_ttl_expiry_settings(TtlExpiryWorkerConfig::default(), &rows);

        assert_eq!(config, TtlExpiryWorkerConfig::default());
        assert_eq!(
            parse_i64_setting_in_range("ttl_expiry_batch_size", "42", 1, 100),
            Some(42)
        );
        assert_eq!(
            parse_i64_setting_in_range("ttl_expiry_batch_size", "0", 1, 100),
            None
        );
    }

    #[test]
    fn ttl_expiry_backlog_detection_requires_full_batch() {
        assert!(!ttl_expiry_has_backlog(0, 1_000));
        assert!(!ttl_expiry_has_backlog(999, 1_000));
        assert!(ttl_expiry_has_backlog(1_000, 1_000));
        assert!(ttl_expiry_has_backlog(1_001, 1_000));
        assert!(!ttl_expiry_has_backlog(1, 0));
    }
}

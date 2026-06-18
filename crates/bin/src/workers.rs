// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Background workers spawned by `extenddb serve`.
//!
//! Each function runs as a `tokio::spawn`-ed task for the lifetime of the
//! server process. Workers handle log-level polling, distributed auth-cache
//! invalidation, control-plane transitions, native TiDB retention repair,
//! and in-memory metrics pruning.
//!
//! Workers are generic over storage traits so they are decoupled from concrete
//! TiDB engine and catalog-store types.

use std::sync::Arc;

use extenddb_auth::{AuthCacheEpochBumper, AuthCacheRegistry};
use extenddb_storage::management_store::{MetricsStore, SettingsStore};
use futures::future::BoxFuture;
use tracing_subscriber::{EnvFilter, reload};

pub(crate) struct SettingsAuthCacheEpochBumper {
    store: Arc<dyn SettingsStore>,
}

impl SettingsAuthCacheEpochBumper {
    pub(crate) fn new(store: Arc<dyn SettingsStore>) -> Self {
        Self { store }
    }
}

impl AuthCacheEpochBumper for SettingsAuthCacheEpochBumper {
    fn bump_auth_cache_epoch<'a>(&'a self) -> BoxFuture<'a, ()> {
        Box::pin(async move {
            match self.store.bump_auth_cache_epoch().await {
                Ok(epoch) => {
                    tracing::debug!(epoch, "auth cache epoch bumped");
                }
                Err(error) => {
                    tracing::warn!(
                        error = ?error,
                        "failed to bump distributed auth cache epoch; relying on TTL"
                    );
                }
            }
        })
    }
}

/// Poll the distributed auth-cache epoch and flush local caches when another
/// frontend changes IAM/auth state.
pub(crate) async fn poll_auth_cache_epoch(
    store: Arc<dyn SettingsStore>,
    auth_cache: AuthCacheRegistry,
    initial_epoch: u64,
) {
    use std::time::Duration;

    const POLL_INTERVAL: Duration = Duration::from_secs(1);
    let mut current_epoch = initial_epoch;

    loop {
        tokio::time::sleep(POLL_INTERVAL).await;

        let new_epoch = match store.auth_cache_epoch().await {
            Ok(epoch) => epoch,
            Err(error) => {
                tracing::debug!(
                    error = ?error,
                    "failed to poll distributed auth cache epoch"
                );
                continue;
            }
        };

        if new_epoch == current_epoch {
            continue;
        }

        tracing::info!(
            old_epoch = current_epoch,
            new_epoch,
            "distributed auth cache epoch changed; flushing local auth caches"
        );
        auth_cache.invalidate_all_local();
        current_epoch = new_epoch;
    }
}

/// Poll the `log_level` and `sqlx_log_level` settings from the database
/// and reload the tracing filter when either changes.
/// The combined filter is `{log_level},sqlx={sqlx_log_level}`.
/// Falls back to `config_level` when `log_level` is absent from the DB.
/// Runs until the process exits.
pub(crate) async fn poll_log_level(
    store: Arc<dyn SettingsStore>,
    handle: reload::Handle<EnvFilter, tracing_subscriber::Registry>,
    config_level: String,
) {
    use std::time::Duration;

    const POLL_INTERVAL: Duration = Duration::from_secs(30);
    let mut current_level = config_level;
    let mut current_sqlx_level = String::from("warn");

    loop {
        tokio::time::sleep(POLL_INTERVAL).await;

        let (log_result, sqlx_result) = tokio::join!(
            store.get_setting("log_level"),
            store.get_setting("sqlx_log_level"),
        );

        let new_level = match log_result {
            Ok(Some(v)) => v,
            Ok(None) => current_level.clone(),
            Err(_) => {
                tracing::debug!("Failed to query log_level setting");
                continue;
            }
        };

        let new_sqlx_level = match sqlx_result {
            Ok(Some(v)) => v,
            Ok(None) => current_sqlx_level.clone(),
            Err(_) => {
                tracing::debug!("Failed to query sqlx_log_level setting");
                continue;
            }
        };

        if new_level == current_level && new_sqlx_level == current_sqlx_level {
            continue;
        }

        // Combined filter encodes both levels.
        let filter_str = format!("{new_level},sqlx={new_sqlx_level}");

        match EnvFilter::try_new(&filter_str) {
            Ok(new_filter) => {
                // H-4: Log at warn so the message is visible even when
                // switching to a more restrictive level (e.g. debug → error).
                if new_level != current_level {
                    tracing::warn!("Log level changing to '{new_level}' (from settings table)");
                }
                if new_sqlx_level != current_sqlx_level {
                    tracing::warn!(
                        "sqlx log level changing to '{new_sqlx_level}' (from settings table)"
                    );
                }
                if let Err(e) = handle.reload(new_filter) {
                    tracing::warn!("Failed to reload log filter: {e}");
                } else {
                    current_level = new_level;
                    current_sqlx_level = new_sqlx_level;
                }
            }
            Err(e) => {
                tracing::warn!("Invalid log filter '{filter_str}': {e}");
            }
        }
    }
}

/// Periodically prune metrics data points older than 1 day.
pub(crate) async fn metrics_prune_worker(metrics: Arc<extenddb_core::metrics::MetricsCollector>) {
    use extenddb_core::metrics::QuerySource;

    const PRUNE_INTERVAL: std::time::Duration = std::time::Duration::from_secs(300);
    loop {
        tokio::time::sleep(PRUNE_INTERVAL).await;
        let cycle_start = std::time::Instant::now();
        metrics.prune();
        #[allow(clippy::cast_precision_loss)]
        let cycle_us = cycle_start.elapsed().as_micros() as f64;
        metrics.record_worker_success(QuerySource::MetricsPrune, cycle_us);
    }
}

/// Periodically flush in-memory metrics to the database.
///
/// Drains data points older than 60 seconds, aggregates them into 1-minute
/// buckets, and upserts via the `MetricsStore` trait. Database retention is
/// TiDB-specific and implemented by storage runtime hooks.
pub(crate) async fn metrics_flush_worker(
    metrics: Arc<extenddb_core::metrics::MetricsCollector>,
    store: Arc<dyn MetricsStore>,
) {
    use extenddb_core::metrics::QuerySource;
    use extenddb_storage::management_store::MetricsRow;

    const FLUSH_INTERVAL: std::time::Duration = std::time::Duration::from_secs(60);
    loop {
        tokio::time::sleep(FLUSH_INTERVAL).await;
        let cycle_start = std::time::Instant::now();
        let buckets = metrics.drain(FLUSH_INTERVAL);
        if !buckets.is_empty() {
            let rows: Vec<MetricsRow> = buckets
                .iter()
                .map(|b| {
                    let secs = b
                        .bucket
                        .duration_since(std::time::SystemTime::UNIX_EPOCH)
                        .unwrap_or_default()
                        .as_secs();
                    #[allow(clippy::cast_possible_wrap)]
                    let bucket_ts = time::OffsetDateTime::from_unix_timestamp(secs as i64)
                        .unwrap_or(time::OffsetDateTime::UNIX_EPOCH);
                    MetricsRow {
                        bucket: bucket_ts,
                        metric: b.metric.to_string(),
                        table_name: if b.table_name.is_empty() {
                            None
                        } else {
                            Some(b.table_name.clone())
                        },
                        index_name: if b.index_name.is_empty() {
                            None
                        } else {
                            Some(b.index_name.clone())
                        },
                        operation: if b.operation.is_empty() {
                            None
                        } else {
                            Some(b.operation.clone())
                        },
                        sum: b.sum,
                        count: i64::try_from(b.count).unwrap_or(i64::MAX),
                        min: b.min,
                        max: b.max,
                    }
                })
                .collect();
            // insert_metrics logs per-row failures internally and always returns Ok.
            let _ = store.insert_metrics(&rows).await;
        }
        #[allow(clippy::cast_precision_loss)]
        let cycle_us = cycle_start.elapsed().as_micros() as f64;
        metrics.record_worker_success(QuerySource::MetricsFlush, cycle_us);
    }
}

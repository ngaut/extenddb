// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Storage runtime hooks for worker spawning and initialization.

use std::sync::Arc;

use async_trait::async_trait;
use tracing_subscriber::{EnvFilter, Registry, reload};

/// Context passed to ServerRuntimeHooks::spawn_workers.
///
/// Contains shared resources that storage-owned workers might need.
pub struct WorkerContext {
    pub metrics: Arc<extenddb_core::metrics::MetricsCollector>,
    pub catalog_store: Arc<dyn crate::CatalogStore>,
    pub reload_handle: reload::Handle<EnvFilter, Registry>,
    pub config_log_level: String,
}

/// Storage readiness failure returned by [`ServerRuntimeHooks::health_check`].
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct StorageHealthError {
    message: String,
}

impl StorageHealthError {
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }

    pub fn message(&self) -> &str {
        &self.message
    }
}

impl std::fmt::Display for StorageHealthError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.message)
    }
}

impl std::error::Error for StorageHealthError {}

/// Storage runtime hooks for worker spawning and initialization.
///
/// Storage implementations use this trait to spawn workers that are tightly
/// coupled to their implementation details, such as control-plane pollers and
/// pool metrics workers.
#[async_trait]
pub trait ServerRuntimeHooks: Send + Sync {
    /// Spawn storage-owned workers.
    ///
    /// Called after server components are created but before the HTTP server
    /// starts. Storage implementations can spawn workers that need access to
    /// internal state (connection pools, notify handles, etc.).
    async fn spawn_workers(&self, ctx: &WorkerContext);

    /// Check the storage resources owned by this frontend.
    ///
    /// HTTP `/health` calls this so load balancers observe the selected
    /// storage layer's real readiness instead of only the web process state.
    async fn health_check(&self) -> Result<(), StorageHealthError> {
        Ok(())
    }

    /// Get storage implementation info for logging (optional).
    ///
    /// Example: "data_db=extenddb_data"
    fn storage_info(&self) -> Option<String> {
        None
    }
}

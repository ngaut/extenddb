// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Server component bundle returned by the concrete storage implementation.

use std::sync::Arc;

use extenddb_auth::CredentialStore;

use crate::hooks::ServerRuntimeHooks;
use crate::{CatalogStore, StorageEngine};

/// Components needed to run the extenddb server.
///
/// Contains all trait objects needed by cmd_serve to start the HTTP server and
/// spawn storage-owned workers.
pub struct ServerComponents {
    /// Storage engine implementing all data/metadata operations
    pub engine: Arc<dyn StorageEngine>,

    /// Catalog store for management API operations
    pub catalog_store: Arc<dyn CatalogStore>,

    /// Raw (uncached) credential store. The bin layer wraps this in
    /// `CachedCredentialStore` using the operator-configured TTL before
    /// constructing the auth provider.
    pub credential_store: Arc<dyn CredentialStore>,

    /// Optional storage-owned runtime hooks for worker spawning and health checks
    pub runtime_hooks: Option<Arc<dyn ServerRuntimeHooks>>,
}

/// Errors that can occur during storage initialization.
#[derive(Debug)]
pub enum StorageInitError {
    /// Failed to connect to TiDB or an associated native service.
    ConnectionFailed { target: String, details: String },

    /// Catalog schema version mismatch
    CatalogVersionMismatch { expected: String, found: String },

    /// Encryption key not found in settings table
    MissingEncryptionKey,

    /// Generic initialization failure
    InitializationFailed(String),
}

impl std::fmt::Display for StorageInitError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ConnectionFailed { target, details } => {
                write!(f, "Failed to connect to {target}: {details}")
            }
            Self::CatalogVersionMismatch { expected, found } => write!(
                f,
                "Catalog version mismatch: expected {expected}, found {found}. Run 'extenddb migrate'"
            ),
            Self::MissingEncryptionKey => write!(
                f,
                "Encryption key not found in settings table. Run 'extenddb init'"
            ),
            Self::InitializationFailed(msg) => write!(f, "Storage initialization failed: {msg}"),
        }
    }
}

impl std::error::Error for StorageInitError {}

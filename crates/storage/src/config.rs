// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Shared storage configuration types.

/// TiDB-native backup tool configuration.
///
/// TiDB maps these fields to BR/PITR options. The type lives in the storage
/// crate because the backup engine and server startup path both need to pass
/// the same physical-backup settings without depending on CLI config types.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct NativeBackupConfig {
    /// Backup executable.
    pub binary: Option<String>,
    /// Optional subcommand/component inserted after `binary`; set it to an
    /// empty string when `binary` is already the native backup executable.
    pub component: Option<String>,
    /// Cluster coordinator endpoint.
    pub coordinator_endpoint: Option<String>,
    /// Base URI for snapshot backups.
    pub storage_uri: Option<String>,
    /// Base URI for log backups / PITR.
    pub log_storage_uri: Option<String>,
    /// Whether to send object-store credentials to storage nodes.
    pub send_credentials_to_storage_nodes: Option<bool>,
}

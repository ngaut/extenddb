// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Shared types for TiDB init/destroy/migrate operations.

/// Connection parameters for bootstrap operations.
///
/// These are the raw parameters needed to connect to TiDB before any databases
/// or schemas exist.
#[derive(Debug, Clone)]
pub struct BootstrapConfig {
    pub host: String,
    pub port: u16,
    pub admin_user: String,
    pub admin_password: Option<String>,
    pub app_user: String,
    pub app_password: String,
    pub catalog_db: String,
    pub data_db: String,
}

/// CLI-provided bootstrap overrides after argument parsing.
///
/// TiDB bootstrap code merges these typed values with config-file defaults. The
/// CLI owns spelling, aliases, and `--flag=value` parsing; storage code should
/// not inspect raw process arguments.
#[derive(Debug, Clone, Default)]
pub struct BootstrapOptions {
    pub storage_host: Option<String>,
    pub storage_port: Option<u16>,
    pub admin_user: Option<String>,
    pub admin_password: Option<String>,
    pub data_db: Option<String>,
    pub catalog_db: Option<String>,
    pub app_user: Option<String>,
    pub app_password: Option<String>,
}

/// Result of a bootstrap admin user creation.
#[derive(Debug)]
pub struct AdminBootstrapResult {
    /// The admin username that was created or already existed.
    pub username: String,
    /// The password, if a new one was generated (not returned for pre-existing
    /// users or environment-sourced credentials).
    pub generated_password: Option<String>,
    /// Whether the user already existed (skipped creation).
    pub already_existed: bool,
    /// Whether credentials came from environment variables.
    pub from_env: bool,
}

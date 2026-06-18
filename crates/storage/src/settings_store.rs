// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Settings store construction errors.

/// Error type for settings store creation.
#[derive(Debug)]
pub enum SettingsStoreError {
    ConnectionFailed(String),
}

impl std::fmt::Display for SettingsStoreError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ConnectionFailed(msg) => write!(f, "Failed to connect: {msg}"),
        }
    }
}

impl std::error::Error for SettingsStoreError {}

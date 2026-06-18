// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Settings validation and write operations.
//!
//! Validation logic lives here in the server layer. The actual database
//! write is delegated to the `SettingsStore` trait implementation.

use extenddb_storage::management_store::{OpError, OpResult};

pub const ALLOW_CREDENTIAL_IMPORT_DEFAULT: bool = true;
pub const ALLOW_CREDENTIAL_IMPORT_DEFAULT_VALUE: &str = if ALLOW_CREDENTIAL_IMPORT_DEFAULT {
    "true"
} else {
    "false"
};

/// Validator function for a setting value.
pub type Validator = fn(&str) -> Result<(), &'static str>;

/// Runtime setting definition.
#[derive(Debug, Clone, Copy)]
pub struct SettingSpec {
    pub key: &'static str,
    pub validator: Validator,
}

/// Known writable setting keys and their validators.
pub const KNOWN_KEYS: &[SettingSpec] = &[
    SettingSpec {
        key: "allow_credential_import",
        validator: validate_bool,
    },
    SettingSpec {
        key: "log_level",
        validator: validate_log_level,
    },
    SettingSpec {
        key: "sqlx_log_level",
        validator: validate_log_level,
    },
    SettingSpec {
        key: "ttl_expiry_interval_ms",
        validator: validate_ttl_expiry_interval_ms,
    },
    SettingSpec {
        key: "ttl_expiry_batch_size",
        validator: validate_ttl_expiry_batch_size,
    },
    SettingSpec {
        key: "ttl_expiry_table_scan_limit",
        validator: validate_ttl_expiry_table_scan_limit,
    },
    SettingSpec {
        key: "ttl_expiry_drain_batches",
        validator: validate_ttl_expiry_drain_batches,
    },
];

/// Legacy frontend-owned settings that are intentionally not writable on TiDB.
const UNSUPPORTED_KEYS: &[(&str, &str)] = &[
    (
        "control_plane_delay_seconds",
        "TiDB uses native online DDL and control-plane coordination",
    ),
    (
        "gsi_propagation_delay_ms",
        "TiDB secondary indexes are native and maintained from base-row writes",
    ),
    (
        "throttling_enabled",
        "TiDB Resource Control provides distributed capacity governance",
    ),
];

/// Read-only keys that cannot be changed via the settings API.
pub const READONLY_KEYS: &[&str] = &[
    "catalog_version",
    "data_database_connection_string",
    "data_database_name",
];

fn validate_log_level(value: &str) -> Result<(), &'static str> {
    match value {
        "trace" | "debug" | "info" | "warn" | "error" => Ok(()),
        _ => Err("must be one of: trace, debug, info, warn, error"),
    }
}

fn validate_bool(value: &str) -> Result<(), &'static str> {
    match value {
        "true" | "false" => Ok(()),
        _ => Err("must be 'true' or 'false'"),
    }
}

fn validate_int_range(value: &str, min: u64, max: u64) -> Result<(), &'static str> {
    match value.parse::<u64>() {
        Ok(parsed) if (min..=max).contains(&parsed) => Ok(()),
        _ => Err("must be an integer in the supported range"),
    }
}

fn validate_ttl_expiry_interval_ms(value: &str) -> Result<(), &'static str> {
    validate_int_range(value, 100, 60_000)
}

fn validate_ttl_expiry_batch_size(value: &str) -> Result<(), &'static str> {
    validate_int_range(value, 1, 10_000)
}

fn validate_ttl_expiry_table_scan_limit(value: &str) -> Result<(), &'static str> {
    validate_int_range(value, 1, 10_000)
}

fn validate_ttl_expiry_drain_batches(value: &str) -> Result<(), &'static str> {
    validate_int_range(value, 1, 100)
}

fn setting_spec(key: &str) -> Option<&'static SettingSpec> {
    KNOWN_KEYS.iter().find(|spec| spec.key == key)
}

pub fn setting_is_supported(key: &str) -> bool {
    setting_spec(key).is_some()
}

pub fn known_writable_keys() -> Vec<&'static str> {
    KNOWN_KEYS.iter().map(|spec| spec.key).collect()
}

pub fn validate_setting(key: &str, value: &str) -> OpResult<()> {
    if READONLY_KEYS.contains(&key) {
        return Err(OpError::Validation(format!("Setting '{key}' is read-only")));
    }

    if let Some((_, reason)) = UNSUPPORTED_KEYS
        .iter()
        .find(|(unsupported, _)| *unsupported == key)
    {
        return Err(OpError::Validation(format!(
            "Setting '{key}' is not supported for TiDB because {reason}"
        )));
    }

    let Some(spec) = setting_spec(key) else {
        return Err(OpError::Validation(format!(
            "Unknown setting '{key}'. Known writable keys: {}",
            known_writable_keys().join(", ")
        )));
    };

    (spec.validator)(value)
        .map_err(|reason| OpError::Validation(format!("Invalid value for '{key}': {reason}")))?;
    Ok(())
}

/// Set a runtime setting with validation.
///
/// Validates the key and value, then delegates the write to the
/// `SettingsStore` implementation. Validation stays in the server layer;
/// the storage layer trusts validated input.
///
/// # Errors
///
/// Returns `OpError::Validation` if the key is read-only, unknown, or the value
/// fails validation. Returns `OpError::Internal` on database errors.
pub async fn set_setting(
    store: &dyn extenddb_storage::management_store::SettingsStore,
    key: &str,
    value: &str,
) -> OpResult<()> {
    validate_setting(key, value)?;
    store.set_setting(key, value).await?;

    tracing::warn!(
        target: "extenddb::audit::settings",
        "settings-set: key={key}, value={value}",
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{known_writable_keys, setting_is_supported, validate_setting};
    use extenddb_storage::management_store::OpError;

    fn validation_message(result: Result<(), OpError>) -> String {
        match result {
            Err(OpError::Validation(message)) => message,
            other => panic!("expected validation error, got {other:?}"),
        }
    }

    #[test]
    fn tidb_rejects_frontend_legacy_settings() {
        let control_plane =
            validation_message(validate_setting("control_plane_delay_seconds", "0.25"));
        assert!(control_plane.contains("native online DDL"));

        let indexes = validation_message(validate_setting("gsi_propagation_delay_ms", "10"));
        assert!(indexes.contains("secondary indexes are native"));

        let throttling = validation_message(validate_setting("throttling_enabled", "true"));
        assert!(throttling.contains("Resource Control"));
    }

    #[test]
    fn writable_key_list_is_tidb_only() {
        assert!(setting_is_supported("log_level"));
        assert!(!setting_is_supported("control_plane_delay_seconds"));
        assert_eq!(
            known_writable_keys(),
            vec![
                "allow_credential_import",
                "log_level",
                "sqlx_log_level",
                "ttl_expiry_interval_ms",
                "ttl_expiry_batch_size",
                "ttl_expiry_table_scan_limit",
                "ttl_expiry_drain_batches",
            ]
        );
    }

    #[test]
    fn ttl_worker_settings_are_range_checked() {
        assert!(validate_setting("ttl_expiry_interval_ms", "100").is_ok());
        assert!(validate_setting("ttl_expiry_batch_size", "10000").is_ok());
        assert!(validate_setting("ttl_expiry_table_scan_limit", "128").is_ok());
        assert!(validate_setting("ttl_expiry_drain_batches", "100").is_ok());

        assert!(validate_setting("ttl_expiry_interval_ms", "99").is_err());
        assert!(validate_setting("ttl_expiry_interval_ms", "60001").is_err());
        assert!(validate_setting("ttl_expiry_batch_size", "0").is_err());
        assert!(validate_setting("ttl_expiry_table_scan_limit", "not-a-number").is_err());
        assert!(validate_setting("ttl_expiry_drain_batches", "0").is_err());
        assert!(validate_setting("ttl_expiry_drain_batches", "101").is_err());
    }

    #[test]
    fn unknown_setting_message_lists_only_supported_keys() {
        let message = validation_message(validate_setting("not_a_setting", "true"));

        assert!(message.contains("allow_credential_import"));
        assert!(message.contains("log_level"));
        assert!(!message.contains("gsi_propagation_delay_ms"));
    }
}

// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Provisioned-throughput accounting helpers.

use crate::error::DynamoDbError;
use crate::types::{ProvisionedThroughput, ProvisionedThroughputDescription};

/// Build a response/catalog throughput description with no update history.
#[must_use]
pub fn provisioned_throughput_description(
    throughput: &ProvisionedThroughput,
) -> ProvisionedThroughputDescription {
    ProvisionedThroughputDescription {
        read_capacity_units: throughput.read_capacity_units,
        write_capacity_units: throughput.write_capacity_units,
        number_of_decreases_today: 0,
        last_increase_date_time: None,
        last_decrease_date_time: None,
    }
}

/// Build a zero-throughput description used for PAY_PER_REQUEST responses.
#[must_use]
pub fn zero_provisioned_throughput_description() -> ProvisionedThroughputDescription {
    ProvisionedThroughputDescription {
        read_capacity_units: 0,
        write_capacity_units: 0,
        number_of_decreases_today: 0,
        last_increase_date_time: None,
        last_decrease_date_time: None,
    }
}

/// Parse current throughput metadata from either the canonical description
/// shape or the older input-only shape.
///
/// # Errors
///
/// Returns a serde error if neither shape can be deserialized.
pub fn provisioned_throughput_description_from_value(
    value: serde_json::Value,
) -> Result<ProvisionedThroughputDescription, serde_json::Error> {
    serde_json::from_value::<ProvisionedThroughputDescription>(value.clone()).or_else(|_| {
        let throughput: ProvisionedThroughput = serde_json::from_value(value)?;
        Ok(provisioned_throughput_description(&throughput))
    })
}

/// Apply a provisioned-throughput UpdateTable request under DynamoDB's daily
/// decrease quota.
///
/// # Errors
///
/// Returns `ValidationException` if the requested update exceeds the currently
/// available decrease quota for the UTC day.
pub fn apply_provisioned_throughput_update(
    current: Option<&ProvisionedThroughputDescription>,
    requested: &ProvisionedThroughput,
    now_epoch_seconds: f64,
) -> Result<ProvisionedThroughputDescription, DynamoDbError> {
    let is_decrease = current.is_some_and(|current| {
        requested.read_capacity_units < current.read_capacity_units
            || requested.write_capacity_units < current.write_capacity_units
    });
    let is_increase = current.is_none_or(|current| {
        requested.read_capacity_units > current.read_capacity_units
            || requested.write_capacity_units > current.write_capacity_units
    });

    let mut decreases_today = current.map_or(0, |current| {
        if current
            .last_decrease_date_time
            .is_some_and(|last| same_utc_day(last, now_epoch_seconds))
        {
            current.number_of_decreases_today.max(0)
        } else {
            0
        }
    });

    if is_decrease {
        let allowed = provisioned_decrease_quota_for_epoch(now_epoch_seconds);
        if decreases_today >= allowed {
            return Err(DynamoDbError::ValidationException(format!(
                "The maximum number of provisioned throughput decreases for this table has been exceeded. \
                 Decreases used today: {decreases_today}. Decreases allowed now: {allowed}."
            )));
        }
        decreases_today += 1;
    }

    Ok(ProvisionedThroughputDescription {
        read_capacity_units: requested.read_capacity_units,
        write_capacity_units: requested.write_capacity_units,
        number_of_decreases_today: decreases_today,
        last_increase_date_time: if is_increase {
            Some(now_epoch_seconds)
        } else {
            current.and_then(|current| current.last_increase_date_time)
        },
        last_decrease_date_time: if is_decrease {
            Some(now_epoch_seconds)
        } else {
            current.and_then(|current| current.last_decrease_date_time)
        },
    })
}

/// Return current Unix epoch seconds as a floating point value used by DynamoDB
/// timestamp fields.
#[must_use]
pub fn current_unix_epoch_seconds() -> f64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs_f64()
}

/// DynamoDB allows four decreases in the first UTC hour of the day and one
/// additional decrease in each following hour, capped at 27 per day.
#[must_use]
pub fn provisioned_decrease_quota_for_epoch(epoch_seconds: f64) -> i64 {
    let seconds = epoch_seconds.max(0.0).floor() as i64;
    let hour = (seconds % 86_400) / 3_600;
    (4 + hour).min(27)
}

fn same_utc_day(left_epoch_seconds: f64, right_epoch_seconds: f64) -> bool {
    utc_day(left_epoch_seconds) == utc_day(right_epoch_seconds)
}

fn utc_day(epoch_seconds: f64) -> i64 {
    (epoch_seconds.max(0.0).floor() as i64) / 86_400
}

#[cfg(test)]
mod tests {
    use super::*;

    fn desc(read: i64, write: i64) -> ProvisionedThroughputDescription {
        ProvisionedThroughputDescription {
            read_capacity_units: read,
            write_capacity_units: write,
            number_of_decreases_today: 0,
            last_increase_date_time: None,
            last_decrease_date_time: None,
        }
    }

    #[test]
    fn decrease_quota_matches_hourly_schedule() {
        assert_eq!(provisioned_decrease_quota_for_epoch(0.0), 4);
        assert_eq!(provisioned_decrease_quota_for_epoch(3_600.0), 5);
        assert_eq!(provisioned_decrease_quota_for_epoch(23.0 * 3_600.0), 27);
    }

    #[test]
    fn throughput_decrease_increments_counter_and_sets_timestamp() {
        let mut current = desc(10, 10);
        current.number_of_decreases_today = 4;
        current.last_decrease_date_time = Some(3_600.0);
        let requested = ProvisionedThroughput {
            read_capacity_units: 9,
            write_capacity_units: 10,
        };

        let updated = apply_provisioned_throughput_update(Some(&current), &requested, 3_700.0)
            .expect("fifth decrease is allowed after first hour");

        assert_eq!(updated.number_of_decreases_today, 5);
        assert_eq!(updated.last_decrease_date_time, Some(3_700.0));
    }

    #[test]
    fn throughput_decrease_rejects_when_quota_is_exhausted() {
        let mut current = desc(10, 10);
        current.number_of_decreases_today = 4;
        current.last_decrease_date_time = Some(0.0);
        let requested = ProvisionedThroughput {
            read_capacity_units: 9,
            write_capacity_units: 10,
        };

        let err =
            apply_provisioned_throughput_update(Some(&current), &requested, 1_000.0).unwrap_err();

        assert!(
            err.to_string().contains("maximum number"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn throughput_counter_resets_on_new_utc_day() {
        let mut current = desc(10, 10);
        current.number_of_decreases_today = 27;
        current.last_decrease_date_time = Some(86_399.0);
        let requested = ProvisionedThroughput {
            read_capacity_units: 9,
            write_capacity_units: 10,
        };

        let updated = apply_provisioned_throughput_update(Some(&current), &requested, 86_400.0)
            .expect("new day gets a fresh quota");

        assert_eq!(updated.number_of_decreases_today, 1);
    }

    #[test]
    fn throughput_increase_sets_increase_time_without_decrease() {
        let current = desc(10, 10);
        let requested = ProvisionedThroughput {
            read_capacity_units: 11,
            write_capacity_units: 10,
        };

        let updated = apply_provisioned_throughput_update(Some(&current), &requested, 42.0)
            .expect("increase is allowed");

        assert_eq!(updated.number_of_decreases_today, 0);
        assert_eq!(updated.last_increase_date_time, Some(42.0));
        assert_eq!(updated.last_decrease_date_time, None);
    }

    #[test]
    fn throughput_metadata_parser_accepts_legacy_input_shape() {
        let value = serde_json::json!({
            "ReadCapacityUnits": 5,
            "WriteCapacityUnits": 7
        });

        let parsed = provisioned_throughput_description_from_value(value).expect("legacy shape");

        assert_eq!(parsed.read_capacity_units, 5);
        assert_eq!(parsed.write_capacity_units, 7);
        assert_eq!(parsed.number_of_decreases_today, 0);
    }

    #[test]
    fn throughput_metadata_parser_preserves_canonical_description_shape() {
        let value = serde_json::json!({
            "ReadCapacityUnits": 5,
            "WriteCapacityUnits": 7,
            "NumberOfDecreasesToday": 3,
            "LastIncreaseDateTime": 12.5,
            "LastDecreaseDateTime": 24.5
        });

        let parsed =
            provisioned_throughput_description_from_value(value).expect("description shape");

        assert_eq!(parsed.read_capacity_units, 5);
        assert_eq!(parsed.write_capacity_units, 7);
        assert_eq!(parsed.number_of_decreases_today, 3);
        assert_eq!(parsed.last_increase_date_time, Some(12.5));
        assert_eq!(parsed.last_decrease_date_time, Some(24.5));
    }
}

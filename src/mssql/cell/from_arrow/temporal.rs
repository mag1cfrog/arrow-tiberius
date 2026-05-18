//! Temporal Arrow-to-MSSQL runtime cell conversion.

mod date;
mod date64;
mod datetime2;

use arrow_array::timezone::Tz;
use arrow_schema::{DataType, TimeUnit};
use chrono::{Offset, TimeZone};

use crate::{DiagnosticCode, MssqlType, Result, SchemaMapping, arrow::cell::ArrowCell};

use super::{
    ArrowToMssqlRuntimeMapping, row_mapping_diagnostic, unsupported_value_conversion,
    value_conversion_error,
};
use crate::mssql::cell::{MssqlCell, MssqlDateTime2, MssqlDateTimeOffset};
pub(super) use date::mssql_date_value;
pub(super) use date64::mssql_datetime2_from_arrow_date64;
use datetime2::{
    mssql_datetime2_from_arrow_timestamp_microsecond,
    mssql_datetime2_from_arrow_timestamp_millisecond,
    mssql_datetime2_from_arrow_timestamp_nanosecond, mssql_datetime2_from_arrow_timestamp_second,
    mssql_datetime2_from_unix_epoch_100ns_ticks, nanoseconds_to_100ns_ticks,
};

const SQL_SERVER_DATE_UNIX_EPOCH_DAYS: i64 = 719_162;
const SQL_SERVER_DATE_MAX_DAYS: i64 = 3_652_058;
const MILLISECONDS_PER_DAY: i64 = 86_400_000;
const SQL_SERVER_DATETIME2_DATE64_SCALE: u8 = 3;
const SQL_SERVER_DATETIME2_TIMESTAMP_SCALE: u8 = 7;
const TICKS_100NS_PER_SECOND: i128 = 10_000_000;
const TICKS_100NS_PER_MILLISECOND: i128 = 10_000;
const TICKS_100NS_PER_MICROSECOND: i128 = 10;
const TICKS_100NS_PER_DAY: i128 = 864_000_000_000;
const NANOSECONDS_PER_100NS_TICK: i64 = 100;
/// SQL Server accepts datetimeoffset offsets from -14:00 through +14:00.
const SQL_SERVER_DATETIMEOFFSET_MAX_OFFSET_MINUTES: i16 = 14 * 60;

pub(super) fn mssql_datetime2_value(
    runtime_mapping: ArrowToMssqlRuntimeMapping<'_>,
    row_index: usize,
    cell: ArrowCell<'_>,
) -> Result<MssqlDateTime2> {
    let mapping = runtime_mapping.mapping();

    match (cell, mapping.arrow().data_type(), mapping.mssql().ty()) {
        (
            ArrowCell::Date64(value),
            DataType::Date64,
            MssqlType::DateTime2 {
                precision: SQL_SERVER_DATETIME2_DATE64_SCALE,
            },
        ) => mssql_datetime2_from_arrow_date64(mapping, row_index, value),
        (
            ArrowCell::TimestampSecond(value),
            DataType::Timestamp(TimeUnit::Second, timezone),
            MssqlType::DateTime2 {
                precision: SQL_SERVER_DATETIME2_TIMESTAMP_SCALE,
            },
        ) => {
            validate_timestamp_timezone_metadata(mapping, row_index, timezone.as_deref())?;
            mssql_datetime2_from_arrow_timestamp_second(mapping, row_index, value)
        }
        (
            ArrowCell::TimestampMillisecond(value),
            DataType::Timestamp(TimeUnit::Millisecond, timezone),
            MssqlType::DateTime2 {
                precision: SQL_SERVER_DATETIME2_TIMESTAMP_SCALE,
            },
        ) => {
            validate_timestamp_timezone_metadata(mapping, row_index, timezone.as_deref())?;
            mssql_datetime2_from_arrow_timestamp_millisecond(mapping, row_index, value)
        }
        (
            ArrowCell::TimestampMicrosecond(value),
            DataType::Timestamp(TimeUnit::Microsecond, timezone),
            MssqlType::DateTime2 {
                precision: SQL_SERVER_DATETIME2_TIMESTAMP_SCALE,
            },
        ) => {
            validate_timestamp_timezone_metadata(mapping, row_index, timezone.as_deref())?;
            mssql_datetime2_from_arrow_timestamp_microsecond(mapping, row_index, value)
        }
        (
            ArrowCell::TimestampNanosecond(value),
            DataType::Timestamp(TimeUnit::Nanosecond, timezone),
            MssqlType::DateTime2 {
                precision: SQL_SERVER_DATETIME2_TIMESTAMP_SCALE,
            },
        ) => {
            validate_timestamp_timezone_metadata(mapping, row_index, timezone.as_deref())?;
            mssql_datetime2_from_arrow_timestamp_nanosecond(
                mapping,
                row_index,
                value,
                runtime_mapping.nanosecond_policy(),
            )
        }
        other => Err(value_conversion_error(row_mapping_diagnostic(
            mapping,
            row_index,
            DiagnosticCode::ValueTypeMismatch,
            format!(
                "expected Arrow Date64 or timestamp payload planned as datetime2, got {other:?}"
            ),
        ))),
    }
}

pub(super) fn mssql_datetimeoffset_value(
    runtime_mapping: ArrowToMssqlRuntimeMapping<'_>,
    row_index: usize,
    cell: ArrowCell<'_>,
) -> Result<MssqlDateTimeOffset> {
    let mapping = runtime_mapping.mapping();

    match (cell, mapping.arrow().data_type(), mapping.mssql().ty()) {
        (
            ArrowCell::TimestampSecond(value),
            DataType::Timestamp(TimeUnit::Second, Some(timezone)),
            MssqlType::DateTimeOffset {
                precision: SQL_SERVER_DATETIME2_TIMESTAMP_SCALE,
            },
        ) => {
            let resolution = timezone_resolution_from_metadata(mapping, row_index, timezone)?;
            let offset_minutes = resolution.offset_for_instant(mapping, row_index, value, 0)?;
            let utc_ticks = i128::from(value) * TICKS_100NS_PER_SECOND;
            mssql_datetimeoffset_from_utc_100ns_ticks(
                mapping,
                row_index,
                utc_ticks,
                offset_minutes,
                "second",
                value,
            )
        }
        (
            ArrowCell::TimestampMillisecond(value),
            DataType::Timestamp(TimeUnit::Millisecond, Some(timezone)),
            MssqlType::DateTimeOffset {
                precision: SQL_SERVER_DATETIME2_TIMESTAMP_SCALE,
            },
        ) => {
            let (seconds, nanoseconds) = epoch_parts_from_milliseconds(mapping, row_index, value)?;
            let resolution = timezone_resolution_from_metadata(mapping, row_index, timezone)?;
            let offset_minutes =
                resolution.offset_for_instant(mapping, row_index, seconds, nanoseconds)?;
            let utc_ticks = i128::from(value) * TICKS_100NS_PER_MILLISECOND;
            mssql_datetimeoffset_from_utc_100ns_ticks(
                mapping,
                row_index,
                utc_ticks,
                offset_minutes,
                "millisecond",
                value,
            )
        }
        (
            ArrowCell::TimestampMicrosecond(value),
            DataType::Timestamp(TimeUnit::Microsecond, Some(timezone)),
            MssqlType::DateTimeOffset {
                precision: SQL_SERVER_DATETIME2_TIMESTAMP_SCALE,
            },
        ) => {
            let (seconds, nanoseconds) = epoch_parts_from_microseconds(mapping, row_index, value)?;
            let resolution = timezone_resolution_from_metadata(mapping, row_index, timezone)?;
            let offset_minutes =
                resolution.offset_for_instant(mapping, row_index, seconds, nanoseconds)?;
            let utc_ticks = i128::from(value) * TICKS_100NS_PER_MICROSECOND;
            mssql_datetimeoffset_from_utc_100ns_ticks(
                mapping,
                row_index,
                utc_ticks,
                offset_minutes,
                "microsecond",
                value,
            )
        }
        (
            ArrowCell::TimestampNanosecond(value),
            DataType::Timestamp(TimeUnit::Nanosecond, Some(timezone)),
            MssqlType::DateTimeOffset {
                precision: SQL_SERVER_DATETIME2_TIMESTAMP_SCALE,
            },
        ) => {
            let (seconds, nanoseconds) = epoch_parts_from_nanoseconds(mapping, row_index, value)?;
            let resolution = timezone_resolution_from_metadata(mapping, row_index, timezone)?;
            let offset_minutes =
                resolution.offset_for_instant(mapping, row_index, seconds, nanoseconds)?;
            let utc_ticks = nanoseconds_to_100ns_ticks(
                mapping,
                row_index,
                value,
                runtime_mapping.nanosecond_policy(),
            )?;
            mssql_datetimeoffset_from_utc_100ns_ticks(
                mapping,
                row_index,
                utc_ticks,
                offset_minutes,
                "nanosecond",
                value,
            )
        }
        other => Err(value_conversion_error(row_mapping_diagnostic(
            mapping,
            row_index,
            DiagnosticCode::ValueTypeMismatch,
            format!(
                "expected timezone-aware Arrow timestamp payload planned as datetimeoffset, got {other:?}"
            ),
        ))),
    }
}

pub(super) fn null_datetime2_cell<'a>(
    mapping: &SchemaMapping,
    row_index: usize,
) -> Result<MssqlCell<'a>> {
    if !supports_null_datetime2_cell(mapping) {
        return Err(unsupported_value_conversion(
            mapping,
            row_index,
            format!(
                "planned SQL Server type {} is not supported yet",
                mapping.mssql().ty().to_sql()
            ),
        ));
    }

    validate_null_timestamp_timezone_metadata(mapping, row_index)?;
    Ok(MssqlCell::DateTime2(None))
}

fn supports_null_datetime2_cell(mapping: &SchemaMapping) -> bool {
    matches!(
        (mapping.arrow().data_type(), mapping.mssql().ty()),
        (
            DataType::Date64,
            MssqlType::DateTime2 {
                precision: SQL_SERVER_DATETIME2_DATE64_SCALE,
            },
        ) | (
            DataType::Timestamp(
                TimeUnit::Second
                    | TimeUnit::Millisecond
                    | TimeUnit::Microsecond
                    | TimeUnit::Nanosecond,
                _,
            ),
            MssqlType::DateTime2 {
                precision: SQL_SERVER_DATETIME2_TIMESTAMP_SCALE,
            },
        )
    )
}

pub(super) fn null_datetimeoffset_cell<'a>(
    mapping: &SchemaMapping,
    row_index: usize,
) -> Result<MssqlCell<'a>> {
    if !supports_null_datetimeoffset_cell(mapping) {
        return Err(unsupported_value_conversion(
            mapping,
            row_index,
            format!(
                "planned SQL Server type {} is not supported yet",
                mapping.mssql().ty().to_sql()
            ),
        ));
    }

    validate_null_timestamp_timezone_metadata(mapping, row_index)?;
    Ok(MssqlCell::DateTimeOffset(None))
}

fn supports_null_datetimeoffset_cell(mapping: &SchemaMapping) -> bool {
    matches!(
        (mapping.arrow().data_type(), mapping.mssql().ty()),
        (
            DataType::Timestamp(
                TimeUnit::Second
                    | TimeUnit::Millisecond
                    | TimeUnit::Microsecond
                    | TimeUnit::Nanosecond,
                Some(_)
            ),
            MssqlType::DateTimeOffset {
                precision: SQL_SERVER_DATETIME2_TIMESTAMP_SCALE,
            }
        )
    )
}

fn validate_null_timestamp_timezone_metadata(
    mapping: &SchemaMapping,
    row_index: usize,
) -> Result<()> {
    if let DataType::Timestamp(_, timezone) = mapping.arrow().data_type() {
        validate_timestamp_timezone_metadata(mapping, row_index, timezone.as_deref())?;
    }

    Ok(())
}

fn validate_timestamp_timezone_metadata(
    mapping: &SchemaMapping,
    row_index: usize,
    timezone: Option<&str>,
) -> Result<()> {
    let Some(timezone) = timezone.filter(|timezone| !timezone.is_empty()) else {
        return Ok(());
    };

    timezone_resolution_from_metadata(mapping, row_index, timezone).map(|_| ())
}

/// Resolved timezone metadata for a planned Arrow timestamp column.
///
/// Arrow timestamp timezone metadata can contain either a fixed offset or a
/// timezone database name. Fixed offsets are row-independent, while named
/// timezones need the row timestamp instant to account for historical and DST
/// offset rules.
#[derive(Debug, Clone, Copy)]
pub(crate) enum TimezoneResolution {
    FixedOffset { offset_minutes: i16 },
    Named { timezone: Tz },
}

impl TimezoneResolution {
    /// Returns the SQL Server offset for one timestamp instant.
    pub(crate) fn offset_for_instant(
        self,
        mapping: &SchemaMapping,
        row_index: usize,
        seconds_from_unix_epoch: i64,
        nanoseconds: u32,
    ) -> Result<i16> {
        match self {
            Self::FixedOffset { offset_minutes } => Ok(offset_minutes),
            Self::Named { timezone } => {
                let datetime = timezone
                    .timestamp_opt(seconds_from_unix_epoch, nanoseconds)
                    .single()
                    .ok_or_else(|| {
                        timezone_instant_error(mapping, row_index, seconds_from_unix_epoch)
                    })?;
                let offset_seconds = datetime.offset().fix().local_minus_utc();
                sql_server_offset_minutes(mapping, row_index, offset_seconds)
            }
        }
    }
}

/// Resolves Arrow timestamp timezone metadata once for a planned column.
pub(crate) fn timezone_resolution_from_metadata(
    mapping: &SchemaMapping,
    row_index: usize,
    timezone: &str,
) -> Result<TimezoneResolution> {
    if timezone.eq_ignore_ascii_case("Z") || timezone.eq_ignore_ascii_case("UTC") {
        return Ok(TimezoneResolution::FixedOffset { offset_minutes: 0 });
    }

    if let Some(offset) = parse_sql_server_fixed_timezone_offset(mapping, row_index, timezone) {
        return offset.map(|offset_minutes| TimezoneResolution::FixedOffset { offset_minutes });
    }

    let timezone = timezone
        .parse::<Tz>()
        .map_err(|_| unsupported_timezone_error(mapping, row_index, timezone))?;

    Ok(TimezoneResolution::Named { timezone })
}

fn sql_server_offset_minutes(
    mapping: &SchemaMapping,
    row_index: usize,
    offset_seconds: i32,
) -> Result<i16> {
    if offset_seconds % 60 != 0 {
        return Err(unsupported_timezone_offset_error(
            mapping,
            row_index,
            offset_seconds,
        ));
    }

    let offset_minutes = i16::try_from(offset_seconds / 60)
        .map_err(|_| unsupported_timezone_offset_error(mapping, row_index, offset_seconds))?;

    if offset_minutes.unsigned_abs() > SQL_SERVER_DATETIMEOFFSET_MAX_OFFSET_MINUTES as u16 {
        return Err(unsupported_timezone_offset_error(
            mapping,
            row_index,
            offset_seconds,
        ));
    }

    Ok(offset_minutes)
}

fn parse_sql_server_fixed_timezone_offset(
    mapping: &SchemaMapping,
    row_index: usize,
    timezone: &str,
) -> Option<Result<i16>> {
    let timezone_bytes = timezone.as_bytes();
    if !matches!(timezone_bytes.first(), Some(b'+' | b'-')) {
        return None;
    }

    // Arrow accepts some offset spellings that SQL Server would not accept as
    // written, such as `+12:60`. Validate fixed offsets ourselves before
    // falling back to the Arrow timezone database parser for named zones.
    let digits = match timezone_bytes.len() {
        3 => [timezone_bytes[1], timezone_bytes[2], b'0', b'0'],
        5 => [
            timezone_bytes[1],
            timezone_bytes[2],
            timezone_bytes[3],
            timezone_bytes[4],
        ],
        6 if timezone_bytes[3] == b':' => [
            timezone_bytes[1],
            timezone_bytes[2],
            timezone_bytes[4],
            timezone_bytes[5],
        ],
        _ => {
            return Some(Err(unsupported_timezone_error(
                mapping, row_index, timezone,
            )));
        }
    };

    if digits.iter().any(|digit| !digit.is_ascii_digit()) {
        return Some(Err(unsupported_timezone_error(
            mapping, row_index, timezone,
        )));
    }

    let hours = i16::from((digits[0] - b'0') * 10 + (digits[1] - b'0'));
    let minutes = i16::from((digits[2] - b'0') * 10 + (digits[3] - b'0'));

    if minutes >= 60 {
        return Some(Err(unsupported_timezone_error(
            mapping, row_index, timezone,
        )));
    }

    let Some(total_minutes) = hours
        .checked_mul(60)
        .and_then(|value| value.checked_add(minutes))
    else {
        return Some(Err(unsupported_timezone_error(
            mapping, row_index, timezone,
        )));
    };

    if total_minutes > SQL_SERVER_DATETIMEOFFSET_MAX_OFFSET_MINUTES {
        return Some(Err(unsupported_timezone_error(
            mapping, row_index, timezone,
        )));
    }

    if timezone_bytes[0] == b'-' {
        Some(Ok(-total_minutes))
    } else {
        Some(Ok(total_minutes))
    }
}

fn validate_datetimeoffset_local_range(
    mapping: &SchemaMapping,
    row_index: usize,
    local_ticks_from_unix_epoch: i128,
    unit_name: &str,
    source_value: i64,
) -> Result<()> {
    mssql_datetime2_from_unix_epoch_100ns_ticks(
        mapping,
        row_index,
        local_ticks_from_unix_epoch,
        unit_name,
        source_value,
    )
    .map(|_| ())
}

fn mssql_datetimeoffset_from_utc_100ns_ticks(
    mapping: &SchemaMapping,
    row_index: usize,
    utc_ticks_from_unix_epoch: i128,
    offset_minutes: i16,
    unit_name: &str,
    source_value: i64,
) -> Result<MssqlDateTimeOffset> {
    let offset_ticks = i128::from(offset_minutes) * 60 * TICKS_100NS_PER_SECOND;
    let local_ticks = utc_ticks_from_unix_epoch
        .checked_add(offset_ticks)
        .ok_or_else(|| {
            value_conversion_error(row_mapping_diagnostic(
                mapping,
                row_index,
                DiagnosticCode::TimestampOutOfRange,
                format!(
                    "Arrow timestamp {unit_name} value {source_value} overflows while applying timezone offset {offset_minutes} minute(s)"
                ),
            ))
        })?;
    validate_datetimeoffset_local_range(mapping, row_index, local_ticks, unit_name, source_value)?;
    let utc_datetime2 = mssql_datetime2_from_unix_epoch_100ns_ticks(
        mapping,
        row_index,
        utc_ticks_from_unix_epoch,
        unit_name,
        source_value,
    )?;

    Ok(MssqlDateTimeOffset::new(utc_datetime2, offset_minutes))
}

fn epoch_parts_from_milliseconds(
    mapping: &SchemaMapping,
    row_index: usize,
    milliseconds_from_unix_epoch: i64,
) -> Result<(i64, u32)> {
    let seconds = milliseconds_from_unix_epoch.div_euclid(1_000);
    let nanoseconds = milliseconds_from_unix_epoch.rem_euclid(1_000) * 1_000_000;
    epoch_parts(mapping, row_index, seconds, nanoseconds)
}

fn epoch_parts_from_microseconds(
    mapping: &SchemaMapping,
    row_index: usize,
    microseconds_from_unix_epoch: i64,
) -> Result<(i64, u32)> {
    let seconds = microseconds_from_unix_epoch.div_euclid(1_000_000);
    let nanoseconds = microseconds_from_unix_epoch.rem_euclid(1_000_000) * 1_000;
    epoch_parts(mapping, row_index, seconds, nanoseconds)
}

fn epoch_parts_from_nanoseconds(
    mapping: &SchemaMapping,
    row_index: usize,
    nanoseconds_from_unix_epoch: i64,
) -> Result<(i64, u32)> {
    let seconds = nanoseconds_from_unix_epoch.div_euclid(1_000_000_000);
    let nanoseconds = nanoseconds_from_unix_epoch.rem_euclid(1_000_000_000);
    epoch_parts(mapping, row_index, seconds, nanoseconds)
}

fn epoch_parts(
    mapping: &SchemaMapping,
    row_index: usize,
    seconds_from_unix_epoch: i64,
    nanoseconds: i64,
) -> Result<(i64, u32)> {
    let nanoseconds = u32::try_from(nanoseconds).map_err(|_| {
        value_conversion_error(row_mapping_diagnostic(
            mapping,
            row_index,
            DiagnosticCode::TimestampOutOfRange,
            format!("timestamp nanosecond component {nanoseconds} is outside valid range"),
        ))
    })?;

    Ok((seconds_from_unix_epoch, nanoseconds))
}

fn unsupported_timezone_error(
    mapping: &SchemaMapping,
    row_index: usize,
    timezone: &str,
) -> crate::Error {
    value_conversion_error(row_mapping_diagnostic(
        mapping,
        row_index,
        DiagnosticCode::TimezoneUnsupported,
        format!(
            "Arrow timestamp timezone {timezone:?} is not a valid Arrow timezone name or fixed offset"
        ),
    ))
}

fn timezone_instant_error(
    mapping: &SchemaMapping,
    row_index: usize,
    seconds_from_unix_epoch: i64,
) -> crate::Error {
    value_conversion_error(row_mapping_diagnostic(
        mapping,
        row_index,
        DiagnosticCode::TimestampOutOfRange,
        format!(
            "Arrow timestamp second value {seconds_from_unix_epoch} cannot be represented in the planned timezone"
        ),
    ))
}

fn unsupported_timezone_offset_error(
    mapping: &SchemaMapping,
    row_index: usize,
    offset_seconds: i32,
) -> crate::Error {
    value_conversion_error(row_mapping_diagnostic(
        mapping,
        row_index,
        DiagnosticCode::TimezoneUnsupported,
        format!(
            "resolved timezone offset {offset_seconds} second(s) cannot be represented as a SQL Server datetimeoffset minute offset"
        ),
    ))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_schema::{DataType, Field, Schema, TimeUnit};

    use super::{
        MssqlCell, MssqlDateTime2, MssqlDateTimeOffset, timezone_resolution_from_metadata,
    };
    use crate::{
        DiagnosticCode, MssqlProfile, NanosecondPolicy, PlanOptions, SchemaMapping, TimezonePolicy,
        arrow::cell::ArrowCell,
        mssql::cell::from_arrow::{ArrowToMssqlRuntimeMapping, mssql_cell_from_arrow_cell},
        mssql::cell::{MssqlDate, MssqlTime},
        plan_arrow_schema_to_mssql_mappings,
    };

    #[test]
    fn converts_timezone_aware_timestamp_cells_to_normalized_utc_datetime2() {
        let options = PlanOptions {
            timezone_policy: TimezonePolicy::NormalizeUtcDateTime2,
            ..PlanOptions::default()
        };
        let mappings = mappings_for_schema_with_options(
            Schema::new(vec![
                Field::new(
                    "new_york",
                    DataType::Timestamp(TimeUnit::Second, Some("America/New_York".into())),
                    true,
                ),
                Field::new(
                    "offset",
                    DataType::Timestamp(TimeUnit::Millisecond, Some("+02:30".into())),
                    true,
                ),
                Field::new(
                    "utc",
                    DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
                    true,
                ),
            ]),
            options,
        );

        assert_eq!(
            convert_cell(&mappings[0], ArrowCell::TimestampSecond(0), 0).unwrap(),
            MssqlCell::DateTime2(Some(MssqlDateTime2::new(
                MssqlDate::new(719_162),
                MssqlTime::new(0, 7),
            )))
        );
        assert_eq!(
            convert_cell(&mappings[0], ArrowCell::Null, 1).unwrap(),
            MssqlCell::DateTime2(None)
        );
        assert_eq!(
            convert_cell(&mappings[1], ArrowCell::TimestampMillisecond(0), 0).unwrap(),
            MssqlCell::DateTime2(Some(MssqlDateTime2::new(
                MssqlDate::new(719_162),
                MssqlTime::new(0, 7),
            )))
        );
        assert_eq!(
            convert_cell(&mappings[1], ArrowCell::Null, 1).unwrap(),
            MssqlCell::DateTime2(None)
        );
        assert_eq!(
            convert_cell(&mappings[2], ArrowCell::TimestampMicrosecond(1_234_567), 0).unwrap(),
            MssqlCell::DateTime2(Some(MssqlDateTime2::new(
                MssqlDate::new(719_162),
                MssqlTime::new(12_345_670, 7),
            )))
        );
        assert_eq!(
            convert_cell(&mappings[2], ArrowCell::Null, 1).unwrap(),
            MssqlCell::DateTime2(None)
        );
    }

    #[test]
    fn rejects_invalid_timezone_metadata_for_normalized_utc_datetime2() {
        let options = PlanOptions {
            timezone_policy: TimezonePolicy::NormalizeUtcDateTime2,
            ..PlanOptions::default()
        };
        let mappings = mappings_for_schema_with_options(
            Schema::new(vec![Field::new(
                "ts",
                DataType::Timestamp(TimeUnit::Second, Some("Foobar".into())),
                false,
            )]),
            options,
        );

        let err = convert_cell(&mappings[0], ArrowCell::TimestampSecond(0), 0).unwrap_err();

        assert_single_diagnostic(
            err,
            DiagnosticCode::TimezoneUnsupported,
            Some(0),
            Some((0, "ts")),
        );
    }

    #[test]
    fn rejects_invalid_timezone_metadata_for_null_normalized_utc_datetime2() {
        let options = PlanOptions {
            timezone_policy: TimezonePolicy::NormalizeUtcDateTime2,
            ..PlanOptions::default()
        };
        let mappings = mappings_for_schema_with_options(
            Schema::new(vec![Field::new(
                "ts",
                DataType::Timestamp(TimeUnit::Second, Some("Foobar".into())),
                true,
            )]),
            options,
        );

        let err = convert_cell(&mappings[0], ArrowCell::Null, 0).unwrap_err();

        assert_single_diagnostic(
            err,
            DiagnosticCode::TimezoneUnsupported,
            Some(0),
            Some((0, "ts")),
        );
    }

    #[test]
    fn applies_nanosecond_policy_to_timezone_aware_normalized_utc_datetime2() {
        let options = PlanOptions {
            timezone_policy: TimezonePolicy::NormalizeUtcDateTime2,
            nanosecond_policy: NanosecondPolicy::RoundTo100ns,
            ..PlanOptions::default()
        };
        let mappings = mappings_for_schema_with_options(
            Schema::new(vec![Field::new(
                "ts_ns",
                DataType::Timestamp(TimeUnit::Nanosecond, Some("America/New_York".into())),
                false,
            )]),
            options,
        );

        assert_eq!(
            convert_cell_with_options(
                &mappings[0],
                ArrowCell::TimestampNanosecond(150),
                0,
                &options,
            )
            .unwrap(),
            MssqlCell::DateTime2(Some(MssqlDateTime2::new(
                MssqlDate::new(719_162),
                MssqlTime::new(2, 7),
            )))
        );
    }

    #[test]
    fn rejects_timezone_aware_normalized_utc_values_outside_datetime2_range() {
        let options = PlanOptions {
            timezone_policy: TimezonePolicy::NormalizeUtcDateTime2,
            ..PlanOptions::default()
        };
        let mappings = mappings_for_schema_with_options(
            Schema::new(vec![Field::new(
                "ts_s",
                DataType::Timestamp(TimeUnit::Second, Some("America/New_York".into())),
                false,
            )]),
            options,
        );

        let below =
            convert_cell(&mappings[0], ArrowCell::TimestampSecond(i64::MIN), 0).unwrap_err();
        assert_single_diagnostic(
            below,
            DiagnosticCode::TimestampOutOfRange,
            Some(0),
            Some((0, "ts_s")),
        );

        let above =
            convert_cell(&mappings[0], ArrowCell::TimestampSecond(i64::MAX), 1).unwrap_err();
        assert_single_diagnostic(
            above,
            DiagnosticCode::TimestampOutOfRange,
            Some(1),
            Some((0, "ts_s")),
        );
    }

    #[test]
    fn converts_timezone_aware_timestamp_cells_to_datetimeoffset() {
        let options = PlanOptions {
            timezone_policy: TimezonePolicy::DateTimeOffset,
            ..PlanOptions::default()
        };
        let mappings = mappings_for_schema_with_options(
            Schema::new(vec![
                Field::new(
                    "fixed_positive",
                    DataType::Timestamp(TimeUnit::Millisecond, Some("+02:30".into())),
                    true,
                ),
                Field::new(
                    "fixed_negative",
                    DataType::Timestamp(TimeUnit::Nanosecond, Some("-07".into())),
                    true,
                ),
                Field::new(
                    "utc",
                    DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
                    true,
                ),
            ]),
            options,
        );

        assert_eq!(
            convert_cell(&mappings[0], ArrowCell::TimestampMillisecond(0), 0).unwrap(),
            MssqlCell::DateTimeOffset(Some(MssqlDateTimeOffset::new(
                MssqlDateTime2::new(MssqlDate::new(719_162), MssqlTime::new(0, 7)),
                150,
            )))
        );
        assert_eq!(
            convert_cell(&mappings[0], ArrowCell::Null, 1).unwrap(),
            MssqlCell::DateTimeOffset(None)
        );
        assert_eq!(
            convert_cell(&mappings[1], ArrowCell::TimestampNanosecond(0), 0).unwrap(),
            MssqlCell::DateTimeOffset(Some(MssqlDateTimeOffset::new(
                MssqlDateTime2::new(MssqlDate::new(719_162), MssqlTime::new(0, 7)),
                -420,
            )))
        );
        assert_eq!(
            convert_cell(&mappings[1], ArrowCell::Null, 1).unwrap(),
            MssqlCell::DateTimeOffset(None)
        );
        assert_eq!(
            convert_cell(&mappings[2], ArrowCell::TimestampMicrosecond(1_234_567), 0).unwrap(),
            MssqlCell::DateTimeOffset(Some(MssqlDateTimeOffset::new(
                MssqlDateTime2::new(MssqlDate::new(719_162), MssqlTime::new(12_345_670, 7)),
                0,
            )))
        );
    }

    #[test]
    fn resolves_named_timezone_datetimeoffset_per_timestamp_instant() {
        let options = PlanOptions {
            timezone_policy: TimezonePolicy::DateTimeOffset,
            ..PlanOptions::default()
        };
        let mappings = mappings_for_schema_with_options(
            Schema::new(vec![Field::new(
                "new_york",
                DataType::Timestamp(TimeUnit::Second, Some("America/New_York".into())),
                false,
            )]),
            options,
        );

        assert_eq!(
            convert_cell(&mappings[0], ArrowCell::TimestampSecond(1_738_411_200), 0).unwrap(),
            MssqlCell::DateTimeOffset(Some(MssqlDateTimeOffset::new(
                MssqlDateTime2::new(MssqlDate::new(739_282), MssqlTime::new(432_000_000_000, 7)),
                -300,
            )))
        );
        assert_eq!(
            convert_cell(&mappings[0], ArrowCell::TimestampSecond(1_750_593_600), 1).unwrap(),
            MssqlCell::DateTimeOffset(Some(MssqlDateTimeOffset::new(
                MssqlDateTime2::new(MssqlDate::new(739_423), MssqlTime::new(432_000_000_000, 7)),
                -240,
            )))
        );
    }

    #[test]
    fn rejects_invalid_timezone_metadata_for_datetimeoffset() {
        let options = PlanOptions {
            timezone_policy: TimezonePolicy::DateTimeOffset,
            ..PlanOptions::default()
        };
        let mappings = mappings_for_schema_with_options(
            Schema::new(vec![Field::new(
                "ts",
                DataType::Timestamp(TimeUnit::Second, Some("Foobar".into())),
                false,
            )]),
            options,
        );

        let err = convert_cell(&mappings[0], ArrowCell::TimestampSecond(0), 0).unwrap_err();

        assert_single_diagnostic(
            err,
            DiagnosticCode::TimezoneUnsupported,
            Some(0),
            Some((0, "ts")),
        );
    }

    #[test]
    fn rejects_invalid_timezone_metadata_for_null_datetimeoffset() {
        let options = PlanOptions {
            timezone_policy: TimezonePolicy::DateTimeOffset,
            ..PlanOptions::default()
        };
        let mappings = mappings_for_schema_with_options(
            Schema::new(vec![Field::new(
                "ts",
                DataType::Timestamp(TimeUnit::Second, Some("Foobar".into())),
                true,
            )]),
            options,
        );

        let err = convert_cell(&mappings[0], ArrowCell::Null, 0).unwrap_err();

        assert_single_diagnostic(
            err,
            DiagnosticCode::TimezoneUnsupported,
            Some(0),
            Some((0, "ts")),
        );
    }

    #[test]
    fn applies_nanosecond_policy_to_datetimeoffset() {
        let options = PlanOptions {
            timezone_policy: TimezonePolicy::DateTimeOffset,
            nanosecond_policy: NanosecondPolicy::RoundTo100ns,
            ..PlanOptions::default()
        };
        let mappings = mappings_for_schema_with_options(
            Schema::new(vec![Field::new(
                "ts_ns",
                DataType::Timestamp(TimeUnit::Nanosecond, Some("+00:00".into())),
                false,
            )]),
            options,
        );

        assert_eq!(
            convert_cell_with_options(
                &mappings[0],
                ArrowCell::TimestampNanosecond(150),
                0,
                &options,
            )
            .unwrap(),
            MssqlCell::DateTimeOffset(Some(MssqlDateTimeOffset::new(
                MssqlDateTime2::new(MssqlDate::new(719_162), MssqlTime::new(2, 7)),
                0,
            )))
        );
    }

    #[test]
    fn rejects_datetimeoffset_values_outside_local_sql_server_range_after_offset() {
        let options = PlanOptions {
            timezone_policy: TimezonePolicy::DateTimeOffset,
            ..PlanOptions::default()
        };
        let mappings = mappings_for_schema_with_options(
            Schema::new(vec![
                Field::new(
                    "too_early",
                    DataType::Timestamp(TimeUnit::Second, Some("-14:00".into())),
                    false,
                ),
                Field::new(
                    "too_late",
                    DataType::Timestamp(TimeUnit::Second, Some("+14:00".into())),
                    false,
                ),
            ]),
            options,
        );

        let below =
            convert_cell(&mappings[0], ArrowCell::TimestampSecond(-62_135_596_800), 0).unwrap_err();
        assert_single_diagnostic(
            below,
            DiagnosticCode::TimestampOutOfRange,
            Some(0),
            Some((0, "too_early")),
        );

        let above =
            convert_cell(&mappings[1], ArrowCell::TimestampSecond(253_402_300_799), 0).unwrap_err();
        assert_single_diagnostic(
            above,
            DiagnosticCode::TimestampOutOfRange,
            Some(0),
            Some((1, "too_late")),
        );
    }

    #[test]
    fn resolves_fixed_timezone_offsets_for_datetimeoffset() {
        let mapping = timezone_timestamp_mapping("+00:00", TimezonePolicy::DateTimeOffset);

        for (timezone, expected_minutes) in [
            ("UTC", 0),
            ("+00:00", 0),
            ("-00:00", 0),
            ("+02:30", 150),
            ("+0230", 150),
            ("-07", -420),
            ("-07:45", -465),
            ("+14:00", 840),
            ("-14:00", -840),
        ] {
            let resolution = timezone_resolution_from_metadata(&mapping, 7, timezone).unwrap();

            assert_eq!(
                resolution.offset_for_instant(&mapping, 7, 0, 0).unwrap(),
                expected_minutes
            );
            assert_eq!(
                resolution
                    .offset_for_instant(&mapping, 7, 1_750_594_400, 0)
                    .unwrap(),
                expected_minutes
            );
        }
    }

    #[test]
    fn resolves_named_timezone_offsets_for_each_instant() {
        let mapping =
            timezone_timestamp_mapping("America/New_York", TimezonePolicy::DateTimeOffset);
        let resolution =
            timezone_resolution_from_metadata(&mapping, 0, "America/New_York").unwrap();

        let winter_epoch = 1_738_411_200;
        let summer_epoch = 1_750_594_400;

        assert_eq!(
            resolution
                .offset_for_instant(&mapping, 0, winter_epoch, 0)
                .unwrap(),
            -300
        );
        assert_eq!(
            resolution
                .offset_for_instant(&mapping, 1, summer_epoch, 0)
                .unwrap(),
            -240
        );
    }

    #[test]
    fn rejects_invalid_timezone_names_and_unrepresentable_offsets() {
        let mapping = timezone_timestamp_mapping("+00:00", TimezonePolicy::DateTimeOffset);

        for timezone in ["", " ", "Foobar", "+1:00", "+ab:cd", "+02:3x", "+12:60"] {
            let err = timezone_resolution_from_metadata(&mapping, 7, timezone).unwrap_err();
            assert_single_diagnostic(
                err,
                DiagnosticCode::TimezoneUnsupported,
                Some(7),
                Some((0, "ts")),
            );
        }

        let err = timezone_resolution_from_metadata(&mapping, 7, "+14:01").unwrap_err();
        assert_single_diagnostic(
            err,
            DiagnosticCode::TimezoneUnsupported,
            Some(7),
            Some((0, "ts")),
        );
    }

    fn convert_cell<'a>(
        mapping: &SchemaMapping,
        cell: ArrowCell<'a>,
        row_index: usize,
    ) -> crate::Result<MssqlCell<'a>> {
        let options = PlanOptions::default();
        convert_cell_with_options(mapping, cell, row_index, &options)
    }

    fn convert_cell_with_options<'a>(
        mapping: &SchemaMapping,
        cell: ArrowCell<'a>,
        row_index: usize,
        options: &PlanOptions,
    ) -> crate::Result<MssqlCell<'a>> {
        let runtime_mapping = ArrowToMssqlRuntimeMapping::new(mapping, options);
        mssql_cell_from_arrow_cell(runtime_mapping, cell, row_index)
    }

    fn mappings_for_schema_with_options(
        schema: Schema,
        options: PlanOptions,
    ) -> Vec<SchemaMapping> {
        plan_arrow_schema_to_mssql_mappings(
            Arc::new(schema),
            MssqlProfile::sql_server_2016_compat_100(),
            options,
        )
        .unwrap()
        .into_parts()
        .0
    }

    fn timezone_timestamp_mapping(
        timezone: &str,
        timezone_policy: TimezonePolicy,
    ) -> SchemaMapping {
        mappings_for_schema_with_options(
            Schema::new(vec![Field::new(
                "ts",
                DataType::Timestamp(TimeUnit::Second, Some(timezone.into())),
                true,
            )]),
            PlanOptions {
                timezone_policy,
                ..PlanOptions::default()
            },
        )
        .remove(0)
    }

    fn assert_single_diagnostic(
        err: crate::Error,
        expected_code: DiagnosticCode,
        expected_row: Option<usize>,
        expected_field: Option<(usize, &str)>,
    ) {
        let crate::Error::ValueConversion { diagnostics } = err else {
            panic!("expected value conversion error");
        };

        assert_eq!(diagnostics.len(), 1);
        let diagnostic = &diagnostics.all()[0];
        assert_eq!(diagnostic.code(), expected_code);
        assert_eq!(diagnostic.row(), expected_row);
        assert_eq!(
            diagnostic
                .field()
                .map(|field| (field.index(), field.name())),
            expected_field
        );
    }
}

//! Arrow timestamp timezone metadata resolution.

use arrow_array::timezone::Tz;
use chrono::{Offset, TimeZone};

use crate::{DiagnosticCode, Result, SchemaMapping};

use super::{row_mapping_diagnostic, value_conversion_error};

/// SQL Server accepts datetimeoffset offsets from -14:00 through +14:00.
const SQL_SERVER_DATETIMEOFFSET_MAX_OFFSET_MINUTES: i16 = 14 * 60;

/// Resolved timezone metadata for a planned Arrow timestamp column.
///
/// Arrow timestamp timezone metadata can contain either a fixed offset or a
/// timezone database name. Fixed offsets are row-independent, while named
/// timezones need the row timestamp instant to account for historical and DST
/// offset rules.
#[derive(Debug, Clone, Copy)]
pub(super) enum TimezoneResolution {
    FixedOffset { offset_minutes: i16 },
    Named { timezone: Tz },
}

impl TimezoneResolution {
    /// Returns the SQL Server offset for one timestamp instant.
    pub(super) fn offset_for_instant(
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
pub(super) fn timezone_resolution_from_metadata(
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

    use super::timezone_resolution_from_metadata;
    use crate::{
        DiagnosticCode, MssqlProfile, PlanOptions, SchemaMapping, TimezonePolicy,
        plan_arrow_schema_to_mssql_mappings,
    };

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

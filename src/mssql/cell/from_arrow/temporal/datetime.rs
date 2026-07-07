//! Timestamp Arrow-to-MSSQL datetime runtime cell conversion.

use crate::mssql::{cell::MssqlDateTime, profile::DateTimeRounding};
use crate::{DiagnosticCode, NanosecondPolicy, Result, SchemaMapping};

use super::datetime2::nanoseconds_to_100ns_ticks;
use super::{
    TICKS_100NS_PER_DAY, TICKS_100NS_PER_MICROSECOND, TICKS_100NS_PER_MILLISECOND,
    TICKS_100NS_PER_SECOND, row_mapping_diagnostic, value_conversion_error,
};

const SQL_SERVER_DATETIME_DAYS_FROM_1900_TO_UNIX_EPOCH: i128 = 25_567;
const SQL_SERVER_DATETIME_MIN_DAYS: i128 = -53_690;
const SQL_SERVER_DATETIME_MAX_DAYS: i128 = 2_958_463;
const SQL_SERVER_DATETIME_FRAGMENTS_PER_SECOND: i128 = 300;
const SQL_SERVER_DATETIME_FRAGMENTS_PER_DAY: i128 =
    86_400 * SQL_SERVER_DATETIME_FRAGMENTS_PER_SECOND;
const SQL_SERVER_DATETIME_MIN_UNIX_EPOCH_100NS_TICKS: i128 = (SQL_SERVER_DATETIME_MIN_DAYS
    - SQL_SERVER_DATETIME_DAYS_FROM_1900_TO_UNIX_EPOCH)
    * TICKS_100NS_PER_DAY;

/// Converts an Arrow second timestamp to SQL Server `datetime`.
pub(crate) fn mssql_datetime_from_arrow_timestamp_second(
    mapping: &SchemaMapping,
    row_index: usize,
    seconds_from_unix_epoch: i64,
    rounding: DateTimeRounding,
) -> Result<MssqlDateTime> {
    let ticks = i128::from(seconds_from_unix_epoch) * TICKS_100NS_PER_SECOND;
    mssql_datetime_from_unix_epoch_100ns_ticks(
        mapping,
        row_index,
        ticks,
        "second",
        seconds_from_unix_epoch,
        rounding,
    )
}

/// Converts an Arrow millisecond timestamp to SQL Server `datetime`.
pub(crate) fn mssql_datetime_from_arrow_timestamp_millisecond(
    mapping: &SchemaMapping,
    row_index: usize,
    milliseconds_from_unix_epoch: i64,
    rounding: DateTimeRounding,
) -> Result<MssqlDateTime> {
    let ticks = i128::from(milliseconds_from_unix_epoch) * TICKS_100NS_PER_MILLISECOND;
    mssql_datetime_from_unix_epoch_100ns_ticks(
        mapping,
        row_index,
        ticks,
        "millisecond",
        milliseconds_from_unix_epoch,
        rounding,
    )
}

/// Converts an Arrow microsecond timestamp to SQL Server `datetime`.
pub(crate) fn mssql_datetime_from_arrow_timestamp_microsecond(
    mapping: &SchemaMapping,
    row_index: usize,
    microseconds_from_unix_epoch: i64,
    rounding: DateTimeRounding,
) -> Result<MssqlDateTime> {
    let ticks = i128::from(microseconds_from_unix_epoch) * TICKS_100NS_PER_MICROSECOND;
    mssql_datetime_from_unix_epoch_100ns_ticks(
        mapping,
        row_index,
        ticks,
        "microsecond",
        microseconds_from_unix_epoch,
        rounding,
    )
}

/// Converts an Arrow nanosecond timestamp to SQL Server `datetime`.
///
/// Nanoseconds are first normalized according to the runtime nanosecond policy,
/// then rounded to SQL Server `datetime` fragments according to the selected
/// profile behavior.
pub(crate) fn mssql_datetime_from_arrow_timestamp_nanosecond(
    mapping: &SchemaMapping,
    row_index: usize,
    nanoseconds_from_unix_epoch: i64,
    policy: NanosecondPolicy,
    rounding: DateTimeRounding,
) -> Result<MssqlDateTime> {
    let ticks =
        nanoseconds_to_100ns_ticks(mapping, row_index, nanoseconds_from_unix_epoch, policy)?;
    mssql_datetime_from_unix_epoch_100ns_ticks(
        mapping,
        row_index,
        ticks,
        "nanosecond",
        nanoseconds_from_unix_epoch,
        rounding,
    )
}

/// Converts Unix-epoch 100ns ticks to SQL Server `datetime`.
///
/// The caller supplies the source unit name and value so range errors can point
/// back to the original Arrow timestamp payload.
pub(crate) fn mssql_datetime_from_unix_epoch_100ns_ticks(
    mapping: &SchemaMapping,
    row_index: usize,
    ticks_from_unix_epoch: i128,
    unit_name: &str,
    source_value: i64,
    rounding: DateTimeRounding,
) -> Result<MssqlDateTime> {
    if ticks_from_unix_epoch < SQL_SERVER_DATETIME_MIN_UNIX_EPOCH_100NS_TICKS {
        return Err(timestamp_out_of_datetime_range(
            mapping,
            row_index,
            unit_name,
            source_value,
        ));
    }

    let fragments = round_100ns_ticks_to_datetime_fragments(
        mapping,
        row_index,
        ticks_from_unix_epoch,
        rounding,
    )?;
    let days_from_unix_epoch = fragments.div_euclid(SQL_SERVER_DATETIME_FRAGMENTS_PER_DAY);
    let seconds_fragments = fragments.rem_euclid(SQL_SERVER_DATETIME_FRAGMENTS_PER_DAY);
    let days = days_from_unix_epoch + SQL_SERVER_DATETIME_DAYS_FROM_1900_TO_UNIX_EPOCH;

    if !(SQL_SERVER_DATETIME_MIN_DAYS..=SQL_SERVER_DATETIME_MAX_DAYS).contains(&days) {
        return Err(timestamp_out_of_datetime_range(
            mapping,
            row_index,
            unit_name,
            source_value,
        ));
    }

    let days = i32::try_from(days).map_err(|_| {
        value_conversion_error(row_mapping_diagnostic(
            mapping,
            row_index,
            DiagnosticCode::TimestampOutOfRange,
            format!(
                "Arrow timestamp {unit_name} value {source_value} has an invalid SQL Server datetime date component"
            ),
        ))
    })?;
    let seconds_fragments = u32::try_from(seconds_fragments).map_err(|_| {
        value_conversion_error(row_mapping_diagnostic(
            mapping,
            row_index,
            DiagnosticCode::TimestampOutOfRange,
            format!(
                "Arrow timestamp {unit_name} value {source_value} has an invalid SQL Server datetime time component"
            ),
        ))
    })?;

    Ok(MssqlDateTime::new(days, seconds_fragments))
}

fn timestamp_out_of_datetime_range(
    mapping: &SchemaMapping,
    row_index: usize,
    unit_name: &str,
    source_value: i64,
) -> crate::Error {
    value_conversion_error(row_mapping_diagnostic(
        mapping,
        row_index,
        DiagnosticCode::TimestampOutOfRange,
        format!(
            "Arrow timestamp {unit_name} value {source_value} is outside SQL Server datetime range"
        ),
    ))
}

/// Selects the SQL Server `datetime` fragment-rounding rule for a profile.
///
/// The returned value is a fragment count measured from the Unix epoch, not a
/// time-of-day fragment count. The caller splits it into date and time
/// components after range validation, so rounding can safely carry across day
/// boundaries.
///
/// Compatibility levels before 130 emulate SQL Server's legacy high-precision
/// temporal cast path: round the source instant to whole milliseconds first,
/// then round that millisecond value to `datetime` fragments. Compatibility
/// level 130 and later skip the millisecond step and round the original 100ns
/// ticks directly to the nearest `datetime` fragment.
///
/// The legacy path can produce a larger displayed `datetime` tick, but that is
/// not higher precision. For `.684582`, legacy rounds to `.685` first and then
/// stores the `.687` fragment; compat 130+ rounds the original `.684582`
/// directly and stores the closer `.683` fragment.
fn round_100ns_ticks_to_datetime_fragments(
    mapping: &SchemaMapping,
    row_index: usize,
    ticks: i128,
    rounding: DateTimeRounding,
) -> Result<i128> {
    match rounding {
        DateTimeRounding::LegacyPre130 => {
            let ticks = round_100ns_ticks_to_nearest_millisecond(mapping, row_index, ticks)?;
            round_100ns_ticks_to_nearest_datetime_fragment(mapping, row_index, ticks)
        }
        DateTimeRounding::Compat130Plus => {
            round_100ns_ticks_to_nearest_datetime_fragment(mapping, row_index, ticks)
        }
    }
}

/// Rounds Unix-epoch 100ns ticks to the nearest whole millisecond.
///
/// This is the legacy pre-130 compatibility step for high-precision temporal
/// casts to `datetime`. SQL Server `datetime` ultimately stores 1/300-second
/// fragments, but older compatibility levels first lose precision to
/// milliseconds. That intermediate step is what makes `.684582` behave like
/// `.685`, which then ties upward to the `.687` `datetime` fragment.
/// Compatibility level 130+ avoids this intermediate precision loss.
///
/// Euclidean division keeps the remainder non-negative for negative timestamps,
/// so the same half-millisecond threshold applies consistently across the Unix
/// epoch and day-boundary rollovers.
fn round_100ns_ticks_to_nearest_millisecond(
    mapping: &SchemaMapping,
    row_index: usize,
    ticks: i128,
) -> Result<i128> {
    let base = ticks.div_euclid(TICKS_100NS_PER_MILLISECOND);
    let remainder = ticks.rem_euclid(TICKS_100NS_PER_MILLISECOND);
    let milliseconds = if remainder * 2 >= TICKS_100NS_PER_MILLISECOND {
        base.checked_add(1).ok_or_else(|| {
            value_conversion_error(row_mapping_diagnostic(
                mapping,
                row_index,
                DiagnosticCode::TimestampOutOfRange,
                "Arrow timestamp overflows while rounding to millisecond precision",
            ))
        })?
    } else {
        base
    };

    milliseconds
        .checked_mul(TICKS_100NS_PER_MILLISECOND)
        .ok_or_else(|| {
            value_conversion_error(row_mapping_diagnostic(
                mapping,
                row_index,
                DiagnosticCode::TimestampOutOfRange,
                "Arrow timestamp overflows while rounding to millisecond precision",
            ))
        })
}

/// Rounds Unix-epoch 100ns ticks to SQL Server `datetime` 1/300-second fragments.
///
/// SQL Server `datetime` stores the time component as 300 fragments per second.
/// This helper performs the final nearest-fragment conversion using integer
/// arithmetic so fractional boundaries and large values do not depend on
/// floating-point precision.
fn round_100ns_ticks_to_nearest_datetime_fragment(
    mapping: &SchemaMapping,
    row_index: usize,
    ticks: i128,
) -> Result<i128> {
    let numerator = ticks
        .checked_mul(SQL_SERVER_DATETIME_FRAGMENTS_PER_SECOND)
        .ok_or_else(|| {
            value_conversion_error(row_mapping_diagnostic(
                mapping,
                row_index,
                DiagnosticCode::TimestampOutOfRange,
                "Arrow timestamp overflows while converting to SQL Server datetime",
            ))
        })?;
    let base = numerator.div_euclid(TICKS_100NS_PER_SECOND);
    let remainder = numerator.rem_euclid(TICKS_100NS_PER_SECOND);

    if remainder * 2 >= TICKS_100NS_PER_SECOND {
        base.checked_add(1).ok_or_else(|| {
            value_conversion_error(row_mapping_diagnostic(
                mapping,
                row_index,
                DiagnosticCode::TimestampOutOfRange,
                "Arrow timestamp overflows while rounding to SQL Server datetime",
            ))
        })
    } else {
        Ok(base)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_schema::{DataType, Field, Schema, TimeUnit};

    use super::super::super::{ArrowToMssqlRuntimeMapping, mssql_cell_from_arrow_cell};
    use crate::{
        DiagnosticCode, MssqlProfile, NanosecondPolicy, PlanOptions, SchemaMapping,
        TimestampPolicy, TimezonePolicy,
        arrow::cell::ArrowCell,
        mssql::cell::{MssqlCell, MssqlDateTime},
        mssql::profile::DateTimeRounding,
        plan_arrow_schema_to_mssql_mappings,
        write::context::RuntimeConversionContext,
    };

    #[test]
    fn converts_timezone_free_timestamp_cells_to_datetime_with_rounding_and_nulls() {
        let options = PlanOptions {
            timestamp_policy: TimestampPolicy::DateTime,
            ..PlanOptions::default()
        };
        let mappings = mappings_for_schema_with_options(
            Schema::new(vec![
                Field::new("ts_s", DataType::Timestamp(TimeUnit::Second, None), true),
                Field::new(
                    "ts_ms",
                    DataType::Timestamp(TimeUnit::Millisecond, None),
                    true,
                ),
                Field::new(
                    "ts_us",
                    DataType::Timestamp(TimeUnit::Microsecond, None),
                    true,
                ),
            ]),
            options,
        );
        let cases = [
            (
                0,
                ArrowCell::TimestampSecond(0),
                MssqlCell::DateTime(Some(MssqlDateTime::new(25_567, 0))),
            ),
            (
                0,
                ArrowCell::TimestampSecond(-1),
                MssqlCell::DateTime(Some(MssqlDateTime::new(25_566, 25_919_700))),
            ),
            (
                1,
                ArrowCell::TimestampMillisecond(1),
                MssqlCell::DateTime(Some(MssqlDateTime::new(25_567, 0))),
            ),
            (
                2,
                ArrowCell::TimestampMicrosecond(1_700),
                MssqlCell::DateTime(Some(MssqlDateTime::new(25_567, 1))),
            ),
            (
                2,
                ArrowCell::TimestampMicrosecond(86_399_999_000),
                MssqlCell::DateTime(Some(MssqlDateTime::new(25_568, 0))),
            ),
            (
                2,
                ArrowCell::TimestampMicrosecond(-6_847_804_800_000_000),
                MssqlCell::DateTime(Some(MssqlDateTime::new(-53_690, 0))),
            ),
            (2, ArrowCell::Null, MssqlCell::DateTime(None)),
        ];

        for (mapping_index, cell, expected) in cases {
            assert_eq!(
                convert_cell_with_options(&mappings[mapping_index], cell, mapping_index, &options)
                    .unwrap(),
                expected
            );
        }
    }

    #[test]
    fn converts_timezone_aware_normalized_timestamp_cells_to_datetime() {
        let options = PlanOptions {
            timezone_policy: TimezonePolicy::NormalizeUtcDateTime2,
            timestamp_policy: TimestampPolicy::DateTime,
            ..PlanOptions::default()
        };
        let mappings = mappings_for_schema_with_options(
            Schema::new(vec![Field::new(
                "ts",
                DataType::Timestamp(TimeUnit::Millisecond, Some("+02:30".into())),
                true,
            )]),
            options,
        );

        assert_eq!(
            convert_cell_with_options(
                &mappings[0],
                ArrowCell::TimestampMillisecond(12_345),
                0,
                &options,
            )
            .unwrap(),
            MssqlCell::DateTime(Some(MssqlDateTime::new(25_567, 3_704)))
        );
        assert_eq!(
            convert_cell_with_options(&mappings[0], ArrowCell::Null, 1, &options).unwrap(),
            MssqlCell::DateTime(None)
        );
    }

    #[test]
    fn applies_nanosecond_policy_before_datetime_rounding() {
        let options = PlanOptions {
            timestamp_policy: TimestampPolicy::DateTime,
            nanosecond_policy: NanosecondPolicy::RoundTo100ns,
            ..PlanOptions::default()
        };
        let mappings = mappings_for_schema_with_options(
            Schema::new(vec![Field::new(
                "ts",
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                false,
            )]),
            options,
        );

        assert_eq!(
            convert_cell_with_options(
                &mappings[0],
                ArrowCell::TimestampNanosecond(150),
                0,
                &options
            )
            .unwrap(),
            MssqlCell::DateTime(Some(MssqlDateTime::new(25_567, 0)))
        );

        let reject_options = PlanOptions {
            timestamp_policy: TimestampPolicy::DateTime,
            ..PlanOptions::default()
        };
        let reject_mappings = mappings_for_schema_with_options(
            Schema::new(vec![Field::new(
                "ts",
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                false,
            )]),
            reject_options,
        );
        let err = convert_cell_with_options(
            &reject_mappings[0],
            ArrowCell::TimestampNanosecond(150),
            0,
            &reject_options,
        )
        .unwrap_err();

        assert_single_diagnostic(
            err,
            DiagnosticCode::LossyConversionRequiresPolicy,
            Some(0),
            Some((0, "ts")),
        );
    }

    #[test]
    fn selects_datetime_rounding_by_profile_compatibility_level() {
        let options = PlanOptions {
            timestamp_policy: TimestampPolicy::DateTime,
            ..PlanOptions::default()
        };
        let mappings = mappings_for_schema_with_options(
            Schema::new(vec![Field::new(
                "ts",
                DataType::Timestamp(TimeUnit::Microsecond, None),
                false,
            )]),
            options,
        );
        // Each case is `(source_micros, legacy_pre_130, compat_130_plus)`.
        // The first case mirrors Microsoft's documented 4.5ms boundary; the
        // .684582 case is the downstream repro from issue #170.
        let cases = [
            (
                4_500,
                MssqlDateTime::new(25_567, 2),
                MssqlDateTime::new(25_567, 1),
            ),
            (
                1_780_529_793_684_400,
                MssqlDateTime::new(46_174, 25_498_105),
                MssqlDateTime::new(46_174, 25_498_105),
            ),
            (
                1_780_529_793_684_582,
                MssqlDateTime::new(46_174, 25_498_106),
                MssqlDateTime::new(46_174, 25_498_105),
            ),
            (
                1_780_529_793_685_000,
                MssqlDateTime::new(46_174, 25_498_106),
                MssqlDateTime::new(46_174, 25_498_106),
            ),
            (
                1_767_311_999_999_500,
                MssqlDateTime::new(46_022, 0),
                MssqlDateTime::new(46_022, 0),
            ),
            (
                -1_584,
                // Legacy rounds to -2ms before datetime-fragment rounding;
                // compat 130+ rounds the original instant directly to epoch.
                MssqlDateTime::new(25_566, 25_919_999),
                MssqlDateTime::new(25_567, 0),
            ),
        ];

        for (row_index, (micros, legacy_expected, compat_expected)) in cases.into_iter().enumerate()
        {
            for profile in [
                MssqlProfile::sql_server_2016_compat_100(),
                MssqlProfile::sql_server_2017_compat_100(),
                MssqlProfile::sql_server_2017_compat_110(),
                MssqlProfile::sql_server_2017_compat_120(),
            ] {
                assert_eq!(
                    convert_cell_with_profile_and_options(
                        &mappings[0],
                        ArrowCell::TimestampMicrosecond(micros),
                        row_index,
                        profile,
                        &options,
                    )
                    .unwrap(),
                    MssqlCell::DateTime(Some(legacy_expected))
                );
            }

            for profile in [
                MssqlProfile::sql_server_2017_compat_130(),
                MssqlProfile::sql_server_2017_compat_140(),
            ] {
                assert_eq!(
                    convert_cell_with_profile_and_options(
                        &mappings[0],
                        ArrowCell::TimestampMicrosecond(micros),
                        row_index,
                        profile,
                        &options,
                    )
                    .unwrap(),
                    MssqlCell::DateTime(Some(compat_expected))
                );
            }
        }
    }

    #[test]
    fn applies_nanosecond_policy_before_profile_datetime_rounding() {
        let options = PlanOptions {
            timestamp_policy: TimestampPolicy::DateTime,
            nanosecond_policy: NanosecondPolicy::RoundTo100ns,
            ..PlanOptions::default()
        };
        let mappings = mappings_for_schema_with_options(
            Schema::new(vec![Field::new(
                "ts",
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                false,
            )]),
            options,
        );

        assert_eq!(
            convert_cell_with_profile_and_options(
                &mappings[0],
                ArrowCell::TimestampNanosecond(4_500_050),
                0,
                MssqlProfile::sql_server_2017_compat_100(),
                &options,
            )
            .unwrap(),
            MssqlCell::DateTime(Some(MssqlDateTime::new(25_567, 2)))
        );
        assert_eq!(
            convert_cell_with_profile_and_options(
                &mappings[0],
                ArrowCell::TimestampNanosecond(4_500_050),
                0,
                MssqlProfile::sql_server_2017_compat_140(),
                &options,
            )
            .unwrap(),
            MssqlCell::DateTime(Some(MssqlDateTime::new(25_567, 1)))
        );
    }

    #[test]
    fn rejects_overflow_while_profile_datetime_rounding() {
        let options = PlanOptions {
            timestamp_policy: TimestampPolicy::DateTime,
            ..PlanOptions::default()
        };
        let mappings = mappings_for_schema_with_options(
            Schema::new(vec![Field::new(
                "ts",
                DataType::Timestamp(TimeUnit::Microsecond, None),
                false,
            )]),
            options,
        );

        for rounding in [
            DateTimeRounding::LegacyPre130,
            DateTimeRounding::Compat130Plus,
        ] {
            let err = super::mssql_datetime_from_unix_epoch_100ns_ticks(
                &mappings[0],
                0,
                i128::MAX,
                "test",
                0,
                rounding,
            )
            .expect_err("overflow-prone datetime tick values should fail");

            assert_single_diagnostic(
                err,
                DiagnosticCode::TimestampOutOfRange,
                Some(0),
                Some((0, "ts")),
            );
        }
    }

    #[test]
    fn rejects_datetime_values_outside_sql_server_range_after_rounding() {
        let options = PlanOptions {
            timestamp_policy: TimestampPolicy::DateTime,
            ..PlanOptions::default()
        };
        let mappings = mappings_for_schema_with_options(
            Schema::new(vec![Field::new(
                "ts",
                DataType::Timestamp(TimeUnit::Microsecond, None),
                false,
            )]),
            options,
        );
        let cases = [
            ArrowCell::TimestampMicrosecond(-6_847_804_800_001_000),
            ArrowCell::TimestampMicrosecond(-6_847_891_200_000_000),
            ArrowCell::TimestampMicrosecond(253_402_300_799_999_000),
        ];

        for (row_index, cell) in cases.into_iter().enumerate() {
            let err = convert_cell_with_options(&mappings[0], cell, row_index, &options)
                .expect_err("datetime out-of-range values should fail");

            assert_single_diagnostic(
                err,
                DiagnosticCode::TimestampOutOfRange,
                Some(row_index),
                Some((0, "ts")),
            );
        }
    }

    #[test]
    fn rejects_invalid_timezone_metadata_for_datetime() {
        let options = PlanOptions {
            timezone_policy: TimezonePolicy::NormalizeUtcDateTime2,
            timestamp_policy: TimestampPolicy::DateTime,
            ..PlanOptions::default()
        };
        let mapping = SchemaMapping::new(
            crate::ArrowFieldRef::new(
                0,
                "ts".to_owned(),
                false,
                DataType::Timestamp(TimeUnit::Second, Some("not a zone".into())),
            ),
            crate::MssqlColumn::new(
                crate::Identifier::new("ts").unwrap(),
                crate::MssqlType::DateTime,
                false,
            ),
        );

        let err = convert_cell_with_options(&mapping, ArrowCell::TimestampSecond(0), 0, &options)
            .unwrap_err();

        assert_single_diagnostic(
            err,
            DiagnosticCode::TimezoneUnsupported,
            Some(0),
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

    fn convert_cell_with_options<'a>(
        mapping: &SchemaMapping,
        cell: ArrowCell<'a>,
        row_index: usize,
        options: &PlanOptions,
    ) -> crate::Result<MssqlCell<'a>> {
        mssql_cell_from_arrow_cell(
            ArrowToMssqlRuntimeMapping::new_with_options(mapping, options),
            cell,
            row_index,
        )
    }

    fn convert_cell_with_profile_and_options<'a>(
        mapping: &SchemaMapping,
        cell: ArrowCell<'a>,
        row_index: usize,
        profile: MssqlProfile,
        options: &PlanOptions,
    ) -> crate::Result<MssqlCell<'a>> {
        mssql_cell_from_arrow_cell(
            ArrowToMssqlRuntimeMapping::new(
                mapping,
                RuntimeConversionContext::new(profile, *options),
            ),
            cell,
            row_index,
        )
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

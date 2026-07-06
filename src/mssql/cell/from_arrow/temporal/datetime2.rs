//! Timestamp Arrow-to-MSSQL datetime2 runtime cell conversion.

use crate::{DiagnosticCode, MssqlType, NanosecondPolicy, Result, SchemaMapping};

use super::{
    NANOSECONDS_PER_100NS_TICK, SQL_SERVER_DATE_MAX_DAYS, SQL_SERVER_DATE_UNIX_EPOCH_DAYS,
    SQL_SERVER_DATETIME2_TIMESTAMP_SCALE, TICKS_100NS_PER_DAY, TICKS_100NS_PER_MICROSECOND,
    TICKS_100NS_PER_MILLISECOND, TICKS_100NS_PER_SECOND, row_mapping_diagnostic,
    value_conversion_error,
};
use crate::mssql::cell::{MssqlDate, MssqlDateTime2, MssqlTime};

const SQL_SERVER_DATETIME2_MIN_UNIX_EPOCH_100NS_TICKS: i128 =
    -(SQL_SERVER_DATE_UNIX_EPOCH_DAYS as i128) * TICKS_100NS_PER_DAY;

pub(crate) fn nanoseconds_to_100ns_ticks(
    mapping: &SchemaMapping,
    row_index: usize,
    nanoseconds_from_unix_epoch: i64,
    policy: NanosecondPolicy,
) -> Result<i128> {
    let base_ticks = nanoseconds_from_unix_epoch.div_euclid(NANOSECONDS_PER_100NS_TICK);
    let remainder = nanoseconds_from_unix_epoch.rem_euclid(NANOSECONDS_PER_100NS_TICK);

    match policy {
        NanosecondPolicy::RejectNon100ns if remainder == 0 => Ok(i128::from(base_ticks)),
        NanosecondPolicy::RejectNon100ns => Err(value_conversion_error(row_mapping_diagnostic(
            mapping,
            row_index,
            DiagnosticCode::LossyConversionRequiresPolicy,
            format!(
                "Arrow timestamp nanosecond value {nanoseconds_from_unix_epoch} is not divisible by 100ns"
            ),
        ))),
        NanosecondPolicy::TruncateTo100ns => Ok(i128::from(base_ticks)),
        NanosecondPolicy::RoundTo100ns => {
            let rounded_ticks = if remainder >= 50 {
                base_ticks.checked_add(1).ok_or_else(|| {
                    value_conversion_error(row_mapping_diagnostic(
                        mapping,
                        row_index,
                        DiagnosticCode::TimestampOutOfRange,
                        format!(
                            "Arrow timestamp nanosecond value {nanoseconds_from_unix_epoch} overflows while rounding to 100ns"
                        ),
                    ))
                })?
            } else {
                base_ticks
            };
            Ok(i128::from(rounded_ticks))
        }
    }
}

pub(crate) fn mssql_datetime2_from_unix_epoch_100ns_ticks(
    mapping: &SchemaMapping,
    row_index: usize,
    ticks_from_unix_epoch: i128,
    unit_name: &str,
    source_value: i64,
) -> Result<MssqlDateTime2> {
    mssql_datetime2_from_unix_epoch_100ns_ticks_at_scale(
        mapping,
        row_index,
        ticks_from_unix_epoch,
        unit_name,
        source_value,
        SQL_SERVER_DATETIME2_TIMESTAMP_SCALE,
    )
}

fn mssql_datetime2_from_unix_epoch_100ns_ticks_at_scale(
    mapping: &SchemaMapping,
    row_index: usize,
    ticks_from_unix_epoch: i128,
    unit_name: &str,
    source_value: i64,
    scale: u8,
) -> Result<MssqlDateTime2> {
    if ticks_from_unix_epoch < SQL_SERVER_DATETIME2_MIN_UNIX_EPOCH_100NS_TICKS {
        return Err(timestamp_out_of_datetime2_range(
            mapping,
            row_index,
            unit_name,
            source_value,
        ));
    }

    let ticks_from_unix_epoch =
        round_100ns_ticks_to_datetime2_scale(mapping, row_index, ticks_from_unix_epoch, scale)?;
    let days_from_unix_epoch = ticks_from_unix_epoch.div_euclid(TICKS_100NS_PER_DAY);
    let ticks_since_midnight = ticks_from_unix_epoch.rem_euclid(TICKS_100NS_PER_DAY);
    let days = days_from_unix_epoch + i128::from(SQL_SERVER_DATE_UNIX_EPOCH_DAYS);

    if !(0..=i128::from(SQL_SERVER_DATE_MAX_DAYS)).contains(&days) {
        return Err(timestamp_out_of_datetime2_range(
            mapping,
            row_index,
            unit_name,
            source_value,
        ));
    }

    let days = u32::try_from(days).map_err(|_| {
        value_conversion_error(row_mapping_diagnostic(
            mapping,
            row_index,
            DiagnosticCode::TimestampOutOfRange,
            format!(
                "Arrow timestamp {unit_name} value {source_value} has an invalid SQL Server date component"
            ),
        ))
    })?;
    let ticks_since_midnight = u64::try_from(ticks_since_midnight).map_err(|_| {
        value_conversion_error(row_mapping_diagnostic(
            mapping,
            row_index,
            DiagnosticCode::TimestampOutOfRange,
            format!(
                "Arrow timestamp {unit_name} value {source_value} has an invalid SQL Server time component"
            ),
        ))
    })?;
    let increment_factor = ticks_per_datetime2_increment(scale);
    let increments = ticks_since_midnight / increment_factor;

    Ok(MssqlDateTime2::new(
        MssqlDate::new(days),
        MssqlTime::new(increments, scale),
    ))
}

fn timestamp_out_of_datetime2_range(
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
            "Arrow timestamp {unit_name} value {source_value} is outside SQL Server datetime2 range"
        ),
    ))
}

pub(crate) fn mssql_datetime2_from_arrow_timestamp_second(
    mapping: &SchemaMapping,
    row_index: usize,
    seconds_from_unix_epoch: i64,
) -> Result<MssqlDateTime2> {
    let ticks = i128::from(seconds_from_unix_epoch) * TICKS_100NS_PER_SECOND;
    mssql_datetime2_from_arrow_timestamp_ticks(
        mapping,
        row_index,
        ticks,
        "second",
        seconds_from_unix_epoch,
    )
}

pub(crate) fn mssql_datetime2_from_arrow_timestamp_millisecond(
    mapping: &SchemaMapping,
    row_index: usize,
    milliseconds_from_unix_epoch: i64,
) -> Result<MssqlDateTime2> {
    let ticks = i128::from(milliseconds_from_unix_epoch) * TICKS_100NS_PER_MILLISECOND;
    mssql_datetime2_from_arrow_timestamp_ticks(
        mapping,
        row_index,
        ticks,
        "millisecond",
        milliseconds_from_unix_epoch,
    )
}

pub(crate) fn mssql_datetime2_from_arrow_timestamp_microsecond(
    mapping: &SchemaMapping,
    row_index: usize,
    microseconds_from_unix_epoch: i64,
) -> Result<MssqlDateTime2> {
    let ticks = i128::from(microseconds_from_unix_epoch) * TICKS_100NS_PER_MICROSECOND;
    mssql_datetime2_from_arrow_timestamp_ticks(
        mapping,
        row_index,
        ticks,
        "microsecond",
        microseconds_from_unix_epoch,
    )
}

pub(crate) fn mssql_datetime2_from_arrow_timestamp_nanosecond(
    mapping: &SchemaMapping,
    row_index: usize,
    nanoseconds_from_unix_epoch: i64,
    policy: NanosecondPolicy,
) -> Result<MssqlDateTime2> {
    let ticks =
        nanoseconds_to_100ns_ticks(mapping, row_index, nanoseconds_from_unix_epoch, policy)?;
    mssql_datetime2_from_arrow_timestamp_ticks(
        mapping,
        row_index,
        ticks,
        "nanosecond",
        nanoseconds_from_unix_epoch,
    )
}

fn mssql_datetime2_from_arrow_timestamp_ticks(
    mapping: &SchemaMapping,
    row_index: usize,
    ticks_from_unix_epoch: i128,
    unit_name: &str,
    source_value: i64,
) -> Result<MssqlDateTime2> {
    let scale = target_datetime2_timestamp_scale(mapping, row_index)?;
    mssql_datetime2_from_unix_epoch_100ns_ticks_at_scale(
        mapping,
        row_index,
        ticks_from_unix_epoch,
        unit_name,
        source_value,
        scale,
    )
}

fn target_datetime2_timestamp_scale(mapping: &SchemaMapping, row_index: usize) -> Result<u8> {
    match mapping.mssql().ty() {
        MssqlType::DateTime2 { precision }
            if *precision <= SQL_SERVER_DATETIME2_TIMESTAMP_SCALE =>
        {
            Ok(*precision)
        }
        MssqlType::DateTime2 { precision } => Err(value_conversion_error(row_mapping_diagnostic(
            mapping,
            row_index,
            DiagnosticCode::ValueConversionUnsupported,
            format!("SQL Server datetime2 precision {precision} is outside range 0..=7"),
        ))),
        ty => Err(value_conversion_error(row_mapping_diagnostic(
            mapping,
            row_index,
            DiagnosticCode::ValueConversionUnsupported,
            format!(
                "expected Arrow timestamp planned as datetime2, got SQL Server {}",
                ty.to_sql()
            ),
        ))),
    }
}

fn round_100ns_ticks_to_datetime2_scale(
    mapping: &SchemaMapping,
    row_index: usize,
    ticks: i128,
    scale: u8,
) -> Result<i128> {
    if scale == SQL_SERVER_DATETIME2_TIMESTAMP_SCALE {
        return Ok(ticks);
    }

    let factor = i128::from(ticks_per_datetime2_increment(scale));
    let base = ticks.div_euclid(factor);
    let remainder = ticks.rem_euclid(factor);
    let rounded_base = if remainder * 2 >= factor {
        base.checked_add(1).ok_or_else(|| {
            value_conversion_error(row_mapping_diagnostic(
                mapping,
                row_index,
                DiagnosticCode::TimestampOutOfRange,
                "Arrow timestamp overflows while rounding to datetime2 precision",
            ))
        })?
    } else {
        base
    };

    rounded_base.checked_mul(factor).ok_or_else(|| {
        value_conversion_error(row_mapping_diagnostic(
            mapping,
            row_index,
            DiagnosticCode::TimestampOutOfRange,
            "Arrow timestamp overflows while rounding to datetime2 precision",
        ))
    })
}

fn ticks_per_datetime2_increment(scale: u8) -> u64 {
    10_u64.pow(u32::from(SQL_SERVER_DATETIME2_TIMESTAMP_SCALE - scale))
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
        mssql::cell::{MssqlCell, MssqlDate, MssqlDateTime2, MssqlTime},
        plan_arrow_schema_to_mssql_mappings,
    };

    #[test]
    fn converts_timezone_free_timestamp_cells_to_datetime2_7_with_boundaries_and_nulls() {
        let mappings = mappings_for_schema(Schema::new(vec![
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
            Field::new(
                "ts_ns",
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                true,
            ),
        ]));
        let cases = [
            (
                0,
                ArrowCell::TimestampSecond(0),
                MssqlCell::DateTime2(Some(MssqlDateTime2::new(
                    MssqlDate::new(719_162),
                    MssqlTime::new(0, 7),
                ))),
            ),
            (
                0,
                ArrowCell::TimestampSecond(-1),
                MssqlCell::DateTime2(Some(MssqlDateTime2::new(
                    MssqlDate::new(719_161),
                    MssqlTime::new(863_990_000_000, 7),
                ))),
            ),
            (0, ArrowCell::Null, MssqlCell::DateTime2(None)),
            (
                1,
                ArrowCell::TimestampMillisecond(-1),
                MssqlCell::DateTime2(Some(MssqlDateTime2::new(
                    MssqlDate::new(719_161),
                    MssqlTime::new(863_999_990_000, 7),
                ))),
            ),
            (
                2,
                ArrowCell::TimestampMicrosecond(1_234_567),
                MssqlCell::DateTime2(Some(MssqlDateTime2::new(
                    MssqlDate::new(719_162),
                    MssqlTime::new(12_345_670, 7),
                ))),
            ),
            (
                2,
                ArrowCell::TimestampMicrosecond(-1),
                MssqlCell::DateTime2(Some(MssqlDateTime2::new(
                    MssqlDate::new(719_161),
                    MssqlTime::new(863_999_999_990, 7),
                ))),
            ),
            (
                3,
                ArrowCell::TimestampNanosecond(123_456_700),
                MssqlCell::DateTime2(Some(MssqlDateTime2::new(
                    MssqlDate::new(719_162),
                    MssqlTime::new(1_234_567, 7),
                ))),
            ),
            (
                3,
                ArrowCell::TimestampNanosecond(-100),
                MssqlCell::DateTime2(Some(MssqlDateTime2::new(
                    MssqlDate::new(719_161),
                    MssqlTime::new(863_999_999_999, 7),
                ))),
            ),
        ];

        for (mapping_index, cell, expected) in cases {
            assert_eq!(
                convert_cell(&mappings[mapping_index], cell, mapping_index).unwrap(),
                expected
            );
        }
    }

    #[test]
    fn converts_timezone_free_timestamp_cells_to_planned_datetime2_precision() {
        let cases = [
            (
                3,
                ArrowCell::TimestampMicrosecond(1_234_567),
                MssqlCell::DateTime2(Some(MssqlDateTime2::new(
                    MssqlDate::new(719_162),
                    MssqlTime::new(1_235, 3),
                ))),
            ),
            (
                6,
                ArrowCell::TimestampMicrosecond(1_234_567),
                MssqlCell::DateTime2(Some(MssqlDateTime2::new(
                    MssqlDate::new(719_162),
                    MssqlTime::new(1_234_567, 6),
                ))),
            ),
            (
                0,
                ArrowCell::TimestampMicrosecond(1_500_000),
                MssqlCell::DateTime2(Some(MssqlDateTime2::new(
                    MssqlDate::new(719_162),
                    MssqlTime::new(2, 0),
                ))),
            ),
            (
                3,
                ArrowCell::TimestampMicrosecond(86_399_999_500),
                MssqlCell::DateTime2(Some(MssqlDateTime2::new(
                    MssqlDate::new(719_163),
                    MssqlTime::new(0, 3),
                ))),
            ),
            (
                3,
                ArrowCell::TimestampMicrosecond(-62_135_596_800_000_000),
                MssqlCell::DateTime2(Some(MssqlDateTime2::new(
                    MssqlDate::new(0),
                    MssqlTime::new(0, 3),
                ))),
            ),
        ];

        for (precision, cell, expected) in cases {
            let options = PlanOptions {
                timestamp_policy: TimestampPolicy::DateTime2 { precision },
                ..PlanOptions::default()
            };
            let mappings = mappings_for_schema_with_options(
                Schema::new(vec![Field::new(
                    "ts",
                    DataType::Timestamp(TimeUnit::Microsecond, None),
                    true,
                )]),
                options,
            );

            assert_eq!(
                convert_cell_with_options(&mappings[0], cell, 0, &options).unwrap(),
                expected
            );
        }
    }

    #[test]
    fn converts_datetime2_precision_timestamp_nulls() {
        let options = PlanOptions {
            timestamp_policy: TimestampPolicy::DateTime2 { precision: 3 },
            ..PlanOptions::default()
        };
        let mappings = mappings_for_schema_with_options(
            Schema::new(vec![Field::new(
                "ts",
                DataType::Timestamp(TimeUnit::Second, None),
                true,
            )]),
            options,
        );

        assert_eq!(
            convert_cell_with_options(&mappings[0], ArrowCell::Null, 0, &options).unwrap(),
            MssqlCell::DateTime2(None)
        );
    }

    #[test]
    fn applies_datetime2_precision_after_nanosecond_policy() {
        let options = PlanOptions {
            timestamp_policy: TimestampPolicy::DateTime2 { precision: 6 },
            nanosecond_policy: NanosecondPolicy::RoundTo100ns,
            ..PlanOptions::default()
        };
        let mappings = mappings_for_schema_with_options(
            Schema::new(vec![Field::new(
                "ts_ns",
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                false,
            )]),
            options,
        );

        assert_eq!(
            convert_cell_with_options(
                &mappings[0],
                ArrowCell::TimestampNanosecond(1_234_567_500),
                0,
                &options,
            )
            .unwrap(),
            MssqlCell::DateTime2(Some(MssqlDateTime2::new(
                MssqlDate::new(719_162),
                MssqlTime::new(1_234_568, 6),
            )))
        );
    }

    #[test]
    fn rejects_nanosecond_timestamp_precision_loss_by_default() {
        let mappings = mappings_for_schema(Schema::new(vec![Field::new(
            "ts_ns",
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            false,
        )]));

        let err = convert_cell(&mappings[0], ArrowCell::TimestampNanosecond(101), 0).unwrap_err();

        assert_single_diagnostic(
            err,
            DiagnosticCode::LossyConversionRequiresPolicy,
            Some(0),
            Some((0, "ts_ns")),
        );
    }

    #[test]
    fn applies_nanosecond_round_and_truncate_policies_at_runtime() {
        let round_options = PlanOptions {
            nanosecond_policy: NanosecondPolicy::RoundTo100ns,
            ..PlanOptions::default()
        };
        let truncate_options = PlanOptions {
            nanosecond_policy: NanosecondPolicy::TruncateTo100ns,
            ..PlanOptions::default()
        };
        let mappings = mappings_for_schema_with_options(
            Schema::new(vec![Field::new(
                "ts_ns",
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                false,
            )]),
            round_options,
        );

        let round_cases = [
            (
                ArrowCell::TimestampNanosecond(149),
                MssqlCell::DateTime2(Some(MssqlDateTime2::new(
                    MssqlDate::new(719_162),
                    MssqlTime::new(1, 7),
                ))),
            ),
            (
                ArrowCell::TimestampNanosecond(150),
                MssqlCell::DateTime2(Some(MssqlDateTime2::new(
                    MssqlDate::new(719_162),
                    MssqlTime::new(2, 7),
                ))),
            ),
            (
                ArrowCell::TimestampNanosecond(-149),
                MssqlCell::DateTime2(Some(MssqlDateTime2::new(
                    MssqlDate::new(719_161),
                    MssqlTime::new(863_999_999_999, 7),
                ))),
            ),
        ];
        for (row_index, (cell, expected)) in round_cases.into_iter().enumerate() {
            assert_eq!(
                convert_cell_with_options(&mappings[0], cell, row_index, &round_options).unwrap(),
                expected
            );
        }

        let truncate_cases = [
            (
                ArrowCell::TimestampNanosecond(149),
                MssqlCell::DateTime2(Some(MssqlDateTime2::new(
                    MssqlDate::new(719_162),
                    MssqlTime::new(1, 7),
                ))),
            ),
            (
                ArrowCell::TimestampNanosecond(150),
                MssqlCell::DateTime2(Some(MssqlDateTime2::new(
                    MssqlDate::new(719_162),
                    MssqlTime::new(1, 7),
                ))),
            ),
            (
                ArrowCell::TimestampNanosecond(-149),
                MssqlCell::DateTime2(Some(MssqlDateTime2::new(
                    MssqlDate::new(719_161),
                    MssqlTime::new(863_999_999_998, 7),
                ))),
            ),
        ];
        for (row_index, (cell, expected)) in truncate_cases.into_iter().enumerate() {
            assert_eq!(
                convert_cell_with_options(&mappings[0], cell, row_index, &truncate_options)
                    .unwrap(),
                expected
            );
        }
    }

    #[test]
    fn rejects_datetime2_values_below_min_before_precision_rounding() {
        for (row_index, precision) in [0, 3].into_iter().enumerate() {
            let options = PlanOptions {
                timestamp_policy: TimestampPolicy::DateTime2 { precision },
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
            let err = convert_cell_with_options(
                &mappings[0],
                ArrowCell::TimestampMicrosecond(-62_135_596_800_000_001),
                row_index,
                &options,
            )
            .expect_err("datetime2 values below min should fail before precision rounding");

            assert_single_diagnostic(
                err,
                DiagnosticCode::TimestampOutOfRange,
                Some(row_index),
                Some((0, "ts")),
            );
        }
    }

    #[test]
    fn rejects_timestamp_values_outside_sql_server_datetime2_range() {
        let mappings = mappings_for_schema(Schema::new(vec![Field::new(
            "ts_s",
            DataType::Timestamp(TimeUnit::Second, None),
            false,
        )]));

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
    fn converts_timezone_aware_normalized_timestamp_cells_to_planned_datetime2_precision() {
        let options = PlanOptions {
            timezone_policy: TimezonePolicy::NormalizeUtcDateTime2,
            timestamp_policy: TimestampPolicy::DateTime2 { precision: 3 },
            ..PlanOptions::default()
        };
        let mappings = mappings_for_schema_with_options(
            Schema::new(vec![Field::new(
                "utc",
                DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
                true,
            )]),
            options,
        );

        assert_eq!(
            convert_cell_with_options(
                &mappings[0],
                ArrowCell::TimestampMicrosecond(1_234_567),
                0,
                &options,
            )
            .unwrap(),
            MssqlCell::DateTime2(Some(MssqlDateTime2::new(
                MssqlDate::new(719_162),
                MssqlTime::new(1_235, 3),
            )))
        );
        assert_eq!(
            convert_cell_with_options(&mappings[0], ArrowCell::Null, 1, &options).unwrap(),
            MssqlCell::DateTime2(None)
        );
    }

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
        let runtime_mapping = ArrowToMssqlRuntimeMapping::new_with_options(mapping, options);
        mssql_cell_from_arrow_cell(runtime_mapping, cell, row_index)
    }

    fn mappings_for_schema(schema: Schema) -> Vec<SchemaMapping> {
        mappings_for_schema_with_options(schema, PlanOptions::default())
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

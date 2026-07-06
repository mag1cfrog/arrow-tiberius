//! Time-only Arrow-to-MSSQL runtime cell conversion.

use crate::{
    DiagnosticCode, NanosecondPolicy, Result, SchemaMapping,
    arrow::cell::ArrowCell,
    conversion::arrow_to_mssql::temporal::TemporalArrowToMssql,
    mssql::cell::{MssqlCell, MssqlTime},
};

use super::{
    ArrowToMssqlRuntimeMapping, NANOSECONDS_PER_100NS_TICK, row_mapping_diagnostic,
    value_conversion_error,
};

const SECONDS_PER_DAY: i64 = 86_400;
const MILLISECONDS_PER_DAY: i64 = 86_400_000;
const MICROSECONDS_PER_DAY: i64 = 86_400_000_000;
const NANOSECONDS_PER_DAY: i64 = 86_400_000_000_000;
const TICKS_100NS_PER_DAY_U64: u64 = 864_000_000_000;

pub(in crate::mssql::cell::from_arrow) fn mssql_time_value(
    runtime_mapping: ArrowToMssqlRuntimeMapping<'_>,
    row_index: usize,
    cell: ArrowCell<'_>,
) -> Result<MssqlTime> {
    let mapping = runtime_mapping.mapping();

    let classification = TemporalArrowToMssql::classify(mapping, row_index)?;

    match (cell, classification) {
        (ArrowCell::Time32Second(value), TemporalArrowToMssql::Time32SecondToTime) => {
            mssql_time_from_i64(
                mapping,
                row_index,
                i64::from(value),
                SECONDS_PER_DAY,
                0,
                "Time32 second",
            )
        }
        (ArrowCell::Time32Millisecond(value), TemporalArrowToMssql::Time32MillisecondToTime) => {
            mssql_time_from_i64(
                mapping,
                row_index,
                i64::from(value),
                MILLISECONDS_PER_DAY,
                3,
                "Time32 millisecond",
            )
        }
        (ArrowCell::Time64Microsecond(value), TemporalArrowToMssql::Time64MicrosecondToTime) => {
            mssql_time_from_i64(
                mapping,
                row_index,
                value,
                MICROSECONDS_PER_DAY,
                6,
                "Time64 microsecond",
            )
        }
        (ArrowCell::Time64Nanosecond(value), TemporalArrowToMssql::Time64NanosecondToTime) => {
            mssql_time_from_nanoseconds(
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
            format!("expected Arrow time-only payload planned as time, got {other:?}"),
        ))),
    }
}

pub(in crate::mssql::cell::from_arrow) fn null_time_cell<'a>(
    mapping: &SchemaMapping,
    row_index: usize,
) -> Result<MssqlCell<'a>> {
    match TemporalArrowToMssql::classify(mapping, row_index)? {
        TemporalArrowToMssql::Time32SecondToTime
        | TemporalArrowToMssql::Time32MillisecondToTime
        | TemporalArrowToMssql::Time64MicrosecondToTime
        | TemporalArrowToMssql::Time64NanosecondToTime => Ok(MssqlCell::Time(None)),
        classification => Err(value_conversion_error(row_mapping_diagnostic(
            mapping,
            row_index,
            DiagnosticCode::ValueConversionUnsupported,
            format!("planned temporal mapping {classification:?} is not a time conversion"),
        ))),
    }
}

pub(crate) fn mssql_time_from_arrow_time32_second(
    mapping: &SchemaMapping,
    row_index: usize,
    seconds_since_midnight: i32,
) -> Result<MssqlTime> {
    mssql_time_from_i64(
        mapping,
        row_index,
        i64::from(seconds_since_midnight),
        SECONDS_PER_DAY,
        0,
        "Time32 second",
    )
}

pub(crate) fn mssql_time_from_arrow_time32_millisecond(
    mapping: &SchemaMapping,
    row_index: usize,
    milliseconds_since_midnight: i32,
) -> Result<MssqlTime> {
    mssql_time_from_i64(
        mapping,
        row_index,
        i64::from(milliseconds_since_midnight),
        MILLISECONDS_PER_DAY,
        3,
        "Time32 millisecond",
    )
}

pub(crate) fn mssql_time_from_arrow_time64_microsecond(
    mapping: &SchemaMapping,
    row_index: usize,
    microseconds_since_midnight: i64,
) -> Result<MssqlTime> {
    mssql_time_from_i64(
        mapping,
        row_index,
        microseconds_since_midnight,
        MICROSECONDS_PER_DAY,
        6,
        "Time64 microsecond",
    )
}

pub(crate) fn mssql_time_from_arrow_time64_nanosecond(
    mapping: &SchemaMapping,
    row_index: usize,
    nanoseconds_since_midnight: i64,
    policy: NanosecondPolicy,
) -> Result<MssqlTime> {
    mssql_time_from_nanoseconds(mapping, row_index, nanoseconds_since_midnight, policy)
}

fn mssql_time_from_i64(
    mapping: &SchemaMapping,
    row_index: usize,
    increments: i64,
    increments_per_day: i64,
    scale: u8,
    unit_name: &str,
) -> Result<MssqlTime> {
    if !(0..increments_per_day).contains(&increments) {
        return Err(time_out_of_range(mapping, row_index, unit_name, increments));
    }

    Ok(MssqlTime::new(increments as u64, scale))
}

fn mssql_time_from_nanoseconds(
    mapping: &SchemaMapping,
    row_index: usize,
    nanoseconds_since_midnight: i64,
    policy: NanosecondPolicy,
) -> Result<MssqlTime> {
    if !(0..NANOSECONDS_PER_DAY).contains(&nanoseconds_since_midnight) {
        return Err(time_out_of_range(
            mapping,
            row_index,
            "Time64 nanosecond",
            nanoseconds_since_midnight,
        ));
    }

    let ticks = nanoseconds_to_100ns_ticks(mapping, row_index, nanoseconds_since_midnight, policy)?;
    let ticks = u64::try_from(ticks).map_err(|_| {
        time_out_of_range(
            mapping,
            row_index,
            "Time64 nanosecond",
            nanoseconds_since_midnight,
        )
    })?;

    if ticks >= TICKS_100NS_PER_DAY_U64 {
        return Err(time_out_of_range(
            mapping,
            row_index,
            "Time64 nanosecond",
            nanoseconds_since_midnight,
        ));
    }

    Ok(MssqlTime::new(ticks, 7))
}

fn nanoseconds_to_100ns_ticks(
    mapping: &SchemaMapping,
    row_index: usize,
    nanoseconds_since_midnight: i64,
    policy: NanosecondPolicy,
) -> Result<i64> {
    let base_ticks = nanoseconds_since_midnight.div_euclid(NANOSECONDS_PER_100NS_TICK);
    let remainder = nanoseconds_since_midnight.rem_euclid(NANOSECONDS_PER_100NS_TICK);

    match policy {
        NanosecondPolicy::RejectNon100ns if remainder == 0 => Ok(base_ticks),
        NanosecondPolicy::RejectNon100ns => Err(value_conversion_error(row_mapping_diagnostic(
            mapping,
            row_index,
            DiagnosticCode::LossyConversionRequiresPolicy,
            format!(
                "Arrow Time64 nanosecond value {nanoseconds_since_midnight} is not divisible by 100ns"
            ),
        ))),
        NanosecondPolicy::TruncateTo100ns => Ok(base_ticks),
        NanosecondPolicy::RoundTo100ns => {
            let rounded_ticks = if remainder >= 50 {
                base_ticks.checked_add(1).ok_or_else(|| {
                    time_out_of_range(
                        mapping,
                        row_index,
                        "Time64 nanosecond",
                        nanoseconds_since_midnight,
                    )
                })?
            } else {
                base_ticks
            };
            Ok(rounded_ticks)
        }
    }
}

fn time_out_of_range(
    mapping: &SchemaMapping,
    row_index: usize,
    unit_name: &str,
    value: i64,
) -> crate::Error {
    value_conversion_error(row_mapping_diagnostic(
        mapping,
        row_index,
        DiagnosticCode::TimestampOutOfRange,
        format!("Arrow {unit_name} value {value} is outside SQL Server time range"),
    ))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_schema::{DataType, Field, Schema, TimeUnit};

    use super::super::super::{ArrowToMssqlRuntimeMapping, mssql_cell_from_arrow_cell};
    use crate::{
        ArrowFieldRef, DiagnosticCode, Identifier, MssqlColumn, MssqlProfile, MssqlTimePrecision,
        MssqlType, NanosecondPolicy, PlanOptions, SchemaMapping,
        arrow::cell::ArrowCell,
        mssql::cell::{MssqlCell, MssqlTime},
        plan_arrow_schema_to_mssql_mappings,
    };

    #[test]
    fn converts_time_only_cells_with_boundaries_and_nulls() {
        let mappings = mappings_for_schema(Schema::new(vec![
            Field::new("time32_s", DataType::Time32(TimeUnit::Second), true),
            Field::new("time32_ms", DataType::Time32(TimeUnit::Millisecond), true),
            Field::new("time64_us", DataType::Time64(TimeUnit::Microsecond), true),
            Field::new("time64_ns", DataType::Time64(TimeUnit::Nanosecond), true),
        ]));
        let cases = [
            (
                0,
                ArrowCell::Time32Second(0),
                MssqlCell::Time(Some(MssqlTime::new(0, 0))),
            ),
            (
                0,
                ArrowCell::Time32Second(86_399),
                MssqlCell::Time(Some(MssqlTime::new(86_399, 0))),
            ),
            (0, ArrowCell::Null, MssqlCell::Time(None)),
            (
                1,
                ArrowCell::Time32Millisecond(86_399_999),
                MssqlCell::Time(Some(MssqlTime::new(86_399_999, 3))),
            ),
            (
                2,
                ArrowCell::Time64Microsecond(86_399_999_999),
                MssqlCell::Time(Some(MssqlTime::new(86_399_999_999, 6))),
            ),
            (
                3,
                ArrowCell::Time64Nanosecond(86_399_999_999_900),
                MssqlCell::Time(Some(MssqlTime::new(863_999_999_999, 7))),
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
    fn rejects_time_only_null_in_non_nullable_column() {
        let mappings = mappings_for_schema(Schema::new(vec![Field::new(
            "time_value",
            DataType::Time32(TimeUnit::Second),
            false,
        )]));

        let err = convert_cell(&mappings[0], ArrowCell::Null, 0).unwrap_err();

        assert_single_diagnostic(
            err,
            DiagnosticCode::NullInNonNullableColumn,
            Some(0),
            Some((0, "time_value")),
        );
    }

    #[test]
    fn rejects_time_only_values_outside_one_day() {
        let mappings = mappings_for_schema(Schema::new(vec![
            Field::new("time32_s", DataType::Time32(TimeUnit::Second), false),
            Field::new("time32_ms", DataType::Time32(TimeUnit::Millisecond), false),
            Field::new("time64_us", DataType::Time64(TimeUnit::Microsecond), false),
            Field::new("time64_ns", DataType::Time64(TimeUnit::Nanosecond), false),
        ]));
        let cases = [
            (0, ArrowCell::Time32Second(-1)),
            (0, ArrowCell::Time32Second(86_400)),
            (1, ArrowCell::Time32Millisecond(-1)),
            (1, ArrowCell::Time32Millisecond(86_400_000)),
            (2, ArrowCell::Time64Microsecond(-1)),
            (2, ArrowCell::Time64Microsecond(86_400_000_000)),
            (3, ArrowCell::Time64Nanosecond(-1)),
            (3, ArrowCell::Time64Nanosecond(86_400_000_000_000)),
        ];

        for (row_index, (mapping_index, cell)) in cases.into_iter().enumerate() {
            let err = convert_cell(&mappings[mapping_index], cell, row_index).unwrap_err();

            assert_single_diagnostic(
                err,
                DiagnosticCode::TimestampOutOfRange,
                Some(row_index),
                Some((mapping_index, mappings[mapping_index].arrow().name())),
            );
        }
    }

    #[test]
    fn applies_nanosecond_policy_to_time64_nanosecond_values() {
        let mappings = mappings_for_schema(Schema::new(vec![Field::new(
            "time64_ns",
            DataType::Time64(TimeUnit::Nanosecond),
            false,
        )]));

        let reject = convert_cell(&mappings[0], ArrowCell::Time64Nanosecond(101), 0).unwrap_err();
        assert_single_diagnostic(
            reject,
            DiagnosticCode::LossyConversionRequiresPolicy,
            Some(0),
            Some((0, "time64_ns")),
        );

        assert_eq!(
            convert_cell_with_options(
                &mappings[0],
                ArrowCell::Time64Nanosecond(149),
                1,
                &PlanOptions {
                    nanosecond_policy: NanosecondPolicy::RoundTo100ns,
                    ..PlanOptions::default()
                },
            )
            .unwrap(),
            MssqlCell::Time(Some(MssqlTime::new(1, 7)))
        );
        assert_eq!(
            convert_cell_with_options(
                &mappings[0],
                ArrowCell::Time64Nanosecond(150),
                2,
                &PlanOptions {
                    nanosecond_policy: NanosecondPolicy::RoundTo100ns,
                    ..PlanOptions::default()
                },
            )
            .unwrap(),
            MssqlCell::Time(Some(MssqlTime::new(2, 7)))
        );
        assert_eq!(
            convert_cell_with_options(
                &mappings[0],
                ArrowCell::Time64Nanosecond(199),
                3,
                &PlanOptions {
                    nanosecond_policy: NanosecondPolicy::TruncateTo100ns,
                    ..PlanOptions::default()
                },
            )
            .unwrap(),
            MssqlCell::Time(Some(MssqlTime::new(1, 7)))
        );
    }

    #[test]
    fn rejects_nanosecond_rounding_to_exactly_one_day() {
        let mappings = mappings_for_schema(Schema::new(vec![Field::new(
            "time64_ns",
            DataType::Time64(TimeUnit::Nanosecond),
            false,
        )]));

        let err = convert_cell_with_options(
            &mappings[0],
            ArrowCell::Time64Nanosecond(86_399_999_999_950),
            0,
            &PlanOptions {
                nanosecond_policy: NanosecondPolicy::RoundTo100ns,
                ..PlanOptions::default()
            },
        )
        .unwrap_err();

        assert_single_diagnostic(
            err,
            DiagnosticCode::TimestampOutOfRange,
            Some(0),
            Some((0, "time64_ns")),
        );
    }

    #[test]
    fn rejects_forged_time_mapping_with_unsupported_precision() {
        let mapping = SchemaMapping::new(
            ArrowFieldRef::new(
                0,
                "time_value".to_owned(),
                false,
                DataType::Time32(TimeUnit::Second),
            ),
            MssqlColumn::new(
                Identifier::new("time_value").unwrap(),
                MssqlType::Time(MssqlTimePrecision::THREE),
                false,
            ),
        );

        let err = convert_cell(&mapping, ArrowCell::Time32Second(0), 0).unwrap_err();

        assert_single_diagnostic(
            err,
            DiagnosticCode::ValueConversionUnsupported,
            Some(0),
            Some((0, "time_value")),
        );
    }

    #[test]
    fn rejects_forged_time_mapping_with_valid_but_unmapped_precision() {
        let mapping = SchemaMapping::new(
            ArrowFieldRef::new(
                0,
                "time_value".to_owned(),
                false,
                DataType::Time64(TimeUnit::Microsecond),
            ),
            MssqlColumn::new(
                Identifier::new("time_value").unwrap(),
                MssqlType::Time(MssqlTimePrecision::new(4).unwrap()),
                false,
            ),
        );

        let err = convert_cell(&mapping, ArrowCell::Time64Microsecond(0), 0).unwrap_err();

        assert_single_diagnostic(
            err,
            DiagnosticCode::ValueConversionUnsupported,
            Some(0),
            Some((0, "time_value")),
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
        plan_arrow_schema_to_mssql_mappings(
            Arc::new(schema),
            MssqlProfile::sql_server_2016_compat_100(),
            PlanOptions::default(),
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

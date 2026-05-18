//! Temporal Arrow-to-MSSQL runtime cell conversion.

mod date;
mod date64;
mod datetime2;
mod datetimeoffset;
mod timezone;

use crate::{DiagnosticCode, MssqlType, Result, SchemaMapping, arrow::cell::ArrowCell};
use arrow_schema::{DataType, TimeUnit};

use super::{
    ArrowToMssqlRuntimeMapping, row_mapping_diagnostic, unsupported_value_conversion,
    value_conversion_error,
};
use crate::mssql::cell::{MssqlCell, MssqlDateTime2};
pub(super) use date::mssql_date_value;
pub(super) use date64::mssql_datetime2_from_arrow_date64;
use datetime2::{
    mssql_datetime2_from_arrow_timestamp_microsecond,
    mssql_datetime2_from_arrow_timestamp_millisecond,
    mssql_datetime2_from_arrow_timestamp_nanosecond, mssql_datetime2_from_arrow_timestamp_second,
};
pub(super) use datetimeoffset::mssql_datetimeoffset_value;
use timezone::timezone_resolution_from_metadata;

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

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_schema::{DataType, Field, Schema, TimeUnit};

    use super::{MssqlCell, MssqlDateTime2};
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

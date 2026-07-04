//! Temporal Arrow-to-MSSQL runtime cell conversion.

mod date;
mod date64;
mod datetime;
pub(crate) mod datetime2;
pub(crate) mod datetimeoffset;
pub(crate) mod time;
mod timezone;

use crate::{
    DiagnosticCode, Result, SchemaMapping, arrow::cell::ArrowCell,
    conversion::arrow_to_mssql::temporal::TemporalArrowToMssql,
};
use arrow_schema::{DataType, TimeUnit};

use super::{
    ArrowToMssqlRuntimeMapping, row_mapping_diagnostic, unsupported_value_conversion,
    value_conversion_error,
};
use crate::MssqlType;
use crate::mssql::cell::{MssqlCell, MssqlDateTime, MssqlDateTime2};
pub(super) use date::mssql_date_value;
pub(super) use date64::mssql_datetime2_from_arrow_date64;
use datetime::{
    mssql_datetime_from_arrow_timestamp_microsecond,
    mssql_datetime_from_arrow_timestamp_millisecond,
    mssql_datetime_from_arrow_timestamp_nanosecond, mssql_datetime_from_arrow_timestamp_second,
};
use datetime2::{
    mssql_datetime2_from_arrow_timestamp_microsecond,
    mssql_datetime2_from_arrow_timestamp_millisecond,
    mssql_datetime2_from_arrow_timestamp_nanosecond, mssql_datetime2_from_arrow_timestamp_second,
};
pub(super) use datetimeoffset::mssql_datetimeoffset_value;
pub(super) use time::{mssql_time_value, null_time_cell};
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

pub(super) fn mssql_datetime_value(
    runtime_mapping: ArrowToMssqlRuntimeMapping<'_>,
    row_index: usize,
    cell: ArrowCell<'_>,
) -> Result<MssqlDateTime> {
    let mapping = runtime_mapping.mapping();
    datetime_mapping_kind(mapping, row_index)?;

    match cell {
        ArrowCell::TimestampSecond(value) => {
            validate_mapping_timestamp_timezone_metadata(mapping, row_index)?;
            mssql_datetime_from_arrow_timestamp_second(mapping, row_index, value)
        }
        ArrowCell::TimestampMillisecond(value) => {
            validate_mapping_timestamp_timezone_metadata(mapping, row_index)?;
            mssql_datetime_from_arrow_timestamp_millisecond(mapping, row_index, value)
        }
        ArrowCell::TimestampMicrosecond(value) => {
            validate_mapping_timestamp_timezone_metadata(mapping, row_index)?;
            mssql_datetime_from_arrow_timestamp_microsecond(mapping, row_index, value)
        }
        ArrowCell::TimestampNanosecond(value) => {
            validate_mapping_timestamp_timezone_metadata(mapping, row_index)?;
            mssql_datetime_from_arrow_timestamp_nanosecond(
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
            format!("expected Arrow timestamp payload planned as datetime, got {other:?}"),
        ))),
    }
}

pub(super) fn null_datetime_cell<'a>(
    mapping: &SchemaMapping,
    row_index: usize,
) -> Result<MssqlCell<'a>> {
    datetime_mapping_kind(mapping, row_index)?;
    validate_null_timestamp_timezone_metadata(mapping, row_index)?;
    Ok(MssqlCell::DateTime(None))
}

fn datetime_mapping_kind(mapping: &SchemaMapping, row_index: usize) -> Result<()> {
    match (mapping.arrow().data_type(), mapping.mssql().ty()) {
        (
            DataType::Timestamp(
                TimeUnit::Second
                | TimeUnit::Millisecond
                | TimeUnit::Microsecond
                | TimeUnit::Nanosecond,
                _,
            ),
            MssqlType::DateTime,
        ) => Ok(()),
        _ => Err(unsupported_value_conversion(
            mapping,
            row_index,
            format!(
                "temporal conversion from Arrow {} to SQL Server {} is not supported",
                mapping.arrow().data_type(),
                mapping.mssql().ty().to_sql()
            ),
        )),
    }
}

pub(super) fn mssql_datetime2_value(
    runtime_mapping: ArrowToMssqlRuntimeMapping<'_>,
    row_index: usize,
    cell: ArrowCell<'_>,
) -> Result<MssqlDateTime2> {
    let mapping = runtime_mapping.mapping();
    let mapping_kind = datetime2_mapping_kind(mapping, row_index)?;

    match (cell, mapping_kind) {
        (ArrowCell::Date64(value), DateTime2MappingKind::Date64) => {
            mssql_datetime2_from_arrow_date64(mapping, row_index, value)
        }
        (ArrowCell::TimestampSecond(value), DateTime2MappingKind::Timestamp) => {
            validate_mapping_timestamp_timezone_metadata(mapping, row_index)?;
            mssql_datetime2_from_arrow_timestamp_second(mapping, row_index, value)
        }
        (ArrowCell::TimestampMillisecond(value), DateTime2MappingKind::Timestamp) => {
            validate_mapping_timestamp_timezone_metadata(mapping, row_index)?;
            mssql_datetime2_from_arrow_timestamp_millisecond(mapping, row_index, value)
        }
        (ArrowCell::TimestampMicrosecond(value), DateTime2MappingKind::Timestamp) => {
            validate_mapping_timestamp_timezone_metadata(mapping, row_index)?;
            mssql_datetime2_from_arrow_timestamp_microsecond(mapping, row_index, value)
        }
        (ArrowCell::TimestampNanosecond(value), DateTime2MappingKind::Timestamp) => {
            validate_mapping_timestamp_timezone_metadata(mapping, row_index)?;
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
    match datetime2_mapping_kind(mapping, row_index)? {
        DateTime2MappingKind::Date64 | DateTime2MappingKind::Timestamp => {
            validate_null_timestamp_timezone_metadata(mapping, row_index)?;
            Ok(MssqlCell::DateTime2(None))
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DateTime2MappingKind {
    Date64,
    Timestamp,
}

fn datetime2_mapping_kind(
    mapping: &SchemaMapping,
    row_index: usize,
) -> Result<DateTime2MappingKind> {
    match (mapping.arrow().data_type(), mapping.mssql().ty()) {
        (DataType::Date64, MssqlType::DateTime2 { precision: 3 }) => {
            Ok(DateTime2MappingKind::Date64)
        }
        (
            DataType::Timestamp(
                TimeUnit::Second
                | TimeUnit::Millisecond
                | TimeUnit::Microsecond
                | TimeUnit::Nanosecond,
                _,
            ),
            MssqlType::DateTime2 { precision },
        ) if *precision <= SQL_SERVER_DATETIME2_TIMESTAMP_SCALE => {
            Ok(DateTime2MappingKind::Timestamp)
        }
        _ => Err(unsupported_value_conversion(
            mapping,
            row_index,
            format!(
                "temporal conversion from Arrow {} to SQL Server {} is not supported",
                mapping.arrow().data_type(),
                mapping.mssql().ty().to_sql()
            ),
        )),
    }
}

pub(super) fn null_datetimeoffset_cell<'a>(
    mapping: &SchemaMapping,
    row_index: usize,
) -> Result<MssqlCell<'a>> {
    match TemporalArrowToMssql::classify(mapping, row_index)? {
        TemporalArrowToMssql::TimestampSecondTzToDateTimeOffset
        | TemporalArrowToMssql::TimestampMillisecondTzToDateTimeOffset
        | TemporalArrowToMssql::TimestampMicrosecondTzToDateTimeOffset
        | TemporalArrowToMssql::TimestampNanosecondTzToDateTimeOffset => {
            validate_null_timestamp_timezone_metadata(mapping, row_index)?;
            Ok(MssqlCell::DateTimeOffset(None))
        }
        classification => Err(unsupported_value_conversion(
            mapping,
            row_index,
            format!(
                "planned temporal mapping {classification:?} is not a datetimeoffset conversion"
            ),
        )),
    }
}

pub(crate) fn validate_null_timestamp_timezone_metadata(
    mapping: &SchemaMapping,
    row_index: usize,
) -> Result<()> {
    if let DataType::Timestamp(_, timezone) = mapping.arrow().data_type() {
        validate_timestamp_timezone_metadata(mapping, row_index, timezone.as_deref())?;
    }

    Ok(())
}

pub(crate) fn validate_mapping_timestamp_timezone_metadata(
    mapping: &SchemaMapping,
    row_index: usize,
) -> Result<()> {
    if let DataType::Timestamp(_, timezone) = mapping.arrow().data_type() {
        validate_timestamp_timezone_metadata(mapping, row_index, timezone.as_deref())
    } else {
        Ok(())
    }
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
    use super::{null_datetime2_cell, null_datetimeoffset_cell};
    use crate::{DiagnosticCode, Identifier, MssqlColumn, MssqlType, SchemaMapping};

    #[test]
    fn null_datetime2_cell_rejects_forged_unsupported_temporal_mapping() {
        let mapping = SchemaMapping::new(
            crate::ArrowFieldRef::new(0, "ts".to_owned(), true, arrow_schema::DataType::Date32),
            MssqlColumn::new(
                Identifier::new("ts").unwrap(),
                MssqlType::DateTime2 { precision: 7 },
                true,
            ),
        );

        let err = null_datetime2_cell(&mapping, 3).unwrap_err();

        assert_single_diagnostic(
            err,
            DiagnosticCode::ValueConversionUnsupported,
            Some(3),
            Some((0, "ts")),
        );
    }

    #[test]
    fn null_datetimeoffset_cell_rejects_forged_unsupported_temporal_mapping() {
        let mapping = SchemaMapping::new(
            crate::ArrowFieldRef::new(0, "ts".to_owned(), true, arrow_schema::DataType::Date32),
            MssqlColumn::new(
                Identifier::new("ts").unwrap(),
                MssqlType::DateTimeOffset { precision: 7 },
                true,
            ),
        );

        let err = null_datetimeoffset_cell(&mapping, 4).unwrap_err();

        assert_single_diagnostic(
            err,
            DiagnosticCode::ValueConversionUnsupported,
            Some(4),
            Some((0, "ts")),
        );
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

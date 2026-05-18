//! Arrow-to-MSSQL runtime cell conversion.

use arrow_array::timezone::Tz;
use arrow_schema::{DataType, TimeUnit};
use chrono::{Offset, TimeZone};

use crate::{
    Diagnostic, DiagnosticCode, DiagnosticSet, FieldRef, MssqlType, MssqlTypeLength,
    NanosecondPolicy, PlanOptions, Result, SchemaMapping, arrow::cell::ArrowCell,
};

use super::{MssqlCell, MssqlDate, MssqlDateTime2, MssqlDateTimeOffset, MssqlDecimal, MssqlTime};

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

/// Direction-specific runtime context for Arrow-to-MSSQL value conversion.
#[derive(Debug, Clone, Copy)]
pub(crate) struct ArrowToMssqlRuntimeMapping<'a> {
    mapping: &'a SchemaMapping,
    nanosecond_policy: NanosecondPolicy,
}

impl<'a> ArrowToMssqlRuntimeMapping<'a> {
    /// Creates runtime conversion context from structural mapping and write options.
    pub(crate) const fn new(mapping: &'a SchemaMapping, options: &PlanOptions) -> Self {
        Self {
            mapping,
            nanosecond_policy: options.nanosecond_policy,
        }
    }

    /// Returns the structural Arrow/MSSQL mapping.
    pub(crate) const fn mapping(self) -> &'a SchemaMapping {
        self.mapping
    }

    /// Returns the nanosecond timestamp policy selected for write conversion.
    pub(crate) const fn nanosecond_policy(self) -> NanosecondPolicy {
        self.nanosecond_policy
    }
}

fn mssql_bit_value(mapping: &SchemaMapping, row_index: usize, cell: ArrowCell<'_>) -> Result<bool> {
    match cell {
        ArrowCell::Boolean(value) => Ok(value),
        other => Err(value_conversion_error(row_mapping_diagnostic(
            mapping,
            row_index,
            DiagnosticCode::ValueTypeMismatch,
            format!("expected Arrow boolean payload, got {other:?}"),
        ))),
    }
}

fn mssql_tinyint_value(
    mapping: &SchemaMapping,
    row_index: usize,
    cell: ArrowCell<'_>,
) -> Result<u8> {
    match cell {
        ArrowCell::UInt8(value) => Ok(value),
        other => Err(value_conversion_error(row_mapping_diagnostic(
            mapping,
            row_index,
            DiagnosticCode::ValueTypeMismatch,
            format!("expected Arrow UInt8 payload, got {other:?}"),
        ))),
    }
}

fn mssql_smallint_value(
    mapping: &SchemaMapping,
    row_index: usize,
    cell: ArrowCell<'_>,
) -> Result<i16> {
    match cell {
        ArrowCell::Int8(value) => Ok(i16::from(value)),
        ArrowCell::Int16(value) => Ok(value),
        other => Err(value_conversion_error(row_mapping_diagnostic(
            mapping,
            row_index,
            DiagnosticCode::ValueTypeMismatch,
            format!("expected Arrow Int8 or Int16 payload, got {other:?}"),
        ))),
    }
}

fn mssql_int_value(mapping: &SchemaMapping, row_index: usize, cell: ArrowCell<'_>) -> Result<i32> {
    match cell {
        ArrowCell::Int32(value) => Ok(value),
        ArrowCell::UInt16(value) => Ok(i32::from(value)),
        other => Err(value_conversion_error(row_mapping_diagnostic(
            mapping,
            row_index,
            DiagnosticCode::ValueTypeMismatch,
            format!("expected Arrow Int32 or UInt16 payload, got {other:?}"),
        ))),
    }
}

fn mssql_bigint_value(
    mapping: &SchemaMapping,
    row_index: usize,
    cell: ArrowCell<'_>,
) -> Result<i64> {
    match cell {
        ArrowCell::Int64(value) => Ok(value),
        ArrowCell::UInt32(value) => Ok(i64::from(value)),
        ArrowCell::UInt64(value) => i64::try_from(value).map_err(|_| {
            value_conversion_error(row_mapping_diagnostic(
                mapping,
                row_index,
                DiagnosticCode::IntegerOutOfRange,
                format!("Arrow UInt64 value {value} does not fit planned SQL Server bigint"),
            ))
        }),
        other => Err(value_conversion_error(row_mapping_diagnostic(
            mapping,
            row_index,
            DiagnosticCode::ValueTypeMismatch,
            format!("expected Arrow Int64, UInt32, or UInt64 payload, got {other:?}"),
        ))),
    }
}

fn mssql_decimal_value(
    mapping: &SchemaMapping,
    row_index: usize,
    cell: ArrowCell<'_>,
) -> Result<MssqlDecimal> {
    let scale = decimal_scale(mapping, row_index)?;
    validate_decimal_scale_compatibility(mapping, row_index, scale)?;

    match (cell, mapping.arrow().data_type()) {
        (ArrowCell::UInt64(value), DataType::UInt64) if is_uint64_decimal20_0_mapping(mapping) => {
            mssql_decimal(mapping, row_index, i128::from(value), scale)
        }
        (ArrowCell::Decimal32(value), DataType::Decimal32(_, arrow_scale)) if *arrow_scale >= 0 => {
            mssql_decimal(mapping, row_index, i128::from(value), scale)
        }
        (ArrowCell::Decimal32(value), DataType::Decimal32(_, arrow_scale)) => {
            let value =
                normalize_negative_scale(mapping, row_index, i128::from(value), *arrow_scale)?;
            mssql_decimal(mapping, row_index, value, scale)
        }
        (ArrowCell::Decimal64(value), DataType::Decimal64(_, arrow_scale)) if *arrow_scale >= 0 => {
            mssql_decimal(mapping, row_index, i128::from(value), scale)
        }
        (ArrowCell::Decimal64(value), DataType::Decimal64(_, arrow_scale)) => {
            let value =
                normalize_negative_scale(mapping, row_index, i128::from(value), *arrow_scale)?;
            mssql_decimal(mapping, row_index, value, scale)
        }
        (ArrowCell::Decimal128(value), DataType::Decimal128(_, arrow_scale))
            if *arrow_scale >= 0 =>
        {
            mssql_decimal(mapping, row_index, value, scale)
        }
        (ArrowCell::Decimal128(value), DataType::Decimal128(_, arrow_scale)) => {
            let value = normalize_negative_scale(mapping, row_index, value, *arrow_scale)?;
            mssql_decimal(mapping, row_index, value, scale)
        }
        (ArrowCell::Decimal256(value), DataType::Decimal256(_, arrow_scale))
            if *arrow_scale >= 0 =>
        {
            let value = value.to_i128().ok_or_else(|| {
                value_conversion_error(row_mapping_diagnostic(
                    mapping,
                    row_index,
                    DiagnosticCode::DecimalOutOfRange,
                    "Arrow Decimal256 value does not fit runtime i128 decimal representation",
                ))
            })?;
            mssql_decimal(mapping, row_index, value, scale)
        }
        (ArrowCell::Decimal256(value), DataType::Decimal256(_, arrow_scale)) => {
            let value = value.to_i128().ok_or_else(|| {
                value_conversion_error(row_mapping_diagnostic(
                    mapping,
                    row_index,
                    DiagnosticCode::DecimalOutOfRange,
                    "Arrow Decimal256 value does not fit runtime i128 decimal representation",
                ))
            })?;
            let value = normalize_negative_scale(mapping, row_index, value, *arrow_scale)?;
            mssql_decimal(mapping, row_index, value, scale)
        }
        other => Err(value_conversion_error(row_mapping_diagnostic(
            mapping,
            row_index,
            DiagnosticCode::ValueTypeMismatch,
            format!("expected Arrow decimal-compatible payload, got {other:?}"),
        ))),
    }
}

fn mssql_date_value(
    mapping: &SchemaMapping,
    row_index: usize,
    cell: ArrowCell<'_>,
) -> Result<MssqlDate> {
    match (cell, mapping.arrow().data_type()) {
        (ArrowCell::Date32(value), DataType::Date32) => {
            mssql_date_from_arrow_date32(mapping, row_index, value)
        }
        other => Err(value_conversion_error(row_mapping_diagnostic(
            mapping,
            row_index,
            DiagnosticCode::ValueTypeMismatch,
            format!("expected Arrow Date32 payload, got {other:?}"),
        ))),
    }
}

fn mssql_datetime2_value(
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

fn mssql_datetimeoffset_value(
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

fn mssql_real_value(mapping: &SchemaMapping, row_index: usize, cell: ArrowCell<'_>) -> Result<f32> {
    match cell {
        ArrowCell::Float32(value) if value.is_finite() => Ok(value),
        ArrowCell::Float32(value) => Err(non_finite_float_error(mapping, row_index, value)),
        other => Err(value_conversion_error(row_mapping_diagnostic(
            mapping,
            row_index,
            DiagnosticCode::ValueTypeMismatch,
            format!("expected Arrow Float32 payload, got {other:?}"),
        ))),
    }
}

fn mssql_float_value(
    mapping: &SchemaMapping,
    row_index: usize,
    cell: ArrowCell<'_>,
) -> Result<f64> {
    match cell {
        ArrowCell::Float64(value) if value.is_finite() => Ok(value),
        ArrowCell::Float64(value) => Err(non_finite_float_error(mapping, row_index, value)),
        other => Err(value_conversion_error(row_mapping_diagnostic(
            mapping,
            row_index,
            DiagnosticCode::ValueTypeMismatch,
            format!("expected Arrow Float64 payload, got {other:?}"),
        ))),
    }
}

fn mssql_nvarchar_value<'a>(
    mapping: &SchemaMapping,
    row_index: usize,
    cell: ArrowCell<'a>,
) -> Result<&'a str> {
    match cell {
        ArrowCell::Utf8(value) => Ok(value),
        other => Err(value_conversion_error(row_mapping_diagnostic(
            mapping,
            row_index,
            DiagnosticCode::ValueTypeMismatch,
            format!("expected Arrow UTF-8 payload, got {other:?}"),
        ))),
    }
}

fn mssql_varbinary_value<'a>(
    mapping: &SchemaMapping,
    row_index: usize,
    cell: ArrowCell<'a>,
) -> Result<&'a [u8]> {
    match cell {
        ArrowCell::Binary(value) => Ok(value),
        other => Err(value_conversion_error(row_mapping_diagnostic(
            mapping,
            row_index,
            DiagnosticCode::ValueTypeMismatch,
            format!("expected Arrow binary payload, got {other:?}"),
        ))),
    }
}

pub(crate) fn mssql_cell_from_arrow_cell<'a>(
    runtime_mapping: ArrowToMssqlRuntimeMapping<'_>,
    cell: ArrowCell<'a>,
    row_index: usize,
) -> Result<MssqlCell<'a>> {
    let mapping = runtime_mapping.mapping();

    if matches!(cell, ArrowCell::Null) {
        if !mapping.mssql().nullable() {
            return Err(value_conversion_error(row_mapping_diagnostic(
                mapping,
                row_index,
                DiagnosticCode::NullInNonNullableColumn,
                "null value in non-nullable planned column",
            )));
        }

        return null_mssql_cell(mapping, row_index);
    }

    match mapping.mssql().ty() {
        MssqlType::Bit => Ok(MssqlCell::Bit(Some(mssql_bit_value(
            mapping, row_index, cell,
        )?))),
        MssqlType::TinyInt => Ok(MssqlCell::TinyInt(Some(mssql_tinyint_value(
            mapping, row_index, cell,
        )?))),
        MssqlType::SmallInt => Ok(MssqlCell::SmallInt(Some(mssql_smallint_value(
            mapping, row_index, cell,
        )?))),
        MssqlType::Int => Ok(MssqlCell::Int(Some(mssql_int_value(
            mapping, row_index, cell,
        )?))),
        MssqlType::BigInt => Ok(MssqlCell::BigInt(Some(mssql_bigint_value(
            mapping, row_index, cell,
        )?))),
        MssqlType::Decimal { .. } => Ok(MssqlCell::Decimal(Some(mssql_decimal_value(
            mapping, row_index, cell,
        )?))),
        MssqlType::Date => Ok(MssqlCell::Date(Some(mssql_date_value(
            mapping, row_index, cell,
        )?))),
        MssqlType::DateTime2 { .. } => Ok(MssqlCell::DateTime2(Some(mssql_datetime2_value(
            runtime_mapping,
            row_index,
            cell,
        )?))),
        MssqlType::DateTimeOffset { .. } => Ok(MssqlCell::DateTimeOffset(Some(
            mssql_datetimeoffset_value(runtime_mapping, row_index, cell)?,
        ))),
        MssqlType::Real => Ok(MssqlCell::Real(Some(mssql_real_value(
            mapping, row_index, cell,
        )?))),
        MssqlType::Float { .. } => Ok(MssqlCell::Float(Some(mssql_float_value(
            mapping, row_index, cell,
        )?))),
        MssqlType::NVarChar(length) => nvar_char_cell(mapping, row_index, *length, cell),
        MssqlType::VarBinary(length) => var_binary_cell(mapping, row_index, *length, cell),
    }
}

fn null_mssql_cell<'a>(mapping: &SchemaMapping, row_index: usize) -> Result<MssqlCell<'a>> {
    match mapping.mssql().ty() {
        MssqlType::Bit => Ok(MssqlCell::Bit(None)),
        MssqlType::TinyInt => Ok(MssqlCell::TinyInt(None)),
        MssqlType::SmallInt => Ok(MssqlCell::SmallInt(None)),
        MssqlType::Int => Ok(MssqlCell::Int(None)),
        MssqlType::BigInt => Ok(MssqlCell::BigInt(None)),
        MssqlType::Decimal { .. } if supports_null_decimal_cell(mapping) => {
            Ok(MssqlCell::Decimal(None))
        }
        MssqlType::Date => Ok(MssqlCell::Date(None)),
        MssqlType::DateTime2 { .. } => null_datetime2_cell(mapping, row_index),
        MssqlType::DateTimeOffset { .. } => null_datetimeoffset_cell(mapping, row_index),
        MssqlType::Real => Ok(MssqlCell::Real(None)),
        MssqlType::Float { .. } => Ok(MssqlCell::Float(None)),
        MssqlType::NVarChar(_) => Ok(MssqlCell::NVarChar(None)),
        MssqlType::VarBinary(_) => Ok(MssqlCell::VarBinary(None)),
        ty => Err(unsupported_value_conversion(
            mapping,
            row_index,
            format!(
                "planned SQL Server type {} is not supported yet",
                ty.to_sql()
            ),
        )),
    }
}

fn is_uint64_decimal20_0_mapping(mapping: &SchemaMapping) -> bool {
    matches!(
        (mapping.arrow().data_type(), mapping.mssql().ty()),
        (
            DataType::UInt64,
            MssqlType::Decimal {
                precision: 20,
                scale: 0
            }
        )
    )
}

fn supports_null_decimal_cell(mapping: &SchemaMapping) -> bool {
    matches!(
        mapping.arrow().data_type(),
        DataType::UInt64
            | DataType::Decimal32(_, _)
            | DataType::Decimal64(_, _)
            | DataType::Decimal128(_, _)
            | DataType::Decimal256(_, _)
    ) && matches!(mapping.mssql().ty(), MssqlType::Decimal { .. })
}

fn null_datetime2_cell<'a>(mapping: &SchemaMapping, row_index: usize) -> Result<MssqlCell<'a>> {
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

fn null_datetimeoffset_cell<'a>(
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

fn decimal_scale(mapping: &SchemaMapping, row_index: usize) -> Result<u8> {
    let MssqlType::Decimal { scale, .. } = mapping.mssql().ty() else {
        return Err(value_conversion_error(row_mapping_diagnostic(
            mapping,
            row_index,
            DiagnosticCode::ValueTypeMismatch,
            "planned SQL Server type is not decimal",
        )));
    };

    let scale = u8::try_from(*scale).map_err(|_| {
        value_conversion_error(row_mapping_diagnostic(
            mapping,
            row_index,
            DiagnosticCode::DecimalOutOfRange,
            format!(
                "planned SQL Server decimal scale {scale} cannot be represented by Tiberius Numeric"
            ),
        ))
    })?;

    if scale >= 38 {
        return Err(value_conversion_error(row_mapping_diagnostic(
            mapping,
            row_index,
            DiagnosticCode::DecimalOutOfRange,
            format!(
                "planned SQL Server decimal scale {scale} cannot be represented by Tiberius Numeric"
            ),
        )));
    }

    Ok(scale)
}

fn validate_decimal_scale_compatibility(
    mapping: &SchemaMapping,
    row_index: usize,
    planned_scale: u8,
) -> Result<()> {
    let expected_scale = match mapping.arrow().data_type() {
        DataType::UInt64 if is_uint64_decimal20_0_mapping(mapping) => 0,
        DataType::Decimal32(_, arrow_scale)
        | DataType::Decimal64(_, arrow_scale)
        | DataType::Decimal128(_, arrow_scale)
        | DataType::Decimal256(_, arrow_scale)
            if *arrow_scale < 0 =>
        {
            0
        }
        DataType::Decimal32(_, arrow_scale)
        | DataType::Decimal64(_, arrow_scale)
        | DataType::Decimal128(_, arrow_scale)
        | DataType::Decimal256(_, arrow_scale) => u8::try_from(*arrow_scale).map_err(|_| {
            value_conversion_error(row_mapping_diagnostic(
                mapping,
                row_index,
                DiagnosticCode::DecimalOutOfRange,
                format!("Arrow decimal scale {arrow_scale} cannot be represented at runtime"),
            ))
        })?,
        _ => return Ok(()),
    };

    if planned_scale == expected_scale {
        return Ok(());
    }

    Err(value_conversion_error(row_mapping_diagnostic(
        mapping,
        row_index,
        DiagnosticCode::SchemaMismatch,
        format!(
            "planned SQL Server decimal scale {planned_scale} is incompatible with Arrow decimal scale {expected_scale}"
        ),
    )))
}

fn mssql_decimal(
    mapping: &SchemaMapping,
    row_index: usize,
    unscaled: i128,
    scale: u8,
) -> Result<MssqlDecimal> {
    let MssqlType::Decimal { precision, .. } = mapping.mssql().ty() else {
        return Err(value_conversion_error(row_mapping_diagnostic(
            mapping,
            row_index,
            DiagnosticCode::ValueTypeMismatch,
            "planned SQL Server type is not decimal",
        )));
    };

    if decimal_unscaled_fits_precision(unscaled, *precision) {
        return Ok(MssqlDecimal::new(unscaled, scale));
    }

    Err(value_conversion_error(row_mapping_diagnostic(
        mapping,
        row_index,
        DiagnosticCode::DecimalOutOfRange,
        format!("decimal value {unscaled} does not fit planned precision {precision}"),
    )))
}

fn normalize_negative_scale(
    mapping: &SchemaMapping,
    row_index: usize,
    unscaled: i128,
    arrow_scale: i8,
) -> Result<i128> {
    if arrow_scale >= 0 {
        return Ok(unscaled);
    }

    let factor = 10_i128
        .checked_pow(u32::from(arrow_scale.unsigned_abs()))
        .ok_or_else(|| {
            value_conversion_error(row_mapping_diagnostic(
                mapping,
                row_index,
                DiagnosticCode::DecimalOutOfRange,
                format!("negative decimal scale {arrow_scale} normalization factor overflows"),
            ))
        })?;

    unscaled.checked_mul(factor).ok_or_else(|| {
        value_conversion_error(row_mapping_diagnostic(
            mapping,
            row_index,
            DiagnosticCode::DecimalOutOfRange,
            format!("decimal value {unscaled} overflows while normalizing scale {arrow_scale}"),
        ))
    })
}

fn decimal_unscaled_fits_precision(value: i128, precision: u8) -> bool {
    if precision == 0 {
        return false;
    }

    let Some(max) = decimal_max_unscaled(precision) else {
        return false;
    };

    value <= max && value >= -max
}

fn decimal_max_unscaled(precision: u8) -> Option<i128> {
    10_i128.checked_pow(u32::from(precision))?.checked_sub(1)
}

fn mssql_date_from_arrow_date32(
    mapping: &SchemaMapping,
    row_index: usize,
    days_from_unix_epoch: i32,
) -> Result<MssqlDate> {
    let days = i64::from(days_from_unix_epoch) + SQL_SERVER_DATE_UNIX_EPOCH_DAYS;

    if (0..=SQL_SERVER_DATE_MAX_DAYS).contains(&days) {
        return Ok(MssqlDate::new(days as u32));
    }

    Err(value_conversion_error(row_mapping_diagnostic(
        mapping,
        row_index,
        DiagnosticCode::TimestampOutOfRange,
        format!("Arrow Date32 day offset {days_from_unix_epoch} is outside SQL Server date range"),
    )))
}

fn mssql_datetime2_from_arrow_date64(
    mapping: &SchemaMapping,
    row_index: usize,
    milliseconds_from_unix_epoch: i64,
) -> Result<MssqlDateTime2> {
    let days_from_unix_epoch = milliseconds_from_unix_epoch.div_euclid(MILLISECONDS_PER_DAY);
    let milliseconds_since_midnight = milliseconds_from_unix_epoch.rem_euclid(MILLISECONDS_PER_DAY);
    let days = days_from_unix_epoch + SQL_SERVER_DATE_UNIX_EPOCH_DAYS;

    if !(0..=SQL_SERVER_DATE_MAX_DAYS).contains(&days) {
        return Err(value_conversion_error(row_mapping_diagnostic(
            mapping,
            row_index,
            DiagnosticCode::TimestampOutOfRange,
            format!(
                "Arrow Date64 millisecond value {milliseconds_from_unix_epoch} is outside SQL Server datetime2 range"
            ),
        )));
    }

    Ok(MssqlDateTime2::new(
        MssqlDate::new(days as u32),
        MssqlTime::new(
            milliseconds_since_midnight as u64,
            SQL_SERVER_DATETIME2_DATE64_SCALE,
        ),
    ))
}

fn nanoseconds_to_100ns_ticks(
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

fn mssql_datetime2_from_unix_epoch_100ns_ticks(
    mapping: &SchemaMapping,
    row_index: usize,
    ticks_from_unix_epoch: i128,
    unit_name: &str,
    source_value: i64,
) -> Result<MssqlDateTime2> {
    let days_from_unix_epoch = ticks_from_unix_epoch.div_euclid(TICKS_100NS_PER_DAY);
    let ticks_since_midnight = ticks_from_unix_epoch.rem_euclid(TICKS_100NS_PER_DAY);
    let days = days_from_unix_epoch + i128::from(SQL_SERVER_DATE_UNIX_EPOCH_DAYS);

    if !(0..=i128::from(SQL_SERVER_DATE_MAX_DAYS)).contains(&days) {
        return Err(value_conversion_error(row_mapping_diagnostic(
            mapping,
            row_index,
            DiagnosticCode::TimestampOutOfRange,
            format!(
                "Arrow timestamp {unit_name} value {source_value} is outside SQL Server datetime2 range"
            ),
        )));
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

    Ok(MssqlDateTime2::new(
        MssqlDate::new(days),
        MssqlTime::new(ticks_since_midnight, SQL_SERVER_DATETIME2_TIMESTAMP_SCALE),
    ))
}

fn mssql_datetime2_from_arrow_timestamp_second(
    mapping: &SchemaMapping,
    row_index: usize,
    seconds_from_unix_epoch: i64,
) -> Result<MssqlDateTime2> {
    let ticks = i128::from(seconds_from_unix_epoch) * TICKS_100NS_PER_SECOND;
    mssql_datetime2_from_unix_epoch_100ns_ticks(
        mapping,
        row_index,
        ticks,
        "second",
        seconds_from_unix_epoch,
    )
}

fn mssql_datetime2_from_arrow_timestamp_millisecond(
    mapping: &SchemaMapping,
    row_index: usize,
    milliseconds_from_unix_epoch: i64,
) -> Result<MssqlDateTime2> {
    let ticks = i128::from(milliseconds_from_unix_epoch) * TICKS_100NS_PER_MILLISECOND;
    mssql_datetime2_from_unix_epoch_100ns_ticks(
        mapping,
        row_index,
        ticks,
        "millisecond",
        milliseconds_from_unix_epoch,
    )
}

fn mssql_datetime2_from_arrow_timestamp_microsecond(
    mapping: &SchemaMapping,
    row_index: usize,
    microseconds_from_unix_epoch: i64,
) -> Result<MssqlDateTime2> {
    let ticks = i128::from(microseconds_from_unix_epoch) * TICKS_100NS_PER_MICROSECOND;
    mssql_datetime2_from_unix_epoch_100ns_ticks(
        mapping,
        row_index,
        ticks,
        "microsecond",
        microseconds_from_unix_epoch,
    )
}

fn mssql_datetime2_from_arrow_timestamp_nanosecond(
    mapping: &SchemaMapping,
    row_index: usize,
    nanoseconds_from_unix_epoch: i64,
    policy: NanosecondPolicy,
) -> Result<MssqlDateTime2> {
    let ticks =
        nanoseconds_to_100ns_ticks(mapping, row_index, nanoseconds_from_unix_epoch, policy)?;
    mssql_datetime2_from_unix_epoch_100ns_ticks(
        mapping,
        row_index,
        ticks,
        "nanosecond",
        nanoseconds_from_unix_epoch,
    )
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

fn nvar_char_cell<'a>(
    mapping: &SchemaMapping,
    row_index: usize,
    length: MssqlTypeLength,
    cell: ArrowCell<'a>,
) -> Result<MssqlCell<'a>> {
    let value = mssql_nvarchar_value(mapping, row_index, cell)?;
    let code_units = value.encode_utf16().count();

    if exceeds_length(length, code_units) {
        return Err(value_too_long_error(
            mapping,
            row_index,
            format!(
                "string value has {code_units} UTF-16 code unit(s), exceeding planned {}",
                mapping.mssql().ty().to_sql()
            ),
        ));
    }

    Ok(MssqlCell::NVarChar(Some(value)))
}

fn var_binary_cell<'a>(
    mapping: &SchemaMapping,
    row_index: usize,
    length: MssqlTypeLength,
    cell: ArrowCell<'a>,
) -> Result<MssqlCell<'a>> {
    let value = mssql_varbinary_value(mapping, row_index, cell)?;
    let bytes = value.len();

    if exceeds_length(length, bytes) {
        return Err(value_too_long_error(
            mapping,
            row_index,
            format!(
                "binary value has {bytes} byte(s), exceeding planned {}",
                mapping.mssql().ty().to_sql()
            ),
        ));
    }

    Ok(MssqlCell::VarBinary(Some(value)))
}

fn exceeds_length(length: MssqlTypeLength, actual: usize) -> bool {
    match length {
        MssqlTypeLength::Bounded(limit) => actual > limit,
        MssqlTypeLength::Max => false,
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

fn unsupported_value_conversion(
    mapping: &SchemaMapping,
    row_index: usize,
    message: impl Into<String>,
) -> crate::Error {
    value_conversion_error(row_mapping_diagnostic(
        mapping,
        row_index,
        DiagnosticCode::ValueConversionUnsupported,
        message,
    ))
}

fn non_finite_float_error(
    mapping: &SchemaMapping,
    row_index: usize,
    value: impl std::fmt::Display,
) -> crate::Error {
    value_conversion_error(row_mapping_diagnostic(
        mapping,
        row_index,
        DiagnosticCode::NonFiniteFloat,
        format!("non-finite floating point value {value} is not supported"),
    ))
}

fn value_too_long_error(
    mapping: &SchemaMapping,
    row_index: usize,
    message: impl Into<String>,
) -> crate::Error {
    value_conversion_error(row_mapping_diagnostic(
        mapping,
        row_index,
        DiagnosticCode::ValueTooLong,
        message,
    ))
}

fn mapping_diagnostic(
    mapping: &SchemaMapping,
    code: DiagnosticCode,
    message: impl Into<String>,
) -> Diagnostic {
    Diagnostic::error(code, message).with_field(FieldRef::new(
        mapping.arrow().index(),
        mapping.arrow().name(),
    ))
}

fn row_mapping_diagnostic(
    mapping: &SchemaMapping,
    row_index: usize,
    code: DiagnosticCode,
    message: impl Into<String>,
) -> Diagnostic {
    mapping_diagnostic(mapping, code, message).with_row(row_index)
}

fn value_conversion_error(diagnostic: Diagnostic) -> crate::Error {
    crate::Error::ValueConversion {
        diagnostics: DiagnosticSet::from(vec![diagnostic]),
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_buffer::i256;
    use arrow_schema::{DataType, Field, Schema, TimeUnit};

    use super::{
        ArrowToMssqlRuntimeMapping, MssqlCell, MssqlDate, MssqlDateTime2, MssqlDecimal, MssqlTime,
        mssql_cell_from_arrow_cell,
    };
    use crate::{
        ArrowFieldRef, Date64Policy, DecimalPolicy, DiagnosticCode, Identifier, MssqlColumn,
        MssqlProfile, MssqlType, NanosecondPolicy, PlanOptions, SchemaMapping, UInt64Policy,
        arrow::cell::ArrowCell, plan_arrow_schema_to_mssql_mappings,
    };

    #[test]
    fn runtime_mapping_keeps_write_policy_out_of_schema_mapping() {
        let options = PlanOptions {
            nanosecond_policy: NanosecondPolicy::TruncateTo100ns,
            ..PlanOptions::default()
        };
        let mappings = mappings_for_schema_with_options(
            Schema::new(vec![Field::new(
                "created_at",
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                true,
            )]),
            options,
        );

        let runtime_mapping = ArrowToMssqlRuntimeMapping::new(&mappings[0], &options);

        assert_eq!(runtime_mapping.mapping(), &mappings[0]);
        assert_eq!(
            runtime_mapping.nanosecond_policy(),
            NanosecondPolicy::TruncateTo100ns
        );
        assert_eq!(
            mappings[0].mssql().ty(),
            &MssqlType::DateTime2 { precision: 7 }
        );
    }

    #[test]
    fn converts_supported_initial_primitives_to_mssql_cells() {
        let mappings = mappings_for_schema(Schema::new(vec![
            Field::new("active", DataType::Boolean, true),
            Field::new("tiny", DataType::Int8, true),
            Field::new("small", DataType::Int16, true),
            Field::new("quantity", DataType::Int32, true),
            Field::new("total", DataType::Int64, true),
            Field::new("unsigned_tiny", DataType::UInt8, true),
            Field::new("unsigned_medium", DataType::UInt16, true),
            Field::new("unsigned_large", DataType::UInt32, true),
            Field::new("real_value", DataType::Float32, true),
            Field::new("float_value", DataType::Float64, true),
            Field::new("text", DataType::Utf8, true),
            Field::new("large_text", DataType::LargeUtf8, true),
            Field::new("bytes", DataType::Binary, true),
            Field::new("large_bytes", DataType::LargeBinary, true),
        ]));
        let cases = [
            (0, ArrowCell::Boolean(true), MssqlCell::Bit(Some(true))),
            (1, ArrowCell::Int8(-8), MssqlCell::SmallInt(Some(-8))),
            (2, ArrowCell::Int16(-16), MssqlCell::SmallInt(Some(-16))),
            (3, ArrowCell::Int32(12), MssqlCell::Int(Some(12))),
            (4, ArrowCell::Int64(34), MssqlCell::BigInt(Some(34))),
            (5, ArrowCell::UInt8(8), MssqlCell::TinyInt(Some(8))),
            (6, ArrowCell::UInt16(16), MssqlCell::Int(Some(16))),
            (7, ArrowCell::UInt32(32), MssqlCell::BigInt(Some(32))),
            (8, ArrowCell::Float32(1.25), MssqlCell::Real(Some(1.25))),
            (9, ArrowCell::Float64(2.5), MssqlCell::Float(Some(2.5))),
            (
                10,
                ArrowCell::Utf8("hello"),
                MssqlCell::NVarChar(Some("hello")),
            ),
            (
                11,
                ArrowCell::Utf8("Tokyo"),
                MssqlCell::NVarChar(Some("Tokyo")),
            ),
            (
                12,
                ArrowCell::Binary(b"abc"),
                MssqlCell::VarBinary(Some(b"abc")),
            ),
            (
                13,
                ArrowCell::Binary(b"large"),
                MssqlCell::VarBinary(Some(b"large")),
            ),
        ];

        for (index, arrow_cell, expected) in cases {
            assert_eq!(
                convert_cell(&mappings[index], arrow_cell, 0).unwrap(),
                expected
            );
        }

        let null_cases = [
            (0, MssqlCell::Bit(None)),
            (1, MssqlCell::SmallInt(None)),
            (2, MssqlCell::SmallInt(None)),
            (3, MssqlCell::Int(None)),
            (4, MssqlCell::BigInt(None)),
            (5, MssqlCell::TinyInt(None)),
            (6, MssqlCell::Int(None)),
            (7, MssqlCell::BigInt(None)),
            (8, MssqlCell::Real(None)),
            (9, MssqlCell::Float(None)),
            (10, MssqlCell::NVarChar(None)),
            (11, MssqlCell::NVarChar(None)),
            (12, MssqlCell::VarBinary(None)),
            (13, MssqlCell::VarBinary(None)),
        ];

        for (index, expected) in null_cases {
            assert_eq!(
                convert_cell(&mappings[index], ArrowCell::Null, 1).unwrap(),
                expected
            );
        }
    }

    #[test]
    fn converts_empty_ascii_and_non_ascii_strings() {
        let mappings = mappings_for_schema(Schema::new(vec![
            Field::new("text", DataType::Utf8, true),
            Field::new("large_text", DataType::LargeUtf8, true),
        ]));

        assert_eq!(
            convert_cell(&mappings[0], ArrowCell::Utf8(""), 0).unwrap(),
            MssqlCell::NVarChar(Some(""))
        );
        assert_eq!(
            convert_cell(&mappings[0], ArrowCell::Utf8("ascii"), 1).unwrap(),
            MssqlCell::NVarChar(Some("ascii"))
        );
        assert_eq!(
            convert_cell(&mappings[0], ArrowCell::Utf8("Tokyo"), 2).unwrap(),
            MssqlCell::NVarChar(Some("Tokyo"))
        );
        assert_eq!(
            convert_cell(&mappings[1], ArrowCell::Utf8(""), 0).unwrap(),
            MssqlCell::NVarChar(Some(""))
        );
        assert_eq!(
            convert_cell(&mappings[1], ArrowCell::Utf8("ascii"), 1).unwrap(),
            MssqlCell::NVarChar(Some("ascii"))
        );
        assert_eq!(
            convert_cell(&mappings[1], ArrowCell::Utf8("emoji"), 2).unwrap(),
            MssqlCell::NVarChar(Some("emoji"))
        );
    }

    #[test]
    fn converts_empty_and_non_empty_binary_values() {
        let mappings = mappings_for_schema(Schema::new(vec![
            Field::new("bytes", DataType::Binary, true),
            Field::new("large_bytes", DataType::LargeBinary, true),
        ]));

        assert_eq!(
            convert_cell(&mappings[0], ArrowCell::Binary(b""), 0).unwrap(),
            MssqlCell::VarBinary(Some(b""))
        );
        assert_eq!(
            convert_cell(&mappings[0], ArrowCell::Binary(b"abc"), 1).unwrap(),
            MssqlCell::VarBinary(Some(b"abc"))
        );
        assert_eq!(
            convert_cell(&mappings[1], ArrowCell::Binary(b""), 0).unwrap(),
            MssqlCell::VarBinary(Some(b""))
        );
        assert_eq!(
            convert_cell(&mappings[1], ArrowCell::Binary(b"large"), 1).unwrap(),
            MssqlCell::VarBinary(Some(b"large"))
        );
    }

    #[test]
    fn rejects_bounded_nvarchar_by_utf16_code_units() {
        let mappings = mappings_for_schema_with_options(
            Schema::new(vec![Field::new("text", DataType::Utf8, true)]),
            PlanOptions {
                string_policy: crate::StringPolicy::NVarChar(2),
                ..PlanOptions::default()
            },
        );

        assert_eq!(
            convert_cell(&mappings[0], ArrowCell::Utf8("ab"), 0).unwrap(),
            MssqlCell::NVarChar(Some("ab"))
        );
        assert_eq!(
            convert_cell(&mappings[0], ArrowCell::Utf8("🙂"), 1).unwrap(),
            MssqlCell::NVarChar(Some("🙂"))
        );
        let err = convert_cell(&mappings[0], ArrowCell::Utf8("abc"), 2).unwrap_err();

        assert_single_diagnostic(
            err,
            DiagnosticCode::ValueTooLong,
            Some(2),
            Some((0, "text")),
        );
    }

    #[test]
    fn rejects_bounded_varbinary_by_byte_count() {
        let mappings = mappings_for_schema_with_options(
            Schema::new(vec![Field::new("bytes", DataType::Binary, true)]),
            PlanOptions {
                binary_policy: crate::BinaryPolicy::VarBinary(2),
                ..PlanOptions::default()
            },
        );

        assert_eq!(
            convert_cell(&mappings[0], ArrowCell::Binary(b""), 0).unwrap(),
            MssqlCell::VarBinary(Some(b""))
        );
        assert_eq!(
            convert_cell(&mappings[0], ArrowCell::Binary(b"ab"), 1).unwrap(),
            MssqlCell::VarBinary(Some(b"ab"))
        );
        let err = convert_cell(&mappings[0], ArrowCell::Binary(b"abc"), 2).unwrap_err();

        assert_single_diagnostic(
            err,
            DiagnosticCode::ValueTooLong,
            Some(2),
            Some((0, "bytes")),
        );
    }

    #[test]
    fn converts_uint64_decimal20_0_boundary_values() {
        let mappings = mappings_for_schema_with_options(
            Schema::new(vec![Field::new(
                "unsigned_as_decimal",
                DataType::UInt64,
                true,
            )]),
            PlanOptions {
                uint64_policy: UInt64Policy::Decimal20_0,
                ..PlanOptions::default()
            },
        );

        assert_eq!(
            convert_cell(&mappings[0], ArrowCell::UInt64(0), 0).unwrap(),
            MssqlCell::Decimal(Some(MssqlDecimal::new(0, 0)))
        );
        assert_eq!(
            convert_cell(&mappings[0], ArrowCell::UInt64(i64::MAX as u64), 1).unwrap(),
            MssqlCell::Decimal(Some(MssqlDecimal::new(i128::from(i64::MAX), 0)))
        );
        assert_eq!(
            convert_cell(&mappings[0], ArrowCell::UInt64((i64::MAX as u64) + 1), 2).unwrap(),
            MssqlCell::Decimal(Some(MssqlDecimal::new(i128::from(i64::MAX) + 1, 0)))
        );
        assert_eq!(
            convert_cell(&mappings[0], ArrowCell::UInt64(u64::MAX), 3).unwrap(),
            MssqlCell::Decimal(Some(MssqlDecimal::new(i128::from(u64::MAX), 0)))
        );
        assert_eq!(
            convert_cell(&mappings[0], ArrowCell::Null, 4).unwrap(),
            MssqlCell::Decimal(None)
        );
    }

    #[test]
    fn converts_decimal32_64_128_cells_with_sign_zero_scale_and_null() {
        let mappings = mappings_for_schema(Schema::new(vec![
            Field::new("decimal32", DataType::Decimal32(9, 2), true),
            Field::new("decimal64", DataType::Decimal64(18, 4), true),
            Field::new("decimal128", DataType::Decimal128(38, 9), true),
        ]));

        let cases = [
            (
                0,
                ArrowCell::Decimal32(12_345),
                MssqlCell::Decimal(Some(MssqlDecimal::new(12_345, 2))),
            ),
            (
                0,
                ArrowCell::Decimal32(-12_345),
                MssqlCell::Decimal(Some(MssqlDecimal::new(-12_345, 2))),
            ),
            (
                0,
                ArrowCell::Decimal32(0),
                MssqlCell::Decimal(Some(MssqlDecimal::new(0, 2))),
            ),
            (
                1,
                ArrowCell::Decimal64(1_234_567_890),
                MssqlCell::Decimal(Some(MssqlDecimal::new(1_234_567_890, 4))),
            ),
            (
                1,
                ArrowCell::Decimal64(-1_234_567_890),
                MssqlCell::Decimal(Some(MssqlDecimal::new(-1_234_567_890, 4))),
            ),
            (
                1,
                ArrowCell::Decimal64(0),
                MssqlCell::Decimal(Some(MssqlDecimal::new(0, 4))),
            ),
            (
                2,
                ArrowCell::Decimal128(123_456_789_012_345_678_901_234_567_890),
                MssqlCell::Decimal(Some(MssqlDecimal::new(
                    123_456_789_012_345_678_901_234_567_890,
                    9,
                ))),
            ),
            (
                2,
                ArrowCell::Decimal128(-123_456_789_012_345_678_901_234_567_890),
                MssqlCell::Decimal(Some(MssqlDecimal::new(
                    -123_456_789_012_345_678_901_234_567_890,
                    9,
                ))),
            ),
            (
                2,
                ArrowCell::Decimal128(0),
                MssqlCell::Decimal(Some(MssqlDecimal::new(0, 9))),
            ),
        ];

        for (index, cell, expected) in cases {
            assert_eq!(convert_cell(&mappings[index], cell, 0).unwrap(), expected);
        }

        for mapping in mappings.iter().take(3) {
            assert_eq!(
                convert_cell(mapping, ArrowCell::Null, 3).unwrap(),
                MssqlCell::Decimal(None)
            );
        }
    }

    #[test]
    fn normalizes_negative_decimal_scale_at_runtime() {
        let mappings = mappings_for_schema_with_options(
            Schema::new(vec![Field::new(
                "amount",
                DataType::Decimal128(3, -2),
                true,
            )]),
            PlanOptions {
                decimal_policy: DecimalPolicy::NormalizeNegativeScale,
                ..PlanOptions::default()
            },
        );

        assert_eq!(
            convert_cell(&mappings[0], ArrowCell::Decimal128(123), 0).unwrap(),
            MssqlCell::Decimal(Some(MssqlDecimal::new(12_300, 0)))
        );
        assert_eq!(
            convert_cell(&mappings[0], ArrowCell::Decimal128(-123), 1).unwrap(),
            MssqlCell::Decimal(Some(MssqlDecimal::new(-12_300, 0)))
        );
        assert_eq!(
            convert_cell(&mappings[0], ArrowCell::Decimal128(0), 2).unwrap(),
            MssqlCell::Decimal(Some(MssqlDecimal::new(0, 0)))
        );
        assert_eq!(
            convert_cell(&mappings[0], ArrowCell::Null, 3).unwrap(),
            MssqlCell::Decimal(None)
        );
    }

    #[test]
    fn rejects_negative_decimal_scale_normalization_overflow() {
        let mappings = mappings_for_schema_with_options(
            Schema::new(vec![Field::new(
                "amount",
                DataType::Decimal128(37, -1),
                false,
            )]),
            PlanOptions {
                decimal_policy: DecimalPolicy::NormalizeNegativeScale,
                ..PlanOptions::default()
            },
        );

        let err = convert_cell(&mappings[0], ArrowCell::Decimal128(i128::MAX), 0).unwrap_err();

        assert_single_diagnostic(
            err,
            DiagnosticCode::DecimalOutOfRange,
            Some(0),
            Some((0, "amount")),
        );
    }

    #[test]
    fn rejects_decimal_scale_that_tiberius_numeric_cannot_represent() {
        let mappings = mappings_for_schema(Schema::new(vec![Field::new(
            "amount",
            DataType::Decimal128(38, 38),
            true,
        )]));

        let err = convert_cell(&mappings[0], ArrowCell::Decimal128(1), 0).unwrap_err();

        assert_single_diagnostic(
            err,
            DiagnosticCode::DecimalOutOfRange,
            Some(0),
            Some((0, "amount")),
        );
    }

    #[test]
    fn accepts_decimal_values_at_planned_precision_boundaries() {
        let mappings = mappings_for_schema(Schema::new(vec![Field::new(
            "amount",
            DataType::Decimal128(5, 2),
            false,
        )]));

        assert_eq!(
            convert_cell(&mappings[0], ArrowCell::Decimal128(99_999), 0).unwrap(),
            MssqlCell::Decimal(Some(MssqlDecimal::new(99_999, 2)))
        );
        assert_eq!(
            convert_cell(&mappings[0], ArrowCell::Decimal128(-99_999), 1).unwrap(),
            MssqlCell::Decimal(Some(MssqlDecimal::new(-99_999, 2)))
        );
    }

    #[test]
    fn rejects_decimal_values_outside_planned_precision() {
        let mappings = mappings_for_schema(Schema::new(vec![Field::new(
            "amount",
            DataType::Decimal128(5, 2),
            false,
        )]));

        let positive = convert_cell(&mappings[0], ArrowCell::Decimal128(100_000), 0).unwrap_err();
        assert_single_diagnostic(
            positive,
            DiagnosticCode::DecimalOutOfRange,
            Some(0),
            Some((0, "amount")),
        );

        let negative = convert_cell(&mappings[0], ArrowCell::Decimal128(-100_000), 1).unwrap_err();
        assert_single_diagnostic(
            negative,
            DiagnosticCode::DecimalOutOfRange,
            Some(1),
            Some((0, "amount")),
        );
    }

    #[test]
    fn converts_uint64_checked_bigint_boundary_values() {
        let mappings = mappings_for_schema_with_options(
            Schema::new(vec![Field::new(
                "unsigned_as_bigint",
                DataType::UInt64,
                true,
            )]),
            PlanOptions {
                uint64_policy: UInt64Policy::CheckedBigInt,
                ..PlanOptions::default()
            },
        );

        assert_eq!(
            convert_cell(&mappings[0], ArrowCell::UInt64(0), 0).unwrap(),
            MssqlCell::BigInt(Some(0))
        );
        assert_eq!(
            convert_cell(&mappings[0], ArrowCell::UInt64(i64::MAX as u64), 1).unwrap(),
            MssqlCell::BigInt(Some(i64::MAX))
        );
        assert_eq!(
            convert_cell(&mappings[0], ArrowCell::Null, 2).unwrap(),
            MssqlCell::BigInt(None)
        );
    }

    #[test]
    fn rejects_uint64_checked_bigint_overflow_without_wrapping() {
        let mappings = mappings_for_schema_with_options(
            Schema::new(vec![Field::new(
                "unsigned_as_bigint",
                DataType::UInt64,
                false,
            )]),
            PlanOptions {
                uint64_policy: UInt64Policy::CheckedBigInt,
                ..PlanOptions::default()
            },
        );

        let just_over =
            convert_cell(&mappings[0], ArrowCell::UInt64((i64::MAX as u64) + 1), 0).unwrap_err();
        assert_single_diagnostic(
            just_over,
            DiagnosticCode::IntegerOutOfRange,
            Some(0),
            Some((0, "unsigned_as_bigint")),
        );

        let max = convert_cell(&mappings[0], ArrowCell::UInt64(u64::MAX), 1).unwrap_err();
        assert_single_diagnostic(
            max,
            DiagnosticCode::IntegerOutOfRange,
            Some(1),
            Some((0, "unsigned_as_bigint")),
        );
    }

    #[test]
    fn converts_decimal256_checked_downcast_values() {
        let mappings = mappings_for_schema(Schema::new(vec![Field::new(
            "amount",
            DataType::Decimal256(38, 4),
            true,
        )]));

        assert_eq!(
            convert_cell(
                &mappings[0],
                ArrowCell::Decimal256(i256::from_i128(123_456_789_012_345_678_901_234_567_890)),
                0,
            )
            .unwrap(),
            MssqlCell::Decimal(Some(MssqlDecimal::new(
                123_456_789_012_345_678_901_234_567_890,
                4,
            )))
        );
        assert_eq!(
            convert_cell(
                &mappings[0],
                ArrowCell::Decimal256(i256::from_i128(-123_456_789_012_345_678_901_234_567_890)),
                1,
            )
            .unwrap(),
            MssqlCell::Decimal(Some(MssqlDecimal::new(
                -123_456_789_012_345_678_901_234_567_890,
                4,
            )))
        );
        assert_eq!(
            convert_cell(&mappings[0], ArrowCell::Decimal256(i256::ZERO), 2).unwrap(),
            MssqlCell::Decimal(Some(MssqlDecimal::new(0, 4)))
        );
        assert_eq!(
            convert_cell(&mappings[0], ArrowCell::Null, 3).unwrap(),
            MssqlCell::Decimal(None)
        );
    }

    #[test]
    fn rejects_decimal256_values_that_do_not_fit_i128_runtime_representation() {
        let mappings = mappings_for_schema(Schema::new(vec![Field::new(
            "amount",
            DataType::Decimal256(38, 0),
            false,
        )]));

        let err = convert_cell(
            &mappings[0],
            ArrowCell::Decimal256(i256::from_i128(i128::MAX) + i256::ONE),
            0,
        )
        .unwrap_err();

        assert_single_diagnostic(
            err,
            DiagnosticCode::DecimalOutOfRange,
            Some(0),
            Some((0, "amount")),
        );
    }

    #[test]
    fn rejects_decimal256_checked_downcast_values_outside_planned_precision() {
        let mappings = mappings_for_schema(Schema::new(vec![Field::new(
            "amount",
            DataType::Decimal256(5, 2),
            false,
        )]));

        let err = convert_cell(
            &mappings[0],
            ArrowCell::Decimal256(i256::from_i128(100_000)),
            0,
        )
        .unwrap_err();

        assert_single_diagnostic(
            err,
            DiagnosticCode::DecimalOutOfRange,
            Some(0),
            Some((0, "amount")),
        );
    }

    #[test]
    fn converts_date32_cells_to_mssql_date_with_boundaries_and_null() {
        let mappings = mappings_for_schema(Schema::new(vec![Field::new(
            "date_value",
            DataType::Date32,
            true,
        )]));
        let cases = [
            (
                0,
                ArrowCell::Date32(0),
                MssqlCell::Date(Some(MssqlDate::new(719_162))),
            ),
            (
                1,
                ArrowCell::Date32(-1),
                MssqlCell::Date(Some(MssqlDate::new(719_161))),
            ),
            (
                2,
                ArrowCell::Date32(1),
                MssqlCell::Date(Some(MssqlDate::new(719_163))),
            ),
            (
                3,
                ArrowCell::Date32(-719_162),
                MssqlCell::Date(Some(MssqlDate::new(0))),
            ),
            (
                4,
                ArrowCell::Date32(2_932_896),
                MssqlCell::Date(Some(MssqlDate::new(3_652_058))),
            ),
            (5, ArrowCell::Null, MssqlCell::Date(None)),
        ];

        for (row_index, cell, expected) in cases {
            assert_eq!(
                convert_cell(&mappings[0], cell, row_index).unwrap(),
                expected
            );
        }
    }

    #[test]
    fn rejects_date32_null_in_non_nullable_column() {
        let mappings = mappings_for_schema(Schema::new(vec![Field::new(
            "date_value",
            DataType::Date32,
            false,
        )]));

        let err = convert_cell(&mappings[0], ArrowCell::Null, 0).unwrap_err();

        assert_single_diagnostic(
            err,
            DiagnosticCode::NullInNonNullableColumn,
            Some(0),
            Some((0, "date_value")),
        );
    }

    #[test]
    fn rejects_date32_values_outside_sql_server_date_range() {
        let mappings = mappings_for_schema(Schema::new(vec![Field::new(
            "date_value",
            DataType::Date32,
            false,
        )]));

        let below = convert_cell(&mappings[0], ArrowCell::Date32(-719_163), 0).unwrap_err();
        assert_single_diagnostic(
            below,
            DiagnosticCode::TimestampOutOfRange,
            Some(0),
            Some((0, "date_value")),
        );

        let above = convert_cell(&mappings[0], ArrowCell::Date32(2_932_897), 1).unwrap_err();
        assert_single_diagnostic(
            above,
            DiagnosticCode::TimestampOutOfRange,
            Some(1),
            Some((0, "date_value")),
        );
    }

    #[test]
    fn converts_date64_cells_to_mssql_datetime2_with_boundaries_and_null() {
        let mappings = mappings_for_schema_with_options(
            Schema::new(vec![Field::new("date_value", DataType::Date64, true)]),
            PlanOptions {
                date64_policy: Date64Policy::TimestampDateTime2,
                ..PlanOptions::default()
            },
        );
        let cases = [
            (
                0,
                ArrowCell::Date64(0),
                MssqlCell::DateTime2(Some(MssqlDateTime2::new(
                    MssqlDate::new(719_162),
                    MssqlTime::new(0, 3),
                ))),
            ),
            (
                1,
                ArrowCell::Date64(-1),
                MssqlCell::DateTime2(Some(MssqlDateTime2::new(
                    MssqlDate::new(719_161),
                    MssqlTime::new(86_399_999, 3),
                ))),
            ),
            (
                2,
                ArrowCell::Date64(86_400_123),
                MssqlCell::DateTime2(Some(MssqlDateTime2::new(
                    MssqlDate::new(719_163),
                    MssqlTime::new(123, 3),
                ))),
            ),
            (
                3,
                ArrowCell::Date64(-62_135_596_800_000),
                MssqlCell::DateTime2(Some(MssqlDateTime2::new(
                    MssqlDate::new(0),
                    MssqlTime::new(0, 3),
                ))),
            ),
            (
                4,
                ArrowCell::Date64(253_402_300_799_999),
                MssqlCell::DateTime2(Some(MssqlDateTime2::new(
                    MssqlDate::new(3_652_058),
                    MssqlTime::new(86_399_999, 3),
                ))),
            ),
            (5, ArrowCell::Null, MssqlCell::DateTime2(None)),
        ];

        for (row_index, cell, expected) in cases {
            assert_eq!(
                convert_cell(&mappings[0], cell, row_index).unwrap(),
                expected
            );
        }
    }

    #[test]
    fn rejects_date64_null_in_non_nullable_column() {
        let mappings = mappings_for_schema_with_options(
            Schema::new(vec![Field::new("date_value", DataType::Date64, false)]),
            PlanOptions {
                date64_policy: Date64Policy::TimestampDateTime2,
                ..PlanOptions::default()
            },
        );

        let err = convert_cell(&mappings[0], ArrowCell::Null, 0).unwrap_err();

        assert_single_diagnostic(
            err,
            DiagnosticCode::NullInNonNullableColumn,
            Some(0),
            Some((0, "date_value")),
        );
    }

    #[test]
    fn rejects_date64_values_outside_sql_server_datetime2_range() {
        let mappings = mappings_for_schema_with_options(
            Schema::new(vec![Field::new("date_value", DataType::Date64, false)]),
            PlanOptions {
                date64_policy: Date64Policy::TimestampDateTime2,
                ..PlanOptions::default()
            },
        );

        let below =
            convert_cell(&mappings[0], ArrowCell::Date64(-62_135_596_800_001), 0).unwrap_err();
        assert_single_diagnostic(
            below,
            DiagnosticCode::TimestampOutOfRange,
            Some(0),
            Some((0, "date_value")),
        );

        let above =
            convert_cell(&mappings[0], ArrowCell::Date64(253_402_300_800_000), 1).unwrap_err();
        assert_single_diagnostic(
            above,
            DiagnosticCode::TimestampOutOfRange,
            Some(1),
            Some((0, "date_value")),
        );
    }

    #[test]
    fn rejects_forged_date64_mapping_with_unsupported_datetime2_precision() {
        let mapping = SchemaMapping::new(
            ArrowFieldRef::new(0, "date_value".to_owned(), false, DataType::Date64),
            MssqlColumn::new(
                Identifier::new("date_value").unwrap(),
                MssqlType::DateTime2 { precision: 7 },
                false,
            ),
        );

        let err = convert_cell(&mapping, ArrowCell::Date64(0), 0).unwrap_err();

        assert_single_diagnostic(
            err,
            DiagnosticCode::ValueTypeMismatch,
            Some(0),
            Some((0, "date_value")),
        );
    }

    fn convert_cell<'a>(
        mapping: &SchemaMapping,
        cell: ArrowCell<'a>,
        row_index: usize,
    ) -> crate::Result<MssqlCell<'a>> {
        let options = PlanOptions::default();
        let runtime_mapping = ArrowToMssqlRuntimeMapping::new(mapping, &options);
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

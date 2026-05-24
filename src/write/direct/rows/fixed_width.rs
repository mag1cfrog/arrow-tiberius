//! Fixed-width primitive direct TDS row layout.

use arrow_array::{
    Array, BooleanArray, Date32Array, Date64Array, Float32Array, Float64Array, Int8Array,
    Int16Array, Int32Array, Int64Array, RecordBatch, Time32MillisecondArray, Time32SecondArray,
    Time64MicrosecondArray, Time64NanosecondArray, TimestampMicrosecondArray,
    TimestampMillisecondArray, TimestampNanosecondArray, TimestampSecondArray, UInt8Array,
    UInt16Array, UInt32Array, UInt64Array,
};

use crate::{
    Diagnostic, DiagnosticCode, Error, FieldRef, NanosecondPolicy, PlanOptions, Result,
    SchemaMapping,
    conversion::arrow_to_mssql::{
        primitive::PrimitiveArrowToMssql, temporal::TemporalArrowToMssql,
    },
    mssql::cell::{
        MssqlDateTime2, MssqlDateTimeOffset, MssqlTime,
        from_arrow::temporal::datetime2::{
            mssql_datetime2_from_arrow_timestamp_microsecond,
            mssql_datetime2_from_arrow_timestamp_millisecond,
            mssql_datetime2_from_arrow_timestamp_nanosecond,
            mssql_datetime2_from_arrow_timestamp_second,
        },
        from_arrow::temporal::datetimeoffset::{
            mssql_datetimeoffset_from_arrow_timestamp_microsecond,
            mssql_datetimeoffset_from_arrow_timestamp_millisecond,
            mssql_datetimeoffset_from_arrow_timestamp_nanosecond,
            mssql_datetimeoffset_from_arrow_timestamp_second,
        },
        from_arrow::temporal::time::{
            mssql_time_from_arrow_time32_millisecond, mssql_time_from_arrow_time32_second,
            mssql_time_from_arrow_time64_microsecond, mssql_time_from_arrow_time64_nanosecond,
        },
        from_arrow::temporal::{
            validate_mapping_timestamp_timezone_metadata, validate_null_timestamp_timezone_metadata,
        },
    },
    write::profile,
};

use super::super::{
    checked_add, downcast_direct_array, invalid_payload,
    payload::{EncodedRowsPayload, TDS_ROW_TOKEN},
    plan::{DirectColumnEncoding, DirectColumnPlan},
    row_column_diagnostic,
    types::primitive::uint64_checked_bigint_bytes,
    types::temporal::{
        date_cell_len, datetime2_cell_len, datetimeoffset_cell_len, mssql_date_from_arrow_date32,
        mssql_datetime2_from_arrow_date64, time_cell_len, write_date_cell, write_datetime2_cell,
        write_datetimeoffset_cell, write_time_cell,
    },
    value_conversion_error,
};

const ROW_TOKEN_LEN: usize = 1;
const CELL_LEN_PREFIX_LEN: usize = 1;

#[derive(Debug, Clone, Copy)]
struct FixedWidthColumn<'a> {
    plan: &'a DirectColumnPlan,
    mapping: &'a SchemaMapping,
    non_null_cell_len: usize,
}

/// Encodes fixed-size direct columns without building a full per-cell row
/// layout.
///
/// Returns `Ok(None)` when the columns require the general layout path.
pub(crate) fn try_encode_fixed_width_rows(
    batch: &RecordBatch,
    mappings: &[SchemaMapping],
    plan_options: PlanOptions,
    columns: &[DirectColumnPlan],
) -> Result<Option<EncodedRowsPayload>> {
    if batch.num_rows() == 0 {
        return Ok(Some(EncodedRowsPayload::new(Vec::new(), Vec::new())?));
    }

    let Some(columns) = fixed_width_columns(mappings, columns)? else {
        return Ok(None);
    };

    let layout = measure_fixed_width_rows(batch, &columns)?;
    let mut current_offsets = layout.current_offsets.clone();
    let mut bytes = vec![0; layout.payload_len];

    for &row_offset in &layout.row_token_offsets {
        bytes[row_offset] = TDS_ROW_TOKEN;
    }

    for column in columns {
        let Some(array) = batch
            .columns()
            .get(column.plan.source_index())
            .map(AsRef::as_ref)
        else {
            return Err(value_conversion_error(row_column_diagnostic(
                column.plan,
                0,
                DiagnosticCode::ValueTypeMismatch,
                "planned direct column index is outside the runtime batch",
            )));
        };

        match column.plan.encoding() {
            DirectColumnEncoding::Primitive(PrimitiveArrowToMssql::BooleanToBit) => {
                let array = downcast_direct_array::<BooleanArray>(array, column.plan)?;
                fill_boolean_fixed_width_column(
                    array,
                    column.plan,
                    &mut current_offsets,
                    &mut bytes,
                );
            }
            DirectColumnEncoding::Primitive(PrimitiveArrowToMssql::UInt8ToTinyInt) => {
                let array = downcast_direct_array::<UInt8Array>(array, column.plan)?;
                fill_uint8_fixed_width_column(array, column.plan, &mut current_offsets, &mut bytes);
            }
            DirectColumnEncoding::Primitive(PrimitiveArrowToMssql::Int8ToSmallInt) => {
                let array = downcast_direct_array::<Int8Array>(array, column.plan)?;
                fill_int8_fixed_width_column(array, column.plan, &mut current_offsets, &mut bytes);
            }
            DirectColumnEncoding::Primitive(PrimitiveArrowToMssql::Int16ToSmallInt) => {
                let array = downcast_direct_array::<Int16Array>(array, column.plan)?;
                fill_int16_fixed_width_column(array, column.plan, &mut current_offsets, &mut bytes);
            }
            DirectColumnEncoding::Primitive(PrimitiveArrowToMssql::Int32ToInt) => {
                let array = downcast_direct_array::<Int32Array>(array, column.plan)?;
                fill_int32_fixed_width_column(array, column.plan, &mut current_offsets, &mut bytes);
            }
            DirectColumnEncoding::Primitive(PrimitiveArrowToMssql::UInt16ToInt) => {
                let array = downcast_direct_array::<UInt16Array>(array, column.plan)?;
                fill_uint16_fixed_width_column(
                    array,
                    column.plan,
                    &mut current_offsets,
                    &mut bytes,
                );
            }
            DirectColumnEncoding::Primitive(PrimitiveArrowToMssql::Int64ToBigInt) => {
                let array = downcast_direct_array::<Int64Array>(array, column.plan)?;
                fill_int64_fixed_width_column(array, column.plan, &mut current_offsets, &mut bytes);
            }
            DirectColumnEncoding::Primitive(PrimitiveArrowToMssql::UInt32ToBigInt) => {
                let array = downcast_direct_array::<UInt32Array>(array, column.plan)?;
                fill_uint32_fixed_width_column(
                    array,
                    column.plan,
                    &mut current_offsets,
                    &mut bytes,
                );
            }
            DirectColumnEncoding::Primitive(PrimitiveArrowToMssql::UInt64ToCheckedBigInt) => {
                let array = downcast_direct_array::<UInt64Array>(array, column.plan)?;
                fill_uint64_checked_bigint_fixed_width_column(
                    array,
                    column.plan,
                    &mut current_offsets,
                    &mut bytes,
                )?;
            }
            DirectColumnEncoding::Primitive(PrimitiveArrowToMssql::Float32ToReal) => {
                let array = downcast_direct_array::<Float32Array>(array, column.plan)?;
                fill_float32_fixed_width_column(
                    array,
                    column.plan,
                    &mut current_offsets,
                    &mut bytes,
                )?;
            }
            DirectColumnEncoding::Primitive(PrimitiveArrowToMssql::Float64ToFloat) => {
                let array = downcast_direct_array::<Float64Array>(array, column.plan)?;
                fill_float64_fixed_width_column(
                    array,
                    column.plan,
                    &mut current_offsets,
                    &mut bytes,
                )?;
            }
            DirectColumnEncoding::UInt64Decimal20_0 => {
                return Ok(None);
            }
            DirectColumnEncoding::Decimal(_) => {
                return Ok(None);
            }
            DirectColumnEncoding::VariableWidth(_) => {
                return Ok(None);
            }
            DirectColumnEncoding::Temporal(TemporalArrowToMssql::Date32ToDate) => {
                let array = downcast_direct_array::<Date32Array>(array, column.plan)?;
                fill_date32_fixed_width_column(
                    array,
                    column.plan,
                    &mut current_offsets,
                    &mut bytes,
                )?;
            }
            DirectColumnEncoding::Temporal(TemporalArrowToMssql::Date64ToDateTime2) => {
                let array = downcast_direct_array::<Date64Array>(array, column.plan)?;
                fill_date64_fixed_width_column(
                    array,
                    column.plan,
                    &mut current_offsets,
                    &mut bytes,
                )?;
            }
            DirectColumnEncoding::Temporal(
                TemporalArrowToMssql::TimestampSecondToDateTime2
                | TemporalArrowToMssql::TimestampSecondTzToDateTime2,
            ) => {
                let array = downcast_direct_array::<TimestampSecondArray>(array, column.plan)?;
                fill_timestamp_fixed_width_column(
                    array,
                    column.mapping,
                    column.plan,
                    &mut current_offsets,
                    &mut bytes,
                    |mapping, row_index, value, _policy| {
                        mssql_datetime2_from_arrow_timestamp_second(mapping, row_index, value)
                    },
                    plan_options.nanosecond_policy,
                )?;
            }
            DirectColumnEncoding::Temporal(
                TemporalArrowToMssql::TimestampMillisecondToDateTime2
                | TemporalArrowToMssql::TimestampMillisecondTzToDateTime2,
            ) => {
                let array = downcast_direct_array::<TimestampMillisecondArray>(array, column.plan)?;
                fill_timestamp_fixed_width_column(
                    array,
                    column.mapping,
                    column.plan,
                    &mut current_offsets,
                    &mut bytes,
                    |mapping, row_index, value, _policy| {
                        mssql_datetime2_from_arrow_timestamp_millisecond(mapping, row_index, value)
                    },
                    plan_options.nanosecond_policy,
                )?;
            }
            DirectColumnEncoding::Temporal(
                TemporalArrowToMssql::TimestampMicrosecondToDateTime2
                | TemporalArrowToMssql::TimestampMicrosecondTzToDateTime2,
            ) => {
                let array = downcast_direct_array::<TimestampMicrosecondArray>(array, column.plan)?;
                fill_timestamp_fixed_width_column(
                    array,
                    column.mapping,
                    column.plan,
                    &mut current_offsets,
                    &mut bytes,
                    |mapping, row_index, value, _policy| {
                        mssql_datetime2_from_arrow_timestamp_microsecond(mapping, row_index, value)
                    },
                    plan_options.nanosecond_policy,
                )?;
            }
            DirectColumnEncoding::Temporal(
                TemporalArrowToMssql::TimestampNanosecondToDateTime2
                | TemporalArrowToMssql::TimestampNanosecondTzToDateTime2,
            ) => {
                let array = downcast_direct_array::<TimestampNanosecondArray>(array, column.plan)?;
                fill_timestamp_fixed_width_column(
                    array,
                    column.mapping,
                    column.plan,
                    &mut current_offsets,
                    &mut bytes,
                    |mapping, row_index, value, policy| {
                        mssql_datetime2_from_arrow_timestamp_nanosecond(
                            mapping, row_index, value, policy,
                        )
                    },
                    plan_options.nanosecond_policy,
                )?;
            }
            DirectColumnEncoding::Temporal(TemporalArrowToMssql::Time32SecondToTime) => {
                let array = downcast_direct_array::<Time32SecondArray>(array, column.plan)?;
                fill_time_fixed_width_column(
                    array,
                    column.mapping,
                    column.plan,
                    &mut current_offsets,
                    &mut bytes,
                    |array, mapping, row_index, _policy| {
                        mssql_time_from_arrow_time32_second(
                            mapping,
                            row_index,
                            array.value(row_index),
                        )
                    },
                    plan_options.nanosecond_policy,
                )?;
            }
            DirectColumnEncoding::Temporal(TemporalArrowToMssql::Time32MillisecondToTime) => {
                let array = downcast_direct_array::<Time32MillisecondArray>(array, column.plan)?;
                fill_time_fixed_width_column(
                    array,
                    column.mapping,
                    column.plan,
                    &mut current_offsets,
                    &mut bytes,
                    |array, mapping, row_index, _policy| {
                        mssql_time_from_arrow_time32_millisecond(
                            mapping,
                            row_index,
                            array.value(row_index),
                        )
                    },
                    plan_options.nanosecond_policy,
                )?;
            }
            DirectColumnEncoding::Temporal(TemporalArrowToMssql::Time64MicrosecondToTime) => {
                let array = downcast_direct_array::<Time64MicrosecondArray>(array, column.plan)?;
                fill_time_fixed_width_column(
                    array,
                    column.mapping,
                    column.plan,
                    &mut current_offsets,
                    &mut bytes,
                    |array, mapping, row_index, _policy| {
                        mssql_time_from_arrow_time64_microsecond(
                            mapping,
                            row_index,
                            array.value(row_index),
                        )
                    },
                    plan_options.nanosecond_policy,
                )?;
            }
            DirectColumnEncoding::Temporal(TemporalArrowToMssql::Time64NanosecondToTime) => {
                let array = downcast_direct_array::<Time64NanosecondArray>(array, column.plan)?;
                fill_time_fixed_width_column(
                    array,
                    column.mapping,
                    column.plan,
                    &mut current_offsets,
                    &mut bytes,
                    |array, mapping, row_index, policy| {
                        mssql_time_from_arrow_time64_nanosecond(
                            mapping,
                            row_index,
                            array.value(row_index),
                            policy,
                        )
                    },
                    plan_options.nanosecond_policy,
                )?;
            }
            DirectColumnEncoding::Temporal(
                TemporalArrowToMssql::TimestampSecondTzToDateTimeOffset,
            ) => {
                let array = downcast_direct_array::<TimestampSecondArray>(array, column.plan)?;
                fill_datetimeoffset_fixed_width_column(
                    array,
                    column.mapping,
                    column.plan,
                    &mut current_offsets,
                    &mut bytes,
                    |array, mapping, row_index, _policy| {
                        let timezone = timestamp_timezone_for_datetimeoffset(mapping, row_index)?;
                        mssql_datetimeoffset_from_arrow_timestamp_second(
                            mapping,
                            row_index,
                            array.value(row_index),
                            timezone,
                        )
                    },
                    plan_options.nanosecond_policy,
                )?;
            }
            DirectColumnEncoding::Temporal(
                TemporalArrowToMssql::TimestampMillisecondTzToDateTimeOffset,
            ) => {
                let array = downcast_direct_array::<TimestampMillisecondArray>(array, column.plan)?;
                fill_datetimeoffset_fixed_width_column(
                    array,
                    column.mapping,
                    column.plan,
                    &mut current_offsets,
                    &mut bytes,
                    |array, mapping, row_index, _policy| {
                        let timezone = timestamp_timezone_for_datetimeoffset(mapping, row_index)?;
                        mssql_datetimeoffset_from_arrow_timestamp_millisecond(
                            mapping,
                            row_index,
                            array.value(row_index),
                            timezone,
                        )
                    },
                    plan_options.nanosecond_policy,
                )?;
            }
            DirectColumnEncoding::Temporal(
                TemporalArrowToMssql::TimestampMicrosecondTzToDateTimeOffset,
            ) => {
                let array = downcast_direct_array::<TimestampMicrosecondArray>(array, column.plan)?;
                fill_datetimeoffset_fixed_width_column(
                    array,
                    column.mapping,
                    column.plan,
                    &mut current_offsets,
                    &mut bytes,
                    |array, mapping, row_index, _policy| {
                        let timezone = timestamp_timezone_for_datetimeoffset(mapping, row_index)?;
                        mssql_datetimeoffset_from_arrow_timestamp_microsecond(
                            mapping,
                            row_index,
                            array.value(row_index),
                            timezone,
                        )
                    },
                    plan_options.nanosecond_policy,
                )?;
            }
            DirectColumnEncoding::Temporal(
                TemporalArrowToMssql::TimestampNanosecondTzToDateTimeOffset,
            ) => {
                let array = downcast_direct_array::<TimestampNanosecondArray>(array, column.plan)?;
                fill_datetimeoffset_fixed_width_column(
                    array,
                    column.mapping,
                    column.plan,
                    &mut current_offsets,
                    &mut bytes,
                    |array, mapping, row_index, policy| {
                        let timezone = timestamp_timezone_for_datetimeoffset(mapping, row_index)?;
                        mssql_datetimeoffset_from_arrow_timestamp_nanosecond(
                            mapping,
                            row_index,
                            array.value(row_index),
                            timezone,
                            policy,
                        )
                    },
                    plan_options.nanosecond_policy,
                )?;
            }
        }
    }

    let payload = EncodedRowsPayload::new(bytes, layout.row_token_offsets)?;

    Ok(Some(payload))
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct FixedWidthRowsLayout {
    row_token_offsets: Vec<usize>,
    current_offsets: Vec<usize>,
    payload_len: usize,
}

fn fixed_width_columns<'a>(
    mappings: &'a [SchemaMapping],
    columns: &'a [DirectColumnPlan],
) -> Result<Option<Vec<FixedWidthColumn<'a>>>> {
    if profile::direct_fixed_width_fast_path_disabled() {
        return Ok(None);
    }

    let mut fixed_width_columns = Vec::with_capacity(columns.len());

    for (column_index, column) in columns.iter().enumerate() {
        let Some(non_null_cell_len) = fixed_width_non_null_cell_len(column) else {
            return Ok(None);
        };
        let Some(mapping) = mappings.get(column_index) else {
            return Err(invalid_payload(format!(
                "direct mapping index {column_index} is outside mapping count {}",
                mappings.len()
            )));
        };

        fixed_width_columns.push(FixedWidthColumn {
            plan: column,
            mapping,
            non_null_cell_len,
        });
    }

    Ok(Some(fixed_width_columns))
}

fn measure_fixed_width_rows(
    batch: &RecordBatch,
    columns: &[FixedWidthColumn<'_>],
) -> Result<FixedWidthRowsLayout> {
    let row_count = batch.num_rows();
    let mut row_lengths = vec![ROW_TOKEN_LEN; row_count];

    for column in columns {
        let Some(array) = batch
            .columns()
            .get(column.plan.source_index())
            .map(AsRef::as_ref)
        else {
            return Err(value_conversion_error(row_column_diagnostic(
                column.plan,
                0,
                DiagnosticCode::ValueTypeMismatch,
                "planned direct column index is outside the runtime batch",
            )));
        };

        validate_fixed_width_timestamp_timezone_metadata(array, column.mapping, column.plan)?;

        if column.plan.nullable() {
            add_nullable_fixed_width_column_lengths(
                array,
                column.non_null_cell_len,
                &mut row_lengths,
            )?;
        } else {
            if array.null_count() != 0 {
                return Err(first_null_error(array, column.plan));
            }

            add_non_nullable_fixed_width_column_lengths(
                column.non_null_cell_len,
                &mut row_lengths,
            )?;
        }
    }

    let mut row_token_offsets = Vec::with_capacity(row_count);
    let mut current_offsets = Vec::with_capacity(row_count);
    let mut payload_len = 0usize;

    for row_length in row_lengths {
        row_token_offsets.push(payload_len);
        current_offsets.push(checked_add(payload_len, ROW_TOKEN_LEN)?);
        payload_len = checked_add(payload_len, row_length)?;
    }

    Ok(FixedWidthRowsLayout {
        row_token_offsets,
        current_offsets,
        payload_len,
    })
}

fn add_non_nullable_fixed_width_column_lengths(
    non_null_cell_len: usize,
    row_lengths: &mut [usize],
) -> Result<()> {
    for row_length in row_lengths {
        *row_length = checked_add(*row_length, non_null_cell_len)?;
    }

    Ok(())
}

fn add_nullable_fixed_width_column_lengths(
    array: &dyn Array,
    non_null_cell_len: usize,
    row_lengths: &mut [usize],
) -> Result<()> {
    for (row_index, row_length) in row_lengths.iter_mut().enumerate() {
        let cell_len = if array.is_null(row_index) {
            CELL_LEN_PREFIX_LEN
        } else {
            non_null_cell_len
        };

        *row_length = checked_add(*row_length, cell_len)?;
    }

    Ok(())
}

fn validate_fixed_width_timestamp_timezone_metadata(
    array: &dyn Array,
    mapping: &SchemaMapping,
    column: &DirectColumnPlan,
) -> Result<()> {
    if !matches!(
        column.encoding(),
        DirectColumnEncoding::Temporal(
            TemporalArrowToMssql::TimestampSecondToDateTime2
                | TemporalArrowToMssql::TimestampMillisecondToDateTime2
                | TemporalArrowToMssql::TimestampMicrosecondToDateTime2
                | TemporalArrowToMssql::TimestampNanosecondToDateTime2
                | TemporalArrowToMssql::TimestampSecondTzToDateTime2
                | TemporalArrowToMssql::TimestampMillisecondTzToDateTime2
                | TemporalArrowToMssql::TimestampMicrosecondTzToDateTime2
                | TemporalArrowToMssql::TimestampNanosecondTzToDateTime2
                | TemporalArrowToMssql::TimestampSecondTzToDateTimeOffset
                | TemporalArrowToMssql::TimestampMillisecondTzToDateTimeOffset
                | TemporalArrowToMssql::TimestampMicrosecondTzToDateTimeOffset
                | TemporalArrowToMssql::TimestampNanosecondTzToDateTimeOffset
        )
    ) {
        return Ok(());
    }

    for row_index in 0..array.len() {
        if array.is_null(row_index) {
            validate_null_timestamp_timezone_metadata(mapping, row_index)?;
        } else {
            validate_mapping_timestamp_timezone_metadata(mapping, row_index)?;
        }
    }

    Ok(())
}

fn fixed_width_non_null_cell_len(column: &DirectColumnPlan) -> Option<usize> {
    match column.encoding() {
        DirectColumnEncoding::Temporal(
            TemporalArrowToMssql::Date32ToDate | TemporalArrowToMssql::Date64ToDateTime2,
        ) if profile::direct_date_fast_path_disabled() => None,
        DirectColumnEncoding::Temporal(TemporalArrowToMssql::Date32ToDate) => Some(date_cell_len()),
        DirectColumnEncoding::Temporal(TemporalArrowToMssql::Date64ToDateTime2) => {
            Some(datetime2_cell_len(3).ok()?)
        }
        DirectColumnEncoding::Temporal(
            TemporalArrowToMssql::TimestampSecondToDateTime2
            | TemporalArrowToMssql::TimestampMillisecondToDateTime2
            | TemporalArrowToMssql::TimestampMicrosecondToDateTime2
            | TemporalArrowToMssql::TimestampNanosecondToDateTime2
            | TemporalArrowToMssql::TimestampSecondTzToDateTime2
            | TemporalArrowToMssql::TimestampMillisecondTzToDateTime2
            | TemporalArrowToMssql::TimestampMicrosecondTzToDateTime2
            | TemporalArrowToMssql::TimestampNanosecondTzToDateTime2,
        ) => Some(datetime2_cell_len(7).ok()?),
        DirectColumnEncoding::Temporal(TemporalArrowToMssql::Time32SecondToTime) => {
            Some(time_cell_len(0).ok()?)
        }
        DirectColumnEncoding::Temporal(TemporalArrowToMssql::Time32MillisecondToTime) => {
            Some(time_cell_len(3).ok()?)
        }
        DirectColumnEncoding::Temporal(TemporalArrowToMssql::Time64MicrosecondToTime) => {
            Some(time_cell_len(6).ok()?)
        }
        DirectColumnEncoding::Temporal(TemporalArrowToMssql::Time64NanosecondToTime) => {
            Some(time_cell_len(7).ok()?)
        }
        DirectColumnEncoding::Temporal(
            TemporalArrowToMssql::TimestampSecondTzToDateTimeOffset
            | TemporalArrowToMssql::TimestampMillisecondTzToDateTimeOffset
            | TemporalArrowToMssql::TimestampMicrosecondTzToDateTimeOffset
            | TemporalArrowToMssql::TimestampNanosecondTzToDateTimeOffset,
        ) => Some(datetimeoffset_cell_len(7).ok()?),
        encoding => {
            let value_len = fixed_width_value_len(encoding)?;
            if column.nullable() {
                value_len.checked_add(CELL_LEN_PREFIX_LEN)
            } else {
                Some(value_len)
            }
        }
    }
}

fn fixed_width_value_len(encoding: DirectColumnEncoding) -> Option<usize> {
    match encoding {
        DirectColumnEncoding::Primitive(PrimitiveArrowToMssql::BooleanToBit) => Some(1),
        DirectColumnEncoding::Primitive(PrimitiveArrowToMssql::UInt8ToTinyInt) => Some(1),
        DirectColumnEncoding::Primitive(
            PrimitiveArrowToMssql::Int8ToSmallInt | PrimitiveArrowToMssql::Int16ToSmallInt,
        ) => Some(2),
        DirectColumnEncoding::Primitive(PrimitiveArrowToMssql::Int32ToInt) => Some(4),
        DirectColumnEncoding::Primitive(PrimitiveArrowToMssql::UInt16ToInt) => Some(4),
        DirectColumnEncoding::Primitive(PrimitiveArrowToMssql::Int64ToBigInt) => Some(8),
        DirectColumnEncoding::Primitive(PrimitiveArrowToMssql::UInt32ToBigInt) => Some(8),
        DirectColumnEncoding::Primitive(PrimitiveArrowToMssql::UInt64ToCheckedBigInt) => Some(8),
        DirectColumnEncoding::Primitive(PrimitiveArrowToMssql::Float32ToReal) => Some(4),
        DirectColumnEncoding::Primitive(PrimitiveArrowToMssql::Float64ToFloat) => Some(8),
        DirectColumnEncoding::UInt64Decimal20_0 => None,
        DirectColumnEncoding::Decimal(_) => None,
        DirectColumnEncoding::VariableWidth(_) => None,
        DirectColumnEncoding::Temporal(_) => None,
    }
}

fn fill_boolean_fixed_width_column(
    array: &BooleanArray,
    column: &DirectColumnPlan,
    current_offsets: &mut [usize],
    bytes: &mut [u8],
) {
    for (row_index, current_offset) in current_offsets.iter_mut().enumerate().take(array.len()) {
        let offset = *current_offset;
        if column.nullable() {
            if array.is_null(row_index) {
                bytes[offset] = 0;
                *current_offset += CELL_LEN_PREFIX_LEN;
            } else {
                bytes[offset] = 1;
                bytes[offset + CELL_LEN_PREFIX_LEN] = u8::from(array.value(row_index));
                *current_offset += CELL_LEN_PREFIX_LEN + 1;
            }
        } else {
            bytes[offset] = u8::from(array.value(row_index));
            *current_offset += 1;
        }
    }
}

fn fill_uint8_fixed_width_column(
    array: &UInt8Array,
    column: &DirectColumnPlan,
    current_offsets: &mut [usize],
    bytes: &mut [u8],
) {
    for row_index in 0..array.len() {
        write_fixed_width_value(
            array,
            column,
            row_index,
            1,
            current_offsets,
            bytes,
            |array, row_index| [array.value(row_index)],
        );
    }
}

fn fill_int8_fixed_width_column(
    array: &Int8Array,
    column: &DirectColumnPlan,
    current_offsets: &mut [usize],
    bytes: &mut [u8],
) {
    for row_index in 0..array.len() {
        write_fixed_width_value(
            array,
            column,
            row_index,
            2,
            current_offsets,
            bytes,
            |array, row_index| i16::from(array.value(row_index)).to_le_bytes(),
        );
    }
}

fn fill_int16_fixed_width_column(
    array: &Int16Array,
    column: &DirectColumnPlan,
    current_offsets: &mut [usize],
    bytes: &mut [u8],
) {
    for row_index in 0..array.len() {
        write_fixed_width_value(
            array,
            column,
            row_index,
            2,
            current_offsets,
            bytes,
            |array, row_index| array.value(row_index).to_le_bytes(),
        );
    }
}

fn fill_int32_fixed_width_column(
    array: &Int32Array,
    column: &DirectColumnPlan,
    current_offsets: &mut [usize],
    bytes: &mut [u8],
) {
    for row_index in 0..array.len() {
        write_fixed_width_value(
            array,
            column,
            row_index,
            4,
            current_offsets,
            bytes,
            |array, row_index| array.value(row_index).to_le_bytes(),
        );
    }
}

fn fill_uint16_fixed_width_column(
    array: &UInt16Array,
    column: &DirectColumnPlan,
    current_offsets: &mut [usize],
    bytes: &mut [u8],
) {
    for row_index in 0..array.len() {
        write_fixed_width_value(
            array,
            column,
            row_index,
            4,
            current_offsets,
            bytes,
            |array, row_index| i32::from(array.value(row_index)).to_le_bytes(),
        );
    }
}

fn fill_int64_fixed_width_column(
    array: &Int64Array,
    column: &DirectColumnPlan,
    current_offsets: &mut [usize],
    bytes: &mut [u8],
) {
    for row_index in 0..array.len() {
        write_fixed_width_value(
            array,
            column,
            row_index,
            8,
            current_offsets,
            bytes,
            |array, row_index| array.value(row_index).to_le_bytes(),
        );
    }
}

fn fill_uint32_fixed_width_column(
    array: &UInt32Array,
    column: &DirectColumnPlan,
    current_offsets: &mut [usize],
    bytes: &mut [u8],
) {
    for row_index in 0..array.len() {
        write_fixed_width_value(
            array,
            column,
            row_index,
            8,
            current_offsets,
            bytes,
            |array, row_index| i64::from(array.value(row_index)).to_le_bytes(),
        );
    }
}

fn fill_uint64_checked_bigint_fixed_width_column(
    array: &UInt64Array,
    column: &DirectColumnPlan,
    current_offsets: &mut [usize],
    bytes: &mut [u8],
) -> Result<()> {
    for (row_index, current_offset) in current_offsets.iter_mut().enumerate().take(array.len()) {
        let offset = *current_offset;
        if column.nullable() && array.is_null(row_index) {
            bytes[offset] = 0;
            *current_offset += CELL_LEN_PREFIX_LEN;
            continue;
        }

        let value = uint64_checked_bigint_bytes(array.value(row_index), column, row_index)?;
        if column.nullable() {
            bytes[offset] = 8;
            bytes[offset + CELL_LEN_PREFIX_LEN..offset + CELL_LEN_PREFIX_LEN + 8]
                .copy_from_slice(&value);
            *current_offset += CELL_LEN_PREFIX_LEN + 8;
        } else {
            bytes[offset..offset + 8].copy_from_slice(&value);
            *current_offset += 8;
        }
    }

    Ok(())
}

fn fill_float32_fixed_width_column(
    array: &Float32Array,
    column: &DirectColumnPlan,
    current_offsets: &mut [usize],
    bytes: &mut [u8],
) -> Result<()> {
    for (row_index, current_offset) in current_offsets.iter_mut().enumerate().take(array.len()) {
        let offset = *current_offset;
        if column.nullable() && array.is_null(row_index) {
            bytes[offset] = 0;
            *current_offset += CELL_LEN_PREFIX_LEN;
            continue;
        }

        let value = array.value(row_index);
        if !value.is_finite() {
            return Err(value_conversion_error(row_column_diagnostic(
                column,
                row_index,
                DiagnosticCode::NonFiniteFloat,
                format!("non-finite floating point value {value} is not supported"),
            )));
        }

        if column.nullable() {
            bytes[offset] = 4;
            bytes[offset + CELL_LEN_PREFIX_LEN..offset + CELL_LEN_PREFIX_LEN + 4]
                .copy_from_slice(&value.to_le_bytes());
            *current_offset += CELL_LEN_PREFIX_LEN + 4;
        } else {
            bytes[offset..offset + 4].copy_from_slice(&value.to_le_bytes());
            *current_offset += 4;
        }
    }

    Ok(())
}

fn fill_float64_fixed_width_column(
    array: &Float64Array,
    column: &DirectColumnPlan,
    current_offsets: &mut [usize],
    bytes: &mut [u8],
) -> Result<()> {
    for (row_index, current_offset) in current_offsets.iter_mut().enumerate().take(array.len()) {
        let offset = *current_offset;
        if column.nullable() && array.is_null(row_index) {
            bytes[offset] = 0;
            *current_offset += CELL_LEN_PREFIX_LEN;
            continue;
        }

        let value = array.value(row_index);
        if !value.is_finite() {
            return Err(value_conversion_error(row_column_diagnostic(
                column,
                row_index,
                DiagnosticCode::NonFiniteFloat,
                format!("non-finite floating point value {value} is not supported"),
            )));
        }

        if column.nullable() {
            bytes[offset] = 8;
            bytes[offset + CELL_LEN_PREFIX_LEN..offset + CELL_LEN_PREFIX_LEN + 8]
                .copy_from_slice(&value.to_le_bytes());
            *current_offset += CELL_LEN_PREFIX_LEN + 8;
        } else {
            bytes[offset..offset + 8].copy_from_slice(&value.to_le_bytes());
            *current_offset += 8;
        }
    }

    Ok(())
}

fn fill_date32_fixed_width_column(
    array: &Date32Array,
    column: &DirectColumnPlan,
    current_offsets: &mut [usize],
    bytes: &mut [u8],
) -> Result<()> {
    for (row_index, current_offset) in current_offsets.iter_mut().enumerate().take(array.len()) {
        let offset = *current_offset;
        if array.is_null(row_index) {
            debug_assert!(column.nullable());
            bytes[offset] = 0;
            *current_offset += CELL_LEN_PREFIX_LEN;
            continue;
        }

        let value = mssql_date_from_arrow_date32(column, row_index, array.value(row_index))?;
        let cell_len = date_cell_len();
        let cell_bytes = bytes.get_mut(offset..offset + cell_len).ok_or_else(|| {
            invalid_payload("fixed-width Date32 temporal cell range is outside payload")
        })?;
        write_date_cell(cell_bytes, value)?;
        *current_offset += cell_len;
    }

    Ok(())
}

fn fill_date64_fixed_width_column(
    array: &Date64Array,
    column: &DirectColumnPlan,
    current_offsets: &mut [usize],
    bytes: &mut [u8],
) -> Result<()> {
    for (row_index, current_offset) in current_offsets.iter_mut().enumerate().take(array.len()) {
        let offset = *current_offset;
        if array.is_null(row_index) {
            debug_assert!(column.nullable());
            bytes[offset] = 0;
            *current_offset += CELL_LEN_PREFIX_LEN;
            continue;
        }

        let value = mssql_datetime2_from_arrow_date64(column, row_index, array.value(row_index))?;
        let cell_len = datetime2_cell_len(value.time().scale())?;
        let cell_bytes = bytes.get_mut(offset..offset + cell_len).ok_or_else(|| {
            invalid_payload("fixed-width Date64 temporal cell range is outside payload")
        })?;
        write_datetime2_cell(cell_bytes, value)?;
        *current_offset += cell_len;
    }

    Ok(())
}

fn fill_timestamp_fixed_width_column<A, F>(
    array: &A,
    mapping: &SchemaMapping,
    column: &DirectColumnPlan,
    current_offsets: &mut [usize],
    bytes: &mut [u8],
    value: F,
    nanosecond_policy: NanosecondPolicy,
) -> Result<()>
where
    A: Array,
    F: Fn(&SchemaMapping, usize, i64, NanosecondPolicy) -> Result<MssqlDateTime2>,
{
    for (row_index, current_offset) in current_offsets.iter_mut().enumerate().take(array.len()) {
        let offset = *current_offset;
        if array.is_null(row_index) {
            debug_assert!(column.nullable());
            validate_null_timestamp_timezone_metadata(mapping, row_index)?;
            bytes[offset] = 0;
            *current_offset += CELL_LEN_PREFIX_LEN;
            continue;
        }

        validate_mapping_timestamp_timezone_metadata(mapping, row_index)?;
        let value = value(
            mapping,
            row_index,
            timestamp_value(array, row_index),
            nanosecond_policy,
        )?;
        let cell_len = datetime2_cell_len(value.time().scale())?;
        let cell_bytes = bytes.get_mut(offset..offset + cell_len).ok_or_else(|| {
            invalid_payload("fixed-width timestamp temporal cell range is outside payload")
        })?;
        write_datetime2_cell(cell_bytes, value)?;
        *current_offset += cell_len;
    }

    Ok(())
}

fn timestamp_value(array: &dyn Array, row_index: usize) -> i64 {
    if let Some(array) = array.as_any().downcast_ref::<TimestampSecondArray>() {
        array.value(row_index)
    } else if let Some(array) = array.as_any().downcast_ref::<TimestampMillisecondArray>() {
        array.value(row_index)
    } else if let Some(array) = array.as_any().downcast_ref::<TimestampMicrosecondArray>() {
        array.value(row_index)
    } else if let Some(array) = array.as_any().downcast_ref::<TimestampNanosecondArray>() {
        array.value(row_index)
    } else {
        unreachable!("timestamp fixed-width fill only receives timestamp arrays")
    }
}

fn fill_time_fixed_width_column<A, F>(
    array: &A,
    mapping: &SchemaMapping,
    column: &DirectColumnPlan,
    current_offsets: &mut [usize],
    bytes: &mut [u8],
    value: F,
    nanosecond_policy: NanosecondPolicy,
) -> Result<()>
where
    A: Array,
    F: Fn(&A, &SchemaMapping, usize, NanosecondPolicy) -> Result<MssqlTime>,
{
    for (row_index, current_offset) in current_offsets.iter_mut().enumerate().take(array.len()) {
        let offset = *current_offset;
        if array.is_null(row_index) {
            debug_assert!(column.nullable());
            bytes[offset] = 0;
            *current_offset += CELL_LEN_PREFIX_LEN;
            continue;
        }

        let value = value(array, mapping, row_index, nanosecond_policy)?;
        let cell_len = time_cell_len(value.scale())?;
        let cell_bytes = bytes.get_mut(offset..offset + cell_len).ok_or_else(|| {
            invalid_payload("fixed-width time temporal cell range is outside payload")
        })?;
        write_time_cell(cell_bytes, value)?;
        *current_offset += cell_len;
    }

    Ok(())
}

fn fill_datetimeoffset_fixed_width_column<A, F>(
    array: &A,
    mapping: &SchemaMapping,
    column: &DirectColumnPlan,
    current_offsets: &mut [usize],
    bytes: &mut [u8],
    value: F,
    nanosecond_policy: NanosecondPolicy,
) -> Result<()>
where
    A: Array,
    F: Fn(&A, &SchemaMapping, usize, NanosecondPolicy) -> Result<MssqlDateTimeOffset>,
{
    for (row_index, current_offset) in current_offsets.iter_mut().enumerate().take(array.len()) {
        let offset = *current_offset;
        if array.is_null(row_index) {
            debug_assert!(column.nullable());
            validate_null_timestamp_timezone_metadata(mapping, row_index)?;
            bytes[offset] = 0;
            *current_offset += CELL_LEN_PREFIX_LEN;
            continue;
        }

        validate_mapping_timestamp_timezone_metadata(mapping, row_index)?;
        let value = value(array, mapping, row_index, nanosecond_policy)?;
        let cell_len = datetimeoffset_cell_len(value.datetime2().time().scale())?;
        let cell_bytes = bytes.get_mut(offset..offset + cell_len).ok_or_else(|| {
            invalid_payload("fixed-width datetimeoffset temporal cell range is outside payload")
        })?;
        write_datetimeoffset_cell(cell_bytes, value)?;
        *current_offset += cell_len;
    }

    Ok(())
}

fn timestamp_timezone_for_datetimeoffset(
    mapping: &SchemaMapping,
    row_index: usize,
) -> Result<&str> {
    let arrow_schema::DataType::Timestamp(_, Some(timezone)) = mapping.arrow().data_type() else {
        return Err(value_conversion_error(row_mapping_diagnostic(
            mapping,
            row_index,
            DiagnosticCode::ValueTypeMismatch,
            "planned datetimeoffset mapping does not have Arrow timestamp timezone metadata",
        )));
    };

    if timezone.is_empty() {
        return Err(value_conversion_error(row_mapping_diagnostic(
            mapping,
            row_index,
            DiagnosticCode::ValueTypeMismatch,
            "planned datetimeoffset mapping has empty Arrow timestamp timezone metadata",
        )));
    }

    Ok(timezone)
}

fn write_fixed_width_value<Array, ValueBytes>(
    array: &Array,
    column: &DirectColumnPlan,
    row_index: usize,
    value_len: u8,
    current_offsets: &mut [usize],
    bytes: &mut [u8],
    value_bytes: impl FnOnce(&Array, usize) -> ValueBytes,
) where
    Array: arrow_array::Array,
    ValueBytes: AsRef<[u8]>,
{
    let offset = current_offsets[row_index];
    if column.nullable() {
        if array.is_null(row_index) {
            bytes[offset] = 0;
            current_offsets[row_index] += CELL_LEN_PREFIX_LEN;
        } else {
            let value_bytes = value_bytes(array, row_index);
            bytes[offset] = value_len;
            bytes[offset + CELL_LEN_PREFIX_LEN
                ..offset + CELL_LEN_PREFIX_LEN + usize::from(value_len)]
                .copy_from_slice(value_bytes.as_ref());
            current_offsets[row_index] += CELL_LEN_PREFIX_LEN + usize::from(value_len);
        }
    } else {
        let value_bytes = value_bytes(array, row_index);
        bytes[offset..offset + usize::from(value_len)].copy_from_slice(value_bytes.as_ref());
        current_offsets[row_index] += usize::from(value_len);
    }
}

fn first_null_error(array: &dyn Array, column: &DirectColumnPlan) -> Error {
    let row_index = (0..array.len())
        .find(|row_index| array.is_null(*row_index))
        .unwrap_or(0);

    value_conversion_error(row_column_diagnostic(
        column,
        row_index,
        DiagnosticCode::NullInNonNullableColumn,
        "null value in non-nullable fixed-size direct column",
    ))
}

fn row_mapping_diagnostic(
    mapping: &SchemaMapping,
    row_index: usize,
    code: DiagnosticCode,
    message: impl Into<String>,
) -> Diagnostic {
    Diagnostic::error(code, message)
        .with_field(FieldRef::new(
            mapping.arrow().index(),
            mapping.arrow().name(),
        ))
        .with_row(row_index)
}

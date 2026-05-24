//! Fixed-width primitive direct TDS row layout.

use arrow_array::{
    Array, BooleanArray, Date32Array, Date64Array, Float32Array, Float64Array, Int8Array,
    Int16Array, Int32Array, Int64Array, RecordBatch, Time32MillisecondArray, Time32SecondArray,
    Time64MicrosecondArray, Time64NanosecondArray, TimestampMicrosecondArray,
    TimestampMillisecondArray, TimestampNanosecondArray, TimestampSecondArray, UInt8Array,
    UInt16Array, UInt32Array, UInt64Array,
};

use crate::{
    Diagnostic, DiagnosticCode, DiagnosticSet, Error, FieldRef, NanosecondPolicy, PlanOptions,
    Result, SchemaMapping,
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

use super::{
    super::{
        layout::{CellPosition, RowLayout},
        payload::{EncodedRowsPayload, TDS_ROW_TOKEN},
        plan::{DirectColumnEncoding, DirectColumnPlan},
    },
    temporal::{
        date_cell_len, datetime2_cell_len, datetimeoffset_cell_len, mssql_date_from_arrow_date32,
        mssql_datetime2_from_arrow_date64, time_cell_len, write_date_cell, write_datetime2_cell,
        write_datetimeoffset_cell, write_time_cell,
    },
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
pub(crate) fn try_encode_fixed_width_primitive_rows(
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

fn downcast_direct_array<'a, T: Array + 'static>(
    array: &'a dyn Array,
    column: &DirectColumnPlan,
) -> Result<&'a T> {
    array.as_any().downcast_ref::<T>().ok_or_else(|| {
        value_conversion_error(row_column_diagnostic(
            column,
            0,
            DiagnosticCode::ValueTypeMismatch,
            format!(
                "runtime Arrow type {} does not match planned direct column type",
                array.data_type()
            ),
        ))
    })
}

/// Measures one primitive Arrow column into a row-major cell length matrix.
///
/// Cell lengths match the TDS metadata shape implied by the mapping
/// nullability. Non-nullable primitive columns use fixed-width cells with no
/// length byte. Nullable primitive columns use the nullable TDS form with a
/// one-byte length prefix, where null values occupy only the zero length byte.
pub(crate) fn measure_primitive_column_cell_lengths(
    array: &dyn Array,
    column: &DirectColumnPlan,
    column_index: usize,
    column_count: usize,
    cell_lengths: &mut [usize],
) -> Result<()> {
    let value_len = primitive_value_len(column.encoding())?;
    validate_primitive_column_values(array, column)?;

    for row_index in 0..array.len() {
        let cell_len = if array.is_null(row_index) {
            if !column.nullable() {
                return Err(value_conversion_error(row_column_diagnostic(
                    column,
                    row_index,
                    DiagnosticCode::NullInNonNullableColumn,
                    "null value in non-nullable direct primitive column",
                )));
            }

            CELL_LEN_PREFIX_LEN
        } else if column.nullable() {
            CELL_LEN_PREFIX_LEN + value_len
        } else {
            value_len
        };

        cell_lengths[row_index * column_count + column_index] = cell_len;
    }

    Ok(())
}

fn validate_primitive_column_values(array: &dyn Array, column: &DirectColumnPlan) -> Result<()> {
    if matches!(
        column.encoding(),
        DirectColumnEncoding::Primitive(PrimitiveArrowToMssql::Float32ToReal)
    ) {
        let array = downcast_direct_array::<Float32Array>(array, column)?;
        for row_index in 0..array.len() {
            if array.is_null(row_index) {
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
        }
    } else if matches!(
        column.encoding(),
        DirectColumnEncoding::Primitive(PrimitiveArrowToMssql::Float64ToFloat)
    ) {
        let array = downcast_direct_array::<Float64Array>(array, column)?;
        for row_index in 0..array.len() {
            if array.is_null(row_index) {
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
        }
    } else if matches!(
        column.encoding(),
        DirectColumnEncoding::Primitive(PrimitiveArrowToMssql::UInt64ToCheckedBigInt)
    ) {
        let array = downcast_direct_array::<UInt64Array>(array, column)?;
        for row_index in 0..array.len() {
            if array.is_null(row_index) {
                continue;
            }

            uint64_checked_bigint_bytes(array.value(row_index), column, row_index)?;
        }
    }

    Ok(())
}

pub(crate) fn build_fixed_width_row_layout(
    row_count: usize,
    column_count: usize,
    cell_lengths: &[usize],
) -> Result<RowLayout> {
    build_fixed_width_row_range_layout(0, row_count, column_count, cell_lengths)
}

pub(crate) fn build_fixed_width_row_range_layout(
    start_row: usize,
    row_count: usize,
    column_count: usize,
    cell_lengths: &[usize],
) -> Result<RowLayout> {
    let end_row = start_row
        .checked_add(row_count)
        .ok_or_else(|| invalid_payload("direct row range end overflowed usize"))?;
    let mut row_token_offsets = Vec::with_capacity(row_count);
    let mut row_lengths = Vec::with_capacity(row_count);
    let mut cell_positions = Vec::with_capacity(row_count * column_count);
    let mut offset = 0usize;

    for row_index in start_row..end_row {
        let row_offset = offset;
        row_token_offsets.push(row_offset);
        offset = checked_add(offset, ROW_TOKEN_LEN)?;

        for column_index in 0..column_count {
            let cell_len = cell_lengths[row_index * column_count + column_index];
            cell_positions.push(CellPosition::new(
                row_index - start_row,
                column_index,
                offset,
                cell_len,
            ));
            offset = checked_add(offset, cell_len)?;
        }

        // Row length is the byte span from this row's ROW token through the
        // last encoded cell. RowLayout uses it to prove rows are contiguous.
        row_lengths.push(offset - row_offset);
    }

    RowLayout::new(row_token_offsets, row_lengths, cell_positions, offset)
}

/// Allocates a complete payload buffer and writes every row token.
///
/// The measured layout already knows where each row starts inside the payload.
/// This function creates a zero-filled buffer of the final payload size and
/// writes `0xD1` at every absolute row start offset. Later fill steps write
/// encoded cell bytes into the remaining positions.
pub(crate) fn allocate_rows_payload_with_tokens(layout: &RowLayout) -> Vec<u8> {
    let mut bytes = vec![0; layout.payload_len()];

    // One payload can contain many rows. Each row must start with the TDS ROW
    // token byte, and row_token_offsets gives those absolute byte positions.
    for &row_offset in layout.row_token_offsets() {
        bytes[row_offset] = TDS_ROW_TOKEN;
    }

    bytes
}

/// Fills one Boolean-to-bit column into an already allocated rows payload.
pub(crate) fn fill_boolean_column(
    array: &BooleanArray,
    column: &DirectColumnPlan,
    column_index: usize,
    column_count: usize,
    layout: &RowLayout,
    bytes: &mut [u8],
) -> Result<()> {
    for row_index in 0..array.len() {
        let cell = cell_position(layout, row_index, column_index, column_count)?;

        if array.is_null(row_index) {
            if !column.nullable() {
                return Err(value_conversion_error(row_column_diagnostic(
                    column,
                    row_index,
                    DiagnosticCode::NullInNonNullableColumn,
                    "null value in non-nullable direct primitive column",
                )));
            }

            write_null_cell(bytes, cell)?;
        } else {
            write_primitive_cell(bytes, cell, column, &[u8::from(array.value(row_index))])?;
        }
    }

    Ok(())
}

/// Fills one UInt8-to-tinyint column into an already allocated rows payload.
pub(crate) fn fill_uint8_column(
    array: &UInt8Array,
    column: &DirectColumnPlan,
    column_index: usize,
    column_count: usize,
    layout: &RowLayout,
    bytes: &mut [u8],
) -> Result<()> {
    fill_primitive_column(
        array,
        column,
        column_index,
        column_count,
        layout,
        bytes,
        |array, row_index| [array.value(row_index)],
    )
}

/// Fills one Int8-to-smallint column into an already allocated rows payload.
pub(crate) fn fill_int8_column(
    array: &Int8Array,
    column: &DirectColumnPlan,
    column_index: usize,
    column_count: usize,
    layout: &RowLayout,
    bytes: &mut [u8],
) -> Result<()> {
    fill_primitive_column(
        array,
        column,
        column_index,
        column_count,
        layout,
        bytes,
        |array, row_index| i16::from(array.value(row_index)).to_le_bytes(),
    )
}

/// Fills one Int16-to-smallint column into an already allocated rows payload.
pub(crate) fn fill_int16_column(
    array: &Int16Array,
    column: &DirectColumnPlan,
    column_index: usize,
    column_count: usize,
    layout: &RowLayout,
    bytes: &mut [u8],
) -> Result<()> {
    fill_primitive_column(
        array,
        column,
        column_index,
        column_count,
        layout,
        bytes,
        |array, row_index| array.value(row_index).to_le_bytes(),
    )
}

/// Fills one Int32-to-int column into an already allocated rows payload.
pub(crate) fn fill_int32_column(
    array: &Int32Array,
    column: &DirectColumnPlan,
    column_index: usize,
    column_count: usize,
    layout: &RowLayout,
    bytes: &mut [u8],
) -> Result<()> {
    for row_index in 0..array.len() {
        let cell = cell_position(layout, row_index, column_index, column_count)?;

        if array.is_null(row_index) {
            if !column.nullable() {
                return Err(value_conversion_error(row_column_diagnostic(
                    column,
                    row_index,
                    DiagnosticCode::NullInNonNullableColumn,
                    "null value in non-nullable direct primitive column",
                )));
            }

            write_null_cell(bytes, cell)?;
        } else {
            write_primitive_cell(bytes, cell, column, &array.value(row_index).to_le_bytes())?;
        }
    }

    Ok(())
}

/// Fills one UInt16-to-int column into an already allocated rows payload.
pub(crate) fn fill_uint16_column(
    array: &UInt16Array,
    column: &DirectColumnPlan,
    column_index: usize,
    column_count: usize,
    layout: &RowLayout,
    bytes: &mut [u8],
) -> Result<()> {
    fill_primitive_column(
        array,
        column,
        column_index,
        column_count,
        layout,
        bytes,
        |array, row_index| i32::from(array.value(row_index)).to_le_bytes(),
    )
}

/// Fills one Int64-to-bigint column into an already allocated rows payload.
pub(crate) fn fill_int64_column(
    array: &Int64Array,
    column: &DirectColumnPlan,
    column_index: usize,
    column_count: usize,
    layout: &RowLayout,
    bytes: &mut [u8],
) -> Result<()> {
    for row_index in 0..array.len() {
        let cell = cell_position(layout, row_index, column_index, column_count)?;

        if array.is_null(row_index) {
            if !column.nullable() {
                return Err(value_conversion_error(row_column_diagnostic(
                    column,
                    row_index,
                    DiagnosticCode::NullInNonNullableColumn,
                    "null value in non-nullable direct primitive column",
                )));
            }

            write_null_cell(bytes, cell)?;
        } else {
            write_primitive_cell(bytes, cell, column, &array.value(row_index).to_le_bytes())?;
        }
    }

    Ok(())
}

/// Fills one UInt32-to-bigint column into an already allocated rows payload.
pub(crate) fn fill_uint32_column(
    array: &UInt32Array,
    column: &DirectColumnPlan,
    column_index: usize,
    column_count: usize,
    layout: &RowLayout,
    bytes: &mut [u8],
) -> Result<()> {
    fill_primitive_column(
        array,
        column,
        column_index,
        column_count,
        layout,
        bytes,
        |array, row_index| i64::from(array.value(row_index)).to_le_bytes(),
    )
}

/// Fills one UInt64-to-checked-bigint column into an already allocated rows payload.
pub(crate) fn fill_uint64_checked_bigint_column(
    array: &UInt64Array,
    column: &DirectColumnPlan,
    column_index: usize,
    column_count: usize,
    layout: &RowLayout,
    bytes: &mut [u8],
) -> Result<()> {
    for row_index in 0..array.len() {
        let cell = cell_position(layout, row_index, column_index, column_count)?;

        if array.is_null(row_index) {
            if !column.nullable() {
                return Err(value_conversion_error(row_column_diagnostic(
                    column,
                    row_index,
                    DiagnosticCode::NullInNonNullableColumn,
                    "null value in non-nullable direct primitive column",
                )));
            }

            write_null_cell(bytes, cell)?;
        } else {
            let value = uint64_checked_bigint_bytes(array.value(row_index), column, row_index)?;
            write_primitive_cell(bytes, cell, column, &value)?;
        }
    }

    Ok(())
}

/// Fills one Float32-to-real column into an already allocated rows payload.
pub(crate) fn fill_float32_column(
    array: &Float32Array,
    column: &DirectColumnPlan,
    column_index: usize,
    column_count: usize,
    layout: &RowLayout,
    bytes: &mut [u8],
) -> Result<()> {
    for row_index in 0..array.len() {
        let cell = cell_position(layout, row_index, column_index, column_count)?;

        if array.is_null(row_index) {
            if !column.nullable() {
                return Err(value_conversion_error(row_column_diagnostic(
                    column,
                    row_index,
                    DiagnosticCode::NullInNonNullableColumn,
                    "null value in non-nullable direct primitive column",
                )));
            }

            write_null_cell(bytes, cell)?;
        } else {
            let value = array.value(row_index);
            if !value.is_finite() {
                return Err(value_conversion_error(row_column_diagnostic(
                    column,
                    row_index,
                    DiagnosticCode::NonFiniteFloat,
                    format!("non-finite floating point value {value} is not supported"),
                )));
            }

            write_primitive_cell(bytes, cell, column, &value.to_le_bytes())?;
        }
    }

    Ok(())
}

/// Fills one Float64-to-float column into an already allocated rows payload.
pub(crate) fn fill_float64_column(
    array: &Float64Array,
    column: &DirectColumnPlan,
    column_index: usize,
    column_count: usize,
    layout: &RowLayout,
    bytes: &mut [u8],
) -> Result<()> {
    for row_index in 0..array.len() {
        let cell = cell_position(layout, row_index, column_index, column_count)?;

        if array.is_null(row_index) {
            if !column.nullable() {
                return Err(value_conversion_error(row_column_diagnostic(
                    column,
                    row_index,
                    DiagnosticCode::NullInNonNullableColumn,
                    "null value in non-nullable direct primitive column",
                )));
            }

            write_null_cell(bytes, cell)?;
        } else {
            let value = array.value(row_index);
            if !value.is_finite() {
                return Err(value_conversion_error(row_column_diagnostic(
                    column,
                    row_index,
                    DiagnosticCode::NonFiniteFloat,
                    format!("non-finite floating point value {value} is not supported"),
                )));
            }

            write_primitive_cell(bytes, cell, column, &value.to_le_bytes())?;
        }
    }

    Ok(())
}

fn fill_primitive_column<Array, ValueBytes>(
    array: &Array,
    column: &DirectColumnPlan,
    column_index: usize,
    column_count: usize,
    layout: &RowLayout,
    bytes: &mut [u8],
    value_bytes: impl Fn(&Array, usize) -> ValueBytes,
) -> Result<()>
where
    Array: arrow_array::Array,
    ValueBytes: AsRef<[u8]>,
{
    for row_index in 0..array.len() {
        let cell = cell_position(layout, row_index, column_index, column_count)?;

        if array.is_null(row_index) {
            if !column.nullable() {
                return Err(value_conversion_error(row_column_diagnostic(
                    column,
                    row_index,
                    DiagnosticCode::NullInNonNullableColumn,
                    "null value in non-nullable direct primitive column",
                )));
            }

            write_null_cell(bytes, cell)?;
        } else {
            let value_bytes = value_bytes(array, row_index);
            write_primitive_cell(bytes, cell, column, value_bytes.as_ref())?;
        }
    }

    Ok(())
}

fn uint64_checked_bigint_bytes(
    value: u64,
    column: &DirectColumnPlan,
    row_index: usize,
) -> Result<[u8; 8]> {
    i64::try_from(value).map(i64::to_le_bytes).map_err(|_| {
        value_conversion_error(row_column_diagnostic(
            column,
            row_index,
            DiagnosticCode::IntegerOutOfRange,
            format!("Arrow UInt64 value {value} does not fit planned SQL Server bigint"),
        ))
    })
}

/// Appends one Boolean-to-bit cell to a raw bulk append buffer.
pub(crate) fn append_boolean_cell(
    buf: &mut tiberius::RawRowsAppendBuffer<'_>,
    array: &BooleanArray,
    column: &DirectColumnPlan,
    row_index: usize,
    measured_len: usize,
) -> Result<()> {
    if array.is_null(row_index) {
        return append_null_cell(buf, column, row_index, measured_len);
    }

    append_primitive_cell(
        buf,
        column,
        measured_len,
        &[u8::from(array.value(row_index))],
    )
}

/// Appends one UInt8-to-tinyint cell to a raw bulk append buffer.
pub(crate) fn append_uint8_cell(
    buf: &mut tiberius::RawRowsAppendBuffer<'_>,
    array: &UInt8Array,
    column: &DirectColumnPlan,
    row_index: usize,
    measured_len: usize,
) -> Result<()> {
    append_primitive_array_cell(
        buf,
        array,
        column,
        row_index,
        measured_len,
        |array, row_index| [array.value(row_index)],
    )
}

/// Appends one Int8-to-smallint cell to a raw bulk append buffer.
pub(crate) fn append_int8_cell(
    buf: &mut tiberius::RawRowsAppendBuffer<'_>,
    array: &Int8Array,
    column: &DirectColumnPlan,
    row_index: usize,
    measured_len: usize,
) -> Result<()> {
    append_primitive_array_cell(
        buf,
        array,
        column,
        row_index,
        measured_len,
        |array, row_index| i16::from(array.value(row_index)).to_le_bytes(),
    )
}

/// Appends one Int16-to-smallint cell to a raw bulk append buffer.
pub(crate) fn append_int16_cell(
    buf: &mut tiberius::RawRowsAppendBuffer<'_>,
    array: &Int16Array,
    column: &DirectColumnPlan,
    row_index: usize,
    measured_len: usize,
) -> Result<()> {
    append_primitive_array_cell(
        buf,
        array,
        column,
        row_index,
        measured_len,
        |array, row_index| array.value(row_index).to_le_bytes(),
    )
}

/// Appends one Int32-to-int cell to a raw bulk append buffer.
pub(crate) fn append_int32_cell(
    buf: &mut tiberius::RawRowsAppendBuffer<'_>,
    array: &Int32Array,
    column: &DirectColumnPlan,
    row_index: usize,
    measured_len: usize,
) -> Result<()> {
    if array.is_null(row_index) {
        return append_null_cell(buf, column, row_index, measured_len);
    }

    append_primitive_cell(
        buf,
        column,
        measured_len,
        &array.value(row_index).to_le_bytes(),
    )
}

/// Appends one UInt16-to-int cell to a raw bulk append buffer.
pub(crate) fn append_uint16_cell(
    buf: &mut tiberius::RawRowsAppendBuffer<'_>,
    array: &UInt16Array,
    column: &DirectColumnPlan,
    row_index: usize,
    measured_len: usize,
) -> Result<()> {
    append_primitive_array_cell(
        buf,
        array,
        column,
        row_index,
        measured_len,
        |array, row_index| i32::from(array.value(row_index)).to_le_bytes(),
    )
}

/// Appends one Int64-to-bigint cell to a raw bulk append buffer.
pub(crate) fn append_int64_cell(
    buf: &mut tiberius::RawRowsAppendBuffer<'_>,
    array: &Int64Array,
    column: &DirectColumnPlan,
    row_index: usize,
    measured_len: usize,
) -> Result<()> {
    if array.is_null(row_index) {
        return append_null_cell(buf, column, row_index, measured_len);
    }

    append_primitive_cell(
        buf,
        column,
        measured_len,
        &array.value(row_index).to_le_bytes(),
    )
}

/// Appends one UInt32-to-bigint cell to a raw bulk append buffer.
pub(crate) fn append_uint32_cell(
    buf: &mut tiberius::RawRowsAppendBuffer<'_>,
    array: &UInt32Array,
    column: &DirectColumnPlan,
    row_index: usize,
    measured_len: usize,
) -> Result<()> {
    append_primitive_array_cell(
        buf,
        array,
        column,
        row_index,
        measured_len,
        |array, row_index| i64::from(array.value(row_index)).to_le_bytes(),
    )
}

/// Appends one UInt64-to-checked-bigint cell to a raw bulk append buffer.
pub(crate) fn append_uint64_checked_bigint_cell(
    buf: &mut tiberius::RawRowsAppendBuffer<'_>,
    array: &UInt64Array,
    column: &DirectColumnPlan,
    row_index: usize,
    measured_len: usize,
) -> Result<()> {
    if array.is_null(row_index) {
        return append_null_cell(buf, column, row_index, measured_len);
    }

    let value = uint64_checked_bigint_bytes(array.value(row_index), column, row_index)?;
    append_primitive_cell(buf, column, measured_len, &value)
}

/// Appends one Float32-to-real cell to a raw bulk append buffer.
pub(crate) fn append_float32_cell(
    buf: &mut tiberius::RawRowsAppendBuffer<'_>,
    array: &Float32Array,
    column: &DirectColumnPlan,
    row_index: usize,
    measured_len: usize,
) -> Result<()> {
    if array.is_null(row_index) {
        return append_null_cell(buf, column, row_index, measured_len);
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

    append_primitive_cell(buf, column, measured_len, &value.to_le_bytes())
}

/// Appends one Float64-to-float cell to a raw bulk append buffer.
pub(crate) fn append_float64_cell(
    buf: &mut tiberius::RawRowsAppendBuffer<'_>,
    array: &Float64Array,
    column: &DirectColumnPlan,
    row_index: usize,
    measured_len: usize,
) -> Result<()> {
    if array.is_null(row_index) {
        return append_null_cell(buf, column, row_index, measured_len);
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

    append_primitive_cell(buf, column, measured_len, &value.to_le_bytes())
}

fn append_primitive_array_cell<Array, ValueBytes>(
    buf: &mut tiberius::RawRowsAppendBuffer<'_>,
    array: &Array,
    column: &DirectColumnPlan,
    row_index: usize,
    measured_len: usize,
    value_bytes: impl Fn(&Array, usize) -> ValueBytes,
) -> Result<()>
where
    Array: arrow_array::Array,
    ValueBytes: AsRef<[u8]>,
{
    if array.is_null(row_index) {
        return append_null_cell(buf, column, row_index, measured_len);
    }

    let value_bytes = value_bytes(array, row_index);
    append_primitive_cell(buf, column, measured_len, value_bytes.as_ref())
}

fn cell_position(
    layout: &RowLayout,
    row_index: usize,
    column_index: usize,
    column_count: usize,
) -> Result<&CellPosition> {
    let index = row_index
        .checked_mul(column_count)
        .and_then(|base| base.checked_add(column_index))
        .ok_or_else(|| invalid_payload("cell position index overflowed usize"))?;

    layout
        .cell_positions()
        .get(index)
        .ok_or_else(|| invalid_payload("cell position is outside measured row layout"))
}

fn append_null_cell(
    buf: &mut tiberius::RawRowsAppendBuffer<'_>,
    column: &DirectColumnPlan,
    row_index: usize,
    measured_len: usize,
) -> Result<()> {
    profile::record_null_cell();

    if !column.nullable() {
        return Err(value_conversion_error(row_column_diagnostic(
            column,
            row_index,
            DiagnosticCode::NullInNonNullableColumn,
            "null value in non-nullable direct primitive column",
        )));
    }

    if measured_len != CELL_LEN_PREFIX_LEN {
        return Err(invalid_payload(format!(
            "measured null primitive cell at row {row_index} column {} has length {}, expected {CELL_LEN_PREFIX_LEN}",
            column.source_index(),
            measured_len
        )));
    }

    buf.put_u8(0);
    Ok(())
}

fn append_primitive_cell(
    buf: &mut tiberius::RawRowsAppendBuffer<'_>,
    column: &DirectColumnPlan,
    measured_len: usize,
    value: &[u8],
) -> Result<()> {
    if column.nullable() {
        let expected_len = CELL_LEN_PREFIX_LEN
            .checked_add(value.len())
            .ok_or_else(|| invalid_payload("nullable primitive cell length overflowed usize"))?;
        if measured_len != expected_len {
            return Err(invalid_payload(format!(
                "measured nullable primitive cell for column {} has length {}, expected {expected_len}",
                column.source_name(),
                measured_len
            )));
        }

        let value_len = u8::try_from(value.len())
            .map_err(|_| invalid_payload("nullable primitive cell value length does not fit u8"))?;
        buf.put_u8(value_len);
    } else if measured_len != value.len() {
        return Err(invalid_payload(format!(
            "measured fixed-width primitive cell for column {} has length {}, expected {}",
            column.source_name(),
            measured_len,
            value.len()
        )));
    }

    buf.extend_from_slice(value);
    Ok(())
}

fn write_null_cell(bytes: &mut [u8], cell: &CellPosition) -> Result<()> {
    if cell.len() != CELL_LEN_PREFIX_LEN {
        return Err(invalid_payload(format!(
            "null cell at row {} column {} has length {}, expected 1",
            cell.row_index(),
            cell.column_index(),
            cell.len()
        )));
    }

    let Some(byte) = bytes.get_mut(cell.offset()) else {
        return Err(invalid_payload("null cell offset is outside payload"));
    };

    *byte = 0;
    Ok(())
}

fn write_primitive_cell(
    bytes: &mut [u8],
    cell: &CellPosition,
    column: &DirectColumnPlan,
    value: &[u8],
) -> Result<()> {
    if column.nullable() {
        return write_nullable_primitive_cell(bytes, cell, value);
    }

    write_fixed_width_cell(bytes, cell, value)
}

fn write_nullable_primitive_cell(
    bytes: &mut [u8],
    cell: &CellPosition,
    value: &[u8],
) -> Result<()> {
    let expected_len = CELL_LEN_PREFIX_LEN
        .checked_add(value.len())
        .ok_or_else(|| invalid_payload("nullable primitive cell length overflowed usize"))?;

    if cell.len() != expected_len {
        return Err(invalid_payload(format!(
            "nullable primitive cell at row {} column {} has length {}, expected {expected_len}",
            cell.row_index(),
            cell.column_index(),
            cell.len()
        )));
    }

    let start = cell.offset();
    let end = start
        .checked_add(cell.len())
        .ok_or_else(|| invalid_payload("fixed-width cell end overflowed usize"))?;
    let Some(cell_bytes) = bytes.get_mut(start..end) else {
        return Err(invalid_payload("fixed-width cell range is outside payload"));
    };

    cell_bytes[0] = u8::try_from(value.len())
        .map_err(|_| invalid_payload("nullable primitive cell value length does not fit u8"))?;
    cell_bytes[1..].copy_from_slice(value);

    Ok(())
}

fn write_fixed_width_cell(bytes: &mut [u8], cell: &CellPosition, value: &[u8]) -> Result<()> {
    if cell.len() != value.len() {
        return Err(invalid_payload(format!(
            "fixed-width cell at row {} column {} has length {}, expected {}",
            cell.row_index(),
            cell.column_index(),
            cell.len(),
            value.len()
        )));
    }

    let start = cell.offset();
    let end = start
        .checked_add(cell.len())
        .ok_or_else(|| invalid_payload("fixed-width cell end overflowed usize"))?;
    let Some(cell_bytes) = bytes.get_mut(start..end) else {
        return Err(invalid_payload("fixed-width cell range is outside payload"));
    };

    cell_bytes.copy_from_slice(value);

    Ok(())
}

fn primitive_value_len(encoding: DirectColumnEncoding) -> Result<usize> {
    match encoding {
        DirectColumnEncoding::Primitive(PrimitiveArrowToMssql::BooleanToBit) => Ok(1),
        DirectColumnEncoding::Primitive(PrimitiveArrowToMssql::UInt8ToTinyInt) => Ok(1),
        DirectColumnEncoding::Primitive(
            PrimitiveArrowToMssql::Int8ToSmallInt | PrimitiveArrowToMssql::Int16ToSmallInt,
        ) => Ok(2),
        DirectColumnEncoding::Primitive(PrimitiveArrowToMssql::Int32ToInt) => Ok(4),
        DirectColumnEncoding::Primitive(PrimitiveArrowToMssql::UInt16ToInt) => Ok(4),
        DirectColumnEncoding::Primitive(PrimitiveArrowToMssql::Int64ToBigInt) => Ok(8),
        DirectColumnEncoding::Primitive(PrimitiveArrowToMssql::UInt32ToBigInt) => Ok(8),
        DirectColumnEncoding::Primitive(PrimitiveArrowToMssql::UInt64ToCheckedBigInt) => Ok(8),
        DirectColumnEncoding::Primitive(PrimitiveArrowToMssql::Float32ToReal) => Ok(4),
        DirectColumnEncoding::Primitive(PrimitiveArrowToMssql::Float64ToFloat) => Ok(8),
        DirectColumnEncoding::UInt64Decimal20_0 => Err(unsupported_batch(
            "direct primitive layout is not implemented for UInt64 decimal20_0",
        )),
        DirectColumnEncoding::Decimal(classification) => Err(unsupported_batch(format!(
            "direct primitive layout is not implemented for decimal mapping {classification:?}"
        ))),
        DirectColumnEncoding::VariableWidth(other) => Err(unsupported_batch(format!(
            "direct primitive layout is not implemented for variable-width mapping {other:?}"
        ))),
        DirectColumnEncoding::Temporal(other) => Err(unsupported_batch(format!(
            "direct primitive layout is not implemented for temporal mapping {other:?}"
        ))),
    }
}

fn checked_add(lhs: usize, rhs: usize) -> Result<usize> {
    lhs.checked_add(rhs)
        .ok_or_else(|| invalid_payload("direct primitive row layout length overflowed usize"))
}

fn row_column_diagnostic(
    column: &DirectColumnPlan,
    row_index: usize,
    code: DiagnosticCode,
    message: impl Into<String>,
) -> Diagnostic {
    Diagnostic::error(code, message)
        .with_field(FieldRef::new(column.source_index(), column.source_name()))
        .with_row(row_index)
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

fn value_conversion_error(diagnostic: Diagnostic) -> Error {
    Error::ValueConversion {
        diagnostics: DiagnosticSet::from(vec![diagnostic]),
    }
}

fn invalid_payload(message: impl Into<String>) -> Error {
    Error::DirectEncoding {
        diagnostics: DiagnosticSet::from(vec![Diagnostic::error(
            DiagnosticCode::DirectEncodingInvalidPayload,
            message,
        )]),
    }
}

fn unsupported_batch(message: impl Into<String>) -> Error {
    Error::DirectEncoding {
        diagnostics: DiagnosticSet::from(vec![Diagnostic::error(
            DiagnosticCode::DirectEncodingUnsupportedBatch,
            message,
        )]),
    }
}

#[cfg(test)]
mod tests {
    use arrow_array::{
        BooleanArray, Float32Array, Float64Array, Int8Array, Int16Array, Int32Array, Int64Array,
        UInt8Array, UInt16Array, UInt32Array,
    };
    use arrow_schema::DataType;

    use crate::{
        ArrowFieldRef, DiagnosticCode, Error, Identifier, MssqlColumn, MssqlType, SchemaMapping,
    };

    use super::{
        allocate_rows_payload_with_tokens, build_fixed_width_row_layout, fill_boolean_column,
        fill_float32_column, fill_float64_column, fill_int8_column, fill_int16_column,
        fill_int32_column, fill_int64_column, fill_uint8_column, fill_uint16_column,
        fill_uint32_column, measure_primitive_column_cell_lengths,
    };
    use crate::write::direct::payload::TDS_ROW_TOKEN;
    use crate::write::direct::plan::{CurrentDirectMappings, DirectEncoderPlan};

    #[test]
    fn measures_empty_primitive_column_as_empty_layout_input() {
        let mappings = vec![mapping(0, "id", DataType::Int32, MssqlType::Int, false)];
        let plan = plan(&mappings);
        let array = Int32Array::from(Vec::<i32>::new());
        let mut cell_lengths = Vec::new();

        measure_primitive_column_cell_lengths(&array, &plan.columns()[0], 0, 1, &mut cell_lengths)
            .unwrap();

        let layout = build_fixed_width_row_layout(0, 1, &cell_lengths).unwrap();

        assert_eq!(layout.row_count(), 0);
        assert_eq!(layout.row_token_offsets(), []);
        assert_eq!(layout.row_lengths(), []);
        assert_eq!(layout.cell_positions(), []);
        assert_eq!(layout.payload_len(), 0);
    }

    #[test]
    fn measures_primitive_columns_into_row_major_length_matrix() {
        let mappings = vec![
            mapping(0, "is_active", DataType::Boolean, MssqlType::Bit, false),
            mapping(1, "quantity", DataType::Int32, MssqlType::Int, false),
            mapping(2, "total", DataType::Int64, MssqlType::BigInt, false),
            mapping(
                3,
                "ratio",
                DataType::Float64,
                MssqlType::Float { precision: 53 },
                false,
            ),
        ];
        let plan = plan(&mappings);
        let arrays: [&dyn arrow_array::Array; 4] = [
            &BooleanArray::from(vec![true, false]),
            &Int32Array::from(vec![1, 2]),
            &Int64Array::from(vec![10, 20]),
            &Float64Array::from(vec![1.25, 2.5]),
        ];
        let row_count = 2;
        let column_count = plan.column_count();
        let mut cell_lengths = vec![0; row_count * column_count];

        for (column_index, (array, column)) in arrays.iter().zip(plan.columns()).enumerate() {
            measure_primitive_column_cell_lengths(
                *array,
                column,
                column_index,
                column_count,
                &mut cell_lengths,
            )
            .unwrap();
        }

        assert_eq!(cell_lengths, [1, 4, 8, 8, 1, 4, 8, 8]);

        let layout = build_fixed_width_row_layout(row_count, column_count, &cell_lengths).unwrap();

        assert_eq!(layout.row_token_offsets(), [0, 22]);
        assert_eq!(layout.row_lengths(), [22, 22]);
        assert_eq!(layout.payload_len(), 44);
        assert_cell_positions(
            layout.cell_positions(),
            &[
                (0, 0, 1, 1),
                (0, 1, 2, 4),
                (0, 2, 6, 8),
                (0, 3, 14, 8),
                (1, 0, 23, 1),
                (1, 1, 24, 4),
                (1, 2, 28, 8),
                (1, 3, 36, 8),
            ],
        );
    }

    #[test]
    fn measures_nullable_primitive_column_nulls_as_single_zero_length_byte() {
        let mappings = vec![
            mapping(0, "is_active", DataType::Boolean, MssqlType::Bit, true),
            mapping(1, "quantity", DataType::Int32, MssqlType::Int, true),
        ];
        let plan = plan(&mappings);
        let arrays: [&dyn arrow_array::Array; 2] = [
            &BooleanArray::from(vec![Some(true), None]),
            &Int32Array::from(vec![None, Some(7)]),
        ];
        let row_count = 2;
        let column_count = plan.column_count();
        let mut cell_lengths = vec![0; row_count * column_count];

        for (column_index, (array, column)) in arrays.iter().zip(plan.columns()).enumerate() {
            measure_primitive_column_cell_lengths(
                *array,
                column,
                column_index,
                column_count,
                &mut cell_lengths,
            )
            .unwrap();
        }

        assert_eq!(cell_lengths, [2, 1, 1, 5]);

        let layout = build_fixed_width_row_layout(row_count, column_count, &cell_lengths).unwrap();

        assert_eq!(layout.row_token_offsets(), [0, 4]);
        assert_eq!(layout.row_lengths(), [4, 7]);
        assert_eq!(layout.payload_len(), 11);
        assert_cell_positions(
            layout.cell_positions(),
            &[(0, 0, 1, 2), (0, 1, 3, 1), (1, 0, 5, 1), (1, 1, 6, 5)],
        );
    }

    #[test]
    fn allocates_payload_and_writes_row_tokens_from_layout() {
        let layout = build_fixed_width_row_layout(3, 2, &[2, 5, 1, 5, 2, 1]).unwrap();

        let bytes = allocate_rows_payload_with_tokens(&layout);

        assert_eq!(layout.row_token_offsets(), [0, 8, 15]);
        assert_eq!(bytes.len(), 19);
        assert_eq!(bytes[0], TDS_ROW_TOKEN);
        assert_eq!(bytes[8], TDS_ROW_TOKEN);
        assert_eq!(bytes[15], TDS_ROW_TOKEN);
        assert!(
            bytes
                .iter()
                .enumerate()
                .filter(|(_, byte)| **byte == TDS_ROW_TOKEN)
                .all(|(index, _)| layout.row_token_offsets().contains(&index))
        );
    }

    #[test]
    fn fills_boolean_column_into_existing_payload_positions() {
        let mappings = vec![mapping(
            0,
            "is_active",
            DataType::Boolean,
            MssqlType::Bit,
            false,
        )];
        let plan = plan(&mappings);
        let array = BooleanArray::from(vec![true, false]);
        let row_count = 2;
        let column_count = 1;
        let mut cell_lengths = vec![0; row_count * column_count];
        measure_primitive_column_cell_lengths(
            &array,
            &plan.columns()[0],
            0,
            column_count,
            &mut cell_lengths,
        )
        .unwrap();
        let layout = build_fixed_width_row_layout(row_count, column_count, &cell_lengths).unwrap();
        let mut bytes = allocate_rows_payload_with_tokens(&layout);

        fill_boolean_column(
            &array,
            &plan.columns()[0],
            0,
            column_count,
            &layout,
            &mut bytes,
        )
        .unwrap();

        assert_eq!(bytes, [TDS_ROW_TOKEN, 1, TDS_ROW_TOKEN, 0]);
    }

    #[test]
    fn fills_nullable_boolean_column_with_zero_length_null_cell() {
        let mappings = vec![mapping(
            0,
            "is_active",
            DataType::Boolean,
            MssqlType::Bit,
            true,
        )];
        let plan = plan(&mappings);
        let array = BooleanArray::from(vec![Some(true), None, Some(false)]);
        let row_count = 3;
        let column_count = 1;
        let mut cell_lengths = vec![0; row_count * column_count];
        measure_primitive_column_cell_lengths(
            &array,
            &plan.columns()[0],
            0,
            column_count,
            &mut cell_lengths,
        )
        .unwrap();
        let layout = build_fixed_width_row_layout(row_count, column_count, &cell_lengths).unwrap();
        let mut bytes = allocate_rows_payload_with_tokens(&layout);

        fill_boolean_column(
            &array,
            &plan.columns()[0],
            0,
            column_count,
            &layout,
            &mut bytes,
        )
        .unwrap();

        assert_eq!(
            bytes,
            [TDS_ROW_TOKEN, 1, 1, TDS_ROW_TOKEN, 0, TDS_ROW_TOKEN, 1, 0]
        );
    }

    #[test]
    fn fills_issue_75_integer_columns_as_little_endian_values() {
        let mappings = vec![
            mapping(0, "tiny", DataType::UInt8, MssqlType::TinyInt, false),
            mapping(1, "signed_tiny", DataType::Int8, MssqlType::SmallInt, false),
            mapping(2, "small", DataType::Int16, MssqlType::SmallInt, false),
            mapping(
                3,
                "unsigned_medium",
                DataType::UInt16,
                MssqlType::Int,
                false,
            ),
            mapping(
                4,
                "unsigned_total",
                DataType::UInt32,
                MssqlType::BigInt,
                false,
            ),
        ];
        let plan = plan(&mappings);
        let tiny = UInt8Array::from(vec![u8::MIN, u8::MAX]);
        let signed_tiny = Int8Array::from(vec![i8::MIN, i8::MAX]);
        let small = Int16Array::from(vec![i16::MIN, i16::MAX]);
        let unsigned_medium = UInt16Array::from(vec![u16::MIN, u16::MAX]);
        let unsigned_total = UInt32Array::from(vec![u32::MIN, u32::MAX]);
        let arrays: [&dyn arrow_array::Array; 5] = [
            &tiny,
            &signed_tiny,
            &small,
            &unsigned_medium,
            &unsigned_total,
        ];
        let row_count = 2;
        let column_count = plan.column_count();
        let mut cell_lengths = vec![0; row_count * column_count];

        for (column_index, (array, column)) in arrays.iter().zip(plan.columns()).enumerate() {
            measure_primitive_column_cell_lengths(
                *array,
                column,
                column_index,
                column_count,
                &mut cell_lengths,
            )
            .unwrap();
        }

        assert_eq!(cell_lengths, [1, 2, 2, 4, 8, 1, 2, 2, 4, 8]);

        let layout = build_fixed_width_row_layout(row_count, column_count, &cell_lengths).unwrap();
        let mut bytes = allocate_rows_payload_with_tokens(&layout);

        fill_uint8_column(
            &tiny,
            &plan.columns()[0],
            0,
            column_count,
            &layout,
            &mut bytes,
        )
        .unwrap();
        fill_int8_column(
            &signed_tiny,
            &plan.columns()[1],
            1,
            column_count,
            &layout,
            &mut bytes,
        )
        .unwrap();
        fill_int16_column(
            &small,
            &plan.columns()[2],
            2,
            column_count,
            &layout,
            &mut bytes,
        )
        .unwrap();
        fill_uint16_column(
            &unsigned_medium,
            &plan.columns()[3],
            3,
            column_count,
            &layout,
            &mut bytes,
        )
        .unwrap();
        fill_uint32_column(
            &unsigned_total,
            &plan.columns()[4],
            4,
            column_count,
            &layout,
            &mut bytes,
        )
        .unwrap();

        assert_eq!(
            bytes,
            [
                TDS_ROW_TOKEN,
                0x00,
                0x80,
                0xFF,
                0x00,
                0x80,
                0x00,
                0x00,
                0x00,
                0x00,
                0x00,
                0x00,
                0x00,
                0x00,
                0x00,
                0x00,
                0x00,
                0x00,
                TDS_ROW_TOKEN,
                0xFF,
                0x7F,
                0x00,
                0xFF,
                0x7F,
                0xFF,
                0xFF,
                0x00,
                0x00,
                0xFF,
                0xFF,
                0xFF,
                0xFF,
                0x00,
                0x00,
                0x00,
                0x00,
            ]
        );
    }

    #[test]
    fn fills_nullable_issue_75_integer_columns_with_zero_length_null_cells() {
        let mappings = vec![
            mapping(0, "tiny", DataType::UInt8, MssqlType::TinyInt, true),
            mapping(1, "signed_tiny", DataType::Int8, MssqlType::SmallInt, true),
            mapping(2, "small", DataType::Int16, MssqlType::SmallInt, true),
            mapping(3, "unsigned_medium", DataType::UInt16, MssqlType::Int, true),
            mapping(
                4,
                "unsigned_total",
                DataType::UInt32,
                MssqlType::BigInt,
                true,
            ),
        ];
        let plan = plan(&mappings);
        let tiny = UInt8Array::from(vec![Some(7), None]);
        let signed_tiny = Int8Array::from(vec![Some(-1), None]);
        let small = Int16Array::from(vec![Some(-2), None]);
        let unsigned_medium = UInt16Array::from(vec![Some(3), None]);
        let unsigned_total = UInt32Array::from(vec![Some(4), None]);
        let arrays: [&dyn arrow_array::Array; 5] = [
            &tiny,
            &signed_tiny,
            &small,
            &unsigned_medium,
            &unsigned_total,
        ];
        let row_count = 2;
        let column_count = plan.column_count();
        let mut cell_lengths = vec![0; row_count * column_count];

        for (column_index, (array, column)) in arrays.iter().zip(plan.columns()).enumerate() {
            measure_primitive_column_cell_lengths(
                *array,
                column,
                column_index,
                column_count,
                &mut cell_lengths,
            )
            .unwrap();
        }

        assert_eq!(cell_lengths, [2, 3, 3, 5, 9, 1, 1, 1, 1, 1]);

        let layout = build_fixed_width_row_layout(row_count, column_count, &cell_lengths).unwrap();
        let mut bytes = allocate_rows_payload_with_tokens(&layout);

        fill_uint8_column(
            &tiny,
            &plan.columns()[0],
            0,
            column_count,
            &layout,
            &mut bytes,
        )
        .unwrap();
        fill_int8_column(
            &signed_tiny,
            &plan.columns()[1],
            1,
            column_count,
            &layout,
            &mut bytes,
        )
        .unwrap();
        fill_int16_column(
            &small,
            &plan.columns()[2],
            2,
            column_count,
            &layout,
            &mut bytes,
        )
        .unwrap();
        fill_uint16_column(
            &unsigned_medium,
            &plan.columns()[3],
            3,
            column_count,
            &layout,
            &mut bytes,
        )
        .unwrap();
        fill_uint32_column(
            &unsigned_total,
            &plan.columns()[4],
            4,
            column_count,
            &layout,
            &mut bytes,
        )
        .unwrap();

        assert_eq!(
            bytes,
            [
                TDS_ROW_TOKEN,
                1,
                7,
                2,
                0xFF,
                0xFF,
                2,
                0xFE,
                0xFF,
                4,
                3,
                0,
                0,
                0,
                8,
                4,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                TDS_ROW_TOKEN,
                0,
                0,
                0,
                0,
                0,
            ]
        );
    }

    #[test]
    fn fills_int32_column_as_little_endian_int_values() {
        let mappings = vec![mapping(
            0,
            "quantity",
            DataType::Int32,
            MssqlType::Int,
            false,
        )];
        let plan = plan(&mappings);
        let array = Int32Array::from(vec![i32::MIN, -1, 0, i32::MAX]);
        let row_count = array.len();
        let column_count = 1;
        let mut cell_lengths = vec![0; row_count * column_count];
        measure_primitive_column_cell_lengths(
            &array,
            &plan.columns()[0],
            0,
            column_count,
            &mut cell_lengths,
        )
        .unwrap();
        let layout = build_fixed_width_row_layout(row_count, column_count, &cell_lengths).unwrap();
        let mut bytes = allocate_rows_payload_with_tokens(&layout);

        fill_int32_column(
            &array,
            &plan.columns()[0],
            0,
            column_count,
            &layout,
            &mut bytes,
        )
        .unwrap();

        assert_eq!(
            bytes,
            [
                TDS_ROW_TOKEN,
                0x00,
                0x00,
                0x00,
                0x80,
                TDS_ROW_TOKEN,
                0xFF,
                0xFF,
                0xFF,
                0xFF,
                TDS_ROW_TOKEN,
                0x00,
                0x00,
                0x00,
                0x00,
                TDS_ROW_TOKEN,
                0xFF,
                0xFF,
                0xFF,
                0x7F,
            ]
        );
    }

    #[test]
    fn fills_nullable_int32_column_with_zero_length_null_cell() {
        let mappings = vec![mapping(
            0,
            "quantity",
            DataType::Int32,
            MssqlType::Int,
            true,
        )];
        let plan = plan(&mappings);
        let array = Int32Array::from(vec![Some(7), None]);
        let row_count = array.len();
        let column_count = 1;
        let mut cell_lengths = vec![0; row_count * column_count];
        measure_primitive_column_cell_lengths(
            &array,
            &plan.columns()[0],
            0,
            column_count,
            &mut cell_lengths,
        )
        .unwrap();
        let layout = build_fixed_width_row_layout(row_count, column_count, &cell_lengths).unwrap();
        let mut bytes = allocate_rows_payload_with_tokens(&layout);

        fill_int32_column(
            &array,
            &plan.columns()[0],
            0,
            column_count,
            &layout,
            &mut bytes,
        )
        .unwrap();

        assert_eq!(bytes, [TDS_ROW_TOKEN, 4, 7, 0, 0, 0, TDS_ROW_TOKEN, 0]);
    }

    #[test]
    fn fills_int64_column_as_little_endian_bigint_values() {
        let mappings = vec![mapping(
            0,
            "total",
            DataType::Int64,
            MssqlType::BigInt,
            false,
        )];
        let plan = plan(&mappings);
        let array = Int64Array::from(vec![i64::MIN, -1, 0, i64::MAX]);
        let row_count = array.len();
        let column_count = 1;
        let mut cell_lengths = vec![0; row_count * column_count];
        measure_primitive_column_cell_lengths(
            &array,
            &plan.columns()[0],
            0,
            column_count,
            &mut cell_lengths,
        )
        .unwrap();
        let layout = build_fixed_width_row_layout(row_count, column_count, &cell_lengths).unwrap();
        let mut bytes = allocate_rows_payload_with_tokens(&layout);

        fill_int64_column(
            &array,
            &plan.columns()[0],
            0,
            column_count,
            &layout,
            &mut bytes,
        )
        .unwrap();

        assert_eq!(
            bytes,
            [
                TDS_ROW_TOKEN,
                0x00,
                0x00,
                0x00,
                0x00,
                0x00,
                0x00,
                0x00,
                0x80,
                TDS_ROW_TOKEN,
                0xFF,
                0xFF,
                0xFF,
                0xFF,
                0xFF,
                0xFF,
                0xFF,
                0xFF,
                TDS_ROW_TOKEN,
                0x00,
                0x00,
                0x00,
                0x00,
                0x00,
                0x00,
                0x00,
                0x00,
                TDS_ROW_TOKEN,
                0xFF,
                0xFF,
                0xFF,
                0xFF,
                0xFF,
                0xFF,
                0xFF,
                0x7F,
            ]
        );
    }

    #[test]
    fn fills_nullable_int64_column_with_zero_length_null_cell() {
        let mappings = vec![mapping(
            0,
            "total",
            DataType::Int64,
            MssqlType::BigInt,
            true,
        )];
        let plan = plan(&mappings);
        let array = Int64Array::from(vec![Some(7), None]);
        let row_count = array.len();
        let column_count = 1;
        let mut cell_lengths = vec![0; row_count * column_count];
        measure_primitive_column_cell_lengths(
            &array,
            &plan.columns()[0],
            0,
            column_count,
            &mut cell_lengths,
        )
        .unwrap();
        let layout = build_fixed_width_row_layout(row_count, column_count, &cell_lengths).unwrap();
        let mut bytes = allocate_rows_payload_with_tokens(&layout);

        fill_int64_column(
            &array,
            &plan.columns()[0],
            0,
            column_count,
            &layout,
            &mut bytes,
        )
        .unwrap();

        assert_eq!(
            bytes,
            [TDS_ROW_TOKEN, 8, 7, 0, 0, 0, 0, 0, 0, 0, TDS_ROW_TOKEN, 0,]
        );
    }

    #[test]
    fn fills_float32_column_as_little_endian_real_values() {
        let mappings = vec![mapping(
            0,
            "real_value",
            DataType::Float32,
            MssqlType::Real,
            false,
        )];
        let plan = plan(&mappings);
        let array = Float32Array::from(vec![0.0, -0.0, 1.25, -2.5]);
        let row_count = array.len();
        let column_count = 1;
        let mut cell_lengths = vec![0; row_count * column_count];
        measure_primitive_column_cell_lengths(
            &array,
            &plan.columns()[0],
            0,
            column_count,
            &mut cell_lengths,
        )
        .unwrap();
        let layout = build_fixed_width_row_layout(row_count, column_count, &cell_lengths).unwrap();
        let mut bytes = allocate_rows_payload_with_tokens(&layout);

        fill_float32_column(
            &array,
            &plan.columns()[0],
            0,
            column_count,
            &layout,
            &mut bytes,
        )
        .unwrap();

        assert_eq!(
            bytes,
            [
                TDS_ROW_TOKEN,
                0x00,
                0x00,
                0x00,
                0x00,
                TDS_ROW_TOKEN,
                0x00,
                0x00,
                0x00,
                0x80,
                TDS_ROW_TOKEN,
                0x00,
                0x00,
                0xA0,
                0x3F,
                TDS_ROW_TOKEN,
                0x00,
                0x00,
                0x20,
                0xC0,
            ]
        );
    }

    #[test]
    fn fills_nullable_float32_column_with_zero_length_null_cell() {
        let mappings = vec![mapping(
            0,
            "real_value",
            DataType::Float32,
            MssqlType::Real,
            true,
        )];
        let plan = plan(&mappings);
        let array = Float32Array::from(vec![Some(7.0), None]);
        let row_count = array.len();
        let column_count = 1;
        let mut cell_lengths = vec![0; row_count * column_count];
        measure_primitive_column_cell_lengths(
            &array,
            &plan.columns()[0],
            0,
            column_count,
            &mut cell_lengths,
        )
        .unwrap();
        let layout = build_fixed_width_row_layout(row_count, column_count, &cell_lengths).unwrap();
        let mut bytes = allocate_rows_payload_with_tokens(&layout);

        fill_float32_column(
            &array,
            &plan.columns()[0],
            0,
            column_count,
            &layout,
            &mut bytes,
        )
        .unwrap();

        assert_eq!(
            bytes,
            [TDS_ROW_TOKEN, 4, 0, 0, 0xE0, 0x40, TDS_ROW_TOKEN, 0]
        );
    }

    #[test]
    fn fills_float64_column_as_little_endian_float_values() {
        let mappings = vec![mapping(
            0,
            "ratio",
            DataType::Float64,
            MssqlType::Float { precision: 53 },
            false,
        )];
        let plan = plan(&mappings);
        let array = Float64Array::from(vec![0.0, -0.0, 1.25, -2.5]);
        let row_count = array.len();
        let column_count = 1;
        let mut cell_lengths = vec![0; row_count * column_count];
        measure_primitive_column_cell_lengths(
            &array,
            &plan.columns()[0],
            0,
            column_count,
            &mut cell_lengths,
        )
        .unwrap();
        let layout = build_fixed_width_row_layout(row_count, column_count, &cell_lengths).unwrap();
        let mut bytes = allocate_rows_payload_with_tokens(&layout);

        fill_float64_column(
            &array,
            &plan.columns()[0],
            0,
            column_count,
            &layout,
            &mut bytes,
        )
        .unwrap();

        assert_eq!(
            bytes,
            [
                TDS_ROW_TOKEN,
                0x00,
                0x00,
                0x00,
                0x00,
                0x00,
                0x00,
                0x00,
                0x00,
                TDS_ROW_TOKEN,
                0x00,
                0x00,
                0x00,
                0x00,
                0x00,
                0x00,
                0x00,
                0x80,
                TDS_ROW_TOKEN,
                0x00,
                0x00,
                0x00,
                0x00,
                0x00,
                0x00,
                0xF4,
                0x3F,
                TDS_ROW_TOKEN,
                0x00,
                0x00,
                0x00,
                0x00,
                0x00,
                0x00,
                0x04,
                0xC0,
            ]
        );
    }

    #[test]
    fn fills_nullable_float64_column_with_zero_length_null_cell() {
        let mappings = vec![mapping(
            0,
            "ratio",
            DataType::Float64,
            MssqlType::Float { precision: 53 },
            true,
        )];
        let plan = plan(&mappings);
        let array = Float64Array::from(vec![Some(7.0), None]);
        let row_count = array.len();
        let column_count = 1;
        let mut cell_lengths = vec![0; row_count * column_count];
        measure_primitive_column_cell_lengths(
            &array,
            &plan.columns()[0],
            0,
            column_count,
            &mut cell_lengths,
        )
        .unwrap();
        let layout = build_fixed_width_row_layout(row_count, column_count, &cell_lengths).unwrap();
        let mut bytes = allocate_rows_payload_with_tokens(&layout);

        fill_float64_column(
            &array,
            &plan.columns()[0],
            0,
            column_count,
            &layout,
            &mut bytes,
        )
        .unwrap();

        assert_eq!(
            bytes,
            [
                TDS_ROW_TOKEN,
                8,
                0,
                0,
                0,
                0,
                0,
                0,
                0x1C,
                0x40,
                TDS_ROW_TOKEN,
                0,
            ]
        );
    }

    #[test]
    fn rejects_non_finite_float32_values_before_finishing_payload() {
        let mappings = vec![mapping(
            0,
            "real_value",
            DataType::Float32,
            MssqlType::Real,
            false,
        )];
        let plan = plan(&mappings);
        let cases = [f32::NAN, f32::INFINITY, f32::NEG_INFINITY];

        for value in cases {
            let array = Float32Array::from(vec![1.0, value]);
            let row_count = array.len();
            let column_count = 1;
            let mut cell_lengths = vec![0; row_count * column_count];

            let err = measure_primitive_column_cell_lengths(
                &array,
                &plan.columns()[0],
                0,
                column_count,
                &mut cell_lengths,
            )
            .expect_err("non-finite float must fail before layout is accepted");

            assert_value_conversion_diagnostic(
                err,
                DiagnosticCode::NonFiniteFloat,
                Some(1),
                Some((0, "real_value")),
            );
        }
    }

    #[test]
    fn rejects_non_finite_float64_values_before_finishing_payload() {
        let mappings = vec![mapping(
            0,
            "ratio",
            DataType::Float64,
            MssqlType::Float { precision: 53 },
            false,
        )];
        let plan = plan(&mappings);
        let cases = [f64::NAN, f64::INFINITY, f64::NEG_INFINITY];

        for value in cases {
            let array = Float64Array::from(vec![1.0, value]);
            let row_count = array.len();
            let column_count = 1;
            let mut cell_lengths = vec![0; row_count * column_count];

            let err = measure_primitive_column_cell_lengths(
                &array,
                &plan.columns()[0],
                0,
                column_count,
                &mut cell_lengths,
            )
            .expect_err("non-finite float must fail before layout is accepted");

            assert_value_conversion_diagnostic(
                err,
                DiagnosticCode::NonFiniteFloat,
                Some(1),
                Some((0, "ratio")),
            );
        }
    }

    #[test]
    fn rejects_null_in_non_nullable_direct_primitive_column_before_layout_finishes() {
        let mappings = vec![mapping(
            0,
            "quantity",
            DataType::Int32,
            MssqlType::Int,
            false,
        )];
        let plan = plan(&mappings);
        let array = Int32Array::from(vec![Some(1), None]);
        let mut cell_lengths = vec![0; 2];

        let err = measure_primitive_column_cell_lengths(
            &array,
            &plan.columns()[0],
            0,
            1,
            &mut cell_lengths,
        )
        .expect_err("null in non-nullable column must fail");

        assert_value_conversion_diagnostic(
            err,
            DiagnosticCode::NullInNonNullableColumn,
            Some(1),
            Some((0, "quantity")),
        );
    }

    fn plan(mappings: &[SchemaMapping]) -> DirectEncoderPlan {
        DirectEncoderPlan::new(mappings, &CurrentDirectMappings).unwrap()
    }

    fn mapping(
        index: usize,
        name: &str,
        arrow_type: DataType,
        mssql_type: MssqlType,
        nullable: bool,
    ) -> SchemaMapping {
        SchemaMapping::new(
            ArrowFieldRef::new(index, name.to_owned(), nullable, arrow_type),
            MssqlColumn::new(Identifier::new(name).unwrap(), mssql_type, nullable),
        )
    }

    fn assert_cell_positions(
        cells: &[crate::write::direct::layout::CellPosition],
        expected: &[(usize, usize, usize, usize)],
    ) {
        assert_eq!(cells.len(), expected.len());
        for (cell, &(row_index, column_index, offset, len)) in cells.iter().zip(expected) {
            assert_eq!(cell.row_index(), row_index);
            assert_eq!(cell.column_index(), column_index);
            assert_eq!(cell.offset(), offset);
            assert_eq!(cell.len(), len);
        }
    }

    fn assert_value_conversion_diagnostic(
        err: Error,
        expected_code: DiagnosticCode,
        expected_row: Option<usize>,
        expected_field: Option<(usize, &str)>,
    ) {
        let Error::ValueConversion { diagnostics } = err else {
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

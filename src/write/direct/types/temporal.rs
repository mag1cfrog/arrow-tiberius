//! Shared SQL Server temporal direct TDS row payload helpers.

pub(crate) mod value;

use arrow_array::{
    Array, Date32Array, Date64Array, Time32MillisecondArray, Time32SecondArray,
    Time64MicrosecondArray, Time64NanosecondArray, TimestampMicrosecondArray,
    TimestampMillisecondArray, TimestampNanosecondArray, TimestampSecondArray,
};

use crate::{
    Diagnostic, DiagnosticCode, DiagnosticSet, Error, FieldRef, MssqlType, NanosecondPolicy,
    Result, SchemaMapping,
    mssql::cell::{
        MssqlDate, MssqlDateTime, MssqlDateTime2, MssqlDateTimeOffset, MssqlTime,
        from_arrow::temporal::datetime::{
            mssql_datetime_from_arrow_timestamp_microsecond,
            mssql_datetime_from_arrow_timestamp_millisecond,
            mssql_datetime_from_arrow_timestamp_nanosecond,
            mssql_datetime_from_arrow_timestamp_second,
        },
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
    mssql::profile::DateTimeRounding,
    write::{context::RuntimeConversionContext, profile},
};

use super::super::{
    layout::{CellPosition, RowLayout},
    plan::DirectColumnPlan,
};

pub(crate) use value::{
    NULL_TEMPORAL_CELL_LEN, append_date_cell, append_datetime_cell, append_datetime2_cell,
    append_datetimeoffset_cell, append_null_temporal_cell, append_time_cell, date_cell_len,
    datetime_cell_len, datetime_payload_len, datetime2_cell_len, datetimeoffset_cell_len,
    mssql_date_from_arrow_date32, mssql_datetime2_from_arrow_date64, time_cell_len,
    write_date_cell, write_datetime_cell, write_datetime_payload, write_datetime2_cell,
    write_datetimeoffset_cell, write_null_temporal_cell, write_time_cell,
};

/// Shared context for direct temporal column measurement and encoding.
#[derive(Clone, Copy)]
pub(crate) struct TemporalColumnContext<'a> {
    /// Planned Arrow-to-MSSQL schema mapping for diagnostics and validation.
    pub(crate) mapping: &'a SchemaMapping,
    /// Runtime conversion behavior derived from the planned schema.
    pub(crate) runtime_context: RuntimeConversionContext,
    /// Direct column plan used to choose payload encoding and null handling.
    pub(crate) column: &'a DirectColumnPlan,
    /// Zero-based column position in the encoded row.
    pub(crate) column_index: usize,
    /// Number of columns in each encoded row.
    pub(crate) column_count: usize,
}

pub(crate) fn measure_date32_column_cell_lengths(
    array: &Date32Array,
    context: TemporalColumnContext<'_>,
    cell_lengths: &mut [usize],
) -> Result<()> {
    measure_temporal_values(
        array,
        context.mapping,
        context.column,
        context.column_index,
        context.column_count,
        cell_lengths,
        |array, _mapping, row_index| {
            mssql_date_from_arrow_date32(context.column, row_index, array.value(row_index))
                .map(|_| date_cell_len())
        },
    )
}

pub(crate) fn measure_date64_column_cell_lengths(
    array: &Date64Array,
    context: TemporalColumnContext<'_>,
    cell_lengths: &mut [usize],
) -> Result<()> {
    measure_temporal_values(
        array,
        context.mapping,
        context.column,
        context.column_index,
        context.column_count,
        cell_lengths,
        |array, _mapping, row_index| {
            mssql_datetime2_from_arrow_date64(context.column, row_index, array.value(row_index))
                .and_then(datetime2_cell_len_for_value)
        },
    )
}

pub(crate) fn measure_timestamp_second_column_cell_lengths(
    array: &TimestampSecondArray,
    context: TemporalColumnContext<'_>,
    cell_lengths: &mut [usize],
) -> Result<()> {
    measure_temporal_values(
        array,
        context.mapping,
        context.column,
        context.column_index,
        context.column_count,
        cell_lengths,
        |array, mapping, row_index| {
            timestamp_second_value(
                array,
                mapping,
                context.column,
                row_index,
                context.runtime_context.datetime_rounding(),
            )
            .and_then(|value| temporal_value_cell_len(context.column, value))
        },
    )
}

pub(crate) fn measure_timestamp_millisecond_column_cell_lengths(
    array: &TimestampMillisecondArray,
    context: TemporalColumnContext<'_>,
    cell_lengths: &mut [usize],
) -> Result<()> {
    measure_temporal_values(
        array,
        context.mapping,
        context.column,
        context.column_index,
        context.column_count,
        cell_lengths,
        |array, mapping, row_index| {
            timestamp_millisecond_value(
                array,
                mapping,
                context.column,
                row_index,
                context.runtime_context.datetime_rounding(),
            )
            .and_then(|value| temporal_value_cell_len(context.column, value))
        },
    )
}

pub(crate) fn measure_timestamp_microsecond_column_cell_lengths(
    array: &TimestampMicrosecondArray,
    context: TemporalColumnContext<'_>,
    cell_lengths: &mut [usize],
) -> Result<()> {
    measure_temporal_values(
        array,
        context.mapping,
        context.column,
        context.column_index,
        context.column_count,
        cell_lengths,
        |array, mapping, row_index| {
            timestamp_microsecond_value(
                array,
                mapping,
                context.column,
                row_index,
                context.runtime_context.datetime_rounding(),
            )
            .and_then(|value| temporal_value_cell_len(context.column, value))
        },
    )
}

pub(crate) fn measure_timestamp_nanosecond_column_cell_lengths(
    array: &TimestampNanosecondArray,
    context: TemporalColumnContext<'_>,
    cell_lengths: &mut [usize],
) -> Result<()> {
    measure_temporal_values(
        array,
        context.mapping,
        context.column,
        context.column_index,
        context.column_count,
        cell_lengths,
        |array, mapping, row_index| {
            timestamp_nanosecond_value(
                array,
                mapping,
                context.column,
                row_index,
                context.runtime_context.nanosecond_policy(),
                context.runtime_context.datetime_rounding(),
            )
            .and_then(|value| temporal_value_cell_len(context.column, value))
        },
    )
}

pub(crate) fn measure_datetimeoffset_second_column_cell_lengths(
    array: &TimestampSecondArray,
    context: TemporalColumnContext<'_>,
    cell_lengths: &mut [usize],
) -> Result<()> {
    measure_temporal_values(
        array,
        context.mapping,
        context.column,
        context.column_index,
        context.column_count,
        cell_lengths,
        |array, mapping, row_index| {
            let timezone = timestamp_timezone_for_datetimeoffset(mapping, row_index)?;
            mssql_datetimeoffset_from_arrow_timestamp_second(
                mapping,
                row_index,
                array.value(row_index),
                timezone,
            )
            .and_then(datetimeoffset_cell_len_for_value)
        },
    )
}

pub(crate) fn measure_datetimeoffset_millisecond_column_cell_lengths(
    array: &TimestampMillisecondArray,
    context: TemporalColumnContext<'_>,
    cell_lengths: &mut [usize],
) -> Result<()> {
    measure_temporal_values(
        array,
        context.mapping,
        context.column,
        context.column_index,
        context.column_count,
        cell_lengths,
        |array, mapping, row_index| {
            let timezone = timestamp_timezone_for_datetimeoffset(mapping, row_index)?;
            mssql_datetimeoffset_from_arrow_timestamp_millisecond(
                mapping,
                row_index,
                array.value(row_index),
                timezone,
            )
            .and_then(datetimeoffset_cell_len_for_value)
        },
    )
}

pub(crate) fn measure_datetimeoffset_microsecond_column_cell_lengths(
    array: &TimestampMicrosecondArray,
    context: TemporalColumnContext<'_>,
    cell_lengths: &mut [usize],
) -> Result<()> {
    measure_temporal_values(
        array,
        context.mapping,
        context.column,
        context.column_index,
        context.column_count,
        cell_lengths,
        |array, mapping, row_index| {
            let timezone = timestamp_timezone_for_datetimeoffset(mapping, row_index)?;
            mssql_datetimeoffset_from_arrow_timestamp_microsecond(
                mapping,
                row_index,
                array.value(row_index),
                timezone,
            )
            .and_then(datetimeoffset_cell_len_for_value)
        },
    )
}

pub(crate) fn measure_datetimeoffset_nanosecond_column_cell_lengths(
    array: &TimestampNanosecondArray,
    context: TemporalColumnContext<'_>,
    cell_lengths: &mut [usize],
) -> Result<()> {
    measure_temporal_values(
        array,
        context.mapping,
        context.column,
        context.column_index,
        context.column_count,
        cell_lengths,
        |array, mapping, row_index| {
            let timezone = timestamp_timezone_for_datetimeoffset(mapping, row_index)?;
            mssql_datetimeoffset_from_arrow_timestamp_nanosecond(
                mapping,
                row_index,
                array.value(row_index),
                timezone,
                context.runtime_context.nanosecond_policy(),
            )
            .and_then(datetimeoffset_cell_len_for_value)
        },
    )
}

pub(crate) fn measure_time32_second_column_cell_lengths(
    array: &Time32SecondArray,
    context: TemporalColumnContext<'_>,
    cell_lengths: &mut [usize],
) -> Result<()> {
    measure_temporal_values(
        array,
        context.mapping,
        context.column,
        context.column_index,
        context.column_count,
        cell_lengths,
        |array, mapping, row_index| {
            mssql_time_from_arrow_time32_second(mapping, row_index, array.value(row_index))
                .and_then(time_cell_len_for_value)
        },
    )
}

pub(crate) fn measure_time32_millisecond_column_cell_lengths(
    array: &Time32MillisecondArray,
    context: TemporalColumnContext<'_>,
    cell_lengths: &mut [usize],
) -> Result<()> {
    measure_temporal_values(
        array,
        context.mapping,
        context.column,
        context.column_index,
        context.column_count,
        cell_lengths,
        |array, mapping, row_index| {
            mssql_time_from_arrow_time32_millisecond(mapping, row_index, array.value(row_index))
                .and_then(time_cell_len_for_value)
        },
    )
}

pub(crate) fn measure_time64_microsecond_column_cell_lengths(
    array: &Time64MicrosecondArray,
    context: TemporalColumnContext<'_>,
    cell_lengths: &mut [usize],
) -> Result<()> {
    measure_temporal_values(
        array,
        context.mapping,
        context.column,
        context.column_index,
        context.column_count,
        cell_lengths,
        |array, mapping, row_index| {
            mssql_time_from_arrow_time64_microsecond(mapping, row_index, array.value(row_index))
                .and_then(time_cell_len_for_value)
        },
    )
}

pub(crate) fn measure_time64_nanosecond_column_cell_lengths(
    array: &Time64NanosecondArray,
    context: TemporalColumnContext<'_>,
    cell_lengths: &mut [usize],
) -> Result<()> {
    measure_temporal_values(
        array,
        context.mapping,
        context.column,
        context.column_index,
        context.column_count,
        cell_lengths,
        |array, mapping, row_index| {
            mssql_time_from_arrow_time64_nanosecond(
                mapping,
                row_index,
                array.value(row_index),
                context.runtime_context.nanosecond_policy(),
            )
            .and_then(time_cell_len_for_value)
        },
    )
}

pub(crate) fn fill_date32_direct_column(
    array: &Date32Array,
    context: TemporalColumnContext<'_>,
    layout: &RowLayout,
    bytes: &mut [u8],
) -> Result<()> {
    fill_date32_column(
        array,
        context.column,
        context.column_index,
        context.column_count,
        layout,
        bytes,
    )
}

pub(crate) fn fill_date64_direct_column(
    array: &Date64Array,
    context: TemporalColumnContext<'_>,
    layout: &RowLayout,
    bytes: &mut [u8],
) -> Result<()> {
    fill_date64_column(
        array,
        context.column,
        context.column_index,
        context.column_count,
        layout,
        bytes,
    )
}

pub(crate) fn fill_timestamp_second_direct_column(
    array: &TimestampSecondArray,
    context: TemporalColumnContext<'_>,
    layout: &RowLayout,
    bytes: &mut [u8],
) -> Result<()> {
    fill_timestamp_column(array, context, layout, bytes, timestamp_second_value)
}

pub(crate) fn fill_timestamp_millisecond_direct_column(
    array: &TimestampMillisecondArray,
    context: TemporalColumnContext<'_>,
    layout: &RowLayout,
    bytes: &mut [u8],
) -> Result<()> {
    fill_timestamp_column(array, context, layout, bytes, timestamp_millisecond_value)
}

pub(crate) fn fill_timestamp_microsecond_direct_column(
    array: &TimestampMicrosecondArray,
    context: TemporalColumnContext<'_>,
    layout: &RowLayout,
    bytes: &mut [u8],
) -> Result<()> {
    fill_timestamp_column(array, context, layout, bytes, timestamp_microsecond_value)
}

pub(crate) fn fill_timestamp_nanosecond_direct_column(
    array: &TimestampNanosecondArray,
    context: TemporalColumnContext<'_>,
    layout: &RowLayout,
    bytes: &mut [u8],
) -> Result<()> {
    let nanosecond_policy = context.runtime_context.nanosecond_policy();
    fill_timestamp_column(
        array,
        context,
        layout,
        bytes,
        |array, mapping, column, row_index, rounding| {
            timestamp_nanosecond_value(
                array,
                mapping,
                column,
                row_index,
                nanosecond_policy,
                rounding,
            )
        },
    )
}

pub(crate) fn fill_datetimeoffset_second_direct_column(
    array: &TimestampSecondArray,
    context: TemporalColumnContext<'_>,
    layout: &RowLayout,
    bytes: &mut [u8],
) -> Result<()> {
    fill_datetimeoffset_column(
        array,
        context,
        layout,
        bytes,
        |array, mapping, row_index| {
            let timezone = timestamp_timezone_for_datetimeoffset(mapping, row_index)?;
            mssql_datetimeoffset_from_arrow_timestamp_second(
                mapping,
                row_index,
                array.value(row_index),
                timezone,
            )
        },
    )
}

pub(crate) fn fill_datetimeoffset_millisecond_direct_column(
    array: &TimestampMillisecondArray,
    context: TemporalColumnContext<'_>,
    layout: &RowLayout,
    bytes: &mut [u8],
) -> Result<()> {
    fill_datetimeoffset_column(
        array,
        context,
        layout,
        bytes,
        |array, mapping, row_index| {
            let timezone = timestamp_timezone_for_datetimeoffset(mapping, row_index)?;
            mssql_datetimeoffset_from_arrow_timestamp_millisecond(
                mapping,
                row_index,
                array.value(row_index),
                timezone,
            )
        },
    )
}

pub(crate) fn fill_datetimeoffset_microsecond_direct_column(
    array: &TimestampMicrosecondArray,
    context: TemporalColumnContext<'_>,
    layout: &RowLayout,
    bytes: &mut [u8],
) -> Result<()> {
    fill_datetimeoffset_column(
        array,
        context,
        layout,
        bytes,
        |array, mapping, row_index| {
            let timezone = timestamp_timezone_for_datetimeoffset(mapping, row_index)?;
            mssql_datetimeoffset_from_arrow_timestamp_microsecond(
                mapping,
                row_index,
                array.value(row_index),
                timezone,
            )
        },
    )
}

pub(crate) fn fill_datetimeoffset_nanosecond_direct_column(
    array: &TimestampNanosecondArray,
    context: TemporalColumnContext<'_>,
    layout: &RowLayout,
    bytes: &mut [u8],
) -> Result<()> {
    fill_datetimeoffset_column(
        array,
        context,
        layout,
        bytes,
        |array, mapping, row_index| {
            let timezone = timestamp_timezone_for_datetimeoffset(mapping, row_index)?;
            mssql_datetimeoffset_from_arrow_timestamp_nanosecond(
                mapping,
                row_index,
                array.value(row_index),
                timezone,
                context.runtime_context.nanosecond_policy(),
            )
        },
    )
}

pub(crate) fn fill_time32_second_direct_column(
    array: &Time32SecondArray,
    context: TemporalColumnContext<'_>,
    layout: &RowLayout,
    bytes: &mut [u8],
) -> Result<()> {
    fill_time_column(
        array,
        context,
        layout,
        bytes,
        |array, mapping, row_index| {
            mssql_time_from_arrow_time32_second(mapping, row_index, array.value(row_index))
        },
    )
}

pub(crate) fn fill_time32_millisecond_direct_column(
    array: &Time32MillisecondArray,
    context: TemporalColumnContext<'_>,
    layout: &RowLayout,
    bytes: &mut [u8],
) -> Result<()> {
    fill_time_column(
        array,
        context,
        layout,
        bytes,
        |array, mapping, row_index| {
            mssql_time_from_arrow_time32_millisecond(mapping, row_index, array.value(row_index))
        },
    )
}

pub(crate) fn fill_time64_microsecond_direct_column(
    array: &Time64MicrosecondArray,
    context: TemporalColumnContext<'_>,
    layout: &RowLayout,
    bytes: &mut [u8],
) -> Result<()> {
    fill_time_column(
        array,
        context,
        layout,
        bytes,
        |array, mapping, row_index| {
            mssql_time_from_arrow_time64_microsecond(mapping, row_index, array.value(row_index))
        },
    )
}

pub(crate) fn fill_time64_nanosecond_direct_column(
    array: &Time64NanosecondArray,
    context: TemporalColumnContext<'_>,
    layout: &RowLayout,
    bytes: &mut [u8],
) -> Result<()> {
    fill_time_column(
        array,
        context,
        layout,
        bytes,
        |array, mapping, row_index| {
            mssql_time_from_arrow_time64_nanosecond(
                mapping,
                row_index,
                array.value(row_index),
                context.runtime_context.nanosecond_policy(),
            )
        },
    )
}

/// Appends one Arrow Date32 cell to a raw bulk append buffer.
pub(crate) fn append_date32_cell(
    buf: &mut tiberius::RawRowsAppendBuffer<'_>,
    array: &Date32Array,
    mapping: &SchemaMapping,
    column: &DirectColumnPlan,
    row_index: usize,
    measured_len: usize,
) -> Result<()> {
    append_temporal_value(
        buf,
        array,
        mapping,
        column,
        row_index,
        measured_len,
        |array, _mapping, row_index| {
            let value = mssql_date_from_arrow_date32(column, row_index, array.value(row_index))?;
            Ok((date_cell_len(), TemporalValue::Date(value)))
        },
    )
}

/// Appends one Arrow Date64 cell to a raw bulk append buffer.
pub(crate) fn append_date64_cell(
    buf: &mut tiberius::RawRowsAppendBuffer<'_>,
    array: &Date64Array,
    _mapping: &SchemaMapping,
    column: &DirectColumnPlan,
    row_index: usize,
    measured_len: usize,
) -> Result<()> {
    append_temporal_value(
        buf,
        array,
        _mapping,
        column,
        row_index,
        measured_len,
        |array, _mapping, row_index| {
            let value =
                mssql_datetime2_from_arrow_date64(column, row_index, array.value(row_index))?;
            Ok((
                datetime2_cell_len_for_value(value)?,
                TemporalValue::DateTime2(value),
            ))
        },
    )
}

/// Appends one Arrow second timestamp cell to a direct raw-row buffer.
pub(crate) fn append_timestamp_second_cell(
    buf: &mut tiberius::RawRowsAppendBuffer<'_>,
    array: &TimestampSecondArray,
    mapping: &SchemaMapping,
    column: &DirectColumnPlan,
    datetime_rounding: DateTimeRounding,
    row_index: usize,
    measured_len: usize,
) -> Result<()> {
    append_temporal_value(
        buf,
        array,
        mapping,
        column,
        row_index,
        measured_len,
        |array, mapping, row_index| {
            let value =
                timestamp_second_value(array, mapping, column, row_index, datetime_rounding)?;
            Ok((temporal_value_cell_len(column, value)?, value))
        },
    )
}

/// Appends one Arrow millisecond timestamp cell to a direct raw-row buffer.
pub(crate) fn append_timestamp_millisecond_cell(
    buf: &mut tiberius::RawRowsAppendBuffer<'_>,
    array: &TimestampMillisecondArray,
    mapping: &SchemaMapping,
    column: &DirectColumnPlan,
    datetime_rounding: DateTimeRounding,
    row_index: usize,
    measured_len: usize,
) -> Result<()> {
    append_temporal_value(
        buf,
        array,
        mapping,
        column,
        row_index,
        measured_len,
        |array, mapping, row_index| {
            let value =
                timestamp_millisecond_value(array, mapping, column, row_index, datetime_rounding)?;
            Ok((temporal_value_cell_len(column, value)?, value))
        },
    )
}

/// Appends one Arrow microsecond timestamp cell to a direct raw-row buffer.
pub(crate) fn append_timestamp_microsecond_cell(
    buf: &mut tiberius::RawRowsAppendBuffer<'_>,
    array: &TimestampMicrosecondArray,
    mapping: &SchemaMapping,
    column: &DirectColumnPlan,
    datetime_rounding: DateTimeRounding,
    row_index: usize,
    measured_len: usize,
) -> Result<()> {
    append_temporal_value(
        buf,
        array,
        mapping,
        column,
        row_index,
        measured_len,
        |array, mapping, row_index| {
            let value =
                timestamp_microsecond_value(array, mapping, column, row_index, datetime_rounding)?;
            Ok((temporal_value_cell_len(column, value)?, value))
        },
    )
}

/// Appends one Arrow nanosecond timestamp cell to a direct raw-row buffer.
///
/// Nanosecond normalization and SQL Server `datetime` rounding are both taken
/// from the runtime context by the caller.
pub(crate) fn append_timestamp_nanosecond_cell(
    buf: &mut tiberius::RawRowsAppendBuffer<'_>,
    array: &TimestampNanosecondArray,
    mapping: &SchemaMapping,
    column: &DirectColumnPlan,
    nanosecond_policy: NanosecondPolicy,
    datetime_rounding: DateTimeRounding,
    row_index: usize,
    measured_len: usize,
) -> Result<()> {
    append_temporal_value(
        buf,
        array,
        mapping,
        column,
        row_index,
        measured_len,
        |array, mapping, row_index| {
            let value = timestamp_nanosecond_value(
                array,
                mapping,
                column,
                row_index,
                nanosecond_policy,
                datetime_rounding,
            )?;
            Ok((temporal_value_cell_len(column, value)?, value))
        },
    )
}

pub(crate) fn append_datetimeoffset_second_cell(
    buf: &mut tiberius::RawRowsAppendBuffer<'_>,
    array: &TimestampSecondArray,
    mapping: &SchemaMapping,
    column: &DirectColumnPlan,
    row_index: usize,
    measured_len: usize,
) -> Result<()> {
    append_temporal_value(
        buf,
        array,
        mapping,
        column,
        row_index,
        measured_len,
        |array, mapping, row_index| {
            let timezone = timestamp_timezone_for_datetimeoffset(mapping, row_index)?;
            let value = mssql_datetimeoffset_from_arrow_timestamp_second(
                mapping,
                row_index,
                array.value(row_index),
                timezone,
            )?;
            Ok((
                datetimeoffset_cell_len_for_value(value)?,
                TemporalValue::DateTimeOffset(value),
            ))
        },
    )
}

pub(crate) fn append_datetimeoffset_millisecond_cell(
    buf: &mut tiberius::RawRowsAppendBuffer<'_>,
    array: &TimestampMillisecondArray,
    mapping: &SchemaMapping,
    column: &DirectColumnPlan,
    row_index: usize,
    measured_len: usize,
) -> Result<()> {
    append_temporal_value(
        buf,
        array,
        mapping,
        column,
        row_index,
        measured_len,
        |array, mapping, row_index| {
            let timezone = timestamp_timezone_for_datetimeoffset(mapping, row_index)?;
            let value = mssql_datetimeoffset_from_arrow_timestamp_millisecond(
                mapping,
                row_index,
                array.value(row_index),
                timezone,
            )?;
            Ok((
                datetimeoffset_cell_len_for_value(value)?,
                TemporalValue::DateTimeOffset(value),
            ))
        },
    )
}

pub(crate) fn append_datetimeoffset_microsecond_cell(
    buf: &mut tiberius::RawRowsAppendBuffer<'_>,
    array: &TimestampMicrosecondArray,
    mapping: &SchemaMapping,
    column: &DirectColumnPlan,
    row_index: usize,
    measured_len: usize,
) -> Result<()> {
    append_temporal_value(
        buf,
        array,
        mapping,
        column,
        row_index,
        measured_len,
        |array, mapping, row_index| {
            let timezone = timestamp_timezone_for_datetimeoffset(mapping, row_index)?;
            let value = mssql_datetimeoffset_from_arrow_timestamp_microsecond(
                mapping,
                row_index,
                array.value(row_index),
                timezone,
            )?;
            Ok((
                datetimeoffset_cell_len_for_value(value)?,
                TemporalValue::DateTimeOffset(value),
            ))
        },
    )
}

pub(crate) fn append_datetimeoffset_nanosecond_cell(
    buf: &mut tiberius::RawRowsAppendBuffer<'_>,
    array: &TimestampNanosecondArray,
    mapping: &SchemaMapping,
    column: &DirectColumnPlan,
    nanosecond_policy: NanosecondPolicy,
    row_index: usize,
    measured_len: usize,
) -> Result<()> {
    append_temporal_value(
        buf,
        array,
        mapping,
        column,
        row_index,
        measured_len,
        |array, mapping, row_index| {
            let timezone = timestamp_timezone_for_datetimeoffset(mapping, row_index)?;
            let value = mssql_datetimeoffset_from_arrow_timestamp_nanosecond(
                mapping,
                row_index,
                array.value(row_index),
                timezone,
                nanosecond_policy,
            )?;
            Ok((
                datetimeoffset_cell_len_for_value(value)?,
                TemporalValue::DateTimeOffset(value),
            ))
        },
    )
}

pub(crate) fn append_time32_second_cell(
    buf: &mut tiberius::RawRowsAppendBuffer<'_>,
    array: &Time32SecondArray,
    mapping: &SchemaMapping,
    column: &DirectColumnPlan,
    row_index: usize,
    measured_len: usize,
) -> Result<()> {
    append_temporal_value(
        buf,
        array,
        mapping,
        column,
        row_index,
        measured_len,
        |array, mapping, row_index| {
            let value =
                mssql_time_from_arrow_time32_second(mapping, row_index, array.value(row_index))?;
            Ok((time_cell_len_for_value(value)?, TemporalValue::Time(value)))
        },
    )
}

pub(crate) fn append_time32_millisecond_cell(
    buf: &mut tiberius::RawRowsAppendBuffer<'_>,
    array: &Time32MillisecondArray,
    mapping: &SchemaMapping,
    column: &DirectColumnPlan,
    row_index: usize,
    measured_len: usize,
) -> Result<()> {
    append_temporal_value(
        buf,
        array,
        mapping,
        column,
        row_index,
        measured_len,
        |array, mapping, row_index| {
            let value = mssql_time_from_arrow_time32_millisecond(
                mapping,
                row_index,
                array.value(row_index),
            )?;
            Ok((time_cell_len_for_value(value)?, TemporalValue::Time(value)))
        },
    )
}

pub(crate) fn append_time64_microsecond_cell(
    buf: &mut tiberius::RawRowsAppendBuffer<'_>,
    array: &Time64MicrosecondArray,
    mapping: &SchemaMapping,
    column: &DirectColumnPlan,
    row_index: usize,
    measured_len: usize,
) -> Result<()> {
    append_temporal_value(
        buf,
        array,
        mapping,
        column,
        row_index,
        measured_len,
        |array, mapping, row_index| {
            let value = mssql_time_from_arrow_time64_microsecond(
                mapping,
                row_index,
                array.value(row_index),
            )?;
            Ok((time_cell_len_for_value(value)?, TemporalValue::Time(value)))
        },
    )
}

pub(crate) fn append_time64_nanosecond_cell(
    buf: &mut tiberius::RawRowsAppendBuffer<'_>,
    array: &Time64NanosecondArray,
    mapping: &SchemaMapping,
    column: &DirectColumnPlan,
    nanosecond_policy: NanosecondPolicy,
    row_index: usize,
    measured_len: usize,
) -> Result<()> {
    append_temporal_value(
        buf,
        array,
        mapping,
        column,
        row_index,
        measured_len,
        |array, mapping, row_index| {
            let value = mssql_time_from_arrow_time64_nanosecond(
                mapping,
                row_index,
                array.value(row_index),
                nanosecond_policy,
            )?;
            Ok((time_cell_len_for_value(value)?, TemporalValue::Time(value)))
        },
    )
}

/// Converts one timestamp-second value to the planned SQL Server temporal value.
fn timestamp_second_value(
    array: &TimestampSecondArray,
    mapping: &SchemaMapping,
    column: &DirectColumnPlan,
    row_index: usize,
    rounding: DateTimeRounding,
) -> Result<TemporalValue> {
    match column.target_type() {
        MssqlType::DateTime => mssql_datetime_from_arrow_timestamp_second(
            mapping,
            row_index,
            array.value(row_index),
            rounding,
        )
        .map(TemporalValue::DateTime),
        MssqlType::DateTime2 { .. } => {
            mssql_datetime2_from_arrow_timestamp_second(mapping, row_index, array.value(row_index))
                .map(TemporalValue::DateTime2)
        }
        _ => Err(unsupported_timestamp_target(column, row_index)),
    }
}

/// Converts one timestamp-millisecond value to the planned SQL Server temporal value.
fn timestamp_millisecond_value(
    array: &TimestampMillisecondArray,
    mapping: &SchemaMapping,
    column: &DirectColumnPlan,
    row_index: usize,
    rounding: DateTimeRounding,
) -> Result<TemporalValue> {
    match column.target_type() {
        MssqlType::DateTime => mssql_datetime_from_arrow_timestamp_millisecond(
            mapping,
            row_index,
            array.value(row_index),
            rounding,
        )
        .map(TemporalValue::DateTime),
        MssqlType::DateTime2 { .. } => mssql_datetime2_from_arrow_timestamp_millisecond(
            mapping,
            row_index,
            array.value(row_index),
        )
        .map(TemporalValue::DateTime2),
        _ => Err(unsupported_timestamp_target(column, row_index)),
    }
}

/// Converts one timestamp-microsecond value to the planned SQL Server temporal value.
fn timestamp_microsecond_value(
    array: &TimestampMicrosecondArray,
    mapping: &SchemaMapping,
    column: &DirectColumnPlan,
    row_index: usize,
    rounding: DateTimeRounding,
) -> Result<TemporalValue> {
    match column.target_type() {
        MssqlType::DateTime => mssql_datetime_from_arrow_timestamp_microsecond(
            mapping,
            row_index,
            array.value(row_index),
            rounding,
        )
        .map(TemporalValue::DateTime),
        MssqlType::DateTime2 { .. } => mssql_datetime2_from_arrow_timestamp_microsecond(
            mapping,
            row_index,
            array.value(row_index),
        )
        .map(TemporalValue::DateTime2),
        _ => Err(unsupported_timestamp_target(column, row_index)),
    }
}

/// Converts one timestamp-nanosecond value to the planned SQL Server temporal value.
///
/// The nanosecond policy controls 100ns normalization before any SQL Server
/// `datetime` rounding behavior is applied.
fn timestamp_nanosecond_value(
    array: &TimestampNanosecondArray,
    mapping: &SchemaMapping,
    column: &DirectColumnPlan,
    row_index: usize,
    policy: NanosecondPolicy,
    rounding: DateTimeRounding,
) -> Result<TemporalValue> {
    match column.target_type() {
        MssqlType::DateTime => mssql_datetime_from_arrow_timestamp_nanosecond(
            mapping,
            row_index,
            array.value(row_index),
            policy,
            rounding,
        )
        .map(TemporalValue::DateTime),
        MssqlType::DateTime2 { .. } => mssql_datetime2_from_arrow_timestamp_nanosecond(
            mapping,
            row_index,
            array.value(row_index),
            policy,
        )
        .map(TemporalValue::DateTime2),
        _ => Err(unsupported_timestamp_target(column, row_index)),
    }
}

/// Returns the byte length of a SQL Server NULL temporal cell.
fn measure_temporal_values<A, F>(
    array: &A,
    mapping: &SchemaMapping,
    column: &DirectColumnPlan,
    column_index: usize,
    column_count: usize,
    cell_lengths: &mut [usize],
    value_len: F,
) -> Result<()>
where
    A: Array,
    F: Fn(&A, &SchemaMapping, usize) -> Result<usize>,
{
    for row_index in 0..array.len() {
        let cell_len = if array.is_null(row_index) {
            validate_null_timestamp_timezone_metadata(mapping, row_index)?;
            null_temporal_cell_len_for_column(column, row_index)?
        } else {
            validate_mapping_timestamp_timezone_metadata(mapping, row_index)?;
            value_len(array, mapping, row_index)?
        };

        cell_lengths[row_index * column_count + column_index] = cell_len;
    }

    Ok(())
}

/// Fills one direct timestamp column into an already allocated row payload.
///
/// The supplied value converter receives the profile-selected `datetime`
/// rounding behavior for every non-null value.
fn fill_timestamp_column<A, F>(
    array: &A,
    context: TemporalColumnContext<'_>,
    layout: &RowLayout,
    bytes: &mut [u8],
    value: F,
) -> Result<()>
where
    A: Array,
    F: Fn(&A, &SchemaMapping, &DirectColumnPlan, usize, DateTimeRounding) -> Result<TemporalValue>,
{
    for row_index in 0..array.len() {
        let cell = cell_position(
            layout,
            row_index,
            context.column_index,
            context.column_count,
        )?;

        if array.is_null(row_index) {
            validate_null_timestamp_timezone_metadata(context.mapping, row_index)?;
            write_null_direct_temporal_cell(bytes, cell, context.column, row_index)?;
        } else {
            validate_mapping_timestamp_timezone_metadata(context.mapping, row_index)?;
            let value = value(
                array,
                context.mapping,
                context.column,
                row_index,
                context.runtime_context.datetime_rounding(),
            )?;
            write_direct_temporal_value_cell(bytes, cell, context.column, value)?;
        }
    }

    Ok(())
}

fn fill_time_column<A, F>(
    array: &A,
    context: TemporalColumnContext<'_>,
    layout: &RowLayout,
    bytes: &mut [u8],
    value: F,
) -> Result<()>
where
    A: Array,
    F: Fn(&A, &SchemaMapping, usize) -> Result<MssqlTime>,
{
    for row_index in 0..array.len() {
        let cell = cell_position(
            layout,
            row_index,
            context.column_index,
            context.column_count,
        )?;

        if array.is_null(row_index) {
            write_null_direct_temporal_cell(bytes, cell, context.column, row_index)?;
        } else {
            let value = value(array, context.mapping, row_index)?;
            write_direct_time_cell(bytes, cell, context.column, value)?;
        }
    }

    Ok(())
}

fn fill_datetimeoffset_column<A, F>(
    array: &A,
    context: TemporalColumnContext<'_>,
    layout: &RowLayout,
    bytes: &mut [u8],
    value: F,
) -> Result<()>
where
    A: Array,
    F: Fn(&A, &SchemaMapping, usize) -> Result<MssqlDateTimeOffset>,
{
    for row_index in 0..array.len() {
        let cell = cell_position(
            layout,
            row_index,
            context.column_index,
            context.column_count,
        )?;

        if array.is_null(row_index) {
            validate_null_timestamp_timezone_metadata(context.mapping, row_index)?;
            write_null_direct_temporal_cell(bytes, cell, context.column, row_index)?;
        } else {
            validate_mapping_timestamp_timezone_metadata(context.mapping, row_index)?;
            let value = value(array, context.mapping, row_index)?;
            write_direct_datetimeoffset_cell(bytes, cell, context.column, value)?;
        }
    }

    Ok(())
}

fn fill_date32_column(
    array: &Date32Array,
    column: &DirectColumnPlan,
    column_index: usize,
    column_count: usize,
    layout: &RowLayout,
    bytes: &mut [u8],
) -> Result<()> {
    for row_index in 0..array.len() {
        let cell = cell_position(layout, row_index, column_index, column_count)?;

        if array.is_null(row_index) {
            write_null_direct_temporal_cell(bytes, cell, column, row_index)?;
        } else {
            let value = mssql_date_from_arrow_date32(column, row_index, array.value(row_index))?;
            write_direct_date_cell(bytes, cell, column, value)?;
        }
    }

    Ok(())
}

fn fill_date64_column(
    array: &Date64Array,
    column: &DirectColumnPlan,
    column_index: usize,
    column_count: usize,
    layout: &RowLayout,
    bytes: &mut [u8],
) -> Result<()> {
    for row_index in 0..array.len() {
        let cell = cell_position(layout, row_index, column_index, column_count)?;

        if array.is_null(row_index) {
            write_null_direct_temporal_cell(bytes, cell, column, row_index)?;
        } else {
            let value =
                mssql_datetime2_from_arrow_date64(column, row_index, array.value(row_index))?;
            write_direct_datetime2_cell(bytes, cell, column, value)?;
        }
    }

    Ok(())
}

fn append_temporal_value<A, F>(
    buf: &mut tiberius::RawRowsAppendBuffer<'_>,
    array: &A,
    mapping: &SchemaMapping,
    column: &DirectColumnPlan,
    row_index: usize,
    measured_len: usize,
    value: F,
) -> Result<()>
where
    A: Array,
    F: Fn(&A, &SchemaMapping, usize) -> Result<(usize, TemporalValue)>,
{
    if array.is_null(row_index) {
        profile::record_null_cell();
        validate_null_timestamp_timezone_metadata(mapping, row_index)?;
        let expected_len = null_temporal_cell_len_for_column(column, row_index)?;
        if measured_len != expected_len {
            return Err(invalid_payload(format!(
                "measured null temporal cell at row {row_index} column {} has length {}, expected {expected_len}",
                column.source_name(),
                measured_len
            )));
        }

        append_null_temporal_cell(buf);
        return Ok(());
    }

    validate_mapping_timestamp_timezone_metadata(mapping, row_index)?;
    let (expected_len, value) = value(array, mapping, row_index)?;
    if measured_len != expected_len {
        return Err(invalid_payload(format!(
            "measured temporal cell at row {row_index} column {} has length {}, expected {expected_len}",
            column.source_name(),
            measured_len
        )));
    }

    match value {
        TemporalValue::Date(value) => {
            append_date_cell(buf, value).map_err(|err| add_temporal_field(err, column))
        }
        TemporalValue::Time(value) => {
            append_time_cell(buf, value).map_err(|err| add_temporal_field(err, column))
        }
        TemporalValue::DateTime2(value) => {
            append_datetime2_cell(buf, value).map_err(|err| add_temporal_field(err, column))
        }
        TemporalValue::DateTime(value) => append_direct_datetime_cell(buf, column, value),
        TemporalValue::DateTimeOffset(value) => {
            append_datetimeoffset_cell(buf, value).map_err(|err| add_temporal_field(err, column))
        }
    }
}

#[derive(Clone, Copy)]
enum TemporalValue {
    Date(MssqlDate),
    Time(MssqlTime),
    DateTime2(MssqlDateTime2),
    DateTime(MssqlDateTime),
    DateTimeOffset(MssqlDateTimeOffset),
}

fn temporal_value_cell_len(column: &DirectColumnPlan, value: TemporalValue) -> Result<usize> {
    match value {
        TemporalValue::Date(_) => Ok(date_cell_len()),
        TemporalValue::Time(value) => time_cell_len_for_value(value),
        TemporalValue::DateTime2(value) => datetime2_cell_len_for_value(value),
        TemporalValue::DateTime(value) => Ok(datetime_cell_len_for_column(column, value)),
        TemporalValue::DateTimeOffset(value) => datetimeoffset_cell_len_for_value(value),
    }
}

fn datetime2_cell_len_for_value(value: MssqlDateTime2) -> Result<usize> {
    datetime2_cell_len(value.time().scale())
}

fn datetime_cell_len_for_column(column: &DirectColumnPlan, _value: MssqlDateTime) -> usize {
    if column.nullable() {
        datetime_cell_len()
    } else {
        datetime_payload_len()
    }
}

fn time_cell_len_for_value(value: MssqlTime) -> Result<usize> {
    time_cell_len(value.scale())
}

fn datetimeoffset_cell_len_for_value(value: MssqlDateTimeOffset) -> Result<usize> {
    datetimeoffset_cell_len(value.datetime2().time().scale())
}

fn append_direct_datetime_cell(
    buf: &mut tiberius::RawRowsAppendBuffer<'_>,
    column: &DirectColumnPlan,
    value: MssqlDateTime,
) -> Result<()> {
    if column.nullable() {
        append_datetime_cell(buf, value).map_err(|err| add_temporal_field(err, column))
    } else {
        let mut bytes = [0; datetime_payload_len()];
        write_datetime_payload(&mut bytes, value).map_err(|err| add_temporal_field(err, column))?;
        buf.extend_from_slice(&bytes);
        Ok(())
    }
}

fn null_temporal_cell_len_for_column(column: &DirectColumnPlan, row_index: usize) -> Result<usize> {
    if !column.nullable() {
        return Err(value_conversion_error(row_column_diagnostic(
            column,
            row_index,
            DiagnosticCode::NullInNonNullableColumn,
            "null value in non-nullable direct temporal column",
        )));
    }

    Ok(NULL_TEMPORAL_CELL_LEN)
}

fn unsupported_timestamp_target(column: &DirectColumnPlan, row_index: usize) -> Error {
    value_conversion_error(row_column_diagnostic(
        column,
        row_index,
        DiagnosticCode::ValueTypeMismatch,
        format!(
            "planned direct timestamp target {} is not supported",
            column.target_type().to_sql()
        ),
    ))
}

fn write_null_direct_temporal_cell(
    bytes: &mut [u8],
    cell: &CellPosition,
    column: &DirectColumnPlan,
    row_index: usize,
) -> Result<()> {
    let expected_len = null_temporal_cell_len_for_column(column, row_index)?;
    if cell.len() != expected_len {
        return Err(invalid_payload(format!(
            "null temporal cell at row {} column {} has length {}, expected {expected_len}",
            cell.row_index(),
            cell.column_index(),
            cell.len()
        )));
    }

    let cell_bytes = cell_bytes_mut(bytes, cell)?;
    write_null_temporal_cell(cell_bytes)
}

fn write_direct_date_cell(
    bytes: &mut [u8],
    cell: &CellPosition,
    column: &DirectColumnPlan,
    value: MssqlDate,
) -> Result<()> {
    let expected_len = date_cell_len();
    if cell.len() != expected_len {
        return Err(invalid_payload(format!(
            "date cell at row {} column {} has length {}, expected {expected_len}",
            cell.row_index(),
            cell.column_index(),
            cell.len()
        )));
    }

    let cell_bytes = cell_bytes_mut(bytes, cell)?;
    write_date_cell(cell_bytes, value).map_err(|err| add_temporal_field(err, column))
}

fn write_direct_time_cell(
    bytes: &mut [u8],
    cell: &CellPosition,
    column: &DirectColumnPlan,
    value: MssqlTime,
) -> Result<()> {
    let expected_len = time_cell_len_for_value(value)?;
    if cell.len() != expected_len {
        return Err(invalid_payload(format!(
            "time cell at row {} column {} has length {}, expected {expected_len}",
            cell.row_index(),
            cell.column_index(),
            cell.len()
        )));
    }

    let cell_bytes = cell_bytes_mut(bytes, cell)?;
    write_time_cell(cell_bytes, value).map_err(|err| add_temporal_field(err, column))
}

fn write_direct_temporal_value_cell(
    bytes: &mut [u8],
    cell: &CellPosition,
    column: &DirectColumnPlan,
    value: TemporalValue,
) -> Result<()> {
    match value {
        TemporalValue::Date(value) => write_direct_date_cell(bytes, cell, column, value),
        TemporalValue::Time(value) => write_direct_time_cell(bytes, cell, column, value),
        TemporalValue::DateTime2(value) => write_direct_datetime2_cell(bytes, cell, column, value),
        TemporalValue::DateTime(value) => write_direct_datetime_cell(bytes, cell, column, value),
        TemporalValue::DateTimeOffset(value) => {
            write_direct_datetimeoffset_cell(bytes, cell, column, value)
        }
    }
}

fn write_direct_datetime_cell(
    bytes: &mut [u8],
    cell: &CellPosition,
    column: &DirectColumnPlan,
    value: MssqlDateTime,
) -> Result<()> {
    let expected_len = datetime_cell_len_for_column(column, value);
    if cell.len() != expected_len {
        return Err(invalid_payload(format!(
            "datetime cell at row {} column {} has length {}, expected {expected_len}",
            cell.row_index(),
            cell.column_index(),
            cell.len()
        )));
    }

    let cell_bytes = cell_bytes_mut(bytes, cell)?;
    if column.nullable() {
        write_datetime_cell(cell_bytes, value).map_err(|err| add_temporal_field(err, column))
    } else {
        write_datetime_payload(cell_bytes, value).map_err(|err| add_temporal_field(err, column))
    }
}

fn write_direct_datetime2_cell(
    bytes: &mut [u8],
    cell: &CellPosition,
    column: &DirectColumnPlan,
    value: MssqlDateTime2,
) -> Result<()> {
    let expected_len = datetime2_cell_len_for_value(value)?;
    if cell.len() != expected_len {
        return Err(invalid_payload(format!(
            "datetime2 cell at row {} column {} has length {}, expected {expected_len}",
            cell.row_index(),
            cell.column_index(),
            cell.len()
        )));
    }

    let cell_bytes = cell_bytes_mut(bytes, cell)?;
    write_datetime2_cell(cell_bytes, value).map_err(|err| add_temporal_field(err, column))
}

fn write_direct_datetimeoffset_cell(
    bytes: &mut [u8],
    cell: &CellPosition,
    column: &DirectColumnPlan,
    value: MssqlDateTimeOffset,
) -> Result<()> {
    let expected_len = datetimeoffset_cell_len_for_value(value)?;
    if cell.len() != expected_len {
        return Err(invalid_payload(format!(
            "datetimeoffset cell at row {} column {} has length {}, expected {expected_len}",
            cell.row_index(),
            cell.column_index(),
            cell.len()
        )));
    }

    let cell_bytes = cell_bytes_mut(bytes, cell)?;
    write_datetimeoffset_cell(cell_bytes, value).map_err(|err| add_temporal_field(err, column))
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

fn cell_bytes_mut<'a>(bytes: &'a mut [u8], cell: &CellPosition) -> Result<&'a mut [u8]> {
    let end = cell
        .offset()
        .checked_add(cell.len())
        .ok_or_else(|| invalid_payload("temporal cell end overflowed usize"))?;

    bytes
        .get_mut(cell.offset()..end)
        .ok_or_else(|| invalid_payload("temporal cell range is outside payload"))
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

fn add_temporal_field(err: Error, column: &DirectColumnPlan) -> Error {
    let Error::DirectEncoding { diagnostics } = err else {
        return err;
    };

    let diagnostics = diagnostics
        .into_iter()
        .map(|diagnostic| {
            diagnostic.with_field(FieldRef::new(column.source_index(), column.source_name()))
        })
        .collect::<Vec<_>>();

    Error::DirectEncoding {
        diagnostics: DiagnosticSet::from(diagnostics),
    }
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

fn unsupported_temporal_batch(message: impl Into<String>) -> Error {
    Error::DirectEncoding {
        diagnostics: DiagnosticSet::from(vec![Diagnostic::error(
            DiagnosticCode::DirectEncodingUnsupportedBatch,
            message,
        )]),
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

#[cfg(test)]
mod tests {
    use crate::{
        DiagnosticCode, Error,
        mssql::cell::{MssqlDate, MssqlDateTime, MssqlDateTime2, MssqlDateTimeOffset, MssqlTime},
        write::direct::payload::TDS_ROW_TOKEN,
    };

    use super::value::null_temporal_cell_len;
    use super::{
        date_cell_len, datetime_cell_len, datetime2_cell_len, datetimeoffset_cell_len,
        time_cell_len, write_date_cell, write_datetime_cell, write_datetime2_cell,
        write_datetimeoffset_cell, write_null_temporal_cell, write_time_cell,
    };

    #[test]
    fn writes_null_temporal_cells_distinct_from_zero_values() {
        let mut null = vec![255; null_temporal_cell_len()];
        write_null_temporal_cell(&mut null).unwrap();

        let mut date_zero = vec![255; date_cell_len()];
        write_date_cell(&mut date_zero, MssqlDate::new(0)).unwrap();

        let mut time_zero = vec![255; time_cell_len(7).unwrap()];
        write_time_cell(&mut time_zero, MssqlTime::new(0, 7)).unwrap();

        let mut datetime2_zero = vec![255; datetime2_cell_len(7).unwrap()];
        write_datetime2_cell(
            &mut datetime2_zero,
            MssqlDateTime2::new(MssqlDate::new(0), MssqlTime::new(0, 7)),
        )
        .unwrap();

        let mut datetime_zero = vec![255; datetime_cell_len()];
        write_datetime_cell(&mut datetime_zero, MssqlDateTime::new(0, 0)).unwrap();

        let mut datetimeoffset_zero = vec![255; datetimeoffset_cell_len(7).unwrap()];
        write_datetimeoffset_cell(
            &mut datetimeoffset_zero,
            MssqlDateTimeOffset::new(
                MssqlDateTime2::new(MssqlDate::new(0), MssqlTime::new(0, 7)),
                0,
            ),
        )
        .unwrap();

        assert_eq!(null, [0]);
        assert_eq!(date_zero, [3, 0, 0, 0]);
        assert_ne!(time_zero, null);
        assert_ne!(datetime_zero, null);
        assert_ne!(datetime2_zero, null);
        assert_ne!(datetimeoffset_zero, null);
    }

    #[test]
    fn writes_date_boundaries_as_three_little_endian_bytes() {
        let mut lower = vec![0; date_cell_len()];
        write_date_cell(&mut lower, MssqlDate::new(0)).unwrap();
        assert_eq!(lower, [3, 0x00, 0x00, 0x00]);

        let mut upper = vec![0; date_cell_len()];
        write_date_cell(&mut upper, MssqlDate::new(3_652_058)).unwrap();
        assert_eq!(upper, [3, 0xDA, 0xB9, 0x37]);
    }

    #[test]
    fn writes_time_payload_widths_for_supported_precisions() {
        let cases = [
            (0, 3, vec![3, 0x7F, 0x51, 0x01]),
            (2, 3, vec![3, 0xFF, 0xD5, 0x83]),
            (3, 4, vec![4, 0xFF, 0x5B, 0x26, 0x05]),
            (4, 4, vec![4, 0xFF, 0x97, 0x7F, 0x33]),
            (5, 5, vec![5, 0xFF, 0xEF, 0xFB, 0x02, 0x02]),
            (6, 5, vec![5, 0xFF, 0x5F, 0xD7, 0x1D, 0x14]),
            (7, 5, vec![5, 0xFF, 0xBF, 0x69, 0x2A, 0xC9]),
        ];

        for (precision, payload_len, expected) in cases {
            assert_eq!(time_cell_len(precision).unwrap(), 1 + payload_len);
            let mut bytes = vec![0; time_cell_len(precision).unwrap()];
            write_time_cell(
                &mut bytes,
                MssqlTime::new(max_time_increment_for_test(precision), precision),
            )
            .unwrap();
            assert_eq!(bytes, expected);
        }
    }

    #[test]
    fn writes_time_midnight_and_max_tick_before_midnight() {
        let mut midnight = vec![0; time_cell_len(7).unwrap()];
        write_time_cell(&mut midnight, MssqlTime::new(0, 7)).unwrap();
        assert_eq!(midnight, [5, 0, 0, 0, 0, 0]);

        let mut max = vec![0; time_cell_len(7).unwrap()];
        write_time_cell(&mut max, MssqlTime::new(863_999_999_999, 7)).unwrap();
        assert_eq!(max, [5, 0xFF, 0xBF, 0x69, 0x2A, 0xC9]);
    }

    #[test]
    fn writes_datetime2_time_bytes_before_date_bytes() {
        let value = MssqlDateTime2::new(MssqlDate::new(719_162), MssqlTime::new(12_345, 3));
        let mut bytes = vec![0; datetime2_cell_len(3).unwrap()];

        write_datetime2_cell(&mut bytes, value).unwrap();

        assert_eq!(bytes, [7, 0x39, 0x30, 0x00, 0x00, 0x3A, 0xF9, 0x0A]);
    }

    #[test]
    fn writes_datetime_days_before_fragments() {
        let mut bytes = vec![0; datetime_cell_len()];

        write_datetime_cell(&mut bytes, MssqlDateTime::new(25_567, 300)).unwrap();

        assert_eq!(bytes, [8, 0xDF, 0x63, 0x00, 0x00, 0x2C, 0x01, 0x00, 0x00]);
    }

    #[test]
    fn writes_datetimeoffset_datetime2_then_signed_offset_minutes() {
        let cases = [
            (0, [0x00, 0x00]),
            (150, [0x96, 0x00]),
            (840, [0x48, 0x03]),
            (-840, [0xB8, 0xFC]),
        ];

        for (offset, offset_bytes) in cases {
            let value = MssqlDateTimeOffset::new(
                MssqlDateTime2::new(MssqlDate::new(719_162), MssqlTime::new(12_345, 7)),
                offset,
            );
            let mut bytes = vec![0; datetimeoffset_cell_len(7).unwrap()];

            write_datetimeoffset_cell(&mut bytes, value).unwrap();

            assert_eq!(
                bytes,
                [
                    10,
                    0x39,
                    0x30,
                    0x00,
                    0x00,
                    0x00,
                    0x3A,
                    0xF9,
                    0x0A,
                    offset_bytes[0],
                    offset_bytes[1],
                ]
            );
        }
    }

    #[test]
    fn rejects_invalid_precision_and_out_of_day_time() {
        assert_invalid_payload(time_cell_len(8).unwrap_err());

        let mut bytes = vec![0; 6];
        assert_invalid_payload(write_time_cell(&mut bytes, MssqlTime::new(0, 8)).unwrap_err());
        assert_invalid_payload(
            write_time_cell(&mut bytes, MssqlTime::new(864_000_000_000, 7)).unwrap_err(),
        );
    }

    #[test]
    fn rejects_datetimeoffset_offsets_outside_sql_server_range() {
        let mut bytes = vec![0; datetimeoffset_cell_len(7).unwrap()];

        for offset in [-841, 841] {
            assert_invalid_payload(
                write_datetimeoffset_cell(
                    &mut bytes,
                    MssqlDateTimeOffset::new(
                        MssqlDateTime2::new(MssqlDate::new(0), MssqlTime::new(0, 7)),
                        offset,
                    ),
                )
                .unwrap_err(),
            );
        }
    }

    #[test]
    fn rejects_invalid_destination_lengths() {
        assert_invalid_payload(write_null_temporal_cell(&mut []).unwrap_err());
        assert_invalid_payload(write_null_temporal_cell(&mut [0, 0]).unwrap_err());
        assert_invalid_payload(write_date_cell(&mut [0, 0, 0], MssqlDate::new(0)).unwrap_err());
        assert_invalid_payload(write_time_cell(&mut [0, 0, 0], MssqlTime::new(0, 3)).unwrap_err());
        assert_invalid_payload(
            write_datetime2_cell(
                &mut [0; 7],
                MssqlDateTime2::new(MssqlDate::new(0), MssqlTime::new(0, 3)),
            )
            .unwrap_err(),
        );
        assert_invalid_payload(
            write_datetimeoffset_cell(
                &mut [0; 10],
                MssqlDateTimeOffset::new(
                    MssqlDateTime2::new(MssqlDate::new(0), MssqlTime::new(0, 7)),
                    0,
                ),
            )
            .unwrap_err(),
        );
    }

    #[test]
    fn rejects_date_values_outside_sql_server_range() {
        let mut bytes = vec![0; date_cell_len()];

        assert_invalid_payload(write_date_cell(&mut bytes, MssqlDate::new(3_652_059)).unwrap_err());
    }

    #[test]
    fn helpers_write_cells_only_without_row_token_or_metadata() {
        let mut bytes = vec![0; datetime2_cell_len(7).unwrap()];
        write_datetime2_cell(
            &mut bytes,
            MssqlDateTime2::new(MssqlDate::new(719_162), MssqlTime::new(0, 7)),
        )
        .unwrap();

        assert_eq!(bytes[0], 8);
        assert!(!bytes.contains(&TDS_ROW_TOKEN));
    }

    fn max_time_increment_for_test(precision: u8) -> u64 {
        86_400 * 10_u64.pow(u32::from(precision)) - 1
    }

    fn assert_invalid_payload(err: Error) {
        let Error::DirectEncoding { diagnostics } = err else {
            panic!("expected direct encoding error");
        };

        assert_eq!(diagnostics.len(), 1);
        assert_eq!(
            diagnostics.all()[0].code(),
            DiagnosticCode::DirectEncodingInvalidPayload
        );
    }
}

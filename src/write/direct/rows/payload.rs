//! Payload-returning direct TDS row execution.

use arrow_array::{
    BinaryArray, BooleanArray, Float32Array, Float64Array, Int8Array, Int16Array, Int32Array,
    Int64Array, RecordBatch, StringArray, UInt8Array, UInt16Array, UInt32Array, UInt64Array,
};

use crate::{
    DiagnosticCode, Result,
    conversion::arrow_to_mssql::{
        primitive::PrimitiveArrowToMssql, variable_width::VariableWidthArrowToMssql,
    },
    write::record_batch::validate_runtime_columns,
};

use super::super::{
    DirectEncoder, MeasuredDirectBatch, downcast_direct_array, invalid_payload, layout,
    measure::measure_layout,
    payload::EncodedRowsPayload,
    plan::DirectColumnEncoding,
    row_column_diagnostic,
    rows::fixed_width::try_encode_fixed_width_rows,
    types::{
        decimal::fill_decimal_column,
        primitive::{
            allocate_rows_payload_with_tokens, fill_boolean_column, fill_float32_column,
            fill_float64_column, fill_int8_column, fill_int16_column, fill_int32_column,
            fill_int64_column, fill_uint8_column, fill_uint16_column, fill_uint32_column,
            fill_uint64_checked_bigint_column,
        },
        temporal::{TemporalColumnContext, fill_temporal_column},
        uint64::fill_uint64_decimal20_column,
        variable_width::{fill_nvarchar_column, fill_varbinary_column},
    },
    unsupported_batch, value_conversion_error,
};

/// Encodes a runtime batch into complete raw TDS row payload bytes.
pub(crate) fn encode_batch(
    encoder: &DirectEncoder,
    batch: &RecordBatch,
) -> Result<EncodedRowsPayload> {
    encode_checked_batch(encoder, batch)
}

/// Encodes a contiguous row range from a runtime batch.
///
/// Returned row-token offsets are relative to the returned payload, so the
/// first non-empty range always starts at offset zero.
pub(crate) fn encode_batch_range(
    encoder: &DirectEncoder,
    batch: &RecordBatch,
    start_row: usize,
    row_count: usize,
) -> Result<EncodedRowsPayload> {
    let end_row = start_row
        .checked_add(row_count)
        .ok_or_else(|| invalid_payload("direct row range end overflowed usize"))?;
    if end_row > batch.num_rows() {
        return Err(invalid_payload(format!(
            "direct row range {start_row}..{end_row} is outside batch row count {}",
            batch.num_rows()
        )));
    }

    let batch = batch.slice(start_row, row_count);
    encode_checked_batch(encoder, &batch)
}

/// Encodes one range from a pre-measured direct batch.
pub(crate) fn encode_measured_batch_range(
    encoder: &DirectEncoder,
    batch: &RecordBatch,
    measured: &MeasuredDirectBatch,
    start_row: usize,
    row_count: usize,
) -> Result<EncodedRowsPayload> {
    measured.check_range(start_row, row_count)?;

    if row_count == 0 {
        return EncodedRowsPayload::new(Vec::new(), Vec::new());
    }

    if measured.row_count() != batch.num_rows() {
        return Err(invalid_payload(format!(
            "measured row count {} does not match runtime batch row count {}",
            measured.row_count(),
            batch.num_rows()
        )));
    }

    if measured.column_count() != encoder.plan.column_count() {
        return Err(invalid_payload(format!(
            "measured column count {} does not match direct plan column count {}",
            measured.column_count(),
            encoder.plan.column_count()
        )));
    }

    let batch = batch.slice(start_row, row_count);
    if let Some(payload) = try_encode_fixed_width_rows(
        &batch,
        &encoder.mappings,
        encoder.plan_options,
        encoder.plan.columns(),
    )? {
        return Ok(payload);
    }

    let layout = measured.range_layout(start_row, row_count)?;
    let mut bytes = allocate_rows_payload_with_tokens(&layout);
    fill_columns(encoder, &batch, &layout, &mut bytes)?;

    EncodedRowsPayload::new(bytes, layout.row_token_offsets().to_vec())
}

fn encode_checked_batch(
    encoder: &DirectEncoder,
    batch: &RecordBatch,
) -> Result<EncodedRowsPayload> {
    validate_runtime_columns(batch, &encoder.mappings)?;

    if encoder.plan.is_empty() && batch.num_rows() == 0 {
        return EncodedRowsPayload::new(Vec::new(), Vec::new());
    }

    if let Some(payload) = try_encode_fixed_width_rows(
        batch,
        &encoder.mappings,
        encoder.plan_options,
        encoder.plan.columns(),
    )? {
        return Ok(payload);
    }

    let layout = measure_layout(encoder, batch)?;
    let mut bytes = allocate_rows_payload_with_tokens(&layout);
    fill_columns(encoder, batch, &layout, &mut bytes)?;

    EncodedRowsPayload::new(bytes, layout.row_token_offsets().to_vec())
}

fn fill_columns(
    encoder: &DirectEncoder,
    batch: &RecordBatch,
    layout: &layout::RowLayout,
    bytes: &mut [u8],
) -> Result<()> {
    let column_count = encoder.plan.column_count();

    for (column_index, column) in encoder.plan.columns().iter().enumerate() {
        let Some(array) = batch
            .columns()
            .get(column.source_index())
            .map(AsRef::as_ref)
        else {
            return Err(value_conversion_error(row_column_diagnostic(
                column,
                0,
                DiagnosticCode::ValueTypeMismatch,
                "planned direct column index is outside the runtime batch",
            )));
        };

        match column.encoding() {
            DirectColumnEncoding::Primitive(PrimitiveArrowToMssql::BooleanToBit) => {
                let array = downcast_direct_array::<BooleanArray>(array, column)?;
                fill_boolean_column(array, column, column_index, column_count, layout, bytes)?;
            }
            DirectColumnEncoding::Primitive(PrimitiveArrowToMssql::UInt8ToTinyInt) => {
                let array = downcast_direct_array::<UInt8Array>(array, column)?;
                fill_uint8_column(array, column, column_index, column_count, layout, bytes)?;
            }
            DirectColumnEncoding::Primitive(PrimitiveArrowToMssql::Int8ToSmallInt) => {
                let array = downcast_direct_array::<Int8Array>(array, column)?;
                fill_int8_column(array, column, column_index, column_count, layout, bytes)?;
            }
            DirectColumnEncoding::Primitive(PrimitiveArrowToMssql::Int16ToSmallInt) => {
                let array = downcast_direct_array::<Int16Array>(array, column)?;
                fill_int16_column(array, column, column_index, column_count, layout, bytes)?;
            }
            DirectColumnEncoding::Primitive(PrimitiveArrowToMssql::Int32ToInt) => {
                let array = downcast_direct_array::<Int32Array>(array, column)?;
                fill_int32_column(array, column, column_index, column_count, layout, bytes)?;
            }
            DirectColumnEncoding::Primitive(PrimitiveArrowToMssql::UInt16ToInt) => {
                let array = downcast_direct_array::<UInt16Array>(array, column)?;
                fill_uint16_column(array, column, column_index, column_count, layout, bytes)?;
            }
            DirectColumnEncoding::Primitive(PrimitiveArrowToMssql::Int64ToBigInt) => {
                let array = downcast_direct_array::<Int64Array>(array, column)?;
                fill_int64_column(array, column, column_index, column_count, layout, bytes)?;
            }
            DirectColumnEncoding::Primitive(PrimitiveArrowToMssql::UInt32ToBigInt) => {
                let array = downcast_direct_array::<UInt32Array>(array, column)?;
                fill_uint32_column(array, column, column_index, column_count, layout, bytes)?;
            }
            DirectColumnEncoding::Primitive(PrimitiveArrowToMssql::UInt64ToCheckedBigInt) => {
                let array = downcast_direct_array::<UInt64Array>(array, column)?;
                fill_uint64_checked_bigint_column(
                    array,
                    column,
                    column_index,
                    column_count,
                    layout,
                    bytes,
                )?;
            }
            DirectColumnEncoding::Primitive(PrimitiveArrowToMssql::Float32ToReal) => {
                let array = downcast_direct_array::<Float32Array>(array, column)?;
                fill_float32_column(array, column, column_index, column_count, layout, bytes)?;
            }
            DirectColumnEncoding::Primitive(PrimitiveArrowToMssql::Float64ToFloat) => {
                let array = downcast_direct_array::<Float64Array>(array, column)?;
                fill_float64_column(array, column, column_index, column_count, layout, bytes)?;
            }
            DirectColumnEncoding::UInt64Decimal20_0 => {
                let array = downcast_direct_array::<UInt64Array>(array, column)?;
                fill_uint64_decimal20_column(
                    array,
                    column,
                    column_index,
                    column_count,
                    layout,
                    bytes,
                )?;
            }
            DirectColumnEncoding::Decimal(classification) => {
                fill_decimal_column(
                    array,
                    column,
                    classification,
                    column_index,
                    column_count,
                    layout,
                    bytes,
                )?;
            }
            DirectColumnEncoding::VariableWidth(other) => match other {
                VariableWidthArrowToMssql::Utf8ToNVarChar { .. } => {
                    let array = downcast_direct_array::<StringArray>(array, column)?;
                    fill_nvarchar_column(array, column, column_index, column_count, layout, bytes)?;
                }
                VariableWidthArrowToMssql::BinaryToVarBinary { .. } => {
                    let array = downcast_direct_array::<BinaryArray>(array, column)?;
                    fill_varbinary_column(
                        array,
                        column,
                        column_index,
                        column_count,
                        layout,
                        bytes,
                    )?;
                }
                unsupported => {
                    return Err(unsupported_batch(format!(
                        "direct variable-width fill is not implemented yet for {unsupported:?}"
                    )));
                }
            },
            DirectColumnEncoding::Temporal(classification) => {
                let mapping = encoder.mapping_for_column_index(column_index)?;
                fill_temporal_column(
                    array,
                    TemporalColumnContext {
                        mapping,
                        plan_options: encoder.plan_options,
                        column,
                        classification,
                        column_index,
                        column_count,
                    },
                    layout,
                    bytes,
                )?;
            }
        }
    }

    Ok(())
}

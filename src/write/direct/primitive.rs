//! Fixed-width primitive direct TDS row layout.

use arrow_array::{Array, BooleanArray, Float64Array, Int32Array, Int64Array, RecordBatch};

use crate::{
    Diagnostic, DiagnosticCode, DiagnosticSet, Error, FieldRef, Result,
    conversion::arrow_to_mssql::primitive::PrimitiveArrowToMssql,
};

use super::{
    layout::{CellPosition, RowLayout},
    payload::{EncodedRowsPayload, TDS_ROW_TOKEN},
    plan::{DirectColumnEncoding, DirectColumnPlan},
};

const ROW_TOKEN_LEN: usize = 1;
const CELL_LEN_PREFIX_LEN: usize = 1;

#[derive(Debug, Clone, Copy)]
struct FixedWidthColumn<'a> {
    plan: &'a DirectColumnPlan,
    value_len: usize,
}

/// Encodes fixed-width primitive columns without building a full per-cell row
/// layout.
///
/// Returns `Ok(None)` when the columns require the general layout path.
pub(crate) fn try_encode_fixed_width_primitive_rows(
    batch: &RecordBatch,
    columns: &[DirectColumnPlan],
) -> Result<Option<EncodedRowsPayload>> {
    if batch.num_rows() == 0 {
        return Ok(Some(EncodedRowsPayload::new(Vec::new(), Vec::new())?));
    }

    let Some(columns) = fixed_width_columns(columns)? else {
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
            DirectColumnEncoding::Primitive(PrimitiveArrowToMssql::Int32ToInt) => {
                let array = downcast_direct_array::<Int32Array>(array, column.plan)?;
                fill_int32_fixed_width_column(array, column.plan, &mut current_offsets, &mut bytes);
            }
            DirectColumnEncoding::Primitive(PrimitiveArrowToMssql::Int64ToBigInt) => {
                let array = downcast_direct_array::<Int64Array>(array, column.plan)?;
                fill_int64_fixed_width_column(array, column.plan, &mut current_offsets, &mut bytes);
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
            DirectColumnEncoding::Primitive(_) => {
                return Ok(None);
            }
            DirectColumnEncoding::VariableWidth(_) => {
                return Ok(None);
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

fn fixed_width_columns(columns: &[DirectColumnPlan]) -> Result<Option<Vec<FixedWidthColumn<'_>>>> {
    let mut fixed_width_columns = Vec::with_capacity(columns.len());

    for column in columns {
        let Some(value_len) = fixed_width_value_len(column.encoding()) else {
            return Ok(None);
        };

        fixed_width_columns.push(FixedWidthColumn {
            plan: column,
            value_len,
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

        if column.plan.nullable() {
            add_nullable_fixed_width_column_lengths(array, column.value_len, &mut row_lengths)?;
        } else {
            if array.null_count() != 0 {
                return Err(first_null_error(array, column.plan));
            }

            add_non_nullable_fixed_width_column_lengths(column.value_len, &mut row_lengths)?;
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
    value_len: usize,
    row_lengths: &mut [usize],
) -> Result<()> {
    for row_length in row_lengths {
        *row_length = checked_add(*row_length, value_len)?;
    }

    Ok(())
}

fn add_nullable_fixed_width_column_lengths(
    array: &dyn Array,
    value_len: usize,
    row_lengths: &mut [usize],
) -> Result<()> {
    for (row_index, row_length) in row_lengths.iter_mut().enumerate() {
        let cell_len = if array.is_null(row_index) {
            CELL_LEN_PREFIX_LEN
        } else {
            checked_add(CELL_LEN_PREFIX_LEN, value_len)?
        };

        *row_length = checked_add(*row_length, cell_len)?;
    }

    Ok(())
}

fn fixed_width_value_len(encoding: DirectColumnEncoding) -> Option<usize> {
    match encoding {
        DirectColumnEncoding::Primitive(PrimitiveArrowToMssql::BooleanToBit) => Some(1),
        DirectColumnEncoding::Primitive(PrimitiveArrowToMssql::Int32ToInt) => Some(4),
        DirectColumnEncoding::Primitive(PrimitiveArrowToMssql::Int64ToBigInt) => Some(8),
        DirectColumnEncoding::Primitive(PrimitiveArrowToMssql::Float64ToFloat) => Some(8),
        DirectColumnEncoding::Primitive(_) => None,
        DirectColumnEncoding::VariableWidth(_) => None,
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
        "null value in non-nullable direct primitive column",
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
        DirectColumnEncoding::Primitive(PrimitiveArrowToMssql::Int32ToInt) => Ok(4),
        DirectColumnEncoding::Primitive(PrimitiveArrowToMssql::Int64ToBigInt) => Ok(8),
        DirectColumnEncoding::Primitive(PrimitiveArrowToMssql::Float64ToFloat) => Ok(8),
        DirectColumnEncoding::Primitive(other) => Err(unsupported_batch(format!(
            "direct primitive layout is not implemented yet for {other:?}"
        ))),
        DirectColumnEncoding::VariableWidth(other) => Err(unsupported_batch(format!(
            "direct primitive layout is not implemented for variable-width mapping {other:?}"
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
    use arrow_array::{BooleanArray, Float64Array, Int32Array, Int64Array};
    use arrow_schema::DataType;

    use crate::{
        ArrowFieldRef, DiagnosticCode, Error, Identifier, MssqlColumn, MssqlType, SchemaMapping,
    };

    use super::{
        allocate_rows_payload_with_tokens, build_fixed_width_row_layout, fill_boolean_column,
        fill_float64_column, fill_int32_column, fill_int64_column,
        measure_primitive_column_cell_lengths,
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

//! UInt64 policy-specific direct TDS row layout.

use arrow_array::{Array, UInt64Array};

use crate::{Diagnostic, DiagnosticCode, DiagnosticSet, Error, FieldRef, Result, write::profile};

use super::{
    layout::{CellPosition, RowLayout},
    plan::DirectColumnPlan,
};

const DECIMAL_CELL_LEN_PREFIX_LEN: usize = 1;
const DECIMAL_SIGN_LEN: usize = 1;
const DECIMAL_POSITIVE_SIGN: u8 = 1;

/// Measures one UInt64-to-decimal(20,0) column into a row-major cell length matrix.
pub(crate) fn measure_uint64_decimal20_cell_lengths(
    array: &UInt64Array,
    column: &DirectColumnPlan,
    column_index: usize,
    column_count: usize,
    cell_lengths: &mut [usize],
) -> Result<()> {
    for row_index in 0..array.len() {
        let cell_len = if array.is_null(row_index) {
            null_decimal20_cell_len(column, row_index)?
        } else {
            uint64_decimal20_cell_len(array.value(row_index))
        };

        cell_lengths[row_index * column_count + column_index] = cell_len;
    }

    Ok(())
}

/// Fills one UInt64-to-decimal(20,0) column into an already allocated rows payload.
pub(crate) fn fill_uint64_decimal20_column(
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
            write_null_decimal20_cell(bytes, cell, column, row_index)?;
        } else {
            write_uint64_decimal20_cell(bytes, cell, array.value(row_index))?;
        }
    }

    Ok(())
}

/// Appends one UInt64-to-decimal(20,0) cell to a raw bulk append buffer.
pub(crate) fn append_uint64_decimal20_cell(
    buf: &mut tiberius::RawRowsAppendBuffer<'_>,
    array: &UInt64Array,
    column: &DirectColumnPlan,
    row_index: usize,
    measured_len: usize,
) -> Result<()> {
    if array.is_null(row_index) {
        profile::record_null_cell();
        let expected_len = null_decimal20_cell_len(column, row_index)?;
        if measured_len != expected_len {
            return Err(invalid_payload(format!(
                "measured null UInt64 decimal20_0 cell at row {row_index} column {} has length {}, expected {expected_len}",
                column.source_name(),
                measured_len
            )));
        }

        buf.put_u8(0);
        return Ok(());
    }

    let value = array.value(row_index);
    let expected_len = uint64_decimal20_cell_len(value);
    if measured_len != expected_len {
        return Err(invalid_payload(format!(
            "measured UInt64 decimal20_0 cell at row {row_index} column {} has length {}, expected {expected_len}",
            column.source_name(),
            measured_len
        )));
    }

    append_uint64_decimal20_value(buf, value)
}

fn write_null_decimal20_cell(
    bytes: &mut [u8],
    cell: &CellPosition,
    column: &DirectColumnPlan,
    row_index: usize,
) -> Result<()> {
    let expected_len = null_decimal20_cell_len(column, row_index)?;
    if cell.len() != expected_len {
        return Err(invalid_payload(format!(
            "null UInt64 decimal20_0 cell at row {} column {} has length {}, expected {expected_len}",
            cell.row_index(),
            cell.column_index(),
            cell.len()
        )));
    }

    let cell_bytes = cell_bytes_mut(bytes, cell)?;
    cell_bytes[0] = 0;
    Ok(())
}

fn write_uint64_decimal20_cell(bytes: &mut [u8], cell: &CellPosition, value: u64) -> Result<()> {
    let expected_len = uint64_decimal20_cell_len(value);
    if cell.len() != expected_len {
        return Err(invalid_payload(format!(
            "UInt64 decimal20_0 cell at row {} column {} has length {}, expected {expected_len}",
            cell.row_index(),
            cell.column_index(),
            cell.len()
        )));
    }

    let cell_bytes = cell_bytes_mut(bytes, cell)?;
    write_uint64_decimal20_value(cell_bytes, value)
}

fn append_uint64_decimal20_value(
    buf: &mut tiberius::RawRowsAppendBuffer<'_>,
    value: u64,
) -> Result<()> {
    let value_len = uint64_decimal20_value_len(value);
    buf.put_u8(value_len);
    buf.put_u8(DECIMAL_POSITIVE_SIGN);

    match decimal_magnitude_len(value) {
        4 => buf.put_u32_le(value as u32),
        8 => buf.put_u64_le(value),
        12 => {
            buf.put_u64_le(value);
            buf.put_u32_le(0);
        }
        other => {
            return Err(invalid_payload(format!(
                "unsupported UInt64 decimal20_0 magnitude length {other}"
            )));
        }
    }

    Ok(())
}

fn write_uint64_decimal20_value(dst: &mut [u8], value: u64) -> Result<()> {
    let value_len = uint64_decimal20_value_len(value);
    dst[0] = value_len;
    dst[1] = DECIMAL_POSITIVE_SIGN;

    match decimal_magnitude_len(value) {
        4 => dst[2..6].copy_from_slice(&(value as u32).to_le_bytes()),
        8 => dst[2..10].copy_from_slice(&value.to_le_bytes()),
        12 => {
            dst[2..10].copy_from_slice(&value.to_le_bytes());
            dst[10..14].copy_from_slice(&0u32.to_le_bytes());
        }
        other => {
            return Err(invalid_payload(format!(
                "unsupported UInt64 decimal20_0 magnitude length {other}"
            )));
        }
    }

    Ok(())
}

fn uint64_decimal20_cell_len(value: u64) -> usize {
    DECIMAL_CELL_LEN_PREFIX_LEN + usize::from(uint64_decimal20_value_len(value))
}

fn uint64_decimal20_value_len(value: u64) -> u8 {
    (DECIMAL_SIGN_LEN + decimal_magnitude_len(value)) as u8
}

fn decimal_magnitude_len(value: u64) -> usize {
    match decimal_precision(value) {
        1..=9 => 4,
        10..=19 => 8,
        20 => 12,
        _ => unreachable!("u64 decimal precision cannot exceed 20"),
    }
}

fn decimal_precision(mut value: u64) -> u8 {
    let mut digits = 1;
    while value >= 10 {
        value /= 10;
        digits += 1;
    }
    digits
}

fn null_decimal20_cell_len(column: &DirectColumnPlan, row_index: usize) -> Result<usize> {
    if !column.nullable() {
        return Err(value_conversion_error(row_column_diagnostic(
            column,
            row_index,
            DiagnosticCode::NullInNonNullableColumn,
            "null value in non-nullable direct UInt64 decimal20_0 column",
        )));
    }

    Ok(DECIMAL_CELL_LEN_PREFIX_LEN)
}

fn cell_bytes_mut<'a>(bytes: &'a mut [u8], cell: &CellPosition) -> Result<&'a mut [u8]> {
    let end = cell
        .offset()
        .checked_add(cell.len())
        .ok_or_else(|| invalid_payload("UInt64 decimal20_0 cell end overflowed usize"))?;

    bytes
        .get_mut(cell.offset()..end)
        .ok_or_else(|| invalid_payload("UInt64 decimal20_0 cell range is outside payload"))
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

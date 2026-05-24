//! UInt64 policy-specific direct TDS row layout.

use arrow_array::{Array, UInt64Array};

use crate::{Diagnostic, DiagnosticCode, DiagnosticSet, Error, FieldRef, Result, write::profile};

use super::{
    super::{
        layout::{CellPosition, RowLayout},
        plan::DirectColumnPlan,
    },
    decimal::{
        NULL_DECIMAL_CELL_LEN, append_decimal_cell, append_null_decimal_cell, decimal_cell_len,
        write_decimal_cell, write_null_decimal_cell as write_null_decimal_payload_cell,
    },
};

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
            decimal_cell_len(i128::from(array.value(row_index)))
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

        append_null_decimal_cell(buf);
        return Ok(());
    }

    let value = array.value(row_index);
    let expected_len = decimal_cell_len(i128::from(value));
    if measured_len != expected_len {
        return Err(invalid_payload(format!(
            "measured UInt64 decimal20_0 cell at row {row_index} column {} has length {}, expected {expected_len}",
            column.source_name(),
            measured_len
        )));
    }

    append_decimal_cell(buf, i128::from(value))
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
    write_null_decimal_payload_cell(cell_bytes)
}

fn write_uint64_decimal20_cell(bytes: &mut [u8], cell: &CellPosition, value: u64) -> Result<()> {
    let expected_len = decimal_cell_len(i128::from(value));
    if cell.len() != expected_len {
        return Err(invalid_payload(format!(
            "UInt64 decimal20_0 cell at row {} column {} has length {}, expected {expected_len}",
            cell.row_index(),
            cell.column_index(),
            cell.len()
        )));
    }

    let cell_bytes = cell_bytes_mut(bytes, cell)?;
    write_decimal_cell(cell_bytes, i128::from(value))
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

    Ok(NULL_DECIMAL_CELL_LEN)
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

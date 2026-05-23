//! Shared SQL Server decimal/numeric direct TDS row payload helpers.

use arrow_array::{Array, Decimal32Array, Decimal64Array, Decimal128Array, Decimal256Array};

use crate::{
    Diagnostic, DiagnosticCode, DiagnosticSet, Error, FieldRef, Result,
    conversion::arrow_to_mssql::decimal::DecimalArrowToMssql, write::profile,
};

use super::{
    layout::{CellPosition, RowLayout},
    plan::DirectColumnPlan,
};

pub(crate) const NULL_DECIMAL_CELL_LEN: usize = 1;
const DECIMAL_SIGN_LEN: usize = 1;
const DECIMAL_NEGATIVE_SIGN: u8 = 0;
const DECIMAL_POSITIVE_SIGN: u8 = 1;

/// Measures one Arrow decimal column into a row-major cell length matrix.
pub(crate) fn measure_decimal_column_cell_lengths(
    array: &dyn Array,
    column: &DirectColumnPlan,
    classification: DecimalArrowToMssql,
    column_index: usize,
    column_count: usize,
    cell_lengths: &mut [usize],
) -> Result<()> {
    match classification {
        DecimalArrowToMssql::Decimal32 { .. } => {
            let array = downcast_direct_array::<Decimal32Array>(array, column)?;
            measure_decimal_values(
                array,
                column,
                classification,
                column_index,
                column_count,
                cell_lengths,
                |array, row_index| Ok(i128::from(array.value(row_index))),
            )
        }
        DecimalArrowToMssql::Decimal64 { .. } => {
            let array = downcast_direct_array::<Decimal64Array>(array, column)?;
            measure_decimal_values(
                array,
                column,
                classification,
                column_index,
                column_count,
                cell_lengths,
                |array, row_index| Ok(i128::from(array.value(row_index))),
            )
        }
        DecimalArrowToMssql::Decimal128 { .. } => {
            let array = downcast_direct_array::<Decimal128Array>(array, column)?;
            measure_decimal_values(
                array,
                column,
                classification,
                column_index,
                column_count,
                cell_lengths,
                |array, row_index| Ok(array.value(row_index)),
            )
        }
        DecimalArrowToMssql::Decimal256CheckedDowncast { .. } => {
            let array = downcast_direct_array::<Decimal256Array>(array, column)?;
            measure_decimal_values(
                array,
                column,
                classification,
                column_index,
                column_count,
                cell_lengths,
                |array, row_index| decimal256_to_i128(array.value(row_index), column, row_index),
            )
        }
    }
}

/// Fills one Arrow decimal column into an already allocated rows payload.
pub(crate) fn fill_decimal_column(
    array: &dyn Array,
    column: &DirectColumnPlan,
    classification: DecimalArrowToMssql,
    column_index: usize,
    column_count: usize,
    layout: &RowLayout,
    bytes: &mut [u8],
) -> Result<()> {
    match classification {
        DecimalArrowToMssql::Decimal32 { .. } => {
            let array = downcast_direct_array::<Decimal32Array>(array, column)?;
            fill_decimal_values(
                array,
                column,
                classification,
                DecimalFillTarget {
                    column_index,
                    column_count,
                    layout,
                    bytes,
                },
                |array, row_index| Ok(i128::from(array.value(row_index))),
            )
        }
        DecimalArrowToMssql::Decimal64 { .. } => {
            let array = downcast_direct_array::<Decimal64Array>(array, column)?;
            fill_decimal_values(
                array,
                column,
                classification,
                DecimalFillTarget {
                    column_index,
                    column_count,
                    layout,
                    bytes,
                },
                |array, row_index| Ok(i128::from(array.value(row_index))),
            )
        }
        DecimalArrowToMssql::Decimal128 { .. } => {
            let array = downcast_direct_array::<Decimal128Array>(array, column)?;
            fill_decimal_values(
                array,
                column,
                classification,
                DecimalFillTarget {
                    column_index,
                    column_count,
                    layout,
                    bytes,
                },
                |array, row_index| Ok(array.value(row_index)),
            )
        }
        DecimalArrowToMssql::Decimal256CheckedDowncast { .. } => {
            let array = downcast_direct_array::<Decimal256Array>(array, column)?;
            fill_decimal_values(
                array,
                column,
                classification,
                DecimalFillTarget {
                    column_index,
                    column_count,
                    layout,
                    bytes,
                },
                |array, row_index| decimal256_to_i128(array.value(row_index), column, row_index),
            )
        }
    }
}

/// Appends one Arrow decimal cell to a raw bulk append buffer.
pub(crate) fn append_decimal32_cell(
    buf: &mut tiberius::RawRowsAppendBuffer<'_>,
    array: &Decimal32Array,
    column: &DirectColumnPlan,
    classification: DecimalArrowToMssql,
    row_index: usize,
    measured_len: usize,
) -> Result<()> {
    append_decimal_value(
        buf,
        array,
        column,
        classification,
        row_index,
        measured_len,
        |array, row_index| Ok(i128::from(array.value(row_index))),
    )
}

/// Appends one Arrow Decimal64 cell to a raw bulk append buffer.
pub(crate) fn append_decimal64_cell(
    buf: &mut tiberius::RawRowsAppendBuffer<'_>,
    array: &Decimal64Array,
    column: &DirectColumnPlan,
    classification: DecimalArrowToMssql,
    row_index: usize,
    measured_len: usize,
) -> Result<()> {
    append_decimal_value(
        buf,
        array,
        column,
        classification,
        row_index,
        measured_len,
        |array, row_index| Ok(i128::from(array.value(row_index))),
    )
}

/// Appends one Arrow Decimal128 cell to a raw bulk append buffer.
pub(crate) fn append_decimal128_cell(
    buf: &mut tiberius::RawRowsAppendBuffer<'_>,
    array: &Decimal128Array,
    column: &DirectColumnPlan,
    classification: DecimalArrowToMssql,
    row_index: usize,
    measured_len: usize,
) -> Result<()> {
    append_decimal_value(
        buf,
        array,
        column,
        classification,
        row_index,
        measured_len,
        |array, row_index| Ok(array.value(row_index)),
    )
}

/// Appends one Arrow Decimal256 cell to a raw bulk append buffer.
pub(crate) fn append_decimal256_cell(
    buf: &mut tiberius::RawRowsAppendBuffer<'_>,
    array: &Decimal256Array,
    column: &DirectColumnPlan,
    classification: DecimalArrowToMssql,
    row_index: usize,
    measured_len: usize,
) -> Result<()> {
    append_decimal_value(
        buf,
        array,
        column,
        classification,
        row_index,
        measured_len,
        |array, row_index| decimal256_to_i128(array.value(row_index), column, row_index),
    )
}

/// Returns the byte length of a non-null decimal cell in a TDS row payload.
pub(crate) fn decimal_cell_len(unscaled: i128) -> usize {
    NULL_DECIMAL_CELL_LEN + decimal_value_len(unscaled)
}

/// Writes a SQL Server NULL decimal cell into an exactly sized cell buffer.
pub(crate) fn write_null_decimal_cell(dst: &mut [u8]) -> Result<()> {
    if dst.len() != NULL_DECIMAL_CELL_LEN {
        return Err(invalid_payload(format!(
            "null decimal cell has length {}, expected {NULL_DECIMAL_CELL_LEN}",
            dst.len()
        )));
    }

    dst[0] = 0;
    Ok(())
}

/// Writes a non-null SQL Server decimal cell into an exactly sized cell buffer.
pub(crate) fn write_decimal_cell(dst: &mut [u8], unscaled: i128) -> Result<()> {
    let expected_len = decimal_cell_len(unscaled);
    if dst.len() != expected_len {
        return Err(invalid_payload(format!(
            "decimal cell has length {}, expected {expected_len}",
            dst.len()
        )));
    }

    write_decimal_value(dst, unscaled)
}

/// Appends a SQL Server NULL decimal cell to a raw rows append buffer.
pub(crate) fn append_null_decimal_cell(buf: &mut tiberius::RawRowsAppendBuffer<'_>) {
    buf.put_u8(0);
}

/// Appends a non-null SQL Server decimal cell to a raw rows append buffer.
pub(crate) fn append_decimal_cell(
    buf: &mut tiberius::RawRowsAppendBuffer<'_>,
    unscaled: i128,
) -> Result<()> {
    let value_len = decimal_value_len(unscaled);
    buf.put_u8(value_len as u8);
    buf.put_u8(decimal_sign(unscaled));
    append_decimal_magnitude(buf, unscaled.unsigned_abs())
}

fn measure_decimal_values<A, F>(
    array: &A,
    column: &DirectColumnPlan,
    classification: DecimalArrowToMssql,
    column_index: usize,
    column_count: usize,
    cell_lengths: &mut [usize],
    value: F,
) -> Result<()>
where
    A: Array,
    F: Fn(&A, usize) -> Result<i128>,
{
    for row_index in 0..array.len() {
        let cell_len = if array.is_null(row_index) {
            null_decimal_cell_len(column, row_index)?
        } else {
            let unscaled = normalized_decimal_value(
                column,
                classification,
                row_index,
                value(array, row_index)?,
            )?;
            decimal_cell_len(unscaled)
        };

        cell_lengths[row_index * column_count + column_index] = cell_len;
    }

    Ok(())
}

fn fill_decimal_values<A, F>(
    array: &A,
    column: &DirectColumnPlan,
    classification: DecimalArrowToMssql,
    target: DecimalFillTarget<'_>,
    value: F,
) -> Result<()>
where
    A: Array,
    F: Fn(&A, usize) -> Result<i128>,
{
    let DecimalFillTarget {
        column_index,
        column_count,
        layout,
        bytes,
    } = target;

    for row_index in 0..array.len() {
        let cell = cell_position(layout, row_index, column_index, column_count)?;

        if array.is_null(row_index) {
            write_null_direct_decimal_cell(bytes, cell, column, row_index)?;
        } else {
            let unscaled = normalized_decimal_value(
                column,
                classification,
                row_index,
                value(array, row_index)?,
            )?;
            write_direct_decimal_cell(bytes, cell, column, unscaled)?;
        }
    }

    Ok(())
}

struct DecimalFillTarget<'a> {
    column_index: usize,
    column_count: usize,
    layout: &'a RowLayout,
    bytes: &'a mut [u8],
}

fn append_decimal_value<A, F>(
    buf: &mut tiberius::RawRowsAppendBuffer<'_>,
    array: &A,
    column: &DirectColumnPlan,
    classification: DecimalArrowToMssql,
    row_index: usize,
    measured_len: usize,
    value: F,
) -> Result<()>
where
    A: Array,
    F: Fn(&A, usize) -> Result<i128>,
{
    if array.is_null(row_index) {
        profile::record_null_cell();
        let expected_len = null_decimal_cell_len(column, row_index)?;
        if measured_len != expected_len {
            return Err(invalid_payload(format!(
                "measured null decimal cell at row {row_index} column {} has length {}, expected {expected_len}",
                column.source_name(),
                measured_len
            )));
        }

        append_null_decimal_cell(buf);
        return Ok(());
    }

    let unscaled =
        normalized_decimal_value(column, classification, row_index, value(array, row_index)?)?;
    let expected_len = decimal_cell_len(unscaled);
    if measured_len != expected_len {
        return Err(invalid_payload(format!(
            "measured decimal cell at row {row_index} column {} has length {}, expected {expected_len}",
            column.source_name(),
            measured_len
        )));
    }

    append_decimal_cell(buf, unscaled)
}

fn normalized_decimal_value(
    column: &DirectColumnPlan,
    classification: DecimalArrowToMssql,
    row_index: usize,
    unscaled: i128,
) -> Result<i128> {
    let unscaled =
        normalize_negative_scale(column, row_index, unscaled, classification.arrow_scale())?;

    if decimal_unscaled_fits_precision(unscaled, classification.target_precision()) {
        return Ok(unscaled);
    }

    Err(value_conversion_error(row_column_diagnostic(
        column,
        row_index,
        DiagnosticCode::DecimalOutOfRange,
        format!(
            "decimal value {unscaled} does not fit planned precision {}",
            classification.target_precision()
        ),
    )))
}

fn decimal256_to_i128(
    value: arrow_buffer::i256,
    column: &DirectColumnPlan,
    row_index: usize,
) -> Result<i128> {
    value.to_i128().ok_or_else(|| {
        value_conversion_error(row_column_diagnostic(
            column,
            row_index,
            DiagnosticCode::DecimalOutOfRange,
            "Arrow Decimal256 value does not fit runtime i128 decimal representation",
        ))
    })
}

fn normalize_negative_scale(
    column: &DirectColumnPlan,
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
            value_conversion_error(row_column_diagnostic(
                column,
                row_index,
                DiagnosticCode::DecimalOutOfRange,
                format!("negative decimal scale {arrow_scale} normalization factor overflows"),
            ))
        })?;

    unscaled.checked_mul(factor).ok_or_else(|| {
        value_conversion_error(row_column_diagnostic(
            column,
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

fn null_decimal_cell_len(column: &DirectColumnPlan, row_index: usize) -> Result<usize> {
    if !column.nullable() {
        return Err(value_conversion_error(row_column_diagnostic(
            column,
            row_index,
            DiagnosticCode::NullInNonNullableColumn,
            "null value in non-nullable direct decimal column",
        )));
    }

    Ok(NULL_DECIMAL_CELL_LEN)
}

fn write_null_direct_decimal_cell(
    bytes: &mut [u8],
    cell: &CellPosition,
    column: &DirectColumnPlan,
    row_index: usize,
) -> Result<()> {
    let expected_len = null_decimal_cell_len(column, row_index)?;
    if cell.len() != expected_len {
        return Err(invalid_payload(format!(
            "null decimal cell at row {} column {} has length {}, expected {expected_len}",
            cell.row_index(),
            cell.column_index(),
            cell.len()
        )));
    }

    let cell_bytes = cell_bytes_mut(bytes, cell)?;
    write_null_decimal_cell(cell_bytes)
}

fn write_direct_decimal_cell(
    bytes: &mut [u8],
    cell: &CellPosition,
    column: &DirectColumnPlan,
    unscaled: i128,
) -> Result<()> {
    let expected_len = decimal_cell_len(unscaled);
    if cell.len() != expected_len {
        return Err(invalid_payload(format!(
            "decimal cell at row {} column {} has length {}, expected {expected_len}",
            cell.row_index(),
            cell.column_index(),
            cell.len()
        )));
    }

    let cell_bytes = cell_bytes_mut(bytes, cell)?;
    write_decimal_cell(cell_bytes, unscaled).map_err(|err| add_decimal_field(err, column))
}

fn cell_bytes_mut<'a>(bytes: &'a mut [u8], cell: &CellPosition) -> Result<&'a mut [u8]> {
    let end = cell
        .offset()
        .checked_add(cell.len())
        .ok_or_else(|| invalid_payload("decimal cell end overflowed usize"))?;

    bytes
        .get_mut(cell.offset()..end)
        .ok_or_else(|| invalid_payload("decimal cell range is outside payload"))
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
                "runtime Arrow type {} does not match planned direct decimal column type",
                array.data_type()
            ),
        ))
    })
}

fn add_decimal_field(err: Error, column: &DirectColumnPlan) -> Error {
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

fn value_conversion_error(diagnostic: Diagnostic) -> Error {
    Error::ValueConversion {
        diagnostics: DiagnosticSet::from(vec![diagnostic]),
    }
}

fn write_decimal_value(dst: &mut [u8], unscaled: i128) -> Result<()> {
    let value_len = decimal_value_len(unscaled);
    dst[0] = value_len as u8;
    dst[1] = decimal_sign(unscaled);
    write_decimal_magnitude(&mut dst[2..], unscaled.unsigned_abs())
}

fn decimal_value_len(unscaled: i128) -> usize {
    DECIMAL_SIGN_LEN + decimal_magnitude_len(unscaled.unsigned_abs())
}

fn decimal_sign(unscaled: i128) -> u8 {
    if unscaled < 0 {
        DECIMAL_NEGATIVE_SIGN
    } else {
        DECIMAL_POSITIVE_SIGN
    }
}

fn decimal_magnitude_len(magnitude: u128) -> usize {
    match decimal_precision(magnitude) {
        1..=9 => 4,
        10..=19 => 8,
        20..=28 => 12,
        _ => 16,
    }
}

fn decimal_precision(mut magnitude: u128) -> u8 {
    let mut digits = 1;
    while magnitude >= 10 {
        magnitude /= 10;
        digits += 1;
    }
    digits
}

fn write_decimal_magnitude(dst: &mut [u8], magnitude: u128) -> Result<()> {
    match decimal_magnitude_len(magnitude) {
        4 => dst.copy_from_slice(&(magnitude as u32).to_le_bytes()),
        8 => dst.copy_from_slice(&(magnitude as u64).to_le_bytes()),
        12 => {
            dst[0..8].copy_from_slice(&(magnitude as u64).to_le_bytes());
            dst[8..12].copy_from_slice(&((magnitude >> 64) as u32).to_le_bytes());
        }
        16 => dst.copy_from_slice(&magnitude.to_le_bytes()),
        other => {
            return Err(invalid_payload(format!(
                "unsupported decimal magnitude length {other}"
            )));
        }
    }

    Ok(())
}

fn append_decimal_magnitude(
    buf: &mut tiberius::RawRowsAppendBuffer<'_>,
    magnitude: u128,
) -> Result<()> {
    match decimal_magnitude_len(magnitude) {
        4 => buf.put_u32_le(magnitude as u32),
        8 => buf.put_u64_le(magnitude as u64),
        12 => {
            buf.put_u64_le(magnitude as u64);
            buf.put_u32_le((magnitude >> 64) as u32);
        }
        16 => {
            buf.put_u64_le(magnitude as u64);
            buf.put_u64_le((magnitude >> 64) as u64);
        }
        other => {
            return Err(invalid_payload(format!(
                "unsupported decimal magnitude length {other}"
            )));
        }
    }

    Ok(())
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
    use super::{decimal_cell_len, write_decimal_cell, write_null_decimal_cell};

    #[test]
    fn writes_decimal_cells_with_tiberius_numeric_lengths() {
        let cases = [
            (0_i128, vec![5, 1, 0, 0, 0, 0]),
            (123_456_789, vec![5, 1, 0x15, 0xCD, 0x5B, 0x07]),
            (-123_456_789, vec![5, 0, 0x15, 0xCD, 0x5B, 0x07]),
            (
                9_223_372_036_854_775_808,
                vec![9, 1, 0, 0, 0, 0, 0, 0, 0, 0x80],
            ),
            (
                u64::MAX as i128,
                vec![
                    13, 1, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0, 0, 0, 0,
                ],
            ),
            (
                123_456_789_012_345_678_901_234_567_890_i128,
                vec![
                    17, 1, 0xD2, 0x0A, 0x3F, 0x4E, 0xEE, 0xE0, 0x73, 0xC3, 0xF6, 0x0F, 0xE9, 0x8E,
                    0x01, 0, 0, 0,
                ],
            ),
        ];

        for (unscaled, expected) in cases {
            let mut bytes = vec![0; decimal_cell_len(unscaled)];
            write_decimal_cell(&mut bytes, unscaled).unwrap();
            assert_eq!(bytes, expected);
        }
    }

    #[test]
    fn writes_null_decimal_cell_distinct_from_zero() {
        let mut null = vec![255];
        write_null_decimal_cell(&mut null).unwrap();

        let mut zero = vec![255; decimal_cell_len(0)];
        write_decimal_cell(&mut zero, 0).unwrap();

        assert_eq!(null, [0]);
        assert_eq!(zero, [5, 1, 0, 0, 0, 0]);
    }
}

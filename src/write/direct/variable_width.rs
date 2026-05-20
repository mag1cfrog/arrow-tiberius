//! Variable-width direct TDS row layout measurement.

use arrow_array::{Array, BinaryArray, StringArray};

use crate::{
    Diagnostic, DiagnosticCode, DiagnosticSet, Error, FieldRef, MssqlTypeLength, Result,
    conversion::arrow_to_mssql::variable_width::VariableWidthArrowToMssql,
};

use super::{
    layout::{CellPosition, RowLayout},
    plan::{DirectColumnEncoding, DirectColumnPlan},
};

const BOUNDED_LEN_PREFIX_LEN: usize = 2;
const PLP_LEN_PREFIX_LEN: usize = 8;
const PLP_CHUNK_LEN_PREFIX_LEN: usize = 4;
const PLP_TERMINATOR_LEN: usize = 4;
const MAX_BOUNDED_TDS_VALUE_LEN: usize = 0xfffe;
const MAX_PLP_CHUNK_LEN: usize = u32::MAX as usize;

/// Measures one variable-width Arrow column into a row-major cell length matrix.
///
/// Bounded SQL Server variable-width cells use a two-byte byte-length prefix.
/// `max` cells use PLP encoding: an eight-byte total-length marker followed by
/// one chunk length, the chunk bytes, and a terminator for non-empty values.
pub(crate) fn measure_variable_width_column_cell_lengths(
    array: &dyn Array,
    column: &DirectColumnPlan,
    column_index: usize,
    column_count: usize,
    cell_lengths: &mut [usize],
) -> Result<()> {
    match column.encoding() {
        DirectColumnEncoding::VariableWidth(VariableWidthArrowToMssql::Utf8ToNVarChar {
            length,
        }) => {
            let array = downcast_direct_array::<StringArray>(array, column)?;
            measure_nvarchar_cell_lengths(
                array,
                column,
                column_index,
                column_count,
                length,
                cell_lengths,
            )
        }
        DirectColumnEncoding::VariableWidth(VariableWidthArrowToMssql::BinaryToVarBinary {
            length,
        }) => {
            let array = downcast_direct_array::<BinaryArray>(array, column)?;
            measure_varbinary_cell_lengths(
                array,
                column,
                column_index,
                column_count,
                length,
                cell_lengths,
            )
        }
        DirectColumnEncoding::VariableWidth(other) => Err(unsupported_batch(format!(
            "direct variable-width layout is not implemented yet for {other:?}"
        ))),
        DirectColumnEncoding::Primitive(other) => Err(unsupported_batch(format!(
            "direct variable-width layout cannot measure primitive mapping {other:?}"
        ))),
    }
}

/// Fills one Utf8-to-nvarchar column into an already allocated rows payload.
pub(crate) fn fill_nvarchar_column(
    array: &StringArray,
    column: &DirectColumnPlan,
    column_index: usize,
    column_count: usize,
    layout: &RowLayout,
    bytes: &mut [u8],
) -> Result<()> {
    let length = match column.encoding() {
        DirectColumnEncoding::VariableWidth(VariableWidthArrowToMssql::Utf8ToNVarChar {
            length,
        }) => length,
        other => {
            return Err(unsupported_batch(format!(
                "direct nvarchar fill cannot encode mapping {other:?}"
            )));
        }
    };

    for row_index in 0..array.len() {
        let cell = cell_position(layout, row_index, column_index, column_count)?;

        if array.is_null(row_index) {
            write_null_cell(bytes, cell, column, row_index, length)?;
        } else {
            write_nvarchar_cell(
                bytes,
                cell,
                column,
                row_index,
                length,
                array.value(row_index),
            )?;
        }
    }

    Ok(())
}

/// Fills one Binary-to-varbinary column into an already allocated rows payload.
pub(crate) fn fill_varbinary_column(
    array: &BinaryArray,
    column: &DirectColumnPlan,
    column_index: usize,
    column_count: usize,
    layout: &RowLayout,
    bytes: &mut [u8],
) -> Result<()> {
    let length = match column.encoding() {
        DirectColumnEncoding::VariableWidth(VariableWidthArrowToMssql::BinaryToVarBinary {
            length,
        }) => length,
        other => {
            return Err(unsupported_batch(format!(
                "direct varbinary fill cannot encode mapping {other:?}"
            )));
        }
    };

    for row_index in 0..array.len() {
        let cell = cell_position(layout, row_index, column_index, column_count)?;

        if array.is_null(row_index) {
            write_null_cell(bytes, cell, column, row_index, length)?;
        } else {
            write_varbinary_cell(
                bytes,
                cell,
                column,
                row_index,
                length,
                array.value(row_index),
            )?;
        }
    }

    Ok(())
}

fn measure_nvarchar_cell_lengths(
    array: &StringArray,
    column: &DirectColumnPlan,
    column_index: usize,
    column_count: usize,
    length: MssqlTypeLength,
    cell_lengths: &mut [usize],
) -> Result<()> {
    for row_index in 0..array.len() {
        let cell_len = if array.is_null(row_index) {
            null_cell_len(column, row_index, length)?
        } else {
            let value = array.value(row_index);
            let code_units = value.encode_utf16().count();
            let encoded_bytes = checked_mul(code_units, 2)?;

            match length {
                MssqlTypeLength::Bounded(limit) => {
                    if code_units > limit {
                        return Err(value_too_long_error(
                            column,
                            row_index,
                            format!(
                                "string value has {code_units} UTF-16 code unit(s), exceeding planned {}",
                                column.target_type().to_sql()
                            ),
                        ));
                    }

                    bounded_cell_len(encoded_bytes)?
                }
                MssqlTypeLength::Max => plp_cell_len(encoded_bytes)?,
            }
        };

        cell_lengths[row_index * column_count + column_index] = cell_len;
    }

    Ok(())
}

fn measure_varbinary_cell_lengths(
    array: &BinaryArray,
    column: &DirectColumnPlan,
    column_index: usize,
    column_count: usize,
    length: MssqlTypeLength,
    cell_lengths: &mut [usize],
) -> Result<()> {
    for row_index in 0..array.len() {
        let cell_len = if array.is_null(row_index) {
            null_cell_len(column, row_index, length)?
        } else {
            let encoded_bytes = array.value(row_index).len();

            match length {
                MssqlTypeLength::Bounded(limit) => {
                    if encoded_bytes > limit {
                        return Err(value_too_long_error(
                            column,
                            row_index,
                            format!(
                                "binary value has {encoded_bytes} byte(s), exceeding planned {}",
                                column.target_type().to_sql()
                            ),
                        ));
                    }

                    bounded_cell_len(encoded_bytes)?
                }
                MssqlTypeLength::Max => plp_cell_len(encoded_bytes)?,
            }
        };

        cell_lengths[row_index * column_count + column_index] = cell_len;
    }

    Ok(())
}

fn null_cell_len(
    column: &DirectColumnPlan,
    row_index: usize,
    length: MssqlTypeLength,
) -> Result<usize> {
    if !column.nullable() {
        return Err(value_conversion_error(row_column_diagnostic(
            column,
            row_index,
            DiagnosticCode::NullInNonNullableColumn,
            "null value in non-nullable direct variable-width column",
        )));
    }

    Ok(match length {
        MssqlTypeLength::Bounded(_) => BOUNDED_LEN_PREFIX_LEN,
        MssqlTypeLength::Max => PLP_LEN_PREFIX_LEN,
    })
}

fn bounded_cell_len(encoded_bytes: usize) -> Result<usize> {
    if encoded_bytes > MAX_BOUNDED_TDS_VALUE_LEN {
        return Err(invalid_payload(format!(
            "bounded direct variable-width value length {encoded_bytes} exceeds TDS row limit {MAX_BOUNDED_TDS_VALUE_LEN}"
        )));
    }

    checked_add(BOUNDED_LEN_PREFIX_LEN, encoded_bytes)
}

fn plp_cell_len(encoded_bytes: usize) -> Result<usize> {
    if encoded_bytes > MAX_PLP_CHUNK_LEN {
        return Err(invalid_payload(format!(
            "direct variable-width PLP chunk length {encoded_bytes} exceeds u32::MAX"
        )));
    }

    let mut len = checked_add(PLP_LEN_PREFIX_LEN, PLP_CHUNK_LEN_PREFIX_LEN)?;
    len = checked_add(len, encoded_bytes)?;

    if encoded_bytes != 0 {
        len = checked_add(len, PLP_TERMINATOR_LEN)?;
    }

    Ok(len)
}

fn write_null_cell(
    bytes: &mut [u8],
    cell: &CellPosition,
    column: &DirectColumnPlan,
    row_index: usize,
    length: MssqlTypeLength,
) -> Result<()> {
    let expected_len = null_cell_len(column, row_index, length)?;
    if cell.len() != expected_len {
        return Err(invalid_payload(format!(
            "null variable-width cell at row {} column {} has length {}, expected {expected_len}",
            cell.row_index(),
            cell.column_index(),
            cell.len()
        )));
    }

    let cell_bytes = cell_bytes_mut(bytes, cell)?;
    match length {
        MssqlTypeLength::Bounded(_) => cell_bytes.copy_from_slice(&u16::MAX.to_le_bytes()),
        MssqlTypeLength::Max => cell_bytes.copy_from_slice(&u64::MAX.to_le_bytes()),
    }

    Ok(())
}

fn write_nvarchar_cell(
    bytes: &mut [u8],
    cell: &CellPosition,
    column: &DirectColumnPlan,
    row_index: usize,
    length: MssqlTypeLength,
    value: &str,
) -> Result<()> {
    match length {
        MssqlTypeLength::Bounded(limit) => {
            let encoded_bytes = bounded_nvarchar_encoded_bytes(cell)?;
            if encoded_bytes / 2 > limit {
                return Err(value_too_long_error(
                    column,
                    row_index,
                    format!(
                        "string value has {} UTF-16 code unit(s), exceeding planned {}",
                        encoded_bytes / 2,
                        column.target_type().to_sql()
                    ),
                ));
            }
            write_bounded_nvarchar_cell(bytes, cell, value, encoded_bytes)
        }
        MssqlTypeLength::Max => {
            let encoded_bytes = plp_nvarchar_encoded_bytes(cell)?;
            write_plp_nvarchar_cell(bytes, cell, value, encoded_bytes)
        }
    }
}

fn write_varbinary_cell(
    bytes: &mut [u8],
    cell: &CellPosition,
    column: &DirectColumnPlan,
    row_index: usize,
    length: MssqlTypeLength,
    value: &[u8],
) -> Result<()> {
    let encoded_bytes = value.len();

    match length {
        MssqlTypeLength::Bounded(limit) => {
            if encoded_bytes > limit {
                return Err(value_too_long_error(
                    column,
                    row_index,
                    format!(
                        "binary value has {encoded_bytes} byte(s), exceeding planned {}",
                        column.target_type().to_sql()
                    ),
                ));
            }
            write_bounded_payload_cell(bytes, cell, value)
        }
        MssqlTypeLength::Max => write_plp_payload_cell(bytes, cell, value),
    }
}

fn write_bounded_nvarchar_cell(
    bytes: &mut [u8],
    cell: &CellPosition,
    value: &str,
    encoded_bytes: usize,
) -> Result<()> {
    let expected_len = bounded_cell_len(encoded_bytes)?;
    if cell.len() != expected_len {
        return Err(invalid_payload(format!(
            "bounded nvarchar cell at row {} column {} has length {}, expected {expected_len}",
            cell.row_index(),
            cell.column_index(),
            cell.len()
        )));
    }

    let cell_bytes = cell_bytes_mut(bytes, cell)?;
    write_u16_prefix(cell_bytes, encoded_bytes)?;
    write_utf16le(&mut cell_bytes[BOUNDED_LEN_PREFIX_LEN..], value);

    Ok(())
}

fn write_bounded_payload_cell(bytes: &mut [u8], cell: &CellPosition, value: &[u8]) -> Result<()> {
    let expected_len = bounded_cell_len(value.len())?;
    if cell.len() != expected_len {
        return Err(invalid_payload(format!(
            "bounded varbinary cell at row {} column {} has length {}, expected {expected_len}",
            cell.row_index(),
            cell.column_index(),
            cell.len()
        )));
    }

    let cell_bytes = cell_bytes_mut(bytes, cell)?;
    write_u16_prefix(cell_bytes, value.len())?;
    cell_bytes[BOUNDED_LEN_PREFIX_LEN..].copy_from_slice(value);

    Ok(())
}

fn write_plp_nvarchar_cell(
    bytes: &mut [u8],
    cell: &CellPosition,
    value: &str,
    encoded_bytes: usize,
) -> Result<()> {
    let expected_len = plp_cell_len(encoded_bytes)?;
    if cell.len() != expected_len {
        return Err(invalid_payload(format!(
            "PLP nvarchar cell at row {} column {} has length {}, expected {expected_len}",
            cell.row_index(),
            cell.column_index(),
            cell.len()
        )));
    }

    let cell_bytes = cell_bytes_mut(bytes, cell)?;
    write_plp_header(cell_bytes, encoded_bytes)?;
    let payload_start = PLP_LEN_PREFIX_LEN + PLP_CHUNK_LEN_PREFIX_LEN;
    let payload_end = payload_start + encoded_bytes;
    write_utf16le(&mut cell_bytes[payload_start..payload_end], value);
    write_plp_terminator(cell_bytes, payload_end);

    Ok(())
}

fn write_plp_payload_cell(bytes: &mut [u8], cell: &CellPosition, value: &[u8]) -> Result<()> {
    let expected_len = plp_cell_len(value.len())?;
    if cell.len() != expected_len {
        return Err(invalid_payload(format!(
            "PLP varbinary cell at row {} column {} has length {}, expected {expected_len}",
            cell.row_index(),
            cell.column_index(),
            cell.len()
        )));
    }

    let cell_bytes = cell_bytes_mut(bytes, cell)?;
    write_plp_header(cell_bytes, value.len())?;
    let payload_start = PLP_LEN_PREFIX_LEN + PLP_CHUNK_LEN_PREFIX_LEN;
    let payload_end = payload_start + value.len();
    cell_bytes[payload_start..payload_end].copy_from_slice(value);
    write_plp_terminator(cell_bytes, payload_end);

    Ok(())
}

fn bounded_nvarchar_encoded_bytes(cell: &CellPosition) -> Result<usize> {
    let encoded_bytes = cell.len().checked_sub(BOUNDED_LEN_PREFIX_LEN).ok_or_else(|| {
        invalid_payload(format!(
            "bounded nvarchar cell at row {} column {} has length {}, shorter than prefix length {BOUNDED_LEN_PREFIX_LEN}",
            cell.row_index(),
            cell.column_index(),
            cell.len()
        ))
    })?;
    validate_utf16_byte_len(cell, encoded_bytes)?;
    Ok(encoded_bytes)
}

fn plp_nvarchar_encoded_bytes(cell: &CellPosition) -> Result<usize> {
    let minimum_non_null_len = PLP_LEN_PREFIX_LEN + PLP_CHUNK_LEN_PREFIX_LEN;
    if cell.len() == minimum_non_null_len {
        return Ok(0);
    }

    let non_empty_overhead = minimum_non_null_len + PLP_TERMINATOR_LEN;
    let encoded_bytes = cell.len().checked_sub(non_empty_overhead).ok_or_else(|| {
        invalid_payload(format!(
            "PLP nvarchar cell at row {} column {} has length {}, shorter than non-empty overhead {non_empty_overhead}",
            cell.row_index(),
            cell.column_index(),
            cell.len()
        ))
    })?;
    validate_utf16_byte_len(cell, encoded_bytes)?;
    Ok(encoded_bytes)
}

fn validate_utf16_byte_len(cell: &CellPosition, encoded_bytes: usize) -> Result<()> {
    if encoded_bytes.is_multiple_of(2) {
        return Ok(());
    }

    Err(invalid_payload(format!(
        "nvarchar cell at row {} column {} has odd UTF-16 byte length {encoded_bytes}",
        cell.row_index(),
        cell.column_index()
    )))
}

fn write_utf16le(dst: &mut [u8], value: &str) {
    for (chunk, code_unit) in dst.chunks_exact_mut(2).zip(value.encode_utf16()) {
        chunk.copy_from_slice(&code_unit.to_le_bytes());
    }
}

fn write_u16_prefix(dst: &mut [u8], len: usize) -> Result<()> {
    let len = u16::try_from(len)
        .map_err(|_| invalid_payload("bounded variable-width cell length does not fit u16"))?;
    dst[..BOUNDED_LEN_PREFIX_LEN].copy_from_slice(&len.to_le_bytes());
    Ok(())
}

fn write_plp_header(dst: &mut [u8], len: usize) -> Result<()> {
    if len > MAX_PLP_CHUNK_LEN {
        return Err(invalid_payload(format!(
            "direct variable-width PLP chunk length {len} exceeds u32::MAX"
        )));
    }

    dst[..PLP_LEN_PREFIX_LEN].copy_from_slice(&0xfffffffffffffffe_u64.to_le_bytes());
    dst[PLP_LEN_PREFIX_LEN..PLP_LEN_PREFIX_LEN + PLP_CHUNK_LEN_PREFIX_LEN]
        .copy_from_slice(&(len as u32).to_le_bytes());
    Ok(())
}

fn write_plp_terminator(dst: &mut [u8], offset: usize) {
    if offset < dst.len() {
        dst[offset..offset + PLP_TERMINATOR_LEN].copy_from_slice(&0u32.to_le_bytes());
    }
}

fn cell_bytes_mut<'a>(bytes: &'a mut [u8], cell: &CellPosition) -> Result<&'a mut [u8]> {
    let end = cell
        .offset()
        .checked_add(cell.len())
        .ok_or_else(|| invalid_payload("variable-width cell end overflowed usize"))?;

    // `offset..end` is a half-open byte range: it includes `offset` and stops
    // before `end`. For example, `1..3` selects bytes at indexes 1 and 2.
    bytes
        .get_mut(cell.offset()..end)
        .ok_or_else(|| invalid_payload("variable-width cell range is outside payload"))
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
                "runtime Arrow type {} does not match planned direct column type",
                array.data_type()
            ),
        ))
    })
}

fn checked_add(lhs: usize, rhs: usize) -> Result<usize> {
    lhs.checked_add(rhs)
        .ok_or_else(|| invalid_payload("direct variable-width row layout length overflowed usize"))
}

fn checked_mul(lhs: usize, rhs: usize) -> Result<usize> {
    lhs.checked_mul(rhs)
        .ok_or_else(|| invalid_payload("direct variable-width value length overflowed usize"))
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

fn value_too_long_error(
    column: &DirectColumnPlan,
    row_index: usize,
    message: impl Into<String>,
) -> Error {
    value_conversion_error(row_column_diagnostic(
        column,
        row_index,
        DiagnosticCode::ValueTooLong,
        message,
    ))
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
    use arrow_array::{Array, BinaryArray, StringArray};
    use arrow_schema::DataType;

    use crate::{
        ArrowFieldRef, DiagnosticCode, Error, Identifier, MssqlColumn, MssqlType, MssqlTypeLength,
        SchemaMapping,
        write::direct::layout::CellPosition,
        write::direct::plan::{CurrentDirectMappings, DirectEncoderPlan},
        write::direct::primitive::{
            allocate_rows_payload_with_tokens, build_fixed_width_row_layout,
        },
    };

    use super::{
        MAX_BOUNDED_TDS_VALUE_LEN, MAX_PLP_CHUNK_LEN, bounded_cell_len,
        bounded_nvarchar_encoded_bytes, fill_nvarchar_column, fill_varbinary_column,
        measure_variable_width_column_cell_lengths, plp_cell_len, plp_nvarchar_encoded_bytes,
    };

    #[test]
    fn measures_bounded_nvarchar_cells_by_encoded_utf16_bytes() {
        let array = StringArray::from(vec![Some("ab"), Some("🙂"), None]);
        let plan = plan(&[mapping(
            0,
            "text",
            DataType::Utf8,
            MssqlType::NVarChar(MssqlTypeLength::Bounded(2)),
            true,
        )]);
        let mut cell_lengths = vec![0; array.len()];

        measure_variable_width_column_cell_lengths(
            &array,
            &plan.columns()[0],
            0,
            1,
            &mut cell_lengths,
        )
        .unwrap();

        assert_eq!(cell_lengths, [6, 6, 2]);
    }

    #[test]
    fn measures_max_nvarchar_cells_as_plp() {
        let array = StringArray::from(vec![Some("a"), Some(""), None]);
        let plan = plan(&[mapping(
            0,
            "text",
            DataType::Utf8,
            MssqlType::NVarChar(MssqlTypeLength::Max),
            true,
        )]);
        let mut cell_lengths = vec![0; array.len()];

        measure_variable_width_column_cell_lengths(
            &array,
            &plan.columns()[0],
            0,
            1,
            &mut cell_lengths,
        )
        .unwrap();

        assert_eq!(cell_lengths, [18, 12, 8]);
    }

    #[test]
    fn measures_bounded_varbinary_cells_by_byte_count() {
        let array = BinaryArray::from_iter(vec![Some(&b"abc"[..]), Some(&b""[..]), None]);
        let plan = plan(&[mapping(
            0,
            "bytes",
            DataType::Binary,
            MssqlType::VarBinary(MssqlTypeLength::Bounded(3)),
            true,
        )]);
        let mut cell_lengths = vec![0; array.len()];

        measure_variable_width_column_cell_lengths(
            &array,
            &plan.columns()[0],
            0,
            1,
            &mut cell_lengths,
        )
        .unwrap();

        assert_eq!(cell_lengths, [5, 2, 2]);
    }

    #[test]
    fn measures_max_varbinary_cells_as_plp() {
        let array = BinaryArray::from_iter(vec![Some(&b"abc"[..]), Some(&b""[..]), None]);
        let plan = plan(&[mapping(
            0,
            "bytes",
            DataType::Binary,
            MssqlType::VarBinary(MssqlTypeLength::Max),
            true,
        )]);
        let mut cell_lengths = vec![0; array.len()];

        measure_variable_width_column_cell_lengths(
            &array,
            &plan.columns()[0],
            0,
            1,
            &mut cell_lengths,
        )
        .unwrap();

        assert_eq!(cell_lengths, [19, 12, 8]);
    }

    #[test]
    fn rejects_bounded_nvarchar_values_over_planned_code_units() {
        let array = StringArray::from(vec![Some("abc")]);
        let plan = plan(&[mapping(
            0,
            "text",
            DataType::Utf8,
            MssqlType::NVarChar(MssqlTypeLength::Bounded(2)),
            true,
        )]);
        let mut cell_lengths = vec![0; array.len()];

        let err = measure_variable_width_column_cell_lengths(
            &array,
            &plan.columns()[0],
            0,
            1,
            &mut cell_lengths,
        )
        .unwrap_err();

        assert_single_diagnostic(
            err,
            DiagnosticCode::ValueTooLong,
            Some(0),
            Some((0, "text")),
        );
    }

    #[test]
    fn rejects_bounded_varbinary_values_over_planned_bytes() {
        let array = BinaryArray::from_iter(vec![Some(&b"abcd"[..])]);
        let plan = plan(&[mapping(
            0,
            "bytes",
            DataType::Binary,
            MssqlType::VarBinary(MssqlTypeLength::Bounded(3)),
            true,
        )]);
        let mut cell_lengths = vec![0; array.len()];

        let err = measure_variable_width_column_cell_lengths(
            &array,
            &plan.columns()[0],
            0,
            1,
            &mut cell_lengths,
        )
        .unwrap_err();

        assert_single_diagnostic(
            err,
            DiagnosticCode::ValueTooLong,
            Some(0),
            Some((0, "bytes")),
        );
    }

    #[test]
    fn rejects_null_in_non_nullable_variable_width_column() {
        let array = StringArray::from(vec![None::<&str>]);
        let plan = plan(&[mapping(
            0,
            "text",
            DataType::Utf8,
            MssqlType::NVarChar(MssqlTypeLength::Max),
            false,
        )]);
        let mut cell_lengths = vec![0; array.len()];

        let err = measure_variable_width_column_cell_lengths(
            &array,
            &plan.columns()[0],
            0,
            1,
            &mut cell_lengths,
        )
        .unwrap_err();

        assert_single_diagnostic(
            err,
            DiagnosticCode::NullInNonNullableColumn,
            Some(0),
            Some((0, "text")),
        );
    }

    #[test]
    fn rejects_bounded_cell_lengths_that_collide_with_tds_null_sentinel() {
        let err = bounded_cell_len(MAX_BOUNDED_TDS_VALUE_LEN + 1).unwrap_err();

        assert_direct_encoding_diagnostic(err, DiagnosticCode::DirectEncodingInvalidPayload);
    }

    #[test]
    fn rejects_plp_chunk_lengths_that_do_not_fit_u32() {
        let err = plp_cell_len(MAX_PLP_CHUNK_LEN + 1).unwrap_err();

        assert_direct_encoding_diagnostic(err, DiagnosticCode::DirectEncodingInvalidPayload);
    }

    #[test]
    fn derives_nvarchar_fill_lengths_from_measured_cell_lengths() {
        let bounded = CellPosition::new(0, 0, 1, 6);
        let max_empty = CellPosition::new(0, 0, 1, 12);
        let max_non_empty = CellPosition::new(0, 0, 1, 18);

        assert_eq!(bounded_nvarchar_encoded_bytes(&bounded).unwrap(), 4);
        assert_eq!(plp_nvarchar_encoded_bytes(&max_empty).unwrap(), 0);
        assert_eq!(plp_nvarchar_encoded_bytes(&max_non_empty).unwrap(), 2);
    }

    #[test]
    fn rejects_malformed_odd_nvarchar_fill_lengths() {
        let bounded = CellPosition::new(0, 0, 1, 5);
        let max = CellPosition::new(0, 0, 1, 17);

        assert_direct_encoding_diagnostic(
            bounded_nvarchar_encoded_bytes(&bounded).unwrap_err(),
            DiagnosticCode::DirectEncodingInvalidPayload,
        );
        assert_direct_encoding_diagnostic(
            plp_nvarchar_encoded_bytes(&max).unwrap_err(),
            DiagnosticCode::DirectEncodingInvalidPayload,
        );
    }

    #[test]
    fn fills_bounded_nvarchar_cells_as_utf16le_with_null_sentinel() {
        let array = StringArray::from(vec![Some("ab"), Some("🙂"), None]);
        let plan = plan(&[mapping(
            0,
            "text",
            DataType::Utf8,
            MssqlType::NVarChar(MssqlTypeLength::Bounded(2)),
            true,
        )]);
        let layout = build_fixed_width_row_layout(3, 1, &[6, 6, 2]).unwrap();
        let mut bytes = allocate_rows_payload_with_tokens(&layout);

        fill_nvarchar_column(&array, &plan.columns()[0], 0, 1, &layout, &mut bytes).unwrap();

        assert_eq!(
            bytes,
            [
                0xd1, 4, 0, b'a', 0, b'b', 0, 0xd1, 4, 0, 0x3d, 0xd8, 0x42, 0xde, 0xd1, 0xff, 0xff,
            ]
        );
    }

    #[test]
    fn fills_max_nvarchar_cells_as_single_chunk_plp() {
        let array = StringArray::from(vec![Some("a"), Some(""), None]);
        let plan = plan(&[mapping(
            0,
            "text",
            DataType::Utf8,
            MssqlType::NVarChar(MssqlTypeLength::Max),
            true,
        )]);
        let layout = build_fixed_width_row_layout(3, 1, &[18, 12, 8]).unwrap();
        let mut bytes = allocate_rows_payload_with_tokens(&layout);

        fill_nvarchar_column(&array, &plan.columns()[0], 0, 1, &layout, &mut bytes).unwrap();

        assert_eq!(
            bytes,
            [
                0xd1, 0xfe, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 2, 0, 0, 0, b'a', 0, 0, 0, 0,
                0, 0xd1, 0xfe, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0, 0, 0, 0, 0xd1, 0xff,
                0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
            ]
        );
    }

    #[test]
    fn fills_bounded_varbinary_cells_with_null_sentinel() {
        let array = BinaryArray::from_iter(vec![Some(&b"abc"[..]), Some(&b""[..]), None]);
        let plan = plan(&[mapping(
            0,
            "bytes",
            DataType::Binary,
            MssqlType::VarBinary(MssqlTypeLength::Bounded(3)),
            true,
        )]);
        let layout = build_fixed_width_row_layout(3, 1, &[5, 2, 2]).unwrap();
        let mut bytes = allocate_rows_payload_with_tokens(&layout);

        fill_varbinary_column(&array, &plan.columns()[0], 0, 1, &layout, &mut bytes).unwrap();

        assert_eq!(
            bytes,
            [0xd1, 3, 0, b'a', b'b', b'c', 0xd1, 0, 0, 0xd1, 0xff, 0xff,]
        );
    }

    #[test]
    fn fills_max_varbinary_cells_as_single_chunk_plp() {
        let array = BinaryArray::from_iter(vec![Some(&b"abc"[..]), Some(&b""[..]), None]);
        let plan = plan(&[mapping(
            0,
            "bytes",
            DataType::Binary,
            MssqlType::VarBinary(MssqlTypeLength::Max),
            true,
        )]);
        let layout = build_fixed_width_row_layout(3, 1, &[19, 12, 8]).unwrap();
        let mut bytes = allocate_rows_payload_with_tokens(&layout);

        fill_varbinary_column(&array, &plan.columns()[0], 0, 1, &layout, &mut bytes).unwrap();

        assert_eq!(
            bytes,
            [
                0xd1, 0xfe, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 3, 0, 0, 0, b'a', b'b', b'c',
                0, 0, 0, 0, 0xd1, 0xfe, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0, 0, 0, 0, 0xd1,
                0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
            ]
        );
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

    fn plan(mappings: &[SchemaMapping]) -> DirectEncoderPlan {
        DirectEncoderPlan::new(mappings, &CurrentDirectMappings).unwrap()
    }

    fn assert_single_diagnostic(
        err: crate::Error,
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

    fn assert_direct_encoding_diagnostic(err: crate::Error, expected_code: DiagnosticCode) {
        let Error::DirectEncoding { diagnostics } = err else {
            panic!("expected direct encoding error");
        };

        assert_eq!(diagnostics.len(), 1);
        assert_eq!(diagnostics.all()[0].code(), expected_code);
    }
}

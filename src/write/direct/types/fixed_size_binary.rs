//! Fixed-size binary direct TDS row payload helpers.

use arrow_array::{Array, FixedSizeBinaryArray};

use crate::{
    Diagnostic, DiagnosticCode, DiagnosticSet, Error, FieldRef, Result,
    conversion::arrow_to_mssql::fixed_size_binary::FixedSizeBinaryArrowToMssql, write::profile,
};

use super::super::{
    layout::{CellPosition, RowLayout},
    plan::DirectColumnPlan,
};

const LEN_PREFIX_LEN: usize = 2;

pub(crate) fn measure_fixed_size_binary_column_cell_lengths(
    array: &FixedSizeBinaryArray,
    column: &DirectColumnPlan,
    classification: FixedSizeBinaryArrowToMssql,
    column_index: usize,
    column_count: usize,
    cell_lengths: &mut [usize],
) -> Result<()> {
    let length = binary_length(classification);

    for row_index in 0..array.len() {
        let cell_len = if array.is_null(row_index) {
            null_cell_len(column, row_index)?
        } else {
            validate_binary_value_len(array.value(row_index), column, row_index, length)?;
            non_null_cell_len(length)?
        };

        cell_lengths[row_index * column_count + column_index] = cell_len;
    }

    Ok(())
}

pub(crate) fn fill_fixed_size_binary_column(
    array: &FixedSizeBinaryArray,
    column: &DirectColumnPlan,
    classification: FixedSizeBinaryArrowToMssql,
    column_index: usize,
    column_count: usize,
    layout: &RowLayout,
    bytes: &mut [u8],
) -> Result<()> {
    let length = binary_length(classification);

    for row_index in 0..array.len() {
        let cell = cell_position(layout, row_index, column_index, column_count)?;

        if array.is_null(row_index) {
            write_null_cell(bytes, cell, column, row_index)?;
        } else {
            write_binary_cell(
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

pub(crate) fn append_fixed_size_binary_cell(
    buf: &mut tiberius::RawRowsAppendBuffer<'_>,
    array: &FixedSizeBinaryArray,
    column: &DirectColumnPlan,
    classification: FixedSizeBinaryArrowToMssql,
    row_index: usize,
    measured_len: usize,
) -> Result<()> {
    let length = binary_length(classification);

    if array.is_null(row_index) {
        let expected_len = null_cell_len(column, row_index)?;
        if measured_len != expected_len {
            return Err(invalid_payload(format!(
                "measured null fixed-size binary cell at row {row_index} column {} has length {}, expected {expected_len}",
                column.source_name(),
                measured_len
            )));
        }

        profile::record_null_cell();
        buf.put_u16_le(u16::MAX);
        return Ok(());
    }

    let value = array.value(row_index);
    validate_binary_value_len(value, column, row_index, length)?;
    let expected_len = non_null_cell_len(length)?;
    if measured_len != expected_len {
        return Err(invalid_payload(format!(
            "measured fixed-size binary cell at row {row_index} column {} has length {}, expected {expected_len}",
            column.source_name(),
            measured_len
        )));
    }

    profile::record_varbinary_bytes(value.len());
    write_len_prefix_to_buffer(buf, length)?;
    buf.extend_from_slice(value);
    Ok(())
}

fn binary_length(classification: FixedSizeBinaryArrowToMssql) -> usize {
    match classification {
        FixedSizeBinaryArrowToMssql::FixedSizeBinaryToBinary { length } => length,
    }
}

fn null_cell_len(column: &DirectColumnPlan, row_index: usize) -> Result<usize> {
    if column.nullable() {
        return Ok(LEN_PREFIX_LEN);
    }

    Err(value_conversion_error(row_column_diagnostic(
        column,
        row_index,
        DiagnosticCode::NullInNonNullableColumn,
        "null value in non-nullable direct fixed-size binary column",
    )))
}

fn non_null_cell_len(length: usize) -> Result<usize> {
    checked_add(LEN_PREFIX_LEN, length)
}

fn validate_binary_value_len(
    value: &[u8],
    column: &DirectColumnPlan,
    row_index: usize,
    expected_len: usize,
) -> Result<()> {
    if value.len() == expected_len {
        return Ok(());
    }

    Err(value_conversion_error(row_column_diagnostic(
        column,
        row_index,
        DiagnosticCode::ValueTypeMismatch,
        format!(
            "fixed-size binary value has {} byte(s), but planned {} requires exactly {expected_len}",
            value.len(),
            column.target_type().to_sql()
        ),
    )))
}

fn write_null_cell(
    bytes: &mut [u8],
    cell: &CellPosition,
    column: &DirectColumnPlan,
    row_index: usize,
) -> Result<()> {
    let expected_len = null_cell_len(column, row_index)?;
    if cell.len() != expected_len {
        return Err(invalid_payload(format!(
            "null fixed-size binary cell at row {} column {} has length {}, expected {expected_len}",
            cell.row_index(),
            cell.column_index(),
            cell.len()
        )));
    }

    profile::record_null_cell();
    let cell_bytes = cell_bytes_mut(bytes, cell)?;
    cell_bytes.copy_from_slice(&u16::MAX.to_le_bytes());
    Ok(())
}

fn write_binary_cell(
    bytes: &mut [u8],
    cell: &CellPosition,
    column: &DirectColumnPlan,
    row_index: usize,
    expected_value_len: usize,
    value: &[u8],
) -> Result<()> {
    validate_binary_value_len(value, column, row_index, expected_value_len)?;

    let expected_cell_len = non_null_cell_len(expected_value_len)?;
    if cell.len() != expected_cell_len {
        return Err(invalid_payload(format!(
            "fixed-size binary cell at row {} column {} has length {}, expected {expected_cell_len}",
            cell.row_index(),
            cell.column_index(),
            cell.len()
        )));
    }

    profile::record_varbinary_bytes(value.len());
    let cell_bytes = cell_bytes_mut(bytes, cell)?;
    write_len_prefix(cell_bytes, expected_value_len)?;
    cell_bytes[LEN_PREFIX_LEN..].copy_from_slice(value);
    Ok(())
}

fn write_len_prefix_to_buffer(
    buf: &mut tiberius::RawRowsAppendBuffer<'_>,
    length: usize,
) -> Result<()> {
    let length = u16::try_from(length)
        .map_err(|_| invalid_payload("fixed-size binary length does not fit u16"))?;
    buf.put_u16_le(length);
    Ok(())
}

fn write_len_prefix(bytes: &mut [u8], length: usize) -> Result<()> {
    let length = u16::try_from(length)
        .map_err(|_| invalid_payload("fixed-size binary length does not fit u16"))?;
    bytes[..LEN_PREFIX_LEN].copy_from_slice(&length.to_le_bytes());
    Ok(())
}

fn cell_bytes_mut<'a>(bytes: &'a mut [u8], cell: &CellPosition) -> Result<&'a mut [u8]> {
    let start = cell.offset();
    let end = start
        .checked_add(cell.len())
        .ok_or_else(|| invalid_payload("fixed-size binary cell end overflowed usize"))?;

    bytes
        .get_mut(start..end)
        .ok_or_else(|| invalid_payload("fixed-size binary cell position is outside payload"))
}

fn cell_position(
    layout: &RowLayout,
    row_index: usize,
    column_index: usize,
    column_count: usize,
) -> Result<&CellPosition> {
    let position = row_index
        .checked_mul(column_count)
        .and_then(|offset| offset.checked_add(column_index))
        .ok_or_else(|| invalid_payload("fixed-size binary layout cell index overflowed usize"))?;

    layout.cell_positions().get(position).ok_or_else(|| {
        invalid_payload(format!(
            "fixed-size binary cell position missing for row {row_index} column {column_index}"
        ))
    })
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

fn checked_add(lhs: usize, rhs: usize) -> Result<usize> {
    lhs.checked_add(rhs).ok_or_else(|| {
        invalid_payload(format!(
            "direct fixed-size binary row payload length overflow while adding {lhs} and {rhs}"
        ))
    })
}

#[cfg(test)]
mod tests {
    use arrow_array::FixedSizeBinaryArray;
    use arrow_schema::DataType;

    use super::*;
    use crate::{
        ArrowFieldRef, Identifier, MssqlColumn, MssqlType, SchemaMapping,
        write::direct::layout::{allocate_rows_payload_with_tokens, build_fixed_width_row_layout},
        write::direct::plan::DirectColumnEncoding,
    };

    #[test]
    fn measures_fixed_size_binary_cells_with_null_sentinel() {
        let array = fixed_array([Some(&b"abc"[..]), None, Some(&b"\x00\xff\x7f"[..])], 3);
        let column = column(true, 3);
        let mut lengths = vec![0; 3];

        measure_fixed_size_binary_column_cell_lengths(
            &array,
            &column,
            classification(3),
            0,
            1,
            &mut lengths,
        )
        .unwrap();

        assert_eq!(lengths, [5, 2, 5]);
    }

    #[test]
    fn fills_fixed_size_binary_cells_with_lengths_and_null_sentinel() {
        let array = fixed_array([Some(&b"abc"[..]), None, Some(&b"\x00\xff\x7f"[..])], 3);
        let column = column(true, 3);
        let layout = build_fixed_width_row_layout(3, 1, &[5, 2, 5]).unwrap();
        let mut bytes = allocate_rows_payload_with_tokens(&layout);

        fill_fixed_size_binary_column(
            &array,
            &column,
            classification(3),
            0,
            1,
            &layout,
            &mut bytes,
        )
        .unwrap();

        assert_eq!(
            bytes,
            [
                0xD1, 3, 0, b'a', b'b', b'c', 0xD1, 0xff, 0xff, 0xD1, 3, 0, 0, 0xff, 0x7f
            ]
        );
    }

    #[test]
    fn rejects_fixed_size_binary_null_in_non_nullable_column() {
        let array = fixed_array([Some(&b"abc"[..]), None], 3);
        let column = column(false, 3);
        let mut lengths = vec![0; 2];

        let err = measure_fixed_size_binary_column_cell_lengths(
            &array,
            &column,
            classification(3),
            0,
            1,
            &mut lengths,
        )
        .unwrap_err();

        assert_single_diagnostic(
            err,
            DiagnosticCode::NullInNonNullableColumn,
            Some(1),
            Some((0, "digest")),
        );
    }

    fn fixed_array<const N: usize>(values: [Option<&[u8]>; N], width: i32) -> FixedSizeBinaryArray {
        FixedSizeBinaryArray::try_from_sparse_iter_with_size(values.into_iter(), width).unwrap()
    }

    fn column(nullable: bool, length: usize) -> DirectColumnPlan {
        DirectColumnPlan::from_mapping(
            &SchemaMapping::new(
                ArrowFieldRef::new(
                    0,
                    "digest".to_owned(),
                    nullable,
                    DataType::FixedSizeBinary(i32::try_from(length).unwrap()),
                ),
                MssqlColumn::new(
                    Identifier::new("digest").unwrap(),
                    MssqlType::Binary(length),
                    nullable,
                ),
            ),
            DirectColumnEncoding::FixedSizeBinary(classification(length)),
        )
    }

    fn classification(length: usize) -> FixedSizeBinaryArrowToMssql {
        FixedSizeBinaryArrowToMssql::FixedSizeBinaryToBinary { length }
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

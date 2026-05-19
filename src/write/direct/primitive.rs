//! Fixed-width primitive direct TDS row layout.

use arrow_array::{Array, BooleanArray};

use crate::{
    Diagnostic, DiagnosticCode, DiagnosticSet, Error, FieldRef, Result,
    conversion::arrow_to_mssql::primitive::PrimitiveArrowToMssql,
};

use super::{
    layout::{CellPosition, RowLayout},
    payload::TDS_ROW_TOKEN,
    plan::{DirectColumnEncoding, DirectColumnPlan},
};

const ROW_TOKEN_LEN: usize = 1;
const CELL_LEN_PREFIX_LEN: usize = 1;

/// Measures one primitive Arrow column into a row-major cell length matrix.
///
/// Cell lengths include the TDS length byte for nullable fixed-width primitive
/// encodings. Non-null values occupy `1 + value_width` bytes, while null values
/// occupy only the one zero length byte.
pub(crate) fn measure_primitive_column_cell_lengths(
    array: &dyn Array,
    column: &DirectColumnPlan,
    column_index: usize,
    column_count: usize,
    cell_lengths: &mut [usize],
) -> Result<()> {
    let value_len = primitive_value_len(column.encoding())?;

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
        } else {
            CELL_LEN_PREFIX_LEN + value_len
        };

        cell_lengths[row_index * column_count + column_index] = cell_len;
    }

    Ok(())
}

pub(crate) fn build_fixed_width_row_layout(
    row_count: usize,
    column_count: usize,
    cell_lengths: &[usize],
) -> Result<RowLayout> {
    let mut row_token_offsets = Vec::with_capacity(row_count);
    let mut row_lengths = Vec::with_capacity(row_count);
    let mut cell_positions = Vec::with_capacity(cell_lengths.len());
    let mut offset = 0usize;

    for row_index in 0..row_count {
        let row_offset = offset;
        row_token_offsets.push(row_offset);
        offset = checked_add(offset, ROW_TOKEN_LEN)?;

        for column_index in 0..column_count {
            let cell_len = cell_lengths[row_index * column_count + column_index];
            cell_positions.push(CellPosition::new(row_index, column_index, offset, cell_len));
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
            write_fixed_width_cell(bytes, cell, &[u8::from(array.value(row_index))])?;
        }
    }

    Ok(())
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

fn write_fixed_width_cell(bytes: &mut [u8], cell: &CellPosition, value: &[u8]) -> Result<()> {
    let expected_len = CELL_LEN_PREFIX_LEN
        .checked_add(value.len())
        .ok_or_else(|| invalid_payload("fixed-width cell length overflowed usize"))?;

    if cell.len() != expected_len {
        return Err(invalid_payload(format!(
            "fixed-width cell at row {} column {} has length {}, expected {expected_len}",
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
        .map_err(|_| invalid_payload("fixed-width cell value length does not fit u8"))?;
    cell_bytes[1..].copy_from_slice(value);

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
        measure_primitive_column_cell_lengths,
    };
    use crate::write::direct::payload::TDS_ROW_TOKEN;
    use crate::write::direct::plan::{DirectEncoderPlan, PrimitiveDirectMappings};

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

        assert_eq!(cell_lengths, [2, 5, 9, 9, 2, 5, 9, 9]);

        let layout = build_fixed_width_row_layout(row_count, column_count, &cell_lengths).unwrap();

        assert_eq!(layout.row_token_offsets(), [0, 26]);
        assert_eq!(layout.row_lengths(), [26, 26]);
        assert_eq!(layout.payload_len(), 52);
        assert_cell_positions(
            layout.cell_positions(),
            &[
                (0, 0, 1, 2),
                (0, 1, 3, 5),
                (0, 2, 8, 9),
                (0, 3, 17, 9),
                (1, 0, 27, 2),
                (1, 1, 29, 5),
                (1, 2, 34, 9),
                (1, 3, 43, 9),
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

        assert_eq!(bytes, [TDS_ROW_TOKEN, 1, 1, TDS_ROW_TOKEN, 1, 0]);
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
        DirectEncoderPlan::new(mappings, &PrimitiveDirectMappings).unwrap()
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

//! Primitive Arrow-to-MSSQL runtime cell conversion.

use crate::{DiagnosticCode, Result, SchemaMapping, arrow::cell::ArrowCell};

use super::{non_finite_float_error, row_mapping_diagnostic, value_conversion_error};

pub(super) fn mssql_bit_value(
    mapping: &SchemaMapping,
    row_index: usize,
    cell: ArrowCell<'_>,
) -> Result<bool> {
    match cell {
        ArrowCell::Boolean(value) => Ok(value),
        other => Err(value_conversion_error(row_mapping_diagnostic(
            mapping,
            row_index,
            DiagnosticCode::ValueTypeMismatch,
            format!("expected Arrow boolean payload, got {other:?}"),
        ))),
    }
}

pub(super) fn mssql_tinyint_value(
    mapping: &SchemaMapping,
    row_index: usize,
    cell: ArrowCell<'_>,
) -> Result<u8> {
    match cell {
        ArrowCell::UInt8(value) => Ok(value),
        other => Err(value_conversion_error(row_mapping_diagnostic(
            mapping,
            row_index,
            DiagnosticCode::ValueTypeMismatch,
            format!("expected Arrow UInt8 payload, got {other:?}"),
        ))),
    }
}

pub(super) fn mssql_smallint_value(
    mapping: &SchemaMapping,
    row_index: usize,
    cell: ArrowCell<'_>,
) -> Result<i16> {
    match cell {
        ArrowCell::Int8(value) => Ok(i16::from(value)),
        ArrowCell::Int16(value) => Ok(value),
        other => Err(value_conversion_error(row_mapping_diagnostic(
            mapping,
            row_index,
            DiagnosticCode::ValueTypeMismatch,
            format!("expected Arrow Int8 or Int16 payload, got {other:?}"),
        ))),
    }
}

pub(super) fn mssql_int_value(
    mapping: &SchemaMapping,
    row_index: usize,
    cell: ArrowCell<'_>,
) -> Result<i32> {
    match cell {
        ArrowCell::Int32(value) => Ok(value),
        ArrowCell::UInt16(value) => Ok(i32::from(value)),
        other => Err(value_conversion_error(row_mapping_diagnostic(
            mapping,
            row_index,
            DiagnosticCode::ValueTypeMismatch,
            format!("expected Arrow Int32 or UInt16 payload, got {other:?}"),
        ))),
    }
}

pub(super) fn mssql_bigint_value(
    mapping: &SchemaMapping,
    row_index: usize,
    cell: ArrowCell<'_>,
) -> Result<i64> {
    match cell {
        ArrowCell::Int64(value) => Ok(value),
        ArrowCell::UInt32(value) => Ok(i64::from(value)),
        ArrowCell::UInt64(value) => i64::try_from(value).map_err(|_| {
            value_conversion_error(row_mapping_diagnostic(
                mapping,
                row_index,
                DiagnosticCode::IntegerOutOfRange,
                format!("Arrow UInt64 value {value} does not fit planned SQL Server bigint"),
            ))
        }),
        other => Err(value_conversion_error(row_mapping_diagnostic(
            mapping,
            row_index,
            DiagnosticCode::ValueTypeMismatch,
            format!("expected Arrow Int64, UInt32, or UInt64 payload, got {other:?}"),
        ))),
    }
}

pub(super) fn mssql_real_value(
    mapping: &SchemaMapping,
    row_index: usize,
    cell: ArrowCell<'_>,
) -> Result<f32> {
    match cell {
        ArrowCell::Float32(value) if value.is_finite() => Ok(value),
        ArrowCell::Float32(value) => Err(non_finite_float_error(mapping, row_index, value)),
        other => Err(value_conversion_error(row_mapping_diagnostic(
            mapping,
            row_index,
            DiagnosticCode::ValueTypeMismatch,
            format!("expected Arrow Float32 payload, got {other:?}"),
        ))),
    }
}

pub(super) fn mssql_float_value(
    mapping: &SchemaMapping,
    row_index: usize,
    cell: ArrowCell<'_>,
) -> Result<f64> {
    match cell {
        ArrowCell::Float64(value) if value.is_finite() => Ok(value),
        ArrowCell::Float64(value) => Err(non_finite_float_error(mapping, row_index, value)),
        other => Err(value_conversion_error(row_mapping_diagnostic(
            mapping,
            row_index,
            DiagnosticCode::ValueTypeMismatch,
            format!("expected Arrow Float64 payload, got {other:?}"),
        ))),
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_schema::{DataType, Field, Schema};

    use super::super::{ArrowToMssqlRuntimeMapping, mssql_cell_from_arrow_cell};
    use crate::{
        ArrowFieldRef, DiagnosticCode, Identifier, MssqlColumn, MssqlProfile, MssqlType,
        PlanOptions, SchemaMapping, arrow::cell::ArrowCell, mssql::cell::MssqlCell,
        plan_arrow_schema_to_mssql_mappings,
    };

    #[test]
    fn converts_supported_initial_primitives_to_mssql_cells() {
        let mappings = mappings_for_schema(Schema::new(vec![
            Field::new("active", DataType::Boolean, true),
            Field::new("tiny", DataType::Int8, true),
            Field::new("small", DataType::Int16, true),
            Field::new("quantity", DataType::Int32, true),
            Field::new("total", DataType::Int64, true),
            Field::new("unsigned_tiny", DataType::UInt8, true),
            Field::new("unsigned_medium", DataType::UInt16, true),
            Field::new("unsigned_large", DataType::UInt32, true),
            Field::new("real_value", DataType::Float32, true),
            Field::new("float_value", DataType::Float64, true),
            Field::new("text", DataType::Utf8, true),
            Field::new("large_text", DataType::LargeUtf8, true),
            Field::new("bytes", DataType::Binary, true),
            Field::new("large_bytes", DataType::LargeBinary, true),
        ]));
        let cases = [
            (0, ArrowCell::Boolean(true), MssqlCell::Bit(Some(true))),
            (1, ArrowCell::Int8(-8), MssqlCell::SmallInt(Some(-8))),
            (2, ArrowCell::Int16(-16), MssqlCell::SmallInt(Some(-16))),
            (3, ArrowCell::Int32(12), MssqlCell::Int(Some(12))),
            (4, ArrowCell::Int64(34), MssqlCell::BigInt(Some(34))),
            (5, ArrowCell::UInt8(8), MssqlCell::TinyInt(Some(8))),
            (6, ArrowCell::UInt16(16), MssqlCell::Int(Some(16))),
            (7, ArrowCell::UInt32(32), MssqlCell::BigInt(Some(32))),
            (8, ArrowCell::Float32(1.25), MssqlCell::Real(Some(1.25))),
            (9, ArrowCell::Float64(2.5), MssqlCell::Float(Some(2.5))),
            (
                10,
                ArrowCell::Utf8("hello"),
                MssqlCell::NVarChar(Some("hello")),
            ),
            (
                11,
                ArrowCell::Utf8("Tokyo"),
                MssqlCell::NVarChar(Some("Tokyo")),
            ),
            (
                12,
                ArrowCell::Binary(b"abc"),
                MssqlCell::VarBinary(Some(b"abc")),
            ),
            (
                13,
                ArrowCell::Binary(b"large"),
                MssqlCell::VarBinary(Some(b"large")),
            ),
        ];

        for (index, arrow_cell, expected) in cases {
            assert_eq!(
                convert_cell(&mappings[index], arrow_cell, 0).unwrap(),
                expected
            );
        }

        let null_cases = [
            (0, MssqlCell::Bit(None)),
            (1, MssqlCell::SmallInt(None)),
            (2, MssqlCell::SmallInt(None)),
            (3, MssqlCell::Int(None)),
            (4, MssqlCell::BigInt(None)),
            (5, MssqlCell::TinyInt(None)),
            (6, MssqlCell::Int(None)),
            (7, MssqlCell::BigInt(None)),
            (8, MssqlCell::Real(None)),
            (9, MssqlCell::Float(None)),
            (10, MssqlCell::NVarChar(None)),
            (11, MssqlCell::NVarChar(None)),
            (12, MssqlCell::VarBinary(None)),
            (13, MssqlCell::VarBinary(None)),
        ];

        for (index, expected) in null_cases {
            assert_eq!(
                convert_cell(&mappings[index], ArrowCell::Null, 1).unwrap(),
                expected
            );
        }
    }

    #[test]
    fn preserves_integer_boundaries_during_widening() {
        let mappings = mappings_for_schema(Schema::new(vec![
            Field::new("tiny", DataType::Int8, false),
            Field::new("small", DataType::Int16, false),
            Field::new("unsigned_tiny", DataType::UInt8, false),
            Field::new("unsigned_medium", DataType::UInt16, false),
            Field::new("unsigned_large", DataType::UInt32, false),
        ]));
        let cases = [
            (
                0,
                ArrowCell::Int8(i8::MIN),
                MssqlCell::SmallInt(Some(i16::from(i8::MIN))),
            ),
            (
                0,
                ArrowCell::Int8(i8::MAX),
                MssqlCell::SmallInt(Some(i16::from(i8::MAX))),
            ),
            (
                1,
                ArrowCell::Int16(i16::MIN),
                MssqlCell::SmallInt(Some(i16::MIN)),
            ),
            (
                1,
                ArrowCell::Int16(i16::MAX),
                MssqlCell::SmallInt(Some(i16::MAX)),
            ),
            (
                2,
                ArrowCell::UInt8(u8::MIN),
                MssqlCell::TinyInt(Some(u8::MIN)),
            ),
            (
                2,
                ArrowCell::UInt8(u8::MAX),
                MssqlCell::TinyInt(Some(u8::MAX)),
            ),
            (
                3,
                ArrowCell::UInt16(u16::MIN),
                MssqlCell::Int(Some(i32::from(u16::MIN))),
            ),
            (
                3,
                ArrowCell::UInt16(u16::MAX),
                MssqlCell::Int(Some(i32::from(u16::MAX))),
            ),
            (
                4,
                ArrowCell::UInt32(u32::MIN),
                MssqlCell::BigInt(Some(i64::from(u32::MIN))),
            ),
            (
                4,
                ArrowCell::UInt32(u32::MAX),
                MssqlCell::BigInt(Some(i64::from(u32::MAX))),
            ),
        ];

        for (mapping_index, cell, expected) in cases {
            assert_eq!(
                convert_cell(&mappings[mapping_index], cell, mapping_index).unwrap(),
                expected
            );
        }
    }

    #[test]
    fn rejects_null_in_non_nullable_planned_column() {
        let mappings = mappings_for_schema(Schema::new(vec![Field::new(
            "active",
            DataType::Boolean,
            false,
        )]));

        let err = convert_cell(&mappings[0], ArrowCell::Null, 0).unwrap_err();

        assert_single_diagnostic(
            err,
            DiagnosticCode::NullInNonNullableColumn,
            Some(0),
            Some((0, "active")),
        );
    }

    #[test]
    fn rejects_non_finite_float32_values() {
        let mappings = mappings_for_schema(Schema::new(vec![Field::new(
            "ratio",
            DataType::Float32,
            true,
        )]));

        for (row_index, value) in [f32::NAN, f32::INFINITY, f32::NEG_INFINITY]
            .into_iter()
            .enumerate()
        {
            let err = convert_cell(&mappings[0], ArrowCell::Float32(value), row_index).unwrap_err();

            assert_single_diagnostic(
                err,
                DiagnosticCode::NonFiniteFloat,
                Some(row_index),
                Some((0, "ratio")),
            );
        }
    }

    #[test]
    fn rejects_non_finite_float64_values() {
        let mappings = mappings_for_schema(Schema::new(vec![Field::new(
            "ratio",
            DataType::Float64,
            true,
        )]));

        for (row_index, value) in [f64::NAN, f64::INFINITY, f64::NEG_INFINITY]
            .into_iter()
            .enumerate()
        {
            let err = convert_cell(&mappings[0], ArrowCell::Float64(value), row_index).unwrap_err();

            assert_single_diagnostic(
                err,
                DiagnosticCode::NonFiniteFloat,
                Some(row_index),
                Some((0, "ratio")),
            );
        }
    }

    #[test]
    fn rejects_payload_that_does_not_fit_planned_mssql_type() {
        let mapping = SchemaMapping::new(
            ArrowFieldRef::new(0, "id".to_owned(), false, DataType::Int32),
            MssqlColumn::new(Identifier::new("id").unwrap(), MssqlType::BigInt, false),
        );

        let err = convert_cell(&mapping, ArrowCell::Int32(7), 0).unwrap_err();

        assert_single_diagnostic(
            err,
            DiagnosticCode::ValueTypeMismatch,
            Some(0),
            Some((0, "id")),
        );
    }

    fn convert_cell<'a>(
        mapping: &SchemaMapping,
        cell: ArrowCell<'a>,
        row_index: usize,
    ) -> crate::Result<MssqlCell<'a>> {
        let options = PlanOptions::default();
        let runtime_mapping = ArrowToMssqlRuntimeMapping::new(mapping, &options);
        mssql_cell_from_arrow_cell(runtime_mapping, cell, row_index)
    }

    fn mappings_for_schema(schema: Schema) -> Vec<SchemaMapping> {
        plan_arrow_schema_to_mssql_mappings(
            Arc::new(schema),
            MssqlProfile::sql_server_2016_compat_100(),
            PlanOptions::default(),
        )
        .unwrap()
        .into_parts()
        .0
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

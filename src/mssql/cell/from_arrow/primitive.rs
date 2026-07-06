//! Primitive Arrow-to-MSSQL runtime cell conversion.

use crate::{
    DiagnosticCode, Result, SchemaMapping, arrow::cell::ArrowCell,
    conversion::arrow_to_mssql::primitive::PrimitiveArrowToMssql, mssql::cell::MssqlCell,
};

use super::{non_finite_float_error, row_mapping_diagnostic, value_conversion_error};

pub(super) fn primitive_mssql_cell<'a>(
    mapping: &SchemaMapping,
    row_index: usize,
    cell: ArrowCell<'a>,
) -> Result<MssqlCell<'a>> {
    match (classify_for_scalar(mapping, row_index), cell) {
        (Some(PrimitiveArrowToMssql::BooleanToBit), ArrowCell::Boolean(value)) => {
            Ok(MssqlCell::Bit(Some(value)))
        }
        (Some(PrimitiveArrowToMssql::UInt8ToTinyInt), ArrowCell::UInt8(value)) => {
            Ok(MssqlCell::TinyInt(Some(value)))
        }
        (Some(PrimitiveArrowToMssql::Int8ToSmallInt), ArrowCell::Int8(value)) => {
            Ok(MssqlCell::SmallInt(Some(i16::from(value))))
        }
        (Some(PrimitiveArrowToMssql::Int16ToSmallInt), ArrowCell::Int16(value)) => {
            Ok(MssqlCell::SmallInt(Some(value)))
        }
        (Some(PrimitiveArrowToMssql::Int32ToInt), ArrowCell::Int32(value)) => {
            Ok(MssqlCell::Int(Some(value)))
        }
        (Some(PrimitiveArrowToMssql::UInt16ToInt), ArrowCell::UInt16(value)) => {
            Ok(MssqlCell::Int(Some(i32::from(value))))
        }
        (Some(PrimitiveArrowToMssql::Int64ToBigInt), ArrowCell::Int64(value)) => {
            Ok(MssqlCell::BigInt(Some(value)))
        }
        (Some(PrimitiveArrowToMssql::UInt32ToBigInt), ArrowCell::UInt32(value)) => {
            Ok(MssqlCell::BigInt(Some(i64::from(value))))
        }
        (Some(PrimitiveArrowToMssql::UInt64ToCheckedBigInt), ArrowCell::UInt64(value)) => {
            i64::try_from(value)
                .map_err(|_| {
                    value_conversion_error(row_mapping_diagnostic(
                        mapping,
                        row_index,
                        DiagnosticCode::IntegerOutOfRange,
                        format!(
                            "Arrow UInt64 value {value} does not fit planned SQL Server bigint"
                        ),
                    ))
                })
                .map(|value| MssqlCell::BigInt(Some(value)))
        }
        (Some(PrimitiveArrowToMssql::Float16ToReal), ArrowCell::Float16(value))
        | (Some(PrimitiveArrowToMssql::Float32ToReal), ArrowCell::Float32(value))
            if value.is_finite() =>
        {
            Ok(MssqlCell::Real(Some(value)))
        }
        (Some(PrimitiveArrowToMssql::Float16ToReal), ArrowCell::Float16(value))
        | (Some(PrimitiveArrowToMssql::Float32ToReal), ArrowCell::Float32(value)) => {
            Err(non_finite_float_error(mapping, row_index, value))
        }
        (Some(PrimitiveArrowToMssql::Float64ToFloat), ArrowCell::Float64(value))
            if value.is_finite() =>
        {
            Ok(MssqlCell::Float(Some(value)))
        }
        (Some(PrimitiveArrowToMssql::Float64ToFloat), ArrowCell::Float64(value)) => {
            Err(non_finite_float_error(mapping, row_index, value))
        }
        other => Err(primitive_type_mismatch(mapping, row_index, other)),
    }
}

fn classify_for_scalar(mapping: &SchemaMapping, row_index: usize) -> Option<PrimitiveArrowToMssql> {
    PrimitiveArrowToMssql::classify(mapping, row_index).ok()
}

fn primitive_type_mismatch(
    mapping: &SchemaMapping,
    row_index: usize,
    actual: (Option<PrimitiveArrowToMssql>, ArrowCell<'_>),
) -> crate::Error {
    value_conversion_error(row_mapping_diagnostic(
        mapping,
        row_index,
        DiagnosticCode::ValueTypeMismatch,
        format!("Arrow primitive payload does not match planned primitive conversion: {actual:?}"),
    ))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_schema::{DataType, Field, Schema};

    use super::super::{ArrowToMssqlRuntimeMapping, mssql_cell_from_arrow_cell};
    use crate::{
        ArrowFieldRef, DiagnosticCode, Identifier, MssqlColumn, MssqlProfile, MssqlType,
        PlanOptions, SchemaMapping, UInt64Policy, arrow::cell::ArrowCell, mssql::cell::MssqlCell,
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
            Field::new("half_value", DataType::Float16, true),
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
            (8, ArrowCell::Float16(1.5), MssqlCell::Real(Some(1.5))),
            (9, ArrowCell::Float32(1.25), MssqlCell::Real(Some(1.25))),
            (10, ArrowCell::Float64(2.5), MssqlCell::Float(Some(2.5))),
            (
                11,
                ArrowCell::Utf8("hello"),
                MssqlCell::NVarChar(Some("hello")),
            ),
            (
                12,
                ArrowCell::Utf8("Tokyo"),
                MssqlCell::NVarChar(Some("Tokyo")),
            ),
            (
                13,
                ArrowCell::Binary(b"abc"),
                MssqlCell::VarBinary(Some(b"abc")),
            ),
            (
                14,
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
            (9, MssqlCell::Real(None)),
            (10, MssqlCell::Float(None)),
            (11, MssqlCell::NVarChar(None)),
            (12, MssqlCell::NVarChar(None)),
            (13, MssqlCell::VarBinary(None)),
            (14, MssqlCell::VarBinary(None)),
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
    fn converts_uint64_checked_bigint_boundary_values() {
        let mappings = mappings_for_schema_with_options(
            Schema::new(vec![Field::new(
                "unsigned_as_bigint",
                DataType::UInt64,
                true,
            )]),
            PlanOptions {
                uint64_policy: UInt64Policy::CheckedBigInt,
                ..PlanOptions::default()
            },
        );

        assert_eq!(
            convert_cell(&mappings[0], ArrowCell::UInt64(0), 0).unwrap(),
            MssqlCell::BigInt(Some(0))
        );
        assert_eq!(
            convert_cell(&mappings[0], ArrowCell::UInt64(i64::MAX as u64), 1).unwrap(),
            MssqlCell::BigInt(Some(i64::MAX))
        );
        assert_eq!(
            convert_cell(&mappings[0], ArrowCell::Null, 2).unwrap(),
            MssqlCell::BigInt(None)
        );
    }

    #[test]
    fn rejects_uint64_checked_bigint_overflow_without_wrapping() {
        let mappings = mappings_for_schema_with_options(
            Schema::new(vec![Field::new(
                "unsigned_as_bigint",
                DataType::UInt64,
                false,
            )]),
            PlanOptions {
                uint64_policy: UInt64Policy::CheckedBigInt,
                ..PlanOptions::default()
            },
        );

        let just_over =
            convert_cell(&mappings[0], ArrowCell::UInt64((i64::MAX as u64) + 1), 0).unwrap_err();
        assert_single_diagnostic(
            just_over,
            DiagnosticCode::IntegerOutOfRange,
            Some(0),
            Some((0, "unsigned_as_bigint")),
        );

        let max = convert_cell(&mappings[0], ArrowCell::UInt64(u64::MAX), 1).unwrap_err();
        assert_single_diagnostic(
            max,
            DiagnosticCode::IntegerOutOfRange,
            Some(1),
            Some((0, "unsigned_as_bigint")),
        );
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
    fn rejects_non_finite_float16_values() {
        let mappings = mappings_for_schema(Schema::new(vec![Field::new(
            "ratio",
            DataType::Float16,
            true,
        )]));

        for (row_index, value) in [f32::NAN, f32::INFINITY, f32::NEG_INFINITY]
            .into_iter()
            .enumerate()
        {
            let err = convert_cell(&mappings[0], ArrowCell::Float16(value), row_index).unwrap_err();

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
        convert_cell_with_options(mapping, cell, row_index, &PlanOptions::default())
    }

    fn convert_cell_with_options<'a>(
        mapping: &SchemaMapping,
        cell: ArrowCell<'a>,
        row_index: usize,
        options: &PlanOptions,
    ) -> crate::Result<MssqlCell<'a>> {
        let runtime_mapping = ArrowToMssqlRuntimeMapping::new_with_options(mapping, options);
        mssql_cell_from_arrow_cell(runtime_mapping, cell, row_index)
    }

    fn mappings_for_schema(schema: Schema) -> Vec<SchemaMapping> {
        mappings_for_schema_with_options(schema, PlanOptions::default())
    }

    fn mappings_for_schema_with_options(
        schema: Schema,
        options: PlanOptions,
    ) -> Vec<SchemaMapping> {
        plan_arrow_schema_to_mssql_mappings(
            Arc::new(schema),
            MssqlProfile::sql_server_2016_compat_100(),
            options,
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

//! Primitive Arrow-to-SQL Server conversion classification.

use arrow_schema::DataType;

use crate::{
    Diagnostic, DiagnosticCode, DiagnosticSet, FieldRef, MssqlType, Result, SchemaMapping,
};

/// Shared semantic conversion class for primitive Arrow-to-MSSQL values.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) enum PrimitiveArrowToMssql {
    /// Arrow Boolean to SQL Server `bit`.
    BooleanToBit,
    /// Arrow UInt8 to SQL Server `tinyint`.
    UInt8ToTinyInt,
    /// Arrow Int8 to SQL Server `smallint`.
    Int8ToSmallInt,
    /// Arrow Int16 to SQL Server `smallint`.
    Int16ToSmallInt,
    /// Arrow Int32 to SQL Server `int`.
    Int32ToInt,
    /// Arrow UInt16 to SQL Server `int`.
    UInt16ToInt,
    /// Arrow Int64 to SQL Server `bigint`.
    Int64ToBigInt,
    /// Arrow UInt32 to SQL Server `bigint`.
    UInt32ToBigInt,
    /// Arrow UInt64 to SQL Server `bigint` with a checked range conversion.
    UInt64ToCheckedBigInt,
    /// Arrow Float32 to SQL Server `real`.
    Float32ToReal,
    /// Arrow Float64 to SQL Server `float(53)`.
    Float64ToFloat,
}

impl PrimitiveArrowToMssql {
    /// Classifies a planned primitive mapping.
    pub(crate) fn classify(mapping: &SchemaMapping, row_index: usize) -> Result<Self> {
        let classification = match (mapping.arrow().data_type(), mapping.mssql().ty()) {
            (DataType::Boolean, MssqlType::Bit) => Self::BooleanToBit,
            (DataType::UInt8, MssqlType::TinyInt) => Self::UInt8ToTinyInt,
            (DataType::Int8, MssqlType::SmallInt) => Self::Int8ToSmallInt,
            (DataType::Int16, MssqlType::SmallInt) => Self::Int16ToSmallInt,
            (DataType::Int32, MssqlType::Int) => Self::Int32ToInt,
            (DataType::UInt16, MssqlType::Int) => Self::UInt16ToInt,
            (DataType::Int64, MssqlType::BigInt) => Self::Int64ToBigInt,
            (DataType::UInt32, MssqlType::BigInt) => Self::UInt32ToBigInt,
            (DataType::UInt64, MssqlType::BigInt) => Self::UInt64ToCheckedBigInt,
            (DataType::Float32, MssqlType::Real) => Self::Float32ToReal,
            (DataType::Float64, MssqlType::Float { .. }) => Self::Float64ToFloat,
            _ => {
                return Err(value_conversion_error(row_mapping_diagnostic(
                    mapping,
                    row_index,
                    DiagnosticCode::ValueConversionUnsupported,
                    format!(
                        "primitive conversion from Arrow {} to SQL Server {} is not supported",
                        mapping.arrow().data_type(),
                        mapping.mssql().ty().to_sql()
                    ),
                )));
            }
        };

        Ok(classification)
    }
}

fn row_mapping_diagnostic(
    mapping: &SchemaMapping,
    row_index: usize,
    code: DiagnosticCode,
    message: impl Into<String>,
) -> Diagnostic {
    Diagnostic::error(code, message)
        .with_field(FieldRef::new(
            mapping.arrow().index(),
            mapping.arrow().name(),
        ))
        .with_row(row_index)
}

fn value_conversion_error(diagnostic: Diagnostic) -> crate::Error {
    crate::Error::ValueConversion {
        diagnostics: DiagnosticSet::from(vec![diagnostic]),
    }
}

#[cfg(test)]
mod tests {
    use arrow_schema::DataType;

    use crate::{ArrowFieldRef, DiagnosticCode, Identifier, MssqlColumn, MssqlType, SchemaMapping};

    use super::PrimitiveArrowToMssql;

    #[test]
    fn classifies_supported_scalar_primitive_mappings() {
        let cases = [
            (
                DataType::Boolean,
                MssqlType::Bit,
                PrimitiveArrowToMssql::BooleanToBit,
            ),
            (
                DataType::UInt8,
                MssqlType::TinyInt,
                PrimitiveArrowToMssql::UInt8ToTinyInt,
            ),
            (
                DataType::Int8,
                MssqlType::SmallInt,
                PrimitiveArrowToMssql::Int8ToSmallInt,
            ),
            (
                DataType::Int16,
                MssqlType::SmallInt,
                PrimitiveArrowToMssql::Int16ToSmallInt,
            ),
            (
                DataType::Int32,
                MssqlType::Int,
                PrimitiveArrowToMssql::Int32ToInt,
            ),
            (
                DataType::UInt16,
                MssqlType::Int,
                PrimitiveArrowToMssql::UInt16ToInt,
            ),
            (
                DataType::Int64,
                MssqlType::BigInt,
                PrimitiveArrowToMssql::Int64ToBigInt,
            ),
            (
                DataType::UInt32,
                MssqlType::BigInt,
                PrimitiveArrowToMssql::UInt32ToBigInt,
            ),
            (
                DataType::UInt64,
                MssqlType::BigInt,
                PrimitiveArrowToMssql::UInt64ToCheckedBigInt,
            ),
            (
                DataType::Float32,
                MssqlType::Real,
                PrimitiveArrowToMssql::Float32ToReal,
            ),
            (
                DataType::Float64,
                MssqlType::Float { precision: 53 },
                PrimitiveArrowToMssql::Float64ToFloat,
            ),
        ];

        for (index, (arrow_type, mssql_type, expected)) in cases.into_iter().enumerate() {
            let mapping = mapping(index, "value", arrow_type, mssql_type);

            assert_eq!(
                PrimitiveArrowToMssql::classify(&mapping, index).unwrap(),
                expected
            );
        }
    }

    #[test]
    fn classifier_rejects_unsupported_primitive_pairs_with_field_diagnostic() {
        let mapping = mapping(3, "id", DataType::Int32, MssqlType::BigInt);

        let err = PrimitiveArrowToMssql::classify(&mapping, 9).unwrap_err();

        assert_single_diagnostic(
            err,
            DiagnosticCode::ValueConversionUnsupported,
            Some(9),
            Some((3, "id")),
        );
    }

    fn mapping(
        index: usize,
        name: &str,
        arrow_type: DataType,
        mssql_type: MssqlType,
    ) -> SchemaMapping {
        SchemaMapping::new(
            ArrowFieldRef::new(index, name.to_owned(), false, arrow_type),
            MssqlColumn::new(Identifier::new(name).unwrap(), mssql_type, false),
        )
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

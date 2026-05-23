//! UInt64 policy-dependent Arrow-to-SQL Server conversion classification.

use arrow_schema::DataType;

use crate::{
    Diagnostic, DiagnosticCode, DiagnosticSet, FieldRef, MssqlType, Result, SchemaMapping,
};

/// Shared semantic conversion class for planned Arrow UInt64 mappings.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) enum UInt64ArrowToMssql {
    /// Arrow UInt64 to SQL Server `bigint` with a checked range conversion.
    CheckedBigInt,
    /// Arrow UInt64 to SQL Server `decimal(20,0)`.
    Decimal20_0,
}

impl UInt64ArrowToMssql {
    /// Classifies a planned UInt64 mapping after schema planning has selected
    /// the concrete SQL Server target type.
    pub(crate) fn classify(mapping: &SchemaMapping, row_index: usize) -> Result<Self> {
        let classification = match (mapping.arrow().data_type(), mapping.mssql().ty()) {
            (DataType::UInt64, MssqlType::BigInt) => Self::CheckedBigInt,
            (
                DataType::UInt64,
                MssqlType::Decimal {
                    precision: 20,
                    scale: 0,
                },
            ) => Self::Decimal20_0,
            _ => {
                return Err(value_conversion_error(row_mapping_diagnostic(
                    mapping,
                    row_index,
                    DiagnosticCode::ValueConversionUnsupported,
                    format!(
                        "UInt64 conversion from Arrow {} to SQL Server {} is not supported",
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

    use crate::{
        ArrowFieldRef, DiagnosticCode, Identifier, MssqlColumn, MssqlType, SchemaMapping,
        conversion::arrow_to_mssql::uint64::UInt64ArrowToMssql,
    };

    #[test]
    fn classifies_supported_uint64_policy_mappings() {
        let cases = [
            (
                MssqlType::BigInt,
                UInt64ArrowToMssql::CheckedBigInt,
                "checked bigint",
            ),
            (
                MssqlType::Decimal {
                    precision: 20,
                    scale: 0,
                },
                UInt64ArrowToMssql::Decimal20_0,
                "decimal20_0",
            ),
        ];

        for (index, (mssql_type, expected, label)) in cases.into_iter().enumerate() {
            let mapping = mapping(index, label, DataType::UInt64, mssql_type);

            assert_eq!(
                UInt64ArrowToMssql::classify(&mapping, index).unwrap(),
                expected
            );
        }
    }

    #[test]
    fn rejects_non_uint64_arrow_sources() {
        let mapping = mapping(2, "signed", DataType::Int64, MssqlType::BigInt);

        let err = UInt64ArrowToMssql::classify(&mapping, 9).unwrap_err();

        assert_single_diagnostic(
            err,
            DiagnosticCode::ValueConversionUnsupported,
            Some(9),
            Some((2, "signed")),
        );
    }

    #[test]
    fn rejects_forged_decimal_shapes() {
        for mssql_type in [
            MssqlType::Decimal {
                precision: 19,
                scale: 0,
            },
            MssqlType::Decimal {
                precision: 20,
                scale: 1,
            },
        ] {
            let mapping = mapping(3, "unsigned", DataType::UInt64, mssql_type);

            let err = UInt64ArrowToMssql::classify(&mapping, 7).unwrap_err();

            assert_single_diagnostic(
                err,
                DiagnosticCode::ValueConversionUnsupported,
                Some(7),
                Some((3, "unsigned")),
            );
        }
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

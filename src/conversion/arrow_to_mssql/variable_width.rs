//! Variable-width Arrow-to-SQL Server conversion classification.

use arrow_schema::DataType;

use crate::{
    Diagnostic, DiagnosticCode, DiagnosticSet, FieldRef, MssqlType, MssqlTypeLength, Result,
    SchemaMapping,
};

/// Shared semantic conversion class for variable-width Arrow-to-MSSQL values.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) enum VariableWidthArrowToMssql {
    /// Arrow string family to SQL Server `nvarchar(n|max)`.
    StringToNVarChar { length: MssqlTypeLength },
    /// Arrow binary family to SQL Server `varbinary(n|max)`.
    BytesToVarBinary { length: MssqlTypeLength },
}

impl VariableWidthArrowToMssql {
    /// Classifies a planned variable-width mapping.
    pub(crate) fn classify(mapping: &SchemaMapping, row_index: usize) -> Result<Self> {
        let classification = match (mapping.arrow().data_type(), mapping.mssql().ty()) {
            (DataType::Utf8 | DataType::LargeUtf8, MssqlType::NVarChar(length)) => {
                Self::StringToNVarChar { length: *length }
            }
            (DataType::Binary | DataType::LargeBinary, MssqlType::VarBinary(length)) => {
                Self::BytesToVarBinary { length: *length }
            }
            _ => {
                return Err(value_conversion_error(row_mapping_diagnostic(
                    mapping,
                    row_index,
                    DiagnosticCode::ValueConversionUnsupported,
                    format!(
                        "variable-width conversion from Arrow {} to SQL Server {} is not supported",
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
        ArrowFieldRef, DiagnosticCode, Identifier, MssqlColumn, MssqlType, MssqlTypeLength,
        SchemaMapping,
    };

    use super::VariableWidthArrowToMssql;

    #[test]
    fn classifies_variable_width_mappings() {
        let cases = [
            (
                DataType::Utf8,
                MssqlType::NVarChar(MssqlTypeLength::Max),
                VariableWidthArrowToMssql::StringToNVarChar {
                    length: MssqlTypeLength::Max,
                },
            ),
            (
                DataType::Utf8,
                MssqlType::NVarChar(MssqlTypeLength::Bounded(32)),
                VariableWidthArrowToMssql::StringToNVarChar {
                    length: MssqlTypeLength::Bounded(32),
                },
            ),
            (
                DataType::LargeUtf8,
                MssqlType::NVarChar(MssqlTypeLength::Max),
                VariableWidthArrowToMssql::StringToNVarChar {
                    length: MssqlTypeLength::Max,
                },
            ),
            (
                DataType::Binary,
                MssqlType::VarBinary(MssqlTypeLength::Max),
                VariableWidthArrowToMssql::BytesToVarBinary {
                    length: MssqlTypeLength::Max,
                },
            ),
            (
                DataType::Binary,
                MssqlType::VarBinary(MssqlTypeLength::Bounded(16)),
                VariableWidthArrowToMssql::BytesToVarBinary {
                    length: MssqlTypeLength::Bounded(16),
                },
            ),
            (
                DataType::LargeBinary,
                MssqlType::VarBinary(MssqlTypeLength::Max),
                VariableWidthArrowToMssql::BytesToVarBinary {
                    length: MssqlTypeLength::Max,
                },
            ),
        ];

        for (index, (arrow_type, mssql_type, expected)) in cases.into_iter().enumerate() {
            let mapping = mapping(index, "value", arrow_type, mssql_type);

            assert_eq!(
                VariableWidthArrowToMssql::classify(&mapping, index).unwrap(),
                expected
            );
        }
    }

    #[test]
    fn classifies_large_variable_width_mappings_as_semantic_families() {
        let cases = [
            (
                DataType::LargeUtf8,
                MssqlType::NVarChar(MssqlTypeLength::Max),
                "large_text",
                VariableWidthArrowToMssql::StringToNVarChar {
                    length: MssqlTypeLength::Max,
                },
            ),
            (
                DataType::LargeBinary,
                MssqlType::VarBinary(MssqlTypeLength::Max),
                "large_bytes",
                VariableWidthArrowToMssql::BytesToVarBinary {
                    length: MssqlTypeLength::Max,
                },
            ),
        ];

        for (index, (arrow_type, mssql_type, name, expected)) in cases.into_iter().enumerate() {
            let mapping = mapping(index, name, arrow_type, mssql_type);

            // Direct encoder support is filtered by the direct planning layer.
            // This classifier stays semantic so scalar conversion has one source of truth.
            let classification = VariableWidthArrowToMssql::classify(&mapping, 7).unwrap();

            assert_eq!(classification, expected);
        }
    }

    #[test]
    fn classifier_rejects_mismatched_variable_width_pairs() {
        let mapping = mapping(
            3,
            "text",
            DataType::Utf8,
            MssqlType::VarBinary(MssqlTypeLength::Max),
        );

        let err = VariableWidthArrowToMssql::classify(&mapping, 9).unwrap_err();

        assert_single_diagnostic(
            err,
            DiagnosticCode::ValueConversionUnsupported,
            Some(9),
            Some((3, "text")),
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

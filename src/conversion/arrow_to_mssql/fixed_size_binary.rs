//! Fixed-size binary Arrow-to-SQL Server conversion classification.

use arrow_schema::DataType;

use crate::{
    Diagnostic, DiagnosticCode, DiagnosticSet, FieldRef, MssqlType, Result, SchemaMapping,
};

/// Shared semantic conversion class for fixed-size binary Arrow-to-MSSQL values.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) enum FixedSizeBinaryArrowToMssql {
    /// Arrow FixedSizeBinary(n) to SQL Server `binary(n)`.
    FixedSizeBinaryToBinary { length: usize },
}

impl FixedSizeBinaryArrowToMssql {
    /// Classifies a planned fixed-size binary mapping.
    pub(crate) fn classify(mapping: &SchemaMapping, row_index: usize) -> Result<Self> {
        let classification = match (mapping.arrow().data_type(), mapping.mssql().ty()) {
            (DataType::FixedSizeBinary(arrow_length), MssqlType::Binary(mssql_length)) => {
                let arrow_length = usize::try_from(*arrow_length).map_err(|_| {
                    value_conversion_error(row_mapping_diagnostic(
                        mapping,
                        row_index,
                        DiagnosticCode::ValueConversionUnsupported,
                        format!(
                            "fixed-size binary length {arrow_length} cannot map to SQL Server {}",
                            mapping.mssql().ty().to_sql()
                        ),
                    ))
                })?;

                if arrow_length != *mssql_length {
                    return Err(value_conversion_error(row_mapping_diagnostic(
                        mapping,
                        row_index,
                        DiagnosticCode::ValueTypeMismatch,
                        format!(
                            "Arrow FixedSizeBinary({arrow_length}) does not match planned SQL Server {}",
                            mapping.mssql().ty().to_sql()
                        ),
                    )));
                }

                Self::FixedSizeBinaryToBinary {
                    length: *mssql_length,
                }
            }
            _ => {
                return Err(value_conversion_error(row_mapping_diagnostic(
                    mapping,
                    row_index,
                    DiagnosticCode::ValueConversionUnsupported,
                    format!(
                        "fixed-size binary conversion from Arrow {} to SQL Server {} is not supported",
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

    use super::FixedSizeBinaryArrowToMssql;
    use crate::{
        ArrowFieldRef, DiagnosticCode, Identifier, MssqlColumn, MssqlType, MssqlTypeLength,
        SchemaMapping,
    };

    #[test]
    fn classifies_fixed_size_binary_mapping() {
        let mapping = mapping(
            2,
            "digest",
            DataType::FixedSizeBinary(32),
            MssqlType::Binary(32),
        );

        assert_eq!(
            FixedSizeBinaryArrowToMssql::classify(&mapping, 7).unwrap(),
            FixedSizeBinaryArrowToMssql::FixedSizeBinaryToBinary { length: 32 }
        );
    }

    #[test]
    fn rejects_mismatched_fixed_size_binary_length() {
        let mapping = mapping(
            2,
            "digest",
            DataType::FixedSizeBinary(16),
            MssqlType::Binary(32),
        );

        let err = FixedSizeBinaryArrowToMssql::classify(&mapping, 7).unwrap_err();

        assert_single_diagnostic(
            err,
            DiagnosticCode::ValueTypeMismatch,
            Some(7),
            Some((2, "digest")),
        );
    }

    #[test]
    fn rejects_non_fixed_size_binary_pairs() {
        let mapping = mapping(
            2,
            "bytes",
            DataType::Binary,
            MssqlType::VarBinary(MssqlTypeLength::Bounded(32)),
        );

        let err = FixedSizeBinaryArrowToMssql::classify(&mapping, 7).unwrap_err();

        assert_single_diagnostic(
            err,
            DiagnosticCode::ValueConversionUnsupported,
            Some(7),
            Some((2, "bytes")),
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

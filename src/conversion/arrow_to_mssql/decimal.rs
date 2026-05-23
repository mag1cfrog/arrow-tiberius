//! Decimal Arrow-to-SQL Server conversion classification.

use arrow_schema::DataType;

use crate::{
    Diagnostic, DiagnosticCode, DiagnosticSet, FieldRef, MssqlType, Result, SchemaMapping,
};

/// Shared semantic conversion class for planned Arrow decimal mappings.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) enum DecimalArrowToMssql {
    /// Arrow Decimal32 to SQL Server `decimal(p,s)`.
    Decimal32 {
        /// Planned SQL Server decimal precision.
        target_precision: u8,
        /// Planned SQL Server decimal scale.
        target_scale: u8,
        /// Arrow source scale.
        arrow_scale: i8,
    },
    /// Arrow Decimal64 to SQL Server `decimal(p,s)`.
    Decimal64 {
        /// Planned SQL Server decimal precision.
        target_precision: u8,
        /// Planned SQL Server decimal scale.
        target_scale: u8,
        /// Arrow source scale.
        arrow_scale: i8,
    },
    /// Arrow Decimal128 to SQL Server `decimal(p,s)`.
    Decimal128 {
        /// Planned SQL Server decimal precision.
        target_precision: u8,
        /// Planned SQL Server decimal scale.
        target_scale: u8,
        /// Arrow source scale.
        arrow_scale: i8,
    },
    /// Arrow Decimal256 to SQL Server `decimal(p,s)` after checked downcast.
    Decimal256CheckedDowncast {
        /// Planned SQL Server decimal precision.
        target_precision: u8,
        /// Planned SQL Server decimal scale.
        target_scale: u8,
        /// Arrow source scale.
        arrow_scale: i8,
    },
}

impl DecimalArrowToMssql {
    /// Classifies a planned Arrow decimal mapping after schema planning has
    /// selected the concrete SQL Server target type.
    pub(crate) fn classify(mapping: &SchemaMapping, row_index: usize) -> Result<Self> {
        let (target_precision, target_scale) = decimal_target(mapping, row_index)?;
        let (source, arrow_scale) = match mapping.arrow().data_type() {
            DataType::Decimal32(_, arrow_scale) => (DecimalSource::Decimal32, *arrow_scale),
            DataType::Decimal64(_, arrow_scale) => (DecimalSource::Decimal64, *arrow_scale),
            DataType::Decimal128(_, arrow_scale) => (DecimalSource::Decimal128, *arrow_scale),
            DataType::Decimal256(_, arrow_scale) => (DecimalSource::Decimal256, *arrow_scale),
            _ => {
                return Err(value_conversion_error(row_mapping_diagnostic(
                    mapping,
                    row_index,
                    DiagnosticCode::ValueConversionUnsupported,
                    format!(
                        "decimal conversion from Arrow {} to SQL Server {} is not supported",
                        mapping.arrow().data_type(),
                        mapping.mssql().ty().to_sql()
                    ),
                )));
            }
        };

        validate_scale_compatibility(mapping, row_index, target_scale, arrow_scale)?;

        let classification = match source {
            DecimalSource::Decimal32 => Self::Decimal32 {
                target_precision,
                target_scale,
                arrow_scale,
            },
            DecimalSource::Decimal64 => Self::Decimal64 {
                target_precision,
                target_scale,
                arrow_scale,
            },
            DecimalSource::Decimal128 => Self::Decimal128 {
                target_precision,
                target_scale,
                arrow_scale,
            },
            DecimalSource::Decimal256 => Self::Decimal256CheckedDowncast {
                target_precision,
                target_scale,
                arrow_scale,
            },
        };

        Ok(classification)
    }

    /// Returns the planned SQL Server decimal precision.
    pub(crate) const fn target_precision(self) -> u8 {
        match self {
            Self::Decimal32 {
                target_precision, ..
            }
            | Self::Decimal64 {
                target_precision, ..
            }
            | Self::Decimal128 {
                target_precision, ..
            }
            | Self::Decimal256CheckedDowncast {
                target_precision, ..
            } => target_precision,
        }
    }

    /// Returns the planned SQL Server decimal scale.
    pub(crate) const fn target_scale(self) -> u8 {
        match self {
            Self::Decimal32 { target_scale, .. }
            | Self::Decimal64 { target_scale, .. }
            | Self::Decimal128 { target_scale, .. }
            | Self::Decimal256CheckedDowncast { target_scale, .. } => target_scale,
        }
    }

    /// Returns the Arrow source decimal scale.
    pub(crate) const fn arrow_scale(self) -> i8 {
        match self {
            Self::Decimal32 { arrow_scale, .. }
            | Self::Decimal64 { arrow_scale, .. }
            | Self::Decimal128 { arrow_scale, .. }
            | Self::Decimal256CheckedDowncast { arrow_scale, .. } => arrow_scale,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum DecimalSource {
    Decimal32,
    Decimal64,
    Decimal128,
    Decimal256,
}

fn decimal_target(mapping: &SchemaMapping, row_index: usize) -> Result<(u8, u8)> {
    let MssqlType::Decimal { precision, scale } = mapping.mssql().ty() else {
        return Err(value_conversion_error(row_mapping_diagnostic(
            mapping,
            row_index,
            DiagnosticCode::ValueTypeMismatch,
            "planned SQL Server type is not decimal",
        )));
    };

    let target_scale = u8::try_from(*scale).map_err(|_| {
        value_conversion_error(row_mapping_diagnostic(
            mapping,
            row_index,
            DiagnosticCode::DecimalOutOfRange,
            format!(
                "planned SQL Server decimal scale {scale} cannot be represented by Tiberius Numeric"
            ),
        ))
    })?;

    if target_scale >= 38 {
        return Err(value_conversion_error(row_mapping_diagnostic(
            mapping,
            row_index,
            DiagnosticCode::DecimalOutOfRange,
            format!(
                "planned SQL Server decimal scale {target_scale} cannot be represented by Tiberius Numeric"
            ),
        )));
    }

    Ok((*precision, target_scale))
}

fn validate_scale_compatibility(
    mapping: &SchemaMapping,
    row_index: usize,
    target_scale: u8,
    arrow_scale: i8,
) -> Result<()> {
    let expected_scale = if arrow_scale < 0 {
        0
    } else {
        u8::try_from(arrow_scale).map_err(|_| {
            value_conversion_error(row_mapping_diagnostic(
                mapping,
                row_index,
                DiagnosticCode::DecimalOutOfRange,
                format!("Arrow decimal scale {arrow_scale} cannot be represented at runtime"),
            ))
        })?
    };

    if target_scale == expected_scale {
        return Ok(());
    }

    Err(value_conversion_error(row_mapping_diagnostic(
        mapping,
        row_index,
        DiagnosticCode::SchemaMismatch,
        format!(
            "planned SQL Server decimal scale {target_scale} is incompatible with Arrow decimal scale {expected_scale}"
        ),
    )))
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
        conversion::arrow_to_mssql::decimal::DecimalArrowToMssql,
    };

    #[test]
    fn classifies_supported_decimal_mappings() {
        let cases = [
            (
                DataType::Decimal32(9, 2),
                DecimalArrowToMssql::Decimal32 {
                    target_precision: 9,
                    target_scale: 2,
                    arrow_scale: 2,
                },
            ),
            (
                DataType::Decimal64(18, 4),
                DecimalArrowToMssql::Decimal64 {
                    target_precision: 18,
                    target_scale: 4,
                    arrow_scale: 4,
                },
            ),
            (
                DataType::Decimal128(38, 9),
                DecimalArrowToMssql::Decimal128 {
                    target_precision: 38,
                    target_scale: 9,
                    arrow_scale: 9,
                },
            ),
            (
                DataType::Decimal256(38, 0),
                DecimalArrowToMssql::Decimal256CheckedDowncast {
                    target_precision: 38,
                    target_scale: 0,
                    arrow_scale: 0,
                },
            ),
            (
                DataType::Decimal128(3, -2),
                DecimalArrowToMssql::Decimal128 {
                    target_precision: 5,
                    target_scale: 0,
                    arrow_scale: -2,
                },
            ),
        ];

        for (index, (arrow_type, expected)) in cases.into_iter().enumerate() {
            let mapping = mapping(
                index,
                "amount",
                arrow_type,
                MssqlType::Decimal {
                    precision: expected.target_precision(),
                    scale: expected.target_scale() as i8,
                },
            );

            assert_eq!(
                DecimalArrowToMssql::classify(&mapping, index).unwrap(),
                expected
            );
        }
    }

    #[test]
    fn rejects_non_decimal_sources_and_targets() {
        let non_decimal_source = mapping(0, "id", DataType::Int32, decimal_type(10, 0));
        let err = DecimalArrowToMssql::classify(&non_decimal_source, 3).unwrap_err();
        assert_single_diagnostic(
            err,
            DiagnosticCode::ValueConversionUnsupported,
            Some(3),
            Some((0, "id")),
        );

        let non_decimal_target = mapping(1, "amount", DataType::Decimal128(10, 0), MssqlType::Int);
        let err = DecimalArrowToMssql::classify(&non_decimal_target, 4).unwrap_err();
        assert_single_diagnostic(
            err,
            DiagnosticCode::ValueTypeMismatch,
            Some(4),
            Some((1, "amount")),
        );
    }

    #[test]
    fn rejects_decimal_scale_mismatch() {
        let mapping = mapping(2, "amount", DataType::Decimal128(5, 2), decimal_type(5, 0));

        let err = DecimalArrowToMssql::classify(&mapping, 7).unwrap_err();

        assert_single_diagnostic(
            err,
            DiagnosticCode::SchemaMismatch,
            Some(7),
            Some((2, "amount")),
        );
    }

    #[test]
    fn rejects_tiberius_unrepresentable_target_scale() {
        let mapping = mapping(
            3,
            "amount",
            DataType::Decimal128(38, 38),
            decimal_type(38, 38),
        );

        let err = DecimalArrowToMssql::classify(&mapping, 11).unwrap_err();

        assert_single_diagnostic(
            err,
            DiagnosticCode::DecimalOutOfRange,
            Some(11),
            Some((3, "amount")),
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

    fn decimal_type(precision: u8, scale: i8) -> MssqlType {
        MssqlType::Decimal { precision, scale }
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

//! Support checks for the direct raw TDS encoder.

use arrow_schema::DataType;

use crate::{
    Diagnostic, DiagnosticCode, DiagnosticSet, Error, FieldRef, MssqlType, Result, SchemaMapping,
    conversion::arrow_to_mssql::primitive::PrimitiveArrowToMssql,
};

/// Support status for one planned mapping in the direct encoder.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum DirectMappingSupport {
    /// The planned mapping is supported by the current direct encoder.
    Supported {
        /// Concrete direct encoding selected for the mapping.
        encoding: DirectColumnEncoding,
    },
    /// The planned mapping is not supported by the current direct encoder.
    Unsupported {
        /// Human-readable reason the mapping is unsupported.
        reason: String,
    },
}

/// Concrete direct encoding selected for one planned column.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) enum DirectColumnEncoding {
    /// Fixed-width primitive encoding.
    Primitive(PrimitiveArrowToMssql),
}

/// Direct encoder support policy.
pub(crate) trait DirectEncoderSupport {
    /// Returns support status for a planned mapping.
    fn support_mapping(&self, mapping: &SchemaMapping) -> DirectMappingSupport;
}

/// Direct encoder support policy before concrete encoders are implemented.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash)]
pub(crate) struct NoDirectMappings;

impl DirectEncoderSupport for NoDirectMappings {
    fn support_mapping(&self, mapping: &SchemaMapping) -> DirectMappingSupport {
        DirectMappingSupport::Unsupported {
            reason: format!(
                "direct encoding is not implemented yet for Arrow {} to SQL Server {}",
                arrow_type_name(mapping.arrow().data_type()),
                mapping.mssql().ty().to_sql()
            ),
        }
    }
}

/// Direct encoder support policy for the first fixed-width primitive slice.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash)]
pub(crate) struct PrimitiveDirectMappings;

impl DirectEncoderSupport for PrimitiveDirectMappings {
    fn support_mapping(&self, mapping: &SchemaMapping) -> DirectMappingSupport {
        match PrimitiveArrowToMssql::classify(mapping, 0) {
            Ok(
                classification @ (PrimitiveArrowToMssql::BooleanToBit
                | PrimitiveArrowToMssql::Int32ToInt
                | PrimitiveArrowToMssql::Int64ToBigInt
                | PrimitiveArrowToMssql::Float64ToFloat),
            ) => DirectMappingSupport::Supported {
                encoding: DirectColumnEncoding::Primitive(classification),
            },
            Ok(classification) => DirectMappingSupport::Unsupported {
                reason: format!(
                    "direct encoding support for primitive mapping {classification:?} is not implemented yet"
                ),
            },
            Err(_) => DirectMappingSupport::Unsupported {
                reason: format!(
                    "direct encoding is not implemented yet for Arrow {} to SQL Server {}",
                    arrow_type_name(mapping.arrow().data_type()),
                    mapping.mssql().ty().to_sql()
                ),
            },
        }
    }
}

/// Planned direct encoder column.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct DirectColumnPlan {
    source_index: usize,
    source_name: String,
    target_type: MssqlType,
    nullable: bool,
    encoding: DirectColumnEncoding,
}

impl DirectColumnPlan {
    /// Creates a direct encoder column plan from a schema mapping.
    fn from_mapping(mapping: &SchemaMapping, encoding: DirectColumnEncoding) -> Self {
        Self {
            source_index: mapping.arrow().index(),
            source_name: mapping.arrow().name().to_owned(),
            target_type: mapping.mssql().ty().clone(),
            nullable: mapping.mssql().nullable(),
            encoding,
        }
    }

    /// Returns the Arrow source index.
    pub(crate) const fn source_index(&self) -> usize {
        self.source_index
    }

    /// Returns the Arrow source name.
    pub(crate) fn source_name(&self) -> &str {
        &self.source_name
    }

    /// Returns the planned SQL Server type.
    pub(crate) const fn target_type(&self) -> &MssqlType {
        &self.target_type
    }

    /// Returns true when the target column allows nulls.
    pub(crate) const fn nullable(&self) -> bool {
        self.nullable
    }

    /// Returns the selected direct encoding.
    pub(crate) const fn encoding(&self) -> DirectColumnEncoding {
        self.encoding
    }
}

/// Direct encoder support-checked plan.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct DirectEncoderPlan {
    columns: Vec<DirectColumnPlan>,
}

impl DirectEncoderPlan {
    /// Builds a direct encoder plan after validating all mappings are supported.
    pub(crate) fn new(
        mappings: &[SchemaMapping],
        support: &impl DirectEncoderSupport,
    ) -> Result<Self> {
        let mut diagnostics = DiagnosticSet::new();
        let mut columns = Vec::with_capacity(mappings.len());

        for mapping in mappings {
            match support.support_mapping(mapping) {
                DirectMappingSupport::Supported { encoding } => {
                    columns.push(DirectColumnPlan::from_mapping(mapping, encoding));
                }
                DirectMappingSupport::Unsupported { reason } => {
                    diagnostics.push(unsupported_mapping_diagnostic(mapping, reason));
                }
            }
        }

        if diagnostics.has_errors() {
            return Err(Error::DirectEncoding { diagnostics });
        }

        Ok(Self { columns })
    }

    /// Returns the checked direct columns.
    pub(crate) fn columns(&self) -> &[DirectColumnPlan] {
        &self.columns
    }

    /// Returns the number of checked direct columns.
    pub(crate) fn column_count(&self) -> usize {
        self.columns.len()
    }

    /// Returns true when there are no columns.
    pub(crate) fn is_empty(&self) -> bool {
        self.columns.is_empty()
    }
}

fn unsupported_mapping_diagnostic(mapping: &SchemaMapping, reason: String) -> Diagnostic {
    Diagnostic::error(DiagnosticCode::DirectEncodingUnsupportedMapping, reason).with_field(
        FieldRef::new(mapping.arrow().index(), mapping.arrow().name()),
    )
}

fn arrow_type_name(data_type: &DataType) -> String {
    data_type.to_string()
}

#[cfg(test)]
mod tests {
    use arrow_schema::DataType;

    use crate::{
        ArrowFieldRef, DiagnosticCode, Error, Identifier, MssqlColumn, MssqlType, SchemaMapping,
    };

    use super::{
        DirectColumnEncoding, DirectEncoderPlan, DirectEncoderSupport, DirectMappingSupport,
        NoDirectMappings, PrimitiveDirectMappings,
    };
    use crate::conversion::arrow_to_mssql::primitive::PrimitiveArrowToMssql;

    #[test]
    fn empty_mapping_set_is_supported_before_type_encoders_exist() {
        let plan = DirectEncoderPlan::new(&[], &NoDirectMappings).expect("empty plan is valid");

        assert!(plan.is_empty());
        assert_eq!(plan.column_count(), 0);
        assert_eq!(plan.columns(), []);
    }

    #[test]
    fn unsupported_mapping_returns_field_and_type_diagnostic() {
        let mappings = vec![mapping(0, "quantity", DataType::Int32, MssqlType::Int)];

        let err = DirectEncoderPlan::new(&mappings, &NoDirectMappings)
            .expect_err("default direct support should reject concrete mappings");

        let Error::DirectEncoding { diagnostics } = err else {
            panic!("expected direct encoding error");
        };

        assert_eq!(diagnostics.len(), 1);
        let diagnostic = &diagnostics.all()[0];
        assert_eq!(
            diagnostic.code(),
            DiagnosticCode::DirectEncodingUnsupportedMapping
        );
        assert_eq!(
            diagnostic.message(),
            "direct encoding is not implemented yet for Arrow Int32 to SQL Server int"
        );
        let field = diagnostic.field().expect("field should be attached");
        assert_eq!(field.index(), 0);
        assert_eq!(field.name(), "quantity");
    }

    #[test]
    fn collects_multiple_unsupported_mappings_in_schema_order() {
        let mappings = vec![
            mapping(0, "a", DataType::Int32, MssqlType::Int),
            mapping(1, "b", DataType::Boolean, MssqlType::Bit),
        ];

        let err = DirectEncoderPlan::new(&mappings, &NoDirectMappings)
            .expect_err("all concrete mappings are unsupported for now");

        let Error::DirectEncoding { diagnostics } = err else {
            panic!("expected direct encoding error");
        };

        assert_eq!(diagnostics.len(), 2);
        assert_eq!(diagnostics.all()[0].field().unwrap().name(), "a");
        assert_eq!(diagnostics.all()[1].field().unwrap().name(), "b");
    }

    #[test]
    fn supported_fixture_builds_column_plans_without_token_row_types() {
        let mappings = vec![
            mapping(0, "is_active", DataType::Boolean, MssqlType::Bit),
            mapping(1, "quantity", DataType::Int32, MssqlType::Int),
        ];

        let plan =
            DirectEncoderPlan::new(&mappings, &FixtureSupport).expect("fixture supports all");

        assert_eq!(plan.column_count(), 2);
        assert_eq!(plan.columns()[0].source_index(), 0);
        assert_eq!(plan.columns()[0].source_name(), "is_active");
        assert_eq!(plan.columns()[0].target_type(), &MssqlType::Bit);
        assert!(!plan.columns()[0].nullable());
        assert_eq!(
            plan.columns()[0].encoding(),
            DirectColumnEncoding::Primitive(PrimitiveArrowToMssql::BooleanToBit)
        );
        assert_eq!(plan.columns()[1].source_index(), 1);
        assert_eq!(plan.columns()[1].source_name(), "quantity");
        assert_eq!(plan.columns()[1].target_type(), &MssqlType::Int);
        assert!(!plan.columns()[1].nullable());
        assert_eq!(
            plan.columns()[1].encoding(),
            DirectColumnEncoding::Primitive(PrimitiveArrowToMssql::Int32ToInt)
        );
    }

    #[test]
    fn primitive_direct_support_accepts_only_issue_66_mappings() {
        let mappings = vec![
            mapping(0, "is_active", DataType::Boolean, MssqlType::Bit),
            mapping(1, "quantity", DataType::Int32, MssqlType::Int),
            mapping(2, "total", DataType::Int64, MssqlType::BigInt),
            mapping(
                3,
                "ratio",
                DataType::Float64,
                MssqlType::Float { precision: 53 },
            ),
        ];

        let plan = DirectEncoderPlan::new(&mappings, &PrimitiveDirectMappings)
            .expect("issue 66 primitive mappings should be supported");

        assert_eq!(plan.column_count(), 4);
        assert_eq!(plan.columns()[0].target_type(), &MssqlType::Bit);
        assert_eq!(plan.columns()[1].target_type(), &MssqlType::Int);
        assert_eq!(plan.columns()[2].target_type(), &MssqlType::BigInt);
        assert_eq!(
            plan.columns()[3].target_type(),
            &MssqlType::Float { precision: 53 }
        );
        assert_eq!(
            plan.columns()[0].encoding(),
            DirectColumnEncoding::Primitive(PrimitiveArrowToMssql::BooleanToBit)
        );
        assert_eq!(
            plan.columns()[1].encoding(),
            DirectColumnEncoding::Primitive(PrimitiveArrowToMssql::Int32ToInt)
        );
        assert_eq!(
            plan.columns()[2].encoding(),
            DirectColumnEncoding::Primitive(PrimitiveArrowToMssql::Int64ToBigInt)
        );
        assert_eq!(
            plan.columns()[3].encoding(),
            DirectColumnEncoding::Primitive(PrimitiveArrowToMssql::Float64ToFloat)
        );
    }

    #[test]
    fn primitive_direct_support_rejects_scalar_primitives_outside_issue_66() {
        let mappings = vec![
            mapping(0, "tiny", DataType::UInt8, MssqlType::TinyInt),
            mapping(1, "small", DataType::Int16, MssqlType::SmallInt),
            mapping(2, "unsigned", DataType::UInt32, MssqlType::BigInt),
            mapping(3, "real_value", DataType::Float32, MssqlType::Real),
        ];

        let err = DirectEncoderPlan::new(&mappings, &PrimitiveDirectMappings)
            .expect_err("non-issue-66 primitives are still unsupported");

        let Error::DirectEncoding { diagnostics } = err else {
            panic!("expected direct encoding error");
        };

        assert_eq!(diagnostics.len(), 4);
        for (index, diagnostic) in diagnostics.all().iter().enumerate() {
            assert_eq!(
                diagnostic.code(),
                DiagnosticCode::DirectEncodingUnsupportedMapping
            );
            assert_eq!(diagnostic.field().unwrap().index(), index);
            assert!(
                diagnostic
                    .message()
                    .contains("direct encoding support for primitive mapping")
            );
        }
    }

    #[test]
    fn primitive_direct_support_rejects_forged_float64_non_53_precision_mapping() {
        let mappings = vec![mapping(
            0,
            "ratio",
            DataType::Float64,
            MssqlType::Float { precision: 24 },
        )];

        let err = DirectEncoderPlan::new(&mappings, &PrimitiveDirectMappings)
            .expect_err("direct Float64 support requires SQL Server float(53)");

        let Error::DirectEncoding { diagnostics } = err else {
            panic!("expected direct encoding error");
        };

        assert_eq!(diagnostics.len(), 1);
        assert_eq!(
            diagnostics.all()[0].code(),
            DiagnosticCode::DirectEncodingUnsupportedMapping
        );
        assert_eq!(diagnostics.all()[0].field().unwrap().name(), "ratio");
    }

    #[test]
    fn primitive_direct_support_rejects_non_primitive_mapping_with_type_reason() {
        let mappings = vec![mapping(
            0,
            "name",
            DataType::Utf8,
            MssqlType::NVarChar(crate::MssqlTypeLength::Max),
        )];

        let err = DirectEncoderPlan::new(&mappings, &PrimitiveDirectMappings)
            .expect_err("string direct encoding is not in issue 66 scope");

        let Error::DirectEncoding { diagnostics } = err else {
            panic!("expected direct encoding error");
        };

        assert_eq!(diagnostics.len(), 1);
        assert_eq!(
            diagnostics.all()[0].code(),
            DiagnosticCode::DirectEncodingUnsupportedMapping
        );
        assert_eq!(
            diagnostics.all()[0].message(),
            "direct encoding is not implemented yet for Arrow Utf8 to SQL Server nvarchar(max)"
        );
    }

    #[derive(Debug, Clone, Copy)]
    struct FixtureSupport;

    impl DirectEncoderSupport for FixtureSupport {
        fn support_mapping(&self, mapping: &SchemaMapping) -> DirectMappingSupport {
            DirectMappingSupport::Supported {
                encoding: DirectColumnEncoding::Primitive(
                    PrimitiveArrowToMssql::classify(mapping, 0).unwrap(),
                ),
            }
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
}

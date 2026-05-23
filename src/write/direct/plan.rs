//! Support checks for the direct raw TDS encoder.

use arrow_schema::DataType;

use crate::{
    Diagnostic, DiagnosticCode, DiagnosticSet, Error, FieldRef, MssqlType, Result, SchemaMapping,
    conversion::arrow_to_mssql::{
        primitive::PrimitiveArrowToMssql, uint64::UInt64ArrowToMssql,
        variable_width::VariableWidthArrowToMssql,
    },
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
    /// Arrow UInt64 to SQL Server `decimal(20,0)`.
    UInt64Decimal20_0,
    /// Variable-width encoding.
    VariableWidth(VariableWidthArrowToMssql),
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

/// Direct encoder support policy for currently implemented direct mappings.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash)]
pub(crate) struct CurrentDirectMappings;

impl DirectEncoderSupport for CurrentDirectMappings {
    fn support_mapping(&self, mapping: &SchemaMapping) -> DirectMappingSupport {
        match PrimitiveArrowToMssql::classify(mapping, 0) {
            Ok(classification) => DirectMappingSupport::Supported {
                encoding: DirectColumnEncoding::Primitive(classification),
            },
            Err(_) => uint64_support(mapping),
        }
    }
}

fn uint64_support(mapping: &SchemaMapping) -> DirectMappingSupport {
    match UInt64ArrowToMssql::classify(mapping, 0) {
        Ok(UInt64ArrowToMssql::Decimal20_0) => DirectMappingSupport::Supported {
            encoding: DirectColumnEncoding::UInt64Decimal20_0,
        },
        Ok(UInt64ArrowToMssql::CheckedBigInt) => DirectMappingSupport::Supported {
            encoding: DirectColumnEncoding::Primitive(PrimitiveArrowToMssql::UInt64ToCheckedBigInt),
        },
        Err(_) => variable_width_support(mapping),
    }
}

fn variable_width_support(mapping: &SchemaMapping) -> DirectMappingSupport {
    match VariableWidthArrowToMssql::classify(mapping, 0) {
        Ok(
            classification @ (VariableWidthArrowToMssql::Utf8ToNVarChar { .. }
            | VariableWidthArrowToMssql::BinaryToVarBinary { .. }),
        ) => DirectMappingSupport::Supported {
            encoding: DirectColumnEncoding::VariableWidth(classification),
        },
        Ok(classification) => DirectMappingSupport::Unsupported {
            reason: format!(
                "direct encoding support for variable-width mapping {classification:?} is not implemented yet"
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
        ArrowFieldRef, DiagnosticCode, Error, Identifier, MssqlColumn, MssqlType, MssqlTypeLength,
        SchemaMapping,
    };

    use super::{
        CurrentDirectMappings, DirectColumnEncoding, DirectEncoderPlan, DirectEncoderSupport,
        DirectMappingSupport, NoDirectMappings,
    };
    use crate::conversion::arrow_to_mssql::{
        primitive::PrimitiveArrowToMssql, variable_width::VariableWidthArrowToMssql,
    };

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
    fn current_direct_support_accepts_implemented_primitive_mappings() {
        let mappings = vec![
            mapping(0, "is_active", DataType::Boolean, MssqlType::Bit),
            mapping(1, "tiny", DataType::UInt8, MssqlType::TinyInt),
            mapping(2, "signed_tiny", DataType::Int8, MssqlType::SmallInt),
            mapping(3, "small", DataType::Int16, MssqlType::SmallInt),
            mapping(4, "quantity", DataType::Int32, MssqlType::Int),
            mapping(5, "unsigned_medium", DataType::UInt16, MssqlType::Int),
            mapping(6, "total", DataType::Int64, MssqlType::BigInt),
            mapping(7, "unsigned_total", DataType::UInt32, MssqlType::BigInt),
            mapping(8, "unsigned_huge", DataType::UInt64, MssqlType::BigInt),
            mapping(9, "real_value", DataType::Float32, MssqlType::Real),
            mapping(
                10,
                "ratio",
                DataType::Float64,
                MssqlType::Float { precision: 53 },
            ),
        ];

        let plan = DirectEncoderPlan::new(&mappings, &CurrentDirectMappings)
            .expect("implemented primitive mappings should be supported");

        assert_eq!(plan.column_count(), 11);
        assert_eq!(plan.columns()[0].target_type(), &MssqlType::Bit);
        assert_eq!(plan.columns()[1].target_type(), &MssqlType::TinyInt);
        assert_eq!(plan.columns()[2].target_type(), &MssqlType::SmallInt);
        assert_eq!(plan.columns()[3].target_type(), &MssqlType::SmallInt);
        assert_eq!(plan.columns()[4].target_type(), &MssqlType::Int);
        assert_eq!(plan.columns()[5].target_type(), &MssqlType::Int);
        assert_eq!(plan.columns()[6].target_type(), &MssqlType::BigInt);
        assert_eq!(plan.columns()[7].target_type(), &MssqlType::BigInt);
        assert_eq!(plan.columns()[8].target_type(), &MssqlType::BigInt);
        assert_eq!(plan.columns()[9].target_type(), &MssqlType::Real);
        assert_eq!(
            plan.columns()[10].target_type(),
            &MssqlType::Float { precision: 53 }
        );
        assert_eq!(
            plan.columns()[0].encoding(),
            DirectColumnEncoding::Primitive(PrimitiveArrowToMssql::BooleanToBit)
        );
        assert_eq!(
            plan.columns()[1].encoding(),
            DirectColumnEncoding::Primitive(PrimitiveArrowToMssql::UInt8ToTinyInt)
        );
        assert_eq!(
            plan.columns()[2].encoding(),
            DirectColumnEncoding::Primitive(PrimitiveArrowToMssql::Int8ToSmallInt)
        );
        assert_eq!(
            plan.columns()[3].encoding(),
            DirectColumnEncoding::Primitive(PrimitiveArrowToMssql::Int16ToSmallInt)
        );
        assert_eq!(
            plan.columns()[4].encoding(),
            DirectColumnEncoding::Primitive(PrimitiveArrowToMssql::Int32ToInt)
        );
        assert_eq!(
            plan.columns()[5].encoding(),
            DirectColumnEncoding::Primitive(PrimitiveArrowToMssql::UInt16ToInt)
        );
        assert_eq!(
            plan.columns()[6].encoding(),
            DirectColumnEncoding::Primitive(PrimitiveArrowToMssql::Int64ToBigInt)
        );
        assert_eq!(
            plan.columns()[7].encoding(),
            DirectColumnEncoding::Primitive(PrimitiveArrowToMssql::UInt32ToBigInt)
        );
        assert_eq!(
            plan.columns()[8].encoding(),
            DirectColumnEncoding::Primitive(PrimitiveArrowToMssql::UInt64ToCheckedBigInt)
        );
        assert_eq!(
            plan.columns()[9].encoding(),
            DirectColumnEncoding::Primitive(PrimitiveArrowToMssql::Float32ToReal)
        );
        assert_eq!(
            plan.columns()[10].encoding(),
            DirectColumnEncoding::Primitive(PrimitiveArrowToMssql::Float64ToFloat)
        );
    }

    #[test]
    fn current_direct_support_accepts_uint64_decimal20_mapping() {
        let mappings = vec![mapping(
            0,
            "unsigned_huge",
            DataType::UInt64,
            MssqlType::Decimal {
                precision: 20,
                scale: 0,
            },
        )];

        let plan = DirectEncoderPlan::new(&mappings, &CurrentDirectMappings)
            .expect("UInt64 decimal20 direct encoding should be supported");

        assert_eq!(plan.column_count(), 1);
        assert_eq!(
            plan.columns()[0].encoding(),
            DirectColumnEncoding::UInt64Decimal20_0
        );
    }

    #[test]
    fn current_direct_support_rejects_forged_float64_non_53_precision_mapping() {
        let mappings = vec![mapping(
            0,
            "ratio",
            DataType::Float64,
            MssqlType::Float { precision: 24 },
        )];

        let err = DirectEncoderPlan::new(&mappings, &CurrentDirectMappings)
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
    fn current_direct_support_accepts_issue_67_variable_width_mappings() {
        let mappings = vec![
            mapping(
                0,
                "name",
                DataType::Utf8,
                MssqlType::NVarChar(MssqlTypeLength::Max),
            ),
            mapping(
                1,
                "bytes",
                DataType::Binary,
                MssqlType::VarBinary(MssqlTypeLength::Bounded(100)),
            ),
        ];

        let plan = DirectEncoderPlan::new(&mappings, &CurrentDirectMappings)
            .expect("issue 67 variable-width mappings should be supported by the plan");

        assert_eq!(plan.column_count(), 2);
        assert_eq!(
            plan.columns()[0].encoding(),
            DirectColumnEncoding::VariableWidth(VariableWidthArrowToMssql::Utf8ToNVarChar {
                length: MssqlTypeLength::Max,
            })
        );
        assert_eq!(
            plan.columns()[1].encoding(),
            DirectColumnEncoding::VariableWidth(VariableWidthArrowToMssql::BinaryToVarBinary {
                length: MssqlTypeLength::Bounded(100),
            })
        );
    }

    #[test]
    fn current_direct_support_rejects_large_variable_width_until_dedicated_slice() {
        let mappings = vec![
            mapping(
                0,
                "large_name",
                DataType::LargeUtf8,
                MssqlType::NVarChar(MssqlTypeLength::Max),
            ),
            mapping(
                1,
                "large_bytes",
                DataType::LargeBinary,
                MssqlType::VarBinary(MssqlTypeLength::Max),
            ),
        ];

        let err = DirectEncoderPlan::new(&mappings, &CurrentDirectMappings)
            .expect_err("large variable-width mappings are implemented in a later slice");

        let Error::DirectEncoding { diagnostics } = err else {
            panic!("expected direct encoding error");
        };

        assert_eq!(diagnostics.len(), 2);
        assert_eq!(
            diagnostics.all()[0].code(),
            DiagnosticCode::DirectEncodingUnsupportedMapping
        );
        assert_eq!(diagnostics.all()[0].field().unwrap().name(), "large_name");
        assert!(
            diagnostics.all()[0]
                .message()
                .contains("LargeUtf8ToNVarChar")
        );
        assert_eq!(diagnostics.all()[1].field().unwrap().name(), "large_bytes");
        assert!(
            diagnostics.all()[1]
                .message()
                .contains("LargeBinaryToVarBinary")
        );
    }

    #[test]
    fn current_direct_support_rejects_non_direct_mapping_with_type_reason() {
        let mappings = vec![mapping(0, "created_on", DataType::Date32, MssqlType::Date)];

        let err = DirectEncoderPlan::new(&mappings, &CurrentDirectMappings)
            .expect_err("date direct encoding is implemented in a later slice");

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
            "direct encoding is not implemented yet for Arrow Date32 to SQL Server date"
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

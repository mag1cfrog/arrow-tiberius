//! Direct raw TDS bulk encoder internals.
#![allow(dead_code)]

use arrow_array::RecordBatch;

use crate::{
    Diagnostic, DiagnosticCode, DiagnosticSet, Error, Result, SchemaMapping,
    write::record_batch::validate_runtime_columns,
};

pub(crate) mod layout;
pub(crate) mod payload;
pub(crate) mod plan;
pub(crate) mod primitive;

use payload::EncodedRowsPayload;
use plan::{DirectEncoderPlan, PrimitiveDirectMappings};
use primitive::{build_fixed_width_row_layout, measure_primitive_column_cell_lengths};

/// Direct raw TDS encoder facade.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct DirectEncoder {
    mappings: Vec<SchemaMapping>,
    plan: DirectEncoderPlan,
}

impl DirectEncoder {
    /// Creates a direct encoder using the current supported direct mappings.
    pub(crate) fn new(mappings: &[SchemaMapping]) -> Result<Self> {
        Self::new_with_support(mappings, &PrimitiveDirectMappings)
    }

    /// Creates a direct encoder using an explicit support checker.
    pub(crate) fn new_with_support(
        mappings: &[SchemaMapping],
        support: &impl plan::DirectEncoderSupport,
    ) -> Result<Self> {
        Ok(Self {
            mappings: mappings.to_vec(),
            plan: DirectEncoderPlan::new(mappings, support)?,
        })
    }

    /// Returns the checked schema mappings consumed by this encoder.
    pub(crate) fn mappings(&self) -> &[SchemaMapping] {
        &self.mappings
    }

    /// Returns the checked direct encoder plan.
    pub(crate) const fn plan(&self) -> &DirectEncoderPlan {
        &self.plan
    }

    /// Encodes a runtime batch into complete raw TDS row payload bytes.
    pub(crate) fn encode_batch(&self, batch: &RecordBatch) -> Result<EncodedRowsPayload> {
        validate_runtime_columns(batch, &self.mappings)?;

        if self.plan.is_empty() && batch.num_rows() == 0 {
            return EncodedRowsPayload::new(Vec::new(), Vec::new());
        }

        let _layout = self.measure_layout(batch)?;

        Err(unsupported_batch(
            "direct batch encoding requires concrete type encoders from follow-up issues",
        ))
    }

    fn measure_layout(&self, batch: &RecordBatch) -> Result<layout::RowLayout> {
        let row_count = batch.num_rows();
        let column_count = self.plan.column_count();

        if row_count == 0 {
            return layout::RowLayout::new(Vec::new(), Vec::new(), Vec::new(), 0);
        }

        let mut cell_lengths = vec![0; row_count * column_count];

        for (column_index, column) in self.plan.columns().iter().enumerate() {
            let Some(array) = batch
                .columns()
                .get(column.source_index())
                .map(AsRef::as_ref)
            else {
                return Err(value_conversion_error(row_column_diagnostic(
                    column,
                    0,
                    DiagnosticCode::ValueTypeMismatch,
                    "planned direct column index is outside the runtime batch",
                )));
            };

            measure_primitive_column_cell_lengths(
                array,
                column,
                column_index,
                column_count,
                &mut cell_lengths,
            )?;
        }

        build_fixed_width_row_layout(row_count, column_count, &cell_lengths)
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

fn row_column_diagnostic(
    column: &plan::DirectColumnPlan,
    row_index: usize,
    code: DiagnosticCode,
    message: impl Into<String>,
) -> Diagnostic {
    Diagnostic::error(code, message)
        .with_field(crate::FieldRef::new(
            column.source_index(),
            column.source_name(),
        ))
        .with_row(row_index)
}

fn value_conversion_error(diagnostic: Diagnostic) -> Error {
    Error::ValueConversion {
        diagnostics: DiagnosticSet::from(vec![diagnostic]),
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_array::{BooleanArray, RecordBatch};
    use arrow_schema::{DataType, Field, Schema};

    use crate::{
        ArrowFieldRef, DiagnosticCode, Error, Identifier, MssqlColumn, MssqlType, SchemaMapping,
        conversion::arrow_to_mssql::primitive::PrimitiveArrowToMssql,
    };

    use super::DirectEncoder;
    use super::plan::{DirectColumnEncoding, DirectEncoderSupport, DirectMappingSupport};

    #[test]
    fn default_direct_encoder_accepts_empty_mapping_set() {
        let encoder = DirectEncoder::new(&[]).expect("empty mapping set is supported");

        assert!(encoder.plan().is_empty());
        assert_eq!(encoder.plan().column_count(), 0);
        assert_eq!(encoder.mappings(), []);
    }

    #[test]
    fn default_direct_encoder_returns_empty_payload_for_empty_batch_and_empty_mapping_set() {
        let encoder = DirectEncoder::new(&[]).expect("empty mapping set is supported");
        let batch = RecordBatch::new_empty(Arc::new(Schema::empty()));

        let payload = encoder
            .encode_batch(&batch)
            .expect("empty batch should encode as empty payload");

        assert!(payload.is_empty());
        assert_eq!(payload.row_count(), 0);
    }

    #[test]
    fn default_direct_encoder_rejects_non_empty_row_batch_until_type_encoders_exist() {
        let mapping = SchemaMapping::new(
            ArrowFieldRef::new(0, "is_active".to_owned(), false, DataType::Boolean),
            MssqlColumn::new(Identifier::new("is_active").unwrap(), MssqlType::Bit, false),
        );
        let encoder =
            DirectEncoder::new_with_support(std::slice::from_ref(&mapping), &FixtureSupport)
                .expect("fixture supports the mapping");
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "is_active",
                DataType::Boolean,
                false,
            )])),
            vec![Arc::new(BooleanArray::from(vec![true]))],
        )
        .unwrap();

        let err = encoder
            .encode_batch(&batch)
            .expect_err("row encoding is not implemented yet");

        assert_unsupported_batch(err);
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

    fn assert_unsupported_batch(err: Error) {
        let Error::DirectEncoding { diagnostics } = err else {
            panic!("expected direct encoding error");
        };

        assert_eq!(diagnostics.len(), 1);
        assert_eq!(
            diagnostics.all()[0].code(),
            DiagnosticCode::DirectEncodingUnsupportedBatch
        );
    }
}

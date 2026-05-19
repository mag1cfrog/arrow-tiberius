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

use payload::EncodedRowsPayload;
use plan::{DirectEncoderPlan, NoDirectMappings};

/// Direct raw TDS encoder facade.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct DirectEncoder {
    mappings: Vec<SchemaMapping>,
    plan: DirectEncoderPlan,
}

impl DirectEncoder {
    /// Creates a direct encoder using the current supported direct mappings.
    pub(crate) fn new(mappings: &[SchemaMapping]) -> Result<Self> {
        Self::new_with_support(mappings, &NoDirectMappings)
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

        Err(unsupported_batch(
            "direct batch encoding requires concrete type encoders from follow-up issues",
        ))
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

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_array::{BooleanArray, RecordBatch};
    use arrow_schema::{DataType, Field, Schema};

    use crate::{
        ArrowFieldRef, DiagnosticCode, Error, Identifier, MssqlColumn, MssqlType, SchemaMapping,
    };

    use super::DirectEncoder;
    use super::plan::{DirectEncoderSupport, DirectMappingSupport};

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
        fn support_mapping(&self, _mapping: &SchemaMapping) -> DirectMappingSupport {
            DirectMappingSupport::Supported
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

//! Direct raw TDS bulk encoder facade and shared helpers.

use arrow_array::RecordBatch;

use crate::{
    Diagnostic, DiagnosticCode, DiagnosticSet, Error, MssqlProfile, PlanOptions, Result,
    SchemaMapping,
    write::{
        context::RuntimeConversionContext, record_batch::validate_record_batch_encoding_shape,
    },
};

use super::{
    MeasuredDirectBatch,
    payload::EncodedRowsPayload,
    plan::{CurrentDirectMappings, DirectColumnEncoding, DirectEncoderPlan},
    rows,
};

/// Direct raw TDS encoder facade.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct DirectEncoder {
    pub(crate) mappings: Vec<SchemaMapping>,
    pub(crate) runtime_context: RuntimeConversionContext,
    pub(crate) plan: DirectEncoderPlan,
}

impl DirectEncoder {
    /// Creates a direct encoder using the current supported direct mappings.
    pub(crate) fn new(mappings: &[SchemaMapping]) -> Result<Self> {
        Self::new_with_options(mappings, PlanOptions::default())
    }

    /// Creates a direct encoder using the current supported direct mappings and
    /// runtime conversion policies.
    pub(crate) fn new_with_options(
        mappings: &[SchemaMapping],
        plan_options: PlanOptions,
    ) -> Result<Self> {
        Self::new_with_context(
            mappings,
            RuntimeConversionContext::new(MssqlProfile::sql_server_2016_compat_100(), plan_options),
        )
    }

    /// Creates a direct encoder using the current supported direct mappings and
    /// runtime conversion context.
    pub(crate) fn new_with_context(
        mappings: &[SchemaMapping],
        runtime_context: RuntimeConversionContext,
    ) -> Result<Self> {
        Self::new_with_context_and_support(mappings, runtime_context, &CurrentDirectMappings)
    }

    /// Creates a direct encoder using an explicit support checker.
    pub(crate) fn new_with_support(
        mappings: &[SchemaMapping],
        support: &impl super::plan::DirectEncoderSupport,
    ) -> Result<Self> {
        Self::new_with_context_and_support(
            mappings,
            RuntimeConversionContext::new(
                MssqlProfile::sql_server_2016_compat_100(),
                PlanOptions::default(),
            ),
            support,
        )
    }

    pub(crate) fn new_with_context_and_support(
        mappings: &[SchemaMapping],
        runtime_context: RuntimeConversionContext,
        support: &impl super::plan::DirectEncoderSupport,
    ) -> Result<Self> {
        Ok(Self {
            mappings: mappings.to_vec(),
            runtime_context,
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

    /// Returns true when this encoder contains at least one variable-width column.
    pub(crate) fn has_variable_width_column(&self) -> bool {
        self.plan
            .columns()
            .iter()
            .any(|column| matches!(column.encoding(), DirectColumnEncoding::VariableWidth(_)))
    }

    /// Encodes a runtime batch into complete raw TDS row payload bytes.
    pub(crate) fn encode_batch(&self, batch: &RecordBatch) -> Result<EncodedRowsPayload> {
        rows::payload::encode_batch(self, batch)
    }

    /// Measures and validates a runtime batch without allocating encoded bytes.
    pub(crate) fn measure_batch(&self, batch: &RecordBatch) -> Result<MeasuredDirectBatch> {
        validate_record_batch_encoding_shape(batch, &self.mappings)?;

        let row_count = batch.num_rows();
        let column_count = self.plan.column_count();

        if row_count == 0 {
            return Ok(MeasuredDirectBatch::empty(column_count));
        }

        let cell_lengths = super::measure::measure_cell_lengths(self, batch)?;
        MeasuredDirectBatch::new(row_count, column_count, cell_lengths)
    }

    /// Encodes a contiguous row range from a runtime batch.
    ///
    /// Returned row-token offsets are relative to the returned payload, so the
    /// first non-empty range always starts at offset zero.
    pub(crate) fn encode_batch_range(
        &self,
        batch: &RecordBatch,
        start_row: usize,
        row_count: usize,
    ) -> Result<EncodedRowsPayload> {
        rows::payload::encode_batch_range(self, batch, start_row, row_count)
    }

    /// Encodes one range from a pre-measured direct batch.
    pub(crate) fn encode_measured_batch_range(
        &self,
        batch: &RecordBatch,
        measured: &MeasuredDirectBatch,
        start_row: usize,
        row_count: usize,
    ) -> Result<EncodedRowsPayload> {
        rows::payload::encode_measured_batch_range(self, batch, measured, start_row, row_count)
    }

    /// Encodes one measured range directly into a Tiberius raw rows buffer.
    pub(crate) fn encode_measured_batch_range_into(
        &self,
        batch: &RecordBatch,
        measured: &MeasuredDirectBatch,
        start_row: usize,
        row_count: usize,
        buf: &mut tiberius::RawRowsAppendBuffer<'_>,
    ) -> Result<tiberius::RawRowsAppend> {
        rows::append::encode_measured_batch_range_into(
            self, batch, measured, start_row, row_count, buf,
        )
    }

    pub(crate) fn mapping_for_column_index(&self, column_index: usize) -> Result<&SchemaMapping> {
        self.mappings.get(column_index).ok_or_else(|| {
            invalid_payload(format!(
                "direct mapping index {column_index} is outside mapping count {}",
                self.mappings.len()
            ))
        })
    }
}

pub(crate) fn unsupported_batch(message: impl Into<String>) -> Error {
    Error::DirectEncoding {
        diagnostics: DiagnosticSet::from(vec![Diagnostic::error(
            DiagnosticCode::DirectEncodingUnsupportedBatch,
            message,
        )]),
    }
}

pub(crate) fn invalid_payload(message: impl Into<String>) -> Error {
    Error::DirectEncoding {
        diagnostics: DiagnosticSet::from(vec![Diagnostic::error(
            DiagnosticCode::DirectEncodingInvalidPayload,
            message,
        )]),
    }
}

pub(crate) fn checked_add(lhs: usize, rhs: usize) -> Result<usize> {
    lhs.checked_add(rhs)
        .ok_or_else(|| invalid_payload("direct encoded length overflowed usize"))
}

pub(crate) fn row_column_diagnostic(
    column: &super::plan::DirectColumnPlan,
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

pub(crate) fn value_conversion_error(diagnostic: Diagnostic) -> Error {
    Error::ValueConversion {
        diagnostics: DiagnosticSet::from(vec![diagnostic]),
    }
}

use std::time::Instant;

use crate::{
    TableName,
    observability::{
        TARGET_METADATA_VALIDATION_COMPLETED_EVENT, TARGET_METADATA_VALIDATION_FAILED_EVENT,
        TARGET_METADATA_VALIDATION_PHASE, TARGET_METADATA_VALIDATION_STARTED_EVENT, TRACE_TARGET,
        WRITER_INITIALIZATION_COMPLETED_EVENT, WRITER_INITIALIZATION_FAILED_EVENT,
        WRITER_INITIALIZATION_PHASE, WRITER_INITIALIZATION_SPAN,
        WRITER_INITIALIZATION_STARTED_EVENT, duration_micros_u64, usize_to_u64_saturating,
    },
    write::writer::WriteBackend,
};

use super::{backend_trace_name, diagnostic_codes_for_error, sanitized_error_summary};

#[derive(Debug)]
pub(crate) struct WriterInitializationTrace {
    span: tracing::Span,
    started: Instant,
    requested_backend: WriteBackend,
    resolved_backend: Option<WriteBackend>,
    target_schema: String,
    target_table: String,
    planned_column_count: usize,
    direct_target_validation_required: Option<bool>,
}

impl WriterInitializationTrace {
    pub(crate) fn new(
        table: &TableName,
        requested_backend: WriteBackend,
        planned_column_count: usize,
    ) -> Self {
        let target_schema = table
            .schema()
            .map(|schema| schema.as_str().to_owned())
            .unwrap_or_default();
        let target_table = table.table().as_str().to_owned();
        let span = tracing::info_span!(
            target: TRACE_TARGET,
            WRITER_INITIALIZATION_SPAN,
            phase = WRITER_INITIALIZATION_PHASE,
            requested_backend = backend_trace_name(requested_backend),
            resolved_backend = tracing::field::Empty,
            target_schema = target_schema.as_str(),
            target_table = target_table.as_str(),
            planned_column_count = usize_to_u64_saturating(planned_column_count),
            direct_target_validation_required = tracing::field::Empty,
        );

        Self {
            span,
            started: Instant::now(),
            requested_backend,
            resolved_backend: None,
            target_schema,
            target_table,
            planned_column_count,
            direct_target_validation_required: None,
        }
    }

    pub(crate) fn emit_started(&self) {
        let _span_guard = self.span.enter();
        tracing::info!(
            target: TRACE_TARGET,
            phase = WRITER_INITIALIZATION_PHASE,
            telemetry_event = WRITER_INITIALIZATION_STARTED_EVENT,
            requested_backend = backend_trace_name(self.requested_backend),
            target_schema = self.target_schema.as_str(),
            target_table = self.target_table.as_str(),
            planned_column_count = usize_to_u64_saturating(self.planned_column_count),
            initialization_result = "started"
        );
    }

    pub(crate) fn record_resolved_backend(&mut self, resolved_backend: WriteBackend) {
        self.span
            .record("resolved_backend", backend_trace_name(resolved_backend));
        self.resolved_backend = Some(resolved_backend);
    }

    pub(crate) fn record_direct_target_validation_required(&mut self, required: bool) {
        self.span
            .record("direct_target_validation_required", required);
        self.direct_target_validation_required = Some(required);
    }

    pub(crate) fn emit_target_metadata_validation_started(&self) {
        let _span_guard = self.span.enter();
        tracing::info!(
            target: TRACE_TARGET,
            phase = TARGET_METADATA_VALIDATION_PHASE,
            telemetry_event = TARGET_METADATA_VALIDATION_STARTED_EVENT,
            requested_backend = backend_trace_name(self.requested_backend),
            resolved_backend = self.resolved_backend_name(),
            target_schema = self.target_schema.as_str(),
            target_table = self.target_table.as_str(),
            planned_column_count = usize_to_u64_saturating(self.planned_column_count),
            direct_target_validation_required = self.direct_target_validation_required(),
            initialization_result = "validating"
        );
    }

    pub(crate) fn emit_target_metadata_validation_completed(&self) {
        let _span_guard = self.span.enter();
        tracing::info!(
            target: TRACE_TARGET,
            phase = TARGET_METADATA_VALIDATION_PHASE,
            telemetry_event = TARGET_METADATA_VALIDATION_COMPLETED_EVENT,
            requested_backend = backend_trace_name(self.requested_backend),
            resolved_backend = self.resolved_backend_name(),
            target_schema = self.target_schema.as_str(),
            target_table = self.target_table.as_str(),
            planned_column_count = usize_to_u64_saturating(self.planned_column_count),
            direct_target_validation_required = self.direct_target_validation_required(),
            initialization_result = "success",
            elapsed_us = duration_micros_u64(self.started.elapsed())
        );
    }

    pub(crate) fn emit_completed(&self) {
        let _span_guard = self.span.enter();
        tracing::info!(
            target: TRACE_TARGET,
            phase = WRITER_INITIALIZATION_PHASE,
            telemetry_event = WRITER_INITIALIZATION_COMPLETED_EVENT,
            requested_backend = backend_trace_name(self.requested_backend),
            resolved_backend = self.resolved_backend_name(),
            target_schema = self.target_schema.as_str(),
            target_table = self.target_table.as_str(),
            planned_column_count = usize_to_u64_saturating(self.planned_column_count),
            direct_target_validation_required = self.direct_target_validation_required(),
            initialization_result = "success",
            elapsed_us = duration_micros_u64(self.started.elapsed())
        );
    }

    pub(crate) fn emit_failed(&self, phase: &'static str, error: &crate::Error) {
        let telemetry_event = if phase == TARGET_METADATA_VALIDATION_PHASE {
            TARGET_METADATA_VALIDATION_FAILED_EVENT
        } else {
            WRITER_INITIALIZATION_FAILED_EVENT
        };
        let diagnostic_codes = diagnostic_codes_for_error(error);
        let _span_guard = self.span.enter();
        tracing::error!(
            target: TRACE_TARGET,
            phase,
            telemetry_event,
            requested_backend = backend_trace_name(self.requested_backend),
            resolved_backend = self.resolved_backend_name(),
            target_schema = self.target_schema.as_str(),
            target_table = self.target_table.as_str(),
            planned_column_count = usize_to_u64_saturating(self.planned_column_count),
            direct_target_validation_required = self.direct_target_validation_required(),
            initialization_result = "failure",
            error_summary = sanitized_error_summary(error),
            diagnostic_codes = diagnostic_codes.as_str(),
            elapsed_us = duration_micros_u64(self.started.elapsed())
        );
    }

    fn resolved_backend_name(&self) -> &'static str {
        self.resolved_backend
            .map(backend_trace_name)
            .unwrap_or("unresolved")
    }

    fn direct_target_validation_required(&self) -> bool {
        self.direct_target_validation_required.unwrap_or(false)
    }
}

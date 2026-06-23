//! Writer tracing helpers.

use std::time::{Duration, Instant};

use arrow_array::RecordBatch;

use crate::{
    DiagnosticCode, TableName,
    write::{
        direct::{MeasuredDirectBatch, MeasuredRowRange},
        writer::{WriteBackend, WriteStats},
    },
};

use super::{
    BATCH_WRITE_COMPLETED_EVENT, BATCH_WRITE_FAILED_EVENT, BATCH_WRITE_PHASE, BATCH_WRITE_SPAN,
    BATCH_WRITE_STARTED_EVENT, DIRECT_ENCODING_PHASE, DIRECT_RAW_FAILED_EVENT,
    DIRECT_RAW_MEASURED_EVENT, DIRECT_RAW_PACKET_WRITE_COMPLETED_EVENT,
    DIRECT_RAW_RANGES_PLANNED_EVENT, FINALIZE_PHASE, FINISH_COMPLETED_EVENT, FINISH_FAILED_EVENT,
    FINISH_PHASE, FINISH_SPAN, FINISH_STARTED_EVENT, PACKET_WRITE_PHASE,
    TARGET_METADATA_VALIDATION_COMPLETED_EVENT, TARGET_METADATA_VALIDATION_FAILED_EVENT,
    TARGET_METADATA_VALIDATION_PHASE, TARGET_METADATA_VALIDATION_STARTED_EVENT, TRACE_TARGET,
    WRITER_INITIALIZATION_COMPLETED_EVENT, WRITER_INITIALIZATION_FAILED_EVENT,
    WRITER_INITIALIZATION_PHASE, WRITER_INITIALIZATION_SPAN, WRITER_INITIALIZATION_STARTED_EVENT,
    diagnostic_codes, duration_micros_u64, usize_to_u64_saturating,
};

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

#[derive(Debug)]
pub(crate) struct BatchWriteTrace {
    span: tracing::Span,
    started: Instant,
    backend: WriteBackend,
    attempted_batch_ordinal: u64,
    batch_row_count: u64,
    batch_column_count: u64,
    batch_is_empty: bool,
    accepted_rows_before: u64,
    accepted_batches_before: u64,
}

impl BatchWriteTrace {
    pub(crate) fn new(backend: WriteBackend, stats: WriteStats, batch: &RecordBatch) -> Self {
        let attempted_batch_ordinal = stats.batches_written.saturating_add(1);
        let batch_row_count = usize_to_u64_saturating(batch.num_rows());
        let batch_column_count = usize_to_u64_saturating(batch.num_columns());
        let batch_is_empty = batch.num_rows() == 0;
        let span = tracing::info_span!(
            target: TRACE_TARGET,
            BATCH_WRITE_SPAN,
            phase = BATCH_WRITE_PHASE,
            backend = backend_trace_name(backend),
            attempted_batch_ordinal,
            batch_row_count,
            batch_column_count,
            batch_is_empty,
            accepted_rows_before = stats.rows_written,
            accepted_batches_before = stats.batches_written,
        );

        Self {
            span,
            started: Instant::now(),
            backend,
            attempted_batch_ordinal,
            batch_row_count,
            batch_column_count,
            batch_is_empty,
            accepted_rows_before: stats.rows_written,
            accepted_batches_before: stats.batches_written,
        }
    }

    pub(crate) fn emit_started(&self) {
        let _span_guard = self.span.enter();
        tracing::info!(
            target: TRACE_TARGET,
            phase = BATCH_WRITE_PHASE,
            telemetry_event = BATCH_WRITE_STARTED_EVENT,
            backend = backend_trace_name(self.backend),
            attempted_batch_ordinal = self.attempted_batch_ordinal,
            batch_row_count = self.batch_row_count,
            batch_column_count = self.batch_column_count,
            batch_is_empty = self.batch_is_empty,
            accepted_rows_before = self.accepted_rows_before,
            accepted_batches_before = self.accepted_batches_before,
            batch_write_result = "started"
        );
    }

    pub(crate) fn emit_completed(&self, stats: WriteStats) {
        let _span_guard = self.span.enter();
        tracing::info!(
            target: TRACE_TARGET,
            phase = BATCH_WRITE_PHASE,
            telemetry_event = BATCH_WRITE_COMPLETED_EVENT,
            backend = backend_trace_name(self.backend),
            attempted_batch_ordinal = self.attempted_batch_ordinal,
            batch_row_count = self.batch_row_count,
            batch_column_count = self.batch_column_count,
            batch_is_empty = self.batch_is_empty,
            accepted_rows_before = self.accepted_rows_before,
            accepted_batches_before = self.accepted_batches_before,
            accepted_rows_after = stats.rows_written,
            accepted_batches_after = stats.batches_written,
            batch_write_result = "success",
            elapsed_us = duration_micros_u64(self.started.elapsed())
        );
    }

    pub(crate) fn emit_failed(&self, phase: &'static str, error: &crate::Error) {
        let diagnostic_codes = diagnostic_codes_for_error(error);
        let _span_guard = self.span.enter();
        tracing::error!(
            target: TRACE_TARGET,
            phase,
            telemetry_event = BATCH_WRITE_FAILED_EVENT,
            backend = backend_trace_name(self.backend),
            attempted_batch_ordinal = self.attempted_batch_ordinal,
            batch_row_count = self.batch_row_count,
            batch_column_count = self.batch_column_count,
            batch_is_empty = self.batch_is_empty,
            accepted_rows_before = self.accepted_rows_before,
            accepted_batches_before = self.accepted_batches_before,
            batch_write_result = "failure",
            error_summary = sanitized_error_summary(error),
            diagnostic_codes = diagnostic_codes.as_str(),
            elapsed_us = duration_micros_u64(self.started.elapsed())
        );
    }
}

#[derive(Debug)]
pub(crate) struct FinishTrace {
    span: tracing::Span,
    started: Instant,
    backend: WriteBackend,
    stats: WriteStats,
}

impl FinishTrace {
    pub(crate) fn new(backend: WriteBackend, stats: WriteStats) -> Self {
        let span = tracing::info_span!(
            target: TRACE_TARGET,
            FINISH_SPAN,
            phase = FINISH_PHASE,
            backend = backend_trace_name(backend),
            rows_written = stats.rows_written,
            batches_written = stats.batches_written,
        );

        Self {
            span,
            started: Instant::now(),
            backend,
            stats,
        }
    }

    pub(crate) fn emit_started(&self) {
        let _span_guard = self.span.enter();
        tracing::info!(
            target: TRACE_TARGET,
            phase = FINISH_PHASE,
            telemetry_event = FINISH_STARTED_EVENT,
            backend = backend_trace_name(self.backend),
            rows_written = self.stats.rows_written,
            batches_written = self.stats.batches_written,
            finish_result = "started"
        );
    }

    pub(crate) fn emit_completed(&self) {
        let _span_guard = self.span.enter();
        tracing::info!(
            target: TRACE_TARGET,
            phase = FINISH_PHASE,
            telemetry_event = FINISH_COMPLETED_EVENT,
            backend = backend_trace_name(self.backend),
            rows_written = self.stats.rows_written,
            batches_written = self.stats.batches_written,
            finish_result = "success",
            elapsed_us = duration_micros_u64(self.started.elapsed())
        );
    }

    pub(crate) fn emit_failed(&self, error: &crate::Error) {
        let diagnostic_codes = diagnostic_codes_for_error(error);
        let _span_guard = self.span.enter();
        tracing::error!(
            target: TRACE_TARGET,
            phase = FINALIZE_PHASE,
            telemetry_event = FINISH_FAILED_EVENT,
            backend = backend_trace_name(self.backend),
            accepted_rows_before = self.stats.rows_written,
            accepted_batches_before = self.stats.batches_written,
            finish_result = "failure",
            error_summary = sanitized_error_summary(error),
            diagnostic_codes = diagnostic_codes.as_str(),
            elapsed_us = duration_micros_u64(self.started.elapsed())
        );
    }
}

pub(crate) fn emit_direct_raw_measured(
    backend: WriteBackend,
    measured: &MeasuredDirectBatch,
    elapsed: Duration,
) {
    if backend != WriteBackend::DirectRawBulk {
        return;
    }

    tracing::debug!(
        target: TRACE_TARGET,
        phase = DIRECT_ENCODING_PHASE,
        telemetry_event = DIRECT_RAW_MEASURED_EVENT,
        backend = backend_trace_name(backend),
        batch_row_count = usize_to_u64_saturating(measured.row_count()),
        batch_column_count = usize_to_u64_saturating(measured.column_count()),
        encoded_row_count = usize_to_u64_saturating(measured.row_count()),
        encoded_byte_count = usize_to_u64_saturating(measured.payload_len()),
        elapsed_us = duration_micros_u64(elapsed)
    );
}

pub(crate) fn emit_direct_raw_ranges_planned(
    backend: WriteBackend,
    measured: &MeasuredDirectBatch,
    ranges: &[MeasuredRowRange],
    elapsed: Duration,
) {
    if backend != WriteBackend::DirectRawBulk {
        return;
    }

    tracing::debug!(
        target: TRACE_TARGET,
        phase = DIRECT_ENCODING_PHASE,
        telemetry_event = DIRECT_RAW_RANGES_PLANNED_EVENT,
        backend = backend_trace_name(backend),
        batch_row_count = usize_to_u64_saturating(measured.row_count()),
        batch_column_count = usize_to_u64_saturating(measured.column_count()),
        encoded_byte_count = usize_to_u64_saturating(measured.payload_len()),
        encoded_range_count = usize_to_u64_saturating(ranges.len()),
        elapsed_us = duration_micros_u64(elapsed)
    );
}

pub(crate) fn emit_direct_raw_packet_write_completed(
    backend: WriteBackend,
    range: MeasuredRowRange,
    encoded_row_count: usize,
    encoded_byte_count: usize,
    elapsed: Duration,
) {
    if backend != WriteBackend::DirectRawBulk {
        return;
    }

    // `tiberius-raw-bulk` does not expose raw packet counts on the normal
    // write path. Emit safe range and byte summaries instead of guessing.
    tracing::debug!(
        target: TRACE_TARGET,
        phase = PACKET_WRITE_PHASE,
        telemetry_event = DIRECT_RAW_PACKET_WRITE_COMPLETED_EVENT,
        backend = backend_trace_name(backend),
        encoded_row_start = usize_to_u64_saturating(range.start),
        encoded_row_count = usize_to_u64_saturating(encoded_row_count),
        encoded_byte_count = usize_to_u64_saturating(encoded_byte_count),
        elapsed_us = duration_micros_u64(elapsed)
    );
}

pub(crate) fn emit_direct_raw_failed(
    backend: WriteBackend,
    phase: &'static str,
    batch: &RecordBatch,
    range: Option<MeasuredRowRange>,
    error: &crate::Error,
) {
    if backend != WriteBackend::DirectRawBulk {
        return;
    }

    let diagnostic_codes = diagnostic_codes_for_error(error);
    tracing::error!(
        target: TRACE_TARGET,
        phase,
        telemetry_event = DIRECT_RAW_FAILED_EVENT,
        backend = backend_trace_name(backend),
        batch_row_count = usize_to_u64_saturating(batch.num_rows()),
        batch_column_count = usize_to_u64_saturating(batch.num_columns()),
        encoded_row_start = range.map(|range| usize_to_u64_saturating(range.start)),
        encoded_row_count = range.map(|range| usize_to_u64_saturating(range.len)),
        diagnostic_codes = diagnostic_codes.as_str(),
        error_summary = sanitized_error_summary(error)
    );
}

fn backend_trace_name(backend: WriteBackend) -> &'static str {
    match backend {
        WriteBackend::Auto => "Auto",
        WriteBackend::BaselineTokenRow => "BaselineTokenRow",
        WriteBackend::DirectFramedBulk => "DirectFramedBulk",
        WriteBackend::DirectRawBulk => "DirectRawBulk",
    }
}

fn sanitized_error_summary(error: &crate::Error) -> &'static str {
    match error {
        crate::Error::WritePhaseContext { source, .. } => sanitized_error_summary(source),
        crate::Error::InvalidCompatibilityLevel { .. } => "invalid compatibility level",
        crate::Error::InvalidIdentifier { .. } => "invalid identifier",
        crate::Error::Planning { .. } => "planning failed with diagnostics",
        crate::Error::ValueConversion { .. } => "value conversion failed with diagnostics",
        crate::Error::DirectEncoding { .. } => "direct encoding failed with diagnostics",
        crate::Error::BackendUnavailable { .. } => "write backend unavailable",
        crate::Error::InvalidConnectionString => "invalid connection string",
        crate::Error::ConnectionTcpConnect { .. } => "TCP connection failed",
        crate::Error::ConnectionClientSetup { .. } => "SQL Server client setup failed",
        crate::Error::TableExistsQuery { .. } => "table existence query failed",
        crate::Error::TableExistsUnexpectedResult { .. } => {
            "table existence query returned unexpected result"
        }
        crate::Error::SqlExecution { .. } => "SQL statement execution failed",
        crate::Error::Tiberius { .. } => "tiberius operation failed",
    }
}

fn diagnostic_codes_for_error(error: &crate::Error) -> String {
    match error {
        crate::Error::WritePhaseContext { source, .. } => diagnostic_codes_for_error(source),
        crate::Error::Planning { diagnostics }
        | crate::Error::ValueConversion { diagnostics }
        | crate::Error::DirectEncoding { diagnostics } => diagnostic_codes(diagnostics),
        crate::Error::BackendUnavailable { .. } => {
            format!("{:?}", DiagnosticCode::BackendUnavailable)
        }
        _ => String::new(),
    }
}

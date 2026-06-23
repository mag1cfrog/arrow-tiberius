//! Writer tracing helpers.

use std::time::{Duration, Instant};

use arrow_array::RecordBatch;

use crate::{
    DiagnosticCode,
    write::{
        direct::{MeasuredDirectBatch, MeasuredRowRange},
        writer::{WriteBackend, WriteStats},
    },
};

mod batch;
mod initialization;

pub(crate) use batch::BatchWriteTrace;
pub(crate) use initialization::WriterInitializationTrace;

use super::{
    DIRECT_ENCODING_PHASE, DIRECT_RAW_FAILED_EVENT, DIRECT_RAW_MEASURED_EVENT,
    DIRECT_RAW_PACKET_WRITE_COMPLETED_EVENT, DIRECT_RAW_RANGES_PLANNED_EVENT, FINALIZE_PHASE,
    FINISH_COMPLETED_EVENT, FINISH_FAILED_EVENT, FINISH_PHASE, FINISH_SPAN, FINISH_STARTED_EVENT,
    PACKET_WRITE_PHASE, TRACE_TARGET, diagnostic_codes, duration_micros_u64,
    usize_to_u64_saturating,
};

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

pub(super) fn backend_trace_name(backend: WriteBackend) -> &'static str {
    match backend {
        WriteBackend::Auto => "Auto",
        WriteBackend::BaselineTokenRow => "BaselineTokenRow",
        WriteBackend::DirectFramedBulk => "DirectFramedBulk",
        WriteBackend::DirectRawBulk => "DirectRawBulk",
    }
}

pub(super) fn sanitized_error_summary(error: &crate::Error) -> &'static str {
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

pub(super) fn diagnostic_codes_for_error(error: &crate::Error) -> String {
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

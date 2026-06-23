use std::time::Instant;

use arrow_array::RecordBatch;

use crate::{
    observability::{
        BATCH_WRITE_COMPLETED_EVENT, BATCH_WRITE_FAILED_EVENT, BATCH_WRITE_PHASE, BATCH_WRITE_SPAN,
        BATCH_WRITE_STARTED_EVENT, TRACE_TARGET, duration_micros_u64, usize_to_u64_saturating,
    },
    write::writer::{WriteBackend, WriteStats},
};

use super::{backend_trace_name, diagnostic_codes_for_error, sanitized_error_summary};

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

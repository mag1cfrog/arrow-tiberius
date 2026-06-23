use std::time::Instant;

use crate::{
    observability::{
        FINALIZE_PHASE, FINISH_COMPLETED_EVENT, FINISH_FAILED_EVENT, FINISH_PHASE, FINISH_SPAN,
        FINISH_STARTED_EVENT, TRACE_TARGET, duration_micros_u64,
    },
    write::writer::{WriteBackend, WriteStats},
};

use super::{backend_trace_name, diagnostic_codes_for_error, sanitized_error_summary};

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

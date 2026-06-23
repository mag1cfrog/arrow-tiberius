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

#[cfg(test)]
mod tests {
    use std::borrow::Cow;

    use super::FinishTrace;
    use crate::{
        Error,
        observability::{
            FINALIZE_PHASE, FINISH_COMPLETED_EVENT, FINISH_FAILED_EVENT, FINISH_PHASE, FINISH_SPAN,
            test_support::{assert_trace_field, capture_traces, trace_event},
        },
        write::writer::{WriteBackend, WriteStats},
    };

    #[test]
    fn finish_success_emits_final_stats_trace() -> Result<(), String> {
        let stats = WriteStats {
            rows_written: 5,
            batches_written: 2,
        };

        let (_result, traces) = capture_traces(|| {
            let trace = FinishTrace::new(WriteBackend::BaselineTokenRow, stats);
            trace.emit_started();
            trace.emit_completed();
        });

        let records = traces.records()?;
        let event = trace_event(&records, FINISH_COMPLETED_EVENT)?;
        assert_eq!(event.span_name(), Some(FINISH_SPAN));
        assert_trace_field(event, "phase", FINISH_PHASE);
        assert_trace_field(event, "backend", "BaselineTokenRow");
        assert_trace_field(event, "rows_written", "5");
        assert_trace_field(event, "batches_written", "2");
        assert_trace_field(event, "finish_result", "success");
        assert!(event.fields().contains_key("elapsed_us"));

        Ok(())
    }

    #[test]
    fn finish_failure_emits_finalize_trace_with_accepted_stats() -> Result<(), String> {
        let stats = WriteStats {
            rows_written: 7,
            batches_written: 1,
        };
        let error = Error::Tiberius {
            source: tiberius::error::Error::BulkInput(Cow::Borrowed("fake finalize failure")),
        };

        let (_result, traces) = capture_traces(|| {
            let trace = FinishTrace::new(WriteBackend::DirectRawBulk, stats);
            trace.emit_started();
            trace.emit_failed(&error);
        });

        let records = traces.records()?;
        let event = trace_event(&records, FINISH_FAILED_EVENT)?;
        assert_eq!(event.span_name(), Some(FINISH_SPAN));
        assert_trace_field(event, "phase", FINALIZE_PHASE);
        assert_trace_field(event, "backend", "DirectRawBulk");
        assert_trace_field(event, "accepted_rows_before", "7");
        assert_trace_field(event, "accepted_batches_before", "1");
        assert_trace_field(event, "finish_result", "failure");
        assert_trace_field(event, "error_summary", "tiberius operation failed");
        assert_trace_field(event, "diagnostic_codes", "");
        assert!(event.fields().contains_key("elapsed_us"));

        Ok(())
    }

    #[test]
    fn finish_failure_trace_is_sanitized() -> Result<(), String> {
        let error = Error::Tiberius {
            source: tiberius::error::Error::BulkInput(Cow::Borrowed(
                "server=tcp:sql.example.com;User ID=sa;password=secret",
            )),
        };

        let (_result, traces) = capture_traces(|| {
            let trace = FinishTrace::new(WriteBackend::DirectRawBulk, WriteStats::default());
            trace.emit_failed(&error);
        });

        let records = traces.records()?;
        let event = trace_event(&records, FINISH_FAILED_EVENT)?;
        assert_trace_field(event, "error_summary", "tiberius operation failed");
        traces.assert_no_forbidden_text(&[
            "server=tcp:sql.example.com",
            "User ID=sa",
            "password=secret",
        ])?;

        Ok(())
    }
}

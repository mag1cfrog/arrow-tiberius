use std::{future::Future, time::Instant};

use arrow_array::RecordBatch;
use tracing::Instrument as _;

use crate::{
    Result, WritePhase,
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

    /// Emits batch lifecycle events while polling the write future inside this span.
    pub(crate) async fn trace_result<F>(&self, future: F) -> Result<WriteStats>
    where
        F: Future<Output = Result<WriteStats>>,
    {
        self.emit_started();
        let result = future.instrument(self.span.clone()).await;
        match &result {
            Ok(stats) => self.emit_completed(*stats),
            Err(err) => {
                let phase = err.write_phase().unwrap_or(WritePhase::BatchWrite);
                self.emit_failed(phase.as_str(), err);
            }
        }
        result
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

#[cfg(test)]
mod tests {
    use std::{borrow::Cow, sync::Arc};

    use arrow_array::{Int32Array, RecordBatch, StringArray};
    use arrow_schema::{DataType, Field, Schema};

    use super::BatchWriteTrace;
    use crate::{
        Diagnostic, DiagnosticCode, DiagnosticSet, Error,
        observability::{
            BATCH_SCHEMA_VALIDATION_PHASE, BATCH_WRITE_COMPLETED_EVENT, BATCH_WRITE_FAILED_EVENT,
            BATCH_WRITE_PHASE, BATCH_WRITE_SPAN, PACKET_WRITE_PHASE, VALUE_CONVERSION_PHASE,
            test_support::{assert_trace_field, capture_traces, trace_event},
        },
        write::writer::{WriteBackend, WriteStats},
    };

    #[test]
    fn baseline_batch_write_success_emits_stats_trace() -> Result<(), String> {
        let batch = int32_batch("id", &[10, 20]);
        let stats_before = WriteStats::default();
        let stats_after = WriteStats {
            rows_written: 2,
            batches_written: 1,
        };

        let (_result, traces) = capture_traces(|| {
            let trace = BatchWriteTrace::new(WriteBackend::BaselineTokenRow, stats_before, &batch);
            trace.emit_started();
            trace.emit_completed(stats_after);
        });

        let records = traces.records()?;
        let event = trace_event(&records, BATCH_WRITE_COMPLETED_EVENT)?;
        assert_eq!(event.span_name(), Some(BATCH_WRITE_SPAN));
        assert_trace_field(event, "phase", BATCH_WRITE_PHASE);
        assert_trace_field(event, "backend", "BaselineTokenRow");
        assert_trace_field(event, "attempted_batch_ordinal", "1");
        assert_trace_field(event, "batch_row_count", "2");
        assert_trace_field(event, "batch_column_count", "1");
        assert_trace_field(event, "batch_is_empty", "false");
        assert_trace_field(event, "accepted_rows_before", "0");
        assert_trace_field(event, "accepted_batches_before", "0");
        assert_trace_field(event, "accepted_rows_after", "2");
        assert_trace_field(event, "accepted_batches_after", "1");
        assert_trace_field(event, "batch_write_result", "success");
        assert!(event.fields().contains_key("elapsed_us"));

        Ok(())
    }

    #[test]
    fn empty_batch_write_success_trace_matches_stats() -> Result<(), String> {
        let batch = int32_batch("id", &[]);
        let stats_after = WriteStats {
            rows_written: 0,
            batches_written: 1,
        };

        let (_result, traces) = capture_traces(|| {
            let trace = BatchWriteTrace::new(
                WriteBackend::BaselineTokenRow,
                WriteStats::default(),
                &batch,
            );
            trace.emit_completed(stats_after);
        });

        let records = traces.records()?;
        let event = trace_event(&records, BATCH_WRITE_COMPLETED_EVENT)?;
        assert_trace_field(event, "batch_row_count", "0");
        assert_trace_field(event, "batch_is_empty", "true");
        assert_trace_field(event, "accepted_rows_after", "0");
        assert_trace_field(event, "accepted_batches_after", "1");

        Ok(())
    }

    #[test]
    fn batch_schema_validation_failure_emits_trace_without_accepting_batch() -> Result<(), String> {
        let batch = int32_batch("id", &[1]);
        let error = value_conversion_error(DiagnosticCode::SchemaMismatch);

        let (_result, traces) = capture_traces(|| {
            let trace = BatchWriteTrace::new(
                WriteBackend::BaselineTokenRow,
                WriteStats::default(),
                &batch,
            );
            trace.emit_failed(BATCH_SCHEMA_VALIDATION_PHASE, &error);
        });

        let records = traces.records()?;
        let event = trace_event(&records, BATCH_WRITE_FAILED_EVENT)?;
        assert_eq!(event.span_name(), Some(BATCH_WRITE_SPAN));
        assert_trace_field(event, "phase", BATCH_SCHEMA_VALIDATION_PHASE);
        assert_trace_field(event, "backend", "BaselineTokenRow");
        assert_trace_field(event, "batch_row_count", "1");
        assert_trace_field(event, "batch_column_count", "1");
        assert_trace_field(event, "accepted_rows_before", "0");
        assert_trace_field(event, "accepted_batches_before", "0");
        assert_trace_field(event, "batch_write_result", "failure");
        assert_trace_field(event, "diagnostic_codes", "SchemaMismatch");

        Ok(())
    }

    #[test]
    fn value_conversion_failure_emits_trace_without_accepting_batch() -> Result<(), String> {
        let batch = int32_batch("amount", &[1, 2]);
        let error = value_conversion_error(DiagnosticCode::NonFiniteFloat);

        let (_result, traces) = capture_traces(|| {
            let trace = BatchWriteTrace::new(
                WriteBackend::BaselineTokenRow,
                WriteStats::default(),
                &batch,
            );
            trace.emit_failed(VALUE_CONVERSION_PHASE, &error);
        });

        let records = traces.records()?;
        let event = trace_event(&records, BATCH_WRITE_FAILED_EVENT)?;
        assert_trace_field(event, "phase", VALUE_CONVERSION_PHASE);
        assert_trace_field(event, "diagnostic_codes", "NonFiniteFloat");
        assert_trace_field(
            event,
            "error_summary",
            "value conversion failed with diagnostics",
        );

        Ok(())
    }

    #[test]
    fn packet_write_failure_emits_trace_without_accepting_batch() -> Result<(), String> {
        let batch = int32_batch("id", &[1, 2, 3]);
        let error = Error::Tiberius {
            source: tiberius::error::Error::BulkInput(Cow::Borrowed("fake send failure")),
        };

        let (_result, traces) = capture_traces(|| {
            let trace = BatchWriteTrace::new(
                WriteBackend::BaselineTokenRow,
                WriteStats::default(),
                &batch,
            );
            trace.emit_failed(PACKET_WRITE_PHASE, &error);
        });

        let records = traces.records()?;
        let event = trace_event(&records, BATCH_WRITE_FAILED_EVENT)?;
        assert_trace_field(event, "phase", PACKET_WRITE_PHASE);
        assert_trace_field(event, "backend", "BaselineTokenRow");
        assert_trace_field(event, "diagnostic_codes", "");
        assert_trace_field(event, "error_summary", "tiberius operation failed");

        Ok(())
    }

    #[test]
    fn batch_write_trace_does_not_emit_row_values() -> Result<(), String> {
        let batch = utf8_batch("secret_value", &["password=secret"]);

        let (_result, traces) = capture_traces(|| {
            let trace = BatchWriteTrace::new(
                WriteBackend::BaselineTokenRow,
                WriteStats::default(),
                &batch,
            );
            trace.emit_completed(WriteStats {
                rows_written: 1,
                batches_written: 1,
            });
        });

        traces.assert_no_forbidden_text(&["password=secret"])?;

        Ok(())
    }

    #[test]
    fn direct_batch_write_success_emits_stats_trace() -> Result<(), String> {
        let batch = int32_batch("id", &[10, 20]);

        for backend in [WriteBackend::DirectFramedBulk, WriteBackend::DirectRawBulk] {
            let (_result, traces) = capture_traces(|| {
                let trace = BatchWriteTrace::new(backend, WriteStats::default(), &batch);
                trace.emit_completed(WriteStats {
                    rows_written: 2,
                    batches_written: 1,
                });
            });

            let records = traces.records()?;
            let event = trace_event(&records, BATCH_WRITE_COMPLETED_EVENT)?;
            let expected_backend = match backend {
                WriteBackend::DirectFramedBulk => "DirectFramedBulk",
                WriteBackend::DirectRawBulk => "DirectRawBulk",
                WriteBackend::Auto => "Auto",
                WriteBackend::BaselineTokenRow => "BaselineTokenRow",
            };
            assert_trace_field(event, "backend", expected_backend);
            assert_trace_field(event, "batch_row_count", "2");
            assert_trace_field(event, "batch_column_count", "1");
            assert_trace_field(event, "accepted_rows_after", "2");
            assert_trace_field(event, "accepted_batches_after", "1");
        }

        Ok(())
    }

    #[test]
    fn direct_raw_value_conversion_failure_trace_includes_diagnostic_codes() -> Result<(), String> {
        let batch = int32_batch("u64_value", &[1]);
        let error = value_conversion_error(DiagnosticCode::IntegerOutOfRange);

        let (_result, traces) = capture_traces(|| {
            let trace =
                BatchWriteTrace::new(WriteBackend::DirectRawBulk, WriteStats::default(), &batch);
            trace.emit_failed(VALUE_CONVERSION_PHASE, &error);
        });

        let records = traces.records()?;
        let failure = trace_event(&records, BATCH_WRITE_FAILED_EVENT)?;
        assert_eq!(failure.span_name(), Some(BATCH_WRITE_SPAN));
        assert_trace_field(failure, "phase", VALUE_CONVERSION_PHASE);
        assert_trace_field(failure, "backend", "DirectRawBulk");
        assert_trace_field(failure, "batch_row_count", "1");
        assert_trace_field(failure, "batch_column_count", "1");
        assert_trace_field(failure, "diagnostic_codes", "IntegerOutOfRange");
        assert_trace_field(
            failure,
            "error_summary",
            "value conversion failed with diagnostics",
        );

        Ok(())
    }

    fn value_conversion_error(code: DiagnosticCode) -> Error {
        Error::ValueConversion {
            diagnostics: DiagnosticSet::from(vec![Diagnostic::error(code, "test diagnostic")]),
        }
    }

    fn int32_batch(name: &str, values: &[i32]) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new(name, DataType::Int32, false)]));
        RecordBatch::try_new(schema, vec![Arc::new(Int32Array::from(values.to_vec()))]).unwrap()
    }

    fn utf8_batch(name: &str, values: &[&str]) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new(name, DataType::Utf8, false)]));
        RecordBatch::try_new(schema, vec![Arc::new(StringArray::from(values.to_vec()))]).unwrap()
    }
}

use std::time::Duration;

use arrow_array::RecordBatch;

use crate::{
    observability::{
        DIRECT_ENCODING_PHASE, DIRECT_RAW_FAILED_EVENT, DIRECT_RAW_MEASURED_EVENT,
        DIRECT_RAW_PACKET_WRITE_COMPLETED_EVENT, DIRECT_RAW_RANGES_PLANNED_EVENT,
        PACKET_WRITE_PHASE, TRACE_TARGET, duration_micros_u64, usize_to_u64_saturating,
    },
    write::{
        direct::{MeasuredDirectBatch, MeasuredRowRange},
        writer::WriteBackend,
    },
};

use super::{backend_trace_name, diagnostic_codes_for_error, sanitized_error_summary};

/// Records optional direct raw detail events from the direct writer path.
#[derive(Debug, Clone, Copy)]
pub(crate) struct DirectRawBatchObserver {
    backend: Option<WriteBackend>,
}

impl DirectRawBatchObserver {
    #[cfg(test)]
    pub(crate) const fn disabled() -> Self {
        Self { backend: None }
    }

    pub(crate) const fn enabled(backend: WriteBackend) -> Self {
        Self {
            backend: Some(backend),
        }
    }

    pub(crate) fn record_measured(&self, measured: &MeasuredDirectBatch, elapsed: Duration) {
        let Some(backend) = self.backend else {
            return;
        };
        emit_direct_raw_measured(backend, measured, elapsed);
    }

    pub(crate) fn record_ranges_planned(
        &self,
        measured: &MeasuredDirectBatch,
        ranges: &[MeasuredRowRange],
        elapsed: Duration,
    ) {
        let Some(backend) = self.backend else {
            return;
        };
        emit_direct_raw_ranges_planned(backend, measured, ranges, elapsed);
    }

    pub(crate) fn record_packet_write_completed(
        &self,
        range: MeasuredRowRange,
        encoded_row_count: usize,
        encoded_byte_count: usize,
        elapsed: Duration,
    ) {
        let Some(backend) = self.backend else {
            return;
        };
        emit_direct_raw_packet_write_completed(
            backend,
            range,
            encoded_row_count,
            encoded_byte_count,
            elapsed,
        );
    }

    pub(crate) fn record_failed(
        &self,
        phase: &'static str,
        batch: &RecordBatch,
        range: Option<MeasuredRowRange>,
        error: &crate::Error,
    ) {
        let Some(backend) = self.backend else {
            return;
        };
        emit_direct_raw_failed(backend, phase, batch, range, error);
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

#[cfg(test)]
mod tests {
    use std::{
        future::Future,
        sync::Arc,
        task::{Context, Poll, Waker},
        time::Duration,
    };

    use super::super::BatchWriteTrace;

    use arrow_array::{Int32Array, RecordBatch, StringArray};
    use arrow_schema::{DataType, Field, Schema};

    use super::{
        emit_direct_raw_measured, emit_direct_raw_packet_write_completed,
        emit_direct_raw_ranges_planned,
    };
    use crate::{
        ArrowFieldRef, Identifier, MssqlColumn, MssqlType, MssqlTypeLength, SchemaMapping,
        observability::{
            BATCH_WRITE_SPAN, DIRECT_ENCODING_PHASE, DIRECT_RAW_MEASURED_EVENT,
            DIRECT_RAW_PACKET_WRITE_COMPLETED_EVENT, DIRECT_RAW_RANGES_PLANNED_EVENT,
            PACKET_WRITE_PHASE,
            test_support::{assert_trace_field, capture_traces, trace_event},
        },
        write::{
            direct::{DirectEncoder, MeasuredRowRange},
            writer::{WriteBackend, WriteStats},
        },
    };

    #[test]
    fn direct_raw_batch_write_emits_encoding_summary_trace() -> Result<(), String> {
        let batch = int32_batch("id", &[10, 20]);
        let encoder = DirectEncoder::new(&[int32_mapping("id")]).unwrap();
        let measured = encoder.measure_batch(&batch).unwrap();
        let ranges = measured.row_ranges(10).unwrap();

        let (_result, traces) = capture_traces(|| {
            emit_direct_raw_measured(WriteBackend::DirectRawBulk, &measured, Duration::ZERO);
            emit_direct_raw_ranges_planned(
                WriteBackend::DirectRawBulk,
                &measured,
                &ranges,
                Duration::ZERO,
            );
        });

        let records = traces.records()?;
        let measured = trace_event(&records, DIRECT_RAW_MEASURED_EVENT)?;
        assert_trace_field(measured, "phase", DIRECT_ENCODING_PHASE);
        assert_trace_field(measured, "backend", "DirectRawBulk");
        assert_trace_field(measured, "batch_row_count", "2");
        assert_trace_field(measured, "batch_column_count", "1");
        assert_trace_field(measured, "encoded_row_count", "2");
        assert_trace_field(measured, "encoded_byte_count", "10");
        assert!(measured.fields().contains_key("elapsed_us"));

        let ranges = trace_event(&records, DIRECT_RAW_RANGES_PLANNED_EVENT)?;
        assert_trace_field(ranges, "phase", DIRECT_ENCODING_PHASE);
        assert_trace_field(ranges, "encoded_range_count", "1");
        assert_trace_field(ranges, "encoded_byte_count", "10");

        Ok(())
    }

    #[test]
    fn direct_raw_packet_write_emits_sanitized_summary_trace() -> Result<(), String> {
        let (_result, traces) = capture_traces(|| {
            emit_direct_raw_packet_write_completed(
                WriteBackend::DirectRawBulk,
                MeasuredRowRange { start: 0, len: 2 },
                2,
                10,
                Duration::ZERO,
            );
        });

        let records = traces.records()?;
        let packet = trace_event(&records, DIRECT_RAW_PACKET_WRITE_COMPLETED_EVENT)?;
        assert_trace_field(packet, "phase", PACKET_WRITE_PHASE);
        assert_trace_field(packet, "backend", "DirectRawBulk");
        assert_trace_field(packet, "encoded_row_start", "0");
        assert_trace_field(packet, "encoded_row_count", "2");
        assert_trace_field(packet, "encoded_byte_count", "10");
        assert!(packet.fields().contains_key("elapsed_us"));
        assert!(!packet.fields().contains_key("raw_packet_count"));

        Ok(())
    }

    #[test]
    fn direct_raw_events_can_be_parented_to_batch_write_span() -> Result<(), String> {
        let batch = int32_batch("id", &[10, 20]);
        let encoder = DirectEncoder::new(&[int32_mapping("id")]).unwrap();
        let measured = encoder.measure_batch(&batch).unwrap();
        let ranges = measured.row_ranges(10).unwrap();

        let (_result, traces) = capture_traces(|| {
            let trace =
                BatchWriteTrace::new(WriteBackend::DirectRawBulk, WriteStats::default(), &batch);
            poll_ready(trace.trace_result(async {
                emit_direct_raw_measured(WriteBackend::DirectRawBulk, &measured, Duration::ZERO);
                emit_direct_raw_ranges_planned(
                    WriteBackend::DirectRawBulk,
                    &measured,
                    &ranges,
                    Duration::ZERO,
                );
                emit_direct_raw_packet_write_completed(
                    WriteBackend::DirectRawBulk,
                    MeasuredRowRange { start: 0, len: 2 },
                    2,
                    10,
                    Duration::ZERO,
                );
                Ok(WriteStats::default())
            }))
            .unwrap();
        });

        let records = traces.records()?;
        let measured = trace_event(&records, DIRECT_RAW_MEASURED_EVENT)?;
        assert_eq!(measured.span_name(), Some(BATCH_WRITE_SPAN));
        let ranges = trace_event(&records, DIRECT_RAW_RANGES_PLANNED_EVENT)?;
        assert_eq!(ranges.span_name(), Some(BATCH_WRITE_SPAN));
        let packet = trace_event(&records, DIRECT_RAW_PACKET_WRITE_COMPLETED_EVENT)?;
        assert_eq!(packet.span_name(), Some(BATCH_WRITE_SPAN));

        Ok(())
    }

    #[test]
    fn direct_raw_trace_does_not_emit_values_or_payload_bytes() -> Result<(), String> {
        let secret_batch = utf8_batch("secret_value", &["password=secret"]);
        let secret_encoder = DirectEncoder::new(&[utf8_mapping("secret_value")]).unwrap();
        let secret_measured = secret_encoder.measure_batch(&secret_batch).unwrap();
        let secret_ranges = secret_measured.row_ranges(1024).unwrap();

        let (_result, traces) = capture_traces(|| {
            emit_direct_raw_measured(
                WriteBackend::DirectRawBulk,
                &secret_measured,
                Duration::ZERO,
            );
            emit_direct_raw_ranges_planned(
                WriteBackend::DirectRawBulk,
                &secret_measured,
                &secret_ranges,
                Duration::ZERO,
            );
        });

        traces.assert_no_forbidden_text(&["password=secret"])?;

        let numeric_batch = int32_batch("id", &[987_654_321]);
        let numeric_encoder = DirectEncoder::new(&[int32_mapping("id")]).unwrap();
        let numeric_measured = numeric_encoder.measure_batch(&numeric_batch).unwrap();
        let (_result, numeric_traces) = capture_traces(|| {
            emit_direct_raw_measured(
                WriteBackend::DirectRawBulk,
                &numeric_measured,
                Duration::ZERO,
            );
            emit_direct_raw_packet_write_completed(
                WriteBackend::DirectRawBulk,
                MeasuredRowRange { start: 0, len: 1 },
                1,
                numeric_measured.payload_len(),
                Duration::ZERO,
            );
        });

        numeric_traces.assert_no_forbidden_text(&["987654321"])?;

        Ok(())
    }

    fn int32_batch(name: &str, values: &[i32]) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new(name, DataType::Int32, false)]));
        RecordBatch::try_new(schema, vec![Arc::new(Int32Array::from(values.to_vec()))]).unwrap()
    }

    fn utf8_batch(name: &str, values: &[&str]) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new(name, DataType::Utf8, false)]));
        RecordBatch::try_new(schema, vec![Arc::new(StringArray::from(values.to_vec()))]).unwrap()
    }

    fn int32_mapping(name: &str) -> SchemaMapping {
        SchemaMapping::new(
            ArrowFieldRef::new(0, name.to_owned(), false, DataType::Int32),
            MssqlColumn::new(Identifier::new(name).unwrap(), MssqlType::Int, false),
        )
    }

    fn utf8_mapping(name: &str) -> SchemaMapping {
        SchemaMapping::new(
            ArrowFieldRef::new(0, name.to_owned(), false, DataType::Utf8),
            MssqlColumn::new(
                Identifier::new(name).unwrap(),
                MssqlType::NVarChar(MssqlTypeLength::Max),
                false,
            ),
        )
    }

    fn poll_ready<F>(future: F) -> F::Output
    where
        F: Future,
    {
        let mut future = Box::pin(future);
        let mut context = Context::from_waker(Waker::noop());
        match future.as_mut().poll(&mut context) {
            Poll::Ready(output) => output,
            Poll::Pending => panic!("test future unexpectedly pending"),
        }
    }
}

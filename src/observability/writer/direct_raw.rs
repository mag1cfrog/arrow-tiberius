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

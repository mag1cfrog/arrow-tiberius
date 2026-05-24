//! Payload-returning direct TDS row execution.

use arrow_array::RecordBatch;

use crate::{Result, write::record_batch::validate_runtime_columns};

use super::super::{
    DirectEncoder, MeasuredDirectBatch, bound::BoundDirectBatch, invalid_payload,
    layout::allocate_rows_payload_with_tokens, payload::EncodedRowsPayload,
    rows::fixed_width::try_encode_fixed_width_rows,
};

/// Encodes a runtime batch into complete raw TDS row payload bytes.
pub(crate) fn encode_batch(
    encoder: &DirectEncoder,
    batch: &RecordBatch,
) -> Result<EncodedRowsPayload> {
    encode_checked_batch(encoder, batch)
}

/// Encodes a contiguous row range from a runtime batch.
///
/// Returned row-token offsets are relative to the returned payload, so the
/// first non-empty range always starts at offset zero.
pub(crate) fn encode_batch_range(
    encoder: &DirectEncoder,
    batch: &RecordBatch,
    start_row: usize,
    row_count: usize,
) -> Result<EncodedRowsPayload> {
    let end_row = start_row
        .checked_add(row_count)
        .ok_or_else(|| invalid_payload("direct row range end overflowed usize"))?;
    if end_row > batch.num_rows() {
        return Err(invalid_payload(format!(
            "direct row range {start_row}..{end_row} is outside batch row count {}",
            batch.num_rows()
        )));
    }

    let batch = batch.slice(start_row, row_count);
    encode_checked_batch(encoder, &batch)
}

/// Encodes one range from a pre-measured direct batch.
pub(crate) fn encode_measured_batch_range(
    encoder: &DirectEncoder,
    batch: &RecordBatch,
    measured: &MeasuredDirectBatch,
    start_row: usize,
    row_count: usize,
) -> Result<EncodedRowsPayload> {
    measured.check_range(start_row, row_count)?;

    if row_count == 0 {
        return EncodedRowsPayload::new(Vec::new(), Vec::new());
    }

    if measured.row_count() != batch.num_rows() {
        return Err(invalid_payload(format!(
            "measured row count {} does not match runtime batch row count {}",
            measured.row_count(),
            batch.num_rows()
        )));
    }

    if measured.column_count() != encoder.plan.column_count() {
        return Err(invalid_payload(format!(
            "measured column count {} does not match direct plan column count {}",
            measured.column_count(),
            encoder.plan.column_count()
        )));
    }

    let batch = batch.slice(start_row, row_count);
    let bound = BoundDirectBatch::new(encoder, &batch)?;
    if let Some(payload) = try_encode_fixed_width_rows(&bound)? {
        return Ok(payload);
    }

    let layout = measured.range_layout(start_row, row_count)?;
    let mut bytes = allocate_rows_payload_with_tokens(&layout);
    bound.fill_columns(&layout, &mut bytes)?;

    EncodedRowsPayload::new(bytes, layout.row_token_offsets().to_vec())
}

fn encode_checked_batch(
    encoder: &DirectEncoder,
    batch: &RecordBatch,
) -> Result<EncodedRowsPayload> {
    validate_runtime_columns(batch, &encoder.mappings)?;

    if encoder.plan.is_empty() && batch.num_rows() == 0 {
        return EncodedRowsPayload::new(Vec::new(), Vec::new());
    }

    let bound = BoundDirectBatch::new(encoder, batch)?;
    if let Some(payload) = try_encode_fixed_width_rows(&bound)? {
        return Ok(payload);
    }

    let layout = bound.measure_layout()?;
    let mut bytes = allocate_rows_payload_with_tokens(&layout);
    bound.fill_columns(&layout, &mut bytes)?;

    EncodedRowsPayload::new(bytes, layout.row_token_offsets().to_vec())
}

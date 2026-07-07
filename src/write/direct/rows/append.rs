//! Append-buffer direct TDS row execution.

use arrow_array::RecordBatch;

use crate::Result;

use super::super::{
    DirectEncoder, MeasuredDirectBatch, binding::BoundDirectBatch, checked_add, invalid_payload,
    payload,
};

/// Encodes one measured range directly into a Tiberius raw rows buffer.
pub(crate) fn encode_measured_batch_range_into(
    encoder: &DirectEncoder,
    batch: &RecordBatch,
    measured: &MeasuredDirectBatch,
    start_row: usize,
    row_count: usize,
    buf: &mut tiberius::RawRowsAppendBuffer<'_>,
) -> Result<tiberius::RawRowsAppend> {
    measured.check_range(start_row, row_count)?;

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

    let bound = BoundDirectBatch::new(encoder, batch)?;
    let mut row_token_offsets = Vec::with_capacity(row_count);
    let mut written = 0usize;

    let end_row = start_row
        .checked_add(row_count)
        .ok_or_else(|| invalid_payload("direct row range end overflowed usize"))?;

    for row_index in start_row..end_row {
        row_token_offsets.push(written);
        buf.put_u8(payload::TDS_ROW_TOKEN);
        written = checked_add(written, 1)?;

        for (column_index, column) in bound.columns().iter().enumerate() {
            let measured_len = measured.cell_len(row_index, column_index)?;
            column.append_cell(bound.runtime_context(), buf, row_index, measured_len)?;
            written = checked_add(written, measured_len)?;
        }
    }

    Ok(tiberius::RawRowsAppend::new(row_token_offsets))
}

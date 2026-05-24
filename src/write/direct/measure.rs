//! Direct row payload measurement.

use crate::Result;

use super::{invalid_payload, layout, primitive::build_fixed_width_row_range_layout};

/// Direct row payload measurement for one runtime batch.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct MeasuredDirectBatch {
    row_count: usize,
    column_count: usize,
    cell_lengths: Vec<usize>,
    row_lengths: Vec<usize>,
    payload_len: usize,
}

impl MeasuredDirectBatch {
    pub(super) fn empty(column_count: usize) -> Self {
        Self {
            row_count: 0,
            column_count,
            cell_lengths: Vec::new(),
            row_lengths: Vec::new(),
            payload_len: 0,
        }
    }

    pub(super) fn new(
        row_count: usize,
        column_count: usize,
        cell_lengths: Vec<usize>,
    ) -> Result<Self> {
        let expected_cell_count = row_count
            .checked_mul(column_count)
            .ok_or_else(|| invalid_payload("measured cell count overflowed usize"))?;
        if cell_lengths.len() != expected_cell_count {
            return Err(invalid_payload(format!(
                "measured cell length count {} does not match row count {row_count} and column count {column_count}",
                cell_lengths.len()
            )));
        }

        let (row_lengths, payload_len) =
            measure_row_lengths(row_count, column_count, &cell_lengths)?;

        Ok(Self {
            row_count,
            column_count,
            cell_lengths,
            row_lengths,
            payload_len,
        })
    }

    /// Returns the measured row count.
    pub(crate) const fn row_count(&self) -> usize {
        self.row_count
    }

    /// Returns the measured column count.
    pub(crate) const fn column_count(&self) -> usize {
        self.column_count
    }

    /// Returns the complete measured payload length.
    pub(crate) const fn payload_len(&self) -> usize {
        self.payload_len
    }

    /// Splits measured rows into payload ranges capped by byte length.
    pub(crate) fn row_ranges(&self, max_payload_bytes: usize) -> Result<Vec<MeasuredRowRange>> {
        if max_payload_bytes == 0 {
            return Err(invalid_payload(
                "direct row range byte limit must be greater than zero",
            ));
        }

        let mut ranges = Vec::new();
        let mut start = 0usize;
        let mut len = 0usize;
        let mut bytes = 0usize;

        for (row_index, row_len) in self.row_lengths.iter().copied().enumerate() {
            let next_bytes = bytes
                .checked_add(row_len)
                .ok_or_else(|| invalid_payload("measured row range length overflowed usize"))?;

            if len > 0 && next_bytes > max_payload_bytes {
                ranges.push(MeasuredRowRange { start, len });
                start = row_index;
                len = 0;
                bytes = row_len;
            } else {
                bytes = next_bytes;
            }

            len += 1;
        }

        if len > 0 {
            ranges.push(MeasuredRowRange { start, len });
        }

        Ok(ranges)
    }

    pub(crate) fn range_payload_len(&self, start_row: usize, row_count: usize) -> Result<usize> {
        self.check_range(start_row, row_count)?;

        let end_row = start_row
            .checked_add(row_count)
            .ok_or_else(|| invalid_payload("direct row range end overflowed usize"))?;
        self.row_lengths[start_row..end_row]
            .iter()
            .try_fold(0usize, |total, row_len| {
                total
                    .checked_add(*row_len)
                    .ok_or_else(|| invalid_payload("measured row range length overflowed usize"))
            })
    }

    pub(super) fn cell_len(&self, row_index: usize, column_index: usize) -> Result<usize> {
        self.check_range(row_index, 1)?;

        if column_index >= self.column_count {
            return Err(invalid_payload(format!(
                "direct measured column index {column_index} is outside measured column count {}",
                self.column_count
            )));
        }

        let index = row_index
            .checked_mul(self.column_count)
            .and_then(|base| base.checked_add(column_index))
            .ok_or_else(|| invalid_payload("measured cell length index overflowed usize"))?;

        self.cell_lengths.get(index).copied().ok_or_else(|| {
            invalid_payload(format!(
                "measured cell length index {index} is outside measured cell length count {}",
                self.cell_lengths.len()
            ))
        })
    }

    pub(super) fn range_layout(
        &self,
        start_row: usize,
        row_count: usize,
    ) -> Result<layout::RowLayout> {
        self.check_range(start_row, row_count)?;
        build_fixed_width_row_range_layout(
            start_row,
            row_count,
            self.column_count,
            &self.cell_lengths,
        )
    }

    pub(super) fn check_range(&self, start_row: usize, row_count: usize) -> Result<()> {
        let end_row = start_row
            .checked_add(row_count)
            .ok_or_else(|| invalid_payload("direct row range end overflowed usize"))?;
        if end_row > self.row_count {
            return Err(invalid_payload(format!(
                "direct measured row range {start_row}..{end_row} is outside measured row count {}",
                self.row_count
            )));
        }

        Ok(())
    }
}

fn measure_row_lengths(
    row_count: usize,
    column_count: usize,
    cell_lengths: &[usize],
) -> Result<(Vec<usize>, usize)> {
    let mut row_lengths = Vec::with_capacity(row_count);
    let mut payload_len = 0usize;

    for row_index in 0..row_count {
        let mut row_len = 1usize;

        for column_index in 0..column_count {
            row_len = row_len
                .checked_add(cell_lengths[row_index * column_count + column_index])
                .ok_or_else(|| invalid_payload("measured row length overflowed usize"))?;
        }

        payload_len = payload_len
            .checked_add(row_len)
            .ok_or_else(|| invalid_payload("measured payload length overflowed usize"))?;
        row_lengths.push(row_len);
    }

    Ok((row_lengths, payload_len))
}

/// Contiguous measured row range.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct MeasuredRowRange {
    /// First row in the measured batch.
    pub(crate) start: usize,
    /// Number of rows in this range.
    pub(crate) len: usize,
}

#[cfg(test)]
mod tests {
    use super::{MeasuredDirectBatch, MeasuredRowRange};
    use crate::{DiagnosticCode, Error};

    #[test]
    fn measured_direct_batch_builds_payload_lengths() {
        let measured = MeasuredDirectBatch::new(2, 3, vec![4, 1, 8, 4, 5, 1]).unwrap();

        assert_eq!(measured.row_count(), 2);
        assert_eq!(measured.column_count(), 3);
        assert_eq!(measured.payload_len(), 25);
        assert_eq!(measured.range_payload_len(0, 1).unwrap(), 14);
        assert_eq!(measured.range_payload_len(1, 1).unwrap(), 11);
        assert_eq!(measured.cell_len(1, 1).unwrap(), 5);
    }

    #[test]
    fn measured_direct_batch_ranges_split_by_payload_byte_limit() {
        let measured = MeasuredDirectBatch::new(4, 1, vec![4, 4, 4, 4]).unwrap();

        assert_eq!(measured.payload_len(), 20);
        assert_eq!(
            measured.row_ranges(10).unwrap(),
            [
                MeasuredRowRange { start: 0, len: 2 },
                MeasuredRowRange { start: 2, len: 2 },
            ]
        );
        assert_eq!(
            measured.row_ranges(4).unwrap(),
            [
                MeasuredRowRange { start: 0, len: 1 },
                MeasuredRowRange { start: 1, len: 1 },
                MeasuredRowRange { start: 2, len: 1 },
                MeasuredRowRange { start: 3, len: 1 },
            ]
        );
    }

    #[test]
    fn measured_direct_batch_rejects_invalid_cell_length_count() {
        let err = MeasuredDirectBatch::new(2, 2, vec![1, 2, 3])
            .expect_err("cell length count must match shape");

        assert_direct_encoding_invalid_payload(err);
    }

    #[test]
    fn measured_direct_batch_rejects_invalid_ranges() {
        let measured = MeasuredDirectBatch::new(2, 1, vec![4, 4]).unwrap();

        assert_direct_encoding_invalid_payload(
            measured
                .range_payload_len(1, 2)
                .expect_err("range past measured rows must fail"),
        );
        assert_direct_encoding_invalid_payload(
            measured
                .row_ranges(0)
                .expect_err("zero byte limit must fail"),
        );
    }

    fn assert_direct_encoding_invalid_payload(err: Error) {
        let Error::DirectEncoding { diagnostics } = err else {
            panic!("expected direct encoding error");
        };

        assert_eq!(diagnostics.len(), 1);
        assert_eq!(
            diagnostics.all()[0].code(),
            DiagnosticCode::DirectEncodingInvalidPayload
        );
    }
}

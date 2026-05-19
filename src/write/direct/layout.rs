//! Row-major payload layout for direct raw TDS encoding.

use crate::{Diagnostic, DiagnosticCode, DiagnosticSet, Error, Result};

/// Byte position for one encoded cell inside an encoded rows payload.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) struct CellPosition {
    row_index: usize,
    column_index: usize,
    offset: usize,
    len: usize,
}

impl CellPosition {
    /// Creates a cell byte position.
    pub(crate) const fn new(
        row_index: usize,
        column_index: usize,
        offset: usize,
        len: usize,
    ) -> Self {
        Self {
            row_index,
            column_index,
            offset,
            len,
        }
    }

    /// Returns the row index.
    pub(crate) const fn row_index(&self) -> usize {
        self.row_index
    }

    /// Returns the column index.
    pub(crate) const fn column_index(&self) -> usize {
        self.column_index
    }

    /// Returns the byte offset.
    pub(crate) const fn offset(&self) -> usize {
        self.offset
    }

    /// Returns the encoded byte length.
    pub(crate) const fn len(&self) -> usize {
        self.len
    }

    /// Returns true when the position has no encoded bytes.
    pub(crate) const fn is_empty(&self) -> bool {
        self.len == 0
    }
}

/// Row-major layout metadata for one direct encoded rows payload.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct RowLayout {
    row_token_offsets: Vec<usize>,
    row_lengths: Vec<usize>,
    cell_positions: Vec<CellPosition>,
    payload_len: usize,
}

impl RowLayout {
    /// Creates row-major layout metadata.
    pub(crate) fn new(
        row_token_offsets: Vec<usize>,
        row_lengths: Vec<usize>,
        cell_positions: Vec<CellPosition>,
        payload_len: usize,
    ) -> Result<Self> {
        validate_rows(&row_token_offsets, &row_lengths, payload_len)?;
        validate_cells(&cell_positions, row_token_offsets.len(), payload_len)?;

        Ok(Self {
            row_token_offsets,
            row_lengths,
            cell_positions,
            payload_len,
        })
    }

    /// Returns the number of encoded rows.
    pub(crate) fn row_count(&self) -> usize {
        self.row_token_offsets.len()
    }

    /// Returns the row-token offsets.
    pub(crate) fn row_token_offsets(&self) -> &[usize] {
        &self.row_token_offsets
    }

    /// Returns row lengths in bytes.
    pub(crate) fn row_lengths(&self) -> &[usize] {
        &self.row_lengths
    }

    /// Returns cell byte positions.
    pub(crate) fn cell_positions(&self) -> &[CellPosition] {
        &self.cell_positions
    }

    /// Returns the total payload length in bytes.
    pub(crate) const fn payload_len(&self) -> usize {
        self.payload_len
    }
}

fn validate_rows(
    row_token_offsets: &[usize],
    row_lengths: &[usize],
    payload_len: usize,
) -> Result<()> {
    if row_token_offsets.len() != row_lengths.len() {
        return Err(invalid_layout(format!(
            "row layout has {} row-token offset(s) but {} row length(s)",
            row_token_offsets.len(),
            row_lengths.len()
        )));
    }

    if row_token_offsets.is_empty() {
        if payload_len == 0 {
            return Ok(());
        }

        return Err(invalid_layout(format!(
            "empty row layout cannot describe non-empty payload length {payload_len}"
        )));
    }

    if row_token_offsets[0] != 0 {
        return Err(invalid_layout(format!(
            "first row token offset must be 0, got {}",
            row_token_offsets[0]
        )));
    }

    for (index, (&offset, &len)) in row_token_offsets.iter().zip(row_lengths).enumerate() {
        let end = offset.checked_add(len).ok_or_else(|| {
            invalid_layout(format!(
                "row {index} offset {offset} plus length {len} overflows usize"
            ))
        })?;

        if end > payload_len {
            return Err(invalid_layout(format!(
                "row {index} ends at {end}, outside payload length {payload_len}"
            )));
        }

        if let Some(&next_offset) = row_token_offsets.get(index + 1)
            && next_offset != end
        {
            return Err(invalid_layout(format!(
                "row {index} ends at {end}, but next row token offset is {next_offset}"
            )));
        }
    }

    let last_end = row_token_offsets[row_token_offsets.len() - 1]
        .checked_add(row_lengths[row_lengths.len() - 1])
        .ok_or_else(|| invalid_layout("last row end overflows usize"))?;

    if last_end != payload_len {
        return Err(invalid_layout(format!(
            "last row ends at {last_end}, but payload length is {payload_len}"
        )));
    }

    Ok(())
}

fn validate_cells(
    cell_positions: &[CellPosition],
    row_count: usize,
    payload_len: usize,
) -> Result<()> {
    for cell in cell_positions {
        if cell.row_index >= row_count {
            return Err(invalid_layout(format!(
                "cell row index {} is outside row count {row_count}",
                cell.row_index
            )));
        }

        let end = cell.offset.checked_add(cell.len).ok_or_else(|| {
            invalid_layout(format!(
                "cell offset {} plus length {} overflows usize",
                cell.offset, cell.len
            ))
        })?;

        if end > payload_len {
            return Err(invalid_layout(format!(
                "cell at row {} column {} ends at {end}, outside payload length {payload_len}",
                cell.row_index, cell.column_index
            )));
        }
    }

    Ok(())
}

fn invalid_layout(message: impl Into<String>) -> Error {
    Error::DirectEncoding {
        diagnostics: DiagnosticSet::from(vec![Diagnostic::error(
            DiagnosticCode::DirectEncodingInvalidPayload,
            message,
        )]),
    }
}

#[cfg(test)]
mod tests {
    use crate::{DiagnosticCode, Error};

    use super::{CellPosition, RowLayout};

    #[test]
    fn accepts_empty_layout() {
        let layout =
            RowLayout::new(Vec::new(), Vec::new(), Vec::new(), 0).expect("empty layout is valid");

        assert_eq!(layout.row_count(), 0);
        assert_eq!(layout.row_token_offsets(), []);
        assert_eq!(layout.row_lengths(), []);
        assert_eq!(layout.cell_positions(), []);
        assert_eq!(layout.payload_len(), 0);
    }

    #[test]
    fn accepts_contiguous_multi_row_layout_with_cells() {
        let cells = vec![CellPosition::new(0, 0, 1, 4), CellPosition::new(1, 0, 7, 4)];
        let layout = RowLayout::new(vec![0, 6], vec![6, 6], cells.clone(), 12)
            .expect("contiguous layout is valid");

        assert_eq!(layout.row_count(), 2);
        assert_eq!(layout.row_token_offsets(), [0, 6]);
        assert_eq!(layout.row_lengths(), [6, 6]);
        assert_eq!(layout.cell_positions(), cells);
        assert_eq!(layout.cell_positions()[0].row_index(), 0);
        assert_eq!(layout.cell_positions()[0].column_index(), 0);
        assert_eq!(layout.cell_positions()[0].offset(), 1);
        assert_eq!(layout.cell_positions()[0].len(), 4);
        assert!(!layout.cell_positions()[0].is_empty());
    }

    #[test]
    fn rejects_row_count_mismatch() {
        let err = RowLayout::new(vec![0], Vec::new(), Vec::new(), 1)
            .expect_err("row offsets and lengths must match");

        assert_invalid_layout(err);
    }

    #[test]
    fn rejects_non_zero_first_row_offset() {
        let err = RowLayout::new(vec![1], vec![1], Vec::new(), 2)
            .expect_err("first row must start at zero");

        assert_invalid_layout(err);
    }

    #[test]
    fn rejects_gaps_between_rows() {
        let err = RowLayout::new(vec![0, 3], vec![2, 1], Vec::new(), 4)
            .expect_err("rows must be contiguous");

        assert_invalid_layout(err);
    }

    #[test]
    fn rejects_layout_that_does_not_cover_payload() {
        let err = RowLayout::new(vec![0], vec![1], Vec::new(), 2)
            .expect_err("layout must cover payload exactly");

        assert_invalid_layout(err);
    }

    #[test]
    fn rejects_cell_outside_row_count() {
        let err = RowLayout::new(vec![0], vec![1], vec![CellPosition::new(1, 0, 0, 1)], 1)
            .expect_err("cell row must exist");

        assert_invalid_layout(err);
    }

    #[test]
    fn rejects_cell_outside_payload() {
        let err = RowLayout::new(vec![0], vec![1], vec![CellPosition::new(0, 0, 0, 2)], 1)
            .expect_err("cell must fit payload");

        assert_invalid_layout(err);
    }

    fn assert_invalid_layout(err: Error) {
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

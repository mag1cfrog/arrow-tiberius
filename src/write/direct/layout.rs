//! Row-major payload layout for direct raw TDS encoding.

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
    pub(crate) const fn new(
        row_token_offsets: Vec<usize>,
        row_lengths: Vec<usize>,
        cell_positions: Vec<CellPosition>,
        payload_len: usize,
    ) -> Self {
        Self {
            row_token_offsets,
            row_lengths,
            cell_positions,
            payload_len,
        }
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

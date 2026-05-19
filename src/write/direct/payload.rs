//! Encoded direct raw TDS row payloads.

use crate::{Diagnostic, DiagnosticCode, DiagnosticSet, Error, Result};

/// SQL Server TDS ROW token used at the start of each bulk row payload.
pub(crate) const TDS_ROW_TOKEN: u8 = 0xD1;

/// Complete direct-encoded TDS rows plus row-token offsets.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct EncodedRowsPayload {
    bytes: Vec<u8>,
    row_token_offsets: Vec<usize>,
}

impl EncodedRowsPayload {
    /// Creates an encoded rows payload after validating row-token offsets.
    pub(crate) fn new(bytes: Vec<u8>, row_token_offsets: Vec<usize>) -> Result<Self> {
        validate_row_token_offsets(&bytes, &row_token_offsets)?;

        Ok(Self {
            bytes,
            row_token_offsets,
        })
    }

    /// Returns the encoded bytes.
    pub(crate) fn bytes(&self) -> &[u8] {
        &self.bytes
    }

    /// Returns offsets where each TDS ROW token begins.
    pub(crate) fn row_token_offsets(&self) -> &[usize] {
        &self.row_token_offsets
    }

    /// Returns the number of encoded rows.
    pub(crate) fn row_count(&self) -> usize {
        self.row_token_offsets.len()
    }

    /// Returns true when there are no encoded rows and no encoded bytes.
    pub(crate) fn is_empty(&self) -> bool {
        self.bytes.is_empty() && self.row_token_offsets.is_empty()
    }

    /// Consumes this payload into encoded bytes and row-token offsets.
    pub(crate) fn into_parts(self) -> (Vec<u8>, Vec<usize>) {
        (self.bytes, self.row_token_offsets)
    }
}

fn validate_row_token_offsets(bytes: &[u8], row_token_offsets: &[usize]) -> Result<()> {
    let mut previous = None;

    for &offset in row_token_offsets {
        if offset >= bytes.len() {
            return Err(invalid_payload(format!(
                "row token offset {offset} is outside payload length {}",
                bytes.len()
            )));
        }

        if let Some(previous_offset) = previous
            && offset <= previous_offset
        {
            return Err(invalid_payload(format!(
                "row token offsets must be strictly increasing: {offset} followed {previous_offset}"
            )));
        }

        if bytes[offset] != TDS_ROW_TOKEN {
            return Err(invalid_payload(format!(
                "row token offset {offset} points to byte 0x{:02X}, not 0x{TDS_ROW_TOKEN:02X}",
                bytes[offset]
            )));
        }

        previous = Some(offset);
    }

    if row_token_offsets.is_empty() && !bytes.is_empty() {
        return Err(invalid_payload(
            "non-empty direct payload must include at least one row token offset",
        ));
    }

    Ok(())
}

fn invalid_payload(message: impl Into<String>) -> Error {
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

    use super::{EncodedRowsPayload, TDS_ROW_TOKEN};

    #[test]
    fn accepts_empty_payload() {
        let payload = EncodedRowsPayload::new(Vec::new(), Vec::new()).expect("empty is valid");

        assert!(payload.is_empty());
        assert_eq!(payload.row_count(), 0);
        assert_eq!(payload.bytes(), []);
        assert_eq!(payload.row_token_offsets(), []);
    }

    #[test]
    fn accepts_multi_row_payload_with_token_offsets() {
        let bytes = vec![TDS_ROW_TOKEN, 0x01, 0x02, TDS_ROW_TOKEN, 0x03];
        let payload =
            EncodedRowsPayload::new(bytes.clone(), vec![0, 3]).expect("offsets are valid");

        assert_eq!(payload.row_count(), 2);
        assert_eq!(payload.bytes(), bytes);
        assert_eq!(payload.row_token_offsets(), [0, 3]);
        assert_eq!(payload.into_parts(), (bytes, vec![0, 3]));
    }

    #[test]
    fn rejects_non_empty_payload_without_offsets() {
        let err = EncodedRowsPayload::new(vec![TDS_ROW_TOKEN], Vec::new())
            .expect_err("non-empty payload without offsets is invalid");

        assert_invalid_payload(err);
    }

    #[test]
    fn rejects_offset_outside_payload() {
        let err = EncodedRowsPayload::new(vec![TDS_ROW_TOKEN], vec![1])
            .expect_err("outside offset is invalid");

        assert_invalid_payload(err);
    }

    #[test]
    fn rejects_offsets_that_do_not_point_at_row_tokens() {
        let err = EncodedRowsPayload::new(vec![TDS_ROW_TOKEN, 0x00], vec![1])
            .expect_err("offset must point to row token");

        assert_invalid_payload(err);
    }

    #[test]
    fn rejects_duplicate_or_unsorted_offsets() {
        let duplicate = EncodedRowsPayload::new(vec![TDS_ROW_TOKEN, TDS_ROW_TOKEN], vec![0, 0])
            .expect_err("duplicate offsets are invalid");
        assert_invalid_payload(duplicate);

        let unsorted = EncodedRowsPayload::new(vec![TDS_ROW_TOKEN, TDS_ROW_TOKEN], vec![1, 0])
            .expect_err("unsorted offsets are invalid");
        assert_invalid_payload(unsorted);
    }

    fn assert_invalid_payload(err: Error) {
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

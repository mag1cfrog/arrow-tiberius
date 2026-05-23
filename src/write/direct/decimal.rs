//! Shared SQL Server decimal/numeric direct TDS row payload helpers.

use crate::{Diagnostic, DiagnosticCode, DiagnosticSet, Error, Result};

pub(crate) const NULL_DECIMAL_CELL_LEN: usize = 1;
const DECIMAL_SIGN_LEN: usize = 1;
const DECIMAL_NEGATIVE_SIGN: u8 = 0;
const DECIMAL_POSITIVE_SIGN: u8 = 1;

/// Returns the byte length of a non-null decimal cell in a TDS row payload.
pub(crate) fn decimal_cell_len(unscaled: i128) -> usize {
    NULL_DECIMAL_CELL_LEN + decimal_value_len(unscaled)
}

/// Writes a SQL Server NULL decimal cell into an exactly sized cell buffer.
pub(crate) fn write_null_decimal_cell(dst: &mut [u8]) -> Result<()> {
    if dst.len() != NULL_DECIMAL_CELL_LEN {
        return Err(invalid_payload(format!(
            "null decimal cell has length {}, expected {NULL_DECIMAL_CELL_LEN}",
            dst.len()
        )));
    }

    dst[0] = 0;
    Ok(())
}

/// Writes a non-null SQL Server decimal cell into an exactly sized cell buffer.
pub(crate) fn write_decimal_cell(dst: &mut [u8], unscaled: i128) -> Result<()> {
    let expected_len = decimal_cell_len(unscaled);
    if dst.len() != expected_len {
        return Err(invalid_payload(format!(
            "decimal cell has length {}, expected {expected_len}",
            dst.len()
        )));
    }

    write_decimal_value(dst, unscaled)
}

/// Appends a SQL Server NULL decimal cell to a raw rows append buffer.
pub(crate) fn append_null_decimal_cell(buf: &mut tiberius::RawRowsAppendBuffer<'_>) {
    buf.put_u8(0);
}

/// Appends a non-null SQL Server decimal cell to a raw rows append buffer.
pub(crate) fn append_decimal_cell(
    buf: &mut tiberius::RawRowsAppendBuffer<'_>,
    unscaled: i128,
) -> Result<()> {
    let value_len = decimal_value_len(unscaled);
    buf.put_u8(value_len as u8);
    buf.put_u8(decimal_sign(unscaled));
    append_decimal_magnitude(buf, unscaled.unsigned_abs())
}

fn write_decimal_value(dst: &mut [u8], unscaled: i128) -> Result<()> {
    let value_len = decimal_value_len(unscaled);
    dst[0] = value_len as u8;
    dst[1] = decimal_sign(unscaled);
    write_decimal_magnitude(&mut dst[2..], unscaled.unsigned_abs())
}

fn decimal_value_len(unscaled: i128) -> usize {
    DECIMAL_SIGN_LEN + decimal_magnitude_len(unscaled.unsigned_abs())
}

fn decimal_sign(unscaled: i128) -> u8 {
    if unscaled < 0 {
        DECIMAL_NEGATIVE_SIGN
    } else {
        DECIMAL_POSITIVE_SIGN
    }
}

fn decimal_magnitude_len(magnitude: u128) -> usize {
    match decimal_precision(magnitude) {
        1..=9 => 4,
        10..=19 => 8,
        20..=28 => 12,
        _ => 16,
    }
}

fn decimal_precision(mut magnitude: u128) -> u8 {
    let mut digits = 1;
    while magnitude >= 10 {
        magnitude /= 10;
        digits += 1;
    }
    digits
}

fn write_decimal_magnitude(dst: &mut [u8], magnitude: u128) -> Result<()> {
    match decimal_magnitude_len(magnitude) {
        4 => dst.copy_from_slice(&(magnitude as u32).to_le_bytes()),
        8 => dst.copy_from_slice(&(magnitude as u64).to_le_bytes()),
        12 => {
            dst[0..8].copy_from_slice(&(magnitude as u64).to_le_bytes());
            dst[8..12].copy_from_slice(&((magnitude >> 64) as u32).to_le_bytes());
        }
        16 => dst.copy_from_slice(&magnitude.to_le_bytes()),
        other => {
            return Err(invalid_payload(format!(
                "unsupported decimal magnitude length {other}"
            )));
        }
    }

    Ok(())
}

fn append_decimal_magnitude(
    buf: &mut tiberius::RawRowsAppendBuffer<'_>,
    magnitude: u128,
) -> Result<()> {
    match decimal_magnitude_len(magnitude) {
        4 => buf.put_u32_le(magnitude as u32),
        8 => buf.put_u64_le(magnitude as u64),
        12 => {
            buf.put_u64_le(magnitude as u64);
            buf.put_u32_le((magnitude >> 64) as u32);
        }
        16 => {
            buf.put_u64_le(magnitude as u64);
            buf.put_u64_le((magnitude >> 64) as u64);
        }
        other => {
            return Err(invalid_payload(format!(
                "unsupported decimal magnitude length {other}"
            )));
        }
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
    use super::{decimal_cell_len, write_decimal_cell, write_null_decimal_cell};

    #[test]
    fn writes_decimal_cells_with_tiberius_numeric_lengths() {
        let cases = [
            (0_i128, vec![5, 1, 0, 0, 0, 0]),
            (123_456_789, vec![5, 1, 0x15, 0xCD, 0x5B, 0x07]),
            (-123_456_789, vec![5, 0, 0x15, 0xCD, 0x5B, 0x07]),
            (
                9_223_372_036_854_775_808,
                vec![9, 1, 0, 0, 0, 0, 0, 0, 0, 0x80],
            ),
            (
                u64::MAX as i128,
                vec![
                    13, 1, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0, 0, 0, 0,
                ],
            ),
            (
                123_456_789_012_345_678_901_234_567_890_i128,
                vec![
                    17, 1, 0xD2, 0x0A, 0x3F, 0x4E, 0xEE, 0xE0, 0x73, 0xC3, 0xF6, 0x0F, 0xE9, 0x8E,
                    0x01, 0, 0, 0,
                ],
            ),
        ];

        for (unscaled, expected) in cases {
            let mut bytes = vec![0; decimal_cell_len(unscaled)];
            write_decimal_cell(&mut bytes, unscaled).unwrap();
            assert_eq!(bytes, expected);
        }
    }

    #[test]
    fn writes_null_decimal_cell_distinct_from_zero() {
        let mut null = vec![255];
        write_null_decimal_cell(&mut null).unwrap();

        let mut zero = vec![255; decimal_cell_len(0)];
        write_decimal_cell(&mut zero, 0).unwrap();

        assert_eq!(null, [0]);
        assert_eq!(zero, [5, 1, 0, 0, 0, 0]);
    }
}

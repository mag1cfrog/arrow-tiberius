//! Shared SQL Server temporal direct TDS row payload helpers.

use crate::{
    Diagnostic, DiagnosticCode, DiagnosticSet, Error, Result,
    mssql::cell::{MssqlDate, MssqlDateTime2, MssqlDateTimeOffset, MssqlTime},
};

pub(crate) const NULL_TEMPORAL_CELL_LEN: usize = 1;
const DATE_PAYLOAD_LEN: usize = 3;
const DATETIMEOFFSET_OFFSET_LEN: usize = 2;
const SQL_SERVER_DATE_MAX_DAYS: u32 = 3_652_058;
const SQL_SERVER_DATETIMEOFFSET_MAX_OFFSET_MINUTES: i16 = 14 * 60;
const SECONDS_PER_DAY: u64 = 86_400;

/// Returns the byte length of a SQL Server NULL temporal cell.
pub(crate) const fn null_temporal_cell_len() -> usize {
    NULL_TEMPORAL_CELL_LEN
}

/// Returns the byte length of a non-null SQL Server `date` cell.
pub(crate) const fn date_cell_len() -> usize {
    NULL_TEMPORAL_CELL_LEN + DATE_PAYLOAD_LEN
}

/// Returns the byte length of a non-null SQL Server `time(p)` cell.
pub(crate) fn time_cell_len(precision: u8) -> Result<usize> {
    Ok(NULL_TEMPORAL_CELL_LEN + time_payload_len(precision)?)
}

/// Returns the byte length of a non-null SQL Server `datetime2(p)` cell.
pub(crate) fn datetime2_cell_len(precision: u8) -> Result<usize> {
    Ok(NULL_TEMPORAL_CELL_LEN + time_payload_len(precision)? + DATE_PAYLOAD_LEN)
}

/// Returns the byte length of a non-null SQL Server `datetimeoffset(p)` cell.
pub(crate) fn datetimeoffset_cell_len(precision: u8) -> Result<usize> {
    Ok(NULL_TEMPORAL_CELL_LEN
        + time_payload_len(precision)?
        + DATE_PAYLOAD_LEN
        + DATETIMEOFFSET_OFFSET_LEN)
}

/// Writes a SQL Server NULL temporal cell into an exactly sized cell buffer.
pub(crate) fn write_null_temporal_cell(dst: &mut [u8]) -> Result<()> {
    if dst.len() != NULL_TEMPORAL_CELL_LEN {
        return Err(invalid_payload(format!(
            "null temporal cell has length {}, expected {NULL_TEMPORAL_CELL_LEN}",
            dst.len()
        )));
    }

    dst[0] = 0;
    Ok(())
}

/// Writes a non-null SQL Server `date` cell into an exactly sized cell buffer.
pub(crate) fn write_date_cell(dst: &mut [u8], value: MssqlDate) -> Result<()> {
    let expected_len = date_cell_len();
    if dst.len() != expected_len {
        return Err(invalid_payload(format!(
            "date cell has length {}, expected {expected_len}",
            dst.len()
        )));
    }

    validate_date(value)?;
    dst[0] = DATE_PAYLOAD_LEN as u8;
    write_u64_le_n(&mut dst[1..], u64::from(value.days()))
}

/// Writes a non-null SQL Server `time(p)` cell into an exactly sized cell buffer.
pub(crate) fn write_time_cell(dst: &mut [u8], value: MssqlTime) -> Result<()> {
    let expected_len = time_cell_len(value.scale())?;
    if dst.len() != expected_len {
        return Err(invalid_payload(format!(
            "time cell has length {}, expected {expected_len}",
            dst.len()
        )));
    }

    validate_time(value)?;
    let payload_len = time_payload_len(value.scale())?;
    dst[0] = payload_len as u8;
    write_u64_le_n(&mut dst[1..], value.increments())
}

/// Writes a non-null SQL Server `datetime2(p)` cell into an exactly sized cell buffer.
pub(crate) fn write_datetime2_cell(dst: &mut [u8], value: MssqlDateTime2) -> Result<()> {
    let expected_len = datetime2_cell_len(value.time().scale())?;
    if dst.len() != expected_len {
        return Err(invalid_payload(format!(
            "datetime2 cell has length {}, expected {expected_len}",
            dst.len()
        )));
    }

    validate_date(value.date())?;
    validate_time(value.time())?;
    let time_payload_len = time_payload_len(value.time().scale())?;
    dst[0] = (time_payload_len + DATE_PAYLOAD_LEN) as u8;
    write_u64_le_n(&mut dst[1..1 + time_payload_len], value.time().increments())?;
    write_u64_le_n(
        &mut dst[1 + time_payload_len..1 + time_payload_len + DATE_PAYLOAD_LEN],
        u64::from(value.date().days()),
    )
}

/// Writes a non-null SQL Server `datetimeoffset(p)` cell into an exactly sized cell buffer.
pub(crate) fn write_datetimeoffset_cell(dst: &mut [u8], value: MssqlDateTimeOffset) -> Result<()> {
    let datetime2 = value.datetime2();
    let expected_len = datetimeoffset_cell_len(datetime2.time().scale())?;
    if dst.len() != expected_len {
        return Err(invalid_payload(format!(
            "datetimeoffset cell has length {}, expected {expected_len}",
            dst.len()
        )));
    }

    validate_date(datetime2.date())?;
    validate_time(datetime2.time())?;
    validate_datetimeoffset_offset(value.offset_minutes())?;
    let time_payload_len = time_payload_len(datetime2.time().scale())?;
    dst[0] = (time_payload_len + DATE_PAYLOAD_LEN + DATETIMEOFFSET_OFFSET_LEN) as u8;
    write_u64_le_n(
        &mut dst[1..1 + time_payload_len],
        datetime2.time().increments(),
    )?;
    write_u64_le_n(
        &mut dst[1 + time_payload_len..1 + time_payload_len + DATE_PAYLOAD_LEN],
        u64::from(datetime2.date().days()),
    )?;
    let offset_start = 1 + time_payload_len + DATE_PAYLOAD_LEN;
    dst[offset_start..offset_start + DATETIMEOFFSET_OFFSET_LEN]
        .copy_from_slice(&value.offset_minutes().to_le_bytes());
    Ok(())
}

/// Appends a SQL Server NULL temporal cell to a raw rows append buffer.
pub(crate) fn append_null_temporal_cell(buf: &mut tiberius::RawRowsAppendBuffer<'_>) {
    buf.put_u8(0);
}

/// Appends a non-null SQL Server `date` cell to a raw rows append buffer.
pub(crate) fn append_date_cell(
    buf: &mut tiberius::RawRowsAppendBuffer<'_>,
    value: MssqlDate,
) -> Result<()> {
    let mut bytes = [0; NULL_TEMPORAL_CELL_LEN + DATE_PAYLOAD_LEN];
    write_date_cell(&mut bytes, value)?;
    buf.extend_from_slice(&bytes);
    Ok(())
}

/// Appends a non-null SQL Server `time(p)` cell to a raw rows append buffer.
pub(crate) fn append_time_cell(
    buf: &mut tiberius::RawRowsAppendBuffer<'_>,
    value: MssqlTime,
) -> Result<()> {
    let mut bytes = vec![0; time_cell_len(value.scale())?];
    write_time_cell(&mut bytes, value)?;
    buf.extend_from_slice(&bytes);
    Ok(())
}

/// Appends a non-null SQL Server `datetime2(p)` cell to a raw rows append buffer.
pub(crate) fn append_datetime2_cell(
    buf: &mut tiberius::RawRowsAppendBuffer<'_>,
    value: MssqlDateTime2,
) -> Result<()> {
    let mut bytes = vec![0; datetime2_cell_len(value.time().scale())?];
    write_datetime2_cell(&mut bytes, value)?;
    buf.extend_from_slice(&bytes);
    Ok(())
}

/// Appends a non-null SQL Server `datetimeoffset(p)` cell to a raw rows append buffer.
pub(crate) fn append_datetimeoffset_cell(
    buf: &mut tiberius::RawRowsAppendBuffer<'_>,
    value: MssqlDateTimeOffset,
) -> Result<()> {
    let mut bytes = vec![0; datetimeoffset_cell_len(value.datetime2().time().scale())?];
    write_datetimeoffset_cell(&mut bytes, value)?;
    buf.extend_from_slice(&bytes);
    Ok(())
}

fn validate_date(value: MssqlDate) -> Result<()> {
    if value.days() <= SQL_SERVER_DATE_MAX_DAYS {
        Ok(())
    } else {
        Err(invalid_payload(format!(
            "date day count {} is outside SQL Server date range",
            value.days()
        )))
    }
}

fn validate_time(value: MssqlTime) -> Result<()> {
    validate_precision(value.scale())?;
    let max = max_time_increments(value.scale())?;
    if value.increments() < max {
        Ok(())
    } else {
        Err(invalid_payload(format!(
            "time increment count {} is outside one day at precision {}",
            value.increments(),
            value.scale()
        )))
    }
}

fn validate_datetimeoffset_offset(offset_minutes: i16) -> Result<()> {
    if offset_minutes.unsigned_abs() <= SQL_SERVER_DATETIMEOFFSET_MAX_OFFSET_MINUTES as u16 {
        Ok(())
    } else {
        Err(invalid_payload(format!(
            "datetimeoffset offset {offset_minutes} minute(s) is outside SQL Server range -840..=840"
        )))
    }
}

fn max_time_increments(precision: u8) -> Result<u64> {
    validate_precision(precision)?;
    Ok(SECONDS_PER_DAY * 10_u64.pow(u32::from(precision)))
}

fn time_payload_len(precision: u8) -> Result<usize> {
    match precision {
        0..=2 => Ok(3),
        3..=4 => Ok(4),
        5..=7 => Ok(5),
        _ => Err(invalid_payload(format!(
            "time precision {precision} is outside SQL Server range 0..=7"
        ))),
    }
}

fn validate_precision(precision: u8) -> Result<()> {
    time_payload_len(precision).map(|_| ())
}

fn write_u64_le_n(dst: &mut [u8], value: u64) -> Result<()> {
    if dst.len() > 8 {
        return Err(invalid_payload(format!(
            "little-endian temporal integer destination has length {}, expected at most 8",
            dst.len()
        )));
    }

    dst.copy_from_slice(&value.to_le_bytes()[..dst.len()]);
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
    use crate::{
        DiagnosticCode, Error,
        mssql::cell::{MssqlDate, MssqlDateTime2, MssqlDateTimeOffset, MssqlTime},
        write::direct::payload::TDS_ROW_TOKEN,
    };

    use super::{
        date_cell_len, datetime2_cell_len, datetimeoffset_cell_len, null_temporal_cell_len,
        time_cell_len, write_date_cell, write_datetime2_cell, write_datetimeoffset_cell,
        write_null_temporal_cell, write_time_cell,
    };

    #[test]
    fn writes_null_temporal_cells_distinct_from_zero_values() {
        let mut null = vec![255; null_temporal_cell_len()];
        write_null_temporal_cell(&mut null).unwrap();

        let mut date_zero = vec![255; date_cell_len()];
        write_date_cell(&mut date_zero, MssqlDate::new(0)).unwrap();

        let mut time_zero = vec![255; time_cell_len(7).unwrap()];
        write_time_cell(&mut time_zero, MssqlTime::new(0, 7)).unwrap();

        let mut datetime2_zero = vec![255; datetime2_cell_len(7).unwrap()];
        write_datetime2_cell(
            &mut datetime2_zero,
            MssqlDateTime2::new(MssqlDate::new(0), MssqlTime::new(0, 7)),
        )
        .unwrap();

        let mut datetimeoffset_zero = vec![255; datetimeoffset_cell_len(7).unwrap()];
        write_datetimeoffset_cell(
            &mut datetimeoffset_zero,
            MssqlDateTimeOffset::new(
                MssqlDateTime2::new(MssqlDate::new(0), MssqlTime::new(0, 7)),
                0,
            ),
        )
        .unwrap();

        assert_eq!(null, [0]);
        assert_eq!(date_zero, [3, 0, 0, 0]);
        assert_ne!(time_zero, null);
        assert_ne!(datetime2_zero, null);
        assert_ne!(datetimeoffset_zero, null);
    }

    #[test]
    fn writes_date_boundaries_as_three_little_endian_bytes() {
        let mut lower = vec![0; date_cell_len()];
        write_date_cell(&mut lower, MssqlDate::new(0)).unwrap();
        assert_eq!(lower, [3, 0x00, 0x00, 0x00]);

        let mut upper = vec![0; date_cell_len()];
        write_date_cell(&mut upper, MssqlDate::new(3_652_058)).unwrap();
        assert_eq!(upper, [3, 0xDA, 0xB9, 0x37]);
    }

    #[test]
    fn writes_time_payload_widths_for_supported_precisions() {
        let cases = [
            (0, 3, vec![3, 0x7F, 0x51, 0x01]),
            (2, 3, vec![3, 0xFF, 0xD5, 0x83]),
            (3, 4, vec![4, 0xFF, 0x5B, 0x26, 0x05]),
            (4, 4, vec![4, 0xFF, 0x97, 0x7F, 0x33]),
            (5, 5, vec![5, 0xFF, 0xEF, 0xFB, 0x02, 0x02]),
            (6, 5, vec![5, 0xFF, 0x5F, 0xD7, 0x1D, 0x14]),
            (7, 5, vec![5, 0xFF, 0xBF, 0x69, 0x2A, 0xC9]),
        ];

        for (precision, payload_len, expected) in cases {
            assert_eq!(time_cell_len(precision).unwrap(), 1 + payload_len);
            let mut bytes = vec![0; time_cell_len(precision).unwrap()];
            write_time_cell(
                &mut bytes,
                MssqlTime::new(max_time_increment_for_test(precision), precision),
            )
            .unwrap();
            assert_eq!(bytes, expected);
        }
    }

    #[test]
    fn writes_time_midnight_and_max_tick_before_midnight() {
        let mut midnight = vec![0; time_cell_len(7).unwrap()];
        write_time_cell(&mut midnight, MssqlTime::new(0, 7)).unwrap();
        assert_eq!(midnight, [5, 0, 0, 0, 0, 0]);

        let mut max = vec![0; time_cell_len(7).unwrap()];
        write_time_cell(&mut max, MssqlTime::new(863_999_999_999, 7)).unwrap();
        assert_eq!(max, [5, 0xFF, 0xBF, 0x69, 0x2A, 0xC9]);
    }

    #[test]
    fn writes_datetime2_time_bytes_before_date_bytes() {
        let value = MssqlDateTime2::new(MssqlDate::new(719_162), MssqlTime::new(12_345, 3));
        let mut bytes = vec![0; datetime2_cell_len(3).unwrap()];

        write_datetime2_cell(&mut bytes, value).unwrap();

        assert_eq!(bytes, [7, 0x39, 0x30, 0x00, 0x00, 0x3A, 0xF9, 0x0A]);
    }

    #[test]
    fn writes_datetimeoffset_datetime2_then_signed_offset_minutes() {
        let cases = [
            (0, [0x00, 0x00]),
            (150, [0x96, 0x00]),
            (840, [0x48, 0x03]),
            (-840, [0xB8, 0xFC]),
        ];

        for (offset, offset_bytes) in cases {
            let value = MssqlDateTimeOffset::new(
                MssqlDateTime2::new(MssqlDate::new(719_162), MssqlTime::new(12_345, 7)),
                offset,
            );
            let mut bytes = vec![0; datetimeoffset_cell_len(7).unwrap()];

            write_datetimeoffset_cell(&mut bytes, value).unwrap();

            assert_eq!(
                bytes,
                [
                    10,
                    0x39,
                    0x30,
                    0x00,
                    0x00,
                    0x00,
                    0x3A,
                    0xF9,
                    0x0A,
                    offset_bytes[0],
                    offset_bytes[1],
                ]
            );
        }
    }

    #[test]
    fn rejects_invalid_precision_and_out_of_day_time() {
        assert_invalid_payload(time_cell_len(8).unwrap_err());

        let mut bytes = vec![0; 6];
        assert_invalid_payload(write_time_cell(&mut bytes, MssqlTime::new(0, 8)).unwrap_err());
        assert_invalid_payload(
            write_time_cell(&mut bytes, MssqlTime::new(864_000_000_000, 7)).unwrap_err(),
        );
    }

    #[test]
    fn rejects_datetimeoffset_offsets_outside_sql_server_range() {
        let mut bytes = vec![0; datetimeoffset_cell_len(7).unwrap()];

        for offset in [-841, 841] {
            assert_invalid_payload(
                write_datetimeoffset_cell(
                    &mut bytes,
                    MssqlDateTimeOffset::new(
                        MssqlDateTime2::new(MssqlDate::new(0), MssqlTime::new(0, 7)),
                        offset,
                    ),
                )
                .unwrap_err(),
            );
        }
    }

    #[test]
    fn rejects_invalid_destination_lengths() {
        assert_invalid_payload(write_null_temporal_cell(&mut []).unwrap_err());
        assert_invalid_payload(write_null_temporal_cell(&mut [0, 0]).unwrap_err());
        assert_invalid_payload(write_date_cell(&mut [0, 0, 0], MssqlDate::new(0)).unwrap_err());
        assert_invalid_payload(write_time_cell(&mut [0, 0, 0], MssqlTime::new(0, 3)).unwrap_err());
        assert_invalid_payload(
            write_datetime2_cell(
                &mut [0; 7],
                MssqlDateTime2::new(MssqlDate::new(0), MssqlTime::new(0, 3)),
            )
            .unwrap_err(),
        );
        assert_invalid_payload(
            write_datetimeoffset_cell(
                &mut [0; 10],
                MssqlDateTimeOffset::new(
                    MssqlDateTime2::new(MssqlDate::new(0), MssqlTime::new(0, 7)),
                    0,
                ),
            )
            .unwrap_err(),
        );
    }

    #[test]
    fn rejects_date_values_outside_sql_server_range() {
        let mut bytes = vec![0; date_cell_len()];

        assert_invalid_payload(write_date_cell(&mut bytes, MssqlDate::new(3_652_059)).unwrap_err());
    }

    #[test]
    fn helpers_write_cells_only_without_row_token_or_metadata() {
        let mut bytes = vec![0; datetime2_cell_len(7).unwrap()];
        write_datetime2_cell(
            &mut bytes,
            MssqlDateTime2::new(MssqlDate::new(719_162), MssqlTime::new(0, 7)),
        )
        .unwrap();

        assert_eq!(bytes[0], 8);
        assert!(!bytes.contains(&TDS_ROW_TOKEN));
    }

    fn max_time_increment_for_test(precision: u8) -> u64 {
        86_400 * 10_u64.pow(u32::from(precision)) - 1
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

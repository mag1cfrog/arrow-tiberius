//! SQL Server temporal cell value encoding helpers.

use crate::{
    Diagnostic, DiagnosticCode, DiagnosticSet, Error, FieldRef, Result,
    mssql::cell::{MssqlDate, MssqlDateTime, MssqlDateTime2, MssqlDateTimeOffset, MssqlTime},
    write::direct::plan::DirectColumnPlan,
};

pub(crate) const NULL_TEMPORAL_CELL_LEN: usize = 1;
const DATE_PAYLOAD_LEN: usize = 3;
const DATETIME_PAYLOAD_LEN: usize = 8;
const DATETIMEOFFSET_OFFSET_LEN: usize = 2;
const SQL_SERVER_DATE_MAX_DAYS: u32 = 3_652_058;
const SQL_SERVER_DATETIME_MIN_DAYS: i32 = -53_690;
const SQL_SERVER_DATETIME_MAX_DAYS: i32 = 2_958_463;
const SQL_SERVER_DATETIME_FRAGMENTS_PER_DAY: u32 = 25_920_000;
const SQL_SERVER_DATETIMEOFFSET_MAX_OFFSET_MINUTES: i16 = 14 * 60;
const SECONDS_PER_DAY: u64 = 86_400;
const SQL_SERVER_DATE_UNIX_EPOCH_DAYS: i64 = 719_162;
const MILLISECONDS_PER_DAY: i64 = 86_400_000;
const SQL_SERVER_DATETIME2_DATE64_SCALE: u8 = 3;

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

/// Returns the byte length of a non-null SQL Server `datetime` cell.
pub(crate) const fn datetime_cell_len() -> usize {
    NULL_TEMPORAL_CELL_LEN + DATETIME_PAYLOAD_LEN
}

/// Returns the byte length of a fixed SQL Server `datetime` payload.
pub(crate) const fn datetime_payload_len() -> usize {
    DATETIME_PAYLOAD_LEN
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

/// Writes a non-null SQL Server `datetime` cell into an exactly sized cell buffer.
pub(crate) fn write_datetime_cell(dst: &mut [u8], value: MssqlDateTime) -> Result<()> {
    let expected_len = datetime_cell_len();
    if dst.len() != expected_len {
        return Err(invalid_payload(format!(
            "datetime cell has length {}, expected {expected_len}",
            dst.len()
        )));
    }

    dst[0] = DATETIME_PAYLOAD_LEN as u8;
    write_datetime_payload(&mut dst[1..], value)
}

/// Writes a fixed SQL Server `datetime` payload into an exactly sized buffer.
pub(crate) fn write_datetime_payload(dst: &mut [u8], value: MssqlDateTime) -> Result<()> {
    let expected_len = datetime_payload_len();
    if dst.len() != expected_len {
        return Err(invalid_payload(format!(
            "datetime payload has length {}, expected {expected_len}",
            dst.len()
        )));
    }

    validate_datetime(value)?;
    dst[..4].copy_from_slice(&value.days().to_le_bytes());
    dst[4..8].copy_from_slice(&value.seconds_fragments().to_le_bytes());
    Ok(())
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

/// Appends a non-null SQL Server `datetime` cell to a raw rows append buffer.
pub(crate) fn append_datetime_cell(
    buf: &mut tiberius::RawRowsAppendBuffer<'_>,
    value: MssqlDateTime,
) -> Result<()> {
    let mut bytes = [0; NULL_TEMPORAL_CELL_LEN + DATETIME_PAYLOAD_LEN];
    write_datetime_cell(&mut bytes, value)?;
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

pub(crate) fn mssql_date_from_arrow_date32(
    column: &DirectColumnPlan,
    row_index: usize,
    days_from_unix_epoch: i32,
) -> Result<MssqlDate> {
    let days = i64::from(days_from_unix_epoch) + SQL_SERVER_DATE_UNIX_EPOCH_DAYS;

    if (0..=i64::from(SQL_SERVER_DATE_MAX_DAYS)).contains(&days) {
        return Ok(MssqlDate::new(days as u32));
    }

    Err(value_conversion_error(row_column_diagnostic(
        column,
        row_index,
        DiagnosticCode::TimestampOutOfRange,
        format!("Arrow Date32 day offset {days_from_unix_epoch} is outside SQL Server date range"),
    )))
}

pub(crate) fn mssql_datetime2_from_arrow_date64(
    column: &DirectColumnPlan,
    row_index: usize,
    milliseconds_from_unix_epoch: i64,
) -> Result<MssqlDateTime2> {
    let days_from_unix_epoch = milliseconds_from_unix_epoch.div_euclid(MILLISECONDS_PER_DAY);
    let milliseconds_since_midnight = milliseconds_from_unix_epoch.rem_euclid(MILLISECONDS_PER_DAY);
    let days = days_from_unix_epoch + SQL_SERVER_DATE_UNIX_EPOCH_DAYS;

    if !(0..=i64::from(SQL_SERVER_DATE_MAX_DAYS)).contains(&days) {
        return Err(value_conversion_error(row_column_diagnostic(
            column,
            row_index,
            DiagnosticCode::TimestampOutOfRange,
            format!(
                "Arrow Date64 millisecond value {milliseconds_from_unix_epoch} is outside SQL Server datetime2 range"
            ),
        )));
    }

    Ok(MssqlDateTime2::new(
        MssqlDate::new(days as u32),
        MssqlTime::new(
            milliseconds_since_midnight as u64,
            SQL_SERVER_DATETIME2_DATE64_SCALE,
        ),
    ))
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

fn validate_datetime(value: MssqlDateTime) -> Result<()> {
    if !(SQL_SERVER_DATETIME_MIN_DAYS..=SQL_SERVER_DATETIME_MAX_DAYS).contains(&value.days()) {
        return Err(invalid_payload(format!(
            "datetime day count {} is outside SQL Server datetime range",
            value.days()
        )));
    }

    if value.seconds_fragments() >= SQL_SERVER_DATETIME_FRAGMENTS_PER_DAY {
        return Err(invalid_payload(format!(
            "datetime fragment count {} is outside one day",
            value.seconds_fragments()
        )));
    }

    Ok(())
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

fn row_column_diagnostic(
    column: &DirectColumnPlan,
    row_index: usize,
    code: DiagnosticCode,
    message: impl Into<String>,
) -> Diagnostic {
    Diagnostic::error(code, message)
        .with_field(FieldRef::new(column.source_index(), column.source_name()))
        .with_row(row_index)
}

fn value_conversion_error(diagnostic: Diagnostic) -> Error {
    Error::ValueConversion {
        diagnostics: DiagnosticSet::from(vec![diagnostic]),
    }
}

fn invalid_payload(message: impl Into<String>) -> Error {
    Error::DirectEncoding {
        diagnostics: DiagnosticSet::from(vec![Diagnostic::error(
            DiagnosticCode::DirectEncodingInvalidPayload,
            message,
        )]),
    }
}

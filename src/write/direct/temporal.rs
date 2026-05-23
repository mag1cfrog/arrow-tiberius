//! Shared SQL Server temporal direct TDS row payload helpers.

use arrow_array::{Array, Date32Array, Date64Array};

use crate::{
    Diagnostic, DiagnosticCode, DiagnosticSet, Error, FieldRef, Result,
    conversion::arrow_to_mssql::temporal::TemporalArrowToMssql,
    mssql::cell::{MssqlDate, MssqlDateTime2, MssqlDateTimeOffset, MssqlTime},
    write::profile,
};

use super::{
    layout::{CellPosition, RowLayout},
    plan::DirectColumnPlan,
};

pub(crate) const NULL_TEMPORAL_CELL_LEN: usize = 1;
const DATE_PAYLOAD_LEN: usize = 3;
const DATETIMEOFFSET_OFFSET_LEN: usize = 2;
const SQL_SERVER_DATE_MAX_DAYS: u32 = 3_652_058;
const SQL_SERVER_DATETIMEOFFSET_MAX_OFFSET_MINUTES: i16 = 14 * 60;
const SECONDS_PER_DAY: u64 = 86_400;
const SQL_SERVER_DATE_UNIX_EPOCH_DAYS: i64 = 719_162;
const MILLISECONDS_PER_DAY: i64 = 86_400_000;
const SQL_SERVER_DATETIME2_DATE64_SCALE: u8 = 3;

/// Measures one Arrow date-family column into a row-major cell length matrix.
pub(crate) fn measure_temporal_column_cell_lengths(
    array: &dyn Array,
    column: &DirectColumnPlan,
    classification: TemporalArrowToMssql,
    column_index: usize,
    column_count: usize,
    cell_lengths: &mut [usize],
) -> Result<()> {
    match classification {
        TemporalArrowToMssql::Date32ToDate => {
            let array = downcast_direct_array::<Date32Array>(array, column)?;
            measure_temporal_values(
                array,
                column,
                column_index,
                column_count,
                cell_lengths,
                |array, row_index| {
                    mssql_date_from_arrow_date32(column, row_index, array.value(row_index))
                        .map(|_| date_cell_len())
                },
            )
        }
        TemporalArrowToMssql::Date64ToDateTime2 => {
            let array = downcast_direct_array::<Date64Array>(array, column)?;
            measure_temporal_values(
                array,
                column,
                column_index,
                column_count,
                cell_lengths,
                |array, row_index| {
                    mssql_datetime2_from_arrow_date64(column, row_index, array.value(row_index))
                        .and_then(datetime2_cell_len_for_value)
                },
            )
        }
        unsupported => Err(unsupported_temporal_batch(format!(
            "direct temporal measurement is not implemented yet for {unsupported:?}"
        ))),
    }
}

/// Fills one Arrow date-family column into an already allocated rows payload.
pub(crate) fn fill_temporal_column(
    array: &dyn Array,
    column: &DirectColumnPlan,
    classification: TemporalArrowToMssql,
    column_index: usize,
    column_count: usize,
    layout: &RowLayout,
    bytes: &mut [u8],
) -> Result<()> {
    match classification {
        TemporalArrowToMssql::Date32ToDate => {
            let array = downcast_direct_array::<Date32Array>(array, column)?;
            fill_date32_column(array, column, column_index, column_count, layout, bytes)
        }
        TemporalArrowToMssql::Date64ToDateTime2 => {
            let array = downcast_direct_array::<Date64Array>(array, column)?;
            fill_date64_column(array, column, column_index, column_count, layout, bytes)
        }
        unsupported => Err(unsupported_temporal_batch(format!(
            "direct temporal fill is not implemented yet for {unsupported:?}"
        ))),
    }
}

/// Appends one Arrow Date32 cell to a raw bulk append buffer.
pub(crate) fn append_date32_cell(
    buf: &mut tiberius::RawRowsAppendBuffer<'_>,
    array: &Date32Array,
    column: &DirectColumnPlan,
    row_index: usize,
    measured_len: usize,
) -> Result<()> {
    append_temporal_value(
        buf,
        array,
        column,
        row_index,
        measured_len,
        |array, row_index| {
            let value = mssql_date_from_arrow_date32(column, row_index, array.value(row_index))?;
            Ok((date_cell_len(), TemporalValue::Date(value)))
        },
    )
}

/// Appends one Arrow Date64 cell to a raw bulk append buffer.
pub(crate) fn append_date64_cell(
    buf: &mut tiberius::RawRowsAppendBuffer<'_>,
    array: &Date64Array,
    column: &DirectColumnPlan,
    row_index: usize,
    measured_len: usize,
) -> Result<()> {
    append_temporal_value(
        buf,
        array,
        column,
        row_index,
        measured_len,
        |array, row_index| {
            let value =
                mssql_datetime2_from_arrow_date64(column, row_index, array.value(row_index))?;
            Ok((
                datetime2_cell_len_for_value(value)?,
                TemporalValue::DateTime2(value),
            ))
        },
    )
}

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

fn measure_temporal_values<A, F>(
    array: &A,
    column: &DirectColumnPlan,
    column_index: usize,
    column_count: usize,
    cell_lengths: &mut [usize],
    value_len: F,
) -> Result<()>
where
    A: Array,
    F: Fn(&A, usize) -> Result<usize>,
{
    for row_index in 0..array.len() {
        let cell_len = if array.is_null(row_index) {
            null_temporal_cell_len_for_column(column, row_index)?
        } else {
            value_len(array, row_index)?
        };

        cell_lengths[row_index * column_count + column_index] = cell_len;
    }

    Ok(())
}

fn fill_date32_column(
    array: &Date32Array,
    column: &DirectColumnPlan,
    column_index: usize,
    column_count: usize,
    layout: &RowLayout,
    bytes: &mut [u8],
) -> Result<()> {
    for row_index in 0..array.len() {
        let cell = cell_position(layout, row_index, column_index, column_count)?;

        if array.is_null(row_index) {
            write_null_direct_temporal_cell(bytes, cell, column, row_index)?;
        } else {
            let value = mssql_date_from_arrow_date32(column, row_index, array.value(row_index))?;
            write_direct_date_cell(bytes, cell, column, value)?;
        }
    }

    Ok(())
}

fn fill_date64_column(
    array: &Date64Array,
    column: &DirectColumnPlan,
    column_index: usize,
    column_count: usize,
    layout: &RowLayout,
    bytes: &mut [u8],
) -> Result<()> {
    for row_index in 0..array.len() {
        let cell = cell_position(layout, row_index, column_index, column_count)?;

        if array.is_null(row_index) {
            write_null_direct_temporal_cell(bytes, cell, column, row_index)?;
        } else {
            let value =
                mssql_datetime2_from_arrow_date64(column, row_index, array.value(row_index))?;
            write_direct_datetime2_cell(bytes, cell, column, value)?;
        }
    }

    Ok(())
}

fn append_temporal_value<A, F>(
    buf: &mut tiberius::RawRowsAppendBuffer<'_>,
    array: &A,
    column: &DirectColumnPlan,
    row_index: usize,
    measured_len: usize,
    value: F,
) -> Result<()>
where
    A: Array,
    F: Fn(&A, usize) -> Result<(usize, TemporalValue)>,
{
    if array.is_null(row_index) {
        profile::record_null_cell();
        let expected_len = null_temporal_cell_len_for_column(column, row_index)?;
        if measured_len != expected_len {
            return Err(invalid_payload(format!(
                "measured null temporal cell at row {row_index} column {} has length {}, expected {expected_len}",
                column.source_name(),
                measured_len
            )));
        }

        append_null_temporal_cell(buf);
        return Ok(());
    }

    let (expected_len, value) = value(array, row_index)?;
    if measured_len != expected_len {
        return Err(invalid_payload(format!(
            "measured temporal cell at row {row_index} column {} has length {}, expected {expected_len}",
            column.source_name(),
            measured_len
        )));
    }

    match value {
        TemporalValue::Date(value) => {
            append_date_cell(buf, value).map_err(|err| add_temporal_field(err, column))
        }
        TemporalValue::DateTime2(value) => {
            append_datetime2_cell(buf, value).map_err(|err| add_temporal_field(err, column))
        }
    }
}

enum TemporalValue {
    Date(MssqlDate),
    DateTime2(MssqlDateTime2),
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

fn mssql_date_from_arrow_date32(
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

fn mssql_datetime2_from_arrow_date64(
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

fn datetime2_cell_len_for_value(value: MssqlDateTime2) -> Result<usize> {
    datetime2_cell_len(value.time().scale())
}

fn null_temporal_cell_len_for_column(column: &DirectColumnPlan, row_index: usize) -> Result<usize> {
    if !column.nullable() {
        return Err(value_conversion_error(row_column_diagnostic(
            column,
            row_index,
            DiagnosticCode::NullInNonNullableColumn,
            "null value in non-nullable direct temporal column",
        )));
    }

    Ok(NULL_TEMPORAL_CELL_LEN)
}

fn write_null_direct_temporal_cell(
    bytes: &mut [u8],
    cell: &CellPosition,
    column: &DirectColumnPlan,
    row_index: usize,
) -> Result<()> {
    let expected_len = null_temporal_cell_len_for_column(column, row_index)?;
    if cell.len() != expected_len {
        return Err(invalid_payload(format!(
            "null temporal cell at row {} column {} has length {}, expected {expected_len}",
            cell.row_index(),
            cell.column_index(),
            cell.len()
        )));
    }

    let cell_bytes = cell_bytes_mut(bytes, cell)?;
    write_null_temporal_cell(cell_bytes)
}

fn write_direct_date_cell(
    bytes: &mut [u8],
    cell: &CellPosition,
    column: &DirectColumnPlan,
    value: MssqlDate,
) -> Result<()> {
    let expected_len = date_cell_len();
    if cell.len() != expected_len {
        return Err(invalid_payload(format!(
            "date cell at row {} column {} has length {}, expected {expected_len}",
            cell.row_index(),
            cell.column_index(),
            cell.len()
        )));
    }

    let cell_bytes = cell_bytes_mut(bytes, cell)?;
    write_date_cell(cell_bytes, value).map_err(|err| add_temporal_field(err, column))
}

fn write_direct_datetime2_cell(
    bytes: &mut [u8],
    cell: &CellPosition,
    column: &DirectColumnPlan,
    value: MssqlDateTime2,
) -> Result<()> {
    let expected_len = datetime2_cell_len_for_value(value)?;
    if cell.len() != expected_len {
        return Err(invalid_payload(format!(
            "datetime2 cell at row {} column {} has length {}, expected {expected_len}",
            cell.row_index(),
            cell.column_index(),
            cell.len()
        )));
    }

    let cell_bytes = cell_bytes_mut(bytes, cell)?;
    write_datetime2_cell(cell_bytes, value).map_err(|err| add_temporal_field(err, column))
}

fn cell_bytes_mut<'a>(bytes: &'a mut [u8], cell: &CellPosition) -> Result<&'a mut [u8]> {
    let end = cell
        .offset()
        .checked_add(cell.len())
        .ok_or_else(|| invalid_payload("temporal cell end overflowed usize"))?;

    bytes
        .get_mut(cell.offset()..end)
        .ok_or_else(|| invalid_payload("temporal cell range is outside payload"))
}

fn cell_position(
    layout: &RowLayout,
    row_index: usize,
    column_index: usize,
    column_count: usize,
) -> Result<&CellPosition> {
    let index = row_index
        .checked_mul(column_count)
        .and_then(|base| base.checked_add(column_index))
        .ok_or_else(|| invalid_payload("cell position index overflowed usize"))?;

    layout
        .cell_positions()
        .get(index)
        .ok_or_else(|| invalid_payload("cell position is outside measured row layout"))
}

fn downcast_direct_array<'a, T: Array + 'static>(
    array: &'a dyn Array,
    column: &DirectColumnPlan,
) -> Result<&'a T> {
    array.as_any().downcast_ref::<T>().ok_or_else(|| {
        value_conversion_error(row_column_diagnostic(
            column,
            0,
            DiagnosticCode::ValueTypeMismatch,
            format!(
                "runtime Arrow type {} does not match planned direct temporal column type",
                array.data_type()
            ),
        ))
    })
}

fn add_temporal_field(err: Error, column: &DirectColumnPlan) -> Error {
    let Error::DirectEncoding { diagnostics } = err else {
        return err;
    };

    let diagnostics = diagnostics
        .into_iter()
        .map(|diagnostic| {
            diagnostic.with_field(FieldRef::new(column.source_index(), column.source_name()))
        })
        .collect::<Vec<_>>();

    Error::DirectEncoding {
        diagnostics: DiagnosticSet::from(diagnostics),
    }
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

fn unsupported_temporal_batch(message: impl Into<String>) -> Error {
    Error::DirectEncoding {
        diagnostics: DiagnosticSet::from(vec![Diagnostic::error(
            DiagnosticCode::DirectEncodingUnsupportedBatch,
            message,
        )]),
    }
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

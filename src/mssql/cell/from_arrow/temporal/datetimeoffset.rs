//! Timezone-aware Arrow timestamp to MSSQL datetimeoffset conversion.

use crate::{
    DiagnosticCode, Result, SchemaMapping, arrow::cell::ArrowCell,
    conversion::arrow_to_mssql::temporal::TemporalArrowToMssql,
};

use super::{
    ArrowToMssqlRuntimeMapping, TICKS_100NS_PER_MICROSECOND, TICKS_100NS_PER_MILLISECOND,
    TICKS_100NS_PER_SECOND, datetime2::mssql_datetime2_from_unix_epoch_100ns_ticks,
    datetime2::nanoseconds_to_100ns_ticks, row_mapping_diagnostic,
    timezone::timezone_resolution_from_metadata, value_conversion_error,
};
use crate::mssql::cell::MssqlDateTimeOffset;

pub(in crate::mssql::cell::from_arrow) fn mssql_datetimeoffset_value(
    runtime_mapping: ArrowToMssqlRuntimeMapping<'_>,
    row_index: usize,
    cell: ArrowCell<'_>,
) -> Result<MssqlDateTimeOffset> {
    let mapping = runtime_mapping.mapping();
    let classification = TemporalArrowToMssql::classify(mapping, row_index)?;

    match (cell, classification, timestamp_timezone(mapping)) {
        (
            ArrowCell::TimestampSecond(value),
            TemporalArrowToMssql::TimestampSecondTzToDateTimeOffset,
            Some(timezone),
        ) => mssql_datetimeoffset_from_arrow_timestamp_second(mapping, row_index, value, timezone),
        (
            ArrowCell::TimestampMillisecond(value),
            TemporalArrowToMssql::TimestampMillisecondTzToDateTimeOffset,
            Some(timezone),
        ) => mssql_datetimeoffset_from_arrow_timestamp_millisecond(
            mapping, row_index, value, timezone,
        ),
        (
            ArrowCell::TimestampMicrosecond(value),
            TemporalArrowToMssql::TimestampMicrosecondTzToDateTimeOffset,
            Some(timezone),
        ) => mssql_datetimeoffset_from_arrow_timestamp_microsecond(
            mapping, row_index, value, timezone,
        ),
        (
            ArrowCell::TimestampNanosecond(value),
            TemporalArrowToMssql::TimestampNanosecondTzToDateTimeOffset,
            Some(timezone),
        ) => mssql_datetimeoffset_from_arrow_timestamp_nanosecond(
            mapping,
            row_index,
            value,
            timezone,
            runtime_mapping.nanosecond_policy(),
        ),
        other => Err(value_conversion_error(row_mapping_diagnostic(
            mapping,
            row_index,
            DiagnosticCode::ValueTypeMismatch,
            format!(
                "expected timezone-aware Arrow timestamp payload planned as datetimeoffset, got {other:?}"
            ),
        ))),
    }
}

pub(crate) fn mssql_datetimeoffset_from_arrow_timestamp_second(
    mapping: &SchemaMapping,
    row_index: usize,
    seconds_from_unix_epoch: i64,
    timezone: &str,
) -> Result<MssqlDateTimeOffset> {
    let resolution = timezone_resolution_from_metadata(mapping, row_index, timezone)?;
    let offset_minutes =
        resolution.offset_for_instant(mapping, row_index, seconds_from_unix_epoch, 0)?;
    let utc_ticks = i128::from(seconds_from_unix_epoch) * TICKS_100NS_PER_SECOND;
    mssql_datetimeoffset_from_utc_100ns_ticks(
        mapping,
        row_index,
        utc_ticks,
        offset_minutes,
        "second",
        seconds_from_unix_epoch,
    )
}

pub(crate) fn mssql_datetimeoffset_from_arrow_timestamp_millisecond(
    mapping: &SchemaMapping,
    row_index: usize,
    milliseconds_from_unix_epoch: i64,
    timezone: &str,
) -> Result<MssqlDateTimeOffset> {
    let (seconds, nanoseconds) =
        epoch_parts_from_milliseconds(mapping, row_index, milliseconds_from_unix_epoch)?;
    let resolution = timezone_resolution_from_metadata(mapping, row_index, timezone)?;
    let offset_minutes = resolution.offset_for_instant(mapping, row_index, seconds, nanoseconds)?;
    let utc_ticks = i128::from(milliseconds_from_unix_epoch) * TICKS_100NS_PER_MILLISECOND;
    mssql_datetimeoffset_from_utc_100ns_ticks(
        mapping,
        row_index,
        utc_ticks,
        offset_minutes,
        "millisecond",
        milliseconds_from_unix_epoch,
    )
}

pub(crate) fn mssql_datetimeoffset_from_arrow_timestamp_microsecond(
    mapping: &SchemaMapping,
    row_index: usize,
    microseconds_from_unix_epoch: i64,
    timezone: &str,
) -> Result<MssqlDateTimeOffset> {
    let (seconds, nanoseconds) =
        epoch_parts_from_microseconds(mapping, row_index, microseconds_from_unix_epoch)?;
    let resolution = timezone_resolution_from_metadata(mapping, row_index, timezone)?;
    let offset_minutes = resolution.offset_for_instant(mapping, row_index, seconds, nanoseconds)?;
    let utc_ticks = i128::from(microseconds_from_unix_epoch) * TICKS_100NS_PER_MICROSECOND;
    mssql_datetimeoffset_from_utc_100ns_ticks(
        mapping,
        row_index,
        utc_ticks,
        offset_minutes,
        "microsecond",
        microseconds_from_unix_epoch,
    )
}

pub(crate) fn mssql_datetimeoffset_from_arrow_timestamp_nanosecond(
    mapping: &SchemaMapping,
    row_index: usize,
    nanoseconds_from_unix_epoch: i64,
    timezone: &str,
    policy: crate::NanosecondPolicy,
) -> Result<MssqlDateTimeOffset> {
    let (seconds, nanoseconds) =
        epoch_parts_from_nanoseconds(mapping, row_index, nanoseconds_from_unix_epoch)?;
    let resolution = timezone_resolution_from_metadata(mapping, row_index, timezone)?;
    let offset_minutes = resolution.offset_for_instant(mapping, row_index, seconds, nanoseconds)?;
    let utc_ticks =
        nanoseconds_to_100ns_ticks(mapping, row_index, nanoseconds_from_unix_epoch, policy)?;
    mssql_datetimeoffset_from_utc_100ns_ticks(
        mapping,
        row_index,
        utc_ticks,
        offset_minutes,
        "nanosecond",
        nanoseconds_from_unix_epoch,
    )
}

fn timestamp_timezone(mapping: &SchemaMapping) -> Option<&str> {
    let arrow_schema::DataType::Timestamp(_, Some(timezone)) = mapping.arrow().data_type() else {
        return None;
    };

    Some(timezone)
}

fn validate_datetimeoffset_local_range(
    mapping: &SchemaMapping,
    row_index: usize,
    local_ticks_from_unix_epoch: i128,
    unit_name: &str,
    source_value: i64,
) -> Result<()> {
    mssql_datetime2_from_unix_epoch_100ns_ticks(
        mapping,
        row_index,
        local_ticks_from_unix_epoch,
        unit_name,
        source_value,
    )
    .map(|_| ())
}

fn mssql_datetimeoffset_from_utc_100ns_ticks(
    mapping: &SchemaMapping,
    row_index: usize,
    utc_ticks_from_unix_epoch: i128,
    offset_minutes: i16,
    unit_name: &str,
    source_value: i64,
) -> Result<MssqlDateTimeOffset> {
    let offset_ticks = i128::from(offset_minutes) * 60 * TICKS_100NS_PER_SECOND;
    let local_ticks = utc_ticks_from_unix_epoch
        .checked_add(offset_ticks)
        .ok_or_else(|| {
            value_conversion_error(row_mapping_diagnostic(
                mapping,
                row_index,
                DiagnosticCode::TimestampOutOfRange,
                format!(
                    "Arrow timestamp {unit_name} value {source_value} overflows while applying timezone offset {offset_minutes} minute(s)"
                ),
            ))
        })?;
    validate_datetimeoffset_local_range(mapping, row_index, local_ticks, unit_name, source_value)?;
    let utc_datetime2 = mssql_datetime2_from_unix_epoch_100ns_ticks(
        mapping,
        row_index,
        utc_ticks_from_unix_epoch,
        unit_name,
        source_value,
    )?;

    Ok(MssqlDateTimeOffset::new(utc_datetime2, offset_minutes))
}

fn epoch_parts_from_milliseconds(
    mapping: &SchemaMapping,
    row_index: usize,
    milliseconds_from_unix_epoch: i64,
) -> Result<(i64, u32)> {
    let seconds = milliseconds_from_unix_epoch.div_euclid(1_000);
    let nanoseconds = milliseconds_from_unix_epoch.rem_euclid(1_000) * 1_000_000;
    epoch_parts(mapping, row_index, seconds, nanoseconds)
}

fn epoch_parts_from_microseconds(
    mapping: &SchemaMapping,
    row_index: usize,
    microseconds_from_unix_epoch: i64,
) -> Result<(i64, u32)> {
    let seconds = microseconds_from_unix_epoch.div_euclid(1_000_000);
    let nanoseconds = microseconds_from_unix_epoch.rem_euclid(1_000_000) * 1_000;
    epoch_parts(mapping, row_index, seconds, nanoseconds)
}

fn epoch_parts_from_nanoseconds(
    mapping: &SchemaMapping,
    row_index: usize,
    nanoseconds_from_unix_epoch: i64,
) -> Result<(i64, u32)> {
    let seconds = nanoseconds_from_unix_epoch.div_euclid(1_000_000_000);
    let nanoseconds = nanoseconds_from_unix_epoch.rem_euclid(1_000_000_000);
    epoch_parts(mapping, row_index, seconds, nanoseconds)
}

fn epoch_parts(
    mapping: &SchemaMapping,
    row_index: usize,
    seconds_from_unix_epoch: i64,
    nanoseconds: i64,
) -> Result<(i64, u32)> {
    let nanoseconds = u32::try_from(nanoseconds).map_err(|_| {
        value_conversion_error(row_mapping_diagnostic(
            mapping,
            row_index,
            DiagnosticCode::TimestampOutOfRange,
            format!("timestamp nanosecond component {nanoseconds} is outside valid range"),
        ))
    })?;

    Ok((seconds_from_unix_epoch, nanoseconds))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_schema::{DataType, Field, Schema, TimeUnit};

    use super::super::super::{ArrowToMssqlRuntimeMapping, mssql_cell_from_arrow_cell};
    use crate::{
        DiagnosticCode, MssqlProfile, NanosecondPolicy, PlanOptions, SchemaMapping, TimezonePolicy,
        arrow::cell::ArrowCell,
        mssql::cell::{MssqlCell, MssqlDate, MssqlDateTime2, MssqlDateTimeOffset, MssqlTime},
        plan_arrow_schema_to_mssql_mappings,
    };

    #[test]
    fn converts_timezone_aware_timestamp_cells_to_datetimeoffset() {
        let options = PlanOptions {
            timezone_policy: TimezonePolicy::DateTimeOffset,
            ..PlanOptions::default()
        };
        let mappings = mappings_for_schema_with_options(
            Schema::new(vec![
                Field::new(
                    "fixed_positive",
                    DataType::Timestamp(TimeUnit::Millisecond, Some("+02:30".into())),
                    true,
                ),
                Field::new(
                    "fixed_negative",
                    DataType::Timestamp(TimeUnit::Nanosecond, Some("-07".into())),
                    true,
                ),
                Field::new(
                    "utc",
                    DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
                    true,
                ),
            ]),
            options,
        );

        assert_eq!(
            convert_cell(&mappings[0], ArrowCell::TimestampMillisecond(0), 0).unwrap(),
            MssqlCell::DateTimeOffset(Some(MssqlDateTimeOffset::new(
                MssqlDateTime2::new(MssqlDate::new(719_162), MssqlTime::new(0, 7)),
                150,
            )))
        );
        assert_eq!(
            convert_cell(&mappings[0], ArrowCell::Null, 1).unwrap(),
            MssqlCell::DateTimeOffset(None)
        );
        assert_eq!(
            convert_cell(&mappings[1], ArrowCell::TimestampNanosecond(0), 0).unwrap(),
            MssqlCell::DateTimeOffset(Some(MssqlDateTimeOffset::new(
                MssqlDateTime2::new(MssqlDate::new(719_162), MssqlTime::new(0, 7)),
                -420,
            )))
        );
        assert_eq!(
            convert_cell(&mappings[1], ArrowCell::Null, 1).unwrap(),
            MssqlCell::DateTimeOffset(None)
        );
        assert_eq!(
            convert_cell(&mappings[2], ArrowCell::TimestampMicrosecond(1_234_567), 0).unwrap(),
            MssqlCell::DateTimeOffset(Some(MssqlDateTimeOffset::new(
                MssqlDateTime2::new(MssqlDate::new(719_162), MssqlTime::new(12_345_670, 7)),
                0,
            )))
        );
    }

    #[test]
    fn resolves_named_timezone_datetimeoffset_per_timestamp_instant() {
        let options = PlanOptions {
            timezone_policy: TimezonePolicy::DateTimeOffset,
            ..PlanOptions::default()
        };
        let mappings = mappings_for_schema_with_options(
            Schema::new(vec![Field::new(
                "new_york",
                DataType::Timestamp(TimeUnit::Second, Some("America/New_York".into())),
                false,
            )]),
            options,
        );

        assert_eq!(
            convert_cell(&mappings[0], ArrowCell::TimestampSecond(1_738_411_200), 0).unwrap(),
            MssqlCell::DateTimeOffset(Some(MssqlDateTimeOffset::new(
                MssqlDateTime2::new(MssqlDate::new(739_282), MssqlTime::new(432_000_000_000, 7)),
                -300,
            )))
        );
        assert_eq!(
            convert_cell(&mappings[0], ArrowCell::TimestampSecond(1_750_593_600), 1).unwrap(),
            MssqlCell::DateTimeOffset(Some(MssqlDateTimeOffset::new(
                MssqlDateTime2::new(MssqlDate::new(739_423), MssqlTime::new(432_000_000_000, 7)),
                -240,
            )))
        );
    }

    #[test]
    fn rejects_invalid_timezone_metadata_for_datetimeoffset() {
        let options = PlanOptions {
            timezone_policy: TimezonePolicy::DateTimeOffset,
            ..PlanOptions::default()
        };
        let mappings = mappings_for_schema_with_options(
            Schema::new(vec![Field::new(
                "ts",
                DataType::Timestamp(TimeUnit::Second, Some("Foobar".into())),
                false,
            )]),
            options,
        );

        let err = convert_cell(&mappings[0], ArrowCell::TimestampSecond(0), 0).unwrap_err();

        assert_single_diagnostic(
            err,
            DiagnosticCode::TimezoneUnsupported,
            Some(0),
            Some((0, "ts")),
        );
    }

    #[test]
    fn rejects_invalid_timezone_metadata_for_null_datetimeoffset() {
        let options = PlanOptions {
            timezone_policy: TimezonePolicy::DateTimeOffset,
            ..PlanOptions::default()
        };
        let mappings = mappings_for_schema_with_options(
            Schema::new(vec![Field::new(
                "ts",
                DataType::Timestamp(TimeUnit::Second, Some("Foobar".into())),
                true,
            )]),
            options,
        );

        let err = convert_cell(&mappings[0], ArrowCell::Null, 0).unwrap_err();

        assert_single_diagnostic(
            err,
            DiagnosticCode::TimezoneUnsupported,
            Some(0),
            Some((0, "ts")),
        );
    }

    #[test]
    fn applies_nanosecond_policy_to_datetimeoffset() {
        let options = PlanOptions {
            timezone_policy: TimezonePolicy::DateTimeOffset,
            nanosecond_policy: NanosecondPolicy::RoundTo100ns,
            ..PlanOptions::default()
        };
        let mappings = mappings_for_schema_with_options(
            Schema::new(vec![Field::new(
                "ts_ns",
                DataType::Timestamp(TimeUnit::Nanosecond, Some("+00:00".into())),
                false,
            )]),
            options,
        );

        assert_eq!(
            convert_cell_with_options(
                &mappings[0],
                ArrowCell::TimestampNanosecond(150),
                0,
                &options,
            )
            .unwrap(),
            MssqlCell::DateTimeOffset(Some(MssqlDateTimeOffset::new(
                MssqlDateTime2::new(MssqlDate::new(719_162), MssqlTime::new(2, 7)),
                0,
            )))
        );
    }

    #[test]
    fn rejects_datetimeoffset_values_outside_local_sql_server_range_after_offset() {
        let options = PlanOptions {
            timezone_policy: TimezonePolicy::DateTimeOffset,
            ..PlanOptions::default()
        };
        let mappings = mappings_for_schema_with_options(
            Schema::new(vec![
                Field::new(
                    "too_early",
                    DataType::Timestamp(TimeUnit::Second, Some("-14:00".into())),
                    false,
                ),
                Field::new(
                    "too_late",
                    DataType::Timestamp(TimeUnit::Second, Some("+14:00".into())),
                    false,
                ),
            ]),
            options,
        );

        let below =
            convert_cell(&mappings[0], ArrowCell::TimestampSecond(-62_135_596_800), 0).unwrap_err();
        assert_single_diagnostic(
            below,
            DiagnosticCode::TimestampOutOfRange,
            Some(0),
            Some((0, "too_early")),
        );

        let above =
            convert_cell(&mappings[1], ArrowCell::TimestampSecond(253_402_300_799), 0).unwrap_err();
        assert_single_diagnostic(
            above,
            DiagnosticCode::TimestampOutOfRange,
            Some(0),
            Some((1, "too_late")),
        );
    }

    fn convert_cell<'a>(
        mapping: &SchemaMapping,
        cell: ArrowCell<'a>,
        row_index: usize,
    ) -> crate::Result<MssqlCell<'a>> {
        let options = PlanOptions::default();
        convert_cell_with_options(mapping, cell, row_index, &options)
    }

    fn convert_cell_with_options<'a>(
        mapping: &SchemaMapping,
        cell: ArrowCell<'a>,
        row_index: usize,
        options: &PlanOptions,
    ) -> crate::Result<MssqlCell<'a>> {
        let runtime_mapping = ArrowToMssqlRuntimeMapping::new(mapping, options);
        mssql_cell_from_arrow_cell(runtime_mapping, cell, row_index)
    }

    fn mappings_for_schema_with_options(
        schema: Schema,
        options: PlanOptions,
    ) -> Vec<SchemaMapping> {
        plan_arrow_schema_to_mssql_mappings(
            Arc::new(schema),
            MssqlProfile::sql_server_2016_compat_100(),
            options,
        )
        .unwrap()
        .into_parts()
        .0
    }

    fn assert_single_diagnostic(
        err: crate::Error,
        expected_code: DiagnosticCode,
        expected_row: Option<usize>,
        expected_field: Option<(usize, &str)>,
    ) {
        let crate::Error::ValueConversion { diagnostics } = err else {
            panic!("expected value conversion error");
        };

        assert_eq!(diagnostics.len(), 1);
        let diagnostic = &diagnostics.all()[0];
        assert_eq!(diagnostic.code(), expected_code);
        assert_eq!(diagnostic.row(), expected_row);
        assert_eq!(
            diagnostic
                .field()
                .map(|field| (field.index(), field.name())),
            expected_field
        );
    }
}

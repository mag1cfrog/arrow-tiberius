//! Temporal Arrow-to-SQL Server conversion classification.

use arrow_schema::{DataType, TimeUnit};

use crate::{
    Diagnostic, DiagnosticCode, DiagnosticSet, FieldRef, MssqlTimePrecision, MssqlType, Result,
    SchemaMapping,
};

/// Shared semantic conversion class for planned Arrow temporal mappings.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) enum TemporalArrowToMssql {
    /// Arrow Date32 to SQL Server `date`.
    Date32ToDate,
    /// Arrow Date64 to SQL Server `datetime2(3)`.
    Date64ToDateTime2,
    /// Arrow Time32 second to SQL Server `time(0)`.
    Time32SecondToTime,
    /// Arrow Time32 millisecond to SQL Server `time(3)`.
    Time32MillisecondToTime,
    /// Arrow Time64 microsecond to SQL Server `time(6)`.
    Time64MicrosecondToTime,
    /// Arrow Time64 nanosecond to SQL Server `time(7)`.
    Time64NanosecondToTime,
    /// Timezone-free Arrow timestamp seconds to SQL Server `datetime2(7)`.
    TimestampSecondToDateTime2,
    /// Timezone-free Arrow timestamp milliseconds to SQL Server `datetime2(7)`.
    TimestampMillisecondToDateTime2,
    /// Timezone-free Arrow timestamp microseconds to SQL Server `datetime2(7)`.
    TimestampMicrosecondToDateTime2,
    /// Timezone-free Arrow timestamp nanoseconds to SQL Server `datetime2(7)`.
    TimestampNanosecondToDateTime2,
    /// Timezone-aware Arrow timestamp seconds normalized to SQL Server `datetime2(7)`.
    TimestampSecondTzToDateTime2,
    /// Timezone-aware Arrow timestamp milliseconds normalized to SQL Server `datetime2(7)`.
    TimestampMillisecondTzToDateTime2,
    /// Timezone-aware Arrow timestamp microseconds normalized to SQL Server `datetime2(7)`.
    TimestampMicrosecondTzToDateTime2,
    /// Timezone-aware Arrow timestamp nanoseconds normalized to SQL Server `datetime2(7)`.
    TimestampNanosecondTzToDateTime2,
    /// Timezone-aware Arrow timestamp seconds to SQL Server `datetimeoffset(7)`.
    TimestampSecondTzToDateTimeOffset,
    /// Timezone-aware Arrow timestamp milliseconds to SQL Server `datetimeoffset(7)`.
    TimestampMillisecondTzToDateTimeOffset,
    /// Timezone-aware Arrow timestamp microseconds to SQL Server `datetimeoffset(7)`.
    TimestampMicrosecondTzToDateTimeOffset,
    /// Timezone-aware Arrow timestamp nanoseconds to SQL Server `datetimeoffset(7)`.
    TimestampNanosecondTzToDateTimeOffset,
}

impl TemporalArrowToMssql {
    /// Classifies a planned temporal mapping.
    pub(crate) fn classify(mapping: &SchemaMapping, row_index: usize) -> Result<Self> {
        let classification = match (mapping.arrow().data_type(), mapping.mssql().ty()) {
            (DataType::Date32, MssqlType::Date) => Self::Date32ToDate,
            (DataType::Date64, MssqlType::DateTime2 { precision: 3 }) => Self::Date64ToDateTime2,
            (DataType::Time32(TimeUnit::Second), MssqlType::Time(MssqlTimePrecision::ZERO)) => {
                Self::Time32SecondToTime
            }
            (
                DataType::Time32(TimeUnit::Millisecond),
                MssqlType::Time(MssqlTimePrecision::THREE),
            ) => Self::Time32MillisecondToTime,
            (DataType::Time64(TimeUnit::Microsecond), MssqlType::Time(MssqlTimePrecision::SIX)) => {
                Self::Time64MicrosecondToTime
            }
            (
                DataType::Time64(TimeUnit::Nanosecond),
                MssqlType::Time(MssqlTimePrecision::SEVEN),
            ) => Self::Time64NanosecondToTime,
            (
                DataType::Timestamp(TimeUnit::Second, timezone),
                MssqlType::DateTime2 { precision: 7 },
            ) if is_timezone_free(timezone.as_deref()) => Self::TimestampSecondToDateTime2,
            (
                DataType::Timestamp(TimeUnit::Millisecond, timezone),
                MssqlType::DateTime2 { precision: 7 },
            ) if is_timezone_free(timezone.as_deref()) => Self::TimestampMillisecondToDateTime2,
            (
                DataType::Timestamp(TimeUnit::Microsecond, timezone),
                MssqlType::DateTime2 { precision: 7 },
            ) if is_timezone_free(timezone.as_deref()) => Self::TimestampMicrosecondToDateTime2,
            (
                DataType::Timestamp(TimeUnit::Nanosecond, timezone),
                MssqlType::DateTime2 { precision: 7 },
            ) if is_timezone_free(timezone.as_deref()) => Self::TimestampNanosecondToDateTime2,
            (
                DataType::Timestamp(TimeUnit::Second, Some(timezone)),
                MssqlType::DateTime2 { precision: 7 },
            ) if !timezone.is_empty() => Self::TimestampSecondTzToDateTime2,
            (
                DataType::Timestamp(TimeUnit::Millisecond, Some(timezone)),
                MssqlType::DateTime2 { precision: 7 },
            ) if !timezone.is_empty() => Self::TimestampMillisecondTzToDateTime2,
            (
                DataType::Timestamp(TimeUnit::Microsecond, Some(timezone)),
                MssqlType::DateTime2 { precision: 7 },
            ) if !timezone.is_empty() => Self::TimestampMicrosecondTzToDateTime2,
            (
                DataType::Timestamp(TimeUnit::Nanosecond, Some(timezone)),
                MssqlType::DateTime2 { precision: 7 },
            ) if !timezone.is_empty() => Self::TimestampNanosecondTzToDateTime2,
            (
                DataType::Timestamp(TimeUnit::Second, Some(timezone)),
                MssqlType::DateTimeOffset { precision: 7 },
            ) if !timezone.is_empty() => Self::TimestampSecondTzToDateTimeOffset,
            (
                DataType::Timestamp(TimeUnit::Millisecond, Some(timezone)),
                MssqlType::DateTimeOffset { precision: 7 },
            ) if !timezone.is_empty() => Self::TimestampMillisecondTzToDateTimeOffset,
            (
                DataType::Timestamp(TimeUnit::Microsecond, Some(timezone)),
                MssqlType::DateTimeOffset { precision: 7 },
            ) if !timezone.is_empty() => Self::TimestampMicrosecondTzToDateTimeOffset,
            (
                DataType::Timestamp(TimeUnit::Nanosecond, Some(timezone)),
                MssqlType::DateTimeOffset { precision: 7 },
            ) if !timezone.is_empty() => Self::TimestampNanosecondTzToDateTimeOffset,
            _ => {
                return Err(value_conversion_error(row_mapping_diagnostic(
                    mapping,
                    row_index,
                    DiagnosticCode::ValueConversionUnsupported,
                    format!(
                        "temporal conversion from Arrow {} to SQL Server {} is not supported",
                        mapping.arrow().data_type(),
                        mapping.mssql().ty().to_sql()
                    ),
                )));
            }
        };

        Ok(classification)
    }
}

fn is_timezone_free(timezone: Option<&str>) -> bool {
    timezone.is_none_or(str::is_empty)
}

fn row_mapping_diagnostic(
    mapping: &SchemaMapping,
    row_index: usize,
    code: DiagnosticCode,
    message: impl Into<String>,
) -> Diagnostic {
    Diagnostic::error(code, message)
        .with_field(FieldRef::new(
            mapping.arrow().index(),
            mapping.arrow().name(),
        ))
        .with_row(row_index)
}

fn value_conversion_error(diagnostic: Diagnostic) -> crate::Error {
    crate::Error::ValueConversion {
        diagnostics: DiagnosticSet::from(vec![diagnostic]),
    }
}

#[cfg(test)]
mod tests {
    use arrow_schema::{DataType, TimeUnit};

    use super::TemporalArrowToMssql;
    use crate::{
        ArrowFieldRef, DiagnosticCode, Identifier, MssqlColumn, MssqlTimePrecision, MssqlType,
        SchemaMapping,
    };

    #[test]
    fn classifies_supported_temporal_mappings() {
        let cases = [
            (
                DataType::Date32,
                MssqlType::Date,
                TemporalArrowToMssql::Date32ToDate,
            ),
            (
                DataType::Date64,
                MssqlType::DateTime2 { precision: 3 },
                TemporalArrowToMssql::Date64ToDateTime2,
            ),
            (
                DataType::Time32(TimeUnit::Second),
                MssqlType::Time(MssqlTimePrecision::ZERO),
                TemporalArrowToMssql::Time32SecondToTime,
            ),
            (
                DataType::Time32(TimeUnit::Millisecond),
                MssqlType::Time(MssqlTimePrecision::THREE),
                TemporalArrowToMssql::Time32MillisecondToTime,
            ),
            (
                DataType::Time64(TimeUnit::Microsecond),
                MssqlType::Time(MssqlTimePrecision::SIX),
                TemporalArrowToMssql::Time64MicrosecondToTime,
            ),
            (
                DataType::Time64(TimeUnit::Nanosecond),
                MssqlType::Time(MssqlTimePrecision::SEVEN),
                TemporalArrowToMssql::Time64NanosecondToTime,
            ),
            (
                DataType::Timestamp(TimeUnit::Second, None),
                MssqlType::DateTime2 { precision: 7 },
                TemporalArrowToMssql::TimestampSecondToDateTime2,
            ),
            (
                DataType::Timestamp(TimeUnit::Millisecond, None),
                MssqlType::DateTime2 { precision: 7 },
                TemporalArrowToMssql::TimestampMillisecondToDateTime2,
            ),
            (
                DataType::Timestamp(TimeUnit::Microsecond, None),
                MssqlType::DateTime2 { precision: 7 },
                TemporalArrowToMssql::TimestampMicrosecondToDateTime2,
            ),
            (
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                MssqlType::DateTime2 { precision: 7 },
                TemporalArrowToMssql::TimestampNanosecondToDateTime2,
            ),
            (
                DataType::Timestamp(TimeUnit::Second, Some("".into())),
                MssqlType::DateTime2 { precision: 7 },
                TemporalArrowToMssql::TimestampSecondToDateTime2,
            ),
            (
                DataType::Timestamp(TimeUnit::Second, Some("America/New_York".into())),
                MssqlType::DateTime2 { precision: 7 },
                TemporalArrowToMssql::TimestampSecondTzToDateTime2,
            ),
            (
                DataType::Timestamp(TimeUnit::Millisecond, Some("America/New_York".into())),
                MssqlType::DateTime2 { precision: 7 },
                TemporalArrowToMssql::TimestampMillisecondTzToDateTime2,
            ),
            (
                DataType::Timestamp(TimeUnit::Microsecond, Some("America/New_York".into())),
                MssqlType::DateTime2 { precision: 7 },
                TemporalArrowToMssql::TimestampMicrosecondTzToDateTime2,
            ),
            (
                DataType::Timestamp(TimeUnit::Nanosecond, Some("America/New_York".into())),
                MssqlType::DateTime2 { precision: 7 },
                TemporalArrowToMssql::TimestampNanosecondTzToDateTime2,
            ),
            (
                DataType::Timestamp(TimeUnit::Second, Some("+02:30".into())),
                MssqlType::DateTimeOffset { precision: 7 },
                TemporalArrowToMssql::TimestampSecondTzToDateTimeOffset,
            ),
            (
                DataType::Timestamp(TimeUnit::Millisecond, Some("+02:30".into())),
                MssqlType::DateTimeOffset { precision: 7 },
                TemporalArrowToMssql::TimestampMillisecondTzToDateTimeOffset,
            ),
            (
                DataType::Timestamp(TimeUnit::Microsecond, Some("+02:30".into())),
                MssqlType::DateTimeOffset { precision: 7 },
                TemporalArrowToMssql::TimestampMicrosecondTzToDateTimeOffset,
            ),
            (
                DataType::Timestamp(TimeUnit::Nanosecond, Some("+02:30".into())),
                MssqlType::DateTimeOffset { precision: 7 },
                TemporalArrowToMssql::TimestampNanosecondTzToDateTimeOffset,
            ),
        ];

        for (index, (arrow_type, mssql_type, expected)) in cases.into_iter().enumerate() {
            let mapping = mapping(index, "value", arrow_type, mssql_type);

            assert_eq!(
                TemporalArrowToMssql::classify(&mapping, index).unwrap(),
                expected
            );
        }
    }

    #[test]
    fn classifier_rejects_mismatched_temporal_pairs_with_field_diagnostic() {
        let cases = [
            (DataType::Date32, MssqlType::DateTime2 { precision: 7 }),
            (DataType::Timestamp(TimeUnit::Second, None), MssqlType::Date),
            (
                DataType::Time32(TimeUnit::Second),
                MssqlType::DateTime2 { precision: 7 },
            ),
            (
                DataType::Time64(TimeUnit::Microsecond),
                MssqlType::Time(MssqlTimePrecision::new(4).unwrap()),
            ),
            (
                DataType::Timestamp(TimeUnit::Second, None),
                MssqlType::DateTimeOffset { precision: 7 },
            ),
        ];

        for (index, (arrow_type, mssql_type)) in cases.into_iter().enumerate() {
            let mapping = mapping(index, "created_at", arrow_type, mssql_type);
            let err = TemporalArrowToMssql::classify(&mapping, index + 10).unwrap_err();

            assert_single_diagnostic(
                err,
                DiagnosticCode::ValueConversionUnsupported,
                Some(index + 10),
                Some((index, "created_at")),
            );
        }
    }

    fn mapping(
        index: usize,
        name: &str,
        arrow_type: DataType,
        mssql_type: MssqlType,
    ) -> SchemaMapping {
        SchemaMapping::new(
            ArrowFieldRef::new(index, name.to_owned(), false, arrow_type),
            MssqlColumn::new(Identifier::new(name).unwrap(), mssql_type, false),
        )
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

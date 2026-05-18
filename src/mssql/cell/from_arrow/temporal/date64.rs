//! Date64 Arrow-to-MSSQL datetime2 runtime cell conversion.

use crate::{DiagnosticCode, Result, SchemaMapping};

use super::{
    MILLISECONDS_PER_DAY, SQL_SERVER_DATE_MAX_DAYS, SQL_SERVER_DATE_UNIX_EPOCH_DAYS,
    SQL_SERVER_DATETIME2_DATE64_SCALE, row_mapping_diagnostic, value_conversion_error,
};
use crate::mssql::cell::{MssqlDate, MssqlDateTime2, MssqlTime};

pub(in crate::mssql::cell::from_arrow) fn mssql_datetime2_from_arrow_date64(
    mapping: &SchemaMapping,
    row_index: usize,
    milliseconds_from_unix_epoch: i64,
) -> Result<MssqlDateTime2> {
    let days_from_unix_epoch = milliseconds_from_unix_epoch.div_euclid(MILLISECONDS_PER_DAY);
    let milliseconds_since_midnight = milliseconds_from_unix_epoch.rem_euclid(MILLISECONDS_PER_DAY);
    let days = days_from_unix_epoch + SQL_SERVER_DATE_UNIX_EPOCH_DAYS;

    if !(0..=SQL_SERVER_DATE_MAX_DAYS).contains(&days) {
        return Err(value_conversion_error(row_mapping_diagnostic(
            mapping,
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

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_schema::{DataType, Field, Schema};

    use super::super::super::{ArrowToMssqlRuntimeMapping, mssql_cell_from_arrow_cell};
    use crate::{
        ArrowFieldRef, Date64Policy, DiagnosticCode, Identifier, MssqlColumn, MssqlProfile,
        MssqlType, PlanOptions, SchemaMapping,
        arrow::cell::ArrowCell,
        mssql::cell::{MssqlCell, MssqlDate, MssqlDateTime2, MssqlTime},
        plan_arrow_schema_to_mssql_mappings,
    };

    #[test]
    fn converts_date64_cells_to_mssql_datetime2_with_boundaries_and_null() {
        let mappings = mappings_for_schema_with_options(
            Schema::new(vec![Field::new("date_value", DataType::Date64, true)]),
            PlanOptions {
                date64_policy: Date64Policy::TimestampDateTime2,
                ..PlanOptions::default()
            },
        );
        let cases = [
            (
                0,
                ArrowCell::Date64(0),
                MssqlCell::DateTime2(Some(MssqlDateTime2::new(
                    MssqlDate::new(719_162),
                    MssqlTime::new(0, 3),
                ))),
            ),
            (
                1,
                ArrowCell::Date64(-1),
                MssqlCell::DateTime2(Some(MssqlDateTime2::new(
                    MssqlDate::new(719_161),
                    MssqlTime::new(86_399_999, 3),
                ))),
            ),
            (
                2,
                ArrowCell::Date64(86_400_123),
                MssqlCell::DateTime2(Some(MssqlDateTime2::new(
                    MssqlDate::new(719_163),
                    MssqlTime::new(123, 3),
                ))),
            ),
            (
                3,
                ArrowCell::Date64(-62_135_596_800_000),
                MssqlCell::DateTime2(Some(MssqlDateTime2::new(
                    MssqlDate::new(0),
                    MssqlTime::new(0, 3),
                ))),
            ),
            (
                4,
                ArrowCell::Date64(253_402_300_799_999),
                MssqlCell::DateTime2(Some(MssqlDateTime2::new(
                    MssqlDate::new(3_652_058),
                    MssqlTime::new(86_399_999, 3),
                ))),
            ),
            (5, ArrowCell::Null, MssqlCell::DateTime2(None)),
        ];

        for (row_index, cell, expected) in cases {
            assert_eq!(
                convert_cell(&mappings[0], cell, row_index).unwrap(),
                expected
            );
        }
    }

    #[test]
    fn rejects_date64_null_in_non_nullable_column() {
        let mappings = mappings_for_schema_with_options(
            Schema::new(vec![Field::new("date_value", DataType::Date64, false)]),
            PlanOptions {
                date64_policy: Date64Policy::TimestampDateTime2,
                ..PlanOptions::default()
            },
        );

        let err = convert_cell(&mappings[0], ArrowCell::Null, 0).unwrap_err();

        assert_single_diagnostic(
            err,
            DiagnosticCode::NullInNonNullableColumn,
            Some(0),
            Some((0, "date_value")),
        );
    }

    #[test]
    fn rejects_date64_values_outside_sql_server_datetime2_range() {
        let mappings = mappings_for_schema_with_options(
            Schema::new(vec![Field::new("date_value", DataType::Date64, false)]),
            PlanOptions {
                date64_policy: Date64Policy::TimestampDateTime2,
                ..PlanOptions::default()
            },
        );

        let below =
            convert_cell(&mappings[0], ArrowCell::Date64(-62_135_596_800_001), 0).unwrap_err();
        assert_single_diagnostic(
            below,
            DiagnosticCode::TimestampOutOfRange,
            Some(0),
            Some((0, "date_value")),
        );

        let above =
            convert_cell(&mappings[0], ArrowCell::Date64(253_402_300_800_000), 1).unwrap_err();
        assert_single_diagnostic(
            above,
            DiagnosticCode::TimestampOutOfRange,
            Some(1),
            Some((0, "date_value")),
        );
    }

    #[test]
    fn rejects_forged_date64_mapping_with_unsupported_datetime2_precision() {
        let mapping = SchemaMapping::new(
            ArrowFieldRef::new(0, "date_value".to_owned(), false, DataType::Date64),
            MssqlColumn::new(
                Identifier::new("date_value").unwrap(),
                MssqlType::DateTime2 { precision: 7 },
                false,
            ),
        );

        let err = convert_cell(&mapping, ArrowCell::Date64(0), 0).unwrap_err();

        assert_single_diagnostic(
            err,
            DiagnosticCode::ValueTypeMismatch,
            Some(0),
            Some((0, "date_value")),
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

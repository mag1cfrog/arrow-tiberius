//! Date32 Arrow-to-MSSQL runtime cell conversion.

use crate::{
    DiagnosticCode, Result, SchemaMapping, arrow::cell::ArrowCell,
    conversion::arrow_to_mssql::temporal::TemporalArrowToMssql,
};

use super::{
    SQL_SERVER_DATE_MAX_DAYS, SQL_SERVER_DATE_UNIX_EPOCH_DAYS, row_mapping_diagnostic,
    value_conversion_error,
};
use crate::mssql::cell::MssqlDate;

pub(in crate::mssql::cell::from_arrow) fn mssql_date_value(
    mapping: &SchemaMapping,
    row_index: usize,
    cell: ArrowCell<'_>,
) -> Result<MssqlDate> {
    let classification = TemporalArrowToMssql::classify(mapping, row_index)?;

    match (cell, classification) {
        (ArrowCell::Date32(value), TemporalArrowToMssql::Date32ToDate) => {
            mssql_date_from_arrow_date32(mapping, row_index, value)
        }
        other => Err(value_conversion_error(row_mapping_diagnostic(
            mapping,
            row_index,
            DiagnosticCode::ValueTypeMismatch,
            format!("expected Arrow Date32 payload, got {other:?}"),
        ))),
    }
}

fn mssql_date_from_arrow_date32(
    mapping: &SchemaMapping,
    row_index: usize,
    days_from_unix_epoch: i32,
) -> Result<MssqlDate> {
    let days = i64::from(days_from_unix_epoch) + SQL_SERVER_DATE_UNIX_EPOCH_DAYS;

    if (0..=SQL_SERVER_DATE_MAX_DAYS).contains(&days) {
        return Ok(MssqlDate::new(days as u32));
    }

    Err(value_conversion_error(row_mapping_diagnostic(
        mapping,
        row_index,
        DiagnosticCode::TimestampOutOfRange,
        format!("Arrow Date32 day offset {days_from_unix_epoch} is outside SQL Server date range"),
    )))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_schema::{DataType, Field, Schema};

    use super::super::super::{ArrowToMssqlRuntimeMapping, mssql_cell_from_arrow_cell};
    use crate::{
        DiagnosticCode, MssqlProfile, PlanOptions, SchemaMapping,
        arrow::cell::ArrowCell,
        mssql::cell::{MssqlCell, MssqlDate},
        plan_arrow_schema_to_mssql_mappings,
    };

    #[test]
    fn converts_date32_cells_to_mssql_date_with_boundaries_and_null() {
        let mappings = mappings_for_schema(Schema::new(vec![Field::new(
            "date_value",
            DataType::Date32,
            true,
        )]));
        let cases = [
            (
                0,
                ArrowCell::Date32(0),
                MssqlCell::Date(Some(MssqlDate::new(719_162))),
            ),
            (
                1,
                ArrowCell::Date32(-1),
                MssqlCell::Date(Some(MssqlDate::new(719_161))),
            ),
            (
                2,
                ArrowCell::Date32(1),
                MssqlCell::Date(Some(MssqlDate::new(719_163))),
            ),
            (
                3,
                ArrowCell::Date32(-719_162),
                MssqlCell::Date(Some(MssqlDate::new(0))),
            ),
            (
                4,
                ArrowCell::Date32(2_932_896),
                MssqlCell::Date(Some(MssqlDate::new(3_652_058))),
            ),
            (5, ArrowCell::Null, MssqlCell::Date(None)),
        ];

        for (row_index, cell, expected) in cases {
            assert_eq!(
                convert_cell(&mappings[0], cell, row_index).unwrap(),
                expected
            );
        }
    }

    #[test]
    fn rejects_date32_null_in_non_nullable_column() {
        let mappings = mappings_for_schema(Schema::new(vec![Field::new(
            "date_value",
            DataType::Date32,
            false,
        )]));

        let err = convert_cell(&mappings[0], ArrowCell::Null, 0).unwrap_err();

        assert_single_diagnostic(
            err,
            DiagnosticCode::NullInNonNullableColumn,
            Some(0),
            Some((0, "date_value")),
        );
    }

    #[test]
    fn rejects_date32_values_outside_sql_server_date_range() {
        let mappings = mappings_for_schema(Schema::new(vec![Field::new(
            "date_value",
            DataType::Date32,
            false,
        )]));

        let below = convert_cell(&mappings[0], ArrowCell::Date32(-719_163), 0).unwrap_err();
        assert_single_diagnostic(
            below,
            DiagnosticCode::TimestampOutOfRange,
            Some(0),
            Some((0, "date_value")),
        );

        let above = convert_cell(&mappings[0], ArrowCell::Date32(2_932_897), 1).unwrap_err();
        assert_single_diagnostic(
            above,
            DiagnosticCode::TimestampOutOfRange,
            Some(1),
            Some((0, "date_value")),
        );
    }

    fn convert_cell<'a>(
        mapping: &SchemaMapping,
        cell: ArrowCell<'a>,
        row_index: usize,
    ) -> crate::Result<MssqlCell<'a>> {
        let options = PlanOptions::default();
        let runtime_mapping = ArrowToMssqlRuntimeMapping::new_with_options(mapping, &options);
        mssql_cell_from_arrow_cell(runtime_mapping, cell, row_index)
    }

    fn mappings_for_schema(schema: Schema) -> Vec<SchemaMapping> {
        plan_arrow_schema_to_mssql_mappings(
            Arc::new(schema),
            MssqlProfile::sql_server_2016_compat_100(),
            PlanOptions::default(),
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

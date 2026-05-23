//! Tiberius TokenRow conversion adapter.

use std::borrow::Cow;

use crate::{
    Result,
    mssql::cell::{
        MssqlCell, MssqlDate, MssqlDateTime2, MssqlDateTimeOffset, MssqlDecimal, MssqlTime,
    },
};

use super::record_batch::RecordBatchView;

/// Converts one runtime row into an owned Tiberius token row.
pub(crate) fn tiberius_row_owned(
    view: &RecordBatchView<'_>,
    row_index: usize,
) -> Result<tiberius::TokenRow<'static>> {
    let cells = view.mssql_row(row_index)?;
    let mut row = tiberius::TokenRow::with_capacity(cells.len());

    for cell in cells {
        row.push(mssql_cell_to_tiberius_owned(cell));
    }

    Ok(row)
}

/// Converts a semantic SQL Server cell into borrowed Tiberius column data.
#[cfg(test)]
pub(crate) fn mssql_cell_to_tiberius_borrowed(cell: MssqlCell<'_>) -> tiberius::ColumnData<'_> {
    match cell {
        MssqlCell::Bit(value) => tiberius::ColumnData::Bit(value),
        MssqlCell::TinyInt(value) => tiberius::ColumnData::U8(value),
        MssqlCell::SmallInt(value) => tiberius::ColumnData::I16(value),
        MssqlCell::Int(value) => tiberius::ColumnData::I32(value),
        MssqlCell::BigInt(value) => tiberius::ColumnData::I64(value),
        MssqlCell::Decimal(value) => tiberius::ColumnData::Numeric(value.map(tiberius_numeric)),
        MssqlCell::Date(value) => tiberius::ColumnData::Date(value.map(tiberius_date)),
        MssqlCell::Time(value) => tiberius::ColumnData::Time(value.map(tiberius_time)),
        MssqlCell::DateTime2(value) => {
            tiberius::ColumnData::DateTime2(value.map(tiberius_datetime2))
        }
        MssqlCell::DateTimeOffset(value) => {
            tiberius::ColumnData::DateTimeOffset(value.map(tiberius_datetimeoffset))
        }
        MssqlCell::Real(value) => tiberius::ColumnData::F32(value),
        MssqlCell::Float(value) => tiberius::ColumnData::F64(value),
        MssqlCell::NVarChar(value) => tiberius::ColumnData::String(value.map(Cow::Borrowed)),
        MssqlCell::VarBinary(value) => tiberius::ColumnData::Binary(value.map(Cow::Borrowed)),
    }
}

/// Converts a semantic SQL Server cell into owned Tiberius column data.
pub(crate) fn mssql_cell_to_tiberius_owned(cell: MssqlCell<'_>) -> tiberius::ColumnData<'static> {
    match cell {
        MssqlCell::Bit(value) => tiberius::ColumnData::Bit(value),
        MssqlCell::TinyInt(value) => tiberius::ColumnData::U8(value),
        MssqlCell::SmallInt(value) => tiberius::ColumnData::I16(value),
        MssqlCell::Int(value) => tiberius::ColumnData::I32(value),
        MssqlCell::BigInt(value) => tiberius::ColumnData::I64(value),
        MssqlCell::Decimal(value) => tiberius::ColumnData::Numeric(value.map(tiberius_numeric)),
        MssqlCell::Date(value) => tiberius::ColumnData::Date(value.map(tiberius_date)),
        MssqlCell::Time(value) => tiberius::ColumnData::Time(value.map(tiberius_time)),
        MssqlCell::DateTime2(value) => {
            tiberius::ColumnData::DateTime2(value.map(tiberius_datetime2))
        }
        MssqlCell::DateTimeOffset(value) => {
            tiberius::ColumnData::DateTimeOffset(value.map(tiberius_datetimeoffset))
        }
        MssqlCell::Real(value) => tiberius::ColumnData::F32(value),
        MssqlCell::Float(value) => tiberius::ColumnData::F64(value),
        MssqlCell::NVarChar(value) => {
            tiberius::ColumnData::String(value.map(|value| Cow::Owned(value.to_owned())))
        }
        MssqlCell::VarBinary(value) => {
            tiberius::ColumnData::Binary(value.map(|value| Cow::Owned(value.to_vec())))
        }
    }
}

fn tiberius_numeric(value: MssqlDecimal) -> tiberius::numeric::Numeric {
    tiberius::numeric::Numeric::new_with_scale(value.unscaled(), value.scale())
}

fn tiberius_date(value: MssqlDate) -> tiberius::time::Date {
    tiberius::time::Date::new(value.days())
}

fn tiberius_time(value: MssqlTime) -> tiberius::time::Time {
    tiberius::time::Time::new(value.increments(), value.scale())
}

fn tiberius_datetime2(value: MssqlDateTime2) -> tiberius::time::DateTime2 {
    tiberius::time::DateTime2::new(tiberius_date(value.date()), tiberius_time(value.time()))
}

fn tiberius_datetimeoffset(value: MssqlDateTimeOffset) -> tiberius::time::DateTimeOffset {
    tiberius::time::DateTimeOffset::new(
        tiberius_datetime2(value.datetime2()),
        value.offset_minutes(),
    )
}

#[cfg(test)]
mod tests {
    use std::{borrow::Cow, sync::Arc};

    use arrow_array::{ArrayRef, BinaryArray, BooleanArray, Int32Array, RecordBatch, StringArray};
    use arrow_schema::{DataType, Field, Schema};

    use super::{
        MssqlCell, MssqlDate, MssqlDateTime2, MssqlDateTimeOffset, MssqlDecimal, MssqlTime,
        mssql_cell_to_tiberius_borrowed, mssql_cell_to_tiberius_owned, tiberius_row_owned,
    };
    use crate::{
        DiagnosticCode, Error, MssqlProfile, PlanOptions, SchemaMapping,
        plan_arrow_schema_to_mssql_mappings,
    };

    #[test]
    fn converts_runtime_row_to_owned_tiberius_token_row() {
        let mappings = mappings_for_schema(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("active", DataType::Boolean, true),
            Field::new("name", DataType::Utf8, true),
            Field::new("payload", DataType::Binary, true),
        ]));
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("id", DataType::Int32, false),
                Field::new("active", DataType::Boolean, true),
                Field::new("name", DataType::Utf8, true),
                Field::new("payload", DataType::Binary, true),
            ])),
            vec![
                Arc::new(Int32Array::from(vec![1_i32])) as ArrayRef,
                Arc::new(BooleanArray::from(vec![Some(true)])),
                Arc::new(StringArray::from(vec![Some("first")])),
                Arc::new(BinaryArray::from(vec![Some(&b"abc"[..])])),
            ],
        )
        .unwrap();
        let view = crate::write::record_batch::RecordBatchView::new(&batch, &mappings).unwrap();

        let row = tiberius_row_owned(&view, 0).unwrap();

        assert_eq!(row.len(), 4);
        assert_eq!(row.get(0), Some(&tiberius::ColumnData::I32(Some(1))));
        assert_eq!(row.get(1), Some(&tiberius::ColumnData::Bit(Some(true))));

        let Some(tiberius::ColumnData::String(Some(Cow::Owned(value)))) = row.get(2) else {
            panic!("expected owned string column data");
        };
        assert_eq!(value, "first");

        let Some(tiberius::ColumnData::Binary(Some(Cow::Owned(value)))) = row.get(3) else {
            panic!("expected owned binary column data");
        };
        assert_eq!(value, b"abc");
    }

    #[test]
    fn token_row_adapter_preserves_conversion_diagnostics() {
        let mappings = mappings_for_schema(Schema::new(vec![Field::new(
            "ratio",
            DataType::Float64,
            true,
        )]));
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "ratio",
                DataType::Float64,
                true,
            )])),
            vec![Arc::new(arrow_array::Float64Array::from(vec![f64::NAN]))],
        )
        .unwrap();
        let view = crate::write::record_batch::RecordBatchView::new(&batch, &mappings).unwrap();

        let err = tiberius_row_owned(&view, 0).unwrap_err();

        assert_single_diagnostic(
            err,
            DiagnosticCode::NonFiniteFloat,
            Some(0),
            Some((0, "ratio")),
        );
    }

    #[test]
    fn converts_mssql_cells_to_borrowed_tiberius_column_data() {
        assert_eq!(
            mssql_cell_to_tiberius_borrowed(MssqlCell::Bit(Some(true))),
            tiberius::ColumnData::Bit(Some(true))
        );
        assert_eq!(
            mssql_cell_to_tiberius_borrowed(MssqlCell::TinyInt(Some(8))),
            tiberius::ColumnData::U8(Some(8))
        );
        assert_eq!(
            mssql_cell_to_tiberius_borrowed(MssqlCell::SmallInt(Some(-16))),
            tiberius::ColumnData::I16(Some(-16))
        );
        assert_eq!(
            mssql_cell_to_tiberius_borrowed(MssqlCell::Int(Some(32))),
            tiberius::ColumnData::I32(Some(32))
        );
        assert_eq!(
            mssql_cell_to_tiberius_borrowed(MssqlCell::BigInt(Some(64))),
            tiberius::ColumnData::I64(Some(64))
        );
        assert_eq!(
            mssql_cell_to_tiberius_borrowed(MssqlCell::Decimal(Some(MssqlDecimal::new(12345, 2)))),
            tiberius::ColumnData::Numeric(Some(tiberius::numeric::Numeric::new_with_scale(
                12345, 2,
            )))
        );
        assert_eq!(
            mssql_cell_to_tiberius_borrowed(MssqlCell::Date(Some(MssqlDate::new(719_163)))),
            tiberius::ColumnData::Date(Some(tiberius::time::Date::new(719_163)))
        );
        assert_eq!(
            mssql_cell_to_tiberius_borrowed(MssqlCell::Time(Some(MssqlTime::new(12_345, 3)))),
            tiberius::ColumnData::Time(Some(tiberius::time::Time::new(12_345, 3)))
        );
        assert_eq!(
            mssql_cell_to_tiberius_borrowed(MssqlCell::DateTime2(Some(MssqlDateTime2::new(
                MssqlDate::new(719_163),
                MssqlTime::new(12_345, 4),
            )))),
            tiberius::ColumnData::DateTime2(Some(tiberius::time::DateTime2::new(
                tiberius::time::Date::new(719_163),
                tiberius::time::Time::new(12_345, 4),
            )))
        );
        assert_eq!(
            mssql_cell_to_tiberius_borrowed(MssqlCell::DateTimeOffset(Some(
                MssqlDateTimeOffset::new(
                    MssqlDateTime2::new(MssqlDate::new(719_163), MssqlTime::new(1, 0)),
                    -420,
                ),
            ))),
            tiberius::ColumnData::DateTimeOffset(Some(tiberius::time::DateTimeOffset::new(
                tiberius::time::DateTime2::new(
                    tiberius::time::Date::new(719_163),
                    tiberius::time::Time::new(1, 0),
                ),
                -420,
            )))
        );
        assert_eq!(
            mssql_cell_to_tiberius_borrowed(MssqlCell::Real(Some(1.25))),
            tiberius::ColumnData::F32(Some(1.25))
        );
        assert_eq!(
            mssql_cell_to_tiberius_borrowed(MssqlCell::Float(Some(2.5))),
            tiberius::ColumnData::F64(Some(2.5))
        );

        let text_data = mssql_cell_to_tiberius_borrowed(MssqlCell::NVarChar(Some("hello")));
        let tiberius::ColumnData::String(Some(Cow::Borrowed(value))) = text_data else {
            panic!("expected borrowed string column data");
        };
        assert_eq!(value, "hello");

        let binary_data = mssql_cell_to_tiberius_borrowed(MssqlCell::VarBinary(Some(b"abc")));
        let tiberius::ColumnData::Binary(Some(Cow::Borrowed(value))) = binary_data else {
            panic!("expected borrowed binary column data");
        };
        assert_eq!(value, b"abc");

        assert_eq!(
            mssql_cell_to_tiberius_borrowed(MssqlCell::NVarChar(None)),
            tiberius::ColumnData::String(None)
        );
        assert_eq!(
            mssql_cell_to_tiberius_borrowed(MssqlCell::VarBinary(None)),
            tiberius::ColumnData::Binary(None)
        );
        assert_eq!(
            mssql_cell_to_tiberius_borrowed(MssqlCell::Time(None)),
            tiberius::ColumnData::Time(None)
        );
    }

    #[test]
    fn converts_mssql_cells_to_owned_tiberius_column_data() {
        assert_eq!(
            mssql_cell_to_tiberius_owned(MssqlCell::Bit(Some(true))),
            tiberius::ColumnData::Bit(Some(true))
        );
        assert_eq!(
            mssql_cell_to_tiberius_owned(MssqlCell::Decimal(Some(MssqlDecimal::new(12345, 2)))),
            tiberius::ColumnData::Numeric(Some(tiberius::numeric::Numeric::new_with_scale(
                12345, 2,
            )))
        );
        assert_eq!(
            mssql_cell_to_tiberius_owned(MssqlCell::Date(Some(MssqlDate::new(719_163)))),
            tiberius::ColumnData::Date(Some(tiberius::time::Date::new(719_163)))
        );
        assert_eq!(
            mssql_cell_to_tiberius_owned(MssqlCell::Time(Some(MssqlTime::new(12_345, 3)))),
            tiberius::ColumnData::Time(Some(tiberius::time::Time::new(12_345, 3)))
        );
        assert_eq!(
            mssql_cell_to_tiberius_owned(MssqlCell::Real(Some(1.25))),
            tiberius::ColumnData::F32(Some(1.25))
        );
        assert_eq!(
            mssql_cell_to_tiberius_owned(MssqlCell::Float(Some(2.5))),
            tiberius::ColumnData::F64(Some(2.5))
        );

        let text_data = mssql_cell_to_tiberius_owned(MssqlCell::NVarChar(Some("hello")));
        let tiberius::ColumnData::String(Some(Cow::Owned(value))) = text_data else {
            panic!("expected owned string column data");
        };
        assert_eq!(value, "hello");

        let binary_data = mssql_cell_to_tiberius_owned(MssqlCell::VarBinary(Some(b"abc")));
        let tiberius::ColumnData::Binary(Some(Cow::Owned(value))) = binary_data else {
            panic!("expected owned binary column data");
        };
        assert_eq!(value, b"abc");
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
        error: Error,
        code: DiagnosticCode,
        row: Option<usize>,
        field: Option<(usize, &str)>,
    ) {
        let Error::ValueConversion { diagnostics } = error else {
            panic!("expected value conversion error");
        };
        assert_eq!(diagnostics.len(), 1);
        let diagnostic = &diagnostics.all()[0];
        assert_eq!(diagnostic.code(), code);
        assert_eq!(diagnostic.row(), row);
        assert_eq!(
            diagnostic
                .field()
                .map(|field| (field.index(), field.name())),
            field
        );
    }
}

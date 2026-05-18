//! Runtime record batch view and Arrow-to-MSSQL semantic conversion.

mod validate;

use arrow_array::RecordBatch;

use crate::{
    Diagnostic, DiagnosticCode, DiagnosticSet, FieldRef, PlanOptions, Result, SchemaMapping,
    arrow::cell::{ArrowCell, extract_arrow_cell},
    mssql::cell::{
        MssqlCell,
        from_arrow::{ArrowToMssqlRuntimeMapping, mssql_cell_from_arrow_cell},
    },
};
use validate::validate_runtime_columns;

/// Borrowed conversion view over one Arrow record batch and schema mappings.
#[derive(Debug)]
pub(crate) struct RecordBatchView<'a> {
    batch: &'a RecordBatch,
    mappings: &'a [SchemaMapping],
    plan_options: PlanOptions,
}

impl<'a> RecordBatchView<'a> {
    /// Creates a conversion view after validating batch columns against mappings.
    #[cfg(test)]
    pub(crate) fn new(batch: &'a RecordBatch, mappings: &'a [SchemaMapping]) -> Result<Self> {
        Self::new_with_options(batch, mappings, &PlanOptions::default())
    }

    /// Creates a conversion view with explicit write conversion policies.
    pub(crate) fn new_with_options(
        batch: &'a RecordBatch,
        mappings: &'a [SchemaMapping],
        plan_options: &PlanOptions,
    ) -> Result<Self> {
        validate_runtime_columns(batch, mappings)?;

        Ok(Self {
            batch,
            mappings,
            plan_options: *plan_options,
        })
    }

    /// Returns the number of rows in the runtime batch.
    pub(crate) fn row_count(&self) -> usize {
        self.batch.num_rows()
    }

    /// Returns the planned mappings in conversion order.
    #[cfg(test)]
    pub(crate) const fn mappings(&self) -> &[SchemaMapping] {
        self.mappings
    }

    /// Checks that a row index is inside the runtime batch.
    pub(crate) fn check_row_index(&self, row_index: usize) -> Result<()> {
        if row_index < self.row_count() {
            return Ok(());
        }

        let message = format!(
            "row index {row_index} is outside runtime batch with {} row(s)",
            self.row_count()
        );
        Err(value_conversion_error(
            Diagnostic::error(DiagnosticCode::RowIndexOutOfBounds, message).with_row(row_index),
        ))
    }

    /// Extracts one borrowed Arrow cell from a planned mapping and row index.
    fn arrow_cell(&self, mapping: &SchemaMapping, row_index: usize) -> Result<ArrowCell<'_>> {
        self.check_row_index(row_index)?;

        let Some(array) = self
            .batch
            .columns()
            .get(mapping.arrow().index())
            .map(AsRef::as_ref)
        else {
            return Err(value_conversion_error(row_mapping_diagnostic(
                mapping,
                row_index,
                DiagnosticCode::ValueTypeMismatch,
                "planned column index is outside the runtime batch",
            )));
        };

        extract_arrow_cell(array, mapping, row_index)
    }

    /// Converts one planned cell into a semantic SQL Server cell.
    fn mssql_cell(&self, mapping: &SchemaMapping, row_index: usize) -> Result<MssqlCell<'_>> {
        let cell = self.arrow_cell(mapping, row_index)?;
        let runtime_mapping = ArrowToMssqlRuntimeMapping::new(mapping, &self.plan_options);
        mssql_cell_from_arrow_cell(runtime_mapping, cell, row_index)
    }

    /// Converts one runtime row into semantic SQL Server cells in mapping order.
    pub(crate) fn mssql_row(&self, row_index: usize) -> Result<Vec<MssqlCell<'_>>> {
        self.check_row_index(row_index)?;

        let mut cells = Vec::with_capacity(self.mappings.len());
        for mapping in self.mappings {
            cells.push(self.mssql_cell(mapping, row_index)?);
        }

        Ok(cells)
    }
}

fn mapping_diagnostic(
    mapping: &SchemaMapping,
    code: DiagnosticCode,
    message: impl Into<String>,
) -> Diagnostic {
    Diagnostic::error(code, message).with_field(FieldRef::new(
        mapping.arrow().index(),
        mapping.arrow().name(),
    ))
}

fn row_mapping_diagnostic(
    mapping: &SchemaMapping,
    row_index: usize,
    code: DiagnosticCode,
    message: impl Into<String>,
) -> Diagnostic {
    mapping_diagnostic(mapping, code, message).with_row(row_index)
}

fn value_conversion_error(diagnostic: Diagnostic) -> crate::Error {
    crate::Error::ValueConversion {
        diagnostics: DiagnosticSet::from(vec![diagnostic]),
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_array::{
        ArrayRef, BinaryArray, BooleanArray, Date32Array, Date64Array, Decimal128Array,
        Float32Array, Float64Array, Int8Array, Int16Array, Int32Array, Int64Array, RecordBatch,
        StringArray, TimestampMicrosecondArray, TimestampMillisecondArray,
        TimestampNanosecondArray, TimestampSecondArray, UInt8Array, UInt16Array, UInt32Array,
    };
    use arrow_schema::{DataType, Field, Schema, TimeUnit};

    use super::RecordBatchView;
    use crate::mssql::cell::{
        MssqlCell, MssqlDate, MssqlDateTime2, MssqlDateTimeOffset, MssqlTime,
        from_arrow::timezone_resolution_from_metadata,
    };
    use crate::{
        ArrowFieldRef, Date64Policy, DiagnosticCode, Error, Identifier, MssqlColumn, MssqlProfile,
        MssqlType, NanosecondPolicy, PlanOptions, SchemaMapping, TimezonePolicy,
        plan_arrow_schema_to_mssql_mappings,
    };

    #[test]
    fn accepts_matching_batch_and_mappings() {
        let mappings = mappings_for_schema(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("active", DataType::Boolean, true),
        ]));
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("id", DataType::Int32, false),
                Field::new("active", DataType::Boolean, true),
            ])),
            vec![
                Arc::new(Int32Array::from(vec![1_i32, 2])) as ArrayRef,
                Arc::new(BooleanArray::from(vec![Some(true), None])),
            ],
        )
        .unwrap();

        let view = RecordBatchView::new(&batch, &mappings).unwrap();

        assert_eq!(view.row_count(), 2);
        assert_eq!(view.mappings().len(), 2);
        view.check_row_index(1).unwrap();
    }

    #[test]
    fn converts_runtime_row_to_mssql_cells_in_mapping_order() {
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
                Arc::new(Int32Array::from(vec![1_i32, 2])) as ArrayRef,
                Arc::new(BooleanArray::from(vec![Some(true), None])),
                Arc::new(StringArray::from(vec![Some("first"), Some("second")])),
                Arc::new(BinaryArray::from(vec![Some(&b"abc"[..]), None])),
            ],
        )
        .unwrap();
        let view = RecordBatchView::new(&batch, &mappings).unwrap();

        let first_row = view.mssql_row(0).unwrap();
        assert_eq!(
            first_row,
            vec![
                MssqlCell::Int(Some(1)),
                MssqlCell::Bit(Some(true)),
                MssqlCell::NVarChar(Some("first")),
                MssqlCell::VarBinary(Some(b"abc")),
            ]
        );

        let second_row = view.mssql_row(1).unwrap();
        assert_eq!(
            second_row,
            vec![
                MssqlCell::Int(Some(2)),
                MssqlCell::Bit(None),
                MssqlCell::NVarChar(Some("second")),
                MssqlCell::VarBinary(None),
            ]
        );
    }

    #[test]
    fn row_helpers_reject_row_index_out_of_bounds() {
        let mappings =
            mappings_for_schema(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)])),
            vec![Arc::new(Int32Array::from(vec![1_i32]))],
        )
        .unwrap();
        let view = RecordBatchView::new(&batch, &mappings).unwrap();

        let err = view.mssql_row(1).unwrap_err();
        assert_single_diagnostic(err, DiagnosticCode::RowIndexOutOfBounds, Some(1), None);
    }

    #[test]
    fn row_helpers_preserve_conversion_diagnostics() {
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
            vec![Arc::new(Float64Array::from(vec![f64::NAN]))],
        )
        .unwrap();
        let view = RecordBatchView::new(&batch, &mappings).unwrap();

        let err = view.mssql_row(0).unwrap_err();
        assert_single_diagnostic(
            err,
            DiagnosticCode::NonFiniteFloat,
            Some(0),
            Some((0, "ratio")),
        );
    }

    #[test]
    fn mssql_datetimeoffset_exposes_datetime_and_offset_components() {
        let datetime2 = MssqlDateTime2::new(MssqlDate::new(719_163), MssqlTime::new(1, 7));
        let datetimeoffset = MssqlDateTimeOffset::new(datetime2, -840);

        assert_eq!(datetimeoffset.datetime2(), datetime2);
        assert_eq!(datetimeoffset.offset_minutes(), -840);
    }

    #[test]
    fn resolves_fixed_timezone_offsets_for_datetimeoffset() {
        let mapping = timezone_timestamp_mapping("+00:00", TimezonePolicy::DateTimeOffset);

        for (timezone, expected_minutes) in [
            ("UTC", 0),
            ("+00:00", 0),
            ("-00:00", 0),
            ("+02:30", 150),
            ("+0230", 150),
            ("-07", -420),
            ("-07:45", -465),
            ("+14:00", 840),
            ("-14:00", -840),
        ] {
            let resolution = timezone_resolution_from_metadata(&mapping, 7, timezone).unwrap();

            assert_eq!(
                resolution.offset_for_instant(&mapping, 7, 0, 0).unwrap(),
                expected_minutes
            );
            assert_eq!(
                resolution
                    .offset_for_instant(&mapping, 7, 1_750_594_400, 0)
                    .unwrap(),
                expected_minutes
            );
        }
    }

    #[test]
    fn resolves_named_timezone_offsets_for_each_instant() {
        let mapping =
            timezone_timestamp_mapping("America/New_York", TimezonePolicy::DateTimeOffset);
        let resolution =
            timezone_resolution_from_metadata(&mapping, 0, "America/New_York").unwrap();

        let winter_epoch = 1_738_411_200;
        let summer_epoch = 1_750_594_400;

        assert_eq!(
            resolution
                .offset_for_instant(&mapping, 0, winter_epoch, 0)
                .unwrap(),
            -300
        );
        assert_eq!(
            resolution
                .offset_for_instant(&mapping, 1, summer_epoch, 0)
                .unwrap(),
            -240
        );
    }

    #[test]
    fn rejects_invalid_timezone_names_and_unrepresentable_offsets() {
        let mapping = timezone_timestamp_mapping("+00:00", TimezonePolicy::DateTimeOffset);

        for timezone in ["", " ", "Foobar", "+1:00", "+ab:cd", "+02:3x", "+12:60"] {
            let err = timezone_resolution_from_metadata(&mapping, 7, timezone).unwrap_err();
            assert_single_diagnostic(
                err,
                DiagnosticCode::TimezoneUnsupported,
                Some(7),
                Some((0, "ts")),
            );
        }

        let err = timezone_resolution_from_metadata(&mapping, 7, "+14:01").unwrap_err();
        assert_single_diagnostic(
            err,
            DiagnosticCode::TimezoneUnsupported,
            Some(7),
            Some((0, "ts")),
        );
    }

    #[test]
    fn converts_date32_cells_to_mssql_date_with_boundaries_and_null() {
        let mappings = mappings_for_schema(Schema::new(vec![Field::new(
            "date_value",
            DataType::Date32,
            true,
        )]));
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "date_value",
                DataType::Date32,
                true,
            )])),
            vec![Arc::new(Date32Array::from(vec![
                Some(0_i32),
                Some(-1_i32),
                Some(1_i32),
                Some(-719_162_i32),
                Some(2_932_896_i32),
                None,
            ]))],
        )
        .unwrap();
        let view = RecordBatchView::new(&batch, &mappings).unwrap();

        assert_eq!(
            view.mssql_cell(&mappings[0], 0).unwrap(),
            MssqlCell::Date(Some(MssqlDate::new(719_162)))
        );
        assert_eq!(
            view.mssql_cell(&mappings[0], 1).unwrap(),
            MssqlCell::Date(Some(MssqlDate::new(719_161)))
        );
        assert_eq!(
            view.mssql_cell(&mappings[0], 2).unwrap(),
            MssqlCell::Date(Some(MssqlDate::new(719_163)))
        );
        assert_eq!(
            view.mssql_cell(&mappings[0], 3).unwrap(),
            MssqlCell::Date(Some(MssqlDate::new(0)))
        );
        assert_eq!(
            view.mssql_cell(&mappings[0], 4).unwrap(),
            MssqlCell::Date(Some(MssqlDate::new(3_652_058)))
        );
        assert_eq!(
            view.mssql_cell(&mappings[0], 5).unwrap(),
            MssqlCell::Date(None)
        );
    }

    #[test]
    fn rejects_date32_null_in_non_nullable_column() {
        let mappings = mappings_for_schema(Schema::new(vec![Field::new(
            "date_value",
            DataType::Date32,
            false,
        )]));
        let batch = unsafe_batch_for_field(
            "date_value",
            DataType::Date32,
            Arc::new(Date32Array::from(vec![None::<i32>])),
            false,
        );
        let view = RecordBatchView::new(&batch, &mappings).unwrap();

        let err = view.mssql_cell(&mappings[0], 0).unwrap_err();

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
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "date_value",
                DataType::Date32,
                false,
            )])),
            vec![Arc::new(Date32Array::from(vec![
                -719_163_i32,
                2_932_897_i32,
            ]))],
        )
        .unwrap();
        let view = RecordBatchView::new(&batch, &mappings).unwrap();

        let below = view.mssql_cell(&mappings[0], 0).unwrap_err();
        assert_single_diagnostic(
            below,
            DiagnosticCode::TimestampOutOfRange,
            Some(0),
            Some((0, "date_value")),
        );

        let above = view.mssql_cell(&mappings[0], 1).unwrap_err();
        assert_single_diagnostic(
            above,
            DiagnosticCode::TimestampOutOfRange,
            Some(1),
            Some((0, "date_value")),
        );
    }

    #[test]
    fn converts_date64_cells_to_mssql_datetime2_with_boundaries_and_null() {
        let mappings = mappings_for_schema_with_options(
            Schema::new(vec![Field::new("date_value", DataType::Date64, true)]),
            PlanOptions {
                date64_policy: Date64Policy::TimestampDateTime2,
                ..PlanOptions::default()
            },
        );
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "date_value",
                DataType::Date64,
                true,
            )])),
            vec![Arc::new(Date64Array::from(vec![
                Some(0_i64),
                Some(-1_i64),
                Some(86_400_123_i64),
                Some(-62_135_596_800_000_i64),
                Some(253_402_300_799_999_i64),
                None,
            ]))],
        )
        .unwrap();
        let view = RecordBatchView::new(&batch, &mappings).unwrap();

        assert_eq!(
            view.mssql_cell(&mappings[0], 0).unwrap(),
            MssqlCell::DateTime2(Some(MssqlDateTime2::new(
                MssqlDate::new(719_162),
                MssqlTime::new(0, 3),
            )))
        );
        assert_eq!(
            view.mssql_cell(&mappings[0], 1).unwrap(),
            MssqlCell::DateTime2(Some(MssqlDateTime2::new(
                MssqlDate::new(719_161),
                MssqlTime::new(86_399_999, 3),
            )))
        );
        assert_eq!(
            view.mssql_cell(&mappings[0], 2).unwrap(),
            MssqlCell::DateTime2(Some(MssqlDateTime2::new(
                MssqlDate::new(719_163),
                MssqlTime::new(123, 3),
            )))
        );
        assert_eq!(
            view.mssql_cell(&mappings[0], 3).unwrap(),
            MssqlCell::DateTime2(Some(MssqlDateTime2::new(
                MssqlDate::new(0),
                MssqlTime::new(0, 3),
            )))
        );
        assert_eq!(
            view.mssql_cell(&mappings[0], 4).unwrap(),
            MssqlCell::DateTime2(Some(MssqlDateTime2::new(
                MssqlDate::new(3_652_058),
                MssqlTime::new(86_399_999, 3),
            )))
        );
        assert_eq!(
            view.mssql_cell(&mappings[0], 5).unwrap(),
            MssqlCell::DateTime2(None)
        );
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
        let batch = unsafe_batch_for_field(
            "date_value",
            DataType::Date64,
            Arc::new(Date64Array::from(vec![None::<i64>])),
            false,
        );
        let view = RecordBatchView::new(&batch, &mappings).unwrap();

        let err = view.mssql_cell(&mappings[0], 0).unwrap_err();

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
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "date_value",
                DataType::Date64,
                false,
            )])),
            vec![Arc::new(Date64Array::from(vec![
                -62_135_596_800_001_i64,
                253_402_300_800_000_i64,
            ]))],
        )
        .unwrap();
        let view = RecordBatchView::new(&batch, &mappings).unwrap();

        let below = view.mssql_cell(&mappings[0], 0).unwrap_err();
        assert_single_diagnostic(
            below,
            DiagnosticCode::TimestampOutOfRange,
            Some(0),
            Some((0, "date_value")),
        );

        let above = view.mssql_cell(&mappings[0], 1).unwrap_err();
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
        let mappings = vec![mapping];
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "date_value",
                DataType::Date64,
                false,
            )])),
            vec![Arc::new(Date64Array::from(vec![0_i64]))],
        )
        .unwrap();
        let view = RecordBatchView::new(&batch, &mappings).unwrap();

        let err = view.mssql_cell(&mappings[0], 0).unwrap_err();

        assert_single_diagnostic(
            err,
            DiagnosticCode::ValueTypeMismatch,
            Some(0),
            Some((0, "date_value")),
        );
    }

    #[test]
    fn converts_timezone_free_timestamp_cells_to_datetime2_7_with_boundaries_and_nulls() {
        let mappings = mappings_for_schema(Schema::new(vec![
            Field::new("ts_s", DataType::Timestamp(TimeUnit::Second, None), true),
            Field::new(
                "ts_ms",
                DataType::Timestamp(TimeUnit::Millisecond, None),
                true,
            ),
            Field::new(
                "ts_us",
                DataType::Timestamp(TimeUnit::Microsecond, None),
                true,
            ),
            Field::new(
                "ts_ns",
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                true,
            ),
        ]));
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("ts_s", DataType::Timestamp(TimeUnit::Second, None), true),
                Field::new(
                    "ts_ms",
                    DataType::Timestamp(TimeUnit::Millisecond, None),
                    true,
                ),
                Field::new(
                    "ts_us",
                    DataType::Timestamp(TimeUnit::Microsecond, None),
                    true,
                ),
                Field::new(
                    "ts_ns",
                    DataType::Timestamp(TimeUnit::Nanosecond, None),
                    true,
                ),
            ])),
            vec![
                Arc::new(TimestampSecondArray::from(vec![
                    Some(0_i64),
                    Some(-1_i64),
                    None,
                ])) as ArrayRef,
                Arc::new(TimestampMillisecondArray::from(vec![
                    Some(0_i64),
                    Some(-1_i64),
                    None,
                ])),
                Arc::new(TimestampMicrosecondArray::from(vec![
                    Some(1_234_567_i64),
                    Some(-1_i64),
                    None,
                ])),
                Arc::new(TimestampNanosecondArray::from(vec![
                    Some(123_456_700_i64),
                    Some(-100_i64),
                    None,
                ])),
            ],
        )
        .unwrap();
        let view = RecordBatchView::new(&batch, &mappings).unwrap();

        assert_eq!(
            view.mssql_cell(&mappings[0], 0).unwrap(),
            MssqlCell::DateTime2(Some(MssqlDateTime2::new(
                MssqlDate::new(719_162),
                MssqlTime::new(0, 7),
            )))
        );
        assert_eq!(
            view.mssql_cell(&mappings[0], 1).unwrap(),
            MssqlCell::DateTime2(Some(MssqlDateTime2::new(
                MssqlDate::new(719_161),
                MssqlTime::new(863_990_000_000, 7),
            )))
        );
        assert_eq!(
            view.mssql_cell(&mappings[0], 2).unwrap(),
            MssqlCell::DateTime2(None)
        );
        assert_eq!(
            view.mssql_cell(&mappings[1], 1).unwrap(),
            MssqlCell::DateTime2(Some(MssqlDateTime2::new(
                MssqlDate::new(719_161),
                MssqlTime::new(863_999_990_000, 7),
            )))
        );
        assert_eq!(
            view.mssql_cell(&mappings[2], 0).unwrap(),
            MssqlCell::DateTime2(Some(MssqlDateTime2::new(
                MssqlDate::new(719_162),
                MssqlTime::new(12_345_670, 7),
            )))
        );
        assert_eq!(
            view.mssql_cell(&mappings[2], 1).unwrap(),
            MssqlCell::DateTime2(Some(MssqlDateTime2::new(
                MssqlDate::new(719_161),
                MssqlTime::new(863_999_999_990, 7),
            )))
        );
        assert_eq!(
            view.mssql_cell(&mappings[3], 0).unwrap(),
            MssqlCell::DateTime2(Some(MssqlDateTime2::new(
                MssqlDate::new(719_162),
                MssqlTime::new(1_234_567, 7),
            )))
        );
        assert_eq!(
            view.mssql_cell(&mappings[3], 1).unwrap(),
            MssqlCell::DateTime2(Some(MssqlDateTime2::new(
                MssqlDate::new(719_161),
                MssqlTime::new(863_999_999_999, 7),
            )))
        );
    }

    #[test]
    fn converts_timezone_aware_timestamp_cells_to_normalized_utc_datetime2() {
        let options = PlanOptions {
            timezone_policy: TimezonePolicy::NormalizeUtcDateTime2,
            ..PlanOptions::default()
        };
        let schema = Schema::new(vec![
            Field::new(
                "new_york",
                DataType::Timestamp(TimeUnit::Second, Some("America/New_York".into())),
                true,
            ),
            Field::new(
                "offset",
                DataType::Timestamp(TimeUnit::Millisecond, Some("+02:30".into())),
                true,
            ),
            Field::new(
                "utc",
                DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
                true,
            ),
        ]);
        let mappings = mappings_for_schema_with_options(schema.clone(), options);
        let batch = RecordBatch::try_new(
            Arc::new(schema),
            vec![
                Arc::new(
                    TimestampSecondArray::from(vec![Some(0_i64), None])
                        .with_timezone("America/New_York"),
                ) as ArrayRef,
                Arc::new(
                    TimestampMillisecondArray::from(vec![Some(0_i64), None])
                        .with_timezone("+02:30"),
                ),
                Arc::new(
                    TimestampMicrosecondArray::from(vec![Some(1_234_567_i64), None])
                        .with_timezone("UTC"),
                ),
            ],
        )
        .unwrap();
        let view = RecordBatchView::new(&batch, &mappings).unwrap();

        assert_eq!(
            view.mssql_cell(&mappings[0], 0).unwrap(),
            MssqlCell::DateTime2(Some(MssqlDateTime2::new(
                MssqlDate::new(719_162),
                MssqlTime::new(0, 7),
            )))
        );
        assert_eq!(
            view.mssql_cell(&mappings[0], 1).unwrap(),
            MssqlCell::DateTime2(None)
        );
        assert_eq!(
            view.mssql_cell(&mappings[1], 0).unwrap(),
            MssqlCell::DateTime2(Some(MssqlDateTime2::new(
                MssqlDate::new(719_162),
                MssqlTime::new(0, 7),
            )))
        );
        assert_eq!(
            view.mssql_cell(&mappings[1], 1).unwrap(),
            MssqlCell::DateTime2(None)
        );
        assert_eq!(
            view.mssql_cell(&mappings[2], 0).unwrap(),
            MssqlCell::DateTime2(Some(MssqlDateTime2::new(
                MssqlDate::new(719_162),
                MssqlTime::new(12_345_670, 7),
            )))
        );
        assert_eq!(
            view.mssql_cell(&mappings[2], 1).unwrap(),
            MssqlCell::DateTime2(None)
        );
    }

    #[test]
    fn rejects_invalid_timezone_metadata_for_normalized_utc_datetime2() {
        let options = PlanOptions {
            timezone_policy: TimezonePolicy::NormalizeUtcDateTime2,
            ..PlanOptions::default()
        };
        let schema = Schema::new(vec![Field::new(
            "ts",
            DataType::Timestamp(TimeUnit::Second, Some("Foobar".into())),
            false,
        )]);
        let mappings = mappings_for_schema_with_options(schema.clone(), options);
        let batch = RecordBatch::try_new(
            Arc::new(schema),
            vec![Arc::new(
                TimestampSecondArray::from(vec![0_i64]).with_timezone("Foobar"),
            )],
        )
        .unwrap();
        let view = RecordBatchView::new(&batch, &mappings).unwrap();

        let err = view.mssql_cell(&mappings[0], 0).unwrap_err();

        assert_single_diagnostic(
            err,
            DiagnosticCode::TimezoneUnsupported,
            Some(0),
            Some((0, "ts")),
        );
    }

    #[test]
    fn rejects_invalid_timezone_metadata_for_null_normalized_utc_datetime2() {
        let options = PlanOptions {
            timezone_policy: TimezonePolicy::NormalizeUtcDateTime2,
            ..PlanOptions::default()
        };
        let schema = Schema::new(vec![Field::new(
            "ts",
            DataType::Timestamp(TimeUnit::Second, Some("Foobar".into())),
            true,
        )]);
        let mappings = mappings_for_schema_with_options(schema.clone(), options);
        let batch = RecordBatch::try_new(
            Arc::new(schema),
            vec![Arc::new(
                TimestampSecondArray::from(vec![None::<i64>]).with_timezone("Foobar"),
            )],
        )
        .unwrap();
        let view = RecordBatchView::new(&batch, &mappings).unwrap();

        let err = view.mssql_cell(&mappings[0], 0).unwrap_err();

        assert_single_diagnostic(
            err,
            DiagnosticCode::TimezoneUnsupported,
            Some(0),
            Some((0, "ts")),
        );
    }

    #[test]
    fn applies_nanosecond_policy_to_timezone_aware_normalized_utc_datetime2() {
        let options = PlanOptions {
            timezone_policy: TimezonePolicy::NormalizeUtcDateTime2,
            nanosecond_policy: NanosecondPolicy::RoundTo100ns,
            ..PlanOptions::default()
        };
        let schema = Schema::new(vec![Field::new(
            "ts_ns",
            DataType::Timestamp(TimeUnit::Nanosecond, Some("America/New_York".into())),
            false,
        )]);
        let mappings = mappings_for_schema_with_options(schema.clone(), options);
        let batch = RecordBatch::try_new(
            Arc::new(schema),
            vec![Arc::new(
                TimestampNanosecondArray::from(vec![150_i64]).with_timezone("America/New_York"),
            )],
        )
        .unwrap();
        let view = RecordBatchView::new_with_options(&batch, &mappings, &options).unwrap();

        assert_eq!(
            view.mssql_cell(&mappings[0], 0).unwrap(),
            MssqlCell::DateTime2(Some(MssqlDateTime2::new(
                MssqlDate::new(719_162),
                MssqlTime::new(2, 7),
            )))
        );
    }

    #[test]
    fn converts_timezone_aware_timestamp_cells_to_datetimeoffset() {
        let options = PlanOptions {
            timezone_policy: TimezonePolicy::DateTimeOffset,
            ..PlanOptions::default()
        };
        let schema = Schema::new(vec![
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
        ]);
        let mappings = mappings_for_schema_with_options(schema.clone(), options);
        let batch = RecordBatch::try_new(
            Arc::new(schema),
            vec![
                Arc::new(
                    TimestampMillisecondArray::from(vec![Some(0_i64), None])
                        .with_timezone("+02:30"),
                ) as ArrayRef,
                Arc::new(
                    TimestampNanosecondArray::from(vec![Some(0_i64), None]).with_timezone("-07"),
                ),
                Arc::new(
                    TimestampMicrosecondArray::from(vec![Some(1_234_567_i64), None])
                        .with_timezone("UTC"),
                ),
            ],
        )
        .unwrap();
        let view = RecordBatchView::new(&batch, &mappings).unwrap();

        assert_eq!(
            view.mssql_cell(&mappings[0], 0).unwrap(),
            MssqlCell::DateTimeOffset(Some(MssqlDateTimeOffset::new(
                MssqlDateTime2::new(MssqlDate::new(719_162), MssqlTime::new(0, 7)),
                150,
            )))
        );
        assert_eq!(
            view.mssql_cell(&mappings[0], 1).unwrap(),
            MssqlCell::DateTimeOffset(None)
        );
        assert_eq!(
            view.mssql_cell(&mappings[1], 0).unwrap(),
            MssqlCell::DateTimeOffset(Some(MssqlDateTimeOffset::new(
                MssqlDateTime2::new(MssqlDate::new(719_162), MssqlTime::new(0, 7)),
                -420,
            )))
        );
        assert_eq!(
            view.mssql_cell(&mappings[1], 1).unwrap(),
            MssqlCell::DateTimeOffset(None)
        );
        assert_eq!(
            view.mssql_cell(&mappings[2], 0).unwrap(),
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
        let schema = Schema::new(vec![Field::new(
            "new_york",
            DataType::Timestamp(TimeUnit::Second, Some("America/New_York".into())),
            false,
        )]);
        let mappings = mappings_for_schema_with_options(schema.clone(), options);
        let batch = RecordBatch::try_new(
            Arc::new(schema),
            vec![Arc::new(
                TimestampSecondArray::from(vec![1_738_411_200_i64, 1_750_593_600_i64])
                    .with_timezone("America/New_York"),
            )],
        )
        .unwrap();
        let view = RecordBatchView::new(&batch, &mappings).unwrap();

        assert_eq!(
            view.mssql_cell(&mappings[0], 0).unwrap(),
            MssqlCell::DateTimeOffset(Some(MssqlDateTimeOffset::new(
                MssqlDateTime2::new(MssqlDate::new(739_282), MssqlTime::new(432_000_000_000, 7)),
                -300,
            )))
        );
        assert_eq!(
            view.mssql_cell(&mappings[0], 1).unwrap(),
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
        let schema = Schema::new(vec![Field::new(
            "ts",
            DataType::Timestamp(TimeUnit::Second, Some("Foobar".into())),
            false,
        )]);
        let mappings = mappings_for_schema_with_options(schema.clone(), options);
        let batch = RecordBatch::try_new(
            Arc::new(schema),
            vec![Arc::new(
                TimestampSecondArray::from(vec![0_i64]).with_timezone("Foobar"),
            )],
        )
        .unwrap();
        let view = RecordBatchView::new(&batch, &mappings).unwrap();

        let err = view.mssql_cell(&mappings[0], 0).unwrap_err();

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
        let schema = Schema::new(vec![Field::new(
            "ts",
            DataType::Timestamp(TimeUnit::Second, Some("Foobar".into())),
            true,
        )]);
        let mappings = mappings_for_schema_with_options(schema.clone(), options);
        let batch = RecordBatch::try_new(
            Arc::new(schema),
            vec![Arc::new(
                TimestampSecondArray::from(vec![None::<i64>]).with_timezone("Foobar"),
            )],
        )
        .unwrap();
        let view = RecordBatchView::new(&batch, &mappings).unwrap();

        let err = view.mssql_cell(&mappings[0], 0).unwrap_err();

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
        let schema = Schema::new(vec![Field::new(
            "ts_ns",
            DataType::Timestamp(TimeUnit::Nanosecond, Some("+00:00".into())),
            false,
        )]);
        let mappings = mappings_for_schema_with_options(schema.clone(), options);
        let batch = RecordBatch::try_new(
            Arc::new(schema),
            vec![Arc::new(
                TimestampNanosecondArray::from(vec![150_i64]).with_timezone("+00:00"),
            )],
        )
        .unwrap();
        let view = RecordBatchView::new_with_options(&batch, &mappings, &options).unwrap();

        assert_eq!(
            view.mssql_cell(&mappings[0], 0).unwrap(),
            MssqlCell::DateTimeOffset(Some(MssqlDateTimeOffset::new(
                MssqlDateTime2::new(MssqlDate::new(719_162), MssqlTime::new(2, 7)),
                0,
            )))
        );
    }

    #[test]
    fn rejects_timezone_aware_normalized_utc_values_outside_datetime2_range() {
        let options = PlanOptions {
            timezone_policy: TimezonePolicy::NormalizeUtcDateTime2,
            ..PlanOptions::default()
        };
        let schema = Schema::new(vec![Field::new(
            "ts_s",
            DataType::Timestamp(TimeUnit::Second, Some("America/New_York".into())),
            false,
        )]);
        let mappings = mappings_for_schema_with_options(schema.clone(), options);
        let batch = RecordBatch::try_new(
            Arc::new(schema),
            vec![Arc::new(
                TimestampSecondArray::from(vec![i64::MIN, i64::MAX])
                    .with_timezone("America/New_York"),
            )],
        )
        .unwrap();
        let view = RecordBatchView::new(&batch, &mappings).unwrap();

        let below = view.mssql_cell(&mappings[0], 0).unwrap_err();
        assert_single_diagnostic(
            below,
            DiagnosticCode::TimestampOutOfRange,
            Some(0),
            Some((0, "ts_s")),
        );

        let above = view.mssql_cell(&mappings[0], 1).unwrap_err();
        assert_single_diagnostic(
            above,
            DiagnosticCode::TimestampOutOfRange,
            Some(1),
            Some((0, "ts_s")),
        );
    }

    #[test]
    fn rejects_datetimeoffset_values_outside_local_sql_server_range_after_offset() {
        let options = PlanOptions {
            timezone_policy: TimezonePolicy::DateTimeOffset,
            ..PlanOptions::default()
        };
        let schema = Schema::new(vec![
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
        ]);
        let mappings = mappings_for_schema_with_options(schema.clone(), options);
        let batch = RecordBatch::try_new(
            Arc::new(schema),
            vec![
                Arc::new(
                    TimestampSecondArray::from(vec![-62_135_596_800_i64]).with_timezone("-14:00"),
                ) as ArrayRef,
                Arc::new(
                    TimestampSecondArray::from(vec![253_402_300_799_i64]).with_timezone("+14:00"),
                ),
            ],
        )
        .unwrap();
        let view = RecordBatchView::new(&batch, &mappings).unwrap();

        let below = view.mssql_cell(&mappings[0], 0).unwrap_err();
        assert_single_diagnostic(
            below,
            DiagnosticCode::TimestampOutOfRange,
            Some(0),
            Some((0, "too_early")),
        );

        let above = view.mssql_cell(&mappings[1], 0).unwrap_err();
        assert_single_diagnostic(
            above,
            DiagnosticCode::TimestampOutOfRange,
            Some(0),
            Some((1, "too_late")),
        );
    }

    #[test]
    fn rejects_nanosecond_timestamp_precision_loss_by_default() {
        let mappings = mappings_for_schema(Schema::new(vec![Field::new(
            "ts_ns",
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            false,
        )]));
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "ts_ns",
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                false,
            )])),
            vec![Arc::new(TimestampNanosecondArray::from(vec![101_i64]))],
        )
        .unwrap();
        let view = RecordBatchView::new(&batch, &mappings).unwrap();

        let err = view.mssql_cell(&mappings[0], 0).unwrap_err();

        assert_single_diagnostic(
            err,
            DiagnosticCode::LossyConversionRequiresPolicy,
            Some(0),
            Some((0, "ts_ns")),
        );
    }

    #[test]
    fn applies_nanosecond_round_and_truncate_policies_at_runtime() {
        let options = PlanOptions {
            nanosecond_policy: NanosecondPolicy::RoundTo100ns,
            ..PlanOptions::default()
        };
        let mappings = mappings_for_schema_with_options(
            Schema::new(vec![Field::new(
                "ts_ns",
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                false,
            )]),
            options,
        );
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "ts_ns",
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                false,
            )])),
            vec![Arc::new(TimestampNanosecondArray::from(vec![
                149_i64, 150_i64, -149_i64,
            ]))],
        )
        .unwrap();
        let round_view = RecordBatchView::new_with_options(&batch, &mappings, &options).unwrap();
        let truncate_view = RecordBatchView::new_with_options(
            &batch,
            &mappings,
            &PlanOptions {
                nanosecond_policy: NanosecondPolicy::TruncateTo100ns,
                ..PlanOptions::default()
            },
        )
        .unwrap();

        assert_eq!(
            round_view.mssql_cell(&mappings[0], 0).unwrap(),
            MssqlCell::DateTime2(Some(MssqlDateTime2::new(
                MssqlDate::new(719_162),
                MssqlTime::new(1, 7),
            )))
        );
        assert_eq!(
            round_view.mssql_cell(&mappings[0], 1).unwrap(),
            MssqlCell::DateTime2(Some(MssqlDateTime2::new(
                MssqlDate::new(719_162),
                MssqlTime::new(2, 7),
            )))
        );
        assert_eq!(
            round_view.mssql_cell(&mappings[0], 2).unwrap(),
            MssqlCell::DateTime2(Some(MssqlDateTime2::new(
                MssqlDate::new(719_161),
                MssqlTime::new(863_999_999_999, 7),
            )))
        );
        assert_eq!(
            truncate_view.mssql_cell(&mappings[0], 0).unwrap(),
            MssqlCell::DateTime2(Some(MssqlDateTime2::new(
                MssqlDate::new(719_162),
                MssqlTime::new(1, 7),
            )))
        );
        assert_eq!(
            truncate_view.mssql_cell(&mappings[0], 1).unwrap(),
            MssqlCell::DateTime2(Some(MssqlDateTime2::new(
                MssqlDate::new(719_162),
                MssqlTime::new(1, 7),
            )))
        );
        assert_eq!(
            truncate_view.mssql_cell(&mappings[0], 2).unwrap(),
            MssqlCell::DateTime2(Some(MssqlDateTime2::new(
                MssqlDate::new(719_161),
                MssqlTime::new(863_999_999_998, 7),
            )))
        );
    }

    #[test]
    fn rejects_timestamp_values_outside_sql_server_datetime2_range() {
        let mappings = mappings_for_schema(Schema::new(vec![Field::new(
            "ts_s",
            DataType::Timestamp(TimeUnit::Second, None),
            false,
        )]));
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "ts_s",
                DataType::Timestamp(TimeUnit::Second, None),
                false,
            )])),
            vec![Arc::new(TimestampSecondArray::from(vec![
                i64::MIN,
                i64::MAX,
            ]))],
        )
        .unwrap();
        let view = RecordBatchView::new(&batch, &mappings).unwrap();

        let below = view.mssql_cell(&mappings[0], 0).unwrap_err();
        assert_single_diagnostic(
            below,
            DiagnosticCode::TimestampOutOfRange,
            Some(0),
            Some((0, "ts_s")),
        );

        let above = view.mssql_cell(&mappings[0], 1).unwrap_err();
        assert_single_diagnostic(
            above,
            DiagnosticCode::TimestampOutOfRange,
            Some(1),
            Some((0, "ts_s")),
        );
    }

    #[test]
    fn preserves_integer_boundaries_during_widening() {
        let mappings = mappings_for_schema(Schema::new(vec![
            Field::new("tiny", DataType::Int8, false),
            Field::new("small", DataType::Int16, false),
            Field::new("unsigned_tiny", DataType::UInt8, false),
            Field::new("unsigned_medium", DataType::UInt16, false),
            Field::new("unsigned_large", DataType::UInt32, false),
        ]));
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("tiny", DataType::Int8, false),
                Field::new("small", DataType::Int16, false),
                Field::new("unsigned_tiny", DataType::UInt8, false),
                Field::new("unsigned_medium", DataType::UInt16, false),
                Field::new("unsigned_large", DataType::UInt32, false),
            ])),
            vec![
                Arc::new(Int8Array::from(vec![i8::MIN, i8::MAX])) as ArrayRef,
                Arc::new(Int16Array::from(vec![i16::MIN, i16::MAX])),
                Arc::new(UInt8Array::from(vec![u8::MIN, u8::MAX])),
                Arc::new(UInt16Array::from(vec![u16::MIN, u16::MAX])),
                Arc::new(UInt32Array::from(vec![u32::MIN, u32::MAX])),
            ],
        )
        .unwrap();
        let view = RecordBatchView::new(&batch, &mappings).unwrap();

        assert_eq!(
            view.mssql_cell(&mappings[0], 0).unwrap(),
            MssqlCell::SmallInt(Some(i16::from(i8::MIN)))
        );
        assert_eq!(
            view.mssql_cell(&mappings[0], 1).unwrap(),
            MssqlCell::SmallInt(Some(i16::from(i8::MAX)))
        );
        assert_eq!(
            view.mssql_cell(&mappings[1], 0).unwrap(),
            MssqlCell::SmallInt(Some(i16::MIN))
        );
        assert_eq!(
            view.mssql_cell(&mappings[1], 1).unwrap(),
            MssqlCell::SmallInt(Some(i16::MAX))
        );
        assert_eq!(
            view.mssql_cell(&mappings[2], 0).unwrap(),
            MssqlCell::TinyInt(Some(u8::MIN))
        );
        assert_eq!(
            view.mssql_cell(&mappings[2], 1).unwrap(),
            MssqlCell::TinyInt(Some(u8::MAX))
        );
        assert_eq!(
            view.mssql_cell(&mappings[3], 0).unwrap(),
            MssqlCell::Int(Some(i32::from(u16::MIN)))
        );
        assert_eq!(
            view.mssql_cell(&mappings[3], 1).unwrap(),
            MssqlCell::Int(Some(i32::from(u16::MAX)))
        );
        assert_eq!(
            view.mssql_cell(&mappings[4], 0).unwrap(),
            MssqlCell::BigInt(Some(i64::from(u32::MIN)))
        );
        assert_eq!(
            view.mssql_cell(&mappings[4], 1).unwrap(),
            MssqlCell::BigInt(Some(i64::from(u32::MAX)))
        );
    }

    #[test]
    fn rejects_null_in_non_nullable_planned_column() {
        let mappings = mappings_for_schema(Schema::new(vec![Field::new(
            "active",
            DataType::Boolean,
            false,
        )]));
        let batch = unsafe_batch_for_field(
            "active",
            DataType::Boolean,
            Arc::new(BooleanArray::from(vec![None::<bool>])),
            false,
        );
        let view = RecordBatchView::new(&batch, &mappings).unwrap();

        let err = view.mssql_cell(&mappings[0], 0).unwrap_err();

        assert_single_diagnostic(
            err,
            DiagnosticCode::NullInNonNullableColumn,
            Some(0),
            Some((0, "active")),
        );
    }

    #[test]
    fn rejects_non_finite_float32_values() {
        let mappings = mappings_for_schema(Schema::new(vec![Field::new(
            "ratio",
            DataType::Float32,
            true,
        )]));
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "ratio",
                DataType::Float32,
                true,
            )])),
            vec![Arc::new(Float32Array::from(vec![
                f32::NAN,
                f32::INFINITY,
                f32::NEG_INFINITY,
            ]))],
        )
        .unwrap();
        let view = RecordBatchView::new(&batch, &mappings).unwrap();

        for row_index in 0..3 {
            let err = view.mssql_cell(&mappings[0], row_index).unwrap_err();

            assert_single_diagnostic(
                err,
                DiagnosticCode::NonFiniteFloat,
                Some(row_index),
                Some((0, "ratio")),
            );
        }
    }

    #[test]
    fn rejects_non_finite_float64_values() {
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
            vec![Arc::new(Float64Array::from(vec![
                f64::NAN,
                f64::INFINITY,
                f64::NEG_INFINITY,
            ]))],
        )
        .unwrap();
        let view = RecordBatchView::new(&batch, &mappings).unwrap();

        for row_index in 0..3 {
            let err = view.mssql_cell(&mappings[0], row_index).unwrap_err();

            assert_single_diagnostic(
                err,
                DiagnosticCode::NonFiniteFloat,
                Some(row_index),
                Some((0, "ratio")),
            );
        }
    }

    #[test]
    fn rejects_payload_that_does_not_fit_planned_mssql_type() {
        let mappings = vec![SchemaMapping::new(
            ArrowFieldRef::new(0, "id".to_owned(), false, DataType::Int32),
            MssqlColumn::new(Identifier::new("id").unwrap(), MssqlType::BigInt, false),
        )];
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)])),
            vec![Arc::new(Int32Array::from(vec![7_i32]))],
        )
        .unwrap();
        let view = RecordBatchView::new(&batch, &mappings).unwrap();

        let err = view.mssql_cell(&mappings[0], 0).unwrap_err();

        assert_single_diagnostic(
            err,
            DiagnosticCode::ValueTypeMismatch,
            Some(0),
            Some((0, "id")),
        );
    }

    #[test]
    fn rejects_decimal_mapping_scale_mismatch_before_value_corruption() {
        let mappings = vec![SchemaMapping::new(
            ArrowFieldRef::new(0, "amount".to_owned(), false, DataType::Decimal128(5, 2)),
            MssqlColumn::new(
                Identifier::new("amount").unwrap(),
                MssqlType::Decimal {
                    precision: 5,
                    scale: 0,
                },
                false,
            ),
        )];
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "amount",
                DataType::Decimal128(5, 2),
                false,
            )])),
            vec![Arc::new(
                Decimal128Array::from(vec![123_i128])
                    .with_precision_and_scale(5, 2)
                    .unwrap(),
            )],
        )
        .unwrap();
        let view = RecordBatchView::new(&batch, &mappings).unwrap();

        let err = view.mssql_cell(&mappings[0], 0).unwrap_err();

        assert_single_diagnostic(
            err,
            DiagnosticCode::SchemaMismatch,
            Some(0),
            Some((0, "amount")),
        );
    }

    #[test]
    fn rejects_planned_column_index_outside_runtime_batch() {
        let mappings = mappings_for_schema(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("active", DataType::Boolean, true),
        ]));
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)])),
            vec![Arc::new(Int32Array::from(vec![1_i32]))],
        )
        .unwrap();

        let err = RecordBatchView::new(&batch, &mappings).unwrap_err();

        assert_single_diagnostic(
            err,
            DiagnosticCode::SchemaMismatch,
            None,
            Some((1, "active")),
        );
    }

    #[test]
    fn rejects_extra_runtime_columns_without_mappings() {
        let mappings =
            mappings_for_schema(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("id", DataType::Int32, false),
                Field::new("extra", DataType::Boolean, true),
            ])),
            vec![
                Arc::new(Int32Array::from(vec![1_i32])) as ArrayRef,
                Arc::new(BooleanArray::from(vec![Some(true)])),
            ],
        )
        .unwrap();

        let err = RecordBatchView::new(&batch, &mappings).unwrap_err();

        assert_single_diagnostic(err, DiagnosticCode::SchemaMismatch, None, None);
    }

    #[test]
    fn rejects_mapping_position_that_disagrees_with_arrow_index() {
        let mappings = vec![SchemaMapping::new(
            ArrowFieldRef::new(1, "id".to_owned(), false, DataType::Int32),
            MssqlColumn::new(Identifier::new("id").unwrap(), MssqlType::Int, false),
        )];
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)])),
            vec![Arc::new(Int32Array::from(vec![1_i32]))],
        )
        .unwrap();

        let err = RecordBatchView::new(&batch, &mappings).unwrap_err();

        assert_single_diagnostic(err, DiagnosticCode::SchemaMismatch, None, Some((1, "id")));
    }

    #[test]
    fn rejects_runtime_field_name_mismatch_even_when_type_matches() {
        let mappings = mappings_for_schema(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("amount", DataType::Int32, false),
        ]));
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("amount", DataType::Int32, false),
                Field::new("id", DataType::Int32, false),
            ])),
            vec![
                Arc::new(Int32Array::from(vec![100_i32])) as ArrayRef,
                Arc::new(Int32Array::from(vec![1_i32])),
            ],
        )
        .unwrap();

        let err = RecordBatchView::new(&batch, &mappings).unwrap_err();

        assert_single_diagnostic(err, DiagnosticCode::SchemaMismatch, None, Some((0, "id")));
    }

    #[test]
    fn rejects_runtime_field_rename_even_when_position_and_type_match() {
        let mappings =
            mappings_for_schema(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "renamed_id",
                DataType::Int32,
                false,
            )])),
            vec![Arc::new(Int32Array::from(vec![1_i32]))],
        )
        .unwrap();

        let err = RecordBatchView::new(&batch, &mappings).unwrap_err();

        assert_single_diagnostic(err, DiagnosticCode::SchemaMismatch, None, Some((0, "id")));
    }

    #[test]
    fn rejects_runtime_arrow_type_mismatch() {
        let mappings = mappings_for_schema(Schema::new(vec![Field::new(
            "number",
            DataType::Int32,
            true,
        )]));
        let batch = unsafe_batch_for_field(
            "number",
            DataType::Int32,
            Arc::new(Int64Array::from(vec![1_i64])),
            true,
        );

        let err = RecordBatchView::new(&batch, &mappings).unwrap_err();

        assert_single_diagnostic(
            err,
            DiagnosticCode::SchemaMismatch,
            None,
            Some((0, "number")),
        );
    }

    #[test]
    fn rejects_row_index_out_of_bounds() {
        let mappings = mappings_for_schema(Schema::new(vec![Field::new(
            "number",
            DataType::Int32,
            true,
        )]));
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "number",
                DataType::Int32,
                true,
            )])),
            vec![Arc::new(Int32Array::from(vec![1_i32]))],
        )
        .unwrap();
        let view = RecordBatchView::new(&batch, &mappings).unwrap();

        let err = view.check_row_index(1).unwrap_err();

        assert_single_diagnostic(err, DiagnosticCode::RowIndexOutOfBounds, Some(1), None);
    }

    fn mappings_for_schema(schema: Schema) -> Vec<SchemaMapping> {
        mappings_for_schema_with_options(schema, PlanOptions::default())
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

    fn timezone_timestamp_mapping(
        timezone: &str,
        timezone_policy: TimezonePolicy,
    ) -> SchemaMapping {
        mappings_for_schema_with_options(
            Schema::new(vec![Field::new(
                "ts",
                DataType::Timestamp(TimeUnit::Second, Some(timezone.into())),
                true,
            )]),
            PlanOptions {
                timezone_policy,
                ..PlanOptions::default()
            },
        )
        .remove(0)
    }

    fn unsafe_batch_for_field(
        name: &str,
        data_type: DataType,
        array: ArrayRef,
        nullable: bool,
    ) -> RecordBatch {
        // SAFETY: this deliberately constructs a mismatched batch for converter
        // validation tests. The test only inspects metadata and never reads the
        // mismatched array through the declared schema type.
        unsafe {
            RecordBatch::new_unchecked(
                Arc::new(Schema::new(vec![Field::new(name, data_type, nullable)])),
                vec![array],
                1,
            )
        }
    }

    fn assert_single_diagnostic(
        err: Error,
        expected_code: DiagnosticCode,
        expected_row: Option<usize>,
        expected_field: Option<(usize, &str)>,
    ) {
        let Error::ValueConversion { diagnostics } = err else {
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

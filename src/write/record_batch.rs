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
        ArrayRef, BinaryArray, BooleanArray, Date32Array, Date64Array, Decimal32Array,
        Decimal64Array, Decimal128Array, Decimal256Array, Float32Array, Float64Array, Int8Array,
        Int16Array, Int32Array, Int64Array, LargeBinaryArray, LargeStringArray, RecordBatch,
        StringArray, TimestampMicrosecondArray, TimestampMillisecondArray,
        TimestampNanosecondArray, TimestampSecondArray, UInt8Array, UInt16Array, UInt32Array,
        UInt64Array,
    };
    use arrow_buffer::i256;
    use arrow_data::ArrayData;
    use arrow_schema::{DataType, Field, Schema, TimeUnit};

    use super::RecordBatchView;
    use crate::arrow::cell::ArrowCell;
    use crate::mssql::cell::{
        MssqlCell, MssqlDate, MssqlDateTime2, MssqlDateTimeOffset, MssqlDecimal, MssqlTime,
        from_arrow::{ArrowToMssqlRuntimeMapping, timezone_resolution_from_metadata},
    };
    use crate::{
        ArrowFieldRef, BinaryPolicy, Date64Policy, DecimalPolicy, DiagnosticCode, Error,
        Identifier, MssqlColumn, MssqlProfile, MssqlType, NanosecondPolicy, PlanOptions,
        SchemaMapping, StringPolicy, TimezonePolicy, UInt64Policy,
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
    fn runtime_mapping_keeps_write_policy_out_of_schema_mapping() {
        let options = PlanOptions {
            nanosecond_policy: NanosecondPolicy::TruncateTo100ns,
            ..PlanOptions::default()
        };
        let mappings = mappings_for_schema_with_options(
            Schema::new(vec![Field::new(
                "created_at",
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                true,
            )]),
            options,
        );

        let runtime_mapping = ArrowToMssqlRuntimeMapping::new(&mappings[0], &options);

        assert_eq!(runtime_mapping.mapping(), &mappings[0]);
        assert_eq!(
            runtime_mapping.nanosecond_policy(),
            NanosecondPolicy::TruncateTo100ns
        );
        assert_eq!(
            mappings[0].mssql().ty(),
            &MssqlType::DateTime2 { precision: 7 }
        );
    }

    #[test]
    fn extracts_arrow_cells_for_supported_initial_primitives() {
        let mappings = mappings_for_schema(Schema::new(vec![
            Field::new("active", DataType::Boolean, true),
            Field::new("tiny", DataType::Int8, true),
            Field::new("small", DataType::Int16, true),
            Field::new("quantity", DataType::Int32, true),
            Field::new("total", DataType::Int64, true),
            Field::new("unsigned_tiny", DataType::UInt8, true),
            Field::new("unsigned_medium", DataType::UInt16, true),
            Field::new("unsigned_large", DataType::UInt32, true),
            Field::new("real_value", DataType::Float32, true),
            Field::new("float_value", DataType::Float64, true),
            Field::new("text", DataType::Utf8, true),
            Field::new("large_text", DataType::LargeUtf8, true),
            Field::new("bytes", DataType::Binary, true),
            Field::new("large_bytes", DataType::LargeBinary, true),
        ]));
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("active", DataType::Boolean, true),
                Field::new("tiny", DataType::Int8, true),
                Field::new("small", DataType::Int16, true),
                Field::new("quantity", DataType::Int32, true),
                Field::new("total", DataType::Int64, true),
                Field::new("unsigned_tiny", DataType::UInt8, true),
                Field::new("unsigned_medium", DataType::UInt16, true),
                Field::new("unsigned_large", DataType::UInt32, true),
                Field::new("real_value", DataType::Float32, true),
                Field::new("float_value", DataType::Float64, true),
                Field::new("text", DataType::Utf8, true),
                Field::new("large_text", DataType::LargeUtf8, true),
                Field::new("bytes", DataType::Binary, true),
                Field::new("large_bytes", DataType::LargeBinary, true),
            ])),
            vec![
                Arc::new(BooleanArray::from(vec![Some(true), None])) as ArrayRef,
                Arc::new(Int8Array::from(vec![Some(-8_i8), None])),
                Arc::new(Int16Array::from(vec![Some(-16_i16), None])),
                Arc::new(Int32Array::from(vec![Some(12_i32), None])),
                Arc::new(Int64Array::from(vec![Some(34_i64), None])),
                Arc::new(UInt8Array::from(vec![Some(8_u8), None])),
                Arc::new(UInt16Array::from(vec![Some(16_u16), None])),
                Arc::new(UInt32Array::from(vec![Some(32_u32), None])),
                Arc::new(Float32Array::from(vec![Some(1.25_f32), None])),
                Arc::new(Float64Array::from(vec![Some(2.5_f64), None])),
                Arc::new(StringArray::from(vec![Some("hello"), None])),
                Arc::new(LargeStringArray::from(vec![Some("東京"), None])),
                Arc::new(BinaryArray::from(vec![Some(&b"abc"[..]), None])),
                Arc::new(LargeBinaryArray::from(vec![Some(&b"large"[..]), None])),
            ],
        )
        .unwrap();
        let view = RecordBatchView::new(&batch, &mappings).unwrap();

        assert_eq!(
            view.arrow_cell(&mappings[0], 0).unwrap(),
            ArrowCell::Boolean(true)
        );
        assert_eq!(view.arrow_cell(&mappings[0], 1).unwrap(), ArrowCell::Null);
        assert_eq!(
            view.arrow_cell(&mappings[1], 0).unwrap(),
            ArrowCell::Int8(-8)
        );
        assert_eq!(view.arrow_cell(&mappings[1], 1).unwrap(), ArrowCell::Null);
        assert_eq!(
            view.arrow_cell(&mappings[2], 0).unwrap(),
            ArrowCell::Int16(-16)
        );
        assert_eq!(view.arrow_cell(&mappings[2], 1).unwrap(), ArrowCell::Null);
        assert_eq!(
            view.arrow_cell(&mappings[3], 0).unwrap(),
            ArrowCell::Int32(12)
        );
        assert_eq!(view.arrow_cell(&mappings[3], 1).unwrap(), ArrowCell::Null);
        assert_eq!(
            view.arrow_cell(&mappings[4], 0).unwrap(),
            ArrowCell::Int64(34)
        );
        assert_eq!(view.arrow_cell(&mappings[4], 1).unwrap(), ArrowCell::Null);
        assert_eq!(
            view.arrow_cell(&mappings[5], 0).unwrap(),
            ArrowCell::UInt8(8)
        );
        assert_eq!(view.arrow_cell(&mappings[5], 1).unwrap(), ArrowCell::Null);
        assert_eq!(
            view.arrow_cell(&mappings[6], 0).unwrap(),
            ArrowCell::UInt16(16)
        );
        assert_eq!(view.arrow_cell(&mappings[6], 1).unwrap(), ArrowCell::Null);
        assert_eq!(
            view.arrow_cell(&mappings[7], 0).unwrap(),
            ArrowCell::UInt32(32)
        );
        assert_eq!(view.arrow_cell(&mappings[7], 1).unwrap(), ArrowCell::Null);
        assert_eq!(
            view.arrow_cell(&mappings[8], 0).unwrap(),
            ArrowCell::Float32(1.25)
        );
        assert_eq!(view.arrow_cell(&mappings[8], 1).unwrap(), ArrowCell::Null);
        assert_eq!(
            view.arrow_cell(&mappings[9], 0).unwrap(),
            ArrowCell::Float64(2.5)
        );
        assert_eq!(view.arrow_cell(&mappings[9], 1).unwrap(), ArrowCell::Null);
        assert_eq!(
            view.arrow_cell(&mappings[10], 0).unwrap(),
            ArrowCell::Utf8("hello")
        );
        assert_eq!(view.arrow_cell(&mappings[10], 1).unwrap(), ArrowCell::Null);
        assert_eq!(
            view.arrow_cell(&mappings[11], 0).unwrap(),
            ArrowCell::Utf8("東京")
        );
        assert_eq!(view.arrow_cell(&mappings[11], 1).unwrap(), ArrowCell::Null);
        assert_eq!(
            view.arrow_cell(&mappings[12], 0).unwrap(),
            ArrowCell::Binary(b"abc")
        );
        assert_eq!(view.arrow_cell(&mappings[12], 1).unwrap(), ArrowCell::Null);
        assert_eq!(
            view.arrow_cell(&mappings[13], 0).unwrap(),
            ArrowCell::Binary(b"large")
        );
        assert_eq!(view.arrow_cell(&mappings[13], 1).unwrap(), ArrowCell::Null);
    }

    #[test]
    fn extracts_uint64_arrow_cells_at_policy_boundaries() {
        let mappings = mappings_for_schema_with_options(
            Schema::new(vec![Field::new("unsigned_huge", DataType::UInt64, true)]),
            PlanOptions {
                uint64_policy: UInt64Policy::Decimal20_0,
                ..PlanOptions::default()
            },
        );
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "unsigned_huge",
                DataType::UInt64,
                true,
            )])),
            vec![Arc::new(UInt64Array::from(vec![
                Some(0_u64),
                Some(i64::MAX as u64),
                Some((i64::MAX as u64) + 1),
                Some(u64::MAX),
                None,
            ]))],
        )
        .unwrap();
        let view = RecordBatchView::new(&batch, &mappings).unwrap();

        assert_eq!(
            view.arrow_cell(&mappings[0], 0).unwrap(),
            ArrowCell::UInt64(0)
        );
        assert_eq!(
            view.arrow_cell(&mappings[0], 1).unwrap(),
            ArrowCell::UInt64(i64::MAX as u64)
        );
        assert_eq!(
            view.arrow_cell(&mappings[0], 2).unwrap(),
            ArrowCell::UInt64((i64::MAX as u64) + 1)
        );
        assert_eq!(
            view.arrow_cell(&mappings[0], 3).unwrap(),
            ArrowCell::UInt64(u64::MAX)
        );
        assert_eq!(view.arrow_cell(&mappings[0], 4).unwrap(), ArrowCell::Null);
    }

    #[test]
    fn extracts_timezone_free_timestamp_arrow_cells_at_i64_boundaries() {
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
                    Some(i64::MIN),
                    Some(0),
                    Some(i64::MAX),
                    None,
                ])) as ArrayRef,
                Arc::new(TimestampMillisecondArray::from(vec![
                    Some(i64::MIN),
                    Some(0),
                    Some(i64::MAX),
                    None,
                ])),
                Arc::new(TimestampMicrosecondArray::from(vec![
                    Some(i64::MIN),
                    Some(0),
                    Some(i64::MAX),
                    None,
                ])),
                Arc::new(TimestampNanosecondArray::from(vec![
                    Some(i64::MIN),
                    Some(0),
                    Some(i64::MAX),
                    None,
                ])),
            ],
        )
        .unwrap();
        let view = RecordBatchView::new(&batch, &mappings).unwrap();

        assert_eq!(
            view.arrow_cell(&mappings[0], 0).unwrap(),
            ArrowCell::TimestampSecond(i64::MIN)
        );
        assert_eq!(
            view.arrow_cell(&mappings[0], 2).unwrap(),
            ArrowCell::TimestampSecond(i64::MAX)
        );
        assert_eq!(view.arrow_cell(&mappings[0], 3).unwrap(), ArrowCell::Null);
        assert_eq!(
            view.arrow_cell(&mappings[1], 0).unwrap(),
            ArrowCell::TimestampMillisecond(i64::MIN)
        );
        assert_eq!(
            view.arrow_cell(&mappings[1], 2).unwrap(),
            ArrowCell::TimestampMillisecond(i64::MAX)
        );
        assert_eq!(view.arrow_cell(&mappings[1], 3).unwrap(), ArrowCell::Null);
        assert_eq!(
            view.arrow_cell(&mappings[2], 0).unwrap(),
            ArrowCell::TimestampMicrosecond(i64::MIN)
        );
        assert_eq!(
            view.arrow_cell(&mappings[2], 2).unwrap(),
            ArrowCell::TimestampMicrosecond(i64::MAX)
        );
        assert_eq!(view.arrow_cell(&mappings[2], 3).unwrap(), ArrowCell::Null);
        assert_eq!(
            view.arrow_cell(&mappings[3], 0).unwrap(),
            ArrowCell::TimestampNanosecond(i64::MIN)
        );
        assert_eq!(
            view.arrow_cell(&mappings[3], 2).unwrap(),
            ArrowCell::TimestampNanosecond(i64::MAX)
        );
        assert_eq!(view.arrow_cell(&mappings[3], 3).unwrap(), ArrowCell::Null);
    }

    #[test]
    fn extracts_timezone_aware_timestamp_arrow_cells_without_losing_epoch_values() {
        let options = PlanOptions {
            timezone_policy: TimezonePolicy::DateTimeOffset,
            ..PlanOptions::default()
        };
        let schema = Schema::new(vec![
            Field::new(
                "ts_s",
                DataType::Timestamp(TimeUnit::Second, Some("America/New_York".into())),
                true,
            ),
            Field::new(
                "ts_ms",
                DataType::Timestamp(TimeUnit::Millisecond, Some("+02:30".into())),
                true,
            ),
            Field::new(
                "ts_us",
                DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
                true,
            ),
            Field::new(
                "ts_ns",
                DataType::Timestamp(TimeUnit::Nanosecond, Some("-07".into())),
                true,
            ),
        ]);
        let mappings = mappings_for_schema_with_options(schema.clone(), options);
        let batch = RecordBatch::try_new(
            Arc::new(schema),
            vec![
                Arc::new(
                    TimestampSecondArray::from(vec![Some(1_i64), None])
                        .with_timezone("America/New_York"),
                ) as ArrayRef,
                Arc::new(
                    TimestampMillisecondArray::from(vec![Some(2_i64), None])
                        .with_timezone("+02:30"),
                ),
                Arc::new(
                    TimestampMicrosecondArray::from(vec![Some(3_i64), None]).with_timezone("UTC"),
                ),
                Arc::new(
                    TimestampNanosecondArray::from(vec![Some(4_i64), None]).with_timezone("-07"),
                ),
            ],
        )
        .unwrap();
        let view = RecordBatchView::new(&batch, &mappings).unwrap();

        assert_eq!(
            view.arrow_cell(&mappings[0], 0).unwrap(),
            ArrowCell::TimestampSecond(1)
        );
        assert_eq!(
            view.arrow_cell(&mappings[1], 0).unwrap(),
            ArrowCell::TimestampMillisecond(2)
        );
        assert_eq!(
            view.arrow_cell(&mappings[2], 0).unwrap(),
            ArrowCell::TimestampMicrosecond(3)
        );
        assert_eq!(
            view.arrow_cell(&mappings[3], 0).unwrap(),
            ArrowCell::TimestampNanosecond(4)
        );

        for mapping in &mappings {
            assert_eq!(view.arrow_cell(mapping, 1).unwrap(), ArrowCell::Null);
        }
    }

    #[test]
    fn extracts_decimal_arrow_cells_for_all_widths() {
        let fields = vec![
            Field::new("decimal32", DataType::Decimal32(9, 2), true),
            Field::new("decimal64", DataType::Decimal64(18, 4), true),
            Field::new("decimal128", DataType::Decimal128(38, 9), true),
            Field::new("decimal256", DataType::Decimal256(38, 0), true),
        ];
        let mappings = mappings_for_schema(Schema::new(fields.clone()));
        let schema = Arc::new(Schema::new(fields));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(
                    Decimal32Array::from(vec![
                        Some(12_345_i32),
                        Some(-12_345_i32),
                        Some(0_i32),
                        None,
                    ])
                    .with_precision_and_scale(9, 2)
                    .unwrap(),
                ) as ArrayRef,
                Arc::new(
                    Decimal64Array::from(vec![
                        Some(1_234_567_890_i64),
                        Some(-1_234_567_890_i64),
                        Some(0_i64),
                        None,
                    ])
                    .with_precision_and_scale(18, 4)
                    .unwrap(),
                ),
                Arc::new(
                    Decimal128Array::from(vec![
                        Some(123_456_789_012_345_678_901_234_567_890_i128),
                        Some(-123_456_789_012_345_678_901_234_567_890_i128),
                        Some(0_i128),
                        None,
                    ])
                    .with_precision_and_scale(38, 9)
                    .unwrap(),
                ),
                Arc::new(
                    Decimal256Array::from(vec![
                        Some(i256::from_i128(
                            123_456_789_012_345_678_901_234_567_890_i128,
                        )),
                        Some(i256::from_i128(
                            -123_456_789_012_345_678_901_234_567_890_i128,
                        )),
                        Some(i256::ZERO),
                        None,
                    ])
                    .with_precision_and_scale(38, 0)
                    .unwrap(),
                ),
            ],
        )
        .unwrap();
        let view = RecordBatchView::new(&batch, &mappings).unwrap();

        assert_eq!(
            view.arrow_cell(&mappings[0], 0).unwrap(),
            ArrowCell::Decimal32(12_345)
        );
        assert_eq!(
            view.arrow_cell(&mappings[0], 1).unwrap(),
            ArrowCell::Decimal32(-12_345)
        );
        assert_eq!(
            view.arrow_cell(&mappings[0], 2).unwrap(),
            ArrowCell::Decimal32(0)
        );
        assert_eq!(view.arrow_cell(&mappings[0], 3).unwrap(), ArrowCell::Null);

        assert_eq!(
            view.arrow_cell(&mappings[1], 0).unwrap(),
            ArrowCell::Decimal64(1_234_567_890)
        );
        assert_eq!(
            view.arrow_cell(&mappings[1], 1).unwrap(),
            ArrowCell::Decimal64(-1_234_567_890)
        );
        assert_eq!(
            view.arrow_cell(&mappings[1], 2).unwrap(),
            ArrowCell::Decimal64(0)
        );
        assert_eq!(view.arrow_cell(&mappings[1], 3).unwrap(), ArrowCell::Null);

        assert_eq!(
            view.arrow_cell(&mappings[2], 0).unwrap(),
            ArrowCell::Decimal128(123_456_789_012_345_678_901_234_567_890)
        );
        assert_eq!(
            view.arrow_cell(&mappings[2], 1).unwrap(),
            ArrowCell::Decimal128(-123_456_789_012_345_678_901_234_567_890)
        );
        assert_eq!(
            view.arrow_cell(&mappings[2], 2).unwrap(),
            ArrowCell::Decimal128(0)
        );
        assert_eq!(view.arrow_cell(&mappings[2], 3).unwrap(), ArrowCell::Null);

        assert_eq!(
            view.arrow_cell(&mappings[3], 0).unwrap(),
            ArrowCell::Decimal256(i256::from_i128(123_456_789_012_345_678_901_234_567_890))
        );
        assert_eq!(
            view.arrow_cell(&mappings[3], 1).unwrap(),
            ArrowCell::Decimal256(i256::from_i128(-123_456_789_012_345_678_901_234_567_890))
        );
        assert_eq!(
            view.arrow_cell(&mappings[3], 2).unwrap(),
            ArrowCell::Decimal256(i256::ZERO)
        );
        assert_eq!(view.arrow_cell(&mappings[3], 3).unwrap(), ArrowCell::Null);
    }

    #[test]
    fn extracts_date_arrow_cells() {
        let fields = vec![
            Field::new("date32", DataType::Date32, true),
            Field::new("date64", DataType::Date64, true),
        ];
        let mappings = mappings_for_schema_with_options(
            Schema::new(fields.clone()),
            PlanOptions {
                date64_policy: Date64Policy::TimestampDateTime2,
                ..PlanOptions::default()
            },
        );
        let schema = Arc::new(Schema::new(fields));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Date32Array::from(vec![
                    Some(0_i32),
                    Some(-1_i32),
                    Some(1_i32),
                    None,
                ])) as ArrayRef,
                Arc::new(Date64Array::from(vec![
                    Some(0_i64),
                    Some(-1_i64),
                    Some(86_400_123_i64),
                    None,
                ])),
            ],
        )
        .unwrap();
        let view = RecordBatchView::new(&batch, &mappings).unwrap();

        assert_eq!(
            view.arrow_cell(&mappings[0], 0).unwrap(),
            ArrowCell::Date32(0)
        );
        assert_eq!(
            view.arrow_cell(&mappings[0], 1).unwrap(),
            ArrowCell::Date32(-1)
        );
        assert_eq!(
            view.arrow_cell(&mappings[0], 2).unwrap(),
            ArrowCell::Date32(1)
        );
        assert_eq!(view.arrow_cell(&mappings[0], 3).unwrap(), ArrowCell::Null);

        assert_eq!(
            view.arrow_cell(&mappings[1], 0).unwrap(),
            ArrowCell::Date64(0)
        );
        assert_eq!(
            view.arrow_cell(&mappings[1], 1).unwrap(),
            ArrowCell::Date64(-1)
        );
        assert_eq!(
            view.arrow_cell(&mappings[1], 2).unwrap(),
            ArrowCell::Date64(86_400_123)
        );
        assert_eq!(view.arrow_cell(&mappings[1], 3).unwrap(), ArrowCell::Null);
    }

    #[test]
    fn converts_supported_initial_primitives_to_mssql_cells() {
        let mappings = mappings_for_schema(Schema::new(vec![
            Field::new("active", DataType::Boolean, true),
            Field::new("tiny", DataType::Int8, true),
            Field::new("small", DataType::Int16, true),
            Field::new("quantity", DataType::Int32, true),
            Field::new("total", DataType::Int64, true),
            Field::new("unsigned_tiny", DataType::UInt8, true),
            Field::new("unsigned_medium", DataType::UInt16, true),
            Field::new("unsigned_large", DataType::UInt32, true),
            Field::new("real_value", DataType::Float32, true),
            Field::new("float_value", DataType::Float64, true),
            Field::new("text", DataType::Utf8, true),
            Field::new("large_text", DataType::LargeUtf8, true),
            Field::new("bytes", DataType::Binary, true),
            Field::new("large_bytes", DataType::LargeBinary, true),
        ]));
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("active", DataType::Boolean, true),
                Field::new("tiny", DataType::Int8, true),
                Field::new("small", DataType::Int16, true),
                Field::new("quantity", DataType::Int32, true),
                Field::new("total", DataType::Int64, true),
                Field::new("unsigned_tiny", DataType::UInt8, true),
                Field::new("unsigned_medium", DataType::UInt16, true),
                Field::new("unsigned_large", DataType::UInt32, true),
                Field::new("real_value", DataType::Float32, true),
                Field::new("float_value", DataType::Float64, true),
                Field::new("text", DataType::Utf8, true),
                Field::new("large_text", DataType::LargeUtf8, true),
                Field::new("bytes", DataType::Binary, true),
                Field::new("large_bytes", DataType::LargeBinary, true),
            ])),
            vec![
                Arc::new(BooleanArray::from(vec![Some(true), None])) as ArrayRef,
                Arc::new(Int8Array::from(vec![Some(-8_i8), None])),
                Arc::new(Int16Array::from(vec![Some(-16_i16), None])),
                Arc::new(Int32Array::from(vec![Some(12_i32), None])),
                Arc::new(Int64Array::from(vec![Some(34_i64), None])),
                Arc::new(UInt8Array::from(vec![Some(8_u8), None])),
                Arc::new(UInt16Array::from(vec![Some(16_u16), None])),
                Arc::new(UInt32Array::from(vec![Some(32_u32), None])),
                Arc::new(Float32Array::from(vec![Some(1.25_f32), None])),
                Arc::new(Float64Array::from(vec![Some(2.5_f64), None])),
                Arc::new(StringArray::from(vec![Some("hello"), None])),
                Arc::new(LargeStringArray::from(vec![Some("東京"), None])),
                Arc::new(BinaryArray::from(vec![Some(&b"abc"[..]), None])),
                Arc::new(LargeBinaryArray::from(vec![Some(&b"large"[..]), None])),
            ],
        )
        .unwrap();
        let view = RecordBatchView::new(&batch, &mappings).unwrap();

        assert_eq!(
            view.mssql_cell(&mappings[0], 0).unwrap(),
            MssqlCell::Bit(Some(true))
        );
        assert_eq!(
            view.mssql_cell(&mappings[0], 1).unwrap(),
            MssqlCell::Bit(None)
        );
        assert_eq!(
            view.mssql_cell(&mappings[1], 0).unwrap(),
            MssqlCell::SmallInt(Some(-8))
        );
        assert_eq!(
            view.mssql_cell(&mappings[1], 1).unwrap(),
            MssqlCell::SmallInt(None)
        );
        assert_eq!(
            view.mssql_cell(&mappings[2], 0).unwrap(),
            MssqlCell::SmallInt(Some(-16))
        );
        assert_eq!(
            view.mssql_cell(&mappings[2], 1).unwrap(),
            MssqlCell::SmallInt(None)
        );
        assert_eq!(
            view.mssql_cell(&mappings[3], 0).unwrap(),
            MssqlCell::Int(Some(12))
        );
        assert_eq!(
            view.mssql_cell(&mappings[3], 1).unwrap(),
            MssqlCell::Int(None)
        );
        assert_eq!(
            view.mssql_cell(&mappings[4], 0).unwrap(),
            MssqlCell::BigInt(Some(34))
        );
        assert_eq!(
            view.mssql_cell(&mappings[4], 1).unwrap(),
            MssqlCell::BigInt(None)
        );
        assert_eq!(
            view.mssql_cell(&mappings[5], 0).unwrap(),
            MssqlCell::TinyInt(Some(8))
        );
        assert_eq!(
            view.mssql_cell(&mappings[5], 1).unwrap(),
            MssqlCell::TinyInt(None)
        );
        assert_eq!(
            view.mssql_cell(&mappings[6], 0).unwrap(),
            MssqlCell::Int(Some(16))
        );
        assert_eq!(
            view.mssql_cell(&mappings[6], 1).unwrap(),
            MssqlCell::Int(None)
        );
        assert_eq!(
            view.mssql_cell(&mappings[7], 0).unwrap(),
            MssqlCell::BigInt(Some(32))
        );
        assert_eq!(
            view.mssql_cell(&mappings[7], 1).unwrap(),
            MssqlCell::BigInt(None)
        );
        assert_eq!(
            view.mssql_cell(&mappings[8], 0).unwrap(),
            MssqlCell::Real(Some(1.25))
        );
        assert_eq!(
            view.mssql_cell(&mappings[8], 1).unwrap(),
            MssqlCell::Real(None)
        );
        assert_eq!(
            view.mssql_cell(&mappings[9], 0).unwrap(),
            MssqlCell::Float(Some(2.5))
        );
        assert_eq!(
            view.mssql_cell(&mappings[9], 1).unwrap(),
            MssqlCell::Float(None)
        );
        assert_eq!(
            view.mssql_cell(&mappings[10], 0).unwrap(),
            MssqlCell::NVarChar(Some("hello"))
        );
        assert_eq!(
            view.mssql_cell(&mappings[10], 1).unwrap(),
            MssqlCell::NVarChar(None)
        );
        assert_eq!(
            view.mssql_cell(&mappings[11], 0).unwrap(),
            MssqlCell::NVarChar(Some("東京"))
        );
        assert_eq!(
            view.mssql_cell(&mappings[11], 1).unwrap(),
            MssqlCell::NVarChar(None)
        );
        assert_eq!(
            view.mssql_cell(&mappings[12], 0).unwrap(),
            MssqlCell::VarBinary(Some(b"abc"))
        );
        assert_eq!(
            view.mssql_cell(&mappings[12], 1).unwrap(),
            MssqlCell::VarBinary(None)
        );
        assert_eq!(
            view.mssql_cell(&mappings[13], 0).unwrap(),
            MssqlCell::VarBinary(Some(b"large"))
        );
        assert_eq!(
            view.mssql_cell(&mappings[13], 1).unwrap(),
            MssqlCell::VarBinary(None)
        );
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
    fn converts_empty_ascii_and_non_ascii_strings() {
        let mappings = mappings_for_schema(Schema::new(vec![
            Field::new("text", DataType::Utf8, true),
            Field::new("large_text", DataType::LargeUtf8, true),
        ]));
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("text", DataType::Utf8, true),
                Field::new("large_text", DataType::LargeUtf8, true),
            ])),
            vec![
                Arc::new(StringArray::from(vec!["", "ascii", "東京"])) as ArrayRef,
                Arc::new(LargeStringArray::from(vec!["", "ascii", "🙂"])),
            ],
        )
        .unwrap();
        let view = RecordBatchView::new(&batch, &mappings).unwrap();

        assert_eq!(
            view.mssql_cell(&mappings[0], 0).unwrap(),
            MssqlCell::NVarChar(Some(""))
        );
        assert_eq!(
            view.mssql_cell(&mappings[0], 1).unwrap(),
            MssqlCell::NVarChar(Some("ascii"))
        );
        assert_eq!(
            view.mssql_cell(&mappings[0], 2).unwrap(),
            MssqlCell::NVarChar(Some("東京"))
        );
        assert_eq!(
            view.mssql_cell(&mappings[1], 0).unwrap(),
            MssqlCell::NVarChar(Some(""))
        );
        assert_eq!(
            view.mssql_cell(&mappings[1], 1).unwrap(),
            MssqlCell::NVarChar(Some("ascii"))
        );
        assert_eq!(
            view.mssql_cell(&mappings[1], 2).unwrap(),
            MssqlCell::NVarChar(Some("🙂"))
        );
    }

    #[test]
    fn converts_empty_and_non_empty_binary_values() {
        let mappings = mappings_for_schema(Schema::new(vec![
            Field::new("bytes", DataType::Binary, true),
            Field::new("large_bytes", DataType::LargeBinary, true),
        ]));
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("bytes", DataType::Binary, true),
                Field::new("large_bytes", DataType::LargeBinary, true),
            ])),
            vec![
                Arc::new(BinaryArray::from(vec![Some(&b""[..]), Some(&b"abc"[..])])) as ArrayRef,
                Arc::new(LargeBinaryArray::from(vec![
                    Some(&b""[..]),
                    Some(&b"large"[..]),
                ])),
            ],
        )
        .unwrap();
        let view = RecordBatchView::new(&batch, &mappings).unwrap();

        assert_eq!(
            view.mssql_cell(&mappings[0], 0).unwrap(),
            MssqlCell::VarBinary(Some(b""))
        );
        assert_eq!(
            view.mssql_cell(&mappings[0], 1).unwrap(),
            MssqlCell::VarBinary(Some(b"abc"))
        );
        assert_eq!(
            view.mssql_cell(&mappings[1], 0).unwrap(),
            MssqlCell::VarBinary(Some(b""))
        );
        assert_eq!(
            view.mssql_cell(&mappings[1], 1).unwrap(),
            MssqlCell::VarBinary(Some(b"large"))
        );
    }

    #[test]
    fn rejects_bounded_nvarchar_by_utf16_code_units() {
        let mappings = mappings_for_schema_with_options(
            Schema::new(vec![Field::new("text", DataType::Utf8, true)]),
            PlanOptions {
                string_policy: StringPolicy::NVarChar(2),
                ..PlanOptions::default()
            },
        );
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("text", DataType::Utf8, true)])),
            vec![Arc::new(StringArray::from(vec!["ab", "🙂", "abc"]))],
        )
        .unwrap();
        let view = RecordBatchView::new(&batch, &mappings).unwrap();

        assert_eq!(
            view.mssql_cell(&mappings[0], 0).unwrap(),
            MssqlCell::NVarChar(Some("ab"))
        );
        assert_eq!(
            view.mssql_cell(&mappings[0], 1).unwrap(),
            MssqlCell::NVarChar(Some("🙂"))
        );
        let err = view.mssql_cell(&mappings[0], 2).unwrap_err();

        assert_single_diagnostic(
            err,
            DiagnosticCode::ValueTooLong,
            Some(2),
            Some((0, "text")),
        );
    }

    #[test]
    fn rejects_bounded_varbinary_by_byte_count() {
        let mappings = mappings_for_schema_with_options(
            Schema::new(vec![Field::new("bytes", DataType::Binary, true)]),
            PlanOptions {
                binary_policy: BinaryPolicy::VarBinary(2),
                ..PlanOptions::default()
            },
        );
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "bytes",
                DataType::Binary,
                true,
            )])),
            vec![Arc::new(BinaryArray::from(vec![
                Some(&b""[..]),
                Some(&b"ab"[..]),
                Some(&b"abc"[..]),
            ]))],
        )
        .unwrap();
        let view = RecordBatchView::new(&batch, &mappings).unwrap();

        assert_eq!(
            view.mssql_cell(&mappings[0], 0).unwrap(),
            MssqlCell::VarBinary(Some(b""))
        );
        assert_eq!(
            view.mssql_cell(&mappings[0], 1).unwrap(),
            MssqlCell::VarBinary(Some(b"ab"))
        );
        let err = view.mssql_cell(&mappings[0], 2).unwrap_err();

        assert_single_diagnostic(
            err,
            DiagnosticCode::ValueTooLong,
            Some(2),
            Some((0, "bytes")),
        );
    }

    #[test]
    fn converts_uint64_decimal20_0_boundary_values() {
        let mappings = mappings_for_schema_with_options(
            Schema::new(vec![Field::new(
                "unsigned_as_decimal",
                DataType::UInt64,
                true,
            )]),
            PlanOptions {
                uint64_policy: UInt64Policy::Decimal20_0,
                ..PlanOptions::default()
            },
        );
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "unsigned_as_decimal",
                DataType::UInt64,
                true,
            )])),
            vec![Arc::new(UInt64Array::from(vec![
                Some(0_u64),
                Some(i64::MAX as u64),
                Some((i64::MAX as u64) + 1),
                Some(u64::MAX),
                None,
            ]))],
        )
        .unwrap();
        let view = RecordBatchView::new(&batch, &mappings).unwrap();

        assert_eq!(
            view.mssql_cell(&mappings[0], 0).unwrap(),
            MssqlCell::Decimal(Some(MssqlDecimal::new(0, 0)))
        );
        assert_eq!(
            view.mssql_cell(&mappings[0], 1).unwrap(),
            MssqlCell::Decimal(Some(MssqlDecimal::new(i128::from(i64::MAX), 0)))
        );
        assert_eq!(
            view.mssql_cell(&mappings[0], 2).unwrap(),
            MssqlCell::Decimal(Some(MssqlDecimal::new(i128::from(i64::MAX) + 1, 0)))
        );
        assert_eq!(
            view.mssql_cell(&mappings[0], 3).unwrap(),
            MssqlCell::Decimal(Some(MssqlDecimal::new(i128::from(u64::MAX), 0)))
        );
        assert_eq!(
            view.mssql_cell(&mappings[0], 4).unwrap(),
            MssqlCell::Decimal(None)
        );
    }

    #[test]
    fn converts_decimal32_64_128_cells_with_sign_zero_scale_and_null() {
        let fields = vec![
            Field::new("decimal32", DataType::Decimal32(9, 2), true),
            Field::new("decimal64", DataType::Decimal64(18, 4), true),
            Field::new("decimal128", DataType::Decimal128(38, 9), true),
        ];
        let mappings = mappings_for_schema(Schema::new(fields.clone()));
        let schema = Arc::new(Schema::new(fields));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(
                    Decimal32Array::from(vec![
                        Some(12_345_i32),
                        Some(-12_345_i32),
                        Some(0_i32),
                        None,
                    ])
                    .with_precision_and_scale(9, 2)
                    .unwrap(),
                ) as ArrayRef,
                Arc::new(
                    Decimal64Array::from(vec![
                        Some(1_234_567_890_i64),
                        Some(-1_234_567_890_i64),
                        Some(0_i64),
                        None,
                    ])
                    .with_precision_and_scale(18, 4)
                    .unwrap(),
                ),
                Arc::new(
                    Decimal128Array::from(vec![
                        Some(123_456_789_012_345_678_901_234_567_890_i128),
                        Some(-123_456_789_012_345_678_901_234_567_890_i128),
                        Some(0_i128),
                        None,
                    ])
                    .with_precision_and_scale(38, 9)
                    .unwrap(),
                ),
            ],
        )
        .unwrap();
        let view = RecordBatchView::new(&batch, &mappings).unwrap();

        assert_eq!(
            view.mssql_cell(&mappings[0], 0).unwrap(),
            MssqlCell::Decimal(Some(MssqlDecimal::new(12_345, 2)))
        );
        assert_eq!(
            view.mssql_cell(&mappings[0], 1).unwrap(),
            MssqlCell::Decimal(Some(MssqlDecimal::new(-12_345, 2)))
        );
        assert_eq!(
            view.mssql_cell(&mappings[0], 2).unwrap(),
            MssqlCell::Decimal(Some(MssqlDecimal::new(0, 2)))
        );
        assert_eq!(
            view.mssql_cell(&mappings[0], 3).unwrap(),
            MssqlCell::Decimal(None)
        );

        assert_eq!(
            view.mssql_cell(&mappings[1], 0).unwrap(),
            MssqlCell::Decimal(Some(MssqlDecimal::new(1_234_567_890, 4)))
        );
        assert_eq!(
            view.mssql_cell(&mappings[1], 1).unwrap(),
            MssqlCell::Decimal(Some(MssqlDecimal::new(-1_234_567_890, 4)))
        );
        assert_eq!(
            view.mssql_cell(&mappings[1], 2).unwrap(),
            MssqlCell::Decimal(Some(MssqlDecimal::new(0, 4)))
        );
        assert_eq!(
            view.mssql_cell(&mappings[1], 3).unwrap(),
            MssqlCell::Decimal(None)
        );

        assert_eq!(
            view.mssql_cell(&mappings[2], 0).unwrap(),
            MssqlCell::Decimal(Some(MssqlDecimal::new(
                123_456_789_012_345_678_901_234_567_890,
                9,
            )))
        );
        assert_eq!(
            view.mssql_cell(&mappings[2], 1).unwrap(),
            MssqlCell::Decimal(Some(MssqlDecimal::new(
                -123_456_789_012_345_678_901_234_567_890,
                9,
            )))
        );
        assert_eq!(
            view.mssql_cell(&mappings[2], 2).unwrap(),
            MssqlCell::Decimal(Some(MssqlDecimal::new(0, 9)))
        );
        assert_eq!(
            view.mssql_cell(&mappings[2], 3).unwrap(),
            MssqlCell::Decimal(None)
        );
    }

    #[test]
    fn normalizes_negative_decimal_scale_at_runtime() {
        let mappings = mappings_for_schema_with_options(
            Schema::new(vec![Field::new(
                "amount",
                DataType::Decimal128(3, -2),
                true,
            )]),
            PlanOptions {
                decimal_policy: DecimalPolicy::NormalizeNegativeScale,
                ..PlanOptions::default()
            },
        );
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "amount",
                DataType::Decimal128(3, -2),
                true,
            )])),
            vec![Arc::new(
                Decimal128Array::from(vec![Some(123_i128), Some(-123_i128), Some(0), None])
                    .with_precision_and_scale(3, -2)
                    .unwrap(),
            )],
        )
        .unwrap();
        let view = RecordBatchView::new(&batch, &mappings).unwrap();

        assert_eq!(
            view.mssql_cell(&mappings[0], 0).unwrap(),
            MssqlCell::Decimal(Some(MssqlDecimal::new(12_300, 0)))
        );
        assert_eq!(
            view.mssql_cell(&mappings[0], 1).unwrap(),
            MssqlCell::Decimal(Some(MssqlDecimal::new(-12_300, 0)))
        );
        assert_eq!(
            view.mssql_cell(&mappings[0], 2).unwrap(),
            MssqlCell::Decimal(Some(MssqlDecimal::new(0, 0)))
        );
        assert_eq!(
            view.mssql_cell(&mappings[0], 3).unwrap(),
            MssqlCell::Decimal(None)
        );
    }

    #[test]
    fn rejects_negative_decimal_scale_normalization_overflow() {
        let mappings = mappings_for_schema_with_options(
            Schema::new(vec![Field::new(
                "amount",
                DataType::Decimal128(37, -1),
                false,
            )]),
            PlanOptions {
                decimal_policy: DecimalPolicy::NormalizeNegativeScale,
                ..PlanOptions::default()
            },
        );
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "amount",
                DataType::Decimal128(37, -1),
                false,
            )])),
            vec![malicious_decimal128_array(
                DataType::Decimal128(37, -1),
                &[i128::MAX],
            )],
        )
        .unwrap();
        let view = RecordBatchView::new(&batch, &mappings).unwrap();

        let err = view.mssql_cell(&mappings[0], 0).unwrap_err();

        assert_single_diagnostic(
            err,
            DiagnosticCode::DecimalOutOfRange,
            Some(0),
            Some((0, "amount")),
        );
    }

    #[test]
    fn rejects_decimal_scale_that_tiberius_numeric_cannot_represent() {
        let mappings = mappings_for_schema(Schema::new(vec![Field::new(
            "amount",
            DataType::Decimal128(38, 38),
            true,
        )]));
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "amount",
                DataType::Decimal128(38, 38),
                true,
            )])),
            vec![Arc::new(
                Decimal128Array::from(vec![Some(1_i128)])
                    .with_precision_and_scale(38, 38)
                    .unwrap(),
            )],
        )
        .unwrap();
        let view = RecordBatchView::new(&batch, &mappings).unwrap();

        let err = view.mssql_cell(&mappings[0], 0).unwrap_err();

        assert_single_diagnostic(
            err,
            DiagnosticCode::DecimalOutOfRange,
            Some(0),
            Some((0, "amount")),
        );
    }

    #[test]
    fn accepts_decimal_values_at_planned_precision_boundaries() {
        let mappings = mappings_for_schema(Schema::new(vec![Field::new(
            "amount",
            DataType::Decimal128(5, 2),
            false,
        )]));
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "amount",
                DataType::Decimal128(5, 2),
                false,
            )])),
            vec![Arc::new(
                Decimal128Array::from(vec![99_999_i128, -99_999_i128])
                    .with_precision_and_scale(5, 2)
                    .unwrap(),
            )],
        )
        .unwrap();
        let view = RecordBatchView::new(&batch, &mappings).unwrap();

        assert_eq!(
            view.mssql_cell(&mappings[0], 0).unwrap(),
            MssqlCell::Decimal(Some(MssqlDecimal::new(99_999, 2)))
        );
        assert_eq!(
            view.mssql_cell(&mappings[0], 1).unwrap(),
            MssqlCell::Decimal(Some(MssqlDecimal::new(-99_999, 2)))
        );
    }

    #[test]
    fn rejects_decimal_values_outside_planned_precision() {
        let mappings = mappings_for_schema(Schema::new(vec![Field::new(
            "amount",
            DataType::Decimal128(5, 2),
            false,
        )]));
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "amount",
                DataType::Decimal128(5, 2),
                false,
            )])),
            vec![malicious_decimal128_array(
                DataType::Decimal128(5, 2),
                &[100_000_i128, -100_000_i128],
            )],
        )
        .unwrap();
        let view = RecordBatchView::new(&batch, &mappings).unwrap();

        let positive = view.mssql_cell(&mappings[0], 0).unwrap_err();
        assert_single_diagnostic(
            positive,
            DiagnosticCode::DecimalOutOfRange,
            Some(0),
            Some((0, "amount")),
        );

        let negative = view.mssql_cell(&mappings[0], 1).unwrap_err();
        assert_single_diagnostic(
            negative,
            DiagnosticCode::DecimalOutOfRange,
            Some(1),
            Some((0, "amount")),
        );
    }

    #[test]
    fn converts_uint64_checked_bigint_boundary_values() {
        let mappings = mappings_for_schema_with_options(
            Schema::new(vec![Field::new(
                "unsigned_as_bigint",
                DataType::UInt64,
                true,
            )]),
            PlanOptions {
                uint64_policy: UInt64Policy::CheckedBigInt,
                ..PlanOptions::default()
            },
        );
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "unsigned_as_bigint",
                DataType::UInt64,
                true,
            )])),
            vec![Arc::new(UInt64Array::from(vec![
                Some(0_u64),
                Some(i64::MAX as u64),
                None,
            ]))],
        )
        .unwrap();
        let view = RecordBatchView::new(&batch, &mappings).unwrap();

        assert_eq!(
            view.mssql_cell(&mappings[0], 0).unwrap(),
            MssqlCell::BigInt(Some(0))
        );
        assert_eq!(
            view.mssql_cell(&mappings[0], 1).unwrap(),
            MssqlCell::BigInt(Some(i64::MAX))
        );
        assert_eq!(
            view.mssql_cell(&mappings[0], 2).unwrap(),
            MssqlCell::BigInt(None)
        );
    }

    #[test]
    fn rejects_uint64_checked_bigint_overflow_without_wrapping() {
        let mappings = mappings_for_schema_with_options(
            Schema::new(vec![Field::new(
                "unsigned_as_bigint",
                DataType::UInt64,
                false,
            )]),
            PlanOptions {
                uint64_policy: UInt64Policy::CheckedBigInt,
                ..PlanOptions::default()
            },
        );
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "unsigned_as_bigint",
                DataType::UInt64,
                false,
            )])),
            vec![Arc::new(UInt64Array::from(vec![
                (i64::MAX as u64) + 1,
                u64::MAX,
            ]))],
        )
        .unwrap();
        let view = RecordBatchView::new(&batch, &mappings).unwrap();

        let just_over = view.mssql_cell(&mappings[0], 0).unwrap_err();
        assert_single_diagnostic(
            just_over,
            DiagnosticCode::IntegerOutOfRange,
            Some(0),
            Some((0, "unsigned_as_bigint")),
        );

        let max = view.mssql_cell(&mappings[0], 1).unwrap_err();
        assert_single_diagnostic(
            max,
            DiagnosticCode::IntegerOutOfRange,
            Some(1),
            Some((0, "unsigned_as_bigint")),
        );
    }

    #[test]
    fn converts_decimal256_checked_downcast_values() {
        let mappings = mappings_for_schema(Schema::new(vec![Field::new(
            "amount",
            DataType::Decimal256(38, 4),
            true,
        )]));
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "amount",
                DataType::Decimal256(38, 4),
                true,
            )])),
            vec![Arc::new(
                Decimal256Array::from(vec![
                    Some(i256::from_i128(123_456_789_012_345_678_901_234_567_890)),
                    Some(i256::from_i128(-123_456_789_012_345_678_901_234_567_890)),
                    Some(i256::ZERO),
                    None,
                ])
                .with_precision_and_scale(38, 4)
                .unwrap(),
            )],
        )
        .unwrap();
        let view = RecordBatchView::new(&batch, &mappings).unwrap();

        assert_eq!(
            view.mssql_cell(&mappings[0], 0).unwrap(),
            MssqlCell::Decimal(Some(MssqlDecimal::new(
                123_456_789_012_345_678_901_234_567_890,
                4,
            )))
        );
        assert_eq!(
            view.mssql_cell(&mappings[0], 1).unwrap(),
            MssqlCell::Decimal(Some(MssqlDecimal::new(
                -123_456_789_012_345_678_901_234_567_890,
                4,
            )))
        );
        assert_eq!(
            view.mssql_cell(&mappings[0], 2).unwrap(),
            MssqlCell::Decimal(Some(MssqlDecimal::new(0, 4)))
        );
        assert_eq!(
            view.mssql_cell(&mappings[0], 3).unwrap(),
            MssqlCell::Decimal(None)
        );
    }

    #[test]
    fn rejects_decimal256_values_that_do_not_fit_i128_runtime_representation() {
        let mappings = mappings_for_schema(Schema::new(vec![Field::new(
            "amount",
            DataType::Decimal256(38, 0),
            false,
        )]));
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "amount",
                DataType::Decimal256(38, 0),
                false,
            )])),
            vec![Arc::new(
                Decimal256Array::from(vec![i256::from_i128(i128::MAX) + i256::ONE])
                    .with_precision_and_scale(38, 0)
                    .unwrap(),
            )],
        )
        .unwrap();
        let view = RecordBatchView::new(&batch, &mappings).unwrap();

        let err = view.mssql_cell(&mappings[0], 0).unwrap_err();

        assert_single_diagnostic(
            err,
            DiagnosticCode::DecimalOutOfRange,
            Some(0),
            Some((0, "amount")),
        );
    }

    #[test]
    fn rejects_decimal256_checked_downcast_values_outside_planned_precision() {
        let mappings = mappings_for_schema(Schema::new(vec![Field::new(
            "amount",
            DataType::Decimal256(5, 2),
            false,
        )]));
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "amount",
                DataType::Decimal256(5, 2),
                false,
            )])),
            vec![Arc::new(
                Decimal256Array::from(vec![i256::from_i128(100_000)])
                    .with_precision_and_scale(5, 2)
                    .unwrap(),
            )],
        )
        .unwrap();
        let view = RecordBatchView::new(&batch, &mappings).unwrap();

        let err = view.mssql_cell(&mappings[0], 0).unwrap_err();

        assert_single_diagnostic(
            err,
            DiagnosticCode::DecimalOutOfRange,
            Some(0),
            Some((0, "amount")),
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

    fn malicious_decimal128_array(data_type: DataType, values: &[i128]) -> ArrayRef {
        let data = ArrayData::builder(data_type)
            .len(values.len())
            .add_buffer(values.to_vec().into())
            .build()
            .unwrap();

        Arc::new(Decimal128Array::from(data))
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

//! Direct raw TDS bulk encoder internals.
#![allow(dead_code)]

pub(crate) mod binding;
pub(crate) mod encoder;
pub(crate) mod layout;
pub(crate) mod measure;
pub(crate) mod payload;
pub(crate) mod plan;
pub(crate) mod rows;
pub(crate) mod types;

pub(crate) use encoder::{
    DirectEncoder, checked_add, invalid_payload, row_column_diagnostic, value_conversion_error,
};
pub(crate) use measure::{MeasuredDirectBatch, MeasuredRowRange};

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_array::{
        ArrayRef, BinaryArray, BinaryViewArray, BooleanArray, Date32Array, Date64Array,
        Decimal32Array, Decimal64Array, Decimal128Array, Decimal256Array, FixedSizeBinaryArray,
        Float16Array, Float32Array, Float64Array, Int32Array, Int64Array, LargeBinaryArray,
        LargeStringArray, RecordBatch, StringArray, StringViewArray, Time32MillisecondArray,
        Time32SecondArray, Time64MicrosecondArray, Time64NanosecondArray,
        TimestampMicrosecondArray, TimestampMillisecondArray, TimestampNanosecondArray,
        TimestampSecondArray, UInt64Array,
        types::{ArrowPrimitiveType, Float16Type},
    };
    use arrow_buffer::{NullBuffer, ScalarBuffer, i256};
    use arrow_schema::{DataType, Field, Schema, TimeUnit};

    use crate::{
        ArrowFieldRef, DiagnosticCode, Error, Identifier, MssqlColumn, MssqlTimePrecision,
        MssqlType, MssqlTypeLength, NanosecondPolicy, PlanOptions, SchemaMapping,
        conversion::arrow_to_mssql::primitive::PrimitiveArrowToMssql,
        mssql::cell::{MssqlDate, MssqlDateTime, MssqlDateTime2, MssqlDateTimeOffset, MssqlTime},
    };

    type F16 = <Float16Type as ArrowPrimitiveType>::Native;

    use super::layout::{CellPosition, RowLayout};
    use super::plan::{DirectColumnEncoding, DirectEncoderSupport, DirectMappingSupport};
    use super::rows::fixed_width::{
        fixed_width_measure_call_count, reset_fixed_width_measure_call_count,
        try_encode_fixed_width_rows, try_encode_fixed_width_rows_with_layout,
    };
    use super::types::temporal::{
        write_datetime_cell, write_datetime2_cell, write_datetimeoffset_cell, write_time_cell,
    };
    use super::{DirectEncoder, binding::BoundDirectBatch, payload};

    #[test]
    fn default_direct_encoder_accepts_empty_mapping_set() {
        let encoder = DirectEncoder::new(&[]).expect("empty mapping set is supported");

        assert!(encoder.plan().is_empty());
        assert_eq!(encoder.plan().column_count(), 0);
        assert_eq!(encoder.mappings(), []);
    }

    #[test]
    fn default_direct_encoder_returns_empty_payload_for_empty_batch_and_empty_mapping_set() {
        let encoder = DirectEncoder::new(&[]).expect("empty mapping set is supported");
        let batch = RecordBatch::new_empty(Arc::new(Schema::empty()));

        let payload = encoder
            .encode_batch(&batch)
            .expect("empty batch should encode as empty payload");

        assert!(payload.is_empty());
        assert_eq!(payload.row_count(), 0);
    }

    #[test]
    fn direct_encoder_reports_variable_width_column_presence() {
        let primitive = DirectEncoder::new(&[mapping(
            0,
            "quantity",
            DataType::Int32,
            MssqlType::Int,
            false,
        )])
        .unwrap();
        assert!(!primitive.has_variable_width_column());

        let mixed = DirectEncoder::new(&[
            mapping(0, "quantity", DataType::Int32, MssqlType::Int, false),
            mapping(
                1,
                "comment",
                DataType::Utf8,
                MssqlType::NVarChar(MssqlTypeLength::Max),
                true,
            ),
        ])
        .unwrap();
        assert!(mixed.has_variable_width_column());
    }

    #[test]
    fn direct_encoder_fast_path_returns_empty_payload_for_empty_batch_with_mappings() {
        let mappings = vec![
            mapping(0, "quantity", DataType::Int32, MssqlType::Int, true),
            mapping(1, "total", DataType::Int64, MssqlType::BigInt, false),
        ];
        let encoder = DirectEncoder::new(&mappings).unwrap();
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("quantity", DataType::Int32, true),
                Field::new("total", DataType::Int64, false),
            ])),
            vec![
                Arc::new(Int32Array::from(Vec::<Option<i32>>::new())) as ArrayRef,
                Arc::new(Int64Array::from(Vec::<i64>::new())),
            ],
        )
        .unwrap();

        let payload = encoder
            .encode_batch(&batch)
            .expect("empty mapped batch should encode as empty payload");

        assert!(payload.is_empty());
        assert_eq!(payload.bytes(), []);
        assert_eq!(payload.row_token_offsets(), []);
    }

    #[test]
    fn default_direct_encoder_rejects_non_empty_row_batch_until_type_encoders_exist() {
        let mapping = SchemaMapping::new(
            ArrowFieldRef::new(0, "is_active".to_owned(), false, DataType::Boolean),
            MssqlColumn::new(Identifier::new("is_active").unwrap(), MssqlType::Bit, false),
        );
        let encoder =
            DirectEncoder::new_with_support(std::slice::from_ref(&mapping), &FixtureSupport)
                .expect("fixture supports the mapping");
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "is_active",
                DataType::Boolean,
                false,
            )])),
            vec![Arc::new(BooleanArray::from(vec![true]))],
        )
        .unwrap();

        let payload = encoder
            .encode_batch(&batch)
            .expect("boolean is supported now");

        assert_eq!(payload.row_count(), 1);
        assert_eq!(payload.bytes(), [payload::TDS_ROW_TOKEN, 1]);
        assert_eq!(payload.row_token_offsets(), [0]);
    }

    #[test]
    fn direct_encoder_encodes_mixed_primitive_rows_in_mapping_order() {
        let mappings = vec![
            mapping(0, "is_active", DataType::Boolean, MssqlType::Bit, false),
            mapping(1, "quantity", DataType::Int32, MssqlType::Int, false),
            mapping(2, "total", DataType::Int64, MssqlType::BigInt, false),
            mapping(3, "real_value", DataType::Float32, MssqlType::Real, false),
            mapping(
                4,
                "ratio",
                DataType::Float64,
                MssqlType::Float { precision: 53 },
                false,
            ),
        ];
        let encoder = DirectEncoder::new(&mappings).unwrap();
        let batch = record_batch(
            vec![
                Field::new("is_active", DataType::Boolean, false),
                Field::new("quantity", DataType::Int32, false),
                Field::new("total", DataType::Int64, false),
                Field::new("real_value", DataType::Float32, false),
                Field::new("ratio", DataType::Float64, false),
            ],
            vec![
                Arc::new(BooleanArray::from(vec![true, false])) as ArrayRef,
                Arc::new(Int32Array::from(vec![1, -2])),
                Arc::new(Int64Array::from(vec![10, -20])),
                Arc::new(Float32Array::from(vec![1.5, -3.25])),
                Arc::new(Float64Array::from(vec![1.25, -2.5])),
            ],
        );

        let payload = encoder.encode_batch(&batch).unwrap();

        assert_eq!(payload.row_token_offsets(), [0, 26]);
        assert_eq!(
            payload.bytes(),
            [
                payload::TDS_ROW_TOKEN,
                1,
                1,
                0,
                0,
                0,
                10,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0x00,
                0x00,
                0xC0,
                0x3F,
                0x00,
                0x00,
                0x00,
                0x00,
                0x00,
                0x00,
                0xF4,
                0x3F,
                payload::TDS_ROW_TOKEN,
                0,
                0xFE,
                0xFF,
                0xFF,
                0xFF,
                0xEC,
                0xFF,
                0xFF,
                0xFF,
                0xFF,
                0xFF,
                0xFF,
                0xFF,
                0x00,
                0x00,
                0x50,
                0xC0,
                0x00,
                0x00,
                0x00,
                0x00,
                0x00,
                0x00,
                0x04,
                0xC0,
            ]
        );
    }

    #[test]
    fn direct_encoder_encodes_mixed_primitive_and_variable_width_rows() {
        let mappings = vec![
            mapping(0, "id", DataType::Int32, MssqlType::Int, false),
            mapping(
                1,
                "name",
                DataType::Utf8,
                MssqlType::NVarChar(MssqlTypeLength::Bounded(3)),
                true,
            ),
            mapping(
                2,
                "bytes",
                DataType::Binary,
                MssqlType::VarBinary(MssqlTypeLength::Max),
                true,
            ),
        ];
        let encoder = DirectEncoder::new(&mappings).unwrap();
        let batch = record_batch(
            vec![
                Field::new("id", DataType::Int32, false),
                Field::new("name", DataType::Utf8, true),
                Field::new("bytes", DataType::Binary, true),
            ],
            vec![
                Arc::new(Int32Array::from(vec![42, -1])) as ArrayRef,
                Arc::new(StringArray::from(vec![Some("A"), None])),
                Arc::new(BinaryArray::from_iter(vec![
                    Some(&b""[..]),
                    Some(&b"xy"[..]),
                ])),
            ],
        );

        let payload = encoder.encode_batch(&batch).unwrap();

        assert_eq!(payload.row_token_offsets(), [0, 21]);
        assert_eq!(
            payload.bytes(),
            [
                payload::TDS_ROW_TOKEN,
                42,
                0,
                0,
                0,
                2,
                0,
                b'A',
                0,
                0xfe,
                0xff,
                0xff,
                0xff,
                0xff,
                0xff,
                0xff,
                0xff,
                0,
                0,
                0,
                0,
                payload::TDS_ROW_TOKEN,
                0xff,
                0xff,
                0xff,
                0xff,
                0xff,
                0xff,
                0xfe,
                0xff,
                0xff,
                0xff,
                0xff,
                0xff,
                0xff,
                0xff,
                2,
                0,
                0,
                0,
                b'x',
                b'y',
                0,
                0,
                0,
                0,
            ]
        );
    }

    #[test]
    fn direct_encoder_encodes_date32_and_date64_rows() {
        let mappings = vec![
            mapping(0, "id", DataType::Int32, MssqlType::Int, false),
            mapping(1, "created_on", DataType::Date32, MssqlType::Date, true),
            mapping(
                2,
                "created_at",
                DataType::Date64,
                MssqlType::DateTime2 { precision: 3 },
                true,
            ),
        ];
        let encoder = DirectEncoder::new(&mappings).unwrap();
        let batch = record_batch(
            vec![
                Field::new("id", DataType::Int32, false),
                Field::new("created_on", DataType::Date32, true),
                Field::new("created_at", DataType::Date64, true),
            ],
            vec![
                Arc::new(Int32Array::from(vec![1, 2])) as ArrayRef,
                Arc::new(Date32Array::from(vec![Some(0), None])),
                Arc::new(Date64Array::from(vec![Some(86_400_123), Some(0)])),
            ],
        );

        let payload = encoder.encode_batch(&batch).unwrap();

        assert_eq!(payload.row_token_offsets(), [0, 17]);
        assert_eq!(
            payload.bytes(),
            [
                payload::TDS_ROW_TOKEN,
                1,
                0,
                0,
                0,
                3,
                0x3A,
                0xF9,
                0x0A,
                7,
                0x7B,
                0,
                0,
                0,
                0x3B,
                0xF9,
                0x0A,
                payload::TDS_ROW_TOKEN,
                2,
                0,
                0,
                0,
                0,
                7,
                0,
                0,
                0,
                0,
                0x3A,
                0xF9,
                0x0A,
            ]
        );
    }

    #[test]
    fn direct_encoder_encodes_date_boundaries_and_preserves_date64_time_of_day() {
        let mappings = vec![
            mapping(0, "date_value", DataType::Date32, MssqlType::Date, false),
            mapping(
                1,
                "datetime_value",
                DataType::Date64,
                MssqlType::DateTime2 { precision: 3 },
                false,
            ),
        ];
        let encoder = DirectEncoder::new(&mappings).unwrap();
        let batch = record_batch(
            vec![
                Field::new("date_value", DataType::Date32, true),
                Field::new("datetime_value", DataType::Date64, true),
            ],
            vec![
                Arc::new(Date32Array::from(vec![-719_162, 2_932_896])) as ArrayRef,
                Arc::new(Date64Array::from(vec![
                    -62_135_596_800_000,
                    253_402_300_799_999,
                ])),
            ],
        );

        let payload = encoder.encode_batch(&batch).unwrap();

        assert_eq!(payload.row_token_offsets(), [0, 13]);
        assert_eq!(
            payload.bytes(),
            [
                payload::TDS_ROW_TOKEN,
                3,
                0,
                0,
                0,
                7,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                payload::TDS_ROW_TOKEN,
                3,
                0xDA,
                0xB9,
                0x37,
                7,
                0xFF,
                0x5B,
                0x26,
                0x05,
                0xDA,
                0xB9,
                0x37,
            ]
        );
    }

    #[test]
    fn direct_encoder_rejects_date_values_outside_sql_server_bounds() {
        let date32_mappings = vec![mapping(
            0,
            "date_value",
            DataType::Date32,
            MssqlType::Date,
            false,
        )];
        let date32_encoder = DirectEncoder::new(&date32_mappings).unwrap();
        let date32_batch = record_batch(
            vec![Field::new("date_value", DataType::Date32, false)],
            vec![Arc::new(Date32Array::from(vec![-719_163]))],
        );

        let err = date32_encoder.encode_batch(&date32_batch).unwrap_err();
        assert_value_conversion_diagnostic(
            err,
            DiagnosticCode::TimestampOutOfRange,
            Some(0),
            Some((0, "date_value")),
        );

        let date64_mappings = vec![mapping(
            0,
            "datetime_value",
            DataType::Date64,
            MssqlType::DateTime2 { precision: 3 },
            false,
        )];
        let date64_encoder = DirectEncoder::new(&date64_mappings).unwrap();
        let date64_batch = record_batch(
            vec![Field::new("datetime_value", DataType::Date64, false)],
            vec![Arc::new(Date64Array::from(vec![253_402_300_800_000]))],
        );

        let err = date64_encoder.encode_batch(&date64_batch).unwrap_err();
        assert_value_conversion_diagnostic(
            err,
            DiagnosticCode::TimestampOutOfRange,
            Some(0),
            Some((0, "datetime_value")),
        );
    }

    #[test]
    fn direct_encoder_rejects_date_nulls_in_non_nullable_columns() {
        let mappings = vec![
            mapping(0, "date_value", DataType::Date32, MssqlType::Date, false),
            mapping(
                1,
                "datetime_value",
                DataType::Date64,
                MssqlType::DateTime2 { precision: 3 },
                false,
            ),
        ];
        let encoder = DirectEncoder::new(&mappings).unwrap();
        let batch = record_batch(
            vec![
                Field::new("date_value", DataType::Date32, true),
                Field::new("datetime_value", DataType::Date64, true),
            ],
            vec![
                Arc::new(Date32Array::from(vec![Some(0)])) as ArrayRef,
                Arc::new(Date64Array::from(vec![None])),
            ],
        );

        let err = encoder.encode_batch(&batch).unwrap_err();
        assert_value_conversion_diagnostic(
            err,
            DiagnosticCode::NullInNonNullableColumn,
            Some(0),
            Some((1, "datetime_value")),
        );
    }

    #[test]
    fn direct_encoder_encodes_timestamp_datetime2_rows() {
        let mappings = vec![
            mapping(
                0,
                "created_at",
                DataType::Timestamp(TimeUnit::Second, None),
                MssqlType::DateTime2 { precision: 7 },
                true,
            ),
            mapping(
                1,
                "precise_at",
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                MssqlType::DateTime2 { precision: 7 },
                true,
            ),
        ];
        let encoder = DirectEncoder::new(&mappings).unwrap();
        let batch = record_batch(
            vec![
                Field::new(
                    "created_at",
                    DataType::Timestamp(TimeUnit::Second, None),
                    true,
                ),
                Field::new(
                    "precise_at",
                    DataType::Timestamp(TimeUnit::Nanosecond, None),
                    true,
                ),
            ],
            vec![
                Arc::new(TimestampSecondArray::from(vec![Some(0), None])) as ArrayRef,
                Arc::new(TimestampNanosecondArray::from(vec![
                    Some(123_456_700),
                    Some(0),
                ])),
            ],
        );

        let payload = encoder.encode_batch(&batch).unwrap();

        assert_eq!(payload.row_token_offsets(), [0, 19]);
        assert_eq!(
            payload.bytes(),
            [
                payload::TDS_ROW_TOKEN,
                8,
                0,
                0,
                0,
                0,
                0,
                0x3A,
                0xF9,
                0x0A,
                8,
                0x87,
                0xD6,
                0x12,
                0,
                0,
                0x3A,
                0xF9,
                0x0A,
                payload::TDS_ROW_TOKEN,
                0,
                8,
                0,
                0,
                0,
                0,
                0,
                0x3A,
                0xF9,
                0x0A,
            ]
        );
    }

    #[test]
    fn direct_encoder_applies_timestamp_nanosecond_policy() {
        let mappings = vec![mapping(
            0,
            "precise_at",
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            MssqlType::DateTime2 { precision: 7 },
            false,
        )];
        let batch = record_batch(
            vec![Field::new(
                "precise_at",
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                false,
            )],
            vec![Arc::new(TimestampNanosecondArray::from(vec![150]))],
        );

        let reject = DirectEncoder::new(&mappings)
            .unwrap()
            .encode_batch(&batch)
            .unwrap_err();
        assert_value_conversion_diagnostic(
            reject,
            DiagnosticCode::LossyConversionRequiresPolicy,
            Some(0),
            Some((0, "precise_at")),
        );

        let round = DirectEncoder::new_with_options(
            &mappings,
            PlanOptions {
                nanosecond_policy: NanosecondPolicy::RoundTo100ns,
                ..PlanOptions::default()
            },
        )
        .unwrap()
        .encode_batch(&batch)
        .unwrap();

        assert_eq!(
            round.bytes(),
            [payload::TDS_ROW_TOKEN, 8, 2, 0, 0, 0, 0, 0x3A, 0xF9, 0x0A,]
        );

        let truncate = DirectEncoder::new_with_options(
            &mappings,
            PlanOptions {
                nanosecond_policy: NanosecondPolicy::TruncateTo100ns,
                ..PlanOptions::default()
            },
        )
        .unwrap()
        .encode_batch(&batch)
        .unwrap();

        assert_eq!(
            truncate.bytes(),
            [payload::TDS_ROW_TOKEN, 8, 1, 0, 0, 0, 0, 0x3A, 0xF9, 0x0A,]
        );
    }

    #[test]
    fn direct_encoder_encodes_all_timestamp_datetime2_units() {
        let mappings = vec![
            mapping(
                0,
                "ts_s",
                DataType::Timestamp(TimeUnit::Second, None),
                MssqlType::DateTime2 { precision: 7 },
                false,
            ),
            mapping(
                1,
                "ts_ms",
                DataType::Timestamp(TimeUnit::Millisecond, Some("".into())),
                MssqlType::DateTime2 { precision: 7 },
                false,
            ),
            mapping(
                2,
                "ts_us",
                DataType::Timestamp(TimeUnit::Microsecond, None),
                MssqlType::DateTime2 { precision: 7 },
                false,
            ),
            mapping(
                3,
                "ts_ns",
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                MssqlType::DateTime2 { precision: 7 },
                false,
            ),
        ];
        let encoder = DirectEncoder::new(&mappings).unwrap();
        let batch = record_batch(
            vec![
                Field::new("ts_s", DataType::Timestamp(TimeUnit::Second, None), false),
                Field::new(
                    "ts_ms",
                    DataType::Timestamp(TimeUnit::Millisecond, Some("".into())),
                    false,
                ),
                Field::new(
                    "ts_us",
                    DataType::Timestamp(TimeUnit::Microsecond, None),
                    false,
                ),
                Field::new(
                    "ts_ns",
                    DataType::Timestamp(TimeUnit::Nanosecond, None),
                    false,
                ),
            ],
            vec![
                Arc::new(TimestampSecondArray::from(vec![1])) as ArrayRef,
                Arc::new(TimestampMillisecondArray::from(vec![1_001]).with_timezone("")),
                Arc::new(TimestampMicrosecondArray::from(vec![1_001_234])),
                Arc::new(TimestampNanosecondArray::from(vec![1_001_234_500])),
            ],
        );

        let payload = encoder.encode_batch(&batch).unwrap();

        assert_eq!(payload.row_token_offsets(), [0]);
        assert_eq!(
            payload.bytes(),
            expected_rows([[
                datetime2_7_cell(719_162, 10_000_000),
                datetime2_7_cell(719_162, 10_010_000),
                datetime2_7_cell(719_162, 10_012_340),
                datetime2_7_cell(719_162, 10_012_345),
            ]])
        );
    }

    #[test]
    fn direct_encoder_encodes_timestamp_datetime_rows_through_general_path() {
        let mappings = vec![
            mapping(
                0,
                "label",
                DataType::Utf8,
                MssqlType::NVarChar(MssqlTypeLength::Bounded(16)),
                false,
            ),
            mapping(
                1,
                "ts_s",
                DataType::Timestamp(TimeUnit::Second, None),
                MssqlType::DateTime,
                false,
            ),
            mapping(
                2,
                "ts_ms",
                DataType::Timestamp(TimeUnit::Millisecond, Some("".into())),
                MssqlType::DateTime,
                false,
            ),
            mapping(
                3,
                "ts_us",
                DataType::Timestamp(TimeUnit::Microsecond, None),
                MssqlType::DateTime,
                false,
            ),
            mapping(
                4,
                "ts_ns",
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                MssqlType::DateTime,
                false,
            ),
            mapping(
                5,
                "ts_tz",
                DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
                MssqlType::DateTime,
                true,
            ),
        ];
        let encoder = DirectEncoder::new(&mappings).unwrap();
        let batch = record_batch(
            vec![
                Field::new("label", DataType::Utf8, false),
                Field::new("ts_s", DataType::Timestamp(TimeUnit::Second, None), false),
                Field::new(
                    "ts_ms",
                    DataType::Timestamp(TimeUnit::Millisecond, Some("".into())),
                    false,
                ),
                Field::new(
                    "ts_us",
                    DataType::Timestamp(TimeUnit::Microsecond, None),
                    false,
                ),
                Field::new(
                    "ts_ns",
                    DataType::Timestamp(TimeUnit::Nanosecond, None),
                    false,
                ),
                Field::new(
                    "ts_tz",
                    DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
                    true,
                ),
            ],
            vec![
                Arc::new(StringArray::from(vec!["a", "b"])) as ArrayRef,
                Arc::new(TimestampSecondArray::from(vec![1, -1])),
                Arc::new(TimestampMillisecondArray::from(vec![1, 86_399_999]).with_timezone("")),
                Arc::new(TimestampMicrosecondArray::from(vec![1_700, 0])),
                Arc::new(TimestampNanosecondArray::from(vec![1_700_000, 0])),
                Arc::new(
                    TimestampMicrosecondArray::from(vec![Some(86_399_999_000), None])
                        .with_timezone("UTC"),
                ),
            ],
        );

        let payload = encoder.encode_batch(&batch).unwrap();

        assert_eq!(payload.row_token_offsets(), [0, 50]);
        assert_eq!(
            payload.bytes(),
            expected_rows([
                [
                    bounded_nvarchar_cell("a"),
                    datetime_cell(25_567, 300),
                    datetime_cell(25_567, 0),
                    datetime_cell(25_567, 1),
                    datetime_cell(25_567, 1),
                    datetime_cell(25_568, 0),
                ],
                [
                    bounded_nvarchar_cell("b"),
                    datetime_cell(25_566, 25_919_700),
                    datetime_cell(25_568, 0),
                    datetime_cell(25_567, 0),
                    datetime_cell(25_567, 0),
                    null_cell(),
                ],
            ])
        );
    }

    #[test]
    fn direct_encoder_applies_datetime_nanosecond_policy() {
        let mappings = vec![mapping(
            0,
            "precise_at",
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            MssqlType::DateTime,
            false,
        )];
        let batch = record_batch(
            vec![Field::new(
                "precise_at",
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                false,
            )],
            vec![Arc::new(TimestampNanosecondArray::from(vec![150]))],
        );

        let reject = DirectEncoder::new(&mappings)
            .unwrap()
            .encode_batch(&batch)
            .unwrap_err();
        assert_value_conversion_diagnostic(
            reject,
            DiagnosticCode::LossyConversionRequiresPolicy,
            Some(0),
            Some((0, "precise_at")),
        );

        let round = DirectEncoder::new_with_options(
            &mappings,
            PlanOptions {
                nanosecond_policy: NanosecondPolicy::RoundTo100ns,
                ..PlanOptions::default()
            },
        )
        .unwrap()
        .encode_batch(&batch)
        .unwrap();

        assert_eq!(round.bytes(), expected_rows([[datetime_cell(25_567, 0)]]));
    }

    #[test]
    fn direct_encoder_rounds_negative_timestamp_nanoseconds_across_day_boundary() {
        let mappings = vec![mapping(
            0,
            "precise_at",
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            MssqlType::DateTime2 { precision: 7 },
            false,
        )];
        let encoder = DirectEncoder::new_with_options(
            &mappings,
            PlanOptions {
                nanosecond_policy: NanosecondPolicy::RoundTo100ns,
                ..PlanOptions::default()
            },
        )
        .unwrap();
        let batch = record_batch(
            vec![Field::new(
                "precise_at",
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                false,
            )],
            vec![Arc::new(TimestampNanosecondArray::from(vec![-149, -50]))],
        );

        let payload = encoder.encode_batch(&batch).unwrap();

        assert_eq!(
            payload.bytes(),
            expected_rows([
                [datetime2_7_cell(719_161, 863_999_999_999)],
                [datetime2_7_cell(719_162, 0)],
            ])
        );
    }

    #[test]
    fn direct_encoder_encodes_timezone_aware_timestamps_as_normalized_datetime2() {
        let mappings = vec![
            mapping(
                0,
                "ny",
                DataType::Timestamp(TimeUnit::Second, Some("America/New_York".into())),
                MssqlType::DateTime2 { precision: 7 },
                true,
            ),
            mapping(
                1,
                "offset",
                DataType::Timestamp(TimeUnit::Millisecond, Some("+02:30".into())),
                MssqlType::DateTime2 { precision: 7 },
                true,
            ),
        ];
        let encoder = DirectEncoder::new(&mappings).unwrap();
        let batch = record_batch(
            vec![
                Field::new(
                    "ny",
                    DataType::Timestamp(TimeUnit::Second, Some("America/New_York".into())),
                    true,
                ),
                Field::new(
                    "offset",
                    DataType::Timestamp(TimeUnit::Millisecond, Some("+02:30".into())),
                    true,
                ),
            ],
            vec![
                Arc::new(
                    TimestampSecondArray::from(vec![Some(0), Some(1_750_593_600)])
                        .with_timezone("America/New_York"),
                ) as ArrayRef,
                Arc::new(
                    TimestampMillisecondArray::from(vec![Some(1_234), None])
                        .with_timezone("+02:30"),
                ),
            ],
        );

        let payload = encoder.encode_batch(&batch).unwrap();

        assert_eq!(
            payload.bytes(),
            expected_rows([
                [
                    datetime2_7_cell(719_162, 0),
                    datetime2_7_cell(719_162, 12_340_000),
                ],
                [datetime2_7_cell(739_423, 432_000_000_000), null_cell()],
            ])
        );
    }

    #[test]
    fn direct_encoder_measured_timestamp_ranges_concatenate_to_full_payload() {
        let mappings = vec![
            mapping(0, "id", DataType::Int32, MssqlType::Int, false),
            mapping(
                1,
                "precise_at",
                DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
                MssqlType::DateTime2 { precision: 7 },
                true,
            ),
        ];
        let encoder = DirectEncoder::new_with_options(
            &mappings,
            PlanOptions {
                nanosecond_policy: NanosecondPolicy::RoundTo100ns,
                ..PlanOptions::default()
            },
        )
        .unwrap();
        let batch = record_batch(
            vec![
                Field::new("id", DataType::Int32, false),
                Field::new(
                    "precise_at",
                    DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
                    true,
                ),
            ],
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3])) as ArrayRef,
                Arc::new(
                    TimestampNanosecondArray::from(vec![Some(150), None, Some(-50)])
                        .with_timezone("UTC"),
                ),
            ],
        );

        let measured = encoder.measure_batch(&batch).unwrap();
        let full = encoder.encode_batch(&batch).unwrap();
        let first = encoder
            .encode_measured_batch_range(&batch, &measured, 0, 2)
            .unwrap();
        let second = encoder
            .encode_measured_batch_range(&batch, &measured, 2, 1)
            .unwrap();
        let mut concatenated = Vec::new();
        concatenated.extend_from_slice(first.bytes());
        concatenated.extend_from_slice(second.bytes());

        assert_eq!(measured.row_count(), 3);
        assert_eq!(measured.cell_len(0, 1).unwrap(), 9);
        assert_eq!(measured.cell_len(1, 1).unwrap(), 1);
        assert_eq!(measured.cell_len(2, 1).unwrap(), 9);
        assert_eq!(concatenated, full.bytes());
        assert_eq!(first.row_token_offsets()[0], 0);
        assert_eq!(second.row_token_offsets()[0], 0);
    }

    #[test]
    fn direct_encoder_fixed_width_measured_range_uses_supplied_layout() {
        let mappings = vec![
            mapping(0, "id", DataType::Int32, MssqlType::Int, false),
            mapping(1, "quantity", DataType::Int32, MssqlType::Int, true),
            mapping(
                2,
                "precise_at",
                DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
                MssqlType::DateTime2 { precision: 7 },
                true,
            ),
        ];
        let encoder = DirectEncoder::new_with_options(
            &mappings,
            PlanOptions {
                nanosecond_policy: NanosecondPolicy::RoundTo100ns,
                ..PlanOptions::default()
            },
        )
        .unwrap();
        let batch = record_batch(
            vec![
                Field::new("id", DataType::Int32, false),
                Field::new("quantity", DataType::Int32, true),
                Field::new(
                    "precise_at",
                    DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
                    true,
                ),
            ],
            vec![
                Arc::new(Int32Array::from(vec![10, 20, 30, 40])) as ArrayRef,
                Arc::new(Int32Array::from(vec![Some(1), None, Some(-3), Some(4)])),
                Arc::new(
                    TimestampNanosecondArray::from(vec![Some(150), None, Some(-50), Some(250)])
                        .with_timezone("UTC"),
                ),
            ],
        );

        let measured = encoder.measure_batch(&batch).unwrap();
        let layout = measured.range_layout(1, 2).unwrap();
        let range_batch = batch.slice(1, 2);
        let bound = BoundDirectBatch::new(&encoder, &range_batch).unwrap();
        let fixed_width_range = try_encode_fixed_width_rows_with_layout(&bound, &layout)
            .unwrap()
            .expect("fixed-width measured range path should be active");
        let slice_range = encoder.encode_batch_range(&batch, 1, 2).unwrap();

        assert_eq!(layout.row_token_offsets(), [0, 7]);
        assert_eq!(fixed_width_range.row_token_offsets(), [0, 7]);
        assert_eq!(fixed_width_range.bytes(), slice_range.bytes());
        assert_eq!(
            fixed_width_range.bytes(),
            expected_rows([
                [int32_cell(20), null_cell(), null_cell()],
                [
                    int32_cell(30),
                    nullable_int32_cell(-3),
                    datetime2_7_cell(719_162, 0)
                ]
            ])
        );
    }

    #[test]
    fn direct_encoder_measured_range_fixed_width_path_falls_back_for_variable_width() {
        let mappings = vec![
            mapping(0, "id", DataType::Int32, MssqlType::Int, false),
            mapping(
                1,
                "name",
                DataType::Utf8,
                MssqlType::NVarChar(MssqlTypeLength::Max),
                true,
            ),
        ];
        let encoder = DirectEncoder::new(&mappings).unwrap();
        let batch = record_batch(
            vec![
                Field::new("id", DataType::Int32, false),
                Field::new("name", DataType::Utf8, true),
            ],
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3])) as ArrayRef,
                Arc::new(StringArray::from(vec![Some("alpha"), None, Some("beta")])),
            ],
        );

        let measured = encoder.measure_batch(&batch).unwrap();
        let layout = measured.range_layout(1, 2).unwrap();
        let range_batch = batch.slice(1, 2);
        let bound = BoundDirectBatch::new(&encoder, &range_batch).unwrap();

        assert!(
            try_encode_fixed_width_rows_with_layout(&bound, &layout)
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn direct_encoder_measured_range_does_not_remeasure_fixed_width_rows() {
        let mappings = vec![
            mapping(0, "id", DataType::Int32, MssqlType::Int, false),
            mapping(1, "quantity", DataType::Int32, MssqlType::Int, true),
        ];
        let encoder = DirectEncoder::new(&mappings).unwrap();
        let batch = record_batch(
            vec![
                Field::new("id", DataType::Int32, false),
                Field::new("quantity", DataType::Int32, true),
            ],
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3])) as ArrayRef,
                Arc::new(Int32Array::from(vec![Some(10), None, Some(30)])),
            ],
        );

        let measured = encoder.measure_batch(&batch).unwrap();

        reset_fixed_width_measure_call_count();
        let payload = encoder
            .encode_measured_batch_range(&batch, &measured, 1, 2)
            .unwrap();

        assert_eq!(payload.row_token_offsets(), [0, 6]);
        assert_eq!(fixed_width_measure_call_count(), 0);
    }

    #[test]
    fn direct_encoder_fixed_width_with_layout_rejects_missing_cells() {
        let mappings = vec![
            mapping(0, "id", DataType::Int32, MssqlType::Int, false),
            mapping(1, "quantity", DataType::Int32, MssqlType::Int, false),
        ];
        let encoder = DirectEncoder::new(&mappings).unwrap();
        let batch = record_batch(
            vec![
                Field::new("id", DataType::Int32, false),
                Field::new("quantity", DataType::Int32, false),
            ],
            vec![
                Arc::new(Int32Array::from(vec![1])) as ArrayRef,
                Arc::new(Int32Array::from(vec![10])),
            ],
        );
        let bound = BoundDirectBatch::new(&encoder, &batch).unwrap();
        let incomplete_layout =
            RowLayout::new(vec![0], vec![5], vec![CellPosition::new(0, 0, 1, 4)], 5).unwrap();

        let err = try_encode_fixed_width_rows_with_layout(&bound, &incomplete_layout)
            .expect_err("missing cell positions must not encode a corrupt payload");

        assert_direct_encoding_diagnostic(err, DiagnosticCode::DirectEncodingInvalidPayload);
    }

    #[test]
    fn direct_encoder_fixed_width_with_layout_rejects_shuffled_cells() {
        let mappings = vec![
            mapping(0, "id", DataType::Int32, MssqlType::Int, false),
            mapping(1, "quantity", DataType::Int32, MssqlType::Int, false),
        ];
        let encoder = DirectEncoder::new(&mappings).unwrap();
        let batch = record_batch(
            vec![
                Field::new("id", DataType::Int32, false),
                Field::new("quantity", DataType::Int32, false),
            ],
            vec![
                Arc::new(Int32Array::from(vec![1])) as ArrayRef,
                Arc::new(Int32Array::from(vec![10])),
            ],
        );
        let bound = BoundDirectBatch::new(&encoder, &batch).unwrap();
        let shuffled_layout = RowLayout::new(
            vec![0],
            vec![9],
            vec![CellPosition::new(0, 1, 1, 4), CellPosition::new(0, 0, 5, 4)],
            9,
        )
        .unwrap();

        let err = try_encode_fixed_width_rows_with_layout(&bound, &shuffled_layout)
            .expect_err("shuffled cell positions must not encode a corrupt payload");

        assert_direct_encoding_diagnostic(err, DiagnosticCode::DirectEncodingInvalidPayload);
    }

    #[test]
    fn direct_encoder_encodes_all_time_units() {
        let mappings = vec![
            mapping(
                0,
                "time_s",
                DataType::Time32(TimeUnit::Second),
                MssqlType::Time(MssqlTimePrecision::ZERO),
                false,
            ),
            mapping(
                1,
                "time_ms",
                DataType::Time32(TimeUnit::Millisecond),
                MssqlType::Time(MssqlTimePrecision::THREE),
                false,
            ),
            mapping(
                2,
                "time_us",
                DataType::Time64(TimeUnit::Microsecond),
                MssqlType::Time(MssqlTimePrecision::SIX),
                false,
            ),
            mapping(
                3,
                "time_ns",
                DataType::Time64(TimeUnit::Nanosecond),
                MssqlType::Time(MssqlTimePrecision::SEVEN),
                false,
            ),
        ];
        let encoder = DirectEncoder::new(&mappings).unwrap();
        let batch = record_batch(
            vec![
                Field::new("time_s", DataType::Time32(TimeUnit::Second), false),
                Field::new("time_ms", DataType::Time32(TimeUnit::Millisecond), false),
                Field::new("time_us", DataType::Time64(TimeUnit::Microsecond), false),
                Field::new("time_ns", DataType::Time64(TimeUnit::Nanosecond), false),
            ],
            vec![
                Arc::new(Time32SecondArray::from(vec![86_399])) as ArrayRef,
                Arc::new(Time32MillisecondArray::from(vec![12_345])),
                Arc::new(Time64MicrosecondArray::from(vec![12_345_678])),
                Arc::new(Time64NanosecondArray::from(vec![12_345_678_900])),
            ],
        );

        let payload = encoder.encode_batch(&batch).unwrap();

        assert_eq!(payload.row_token_offsets(), [0]);
        assert_eq!(
            payload.bytes(),
            expected_rows([[
                time_cell(0, 86_399),
                time_cell(3, 12_345),
                time_cell(6, 12_345_678),
                time_cell(7, 123_456_789),
            ]])
        );
    }

    #[test]
    fn direct_encoder_encodes_time_nulls_and_measured_ranges() {
        let mappings = vec![
            mapping(0, "id", DataType::Int32, MssqlType::Int, false),
            mapping(
                1,
                "time_ms",
                DataType::Time32(TimeUnit::Millisecond),
                MssqlType::Time(MssqlTimePrecision::THREE),
                true,
            ),
            mapping(
                2,
                "time_ns",
                DataType::Time64(TimeUnit::Nanosecond),
                MssqlType::Time(MssqlTimePrecision::SEVEN),
                true,
            ),
        ];
        let encoder = DirectEncoder::new_with_options(
            &mappings,
            PlanOptions {
                nanosecond_policy: NanosecondPolicy::RoundTo100ns,
                ..PlanOptions::default()
            },
        )
        .unwrap();
        let batch = record_batch(
            vec![
                Field::new("id", DataType::Int32, false),
                Field::new("time_ms", DataType::Time32(TimeUnit::Millisecond), true),
                Field::new("time_ns", DataType::Time64(TimeUnit::Nanosecond), true),
            ],
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3])) as ArrayRef,
                Arc::new(Time32MillisecondArray::from(vec![
                    Some(1),
                    None,
                    Some(86_399_999),
                ])),
                Arc::new(Time64NanosecondArray::from(vec![
                    Some(149),
                    Some(150),
                    None,
                ])),
            ],
        );

        let measured = encoder.measure_batch(&batch).unwrap();
        let full = encoder.encode_batch(&batch).unwrap();
        let first = encoder
            .encode_measured_batch_range(&batch, &measured, 0, 2)
            .unwrap();
        let second = encoder
            .encode_measured_batch_range(&batch, &measured, 2, 1)
            .unwrap();
        let mut concatenated = Vec::new();
        concatenated.extend_from_slice(first.bytes());
        concatenated.extend_from_slice(second.bytes());

        assert_eq!(measured.row_count(), 3);
        assert_eq!(measured.cell_len(0, 1).unwrap(), 5);
        assert_eq!(measured.cell_len(1, 1).unwrap(), 1);
        assert_eq!(measured.cell_len(0, 2).unwrap(), 6);
        assert_eq!(measured.cell_len(2, 2).unwrap(), 1);
        assert_eq!(concatenated, full.bytes());
        assert_eq!(
            full.bytes(),
            expected_rows([
                [int32_cell(1), time_cell(3, 1), time_cell(7, 1)],
                [int32_cell(2), null_cell(), time_cell(7, 2)],
                [int32_cell(3), time_cell(3, 86_399_999), null_cell()],
            ])
        );
    }

    #[test]
    fn direct_encoder_rejects_time_out_of_range_lossy_nanoseconds_and_non_nullable_nulls() {
        let mappings = vec![
            mapping(
                0,
                "time_s",
                DataType::Time32(TimeUnit::Second),
                MssqlType::Time(MssqlTimePrecision::ZERO),
                false,
            ),
            mapping(
                1,
                "time_ns",
                DataType::Time64(TimeUnit::Nanosecond),
                MssqlType::Time(MssqlTimePrecision::SEVEN),
                false,
            ),
        ];
        let encoder = DirectEncoder::new(&mappings).unwrap();

        let negative_batch = record_batch(
            vec![
                Field::new("time_s", DataType::Time32(TimeUnit::Second), false),
                Field::new("time_ns", DataType::Time64(TimeUnit::Nanosecond), false),
            ],
            vec![
                Arc::new(Time32SecondArray::from(vec![-1])) as ArrayRef,
                Arc::new(Time64NanosecondArray::from(vec![0])),
            ],
        );
        assert_value_conversion_diagnostic(
            encoder.encode_batch(&negative_batch).unwrap_err(),
            DiagnosticCode::TimestampOutOfRange,
            Some(0),
            Some((0, "time_s")),
        );

        let exact_day_batch = record_batch(
            vec![
                Field::new("time_s", DataType::Time32(TimeUnit::Second), false),
                Field::new("time_ns", DataType::Time64(TimeUnit::Nanosecond), false),
            ],
            vec![
                Arc::new(Time32SecondArray::from(vec![0])) as ArrayRef,
                Arc::new(Time64NanosecondArray::from(vec![86_400_000_000_000])),
            ],
        );
        assert_value_conversion_diagnostic(
            encoder.encode_batch(&exact_day_batch).unwrap_err(),
            DiagnosticCode::TimestampOutOfRange,
            Some(0),
            Some((1, "time_ns")),
        );

        let lossy_batch = record_batch(
            vec![
                Field::new("time_s", DataType::Time32(TimeUnit::Second), false),
                Field::new("time_ns", DataType::Time64(TimeUnit::Nanosecond), false),
            ],
            vec![
                Arc::new(Time32SecondArray::from(vec![0])) as ArrayRef,
                Arc::new(Time64NanosecondArray::from(vec![101])),
            ],
        );
        assert_value_conversion_diagnostic(
            encoder.encode_batch(&lossy_batch).unwrap_err(),
            DiagnosticCode::LossyConversionRequiresPolicy,
            Some(0),
            Some((1, "time_ns")),
        );

        let null_batch = record_batch(
            vec![
                Field::new("time_s", DataType::Time32(TimeUnit::Second), true),
                Field::new("time_ns", DataType::Time64(TimeUnit::Nanosecond), false),
            ],
            vec![
                Arc::new(Time32SecondArray::from(vec![None])) as ArrayRef,
                Arc::new(Time64NanosecondArray::from(vec![0])),
            ],
        );
        assert_value_conversion_diagnostic(
            encoder.encode_batch(&null_batch).unwrap_err(),
            DiagnosticCode::NullInNonNullableColumn,
            Some(0),
            Some((0, "time_s")),
        );
    }

    #[test]
    fn direct_encoder_encodes_all_datetimeoffset_timestamp_units() {
        let mappings = vec![
            mapping(
                0,
                "dto_s",
                DataType::Timestamp(TimeUnit::Second, Some("+02:30".into())),
                MssqlType::DateTimeOffset { precision: 7 },
                false,
            ),
            mapping(
                1,
                "dto_ms",
                DataType::Timestamp(TimeUnit::Millisecond, Some("-07".into())),
                MssqlType::DateTimeOffset { precision: 7 },
                false,
            ),
            mapping(
                2,
                "dto_us",
                DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
                MssqlType::DateTimeOffset { precision: 7 },
                false,
            ),
            mapping(
                3,
                "dto_ns",
                DataType::Timestamp(TimeUnit::Nanosecond, Some("+00:00".into())),
                MssqlType::DateTimeOffset { precision: 7 },
                false,
            ),
        ];
        let encoder = DirectEncoder::new(&mappings).unwrap();
        let batch = record_batch(
            vec![
                Field::new(
                    "dto_s",
                    DataType::Timestamp(TimeUnit::Second, Some("+02:30".into())),
                    false,
                ),
                Field::new(
                    "dto_ms",
                    DataType::Timestamp(TimeUnit::Millisecond, Some("-07".into())),
                    false,
                ),
                Field::new(
                    "dto_us",
                    DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
                    false,
                ),
                Field::new(
                    "dto_ns",
                    DataType::Timestamp(TimeUnit::Nanosecond, Some("+00:00".into())),
                    false,
                ),
            ],
            vec![
                Arc::new(TimestampSecondArray::from(vec![1]).with_timezone("+02:30")) as ArrayRef,
                Arc::new(TimestampMillisecondArray::from(vec![1_001]).with_timezone("-07")),
                Arc::new(TimestampMicrosecondArray::from(vec![1_001_234]).with_timezone("UTC")),
                Arc::new(
                    TimestampNanosecondArray::from(vec![1_001_234_500]).with_timezone("+00:00"),
                ),
            ],
        );

        let payload = encoder.encode_batch(&batch).unwrap();

        assert_eq!(payload.row_token_offsets(), [0]);
        assert_eq!(
            payload.bytes(),
            expected_rows([[
                datetimeoffset_7_cell(719_162, 10_000_000, 150),
                datetimeoffset_7_cell(719_162, 10_010_000, -420),
                datetimeoffset_7_cell(719_162, 10_012_340, 0),
                datetimeoffset_7_cell(719_162, 10_012_345, 0),
            ]])
        );
    }

    #[test]
    fn direct_encoder_encodes_datetimeoffset_named_timezone_nulls_and_measured_ranges() {
        let mappings = vec![
            mapping(0, "id", DataType::Int32, MssqlType::Int, false),
            mapping(
                1,
                "ny",
                DataType::Timestamp(TimeUnit::Second, Some("America/New_York".into())),
                MssqlType::DateTimeOffset { precision: 7 },
                true,
            ),
        ];
        let encoder = DirectEncoder::new(&mappings).unwrap();
        let batch = record_batch(
            vec![
                Field::new("id", DataType::Int32, false),
                Field::new(
                    "ny",
                    DataType::Timestamp(TimeUnit::Second, Some("America/New_York".into())),
                    true,
                ),
            ],
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3])) as ArrayRef,
                Arc::new(
                    TimestampSecondArray::from(vec![
                        Some(1_738_411_200),
                        Some(1_750_593_600),
                        None,
                    ])
                    .with_timezone("America/New_York"),
                ),
            ],
        );

        let measured = encoder.measure_batch(&batch).unwrap();
        let full = encoder.encode_batch(&batch).unwrap();
        let first = encoder
            .encode_measured_batch_range(&batch, &measured, 0, 2)
            .unwrap();
        let second = encoder
            .encode_measured_batch_range(&batch, &measured, 2, 1)
            .unwrap();
        let mut concatenated = Vec::new();
        concatenated.extend_from_slice(first.bytes());
        concatenated.extend_from_slice(second.bytes());

        assert_eq!(measured.row_count(), 3);
        assert_eq!(measured.cell_len(0, 1).unwrap(), 11);
        assert_eq!(measured.cell_len(1, 1).unwrap(), 11);
        assert_eq!(measured.cell_len(2, 1).unwrap(), 1);
        assert_eq!(concatenated, full.bytes());
        assert_eq!(
            full.bytes(),
            expected_rows([
                [
                    int32_cell(1),
                    datetimeoffset_7_cell(739_282, 432_000_000_000, -300),
                ],
                [
                    int32_cell(2),
                    datetimeoffset_7_cell(739_423, 432_000_000_000, -240),
                ],
                [int32_cell(3), null_cell()],
            ])
        );
    }

    #[test]
    fn direct_encoder_rejects_datetimeoffset_invalid_timezone_lossy_ns_range_and_nulls() {
        let invalid_timezone_mappings = vec![mapping(
            0,
            "dto",
            DataType::Timestamp(TimeUnit::Second, Some("Foobar".into())),
            MssqlType::DateTimeOffset { precision: 7 },
            true,
        )];
        let invalid_timezone_encoder = DirectEncoder::new(&invalid_timezone_mappings).unwrap();
        let invalid_timezone_batch = record_batch(
            vec![Field::new(
                "dto",
                DataType::Timestamp(TimeUnit::Second, Some("Foobar".into())),
                true,
            )],
            vec![Arc::new(
                TimestampSecondArray::from(vec![None]).with_timezone("Foobar"),
            )],
        );
        assert_value_conversion_diagnostic(
            invalid_timezone_encoder
                .encode_batch(&invalid_timezone_batch)
                .unwrap_err(),
            DiagnosticCode::TimezoneUnsupported,
            Some(0),
            Some((0, "dto")),
        );

        let lossy_mappings = vec![mapping(
            0,
            "dto",
            DataType::Timestamp(TimeUnit::Nanosecond, Some("+00:00".into())),
            MssqlType::DateTimeOffset { precision: 7 },
            false,
        )];
        let lossy_encoder = DirectEncoder::new(&lossy_mappings).unwrap();
        let lossy_batch = record_batch(
            vec![Field::new(
                "dto",
                DataType::Timestamp(TimeUnit::Nanosecond, Some("+00:00".into())),
                false,
            )],
            vec![Arc::new(
                TimestampNanosecondArray::from(vec![101]).with_timezone("+00:00"),
            )],
        );
        assert_value_conversion_diagnostic(
            lossy_encoder.encode_batch(&lossy_batch).unwrap_err(),
            DiagnosticCode::LossyConversionRequiresPolicy,
            Some(0),
            Some((0, "dto")),
        );

        let range_mappings = vec![mapping(
            0,
            "dto",
            DataType::Timestamp(TimeUnit::Second, Some("-14:00".into())),
            MssqlType::DateTimeOffset { precision: 7 },
            false,
        )];
        let range_encoder = DirectEncoder::new(&range_mappings).unwrap();
        let range_batch = record_batch(
            vec![Field::new(
                "dto",
                DataType::Timestamp(TimeUnit::Second, Some("-14:00".into())),
                false,
            )],
            vec![Arc::new(
                TimestampSecondArray::from(vec![-62_135_596_800]).with_timezone("-14:00"),
            )],
        );
        assert_value_conversion_diagnostic(
            range_encoder.encode_batch(&range_batch).unwrap_err(),
            DiagnosticCode::TimestampOutOfRange,
            Some(0),
            Some((0, "dto")),
        );

        let non_nullable_mappings = vec![mapping(
            0,
            "dto",
            DataType::Timestamp(TimeUnit::Second, Some("+00:00".into())),
            MssqlType::DateTimeOffset { precision: 7 },
            false,
        )];
        let non_nullable_encoder = DirectEncoder::new(&non_nullable_mappings).unwrap();
        let null_batch = record_batch(
            vec![Field::new(
                "dto",
                DataType::Timestamp(TimeUnit::Second, Some("+00:00".into())),
                true,
            )],
            vec![Arc::new(
                TimestampSecondArray::from(vec![None]).with_timezone("+00:00"),
            )],
        );
        assert_value_conversion_diagnostic(
            non_nullable_encoder.encode_batch(&null_batch).unwrap_err(),
            DiagnosticCode::NullInNonNullableColumn,
            Some(0),
            Some((0, "dto")),
        );
    }

    #[test]
    fn direct_encoder_rejects_timestamp_out_of_range_and_non_nullable_nulls() {
        let mappings = vec![mapping(
            0,
            "created_at",
            DataType::Timestamp(TimeUnit::Second, None),
            MssqlType::DateTime2 { precision: 7 },
            false,
        )];
        let encoder = DirectEncoder::new(&mappings).unwrap();
        let out_of_range_batch = record_batch(
            vec![Field::new(
                "created_at",
                DataType::Timestamp(TimeUnit::Second, None),
                false,
            )],
            vec![Arc::new(TimestampSecondArray::from(vec![i64::MIN]))],
        );

        let err = encoder.encode_batch(&out_of_range_batch).unwrap_err();
        assert_value_conversion_diagnostic(
            err,
            DiagnosticCode::TimestampOutOfRange,
            Some(0),
            Some((0, "created_at")),
        );

        let null_batch = record_batch(
            vec![Field::new(
                "created_at",
                DataType::Timestamp(TimeUnit::Second, None),
                true,
            )],
            vec![Arc::new(TimestampSecondArray::from(vec![None]))],
        );

        let err = encoder.encode_batch(&null_batch).unwrap_err();
        assert_value_conversion_diagnostic(
            err,
            DiagnosticCode::NullInNonNullableColumn,
            Some(0),
            Some((0, "created_at")),
        );
    }

    #[test]
    fn direct_encoder_encodes_timestamps_mixed_with_other_direct_columns() {
        let mappings = vec![
            mapping(0, "id", DataType::Int32, MssqlType::Int, false),
            mapping(
                1,
                "created_at",
                DataType::Timestamp(TimeUnit::Microsecond, None),
                MssqlType::DateTime2 { precision: 7 },
                true,
            ),
            mapping(
                2,
                "label",
                DataType::Utf8,
                MssqlType::NVarChar(MssqlTypeLength::Bounded(8)),
                true,
            ),
            mapping(3, "created_on", DataType::Date32, MssqlType::Date, false),
        ];
        let encoder = DirectEncoder::new(&mappings).unwrap();
        let batch = record_batch(
            vec![
                Field::new("id", DataType::Int32, false),
                Field::new(
                    "created_at",
                    DataType::Timestamp(TimeUnit::Microsecond, None),
                    true,
                ),
                Field::new("label", DataType::Utf8, true),
                Field::new("created_on", DataType::Date32, false),
            ],
            vec![
                Arc::new(Int32Array::from(vec![7, 8])) as ArrayRef,
                Arc::new(TimestampMicrosecondArray::from(vec![Some(1_234_567), None])),
                Arc::new(StringArray::from(vec![Some("ok"), None])),
                Arc::new(Date32Array::from(vec![0, 1])),
            ],
        );

        let payload = encoder.encode_batch(&batch).unwrap();

        assert_eq!(payload.row_token_offsets(), [0, 24]);
        assert_eq!(payload.row_count(), 2);
    }

    #[test]
    fn direct_encoder_validates_timestamp_timezone_metadata_for_nulls() {
        let mappings = vec![mapping(
            0,
            "created_at",
            DataType::Timestamp(TimeUnit::Second, Some("Foobar".into())),
            MssqlType::DateTime2 { precision: 7 },
            true,
        )];
        let encoder = DirectEncoder::new(&mappings).unwrap();
        let batch = record_batch(
            vec![Field::new(
                "created_at",
                DataType::Timestamp(TimeUnit::Second, Some("Foobar".into())),
                true,
            )],
            vec![Arc::new(
                TimestampSecondArray::from(vec![None]).with_timezone("Foobar"),
            )],
        );

        let err = encoder.encode_batch(&batch).unwrap_err();

        assert_value_conversion_diagnostic(
            err,
            DiagnosticCode::TimezoneUnsupported,
            Some(0),
            Some((0, "created_at")),
        );
    }

    #[test]
    fn direct_encoder_row_ranges_concatenate_to_full_payload() {
        let mappings = vec![
            mapping(0, "id", DataType::Int32, MssqlType::Int, false),
            mapping(
                1,
                "name",
                DataType::Utf8,
                MssqlType::NVarChar(MssqlTypeLength::Max),
                true,
            ),
            mapping(
                2,
                "bytes",
                DataType::Binary,
                MssqlType::VarBinary(MssqlTypeLength::Max),
                true,
            ),
        ];
        let encoder = DirectEncoder::new(&mappings).unwrap();
        let batch = record_batch(
            vec![
                Field::new("id", DataType::Int32, false),
                Field::new("name", DataType::Utf8, true),
                Field::new("bytes", DataType::Binary, true),
            ],
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3, 4])) as ArrayRef,
                Arc::new(StringArray::from(vec![
                    Some("alpha"),
                    Some("東京"),
                    None,
                    Some(""),
                ])),
                Arc::new(BinaryArray::from_iter(vec![
                    Some(&b"abc"[..]),
                    None,
                    Some(&b""[..]),
                    Some(&b"\x00\xff"[..]),
                ])),
            ],
        );

        let full = encoder.encode_batch(&batch).unwrap();
        let first = encoder.encode_batch_range(&batch, 0, 2).unwrap();
        let second = encoder.encode_batch_range(&batch, 2, 2).unwrap();
        let mut concatenated = Vec::new();
        concatenated.extend_from_slice(first.bytes());
        concatenated.extend_from_slice(second.bytes());

        assert_eq!(concatenated, full.bytes());
        assert_eq!(first.row_count(), 2);
        assert_eq!(second.row_count(), 2);
        assert_eq!(first.row_token_offsets()[0], 0);
        assert_eq!(second.row_token_offsets()[0], 0);
    }

    #[test]
    fn direct_encoder_measured_ranges_concatenate_to_full_payload() {
        let mappings = vec![
            mapping(0, "id", DataType::Int32, MssqlType::Int, false),
            mapping(
                1,
                "name",
                DataType::Utf8,
                MssqlType::NVarChar(MssqlTypeLength::Max),
                true,
            ),
        ];
        let encoder = DirectEncoder::new(&mappings).unwrap();
        let batch = record_batch(
            vec![
                Field::new("id", DataType::Int32, false),
                Field::new("name", DataType::Utf8, true),
            ],
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3])) as ArrayRef,
                Arc::new(StringArray::from(vec![Some("alpha"), None, Some("東京")])),
            ],
        );

        let measured = encoder.measure_batch(&batch).unwrap();
        let full = encoder.encode_batch(&batch).unwrap();
        let first = encoder
            .encode_measured_batch_range(&batch, &measured, 0, 1)
            .unwrap();
        let second = encoder
            .encode_measured_batch_range(&batch, &measured, 1, 2)
            .unwrap();
        let mut concatenated = Vec::new();
        concatenated.extend_from_slice(first.bytes());
        concatenated.extend_from_slice(second.bytes());

        assert_eq!(measured.row_count(), 3);
        assert_eq!(concatenated, full.bytes());
        assert_eq!(first.row_token_offsets()[0], 0);
        assert_eq!(second.row_token_offsets()[0], 0);
    }

    #[test]
    fn direct_encoder_encodes_large_variable_width_rows() {
        let mappings = vec![
            mapping(0, "id", DataType::Int32, MssqlType::Int, false),
            mapping(
                1,
                "large_text",
                DataType::LargeUtf8,
                MssqlType::NVarChar(MssqlTypeLength::Bounded(2)),
                true,
            ),
            mapping(
                2,
                "large_bytes",
                DataType::LargeBinary,
                MssqlType::VarBinary(MssqlTypeLength::Max),
                true,
            ),
        ];
        let encoder = DirectEncoder::new(&mappings).unwrap();
        let batch = record_batch(
            vec![
                Field::new("id", DataType::Int32, false),
                Field::new("large_text", DataType::LargeUtf8, true),
                Field::new("large_bytes", DataType::LargeBinary, true),
            ],
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3])) as ArrayRef,
                Arc::new(LargeStringArray::from(vec![Some("🙂"), Some(""), None])),
                Arc::new(LargeBinaryArray::from_iter(vec![
                    Some(&b"abc"[..]),
                    Some(&b""[..]),
                    None,
                ])),
            ],
        );

        let payload = encoder.encode_batch(&batch).unwrap();

        assert_eq!(payload.row_token_offsets(), [0, 30, 49]);
        assert_eq!(
            payload.bytes(),
            expected_rows([
                [
                    int32_cell(1),
                    bounded_nvarchar_cell("🙂"),
                    max_varbinary_cell(b"abc")
                ],
                [
                    int32_cell(2),
                    bounded_nvarchar_cell(""),
                    max_varbinary_cell(b"")
                ],
                [
                    int32_cell(3),
                    bounded_nvarchar_null_cell(),
                    max_varbinary_null_cell()
                ]
            ])
        );
    }

    #[test]
    fn direct_encoder_encodes_string_view_rows_like_utf8_rows() {
        let mappings = vec![mapping(
            0,
            "name",
            DataType::Utf8,
            MssqlType::NVarChar(MssqlTypeLength::Bounded(2)),
            true,
        )];
        let encoder = DirectEncoder::new(&mappings).unwrap();
        let utf8_batch = record_batch(
            vec![Field::new("name", DataType::Utf8, true)],
            vec![Arc::new(StringArray::from(vec![
                Some("ab"),
                Some("🙂"),
                None,
            ]))],
        );
        let string_view_batch = record_batch(
            vec![Field::new("name", DataType::Utf8View, true)],
            vec![Arc::new(StringViewArray::from(vec![
                Some("ab"),
                Some("🙂"),
                None,
            ]))],
        );

        let expected = encoder.encode_batch(&utf8_batch).unwrap();
        let actual = encoder.encode_batch(&string_view_batch).unwrap();

        assert_eq!(actual.row_token_offsets(), expected.row_token_offsets());
        assert_eq!(actual.bytes(), expected.bytes());
        assert_eq!(
            actual.bytes(),
            expected_rows([
                [bounded_nvarchar_cell("ab")],
                [bounded_nvarchar_cell("🙂")],
                [bounded_nvarchar_null_cell()]
            ])
        );
    }

    #[test]
    fn direct_encoder_encodes_binary_view_rows_like_binary_rows() {
        let mappings = vec![mapping(
            0,
            "payload",
            DataType::Binary,
            MssqlType::VarBinary(MssqlTypeLength::Max),
            true,
        )];
        let encoder = DirectEncoder::new(&mappings).unwrap();
        let binary_batch = record_batch(
            vec![Field::new("payload", DataType::Binary, true)],
            vec![Arc::new(BinaryArray::from_iter(vec![
                Some(&b"abc"[..]),
                Some(&b""[..]),
                None,
            ]))],
        );
        let binary_view_batch = record_batch(
            vec![Field::new("payload", DataType::BinaryView, true)],
            vec![Arc::new(BinaryViewArray::from(vec![
                Some(&b"abc"[..]),
                Some(&b""[..]),
                None,
            ]))],
        );

        let expected = encoder.encode_batch(&binary_batch).unwrap();
        let actual = encoder.encode_batch(&binary_view_batch).unwrap();

        assert_eq!(actual.row_token_offsets(), expected.row_token_offsets());
        assert_eq!(actual.bytes(), expected.bytes());
        assert_eq!(
            actual.bytes(),
            expected_rows([
                [max_varbinary_cell(b"abc")],
                [max_varbinary_cell(b"")],
                [max_varbinary_null_cell()]
            ])
        );
    }

    #[test]
    fn direct_encoder_encodes_string_family_runtime_matrix() {
        for planned in string_family_types() {
            let mappings = vec![mapping(
                0,
                "name",
                planned.clone(),
                MssqlType::NVarChar(MssqlTypeLength::Bounded(8)),
                true,
            )];
            let encoder = DirectEncoder::new(&mappings).unwrap();

            for runtime in string_family_types() {
                let batch = record_batch(
                    vec![Field::new("name", runtime.clone(), true)],
                    vec![string_family_array(
                        runtime,
                        vec![Some("alpha"), Some("🙂"), None],
                    )],
                );

                let measured = encoder.measure_batch(&batch).unwrap();
                let full = encoder.encode_batch(&batch).unwrap();
                let range = encoder
                    .encode_measured_batch_range(&batch, &measured, 0, 3)
                    .unwrap();

                assert_eq!(measured.row_count(), 3);
                assert_eq!(range.bytes(), full.bytes());
                assert_eq!(
                    full.bytes(),
                    expected_rows([
                        [bounded_nvarchar_cell("alpha")],
                        [bounded_nvarchar_cell("🙂")],
                        [bounded_nvarchar_null_cell()]
                    ])
                );
            }
        }
    }

    #[test]
    fn direct_encoder_encodes_binary_family_runtime_matrix() {
        for planned in binary_family_types() {
            let mappings = vec![mapping(
                0,
                "payload",
                planned.clone(),
                MssqlType::VarBinary(MssqlTypeLength::Max),
                true,
            )];
            let encoder = DirectEncoder::new(&mappings).unwrap();

            for runtime in binary_family_types() {
                let batch = record_batch(
                    vec![Field::new("payload", runtime.clone(), true)],
                    vec![binary_family_array(
                        runtime,
                        vec![Some(&b"alpha"[..]), Some(&b""[..]), None],
                    )],
                );

                let measured = encoder.measure_batch(&batch).unwrap();
                let full = encoder.encode_batch(&batch).unwrap();
                let range = encoder
                    .encode_measured_batch_range(&batch, &measured, 0, 3)
                    .unwrap();

                assert_eq!(measured.row_count(), 3);
                assert_eq!(range.bytes(), full.bytes());
                assert_eq!(
                    full.bytes(),
                    expected_rows([
                        [max_varbinary_cell(b"alpha")],
                        [max_varbinary_cell(b"")],
                        [max_varbinary_null_cell()]
                    ])
                );
            }
        }
    }

    #[test]
    fn direct_encoder_rejects_cross_family_runtime_representations() {
        let cases: Vec<(SchemaMapping, ArrayRef)> = vec![
            (
                mapping(
                    0,
                    "name",
                    DataType::Utf8View,
                    MssqlType::NVarChar(MssqlTypeLength::Max),
                    true,
                ),
                Arc::new(BinaryViewArray::from(vec![Some(&b"bytes"[..])])) as ArrayRef,
            ),
            (
                mapping(
                    0,
                    "payload",
                    DataType::BinaryView,
                    MssqlType::VarBinary(MssqlTypeLength::Max),
                    true,
                ),
                Arc::new(StringViewArray::from(vec![Some("text")])) as ArrayRef,
            ),
        ];

        for (mapping, array) in cases {
            let field_name = mapping.arrow().name().to_owned();
            let field = Field::new(
                mapping.arrow().name(),
                mapping.arrow().data_type().clone(),
                mapping.arrow().nullable(),
            );
            let mappings = vec![mapping];
            let encoder = DirectEncoder::new(&mappings).unwrap();
            let batch = unsafe_record_batch(vec![field], vec![array], 1);

            let err = encoder
                .encode_batch(&batch)
                .expect_err("direct encoder must reject cross-family physical arrays");

            assert_value_conversion_diagnostic(
                err,
                DiagnosticCode::SchemaMismatch,
                None,
                Some((0, field_name.as_str())),
            );
        }
    }

    #[test]
    fn direct_encoder_rejects_fixed_size_binary_variable_binary_mismatches() {
        let planned_varbinary = vec![mapping(
            0,
            "payload",
            DataType::BinaryView,
            MssqlType::VarBinary(MssqlTypeLength::Max),
            true,
        )];
        let encoder = DirectEncoder::new(&planned_varbinary).unwrap();
        let batch = unsafe_record_batch(
            vec![Field::new("payload", DataType::BinaryView, true)],
            vec![Arc::new(
                FixedSizeBinaryArray::try_from_sparse_iter_with_size(
                    [Some(&b"abc"[..])].into_iter(),
                    3,
                )
                .unwrap(),
            )],
            1,
        );

        let err = encoder
            .encode_batch(&batch)
            .expect_err("varbinary mapping must not accept FixedSizeBinary physical arrays");

        assert_value_conversion_diagnostic(
            err,
            DiagnosticCode::SchemaMismatch,
            None,
            Some((0, "payload")),
        );

        let planned_fixed = vec![mapping(
            0,
            "payload",
            DataType::FixedSizeBinary(3),
            MssqlType::Binary(3),
            true,
        )];
        let encoder = DirectEncoder::new(&planned_fixed).unwrap();
        let batch = unsafe_record_batch(
            vec![Field::new("payload", DataType::FixedSizeBinary(3), true)],
            vec![Arc::new(BinaryViewArray::from(vec![Some(&b"abc"[..])]))],
            1,
        );

        let err = encoder
            .encode_batch(&batch)
            .expect_err("fixed binary mapping must not accept variable binary physical arrays");

        assert_value_conversion_diagnostic(
            err,
            DiagnosticCode::SchemaMismatch,
            None,
            Some((0, "payload")),
        );
    }

    #[test]
    fn direct_encoder_large_variable_width_measured_ranges_match_full_payload() {
        let mappings = vec![
            mapping(0, "id", DataType::Int32, MssqlType::Int, false),
            mapping(
                1,
                "large_text",
                DataType::LargeUtf8,
                MssqlType::NVarChar(MssqlTypeLength::Max),
                true,
            ),
            mapping(
                2,
                "large_bytes",
                DataType::LargeBinary,
                MssqlType::VarBinary(MssqlTypeLength::Bounded(3)),
                true,
            ),
        ];
        let encoder = DirectEncoder::new(&mappings).unwrap();
        let batch = record_batch(
            vec![
                Field::new("id", DataType::Int32, false),
                Field::new("large_text", DataType::LargeUtf8, true),
                Field::new("large_bytes", DataType::LargeBinary, true),
            ],
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3, 4])) as ArrayRef,
                Arc::new(LargeStringArray::from(vec![
                    Some("alpha"),
                    Some("東京"),
                    None,
                    Some(""),
                ])),
                Arc::new(LargeBinaryArray::from_iter(vec![
                    Some(&b"abc"[..]),
                    None,
                    Some(&b""[..]),
                    Some(&b"\x00\xff"[..]),
                ])),
            ],
        );

        let measured = encoder.measure_batch(&batch).unwrap();
        let full = encoder.encode_batch(&batch).unwrap();
        let first = encoder
            .encode_measured_batch_range(&batch, &measured, 0, 2)
            .unwrap();
        let second = encoder
            .encode_measured_batch_range(&batch, &measured, 2, 2)
            .unwrap();
        let mut concatenated = Vec::new();
        concatenated.extend_from_slice(first.bytes());
        concatenated.extend_from_slice(second.bytes());

        assert_eq!(measured.row_count(), 4);
        assert_eq!(concatenated, full.bytes());
        assert_eq!(first.row_token_offsets()[0], 0);
        assert_eq!(second.row_token_offsets()[0], 0);
    }

    #[test]
    fn direct_encoder_encodes_fixed_size_binary_rows() {
        let mappings = vec![
            mapping(0, "id", DataType::Int32, MssqlType::Int, false),
            mapping(
                1,
                "digest",
                DataType::FixedSizeBinary(3),
                MssqlType::Binary(3),
                true,
            ),
        ];
        let encoder = DirectEncoder::new(&mappings).unwrap();
        let batch = record_batch(
            vec![
                Field::new("id", DataType::Int32, false),
                Field::new("digest", DataType::FixedSizeBinary(3), true),
            ],
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3])) as ArrayRef,
                Arc::new(
                    FixedSizeBinaryArray::try_from_sparse_iter_with_size(
                        [Some(&b"\x00\xff\x7f"[..]), None, Some(&b"abc"[..])].into_iter(),
                        3,
                    )
                    .unwrap(),
                ),
            ],
        );

        let payload = encoder.encode_batch(&batch).unwrap();

        assert_eq!(payload.row_token_offsets(), [0, 10, 17]);
        assert_eq!(
            payload.bytes(),
            expected_rows([
                [int32_cell(1), fixed_binary_cell(b"\x00\xff\x7f")],
                [int32_cell(2), fixed_binary_null_cell()],
                [int32_cell(3), fixed_binary_cell(b"abc")],
            ])
        );
    }

    #[test]
    fn direct_encoder_fixed_size_binary_measured_ranges_match_full_payload() {
        let mappings = vec![
            mapping(0, "id", DataType::Int32, MssqlType::Int, false),
            mapping(
                1,
                "digest",
                DataType::FixedSizeBinary(3),
                MssqlType::Binary(3),
                true,
            ),
        ];
        let encoder = DirectEncoder::new(&mappings).unwrap();
        let batch = record_batch(
            vec![
                Field::new("id", DataType::Int32, false),
                Field::new("digest", DataType::FixedSizeBinary(3), true),
            ],
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3])) as ArrayRef,
                Arc::new(
                    FixedSizeBinaryArray::try_from_sparse_iter_with_size(
                        [Some(&b"abc"[..]), None, Some(&b"xyz"[..])].into_iter(),
                        3,
                    )
                    .unwrap(),
                ),
            ],
        );

        let measured = encoder.measure_batch(&batch).unwrap();
        let full = encoder.encode_batch(&batch).unwrap();
        let first = encoder
            .encode_measured_batch_range(&batch, &measured, 0, 1)
            .unwrap();
        let second = encoder
            .encode_measured_batch_range(&batch, &measured, 1, 2)
            .unwrap();
        let mut concatenated = Vec::new();
        concatenated.extend_from_slice(first.bytes());
        concatenated.extend_from_slice(second.bytes());

        assert_eq!(measured.row_count(), 3);
        assert_eq!(concatenated, full.bytes());
        assert_eq!(first.row_token_offsets()[0], 0);
        assert_eq!(second.row_token_offsets()[0], 0);
    }

    #[test]
    fn direct_encoder_encodes_binary_family_runtime_representations() {
        let mappings = vec![
            mapping(
                0,
                "large_text",
                DataType::LargeUtf8,
                MssqlType::NVarChar(MssqlTypeLength::Bounded(8)),
                true,
            ),
            mapping(
                1,
                "large_bytes",
                DataType::LargeBinary,
                MssqlType::VarBinary(MssqlTypeLength::Max),
                true,
            ),
        ];
        let encoder = DirectEncoder::new(&mappings).unwrap();
        let batch = record_batch(
            vec![
                Field::new("large_text", DataType::LargeUtf8, true),
                Field::new("large_bytes", DataType::Binary, true),
            ],
            vec![
                Arc::new(LargeStringArray::from(vec![Some("large")])) as ArrayRef,
                Arc::new(BinaryArray::from_iter(vec![Some(&b"not-large"[..])])),
            ],
        );

        let payload = encoder.encode_batch(&batch).unwrap();

        assert_eq!(payload.row_token_offsets(), [0]);
        assert_eq!(
            payload.bytes(),
            expected_rows([[
                bounded_nvarchar_cell("large"),
                max_varbinary_cell(b"not-large")
            ]])
        );
    }

    #[test]
    fn direct_encoder_rejects_fixed_size_binary_null_in_non_nullable_column() {
        let mappings = vec![mapping(
            0,
            "digest",
            DataType::FixedSizeBinary(3),
            MssqlType::Binary(3),
            false,
        )];
        let encoder = DirectEncoder::new(&mappings).unwrap();
        let batch = record_batch(
            vec![Field::new("digest", DataType::FixedSizeBinary(3), true)],
            vec![Arc::new(
                FixedSizeBinaryArray::try_from_sparse_iter_with_size(
                    [Some(&b"abc"[..]), None].into_iter(),
                    3,
                )
                .unwrap(),
            )],
        );
        let bound = BoundDirectBatch::new(&encoder, &batch).unwrap();

        let err = try_encode_fixed_width_rows(&bound)
            .expect_err("fixed-size binary null in non-nullable column must fail");

        assert_value_conversion_diagnostic(
            err,
            DiagnosticCode::NullInNonNullableColumn,
            Some(1),
            Some((0, "digest")),
        );
    }

    #[test]
    fn direct_encoder_row_range_rejects_out_of_bounds_range() {
        let mappings = vec![mapping(0, "id", DataType::Int32, MssqlType::Int, false)];
        let encoder = DirectEncoder::new(&mappings).unwrap();
        let batch = record_batch(
            vec![Field::new("id", DataType::Int32, false)],
            vec![Arc::new(Int32Array::from(vec![1, 2]))],
        );

        let err = encoder
            .encode_batch_range(&batch, 1, 2)
            .expect_err("range past batch end must fail");

        assert_direct_encoding_diagnostic(err, DiagnosticCode::DirectEncodingInvalidPayload);
    }

    #[test]
    fn direct_encoder_encodes_nullable_primitive_cells() {
        let mappings = vec![
            mapping(0, "is_active", DataType::Boolean, MssqlType::Bit, true),
            mapping(1, "quantity", DataType::Int32, MssqlType::Int, true),
        ];
        let encoder = DirectEncoder::new(&mappings).unwrap();
        let batch = record_batch(
            vec![
                Field::new("is_active", DataType::Boolean, true),
                Field::new("quantity", DataType::Int32, true),
            ],
            vec![
                Arc::new(BooleanArray::from(vec![Some(true), None])) as ArrayRef,
                Arc::new(Int32Array::from(vec![None, Some(7)])),
            ],
        );

        let payload = encoder.encode_batch(&batch).unwrap();

        assert_eq!(payload.row_token_offsets(), [0, 4]);
        assert_eq!(
            payload.bytes(),
            [
                payload::TDS_ROW_TOKEN,
                1,
                1,
                0,
                payload::TDS_ROW_TOKEN,
                0,
                4,
                7,
                0,
                0,
                0
            ]
        );
    }

    #[test]
    fn direct_encoder_encodes_float16_real_cells() {
        let mappings = vec![mapping(
            0,
            "ratio",
            DataType::Float16,
            MssqlType::Real,
            true,
        )];
        let encoder = DirectEncoder::new(&mappings).unwrap();
        let batch = record_batch(
            vec![Field::new("ratio", DataType::Float16, true)],
            vec![Arc::new(Float16Array::from(vec![
                Some(F16::from_f32(1.5)),
                None,
                Some(F16::from_f32(-2.25)),
            ]))],
        );

        let payload = encoder.encode_batch(&batch).unwrap();

        assert_eq!(payload.row_token_offsets(), [0, 6, 8]);
        assert_eq!(
            payload.bytes(),
            [
                payload::TDS_ROW_TOKEN,
                4,
                0,
                0,
                0xC0,
                0x3F,
                payload::TDS_ROW_TOKEN,
                0,
                payload::TDS_ROW_TOKEN,
                4,
                0,
                0,
                0x10,
                0xC0,
            ]
        );

        let bound = BoundDirectBatch::new(&encoder, &batch).unwrap();
        let fast_payload = try_encode_fixed_width_rows(&bound)
            .unwrap()
            .expect("Float16 real fixed-width fast path should be active");

        assert_eq!(fast_payload.bytes(), payload.bytes());
        assert_eq!(
            fast_payload.row_token_offsets(),
            payload.row_token_offsets()
        );
    }

    #[test]
    fn direct_encoder_fast_path_encodes_mixed_nullable_and_non_nullable_rows() {
        let mappings = vec![
            mapping(0, "quantity", DataType::Int32, MssqlType::Int, true),
            mapping(1, "total", DataType::Int64, MssqlType::BigInt, false),
            mapping(2, "active", DataType::Boolean, MssqlType::Bit, true),
        ];
        let encoder = DirectEncoder::new(&mappings).unwrap();
        let batch = record_batch(
            vec![
                Field::new("quantity", DataType::Int32, true),
                Field::new("total", DataType::Int64, false),
                Field::new("active", DataType::Boolean, true),
            ],
            vec![
                Arc::new(Int32Array::from(vec![Some(10), None, Some(-1)])) as ArrayRef,
                Arc::new(Int64Array::from(vec![100, 200, 300])),
                Arc::new(BooleanArray::from(vec![None, Some(true), Some(false)])),
            ],
        );

        let payload = encoder.encode_batch(&batch).unwrap();

        assert_eq!(payload.row_token_offsets(), [0, 15, 27]);
        assert_eq!(
            payload.bytes(),
            [
                payload::TDS_ROW_TOKEN,
                4,
                10,
                0,
                0,
                0,
                100,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                payload::TDS_ROW_TOKEN,
                0,
                200,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                1,
                1,
                payload::TDS_ROW_TOKEN,
                4,
                0xFF,
                0xFF,
                0xFF,
                0xFF,
                0x2C,
                0x01,
                0,
                0,
                0,
                0,
                0,
                0,
                1,
                0
            ]
        );
    }

    #[test]
    fn direct_encoder_fixed_width_fast_path_is_active_for_mixed_nullability() {
        let mappings = vec![
            mapping(0, "quantity", DataType::Int32, MssqlType::Int, true),
            mapping(1, "total", DataType::Int64, MssqlType::BigInt, false),
            mapping(
                2,
                "ratio",
                DataType::Float64,
                MssqlType::Float { precision: 53 },
                true,
            ),
        ];
        let encoder = DirectEncoder::new(&mappings).unwrap();
        let batch = record_batch(
            vec![
                Field::new("quantity", DataType::Int32, true),
                Field::new("total", DataType::Int64, false),
                Field::new("ratio", DataType::Float64, true),
            ],
            vec![
                Arc::new(Int32Array::from(vec![Some(10), None])) as ArrayRef,
                Arc::new(Int64Array::from(vec![100, 200])),
                Arc::new(Float64Array::from(vec![None, Some(1.5)])),
            ],
        );

        let bound = BoundDirectBatch::new(&encoder, &batch).unwrap();
        let payload = try_encode_fixed_width_rows(&bound)
            .unwrap()
            .expect("fixed-width primitive fast path should be active");

        assert_eq!(payload.row_token_offsets(), [0, 15]);
        assert_eq!(payload.row_count(), 2);
    }

    #[test]
    fn direct_encoder_fixed_width_fast_path_is_active_for_date_columns() {
        let mappings = vec![
            mapping(0, "id", DataType::Int32, MssqlType::Int, false),
            mapping(1, "created_on", DataType::Date32, MssqlType::Date, true),
            mapping(
                2,
                "created_at",
                DataType::Date64,
                MssqlType::DateTime2 { precision: 3 },
                true,
            ),
        ];
        let encoder = DirectEncoder::new(&mappings).unwrap();
        let batch = record_batch(
            vec![
                Field::new("id", DataType::Int32, false),
                Field::new("created_on", DataType::Date32, true),
                Field::new("created_at", DataType::Date64, true),
            ],
            vec![
                Arc::new(Int32Array::from(vec![1, 2])) as ArrayRef,
                Arc::new(Date32Array::from(vec![Some(0), None])),
                Arc::new(Date64Array::from(vec![Some(86_400_123), Some(0)])),
            ],
        );

        let bound = BoundDirectBatch::new(&encoder, &batch).unwrap();
        let payload = try_encode_fixed_width_rows(&bound)
            .unwrap()
            .expect("fixed-width date-family fast path should be active");

        assert_eq!(payload.row_token_offsets(), [0, 17]);
        assert_eq!(
            payload.bytes(),
            encoder.encode_batch(&batch).unwrap().bytes()
        );
    }

    #[test]
    fn direct_encoder_fixed_width_fast_path_is_active_for_fixed_size_binary_columns() {
        let mappings = vec![
            mapping(0, "id", DataType::Int32, MssqlType::Int, false),
            mapping(
                1,
                "digest",
                DataType::FixedSizeBinary(3),
                MssqlType::Binary(3),
                true,
            ),
        ];
        let encoder = DirectEncoder::new(&mappings).unwrap();
        let batch = record_batch(
            vec![
                Field::new("id", DataType::Int32, false),
                Field::new("digest", DataType::FixedSizeBinary(3), true),
            ],
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3])) as ArrayRef,
                Arc::new(
                    FixedSizeBinaryArray::try_from_sparse_iter_with_size(
                        [Some(&b"abc"[..]), None, Some(&b"\x00\xff\x7f"[..])].into_iter(),
                        3,
                    )
                    .unwrap(),
                ),
            ],
        );

        let bound = BoundDirectBatch::new(&encoder, &batch).unwrap();
        let payload = try_encode_fixed_width_rows(&bound)
            .unwrap()
            .expect("fixed-width fixed-size binary fast path should be active");

        assert_eq!(payload.row_token_offsets(), [0, 10, 17]);
        assert_eq!(
            payload.bytes(),
            encoder.encode_batch(&batch).unwrap().bytes()
        );
    }

    #[test]
    fn direct_encoder_fixed_width_fast_path_is_active_for_fixed_size_binary_measured_ranges() {
        let mappings = vec![
            mapping(0, "id", DataType::Int32, MssqlType::Int, false),
            mapping(
                1,
                "digest",
                DataType::FixedSizeBinary(3),
                MssqlType::Binary(3),
                true,
            ),
        ];
        let encoder = DirectEncoder::new(&mappings).unwrap();
        let batch = record_batch(
            vec![
                Field::new("id", DataType::Int32, false),
                Field::new("digest", DataType::FixedSizeBinary(3), true),
            ],
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3])) as ArrayRef,
                Arc::new(
                    FixedSizeBinaryArray::try_from_sparse_iter_with_size(
                        [Some(&b"abc"[..]), None, Some(&b"\x00\xff\x7f"[..])].into_iter(),
                        3,
                    )
                    .unwrap(),
                ),
            ],
        );

        let measured = encoder.measure_batch(&batch).unwrap();

        reset_fixed_width_measure_call_count();
        let range = encoder
            .encode_measured_batch_range(&batch, &measured, 1, 2)
            .unwrap();
        assert_eq!(fixed_width_measure_call_count(), 0);
        assert_eq!(range.row_token_offsets(), [0, 7]);
        assert_eq!(
            range.bytes(),
            expected_rows([
                [int32_cell(2), fixed_binary_null_cell()],
                [int32_cell(3), fixed_binary_cell(b"\x00\xff\x7f")]
            ])
        );
    }

    #[test]
    fn direct_encoder_fixed_width_fast_path_is_active_for_timestamp_datetime2_columns() {
        let mappings = vec![
            mapping(0, "id", DataType::Int32, MssqlType::Int, false),
            mapping(
                1,
                "created_at",
                DataType::Timestamp(TimeUnit::Second, Some("UTC".into())),
                MssqlType::DateTime2 { precision: 7 },
                true,
            ),
            mapping(
                2,
                "precise_at",
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                MssqlType::DateTime2 { precision: 7 },
                false,
            ),
        ];
        let options = PlanOptions {
            nanosecond_policy: NanosecondPolicy::RoundTo100ns,
            ..PlanOptions::default()
        };
        let encoder = DirectEncoder::new_with_options(&mappings, options).unwrap();
        let batch = record_batch(
            vec![
                Field::new("id", DataType::Int32, false),
                Field::new(
                    "created_at",
                    DataType::Timestamp(TimeUnit::Second, Some("UTC".into())),
                    true,
                ),
                Field::new(
                    "precise_at",
                    DataType::Timestamp(TimeUnit::Nanosecond, None),
                    false,
                ),
            ],
            vec![
                Arc::new(Int32Array::from(vec![1, 2])) as ArrayRef,
                Arc::new(TimestampSecondArray::from(vec![Some(0), None]).with_timezone("UTC")),
                Arc::new(TimestampNanosecondArray::from(vec![150, -50])),
            ],
        );

        let bound = BoundDirectBatch::new(&encoder, &batch).unwrap();
        let payload = try_encode_fixed_width_rows(&bound)
            .unwrap()
            .expect("fixed-width timestamp datetime2 fast path should be active");

        assert_eq!(payload.row_token_offsets(), [0, 23]);
        assert_eq!(
            payload.bytes(),
            expected_rows([
                [
                    int32_cell(1),
                    datetime2_7_cell(719_162, 0),
                    datetime2_7_cell(719_162, 2),
                ],
                [int32_cell(2), null_cell(), datetime2_7_cell(719_162, 0)],
            ])
        );
    }

    #[test]
    fn direct_encoder_fixed_width_fast_path_encodes_timestamp_datetime2_precision() {
        let mappings = vec![mapping(
            0,
            "created_at",
            DataType::Timestamp(TimeUnit::Microsecond, None),
            MssqlType::DateTime2 { precision: 3 },
            true,
        )];
        let encoder = DirectEncoder::new(&mappings).unwrap();
        let batch = record_batch(
            vec![Field::new(
                "created_at",
                DataType::Timestamp(TimeUnit::Microsecond, None),
                true,
            )],
            vec![Arc::new(TimestampMicrosecondArray::from(vec![
                Some(1_234_567),
                Some(86_399_999_500),
                None,
            ]))],
        );

        let bound = BoundDirectBatch::new(&encoder, &batch).unwrap();
        let payload = try_encode_fixed_width_rows(&bound)
            .unwrap()
            .expect("fixed-width datetime2 precision fast path should be active");

        assert_eq!(payload.row_token_offsets(), [0, 9, 18]);
        assert_eq!(
            payload.bytes(),
            expected_rows([
                [datetime2_cell(3, 719_162, 1_235)],
                [datetime2_cell(3, 719_163, 0)],
                [null_cell()],
            ])
        );
    }

    #[test]
    fn direct_encoder_fixed_width_fast_path_is_active_for_timestamp_datetime_columns() {
        let mappings = vec![
            mapping(0, "id", DataType::Int32, MssqlType::Int, false),
            mapping(
                1,
                "created_at",
                DataType::Timestamp(TimeUnit::Microsecond, None),
                MssqlType::DateTime,
                true,
            ),
            mapping(
                2,
                "precise_at",
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                MssqlType::DateTime,
                false,
            ),
        ];
        let encoder = DirectEncoder::new(&mappings).unwrap();
        let batch = record_batch(
            vec![
                Field::new("id", DataType::Int32, false),
                Field::new(
                    "created_at",
                    DataType::Timestamp(TimeUnit::Microsecond, None),
                    true,
                ),
                Field::new(
                    "precise_at",
                    DataType::Timestamp(TimeUnit::Nanosecond, None),
                    false,
                ),
            ],
            vec![
                Arc::new(Int32Array::from(vec![1, 2])) as ArrayRef,
                Arc::new(TimestampMicrosecondArray::from(vec![Some(1_700), None])),
                Arc::new(TimestampNanosecondArray::from(vec![1_700_000, 0])),
            ],
        );

        let bound = BoundDirectBatch::new(&encoder, &batch).unwrap();
        let payload = try_encode_fixed_width_rows(&bound)
            .unwrap()
            .expect("fixed-width timestamp datetime fast path should be active");

        assert_eq!(payload.row_token_offsets(), [0, 23]);
        assert_eq!(
            payload.bytes(),
            expected_rows([
                [
                    int32_cell(1),
                    datetime_cell(25_567, 1),
                    datetime_cell(25_567, 1),
                ],
                [int32_cell(2), null_cell(), datetime_cell(25_567, 0)],
            ])
        );
    }

    #[test]
    fn direct_encoder_fixed_width_fast_path_is_active_for_time_columns() {
        let mappings = vec![
            mapping(0, "id", DataType::Int32, MssqlType::Int, false),
            mapping(
                1,
                "time_s",
                DataType::Time32(TimeUnit::Second),
                MssqlType::Time(MssqlTimePrecision::ZERO),
                true,
            ),
            mapping(
                2,
                "time_ms",
                DataType::Time32(TimeUnit::Millisecond),
                MssqlType::Time(MssqlTimePrecision::THREE),
                true,
            ),
            mapping(
                3,
                "time_us",
                DataType::Time64(TimeUnit::Microsecond),
                MssqlType::Time(MssqlTimePrecision::SIX),
                false,
            ),
            mapping(
                4,
                "time_ns",
                DataType::Time64(TimeUnit::Nanosecond),
                MssqlType::Time(MssqlTimePrecision::SEVEN),
                false,
            ),
        ];
        let options = PlanOptions {
            nanosecond_policy: NanosecondPolicy::RoundTo100ns,
            ..PlanOptions::default()
        };
        let encoder = DirectEncoder::new_with_options(&mappings, options).unwrap();
        let batch = record_batch(
            vec![
                Field::new("id", DataType::Int32, false),
                Field::new("time_s", DataType::Time32(TimeUnit::Second), true),
                Field::new("time_ms", DataType::Time32(TimeUnit::Millisecond), true),
                Field::new("time_us", DataType::Time64(TimeUnit::Microsecond), false),
                Field::new("time_ns", DataType::Time64(TimeUnit::Nanosecond), false),
            ],
            vec![
                Arc::new(Int32Array::from(vec![1, 2])) as ArrayRef,
                Arc::new(Time32SecondArray::from(vec![Some(86_399), None])),
                Arc::new(Time32MillisecondArray::from(vec![Some(12_345), None])),
                Arc::new(Time64MicrosecondArray::from(vec![12_345_678, 0])),
                Arc::new(Time64NanosecondArray::from(vec![149, 150])),
            ],
        );

        let bound = BoundDirectBatch::new(&encoder, &batch).unwrap();
        let payload = try_encode_fixed_width_rows(&bound)
            .unwrap()
            .expect("fixed-width time fast path should be active");

        assert_eq!(payload.row_token_offsets(), [0, 26]);
        assert_eq!(
            payload.bytes(),
            expected_rows([
                [
                    int32_cell(1),
                    time_cell(0, 86_399),
                    time_cell(3, 12_345),
                    time_cell(6, 12_345_678),
                    time_cell(7, 1),
                ],
                [
                    int32_cell(2),
                    null_cell(),
                    null_cell(),
                    time_cell(6, 0),
                    time_cell(7, 2),
                ],
            ])
        );
    }

    #[cfg(feature = "bench-profile")]
    #[test]
    fn direct_encoder_timestamp_datetime2_fast_path_matches_general_path() {
        let mappings = vec![
            mapping(0, "id", DataType::Int32, MssqlType::Int, false),
            mapping(
                1,
                "created_at",
                DataType::Timestamp(TimeUnit::Second, Some("America/New_York".into())),
                MssqlType::DateTime2 { precision: 7 },
                true,
            ),
            mapping(
                2,
                "precise_at",
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                MssqlType::DateTime2 { precision: 7 },
                true,
            ),
        ];
        let options = PlanOptions {
            nanosecond_policy: NanosecondPolicy::RoundTo100ns,
            ..PlanOptions::default()
        };
        let encoder = DirectEncoder::new_with_options(&mappings, options).unwrap();
        let batch = record_batch(
            vec![
                Field::new("id", DataType::Int32, false),
                Field::new(
                    "created_at",
                    DataType::Timestamp(TimeUnit::Second, Some("America/New_York".into())),
                    true,
                ),
                Field::new(
                    "precise_at",
                    DataType::Timestamp(TimeUnit::Nanosecond, None),
                    true,
                ),
            ],
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3])) as ArrayRef,
                Arc::new(
                    TimestampSecondArray::from(vec![Some(0), Some(1_750_593_600), None])
                        .with_timezone("America/New_York"),
                ),
                Arc::new(TimestampNanosecondArray::from(vec![
                    Some(150),
                    Some(-50),
                    None,
                ])),
            ],
        );

        let fast_path = encoder.encode_batch(&batch).unwrap();
        let _disable_fast_path =
            crate::write::profile::disable_direct_fixed_width_fast_path_for_scope();
        let general_path = encoder.encode_batch(&batch).unwrap();

        assert_eq!(
            fast_path.row_token_offsets(),
            general_path.row_token_offsets()
        );
        assert_eq!(fast_path.bytes(), general_path.bytes());
    }

    #[cfg(feature = "bench-profile")]
    #[test]
    fn direct_encoder_time_fast_path_matches_general_path() {
        let mappings = vec![
            mapping(0, "id", DataType::Int32, MssqlType::Int, false),
            mapping(
                1,
                "time_s",
                DataType::Time32(TimeUnit::Second),
                MssqlType::Time(MssqlTimePrecision::ZERO),
                true,
            ),
            mapping(
                2,
                "time_ns",
                DataType::Time64(TimeUnit::Nanosecond),
                MssqlType::Time(MssqlTimePrecision::SEVEN),
                true,
            ),
        ];
        let options = PlanOptions {
            nanosecond_policy: NanosecondPolicy::RoundTo100ns,
            ..PlanOptions::default()
        };
        let encoder = DirectEncoder::new_with_options(&mappings, options).unwrap();
        let batch = record_batch(
            vec![
                Field::new("id", DataType::Int32, false),
                Field::new("time_s", DataType::Time32(TimeUnit::Second), true),
                Field::new("time_ns", DataType::Time64(TimeUnit::Nanosecond), true),
            ],
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3])) as ArrayRef,
                Arc::new(Time32SecondArray::from(vec![Some(0), Some(86_399), None])),
                Arc::new(Time64NanosecondArray::from(vec![
                    Some(149),
                    Some(150),
                    None,
                ])),
            ],
        );

        let fast_path = encoder.encode_batch(&batch).unwrap();
        let _disable_fast_path =
            crate::write::profile::disable_direct_fixed_width_fast_path_for_scope();
        let general_path = encoder.encode_batch(&batch).unwrap();

        assert_eq!(
            fast_path.row_token_offsets(),
            general_path.row_token_offsets()
        );
        assert_eq!(fast_path.bytes(), general_path.bytes());
    }

    #[test]
    fn direct_encoder_fixed_width_fast_path_is_active_for_datetimeoffset_columns() {
        let mappings = vec![
            mapping(0, "id", DataType::Int32, MssqlType::Int, false),
            mapping(
                1,
                "dto_s",
                DataType::Timestamp(TimeUnit::Second, Some("+02:30".into())),
                MssqlType::DateTimeOffset { precision: 7 },
                true,
            ),
            mapping(
                2,
                "dto_ms",
                DataType::Timestamp(TimeUnit::Millisecond, Some("-07".into())),
                MssqlType::DateTimeOffset { precision: 7 },
                true,
            ),
            mapping(
                3,
                "dto_us",
                DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
                MssqlType::DateTimeOffset { precision: 7 },
                false,
            ),
            mapping(
                4,
                "dto_ns",
                DataType::Timestamp(TimeUnit::Nanosecond, Some("+00:00".into())),
                MssqlType::DateTimeOffset { precision: 7 },
                false,
            ),
        ];
        let options = PlanOptions {
            nanosecond_policy: NanosecondPolicy::RoundTo100ns,
            ..PlanOptions::default()
        };
        let encoder = DirectEncoder::new_with_options(&mappings, options).unwrap();
        let batch = record_batch(
            vec![
                Field::new("id", DataType::Int32, false),
                Field::new(
                    "dto_s",
                    DataType::Timestamp(TimeUnit::Second, Some("+02:30".into())),
                    true,
                ),
                Field::new(
                    "dto_ms",
                    DataType::Timestamp(TimeUnit::Millisecond, Some("-07".into())),
                    true,
                ),
                Field::new(
                    "dto_us",
                    DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
                    false,
                ),
                Field::new(
                    "dto_ns",
                    DataType::Timestamp(TimeUnit::Nanosecond, Some("+00:00".into())),
                    false,
                ),
            ],
            vec![
                Arc::new(Int32Array::from(vec![1, 2])) as ArrayRef,
                Arc::new(TimestampSecondArray::from(vec![Some(1), None]).with_timezone("+02:30")),
                Arc::new(
                    TimestampMillisecondArray::from(vec![Some(1_001), None]).with_timezone("-07"),
                ),
                Arc::new(TimestampMicrosecondArray::from(vec![1_001_234, 0]).with_timezone("UTC")),
                Arc::new(
                    TimestampNanosecondArray::from(vec![1_001_234_500, 150])
                        .with_timezone("+00:00"),
                ),
            ],
        );

        let bound = BoundDirectBatch::new(&encoder, &batch).unwrap();
        let payload = try_encode_fixed_width_rows(&bound)
            .unwrap()
            .expect("fixed-width datetimeoffset fast path should be active");

        assert_eq!(payload.row_token_offsets(), [0, 49]);
        assert_eq!(
            payload.bytes(),
            expected_rows([
                [
                    int32_cell(1),
                    datetimeoffset_7_cell(719_162, 10_000_000, 150),
                    datetimeoffset_7_cell(719_162, 10_010_000, -420),
                    datetimeoffset_7_cell(719_162, 10_012_340, 0),
                    datetimeoffset_7_cell(719_162, 10_012_345, 0),
                ],
                [
                    int32_cell(2),
                    null_cell(),
                    null_cell(),
                    datetimeoffset_7_cell(719_162, 0, 0),
                    datetimeoffset_7_cell(719_162, 2, 0),
                ],
            ])
        );
    }

    #[cfg(feature = "bench-profile")]
    #[test]
    fn direct_encoder_datetimeoffset_fast_path_matches_general_path() {
        let mappings = vec![
            mapping(0, "id", DataType::Int32, MssqlType::Int, false),
            mapping(
                1,
                "ny",
                DataType::Timestamp(TimeUnit::Second, Some("America/New_York".into())),
                MssqlType::DateTimeOffset { precision: 7 },
                true,
            ),
            mapping(
                2,
                "precise",
                DataType::Timestamp(TimeUnit::Nanosecond, Some("+00:00".into())),
                MssqlType::DateTimeOffset { precision: 7 },
                true,
            ),
        ];
        let options = PlanOptions {
            nanosecond_policy: NanosecondPolicy::RoundTo100ns,
            ..PlanOptions::default()
        };
        let encoder = DirectEncoder::new_with_options(&mappings, options).unwrap();
        let batch = record_batch(
            vec![
                Field::new("id", DataType::Int32, false),
                Field::new(
                    "ny",
                    DataType::Timestamp(TimeUnit::Second, Some("America/New_York".into())),
                    true,
                ),
                Field::new(
                    "precise",
                    DataType::Timestamp(TimeUnit::Nanosecond, Some("+00:00".into())),
                    true,
                ),
            ],
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3])) as ArrayRef,
                Arc::new(
                    TimestampSecondArray::from(vec![
                        Some(1_738_411_200),
                        Some(1_750_593_600),
                        None,
                    ])
                    .with_timezone("America/New_York"),
                ),
                Arc::new(
                    TimestampNanosecondArray::from(vec![Some(149), Some(150), None])
                        .with_timezone("+00:00"),
                ),
            ],
        );

        let fast_path = encoder.encode_batch(&batch).unwrap();
        let _disable_fast_path =
            crate::write::profile::disable_direct_fixed_width_fast_path_for_scope();
        let general_path = encoder.encode_batch(&batch).unwrap();

        assert_eq!(
            fast_path.row_token_offsets(),
            general_path.row_token_offsets()
        );
        assert_eq!(fast_path.bytes(), general_path.bytes());
    }

    #[test]
    fn direct_encoder_fast_path_rejects_invalid_timestamp_timezone_metadata_for_nulls() {
        let mappings = vec![mapping(
            0,
            "created_at",
            DataType::Timestamp(TimeUnit::Second, Some("Foobar".into())),
            MssqlType::DateTime2 { precision: 7 },
            true,
        )];
        let encoder = DirectEncoder::new(&mappings).unwrap();
        let batch = record_batch(
            vec![Field::new(
                "created_at",
                DataType::Timestamp(TimeUnit::Second, Some("Foobar".into())),
                true,
            )],
            vec![Arc::new(
                TimestampSecondArray::from(vec![None]).with_timezone("Foobar"),
            )],
        );

        let err = encoder.encode_batch(&batch).unwrap_err();

        assert_value_conversion_diagnostic(
            err,
            DiagnosticCode::TimezoneUnsupported,
            Some(0),
            Some((0, "created_at")),
        );
    }

    #[cfg(feature = "bench-profile")]
    #[test]
    fn direct_encoder_timestamp_datetime2_fast_path_errors_match_general_path() {
        let mappings = vec![mapping(
            0,
            "precise_at",
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            MssqlType::DateTime2 { precision: 7 },
            false,
        )];
        let encoder = DirectEncoder::new(&mappings).unwrap();
        let lossy_batch = record_batch(
            vec![Field::new(
                "precise_at",
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                false,
            )],
            vec![Arc::new(TimestampNanosecondArray::from(vec![101]))],
        );

        let fast_path = encoder.encode_batch(&lossy_batch).unwrap_err();
        let _disable_fast_path =
            crate::write::profile::disable_direct_fixed_width_fast_path_for_scope();
        let general_path = encoder.encode_batch(&lossy_batch).unwrap_err();

        assert_value_conversion_diagnostic(
            fast_path,
            DiagnosticCode::LossyConversionRequiresPolicy,
            Some(0),
            Some((0, "precise_at")),
        );
        assert_value_conversion_diagnostic(
            general_path,
            DiagnosticCode::LossyConversionRequiresPolicy,
            Some(0),
            Some((0, "precise_at")),
        );

        drop(_disable_fast_path);

        let out_of_range_mappings = vec![mapping(
            0,
            "created_at",
            DataType::Timestamp(TimeUnit::Second, None),
            MssqlType::DateTime2 { precision: 7 },
            false,
        )];
        let out_of_range_encoder = DirectEncoder::new(&out_of_range_mappings).unwrap();
        let out_of_range_batch = record_batch(
            vec![Field::new(
                "created_at",
                DataType::Timestamp(TimeUnit::Second, None),
                false,
            )],
            vec![Arc::new(TimestampSecondArray::from(vec![i64::MAX]))],
        );

        let fast_path = out_of_range_encoder
            .encode_batch(&out_of_range_batch)
            .unwrap_err();
        let _disable_fast_path =
            crate::write::profile::disable_direct_fixed_width_fast_path_for_scope();
        let general_path = out_of_range_encoder
            .encode_batch(&out_of_range_batch)
            .unwrap_err();

        assert_value_conversion_diagnostic(
            fast_path,
            DiagnosticCode::TimestampOutOfRange,
            Some(0),
            Some((0, "created_at")),
        );
        assert_value_conversion_diagnostic(
            general_path,
            DiagnosticCode::TimestampOutOfRange,
            Some(0),
            Some((0, "created_at")),
        );
    }

    #[test]
    fn direct_encoder_encodes_uint64_checked_bigint_boundaries() {
        let mappings = vec![mapping(
            0,
            "unsigned_huge",
            DataType::UInt64,
            MssqlType::BigInt,
            true,
        )];
        let encoder = DirectEncoder::new(&mappings).unwrap();
        let batch = record_batch(
            vec![Field::new("unsigned_huge", DataType::UInt64, true)],
            vec![Arc::new(UInt64Array::from(vec![
                Some(0),
                Some(i64::MAX as u64),
                None,
            ]))],
        );

        let payload = encoder.encode_batch(&batch).unwrap();

        assert_eq!(payload.row_token_offsets(), [0, 10, 20]);
        assert_eq!(
            payload.bytes(),
            [
                payload::TDS_ROW_TOKEN,
                8,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                payload::TDS_ROW_TOKEN,
                8,
                0xFF,
                0xFF,
                0xFF,
                0xFF,
                0xFF,
                0xFF,
                0xFF,
                0x7F,
                payload::TDS_ROW_TOKEN,
                0,
            ]
        );
    }

    #[test]
    fn direct_encoder_encodes_uint64_decimal20_boundaries() {
        let mappings = vec![mapping(
            0,
            "unsigned_huge",
            DataType::UInt64,
            MssqlType::Decimal {
                precision: 20,
                scale: 0,
            },
            true,
        )];
        let encoder = DirectEncoder::new(&mappings).unwrap();
        let batch = record_batch(
            vec![Field::new("unsigned_huge", DataType::UInt64, true)],
            vec![Arc::new(UInt64Array::from(vec![
                Some(0),
                Some((i64::MAX as u64) + 1),
                Some(u64::MAX),
                None,
            ]))],
        );

        let payload = encoder.encode_batch(&batch).unwrap();

        assert_eq!(payload.row_token_offsets(), [0, 7, 18, 33]);
        assert_eq!(
            payload.bytes(),
            [
                payload::TDS_ROW_TOKEN,
                5,
                1,
                0,
                0,
                0,
                0,
                payload::TDS_ROW_TOKEN,
                9,
                1,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0x80,
                payload::TDS_ROW_TOKEN,
                13,
                1,
                0xFF,
                0xFF,
                0xFF,
                0xFF,
                0xFF,
                0xFF,
                0xFF,
                0xFF,
                0,
                0,
                0,
                0,
                payload::TDS_ROW_TOKEN,
                0,
            ]
        );
    }

    #[test]
    fn direct_encoder_rejects_uint64_decimal20_null_in_non_nullable_column() {
        let mappings = vec![mapping(
            0,
            "unsigned_huge",
            DataType::UInt64,
            MssqlType::Decimal {
                precision: 20,
                scale: 0,
            },
            false,
        )];
        let encoder = DirectEncoder::new(&mappings).unwrap();
        let batch = record_batch(
            vec![Field::new("unsigned_huge", DataType::UInt64, true)],
            vec![Arc::new(UInt64Array::from(vec![Some(1), None]))],
        );

        let err = encoder
            .encode_batch(&batch)
            .expect_err("UInt64 decimal20 null must fail for non-nullable target");

        assert_value_conversion_diagnostic(
            err,
            DiagnosticCode::NullInNonNullableColumn,
            Some(1),
            Some((0, "unsigned_huge")),
        );
    }

    #[test]
    fn direct_encoder_encodes_decimal128_sign_zero_and_null() {
        let mappings = vec![mapping(
            0,
            "amount",
            DataType::Decimal128(5, 2),
            MssqlType::Decimal {
                precision: 5,
                scale: 2,
            },
            true,
        )];
        let encoder = DirectEncoder::new(&mappings).unwrap();
        let array = Decimal128Array::from(vec![Some(99_999_i128), Some(-99_999), Some(0), None])
            .with_precision_and_scale(5, 2)
            .unwrap();
        let batch = record_batch(
            vec![Field::new("amount", DataType::Decimal128(5, 2), true)],
            vec![Arc::new(array)],
        );

        let payload = encoder.encode_batch(&batch).unwrap();

        assert_eq!(payload.row_token_offsets(), [0, 7, 14, 21]);
        assert_eq!(
            payload.bytes(),
            [
                payload::TDS_ROW_TOKEN,
                5,
                1,
                0x9F,
                0x86,
                0x01,
                0,
                payload::TDS_ROW_TOKEN,
                5,
                0,
                0x9F,
                0x86,
                0x01,
                0,
                payload::TDS_ROW_TOKEN,
                5,
                1,
                0,
                0,
                0,
                0,
                payload::TDS_ROW_TOKEN,
                0,
            ]
        );
    }

    #[test]
    fn direct_encoder_encodes_decimal256_checked_downcast_value() {
        let mappings = vec![mapping(
            0,
            "amount",
            DataType::Decimal256(38, 0),
            MssqlType::Decimal {
                precision: 38,
                scale: 0,
            },
            false,
        )];
        let encoder = DirectEncoder::new(&mappings).unwrap();
        let value = i256::from_i128(123_456_789_012_345_678_901_234_567_890_i128);
        let array = Decimal256Array::from(vec![value])
            .with_precision_and_scale(38, 0)
            .unwrap();
        let batch = record_batch(
            vec![Field::new("amount", DataType::Decimal256(38, 0), false)],
            vec![Arc::new(array)],
        );

        let payload = encoder.encode_batch(&batch).unwrap();

        assert_eq!(payload.row_count(), 1);
        assert_eq!(
            payload.bytes(),
            [
                payload::TDS_ROW_TOKEN,
                17,
                1,
                0xD2,
                0x0A,
                0x3F,
                0x4E,
                0xEE,
                0xE0,
                0x73,
                0xC3,
                0xF6,
                0x0F,
                0xE9,
                0x8E,
                0x01,
                0,
                0,
                0,
            ]
        );
    }

    #[test]
    fn direct_encoder_encodes_mixed_nullable_and_non_nullable_decimal_columns() {
        let mappings = vec![
            mapping(
                0,
                "amount32",
                DataType::Decimal32(5, 2),
                MssqlType::Decimal {
                    precision: 5,
                    scale: 2,
                },
                false,
            ),
            mapping(
                1,
                "amount64",
                DataType::Decimal64(18, 4),
                MssqlType::Decimal {
                    precision: 18,
                    scale: 4,
                },
                true,
            ),
        ];
        let encoder = DirectEncoder::new(&mappings).unwrap();
        let decimal32 = Decimal32Array::from(vec![12_345_i32, -12_345])
            .with_precision_and_scale(5, 2)
            .unwrap();
        let decimal64 = Decimal64Array::from(vec![None, Some(0_i64)])
            .with_precision_and_scale(18, 4)
            .unwrap();
        let batch = record_batch(
            vec![
                Field::new("amount32", DataType::Decimal32(5, 2), false),
                Field::new("amount64", DataType::Decimal64(18, 4), true),
            ],
            vec![Arc::new(decimal32), Arc::new(decimal64)],
        );

        let payload = encoder.encode_batch(&batch).unwrap();

        assert_eq!(payload.row_token_offsets(), [0, 8]);
        assert_eq!(
            payload.bytes(),
            [
                payload::TDS_ROW_TOKEN,
                5,
                1,
                0x39,
                0x30,
                0,
                0,
                0,
                payload::TDS_ROW_TOKEN,
                5,
                0,
                0x39,
                0x30,
                0,
                0,
                5,
                1,
                0,
                0,
                0,
                0,
            ]
        );
    }

    #[test]
    fn direct_encoder_rejects_decimal_null_in_non_nullable_column() {
        let mappings = vec![mapping(
            0,
            "amount",
            DataType::Decimal128(5, 2),
            MssqlType::Decimal {
                precision: 5,
                scale: 2,
            },
            false,
        )];
        let encoder = DirectEncoder::new(&mappings).unwrap();
        let array = Decimal128Array::from(vec![Some(0_i128), None])
            .with_precision_and_scale(5, 2)
            .unwrap();
        let batch = record_batch(
            vec![Field::new("amount", DataType::Decimal128(5, 2), true)],
            vec![Arc::new(array)],
        );

        let err = encoder
            .encode_batch(&batch)
            .expect_err("decimal null must fail for non-nullable target");

        assert_value_conversion_diagnostic(
            err,
            DiagnosticCode::NullInNonNullableColumn,
            Some(1),
            Some((0, "amount")),
        );
    }

    #[test]
    fn direct_encoder_rejects_decimal_value_outside_planned_precision() {
        let mappings = vec![mapping(
            0,
            "amount",
            DataType::Decimal128(6, 2),
            MssqlType::Decimal {
                precision: 5,
                scale: 2,
            },
            false,
        )];
        let encoder = DirectEncoder::new(&mappings).unwrap();
        let array = Decimal128Array::from(vec![100_000_i128])
            .with_precision_and_scale(6, 2)
            .unwrap();
        let batch = record_batch(
            vec![Field::new("amount", DataType::Decimal128(6, 2), false)],
            vec![Arc::new(array)],
        );

        let err = encoder
            .encode_batch(&batch)
            .expect_err("decimal value outside planned precision must fail");

        assert_value_conversion_diagnostic(
            err,
            DiagnosticCode::DecimalOutOfRange,
            Some(0),
            Some((0, "amount")),
        );
    }

    #[test]
    fn direct_encoder_rejects_decimal256_value_that_cannot_downcast() {
        let mappings = vec![mapping(
            0,
            "amount",
            DataType::Decimal256(39, 0),
            MssqlType::Decimal {
                precision: 38,
                scale: 0,
            },
            false,
        )];
        let encoder = DirectEncoder::new(&mappings).unwrap();
        let array = Decimal256Array::from(vec![i256::from_i128(i128::MAX) + i256::ONE])
            .with_precision_and_scale(39, 0)
            .unwrap();
        let batch = record_batch(
            vec![Field::new("amount", DataType::Decimal256(39, 0), false)],
            vec![Arc::new(array)],
        );

        let err = encoder
            .encode_batch(&batch)
            .expect_err("Decimal256 value outside i128 must fail");

        assert_value_conversion_diagnostic(
            err,
            DiagnosticCode::DecimalOutOfRange,
            Some(0),
            Some((0, "amount")),
        );
    }

    #[test]
    fn direct_encoder_rejects_uint64_checked_bigint_overflow_before_returning_payload() {
        let mappings = vec![mapping(
            0,
            "unsigned_huge",
            DataType::UInt64,
            MssqlType::BigInt,
            false,
        )];
        let encoder = DirectEncoder::new(&mappings).unwrap();
        let batch = record_batch(
            vec![Field::new("unsigned_huge", DataType::UInt64, false)],
            vec![Arc::new(UInt64Array::from(vec![0, (i64::MAX as u64) + 1]))],
        );

        let err = encoder
            .encode_batch(&batch)
            .expect_err("UInt64 checked bigint overflow must fail");

        assert_value_conversion_diagnostic(
            err,
            DiagnosticCode::IntegerOutOfRange,
            Some(1),
            Some((0, "unsigned_huge")),
        );
    }

    #[test]
    fn direct_encoder_rejects_uint64_checked_bigint_overflow_in_append_path() {
        let mappings = vec![
            mapping(
                0,
                "unsigned_huge",
                DataType::UInt64,
                MssqlType::BigInt,
                false,
            ),
            mapping(
                1,
                "label",
                DataType::Utf8,
                MssqlType::NVarChar(MssqlTypeLength::Max),
                false,
            ),
        ];
        let encoder = DirectEncoder::new(&mappings).unwrap();
        let batch = record_batch(
            vec![
                Field::new("unsigned_huge", DataType::UInt64, false),
                Field::new("label", DataType::Utf8, false),
            ],
            vec![
                Arc::new(UInt64Array::from(vec![(i64::MAX as u64) + 1])) as ArrayRef,
                Arc::new(StringArray::from(vec!["overflow"])),
            ],
        );

        let err = encoder
            .encode_batch(&batch)
            .expect_err("append path UInt64 checked bigint overflow must fail");

        assert_value_conversion_diagnostic(
            err,
            DiagnosticCode::IntegerOutOfRange,
            Some(0),
            Some((0, "unsigned_huge")),
        );
    }

    #[test]
    fn direct_encoder_fast_path_does_not_read_non_finite_float_from_null_slot() {
        let mappings = vec![mapping(
            0,
            "ratio",
            DataType::Float64,
            MssqlType::Float { precision: 53 },
            true,
        )];
        let encoder = DirectEncoder::new(&mappings).unwrap();
        let array = Float64Array::new(
            ScalarBuffer::from(vec![f64::NAN, 1.5]),
            Some(NullBuffer::from(vec![false, true])),
        );
        let batch = record_batch(
            vec![Field::new("ratio", DataType::Float64, true)],
            vec![Arc::new(array)],
        );

        let payload = encoder.encode_batch(&batch).unwrap();

        assert_eq!(payload.row_token_offsets(), [0, 2]);
        assert_eq!(
            payload.bytes(),
            [
                payload::TDS_ROW_TOKEN,
                0,
                payload::TDS_ROW_TOKEN,
                8,
                0x00,
                0x00,
                0x00,
                0x00,
                0x00,
                0x00,
                0xF8,
                0x3F
            ]
        );
    }

    #[test]
    fn direct_encoder_fast_path_does_not_read_non_finite_float32_from_null_slot() {
        let mappings = vec![mapping(
            0,
            "real_value",
            DataType::Float32,
            MssqlType::Real,
            true,
        )];
        let encoder = DirectEncoder::new(&mappings).unwrap();
        let array = Float32Array::new(
            ScalarBuffer::from(vec![f32::NAN, 1.5]),
            Some(NullBuffer::from(vec![false, true])),
        );
        let batch = record_batch(
            vec![Field::new("real_value", DataType::Float32, true)],
            vec![Arc::new(array)],
        );

        let payload = encoder.encode_batch(&batch).unwrap();

        assert_eq!(payload.row_token_offsets(), [0, 2]);
        assert_eq!(
            payload.bytes(),
            [
                payload::TDS_ROW_TOKEN,
                0,
                payload::TDS_ROW_TOKEN,
                4,
                0x00,
                0x00,
                0xC0,
                0x3F
            ]
        );
    }

    #[test]
    fn direct_encoder_fast_path_does_not_read_non_finite_float16_from_null_slot() {
        let mappings = vec![mapping(
            0,
            "half_value",
            DataType::Float16,
            MssqlType::Real,
            true,
        )];
        let encoder = DirectEncoder::new(&mappings).unwrap();
        let array = Float16Array::new(
            ScalarBuffer::from(vec![F16::from_f32(f32::NAN), F16::from_f32(1.5)]),
            Some(NullBuffer::from(vec![false, true])),
        );
        let batch = record_batch(
            vec![Field::new("half_value", DataType::Float16, true)],
            vec![Arc::new(array)],
        );

        let payload = encoder.encode_batch(&batch).unwrap();

        assert_eq!(payload.row_token_offsets(), [0, 2]);
        assert_eq!(
            payload.bytes(),
            [
                payload::TDS_ROW_TOKEN,
                0,
                payload::TDS_ROW_TOKEN,
                4,
                0x00,
                0x00,
                0xC0,
                0x3F
            ]
        );
    }

    #[test]
    fn direct_encoder_fast_path_rejects_non_finite_nullable_float_when_non_null() {
        let mappings = vec![mapping(
            0,
            "ratio",
            DataType::Float64,
            MssqlType::Float { precision: 53 },
            true,
        )];
        let encoder = DirectEncoder::new(&mappings).unwrap();
        let batch = record_batch(
            vec![Field::new("ratio", DataType::Float64, true)],
            vec![Arc::new(Float64Array::from(vec![
                Some(1.0),
                Some(f64::NAN),
            ]))],
        );

        let err = encoder
            .encode_batch(&batch)
            .expect_err("non-null non-finite float must fail");

        assert_value_conversion_diagnostic(
            err,
            DiagnosticCode::NonFiniteFloat,
            Some(1),
            Some((0, "ratio")),
        );
    }

    #[test]
    fn direct_encoder_fast_path_rejects_non_finite_float16_when_non_null() {
        let mappings = vec![mapping(
            0,
            "half_value",
            DataType::Float16,
            MssqlType::Real,
            true,
        )];
        let encoder = DirectEncoder::new(&mappings).unwrap();
        let batch = record_batch(
            vec![Field::new("half_value", DataType::Float16, true)],
            vec![Arc::new(Float16Array::from(vec![
                Some(F16::from_f32(1.0)),
                Some(F16::from_f32(f32::NAN)),
            ]))],
        );

        let err = encoder
            .encode_batch(&batch)
            .expect_err("non-null non-finite Float16 must fail");

        assert_value_conversion_diagnostic(
            err,
            DiagnosticCode::NonFiniteFloat,
            Some(1),
            Some((0, "half_value")),
        );
    }

    #[test]
    fn direct_encoder_fast_path_rejects_null_in_non_nullable_column() {
        let mappings = vec![mapping(
            0,
            "quantity",
            DataType::Int32,
            MssqlType::Int,
            false,
        )];
        let encoder = DirectEncoder::new(&mappings).unwrap();
        let batch = record_batch(
            vec![Field::new("quantity", DataType::Int32, true)],
            vec![Arc::new(Int32Array::from(vec![Some(1), None]))],
        );

        let err = encoder
            .encode_batch(&batch)
            .expect_err("null in non-nullable direct column must fail");

        assert_value_conversion_diagnostic(
            err,
            DiagnosticCode::NullInNonNullableColumn,
            Some(1),
            Some((0, "quantity")),
        );
    }

    #[test]
    fn direct_encoder_fast_path_rejects_float16_null_in_non_nullable_column() {
        let mappings = vec![mapping(
            0,
            "half_value",
            DataType::Float16,
            MssqlType::Real,
            false,
        )];
        let encoder = DirectEncoder::new(&mappings).unwrap();
        let batch = record_batch(
            vec![Field::new("half_value", DataType::Float16, true)],
            vec![Arc::new(Float16Array::from(vec![
                Some(F16::from_f32(1.0)),
                None,
            ]))],
        );

        let err = encoder
            .encode_batch(&batch)
            .expect_err("Float16 null in non-nullable direct column must fail");

        assert_value_conversion_diagnostic(
            err,
            DiagnosticCode::NullInNonNullableColumn,
            Some(1),
            Some((0, "half_value")),
        );
    }

    #[test]
    fn direct_encoder_rejects_non_finite_float_before_returning_payload() {
        let mappings = vec![mapping(
            0,
            "ratio",
            DataType::Float64,
            MssqlType::Float { precision: 53 },
            false,
        )];
        let encoder = DirectEncoder::new(&mappings).unwrap();
        let batch = record_batch(
            vec![Field::new("ratio", DataType::Float64, false)],
            vec![Arc::new(Float64Array::from(vec![1.0, f64::NAN]))],
        );

        let err = encoder
            .encode_batch(&batch)
            .expect_err("non-finite float must fail");

        assert_value_conversion_diagnostic(
            err,
            DiagnosticCode::NonFiniteFloat,
            Some(1),
            Some((0, "ratio")),
        );
    }

    #[derive(Debug, Clone, Copy)]
    struct FixtureSupport;

    impl DirectEncoderSupport for FixtureSupport {
        fn support_mapping(&self, mapping: &SchemaMapping) -> DirectMappingSupport {
            DirectMappingSupport::Supported {
                encoding: DirectColumnEncoding::Primitive(
                    PrimitiveArrowToMssql::classify(mapping, 0).unwrap(),
                ),
            }
        }
    }

    fn assert_unsupported_batch(err: Error) {
        assert_direct_encoding_diagnostic(err, DiagnosticCode::DirectEncodingUnsupportedBatch);
    }

    fn assert_direct_encoding_diagnostic(err: Error, expected_code: DiagnosticCode) {
        let Error::DirectEncoding { diagnostics } = err else {
            panic!("expected direct encoding error");
        };

        assert_eq!(diagnostics.len(), 1);
        assert_eq!(diagnostics.all()[0].code(), expected_code);
    }

    fn string_family_types() -> [DataType; 3] {
        [DataType::Utf8, DataType::LargeUtf8, DataType::Utf8View]
    }

    fn binary_family_types() -> [DataType; 3] {
        [
            DataType::Binary,
            DataType::LargeBinary,
            DataType::BinaryView,
        ]
    }

    fn string_family_array(data_type: DataType, values: Vec<Option<&'static str>>) -> ArrayRef {
        match data_type {
            DataType::Utf8 => Arc::new(StringArray::from(values)) as ArrayRef,
            DataType::LargeUtf8 => Arc::new(LargeStringArray::from(values)) as ArrayRef,
            DataType::Utf8View => Arc::new(StringViewArray::from(values)) as ArrayRef,
            _ => unreachable!("test helper only supports Arrow string-family types"),
        }
    }

    fn binary_family_array(data_type: DataType, values: Vec<Option<&'static [u8]>>) -> ArrayRef {
        match data_type {
            DataType::Binary => Arc::new(BinaryArray::from_iter(values)) as ArrayRef,
            DataType::LargeBinary => Arc::new(LargeBinaryArray::from_iter(values)) as ArrayRef,
            DataType::BinaryView => Arc::new(BinaryViewArray::from(values)) as ArrayRef,
            _ => unreachable!("test helper only supports Arrow binary-family types"),
        }
    }

    fn mapping(
        index: usize,
        name: &str,
        arrow_type: DataType,
        mssql_type: MssqlType,
        nullable: bool,
    ) -> SchemaMapping {
        SchemaMapping::new(
            ArrowFieldRef::new(index, name.to_owned(), nullable, arrow_type),
            MssqlColumn::new(Identifier::new(name).unwrap(), mssql_type, nullable),
        )
    }

    fn record_batch(fields: Vec<Field>, arrays: Vec<ArrayRef>) -> RecordBatch {
        RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays).unwrap()
    }

    fn unsafe_record_batch(
        fields: Vec<Field>,
        arrays: Vec<ArrayRef>,
        row_count: usize,
    ) -> RecordBatch {
        // SAFETY: these tests deliberately keep schema metadata compatible
        // with the plan while passing a mismatched physical array to exercise
        // unchecked-batch encoding-shape guards.
        unsafe { RecordBatch::new_unchecked(Arc::new(Schema::new(fields)), arrays, row_count) }
    }

    fn expected_rows<const R: usize, const C: usize>(rows: [[Vec<u8>; C]; R]) -> Vec<u8> {
        let mut bytes = Vec::new();
        for row in rows {
            bytes.push(payload::TDS_ROW_TOKEN);
            for cell in row {
                bytes.extend_from_slice(&cell);
            }
        }
        bytes
    }

    fn datetime2_7_cell(date_days: u32, time_increments: u64) -> Vec<u8> {
        datetime2_cell(7, date_days, time_increments)
    }

    fn datetime_cell(days: i32, seconds_fragments: u32) -> Vec<u8> {
        let mut bytes = vec![0; 9];
        write_datetime_cell(&mut bytes, MssqlDateTime::new(days, seconds_fragments)).unwrap();
        bytes
    }

    fn datetime2_cell(scale: u8, date_days: u32, time_increments: u64) -> Vec<u8> {
        let len = match scale {
            0..=2 => 7,
            3..=4 => 8,
            5..=7 => 9,
            _ => panic!("unsupported test datetime2 scale"),
        };
        let mut bytes = vec![0; len];
        write_datetime2_cell(
            &mut bytes,
            MssqlDateTime2::new(
                MssqlDate::new(date_days),
                MssqlTime::new(time_increments, scale),
            ),
        )
        .unwrap();
        bytes
    }

    fn datetimeoffset_7_cell(date_days: u32, time_increments: u64, offset_minutes: i16) -> Vec<u8> {
        let mut bytes = vec![0; 11];
        write_datetimeoffset_cell(
            &mut bytes,
            MssqlDateTimeOffset::new(
                MssqlDateTime2::new(
                    MssqlDate::new(date_days),
                    MssqlTime::new(time_increments, 7),
                ),
                offset_minutes,
            ),
        )
        .unwrap();
        bytes
    }

    fn time_cell(scale: u8, increments: u64) -> Vec<u8> {
        let len = match scale {
            0..=2 => 4,
            3..=4 => 5,
            5..=7 => 6,
            _ => panic!("unsupported test time scale"),
        };
        let mut bytes = vec![0; len];
        write_time_cell(&mut bytes, MssqlTime::new(increments, scale)).unwrap();
        bytes
    }

    fn int32_cell(value: i32) -> Vec<u8> {
        value.to_le_bytes().to_vec()
    }

    fn bounded_nvarchar_cell(value: &str) -> Vec<u8> {
        let mut bytes = Vec::new();
        let encoded_bytes = value.encode_utf16().count() * 2;
        bytes.extend_from_slice(&(encoded_bytes as u16).to_le_bytes());
        for code_unit in value.encode_utf16() {
            bytes.extend_from_slice(&code_unit.to_le_bytes());
        }
        bytes
    }

    fn bounded_nvarchar_null_cell() -> Vec<u8> {
        u16::MAX.to_le_bytes().to_vec()
    }

    fn max_varbinary_cell(value: &[u8]) -> Vec<u8> {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&0xfffffffffffffffe_u64.to_le_bytes());
        bytes.extend_from_slice(&(value.len() as u32).to_le_bytes());
        bytes.extend_from_slice(value);
        if !value.is_empty() {
            bytes.extend_from_slice(&0u32.to_le_bytes());
        }
        bytes
    }

    fn max_varbinary_null_cell() -> Vec<u8> {
        u64::MAX.to_le_bytes().to_vec()
    }

    fn fixed_binary_cell(value: &[u8]) -> Vec<u8> {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&(value.len() as u16).to_le_bytes());
        bytes.extend_from_slice(value);
        bytes
    }

    fn fixed_binary_null_cell() -> Vec<u8> {
        u16::MAX.to_le_bytes().to_vec()
    }

    fn nullable_int32_cell(value: i32) -> Vec<u8> {
        let mut bytes = vec![4];
        bytes.extend_from_slice(&value.to_le_bytes());
        bytes
    }

    fn null_cell() -> Vec<u8> {
        vec![0]
    }

    fn assert_value_conversion_diagnostic(
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

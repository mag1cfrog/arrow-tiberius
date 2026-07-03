//! Arrow runtime cell values.

use arrow_array::{
    Array, BinaryArray, BinaryViewArray, BooleanArray, Date32Array, Date64Array, Decimal32Array,
    Decimal64Array, Decimal128Array, Decimal256Array, FixedSizeBinaryArray, Float16Array,
    Float32Array, Float64Array, Int8Array, Int16Array, Int32Array, Int64Array, LargeBinaryArray,
    LargeStringArray, StringArray, StringViewArray, Time32MillisecondArray, Time32SecondArray,
    Time64MicrosecondArray, Time64NanosecondArray, TimestampMicrosecondArray,
    TimestampMillisecondArray, TimestampNanosecondArray, TimestampSecondArray, UInt8Array,
    UInt16Array, UInt32Array, UInt64Array,
};
use arrow_buffer::i256;
use arrow_schema::{DataType, TimeUnit};

use crate::{
    Diagnostic, DiagnosticCode, DiagnosticSet, FieldRef, Result, SchemaMapping,
    arrow::field::{is_arrow_binary_family, is_arrow_string_family},
};

/// Borrowed value extracted from one Arrow array cell.
#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) enum ArrowCell<'a> {
    /// Arrow null value.
    Null,
    /// Arrow boolean value.
    Boolean(bool),
    /// Arrow signed 8-bit integer value.
    Int8(i8),
    /// Arrow signed 16-bit integer value.
    Int16(i16),
    /// Arrow signed 32-bit integer value.
    Int32(i32),
    /// Arrow signed 64-bit integer value.
    Int64(i64),
    /// Arrow unsigned 8-bit integer value.
    UInt8(u8),
    /// Arrow unsigned 16-bit integer value.
    UInt16(u16),
    /// Arrow unsigned 32-bit integer value.
    UInt32(u32),
    /// Arrow unsigned 64-bit integer value.
    UInt64(u64),
    /// Arrow 32-bit decimal value.
    Decimal32(i32),
    /// Arrow 64-bit decimal value.
    Decimal64(i64),
    /// Arrow 128-bit decimal value.
    Decimal128(i128),
    /// Arrow 256-bit decimal value.
    Decimal256(i256),
    /// Arrow Date32 day offset from Unix epoch.
    Date32(i32),
    /// Arrow Date64 millisecond timestamp from Unix epoch.
    Date64(i64),
    /// Arrow Time32 second offset from midnight.
    Time32Second(i32),
    /// Arrow Time32 millisecond offset from midnight.
    Time32Millisecond(i32),
    /// Arrow Time64 microsecond offset from midnight.
    Time64Microsecond(i64),
    /// Arrow Time64 nanosecond offset from midnight.
    Time64Nanosecond(i64),
    /// Arrow timestamp second offset from Unix epoch.
    TimestampSecond(i64),
    /// Arrow timestamp millisecond offset from Unix epoch.
    TimestampMillisecond(i64),
    /// Arrow timestamp microsecond offset from Unix epoch.
    TimestampMicrosecond(i64),
    /// Arrow timestamp nanosecond offset from Unix epoch.
    TimestampNanosecond(i64),
    /// Arrow 32-bit floating point value.
    Float32(f32),
    /// Arrow 16-bit floating point value widened to f32.
    Float16(f32),
    /// Arrow 64-bit floating point value.
    Float64(f64),
    /// Arrow UTF-8 string value.
    Utf8(&'a str),
    /// Arrow binary value.
    Binary(&'a [u8]),
}

pub(crate) fn extract_arrow_cell<'a>(
    array: &'a dyn Array,
    mapping: &SchemaMapping,
    row_index: usize,
) -> Result<ArrowCell<'a>> {
    if array.is_null(row_index) {
        return Ok(ArrowCell::Null);
    }

    match mapping.arrow().data_type() {
        DataType::Boolean => {
            let array = downcast_array::<BooleanArray>(array, mapping, row_index)?;
            Ok(ArrowCell::Boolean(array.value(row_index)))
        }
        DataType::Int8 => {
            let array = downcast_array::<Int8Array>(array, mapping, row_index)?;
            Ok(ArrowCell::Int8(array.value(row_index)))
        }
        DataType::Int16 => {
            let array = downcast_array::<Int16Array>(array, mapping, row_index)?;
            Ok(ArrowCell::Int16(array.value(row_index)))
        }
        DataType::Int32 => {
            let array = downcast_array::<Int32Array>(array, mapping, row_index)?;
            Ok(ArrowCell::Int32(array.value(row_index)))
        }
        DataType::Int64 => {
            let array = downcast_array::<Int64Array>(array, mapping, row_index)?;
            Ok(ArrowCell::Int64(array.value(row_index)))
        }
        DataType::UInt8 => {
            let array = downcast_array::<UInt8Array>(array, mapping, row_index)?;
            Ok(ArrowCell::UInt8(array.value(row_index)))
        }
        DataType::UInt16 => {
            let array = downcast_array::<UInt16Array>(array, mapping, row_index)?;
            Ok(ArrowCell::UInt16(array.value(row_index)))
        }
        DataType::UInt32 => {
            let array = downcast_array::<UInt32Array>(array, mapping, row_index)?;
            Ok(ArrowCell::UInt32(array.value(row_index)))
        }
        DataType::UInt64 => {
            let array = downcast_array::<UInt64Array>(array, mapping, row_index)?;
            Ok(ArrowCell::UInt64(array.value(row_index)))
        }
        DataType::Decimal32(_, _) => {
            let array = downcast_array::<Decimal32Array>(array, mapping, row_index)?;
            Ok(ArrowCell::Decimal32(array.value(row_index)))
        }
        DataType::Decimal64(_, _) => {
            let array = downcast_array::<Decimal64Array>(array, mapping, row_index)?;
            Ok(ArrowCell::Decimal64(array.value(row_index)))
        }
        DataType::Decimal128(_, _) => {
            let array = downcast_array::<Decimal128Array>(array, mapping, row_index)?;
            Ok(ArrowCell::Decimal128(array.value(row_index)))
        }
        DataType::Decimal256(_, _) => {
            let array = downcast_array::<Decimal256Array>(array, mapping, row_index)?;
            Ok(ArrowCell::Decimal256(array.value(row_index)))
        }
        DataType::Date32 => {
            let array = downcast_array::<Date32Array>(array, mapping, row_index)?;
            Ok(ArrowCell::Date32(array.value(row_index)))
        }
        DataType::Date64 => {
            let array = downcast_array::<Date64Array>(array, mapping, row_index)?;
            Ok(ArrowCell::Date64(array.value(row_index)))
        }
        DataType::Time32(time_unit) => match time_unit {
            TimeUnit::Second => {
                let array = downcast_array::<Time32SecondArray>(array, mapping, row_index)?;
                Ok(ArrowCell::Time32Second(array.value(row_index)))
            }
            TimeUnit::Millisecond => {
                let array = downcast_array::<Time32MillisecondArray>(array, mapping, row_index)?;
                Ok(ArrowCell::Time32Millisecond(array.value(row_index)))
            }
            other => Err(unsupported_value_conversion(
                mapping,
                row_index,
                format!("Arrow Time32 unit {other:?} is not supported for value extraction"),
            )),
        },
        DataType::Time64(time_unit) => match time_unit {
            TimeUnit::Microsecond => {
                let array = downcast_array::<Time64MicrosecondArray>(array, mapping, row_index)?;
                Ok(ArrowCell::Time64Microsecond(array.value(row_index)))
            }
            TimeUnit::Nanosecond => {
                let array = downcast_array::<Time64NanosecondArray>(array, mapping, row_index)?;
                Ok(ArrowCell::Time64Nanosecond(array.value(row_index)))
            }
            other => Err(unsupported_value_conversion(
                mapping,
                row_index,
                format!("Arrow Time64 unit {other:?} is not supported for value extraction"),
            )),
        },
        DataType::Timestamp(time_unit, _) => match time_unit {
            TimeUnit::Second => {
                let array = downcast_array::<TimestampSecondArray>(array, mapping, row_index)?;
                Ok(ArrowCell::TimestampSecond(array.value(row_index)))
            }
            TimeUnit::Millisecond => {
                let array = downcast_array::<TimestampMillisecondArray>(array, mapping, row_index)?;
                Ok(ArrowCell::TimestampMillisecond(array.value(row_index)))
            }
            TimeUnit::Microsecond => {
                let array = downcast_array::<TimestampMicrosecondArray>(array, mapping, row_index)?;
                Ok(ArrowCell::TimestampMicrosecond(array.value(row_index)))
            }
            TimeUnit::Nanosecond => {
                let array = downcast_array::<TimestampNanosecondArray>(array, mapping, row_index)?;
                Ok(ArrowCell::TimestampNanosecond(array.value(row_index)))
            }
        },
        DataType::Float16 => {
            let array = downcast_array::<Float16Array>(array, mapping, row_index)?;
            Ok(ArrowCell::Float16(array.value(row_index).to_f32()))
        }
        DataType::Float32 => {
            let array = downcast_array::<Float32Array>(array, mapping, row_index)?;
            Ok(ArrowCell::Float32(array.value(row_index)))
        }
        DataType::Float64 => {
            let array = downcast_array::<Float64Array>(array, mapping, row_index)?;
            Ok(ArrowCell::Float64(array.value(row_index)))
        }
        planned if is_arrow_string_family(planned) => extract_utf8_cell(array, mapping, row_index),
        planned if is_arrow_binary_family(planned) => {
            extract_binary_cell(array, mapping, row_index)
        }
        DataType::FixedSizeBinary(_) => {
            let array = downcast_array::<FixedSizeBinaryArray>(array, mapping, row_index)?;
            Ok(ArrowCell::Binary(array.value(row_index)))
        }
        other => Err(unsupported_value_conversion(
            mapping,
            row_index,
            format!("Arrow value extraction for {other} is not supported yet"),
        )),
    }
}

fn extract_binary_cell<'a>(
    array: &'a dyn Array,
    mapping: &SchemaMapping,
    row_index: usize,
) -> Result<ArrowCell<'a>> {
    match array.data_type() {
        DataType::Binary => {
            let array = downcast_array::<BinaryArray>(array, mapping, row_index)?;
            Ok(ArrowCell::Binary(array.value(row_index)))
        }
        DataType::LargeBinary => {
            let array = downcast_array::<LargeBinaryArray>(array, mapping, row_index)?;
            Ok(ArrowCell::Binary(array.value(row_index)))
        }
        DataType::BinaryView => {
            let array = downcast_array::<BinaryViewArray>(array, mapping, row_index)?;
            Ok(ArrowCell::Binary(array.value(row_index)))
        }
        _ => Err(value_conversion_error(row_mapping_diagnostic(
            mapping,
            row_index,
            DiagnosticCode::ValueTypeMismatch,
            format!(
                "runtime Arrow type {} does not match planned Arrow type {}",
                array.data_type(),
                mapping.arrow().data_type()
            ),
        ))),
    }
}

fn extract_utf8_cell<'a>(
    array: &'a dyn Array,
    mapping: &SchemaMapping,
    row_index: usize,
) -> Result<ArrowCell<'a>> {
    match array.data_type() {
        DataType::Utf8 => {
            let array = downcast_array::<StringArray>(array, mapping, row_index)?;
            Ok(ArrowCell::Utf8(array.value(row_index)))
        }
        DataType::LargeUtf8 => {
            let array = downcast_array::<LargeStringArray>(array, mapping, row_index)?;
            Ok(ArrowCell::Utf8(array.value(row_index)))
        }
        DataType::Utf8View => {
            let array = downcast_array::<StringViewArray>(array, mapping, row_index)?;
            Ok(ArrowCell::Utf8(array.value(row_index)))
        }
        _ => Err(value_conversion_error(row_mapping_diagnostic(
            mapping,
            row_index,
            DiagnosticCode::ValueTypeMismatch,
            format!(
                "runtime Arrow type {} does not match planned Arrow type {}",
                array.data_type(),
                mapping.arrow().data_type()
            ),
        ))),
    }
}

fn downcast_array<'a, T: Array + 'static>(
    array: &'a dyn Array,
    mapping: &SchemaMapping,
    row_index: usize,
) -> Result<&'a T> {
    array.as_any().downcast_ref::<T>().ok_or_else(|| {
        value_conversion_error(row_mapping_diagnostic(
            mapping,
            row_index,
            DiagnosticCode::ValueTypeMismatch,
            format!(
                "runtime Arrow type {} does not match planned Arrow type {}",
                array.data_type(),
                mapping.arrow().data_type()
            ),
        ))
    })
}

fn unsupported_value_conversion(
    mapping: &SchemaMapping,
    row_index: usize,
    message: impl Into<String>,
) -> crate::Error {
    value_conversion_error(row_mapping_diagnostic(
        mapping,
        row_index,
        DiagnosticCode::ValueConversionUnsupported,
        message,
    ))
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
    use std::sync::Arc;

    use arrow_array::{
        ArrayRef, BinaryArray, BinaryViewArray, BooleanArray, Date32Array, Date64Array,
        Decimal32Array, Decimal64Array, Decimal128Array, Decimal256Array, FixedSizeBinaryArray,
        Float16Array, Float32Array, Float64Array, Int8Array, Int16Array, Int32Array, Int64Array,
        LargeBinaryArray, LargeStringArray, StringArray, StringViewArray, Time32MillisecondArray,
        Time32SecondArray, Time64MicrosecondArray, Time64NanosecondArray,
        TimestampMicrosecondArray, TimestampMillisecondArray, TimestampNanosecondArray,
        TimestampSecondArray, UInt8Array, UInt16Array, UInt32Array, UInt64Array,
        types::{ArrowPrimitiveType, Float16Type},
    };
    use arrow_buffer::i256;
    use arrow_schema::{DataType, TimeUnit};

    use super::{ArrowCell, extract_arrow_cell};
    use crate::{ArrowFieldRef, Identifier, MssqlColumn, MssqlType, SchemaMapping};

    type F16 = <Float16Type as ArrowPrimitiveType>::Native;

    #[test]
    fn extracts_arrow_cells_for_supported_initial_primitives() {
        let cases: Vec<(SchemaMapping, ArrayRef, ArrowCell<'_>)> = vec![
            (
                mapping("active", DataType::Boolean),
                Arc::new(BooleanArray::from(vec![Some(true), None])),
                ArrowCell::Boolean(true),
            ),
            (
                mapping("tiny", DataType::Int8),
                Arc::new(Int8Array::from(vec![Some(-8_i8), None])),
                ArrowCell::Int8(-8),
            ),
            (
                mapping("small", DataType::Int16),
                Arc::new(Int16Array::from(vec![Some(-16_i16), None])),
                ArrowCell::Int16(-16),
            ),
            (
                mapping("quantity", DataType::Int32),
                Arc::new(Int32Array::from(vec![Some(12_i32), None])),
                ArrowCell::Int32(12),
            ),
            (
                mapping("total", DataType::Int64),
                Arc::new(Int64Array::from(vec![Some(34_i64), None])),
                ArrowCell::Int64(34),
            ),
            (
                mapping("unsigned_tiny", DataType::UInt8),
                Arc::new(UInt8Array::from(vec![Some(8_u8), None])),
                ArrowCell::UInt8(8),
            ),
            (
                mapping("unsigned_medium", DataType::UInt16),
                Arc::new(UInt16Array::from(vec![Some(16_u16), None])),
                ArrowCell::UInt16(16),
            ),
            (
                mapping("unsigned_large", DataType::UInt32),
                Arc::new(UInt32Array::from(vec![Some(32_u32), None])),
                ArrowCell::UInt32(32),
            ),
            (
                mapping("half_value", DataType::Float16),
                Arc::new(Float16Array::from(vec![Some(F16::from_f32(1.5)), None])),
                ArrowCell::Float16(1.5),
            ),
            (
                mapping("real_value", DataType::Float32),
                Arc::new(Float32Array::from(vec![Some(1.25_f32), None])),
                ArrowCell::Float32(1.25),
            ),
            (
                mapping("float_value", DataType::Float64),
                Arc::new(Float64Array::from(vec![Some(2.5_f64), None])),
                ArrowCell::Float64(2.5),
            ),
            (
                mapping("text", DataType::Utf8),
                Arc::new(StringArray::from(vec![Some("hello"), None])),
                ArrowCell::Utf8("hello"),
            ),
            (
                mapping("large_text", DataType::LargeUtf8),
                Arc::new(LargeStringArray::from(vec![Some("Tokyo"), None])),
                ArrowCell::Utf8("Tokyo"),
            ),
            (
                mapping("view_text", DataType::Utf8View),
                Arc::new(StringViewArray::from(vec![Some("view"), None])),
                ArrowCell::Utf8("view"),
            ),
            (
                mapping("bytes", DataType::Binary),
                Arc::new(BinaryArray::from(vec![Some(&b"abc"[..]), None])),
                ArrowCell::Binary(b"abc"),
            ),
            (
                mapping("large_bytes", DataType::LargeBinary),
                Arc::new(LargeBinaryArray::from(vec![Some(&b"large"[..]), None])),
                ArrowCell::Binary(b"large"),
            ),
            (
                mapping("view_bytes", DataType::BinaryView),
                Arc::new(BinaryViewArray::from(vec![Some(&b"view"[..]), None])),
                ArrowCell::Binary(b"view"),
            ),
            (
                mapping("fixed_bytes", DataType::FixedSizeBinary(3)),
                Arc::new(
                    FixedSizeBinaryArray::try_from_sparse_iter_with_size(
                        [Some(&b"abc"[..]), None].into_iter(),
                        3,
                    )
                    .unwrap(),
                ),
                ArrowCell::Binary(b"abc"),
            ),
        ];

        for (mapping, array, expected) in cases {
            assert_eq!(
                extract_arrow_cell(array.as_ref(), &mapping, 0).unwrap(),
                expected
            );
            assert_eq!(
                extract_arrow_cell(array.as_ref(), &mapping, 1).unwrap(),
                ArrowCell::Null
            );
        }
    }

    #[test]
    fn extracts_string_family_cells_from_runtime_representations() {
        let cases: Vec<(SchemaMapping, ArrayRef, ArrowCell<'_>)> = vec![
            (
                mapping("text", DataType::Utf8),
                Arc::new(StringViewArray::from(vec![Some("view"), None])),
                ArrowCell::Utf8("view"),
            ),
            (
                mapping("text", DataType::Utf8View),
                Arc::new(StringArray::from(vec![Some("small"), None])),
                ArrowCell::Utf8("small"),
            ),
            (
                mapping("text", DataType::Utf8View),
                Arc::new(LargeStringArray::from(vec![Some("large"), None])),
                ArrowCell::Utf8("large"),
            ),
        ];

        for (mapping, array, expected) in cases {
            assert_eq!(
                extract_arrow_cell(array.as_ref(), &mapping, 0).unwrap(),
                expected
            );
            assert_eq!(
                extract_arrow_cell(array.as_ref(), &mapping, 1).unwrap(),
                ArrowCell::Null
            );
        }
    }

    #[test]
    fn extracts_binary_family_cells_from_runtime_representations() {
        let cases: Vec<(SchemaMapping, ArrayRef, ArrowCell<'_>)> = vec![
            (
                mapping("bytes", DataType::Binary),
                Arc::new(BinaryViewArray::from(vec![Some(&b"view"[..]), None])),
                ArrowCell::Binary(b"view"),
            ),
            (
                mapping("bytes", DataType::BinaryView),
                Arc::new(BinaryArray::from(vec![Some(&b"small"[..]), None])),
                ArrowCell::Binary(b"small"),
            ),
            (
                mapping("bytes", DataType::BinaryView),
                Arc::new(LargeBinaryArray::from(vec![Some(&b"large"[..]), None])),
                ArrowCell::Binary(b"large"),
            ),
        ];

        for (mapping, array, expected) in cases {
            assert_eq!(
                extract_arrow_cell(array.as_ref(), &mapping, 0).unwrap(),
                expected
            );
            assert_eq!(
                extract_arrow_cell(array.as_ref(), &mapping, 1).unwrap(),
                ArrowCell::Null
            );
        }
    }

    #[test]
    fn extracts_uint64_arrow_cells_at_policy_boundaries() {
        let mapping = mapping("unsigned_huge", DataType::UInt64);
        let array = UInt64Array::from(vec![
            Some(0_u64),
            Some(i64::MAX as u64),
            Some((i64::MAX as u64) + 1),
            Some(u64::MAX),
            None,
        ]);

        assert_eq!(
            extract_arrow_cell(&array, &mapping, 0).unwrap(),
            ArrowCell::UInt64(0)
        );
        assert_eq!(
            extract_arrow_cell(&array, &mapping, 1).unwrap(),
            ArrowCell::UInt64(i64::MAX as u64)
        );
        assert_eq!(
            extract_arrow_cell(&array, &mapping, 2).unwrap(),
            ArrowCell::UInt64((i64::MAX as u64) + 1)
        );
        assert_eq!(
            extract_arrow_cell(&array, &mapping, 3).unwrap(),
            ArrowCell::UInt64(u64::MAX)
        );
        assert_eq!(
            extract_arrow_cell(&array, &mapping, 4).unwrap(),
            ArrowCell::Null
        );
    }

    #[test]
    fn extracts_timestamp_arrow_cells_without_losing_epoch_values() {
        let cases: Vec<(SchemaMapping, ArrayRef, ArrowCell<'_>, ArrowCell<'_>)> = vec![
            (
                mapping("ts_s", DataType::Timestamp(TimeUnit::Second, None)),
                Arc::new(TimestampSecondArray::from(vec![
                    Some(i64::MIN),
                    Some(i64::MAX),
                    None,
                ])),
                ArrowCell::TimestampSecond(i64::MIN),
                ArrowCell::TimestampSecond(i64::MAX),
            ),
            (
                mapping("ts_ms", DataType::Timestamp(TimeUnit::Millisecond, None)),
                Arc::new(TimestampMillisecondArray::from(vec![
                    Some(i64::MIN),
                    Some(i64::MAX),
                    None,
                ])),
                ArrowCell::TimestampMillisecond(i64::MIN),
                ArrowCell::TimestampMillisecond(i64::MAX),
            ),
            (
                mapping("ts_us", DataType::Timestamp(TimeUnit::Microsecond, None)),
                Arc::new(TimestampMicrosecondArray::from(vec![
                    Some(i64::MIN),
                    Some(i64::MAX),
                    None,
                ])),
                ArrowCell::TimestampMicrosecond(i64::MIN),
                ArrowCell::TimestampMicrosecond(i64::MAX),
            ),
            (
                mapping("ts_ns", DataType::Timestamp(TimeUnit::Nanosecond, None)),
                Arc::new(TimestampNanosecondArray::from(vec![
                    Some(i64::MIN),
                    Some(i64::MAX),
                    None,
                ])),
                ArrowCell::TimestampNanosecond(i64::MIN),
                ArrowCell::TimestampNanosecond(i64::MAX),
            ),
            (
                mapping(
                    "ts_tz",
                    DataType::Timestamp(TimeUnit::Second, Some("America/New_York".into())),
                ),
                Arc::new(
                    TimestampSecondArray::from(vec![Some(1_i64), Some(2_i64), None])
                        .with_timezone("America/New_York"),
                ),
                ArrowCell::TimestampSecond(1),
                ArrowCell::TimestampSecond(2),
            ),
        ];

        for (mapping, array, first, second) in cases {
            assert_eq!(
                extract_arrow_cell(array.as_ref(), &mapping, 0).unwrap(),
                first
            );
            assert_eq!(
                extract_arrow_cell(array.as_ref(), &mapping, 1).unwrap(),
                second
            );
            assert_eq!(
                extract_arrow_cell(array.as_ref(), &mapping, 2).unwrap(),
                ArrowCell::Null
            );
        }
    }

    #[test]
    fn extracts_decimal_arrow_cells_for_all_widths() {
        let decimal32 =
            Decimal32Array::from(vec![Some(12_345_i32), Some(-12_345_i32), Some(0_i32), None])
                .with_precision_and_scale(9, 2)
                .unwrap();
        let decimal64 = Decimal64Array::from(vec![
            Some(1_234_567_890_i64),
            Some(-1_234_567_890_i64),
            Some(0_i64),
            None,
        ])
        .with_precision_and_scale(18, 4)
        .unwrap();
        let decimal128 = Decimal128Array::from(vec![
            Some(123_456_789_012_345_678_901_234_567_890_i128),
            Some(-123_456_789_012_345_678_901_234_567_890_i128),
            Some(0_i128),
            None,
        ])
        .with_precision_and_scale(38, 9)
        .unwrap();
        let decimal256 = Decimal256Array::from(vec![
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
        .unwrap();

        let decimal32_mapping = mapping("decimal32", DataType::Decimal32(9, 2));
        assert_eq!(
            extract_arrow_cell(&decimal32, &decimal32_mapping, 0).unwrap(),
            ArrowCell::Decimal32(12_345)
        );
        assert_eq!(
            extract_arrow_cell(&decimal32, &decimal32_mapping, 1).unwrap(),
            ArrowCell::Decimal32(-12_345)
        );
        assert_eq!(
            extract_arrow_cell(&decimal32, &decimal32_mapping, 2).unwrap(),
            ArrowCell::Decimal32(0)
        );
        assert_eq!(
            extract_arrow_cell(&decimal32, &decimal32_mapping, 3).unwrap(),
            ArrowCell::Null
        );

        let decimal64_mapping = mapping("decimal64", DataType::Decimal64(18, 4));
        assert_eq!(
            extract_arrow_cell(&decimal64, &decimal64_mapping, 0).unwrap(),
            ArrowCell::Decimal64(1_234_567_890)
        );
        assert_eq!(
            extract_arrow_cell(&decimal64, &decimal64_mapping, 1).unwrap(),
            ArrowCell::Decimal64(-1_234_567_890)
        );
        assert_eq!(
            extract_arrow_cell(&decimal64, &decimal64_mapping, 2).unwrap(),
            ArrowCell::Decimal64(0)
        );
        assert_eq!(
            extract_arrow_cell(&decimal64, &decimal64_mapping, 3).unwrap(),
            ArrowCell::Null
        );

        let decimal128_mapping = mapping("decimal128", DataType::Decimal128(38, 9));
        assert_eq!(
            extract_arrow_cell(&decimal128, &decimal128_mapping, 0).unwrap(),
            ArrowCell::Decimal128(123_456_789_012_345_678_901_234_567_890)
        );
        assert_eq!(
            extract_arrow_cell(&decimal128, &decimal128_mapping, 1).unwrap(),
            ArrowCell::Decimal128(-123_456_789_012_345_678_901_234_567_890)
        );
        assert_eq!(
            extract_arrow_cell(&decimal128, &decimal128_mapping, 2).unwrap(),
            ArrowCell::Decimal128(0)
        );
        assert_eq!(
            extract_arrow_cell(&decimal128, &decimal128_mapping, 3).unwrap(),
            ArrowCell::Null
        );

        let decimal256_mapping = mapping("decimal256", DataType::Decimal256(38, 0));
        assert_eq!(
            extract_arrow_cell(&decimal256, &decimal256_mapping, 0).unwrap(),
            ArrowCell::Decimal256(i256::from_i128(123_456_789_012_345_678_901_234_567_890))
        );
        assert_eq!(
            extract_arrow_cell(&decimal256, &decimal256_mapping, 1).unwrap(),
            ArrowCell::Decimal256(i256::from_i128(-123_456_789_012_345_678_901_234_567_890))
        );
        assert_eq!(
            extract_arrow_cell(&decimal256, &decimal256_mapping, 2).unwrap(),
            ArrowCell::Decimal256(i256::ZERO)
        );
        assert_eq!(
            extract_arrow_cell(&decimal256, &decimal256_mapping, 3).unwrap(),
            ArrowCell::Null
        );
    }

    #[test]
    fn extracts_date_arrow_cells() {
        let date32 = Date32Array::from(vec![Some(0_i32), Some(-1_i32), Some(1_i32), None]);
        let date64 = Date64Array::from(vec![Some(0_i64), Some(-1_i64), Some(86_400_123_i64), None]);
        let date32_mapping = mapping("date32", DataType::Date32);
        let date64_mapping = mapping("date64", DataType::Date64);

        assert_eq!(
            extract_arrow_cell(&date32, &date32_mapping, 0).unwrap(),
            ArrowCell::Date32(0)
        );
        assert_eq!(
            extract_arrow_cell(&date32, &date32_mapping, 1).unwrap(),
            ArrowCell::Date32(-1)
        );
        assert_eq!(
            extract_arrow_cell(&date32, &date32_mapping, 2).unwrap(),
            ArrowCell::Date32(1)
        );
        assert_eq!(
            extract_arrow_cell(&date32, &date32_mapping, 3).unwrap(),
            ArrowCell::Null
        );

        assert_eq!(
            extract_arrow_cell(&date64, &date64_mapping, 0).unwrap(),
            ArrowCell::Date64(0)
        );
        assert_eq!(
            extract_arrow_cell(&date64, &date64_mapping, 1).unwrap(),
            ArrowCell::Date64(-1)
        );
        assert_eq!(
            extract_arrow_cell(&date64, &date64_mapping, 2).unwrap(),
            ArrowCell::Date64(86_400_123)
        );
        assert_eq!(
            extract_arrow_cell(&date64, &date64_mapping, 3).unwrap(),
            ArrowCell::Null
        );
    }

    #[test]
    fn extracts_time_arrow_cells() {
        let time32_s = Time32SecondArray::from(vec![Some(0_i32), Some(42_i32), None]);
        let time32_ms = Time32MillisecondArray::from(vec![Some(0_i32), Some(42_123_i32), None]);
        let time64_us = Time64MicrosecondArray::from(vec![Some(0_i64), Some(42_123_456_i64), None]);
        let time64_ns =
            Time64NanosecondArray::from(vec![Some(0_i64), Some(42_123_456_789_i64), None]);

        let time32_s_mapping = mapping("time32_s", DataType::Time32(TimeUnit::Second));
        let time32_ms_mapping = mapping("time32_ms", DataType::Time32(TimeUnit::Millisecond));
        let time64_us_mapping = mapping("time64_us", DataType::Time64(TimeUnit::Microsecond));
        let time64_ns_mapping = mapping("time64_ns", DataType::Time64(TimeUnit::Nanosecond));

        assert_eq!(
            extract_arrow_cell(&time32_s, &time32_s_mapping, 0).unwrap(),
            ArrowCell::Time32Second(0)
        );
        assert_eq!(
            extract_arrow_cell(&time32_s, &time32_s_mapping, 1).unwrap(),
            ArrowCell::Time32Second(42)
        );
        assert_eq!(
            extract_arrow_cell(&time32_s, &time32_s_mapping, 2).unwrap(),
            ArrowCell::Null
        );

        assert_eq!(
            extract_arrow_cell(&time32_ms, &time32_ms_mapping, 1).unwrap(),
            ArrowCell::Time32Millisecond(42_123)
        );
        assert_eq!(
            extract_arrow_cell(&time64_us, &time64_us_mapping, 1).unwrap(),
            ArrowCell::Time64Microsecond(42_123_456)
        );
        assert_eq!(
            extract_arrow_cell(&time64_ns, &time64_ns_mapping, 1).unwrap(),
            ArrowCell::Time64Nanosecond(42_123_456_789)
        );
    }

    fn mapping(name: &str, data_type: DataType) -> SchemaMapping {
        SchemaMapping::new(
            ArrowFieldRef::new(0, name.to_owned(), true, data_type),
            MssqlColumn::new(Identifier::new(name).unwrap(), MssqlType::Int, true),
        )
    }
}

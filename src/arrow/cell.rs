//! Arrow runtime cell values.

use arrow_array::{
    Array, BinaryArray, BooleanArray, Date32Array, Date64Array, Decimal32Array, Decimal64Array,
    Decimal128Array, Decimal256Array, Float32Array, Float64Array, Int8Array, Int16Array,
    Int32Array, Int64Array, LargeBinaryArray, LargeStringArray, StringArray,
    TimestampMicrosecondArray, TimestampMillisecondArray, TimestampNanosecondArray,
    TimestampSecondArray, UInt8Array, UInt16Array, UInt32Array, UInt64Array,
};
use arrow_buffer::i256;
use arrow_schema::{DataType, TimeUnit};

use crate::{Diagnostic, DiagnosticCode, DiagnosticSet, FieldRef, Result, SchemaMapping};

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
        DataType::Float32 => {
            let array = downcast_array::<Float32Array>(array, mapping, row_index)?;
            Ok(ArrowCell::Float32(array.value(row_index)))
        }
        DataType::Float64 => {
            let array = downcast_array::<Float64Array>(array, mapping, row_index)?;
            Ok(ArrowCell::Float64(array.value(row_index)))
        }
        DataType::Utf8 => {
            let array = downcast_array::<StringArray>(array, mapping, row_index)?;
            Ok(ArrowCell::Utf8(array.value(row_index)))
        }
        DataType::LargeUtf8 => {
            let array = downcast_array::<LargeStringArray>(array, mapping, row_index)?;
            Ok(ArrowCell::Utf8(array.value(row_index)))
        }
        DataType::Binary => {
            let array = downcast_array::<BinaryArray>(array, mapping, row_index)?;
            Ok(ArrowCell::Binary(array.value(row_index)))
        }
        DataType::LargeBinary => {
            let array = downcast_array::<LargeBinaryArray>(array, mapping, row_index)?;
            Ok(ArrowCell::Binary(array.value(row_index)))
        }
        other => Err(unsupported_value_conversion(
            mapping,
            row_index,
            format!("Arrow value extraction for {other} is not supported yet"),
        )),
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

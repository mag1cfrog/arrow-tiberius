use crate::Result;

use super::super::types::{
    decimal::{
        append_decimal32_cell, append_decimal64_cell, append_decimal128_cell,
        append_decimal256_cell,
    },
    fixed_size_binary::append_fixed_size_binary_cell,
    primitive::{
        append_boolean_cell, append_float32_cell, append_float64_cell, append_int8_cell,
        append_int16_cell, append_int32_cell, append_int64_cell, append_uint8_cell,
        append_uint16_cell, append_uint32_cell, append_uint64_checked_bigint_cell,
    },
    temporal::{
        append_date32_cell, append_date64_cell, append_datetimeoffset_microsecond_cell,
        append_datetimeoffset_millisecond_cell, append_datetimeoffset_nanosecond_cell,
        append_datetimeoffset_second_cell, append_time32_millisecond_cell,
        append_time32_second_cell, append_time64_microsecond_cell, append_time64_nanosecond_cell,
        append_timestamp_microsecond_cell, append_timestamp_millisecond_cell,
        append_timestamp_nanosecond_cell, append_timestamp_second_cell,
    },
    uint64::append_uint64_decimal20_cell,
    variable_width::{append_nvarchar_cell, append_varbinary_cell},
};
use super::BoundDirectColumn;

impl BoundDirectColumn<'_> {
    pub(crate) fn append_cell(
        &self,
        buf: &mut tiberius::RawRowsAppendBuffer<'_>,
        row_index: usize,
        measured_len: usize,
    ) -> Result<()> {
        match self {
            Self::Boolean { column, array } => {
                append_boolean_cell(buf, array, column, row_index, measured_len)
            }
            Self::UInt8 { column, array } => {
                append_uint8_cell(buf, array, column, row_index, measured_len)
            }
            Self::Int8 { column, array } => {
                append_int8_cell(buf, array, column, row_index, measured_len)
            }
            Self::Int16 { column, array } => {
                append_int16_cell(buf, array, column, row_index, measured_len)
            }
            Self::Int32 { column, array } => {
                append_int32_cell(buf, array, column, row_index, measured_len)
            }
            Self::UInt16 { column, array } => {
                append_uint16_cell(buf, array, column, row_index, measured_len)
            }
            Self::Int64 { column, array } => {
                append_int64_cell(buf, array, column, row_index, measured_len)
            }
            Self::UInt32 { column, array } => {
                append_uint32_cell(buf, array, column, row_index, measured_len)
            }
            Self::UInt64 { column, array } => {
                append_uint64_checked_bigint_cell(buf, array, column, row_index, measured_len)
            }
            Self::UInt64Decimal20_0 { column, array } => {
                append_uint64_decimal20_cell(buf, array, column, row_index, measured_len)
            }
            Self::Decimal32 {
                column,
                classification,
                array,
            } => {
                append_decimal32_cell(buf, array, column, *classification, row_index, measured_len)
            }
            Self::Decimal64 {
                column,
                classification,
                array,
            } => {
                append_decimal64_cell(buf, array, column, *classification, row_index, measured_len)
            }
            Self::Decimal128 {
                column,
                classification,
                array,
            } => {
                append_decimal128_cell(buf, array, column, *classification, row_index, measured_len)
            }
            Self::Decimal256 {
                column,
                classification,
                array,
            } => {
                append_decimal256_cell(buf, array, column, *classification, row_index, measured_len)
            }
            Self::Float32 { column, array } => {
                append_float32_cell(buf, array, column, row_index, measured_len)
            }
            Self::Float64 { column, array } => {
                append_float64_cell(buf, array, column, row_index, measured_len)
            }
            Self::Utf8 { column, array } => {
                append_nvarchar_cell(buf, array, column, row_index, measured_len)
            }
            Self::LargeUtf8 { column, array } => {
                append_nvarchar_cell(buf, array, column, row_index, measured_len)
            }
            Self::Binary { column, array } => {
                append_varbinary_cell(buf, array, column, row_index, measured_len)
            }
            Self::LargeBinary { column, array } => {
                append_varbinary_cell(buf, array, column, row_index, measured_len)
            }
            Self::FixedSizeBinary {
                column,
                classification,
                array,
            } => append_fixed_size_binary_cell(
                buf,
                array,
                column,
                *classification,
                row_index,
                measured_len,
            ),
            Self::Date32 {
                column,
                mapping,
                array,
            } => append_date32_cell(buf, array, mapping, column, row_index, measured_len),
            Self::Date64 {
                column,
                mapping,
                array,
            } => append_date64_cell(buf, array, mapping, column, row_index, measured_len),
            Self::TimestampSecond {
                column,
                mapping,
                array,
            } => append_timestamp_second_cell(buf, array, mapping, column, row_index, measured_len),
            Self::TimestampMillisecond {
                column,
                mapping,
                array,
            } => append_timestamp_millisecond_cell(
                buf,
                array,
                mapping,
                column,
                row_index,
                measured_len,
            ),
            Self::TimestampMicrosecond {
                column,
                mapping,
                array,
            } => append_timestamp_microsecond_cell(
                buf,
                array,
                mapping,
                column,
                row_index,
                measured_len,
            ),
            Self::TimestampNanosecond {
                column,
                mapping,
                nanosecond_policy,
                array,
            } => append_timestamp_nanosecond_cell(
                buf,
                array,
                mapping,
                column,
                *nanosecond_policy,
                row_index,
                measured_len,
            ),
            Self::Time32Second {
                column,
                mapping,
                array,
            } => append_time32_second_cell(buf, array, mapping, column, row_index, measured_len),
            Self::Time32Millisecond {
                column,
                mapping,
                array,
            } => {
                append_time32_millisecond_cell(buf, array, mapping, column, row_index, measured_len)
            }
            Self::Time64Microsecond {
                column,
                mapping,
                array,
            } => {
                append_time64_microsecond_cell(buf, array, mapping, column, row_index, measured_len)
            }
            Self::Time64Nanosecond {
                column,
                mapping,
                nanosecond_policy,
                array,
            } => append_time64_nanosecond_cell(
                buf,
                array,
                mapping,
                column,
                *nanosecond_policy,
                row_index,
                measured_len,
            ),
            Self::DateTimeOffsetSecond {
                column,
                mapping,
                array,
            } => append_datetimeoffset_second_cell(
                buf,
                array,
                mapping,
                column,
                row_index,
                measured_len,
            ),
            Self::DateTimeOffsetMillisecond {
                column,
                mapping,
                array,
            } => append_datetimeoffset_millisecond_cell(
                buf,
                array,
                mapping,
                column,
                row_index,
                measured_len,
            ),
            Self::DateTimeOffsetMicrosecond {
                column,
                mapping,
                array,
            } => append_datetimeoffset_microsecond_cell(
                buf,
                array,
                mapping,
                column,
                row_index,
                measured_len,
            ),
            Self::DateTimeOffsetNanosecond {
                column,
                mapping,
                nanosecond_policy,
                array,
            } => append_datetimeoffset_nanosecond_cell(
                buf,
                array,
                mapping,
                column,
                *nanosecond_policy,
                row_index,
                measured_len,
            ),
        }
    }
}

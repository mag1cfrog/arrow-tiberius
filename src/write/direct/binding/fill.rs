use crate::{Result, write::context::RuntimeConversionContext};

use super::super::{
    layout::RowLayout,
    types::{
        decimal::{
            fill_decimal32_column, fill_decimal64_column, fill_decimal128_column,
            fill_decimal256_column,
        },
        fixed_size_binary::fill_fixed_size_binary_column,
        primitive::{
            fill_boolean_column, fill_float16_column, fill_float32_column, fill_float64_column,
            fill_int8_column, fill_int16_column, fill_int32_column, fill_int64_column,
            fill_uint8_column, fill_uint16_column, fill_uint32_column,
            fill_uint64_checked_bigint_column,
        },
        temporal::{
            TemporalColumnContext, fill_date32_direct_column, fill_date64_direct_column,
            fill_datetimeoffset_microsecond_direct_column,
            fill_datetimeoffset_millisecond_direct_column,
            fill_datetimeoffset_nanosecond_direct_column, fill_datetimeoffset_second_direct_column,
            fill_time32_millisecond_direct_column, fill_time32_second_direct_column,
            fill_time64_microsecond_direct_column, fill_time64_nanosecond_direct_column,
            fill_timestamp_microsecond_direct_column, fill_timestamp_millisecond_direct_column,
            fill_timestamp_nanosecond_direct_column, fill_timestamp_second_direct_column,
        },
        uint64::fill_uint64_decimal20_column,
        variable_width::{fill_nvarchar_column, fill_varbinary_column},
    },
};
use super::BoundDirectColumn;

impl BoundDirectColumn<'_> {
    pub(crate) fn fill_column(
        &self,
        runtime_context: RuntimeConversionContext,
        column_index: usize,
        column_count: usize,
        layout: &RowLayout,
        bytes: &mut [u8],
    ) -> Result<()> {
        match self {
            Self::Boolean { column, array } => {
                fill_boolean_column(array, column, column_index, column_count, layout, bytes)
            }
            Self::UInt8 { column, array } => {
                fill_uint8_column(array, column, column_index, column_count, layout, bytes)
            }
            Self::Int8 { column, array } => {
                fill_int8_column(array, column, column_index, column_count, layout, bytes)
            }
            Self::Int16 { column, array } => {
                fill_int16_column(array, column, column_index, column_count, layout, bytes)
            }
            Self::Int32 { column, array } => {
                fill_int32_column(array, column, column_index, column_count, layout, bytes)
            }
            Self::UInt16 { column, array } => {
                fill_uint16_column(array, column, column_index, column_count, layout, bytes)
            }
            Self::Int64 { column, array } => {
                fill_int64_column(array, column, column_index, column_count, layout, bytes)
            }
            Self::UInt32 { column, array } => {
                fill_uint32_column(array, column, column_index, column_count, layout, bytes)
            }
            Self::UInt64 { column, array } => fill_uint64_checked_bigint_column(
                array,
                column,
                column_index,
                column_count,
                layout,
                bytes,
            ),
            Self::UInt64Decimal20_0 { column, array } => fill_uint64_decimal20_column(
                array,
                column,
                column_index,
                column_count,
                layout,
                bytes,
            ),
            Self::Decimal32 {
                column,
                classification,
                array,
            } => fill_decimal32_column(
                array,
                column,
                *classification,
                column_index,
                column_count,
                layout,
                bytes,
            ),
            Self::Decimal64 {
                column,
                classification,
                array,
            } => fill_decimal64_column(
                array,
                column,
                *classification,
                column_index,
                column_count,
                layout,
                bytes,
            ),
            Self::Decimal128 {
                column,
                classification,
                array,
            } => fill_decimal128_column(
                array,
                column,
                *classification,
                column_index,
                column_count,
                layout,
                bytes,
            ),
            Self::Decimal256 {
                column,
                classification,
                array,
            } => fill_decimal256_column(
                array,
                column,
                *classification,
                column_index,
                column_count,
                layout,
                bytes,
            ),
            Self::Float16 { column, array } => {
                fill_float16_column(array, column, column_index, column_count, layout, bytes)
            }
            Self::Float32 { column, array } => {
                fill_float32_column(array, column, column_index, column_count, layout, bytes)
            }
            Self::Float64 { column, array } => {
                fill_float64_column(array, column, column_index, column_count, layout, bytes)
            }
            Self::Utf8 { column, array } => {
                fill_nvarchar_column(*array, column, column_index, column_count, layout, bytes)
            }
            Self::LargeUtf8 { column, array } => {
                fill_nvarchar_column(*array, column, column_index, column_count, layout, bytes)
            }
            Self::Utf8View { column, array } => {
                fill_nvarchar_column(*array, column, column_index, column_count, layout, bytes)
            }
            Self::Binary { column, array } => {
                fill_varbinary_column(*array, column, column_index, column_count, layout, bytes)
            }
            Self::LargeBinary { column, array } => {
                fill_varbinary_column(*array, column, column_index, column_count, layout, bytes)
            }
            Self::BinaryView { column, array } => {
                fill_varbinary_column(*array, column, column_index, column_count, layout, bytes)
            }
            Self::FixedSizeBinary {
                column,
                classification,
                array,
            } => fill_fixed_size_binary_column(
                array,
                column,
                *classification,
                column_index,
                column_count,
                layout,
                bytes,
            ),
            Self::Date32 {
                column,
                mapping,
                array,
            } => fill_date32_direct_column(
                array,
                TemporalColumnContext {
                    mapping,
                    runtime_context,
                    column,
                    column_index,
                    column_count,
                },
                layout,
                bytes,
            ),
            Self::Date64 {
                column,
                mapping,
                array,
            } => fill_date64_direct_column(
                array,
                TemporalColumnContext {
                    mapping,
                    runtime_context,
                    column,
                    column_index,
                    column_count,
                },
                layout,
                bytes,
            ),
            Self::TimestampSecond {
                column,
                mapping,
                array,
            } => fill_timestamp_second_direct_column(
                array,
                TemporalColumnContext {
                    mapping,
                    runtime_context,
                    column,
                    column_index,
                    column_count,
                },
                layout,
                bytes,
            ),
            Self::TimestampMillisecond {
                column,
                mapping,
                array,
            } => fill_timestamp_millisecond_direct_column(
                array,
                TemporalColumnContext {
                    mapping,
                    runtime_context,
                    column,
                    column_index,
                    column_count,
                },
                layout,
                bytes,
            ),
            Self::TimestampMicrosecond {
                column,
                mapping,
                array,
            } => fill_timestamp_microsecond_direct_column(
                array,
                TemporalColumnContext {
                    mapping,
                    runtime_context,
                    column,
                    column_index,
                    column_count,
                },
                layout,
                bytes,
            ),
            Self::TimestampNanosecond {
                column,
                mapping,
                array,
            } => fill_timestamp_nanosecond_direct_column(
                array,
                TemporalColumnContext {
                    mapping,
                    runtime_context,
                    column,
                    column_index,
                    column_count,
                },
                layout,
                bytes,
            ),
            Self::Time32Second {
                column,
                mapping,
                array,
            } => fill_time32_second_direct_column(
                array,
                TemporalColumnContext {
                    mapping,
                    runtime_context,
                    column,
                    column_index,
                    column_count,
                },
                layout,
                bytes,
            ),
            Self::Time32Millisecond {
                column,
                mapping,
                array,
            } => fill_time32_millisecond_direct_column(
                array,
                TemporalColumnContext {
                    mapping,
                    runtime_context,
                    column,
                    column_index,
                    column_count,
                },
                layout,
                bytes,
            ),
            Self::Time64Microsecond {
                column,
                mapping,
                array,
            } => fill_time64_microsecond_direct_column(
                array,
                TemporalColumnContext {
                    mapping,
                    runtime_context,
                    column,
                    column_index,
                    column_count,
                },
                layout,
                bytes,
            ),
            Self::Time64Nanosecond {
                column,
                mapping,
                array,
            } => fill_time64_nanosecond_direct_column(
                array,
                TemporalColumnContext {
                    mapping,
                    runtime_context,
                    column,
                    column_index,
                    column_count,
                },
                layout,
                bytes,
            ),
            Self::DateTimeOffsetSecond {
                column,
                mapping,
                array,
            } => fill_datetimeoffset_second_direct_column(
                array,
                TemporalColumnContext {
                    mapping,
                    runtime_context,
                    column,
                    column_index,
                    column_count,
                },
                layout,
                bytes,
            ),
            Self::DateTimeOffsetMillisecond {
                column,
                mapping,
                array,
            } => fill_datetimeoffset_millisecond_direct_column(
                array,
                TemporalColumnContext {
                    mapping,
                    runtime_context,
                    column,
                    column_index,
                    column_count,
                },
                layout,
                bytes,
            ),
            Self::DateTimeOffsetMicrosecond {
                column,
                mapping,
                array,
            } => fill_datetimeoffset_microsecond_direct_column(
                array,
                TemporalColumnContext {
                    mapping,
                    runtime_context,
                    column,
                    column_index,
                    column_count,
                },
                layout,
                bytes,
            ),
            Self::DateTimeOffsetNanosecond {
                column,
                mapping,
                array,
            } => fill_datetimeoffset_nanosecond_direct_column(
                array,
                TemporalColumnContext {
                    mapping,
                    runtime_context,
                    column,
                    column_index,
                    column_count,
                },
                layout,
                bytes,
            ),
        }
    }
}

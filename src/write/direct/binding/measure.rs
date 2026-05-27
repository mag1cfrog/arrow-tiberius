//! Bound direct column measurement dispatch.

use arrow_array::Array;

use crate::{PlanOptions, Result};

use super::super::{
    plan,
    types::{
        decimal::{
            measure_decimal32_column_cell_lengths, measure_decimal64_column_cell_lengths,
            measure_decimal128_column_cell_lengths, measure_decimal256_column_cell_lengths,
        },
        fixed_size_binary::measure_fixed_size_binary_column_cell_lengths,
        primitive::{
            measure_fixed_primitive_column_cell_lengths, measure_float16_column_cell_lengths,
            measure_float32_column_cell_lengths, measure_float64_column_cell_lengths,
            measure_uint64_checked_bigint_column_cell_lengths,
        },
        temporal::{
            TemporalColumnContext, measure_date32_column_cell_lengths,
            measure_date64_column_cell_lengths,
            measure_datetimeoffset_microsecond_column_cell_lengths,
            measure_datetimeoffset_millisecond_column_cell_lengths,
            measure_datetimeoffset_nanosecond_column_cell_lengths,
            measure_datetimeoffset_second_column_cell_lengths,
            measure_time32_millisecond_column_cell_lengths,
            measure_time32_second_column_cell_lengths,
            measure_time64_microsecond_column_cell_lengths,
            measure_time64_nanosecond_column_cell_lengths,
            measure_timestamp_microsecond_column_cell_lengths,
            measure_timestamp_millisecond_column_cell_lengths,
            measure_timestamp_nanosecond_column_cell_lengths,
            measure_timestamp_second_column_cell_lengths,
        },
        uint64::measure_uint64_decimal20_cell_lengths,
        variable_width::{
            measure_nvarchar_column_cell_lengths, measure_varbinary_column_cell_lengths,
        },
    },
};
use super::BoundDirectColumn;

impl BoundDirectColumn<'_> {
    pub(crate) fn measure_cell_lengths(
        &self,
        column_index: usize,
        column_count: usize,
        cell_lengths: &mut [usize],
    ) -> Result<()> {
        let default_options = PlanOptions::default();

        match self {
            Self::Boolean { column, array } => measure_primitive_bound_column(
                *array,
                column,
                column_index,
                column_count,
                cell_lengths,
            ),
            Self::UInt8 { column, array } => measure_primitive_bound_column(
                *array,
                column,
                column_index,
                column_count,
                cell_lengths,
            ),
            Self::Int8 { column, array } => measure_primitive_bound_column(
                *array,
                column,
                column_index,
                column_count,
                cell_lengths,
            ),
            Self::Int16 { column, array } => measure_primitive_bound_column(
                *array,
                column,
                column_index,
                column_count,
                cell_lengths,
            ),
            Self::Int32 { column, array } => measure_primitive_bound_column(
                *array,
                column,
                column_index,
                column_count,
                cell_lengths,
            ),
            Self::UInt16 { column, array } => measure_primitive_bound_column(
                *array,
                column,
                column_index,
                column_count,
                cell_lengths,
            ),
            Self::Int64 { column, array } => measure_primitive_bound_column(
                *array,
                column,
                column_index,
                column_count,
                cell_lengths,
            ),
            Self::UInt32 { column, array } => measure_primitive_bound_column(
                *array,
                column,
                column_index,
                column_count,
                cell_lengths,
            ),
            Self::UInt64 { column, array } => measure_uint64_checked_bigint_column_cell_lengths(
                array,
                column,
                column_index,
                column_count,
                cell_lengths,
            ),
            Self::Float16 { column, array } => measure_float16_column_cell_lengths(
                array,
                column,
                column_index,
                column_count,
                cell_lengths,
            ),
            Self::Float32 { column, array } => measure_float32_column_cell_lengths(
                array,
                column,
                column_index,
                column_count,
                cell_lengths,
            ),
            Self::Float64 { column, array } => measure_float64_column_cell_lengths(
                array,
                column,
                column_index,
                column_count,
                cell_lengths,
            ),
            Self::UInt64Decimal20_0 { column, array } => measure_uint64_decimal20_cell_lengths(
                array,
                column,
                column_index,
                column_count,
                cell_lengths,
            ),
            Self::Decimal32 {
                column,
                classification,
                array,
            } => measure_decimal32_column_cell_lengths(
                array,
                column,
                *classification,
                column_index,
                column_count,
                cell_lengths,
            ),
            Self::Decimal64 {
                column,
                classification,
                array,
            } => measure_decimal64_column_cell_lengths(
                array,
                column,
                *classification,
                column_index,
                column_count,
                cell_lengths,
            ),
            Self::Decimal128 {
                column,
                classification,
                array,
            } => measure_decimal128_column_cell_lengths(
                array,
                column,
                *classification,
                column_index,
                column_count,
                cell_lengths,
            ),
            Self::Decimal256 {
                column,
                classification,
                array,
            } => measure_decimal256_column_cell_lengths(
                array,
                column,
                *classification,
                column_index,
                column_count,
                cell_lengths,
            ),
            Self::Utf8 { column, array } => measure_nvarchar_column_cell_lengths(
                array,
                column,
                column_index,
                column_count,
                cell_lengths,
            ),
            Self::LargeUtf8 { column, array } => measure_nvarchar_column_cell_lengths(
                array,
                column,
                column_index,
                column_count,
                cell_lengths,
            ),
            Self::Binary { column, array } => measure_varbinary_column_cell_lengths(
                array,
                column,
                column_index,
                column_count,
                cell_lengths,
            ),
            Self::LargeBinary { column, array } => measure_varbinary_column_cell_lengths(
                array,
                column,
                column_index,
                column_count,
                cell_lengths,
            ),
            Self::FixedSizeBinary {
                column,
                classification,
                array,
            } => measure_fixed_size_binary_column_cell_lengths(
                array,
                column,
                *classification,
                column_index,
                column_count,
                cell_lengths,
            ),
            Self::Date32 {
                column,
                mapping,
                array,
            } => measure_date32_column_cell_lengths(
                array,
                TemporalColumnContext {
                    mapping,
                    plan_options: default_options,
                    column,
                    column_index,
                    column_count,
                },
                cell_lengths,
            ),
            Self::Date64 {
                column,
                mapping,
                array,
            } => measure_date64_column_cell_lengths(
                array,
                TemporalColumnContext {
                    mapping,
                    plan_options: default_options,
                    column,
                    column_index,
                    column_count,
                },
                cell_lengths,
            ),
            Self::TimestampSecond {
                column,
                mapping,
                array,
            } => measure_timestamp_second_column_cell_lengths(
                array,
                TemporalColumnContext {
                    mapping,
                    plan_options: default_options,
                    column,
                    column_index,
                    column_count,
                },
                cell_lengths,
            ),
            Self::TimestampMillisecond {
                column,
                mapping,
                array,
            } => measure_timestamp_millisecond_column_cell_lengths(
                array,
                TemporalColumnContext {
                    mapping,
                    plan_options: default_options,
                    column,
                    column_index,
                    column_count,
                },
                cell_lengths,
            ),
            Self::TimestampMicrosecond {
                column,
                mapping,
                array,
            } => measure_timestamp_microsecond_column_cell_lengths(
                array,
                TemporalColumnContext {
                    mapping,
                    plan_options: default_options,
                    column,
                    column_index,
                    column_count,
                },
                cell_lengths,
            ),
            Self::TimestampNanosecond {
                column,
                mapping,
                nanosecond_policy,
                array,
            } => measure_timestamp_nanosecond_column_cell_lengths(
                array,
                TemporalColumnContext {
                    mapping,
                    plan_options: PlanOptions {
                        nanosecond_policy: *nanosecond_policy,
                        ..Default::default()
                    },
                    column,
                    column_index,
                    column_count,
                },
                cell_lengths,
            ),
            Self::Time32Second {
                column,
                mapping,
                array,
            } => measure_time32_second_column_cell_lengths(
                array,
                TemporalColumnContext {
                    mapping,
                    plan_options: default_options,
                    column,
                    column_index,
                    column_count,
                },
                cell_lengths,
            ),
            Self::Time32Millisecond {
                column,
                mapping,
                array,
            } => measure_time32_millisecond_column_cell_lengths(
                array,
                TemporalColumnContext {
                    mapping,
                    plan_options: default_options,
                    column,
                    column_index,
                    column_count,
                },
                cell_lengths,
            ),
            Self::Time64Microsecond {
                column,
                mapping,
                array,
            } => measure_time64_microsecond_column_cell_lengths(
                array,
                TemporalColumnContext {
                    mapping,
                    plan_options: default_options,
                    column,
                    column_index,
                    column_count,
                },
                cell_lengths,
            ),
            Self::Time64Nanosecond {
                column,
                mapping,
                nanosecond_policy,
                array,
            } => measure_time64_nanosecond_column_cell_lengths(
                array,
                TemporalColumnContext {
                    mapping,
                    plan_options: PlanOptions {
                        nanosecond_policy: *nanosecond_policy,
                        ..Default::default()
                    },
                    column,
                    column_index,
                    column_count,
                },
                cell_lengths,
            ),
            Self::DateTimeOffsetSecond {
                column,
                mapping,
                array,
            } => measure_datetimeoffset_second_column_cell_lengths(
                array,
                TemporalColumnContext {
                    mapping,
                    plan_options: default_options,
                    column,
                    column_index,
                    column_count,
                },
                cell_lengths,
            ),
            Self::DateTimeOffsetMillisecond {
                column,
                mapping,
                array,
            } => measure_datetimeoffset_millisecond_column_cell_lengths(
                array,
                TemporalColumnContext {
                    mapping,
                    plan_options: default_options,
                    column,
                    column_index,
                    column_count,
                },
                cell_lengths,
            ),
            Self::DateTimeOffsetMicrosecond {
                column,
                mapping,
                array,
            } => measure_datetimeoffset_microsecond_column_cell_lengths(
                array,
                TemporalColumnContext {
                    mapping,
                    plan_options: default_options,
                    column,
                    column_index,
                    column_count,
                },
                cell_lengths,
            ),
            Self::DateTimeOffsetNanosecond {
                column,
                mapping,
                nanosecond_policy,
                array,
            } => measure_datetimeoffset_nanosecond_column_cell_lengths(
                array,
                TemporalColumnContext {
                    mapping,
                    plan_options: PlanOptions {
                        nanosecond_policy: *nanosecond_policy,
                        ..Default::default()
                    },
                    column,
                    column_index,
                    column_count,
                },
                cell_lengths,
            ),
        }
    }
}

fn measure_primitive_bound_column(
    array: &impl Array,
    column: &plan::DirectColumnPlan,
    column_index: usize,
    column_count: usize,
    cell_lengths: &mut [usize],
) -> Result<()> {
    measure_fixed_primitive_column_cell_lengths(
        array,
        column,
        column_index,
        column_count,
        cell_lengths,
    )
}

//! Bound runtime direct TDS columns.

use arrow_array::{
    Array, BinaryArray, BooleanArray, Date32Array, Date64Array, Decimal32Array, Decimal64Array,
    Decimal128Array, Decimal256Array, Float32Array, Float64Array, Int8Array, Int16Array,
    Int32Array, Int64Array, RecordBatch, StringArray, Time32MillisecondArray, Time32SecondArray,
    Time64MicrosecondArray, Time64NanosecondArray, TimestampMicrosecondArray,
    TimestampMillisecondArray, TimestampNanosecondArray, TimestampSecondArray, UInt8Array,
    UInt16Array, UInt32Array, UInt64Array,
};

use super::{
    DirectEncoder, downcast_direct_array,
    layout::{RowLayout, build_fixed_width_row_layout},
    plan,
    plan::DirectColumnEncoding,
    row_column_diagnostic,
    types::{
        decimal::{
            append_decimal32_cell, append_decimal64_cell, append_decimal128_cell,
            append_decimal256_cell, fill_decimal32_column, fill_decimal64_column,
            fill_decimal128_column, fill_decimal256_column, measure_decimal32_column_cell_lengths,
            measure_decimal64_column_cell_lengths, measure_decimal128_column_cell_lengths,
            measure_decimal256_column_cell_lengths,
        },
        primitive::{
            append_boolean_cell, append_float32_cell, append_float64_cell, append_int8_cell,
            append_int16_cell, append_int32_cell, append_int64_cell, append_uint8_cell,
            append_uint16_cell, append_uint32_cell, append_uint64_checked_bigint_cell,
            fill_boolean_column, fill_float32_column, fill_float64_column, fill_int8_column,
            fill_int16_column, fill_int32_column, fill_int64_column, fill_uint8_column,
            fill_uint16_column, fill_uint32_column, fill_uint64_checked_bigint_column,
            measure_float32_column_cell_lengths, measure_float64_column_cell_lengths,
            measure_primitive_column_cell_lengths,
            measure_uint64_checked_bigint_column_cell_lengths,
        },
        temporal::{
            TemporalColumnContext, append_date32_cell, append_date64_cell,
            append_datetimeoffset_microsecond_cell, append_datetimeoffset_millisecond_cell,
            append_datetimeoffset_nanosecond_cell, append_datetimeoffset_second_cell,
            append_time32_millisecond_cell, append_time32_second_cell,
            append_time64_microsecond_cell, append_time64_nanosecond_cell,
            append_timestamp_microsecond_cell, append_timestamp_millisecond_cell,
            append_timestamp_nanosecond_cell, append_timestamp_second_cell,
            fill_date32_direct_column, fill_date64_direct_column,
            fill_datetimeoffset_microsecond_direct_column,
            fill_datetimeoffset_millisecond_direct_column,
            fill_datetimeoffset_nanosecond_direct_column, fill_datetimeoffset_second_direct_column,
            fill_time32_millisecond_direct_column, fill_time32_second_direct_column,
            fill_time64_microsecond_direct_column, fill_time64_nanosecond_direct_column,
            fill_timestamp_microsecond_direct_column, fill_timestamp_millisecond_direct_column,
            fill_timestamp_nanosecond_direct_column, fill_timestamp_second_direct_column,
            measure_date32_column_cell_lengths, measure_date64_column_cell_lengths,
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
        uint64::{
            append_uint64_decimal20_cell, fill_uint64_decimal20_column,
            measure_uint64_decimal20_cell_lengths,
        },
        variable_width::{
            append_nvarchar_cell, append_varbinary_cell, fill_nvarchar_column,
            fill_varbinary_column, measure_nvarchar_column_cell_lengths,
            measure_varbinary_column_cell_lengths,
        },
    },
    unsupported_batch, value_conversion_error,
};
use crate::{
    DiagnosticCode, NanosecondPolicy, PlanOptions, Result, SchemaMapping,
    conversion::arrow_to_mssql::{
        decimal::DecimalArrowToMssql, primitive::PrimitiveArrowToMssql,
        temporal::TemporalArrowToMssql, variable_width::VariableWidthArrowToMssql,
    },
};

pub(crate) struct BoundDirectBatch<'a> {
    columns: Vec<BoundDirectColumn<'a>>,
    row_count: usize,
}

impl<'a> BoundDirectBatch<'a> {
    pub(crate) fn new(encoder: &'a DirectEncoder, batch: &'a RecordBatch) -> Result<Self> {
        Ok(Self {
            columns: bind_direct_columns(encoder, batch)?,
            row_count: batch.num_rows(),
        })
    }

    pub(crate) fn columns(&self) -> &[BoundDirectColumn<'a>] {
        &self.columns
    }

    pub(crate) const fn row_count(&self) -> usize {
        self.row_count
    }

    pub(crate) fn measure_cell_lengths(&self) -> Result<Vec<usize>> {
        if self.row_count == 0 {
            return Ok(Vec::new());
        }

        let column_count = self.columns.len();
        let mut cell_lengths = vec![0; self.row_count * column_count];

        for (column_index, column) in self.columns.iter().enumerate() {
            column.measure_cell_lengths(column_index, column_count, &mut cell_lengths)?;
        }

        Ok(cell_lengths)
    }

    pub(crate) fn measure_layout(&self) -> Result<RowLayout> {
        if self.row_count == 0 {
            return RowLayout::new(Vec::new(), Vec::new(), Vec::new(), 0);
        }

        let cell_lengths = self.measure_cell_lengths()?;
        build_fixed_width_row_layout(self.row_count, self.columns.len(), &cell_lengths)
    }

    pub(crate) fn fill_columns(&self, layout: &RowLayout, bytes: &mut [u8]) -> Result<()> {
        let column_count = self.columns.len();

        for (column_index, column) in self.columns.iter().enumerate() {
            column.fill_column(column_index, column_count, layout, bytes)?;
        }

        Ok(())
    }
}

fn bind_direct_columns<'a>(
    encoder: &'a DirectEncoder,
    batch: &'a RecordBatch,
) -> Result<Vec<BoundDirectColumn<'a>>> {
    let mut columns = Vec::with_capacity(encoder.plan.column_count());

    for (column_index, column) in encoder.plan.columns().iter().enumerate() {
        let Some(array) = batch
            .columns()
            .get(column.source_index())
            .map(AsRef::as_ref)
        else {
            return Err(value_conversion_error(row_column_diagnostic(
                column,
                0,
                DiagnosticCode::ValueTypeMismatch,
                "planned direct column index is outside the runtime batch",
            )));
        };

        let runtime = match column.encoding() {
            DirectColumnEncoding::Primitive(PrimitiveArrowToMssql::BooleanToBit) => {
                BoundDirectColumn::Boolean {
                    column,
                    array: downcast_direct_array::<BooleanArray>(array, column)?,
                }
            }
            DirectColumnEncoding::Primitive(PrimitiveArrowToMssql::UInt8ToTinyInt) => {
                BoundDirectColumn::UInt8 {
                    column,
                    array: downcast_direct_array::<UInt8Array>(array, column)?,
                }
            }
            DirectColumnEncoding::Primitive(PrimitiveArrowToMssql::Int8ToSmallInt) => {
                BoundDirectColumn::Int8 {
                    column,
                    array: downcast_direct_array::<Int8Array>(array, column)?,
                }
            }
            DirectColumnEncoding::Primitive(PrimitiveArrowToMssql::Int16ToSmallInt) => {
                BoundDirectColumn::Int16 {
                    column,
                    array: downcast_direct_array::<Int16Array>(array, column)?,
                }
            }
            DirectColumnEncoding::Primitive(PrimitiveArrowToMssql::Int32ToInt) => {
                BoundDirectColumn::Int32 {
                    column,
                    array: downcast_direct_array::<Int32Array>(array, column)?,
                }
            }
            DirectColumnEncoding::Primitive(PrimitiveArrowToMssql::UInt16ToInt) => {
                BoundDirectColumn::UInt16 {
                    column,
                    array: downcast_direct_array::<UInt16Array>(array, column)?,
                }
            }
            DirectColumnEncoding::Primitive(PrimitiveArrowToMssql::Int64ToBigInt) => {
                BoundDirectColumn::Int64 {
                    column,
                    array: downcast_direct_array::<Int64Array>(array, column)?,
                }
            }
            DirectColumnEncoding::Primitive(PrimitiveArrowToMssql::UInt32ToBigInt) => {
                BoundDirectColumn::UInt32 {
                    column,
                    array: downcast_direct_array::<UInt32Array>(array, column)?,
                }
            }
            DirectColumnEncoding::Primitive(PrimitiveArrowToMssql::UInt64ToCheckedBigInt) => {
                BoundDirectColumn::UInt64 {
                    column,
                    array: downcast_direct_array::<UInt64Array>(array, column)?,
                }
            }
            DirectColumnEncoding::Primitive(PrimitiveArrowToMssql::Float32ToReal) => {
                BoundDirectColumn::Float32 {
                    column,
                    array: downcast_direct_array::<Float32Array>(array, column)?,
                }
            }
            DirectColumnEncoding::Primitive(PrimitiveArrowToMssql::Float64ToFloat) => {
                BoundDirectColumn::Float64 {
                    column,
                    array: downcast_direct_array::<Float64Array>(array, column)?,
                }
            }
            DirectColumnEncoding::UInt64Decimal20_0 => BoundDirectColumn::UInt64Decimal20_0 {
                column,
                array: downcast_direct_array::<UInt64Array>(array, column)?,
            },
            DirectColumnEncoding::Decimal(
                classification @ DecimalArrowToMssql::Decimal32 { .. },
            ) => BoundDirectColumn::Decimal32 {
                column,
                classification,
                array: downcast_direct_array::<Decimal32Array>(array, column)?,
            },
            DirectColumnEncoding::Decimal(
                classification @ DecimalArrowToMssql::Decimal64 { .. },
            ) => BoundDirectColumn::Decimal64 {
                column,
                classification,
                array: downcast_direct_array::<Decimal64Array>(array, column)?,
            },
            DirectColumnEncoding::Decimal(
                classification @ DecimalArrowToMssql::Decimal128 { .. },
            ) => BoundDirectColumn::Decimal128 {
                column,
                classification,
                array: downcast_direct_array::<Decimal128Array>(array, column)?,
            },
            DirectColumnEncoding::Decimal(
                classification @ DecimalArrowToMssql::Decimal256CheckedDowncast { .. },
            ) => BoundDirectColumn::Decimal256 {
                column,
                classification,
                array: downcast_direct_array::<Decimal256Array>(array, column)?,
            },
            DirectColumnEncoding::VariableWidth(VariableWidthArrowToMssql::Utf8ToNVarChar {
                ..
            }) => BoundDirectColumn::Utf8 {
                column,
                array: downcast_direct_array::<StringArray>(array, column)?,
            },
            DirectColumnEncoding::VariableWidth(VariableWidthArrowToMssql::BinaryToVarBinary {
                ..
            }) => BoundDirectColumn::Binary {
                column,
                array: downcast_direct_array::<BinaryArray>(array, column)?,
            },
            DirectColumnEncoding::VariableWidth(other) => {
                return Err(unsupported_batch(format!(
                    "direct variable-width append is not implemented yet for {other:?}"
                )));
            }
            DirectColumnEncoding::Temporal(TemporalArrowToMssql::Date32ToDate) => {
                BoundDirectColumn::Date32 {
                    column,
                    mapping: encoder.mapping_for_column_index(column_index)?,
                    array: downcast_direct_array::<Date32Array>(array, column)?,
                }
            }
            DirectColumnEncoding::Temporal(TemporalArrowToMssql::Date64ToDateTime2) => {
                BoundDirectColumn::Date64 {
                    column,
                    mapping: encoder.mapping_for_column_index(column_index)?,
                    array: downcast_direct_array::<Date64Array>(array, column)?,
                }
            }
            DirectColumnEncoding::Temporal(
                classification @ (TemporalArrowToMssql::TimestampSecondToDateTime2
                | TemporalArrowToMssql::TimestampSecondTzToDateTime2),
            ) => BoundDirectColumn::TimestampSecond {
                column,
                mapping: encoder.mapping_for_column_index(column_index)?,
                classification,
                array: downcast_direct_array::<TimestampSecondArray>(array, column)?,
            },
            DirectColumnEncoding::Temporal(
                classification @ (TemporalArrowToMssql::TimestampMillisecondToDateTime2
                | TemporalArrowToMssql::TimestampMillisecondTzToDateTime2),
            ) => BoundDirectColumn::TimestampMillisecond {
                column,
                mapping: encoder.mapping_for_column_index(column_index)?,
                classification,
                array: downcast_direct_array::<TimestampMillisecondArray>(array, column)?,
            },
            DirectColumnEncoding::Temporal(
                classification @ (TemporalArrowToMssql::TimestampMicrosecondToDateTime2
                | TemporalArrowToMssql::TimestampMicrosecondTzToDateTime2),
            ) => BoundDirectColumn::TimestampMicrosecond {
                column,
                mapping: encoder.mapping_for_column_index(column_index)?,
                classification,
                array: downcast_direct_array::<TimestampMicrosecondArray>(array, column)?,
            },
            DirectColumnEncoding::Temporal(
                classification @ (TemporalArrowToMssql::TimestampNanosecondToDateTime2
                | TemporalArrowToMssql::TimestampNanosecondTzToDateTime2),
            ) => BoundDirectColumn::TimestampNanosecond {
                column,
                mapping: encoder.mapping_for_column_index(column_index)?,
                classification,
                nanosecond_policy: encoder.plan_options.nanosecond_policy,
                array: downcast_direct_array::<TimestampNanosecondArray>(array, column)?,
            },
            DirectColumnEncoding::Temporal(
                classification @ TemporalArrowToMssql::Time32SecondToTime,
            ) => BoundDirectColumn::Time32Second {
                column,
                mapping: encoder.mapping_for_column_index(column_index)?,
                classification,
                array: downcast_direct_array::<Time32SecondArray>(array, column)?,
            },
            DirectColumnEncoding::Temporal(
                classification @ TemporalArrowToMssql::Time32MillisecondToTime,
            ) => BoundDirectColumn::Time32Millisecond {
                column,
                mapping: encoder.mapping_for_column_index(column_index)?,
                classification,
                array: downcast_direct_array::<Time32MillisecondArray>(array, column)?,
            },
            DirectColumnEncoding::Temporal(
                classification @ TemporalArrowToMssql::Time64MicrosecondToTime,
            ) => BoundDirectColumn::Time64Microsecond {
                column,
                mapping: encoder.mapping_for_column_index(column_index)?,
                classification,
                array: downcast_direct_array::<Time64MicrosecondArray>(array, column)?,
            },
            DirectColumnEncoding::Temporal(
                classification @ TemporalArrowToMssql::Time64NanosecondToTime,
            ) => BoundDirectColumn::Time64Nanosecond {
                column,
                mapping: encoder.mapping_for_column_index(column_index)?,
                classification,
                nanosecond_policy: encoder.plan_options.nanosecond_policy,
                array: downcast_direct_array::<Time64NanosecondArray>(array, column)?,
            },
            DirectColumnEncoding::Temporal(
                classification @ TemporalArrowToMssql::TimestampSecondTzToDateTimeOffset,
            ) => BoundDirectColumn::DateTimeOffsetSecond {
                column,
                mapping: encoder.mapping_for_column_index(column_index)?,
                classification,
                array: downcast_direct_array::<TimestampSecondArray>(array, column)?,
            },
            DirectColumnEncoding::Temporal(
                classification @ TemporalArrowToMssql::TimestampMillisecondTzToDateTimeOffset,
            ) => BoundDirectColumn::DateTimeOffsetMillisecond {
                column,
                mapping: encoder.mapping_for_column_index(column_index)?,
                classification,
                array: downcast_direct_array::<TimestampMillisecondArray>(array, column)?,
            },
            DirectColumnEncoding::Temporal(
                classification @ TemporalArrowToMssql::TimestampMicrosecondTzToDateTimeOffset,
            ) => BoundDirectColumn::DateTimeOffsetMicrosecond {
                column,
                mapping: encoder.mapping_for_column_index(column_index)?,
                classification,
                array: downcast_direct_array::<TimestampMicrosecondArray>(array, column)?,
            },
            DirectColumnEncoding::Temporal(
                classification @ TemporalArrowToMssql::TimestampNanosecondTzToDateTimeOffset,
            ) => BoundDirectColumn::DateTimeOffsetNanosecond {
                column,
                mapping: encoder.mapping_for_column_index(column_index)?,
                classification,
                nanosecond_policy: encoder.plan_options.nanosecond_policy,
                array: downcast_direct_array::<TimestampNanosecondArray>(array, column)?,
            },
        };

        columns.push(runtime);
    }

    Ok(columns)
}

pub(crate) enum BoundDirectColumn<'a> {
    Boolean {
        column: &'a plan::DirectColumnPlan,
        array: &'a BooleanArray,
    },
    UInt8 {
        column: &'a plan::DirectColumnPlan,
        array: &'a UInt8Array,
    },
    Int8 {
        column: &'a plan::DirectColumnPlan,
        array: &'a Int8Array,
    },
    Int16 {
        column: &'a plan::DirectColumnPlan,
        array: &'a Int16Array,
    },
    Int32 {
        column: &'a plan::DirectColumnPlan,
        array: &'a Int32Array,
    },
    UInt16 {
        column: &'a plan::DirectColumnPlan,
        array: &'a UInt16Array,
    },
    Int64 {
        column: &'a plan::DirectColumnPlan,
        array: &'a Int64Array,
    },
    UInt32 {
        column: &'a plan::DirectColumnPlan,
        array: &'a UInt32Array,
    },
    UInt64 {
        column: &'a plan::DirectColumnPlan,
        array: &'a UInt64Array,
    },
    UInt64Decimal20_0 {
        column: &'a plan::DirectColumnPlan,
        array: &'a UInt64Array,
    },
    Decimal32 {
        column: &'a plan::DirectColumnPlan,
        classification: DecimalArrowToMssql,
        array: &'a Decimal32Array,
    },
    Decimal64 {
        column: &'a plan::DirectColumnPlan,
        classification: DecimalArrowToMssql,
        array: &'a Decimal64Array,
    },
    Decimal128 {
        column: &'a plan::DirectColumnPlan,
        classification: DecimalArrowToMssql,
        array: &'a Decimal128Array,
    },
    Decimal256 {
        column: &'a plan::DirectColumnPlan,
        classification: DecimalArrowToMssql,
        array: &'a Decimal256Array,
    },
    Float32 {
        column: &'a plan::DirectColumnPlan,
        array: &'a Float32Array,
    },
    Float64 {
        column: &'a plan::DirectColumnPlan,
        array: &'a Float64Array,
    },
    Utf8 {
        column: &'a plan::DirectColumnPlan,
        array: &'a StringArray,
    },
    Binary {
        column: &'a plan::DirectColumnPlan,
        array: &'a BinaryArray,
    },
    Date32 {
        column: &'a plan::DirectColumnPlan,
        mapping: &'a SchemaMapping,
        array: &'a Date32Array,
    },
    Date64 {
        column: &'a plan::DirectColumnPlan,
        mapping: &'a SchemaMapping,
        array: &'a Date64Array,
    },
    TimestampSecond {
        column: &'a plan::DirectColumnPlan,
        mapping: &'a SchemaMapping,
        classification: TemporalArrowToMssql,
        array: &'a TimestampSecondArray,
    },
    TimestampMillisecond {
        column: &'a plan::DirectColumnPlan,
        mapping: &'a SchemaMapping,
        classification: TemporalArrowToMssql,
        array: &'a TimestampMillisecondArray,
    },
    TimestampMicrosecond {
        column: &'a plan::DirectColumnPlan,
        mapping: &'a SchemaMapping,
        classification: TemporalArrowToMssql,
        array: &'a TimestampMicrosecondArray,
    },
    TimestampNanosecond {
        column: &'a plan::DirectColumnPlan,
        mapping: &'a SchemaMapping,
        classification: TemporalArrowToMssql,
        nanosecond_policy: NanosecondPolicy,
        array: &'a TimestampNanosecondArray,
    },
    Time32Second {
        column: &'a plan::DirectColumnPlan,
        mapping: &'a SchemaMapping,
        classification: TemporalArrowToMssql,
        array: &'a Time32SecondArray,
    },
    Time32Millisecond {
        column: &'a plan::DirectColumnPlan,
        mapping: &'a SchemaMapping,
        classification: TemporalArrowToMssql,
        array: &'a Time32MillisecondArray,
    },
    Time64Microsecond {
        column: &'a plan::DirectColumnPlan,
        mapping: &'a SchemaMapping,
        classification: TemporalArrowToMssql,
        array: &'a Time64MicrosecondArray,
    },
    Time64Nanosecond {
        column: &'a plan::DirectColumnPlan,
        mapping: &'a SchemaMapping,
        classification: TemporalArrowToMssql,
        nanosecond_policy: NanosecondPolicy,
        array: &'a Time64NanosecondArray,
    },
    DateTimeOffsetSecond {
        column: &'a plan::DirectColumnPlan,
        mapping: &'a SchemaMapping,
        classification: TemporalArrowToMssql,
        array: &'a TimestampSecondArray,
    },
    DateTimeOffsetMillisecond {
        column: &'a plan::DirectColumnPlan,
        mapping: &'a SchemaMapping,
        classification: TemporalArrowToMssql,
        array: &'a TimestampMillisecondArray,
    },
    DateTimeOffsetMicrosecond {
        column: &'a plan::DirectColumnPlan,
        mapping: &'a SchemaMapping,
        classification: TemporalArrowToMssql,
        array: &'a TimestampMicrosecondArray,
    },
    DateTimeOffsetNanosecond {
        column: &'a plan::DirectColumnPlan,
        mapping: &'a SchemaMapping,
        classification: TemporalArrowToMssql,
        nanosecond_policy: NanosecondPolicy,
        array: &'a TimestampNanosecondArray,
    },
}

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
            Self::Binary { column, array } => measure_varbinary_column_cell_lengths(
                array,
                column,
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
                    classification: TemporalArrowToMssql::Date32ToDate,
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
                    classification: TemporalArrowToMssql::Date64ToDateTime2,
                    column_index,
                    column_count,
                },
                cell_lengths,
            ),
            Self::TimestampSecond {
                column,
                mapping,
                classification,
                array,
            } => measure_timestamp_second_column_cell_lengths(
                array,
                TemporalColumnContext {
                    mapping,
                    plan_options: default_options,
                    column,
                    classification: *classification,
                    column_index,
                    column_count,
                },
                cell_lengths,
            ),
            Self::TimestampMillisecond {
                column,
                mapping,
                classification,
                array,
            } => measure_timestamp_millisecond_column_cell_lengths(
                array,
                TemporalColumnContext {
                    mapping,
                    plan_options: default_options,
                    column,
                    classification: *classification,
                    column_index,
                    column_count,
                },
                cell_lengths,
            ),
            Self::TimestampMicrosecond {
                column,
                mapping,
                classification,
                array,
            } => measure_timestamp_microsecond_column_cell_lengths(
                array,
                TemporalColumnContext {
                    mapping,
                    plan_options: default_options,
                    column,
                    classification: *classification,
                    column_index,
                    column_count,
                },
                cell_lengths,
            ),
            Self::TimestampNanosecond {
                column,
                mapping,
                classification,
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
                    classification: *classification,
                    column_index,
                    column_count,
                },
                cell_lengths,
            ),
            Self::Time32Second {
                column,
                mapping,
                classification,
                array,
            } => measure_time32_second_column_cell_lengths(
                array,
                TemporalColumnContext {
                    mapping,
                    plan_options: default_options,
                    column,
                    classification: *classification,
                    column_index,
                    column_count,
                },
                cell_lengths,
            ),
            Self::Time32Millisecond {
                column,
                mapping,
                classification,
                array,
            } => measure_time32_millisecond_column_cell_lengths(
                array,
                TemporalColumnContext {
                    mapping,
                    plan_options: default_options,
                    column,
                    classification: *classification,
                    column_index,
                    column_count,
                },
                cell_lengths,
            ),
            Self::Time64Microsecond {
                column,
                mapping,
                classification,
                array,
            } => measure_time64_microsecond_column_cell_lengths(
                array,
                TemporalColumnContext {
                    mapping,
                    plan_options: default_options,
                    column,
                    classification: *classification,
                    column_index,
                    column_count,
                },
                cell_lengths,
            ),
            Self::Time64Nanosecond {
                column,
                mapping,
                classification,
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
                    classification: *classification,
                    column_index,
                    column_count,
                },
                cell_lengths,
            ),
            Self::DateTimeOffsetSecond {
                column,
                mapping,
                classification,
                array,
            } => measure_datetimeoffset_second_column_cell_lengths(
                array,
                TemporalColumnContext {
                    mapping,
                    plan_options: default_options,
                    column,
                    classification: *classification,
                    column_index,
                    column_count,
                },
                cell_lengths,
            ),
            Self::DateTimeOffsetMillisecond {
                column,
                mapping,
                classification,
                array,
            } => measure_datetimeoffset_millisecond_column_cell_lengths(
                array,
                TemporalColumnContext {
                    mapping,
                    plan_options: default_options,
                    column,
                    classification: *classification,
                    column_index,
                    column_count,
                },
                cell_lengths,
            ),
            Self::DateTimeOffsetMicrosecond {
                column,
                mapping,
                classification,
                array,
            } => measure_datetimeoffset_microsecond_column_cell_lengths(
                array,
                TemporalColumnContext {
                    mapping,
                    plan_options: default_options,
                    column,
                    classification: *classification,
                    column_index,
                    column_count,
                },
                cell_lengths,
            ),
            Self::DateTimeOffsetNanosecond {
                column,
                mapping,
                classification,
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
                    classification: *classification,
                    column_index,
                    column_count,
                },
                cell_lengths,
            ),
        }
    }

    pub(crate) fn fill_column(
        &self,
        column_index: usize,
        column_count: usize,
        layout: &RowLayout,
        bytes: &mut [u8],
    ) -> Result<()> {
        let default_options = PlanOptions::default();

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
            Self::Float32 { column, array } => {
                fill_float32_column(array, column, column_index, column_count, layout, bytes)
            }
            Self::Float64 { column, array } => {
                fill_float64_column(array, column, column_index, column_count, layout, bytes)
            }
            Self::Utf8 { column, array } => {
                fill_nvarchar_column(array, column, column_index, column_count, layout, bytes)
            }
            Self::Binary { column, array } => {
                fill_varbinary_column(array, column, column_index, column_count, layout, bytes)
            }
            Self::Date32 {
                column,
                mapping,
                array,
            } => fill_date32_direct_column(
                array,
                TemporalColumnContext {
                    mapping,
                    plan_options: default_options,
                    column,
                    classification: TemporalArrowToMssql::Date32ToDate,
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
                    plan_options: default_options,
                    column,
                    classification: TemporalArrowToMssql::Date64ToDateTime2,
                    column_index,
                    column_count,
                },
                layout,
                bytes,
            ),
            Self::TimestampSecond {
                column,
                mapping,
                classification,
                array,
            } => fill_timestamp_second_direct_column(
                array,
                TemporalColumnContext {
                    mapping,
                    plan_options: default_options,
                    column,
                    classification: *classification,
                    column_index,
                    column_count,
                },
                layout,
                bytes,
            ),
            Self::TimestampMillisecond {
                column,
                mapping,
                classification,
                array,
            } => fill_timestamp_millisecond_direct_column(
                array,
                TemporalColumnContext {
                    mapping,
                    plan_options: default_options,
                    column,
                    classification: *classification,
                    column_index,
                    column_count,
                },
                layout,
                bytes,
            ),
            Self::TimestampMicrosecond {
                column,
                mapping,
                classification,
                array,
            } => fill_timestamp_microsecond_direct_column(
                array,
                TemporalColumnContext {
                    mapping,
                    plan_options: default_options,
                    column,
                    classification: *classification,
                    column_index,
                    column_count,
                },
                layout,
                bytes,
            ),
            Self::TimestampNanosecond {
                column,
                mapping,
                classification,
                nanosecond_policy,
                array,
            } => fill_timestamp_nanosecond_direct_column(
                array,
                TemporalColumnContext {
                    mapping,
                    plan_options: PlanOptions {
                        nanosecond_policy: *nanosecond_policy,
                        ..Default::default()
                    },
                    column,
                    classification: *classification,
                    column_index,
                    column_count,
                },
                layout,
                bytes,
            ),
            Self::Time32Second {
                column,
                mapping,
                classification,
                array,
            } => fill_time32_second_direct_column(
                array,
                TemporalColumnContext {
                    mapping,
                    plan_options: default_options,
                    column,
                    classification: *classification,
                    column_index,
                    column_count,
                },
                layout,
                bytes,
            ),
            Self::Time32Millisecond {
                column,
                mapping,
                classification,
                array,
            } => fill_time32_millisecond_direct_column(
                array,
                TemporalColumnContext {
                    mapping,
                    plan_options: default_options,
                    column,
                    classification: *classification,
                    column_index,
                    column_count,
                },
                layout,
                bytes,
            ),
            Self::Time64Microsecond {
                column,
                mapping,
                classification,
                array,
            } => fill_time64_microsecond_direct_column(
                array,
                TemporalColumnContext {
                    mapping,
                    plan_options: default_options,
                    column,
                    classification: *classification,
                    column_index,
                    column_count,
                },
                layout,
                bytes,
            ),
            Self::Time64Nanosecond {
                column,
                mapping,
                classification,
                nanosecond_policy,
                array,
            } => fill_time64_nanosecond_direct_column(
                array,
                TemporalColumnContext {
                    mapping,
                    plan_options: PlanOptions {
                        nanosecond_policy: *nanosecond_policy,
                        ..Default::default()
                    },
                    column,
                    classification: *classification,
                    column_index,
                    column_count,
                },
                layout,
                bytes,
            ),
            Self::DateTimeOffsetSecond {
                column,
                mapping,
                classification,
                array,
            } => fill_datetimeoffset_second_direct_column(
                array,
                TemporalColumnContext {
                    mapping,
                    plan_options: default_options,
                    column,
                    classification: *classification,
                    column_index,
                    column_count,
                },
                layout,
                bytes,
            ),
            Self::DateTimeOffsetMillisecond {
                column,
                mapping,
                classification,
                array,
            } => fill_datetimeoffset_millisecond_direct_column(
                array,
                TemporalColumnContext {
                    mapping,
                    plan_options: default_options,
                    column,
                    classification: *classification,
                    column_index,
                    column_count,
                },
                layout,
                bytes,
            ),
            Self::DateTimeOffsetMicrosecond {
                column,
                mapping,
                classification,
                array,
            } => fill_datetimeoffset_microsecond_direct_column(
                array,
                TemporalColumnContext {
                    mapping,
                    plan_options: default_options,
                    column,
                    classification: *classification,
                    column_index,
                    column_count,
                },
                layout,
                bytes,
            ),
            Self::DateTimeOffsetNanosecond {
                column,
                mapping,
                classification,
                nanosecond_policy,
                array,
            } => fill_datetimeoffset_nanosecond_direct_column(
                array,
                TemporalColumnContext {
                    mapping,
                    plan_options: PlanOptions {
                        nanosecond_policy: *nanosecond_policy,
                        ..Default::default()
                    },
                    column,
                    classification: *classification,
                    column_index,
                    column_count,
                },
                layout,
                bytes,
            ),
        }
    }

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
            Self::Binary { column, array } => {
                append_varbinary_cell(buf, array, column, row_index, measured_len)
            }
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
                classification: _,
                array,
            } => append_timestamp_second_cell(buf, array, mapping, column, row_index, measured_len),
            Self::TimestampMillisecond {
                column,
                mapping,
                classification: _,
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
                classification: _,
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
                classification: _,
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
                classification: _,
                array,
            } => append_time32_second_cell(buf, array, mapping, column, row_index, measured_len),
            Self::Time32Millisecond {
                column,
                mapping,
                classification: _,
                array,
            } => {
                append_time32_millisecond_cell(buf, array, mapping, column, row_index, measured_len)
            }
            Self::Time64Microsecond {
                column,
                mapping,
                classification: _,
                array,
            } => {
                append_time64_microsecond_cell(buf, array, mapping, column, row_index, measured_len)
            }
            Self::Time64Nanosecond {
                column,
                mapping,
                classification: _,
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
                classification: _,
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
                classification: _,
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
                classification: _,
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
                classification: _,
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

fn measure_primitive_bound_column(
    array: &dyn Array,
    column: &plan::DirectColumnPlan,
    column_index: usize,
    column_count: usize,
    cell_lengths: &mut [usize],
) -> Result<()> {
    measure_primitive_column_cell_lengths(array, column, column_index, column_count, cell_lengths)
}

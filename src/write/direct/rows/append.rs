//! Append-buffer direct TDS row execution.

use arrow_array::{
    BinaryArray, BooleanArray, Date32Array, Date64Array, Decimal32Array, Decimal64Array,
    Decimal128Array, Decimal256Array, Float32Array, Float64Array, Int8Array, Int16Array,
    Int32Array, Int64Array, RecordBatch, StringArray, Time32MillisecondArray, Time32SecondArray,
    Time64MicrosecondArray, Time64NanosecondArray, TimestampMicrosecondArray,
    TimestampMillisecondArray, TimestampNanosecondArray, TimestampSecondArray, UInt8Array,
    UInt16Array, UInt32Array, UInt64Array,
};

use crate::{
    DiagnosticCode, NanosecondPolicy, Result, SchemaMapping,
    conversion::arrow_to_mssql::{
        decimal::DecimalArrowToMssql, primitive::PrimitiveArrowToMssql,
        temporal::TemporalArrowToMssql, variable_width::VariableWidthArrowToMssql,
    },
};

use super::super::{
    DirectEncoder, MeasuredDirectBatch, checked_add, downcast_direct_array, invalid_payload,
    payload, plan,
    plan::DirectColumnEncoding,
    row_column_diagnostic,
    types::{
        decimal::{
            append_decimal32_cell, append_decimal64_cell, append_decimal128_cell,
            append_decimal256_cell,
        },
        primitive::{
            append_boolean_cell, append_float32_cell, append_float64_cell, append_int8_cell,
            append_int16_cell, append_int32_cell, append_int64_cell, append_uint8_cell,
            append_uint16_cell, append_uint32_cell, append_uint64_checked_bigint_cell,
        },
        temporal::{
            append_date32_cell, append_date64_cell, append_datetimeoffset_microsecond_cell,
            append_datetimeoffset_millisecond_cell, append_datetimeoffset_nanosecond_cell,
            append_datetimeoffset_second_cell, append_time32_millisecond_cell,
            append_time32_second_cell, append_time64_microsecond_cell,
            append_time64_nanosecond_cell, append_timestamp_microsecond_cell,
            append_timestamp_millisecond_cell, append_timestamp_nanosecond_cell,
            append_timestamp_second_cell,
        },
        uint64::append_uint64_decimal20_cell,
        variable_width::{append_nvarchar_cell, append_varbinary_cell},
    },
    unsupported_batch, value_conversion_error,
};

/// Encodes one measured range directly into a Tiberius raw rows buffer.
pub(crate) fn encode_measured_batch_range_into(
    encoder: &DirectEncoder,
    batch: &RecordBatch,
    measured: &MeasuredDirectBatch,
    start_row: usize,
    row_count: usize,
    buf: &mut tiberius::RawRowsAppendBuffer<'_>,
) -> Result<tiberius::RawRowsAppend> {
    measured.check_range(start_row, row_count)?;

    if measured.row_count() != batch.num_rows() {
        return Err(invalid_payload(format!(
            "measured row count {} does not match runtime batch row count {}",
            measured.row_count(),
            batch.num_rows()
        )));
    }

    if measured.column_count() != encoder.plan.column_count() {
        return Err(invalid_payload(format!(
            "measured column count {} does not match direct plan column count {}",
            measured.column_count(),
            encoder.plan.column_count()
        )));
    }

    let runtime_columns = runtime_columns(encoder, batch)?;
    let mut row_token_offsets = Vec::with_capacity(row_count);
    let mut written = 0usize;

    let end_row = start_row
        .checked_add(row_count)
        .ok_or_else(|| invalid_payload("direct row range end overflowed usize"))?;

    for row_index in start_row..end_row {
        row_token_offsets.push(written);
        buf.put_u8(payload::TDS_ROW_TOKEN);
        written = checked_add(written, 1)?;

        for (column_index, column) in runtime_columns.iter().enumerate() {
            let measured_len = measured.cell_len(row_index, column_index)?;
            column.append_cell(buf, row_index, measured_len)?;
            written = checked_add(written, measured_len)?;
        }
    }

    Ok(tiberius::RawRowsAppend::new(row_token_offsets))
}

fn runtime_columns<'a>(
    encoder: &'a DirectEncoder,
    batch: &'a RecordBatch,
) -> Result<Vec<RuntimeDirectColumn<'a>>> {
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
                RuntimeDirectColumn::Boolean {
                    column,
                    array: downcast_direct_array::<BooleanArray>(array, column)?,
                }
            }
            DirectColumnEncoding::Primitive(PrimitiveArrowToMssql::UInt8ToTinyInt) => {
                RuntimeDirectColumn::UInt8 {
                    column,
                    array: downcast_direct_array::<UInt8Array>(array, column)?,
                }
            }
            DirectColumnEncoding::Primitive(PrimitiveArrowToMssql::Int8ToSmallInt) => {
                RuntimeDirectColumn::Int8 {
                    column,
                    array: downcast_direct_array::<Int8Array>(array, column)?,
                }
            }
            DirectColumnEncoding::Primitive(PrimitiveArrowToMssql::Int16ToSmallInt) => {
                RuntimeDirectColumn::Int16 {
                    column,
                    array: downcast_direct_array::<Int16Array>(array, column)?,
                }
            }
            DirectColumnEncoding::Primitive(PrimitiveArrowToMssql::Int32ToInt) => {
                RuntimeDirectColumn::Int32 {
                    column,
                    array: downcast_direct_array::<Int32Array>(array, column)?,
                }
            }
            DirectColumnEncoding::Primitive(PrimitiveArrowToMssql::UInt16ToInt) => {
                RuntimeDirectColumn::UInt16 {
                    column,
                    array: downcast_direct_array::<UInt16Array>(array, column)?,
                }
            }
            DirectColumnEncoding::Primitive(PrimitiveArrowToMssql::Int64ToBigInt) => {
                RuntimeDirectColumn::Int64 {
                    column,
                    array: downcast_direct_array::<Int64Array>(array, column)?,
                }
            }
            DirectColumnEncoding::Primitive(PrimitiveArrowToMssql::UInt32ToBigInt) => {
                RuntimeDirectColumn::UInt32 {
                    column,
                    array: downcast_direct_array::<UInt32Array>(array, column)?,
                }
            }
            DirectColumnEncoding::Primitive(PrimitiveArrowToMssql::UInt64ToCheckedBigInt) => {
                RuntimeDirectColumn::UInt64 {
                    column,
                    array: downcast_direct_array::<UInt64Array>(array, column)?,
                }
            }
            DirectColumnEncoding::Primitive(PrimitiveArrowToMssql::Float32ToReal) => {
                RuntimeDirectColumn::Float32 {
                    column,
                    array: downcast_direct_array::<Float32Array>(array, column)?,
                }
            }
            DirectColumnEncoding::Primitive(PrimitiveArrowToMssql::Float64ToFloat) => {
                RuntimeDirectColumn::Float64 {
                    column,
                    array: downcast_direct_array::<Float64Array>(array, column)?,
                }
            }
            DirectColumnEncoding::UInt64Decimal20_0 => RuntimeDirectColumn::UInt64Decimal20_0 {
                column,
                array: downcast_direct_array::<UInt64Array>(array, column)?,
            },
            DirectColumnEncoding::Decimal(
                classification @ DecimalArrowToMssql::Decimal32 { .. },
            ) => RuntimeDirectColumn::Decimal32 {
                column,
                classification,
                array: downcast_direct_array::<Decimal32Array>(array, column)?,
            },
            DirectColumnEncoding::Decimal(
                classification @ DecimalArrowToMssql::Decimal64 { .. },
            ) => RuntimeDirectColumn::Decimal64 {
                column,
                classification,
                array: downcast_direct_array::<Decimal64Array>(array, column)?,
            },
            DirectColumnEncoding::Decimal(
                classification @ DecimalArrowToMssql::Decimal128 { .. },
            ) => RuntimeDirectColumn::Decimal128 {
                column,
                classification,
                array: downcast_direct_array::<Decimal128Array>(array, column)?,
            },
            DirectColumnEncoding::Decimal(
                classification @ DecimalArrowToMssql::Decimal256CheckedDowncast { .. },
            ) => RuntimeDirectColumn::Decimal256 {
                column,
                classification,
                array: downcast_direct_array::<Decimal256Array>(array, column)?,
            },
            DirectColumnEncoding::VariableWidth(VariableWidthArrowToMssql::Utf8ToNVarChar {
                ..
            }) => RuntimeDirectColumn::Utf8 {
                column,
                array: downcast_direct_array::<StringArray>(array, column)?,
            },
            DirectColumnEncoding::VariableWidth(VariableWidthArrowToMssql::BinaryToVarBinary {
                ..
            }) => RuntimeDirectColumn::Binary {
                column,
                array: downcast_direct_array::<BinaryArray>(array, column)?,
            },
            DirectColumnEncoding::VariableWidth(other) => {
                return Err(unsupported_batch(format!(
                    "direct variable-width append is not implemented yet for {other:?}"
                )));
            }
            DirectColumnEncoding::Temporal(TemporalArrowToMssql::Date32ToDate) => {
                RuntimeDirectColumn::Date32 {
                    column,
                    mapping: encoder.mapping_for_column_index(column_index)?,
                    array: downcast_direct_array::<Date32Array>(array, column)?,
                }
            }
            DirectColumnEncoding::Temporal(TemporalArrowToMssql::Date64ToDateTime2) => {
                RuntimeDirectColumn::Date64 {
                    column,
                    mapping: encoder.mapping_for_column_index(column_index)?,
                    array: downcast_direct_array::<Date64Array>(array, column)?,
                }
            }
            DirectColumnEncoding::Temporal(
                classification @ (TemporalArrowToMssql::TimestampSecondToDateTime2
                | TemporalArrowToMssql::TimestampSecondTzToDateTime2),
            ) => RuntimeDirectColumn::TimestampSecond {
                column,
                mapping: encoder.mapping_for_column_index(column_index)?,
                classification,
                array: downcast_direct_array::<TimestampSecondArray>(array, column)?,
            },
            DirectColumnEncoding::Temporal(
                classification @ (TemporalArrowToMssql::TimestampMillisecondToDateTime2
                | TemporalArrowToMssql::TimestampMillisecondTzToDateTime2),
            ) => RuntimeDirectColumn::TimestampMillisecond {
                column,
                mapping: encoder.mapping_for_column_index(column_index)?,
                classification,
                array: downcast_direct_array::<TimestampMillisecondArray>(array, column)?,
            },
            DirectColumnEncoding::Temporal(
                classification @ (TemporalArrowToMssql::TimestampMicrosecondToDateTime2
                | TemporalArrowToMssql::TimestampMicrosecondTzToDateTime2),
            ) => RuntimeDirectColumn::TimestampMicrosecond {
                column,
                mapping: encoder.mapping_for_column_index(column_index)?,
                classification,
                array: downcast_direct_array::<TimestampMicrosecondArray>(array, column)?,
            },
            DirectColumnEncoding::Temporal(
                classification @ (TemporalArrowToMssql::TimestampNanosecondToDateTime2
                | TemporalArrowToMssql::TimestampNanosecondTzToDateTime2),
            ) => RuntimeDirectColumn::TimestampNanosecond {
                column,
                mapping: encoder.mapping_for_column_index(column_index)?,
                classification,
                nanosecond_policy: encoder.plan_options.nanosecond_policy,
                array: downcast_direct_array::<TimestampNanosecondArray>(array, column)?,
            },
            DirectColumnEncoding::Temporal(
                classification @ TemporalArrowToMssql::Time32SecondToTime,
            ) => RuntimeDirectColumn::Time32Second {
                column,
                mapping: encoder.mapping_for_column_index(column_index)?,
                classification,
                array: downcast_direct_array::<Time32SecondArray>(array, column)?,
            },
            DirectColumnEncoding::Temporal(
                classification @ TemporalArrowToMssql::Time32MillisecondToTime,
            ) => RuntimeDirectColumn::Time32Millisecond {
                column,
                mapping: encoder.mapping_for_column_index(column_index)?,
                classification,
                array: downcast_direct_array::<Time32MillisecondArray>(array, column)?,
            },
            DirectColumnEncoding::Temporal(
                classification @ TemporalArrowToMssql::Time64MicrosecondToTime,
            ) => RuntimeDirectColumn::Time64Microsecond {
                column,
                mapping: encoder.mapping_for_column_index(column_index)?,
                classification,
                array: downcast_direct_array::<Time64MicrosecondArray>(array, column)?,
            },
            DirectColumnEncoding::Temporal(
                classification @ TemporalArrowToMssql::Time64NanosecondToTime,
            ) => RuntimeDirectColumn::Time64Nanosecond {
                column,
                mapping: encoder.mapping_for_column_index(column_index)?,
                classification,
                nanosecond_policy: encoder.plan_options.nanosecond_policy,
                array: downcast_direct_array::<Time64NanosecondArray>(array, column)?,
            },
            DirectColumnEncoding::Temporal(
                classification @ TemporalArrowToMssql::TimestampSecondTzToDateTimeOffset,
            ) => RuntimeDirectColumn::DateTimeOffsetSecond {
                column,
                mapping: encoder.mapping_for_column_index(column_index)?,
                classification,
                array: downcast_direct_array::<TimestampSecondArray>(array, column)?,
            },
            DirectColumnEncoding::Temporal(
                classification @ TemporalArrowToMssql::TimestampMillisecondTzToDateTimeOffset,
            ) => RuntimeDirectColumn::DateTimeOffsetMillisecond {
                column,
                mapping: encoder.mapping_for_column_index(column_index)?,
                classification,
                array: downcast_direct_array::<TimestampMillisecondArray>(array, column)?,
            },
            DirectColumnEncoding::Temporal(
                classification @ TemporalArrowToMssql::TimestampMicrosecondTzToDateTimeOffset,
            ) => RuntimeDirectColumn::DateTimeOffsetMicrosecond {
                column,
                mapping: encoder.mapping_for_column_index(column_index)?,
                classification,
                array: downcast_direct_array::<TimestampMicrosecondArray>(array, column)?,
            },
            DirectColumnEncoding::Temporal(
                classification @ TemporalArrowToMssql::TimestampNanosecondTzToDateTimeOffset,
            ) => RuntimeDirectColumn::DateTimeOffsetNanosecond {
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

enum RuntimeDirectColumn<'a> {
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

impl RuntimeDirectColumn<'_> {
    fn append_cell(
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

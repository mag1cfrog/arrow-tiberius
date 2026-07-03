//! Bound runtime direct TDS columns.

mod append;
mod fill;
mod measure;

use arrow_array::{
    Array, BinaryArray, BinaryViewArray, BooleanArray, Date32Array, Date64Array, Decimal32Array,
    Decimal64Array, Decimal128Array, Decimal256Array, FixedSizeBinaryArray, Float16Array,
    Float32Array, Float64Array, Int8Array, Int16Array, Int32Array, Int64Array, LargeBinaryArray,
    LargeStringArray, RecordBatch, StringArray, StringViewArray, Time32MillisecondArray,
    Time32SecondArray, Time64MicrosecondArray, Time64NanosecondArray, TimestampMicrosecondArray,
    TimestampMillisecondArray, TimestampNanosecondArray, TimestampSecondArray, UInt8Array,
    UInt16Array, UInt32Array, UInt64Array,
};
use arrow_schema::DataType;

use super::{
    DirectEncoder,
    layout::{RowLayout, build_fixed_width_row_layout},
    plan,
    plan::DirectColumnEncoding,
    row_column_diagnostic, value_conversion_error,
};
use crate::{
    DiagnosticCode, NanosecondPolicy, Result, SchemaMapping,
    conversion::arrow_to_mssql::{
        decimal::DecimalArrowToMssql,
        fixed_size_binary::FixedSizeBinaryArrowToMssql,
        primitive::PrimitiveArrowToMssql,
        temporal::TemporalArrowToMssql,
        variable_width::{
            VariableWidthArrowToMssql, is_binary_family_to_varbinary, is_string_family_to_nvarchar,
        },
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
            DirectColumnEncoding::Primitive(PrimitiveArrowToMssql::Float16ToReal) => {
                BoundDirectColumn::Float16 {
                    column,
                    array: downcast_direct_array::<Float16Array>(array, column)?,
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
            DirectColumnEncoding::VariableWidth(VariableWidthArrowToMssql::StringToNVarChar {
                ..
            }) => bind_direct_nvarchar_array(
                array,
                column,
                encoder.mapping_for_column_index(column_index)?,
            )?,
            DirectColumnEncoding::VariableWidth(VariableWidthArrowToMssql::BytesToVarBinary {
                ..
            }) => bind_direct_varbinary_array(
                array,
                column,
                encoder.mapping_for_column_index(column_index)?,
            )?,
            DirectColumnEncoding::FixedSizeBinary(classification) => {
                BoundDirectColumn::FixedSizeBinary {
                    column,
                    classification,
                    array: downcast_direct_array::<FixedSizeBinaryArray>(array, column)?,
                }
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
                TemporalArrowToMssql::TimestampSecondToDateTime2
                | TemporalArrowToMssql::TimestampSecondTzToDateTime2,
            ) => BoundDirectColumn::TimestampSecond {
                column,
                mapping: encoder.mapping_for_column_index(column_index)?,
                array: downcast_direct_array::<TimestampSecondArray>(array, column)?,
            },
            DirectColumnEncoding::Temporal(
                TemporalArrowToMssql::TimestampMillisecondToDateTime2
                | TemporalArrowToMssql::TimestampMillisecondTzToDateTime2,
            ) => BoundDirectColumn::TimestampMillisecond {
                column,
                mapping: encoder.mapping_for_column_index(column_index)?,
                array: downcast_direct_array::<TimestampMillisecondArray>(array, column)?,
            },
            DirectColumnEncoding::Temporal(
                TemporalArrowToMssql::TimestampMicrosecondToDateTime2
                | TemporalArrowToMssql::TimestampMicrosecondTzToDateTime2,
            ) => BoundDirectColumn::TimestampMicrosecond {
                column,
                mapping: encoder.mapping_for_column_index(column_index)?,
                array: downcast_direct_array::<TimestampMicrosecondArray>(array, column)?,
            },
            DirectColumnEncoding::Temporal(
                TemporalArrowToMssql::TimestampNanosecondToDateTime2
                | TemporalArrowToMssql::TimestampNanosecondTzToDateTime2,
            ) => BoundDirectColumn::TimestampNanosecond {
                column,
                mapping: encoder.mapping_for_column_index(column_index)?,
                nanosecond_policy: encoder.plan_options.nanosecond_policy,
                array: downcast_direct_array::<TimestampNanosecondArray>(array, column)?,
            },
            DirectColumnEncoding::Temporal(TemporalArrowToMssql::Time32SecondToTime) => {
                BoundDirectColumn::Time32Second {
                    column,
                    mapping: encoder.mapping_for_column_index(column_index)?,
                    array: downcast_direct_array::<Time32SecondArray>(array, column)?,
                }
            }
            DirectColumnEncoding::Temporal(TemporalArrowToMssql::Time32MillisecondToTime) => {
                BoundDirectColumn::Time32Millisecond {
                    column,
                    mapping: encoder.mapping_for_column_index(column_index)?,
                    array: downcast_direct_array::<Time32MillisecondArray>(array, column)?,
                }
            }
            DirectColumnEncoding::Temporal(TemporalArrowToMssql::Time64MicrosecondToTime) => {
                BoundDirectColumn::Time64Microsecond {
                    column,
                    mapping: encoder.mapping_for_column_index(column_index)?,
                    array: downcast_direct_array::<Time64MicrosecondArray>(array, column)?,
                }
            }
            DirectColumnEncoding::Temporal(TemporalArrowToMssql::Time64NanosecondToTime) => {
                BoundDirectColumn::Time64Nanosecond {
                    column,
                    mapping: encoder.mapping_for_column_index(column_index)?,
                    nanosecond_policy: encoder.plan_options.nanosecond_policy,
                    array: downcast_direct_array::<Time64NanosecondArray>(array, column)?,
                }
            }
            DirectColumnEncoding::Temporal(
                TemporalArrowToMssql::TimestampSecondTzToDateTimeOffset,
            ) => BoundDirectColumn::DateTimeOffsetSecond {
                column,
                mapping: encoder.mapping_for_column_index(column_index)?,
                array: downcast_direct_array::<TimestampSecondArray>(array, column)?,
            },
            DirectColumnEncoding::Temporal(
                TemporalArrowToMssql::TimestampMillisecondTzToDateTimeOffset,
            ) => BoundDirectColumn::DateTimeOffsetMillisecond {
                column,
                mapping: encoder.mapping_for_column_index(column_index)?,
                array: downcast_direct_array::<TimestampMillisecondArray>(array, column)?,
            },
            DirectColumnEncoding::Temporal(
                TemporalArrowToMssql::TimestampMicrosecondTzToDateTimeOffset,
            ) => BoundDirectColumn::DateTimeOffsetMicrosecond {
                column,
                mapping: encoder.mapping_for_column_index(column_index)?,
                array: downcast_direct_array::<TimestampMicrosecondArray>(array, column)?,
            },
            DirectColumnEncoding::Temporal(
                TemporalArrowToMssql::TimestampNanosecondTzToDateTimeOffset,
            ) => BoundDirectColumn::DateTimeOffsetNanosecond {
                column,
                mapping: encoder.mapping_for_column_index(column_index)?,
                nanosecond_policy: encoder.plan_options.nanosecond_policy,
                array: downcast_direct_array::<TimestampNanosecondArray>(array, column)?,
            },
        };

        columns.push(runtime);
    }

    Ok(columns)
}

fn bind_direct_nvarchar_array<'a>(
    array: &'a dyn Array,
    column: &'a plan::DirectColumnPlan,
    mapping: &SchemaMapping,
) -> Result<BoundDirectColumn<'a>> {
    if !is_string_family_to_nvarchar(mapping) {
        return Err(unsupported_planned_direct_type(
            column,
            "nvarchar",
            mapping.arrow().data_type(),
        ));
    }

    match array.data_type() {
        DataType::Utf8 => Ok(BoundDirectColumn::Utf8 {
            column,
            array: downcast_direct_array::<StringArray>(array, column)?,
        }),
        DataType::LargeUtf8 => Ok(BoundDirectColumn::LargeUtf8 {
            column,
            array: downcast_direct_array::<LargeStringArray>(array, column)?,
        }),
        DataType::Utf8View => Ok(BoundDirectColumn::Utf8View {
            column,
            array: downcast_direct_array::<StringViewArray>(array, column)?,
        }),
        other => Err(unsupported_planned_direct_type(column, "nvarchar", other)),
    }
}

fn bind_direct_varbinary_array<'a>(
    array: &'a dyn Array,
    column: &'a plan::DirectColumnPlan,
    mapping: &SchemaMapping,
) -> Result<BoundDirectColumn<'a>> {
    if !is_binary_family_to_varbinary(mapping) {
        return Err(unsupported_planned_direct_type(
            column,
            "varbinary",
            mapping.arrow().data_type(),
        ));
    }

    match array.data_type() {
        DataType::Binary => Ok(BoundDirectColumn::Binary {
            column,
            array: downcast_direct_array::<BinaryArray>(array, column)?,
        }),
        DataType::LargeBinary => Ok(BoundDirectColumn::LargeBinary {
            column,
            array: downcast_direct_array::<LargeBinaryArray>(array, column)?,
        }),
        DataType::BinaryView => Ok(BoundDirectColumn::BinaryView {
            column,
            array: downcast_direct_array::<BinaryViewArray>(array, column)?,
        }),
        other => Err(unsupported_planned_direct_type(column, "varbinary", other)),
    }
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
    Float16 {
        column: &'a plan::DirectColumnPlan,
        array: &'a Float16Array,
    },
    Float64 {
        column: &'a plan::DirectColumnPlan,
        array: &'a Float64Array,
    },
    Utf8 {
        column: &'a plan::DirectColumnPlan,
        array: &'a StringArray,
    },
    LargeUtf8 {
        column: &'a plan::DirectColumnPlan,
        array: &'a LargeStringArray,
    },
    Utf8View {
        column: &'a plan::DirectColumnPlan,
        array: &'a StringViewArray,
    },
    Binary {
        column: &'a plan::DirectColumnPlan,
        array: &'a BinaryArray,
    },
    LargeBinary {
        column: &'a plan::DirectColumnPlan,
        array: &'a LargeBinaryArray,
    },
    BinaryView {
        column: &'a plan::DirectColumnPlan,
        array: &'a BinaryViewArray,
    },
    FixedSizeBinary {
        column: &'a plan::DirectColumnPlan,
        classification: FixedSizeBinaryArrowToMssql,
        array: &'a FixedSizeBinaryArray,
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
        array: &'a TimestampSecondArray,
    },
    TimestampMillisecond {
        column: &'a plan::DirectColumnPlan,
        mapping: &'a SchemaMapping,
        array: &'a TimestampMillisecondArray,
    },
    TimestampMicrosecond {
        column: &'a plan::DirectColumnPlan,
        mapping: &'a SchemaMapping,
        array: &'a TimestampMicrosecondArray,
    },
    TimestampNanosecond {
        column: &'a plan::DirectColumnPlan,
        mapping: &'a SchemaMapping,
        nanosecond_policy: NanosecondPolicy,
        array: &'a TimestampNanosecondArray,
    },
    Time32Second {
        column: &'a plan::DirectColumnPlan,
        mapping: &'a SchemaMapping,
        array: &'a Time32SecondArray,
    },
    Time32Millisecond {
        column: &'a plan::DirectColumnPlan,
        mapping: &'a SchemaMapping,
        array: &'a Time32MillisecondArray,
    },
    Time64Microsecond {
        column: &'a plan::DirectColumnPlan,
        mapping: &'a SchemaMapping,
        array: &'a Time64MicrosecondArray,
    },
    Time64Nanosecond {
        column: &'a plan::DirectColumnPlan,
        mapping: &'a SchemaMapping,
        nanosecond_policy: NanosecondPolicy,
        array: &'a Time64NanosecondArray,
    },
    DateTimeOffsetSecond {
        column: &'a plan::DirectColumnPlan,
        mapping: &'a SchemaMapping,
        array: &'a TimestampSecondArray,
    },
    DateTimeOffsetMillisecond {
        column: &'a plan::DirectColumnPlan,
        mapping: &'a SchemaMapping,
        array: &'a TimestampMillisecondArray,
    },
    DateTimeOffsetMicrosecond {
        column: &'a plan::DirectColumnPlan,
        mapping: &'a SchemaMapping,
        array: &'a TimestampMicrosecondArray,
    },
    DateTimeOffsetNanosecond {
        column: &'a plan::DirectColumnPlan,
        mapping: &'a SchemaMapping,
        nanosecond_policy: NanosecondPolicy,
        array: &'a TimestampNanosecondArray,
    },
}

fn downcast_direct_array<'a, T: Array + 'static>(
    array: &'a dyn Array,
    column: &plan::DirectColumnPlan,
) -> Result<&'a T> {
    array.as_any().downcast_ref::<T>().ok_or_else(|| {
        value_conversion_error(row_column_diagnostic(
            column,
            0,
            DiagnosticCode::ValueTypeMismatch,
            format!(
                "runtime Arrow type {} does not match planned direct column type",
                array.data_type()
            ),
        ))
    })
}

fn unsupported_planned_direct_type(
    column: &plan::DirectColumnPlan,
    target_family: &str,
    data_type: &DataType,
) -> crate::Error {
    value_conversion_error(row_column_diagnostic(
        column,
        0,
        DiagnosticCode::ValueConversionUnsupported,
        format!(
            "planned Arrow type {data_type} is not supported by direct {target_family} binding"
        ),
    ))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_array::{
        ArrayRef, BinaryViewArray, FixedSizeBinaryArray, Int32Array, StringViewArray,
    };
    use arrow_schema::{DataType, Field, Schema};

    use super::*;
    use crate::{
        ArrowFieldRef, Identifier, MssqlColumn, MssqlType, MssqlTypeLength, SchemaMapping,
    };

    #[test]
    fn binds_large_variable_width_arrays_to_large_runtime_variants() {
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
                MssqlType::VarBinary(MssqlTypeLength::Max),
                true,
            ),
        ];
        let encoder = DirectEncoder::new(&mappings).unwrap();
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("id", DataType::Int32, false),
                Field::new("large_text", DataType::LargeUtf8, true),
                Field::new("large_bytes", DataType::LargeBinary, true),
            ])),
            vec![
                Arc::new(Int32Array::from(vec![7])) as ArrayRef,
                Arc::new(LargeStringArray::from(vec![Some("large")])) as ArrayRef,
                Arc::new(LargeBinaryArray::from_iter(vec![Some(&b"bytes"[..])])) as ArrayRef,
            ],
        )
        .unwrap();

        let bound = BoundDirectBatch::new(&encoder, &batch).unwrap();

        assert!(matches!(
            bound.columns()[0],
            BoundDirectColumn::Int32 { .. }
        ));
        let BoundDirectColumn::LargeUtf8 { array, .. } = bound.columns()[1] else {
            panic!("LargeUtf8 mapping should bind to LargeStringArray");
        };
        assert_eq!(array.value(0), "large");
        let BoundDirectColumn::LargeBinary { array, .. } = bound.columns()[2] else {
            panic!("LargeBinary mapping should bind to LargeBinaryArray");
        };
        assert_eq!(array.value(0), b"bytes");
    }

    #[test]
    fn binds_string_view_arrays_to_string_runtime_variant() {
        let mappings = vec![
            mapping(0, "id", DataType::Int32, MssqlType::Int, false),
            mapping(
                1,
                "text",
                DataType::Utf8,
                MssqlType::NVarChar(MssqlTypeLength::Max),
                true,
            ),
        ];
        let encoder = DirectEncoder::new(&mappings).unwrap();
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("id", DataType::Int32, false),
                Field::new("text", DataType::Utf8View, true),
            ])),
            vec![
                Arc::new(Int32Array::from(vec![7])) as ArrayRef,
                Arc::new(StringViewArray::from(vec![Some("view")])) as ArrayRef,
            ],
        )
        .unwrap();

        let bound = BoundDirectBatch::new(&encoder, &batch).unwrap();

        let BoundDirectColumn::Utf8View { array, .. } = bound.columns()[1] else {
            panic!("string-family mapping should bind to StringViewArray");
        };
        assert_eq!(array.value(0), "view");
    }

    #[test]
    fn binds_binary_view_arrays_to_binary_runtime_variant() {
        let mappings = vec![
            mapping(0, "id", DataType::Int32, MssqlType::Int, false),
            mapping(
                1,
                "bytes",
                DataType::Binary,
                MssqlType::VarBinary(MssqlTypeLength::Max),
                true,
            ),
        ];
        let encoder = DirectEncoder::new(&mappings).unwrap();
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("id", DataType::Int32, false),
                Field::new("bytes", DataType::BinaryView, true),
            ])),
            vec![
                Arc::new(Int32Array::from(vec![7])) as ArrayRef,
                Arc::new(BinaryViewArray::from(vec![Some(&b"view"[..])])) as ArrayRef,
            ],
        )
        .unwrap();

        let bound = BoundDirectBatch::new(&encoder, &batch).unwrap();

        let BoundDirectColumn::BinaryView { array, .. } = bound.columns()[1] else {
            panic!("binary-family mapping should bind to BinaryViewArray");
        };
        assert_eq!(array.value(0), b"view");
    }

    #[test]
    fn binds_fixed_size_binary_arrays_to_runtime_variant() {
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
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("id", DataType::Int32, false),
                Field::new("digest", DataType::FixedSizeBinary(3), true),
            ])),
            vec![
                Arc::new(Int32Array::from(vec![7])) as ArrayRef,
                Arc::new(
                    FixedSizeBinaryArray::try_from_sparse_iter_with_size(
                        [Some(&b"abc"[..])].into_iter(),
                        3,
                    )
                    .unwrap(),
                ) as ArrayRef,
            ],
        )
        .unwrap();

        let bound = BoundDirectBatch::new(&encoder, &batch).unwrap();

        let BoundDirectColumn::FixedSizeBinary {
            classification,
            array,
            ..
        } = bound.columns()[1]
        else {
            panic!("FixedSizeBinary mapping should bind to FixedSizeBinaryArray");
        };
        assert_eq!(
            classification,
            FixedSizeBinaryArrowToMssql::FixedSizeBinaryToBinary { length: 3 }
        );
        assert_eq!(array.value(0), b"abc");
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
}

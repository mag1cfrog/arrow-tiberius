//! Direct raw TDS bulk encoder internals.
#![allow(dead_code)]

use arrow_array::{BooleanArray, Float64Array, Int32Array, Int64Array, RecordBatch};

use crate::{
    Diagnostic, DiagnosticCode, DiagnosticSet, Error, Result, SchemaMapping,
    conversion::arrow_to_mssql::primitive::PrimitiveArrowToMssql,
    write::record_batch::validate_runtime_columns,
};

pub(crate) mod layout;
pub(crate) mod payload;
pub(crate) mod plan;
pub(crate) mod primitive;

use payload::EncodedRowsPayload;
use plan::{DirectColumnEncoding, DirectEncoderPlan, PrimitiveDirectMappings};
use primitive::{
    allocate_rows_payload_with_tokens, build_fixed_width_row_layout, fill_boolean_column,
    fill_float64_column, fill_int32_column, fill_int64_column,
    measure_primitive_column_cell_lengths, try_encode_non_nullable_fixed_width_primitive_rows,
};

/// Direct raw TDS encoder facade.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct DirectEncoder {
    mappings: Vec<SchemaMapping>,
    plan: DirectEncoderPlan,
}

impl DirectEncoder {
    /// Creates a direct encoder using the current supported direct mappings.
    pub(crate) fn new(mappings: &[SchemaMapping]) -> Result<Self> {
        Self::new_with_support(mappings, &PrimitiveDirectMappings)
    }

    /// Creates a direct encoder using an explicit support checker.
    pub(crate) fn new_with_support(
        mappings: &[SchemaMapping],
        support: &impl plan::DirectEncoderSupport,
    ) -> Result<Self> {
        Ok(Self {
            mappings: mappings.to_vec(),
            plan: DirectEncoderPlan::new(mappings, support)?,
        })
    }

    /// Returns the checked schema mappings consumed by this encoder.
    pub(crate) fn mappings(&self) -> &[SchemaMapping] {
        &self.mappings
    }

    /// Returns the checked direct encoder plan.
    pub(crate) const fn plan(&self) -> &DirectEncoderPlan {
        &self.plan
    }

    /// Encodes a runtime batch into complete raw TDS row payload bytes.
    pub(crate) fn encode_batch(&self, batch: &RecordBatch) -> Result<EncodedRowsPayload> {
        validate_runtime_columns(batch, &self.mappings)?;

        if self.plan.is_empty() && batch.num_rows() == 0 {
            return EncodedRowsPayload::new(Vec::new(), Vec::new());
        }

        if let Some(payload) =
            try_encode_non_nullable_fixed_width_primitive_rows(batch, self.plan.columns())?
        {
            return Ok(payload);
        }

        let layout = self.measure_layout(batch)?;
        let mut bytes = allocate_rows_payload_with_tokens(&layout);
        self.fill_columns(batch, &layout, &mut bytes)?;

        EncodedRowsPayload::new(bytes, layout.row_token_offsets().to_vec())
    }

    fn measure_layout(&self, batch: &RecordBatch) -> Result<layout::RowLayout> {
        let row_count = batch.num_rows();
        let column_count = self.plan.column_count();

        if row_count == 0 {
            return layout::RowLayout::new(Vec::new(), Vec::new(), Vec::new(), 0);
        }

        let mut cell_lengths = vec![0; row_count * column_count];

        for (column_index, column) in self.plan.columns().iter().enumerate() {
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

            measure_primitive_column_cell_lengths(
                array,
                column,
                column_index,
                column_count,
                &mut cell_lengths,
            )?;
        }

        build_fixed_width_row_layout(row_count, column_count, &cell_lengths)
    }

    fn fill_columns(
        &self,
        batch: &RecordBatch,
        layout: &layout::RowLayout,
        bytes: &mut [u8],
    ) -> Result<()> {
        let column_count = self.plan.column_count();

        for (column_index, column) in self.plan.columns().iter().enumerate() {
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

            match column.encoding() {
                DirectColumnEncoding::Primitive(PrimitiveArrowToMssql::BooleanToBit) => {
                    let array = downcast_direct_array::<BooleanArray>(array, column)?;
                    fill_boolean_column(array, column, column_index, column_count, layout, bytes)?;
                }
                DirectColumnEncoding::Primitive(PrimitiveArrowToMssql::Int32ToInt) => {
                    let array = downcast_direct_array::<Int32Array>(array, column)?;
                    fill_int32_column(array, column, column_index, column_count, layout, bytes)?;
                }
                DirectColumnEncoding::Primitive(PrimitiveArrowToMssql::Int64ToBigInt) => {
                    let array = downcast_direct_array::<Int64Array>(array, column)?;
                    fill_int64_column(array, column, column_index, column_count, layout, bytes)?;
                }
                DirectColumnEncoding::Primitive(PrimitiveArrowToMssql::Float64ToFloat) => {
                    let array = downcast_direct_array::<Float64Array>(array, column)?;
                    fill_float64_column(array, column, column_index, column_count, layout, bytes)?;
                }
                DirectColumnEncoding::Primitive(other) => {
                    return Err(unsupported_batch(format!(
                        "direct primitive fill is not implemented yet for {other:?}"
                    )));
                }
            }
        }

        Ok(())
    }
}

fn downcast_direct_array<'a, T: arrow_array::Array + 'static>(
    array: &'a dyn arrow_array::Array,
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

fn unsupported_batch(message: impl Into<String>) -> Error {
    Error::DirectEncoding {
        diagnostics: DiagnosticSet::from(vec![Diagnostic::error(
            DiagnosticCode::DirectEncodingUnsupportedBatch,
            message,
        )]),
    }
}

fn row_column_diagnostic(
    column: &plan::DirectColumnPlan,
    row_index: usize,
    code: DiagnosticCode,
    message: impl Into<String>,
) -> Diagnostic {
    Diagnostic::error(code, message)
        .with_field(crate::FieldRef::new(
            column.source_index(),
            column.source_name(),
        ))
        .with_row(row_index)
}

fn value_conversion_error(diagnostic: Diagnostic) -> Error {
    Error::ValueConversion {
        diagnostics: DiagnosticSet::from(vec![diagnostic]),
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_array::{ArrayRef, BooleanArray, Float64Array, Int32Array, Int64Array, RecordBatch};
    use arrow_schema::{DataType, Field, Schema};

    use crate::{
        ArrowFieldRef, DiagnosticCode, Error, Identifier, MssqlColumn, MssqlType, SchemaMapping,
        conversion::arrow_to_mssql::primitive::PrimitiveArrowToMssql,
    };

    use super::plan::{DirectColumnEncoding, DirectEncoderSupport, DirectMappingSupport};
    use super::{DirectEncoder, payload};

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
            mapping(
                3,
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
                Field::new("ratio", DataType::Float64, false),
            ],
            vec![
                Arc::new(BooleanArray::from(vec![true, false])) as ArrayRef,
                Arc::new(Int32Array::from(vec![1, -2])),
                Arc::new(Int64Array::from(vec![10, -20])),
                Arc::new(Float64Array::from(vec![1.25, -2.5])),
            ],
        );

        let payload = encoder.encode_batch(&batch).unwrap();

        assert_eq!(payload.row_token_offsets(), [0, 22]);
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
        let Error::DirectEncoding { diagnostics } = err else {
            panic!("expected direct encoding error");
        };

        assert_eq!(diagnostics.len(), 1);
        assert_eq!(
            diagnostics.all()[0].code(),
            DiagnosticCode::DirectEncodingUnsupportedBatch
        );
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

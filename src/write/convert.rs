//! Runtime record batch conversion scaffolding.

#![allow(dead_code)]

use arrow_array::{Array, RecordBatch};

use crate::{Diagnostic, DiagnosticCode, DiagnosticSet, FieldRef, Result, SchemaMapping};

/// Borrowed conversion view over one Arrow record batch and schema mappings.
#[derive(Debug)]
pub(crate) struct RecordBatchView<'a> {
    batch: &'a RecordBatch,
    mappings: &'a [SchemaMapping],
}

impl<'a> RecordBatchView<'a> {
    /// Creates a conversion view after validating batch columns against mappings.
    pub(crate) fn new(batch: &'a RecordBatch, mappings: &'a [SchemaMapping]) -> Result<Self> {
        validate_runtime_columns(batch, mappings)?;

        Ok(Self { batch, mappings })
    }

    /// Returns the number of rows in the runtime batch.
    pub(crate) fn row_count(&self) -> usize {
        self.batch.num_rows()
    }

    /// Returns the planned mappings in conversion order.
    pub(crate) const fn mappings(&self) -> &[SchemaMapping] {
        self.mappings
    }

    /// Checks that a row index is inside the runtime batch.
    pub(crate) fn check_row_index(&self, row_index: usize) -> Result<()> {
        if row_index < self.row_count() {
            return Ok(());
        }

        let message = format!(
            "row index {row_index} is outside runtime batch with {} row(s)",
            self.row_count()
        );
        Err(value_conversion_error(
            Diagnostic::error(DiagnosticCode::RowIndexOutOfBounds, message).with_row(row_index),
        ))
    }
}

fn validate_runtime_columns(batch: &RecordBatch, mappings: &[SchemaMapping]) -> Result<()> {
    if batch.num_columns() < mappings.len() {
        let mapping = &mappings[batch.num_columns()];
        return Err(value_conversion_error(mapping_diagnostic(
            mapping,
            DiagnosticCode::ValueTypeMismatch,
            format!(
                "planned column index {} is outside runtime batch with {} column(s)",
                mapping.arrow().index(),
                batch.num_columns()
            ),
        )));
    }

    if batch.num_columns() > mappings.len() {
        return Err(value_conversion_error(Diagnostic::error(
            DiagnosticCode::ValueTypeMismatch,
            format!(
                "runtime batch has {} column(s) but mappings contain {} column(s)",
                batch.num_columns(),
                mappings.len()
            ),
        )));
    }

    for (position, (array, mapping)) in batch.columns().iter().zip(mappings).enumerate() {
        if mapping.arrow().index() != position {
            return Err(value_conversion_error(mapping_diagnostic(
                mapping,
                DiagnosticCode::ValueTypeMismatch,
                format!(
                    "mapping position {position} does not match planned Arrow field index {}",
                    mapping.arrow().index()
                ),
            )));
        }

        validate_runtime_column(array.as_ref(), mapping)?;
    }

    Ok(())
}

fn validate_runtime_column(array: &dyn Array, mapping: &SchemaMapping) -> Result<()> {
    if array.data_type() != mapping.arrow().data_type() {
        return Err(value_conversion_error(mapping_diagnostic(
            mapping,
            DiagnosticCode::ValueTypeMismatch,
            format!(
                "runtime Arrow type {} does not match planned Arrow type {}",
                array.data_type(),
                mapping.arrow().data_type()
            ),
        )));
    }

    Ok(())
}

fn mapping_diagnostic(
    mapping: &SchemaMapping,
    code: DiagnosticCode,
    message: impl Into<String>,
) -> Diagnostic {
    Diagnostic::error(code, message).with_field(FieldRef::new(
        mapping.arrow().index(),
        mapping.arrow().name(),
    ))
}

fn value_conversion_error(diagnostic: Diagnostic) -> crate::Error {
    crate::Error::ValueConversion {
        diagnostics: DiagnosticSet::from(vec![diagnostic]),
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_array::{ArrayRef, BooleanArray, Int32Array, Int64Array, RecordBatch};
    use arrow_schema::{DataType, Field, Schema};

    use super::RecordBatchView;
    use crate::{
        ArrowFieldRef, DiagnosticCode, Error, Identifier, MssqlColumn, MssqlProfile, MssqlType,
        PlanOptions, SchemaMapping, plan_arrow_schema_to_mssql_mappings,
    };

    #[test]
    fn accepts_matching_batch_and_mappings() {
        let mappings = mappings_for_schema(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("active", DataType::Boolean, true),
        ]));
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("id", DataType::Int32, false),
                Field::new("active", DataType::Boolean, true),
            ])),
            vec![
                Arc::new(Int32Array::from(vec![1_i32, 2])) as ArrayRef,
                Arc::new(BooleanArray::from(vec![Some(true), None])),
            ],
        )
        .unwrap();

        let view = RecordBatchView::new(&batch, &mappings).unwrap();

        assert_eq!(view.row_count(), 2);
        assert_eq!(view.mappings().len(), 2);
        view.check_row_index(1).unwrap();
    }

    #[test]
    fn rejects_planned_column_index_outside_runtime_batch() {
        let mappings = mappings_for_schema(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("active", DataType::Boolean, true),
        ]));
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)])),
            vec![Arc::new(Int32Array::from(vec![1_i32]))],
        )
        .unwrap();

        let err = RecordBatchView::new(&batch, &mappings).unwrap_err();

        assert_single_diagnostic(
            err,
            DiagnosticCode::ValueTypeMismatch,
            None,
            Some((1, "active")),
        );
    }

    #[test]
    fn rejects_extra_runtime_columns_without_mappings() {
        let mappings =
            mappings_for_schema(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("id", DataType::Int32, false),
                Field::new("extra", DataType::Boolean, true),
            ])),
            vec![
                Arc::new(Int32Array::from(vec![1_i32])) as ArrayRef,
                Arc::new(BooleanArray::from(vec![Some(true)])),
            ],
        )
        .unwrap();

        let err = RecordBatchView::new(&batch, &mappings).unwrap_err();

        assert_single_diagnostic(err, DiagnosticCode::ValueTypeMismatch, None, None);
    }

    #[test]
    fn rejects_mapping_position_that_disagrees_with_arrow_index() {
        let mappings = vec![SchemaMapping::new(
            ArrowFieldRef::new(1, "id".to_owned(), false, DataType::Int32),
            MssqlColumn::new(Identifier::new("id").unwrap(), MssqlType::Int, false),
        )];
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)])),
            vec![Arc::new(Int32Array::from(vec![1_i32]))],
        )
        .unwrap();

        let err = RecordBatchView::new(&batch, &mappings).unwrap_err();

        assert_single_diagnostic(
            err,
            DiagnosticCode::ValueTypeMismatch,
            None,
            Some((1, "id")),
        );
    }

    #[test]
    fn rejects_runtime_arrow_type_mismatch() {
        let mappings = mappings_for_schema(Schema::new(vec![Field::new(
            "number",
            DataType::Int32,
            true,
        )]));
        let batch = unsafe_batch_for_field(
            "number",
            DataType::Int32,
            Arc::new(Int64Array::from(vec![1_i64])),
            true,
        );

        let err = RecordBatchView::new(&batch, &mappings).unwrap_err();

        assert_single_diagnostic(
            err,
            DiagnosticCode::ValueTypeMismatch,
            None,
            Some((0, "number")),
        );
    }

    #[test]
    fn rejects_row_index_out_of_bounds() {
        let mappings = mappings_for_schema(Schema::new(vec![Field::new(
            "number",
            DataType::Int32,
            true,
        )]));
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "number",
                DataType::Int32,
                true,
            )])),
            vec![Arc::new(Int32Array::from(vec![1_i32]))],
        )
        .unwrap();
        let view = RecordBatchView::new(&batch, &mappings).unwrap();

        let err = view.check_row_index(1).unwrap_err();

        assert_single_diagnostic(err, DiagnosticCode::RowIndexOutOfBounds, Some(1), None);
    }

    fn mappings_for_schema(schema: Schema) -> Vec<SchemaMapping> {
        plan_arrow_schema_to_mssql_mappings(
            Arc::new(schema),
            MssqlProfile::sql_server_2016_compat_100(),
            PlanOptions::default(),
        )
        .unwrap()
        .into_parts()
        .0
    }

    fn unsafe_batch_for_field(
        name: &str,
        data_type: DataType,
        array: ArrayRef,
        nullable: bool,
    ) -> RecordBatch {
        // SAFETY: this deliberately constructs a mismatched batch for converter
        // validation tests. The test only inspects metadata and never reads the
        // mismatched array through the declared schema type.
        unsafe {
            RecordBatch::new_unchecked(
                Arc::new(Schema::new(vec![Field::new(name, data_type, nullable)])),
                vec![array],
                1,
            )
        }
    }

    fn assert_single_diagnostic(
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

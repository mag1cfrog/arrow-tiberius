//! Runtime record batch view and Arrow-to-MSSQL semantic conversion.

mod validate;

use arrow_array::RecordBatch;

use crate::{
    Diagnostic, DiagnosticCode, DiagnosticSet, FieldRef, PlanOptions, Result, SchemaMapping,
    arrow::cell::{ArrowCell, extract_arrow_cell},
    mssql::cell::{
        MssqlCell,
        from_arrow::{ArrowToMssqlRuntimeMapping, mssql_cell_from_arrow_cell},
    },
};
pub(crate) use validate::validate_runtime_columns;

/// Borrowed conversion view over one Arrow record batch and schema mappings.
#[derive(Debug)]
pub(crate) struct RecordBatchView<'a> {
    batch: &'a RecordBatch,
    mappings: &'a [SchemaMapping],
    plan_options: PlanOptions,
}

impl<'a> RecordBatchView<'a> {
    /// Creates a conversion view after validating batch columns against mappings.
    #[cfg(test)]
    pub(crate) fn new(batch: &'a RecordBatch, mappings: &'a [SchemaMapping]) -> Result<Self> {
        Self::new_with_options(batch, mappings, &PlanOptions::default())
    }

    /// Creates a conversion view with explicit write conversion policies.
    pub(crate) fn new_with_options(
        batch: &'a RecordBatch,
        mappings: &'a [SchemaMapping],
        plan_options: &PlanOptions,
    ) -> Result<Self> {
        validate_runtime_columns(batch, mappings)?;

        Ok(Self {
            batch,
            mappings,
            plan_options: *plan_options,
        })
    }

    /// Returns the number of rows in the runtime batch.
    pub(crate) fn row_count(&self) -> usize {
        self.batch.num_rows()
    }

    /// Returns the planned mappings in conversion order.
    #[cfg(test)]
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

    /// Extracts one borrowed Arrow cell from a planned mapping and row index.
    fn arrow_cell(&self, mapping: &SchemaMapping, row_index: usize) -> Result<ArrowCell<'_>> {
        self.check_row_index(row_index)?;

        let Some(array) = self
            .batch
            .columns()
            .get(mapping.arrow().index())
            .map(AsRef::as_ref)
        else {
            return Err(value_conversion_error(row_mapping_diagnostic(
                mapping,
                row_index,
                DiagnosticCode::ValueTypeMismatch,
                "planned column index is outside the runtime batch",
            )));
        };

        extract_arrow_cell(array, mapping, row_index)
    }

    /// Converts one planned cell into a semantic SQL Server cell.
    fn mssql_cell(&self, mapping: &SchemaMapping, row_index: usize) -> Result<MssqlCell<'_>> {
        let cell = self.arrow_cell(mapping, row_index)?;
        let runtime_mapping = ArrowToMssqlRuntimeMapping::new(mapping, &self.plan_options);
        mssql_cell_from_arrow_cell(runtime_mapping, cell, row_index)
    }

    /// Converts one runtime row into semantic SQL Server cells in mapping order.
    pub(crate) fn mssql_row(&self, row_index: usize) -> Result<Vec<MssqlCell<'_>>> {
        self.check_row_index(row_index)?;

        let mut cells = Vec::with_capacity(self.mappings.len());
        for mapping in self.mappings {
            cells.push(self.mssql_cell(mapping, row_index)?);
        }

        Ok(cells)
    }
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

fn row_mapping_diagnostic(
    mapping: &SchemaMapping,
    row_index: usize,
    code: DiagnosticCode,
    message: impl Into<String>,
) -> Diagnostic {
    mapping_diagnostic(mapping, code, message).with_row(row_index)
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
        ArrayRef, BinaryArray, BooleanArray, Float64Array, Int32Array, Int64Array, RecordBatch,
        StringArray,
    };
    use arrow_schema::{DataType, Field, Schema};

    use super::RecordBatchView;
    use crate::mssql::cell::MssqlCell;
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
    fn converts_runtime_row_to_mssql_cells_in_mapping_order() {
        let mappings = mappings_for_schema(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("active", DataType::Boolean, true),
            Field::new("name", DataType::Utf8, true),
            Field::new("payload", DataType::Binary, true),
        ]));
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("id", DataType::Int32, false),
                Field::new("active", DataType::Boolean, true),
                Field::new("name", DataType::Utf8, true),
                Field::new("payload", DataType::Binary, true),
            ])),
            vec![
                Arc::new(Int32Array::from(vec![1_i32, 2])) as ArrayRef,
                Arc::new(BooleanArray::from(vec![Some(true), None])),
                Arc::new(StringArray::from(vec![Some("first"), Some("second")])),
                Arc::new(BinaryArray::from(vec![Some(&b"abc"[..]), None])),
            ],
        )
        .unwrap();
        let view = RecordBatchView::new(&batch, &mappings).unwrap();

        let first_row = view.mssql_row(0).unwrap();
        assert_eq!(
            first_row,
            vec![
                MssqlCell::Int(Some(1)),
                MssqlCell::Bit(Some(true)),
                MssqlCell::NVarChar(Some("first")),
                MssqlCell::VarBinary(Some(b"abc")),
            ]
        );

        let second_row = view.mssql_row(1).unwrap();
        assert_eq!(
            second_row,
            vec![
                MssqlCell::Int(Some(2)),
                MssqlCell::Bit(None),
                MssqlCell::NVarChar(Some("second")),
                MssqlCell::VarBinary(None),
            ]
        );
    }

    #[test]
    fn row_helpers_reject_row_index_out_of_bounds() {
        let mappings =
            mappings_for_schema(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)])),
            vec![Arc::new(Int32Array::from(vec![1_i32]))],
        )
        .unwrap();
        let view = RecordBatchView::new(&batch, &mappings).unwrap();

        let err = view.mssql_row(1).unwrap_err();
        assert_single_diagnostic(err, DiagnosticCode::RowIndexOutOfBounds, Some(1), None);
    }

    #[test]
    fn row_helpers_preserve_conversion_diagnostics() {
        let mappings = mappings_for_schema(Schema::new(vec![Field::new(
            "ratio",
            DataType::Float64,
            true,
        )]));
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "ratio",
                DataType::Float64,
                true,
            )])),
            vec![Arc::new(Float64Array::from(vec![f64::NAN]))],
        )
        .unwrap();
        let view = RecordBatchView::new(&batch, &mappings).unwrap();

        let err = view.mssql_row(0).unwrap_err();
        assert_single_diagnostic(
            err,
            DiagnosticCode::NonFiniteFloat,
            Some(0),
            Some((0, "ratio")),
        );
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
            DiagnosticCode::SchemaMismatch,
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

        assert_single_diagnostic(err, DiagnosticCode::SchemaMismatch, None, None);
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

        assert_single_diagnostic(err, DiagnosticCode::SchemaMismatch, None, Some((1, "id")));
    }

    #[test]
    fn rejects_runtime_field_name_mismatch_even_when_type_matches() {
        let mappings = mappings_for_schema(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("amount", DataType::Int32, false),
        ]));
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("amount", DataType::Int32, false),
                Field::new("id", DataType::Int32, false),
            ])),
            vec![
                Arc::new(Int32Array::from(vec![100_i32])) as ArrayRef,
                Arc::new(Int32Array::from(vec![1_i32])),
            ],
        )
        .unwrap();

        let err = RecordBatchView::new(&batch, &mappings).unwrap_err();

        assert_single_diagnostic(err, DiagnosticCode::SchemaMismatch, None, Some((0, "id")));
    }

    #[test]
    fn rejects_runtime_field_rename_even_when_position_and_type_match() {
        let mappings =
            mappings_for_schema(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "renamed_id",
                DataType::Int32,
                false,
            )])),
            vec![Arc::new(Int32Array::from(vec![1_i32]))],
        )
        .unwrap();

        let err = RecordBatchView::new(&batch, &mappings).unwrap_err();

        assert_single_diagnostic(err, DiagnosticCode::SchemaMismatch, None, Some((0, "id")));
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
            DiagnosticCode::SchemaMismatch,
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
        mappings_for_schema_with_options(schema, PlanOptions::default())
    }

    fn mappings_for_schema_with_options(
        schema: Schema,
        options: PlanOptions,
    ) -> Vec<SchemaMapping> {
        plan_arrow_schema_to_mssql_mappings(
            Arc::new(schema),
            MssqlProfile::sql_server_2016_compat_100(),
            options,
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

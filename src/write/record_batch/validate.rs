//! Runtime RecordBatch validation against planned schema mappings.

use arrow_array::{Array, RecordBatch};

use crate::{Diagnostic, DiagnosticCode, DiagnosticSet, FieldRef, Result, SchemaMapping};

pub(super) fn validate_runtime_columns(
    batch: &RecordBatch,
    mappings: &[SchemaMapping],
) -> Result<()> {
    if batch.num_columns() < mappings.len() {
        let mapping = &mappings[batch.num_columns()];
        return Err(value_conversion_error(mapping_diagnostic(
            mapping,
            DiagnosticCode::SchemaMismatch,
            format!(
                "planned column index {} is outside runtime batch with {} column(s)",
                mapping.arrow().index(),
                batch.num_columns()
            ),
        )));
    }

    if batch.num_columns() > mappings.len() {
        return Err(value_conversion_error(Diagnostic::error(
            DiagnosticCode::SchemaMismatch,
            format!(
                "runtime batch has {} column(s) but mappings contain {} column(s)",
                batch.num_columns(),
                mappings.len()
            ),
        )));
    }

    for (position, (field, (array, mapping))) in batch
        .schema()
        .fields()
        .iter()
        .zip(batch.columns().iter().zip(mappings))
        .enumerate()
    {
        if mapping.arrow().index() != position {
            return Err(value_conversion_error(mapping_diagnostic(
                mapping,
                DiagnosticCode::SchemaMismatch,
                format!(
                    "mapping position {position} does not match planned Arrow field index {}",
                    mapping.arrow().index()
                ),
            )));
        }

        if field.name() != mapping.arrow().name() {
            return Err(value_conversion_error(mapping_diagnostic(
                mapping,
                DiagnosticCode::SchemaMismatch,
                format!(
                    "runtime Arrow field name {} does not match planned Arrow field name {}",
                    field.name(),
                    mapping.arrow().name()
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
            DiagnosticCode::SchemaMismatch,
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

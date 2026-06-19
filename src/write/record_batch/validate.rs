//! Runtime RecordBatch validation against planned schema mappings.

use arrow_array::{Array, RecordBatch};
use arrow_schema::{Field, Schema};

use crate::{Diagnostic, DiagnosticCode, DiagnosticSet, FieldRef, Result, SchemaMapping};

/// Validates a runtime Arrow schema against planned Arrow-side schema mappings.
///
/// This is a strict schema-contract check for callers that plan once and later
/// want to confirm a runtime schema still matches that plan before writing. It
/// verifies field count, field order, planned Arrow index, field name, Arrow
/// data type, and Arrow nullability.
///
/// This function does not inspect row values. A nullable runtime value in a
/// non-nullable SQL Server target column is still a value-conversion error and
/// is reported by writer paths that inspect rows.
pub fn validate_arrow_schema_against_mappings(
    schema: &Schema,
    mappings: &[SchemaMapping],
) -> Result<()> {
    validate_schema_field_count_against_mappings(schema, mappings)?;

    for (position, (field, mapping)) in schema.fields().iter().zip(mappings).enumerate() {
        validate_schema_field_shape_against_mapping(position, field, mapping)?;
        validate_schema_field_nullability_against_mapping(field, mapping)?;
    }

    Ok(())
}

fn validate_schema_shape_for_record_batch_encoding(
    schema: &Schema,
    mappings: &[SchemaMapping],
) -> Result<()> {
    validate_schema_field_count_against_mappings(schema, mappings)?;

    for (position, (field, mapping)) in schema.fields().iter().zip(mappings).enumerate() {
        validate_schema_field_shape_against_mapping(position, field, mapping)?;
    }

    Ok(())
}

fn validate_schema_field_count_against_mappings(
    schema: &Schema,
    mappings: &[SchemaMapping],
) -> Result<()> {
    if schema.fields().len() < mappings.len() {
        let mapping = &mappings[schema.fields().len()];
        return Err(value_conversion_error(mapping_diagnostic(
            mapping,
            DiagnosticCode::SchemaMismatch,
            format!(
                "planned column index {} is outside runtime schema with {} field(s)",
                mapping.arrow().index(),
                schema.fields().len()
            ),
        )));
    }

    if schema.fields().len() > mappings.len() {
        return Err(value_conversion_error(Diagnostic::error(
            DiagnosticCode::SchemaMismatch,
            format!(
                "runtime schema has {} field(s) but mappings contain {} field(s)",
                schema.fields().len(),
                mappings.len()
            ),
        )));
    }

    Ok(())
}

/// Validates a RecordBatch schema against planned Arrow-side schema mappings.
///
/// This is a convenience wrapper around
/// [`validate_arrow_schema_against_mappings`] for callers that already have a
/// batch. It validates `batch.schema()` only; it does not scan arrays or row
/// values.
pub fn validate_record_batch_schema_against_mappings(
    batch: &RecordBatch,
    mappings: &[SchemaMapping],
) -> Result<()> {
    validate_arrow_schema_against_mappings(batch.schema().as_ref(), mappings)
}

pub(crate) fn validate_record_batch_encoding_shape(
    batch: &RecordBatch,
    mappings: &[SchemaMapping],
) -> Result<()> {
    validate_schema_shape_for_record_batch_encoding(batch.schema().as_ref(), mappings)?;

    // RecordBatch::try_new already enforces schema/array type consistency. This
    // internal guard catches unchecked batches before row conversion.
    for (array, mapping) in batch.columns().iter().zip(mappings) {
        validate_unchecked_column_array_against_mapping(array.as_ref(), mapping)?;
    }

    Ok(())
}

fn validate_schema_field_shape_against_mapping(
    position: usize,
    field: &Field,
    mapping: &SchemaMapping,
) -> Result<()> {
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

    if field.data_type() != mapping.arrow().data_type() {
        return Err(value_conversion_error(mapping_diagnostic(
            mapping,
            DiagnosticCode::SchemaMismatch,
            format!(
                "runtime Arrow type {} does not match planned Arrow type {}",
                field.data_type(),
                mapping.arrow().data_type()
            ),
        )));
    }

    Ok(())
}

fn validate_schema_field_nullability_against_mapping(
    field: &Field,
    mapping: &SchemaMapping,
) -> Result<()> {
    if field.is_nullable() != mapping.arrow().nullable() {
        return Err(value_conversion_error(mapping_diagnostic(
            mapping,
            DiagnosticCode::SchemaMismatch,
            format!(
                "runtime Arrow nullability {} does not match planned Arrow nullability {}",
                field.is_nullable(),
                mapping.arrow().nullable()
            ),
        )));
    }

    Ok(())
}

fn validate_unchecked_column_array_against_mapping(
    array: &dyn Array,
    mapping: &SchemaMapping,
) -> Result<()> {
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

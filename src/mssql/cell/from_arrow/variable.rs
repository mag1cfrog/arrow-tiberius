//! Variable-length Arrow-to-MSSQL runtime cell conversion.

use crate::{
    DiagnosticCode, MssqlTypeLength, Result, SchemaMapping, arrow::cell::ArrowCell,
    mssql::cell::MssqlCell,
};

use super::{row_mapping_diagnostic, value_conversion_error, value_too_long_error};

pub(super) fn nvar_char_cell<'a>(
    mapping: &SchemaMapping,
    row_index: usize,
    length: MssqlTypeLength,
    cell: ArrowCell<'a>,
) -> Result<MssqlCell<'a>> {
    let value = mssql_nvarchar_value(mapping, row_index, cell)?;
    let code_units = value.encode_utf16().count();

    if exceeds_length(length, code_units) {
        return Err(value_too_long_error(
            mapping,
            row_index,
            format!(
                "string value has {code_units} UTF-16 code unit(s), exceeding planned {}",
                mapping.mssql().ty().to_sql()
            ),
        ));
    }

    Ok(MssqlCell::NVarChar(Some(value)))
}

pub(super) fn var_binary_cell<'a>(
    mapping: &SchemaMapping,
    row_index: usize,
    length: MssqlTypeLength,
    cell: ArrowCell<'a>,
) -> Result<MssqlCell<'a>> {
    let value = mssql_varbinary_value(mapping, row_index, cell)?;
    let bytes = value.len();

    if exceeds_length(length, bytes) {
        return Err(value_too_long_error(
            mapping,
            row_index,
            format!(
                "binary value has {bytes} byte(s), exceeding planned {}",
                mapping.mssql().ty().to_sql()
            ),
        ));
    }

    Ok(MssqlCell::VarBinary(Some(value)))
}

fn mssql_nvarchar_value<'a>(
    mapping: &SchemaMapping,
    row_index: usize,
    cell: ArrowCell<'a>,
) -> Result<&'a str> {
    match cell {
        ArrowCell::Utf8(value) => Ok(value),
        other => Err(value_conversion_error(row_mapping_diagnostic(
            mapping,
            row_index,
            DiagnosticCode::ValueTypeMismatch,
            format!("expected Arrow UTF-8 payload, got {other:?}"),
        ))),
    }
}

fn mssql_varbinary_value<'a>(
    mapping: &SchemaMapping,
    row_index: usize,
    cell: ArrowCell<'a>,
) -> Result<&'a [u8]> {
    match cell {
        ArrowCell::Binary(value) => Ok(value),
        other => Err(value_conversion_error(row_mapping_diagnostic(
            mapping,
            row_index,
            DiagnosticCode::ValueTypeMismatch,
            format!("expected Arrow binary payload, got {other:?}"),
        ))),
    }
}

fn exceeds_length(length: MssqlTypeLength, actual: usize) -> bool {
    match length {
        MssqlTypeLength::Bounded(limit) => actual > limit,
        MssqlTypeLength::Max => false,
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_schema::{DataType, Field, Schema};

    use super::super::{ArrowToMssqlRuntimeMapping, mssql_cell_from_arrow_cell};
    use crate::{
        BinaryPolicy, DiagnosticCode, MssqlProfile, PlanOptions, SchemaMapping, StringPolicy,
        arrow::cell::ArrowCell, mssql::cell::MssqlCell, plan_arrow_schema_to_mssql_mappings,
    };

    #[test]
    fn converts_empty_ascii_and_non_ascii_strings() {
        let mappings = mappings_for_schema(Schema::new(vec![
            Field::new("text", DataType::Utf8, true),
            Field::new("large_text", DataType::LargeUtf8, true),
        ]));

        assert_eq!(
            convert_cell(&mappings[0], ArrowCell::Utf8(""), 0).unwrap(),
            MssqlCell::NVarChar(Some(""))
        );
        assert_eq!(
            convert_cell(&mappings[0], ArrowCell::Utf8("ascii"), 1).unwrap(),
            MssqlCell::NVarChar(Some("ascii"))
        );
        assert_eq!(
            convert_cell(&mappings[0], ArrowCell::Utf8("Tokyo"), 2).unwrap(),
            MssqlCell::NVarChar(Some("Tokyo"))
        );
        assert_eq!(
            convert_cell(&mappings[1], ArrowCell::Utf8(""), 0).unwrap(),
            MssqlCell::NVarChar(Some(""))
        );
        assert_eq!(
            convert_cell(&mappings[1], ArrowCell::Utf8("ascii"), 1).unwrap(),
            MssqlCell::NVarChar(Some("ascii"))
        );
        assert_eq!(
            convert_cell(&mappings[1], ArrowCell::Utf8("emoji"), 2).unwrap(),
            MssqlCell::NVarChar(Some("emoji"))
        );
    }

    #[test]
    fn converts_empty_and_non_empty_binary_values() {
        let mappings = mappings_for_schema(Schema::new(vec![
            Field::new("bytes", DataType::Binary, true),
            Field::new("large_bytes", DataType::LargeBinary, true),
        ]));

        assert_eq!(
            convert_cell(&mappings[0], ArrowCell::Binary(b""), 0).unwrap(),
            MssqlCell::VarBinary(Some(b""))
        );
        assert_eq!(
            convert_cell(&mappings[0], ArrowCell::Binary(b"abc"), 1).unwrap(),
            MssqlCell::VarBinary(Some(b"abc"))
        );
        assert_eq!(
            convert_cell(&mappings[1], ArrowCell::Binary(b""), 0).unwrap(),
            MssqlCell::VarBinary(Some(b""))
        );
        assert_eq!(
            convert_cell(&mappings[1], ArrowCell::Binary(b"large"), 1).unwrap(),
            MssqlCell::VarBinary(Some(b"large"))
        );
    }

    #[test]
    fn rejects_bounded_nvarchar_by_utf16_code_units() {
        let mappings = mappings_for_schema_with_options(
            Schema::new(vec![Field::new("text", DataType::Utf8, true)]),
            PlanOptions {
                string_policy: StringPolicy::NVarChar(2),
                ..PlanOptions::default()
            },
        );

        assert_eq!(
            convert_cell(&mappings[0], ArrowCell::Utf8("ab"), 0).unwrap(),
            MssqlCell::NVarChar(Some("ab"))
        );
        assert_eq!(
            convert_cell(&mappings[0], ArrowCell::Utf8("🙂"), 1).unwrap(),
            MssqlCell::NVarChar(Some("🙂"))
        );
        let err = convert_cell(&mappings[0], ArrowCell::Utf8("abc"), 2).unwrap_err();

        assert_single_diagnostic(
            err,
            DiagnosticCode::ValueTooLong,
            Some(2),
            Some((0, "text")),
        );
    }

    #[test]
    fn rejects_bounded_varbinary_by_byte_count() {
        let mappings = mappings_for_schema_with_options(
            Schema::new(vec![Field::new("bytes", DataType::Binary, true)]),
            PlanOptions {
                binary_policy: BinaryPolicy::VarBinary(2),
                ..PlanOptions::default()
            },
        );

        assert_eq!(
            convert_cell(&mappings[0], ArrowCell::Binary(b""), 0).unwrap(),
            MssqlCell::VarBinary(Some(b""))
        );
        assert_eq!(
            convert_cell(&mappings[0], ArrowCell::Binary(b"ab"), 1).unwrap(),
            MssqlCell::VarBinary(Some(b"ab"))
        );
        let err = convert_cell(&mappings[0], ArrowCell::Binary(b"abc"), 2).unwrap_err();

        assert_single_diagnostic(
            err,
            DiagnosticCode::ValueTooLong,
            Some(2),
            Some((0, "bytes")),
        );
    }

    fn convert_cell<'a>(
        mapping: &SchemaMapping,
        cell: ArrowCell<'a>,
        row_index: usize,
    ) -> crate::Result<MssqlCell<'a>> {
        let options = PlanOptions::default();
        let runtime_mapping = ArrowToMssqlRuntimeMapping::new(mapping, &options);
        mssql_cell_from_arrow_cell(runtime_mapping, cell, row_index)
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

    fn assert_single_diagnostic(
        err: crate::Error,
        expected_code: DiagnosticCode,
        expected_row: Option<usize>,
        expected_field: Option<(usize, &str)>,
    ) {
        let crate::Error::ValueConversion { diagnostics } = err else {
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

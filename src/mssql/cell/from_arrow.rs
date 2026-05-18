//! Arrow-to-MSSQL runtime cell conversion.

mod decimal;
mod primitive;
mod temporal;
mod variable;

use crate::{
    Diagnostic, DiagnosticCode, DiagnosticSet, FieldRef, MssqlType, NanosecondPolicy, PlanOptions,
    Result, SchemaMapping, arrow::cell::ArrowCell,
};

use super::MssqlCell;
use decimal::{mssql_decimal_value, supports_null_decimal_cell};
use primitive::{
    mssql_bigint_value, mssql_bit_value, mssql_float_value, mssql_int_value, mssql_real_value,
    mssql_smallint_value, mssql_tinyint_value,
};
use temporal::{
    mssql_date_value, mssql_datetime2_value, mssql_datetimeoffset_value, null_datetime2_cell,
    null_datetimeoffset_cell,
};
use variable::{nvar_char_cell, var_binary_cell};

/// Direction-specific runtime context for Arrow-to-MSSQL value conversion.
#[derive(Debug, Clone, Copy)]
pub(crate) struct ArrowToMssqlRuntimeMapping<'a> {
    mapping: &'a SchemaMapping,
    nanosecond_policy: NanosecondPolicy,
}

impl<'a> ArrowToMssqlRuntimeMapping<'a> {
    /// Creates runtime conversion context from structural mapping and write options.
    pub(crate) const fn new(mapping: &'a SchemaMapping, options: &PlanOptions) -> Self {
        Self {
            mapping,
            nanosecond_policy: options.nanosecond_policy,
        }
    }

    /// Returns the structural Arrow/MSSQL mapping.
    pub(crate) const fn mapping(self) -> &'a SchemaMapping {
        self.mapping
    }

    /// Returns the nanosecond timestamp policy selected for write conversion.
    pub(crate) const fn nanosecond_policy(self) -> NanosecondPolicy {
        self.nanosecond_policy
    }
}

pub(crate) fn mssql_cell_from_arrow_cell<'a>(
    runtime_mapping: ArrowToMssqlRuntimeMapping<'_>,
    cell: ArrowCell<'a>,
    row_index: usize,
) -> Result<MssqlCell<'a>> {
    let mapping = runtime_mapping.mapping();

    if matches!(cell, ArrowCell::Null) {
        if !mapping.mssql().nullable() {
            return Err(value_conversion_error(row_mapping_diagnostic(
                mapping,
                row_index,
                DiagnosticCode::NullInNonNullableColumn,
                "null value in non-nullable planned column",
            )));
        }

        return null_mssql_cell(mapping, row_index);
    }

    match mapping.mssql().ty() {
        MssqlType::Bit => Ok(MssqlCell::Bit(Some(mssql_bit_value(
            mapping, row_index, cell,
        )?))),
        MssqlType::TinyInt => Ok(MssqlCell::TinyInt(Some(mssql_tinyint_value(
            mapping, row_index, cell,
        )?))),
        MssqlType::SmallInt => Ok(MssqlCell::SmallInt(Some(mssql_smallint_value(
            mapping, row_index, cell,
        )?))),
        MssqlType::Int => Ok(MssqlCell::Int(Some(mssql_int_value(
            mapping, row_index, cell,
        )?))),
        MssqlType::BigInt => Ok(MssqlCell::BigInt(Some(mssql_bigint_value(
            mapping, row_index, cell,
        )?))),
        MssqlType::Decimal { .. } => Ok(MssqlCell::Decimal(Some(mssql_decimal_value(
            mapping, row_index, cell,
        )?))),
        MssqlType::Date => Ok(MssqlCell::Date(Some(mssql_date_value(
            mapping, row_index, cell,
        )?))),
        MssqlType::DateTime2 { .. } => Ok(MssqlCell::DateTime2(Some(mssql_datetime2_value(
            runtime_mapping,
            row_index,
            cell,
        )?))),
        MssqlType::DateTimeOffset { .. } => Ok(MssqlCell::DateTimeOffset(Some(
            mssql_datetimeoffset_value(runtime_mapping, row_index, cell)?,
        ))),
        MssqlType::Real => Ok(MssqlCell::Real(Some(mssql_real_value(
            mapping, row_index, cell,
        )?))),
        MssqlType::Float { .. } => Ok(MssqlCell::Float(Some(mssql_float_value(
            mapping, row_index, cell,
        )?))),
        MssqlType::NVarChar(length) => nvar_char_cell(mapping, row_index, *length, cell),
        MssqlType::VarBinary(length) => var_binary_cell(mapping, row_index, *length, cell),
    }
}

fn null_mssql_cell<'a>(mapping: &SchemaMapping, row_index: usize) -> Result<MssqlCell<'a>> {
    match mapping.mssql().ty() {
        MssqlType::Bit => Ok(MssqlCell::Bit(None)),
        MssqlType::TinyInt => Ok(MssqlCell::TinyInt(None)),
        MssqlType::SmallInt => Ok(MssqlCell::SmallInt(None)),
        MssqlType::Int => Ok(MssqlCell::Int(None)),
        MssqlType::BigInt => Ok(MssqlCell::BigInt(None)),
        MssqlType::Decimal { .. } if supports_null_decimal_cell(mapping) => {
            Ok(MssqlCell::Decimal(None))
        }
        MssqlType::Date => Ok(MssqlCell::Date(None)),
        MssqlType::DateTime2 { .. } => null_datetime2_cell(mapping, row_index),
        MssqlType::DateTimeOffset { .. } => null_datetimeoffset_cell(mapping, row_index),
        MssqlType::Real => Ok(MssqlCell::Real(None)),
        MssqlType::Float { .. } => Ok(MssqlCell::Float(None)),
        MssqlType::NVarChar(_) => Ok(MssqlCell::NVarChar(None)),
        MssqlType::VarBinary(_) => Ok(MssqlCell::VarBinary(None)),
        ty => Err(unsupported_value_conversion(
            mapping,
            row_index,
            format!(
                "planned SQL Server type {} is not supported yet",
                ty.to_sql()
            ),
        )),
    }
}

fn unsupported_value_conversion(
    mapping: &SchemaMapping,
    row_index: usize,
    message: impl Into<String>,
) -> crate::Error {
    value_conversion_error(row_mapping_diagnostic(
        mapping,
        row_index,
        DiagnosticCode::ValueConversionUnsupported,
        message,
    ))
}

fn non_finite_float_error(
    mapping: &SchemaMapping,
    row_index: usize,
    value: impl std::fmt::Display,
) -> crate::Error {
    value_conversion_error(row_mapping_diagnostic(
        mapping,
        row_index,
        DiagnosticCode::NonFiniteFloat,
        format!("non-finite floating point value {value} is not supported"),
    ))
}

fn value_too_long_error(
    mapping: &SchemaMapping,
    row_index: usize,
    message: impl Into<String>,
) -> crate::Error {
    value_conversion_error(row_mapping_diagnostic(
        mapping,
        row_index,
        DiagnosticCode::ValueTooLong,
        message,
    ))
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

    use arrow_schema::{DataType, Field, Schema, TimeUnit};

    use super::{ArrowToMssqlRuntimeMapping, MssqlCell, mssql_cell_from_arrow_cell};
    use crate::{
        DiagnosticCode, MssqlProfile, MssqlType, NanosecondPolicy, PlanOptions, SchemaMapping,
        UInt64Policy, arrow::cell::ArrowCell, plan_arrow_schema_to_mssql_mappings,
    };

    #[test]
    fn runtime_mapping_keeps_write_policy_out_of_schema_mapping() {
        let options = PlanOptions {
            nanosecond_policy: NanosecondPolicy::TruncateTo100ns,
            ..PlanOptions::default()
        };
        let mappings = mappings_for_schema_with_options(
            Schema::new(vec![Field::new(
                "created_at",
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                true,
            )]),
            options,
        );

        let runtime_mapping = ArrowToMssqlRuntimeMapping::new(&mappings[0], &options);

        assert_eq!(runtime_mapping.mapping(), &mappings[0]);
        assert_eq!(
            runtime_mapping.nanosecond_policy(),
            NanosecondPolicy::TruncateTo100ns
        );
        assert_eq!(
            mappings[0].mssql().ty(),
            &MssqlType::DateTime2 { precision: 7 }
        );
    }

    #[test]
    fn converts_uint64_checked_bigint_boundary_values() {
        let mappings = mappings_for_schema_with_options(
            Schema::new(vec![Field::new(
                "unsigned_as_bigint",
                DataType::UInt64,
                true,
            )]),
            PlanOptions {
                uint64_policy: UInt64Policy::CheckedBigInt,
                ..PlanOptions::default()
            },
        );

        assert_eq!(
            convert_cell(&mappings[0], ArrowCell::UInt64(0), 0).unwrap(),
            MssqlCell::BigInt(Some(0))
        );
        assert_eq!(
            convert_cell(&mappings[0], ArrowCell::UInt64(i64::MAX as u64), 1).unwrap(),
            MssqlCell::BigInt(Some(i64::MAX))
        );
        assert_eq!(
            convert_cell(&mappings[0], ArrowCell::Null, 2).unwrap(),
            MssqlCell::BigInt(None)
        );
    }

    #[test]
    fn rejects_uint64_checked_bigint_overflow_without_wrapping() {
        let mappings = mappings_for_schema_with_options(
            Schema::new(vec![Field::new(
                "unsigned_as_bigint",
                DataType::UInt64,
                false,
            )]),
            PlanOptions {
                uint64_policy: UInt64Policy::CheckedBigInt,
                ..PlanOptions::default()
            },
        );

        let just_over =
            convert_cell(&mappings[0], ArrowCell::UInt64((i64::MAX as u64) + 1), 0).unwrap_err();
        assert_single_diagnostic(
            just_over,
            DiagnosticCode::IntegerOutOfRange,
            Some(0),
            Some((0, "unsigned_as_bigint")),
        );

        let max = convert_cell(&mappings[0], ArrowCell::UInt64(u64::MAX), 1).unwrap_err();
        assert_single_diagnostic(
            max,
            DiagnosticCode::IntegerOutOfRange,
            Some(1),
            Some((0, "unsigned_as_bigint")),
        );
    }

    fn convert_cell<'a>(
        mapping: &SchemaMapping,
        cell: ArrowCell<'a>,
        row_index: usize,
    ) -> crate::Result<MssqlCell<'a>> {
        let options = PlanOptions::default();
        convert_cell_with_options(mapping, cell, row_index, &options)
    }

    fn convert_cell_with_options<'a>(
        mapping: &SchemaMapping,
        cell: ArrowCell<'a>,
        row_index: usize,
        options: &PlanOptions,
    ) -> crate::Result<MssqlCell<'a>> {
        let runtime_mapping = ArrowToMssqlRuntimeMapping::new(mapping, options);
        mssql_cell_from_arrow_cell(runtime_mapping, cell, row_index)
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

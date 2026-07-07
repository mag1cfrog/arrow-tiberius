//! Arrow-to-MSSQL runtime cell conversion.

mod decimal;
mod primitive;
pub(crate) mod temporal;
mod variable_width;

use crate::{
    Diagnostic, DiagnosticCode, DiagnosticSet, FieldRef, MssqlType, NanosecondPolicy, Result,
    SchemaMapping, arrow::cell::ArrowCell, mssql::profile::DateTimeRounding,
    write::context::RuntimeConversionContext,
};
#[cfg(test)]
use crate::{MssqlProfile, PlanOptions};

use super::MssqlCell;
use decimal::{mssql_decimal_value, supports_null_decimal_cell};
use primitive::primitive_mssql_cell;
use temporal::{
    mssql_date_value, mssql_datetime_value, mssql_datetime2_value, mssql_datetimeoffset_value,
    mssql_time_value, null_datetime_cell, null_datetime2_cell, null_datetimeoffset_cell,
    null_time_cell,
};
use variable_width::{binary_cell, nvar_char_cell, var_binary_cell};

/// Direction-specific runtime context for Arrow-to-MSSQL value conversion.
#[derive(Debug, Clone, Copy)]
pub(crate) struct ArrowToMssqlRuntimeMapping<'a> {
    mapping: &'a SchemaMapping,
    runtime_context: RuntimeConversionContext,
}

impl<'a> ArrowToMssqlRuntimeMapping<'a> {
    /// Creates runtime conversion context from structural mapping and write context.
    pub(crate) const fn new(
        mapping: &'a SchemaMapping,
        runtime_context: RuntimeConversionContext,
    ) -> Self {
        Self {
            mapping,
            runtime_context,
        }
    }

    /// Creates runtime mapping with test-only write conversion policies.
    #[cfg(test)]
    pub(crate) fn new_with_options(mapping: &'a SchemaMapping, options: &PlanOptions) -> Self {
        Self::new(
            mapping,
            RuntimeConversionContext::new(MssqlProfile::sql_server_2016_compat_100(), *options),
        )
    }

    /// Returns the structural Arrow/MSSQL mapping.
    pub(crate) const fn mapping(self) -> &'a SchemaMapping {
        self.mapping
    }

    /// Returns the nanosecond timestamp policy selected for write conversion.
    pub(crate) const fn nanosecond_policy(self) -> NanosecondPolicy {
        self.runtime_context.nanosecond_policy()
    }

    /// Returns the SQL Server datetime rounding behavior selected for write conversion.
    #[allow(dead_code)]
    pub(crate) const fn datetime_rounding(self) -> DateTimeRounding {
        self.runtime_context.datetime_rounding()
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
        MssqlType::Bit
        | MssqlType::TinyInt
        | MssqlType::SmallInt
        | MssqlType::Int
        | MssqlType::BigInt
        | MssqlType::Real
        | MssqlType::Float { .. } => primitive_mssql_cell(mapping, row_index, cell),
        MssqlType::Decimal { .. } => Ok(MssqlCell::Decimal(Some(mssql_decimal_value(
            mapping, row_index, cell,
        )?))),
        MssqlType::Date => Ok(MssqlCell::Date(Some(mssql_date_value(
            mapping, row_index, cell,
        )?))),
        MssqlType::Time(_) => Ok(MssqlCell::Time(Some(mssql_time_value(
            runtime_mapping,
            row_index,
            cell,
        )?))),
        MssqlType::DateTime => Ok(MssqlCell::DateTime(Some(mssql_datetime_value(
            runtime_mapping,
            row_index,
            cell,
        )?))),
        MssqlType::DateTime2 { .. } => Ok(MssqlCell::DateTime2(Some(mssql_datetime2_value(
            runtime_mapping,
            row_index,
            cell,
        )?))),
        MssqlType::DateTimeOffset { .. } => Ok(MssqlCell::DateTimeOffset(Some(
            mssql_datetimeoffset_value(runtime_mapping, row_index, cell)?,
        ))),
        MssqlType::NVarChar(length) => nvar_char_cell(mapping, row_index, *length, cell),
        MssqlType::VarBinary(length) => var_binary_cell(mapping, row_index, *length, cell),
        MssqlType::Binary(length) => binary_cell(mapping, row_index, *length, cell),
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
        MssqlType::Time(_) => null_time_cell(mapping, row_index),
        MssqlType::DateTime => null_datetime_cell(mapping, row_index),
        MssqlType::DateTime2 { .. } => null_datetime2_cell(mapping, row_index),
        MssqlType::DateTimeOffset { .. } => null_datetimeoffset_cell(mapping, row_index),
        MssqlType::Real => Ok(MssqlCell::Real(None)),
        MssqlType::Float { .. } => Ok(MssqlCell::Float(None)),
        MssqlType::NVarChar(_) => Ok(MssqlCell::NVarChar(None)),
        MssqlType::VarBinary(_) => Ok(MssqlCell::VarBinary(None)),
        MssqlType::Binary(_) => Ok(MssqlCell::VarBinary(None)),
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

    use super::ArrowToMssqlRuntimeMapping;
    use crate::{
        MssqlProfile, MssqlType, NanosecondPolicy, PlanOptions, SchemaMapping,
        plan_arrow_schema_to_mssql_mappings,
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

        let runtime_mapping = ArrowToMssqlRuntimeMapping::new_with_options(&mappings[0], &options);

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
}

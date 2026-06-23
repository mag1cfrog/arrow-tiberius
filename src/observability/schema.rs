use std::{
    fmt::Write as _,
    time::{Duration, Instant},
};

use crate::diagnostic::DiagnosticSeverity;
use crate::{DiagnosticSet, MssqlProfile, MssqlType, PlanOptions, SchemaMapping};

use super::{
    SCHEMA_PLANNING_COMPLETED_EVENT, SCHEMA_PLANNING_FAILED_EVENT, SCHEMA_PLANNING_PHASE,
    SCHEMA_PLANNING_SPAN, SCHEMA_PLANNING_STARTED_EVENT, TRACE_TARGET,
};

pub(crate) struct SchemaPlanningTrace {
    span: tracing::Span,
    field_count: usize,
    started: Instant,
}

impl SchemaPlanningTrace {
    pub(crate) fn start(field_count: usize, profile: MssqlProfile, options: PlanOptions) -> Self {
        let span = tracing::info_span!(
            target: TRACE_TARGET,
            SCHEMA_PLANNING_SPAN,
            phase = SCHEMA_PLANNING_PHASE,
            arrow_field_count = usize_to_u64(field_count),
            mssql_version = ?profile.version(),
            compatibility_level = u64::from(profile.compatibility_level().as_u16()),
            string_policy = ?options.string_policy,
            binary_policy = ?options.binary_policy,
            timezone_policy = ?options.timezone_policy,
            nanosecond_policy = ?options.nanosecond_policy,
            uint64_policy = ?options.uint64_policy,
            decimal_policy = ?options.decimal_policy,
            decimal256_policy = ?options.decimal256_policy,
            float_policy = ?options.float_policy,
            date64_policy = ?options.date64_policy,
        );

        span.in_scope(|| {
            tracing::info!(
                target: TRACE_TARGET,
                phase = SCHEMA_PLANNING_PHASE,
                telemetry_event = SCHEMA_PLANNING_STARTED_EVENT,
                arrow_field_count = usize_to_u64(field_count)
            );
        });

        Self {
            span,
            field_count,
            started: Instant::now(),
        }
    }

    pub(crate) fn in_scope<R>(&self, operation: impl FnOnce(&Self) -> R) -> R {
        self.span.in_scope(|| operation(self))
    }

    pub(crate) fn completed(&self, mappings: &[SchemaMapping], diagnostics: &DiagnosticSet) {
        let summary = DiagnosticTraceSummary::from_diagnostics(diagnostics);
        let shape = PlanningShapeTraceSummary::from_mappings(mappings);
        self.span.in_scope(|| {
            tracing::info!(
                target: TRACE_TARGET,
                phase = SCHEMA_PLANNING_PHASE,
                telemetry_event = SCHEMA_PLANNING_COMPLETED_EVENT,
                arrow_field_count = usize_to_u64(self.field_count),
                planned_mapping_count = usize_to_u64(mappings.len()),
                arrow_data_type_families = %shape.arrow_data_type_families,
                mssql_type_families = %shape.mssql_type_families,
                diagnostic_count = usize_to_u64(summary.total_count),
                error_diagnostic_count = usize_to_u64(summary.error_count),
                warning_diagnostic_count = usize_to_u64(summary.warning_count),
                diagnostic_codes = %summary.codes,
                diagnostic_field_names = %summary.field_names,
                elapsed_us = duration_micros_u64(self.started.elapsed())
            );
        });
    }

    pub(crate) fn failed(&self, diagnostics: &DiagnosticSet) {
        let summary = DiagnosticTraceSummary::from_diagnostics(diagnostics);
        self.span.in_scope(|| {
            tracing::error!(
                target: TRACE_TARGET,
                phase = SCHEMA_PLANNING_PHASE,
                telemetry_event = SCHEMA_PLANNING_FAILED_EVENT,
                arrow_field_count = usize_to_u64(self.field_count),
                diagnostic_count = usize_to_u64(summary.total_count),
                error_diagnostic_count = usize_to_u64(summary.error_count),
                warning_diagnostic_count = usize_to_u64(summary.warning_count),
                diagnostic_codes = %summary.codes,
                diagnostic_field_names = %summary.field_names,
                error_summary = "schema planning failed with diagnostics",
                elapsed_us = duration_micros_u64(self.started.elapsed())
            );
        });
    }
}

#[derive(Debug, Default, PartialEq, Eq)]
struct PlanningShapeTraceSummary {
    arrow_data_type_families: String,
    mssql_type_families: String,
}

impl PlanningShapeTraceSummary {
    fn from_mappings(mappings: &[SchemaMapping]) -> Self {
        let mut summary = Self::default();

        for mapping in mappings {
            append_unique_text(
                &mut summary.arrow_data_type_families,
                arrow_data_type_family(mapping.arrow().data_type()),
            );
            append_unique_text(
                &mut summary.mssql_type_families,
                mssql_type_family(mapping.mssql().ty()),
            );
        }

        summary
    }
}

#[derive(Debug, Default, PartialEq, Eq)]
struct DiagnosticTraceSummary {
    total_count: usize,
    error_count: usize,
    warning_count: usize,
    codes: String,
    field_names: String,
}

impl DiagnosticTraceSummary {
    fn from_diagnostics(diagnostics: &DiagnosticSet) -> Self {
        let mut summary = Self::default();

        for diagnostic in diagnostics.all() {
            summary.total_count += 1;
            match diagnostic.severity() {
                DiagnosticSeverity::Warning => summary.warning_count += 1,
                DiagnosticSeverity::Error => summary.error_count += 1,
            }

            append_debug_name(&mut summary.codes, diagnostic.code());
            if let Some(field) = diagnostic.field() {
                append_text(&mut summary.field_names, field.name());
            }
        }

        summary
    }
}

fn append_debug_name<T: std::fmt::Debug>(target: &mut String, value: T) {
    if !target.is_empty() {
        target.push(',');
    }
    let _ = write!(target, "{value:?}");
}

fn append_text(target: &mut String, value: &str) {
    if !target.is_empty() {
        target.push(',');
    }
    target.push_str(value);
}

fn append_unique_text(target: &mut String, value: &str) {
    if target.split(',').any(|existing| existing == value) {
        return;
    }

    append_text(target, value);
}

fn arrow_data_type_family(data_type: &arrow_schema::DataType) -> &'static str {
    match data_type {
        arrow_schema::DataType::Null => "null",
        arrow_schema::DataType::Boolean => "boolean",
        arrow_schema::DataType::Int8 => "int8",
        arrow_schema::DataType::Int16 => "int16",
        arrow_schema::DataType::Int32 => "int32",
        arrow_schema::DataType::Int64 => "int64",
        arrow_schema::DataType::UInt8 => "uint8",
        arrow_schema::DataType::UInt16 => "uint16",
        arrow_schema::DataType::UInt32 => "uint32",
        arrow_schema::DataType::UInt64 => "uint64",
        arrow_schema::DataType::Float16 => "float16",
        arrow_schema::DataType::Float32 => "float32",
        arrow_schema::DataType::Float64 => "float64",
        arrow_schema::DataType::Utf8 => "utf8",
        arrow_schema::DataType::LargeUtf8 => "large_utf8",
        arrow_schema::DataType::Binary => "binary",
        arrow_schema::DataType::LargeBinary => "large_binary",
        arrow_schema::DataType::FixedSizeBinary(_) => "fixed_size_binary",
        arrow_schema::DataType::Decimal32(_, _) => "decimal32",
        arrow_schema::DataType::Decimal64(_, _) => "decimal64",
        arrow_schema::DataType::Decimal128(_, _) => "decimal128",
        arrow_schema::DataType::Decimal256(_, _) => "decimal256",
        arrow_schema::DataType::Date32 => "date32",
        arrow_schema::DataType::Date64 => "date64",
        arrow_schema::DataType::Time32(_) => "time32",
        arrow_schema::DataType::Time64(_) => "time64",
        arrow_schema::DataType::Timestamp(_, _) => "timestamp",
        arrow_schema::DataType::Duration(_) => "duration",
        arrow_schema::DataType::Interval(_) => "interval",
        arrow_schema::DataType::List(_)
        | arrow_schema::DataType::ListView(_)
        | arrow_schema::DataType::FixedSizeList(_, _)
        | arrow_schema::DataType::LargeList(_)
        | arrow_schema::DataType::LargeListView(_)
        | arrow_schema::DataType::Struct(_)
        | arrow_schema::DataType::Map(_, _)
        | arrow_schema::DataType::Union(_, _) => "nested",
        arrow_schema::DataType::Dictionary(_, _) | arrow_schema::DataType::RunEndEncoded(_, _) => {
            "encoded"
        }
        arrow_schema::DataType::BinaryView => "binary_view",
        arrow_schema::DataType::Utf8View => "utf8_view",
    }
}

fn mssql_type_family(ty: &MssqlType) -> &'static str {
    match ty {
        MssqlType::Bit => "bit",
        MssqlType::TinyInt => "tinyint",
        MssqlType::SmallInt => "smallint",
        MssqlType::Int => "int",
        MssqlType::BigInt => "bigint",
        MssqlType::Real => "real",
        MssqlType::Float { .. } => "float",
        MssqlType::NVarChar(_) => "nvarchar",
        MssqlType::VarBinary(_) => "varbinary",
        MssqlType::Binary(_) => "binary",
        MssqlType::Decimal { .. } => "decimal",
        MssqlType::Date => "date",
        MssqlType::Time(_) => "time",
        MssqlType::DateTime2 { .. } => "datetime2",
        MssqlType::DateTimeOffset { .. } => "datetimeoffset",
    }
}

fn duration_micros_u64(duration: Duration) -> u64 {
    u64::try_from(duration.as_micros()).unwrap_or(u64::MAX)
}

fn usize_to_u64(value: usize) -> u64 {
    u64::try_from(value).unwrap_or(u64::MAX)
}

#[cfg(test)]
mod tests {
    use crate::{Diagnostic, DiagnosticCode, DiagnosticSet, FieldRef};

    use super::DiagnosticTraceSummary;

    #[test]
    fn diagnostic_trace_summary_counts_warning_codes() {
        let diagnostics = DiagnosticSet::from(vec![
            Diagnostic::warning(DiagnosticCode::PolicyApplied, "policy applied")
                .with_field(FieldRef::new(0, "amount")),
            Diagnostic::error(
                DiagnosticCode::ProfileDependentConversion,
                "policy required",
            )
            .with_field(FieldRef::new(1, "unsigned_huge")),
        ]);

        let summary = DiagnosticTraceSummary::from_diagnostics(&diagnostics);

        assert_eq!(summary.total_count, 2);
        assert_eq!(summary.warning_count, 1);
        assert_eq!(summary.error_count, 1);
        assert_eq!(summary.codes, "PolicyApplied,ProfileDependentConversion");
        assert_eq!(summary.field_names, "amount,unsigned_huge");
    }
}

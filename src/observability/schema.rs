use std::time::Instant;

use crate::diagnostic::DiagnosticSeverity;
use crate::{
    DiagnosticSet, Error, MssqlProfile, MssqlType, PlanOptions, PlanOutcome, Result, SchemaMapping,
};

use super::{
    SCHEMA_PLANNING_COMPLETED_EVENT, SCHEMA_PLANNING_FAILED_EVENT, SCHEMA_PLANNING_PHASE,
    SCHEMA_PLANNING_SPAN, SCHEMA_PLANNING_STARTED_EVENT, TRACE_TARGET, append_debug_name,
    append_text, append_unique_text, duration_micros_u64, usize_to_u64_saturating,
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
            arrow_field_count = usize_to_u64_saturating(field_count),
            mssql_version = ?profile.version(),
            compatibility_level = u64::from(profile.compatibility_level().as_u16()),
            string_policy = ?options.string_policy,
            binary_policy = ?options.binary_policy,
            timezone_policy = ?options.timezone_policy,
            timestamp_policy = ?options.timestamp_policy,
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
                arrow_field_count = usize_to_u64_saturating(field_count)
            );
        });

        Self {
            span,
            field_count,
            started: Instant::now(),
        }
    }

    pub(crate) fn trace_result(
        &self,
        result: Result<PlanOutcome<Vec<SchemaMapping>>>,
    ) -> Result<PlanOutcome<Vec<SchemaMapping>>> {
        match &result {
            Ok(outcome) => self.completed(outcome.value(), outcome.diagnostics()),
            Err(Error::Planning { diagnostics }) => self.failed(diagnostics),
            Err(_) => {}
        }
        result
    }

    pub(crate) fn completed(&self, mappings: &[SchemaMapping], diagnostics: &DiagnosticSet) {
        let summary = DiagnosticTraceSummary::from_diagnostics(diagnostics);
        let shape = PlanningShapeTraceSummary::from_mappings(mappings);
        self.span.in_scope(|| {
            tracing::info!(
                target: TRACE_TARGET,
                phase = SCHEMA_PLANNING_PHASE,
                telemetry_event = SCHEMA_PLANNING_COMPLETED_EVENT,
                arrow_field_count = usize_to_u64_saturating(self.field_count),
                planned_mapping_count = usize_to_u64_saturating(mappings.len()),
                arrow_data_type_families = %shape.arrow_data_type_families,
                mssql_type_families = %shape.mssql_type_families,
                diagnostic_count = usize_to_u64_saturating(summary.total_count),
                error_diagnostic_count = usize_to_u64_saturating(summary.error_count),
                warning_diagnostic_count = usize_to_u64_saturating(summary.warning_count),
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
                arrow_field_count = usize_to_u64_saturating(self.field_count),
                diagnostic_count = usize_to_u64_saturating(summary.total_count),
                error_diagnostic_count = usize_to_u64_saturating(summary.error_count),
                warning_diagnostic_count = usize_to_u64_saturating(summary.warning_count),
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
        MssqlType::DateTime => "datetime",
        MssqlType::DateTime2 { .. } => "datetime2",
        MssqlType::DateTimeOffset { .. } => "datetimeoffset",
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex, MutexGuard};

    use arrow_schema::{DataType, Field, Schema, TimeUnit};
    use tracing::Level;

    use crate::{
        Diagnostic, DiagnosticCode, DiagnosticSet, Error, FieldRef, MssqlProfile, MssqlType,
        PlanOptions, TimezonePolicy, plan_arrow_schema_to_mssql_mappings,
    };

    use super::{
        DiagnosticTraceSummary, SCHEMA_PLANNING_COMPLETED_EVENT, SCHEMA_PLANNING_FAILED_EVENT,
        SCHEMA_PLANNING_PHASE, SCHEMA_PLANNING_SPAN, SCHEMA_PLANNING_STARTED_EVENT, TRACE_TARGET,
        mssql_type_family,
    };
    use crate::observability::test_support::{CapturedTraceKind, capture_traces};

    static SCHEMA_TRACE_TEST_LOCK: Mutex<()> = Mutex::new(());

    fn schema_trace_test_guard() -> MutexGuard<'static, ()> {
        match SCHEMA_TRACE_TEST_LOCK.lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        }
    }

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

    #[test]
    fn mssql_type_family_reports_datetime() {
        assert_eq!(mssql_type_family(&MssqlType::DateTime), "datetime");
    }

    #[test]
    fn successful_schema_planning_emits_structured_trace() -> Result<(), String> {
        let _trace_guard = schema_trace_test_guard();
        let schema = Schema::new(vec![
            Field::new("is_active", DataType::Boolean, false),
            Field::new("quantity", DataType::Int32, true),
        ]);

        let (outcome, traces) = capture_traces(|| {
            plan_arrow_schema_to_mssql_mappings(
                Arc::new(schema),
                MssqlProfile::sql_server_2016_compat_100(),
                PlanOptions::default(),
            )
        });
        let outcome = outcome.map_err(|err| err.to_string())?;
        assert_eq!(outcome.value().len(), 2);

        let records = traces.records()?;
        assert!(
            records.iter().any(|record| {
                record.kind() == CapturedTraceKind::Span
                    && record.name() == SCHEMA_PLANNING_SPAN
                    && record.target() == TRACE_TARGET
                    && record.level() == Level::INFO
                    && record
                        .fields()
                        .get("phase")
                        .is_some_and(|value| value == SCHEMA_PLANNING_PHASE)
                    && record
                        .fields()
                        .get("arrow_field_count")
                        .is_some_and(|value| value == "2")
                    && record
                        .fields()
                        .get("mssql_version")
                        .is_some_and(|value| value == "SqlServer2016")
                    && record
                        .fields()
                        .get("compatibility_level")
                        .is_some_and(|value| value == "100")
                    && record
                        .fields()
                        .get("uint64_policy")
                        .is_some_and(|value| value == "Reject")
                    && record
                        .fields()
                        .get("timestamp_policy")
                        .is_some_and(|value| value == "DateTime2 { precision: 7 }")
            }),
            "captured records: {records:#?}"
        );

        assert!(
            records.iter().any(|record| {
                record.kind() == CapturedTraceKind::Event
                    && record.target() == TRACE_TARGET
                    && record.level() == Level::INFO
                    && record.span_name() == Some(SCHEMA_PLANNING_SPAN)
                    && record
                        .fields()
                        .get("telemetry_event")
                        .is_some_and(|value| value == SCHEMA_PLANNING_STARTED_EVENT)
                    && record
                        .fields()
                        .get("arrow_field_count")
                        .is_some_and(|value| value == "2")
            }),
            "captured records: {records:#?}"
        );

        assert!(
            records.iter().any(|record| {
                record.kind() == CapturedTraceKind::Event
                    && record.target() == TRACE_TARGET
                    && record.level() == Level::INFO
                    && record.span_name() == Some(SCHEMA_PLANNING_SPAN)
                    && record
                        .fields()
                        .get("telemetry_event")
                        .is_some_and(|value| value == SCHEMA_PLANNING_COMPLETED_EVENT)
                    && record
                        .fields()
                        .get("planned_mapping_count")
                        .is_some_and(|value| value == "2")
                    && record
                        .fields()
                        .get("arrow_data_type_families")
                        .is_some_and(|value| value == "boolean,int32")
                    && record
                        .fields()
                        .get("mssql_type_families")
                        .is_some_and(|value| value == "bit,int")
                    && record
                        .fields()
                        .get("diagnostic_count")
                        .is_some_and(|value| value == "0")
                    && record
                        .fields()
                        .get("error_diagnostic_count")
                        .is_some_and(|value| value == "0")
                    && record
                        .fields()
                        .get("warning_diagnostic_count")
                        .is_some_and(|value| value == "0")
                    && record.fields().contains_key("elapsed_us")
            }),
            "captured records: {records:#?}"
        );

        Ok(())
    }

    #[test]
    fn failed_schema_planning_emits_sanitized_diagnostic_trace() -> Result<(), String> {
        let _trace_guard = schema_trace_test_guard();
        let schema = Schema::new(vec![
            Field::new("ok", DataType::Int32, false),
            Field::new("unsigned_huge", DataType::UInt64, true),
            Field::new("items", DataType::new_list(DataType::Int64, true), true),
        ]);

        let (outcome, traces) = capture_traces(|| {
            plan_arrow_schema_to_mssql_mappings(
                Arc::new(schema),
                MssqlProfile::sql_server_2016_compat_100(),
                PlanOptions::default(),
            )
        });
        let err = outcome.expect_err("unsupported planning should fail");
        let Error::Planning { diagnostics } = err else {
            panic!("expected planning error");
        };
        assert_eq!(diagnostics.len(), 2);

        let records = traces.records()?;
        assert!(
            records.iter().any(|record| {
                record.kind() == CapturedTraceKind::Event
                    && record.target() == TRACE_TARGET
                    && record.level() == Level::ERROR
                    && record.span_name() == Some(SCHEMA_PLANNING_SPAN)
                    && record
                        .fields()
                        .get("telemetry_event")
                        .is_some_and(|value| value == SCHEMA_PLANNING_FAILED_EVENT)
                    && record
                        .fields()
                        .get("arrow_field_count")
                        .is_some_and(|value| value == "3")
                    && record
                        .fields()
                        .get("diagnostic_count")
                        .is_some_and(|value| value == "2")
                    && record
                        .fields()
                        .get("error_diagnostic_count")
                        .is_some_and(|value| value == "2")
                    && record
                        .fields()
                        .get("warning_diagnostic_count")
                        .is_some_and(|value| value == "0")
                    && record
                        .fields()
                        .get("diagnostic_codes")
                        .is_some_and(|value| {
                            value.contains("ProfileDependentConversion")
                                && value.contains("UnsupportedArrowType")
                        })
                    && record
                        .fields()
                        .get("diagnostic_field_names")
                        .is_some_and(|value| {
                            value.contains("unsigned_huge") && value.contains("items")
                        })
                    && record
                        .fields()
                        .get("error_summary")
                        .is_some_and(|value| value == "schema planning failed with diagnostics")
                    && record.fields().contains_key("elapsed_us")
            }),
            "captured records: {records:#?}"
        );

        Ok(())
    }

    #[test]
    fn schema_planning_trace_does_not_emit_field_metadata_values() -> Result<(), String> {
        let _trace_guard = schema_trace_test_guard();
        let schema = Schema::new(vec![
            Field::new("id", DataType::Int32, false).with_metadata(
                [(
                    "connection_hint".to_owned(),
                    "server=tcp:sql.example.com;password=secret".to_owned(),
                )]
                .into_iter()
                .collect(),
            ),
            Field::new("label", DataType::Utf8, true).with_metadata(
                [("token".to_owned(), "access_token=abc123".to_owned())]
                    .into_iter()
                    .collect(),
            ),
            Field::new(
                "created_at",
                DataType::Timestamp(TimeUnit::Second, Some("password=timezone-secret".into())),
                true,
            ),
        ]);

        let (outcome, traces) = capture_traces(|| {
            plan_arrow_schema_to_mssql_mappings(
                Arc::new(schema),
                MssqlProfile::sql_server_2016_compat_100(),
                PlanOptions {
                    timezone_policy: TimezonePolicy::DateTimeOffset,
                    ..PlanOptions::default()
                },
            )
        });
        outcome.map_err(|err| err.to_string())?;

        traces.assert_no_forbidden_text(&[
            "server=tcp:sql.example.com",
            "password=secret",
            "access_token=abc123",
            "password=timezone-secret",
        ])?;

        Ok(())
    }
}

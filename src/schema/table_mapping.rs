//! Bidirectional Arrow/MSSQL schema mapping.
//!
//! The initial mapping function starts from an Arrow schema because the first
//! operation is Arrow-to-SQL Server writing. The resulting `SchemaMapping`
//! values keep Arrow field metadata and MSSQL column metadata as peer concepts
//! so future SQL Server-to-Arrow read planning can reuse the shared
//! representation instead of inheriting a write-only column model.

use arrow_schema::{Field, Schema};

use crate::observability::schema::SchemaPlanningTrace;
use crate::schema::type_conversion::plan_arrow_data_type_as_mssql_type;
use crate::write::PlanOptions;
use crate::{
    ArrowFieldRef, Diagnostic, DiagnosticCode, DiagnosticSet, FieldRef, Identifier, MssqlColumn,
    MssqlProfile, PlanOutcome, Result, SchemaMapping, TableName, create_table_sql,
};

/// Plans Arrow/MSSQL column mappings from an Arrow schema.
pub fn plan_arrow_schema_to_mssql_mappings(
    schema: impl AsRef<Schema>,
    profile: MssqlProfile,
    options: PlanOptions,
) -> Result<PlanOutcome<Vec<SchemaMapping>>> {
    let schema = schema.as_ref();
    let field_count = schema.fields().len();
    let trace = SchemaPlanningTrace::start(field_count, profile, options);

    trace.in_scope(|trace| {
        let mut mappings = Vec::with_capacity(schema.fields().len());
        let mut diagnostics = DiagnosticSet::new();

        for (index, field) in schema.fields().iter().enumerate() {
            match plan_arrow_field_to_mssql_column_mapping(index, field, &options) {
                Ok(mapping) => mappings.push(mapping),
                Err(diagnostic) => diagnostics.push(diagnostic),
            }
        }

        if diagnostics.has_errors() {
            trace.failed(&diagnostics);
            return Err(crate::Error::Planning { diagnostics });
        }

        trace.completed(&mappings, &diagnostics);

        Ok(PlanOutcome::new(mappings, diagnostics))
    })
}

/// Returns the planned MSSQL columns in mapping order.
pub fn mssql_columns_from_mappings(mappings: &[SchemaMapping]) -> Vec<MssqlColumn> {
    mappings
        .iter()
        .map(|mapping| mapping.mssql().clone())
        .collect()
}

/// Renders deterministic `CREATE TABLE` SQL from mapping metadata.
pub fn create_table_sql_from_mappings(table: &TableName, mappings: &[SchemaMapping]) -> String {
    create_table_sql(
        table,
        &mssql_columns_from_mappings(mappings),
        crate::CreateTableOptions,
    )
}

fn plan_arrow_field_to_mssql_column_mapping(
    index: usize,
    field: &Field,
    options: &PlanOptions,
) -> std::result::Result<SchemaMapping, Diagnostic> {
    let name = Identifier::new(field.name()).map_err(|err| {
        Diagnostic::error(DiagnosticCode::IdentifierInvalid, err.to_string())
            .with_field(FieldRef::new(index, field.name()))
    })?;

    let ty = plan_arrow_data_type_as_mssql_type(index, field, options)?;

    let arrow = ArrowFieldRef::new(
        index,
        field.name().clone(),
        field.is_nullable(),
        field.data_type().clone(),
    );
    let mssql = MssqlColumn::new(name, ty, field.is_nullable());

    Ok(SchemaMapping::new(arrow, mssql))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_schema::{DataType, Field, Schema, TimeUnit, UnionFields, UnionMode};
    use tracing::Level;

    use crate::observability::{
        SCHEMA_PLANNING_COMPLETED_EVENT, SCHEMA_PLANNING_FAILED_EVENT, SCHEMA_PLANNING_PHASE,
        SCHEMA_PLANNING_SPAN, SCHEMA_PLANNING_STARTED_EVENT, TRACE_TARGET,
        test_support::{CapturedTraceKind, capture_traces},
    };
    use crate::{
        DiagnosticCode, Error, MssqlProfile, MssqlType, PlanOptions, TableName, TimezonePolicy,
        create_table_sql_from_mappings, mssql_columns_from_mappings,
        plan_arrow_schema_to_mssql_mappings,
    };

    #[test]
    fn plans_boolean_and_int32_mappings() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("is_active", DataType::Boolean, false),
            Field::new("quantity", DataType::Int32, true),
        ]));

        let outcome = plan_arrow_schema_to_mssql_mappings(
            Arc::clone(&schema),
            MssqlProfile::sql_server_2016_compat_100(),
            PlanOptions::default(),
        )
        .unwrap();
        let mappings = outcome.value();

        assert_eq!(mappings.len(), 2);

        let is_active = &mappings[0];
        assert_eq!(is_active.arrow().index(), 0);
        assert_eq!(is_active.arrow().name(), "is_active");
        assert_eq!(is_active.arrow().data_type(), &DataType::Boolean);
        assert!(!is_active.arrow().nullable());
        assert_eq!(is_active.mssql().name().quoted_sql(), "[is_active]");
        assert!(!is_active.mssql().nullable());
        assert_eq!(is_active.mssql().ty(), &MssqlType::Bit);

        let quantity = &mappings[1];
        assert_eq!(quantity.arrow().index(), 1);
        assert_eq!(quantity.arrow().name(), "quantity");
        assert_eq!(quantity.arrow().data_type(), &DataType::Int32);
        assert!(quantity.arrow().nullable());
        assert_eq!(quantity.mssql().name().quoted_sql(), "[quantity]");
        assert!(quantity.mssql().nullable());
        assert_eq!(quantity.mssql().ty(), &MssqlType::Int);
    }

    #[test]
    fn renders_create_table_sql_from_mssql_side() {
        let schema = Schema::new(vec![
            Field::new("is_active", DataType::Boolean, false),
            Field::new("quantity", DataType::Int32, true),
        ]);
        let outcome = plan_arrow_schema_to_mssql_mappings(
            Arc::new(schema),
            MssqlProfile::sql_server_2016_compat_100(),
            PlanOptions::default(),
        )
        .unwrap();
        let table = TableName::new("dbo", "target").unwrap();

        let sql = create_table_sql_from_mappings(&table, outcome.value());

        assert_eq!(
            sql,
            "CREATE TABLE [dbo].[target] (\n    [is_active] bit NOT NULL,\n    [quantity] int NULL\n);"
        );
    }

    #[test]
    fn exposes_mssql_columns_without_arrow_identity() {
        let schema = Schema::new(vec![Field::new("is_active", DataType::Boolean, false)]);
        let outcome = plan_arrow_schema_to_mssql_mappings(
            Arc::new(schema),
            MssqlProfile::sql_server_2016_compat_100(),
            PlanOptions::default(),
        )
        .unwrap();

        let columns = mssql_columns_from_mappings(outcome.value());

        assert_eq!(columns.len(), 1);
        assert_eq!(columns[0].name().as_str(), "is_active");
        assert_eq!(columns[0].ty(), &MssqlType::Bit);
        assert!(!columns[0].nullable());
    }

    #[test]
    fn unsupported_nested_and_encoded_types_collect_schema_order_diagnostics() {
        let union_fields = UnionFields::try_new(
            [1_i8, 2],
            [
                Field::new("left", DataType::Int32, true),
                Field::new("right", DataType::Utf8, true),
            ],
        )
        .unwrap();
        let schema = Schema::new(vec![
            Field::new("ok", DataType::Int32, false),
            Field::new("list_col", DataType::new_list(DataType::Int64, true), true),
            Field::new(
                "struct_col",
                DataType::Struct(
                    vec![Field::new("child", DataType::Boolean, true)]
                        .into_iter()
                        .collect(),
                ),
                true,
            ),
            Field::new(
                "union_col",
                DataType::Union(union_fields, UnionMode::Sparse),
                true,
            ),
            Field::new(
                "run_end_col",
                DataType::RunEndEncoded(
                    Arc::new(Field::new("run_ends", DataType::Int32, false)),
                    Arc::new(Field::new("values", DataType::Utf8, true)),
                ),
                true,
            ),
        ]);

        let err = plan_arrow_schema_to_mssql_mappings(
            Arc::new(schema),
            MssqlProfile::sql_server_2016_compat_100(),
            PlanOptions::default(),
        )
        .expect_err("unsupported fields should produce diagnostics");

        let Error::Planning { diagnostics } = err else {
            panic!("expected planning error");
        };

        assert_eq!(diagnostics.len(), 4);
        assert!(
            diagnostics
                .all()
                .iter()
                .all(|diagnostic| diagnostic.code() == DiagnosticCode::UnsupportedArrowType)
        );

        let field_refs = diagnostics
            .all()
            .iter()
            .map(|diagnostic| {
                let field = diagnostic.field().unwrap();
                (field.index(), field.name())
            })
            .collect::<Vec<_>>();

        assert_eq!(
            field_refs,
            vec![
                (1, "list_col"),
                (2, "struct_col"),
                (3, "union_col"),
                (4, "run_end_col"),
            ]
        );

        let messages = diagnostics
            .all()
            .iter()
            .map(crate::Diagnostic::message)
            .collect::<Vec<_>>();
        assert!(messages[0].contains("nested"));
        assert!(messages[1].contains("nested"));
        assert!(messages[2].contains("nested"));
        assert!(messages[3].contains("encoded"));
    }

    #[test]
    fn invalid_identifier_returns_structured_planning_diagnostic() {
        let schema = Schema::new(vec![Field::new("", DataType::Boolean, false)]);

        let err = plan_arrow_schema_to_mssql_mappings(
            Arc::new(schema),
            MssqlProfile::sql_server_2016_compat_100(),
            PlanOptions::default(),
        )
        .expect_err("empty field name should be rejected");

        let Error::Planning { diagnostics } = err else {
            panic!("expected planning error");
        };

        assert_eq!(diagnostics.len(), 1);

        let diagnostic = &diagnostics.all()[0];
        assert_eq!(diagnostic.code(), DiagnosticCode::IdentifierInvalid);
        assert_eq!(diagnostic.field().unwrap().index(), 0);
        assert_eq!(diagnostic.field().unwrap().name(), "");
    }

    #[test]
    fn successful_schema_planning_emits_structured_trace() -> Result<(), String> {
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

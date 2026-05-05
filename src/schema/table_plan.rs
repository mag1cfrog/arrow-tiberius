//! Bidirectional Arrow/MSSQL schema mapping.
//!
//! The plan is built from an Arrow schema in v0.1 because the first operation is
//! Arrow-to-SQL Server writing. The resulting model keeps Arrow field metadata
//! and MSSQL column metadata as peer concepts so future SQL Server-to-Arrow read
//! planning can reuse the shared representation instead of inheriting a
//! write-only column model.

use std::sync::Arc;

use arrow_schema::{DataType, Field, Schema};

use crate::write::PlanOptions;
use crate::{
    ArrowFieldPlan, Diagnostic, DiagnosticCode, DiagnosticSet, FieldRef, Identifier,
    MssqlColumnPlan, MssqlProfile, MssqlType, PlanOutcome, Result, SchemaMapping, TableName,
    create_table_sql,
};

/// Immutable Arrow/MSSQL table schema plan.
#[derive(Debug, Clone)]
pub struct MssqlTablePlan {
    arrow_schema: Arc<Schema>,
    profile: MssqlProfile,
    mappings: Vec<SchemaMapping>,
}

impl MssqlTablePlan {
    /// Creates an Arrow/MSSQL table plan from an Arrow schema.
    pub fn from_arrow_schema(
        schema: impl Into<Arc<Schema>>,
        profile: MssqlProfile,
        _options: PlanOptions,
    ) -> Result<PlanOutcome<Self>> {
        let schema = schema.into();
        let mut mappings = Vec::with_capacity(schema.fields().len());
        let mut diagnostics = DiagnosticSet::new();

        for (index, field) in schema.fields().iter().enumerate() {
            match plan_mapping(index, field) {
                Ok(mapping) => mappings.push(mapping),
                Err(diagnostic) => diagnostics.push(diagnostic),
            }
        }

        if diagnostics.has_errors() {
            return Err(crate::Error::Planning { diagnostics });
        }

        Ok(PlanOutcome::new(
            Self {
                arrow_schema: schema,
                profile,
                mappings,
            },
            diagnostics,
        ))
    }

    /// Returns the source Arrow schema used to build this plan.
    pub fn arrow_schema(&self) -> &Schema {
        &self.arrow_schema
    }

    /// Returns the SQL Server planning profile.
    pub const fn profile(&self) -> MssqlProfile {
        self.profile
    }

    /// Returns the planned Arrow/MSSQL mappings in schema order.
    pub fn mappings(&self) -> &[SchemaMapping] {
        &self.mappings
    }

    /// Returns the planned MSSQL columns in schema order.
    pub fn mssql_columns(&self) -> Vec<MssqlColumnPlan> {
        self.mappings
            .iter()
            .map(|mapping| mapping.mssql().clone())
            .collect()
    }

    /// Renders deterministic `CREATE TABLE` SQL from the MSSQL side.
    pub fn create_table_sql(&self, table: &TableName) -> String {
        create_table_sql(table, &self.mssql_columns(), crate::CreateTableOptions)
    }
}

fn plan_mapping(index: usize, field: &Field) -> std::result::Result<SchemaMapping, Diagnostic> {
    let name = Identifier::new(field.name()).map_err(|err| {
        Diagnostic::error(DiagnosticCode::IdentifierInvalid, err.to_string())
            .with_field(FieldRef::new(index, field.name()))
    })?;

    let ty = match field.data_type() {
        DataType::Boolean => MssqlType::Bit,
        DataType::Int32 => MssqlType::Int,
        other => {
            return Err(Diagnostic::error(
                DiagnosticCode::UnsupportedArrowType,
                format!("unsupported Arrow type {other:?}"),
            )
            .with_field(FieldRef::new(index, field.name())));
        }
    };

    let arrow = ArrowFieldPlan::new(
        index,
        field.name().clone(),
        field.is_nullable(),
        field.data_type().clone(),
    );
    let mssql = MssqlColumnPlan::new(name, ty, field.is_nullable());

    Ok(SchemaMapping::new(arrow, mssql))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_schema::{DataType, Field, Schema};

    use crate::{
        DiagnosticCode, Error, MssqlProfile, MssqlTablePlan, MssqlType, PlanOptions, TableName,
    };

    #[test]
    fn plans_boolean_and_int32_mappings() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("is_active", DataType::Boolean, false),
            Field::new("quantity", DataType::Int32, true),
        ]));

        let outcome = MssqlTablePlan::from_arrow_schema(
            Arc::clone(&schema),
            MssqlProfile::sql_server_2016_compat_100(),
            PlanOptions::default(),
        )
        .unwrap();
        let plan = outcome.value();

        assert_eq!(plan.arrow_schema(), schema.as_ref());
        assert_eq!(plan.profile(), MssqlProfile::sql_server_2016_compat_100());
        assert_eq!(plan.mappings().len(), 2);

        let is_active = &plan.mappings()[0];
        assert_eq!(is_active.arrow().index(), 0);
        assert_eq!(is_active.arrow().name(), "is_active");
        assert_eq!(is_active.arrow().data_type(), &DataType::Boolean);
        assert!(!is_active.arrow().nullable());
        assert_eq!(is_active.mssql().name().quoted_sql(), "[is_active]");
        assert!(!is_active.mssql().nullable());
        assert_eq!(is_active.mssql().ty(), &MssqlType::Bit);

        let quantity = &plan.mappings()[1];
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
        let outcome = MssqlTablePlan::from_arrow_schema(
            Arc::new(schema),
            MssqlProfile::sql_server_2016_compat_100(),
            PlanOptions::default(),
        )
        .unwrap();
        let table = TableName::new("dbo", "target").unwrap();

        let sql = outcome.value().create_table_sql(&table);

        assert_eq!(
            sql,
            "CREATE TABLE [dbo].[target] (\n    [is_active] bit NOT NULL,\n    [quantity] int NULL\n);"
        );
    }

    #[test]
    fn exposes_mssql_columns_without_arrow_identity() {
        let schema = Schema::new(vec![Field::new("is_active", DataType::Boolean, false)]);
        let outcome = MssqlTablePlan::from_arrow_schema(
            Arc::new(schema),
            MssqlProfile::sql_server_2016_compat_100(),
            PlanOptions::default(),
        )
        .unwrap();

        let columns = outcome.value().mssql_columns();

        assert_eq!(columns.len(), 1);
        assert_eq!(columns[0].name().as_str(), "is_active");
        assert_eq!(columns[0].ty(), &MssqlType::Bit);
        assert!(!columns[0].nullable());
    }

    #[test]
    fn unsupported_type_returns_structured_planning_diagnostic() {
        let schema = Schema::new(vec![Field::new("name", DataType::Utf8, true)]);

        let err = MssqlTablePlan::from_arrow_schema(
            Arc::new(schema),
            MssqlProfile::sql_server_2016_compat_100(),
            PlanOptions::default(),
        )
        .expect_err("Utf8 mapping is added in a later step");

        let Error::Planning { diagnostics } = err else {
            panic!("expected planning error");
        };

        assert!(diagnostics.has_errors());
        assert_eq!(diagnostics.len(), 1);

        let diagnostic = &diagnostics.all()[0];
        assert_eq!(diagnostic.code(), DiagnosticCode::UnsupportedArrowType);
        assert_eq!(diagnostic.field().unwrap().index(), 0);
        assert_eq!(diagnostic.field().unwrap().name(), "name");
    }

    #[test]
    fn invalid_identifier_returns_structured_planning_diagnostic() {
        let schema = Schema::new(vec![Field::new("", DataType::Boolean, false)]);

        let err = MssqlTablePlan::from_arrow_schema(
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
}

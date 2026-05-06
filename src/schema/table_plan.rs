//! Bidirectional Arrow/MSSQL schema mapping.
//!
//! The plan is built from an Arrow schema in v0.1 because the first operation is
//! Arrow-to-SQL Server writing. The resulting model keeps Arrow field metadata
//! and MSSQL column metadata as peer concepts so future SQL Server-to-Arrow read
//! planning can reuse the shared representation instead of inheriting a
//! write-only column model.

use std::sync::Arc;

use arrow_schema::{DataType, Field, Schema, TimeUnit};

use crate::write::{
    BinaryPolicy, Date64Policy, Decimal256Policy, DecimalPolicy, NanosecondPolicy, PlanOptions,
    StringPolicy, TimezonePolicy, UInt64Policy,
};
use crate::{
    ArrowFieldPlan, Diagnostic, DiagnosticCode, DiagnosticSet, FieldRef, Identifier,
    MssqlColumnPlan, MssqlProfile, MssqlType, MssqlTypeLength, PlanOutcome, Result, SchemaMapping,
    TableName, create_table_sql,
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
        options: PlanOptions,
    ) -> Result<PlanOutcome<Self>> {
        let schema = schema.into();
        let mut mappings = Vec::with_capacity(schema.fields().len());
        let mut diagnostics = DiagnosticSet::new();

        for (index, field) in schema.fields().iter().enumerate() {
            match plan_arrow_field_to_mssql_column_mapping(index, field, &options) {
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

fn plan_arrow_field_to_mssql_column_mapping(
    index: usize,
    field: &Field,
    options: &PlanOptions,
) -> std::result::Result<SchemaMapping, Diagnostic> {
    let name = Identifier::new(field.name()).map_err(|err| {
        Diagnostic::error(DiagnosticCode::IdentifierInvalid, err.to_string())
            .with_field(FieldRef::new(index, field.name()))
    })?;

    let ty = match field.data_type() {
        DataType::Boolean => MssqlType::Bit,
        DataType::Int8 | DataType::Int16 => MssqlType::SmallInt,
        DataType::Int32 => MssqlType::Int,
        DataType::Int64 => MssqlType::BigInt,
        DataType::UInt8 => MssqlType::TinyInt,
        DataType::UInt16 => MssqlType::Int,
        DataType::UInt32 => MssqlType::BigInt,
        DataType::UInt64 => plan_arrow_uint64_as_mssql_type(options.uint64_policy, index, field)?,
        DataType::Float32 => MssqlType::Real,
        DataType::Float64 => MssqlType::Float { precision: 53 },
        DataType::Utf8 | DataType::LargeUtf8 => {
            plan_arrow_string_as_mssql_type(options.string_policy, index, field)?
        }
        DataType::Binary | DataType::LargeBinary => {
            plan_arrow_binary_as_mssql_type(options.binary_policy, index, field)?
        }
        DataType::Decimal32(precision, scale)
        | DataType::Decimal64(precision, scale)
        | DataType::Decimal128(precision, scale) => plan_arrow_decimal_as_mssql_type(
            *precision,
            *scale,
            options.decimal_policy,
            index,
            field,
        )?,
        DataType::Decimal256(precision, scale) => plan_arrow_decimal256_as_mssql_type(
            *precision,
            *scale,
            options.decimal_policy,
            options.decimal256_policy,
            index,
            field,
        )?,
        DataType::Date32 => MssqlType::Date,
        DataType::Date64 => plan_arrow_date64_as_mssql_type(options.date64_policy, index, field)?,
        DataType::Timestamp(unit, timezone) => plan_arrow_timestamp_as_mssql_type(
            *unit,
            timezone.as_deref(),
            options.timezone_policy,
            options.nanosecond_policy,
            index,
            field,
        )?,
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

fn plan_arrow_uint64_as_mssql_type(
    policy: UInt64Policy,
    index: usize,
    field: &Field,
) -> std::result::Result<MssqlType, Diagnostic> {
    match policy {
        UInt64Policy::Reject => Err(policy_required_for_arrow_to_mssql(
            index,
            field,
            "UInt64 requires UInt64Policy::Decimal20_0 or UInt64Policy::CheckedBigInt",
        )),
        UInt64Policy::Decimal20_0 => Ok(MssqlType::Decimal {
            precision: 20,
            scale: 0,
        }),
        UInt64Policy::CheckedBigInt => Ok(MssqlType::BigInt),
    }
}

fn plan_arrow_string_as_mssql_type(
    policy: StringPolicy,
    index: usize,
    field: &Field,
) -> std::result::Result<MssqlType, Diagnostic> {
    match policy {
        StringPolicy::NVarCharMax => Ok(MssqlType::NVarChar(MssqlTypeLength::Max)),
        StringPolicy::NVarChar(length) => Ok(MssqlType::NVarChar(MssqlTypeLength::Bounded(length))),
        StringPolicy::ObservedNVarChar => Err(observed_data_required_for_arrow_to_mssql(
            index,
            field,
            "ObservedNVarChar requires observed values or statistics",
        )),
    }
}

fn plan_arrow_binary_as_mssql_type(
    policy: BinaryPolicy,
    index: usize,
    field: &Field,
) -> std::result::Result<MssqlType, Diagnostic> {
    match policy {
        BinaryPolicy::VarBinaryMax => Ok(MssqlType::VarBinary(MssqlTypeLength::Max)),
        BinaryPolicy::VarBinary(length) => {
            Ok(MssqlType::VarBinary(MssqlTypeLength::Bounded(length)))
        }
        BinaryPolicy::ObservedVarBinary => Err(observed_data_required_for_arrow_to_mssql(
            index,
            field,
            "ObservedVarBinary requires observed values or statistics",
        )),
    }
}

fn plan_arrow_decimal_as_mssql_type(
    precision: u8,
    scale: i8,
    policy: DecimalPolicy,
    index: usize,
    field: &Field,
) -> std::result::Result<MssqlType, Diagnostic> {
    let (precision, scale) = normalize_arrow_decimal_shape(precision, scale, policy, index, field)?;
    Ok(MssqlType::Decimal { precision, scale })
}

fn plan_arrow_decimal256_as_mssql_type(
    precision: u8,
    scale: i8,
    decimal_policy: DecimalPolicy,
    decimal256_policy: Decimal256Policy,
    index: usize,
    field: &Field,
) -> std::result::Result<MssqlType, Diagnostic> {
    match decimal256_policy {
        Decimal256Policy::CheckedDowncast => {
            plan_arrow_decimal_as_mssql_type(precision, scale, decimal_policy, index, field)
        }
        Decimal256Policy::Reject => Err(policy_required_for_arrow_to_mssql(
            index,
            field,
            "Decimal256 requires Decimal256Policy::CheckedDowncast",
        )),
    }
}

fn normalize_arrow_decimal_shape(
    precision: u8,
    scale: i8,
    policy: DecimalPolicy,
    index: usize,
    field: &Field,
) -> std::result::Result<(u8, i8), Diagnostic> {
    if precision == 0 {
        return Err(decimal_out_of_range_for_arrow_to_mssql(
            index,
            field,
            "decimal precision must be at least 1 for SQL Server",
        ));
    }

    let (precision, scale) = if scale < 0 {
        match policy {
            DecimalPolicy::RejectNegativeScale => {
                return Err(policy_required_for_arrow_to_mssql(
                    index,
                    field,
                    "negative decimal scale requires DecimalPolicy::NormalizeNegativeScale",
                ));
            }
            DecimalPolicy::NormalizeNegativeScale => {
                let extra_digits = scale.unsigned_abs();
                let Some(normalized_precision) = precision.checked_add(extra_digits) else {
                    return Err(decimal_out_of_range_for_arrow_to_mssql(
                        index,
                        field,
                        "normalized decimal precision overflows u8",
                    ));
                };
                (normalized_precision, 0)
            }
        }
    } else {
        (precision, scale)
    };

    if precision > SQL_SERVER_MAX_DECIMAL_PRECISION {
        return Err(decimal_out_of_range_for_arrow_to_mssql(
            index,
            field,
            format!("decimal precision {precision} exceeds SQL Server maximum precision 38"),
        ));
    }

    let scale_as_u8 = u8::try_from(scale).map_err(|_| {
        decimal_out_of_range_for_arrow_to_mssql(
            index,
            field,
            format!("decimal scale {scale} must be non-negative"),
        )
    })?;

    if scale_as_u8 > precision {
        return Err(decimal_out_of_range_for_arrow_to_mssql(
            index,
            field,
            format!("decimal scale {scale} cannot exceed precision {precision}"),
        ));
    }

    Ok((precision, scale))
}

fn plan_arrow_date64_as_mssql_type(
    policy: Date64Policy,
    index: usize,
    field: &Field,
) -> std::result::Result<MssqlType, Diagnostic> {
    match policy {
        Date64Policy::RejectNonMidnight => Err(observed_data_required_for_arrow_to_mssql(
            index,
            field,
            "Date64 requires observed values to verify every value is midnight",
        )),
        Date64Policy::TimestampDateTime2 => Ok(MssqlType::DateTime2 { precision: 3 }),
    }
}

fn plan_arrow_timestamp_as_mssql_type(
    unit: TimeUnit,
    timezone: Option<&str>,
    timezone_policy: TimezonePolicy,
    nanosecond_policy: NanosecondPolicy,
    index: usize,
    field: &Field,
) -> std::result::Result<MssqlType, Diagnostic> {
    let has_timezone = timezone.is_some_and(|timezone| !timezone.is_empty());

    if has_timezone && matches!(timezone_policy, TimezonePolicy::Reject) {
        return Err(policy_required_for_arrow_to_mssql(
            index,
            field,
            "timezone-aware timestamps require TimezonePolicy::DateTimeOffset or TimezonePolicy::NormalizeUtcDateTime2",
        ));
    }

    if matches!(unit, TimeUnit::Nanosecond)
        && matches!(nanosecond_policy, NanosecondPolicy::RejectNon100ns)
    {
        return Err(observed_data_required_for_arrow_to_mssql(
            index,
            field,
            "nanosecond timestamps require observed values to verify 100ns divisibility",
        ));
    }

    let ty = if has_timezone && matches!(timezone_policy, TimezonePolicy::DateTimeOffset) {
        MssqlType::DateTimeOffset { precision: 7 }
    } else {
        MssqlType::DateTime2 { precision: 7 }
    };

    Ok(ty)
}

fn policy_required_for_arrow_to_mssql(
    index: usize,
    field: &Field,
    message: impl Into<String>,
) -> Diagnostic {
    Diagnostic::error(DiagnosticCode::ProfileDependentConversion, message)
        .with_field(FieldRef::new(index, field.name()))
}

fn observed_data_required_for_arrow_to_mssql(
    index: usize,
    field: &Field,
    message: impl Into<String>,
) -> Diagnostic {
    Diagnostic::error(DiagnosticCode::ObservedDataRequired, message)
        .with_field(FieldRef::new(index, field.name()))
}

fn decimal_out_of_range_for_arrow_to_mssql(
    index: usize,
    field: &Field,
    message: impl Into<String>,
) -> Diagnostic {
    Diagnostic::error(DiagnosticCode::DecimalOutOfRange, message)
        .with_field(FieldRef::new(index, field.name()))
}

const SQL_SERVER_MAX_DECIMAL_PRECISION: u8 = 38;

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_schema::{DataType, Field, Schema, TimeUnit};

    use crate::{
        BinaryPolicy, Date64Policy, Decimal256Policy, DecimalPolicy, DiagnosticCode, Error,
        MssqlProfile, MssqlTablePlan, MssqlType, MssqlTypeLength, NanosecondPolicy, PlanOptions,
        StringPolicy, TableName, TimezonePolicy, UInt64Policy,
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
    fn maps_integer_float_string_and_binary_types() {
        let schema = Schema::new(vec![
            Field::new("i8_col", DataType::Int8, false),
            Field::new("i16_col", DataType::Int16, false),
            Field::new("i64_col", DataType::Int64, false),
            Field::new("u8_col", DataType::UInt8, false),
            Field::new("u16_col", DataType::UInt16, false),
            Field::new("u32_col", DataType::UInt32, false),
            Field::new("f32_col", DataType::Float32, false),
            Field::new("f64_col", DataType::Float64, false),
            Field::new("utf8_col", DataType::Utf8, true),
            Field::new("large_utf8_col", DataType::LargeUtf8, true),
            Field::new("binary_col", DataType::Binary, true),
            Field::new("large_binary_col", DataType::LargeBinary, true),
        ]);
        let outcome = MssqlTablePlan::from_arrow_schema(
            Arc::new(schema),
            MssqlProfile::sql_server_2016_compat_100(),
            PlanOptions::default(),
        )
        .unwrap();
        let columns = outcome.value().mssql_columns();

        assert_eq!(columns[0].ty(), &MssqlType::SmallInt);
        assert_eq!(columns[1].ty(), &MssqlType::SmallInt);
        assert_eq!(columns[2].ty(), &MssqlType::BigInt);
        assert_eq!(columns[3].ty(), &MssqlType::TinyInt);
        assert_eq!(columns[4].ty(), &MssqlType::Int);
        assert_eq!(columns[5].ty(), &MssqlType::BigInt);
        assert_eq!(columns[6].ty(), &MssqlType::Real);
        assert_eq!(columns[7].ty(), &MssqlType::Float { precision: 53 });
        assert_eq!(columns[8].ty(), &MssqlType::NVarChar(MssqlTypeLength::Max));
        assert_eq!(columns[9].ty(), &MssqlType::NVarChar(MssqlTypeLength::Max));
        assert_eq!(
            columns[10].ty(),
            &MssqlType::VarBinary(MssqlTypeLength::Max)
        );
        assert_eq!(
            columns[11].ty(),
            &MssqlType::VarBinary(MssqlTypeLength::Max)
        );
    }

    #[test]
    fn applies_bounded_string_and_binary_policies() {
        let schema = Schema::new(vec![
            Field::new("name", DataType::Utf8, true),
            Field::new("payload", DataType::Binary, true),
        ]);
        let options = PlanOptions {
            string_policy: StringPolicy::NVarChar(128),
            binary_policy: BinaryPolicy::VarBinary(256),
            ..PlanOptions::default()
        };
        let outcome = MssqlTablePlan::from_arrow_schema(
            Arc::new(schema),
            MssqlProfile::sql_server_2016_compat_100(),
            options,
        )
        .unwrap();
        let columns = outcome.value().mssql_columns();

        assert_eq!(
            columns[0].ty(),
            &MssqlType::NVarChar(MssqlTypeLength::Bounded(128))
        );
        assert_eq!(
            columns[1].ty(),
            &MssqlType::VarBinary(MssqlTypeLength::Bounded(256))
        );
    }

    #[test]
    fn maps_uint64_when_explicit_policy_is_selected() {
        let schema = Schema::new(vec![Field::new("u64_col", DataType::UInt64, false)]);

        let decimal_outcome = MssqlTablePlan::from_arrow_schema(
            Arc::new(schema.clone()),
            MssqlProfile::sql_server_2016_compat_100(),
            PlanOptions {
                uint64_policy: UInt64Policy::Decimal20_0,
                ..PlanOptions::default()
            },
        )
        .unwrap();
        assert_eq!(
            decimal_outcome.value().mssql_columns()[0].ty(),
            &MssqlType::Decimal {
                precision: 20,
                scale: 0
            }
        );

        let bigint_outcome = MssqlTablePlan::from_arrow_schema(
            Arc::new(schema),
            MssqlProfile::sql_server_2016_compat_100(),
            PlanOptions {
                uint64_policy: UInt64Policy::CheckedBigInt,
                ..PlanOptions::default()
            },
        )
        .unwrap();
        assert_eq!(
            bigint_outcome.value().mssql_columns()[0].ty(),
            &MssqlType::BigInt
        );
    }

    #[test]
    fn default_uint64_policy_returns_structured_planning_diagnostic() {
        let schema = Schema::new(vec![Field::new("u64_col", DataType::UInt64, false)]);

        let err = MssqlTablePlan::from_arrow_schema(
            Arc::new(schema),
            MssqlProfile::sql_server_2016_compat_100(),
            PlanOptions::default(),
        )
        .expect_err("UInt64 should require explicit policy by default");

        let Error::Planning { diagnostics } = err else {
            panic!("expected planning error");
        };

        assert!(diagnostics.has_errors());
        assert_eq!(diagnostics.len(), 1);

        let diagnostic = &diagnostics.all()[0];
        assert_eq!(
            diagnostic.code(),
            DiagnosticCode::ProfileDependentConversion
        );
        assert_eq!(diagnostic.field().unwrap().index(), 0);
        assert_eq!(diagnostic.field().unwrap().name(), "u64_col");
    }

    #[test]
    fn observed_length_policies_return_structured_planning_diagnostics() {
        let schema = Schema::new(vec![
            Field::new("name", DataType::Utf8, true),
            Field::new("payload", DataType::Binary, true),
        ]);
        let options = PlanOptions {
            string_policy: StringPolicy::ObservedNVarChar,
            binary_policy: BinaryPolicy::ObservedVarBinary,
            ..PlanOptions::default()
        };

        let err = MssqlTablePlan::from_arrow_schema(
            Arc::new(schema),
            MssqlProfile::sql_server_2016_compat_100(),
            options,
        )
        .expect_err("observed policies need data, not just schema");

        let Error::Planning { diagnostics } = err else {
            panic!("expected planning error");
        };

        assert_eq!(diagnostics.len(), 2);
        assert!(diagnostics.has_errors());
        assert_eq!(
            diagnostics.all()[0].code(),
            DiagnosticCode::ObservedDataRequired
        );
        assert_eq!(diagnostics.all()[0].field().unwrap().name(), "name");
        assert_eq!(
            diagnostics.all()[1].code(),
            DiagnosticCode::ObservedDataRequired
        );
        assert_eq!(diagnostics.all()[1].field().unwrap().name(), "payload");
    }

    #[test]
    fn maps_decimal_date_and_timestamp_types() {
        let schema = Schema::new(vec![
            Field::new("d32_col", DataType::Decimal32(9, 2), false),
            Field::new("d64_col", DataType::Decimal64(18, 4), false),
            Field::new("d128_col", DataType::Decimal128(38, 9), false),
            Field::new("d256_col", DataType::Decimal256(38, 0), false),
            Field::new("date_col", DataType::Date32, true),
            Field::new(
                "ts_second_col",
                DataType::Timestamp(TimeUnit::Second, None),
                true,
            ),
            Field::new(
                "ts_milli_col",
                DataType::Timestamp(TimeUnit::Millisecond, None),
                true,
            ),
            Field::new(
                "ts_micro_col",
                DataType::Timestamp(TimeUnit::Microsecond, None),
                true,
            ),
            Field::new(
                "ts_empty_timezone_col",
                DataType::Timestamp(TimeUnit::Second, Some("".into())),
                true,
            ),
        ]);

        let outcome = MssqlTablePlan::from_arrow_schema(
            Arc::new(schema),
            MssqlProfile::sql_server_2016_compat_100(),
            PlanOptions::default(),
        )
        .unwrap();
        let columns = outcome.value().mssql_columns();

        assert_eq!(
            columns[0].ty(),
            &MssqlType::Decimal {
                precision: 9,
                scale: 2
            }
        );
        assert_eq!(
            columns[1].ty(),
            &MssqlType::Decimal {
                precision: 18,
                scale: 4
            }
        );
        assert_eq!(
            columns[2].ty(),
            &MssqlType::Decimal {
                precision: 38,
                scale: 9
            }
        );
        assert_eq!(
            columns[3].ty(),
            &MssqlType::Decimal {
                precision: 38,
                scale: 0
            }
        );
        assert_eq!(columns[4].ty(), &MssqlType::Date);
        assert_eq!(columns[5].ty(), &MssqlType::DateTime2 { precision: 7 });
        assert_eq!(columns[6].ty(), &MssqlType::DateTime2 { precision: 7 });
        assert_eq!(columns[7].ty(), &MssqlType::DateTime2 { precision: 7 });
        assert_eq!(columns[8].ty(), &MssqlType::DateTime2 { precision: 7 });
    }

    #[test]
    fn normalizes_negative_decimal_scale_when_policy_is_selected() {
        let schema = Schema::new(vec![Field::new(
            "amount",
            DataType::Decimal128(3, -2),
            false,
        )]);
        let options = PlanOptions {
            decimal_policy: DecimalPolicy::NormalizeNegativeScale,
            ..PlanOptions::default()
        };

        let outcome = MssqlTablePlan::from_arrow_schema(
            Arc::new(schema),
            MssqlProfile::sql_server_2016_compat_100(),
            options,
        )
        .unwrap();

        assert_eq!(
            outcome.value().mssql_columns()[0].ty(),
            &MssqlType::Decimal {
                precision: 5,
                scale: 0
            }
        );
    }

    #[test]
    fn rejects_decimal_shapes_that_sql_server_cannot_represent() {
        let schema = Schema::new(vec![
            Field::new("too_precise", DataType::Decimal256(39, 0), false),
            Field::new("scale_too_large", DataType::Decimal128(3, 4), false),
            Field::new("negative_scale", DataType::Decimal128(3, -2), false),
        ]);

        let err = MssqlTablePlan::from_arrow_schema(
            Arc::new(schema),
            MssqlProfile::sql_server_2016_compat_100(),
            PlanOptions::default(),
        )
        .expect_err("invalid decimal shapes should be rejected");

        let Error::Planning { diagnostics } = err else {
            panic!("expected planning error");
        };

        assert_eq!(diagnostics.len(), 3);
        assert_eq!(
            diagnostics.all()[0].code(),
            DiagnosticCode::DecimalOutOfRange
        );
        assert_eq!(diagnostics.all()[0].field().unwrap().name(), "too_precise");
        assert_eq!(
            diagnostics.all()[1].code(),
            DiagnosticCode::DecimalOutOfRange
        );
        assert_eq!(
            diagnostics.all()[1].field().unwrap().name(),
            "scale_too_large"
        );
        assert_eq!(
            diagnostics.all()[2].code(),
            DiagnosticCode::ProfileDependentConversion
        );
        assert_eq!(
            diagnostics.all()[2].field().unwrap().name(),
            "negative_scale"
        );
    }

    #[test]
    fn decimal256_reject_policy_returns_structured_planning_diagnostic() {
        let schema = Schema::new(vec![Field::new(
            "wide_decimal",
            DataType::Decimal256(38, 0),
            false,
        )]);
        let options = PlanOptions {
            decimal256_policy: Decimal256Policy::Reject,
            ..PlanOptions::default()
        };

        let err = MssqlTablePlan::from_arrow_schema(
            Arc::new(schema),
            MssqlProfile::sql_server_2016_compat_100(),
            options,
        )
        .expect_err("Decimal256 should respect explicit reject policy");

        let Error::Planning { diagnostics } = err else {
            panic!("expected planning error");
        };

        assert_eq!(diagnostics.len(), 1);
        assert_eq!(
            diagnostics.all()[0].code(),
            DiagnosticCode::ProfileDependentConversion
        );
        assert_eq!(diagnostics.all()[0].field().unwrap().name(), "wide_decimal");
    }

    #[test]
    fn date64_requires_policy_or_observed_midnight_validation() {
        let schema = Schema::new(vec![Field::new("date64_col", DataType::Date64, true)]);

        let err = MssqlTablePlan::from_arrow_schema(
            Arc::new(schema.clone()),
            MssqlProfile::sql_server_2016_compat_100(),
            PlanOptions::default(),
        )
        .expect_err("Date64 default requires observed values");

        let Error::Planning { diagnostics } = err else {
            panic!("expected planning error");
        };
        assert_eq!(diagnostics.len(), 1);
        assert_eq!(
            diagnostics.all()[0].code(),
            DiagnosticCode::ObservedDataRequired
        );
        assert_eq!(diagnostics.all()[0].field().unwrap().name(), "date64_col");

        let outcome = MssqlTablePlan::from_arrow_schema(
            Arc::new(schema),
            MssqlProfile::sql_server_2016_compat_100(),
            PlanOptions {
                date64_policy: Date64Policy::TimestampDateTime2,
                ..PlanOptions::default()
            },
        )
        .unwrap();

        assert_eq!(
            outcome.value().mssql_columns()[0].ty(),
            &MssqlType::DateTime2 { precision: 3 }
        );
    }

    #[test]
    fn timezone_timestamp_policy_controls_target_type() {
        let schema = Schema::new(vec![Field::new(
            "ts_col",
            DataType::Timestamp(TimeUnit::Millisecond, Some("+00:00".into())),
            true,
        )]);

        let err = MssqlTablePlan::from_arrow_schema(
            Arc::new(schema.clone()),
            MssqlProfile::sql_server_2016_compat_100(),
            PlanOptions::default(),
        )
        .expect_err("timezone-aware timestamp default should be rejected");

        let Error::Planning { diagnostics } = err else {
            panic!("expected planning error");
        };
        assert_eq!(diagnostics.len(), 1);
        assert_eq!(
            diagnostics.all()[0].code(),
            DiagnosticCode::ProfileDependentConversion
        );

        let offset_outcome = MssqlTablePlan::from_arrow_schema(
            Arc::new(schema.clone()),
            MssqlProfile::sql_server_2016_compat_100(),
            PlanOptions {
                timezone_policy: TimezonePolicy::DateTimeOffset,
                ..PlanOptions::default()
            },
        )
        .unwrap();
        assert_eq!(
            offset_outcome.value().mssql_columns()[0].ty(),
            &MssqlType::DateTimeOffset { precision: 7 }
        );

        let normalized_outcome = MssqlTablePlan::from_arrow_schema(
            Arc::new(schema),
            MssqlProfile::sql_server_2016_compat_100(),
            PlanOptions {
                timezone_policy: TimezonePolicy::NormalizeUtcDateTime2,
                ..PlanOptions::default()
            },
        )
        .unwrap();
        assert_eq!(
            normalized_outcome.value().mssql_columns()[0].ty(),
            &MssqlType::DateTime2 { precision: 7 }
        );
    }

    #[test]
    fn nanosecond_timestamps_require_precision_policy_or_observed_validation() {
        let schema = Schema::new(vec![Field::new(
            "ts_ns_col",
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            true,
        )]);

        let err = MssqlTablePlan::from_arrow_schema(
            Arc::new(schema.clone()),
            MssqlProfile::sql_server_2016_compat_100(),
            PlanOptions::default(),
        )
        .expect_err("nanosecond default requires observed values");

        let Error::Planning { diagnostics } = err else {
            panic!("expected planning error");
        };
        assert_eq!(diagnostics.len(), 1);
        assert_eq!(
            diagnostics.all()[0].code(),
            DiagnosticCode::ObservedDataRequired
        );

        for nanosecond_policy in [
            NanosecondPolicy::RoundTo100ns,
            NanosecondPolicy::TruncateTo100ns,
        ] {
            let outcome = MssqlTablePlan::from_arrow_schema(
                Arc::new(schema.clone()),
                MssqlProfile::sql_server_2016_compat_100(),
                PlanOptions {
                    nanosecond_policy,
                    ..PlanOptions::default()
                },
            )
            .unwrap();

            assert_eq!(
                outcome.value().mssql_columns()[0].ty(),
                &MssqlType::DateTime2 { precision: 7 }
            );
        }
    }

    #[test]
    fn unsupported_type_returns_structured_planning_diagnostic() {
        let schema = Schema::new(vec![Field::new(
            "values",
            DataType::new_list(DataType::Int32, true),
            true,
        )]);

        let err = MssqlTablePlan::from_arrow_schema(
            Arc::new(schema),
            MssqlProfile::sql_server_2016_compat_100(),
            PlanOptions::default(),
        )
        .expect_err("nested mappings are unsupported in v0.1");

        let Error::Planning { diagnostics } = err else {
            panic!("expected planning error");
        };

        assert!(diagnostics.has_errors());
        assert_eq!(diagnostics.len(), 1);

        let diagnostic = &diagnostics.all()[0];
        assert_eq!(diagnostic.code(), DiagnosticCode::UnsupportedArrowType);
        assert_eq!(diagnostic.field().unwrap().index(), 0);
        assert_eq!(diagnostic.field().unwrap().name(), "values");
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

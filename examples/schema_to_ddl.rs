//! Plan an Arrow schema and render SQL Server `CREATE TABLE` DDL.

use arrow_schema::{DataType, Field, Schema};
use arrow_tiberius::{
    CompatibilityLevel, MssqlProfile, MssqlVersion, PlanOptions, TableName,
    create_table_sql_from_mappings,
};

fn main() -> arrow_tiberius::Result<()> {
    let schema = Schema::new(vec![
        Field::new("customer_id", DataType::Int64, false),
        Field::new("display_name", DataType::Utf8, true),
        Field::new("is_active", DataType::Boolean, false),
    ]);

    let profile = MssqlProfile::new(
        MssqlVersion::SqlServer2022,
        CompatibilityLevel::SQL_SERVER_2022,
    )?;
    let outcome = profile.plan_arrow_schema(&schema, PlanOptions::default())?;

    let table = TableName::new("dbo", "customers")?;
    let ddl = create_table_sql_from_mappings(&table, outcome.mappings());

    println!("{ddl}");
    Ok(())
}

//! Plan an Arrow schema and render SQL Server `CREATE TABLE` DDL.

use arrow_schema::{DataType, Field, Schema};
use arrow_tiberius::{
    MssqlProfile, PlanOptions, TableName, create_table_sql_from_mappings,
    plan_arrow_schema_to_mssql_mappings,
};

fn main() -> arrow_tiberius::Result<()> {
    let schema = Schema::new(vec![
        Field::new("customer_id", DataType::Int64, false),
        Field::new("display_name", DataType::Utf8, true),
        Field::new("is_active", DataType::Boolean, false),
    ]);

    let outcome = plan_arrow_schema_to_mssql_mappings(
        &schema,
        MssqlProfile::sql_server_2016_compat_100(),
        PlanOptions::default(),
    )?;

    let table = TableName::new("dbo", "customers")?;
    let ddl = create_table_sql_from_mappings(&table, outcome.value());

    println!("{ddl}");
    Ok(())
}

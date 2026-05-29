//! Apply an explicit planning policy for a policy-dependent Arrow type.

use arrow_schema::{DataType, Field, Schema};
use arrow_tiberius::{
    MssqlProfile, PlanOptions, TableName, UInt64Policy, create_table_sql_from_mappings,
    plan_arrow_schema_to_mssql_mappings,
};

fn main() -> arrow_tiberius::Result<()> {
    let schema = Schema::new(vec![
        Field::new("event_id", DataType::UInt64, false),
        Field::new("payload", DataType::Binary, true),
    ]);

    let plan_options = PlanOptions {
        uint64_policy: UInt64Policy::Decimal20_0,
        ..PlanOptions::default()
    };

    let outcome = plan_arrow_schema_to_mssql_mappings(
        &schema,
        MssqlProfile::sql_server_2016_compat_100(),
        plan_options,
    )?;

    for mapping in outcome.value() {
        println!(
            "{} -> {}",
            mapping.arrow().name(),
            mapping.mssql().ty().to_sql()
        );
    }

    let table = TableName::new("dbo", "events")?;
    let ddl = create_table_sql_from_mappings(&table, outcome.value());
    println!("{ddl}");

    Ok(())
}

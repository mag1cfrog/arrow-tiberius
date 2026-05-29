//! Inspect structured planning diagnostics without string matching.

use std::sync::Arc;

use arrow_schema::{DataType, Field, Schema};
use arrow_tiberius::{Error, MssqlProfile, PlanOptions, plan_arrow_schema_to_mssql_mappings};

fn main() -> arrow_tiberius::Result<()> {
    let schema = Schema::new(vec![
        Field::new("external_id", DataType::UInt64, false),
        Field::new(
            "tags",
            DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
            true,
        ),
    ]);

    match plan_arrow_schema_to_mssql_mappings(
        &schema,
        MssqlProfile::sql_server_2016_compat_100(),
        PlanOptions::default(),
    ) {
        Err(Error::Planning { diagnostics }) => {
            for diagnostic in diagnostics.all() {
                let field = diagnostic
                    .field()
                    .map(|field| format!("{} at index {}", field.name(), field.index()))
                    .unwrap_or_else(|| "schema".to_owned());

                println!(
                    "{:?}\t{:?}\t{}\t{}",
                    diagnostic.severity(),
                    diagnostic.code(),
                    field,
                    diagnostic.message()
                );
            }
        }
        Err(error) => return Err(error),
        Ok(outcome) => {
            println!("planned {} columns without errors", outcome.value().len());
        }
    }

    Ok(())
}

//! Bidirectional Arrow/MSSQL schema mapping.

/// Arrow/MSSQL column mapping.
pub mod mapping;
/// Arrow/MSSQL table schema mapping.
pub mod table_mapping;
pub(crate) mod type_conversion;

pub use mapping::SchemaMapping;
#[cfg(test)]
pub(crate) use table_mapping::plan_arrow_schema_to_mssql_mappings;
pub use table_mapping::{
    PlannedSchema, create_table_sql_from_mappings, mssql_columns_from_mappings,
    plan_arrow_schema_to_mssql_schema,
};

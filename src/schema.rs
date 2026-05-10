//! Bidirectional Arrow/MSSQL schema mapping.

/// Arrow/MSSQL column mapping.
pub mod mapping;
/// Arrow/MSSQL table schema mapping.
pub mod table_mapping;
pub(crate) mod type_conversion;

pub use mapping::SchemaMapping;
pub use table_mapping::{
    create_table_sql_from_mappings, mssql_columns_from_mappings,
    plan_arrow_schema_to_mssql_mappings,
};

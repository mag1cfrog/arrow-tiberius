//! Bidirectional Arrow/MSSQL schema mapping.

/// Arrow/MSSQL column mapping.
pub mod mapping;
/// Arrow/MSSQL table schema plan.
pub mod table_plan;
pub(crate) mod type_conversion;

pub use mapping::SchemaMapping;
pub use table_plan::MssqlTablePlan;

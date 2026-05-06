//! Bidirectional Arrow/MSSQL schema mapping.

/// Arrow/MSSQL column mapping.
pub mod mapping;
/// Arrow/MSSQL table schema plan.
pub mod table_plan;

pub use mapping::SchemaMapping;
pub use table_plan::MssqlTablePlan;

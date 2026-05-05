//! Apache Arrow and SQL Server bridge through Tiberius.
//!
//! This crate is under initial development. v0.1 is focused on reusable
//! library primitives for writing Arrow data into SQL Server while preserving
//! a future module boundary for SQL Server-to-Arrow reads.

/// Deterministic SQL Server DDL rendering helpers.
pub mod ddl;
/// Structured diagnostics for planning and writing.
pub mod diagnostic;
/// Error types for `arrow-tiberius`.
pub mod error;
/// SQL Server identifier types.
pub mod identifier;
/// SQL Server type model.
pub mod mssql_type;
/// SQL Server profile types.
pub mod profile;
/// Write-path options and conversion policies.
pub mod write;

pub use ddl::{ColumnDefinition, CreateTableOptions, create_table_sql};
pub use diagnostic::{
    Diagnostic, DiagnosticCode, DiagnosticSet, DiagnosticSeverity, FieldRef, PlanOutcome,
};
pub use error::{Error, Result};
pub use identifier::{Identifier, IdentifierPolicy, TableName};
pub use mssql_type::{MssqlType, MssqlTypeLength};
pub use profile::{CompatibilityLevel, MssqlProfile, MssqlVersion};

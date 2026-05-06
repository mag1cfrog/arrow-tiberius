//! Apache Arrow and SQL Server bridge through Tiberius.
//!
//! This crate is under initial development. v0.1 is focused on reusable
//! library primitives for writing Arrow data into SQL Server while preserving
//! a future module boundary for SQL Server-to-Arrow reads.

/// Arrow-side schema metadata.
pub mod arrow;
/// Structured diagnostics for planning and writing.
pub mod diagnostic;
/// Error types for `arrow-tiberius`.
pub mod error;
/// MSSQL-side schema metadata, identifiers, profile, and DDL helpers.
pub mod mssql;
/// Bidirectional Arrow/MSSQL schema mapping.
pub mod schema;
/// Write-path options and conversion policies.
pub mod write;

pub use arrow::ArrowFieldPlan;
pub use diagnostic::{
    Diagnostic, DiagnosticCode, DiagnosticSet, DiagnosticSeverity, FieldRef, PlanOutcome,
};
pub use error::{Error, Result};
pub use mssql::{
    CompatibilityLevel, CreateTableOptions, Identifier, IdentifierPolicy, MssqlColumnPlan,
    MssqlProfile, MssqlType, MssqlTypeLength, MssqlVersion, TableName, create_table_sql,
};
pub use schema::{MssqlTablePlan, SchemaMapping};
pub use write::{
    BinaryPolicy, Date64Policy, Decimal256Policy, DecimalPolicy, FloatPolicy, NanosecondPolicy,
    PlanOptions, SchemaCheck, StringPolicy, TimezonePolicy, UInt64Policy,
};

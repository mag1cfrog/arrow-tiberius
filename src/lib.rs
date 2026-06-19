//! Apache Arrow and SQL Server bridge through Tiberius.
//!
//! `arrow-tiberius` bridges Apache Arrow and Microsoft SQL Server through the
//! Tiberius TDS driver. The crate is designed around a bidirectional boundary:
//! Arrow schemas and [`RecordBatch`] values can be planned and written to SQL
//! Server, and future read-side APIs can map SQL Server metadata and rows back
//! to Arrow.
//!
//! The v0.1 API implements the Arrow-to-SQL Server write path first: plan an
//! Arrow schema for SQL Server, render deterministic DDL, inspect structured
//! diagnostics, and bulk load one or more record batches. SQL Server-to-Arrow
//! reads are reserved for a later release.
//!
//! [`RecordBatch`]: arrow_array::RecordBatch
//!
//! # Quick Start
//!
//! Plan an Arrow schema and render `CREATE TABLE` SQL:
//!
//! ```
//! use arrow_schema::{DataType, Field, Schema};
//! use arrow_tiberius::{
//!     MssqlProfile, PlanOptions, TableName, create_table_sql_from_mappings,
//!     plan_arrow_schema_to_mssql_mappings,
//! };
//!
//! # fn main() -> arrow_tiberius::Result<()> {
//! let schema = Schema::new(vec![
//!     Field::new("id", DataType::Int64, false),
//!     Field::new("name", DataType::Utf8, true),
//! ]);
//!
//! let outcome = plan_arrow_schema_to_mssql_mappings(
//!     &schema,
//!     MssqlProfile::sql_server_2016_compat_100(),
//!     PlanOptions::default(),
//! )?;
//!
//! let table = TableName::new("dbo", "people")?;
//! let ddl = create_table_sql_from_mappings(&table, outcome.value());
//! assert!(ddl.contains("CREATE TABLE [dbo].[people]"));
//! # Ok(())
//! # }
//! ```
//!
//! Connect through the crate-owned Tiberius compatibility boundary:
//!
//! ```no_run
//! use arrow_tiberius::{
//!     ConnectedMssqlClient, connect_mssql_client_from_ado_string,
//! };
//!
//! async fn connect(
//!     connection_string: &str,
//! ) -> arrow_tiberius::Result<ConnectedMssqlClient> {
//!     connect_mssql_client_from_ado_string(connection_string).await
//! }
//! ```
//!
//! [`BulkWriter`] validates target table metadata before writing. It does not
//! create tables automatically; callers can use [`create_table_sql_from_mappings`]
//! when they want this crate to produce a table definition.
//!
//! # Main Modules
//!
//! - [`schema`] plans Arrow fields into SQL Server column mappings and DDL
//!   metadata.
//! - [`mssql`] contains SQL Server identifiers, profiles, types, and DDL
//!   helpers.
//! - [`diagnostic`] exposes structured planning and runtime diagnostics.
//! - The [`write` module](crate::write) contains write policies, backend
//!   selection, and [`BulkWriter`].
//!
//! # Writer Backends
//!
//! [`WriteBackend::Auto`] is the default selection and currently resolves to
//! [`WriteBackend::DirectRawBulk`].
//! [`WriteBackend::DirectRawBulk`] is the optimized direct Arrow-to-TDS path for
//! supported mappings. [`WriteBackend::BaselineTokenRow`] remains available as a
//! compatibility and reference path through Tiberius `TokenRow` bulk load.
//! [`WriteBackend::DirectFramedBulk`] uses the direct row encoder through
//! Tiberius framed writes.
//!
//! # SQL Server Compatibility
//!
//! The initial profile is [`MssqlProfile::sql_server_2016_compat_100`], which
//! targets SQL Server 2016 with database compatibility level 100.
//!
//! # Tiberius Dependency Model
//!
//! This crate depends on the published `tiberius-raw-bulk` package as the crate
//! name `tiberius` and owns that compatibility boundary. Downstream crates
//! should use [`connect_mssql_client_from_ado_string`] and
//! [`ConnectedMssqlClient`] instead of constructing a raw Tiberius client for
//! [`BulkWriter`].
//!
//! ```toml
//! [dependencies]
//! arrow-tiberius = "0.1"
//! ```
//!
//! Depending on upstream `tiberius` separately creates a distinct crate type and
//! will not produce a client compatible with this crate's writer internals.
//!
//! # Feature Flags
//!
//! - `bench-profile`: benchmark-only direct write profiling hooks.
//! - `integration-tests`: SQL Server integration tests that are normally run
//!   through `cargo xtask sqlserver-test`.
//!
//! Docs.rs is configured to build with all features so feature-gated public
//! items are visible in API documentation. Normal library use does not require
//! either feature.
//!
//! # More Documentation
//!
//! - [Arrow to SQL Server Type Mapping](https://github.com/mag1cfrog/arrow-tiberius/blob/main/docs/type-mapping.md)
//! - [Integration Tests](https://github.com/mag1cfrog/arrow-tiberius/blob/main/docs/integration-tests.md)
//! - [Writer Benchmarks](https://github.com/mag1cfrog/arrow-tiberius/blob/main/docs/benchmarks.md)

/// Arrow-side schema metadata.
pub mod arrow;
/// SQL Server connection helpers.
pub mod connection;
/// Directional conversion semantics between Arrow and SQL Server.
pub(crate) mod conversion;
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

pub use arrow::ArrowFieldRef;
pub use connection::{
    ConnectedBulkWriter, ConnectedMssqlClient, SqlExecutionOutcome,
    connect_mssql_client_from_ado_string,
};
pub use diagnostic::{
    Diagnostic, DiagnosticCode, DiagnosticSet, DiagnosticSeverity, FieldRef, PlanOutcome,
};
pub use error::{Error, Result};
pub use mssql::{
    CompatibilityLevel, CreateTableOptions, Identifier, IdentifierPolicy, MssqlColumn,
    MssqlProfile, MssqlTimePrecision, MssqlType, MssqlTypeLength, MssqlVersion, TableName,
    create_table_sql,
};
pub use schema::{
    SchemaMapping, create_table_sql_from_mappings, mssql_columns_from_mappings,
    plan_arrow_schema_to_mssql_mappings,
};
pub use write::{
    BinaryPolicy, BulkWriter, Date64Policy, Decimal256Policy, DecimalPolicy, FloatPolicy,
    NanosecondPolicy, PlanOptions, SchemaCheck, StringPolicy, TimezonePolicy, UInt64Policy,
    WriteBackend, WriteOptions, WriteStats, validate_arrow_schema_against_mappings,
    validate_record_batch_schema_against_mappings,
};

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
//! diagnostics, and bulk load one or more record batches. Callers own data
//! sources, Delta Lake reads, S3 or object-store access, CLI runtime,
//! configuration, secrets, connection pooling, retries, scheduling, table
//! publishing or synonym swaps, and broader SQL Server administration.
//!
//! A future downstream Delta Lake to SQL Server exporter should own Delta,
//! object-store, runtime, configuration, and orchestration concerns. It should
//! depend on this crate for Arrow-to-SQL Server planning, DDL, diagnostics, and
//! writing only.
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
//! Write a planned batch to an existing table:
//!
//! ```no_run
//! use arrow_array::RecordBatch;
//! use arrow_tiberius::{
//!     BulkWriter, MssqlProfile, PlanOptions, TableName, WriteBackend,
//!     WriteOptions, plan_arrow_schema_to_mssql_mappings,
//! };
//! use futures_util::io::{AsyncRead, AsyncWrite};
//!
//! async fn write_batch<S>(
//!     client: &mut tiberius::Client<S>,
//!     batch: &RecordBatch,
//! ) -> arrow_tiberius::Result<()>
//! where
//!     S: AsyncRead + AsyncWrite + Unpin + Send,
//! {
//!     let outcome = plan_arrow_schema_to_mssql_mappings(
//!         batch.schema().as_ref(),
//!         MssqlProfile::sql_server_2016_compat_100(),
//!         PlanOptions::default(),
//!     )?;
//!
//!     let mut writer = BulkWriter::new(
//!         client,
//!         TableName::new("dbo", "people")?,
//!         outcome.value().to_vec(),
//!         WriteOptions {
//!             backend: WriteBackend::DirectRawBulk,
//!             ..WriteOptions::default()
//!         },
//!     )
//!     .await?;
//!
//!     writer.write_batch(batch).await?;
//!     writer.finish().await?;
//!     Ok(())
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
//! name `tiberius`. Downstream crates that construct the Tiberius client passed
//! to [`BulkWriter`] should use the same package identity:
//!
//! ```toml
//! [dependencies]
//! arrow-tiberius = "0.1"
//! tiberius = { package = "tiberius-raw-bulk", version = "=0.12.3-raw-bulk.13", default-features = false, features = [
//!     "tds73",
//!     "winauth",
//!     "native-tls",
//! ] }
//! ```
//!
//! Depending on upstream `tiberius` separately creates a distinct crate type and
//! will not produce a client compatible with [`BulkWriter`].
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
    WriteBackend, WriteOptions, WriteStats,
};

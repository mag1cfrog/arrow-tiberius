//! Apache Arrow and SQL Server bridge through Tiberius.
//!
//! This crate is under initial development. v0.1 is focused on reusable
//! library primitives for writing Arrow data into SQL Server while preserving
//! a future module boundary for SQL Server-to-Arrow reads.

/// Error types for `arrow-tiberius`.
pub mod error;
/// SQL Server profile types.
pub mod profile;

pub use error::{Error, Result};
pub use profile::{CompatibilityLevel, MssqlProfile, MssqlVersion};

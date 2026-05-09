//! MSSQL-side schema metadata, identifiers, profile, and DDL helpers.

/// MSSQL column metadata model.
pub mod column;
/// Deterministic MSSQL DDL rendering helpers.
pub mod ddl;
/// MSSQL identifier types.
pub mod identifier;
/// MSSQL profile types.
pub mod profile;
/// MSSQL type model.
pub mod ty;

pub use column::MssqlColumn;
pub use ddl::{CreateTableOptions, create_table_sql};
pub use identifier::{Identifier, IdentifierPolicy, TableName};
pub use profile::{CompatibilityLevel, MssqlProfile, MssqlVersion};
pub use ty::{MssqlType, MssqlTypeLength};

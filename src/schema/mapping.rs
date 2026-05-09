//! Arrow/MSSQL column mapping.

use crate::{ArrowFieldRef, MssqlColumn};

/// Planned mapping between one Arrow field and one MSSQL column.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SchemaMapping {
    arrow: ArrowFieldRef,
    mssql: MssqlColumn,
}

impl SchemaMapping {
    /// Creates a schema mapping.
    pub const fn new(arrow: ArrowFieldRef, mssql: MssqlColumn) -> Self {
        Self { arrow, mssql }
    }

    /// Returns the Arrow side of the mapping.
    pub const fn arrow(&self) -> &ArrowFieldRef {
        &self.arrow
    }

    /// Returns the MSSQL side of the mapping.
    pub const fn mssql(&self) -> &MssqlColumn {
        &self.mssql
    }
}

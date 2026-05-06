//! Arrow/MSSQL column mapping.

use crate::{ArrowFieldPlan, MssqlColumnPlan};

/// Planned mapping between one Arrow field and one MSSQL column.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SchemaMapping {
    arrow: ArrowFieldPlan,
    mssql: MssqlColumnPlan,
}

impl SchemaMapping {
    /// Creates a schema mapping.
    pub const fn new(arrow: ArrowFieldPlan, mssql: MssqlColumnPlan) -> Self {
        Self { arrow, mssql }
    }

    /// Returns the Arrow side of the mapping.
    pub const fn arrow(&self) -> &ArrowFieldPlan {
        &self.arrow
    }

    /// Returns the MSSQL side of the mapping.
    pub const fn mssql(&self) -> &MssqlColumnPlan {
        &self.mssql
    }
}

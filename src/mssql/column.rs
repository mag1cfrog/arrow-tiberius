//! MSSQL column metadata model.

use super::{Identifier, MssqlType};

/// Planned MSSQL column metadata.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MssqlColumnPlan {
    name: Identifier,
    ty: MssqlType,
    nullable: bool,
}

impl MssqlColumnPlan {
    /// Creates planned MSSQL column metadata.
    pub const fn new(name: Identifier, ty: MssqlType, nullable: bool) -> Self {
        Self { name, ty, nullable }
    }

    /// Returns the column name.
    pub const fn name(&self) -> &Identifier {
        &self.name
    }

    /// Returns the MSSQL column type.
    pub const fn ty(&self) -> &MssqlType {
        &self.ty
    }

    /// Returns true when the column allows `NULL`.
    pub const fn nullable(&self) -> bool {
        self.nullable
    }

    pub(crate) fn to_sql(&self) -> String {
        let nullability = if self.nullable { "NULL" } else { "NOT NULL" };
        format!(
            "{} {} {nullability}",
            self.name.quoted_sql(),
            self.ty.to_sql()
        )
    }
}

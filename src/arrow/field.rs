//! Arrow field metadata model.

use arrow_schema::DataType;

/// Planned Arrow field metadata.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ArrowFieldPlan {
    index: usize,
    name: String,
    nullable: bool,
    data_type: DataType,
}

impl ArrowFieldPlan {
    /// Creates planned Arrow field metadata.
    pub const fn new(index: usize, name: String, nullable: bool, data_type: DataType) -> Self {
        Self {
            index,
            name,
            nullable,
            data_type,
        }
    }

    /// Returns the Arrow field index.
    pub const fn index(&self) -> usize {
        self.index
    }

    /// Returns the Arrow field name.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns true when the Arrow field is nullable.
    pub const fn nullable(&self) -> bool {
        self.nullable
    }

    /// Returns the Arrow field data type.
    pub const fn data_type(&self) -> &DataType {
        &self.data_type
    }
}

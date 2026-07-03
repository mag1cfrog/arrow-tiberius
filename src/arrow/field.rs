//! Arrow field metadata model.

use arrow_schema::DataType;

/// Reference to an Arrow source field used by a schema mapping.
///
/// This is this crate's mapped source-field metadata. It is not a replacement
/// for `arrow_schema::Field`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ArrowFieldRef {
    index: usize,
    name: String,
    nullable: bool,
    data_type: DataType,
}

impl ArrowFieldRef {
    /// Creates an Arrow source field reference.
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

/// Returns true for Arrow string representations that carry UTF-8 text values.
pub(crate) fn is_arrow_string_family(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View
    )
}

/// Returns true for Arrow binary representations that carry variable-width bytes.
pub(crate) fn is_arrow_binary_family(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Binary | DataType::LargeBinary | DataType::BinaryView
    )
}

#[cfg(test)]
mod tests {
    use arrow_schema::DataType;

    use super::{is_arrow_binary_family, is_arrow_string_family};

    #[test]
    fn identifies_arrow_variable_width_representation_families() {
        for data_type in [DataType::Utf8, DataType::LargeUtf8, DataType::Utf8View] {
            assert!(is_arrow_string_family(&data_type));
            assert!(!is_arrow_binary_family(&data_type));
        }

        for data_type in [
            DataType::Binary,
            DataType::LargeBinary,
            DataType::BinaryView,
        ] {
            assert!(is_arrow_binary_family(&data_type));
            assert!(!is_arrow_string_family(&data_type));
        }

        for data_type in [DataType::Int32, DataType::FixedSizeBinary(4)] {
            assert!(!is_arrow_string_family(&data_type));
            assert!(!is_arrow_binary_family(&data_type));
        }
    }
}

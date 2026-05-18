//! Arrow-side schema metadata.

/// Arrow runtime cell value model.
pub(crate) mod cell;
/// Arrow field metadata model.
pub mod field;

pub use field::ArrowFieldRef;

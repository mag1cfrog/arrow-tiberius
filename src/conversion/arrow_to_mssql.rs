//! Arrow-to-SQL Server conversion semantics.

/// Primitive Arrow-to-SQL Server conversion classification.
pub(crate) mod primitive;
/// UInt64 policy-dependent Arrow-to-SQL Server conversion classification.
pub(crate) mod uint64;
/// Variable-width Arrow-to-SQL Server conversion classification.
pub(crate) mod variable_width;

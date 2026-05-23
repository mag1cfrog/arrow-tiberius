//! Arrow-to-SQL Server conversion semantics.

/// Decimal Arrow-to-SQL Server conversion classification.
pub(crate) mod decimal;
/// Primitive Arrow-to-SQL Server conversion classification.
pub(crate) mod primitive;
/// Temporal Arrow-to-SQL Server conversion classification.
pub(crate) mod temporal;
/// UInt64 policy-dependent Arrow-to-SQL Server conversion classification.
pub(crate) mod uint64;
/// Variable-width Arrow-to-SQL Server conversion classification.
pub(crate) mod variable_width;

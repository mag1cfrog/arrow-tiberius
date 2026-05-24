//! Per-type direct TDS row encoding helpers.

pub(crate) mod decimal;
pub(crate) mod fixed_size_binary;
pub(crate) mod primitive;
pub(crate) mod temporal;
pub(crate) mod uint64;
pub(crate) mod variable_width;

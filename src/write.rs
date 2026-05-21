//! Write-path options and policies.

pub(crate) mod direct;
/// Write-path planning and conversion policies.
pub mod policy;
/// Benchmark-only write profiling hooks.
#[cfg(feature = "bench-profile")]
pub mod profile;
#[cfg(not(feature = "bench-profile"))]
pub(crate) mod profile;
pub(crate) mod record_batch;
pub(crate) mod token_row;
/// Baseline bulk writer public API skeleton.
pub mod writer;

pub use policy::{
    BinaryPolicy, Date64Policy, Decimal256Policy, DecimalPolicy, FloatPolicy, NanosecondPolicy,
    PlanOptions, SchemaCheck, StringPolicy, TimezonePolicy, UInt64Policy,
};
pub use writer::{BulkWriter, WriteBackend, WriteOptions, WriteStats};

//! Error types for `arrow-tiberius`.

use snafu::Snafu;

/// Crate-local result type.
pub type Result<T> = std::result::Result<T, Error>;

/// Error type for `arrow-tiberius` operations.
#[derive(Debug, Snafu)]
#[non_exhaustive]
pub enum Error {
    /// A SQL Server database compatibility level is not supported.
    #[snafu(display("invalid compatibility level {level}"))]
    InvalidCompatibilityLevel {
        /// The unsupported compatibility level value.
        level: u16,
    },

    /// A SQL Server identifier is invalid for the selected identifier policy.
    #[snafu(display("invalid identifier: {reason}"))]
    InvalidIdentifier {
        /// Human-readable reason the identifier is invalid.
        reason: String,
    },
}

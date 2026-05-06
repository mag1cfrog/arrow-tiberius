//! Error types for `arrow-tiberius`.

use snafu::Snafu;

use crate::DiagnosticSet;

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

    /// Arrow schema planning failed.
    #[snafu(display("write planning failed with {} diagnostic(s)", diagnostics.len()))]
    Planning {
        /// Structured planning diagnostics.
        diagnostics: DiagnosticSet,
    },
}

#[cfg(test)]
mod tests {
    use crate::{Diagnostic, DiagnosticCode, DiagnosticSet, Error};

    #[test]
    fn planning_error_display_includes_diagnostic_count() {
        let diagnostics = DiagnosticSet::from(vec![Diagnostic::error(
            DiagnosticCode::UnsupportedArrowType,
            "unsupported",
        )]);
        let err = Error::Planning { diagnostics };

        assert_eq!(
            err.to_string(),
            "write planning failed with 1 diagnostic(s)"
        );
    }
}

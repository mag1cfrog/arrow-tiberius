//! Error types for `arrow-tiberius`.

use snafu::Snafu;

use crate::{DiagnosticSet, WriteBackend};

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

    /// Runtime value conversion failed.
    #[snafu(display(
        "value conversion failed with {} diagnostic(s)",
        diagnostics.len()
    ))]
    ValueConversion {
        /// Structured value conversion diagnostics.
        diagnostics: DiagnosticSet,
    },

    /// A requested write backend is not available.
    #[snafu(display("write backend {backend:?} is unavailable: {reason}"))]
    BackendUnavailable {
        /// Requested write backend.
        backend: WriteBackend,
        /// Human-readable reason the backend is unavailable.
        reason: String,
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

    #[test]
    fn value_conversion_error_display_includes_diagnostic_count() {
        let diagnostics = DiagnosticSet::from(vec![Diagnostic::error(
            DiagnosticCode::ValueConversionUnsupported,
            "unsupported conversion",
        )]);
        let err = Error::ValueConversion { diagnostics };

        assert_eq!(
            err.to_string(),
            "value conversion failed with 1 diagnostic(s)"
        );
    }

    #[test]
    fn backend_unavailable_error_display_includes_backend() {
        let err = Error::BackendUnavailable {
            backend: crate::WriteBackend::DirectRawBulk,
            reason: "not implemented".to_owned(),
        };

        assert_eq!(
            err.to_string(),
            "write backend DirectRawBulk is unavailable: not implemented"
        );
    }
}

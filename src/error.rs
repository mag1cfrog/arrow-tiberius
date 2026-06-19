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

    /// Direct raw TDS encoding failed.
    #[snafu(display(
        "direct encoding failed with {} diagnostic(s)",
        diagnostics.len()
    ))]
    DirectEncoding {
        /// Structured direct encoding diagnostics.
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

    /// A SQL Server ADO-style connection string is invalid.
    #[snafu(display("SQL Server connection string is invalid"))]
    InvalidConnectionString,

    /// Opening the TCP connection to SQL Server failed.
    #[snafu(display("TCP connection to SQL Server failed: {source}"))]
    ConnectionTcpConnect {
        /// Source I/O error returned by the TCP transport.
        source: std::io::Error,
    },

    /// Tiberius failed while establishing the SQL Server client session.
    #[snafu(display("SQL Server client setup failed: {source}"))]
    ConnectionClientSetup {
        /// Source error returned by Tiberius.
        source: tiberius::error::Error,
    },

    /// SQL Server table-existence metadata query failed.
    #[snafu(display("SQL Server table existence query failed: {source}"))]
    TableExistsQuery {
        /// Source error returned by Tiberius.
        source: tiberius::error::Error,
    },

    /// SQL Server table-existence metadata query returned an unexpected shape.
    #[snafu(display("SQL Server table existence query returned an unexpected result: {reason}"))]
    TableExistsUnexpectedResult {
        /// Human-readable reason the metadata result could not be interpreted.
        reason: String,
    },

    /// SQL Server lifecycle statement execution failed.
    #[snafu(display("SQL Server statement execution failed: {source}"))]
    SqlExecution {
        /// Source error returned by Tiberius.
        source: tiberius::error::Error,
    },

    /// Tiberius returned an error while executing a SQL Server operation.
    #[snafu(display("tiberius operation failed: {source}"))]
    Tiberius {
        /// Source error returned by Tiberius.
        source: tiberius::error::Error,
    },
}

#[cfg(test)]
mod tests {
    use std::{borrow::Cow, error::Error as StdError};

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
    fn direct_encoding_error_display_includes_diagnostic_count() {
        let diagnostics = DiagnosticSet::from(vec![Diagnostic::error(
            DiagnosticCode::DirectEncodingInvalidPayload,
            "invalid payload",
        )]);
        let err = Error::DirectEncoding { diagnostics };

        assert_eq!(
            err.to_string(),
            "direct encoding failed with 1 diagnostic(s)"
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

    #[test]
    fn invalid_connection_string_error_display_is_redacted() {
        let err = Error::InvalidConnectionString;

        assert_eq!(err.to_string(), "SQL Server connection string is invalid");
    }

    #[test]
    fn tiberius_error_display_includes_source_error() {
        let err = Error::Tiberius {
            source: tiberius::error::Error::Protocol(Cow::Borrowed("invalid token")),
        };

        assert_eq!(
            err.to_string(),
            "tiberius operation failed: Protocol error: invalid token"
        );
    }

    #[test]
    fn tiberius_error_preserves_source_error() {
        let err = Error::Tiberius {
            source: tiberius::error::Error::BulkInput(Cow::Borrowed("row payload is malformed")),
        };

        let source = StdError::source(&err).expect("tiberius source should be preserved");
        assert_eq!(
            source.to_string(),
            "BULK UPLOAD input failure: row payload is malformed"
        );
    }
}

//! Error types for `arrow-tiberius`.

use std::fmt;

use snafu::Snafu;

use crate::{DiagnosticSet, WriteBackend};

/// Crate-local result type.
pub type Result<T> = std::result::Result<T, Error>;

/// Stable write-path phase vocabulary for classifying writer failures.
///
/// Phase names match the `phase` field emitted by crate-owned tracing
/// instrumentation. Diagnostics describe what failed; phases describe where the
/// failure happened.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum WritePhase {
    /// Arrow-to-SQL Server schema planning.
    SchemaPlanning,
    /// Bulk writer initialization.
    WriterInitialization,
    /// SQL Server target metadata validation before writer startup completes.
    TargetMetadataValidation,
    /// Bulk writer batch write entry point.
    BatchWrite,
    /// Runtime record batch schema validation before value conversion.
    BatchSchemaValidation,
    /// Runtime Arrow value conversion to SQL Server cells.
    ValueConversion,
    /// Direct raw TDS encoding.
    DirectEncoding,
    /// Packet or row write to the Tiberius bulk load sink.
    PacketWrite,
    /// Bulk writer finish entry point.
    Finish,
    /// Tiberius bulk load finalization.
    Finalize,
}

impl WritePhase {
    /// Returns the stable tracing-compatible phase name.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::SchemaPlanning => "schema_planning",
            Self::WriterInitialization => "writer_initialization",
            Self::TargetMetadataValidation => "target_metadata_validation",
            Self::BatchWrite => "batch_write",
            Self::BatchSchemaValidation => "batch_schema_validation",
            Self::ValueConversion => "value_conversion",
            Self::DirectEncoding => "direct_encoding",
            Self::PacketWrite => "packet_write",
            Self::Finish => "finish",
            Self::Finalize => "finalize",
        }
    }
}

impl fmt::Display for WritePhase {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Error type for `arrow-tiberius` operations.
#[derive(Debug, Snafu)]
#[non_exhaustive]
pub enum Error {
    /// A write-path operation failed in a known writer phase.
    #[snafu(display("write phase {phase} failed"))]
    WritePhaseContext {
        /// Stable writer phase where the failure was observed.
        phase: WritePhase,
        /// Original error raised by the failing operation.
        source: Box<Error>,
    },

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

    /// SQL Server target row-count query failed.
    #[snafu(display("SQL Server target row count query failed: {source}"))]
    TargetRowCountQuery {
        /// Source error returned by Tiberius.
        source: tiberius::error::Error,
    },

    /// SQL Server target row-count query returned an unexpected shape.
    #[snafu(display("SQL Server target row count query returned an unexpected result: {reason}"))]
    TargetRowCountUnexpectedResult {
        /// Human-readable reason the target row-count result could not be interpreted.
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

impl Error {
    /// Attaches stable writer phase context to an error.
    #[must_use]
    pub fn with_write_phase(self, phase: WritePhase) -> Self {
        Self::WritePhaseContext {
            phase,
            source: Box::new(self),
        }
    }

    /// Returns the stable writer phase when the error can be classified.
    pub const fn write_phase(&self) -> Option<WritePhase> {
        match self {
            Self::WritePhaseContext { phase, .. } => Some(*phase),
            Self::Planning { .. } => Some(WritePhase::SchemaPlanning),
            Self::ValueConversion { .. } => Some(WritePhase::ValueConversion),
            Self::DirectEncoding { .. } => Some(WritePhase::DirectEncoding),
            _ => None,
        }
    }

    /// Returns the inner error without writer phase context.
    pub fn without_write_phase(&self) -> &Self {
        match self {
            Self::WritePhaseContext { source, .. } => source.without_write_phase(),
            _ => self,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{borrow::Cow, error::Error as StdError};

    use crate::{
        Diagnostic, DiagnosticCode, DiagnosticSet, Error, WritePhase,
        observability::{
            BATCH_SCHEMA_VALIDATION_PHASE, BATCH_WRITE_PHASE, DIRECT_ENCODING_PHASE,
            FINALIZE_PHASE, FINISH_PHASE, PACKET_WRITE_PHASE, SCHEMA_PLANNING_PHASE,
            TARGET_METADATA_VALIDATION_PHASE, VALUE_CONVERSION_PHASE, WRITER_INITIALIZATION_PHASE,
        },
    };

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
    fn write_phase_names_match_tracing_vocabulary() {
        assert_eq!(WritePhase::SchemaPlanning.as_str(), SCHEMA_PLANNING_PHASE);
        assert_eq!(
            WritePhase::WriterInitialization.as_str(),
            WRITER_INITIALIZATION_PHASE
        );
        assert_eq!(
            WritePhase::TargetMetadataValidation.as_str(),
            TARGET_METADATA_VALIDATION_PHASE
        );
        assert_eq!(WritePhase::BatchWrite.as_str(), BATCH_WRITE_PHASE);
        assert_eq!(
            WritePhase::BatchSchemaValidation.as_str(),
            BATCH_SCHEMA_VALIDATION_PHASE
        );
        assert_eq!(WritePhase::ValueConversion.as_str(), VALUE_CONVERSION_PHASE);
        assert_eq!(WritePhase::DirectEncoding.as_str(), DIRECT_ENCODING_PHASE);
        assert_eq!(WritePhase::PacketWrite.as_str(), PACKET_WRITE_PHASE);
        assert_eq!(WritePhase::Finish.as_str(), FINISH_PHASE);
        assert_eq!(WritePhase::Finalize.as_str(), FINALIZE_PHASE);
    }

    #[test]
    fn write_phase_context_classifies_without_display_parsing() {
        let err = Error::Tiberius {
            source: tiberius::error::Error::BulkInput(Cow::Borrowed("packet failed")),
        }
        .with_write_phase(WritePhase::PacketWrite);

        assert_eq!(err.write_phase(), Some(WritePhase::PacketWrite));
        assert_eq!(err.to_string(), "write phase packet_write failed");
        assert!(matches!(err.without_write_phase(), Error::Tiberius { .. }));
    }

    #[test]
    fn write_phase_context_display_does_not_include_source_text() {
        let err = Error::Tiberius {
            source: tiberius::error::Error::BulkInput(Cow::Borrowed(
                "server=tcp:sql.example.com;User ID=sa;password=secret",
            )),
        }
        .with_write_phase(WritePhase::Finalize);

        assert_eq!(err.to_string(), "write phase finalize failed");
        assert!(!err.to_string().contains("password=secret"));
        assert!(!err.to_string().contains("User ID=sa"));
        assert!(!err.to_string().contains("sql.example.com"));
    }

    #[test]
    fn unwrapped_diagnostic_errors_keep_default_phase_classification() {
        let planning = Error::Planning {
            diagnostics: DiagnosticSet::from(vec![Diagnostic::error(
                DiagnosticCode::UnsupportedArrowType,
                "unsupported",
            )]),
        };
        let value_conversion = Error::ValueConversion {
            diagnostics: DiagnosticSet::from(vec![Diagnostic::error(
                DiagnosticCode::NonFiniteFloat,
                "not finite",
            )]),
        };
        let direct_encoding = Error::DirectEncoding {
            diagnostics: DiagnosticSet::from(vec![Diagnostic::error(
                DiagnosticCode::DirectEncodingInvalidPayload,
                "invalid",
            )]),
        };

        assert_eq!(planning.write_phase(), Some(WritePhase::SchemaPlanning));
        assert_eq!(
            value_conversion.write_phase(),
            Some(WritePhase::ValueConversion)
        );
        assert_eq!(
            direct_encoding.write_phase(),
            Some(WritePhase::DirectEncoding)
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

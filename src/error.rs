//! Error types for `arrow-tiberius`.

use std::fmt::{self, Write as _};

use snafu::Snafu;

use crate::{DiagnosticCode, DiagnosticSet, MssqlVersion, WriteBackend};

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

/// Safe, structured information for user-facing error reports.
///
/// This view keeps dependency source text out of default reports. Use
/// [`std::error::Error::source`] only for trusted debug paths that apply their
/// own redaction policy.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ErrorInfo<'a> {
    phase: Option<WritePhase>,
    kind: &'static str,
    summary: &'static str,
    diagnostics: Option<&'a DiagnosticSet>,
}

impl<'a> ErrorInfo<'a> {
    /// Returns the classified write phase when one is known.
    pub const fn phase(&self) -> Option<WritePhase> {
        self.phase
    }

    /// Returns the inner error kind after write phase context is removed.
    pub const fn kind(&self) -> &'static str {
        self.kind
    }

    /// Returns a sanitized, user-facing summary.
    pub const fn summary(&self) -> &'static str {
        self.summary
    }

    /// Returns structured diagnostics carried by this error, when present.
    pub const fn diagnostics(&self) -> Option<&'a DiagnosticSet> {
        self.diagnostics
    }

    /// Returns comma-separated diagnostic code names for compact reports.
    pub fn diagnostic_codes(&self) -> String {
        if let Some(diagnostics) = self.diagnostics {
            diagnostic_codes(diagnostics)
        } else if self.kind == "BackendUnavailable" {
            format!("{:?}", DiagnosticCode::BackendUnavailable)
        } else {
            String::new()
        }
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

    /// A SQL Server database compatibility level is not supported by the
    /// selected SQL Server engine version.
    #[snafu(display(
        "{version:?} does not support database compatibility level {compatibility_level}"
    ))]
    UnsupportedCompatibilityLevel {
        /// SQL Server engine version.
        version: MssqlVersion,
        /// Unsupported compatibility level value for the selected version.
        compatibility_level: u16,
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

    /// Returns safe, structured information for user-facing reports.
    pub fn safe_error_info(&self) -> ErrorInfo<'_> {
        let inner = self.without_write_phase();
        ErrorInfo {
            phase: self.write_phase(),
            kind: inner.kind(),
            summary: inner.safe_summary(),
            diagnostics: inner.diagnostics(),
        }
    }

    fn kind(&self) -> &'static str {
        match self {
            Self::WritePhaseContext { source, .. } => source.kind(),
            Self::InvalidCompatibilityLevel { .. } => "InvalidCompatibilityLevel",
            Self::UnsupportedCompatibilityLevel { .. } => "UnsupportedCompatibilityLevel",
            Self::InvalidIdentifier { .. } => "InvalidIdentifier",
            Self::Planning { .. } => "Planning",
            Self::ValueConversion { .. } => "ValueConversion",
            Self::DirectEncoding { .. } => "DirectEncoding",
            Self::BackendUnavailable { .. } => "BackendUnavailable",
            Self::InvalidConnectionString => "InvalidConnectionString",
            Self::ConnectionTcpConnect { .. } => "ConnectionTcpConnect",
            Self::ConnectionClientSetup { .. } => "ConnectionClientSetup",
            Self::TableExistsQuery { .. } => "TableExistsQuery",
            Self::TableExistsUnexpectedResult { .. } => "TableExistsUnexpectedResult",
            Self::TargetRowCountQuery { .. } => "TargetRowCountQuery",
            Self::TargetRowCountUnexpectedResult { .. } => "TargetRowCountUnexpectedResult",
            Self::SqlExecution { .. } => "SqlExecution",
            Self::Tiberius { .. } => "Tiberius",
        }
    }

    fn safe_summary(&self) -> &'static str {
        match self {
            Self::WritePhaseContext { source, .. } => source.safe_summary(),
            Self::InvalidCompatibilityLevel { .. } => "invalid compatibility level",
            Self::UnsupportedCompatibilityLevel { .. } => {
                "compatibility level is not supported by SQL Server version"
            }
            Self::InvalidIdentifier { .. } => "invalid identifier",
            Self::Planning { .. } => "planning failed with diagnostics",
            Self::ValueConversion { .. } => "value conversion failed with diagnostics",
            Self::DirectEncoding { .. } => "direct encoding failed with diagnostics",
            Self::BackendUnavailable { .. } => "write backend unavailable",
            Self::InvalidConnectionString => "invalid connection string",
            Self::ConnectionTcpConnect { .. } => "TCP connection failed",
            Self::ConnectionClientSetup { .. } => "SQL Server client setup failed",
            Self::TableExistsQuery { .. } => "table existence query failed",
            Self::TableExistsUnexpectedResult { .. } => {
                "table existence query returned unexpected result"
            }
            Self::TargetRowCountQuery { .. } => "target row count query failed",
            Self::TargetRowCountUnexpectedResult { .. } => {
                "target row count query returned unexpected result"
            }
            Self::SqlExecution { .. } => "SQL statement execution failed",
            Self::Tiberius { .. } => "tiberius operation failed",
        }
    }

    fn diagnostics(&self) -> Option<&DiagnosticSet> {
        match self {
            Self::WritePhaseContext { source, .. } => source.diagnostics(),
            Self::Planning { diagnostics }
            | Self::ValueConversion { diagnostics }
            | Self::DirectEncoding { diagnostics } => Some(diagnostics),
            _ => None,
        }
    }
}

fn diagnostic_codes(diagnostics: &DiagnosticSet) -> String {
    let mut codes = String::new();
    for diagnostic in diagnostics.all() {
        if !codes.is_empty() {
            codes.push(',');
        }
        let _ = write!(codes, "{:?}", diagnostic.code());
    }
    codes
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
    fn safe_error_info_exposes_finalize_tiberius_cause_without_source_text() {
        let err = Error::Tiberius {
            source: tiberius::error::Error::BulkInput(Cow::Borrowed(
                "server=tcp:sql.example.com;User ID=sa;password=secret",
            )),
        }
        .with_write_phase(WritePhase::Finalize);

        let info = err.safe_error_info();

        assert_eq!(info.phase(), Some(WritePhase::Finalize));
        assert_eq!(info.kind(), "Tiberius");
        assert_eq!(info.summary(), "tiberius operation failed");
        assert!(info.diagnostics().is_none());
        assert_eq!(info.diagnostic_codes(), "");
        assert!(!info.summary().contains("password=secret"));
        assert!(!info.summary().contains("User ID=sa"));
        assert!(!info.summary().contains("sql.example.com"));
    }

    #[test]
    fn safe_error_info_exposes_nested_diagnostics() {
        let err = Error::DirectEncoding {
            diagnostics: DiagnosticSet::from(vec![Diagnostic::error(
                DiagnosticCode::DirectEncodingInvalidPayload,
                "invalid payload",
            )]),
        }
        .with_write_phase(WritePhase::WriterInitialization);

        let info = err.safe_error_info();

        assert_eq!(info.phase(), Some(WritePhase::WriterInitialization));
        assert_eq!(info.kind(), "DirectEncoding");
        assert_eq!(info.summary(), "direct encoding failed with diagnostics");
        assert_eq!(info.diagnostic_codes(), "DirectEncodingInvalidPayload");
        assert_eq!(
            info.diagnostics()
                .and_then(|diagnostics| diagnostics.all().first())
                .map(Diagnostic::code),
            Some(DiagnosticCode::DirectEncodingInvalidPayload)
        );
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

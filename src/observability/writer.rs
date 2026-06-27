//! Writer tracing helpers.

use crate::{DiagnosticCode, write::writer::WriteBackend};

mod batch;
mod direct_raw;
mod finish;
mod initialization;

pub(crate) use batch::BatchWriteTrace;
pub(crate) use direct_raw::DirectRawBatchObserver;
pub(crate) use finish::FinishTrace;
pub(crate) use initialization::WriterInitializationTrace;

use super::diagnostic_codes;

pub(super) fn backend_trace_name(backend: WriteBackend) -> &'static str {
    match backend {
        WriteBackend::Auto => "Auto",
        WriteBackend::BaselineTokenRow => "BaselineTokenRow",
        WriteBackend::DirectFramedBulk => "DirectFramedBulk",
        WriteBackend::DirectRawBulk => "DirectRawBulk",
    }
}

pub(super) fn sanitized_error_summary(error: &crate::Error) -> &'static str {
    match error {
        crate::Error::WritePhaseContext { source, .. } => sanitized_error_summary(source),
        crate::Error::InvalidCompatibilityLevel { .. } => "invalid compatibility level",
        crate::Error::InvalidIdentifier { .. } => "invalid identifier",
        crate::Error::Planning { .. } => "planning failed with diagnostics",
        crate::Error::ValueConversion { .. } => "value conversion failed with diagnostics",
        crate::Error::DirectEncoding { .. } => "direct encoding failed with diagnostics",
        crate::Error::BackendUnavailable { .. } => "write backend unavailable",
        crate::Error::InvalidConnectionString => "invalid connection string",
        crate::Error::ConnectionTcpConnect { .. } => "TCP connection failed",
        crate::Error::ConnectionClientSetup { .. } => "SQL Server client setup failed",
        crate::Error::TableExistsQuery { .. } => "table existence query failed",
        crate::Error::TableExistsUnexpectedResult { .. } => {
            "table existence query returned unexpected result"
        }
        crate::Error::TargetRowCountQuery { .. } => "target row count query failed",
        crate::Error::TargetRowCountUnexpectedResult { .. } => {
            "target row count query returned unexpected result"
        }
        crate::Error::SqlExecution { .. } => "SQL statement execution failed",
        crate::Error::Tiberius { .. } => "tiberius operation failed",
    }
}

pub(super) fn diagnostic_codes_for_error(error: &crate::Error) -> String {
    match error {
        crate::Error::WritePhaseContext { source, .. } => diagnostic_codes_for_error(source),
        crate::Error::Planning { diagnostics }
        | crate::Error::ValueConversion { diagnostics }
        | crate::Error::DirectEncoding { diagnostics } => diagnostic_codes(diagnostics),
        crate::Error::BackendUnavailable { .. } => {
            format!("{:?}", DiagnosticCode::BackendUnavailable)
        }
        _ => String::new(),
    }
}

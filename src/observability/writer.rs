//! Writer tracing helpers.

use crate::write::writer::WriteBackend;

mod batch;
mod direct_raw;
mod finish;
mod initialization;

pub(crate) use batch::BatchWriteTrace;
pub(crate) use direct_raw::DirectRawBatchObserver;
pub(crate) use finish::FinishTrace;
pub(crate) use initialization::WriterInitializationTrace;

pub(super) fn backend_trace_name(backend: WriteBackend) -> &'static str {
    match backend {
        WriteBackend::Auto => "Auto",
        WriteBackend::BaselineTokenRow => "BaselineTokenRow",
        WriteBackend::DirectFramedBulk => "DirectFramedBulk",
        WriteBackend::DirectRawBulk => "DirectRawBulk",
    }
}

pub(super) fn sanitized_error_summary(error: &crate::Error) -> &'static str {
    error.safe_error_info().summary()
}

pub(super) fn diagnostic_codes_for_error(error: &crate::Error) -> String {
    error.safe_error_info().diagnostic_codes()
}

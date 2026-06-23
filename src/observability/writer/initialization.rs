use std::time::Instant;

use crate::{
    TableName,
    observability::{
        TARGET_METADATA_VALIDATION_COMPLETED_EVENT, TARGET_METADATA_VALIDATION_FAILED_EVENT,
        TARGET_METADATA_VALIDATION_PHASE, TARGET_METADATA_VALIDATION_STARTED_EVENT, TRACE_TARGET,
        WRITER_INITIALIZATION_COMPLETED_EVENT, WRITER_INITIALIZATION_FAILED_EVENT,
        WRITER_INITIALIZATION_PHASE, WRITER_INITIALIZATION_SPAN,
        WRITER_INITIALIZATION_STARTED_EVENT, duration_micros_u64, usize_to_u64_saturating,
    },
    write::writer::WriteBackend,
};

use super::{backend_trace_name, diagnostic_codes_for_error, sanitized_error_summary};

#[derive(Debug)]
pub(crate) struct WriterInitializationTrace {
    span: tracing::Span,
    started: Instant,
    requested_backend: WriteBackend,
    resolved_backend: Option<WriteBackend>,
    target_schema: String,
    target_table: String,
    planned_column_count: usize,
    direct_target_validation_required: Option<bool>,
}

impl WriterInitializationTrace {
    pub(crate) fn new(
        table: &TableName,
        requested_backend: WriteBackend,
        planned_column_count: usize,
    ) -> Self {
        let target_schema = table
            .schema()
            .map(|schema| schema.as_str().to_owned())
            .unwrap_or_default();
        let target_table = table.table().as_str().to_owned();
        let span = tracing::info_span!(
            target: TRACE_TARGET,
            WRITER_INITIALIZATION_SPAN,
            phase = WRITER_INITIALIZATION_PHASE,
            requested_backend = backend_trace_name(requested_backend),
            resolved_backend = tracing::field::Empty,
            target_schema = target_schema.as_str(),
            target_table = target_table.as_str(),
            planned_column_count = usize_to_u64_saturating(planned_column_count),
            direct_target_validation_required = tracing::field::Empty,
        );

        Self {
            span,
            started: Instant::now(),
            requested_backend,
            resolved_backend: None,
            target_schema,
            target_table,
            planned_column_count,
            direct_target_validation_required: None,
        }
    }

    pub(crate) fn emit_started(&self) {
        let _span_guard = self.span.enter();
        tracing::info!(
            target: TRACE_TARGET,
            phase = WRITER_INITIALIZATION_PHASE,
            telemetry_event = WRITER_INITIALIZATION_STARTED_EVENT,
            requested_backend = backend_trace_name(self.requested_backend),
            target_schema = self.target_schema.as_str(),
            target_table = self.target_table.as_str(),
            planned_column_count = usize_to_u64_saturating(self.planned_column_count),
            initialization_result = "started"
        );
    }

    pub(crate) fn record_resolved_backend(&mut self, resolved_backend: WriteBackend) {
        self.span
            .record("resolved_backend", backend_trace_name(resolved_backend));
        self.resolved_backend = Some(resolved_backend);
    }

    pub(crate) fn record_direct_target_validation_required(&mut self, required: bool) {
        self.span
            .record("direct_target_validation_required", required);
        self.direct_target_validation_required = Some(required);
    }

    pub(crate) fn emit_target_metadata_validation_started(&self) {
        let _span_guard = self.span.enter();
        tracing::info!(
            target: TRACE_TARGET,
            phase = TARGET_METADATA_VALIDATION_PHASE,
            telemetry_event = TARGET_METADATA_VALIDATION_STARTED_EVENT,
            requested_backend = backend_trace_name(self.requested_backend),
            resolved_backend = self.resolved_backend_name(),
            target_schema = self.target_schema.as_str(),
            target_table = self.target_table.as_str(),
            planned_column_count = usize_to_u64_saturating(self.planned_column_count),
            direct_target_validation_required = self.direct_target_validation_required(),
            initialization_result = "validating"
        );
    }

    pub(crate) fn emit_target_metadata_validation_completed(&self) {
        let _span_guard = self.span.enter();
        tracing::info!(
            target: TRACE_TARGET,
            phase = TARGET_METADATA_VALIDATION_PHASE,
            telemetry_event = TARGET_METADATA_VALIDATION_COMPLETED_EVENT,
            requested_backend = backend_trace_name(self.requested_backend),
            resolved_backend = self.resolved_backend_name(),
            target_schema = self.target_schema.as_str(),
            target_table = self.target_table.as_str(),
            planned_column_count = usize_to_u64_saturating(self.planned_column_count),
            direct_target_validation_required = self.direct_target_validation_required(),
            initialization_result = "success",
            elapsed_us = duration_micros_u64(self.started.elapsed())
        );
    }

    pub(crate) fn emit_completed(&self) {
        let _span_guard = self.span.enter();
        tracing::info!(
            target: TRACE_TARGET,
            phase = WRITER_INITIALIZATION_PHASE,
            telemetry_event = WRITER_INITIALIZATION_COMPLETED_EVENT,
            requested_backend = backend_trace_name(self.requested_backend),
            resolved_backend = self.resolved_backend_name(),
            target_schema = self.target_schema.as_str(),
            target_table = self.target_table.as_str(),
            planned_column_count = usize_to_u64_saturating(self.planned_column_count),
            direct_target_validation_required = self.direct_target_validation_required(),
            initialization_result = "success",
            elapsed_us = duration_micros_u64(self.started.elapsed())
        );
    }

    pub(crate) fn emit_failed(&self, phase: &'static str, error: &crate::Error) {
        let telemetry_event = if phase == TARGET_METADATA_VALIDATION_PHASE {
            TARGET_METADATA_VALIDATION_FAILED_EVENT
        } else {
            WRITER_INITIALIZATION_FAILED_EVENT
        };
        let diagnostic_codes = diagnostic_codes_for_error(error);
        let _span_guard = self.span.enter();
        tracing::error!(
            target: TRACE_TARGET,
            phase,
            telemetry_event,
            requested_backend = backend_trace_name(self.requested_backend),
            resolved_backend = self.resolved_backend_name(),
            target_schema = self.target_schema.as_str(),
            target_table = self.target_table.as_str(),
            planned_column_count = usize_to_u64_saturating(self.planned_column_count),
            direct_target_validation_required = self.direct_target_validation_required(),
            initialization_result = "failure",
            error_summary = sanitized_error_summary(error),
            diagnostic_codes = diagnostic_codes.as_str(),
            elapsed_us = duration_micros_u64(self.started.elapsed())
        );
    }

    fn resolved_backend_name(&self) -> &'static str {
        self.resolved_backend
            .map(backend_trace_name)
            .unwrap_or("unresolved")
    }

    fn direct_target_validation_required(&self) -> bool {
        self.direct_target_validation_required.unwrap_or(false)
    }
}

#[cfg(test)]
mod tests {
    use std::borrow::Cow;

    use super::WriterInitializationTrace;
    use crate::{
        Diagnostic, DiagnosticCode, DiagnosticSet, Error, TableName,
        observability::{
            TARGET_METADATA_VALIDATION_COMPLETED_EVENT, TARGET_METADATA_VALIDATION_FAILED_EVENT,
            TARGET_METADATA_VALIDATION_PHASE, WRITER_INITIALIZATION_COMPLETED_EVENT,
            WRITER_INITIALIZATION_PHASE, WRITER_INITIALIZATION_SPAN,
            test_support::{assert_trace_field, capture_traces, trace_event},
        },
        write::writer::WriteBackend,
    };

    #[test]
    fn writer_initialization_trace_records_auto_backend_resolution() -> Result<(), String> {
        let table = TableName::new("dbo", "target").unwrap();

        let (_result, traces) = capture_traces(|| {
            let mut trace = WriterInitializationTrace::new(&table, WriteBackend::Auto, 1);
            trace.emit_started();
            trace.record_resolved_backend(WriteBackend::DirectRawBulk);
            trace.record_direct_target_validation_required(true);
            trace.emit_completed();
        });

        let records = traces.records()?;
        let event = trace_event(&records, WRITER_INITIALIZATION_COMPLETED_EVENT)?;
        assert_eq!(event.span_name(), Some(WRITER_INITIALIZATION_SPAN));
        assert_trace_field(event, "phase", WRITER_INITIALIZATION_PHASE);
        assert_trace_field(event, "requested_backend", "Auto");
        assert_trace_field(event, "resolved_backend", "DirectRawBulk");
        assert_trace_field(event, "target_schema", "dbo");
        assert_trace_field(event, "target_table", "target");
        assert_trace_field(event, "planned_column_count", "1");
        assert_trace_field(event, "direct_target_validation_required", "true");
        assert_trace_field(event, "initialization_result", "success");
        assert!(event.fields().contains_key("elapsed_us"));

        Ok(())
    }

    #[test]
    fn writer_initialization_trace_records_explicit_backend_resolution() -> Result<(), String> {
        for backend in [
            WriteBackend::BaselineTokenRow,
            WriteBackend::DirectFramedBulk,
            WriteBackend::DirectRawBulk,
        ] {
            let table = TableName::new("dbo", "target").unwrap();
            let (_result, traces) = capture_traces(|| {
                let mut trace = WriterInitializationTrace::new(&table, backend, 1);
                trace.emit_started();
                trace.record_resolved_backend(backend);
                trace.record_direct_target_validation_required(matches!(
                    backend,
                    WriteBackend::DirectFramedBulk | WriteBackend::DirectRawBulk
                ));
                trace.emit_completed();
            });

            let records = traces.records()?;
            let event = trace_event(&records, WRITER_INITIALIZATION_COMPLETED_EVENT)?;
            let backend_name = match backend {
                WriteBackend::BaselineTokenRow => "BaselineTokenRow",
                WriteBackend::DirectFramedBulk => "DirectFramedBulk",
                WriteBackend::DirectRawBulk => "DirectRawBulk",
                WriteBackend::Auto => "Auto",
            };
            assert_trace_field(event, "requested_backend", backend_name);
            assert_trace_field(event, "resolved_backend", backend_name);
        }

        Ok(())
    }

    #[test]
    fn target_metadata_validation_trace_records_direct_success() -> Result<(), String> {
        let table = TableName::new("dbo", "target").unwrap();

        let (_result, traces) = capture_traces(|| {
            let mut trace = WriterInitializationTrace::new(&table, WriteBackend::DirectRawBulk, 1);
            trace.record_resolved_backend(WriteBackend::DirectRawBulk);
            trace.record_direct_target_validation_required(true);
            trace.emit_target_metadata_validation_started();
            trace.emit_target_metadata_validation_completed();
        });

        let records = traces.records()?;
        let event = trace_event(&records, TARGET_METADATA_VALIDATION_COMPLETED_EVENT)?;
        assert_trace_field(event, "phase", TARGET_METADATA_VALIDATION_PHASE);
        assert_trace_field(event, "requested_backend", "DirectRawBulk");
        assert_trace_field(event, "resolved_backend", "DirectRawBulk");
        assert_trace_field(event, "direct_target_validation_required", "true");
        assert_trace_field(event, "initialization_result", "success");

        Ok(())
    }

    #[test]
    fn target_metadata_validation_failure_trace_is_sanitized() -> Result<(), String> {
        let table = TableName::new("dbo", "target").unwrap();
        let diagnostics = DiagnosticSet::from(vec![Diagnostic::error(
            DiagnosticCode::SchemaMismatch,
            "schema mismatch",
        )]);
        let error = Error::ValueConversion { diagnostics };

        let (_result, traces) = capture_traces(|| {
            let mut trace = WriterInitializationTrace::new(&table, WriteBackend::DirectRawBulk, 1);
            trace.record_resolved_backend(WriteBackend::DirectRawBulk);
            trace.record_direct_target_validation_required(true);
            trace.emit_failed(TARGET_METADATA_VALIDATION_PHASE, &error);
        });

        let records = traces.records()?;
        let event = trace_event(&records, TARGET_METADATA_VALIDATION_FAILED_EVENT)?;
        assert_trace_field(event, "phase", TARGET_METADATA_VALIDATION_PHASE);
        assert_trace_field(
            event,
            "error_summary",
            "value conversion failed with diagnostics",
        );
        assert_trace_field(event, "diagnostic_codes", "SchemaMismatch");
        traces.assert_no_forbidden_text(&[
            "server=tcp:sql.example.com",
            "password=secret",
            "User ID=sa",
        ])?;

        let secret_error = Error::Tiberius {
            source: tiberius::error::Error::BulkInput(Cow::Borrowed(
                "server=tcp:sql.example.com;User ID=sa;password=secret",
            )),
        };
        let (_result, tiberius_traces) = capture_traces(|| {
            let trace = WriterInitializationTrace::new(&table, WriteBackend::DirectRawBulk, 1);
            trace.emit_failed(TARGET_METADATA_VALIDATION_PHASE, &secret_error);
        });
        tiberius_traces.assert_no_forbidden_text(&[
            "server=tcp:sql.example.com",
            "password=secret",
            "User ID=sa",
        ])?;

        Ok(())
    }
}

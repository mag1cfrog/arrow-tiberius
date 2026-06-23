//! Baseline bulk writer public API skeleton.

use std::{
    borrow::Cow,
    fmt::Write as _,
    time::{Duration, Instant},
};

use arrow_array::RecordBatch;
use futures_util::io::{AsyncRead, AsyncWrite};

use crate::observability::{
    BATCH_SCHEMA_VALIDATION_PHASE, BATCH_WRITE_COMPLETED_EVENT, BATCH_WRITE_FAILED_EVENT,
    BATCH_WRITE_PHASE, BATCH_WRITE_SPAN, BATCH_WRITE_STARTED_EVENT, DIRECT_ENCODING_PHASE,
    DIRECT_RAW_FAILED_EVENT, DIRECT_RAW_MEASURED_EVENT, DIRECT_RAW_PACKET_WRITE_COMPLETED_EVENT,
    DIRECT_RAW_RANGES_PLANNED_EVENT, FINALIZE_PHASE, FINISH_COMPLETED_EVENT, FINISH_FAILED_EVENT,
    FINISH_PHASE, FINISH_SPAN, FINISH_STARTED_EVENT, PACKET_WRITE_PHASE,
    TARGET_METADATA_VALIDATION_COMPLETED_EVENT, TARGET_METADATA_VALIDATION_FAILED_EVENT,
    TARGET_METADATA_VALIDATION_PHASE, TARGET_METADATA_VALIDATION_STARTED_EVENT, TRACE_TARGET,
    VALUE_CONVERSION_PHASE, WRITER_INITIALIZATION_COMPLETED_EVENT,
    WRITER_INITIALIZATION_FAILED_EVENT, WRITER_INITIALIZATION_PHASE, WRITER_INITIALIZATION_SPAN,
    WRITER_INITIALIZATION_STARTED_EVENT,
};
use crate::{
    Diagnostic, DiagnosticCode, DiagnosticSet, FieldRef, PlanOptions, Result, SchemaMapping,
    TableName, WritePhase,
};

use super::{
    SchemaCheck,
    direct::{
        DirectEncoder, MeasuredDirectBatch, MeasuredRowRange,
        plan::{DirectColumnEncoding, DirectColumnPlan, DirectEncoderPlan},
    },
    profile,
    record_batch::RecordBatchView,
    token_row::tiberius_row_owned,
};
use crate::conversion::arrow_to_mssql::{
    fixed_size_binary::FixedSizeBinaryArrowToMssql, primitive::PrimitiveArrowToMssql,
    temporal::TemporalArrowToMssql, variable_width::VariableWidthArrowToMssql,
};

const DIRECT_RAW_MAX_PAYLOAD_BYTES: usize = 8 * 1024 * 1024;

/// Write backend selection.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub enum WriteBackend {
    /// Select the best available backend for the current crate build and plan.
    #[default]
    Auto,
    /// Use Tiberius' row-oriented `TokenRow` bulk-load path.
    BaselineTokenRow,
    /// Use direct bulk-row payload encoding through Tiberius' framed sink.
    DirectFramedBulk,
    /// Use the raw bulk-row payload path exposed by the Tiberius fork.
    DirectRawBulk,
}

/// Execution-time write options.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub struct WriteOptions {
    /// Requested write backend.
    pub backend: WriteBackend,
    /// Batch schema validation policy.
    pub schema_check: SchemaCheck,
    /// Planning/runtime conversion policies used by policy-dependent write conversions.
    pub plan_options: PlanOptions,
}

/// Cumulative write statistics.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash)]
pub struct WriteStats {
    /// Number of rows accepted by the writer.
    pub rows_written: u64,
    /// Number of batches accepted by the writer.
    pub batches_written: u64,
}

#[derive(Debug)]
struct WriterInitializationTrace {
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
    fn new(
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

    fn emit_started(&self) {
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

    fn record_resolved_backend(&mut self, resolved_backend: WriteBackend) {
        self.span
            .record("resolved_backend", backend_trace_name(resolved_backend));
        self.resolved_backend = Some(resolved_backend);
    }

    fn record_direct_target_validation_required(&mut self, required: bool) {
        self.span
            .record("direct_target_validation_required", required);
        self.direct_target_validation_required = Some(required);
    }

    fn emit_target_metadata_validation_started(&self) {
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

    fn emit_target_metadata_validation_completed(&self) {
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

    fn emit_completed(&self) {
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

    fn emit_failed(&self, phase: &'static str, error: &crate::Error) {
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

#[derive(Debug)]
struct BatchWriteTrace {
    span: tracing::Span,
    started: Instant,
    backend: WriteBackend,
    attempted_batch_ordinal: u64,
    batch_row_count: u64,
    batch_column_count: u64,
    batch_is_empty: bool,
    accepted_rows_before: u64,
    accepted_batches_before: u64,
}

impl BatchWriteTrace {
    fn new(state: &WriterState, batch: &RecordBatch) -> Self {
        let stats = state.stats();
        let attempted_batch_ordinal = stats.batches_written.saturating_add(1);
        let batch_row_count = usize_to_u64_saturating(batch.num_rows());
        let batch_column_count = usize_to_u64_saturating(batch.num_columns());
        let batch_is_empty = batch.num_rows() == 0;
        let span = tracing::info_span!(
            target: TRACE_TARGET,
            BATCH_WRITE_SPAN,
            phase = BATCH_WRITE_PHASE,
            backend = backend_trace_name(state.backend()),
            attempted_batch_ordinal,
            batch_row_count,
            batch_column_count,
            batch_is_empty,
            accepted_rows_before = stats.rows_written,
            accepted_batches_before = stats.batches_written,
        );

        Self {
            span,
            started: Instant::now(),
            backend: state.backend(),
            attempted_batch_ordinal,
            batch_row_count,
            batch_column_count,
            batch_is_empty,
            accepted_rows_before: stats.rows_written,
            accepted_batches_before: stats.batches_written,
        }
    }

    fn emit_started(&self) {
        let _span_guard = self.span.enter();
        tracing::info!(
            target: TRACE_TARGET,
            phase = BATCH_WRITE_PHASE,
            telemetry_event = BATCH_WRITE_STARTED_EVENT,
            backend = backend_trace_name(self.backend),
            attempted_batch_ordinal = self.attempted_batch_ordinal,
            batch_row_count = self.batch_row_count,
            batch_column_count = self.batch_column_count,
            batch_is_empty = self.batch_is_empty,
            accepted_rows_before = self.accepted_rows_before,
            accepted_batches_before = self.accepted_batches_before,
            batch_write_result = "started"
        );
    }

    fn emit_completed(&self, stats: WriteStats) {
        let _span_guard = self.span.enter();
        tracing::info!(
            target: TRACE_TARGET,
            phase = BATCH_WRITE_PHASE,
            telemetry_event = BATCH_WRITE_COMPLETED_EVENT,
            backend = backend_trace_name(self.backend),
            attempted_batch_ordinal = self.attempted_batch_ordinal,
            batch_row_count = self.batch_row_count,
            batch_column_count = self.batch_column_count,
            batch_is_empty = self.batch_is_empty,
            accepted_rows_before = self.accepted_rows_before,
            accepted_batches_before = self.accepted_batches_before,
            accepted_rows_after = stats.rows_written,
            accepted_batches_after = stats.batches_written,
            batch_write_result = "success",
            elapsed_us = duration_micros_u64(self.started.elapsed())
        );
    }

    fn emit_failed(&self, phase: &'static str, error: &crate::Error) {
        let diagnostic_codes = diagnostic_codes_for_error(error);
        let _span_guard = self.span.enter();
        tracing::error!(
            target: TRACE_TARGET,
            phase,
            telemetry_event = BATCH_WRITE_FAILED_EVENT,
            backend = backend_trace_name(self.backend),
            attempted_batch_ordinal = self.attempted_batch_ordinal,
            batch_row_count = self.batch_row_count,
            batch_column_count = self.batch_column_count,
            batch_is_empty = self.batch_is_empty,
            accepted_rows_before = self.accepted_rows_before,
            accepted_batches_before = self.accepted_batches_before,
            batch_write_result = "failure",
            error_summary = sanitized_error_summary(error),
            diagnostic_codes = diagnostic_codes.as_str(),
            elapsed_us = duration_micros_u64(self.started.elapsed())
        );
    }
}

#[derive(Debug)]
struct FinishTrace {
    span: tracing::Span,
    started: Instant,
    backend: WriteBackend,
    stats: WriteStats,
}

impl FinishTrace {
    fn new(state: &WriterState) -> Self {
        let stats = state.stats();
        let span = tracing::info_span!(
            target: TRACE_TARGET,
            FINISH_SPAN,
            phase = FINISH_PHASE,
            backend = backend_trace_name(state.backend()),
            rows_written = stats.rows_written,
            batches_written = stats.batches_written,
        );

        Self {
            span,
            started: Instant::now(),
            backend: state.backend(),
            stats,
        }
    }

    fn emit_started(&self) {
        let _span_guard = self.span.enter();
        tracing::info!(
            target: TRACE_TARGET,
            phase = FINISH_PHASE,
            telemetry_event = FINISH_STARTED_EVENT,
            backend = backend_trace_name(self.backend),
            rows_written = self.stats.rows_written,
            batches_written = self.stats.batches_written,
            finish_result = "started"
        );
    }

    fn emit_completed(&self) {
        let _span_guard = self.span.enter();
        tracing::info!(
            target: TRACE_TARGET,
            phase = FINISH_PHASE,
            telemetry_event = FINISH_COMPLETED_EVENT,
            backend = backend_trace_name(self.backend),
            rows_written = self.stats.rows_written,
            batches_written = self.stats.batches_written,
            finish_result = "success",
            elapsed_us = duration_micros_u64(self.started.elapsed())
        );
    }

    fn emit_failed(&self, error: &crate::Error) {
        let diagnostic_codes = diagnostic_codes_for_error(error);
        let _span_guard = self.span.enter();
        tracing::error!(
            target: TRACE_TARGET,
            phase = FINALIZE_PHASE,
            telemetry_event = FINISH_FAILED_EVENT,
            backend = backend_trace_name(self.backend),
            accepted_rows_before = self.stats.rows_written,
            accepted_batches_before = self.stats.batches_written,
            finish_result = "failure",
            error_summary = sanitized_error_summary(error),
            diagnostic_codes = diagnostic_codes.as_str(),
            elapsed_us = duration_micros_u64(self.started.elapsed())
        );
    }
}

#[derive(Debug)]
struct WriterState {
    backend: WriteBackend,
    direct_encoder: Option<DirectEncoder>,
    schema_check: SchemaCheck,
    plan_options: PlanOptions,
    mappings: Vec<SchemaMapping>,
    stats: WriteStats,
}

impl WriterState {
    fn new(
        requested_backend: WriteBackend,
        schema_check: SchemaCheck,
        plan_options: PlanOptions,
        mappings: Vec<SchemaMapping>,
    ) -> Result<Self> {
        let backend = resolve_backend(requested_backend)?;
        let direct_encoder = match backend {
            WriteBackend::DirectFramedBulk | WriteBackend::DirectRawBulk => {
                Some(DirectEncoder::new_with_options(&mappings, plan_options)?)
            }
            WriteBackend::Auto | WriteBackend::BaselineTokenRow => None,
        };

        Ok(Self {
            backend,
            direct_encoder,
            schema_check,
            plan_options,
            mappings,
            stats: WriteStats::default(),
        })
    }

    fn backend(&self) -> WriteBackend {
        self.backend
    }

    fn direct_encoder(&self) -> Option<&DirectEncoder> {
        self.direct_encoder.as_ref()
    }

    fn mappings(&self) -> &[SchemaMapping] {
        &self.mappings
    }

    fn schema_check(&self) -> SchemaCheck {
        self.schema_check
    }

    fn plan_options(&self) -> &PlanOptions {
        &self.plan_options
    }

    fn stats(&self) -> WriteStats {
        self.stats
    }

    fn record_accepted_batch(&mut self, rows: u64) -> WriteStats {
        self.stats.rows_written = self.stats.rows_written.saturating_add(rows);
        self.stats.batches_written = self.stats.batches_written.saturating_add(1);
        self.stats
    }
}

/// SQL Server bulk writer for Arrow record batches.
#[derive(Debug)]
pub struct BulkWriter<'client, S>
where
    S: AsyncRead + AsyncWrite + Unpin + Send,
{
    state: WriterState,
    request: tiberius::BulkLoadRequest<'client, S>,
}

impl<'client, S> BulkWriter<'client, S>
where
    S: AsyncRead + AsyncWrite + Unpin + Send,
{
    /// Starts a bulk writer for a planned SQL Server table target.
    pub async fn new(
        client: &'client mut tiberius::Client<S>,
        table: TableName,
        mappings: Vec<SchemaMapping>,
        options: WriteOptions,
    ) -> Result<Self> {
        let mut trace = WriterInitializationTrace::new(&table, options.backend, mappings.len());
        trace.emit_started();

        let state = match WriterState::new(
            options.backend,
            options.schema_check,
            options.plan_options,
            mappings,
        ) {
            Ok(state) => state,
            Err(err) => {
                trace.emit_failed(WRITER_INITIALIZATION_PHASE, &err);
                return Err(err.with_write_phase(WritePhase::WriterInitialization));
            }
        };
        trace.record_resolved_backend(state.backend());
        trace.record_direct_target_validation_required(matches!(
            state.backend(),
            WriteBackend::DirectFramedBulk | WriteBackend::DirectRawBulk
        ));

        let mut request = match state.backend() {
            WriteBackend::BaselineTokenRow
            | WriteBackend::DirectFramedBulk
            | WriteBackend::DirectRawBulk => {
                let table_sql = bulk_insert_table_sql(&table);
                let columns = match client
                    .bulk_insert_columns(&table_sql)
                    .await
                    .map_err(|source| crate::Error::Tiberius { source })
                {
                    Ok(columns) => columns,
                    Err(err) => {
                        trace.emit_failed(TARGET_METADATA_VALIDATION_PHASE, &err);
                        return Err(err.with_write_phase(WritePhase::TargetMetadataValidation));
                    }
                };
                trace.emit_target_metadata_validation_started();
                if let Err(err) = validate_bulk_target_columns(columns.iter(), state.mappings()) {
                    trace.emit_failed(TARGET_METADATA_VALIDATION_PHASE, &err);
                    return Err(err.with_write_phase(WritePhase::TargetMetadataValidation));
                }
                if matches!(
                    state.backend(),
                    WriteBackend::DirectFramedBulk | WriteBackend::DirectRawBulk
                ) {
                    let encoder = match state.direct_encoder().ok_or_else(|| {
                        crate::Error::BackendUnavailable {
                            backend: state.backend(),
                            reason: "direct bulk encoder is not available for this writer"
                                .to_owned(),
                        }
                    }) {
                        Ok(encoder) => encoder,
                        Err(err) => {
                            trace.emit_failed(TARGET_METADATA_VALIDATION_PHASE, &err);
                            return Err(err.with_write_phase(WritePhase::TargetMetadataValidation));
                        }
                    };
                    if let Err(err) =
                        validate_direct_bulk_target_column_types(columns.iter(), encoder.plan())
                    {
                        trace.emit_failed(TARGET_METADATA_VALIDATION_PHASE, &err);
                        return Err(err.with_write_phase(WritePhase::TargetMetadataValidation));
                    }
                }
                trace.emit_target_metadata_validation_completed();
                match client
                    .bulk_insert_with_columns(&table_sql, columns)
                    .await
                    .map_err(|source| crate::Error::Tiberius { source })
                {
                    Ok(request) => request,
                    Err(err) => {
                        trace.emit_failed(WRITER_INITIALIZATION_PHASE, &err);
                        return Err(err.with_write_phase(WritePhase::WriterInitialization));
                    }
                }
            }
            WriteBackend::Auto => {
                let err = execution_unavailable(state.backend());
                trace.emit_failed(WRITER_INITIALIZATION_PHASE, &err);
                return Err(err.with_write_phase(WritePhase::WriterInitialization));
            }
        };

        if state.backend() == WriteBackend::DirectRawBulk {
            request.enable_direct_packet_writes();
        }

        trace.emit_completed();

        Ok(Self { state, request })
    }

    /// Writes one Arrow record batch.
    pub async fn write_batch(&mut self, batch: &RecordBatch) -> Result<WriteStats> {
        match self.state.backend() {
            WriteBackend::BaselineTokenRow => {
                write_batch_to_sink(&mut self.state, &mut self.request, batch).await
            }
            WriteBackend::DirectFramedBulk | WriteBackend::DirectRawBulk => {
                write_direct_batch_to_sink(&mut self.state, &mut self.request, batch).await
            }
            WriteBackend::Auto => Err(execution_unavailable(WriteBackend::Auto)),
        }
    }

    /// Finalizes the bulk writer and returns cumulative write statistics.
    pub async fn finish(self) -> Result<WriteStats> {
        let Self { state, request } = self;
        finish_writer_to_sink(state, request).await
    }
}

async fn finish_writer_to_sink<Sink>(state: WriterState, sink: Sink) -> Result<WriteStats>
where
    Sink: FinishSink,
{
    let trace = FinishTrace::new(&state);
    trace.emit_started();
    let stats = state.stats();

    if let Err(err) = sink.finalize_bulk_load().await {
        trace.emit_failed(&err);
        return Err(err.with_write_phase(WritePhase::Finalize));
    }

    trace.emit_completed();
    Ok(stats)
}

trait FinishSink {
    async fn finalize_bulk_load(self) -> Result<()>;
}

impl<S> FinishSink for tiberius::BulkLoadRequest<'_, S>
where
    S: AsyncRead + AsyncWrite + Unpin + Send,
{
    async fn finalize_bulk_load(self) -> Result<()> {
        #[cfg(feature = "bench-profile")]
        {
            let (_result, stats) = self
                .finalize_with_stats()
                .await
                .map_err(|source| crate::Error::Tiberius { source })?;
            profile::record_bulk_load_stats(stats);
        }

        #[cfg(not(feature = "bench-profile"))]
        self.finalize()
            .await
            .map_err(|source| crate::Error::Tiberius { source })?;

        Ok(())
    }
}

fn bulk_insert_table_sql(table: &TableName) -> String {
    table.quoted_sql()
}

fn record_batch_view<'a>(
    batch: &'a RecordBatch,
    mappings: &'a [SchemaMapping],
    schema_check: SchemaCheck,
    plan_options: &PlanOptions,
) -> Result<RecordBatchView<'a>> {
    match schema_check {
        SchemaCheck::Strict => RecordBatchView::new_with_options(batch, mappings, plan_options),
    }
}

fn validate_batch_rows(view: &RecordBatchView<'_>) -> Result<()> {
    for row_index in 0..view.row_count() {
        let _cells = view.mssql_row(row_index)?;
    }

    Ok(())
}

fn validate_bulk_target_columns<Column>(
    columns: impl ExactSizeIterator<Item = Column>,
    mappings: &[SchemaMapping],
) -> Result<()>
where
    Column: BulkTargetColumnMetadata,
{
    let column_count = columns.len();
    let mut diagnostics = DiagnosticSet::new();

    if column_count != mappings.len() {
        diagnostics.push(Diagnostic::error(
            DiagnosticCode::SchemaMismatch,
            format!(
                "bulk target has {column_count} updateable column(s) but mappings contain {} column(s)",
                mappings.len()
            ),
        ));
    }

    for (position, (column, mapping)) in columns.zip(mappings).enumerate() {
        validate_bulk_target_column(position, column, mapping, &mut diagnostics);
    }

    if diagnostics.has_errors() {
        return Err(crate::Error::ValueConversion { diagnostics });
    }

    Ok(())
}

fn validate_bulk_target_column(
    position: usize,
    column: impl BulkTargetColumnMetadata,
    mapping: &SchemaMapping,
    diagnostics: &mut DiagnosticSet,
) {
    if column.ordinal() != position {
        diagnostics.push(bulk_target_column_diagnostic(
            mapping,
            format!(
                "bulk target column ordinal {} does not match mapping position {position}",
                column.ordinal()
            ),
        ));
    }

    if column.name() != mapping.mssql().name().as_str() {
        diagnostics.push(bulk_target_column_diagnostic(
            mapping,
            format!(
                "bulk target column name {} does not match planned MSSQL column name {}",
                column.name(),
                mapping.mssql().name().as_str()
            ),
        ));
    }

    if column.is_nullable() != mapping.mssql().nullable() {
        diagnostics.push(bulk_target_column_diagnostic(
            mapping,
            format!(
                "bulk target column nullability {} does not match planned MSSQL column nullability {}",
                column.is_nullable(),
                mapping.mssql().nullable()
            ),
        ));
    }
}

fn validate_direct_bulk_target_column_types<Column>(
    columns: impl ExactSizeIterator<Item = Column>,
    plan: &DirectEncoderPlan,
) -> Result<()>
where
    Column: BulkTargetColumnMetadata,
{
    let column_count = columns.len();
    let mut diagnostics = DiagnosticSet::new();

    if column_count != plan.column_count() {
        diagnostics.push(Diagnostic::error(
            DiagnosticCode::SchemaMismatch,
            format!(
                "bulk target has {column_count} updateable column(s) but direct plan contains {} column(s)",
                plan.column_count()
            ),
        ));
    }

    for (column, plan_column) in columns.zip(plan.columns()) {
        validate_direct_bulk_target_column_type(column, plan_column, &mut diagnostics);
    }

    if diagnostics.has_errors() {
        return Err(crate::Error::ValueConversion { diagnostics });
    }

    Ok(())
}

fn validate_direct_bulk_target_column_type(
    column: impl BulkTargetColumnMetadata,
    plan_column: &DirectColumnPlan,
    diagnostics: &mut DiagnosticSet,
) {
    let Some(expected) = expected_direct_bulk_column_type(plan_column) else {
        diagnostics.push(
            Diagnostic::error(
                DiagnosticCode::DirectEncodingUnsupportedMapping,
                format!(
                    "direct target type validation is not implemented for {:?}",
                    plan_column.encoding()
                ),
            )
            .with_field(FieldRef::new(
                plan_column.source_index(),
                plan_column.source_name(),
            )),
        );
        return;
    };
    let actual = column.column_type();

    if actual != expected {
        diagnostics.push(
            Diagnostic::error(
                DiagnosticCode::SchemaMismatch,
                format!(
                    "bulk target column type {actual:?} does not match direct encoder type {expected:?}"
                ),
            )
            .with_field(FieldRef::new(
                plan_column.source_index(),
                plan_column.source_name(),
            )),
        );
    }

    if let Some((expected_precision, expected_scale)) =
        expected_direct_decimal_precision_scale(plan_column)
    {
        match column.decimal_precision_scale() {
            Some((actual_precision, actual_scale))
                if actual_precision == expected_precision && actual_scale == expected_scale => {}
            Some((actual_precision, actual_scale)) => diagnostics.push(
                Diagnostic::error(
                    DiagnosticCode::SchemaMismatch,
                    format!(
                        "bulk target decimal precision/scale ({actual_precision},{actual_scale}) does not match direct encoder precision/scale ({expected_precision},{expected_scale})"
                    ),
                )
                .with_field(FieldRef::new(
                    plan_column.source_index(),
                    plan_column.source_name(),
                )),
            ),
            None => diagnostics.push(
                Diagnostic::error(
                    DiagnosticCode::SchemaMismatch,
                    "bulk target decimal precision/scale metadata is not available",
                )
                .with_field(FieldRef::new(
                    plan_column.source_index(),
                    plan_column.source_name(),
                )),
            ),
        }
    }
}

fn expected_direct_bulk_column_type(column: &DirectColumnPlan) -> Option<tiberius::ColumnType> {
    match column.encoding() {
        DirectColumnEncoding::Primitive(PrimitiveArrowToMssql::BooleanToBit) => {
            if column.nullable() {
                Some(tiberius::ColumnType::Bitn)
            } else {
                Some(tiberius::ColumnType::Bit)
            }
        }
        DirectColumnEncoding::Primitive(PrimitiveArrowToMssql::UInt8ToTinyInt) => {
            Some(tiberius::ColumnType::Int1)
        }
        DirectColumnEncoding::Primitive(
            PrimitiveArrowToMssql::Int8ToSmallInt | PrimitiveArrowToMssql::Int16ToSmallInt,
        ) => Some(tiberius::ColumnType::Int2),
        DirectColumnEncoding::Primitive(PrimitiveArrowToMssql::Int32ToInt) => {
            Some(tiberius::ColumnType::Int4)
        }
        DirectColumnEncoding::Primitive(PrimitiveArrowToMssql::UInt16ToInt) => {
            Some(tiberius::ColumnType::Int4)
        }
        DirectColumnEncoding::Primitive(PrimitiveArrowToMssql::Int64ToBigInt) => {
            Some(tiberius::ColumnType::Int8)
        }
        DirectColumnEncoding::Primitive(PrimitiveArrowToMssql::UInt32ToBigInt) => {
            Some(tiberius::ColumnType::Int8)
        }
        DirectColumnEncoding::Primitive(PrimitiveArrowToMssql::UInt64ToCheckedBigInt) => {
            Some(tiberius::ColumnType::Int8)
        }
        DirectColumnEncoding::Primitive(
            PrimitiveArrowToMssql::Float16ToReal | PrimitiveArrowToMssql::Float32ToReal,
        ) => Some(tiberius::ColumnType::Float4),
        DirectColumnEncoding::Primitive(PrimitiveArrowToMssql::Float64ToFloat) => {
            Some(tiberius::ColumnType::Float8)
        }
        DirectColumnEncoding::UInt64Decimal20_0 | DirectColumnEncoding::Decimal(_) => {
            Some(tiberius::ColumnType::Decimaln)
        }
        DirectColumnEncoding::VariableWidth(
            VariableWidthArrowToMssql::Utf8ToNVarChar { .. }
            | VariableWidthArrowToMssql::LargeUtf8ToNVarChar { .. },
        ) => Some(tiberius::ColumnType::NVarchar),
        DirectColumnEncoding::VariableWidth(
            VariableWidthArrowToMssql::BinaryToVarBinary { .. }
            | VariableWidthArrowToMssql::LargeBinaryToVarBinary { .. },
        ) => Some(tiberius::ColumnType::BigVarBin),
        DirectColumnEncoding::FixedSizeBinary(
            FixedSizeBinaryArrowToMssql::FixedSizeBinaryToBinary { .. },
        ) => Some(tiberius::ColumnType::BigBinary),
        DirectColumnEncoding::Temporal(TemporalArrowToMssql::Date32ToDate) => {
            Some(tiberius::ColumnType::Daten)
        }
        DirectColumnEncoding::Temporal(TemporalArrowToMssql::Date64ToDateTime2) => {
            Some(tiberius::ColumnType::Datetime2)
        }
        DirectColumnEncoding::Temporal(
            TemporalArrowToMssql::TimestampSecondToDateTime2
            | TemporalArrowToMssql::TimestampMillisecondToDateTime2
            | TemporalArrowToMssql::TimestampMicrosecondToDateTime2
            | TemporalArrowToMssql::TimestampNanosecondToDateTime2
            | TemporalArrowToMssql::TimestampSecondTzToDateTime2
            | TemporalArrowToMssql::TimestampMillisecondTzToDateTime2
            | TemporalArrowToMssql::TimestampMicrosecondTzToDateTime2
            | TemporalArrowToMssql::TimestampNanosecondTzToDateTime2,
        ) => Some(tiberius::ColumnType::Datetime2),
        DirectColumnEncoding::Temporal(
            TemporalArrowToMssql::Time32SecondToTime
            | TemporalArrowToMssql::Time32MillisecondToTime
            | TemporalArrowToMssql::Time64MicrosecondToTime
            | TemporalArrowToMssql::Time64NanosecondToTime,
        ) => Some(tiberius::ColumnType::Timen),
        DirectColumnEncoding::Temporal(
            TemporalArrowToMssql::TimestampSecondTzToDateTimeOffset
            | TemporalArrowToMssql::TimestampMillisecondTzToDateTimeOffset
            | TemporalArrowToMssql::TimestampMicrosecondTzToDateTimeOffset
            | TemporalArrowToMssql::TimestampNanosecondTzToDateTimeOffset,
        ) => Some(tiberius::ColumnType::DatetimeOffsetn),
    }
}

fn expected_direct_decimal_precision_scale(column: &DirectColumnPlan) -> Option<(u8, u8)> {
    match column.encoding() {
        DirectColumnEncoding::UInt64Decimal20_0 => Some((20, 0)),
        DirectColumnEncoding::Decimal(classification) => Some((
            classification.target_precision(),
            classification.target_scale(),
        )),
        _ => None,
    }
}

fn bulk_target_column_diagnostic(
    mapping: &SchemaMapping,
    message: impl Into<String>,
) -> Diagnostic {
    Diagnostic::error(DiagnosticCode::SchemaMismatch, message).with_field(FieldRef::new(
        mapping.arrow().index(),
        mapping.arrow().name(),
    ))
}

trait BulkTargetColumnMetadata {
    fn ordinal(&self) -> usize;

    fn name(&self) -> &str;

    fn is_nullable(&self) -> bool;

    fn column_type(&self) -> tiberius::ColumnType;

    fn decimal_precision_scale(&self) -> Option<(u8, u8)> {
        None
    }
}

impl<T> BulkTargetColumnMetadata for &T
where
    T: BulkTargetColumnMetadata + ?Sized,
{
    fn ordinal(&self) -> usize {
        (*self).ordinal()
    }

    fn name(&self) -> &str {
        (*self).name()
    }

    fn is_nullable(&self) -> bool {
        (*self).is_nullable()
    }

    fn column_type(&self) -> tiberius::ColumnType {
        (*self).column_type()
    }

    fn decimal_precision_scale(&self) -> Option<(u8, u8)> {
        (*self).decimal_precision_scale()
    }
}

impl BulkTargetColumnMetadata for tiberius::BulkLoadColumn<'_> {
    fn ordinal(&self) -> usize {
        self.ordinal()
    }

    fn name(&self) -> &str {
        self.name()
    }

    fn is_nullable(&self) -> bool {
        self.is_nullable()
    }

    fn column_type(&self) -> tiberius::ColumnType {
        self.column_type()
    }

    fn decimal_precision_scale(&self) -> Option<(u8, u8)> {
        match self.type_info() {
            tiberius::TypeInfo::VarLenSizedPrecision {
                ty: tiberius::VarLenType::Decimaln | tiberius::VarLenType::Numericn,
                precision,
                scale,
                ..
            } => Some((*precision, *scale)),
            _ => None,
        }
    }
}

async fn write_batch_to_sink<Sink>(
    state: &mut WriterState,
    sink: &mut Sink,
    batch: &RecordBatch,
) -> Result<WriteStats>
where
    Sink: TokenRowSink,
{
    let trace = BatchWriteTrace::new(state, batch);
    trace.emit_started();

    let view = match record_batch_view(
        batch,
        state.mappings(),
        state.schema_check(),
        state.plan_options(),
    ) {
        Ok(view) => view,
        Err(err) => {
            trace.emit_failed(BATCH_SCHEMA_VALIDATION_PHASE, &err);
            return Err(err.with_write_phase(WritePhase::BatchSchemaValidation));
        }
    };
    if let Err(err) = validate_batch_rows(&view) {
        trace.emit_failed(VALUE_CONVERSION_PHASE, &err);
        return Err(err.with_write_phase(WritePhase::ValueConversion));
    }
    let rows_written = usize_to_u64_saturating(view.row_count());

    for row_index in 0..view.row_count() {
        let row = match tiberius_row_owned(&view, row_index) {
            Ok(row) => row,
            Err(err) => {
                trace.emit_failed(VALUE_CONVERSION_PHASE, &err);
                return Err(err.with_write_phase(WritePhase::ValueConversion));
            }
        };
        if let Err(err) = sink.send_token_row(row).await {
            trace.emit_failed(PACKET_WRITE_PHASE, &err);
            return Err(err.with_write_phase(WritePhase::PacketWrite));
        }
    }

    let stats = state.record_accepted_batch(rows_written);
    trace.emit_completed(stats);
    Ok(stats)
}

trait TokenRowSink {
    async fn send_token_row(&mut self, row: tiberius::TokenRow<'static>) -> Result<()>;
}

impl<S> TokenRowSink for tiberius::BulkLoadRequest<'_, S>
where
    S: AsyncRead + AsyncWrite + Unpin + Send,
{
    async fn send_token_row(&mut self, row: tiberius::TokenRow<'static>) -> Result<()> {
        self.send(row)
            .await
            .map_err(|source| crate::Error::Tiberius { source })
    }
}

async fn write_direct_batch_to_sink<Sink>(
    state: &mut WriterState,
    sink: &mut Sink,
    batch: &RecordBatch,
) -> Result<WriteStats>
where
    Sink: RawRowsSink,
{
    let trace = BatchWriteTrace::new(state, batch);
    trace.emit_started();

    let encoder = match state
        .direct_encoder()
        .ok_or_else(|| crate::Error::BackendUnavailable {
            backend: state.backend(),
            reason: "direct bulk encoder is not available for this writer".to_owned(),
        }) {
        Ok(encoder) => encoder,
        Err(err) => {
            trace.emit_failed(DIRECT_ENCODING_PHASE, &err);
            return Err(err.with_write_phase(WritePhase::DirectEncoding));
        }
    };
    let measure_start = std::time::Instant::now();
    let measured = encoder.measure_batch(batch);
    let measured =
        match profile::record_elapsed(measure_start, profile::record_measure_batch, measured) {
            Ok(measured) => measured,
            Err(err) => {
                let phase = write_phase_for_batch_error(&err);
                trace.emit_failed(phase.as_str(), &err);
                emit_direct_raw_failed(state.backend(), phase.as_str(), batch, None, &err);
                return Err(err.with_write_phase(phase));
            }
        };
    emit_direct_raw_measured(state.backend(), &measured, measure_start.elapsed());
    let rows_written = usize_to_u64_saturating(measured.row_count());

    let split_start = std::time::Instant::now();
    let ranges = measured.row_ranges(DIRECT_RAW_MAX_PAYLOAD_BYTES);
    let ranges = match profile::record_elapsed(split_start, profile::record_row_range_split, ranges)
    {
        Ok(ranges) => ranges,
        Err(err) => {
            trace.emit_failed(DIRECT_ENCODING_PHASE, &err);
            emit_direct_raw_failed(state.backend(), DIRECT_ENCODING_PHASE, batch, None, &err);
            return Err(err.with_write_phase(WritePhase::DirectEncoding));
        }
    };
    emit_direct_raw_ranges_planned(state.backend(), &measured, &ranges, split_start.elapsed());

    for range in ranges {
        if let Err(err) = sink
            .send_measured_raw_rows(state.backend(), encoder, batch, &measured, range)
            .await
        {
            let phase = write_phase_for_batch_error(&err);
            trace.emit_failed(phase.as_str(), &err);
            emit_direct_raw_failed(state.backend(), phase.as_str(), batch, Some(range), &err);
            return Err(err.with_write_phase(phase));
        }
    }

    profile::record_accepted_batch(measured.row_count());
    let stats = state.record_accepted_batch(rows_written);
    trace.emit_completed(stats);
    Ok(stats)
}

trait RawRowsSink {
    async fn send_measured_raw_rows(
        &mut self,
        backend: WriteBackend,
        encoder: &DirectEncoder,
        batch: &RecordBatch,
        measured: &MeasuredDirectBatch,
        range: MeasuredRowRange,
    ) -> Result<()>;
}

impl<S> RawRowsSink for tiberius::BulkLoadRequest<'_, S>
where
    S: AsyncRead + AsyncWrite + Unpin + Send,
{
    async fn send_measured_raw_rows(
        &mut self,
        backend: WriteBackend,
        encoder: &DirectEncoder,
        batch: &RecordBatch,
        measured: &MeasuredDirectBatch,
        range: MeasuredRowRange,
    ) -> Result<()> {
        let encoded_bytes = measured.range_payload_len(range.start, range.len)?;
        profile::record_row_range(encoded_bytes);

        if !encoder.has_variable_width_column() {
            let encode_start = std::time::Instant::now();
            let payload =
                encoder.encode_measured_batch_range(batch, measured, range.start, range.len)?;
            profile::record_append_encode(encode_start.elapsed());

            let send_start = std::time::Instant::now();
            let send_result = self
                .send_raw_rows_payload_checked(payload.bytes(), payload.row_token_offsets())
                .await
                .map_err(|source| crate::Error::Tiberius { source });
            profile::record_send_total(send_start.elapsed());
            if send_result.is_ok() {
                emit_direct_raw_packet_write_completed(
                    backend,
                    range,
                    payload.row_count(),
                    payload.bytes().len(),
                    send_start.elapsed(),
                );
            }
            return send_result;
        }

        let mut encode_error = None;
        let send_start = std::time::Instant::now();
        let send_result = self
            .send_raw_rows_with(|buf| {
                let encode_start = std::time::Instant::now();
                let encoded = encoder.encode_measured_batch_range_into(
                    batch,
                    measured,
                    range.start,
                    range.len,
                    buf,
                );
                profile::record_append_encode(encode_start.elapsed());

                match encoded {
                    Ok(append) => Ok(append),
                    Err(err) => {
                        encode_error = Some(err);
                        Err(tiberius::error::Error::BulkInput(Cow::Borrowed(
                            "direct raw row encoding failed",
                        )))
                    }
                }
            })
            .await;
        profile::record_send_total(send_start.elapsed());

        if let Some(err) = encode_error {
            return Err(err);
        }

        let send_result = send_result.map_err(|source| crate::Error::Tiberius { source });
        if send_result.is_ok() {
            emit_direct_raw_packet_write_completed(
                backend,
                range,
                range.len,
                encoded_bytes,
                send_start.elapsed(),
            );
        }

        send_result
    }
}

fn emit_direct_raw_measured(
    backend: WriteBackend,
    measured: &MeasuredDirectBatch,
    elapsed: Duration,
) {
    if backend != WriteBackend::DirectRawBulk {
        return;
    }

    tracing::debug!(
        target: TRACE_TARGET,
        phase = DIRECT_ENCODING_PHASE,
        telemetry_event = DIRECT_RAW_MEASURED_EVENT,
        backend = backend_trace_name(backend),
        batch_row_count = usize_to_u64_saturating(measured.row_count()),
        batch_column_count = usize_to_u64_saturating(measured.column_count()),
        encoded_row_count = usize_to_u64_saturating(measured.row_count()),
        encoded_byte_count = usize_to_u64_saturating(measured.payload_len()),
        elapsed_us = duration_micros_u64(elapsed)
    );
}

fn emit_direct_raw_ranges_planned(
    backend: WriteBackend,
    measured: &MeasuredDirectBatch,
    ranges: &[MeasuredRowRange],
    elapsed: Duration,
) {
    if backend != WriteBackend::DirectRawBulk {
        return;
    }

    tracing::debug!(
        target: TRACE_TARGET,
        phase = DIRECT_ENCODING_PHASE,
        telemetry_event = DIRECT_RAW_RANGES_PLANNED_EVENT,
        backend = backend_trace_name(backend),
        batch_row_count = usize_to_u64_saturating(measured.row_count()),
        batch_column_count = usize_to_u64_saturating(measured.column_count()),
        encoded_byte_count = usize_to_u64_saturating(measured.payload_len()),
        encoded_range_count = usize_to_u64_saturating(ranges.len()),
        elapsed_us = duration_micros_u64(elapsed)
    );
}

fn emit_direct_raw_packet_write_completed(
    backend: WriteBackend,
    range: MeasuredRowRange,
    encoded_row_count: usize,
    encoded_byte_count: usize,
    elapsed: Duration,
) {
    if backend != WriteBackend::DirectRawBulk {
        return;
    }

    // `tiberius-raw-bulk` does not expose raw packet counts on the normal
    // write path. Emit safe range and byte summaries instead of guessing.
    tracing::debug!(
        target: TRACE_TARGET,
        phase = PACKET_WRITE_PHASE,
        telemetry_event = DIRECT_RAW_PACKET_WRITE_COMPLETED_EVENT,
        backend = backend_trace_name(backend),
        encoded_row_start = usize_to_u64_saturating(range.start),
        encoded_row_count = usize_to_u64_saturating(encoded_row_count),
        encoded_byte_count = usize_to_u64_saturating(encoded_byte_count),
        elapsed_us = duration_micros_u64(elapsed)
    );
}

fn emit_direct_raw_failed(
    backend: WriteBackend,
    phase: &'static str,
    batch: &RecordBatch,
    range: Option<MeasuredRowRange>,
    error: &crate::Error,
) {
    if backend != WriteBackend::DirectRawBulk {
        return;
    }

    let diagnostic_codes = diagnostic_codes_for_error(error);
    tracing::error!(
        target: TRACE_TARGET,
        phase,
        telemetry_event = DIRECT_RAW_FAILED_EVENT,
        backend = backend_trace_name(backend),
        batch_row_count = usize_to_u64_saturating(batch.num_rows()),
        batch_column_count = usize_to_u64_saturating(batch.num_columns()),
        encoded_row_start = range.map(|range| usize_to_u64_saturating(range.start)),
        encoded_row_count = range.map(|range| usize_to_u64_saturating(range.len)),
        diagnostic_codes = diagnostic_codes.as_str(),
        error_summary = sanitized_error_summary(error)
    );
}

fn usize_to_u64_saturating(value: usize) -> u64 {
    u64::try_from(value).unwrap_or(u64::MAX)
}

fn resolve_backend(requested_backend: WriteBackend) -> Result<WriteBackend> {
    match requested_backend {
        WriteBackend::Auto | WriteBackend::DirectRawBulk => Ok(WriteBackend::DirectRawBulk),
        WriteBackend::BaselineTokenRow => Ok(WriteBackend::BaselineTokenRow),
        WriteBackend::DirectFramedBulk => Ok(WriteBackend::DirectFramedBulk),
    }
}

fn execution_unavailable(backend: WriteBackend) -> crate::Error {
    crate::Error::BackendUnavailable {
        backend,
        reason: "bulk writer execution is not implemented yet".to_owned(),
    }
}

fn backend_trace_name(backend: WriteBackend) -> &'static str {
    match backend {
        WriteBackend::Auto => "Auto",
        WriteBackend::BaselineTokenRow => "BaselineTokenRow",
        WriteBackend::DirectFramedBulk => "DirectFramedBulk",
        WriteBackend::DirectRawBulk => "DirectRawBulk",
    }
}

fn sanitized_error_summary(error: &crate::Error) -> &'static str {
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
        crate::Error::SqlExecution { .. } => "SQL statement execution failed",
        crate::Error::Tiberius { .. } => "tiberius operation failed",
    }
}

fn diagnostic_codes_for_error(error: &crate::Error) -> String {
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

fn write_phase_for_batch_error(error: &crate::Error) -> WritePhase {
    match error {
        crate::Error::WritePhaseContext { phase, .. } => *phase,
        crate::Error::ValueConversion { diagnostics }
            if diagnostics
                .all()
                .iter()
                .all(|diagnostic| diagnostic.code() == DiagnosticCode::SchemaMismatch) =>
        {
            WritePhase::BatchSchemaValidation
        }
        crate::Error::ValueConversion { .. } => WritePhase::ValueConversion,
        crate::Error::DirectEncoding { .. } | crate::Error::BackendUnavailable { .. } => {
            WritePhase::DirectEncoding
        }
        crate::Error::Tiberius { .. } => WritePhase::PacketWrite,
        _ => WritePhase::BatchWrite,
    }
}

fn duration_micros_u64(duration: Duration) -> u64 {
    u64::try_from(duration.as_micros()).unwrap_or(u64::MAX)
}

#[cfg(test)]
mod tests {
    use std::{
        borrow::Cow,
        future::Future,
        pin::Pin,
        sync::Arc,
        task::{Context, Poll, Waker},
    };

    use arrow_array::{
        BinaryArray, Float64Array, Int32Array, RecordBatch, StringArray, UInt64Array,
    };
    use arrow_schema::{DataType, Field, Schema};
    use futures_util::io::{AsyncRead, AsyncWrite};

    use super::{
        BulkTargetColumnMetadata, DIRECT_RAW_MAX_PAYLOAD_BYTES, DirectEncoder, FinishSink,
        MeasuredDirectBatch, MeasuredRowRange, RawRowsSink, TokenRowSink, WriteBackend,
        WriteOptions, WriteStats, WriterInitializationTrace, WriterState, bulk_insert_table_sql,
        emit_direct_raw_packet_write_completed, finish_writer_to_sink, record_batch_view,
        resolve_backend, tiberius_row_owned, validate_batch_rows, validate_bulk_target_columns,
        validate_direct_bulk_target_column_types, write_batch_to_sink, write_direct_batch_to_sink,
    };
    use crate::observability::{
        BATCH_SCHEMA_VALIDATION_PHASE, BATCH_WRITE_COMPLETED_EVENT, BATCH_WRITE_FAILED_EVENT,
        BATCH_WRITE_PHASE, BATCH_WRITE_SPAN, DIRECT_ENCODING_PHASE, DIRECT_RAW_MEASURED_EVENT,
        DIRECT_RAW_PACKET_WRITE_COMPLETED_EVENT, DIRECT_RAW_RANGES_PLANNED_EVENT, FINALIZE_PHASE,
        FINISH_COMPLETED_EVENT, FINISH_FAILED_EVENT, FINISH_PHASE, FINISH_SPAN, PACKET_WRITE_PHASE,
        TARGET_METADATA_VALIDATION_COMPLETED_EVENT, TARGET_METADATA_VALIDATION_FAILED_EVENT,
        TARGET_METADATA_VALIDATION_PHASE, VALUE_CONVERSION_PHASE,
        WRITER_INITIALIZATION_COMPLETED_EVENT, WRITER_INITIALIZATION_PHASE,
        WRITER_INITIALIZATION_SPAN,
        test_support::{CapturedTrace, CapturedTraceKind, capture_traces},
    };
    use crate::{
        ArrowFieldRef, DiagnosticCode, Error, Identifier, MssqlColumn, MssqlType, MssqlTypeLength,
        PlanOptions, SchemaCheck, SchemaMapping, TableName, WritePhase,
    };

    #[test]
    fn write_backend_defaults_to_auto() {
        assert_eq!(WriteBackend::default(), WriteBackend::Auto);
    }

    #[test]
    fn write_options_default_to_auto_backend_and_strict_schema_check() {
        let options = WriteOptions::default();

        assert_eq!(options.backend, WriteBackend::Auto);
        assert_eq!(options.schema_check, SchemaCheck::Strict);
        assert_eq!(options.plan_options, PlanOptions::default());
    }

    #[test]
    fn write_options_preserve_explicit_backend_selection() {
        for backend in [
            WriteBackend::Auto,
            WriteBackend::BaselineTokenRow,
            WriteBackend::DirectFramedBulk,
            WriteBackend::DirectRawBulk,
        ] {
            let options = WriteOptions {
                backend,
                schema_check: SchemaCheck::Strict,
                ..WriteOptions::default()
            };

            assert_eq!(options.backend, backend);
            assert_eq!(options.schema_check, SchemaCheck::Strict);
        }
    }

    #[test]
    fn write_stats_default_to_zero() {
        let stats = WriteStats::default();

        assert_eq!(stats.rows_written, 0);
        assert_eq!(stats.batches_written, 0);
    }

    #[test]
    fn auto_backend_resolves_to_direct_raw_bulk() {
        assert_eq!(
            resolve_backend(WriteBackend::Auto).unwrap(),
            WriteBackend::DirectRawBulk
        );
    }

    #[test]
    fn explicit_backends_resolve_to_requested_backend() {
        assert_eq!(
            resolve_backend(WriteBackend::BaselineTokenRow).unwrap(),
            WriteBackend::BaselineTokenRow
        );
        assert_eq!(
            resolve_backend(WriteBackend::DirectFramedBulk).unwrap(),
            WriteBackend::DirectFramedBulk
        );
        assert_eq!(
            resolve_backend(WriteBackend::DirectRawBulk).unwrap(),
            WriteBackend::DirectRawBulk
        );
    }

    #[test]
    fn writer_initialization_trace_records_auto_backend_resolution() -> Result<(), String> {
        let table = TableName::new("dbo", "target").unwrap();
        let mappings = vec![mapping("id")];

        let (state, traces) = capture_traces(|| {
            let mut trace =
                WriterInitializationTrace::new(&table, WriteBackend::Auto, mappings.len());
            trace.emit_started();
            let state = WriterState::new(
                WriteBackend::Auto,
                SchemaCheck::Strict,
                PlanOptions::default(),
                mappings,
            )
            .unwrap();
            trace.record_resolved_backend(state.backend());
            trace.record_direct_target_validation_required(true);
            trace.emit_completed();
            state
        });

        assert_eq!(state.backend(), WriteBackend::DirectRawBulk);
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
            let mappings = vec![mapping("id")];
            let (_state, traces) = capture_traces(|| {
                let mut trace = WriterInitializationTrace::new(&table, backend, mappings.len());
                trace.emit_started();
                let state = WriterState::new(
                    backend,
                    SchemaCheck::Strict,
                    PlanOptions::default(),
                    mappings,
                )
                .unwrap();
                trace.record_resolved_backend(state.backend());
                trace.record_direct_target_validation_required(matches!(
                    state.backend(),
                    WriteBackend::DirectFramedBulk | WriteBackend::DirectRawBulk
                ));
                trace.emit_completed();
                state
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
        let mappings = vec![mapping("id")];
        let columns = vec![bulk_target_column_with_type(
            0,
            "id",
            false,
            tiberius::ColumnType::Int4,
        )];

        let (_state, traces) = capture_traces(|| {
            let mut trace =
                WriterInitializationTrace::new(&table, WriteBackend::DirectRawBulk, mappings.len());
            let state = WriterState::new(
                WriteBackend::DirectRawBulk,
                SchemaCheck::Strict,
                PlanOptions::default(),
                mappings,
            )
            .unwrap();
            trace.record_resolved_backend(state.backend());
            trace.record_direct_target_validation_required(true);
            trace.emit_target_metadata_validation_started();
            validate_bulk_target_columns(columns.iter(), state.mappings()).unwrap();
            validate_direct_bulk_target_column_types(
                columns.iter(),
                state.direct_encoder().unwrap().plan(),
            )
            .unwrap();
            trace.emit_target_metadata_validation_completed();
            state
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
        let mappings = vec![mapping("id")];
        let columns = vec![bulk_target_column_with_type(
            0,
            "id",
            false,
            tiberius::ColumnType::Int8,
        )];

        let (_err, traces) = capture_traces(|| {
            let mut trace =
                WriterInitializationTrace::new(&table, WriteBackend::DirectRawBulk, mappings.len());
            let state = WriterState::new(
                WriteBackend::DirectRawBulk,
                SchemaCheck::Strict,
                PlanOptions::default(),
                mappings,
            )
            .unwrap();
            trace.record_resolved_backend(state.backend());
            trace.record_direct_target_validation_required(true);
            let err = validate_direct_bulk_target_column_types(
                columns.iter(),
                state.direct_encoder().unwrap().plan(),
            )
            .unwrap_err();
            trace.emit_failed(TARGET_METADATA_VALIDATION_PHASE, &err);
            err
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

    #[test]
    fn writer_state_starts_with_resolved_backend_mappings_and_zero_stats() {
        let mappings = vec![mapping("id")];

        let state = WriterState::new(
            WriteBackend::Auto,
            SchemaCheck::Strict,
            PlanOptions::default(),
            mappings.clone(),
        )
        .unwrap();

        assert_eq!(state.backend(), WriteBackend::DirectRawBulk);
        assert!(state.direct_encoder().is_some());
        assert_eq!(state.schema_check(), SchemaCheck::Strict);
        assert_eq!(state.mappings(), mappings.as_slice());
        assert_eq!(state.stats(), WriteStats::default());
    }

    #[test]
    fn direct_writer_state_builds_encoder_for_supported_mappings() {
        let mappings = vec![
            mapping("id32"),
            SchemaMapping::new(
                ArrowFieldRef::new(1, "id64".to_owned(), false, DataType::Int64),
                MssqlColumn::new(Identifier::new("id64").unwrap(), MssqlType::BigInt, false),
            ),
            float_mapping_at(2, "score"),
            SchemaMapping::new(
                ArrowFieldRef::new(3, "name".to_owned(), true, DataType::Utf8),
                MssqlColumn::new(
                    Identifier::new("name").unwrap(),
                    MssqlType::NVarChar(crate::MssqlTypeLength::Max),
                    true,
                ),
            ),
        ];

        for backend in [WriteBackend::DirectFramedBulk, WriteBackend::DirectRawBulk] {
            let state = WriterState::new(
                backend,
                SchemaCheck::Strict,
                PlanOptions::default(),
                mappings.clone(),
            )
            .unwrap();

            assert_eq!(state.backend(), backend);
            assert!(state.direct_encoder().is_some());
        }
    }

    #[test]
    fn direct_writer_state_rejects_unsupported_mappings() {
        let mappings = vec![SchemaMapping::new(
            ArrowFieldRef::new(
                0,
                "list_value".to_owned(),
                true,
                DataType::List(Arc::new(Field::new("item", DataType::Int32, true))),
            ),
            MssqlColumn::new(
                Identifier::new("list_value").unwrap(),
                MssqlType::NVarChar(MssqlTypeLength::Max),
                true,
            ),
        )];

        let err = WriterState::new(
            WriteBackend::DirectRawBulk,
            SchemaCheck::Strict,
            PlanOptions::default(),
            mappings,
        )
        .unwrap_err();

        let Error::DirectEncoding { diagnostics } = err else {
            panic!("expected direct encoding error");
        };
        assert_eq!(diagnostics.len(), 1);
        assert_eq!(
            diagnostics.all()[0].code(),
            DiagnosticCode::DirectEncodingUnsupportedMapping
        );
    }

    #[test]
    fn writer_state_accumulates_accepted_batch_stats() {
        let mut state = WriterState::new(
            WriteBackend::BaselineTokenRow,
            SchemaCheck::Strict,
            PlanOptions::default(),
            Vec::new(),
        )
        .unwrap();

        assert_eq!(
            state.record_accepted_batch(0),
            WriteStats {
                rows_written: 0,
                batches_written: 1
            }
        );
        assert_eq!(
            state.record_accepted_batch(3),
            WriteStats {
                rows_written: 3,
                batches_written: 2
            }
        );
        assert_eq!(
            state.record_accepted_batch(5),
            WriteStats {
                rows_written: 8,
                batches_written: 3
            }
        );
    }

    #[test]
    fn finish_success_emits_final_stats_trace() -> Result<(), String> {
        let mut state = WriterState::new(
            WriteBackend::BaselineTokenRow,
            SchemaCheck::Strict,
            PlanOptions::default(),
            Vec::new(),
        )
        .unwrap();
        state.record_accepted_batch(2);
        state.record_accepted_batch(3);
        let sink = RecordingFinishSink::default();

        let (stats, traces) = capture_traces(|| poll_ready(finish_writer_to_sink(state, sink)));
        let stats = stats.unwrap();

        assert_eq!(
            stats,
            WriteStats {
                rows_written: 5,
                batches_written: 2
            }
        );
        let records = traces.records()?;
        let event = trace_event(&records, FINISH_COMPLETED_EVENT)?;
        assert_eq!(event.span_name(), Some(FINISH_SPAN));
        assert_trace_field(event, "phase", FINISH_PHASE);
        assert_trace_field(event, "backend", "BaselineTokenRow");
        assert_trace_field(event, "rows_written", "5");
        assert_trace_field(event, "batches_written", "2");
        assert_trace_field(event, "finish_result", "success");
        assert!(event.fields().contains_key("elapsed_us"));

        Ok(())
    }

    #[test]
    fn finish_failure_emits_finalize_trace_with_accepted_stats() -> Result<(), String> {
        let mut state = WriterState::new(
            WriteBackend::DirectRawBulk,
            SchemaCheck::Strict,
            PlanOptions::default(),
            Vec::new(),
        )
        .unwrap();
        state.record_accepted_batch(7);
        let sink = RecordingFinishSink {
            fail_message: Some("fake finalize failure"),
        };

        let (err, traces) = capture_traces(|| poll_ready(finish_writer_to_sink(state, sink)));
        let err = err.unwrap_err();

        assert_write_phase(&err, WritePhase::Finalize);
        let Error::Tiberius { source } = inner_error(&err) else {
            panic!("expected tiberius error");
        };
        assert_eq!(
            source.to_string(),
            "BULK UPLOAD input failure: fake finalize failure"
        );
        let records = traces.records()?;
        let event = trace_event(&records, FINISH_FAILED_EVENT)?;
        assert_eq!(event.span_name(), Some(FINISH_SPAN));
        assert_trace_field(event, "phase", FINALIZE_PHASE);
        assert_trace_field(event, "backend", "DirectRawBulk");
        assert_trace_field(event, "accepted_rows_before", "7");
        assert_trace_field(event, "accepted_batches_before", "1");
        assert_trace_field(event, "finish_result", "failure");
        assert_trace_field(event, "error_summary", "tiberius operation failed");
        assert_trace_field(event, "diagnostic_codes", "");
        assert!(event.fields().contains_key("elapsed_us"));

        Ok(())
    }

    #[test]
    fn finish_failure_trace_is_sanitized() -> Result<(), String> {
        let state = WriterState::new(
            WriteBackend::DirectRawBulk,
            SchemaCheck::Strict,
            PlanOptions::default(),
            Vec::new(),
        )
        .unwrap();
        let sink = RecordingFinishSink {
            fail_message: Some("server=tcp:sql.example.com;User ID=sa;password=secret"),
        };

        let (_err, traces) = capture_traces(|| poll_ready(finish_writer_to_sink(state, sink)));

        let records = traces.records()?;
        let event = trace_event(&records, FINISH_FAILED_EVENT)?;
        assert_trace_field(event, "error_summary", "tiberius operation failed");
        traces.assert_no_forbidden_text(&[
            "server=tcp:sql.example.com",
            "User ID=sa",
            "password=secret",
        ])?;

        Ok(())
    }

    #[test]
    fn bulk_insert_table_sql_uses_quoted_table_name() {
        let table = TableName::new("dbo]x", "target.table").unwrap();

        assert_eq!(bulk_insert_table_sql(&table), "[dbo]]x].[target.table]");
    }

    #[test]
    fn strict_batch_validation_accepts_supported_rows_without_owning_payloads() {
        let batch = int32_batch("id", &[1, 2]);
        let mappings = [mapping("id")];
        let view = record_batch_view(
            &batch,
            &mappings,
            SchemaCheck::Strict,
            &PlanOptions::default(),
        )
        .unwrap();

        validate_batch_rows(&view).unwrap();

        let row = tiberius_row_owned(&view, 1).unwrap();
        assert_eq!(row.get(0), Some(&tiberius::ColumnData::I32(Some(2))));
    }

    #[test]
    fn strict_batch_view_rejects_runtime_schema_mismatch_before_send() {
        let batch = int32_batch("renamed_id", &[1]);
        let err = record_batch_view(
            &batch,
            &[mapping("id")],
            SchemaCheck::Strict,
            &PlanOptions::default(),
        )
        .unwrap_err();

        let Error::ValueConversion { diagnostics } = err else {
            panic!("expected value conversion error");
        };
        assert_eq!(diagnostics.len(), 1);
        let diagnostic = &diagnostics.all()[0];
        assert_eq!(diagnostic.code(), DiagnosticCode::SchemaMismatch);
        assert_eq!(diagnostic.field().map(|field| field.name()), Some("id"));
    }

    #[test]
    fn strict_batch_validation_rejects_bad_later_row_before_any_send() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "amount",
            DataType::Float64,
            false,
        )]));
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(Float64Array::from(vec![
                Some(1.0),
                Some(f64::NAN),
            ]))],
        )
        .unwrap();
        let mappings = [SchemaMapping::new(
            ArrowFieldRef::new(0, "amount".to_owned(), false, DataType::Float64),
            MssqlColumn::new(
                Identifier::new("amount").unwrap(),
                MssqlType::Float { precision: 53 },
                false,
            ),
        )];

        let view = record_batch_view(
            &batch,
            &mappings,
            SchemaCheck::Strict,
            &PlanOptions::default(),
        )
        .unwrap();
        let err = validate_batch_rows(&view).unwrap_err();

        let Error::ValueConversion { diagnostics } = err else {
            panic!("expected value conversion error");
        };
        assert_eq!(diagnostics.len(), 1);
        let diagnostic = &diagnostics.all()[0];
        assert_eq!(diagnostic.code(), DiagnosticCode::NonFiniteFloat);
        assert_eq!(diagnostic.row(), Some(1));
    }

    #[test]
    fn bulk_target_column_validation_accepts_matching_metadata() {
        let mappings = vec![mapping("id")];
        let columns = vec![bulk_target_column(0, "id", false)];

        validate_bulk_target_columns(columns.into_iter(), &mappings).unwrap();
    }

    #[test]
    fn bulk_target_column_validation_rejects_missing_target_columns() {
        let mappings = vec![mapping("id")];
        let columns = Vec::<FakeBulkTargetColumn>::new();

        let err = validate_bulk_target_columns(columns.into_iter(), &mappings).unwrap_err();

        let Error::ValueConversion { diagnostics } = err else {
            panic!("expected value conversion error");
        };
        assert_eq!(diagnostics.len(), 1);
        assert_eq!(diagnostics.all()[0].code(), DiagnosticCode::SchemaMismatch);
        assert_eq!(
            diagnostics.all()[0].message(),
            "bulk target has 0 updateable column(s) but mappings contain 1 column(s)"
        );
    }

    #[test]
    fn bulk_target_column_validation_rejects_ordinal_name_and_nullability_drift() {
        let mappings = vec![mapping("id")];
        let columns = vec![bulk_target_column(7, "id]; DROP TABLE target;--", true)];

        let err = validate_bulk_target_columns(columns.into_iter(), &mappings).unwrap_err();

        let Error::ValueConversion { diagnostics } = err else {
            panic!("expected value conversion error");
        };
        assert_eq!(diagnostics.len(), 3);
        assert!(
            diagnostics
                .all()
                .iter()
                .all(|diagnostic| diagnostic.code() == DiagnosticCode::SchemaMismatch)
        );
        assert!(
            diagnostics
                .all()
                .iter()
                .all(|diagnostic| diagnostic.field().map(|field| field.name()) == Some("id"))
        );
        assert!(
            diagnostics
                .all()
                .iter()
                .any(|diagnostic| diagnostic.message().contains("ordinal 7"))
        );
        assert!(
            diagnostics
                .all()
                .iter()
                .any(|diagnostic| diagnostic.message().contains("DROP TABLE"))
        );
        assert!(
            diagnostics
                .all()
                .iter()
                .any(|diagnostic| diagnostic.message().contains("nullability true"))
        );
    }

    #[test]
    fn direct_bulk_target_type_validation_accepts_matching_primitive_metadata() {
        let mappings = vec![mapping("id")];
        let state = WriterState::new(
            WriteBackend::DirectRawBulk,
            SchemaCheck::Strict,
            PlanOptions::default(),
            mappings,
        )
        .unwrap();
        let columns = vec![bulk_target_column_with_type(
            0,
            "id",
            false,
            tiberius::ColumnType::Int4,
        )];

        validate_direct_bulk_target_column_types(
            columns.into_iter(),
            state.direct_encoder().unwrap().plan(),
        )
        .unwrap();
    }

    #[test]
    fn direct_bulk_target_type_validation_accepts_issue_75_integer_metadata() {
        let mappings = vec![
            schema_mapping_at(0, "tiny", DataType::UInt8, MssqlType::TinyInt, false),
            schema_mapping_at(1, "signed_tiny", DataType::Int8, MssqlType::SmallInt, false),
            schema_mapping_at(2, "small", DataType::Int16, MssqlType::SmallInt, false),
            schema_mapping_at(
                3,
                "unsigned_medium",
                DataType::UInt16,
                MssqlType::Int,
                false,
            ),
            schema_mapping_at(
                4,
                "unsigned_total",
                DataType::UInt32,
                MssqlType::BigInt,
                false,
            ),
        ];
        let state = WriterState::new(
            WriteBackend::DirectRawBulk,
            SchemaCheck::Strict,
            PlanOptions::default(),
            mappings,
        )
        .unwrap();
        let columns = vec![
            bulk_target_column_with_type(0, "tiny", false, tiberius::ColumnType::Int1),
            bulk_target_column_with_type(1, "signed_tiny", false, tiberius::ColumnType::Int2),
            bulk_target_column_with_type(2, "small", false, tiberius::ColumnType::Int2),
            bulk_target_column_with_type(3, "unsigned_medium", false, tiberius::ColumnType::Int4),
            bulk_target_column_with_type(4, "unsigned_total", false, tiberius::ColumnType::Int8),
        ];

        validate_direct_bulk_target_column_types(
            columns.into_iter(),
            state.direct_encoder().unwrap().plan(),
        )
        .unwrap();
    }

    #[test]
    fn direct_bulk_target_type_validation_accepts_issue_75_float32_metadata() {
        let mappings = vec![schema_mapping_at(
            0,
            "real_value",
            DataType::Float32,
            MssqlType::Real,
            false,
        )];
        let state = WriterState::new(
            WriteBackend::DirectRawBulk,
            SchemaCheck::Strict,
            PlanOptions::default(),
            mappings,
        )
        .unwrap();
        let columns = vec![bulk_target_column_with_type(
            0,
            "real_value",
            false,
            tiberius::ColumnType::Float4,
        )];

        validate_direct_bulk_target_column_types(
            columns.into_iter(),
            state.direct_encoder().unwrap().plan(),
        )
        .unwrap();
    }

    #[test]
    fn direct_bulk_target_type_validation_accepts_uint64_policy_metadata() {
        let mappings = vec![
            schema_mapping_at(0, "checked", DataType::UInt64, MssqlType::BigInt, false),
            schema_mapping_at(
                1,
                "decimal",
                DataType::UInt64,
                MssqlType::Decimal {
                    precision: 20,
                    scale: 0,
                },
                false,
            ),
        ];
        let state = WriterState::new(
            WriteBackend::DirectRawBulk,
            SchemaCheck::Strict,
            PlanOptions::default(),
            mappings,
        )
        .unwrap();
        let columns = vec![
            bulk_target_column_with_type(0, "checked", false, tiberius::ColumnType::Int8),
            bulk_target_decimal_column(1, "decimal", false, 20, 0),
        ];

        validate_direct_bulk_target_column_types(
            columns.into_iter(),
            state.direct_encoder().unwrap().plan(),
        )
        .unwrap();
    }

    #[test]
    fn direct_bulk_target_type_validation_rejects_uint64_decimal_precision_drift() {
        let mappings = vec![schema_mapping_at(
            0,
            "decimal",
            DataType::UInt64,
            MssqlType::Decimal {
                precision: 20,
                scale: 0,
            },
            false,
        )];
        let state = WriterState::new(
            WriteBackend::DirectRawBulk,
            SchemaCheck::Strict,
            PlanOptions::default(),
            mappings,
        )
        .unwrap();
        let columns = vec![bulk_target_decimal_column(0, "decimal", false, 19, 0)];

        let err = validate_direct_bulk_target_column_types(
            columns.into_iter(),
            state.direct_encoder().unwrap().plan(),
        )
        .unwrap_err();

        let Error::ValueConversion { diagnostics } = err else {
            panic!("expected value conversion error");
        };
        assert_eq!(diagnostics.len(), 1);
        let diagnostic = &diagnostics.all()[0];
        assert_eq!(diagnostic.code(), DiagnosticCode::SchemaMismatch);
        assert!(diagnostic.message().contains("precision/scale (19,0)"));
        assert_eq!(
            diagnostic
                .field()
                .map(|field| (field.index(), field.name())),
            Some((0, "decimal"))
        );
    }

    #[test]
    fn direct_bulk_target_type_validation_accepts_matching_variable_width_metadata() {
        let mappings = vec![utf8_mapping_at(0, "name"), binary_mapping_at(1, "payload")];
        let state = WriterState::new(
            WriteBackend::DirectRawBulk,
            SchemaCheck::Strict,
            PlanOptions::default(),
            mappings,
        )
        .unwrap();
        let columns = vec![
            bulk_target_column_with_type(0, "name", false, tiberius::ColumnType::NVarchar),
            bulk_target_column_with_type(1, "payload", false, tiberius::ColumnType::BigVarBin),
        ];

        validate_direct_bulk_target_column_types(
            columns.into_iter(),
            state.direct_encoder().unwrap().plan(),
        )
        .unwrap();
    }

    #[test]
    fn direct_bulk_target_type_validation_accepts_matching_large_variable_width_metadata() {
        let mappings = vec![
            schema_mapping_at(
                0,
                "large_name",
                DataType::LargeUtf8,
                MssqlType::NVarChar(MssqlTypeLength::Max),
                false,
            ),
            schema_mapping_at(
                1,
                "large_payload",
                DataType::LargeBinary,
                MssqlType::VarBinary(MssqlTypeLength::Max),
                false,
            ),
        ];
        let state = WriterState::new(
            WriteBackend::DirectRawBulk,
            SchemaCheck::Strict,
            PlanOptions::default(),
            mappings,
        )
        .unwrap();
        let columns = vec![
            bulk_target_column_with_type(0, "large_name", false, tiberius::ColumnType::NVarchar),
            bulk_target_column_with_type(
                1,
                "large_payload",
                false,
                tiberius::ColumnType::BigVarBin,
            ),
        ];

        validate_direct_bulk_target_column_types(
            columns.into_iter(),
            state.direct_encoder().unwrap().plan(),
        )
        .unwrap();
    }

    #[test]
    fn direct_bulk_target_type_validation_accepts_fixed_size_binary_metadata() {
        let mappings = vec![fixed_size_binary_mapping_at(0, "digest", 32)];
        let state = WriterState::new(
            WriteBackend::DirectRawBulk,
            SchemaCheck::Strict,
            PlanOptions::default(),
            mappings,
        )
        .unwrap();
        let columns = vec![bulk_target_column_with_type(
            0,
            "digest",
            false,
            tiberius::ColumnType::BigBinary,
        )];

        validate_direct_bulk_target_column_types(
            columns.into_iter(),
            state.direct_encoder().unwrap().plan(),
        )
        .unwrap();
    }

    #[test]
    fn direct_bulk_target_type_validation_rejects_fixed_size_binary_as_varbinary() {
        let mappings = vec![fixed_size_binary_mapping_at(0, "digest", 32)];
        let state = WriterState::new(
            WriteBackend::DirectRawBulk,
            SchemaCheck::Strict,
            PlanOptions::default(),
            mappings,
        )
        .unwrap();
        let columns = vec![bulk_target_column_with_type(
            0,
            "digest",
            false,
            tiberius::ColumnType::BigVarBin,
        )];

        let err = validate_direct_bulk_target_column_types(
            columns.into_iter(),
            state.direct_encoder().unwrap().plan(),
        )
        .unwrap_err();

        let Error::ValueConversion { diagnostics } = err else {
            panic!("expected value conversion error");
        };
        assert_eq!(diagnostics.len(), 1);
        let diagnostic = &diagnostics.all()[0];
        assert_eq!(diagnostic.code(), DiagnosticCode::SchemaMismatch);
        assert_eq!(diagnostic.field().map(|field| field.name()), Some("digest"));
        assert!(diagnostic.message().contains(
            "bulk target column type BigVarBin does not match direct encoder type BigBinary"
        ));
    }

    #[test]
    fn direct_bulk_target_type_validation_accepts_date_metadata() {
        let mappings = vec![
            SchemaMapping::new(
                ArrowFieldRef::new(0, "created_on".to_owned(), true, DataType::Date32),
                MssqlColumn::new(
                    Identifier::new("created_on").unwrap(),
                    MssqlType::Date,
                    true,
                ),
            ),
            SchemaMapping::new(
                ArrowFieldRef::new(1, "created_at".to_owned(), true, DataType::Date64),
                MssqlColumn::new(
                    Identifier::new("created_at").unwrap(),
                    MssqlType::DateTime2 { precision: 3 },
                    true,
                ),
            ),
        ];
        let state = WriterState::new(
            WriteBackend::DirectRawBulk,
            SchemaCheck::Strict,
            PlanOptions::default(),
            mappings,
        )
        .unwrap();
        let columns = vec![
            bulk_target_column_with_type(0, "created_on", true, tiberius::ColumnType::Daten),
            bulk_target_column_with_type(1, "created_at", true, tiberius::ColumnType::Datetime2),
        ];

        validate_direct_bulk_target_column_types(
            columns.into_iter(),
            state.direct_encoder().unwrap().plan(),
        )
        .unwrap();
    }

    #[test]
    fn direct_bulk_target_type_validation_rejects_variable_width_type_swap() {
        let mappings = vec![utf8_mapping_at(0, "name"), binary_mapping_at(1, "payload")];
        let state = WriterState::new(
            WriteBackend::DirectRawBulk,
            SchemaCheck::Strict,
            PlanOptions::default(),
            mappings,
        )
        .unwrap();
        let columns = vec![
            bulk_target_column_with_type(0, "name", false, tiberius::ColumnType::BigVarBin),
            bulk_target_column_with_type(1, "payload", false, tiberius::ColumnType::NVarchar),
        ];

        let err = validate_direct_bulk_target_column_types(
            columns.into_iter(),
            state.direct_encoder().unwrap().plan(),
        )
        .unwrap_err();

        let Error::ValueConversion { diagnostics } = err else {
            panic!("expected value conversion error");
        };
        assert_eq!(diagnostics.len(), 2);
        assert!(
            diagnostics
                .all()
                .iter()
                .any(|diagnostic| diagnostic.message().contains("NVarchar"))
        );
        assert!(
            diagnostics
                .all()
                .iter()
                .any(|diagnostic| diagnostic.message().contains("BigVarBin"))
        );
    }

    #[test]
    fn direct_bulk_target_type_validation_rejects_same_name_with_wrong_type() {
        let mappings = vec![mapping("id")];
        let state = WriterState::new(
            WriteBackend::DirectRawBulk,
            SchemaCheck::Strict,
            PlanOptions::default(),
            mappings,
        )
        .unwrap();
        let columns = vec![bulk_target_column_with_type(
            0,
            "id",
            false,
            tiberius::ColumnType::Int8,
        )];

        let err = validate_direct_bulk_target_column_types(
            columns.into_iter(),
            state.direct_encoder().unwrap().plan(),
        )
        .unwrap_err();

        let Error::ValueConversion { diagnostics } = err else {
            panic!("expected value conversion error");
        };
        assert_eq!(diagnostics.len(), 1);
        let diagnostic = &diagnostics.all()[0];
        assert_eq!(diagnostic.code(), DiagnosticCode::SchemaMismatch);
        assert_eq!(diagnostic.field().map(|field| field.name()), Some("id"));
        assert!(
            diagnostic
                .message()
                .contains("bulk target column type Int8 does not match direct encoder type Int4")
        );
    }

    #[test]
    fn write_batch_to_sink_accepts_empty_matching_batch() {
        let mappings = vec![mapping("id")];
        let mut state = WriterState::new(
            WriteBackend::BaselineTokenRow,
            SchemaCheck::Strict,
            PlanOptions::default(),
            mappings,
        )
        .unwrap();
        let mut sink = RecordingSink::default();
        let batch = int32_batch("id", &[]);

        let stats = poll_ready(write_batch_to_sink(&mut state, &mut sink, &batch)).unwrap();

        assert_eq!(
            stats,
            WriteStats {
                rows_written: 0,
                batches_written: 1
            }
        );
        assert!(sink.rows.is_empty());
    }

    #[test]
    fn write_batch_to_sink_accumulates_multi_batch_stats() {
        let mappings = vec![mapping("id")];
        let mut state = WriterState::new(
            WriteBackend::BaselineTokenRow,
            SchemaCheck::Strict,
            PlanOptions::default(),
            mappings,
        )
        .unwrap();
        let mut sink = RecordingSink::default();

        let first = poll_ready(write_batch_to_sink(
            &mut state,
            &mut sink,
            &int32_batch("id", &[10, 20]),
        ))
        .unwrap();
        let second = poll_ready(write_batch_to_sink(
            &mut state,
            &mut sink,
            &int32_batch("id", &[30]),
        ))
        .unwrap();

        assert_eq!(
            first,
            WriteStats {
                rows_written: 2,
                batches_written: 1
            }
        );
        assert_eq!(
            second,
            WriteStats {
                rows_written: 3,
                batches_written: 2
            }
        );
        assert_eq!(sink.rows.len(), 3);
        assert_eq!(
            sink.rows[2].get(0),
            Some(&tiberius::ColumnData::I32(Some(30)))
        );
    }

    #[test]
    fn write_batch_to_sink_conversion_failure_sends_nothing_and_keeps_stats() {
        let mappings = vec![float_mapping("amount")];
        let mut state = WriterState::new(
            WriteBackend::BaselineTokenRow,
            SchemaCheck::Strict,
            PlanOptions::default(),
            mappings,
        )
        .unwrap();
        let mut sink = RecordingSink::default();
        let batch = float64_batch("amount", &[Some(1.0), Some(f64::NAN)]);

        let err = poll_ready(write_batch_to_sink(&mut state, &mut sink, &batch)).unwrap_err();

        assert_write_phase(&err, WritePhase::ValueConversion);
        let Error::ValueConversion { diagnostics } = inner_error(&err) else {
            panic!("expected value conversion error");
        };
        assert_eq!(diagnostics.all()[0].code(), DiagnosticCode::NonFiniteFloat);
        assert_eq!(diagnostics.all()[0].row(), Some(1));
        assert!(sink.rows.is_empty());
        assert_eq!(state.stats(), WriteStats::default());
    }

    #[test]
    fn write_batch_to_sink_send_failure_preserves_error_and_keeps_stats() {
        let mappings = vec![mapping("id")];
        let mut state = WriterState::new(
            WriteBackend::BaselineTokenRow,
            SchemaCheck::Strict,
            PlanOptions::default(),
            mappings,
        )
        .unwrap();
        let mut sink = RecordingSink {
            fail_on_send: Some(1),
            rows: Vec::new(),
        };
        let batch = int32_batch("id", &[1, 2, 3]);

        let err = poll_ready(write_batch_to_sink(&mut state, &mut sink, &batch)).unwrap_err();

        assert_write_phase(&err, WritePhase::PacketWrite);
        let Error::Tiberius { source } = inner_error(&err) else {
            panic!("expected tiberius error");
        };
        assert_eq!(
            source.to_string(),
            "BULK UPLOAD input failure: fake send failure"
        );
        assert_eq!(sink.rows.len(), 1);
        assert_eq!(state.stats(), WriteStats::default());
    }

    #[test]
    fn baseline_batch_write_success_emits_stats_trace() -> Result<(), String> {
        let mappings = vec![mapping("id")];
        let mut state = WriterState::new(
            WriteBackend::BaselineTokenRow,
            SchemaCheck::Strict,
            PlanOptions::default(),
            mappings,
        )
        .unwrap();
        let mut sink = RecordingSink::default();
        let batch = int32_batch("id", &[10, 20]);

        let (stats, traces) =
            capture_traces(|| poll_ready(write_batch_to_sink(&mut state, &mut sink, &batch)));
        let stats = stats.unwrap();

        assert_eq!(
            stats,
            WriteStats {
                rows_written: 2,
                batches_written: 1
            }
        );
        let records = traces.records()?;
        let event = trace_event(&records, BATCH_WRITE_COMPLETED_EVENT)?;
        assert_eq!(event.span_name(), Some(BATCH_WRITE_SPAN));
        assert_trace_field(event, "phase", BATCH_WRITE_PHASE);
        assert_trace_field(event, "backend", "BaselineTokenRow");
        assert_trace_field(event, "attempted_batch_ordinal", "1");
        assert_trace_field(event, "batch_row_count", "2");
        assert_trace_field(event, "batch_column_count", "1");
        assert_trace_field(event, "batch_is_empty", "false");
        assert_trace_field(event, "accepted_rows_before", "0");
        assert_trace_field(event, "accepted_batches_before", "0");
        assert_trace_field(event, "accepted_rows_after", "2");
        assert_trace_field(event, "accepted_batches_after", "1");
        assert_trace_field(event, "batch_write_result", "success");
        assert!(event.fields().contains_key("elapsed_us"));

        Ok(())
    }

    #[test]
    fn empty_batch_write_success_trace_matches_stats() -> Result<(), String> {
        let mappings = vec![mapping("id")];
        let mut state = WriterState::new(
            WriteBackend::BaselineTokenRow,
            SchemaCheck::Strict,
            PlanOptions::default(),
            mappings,
        )
        .unwrap();
        let mut sink = RecordingSink::default();
        let batch = int32_batch("id", &[]);

        let (stats, traces) =
            capture_traces(|| poll_ready(write_batch_to_sink(&mut state, &mut sink, &batch)));
        let stats = stats.unwrap();

        assert_eq!(
            stats,
            WriteStats {
                rows_written: 0,
                batches_written: 1
            }
        );
        let records = traces.records()?;
        let event = trace_event(&records, BATCH_WRITE_COMPLETED_EVENT)?;
        assert_trace_field(event, "batch_row_count", "0");
        assert_trace_field(event, "batch_is_empty", "true");
        assert_trace_field(event, "accepted_rows_after", "0");
        assert_trace_field(event, "accepted_batches_after", "1");

        Ok(())
    }

    #[test]
    fn batch_schema_validation_failure_emits_trace_without_accepting_batch() -> Result<(), String> {
        let mappings = vec![mapping("id")];
        let mut state = WriterState::new(
            WriteBackend::BaselineTokenRow,
            SchemaCheck::Strict,
            PlanOptions::default(),
            mappings,
        )
        .unwrap();
        let mut sink = RecordingSink::default();
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "id",
                DataType::Float64,
                false,
            )])),
            vec![Arc::new(Float64Array::from(vec![1.0]))],
        )
        .unwrap();

        let (err, traces) =
            capture_traces(|| poll_ready(write_batch_to_sink(&mut state, &mut sink, &batch)));
        let err = err.unwrap_err();

        assert_write_phase(&err, WritePhase::BatchSchemaValidation);
        let Error::ValueConversion { diagnostics } = inner_error(&err) else {
            panic!("expected value conversion error");
        };
        assert_eq!(diagnostics.all()[0].code(), DiagnosticCode::SchemaMismatch);
        assert_eq!(state.stats(), WriteStats::default());
        assert!(sink.rows.is_empty());
        let records = traces.records()?;
        let event = trace_event(&records, BATCH_WRITE_FAILED_EVENT)?;
        assert_eq!(event.span_name(), Some(BATCH_WRITE_SPAN));
        assert_trace_field(event, "phase", BATCH_SCHEMA_VALIDATION_PHASE);
        assert_trace_field(event, "backend", "BaselineTokenRow");
        assert_trace_field(event, "batch_row_count", "1");
        assert_trace_field(event, "batch_column_count", "1");
        assert_trace_field(event, "accepted_rows_before", "0");
        assert_trace_field(event, "accepted_batches_before", "0");
        assert_trace_field(event, "batch_write_result", "failure");
        assert_trace_field(event, "diagnostic_codes", "SchemaMismatch");

        Ok(())
    }

    #[test]
    fn value_conversion_failure_emits_trace_without_accepting_batch() -> Result<(), String> {
        let mappings = vec![float_mapping("amount")];
        let mut state = WriterState::new(
            WriteBackend::BaselineTokenRow,
            SchemaCheck::Strict,
            PlanOptions::default(),
            mappings,
        )
        .unwrap();
        let mut sink = RecordingSink::default();
        let batch = float64_batch("amount", &[Some(1.0), Some(f64::NAN)]);

        let (err, traces) =
            capture_traces(|| poll_ready(write_batch_to_sink(&mut state, &mut sink, &batch)));
        let err = err.unwrap_err();

        assert_write_phase(&err, WritePhase::ValueConversion);
        let Error::ValueConversion { diagnostics } = inner_error(&err) else {
            panic!("expected value conversion error");
        };
        assert_eq!(diagnostics.all()[0].code(), DiagnosticCode::NonFiniteFloat);
        assert_eq!(state.stats(), WriteStats::default());
        assert!(sink.rows.is_empty());
        let records = traces.records()?;
        let event = trace_event(&records, BATCH_WRITE_FAILED_EVENT)?;
        assert_trace_field(event, "phase", VALUE_CONVERSION_PHASE);
        assert_trace_field(event, "diagnostic_codes", "NonFiniteFloat");
        assert_trace_field(
            event,
            "error_summary",
            "value conversion failed with diagnostics",
        );

        Ok(())
    }

    #[test]
    fn packet_write_failure_emits_trace_without_accepting_batch() -> Result<(), String> {
        let mappings = vec![mapping("id")];
        let mut state = WriterState::new(
            WriteBackend::BaselineTokenRow,
            SchemaCheck::Strict,
            PlanOptions::default(),
            mappings,
        )
        .unwrap();
        let mut sink = RecordingSink {
            fail_on_send: Some(0),
            rows: Vec::new(),
        };
        let batch = int32_batch("id", &[1, 2, 3]);

        let (err, traces) =
            capture_traces(|| poll_ready(write_batch_to_sink(&mut state, &mut sink, &batch)));
        let err = err.unwrap_err();

        assert_write_phase(&err, WritePhase::PacketWrite);
        let Error::Tiberius { .. } = inner_error(&err) else {
            panic!("expected tiberius error");
        };
        assert_eq!(state.stats(), WriteStats::default());
        assert!(sink.rows.is_empty());
        let records = traces.records()?;
        let event = trace_event(&records, BATCH_WRITE_FAILED_EVENT)?;
        assert_trace_field(event, "phase", PACKET_WRITE_PHASE);
        assert_trace_field(event, "backend", "BaselineTokenRow");
        assert_trace_field(event, "diagnostic_codes", "");
        assert_trace_field(event, "error_summary", "tiberius operation failed");

        Ok(())
    }

    #[test]
    fn batch_write_trace_does_not_emit_row_values() -> Result<(), String> {
        let mappings = vec![utf8_mapping_at(0, "secret_value")];
        let mut state = WriterState::new(
            WriteBackend::BaselineTokenRow,
            SchemaCheck::Strict,
            PlanOptions::default(),
            mappings,
        )
        .unwrap();
        let mut sink = RecordingSink::default();
        let batch = utf8_batch("secret_value", &["password=secret"]);

        let (_stats, traces) =
            capture_traces(|| poll_ready(write_batch_to_sink(&mut state, &mut sink, &batch)));

        traces.assert_no_forbidden_text(&["password=secret"])?;

        Ok(())
    }

    #[test]
    fn write_direct_batch_to_sink_sends_one_checked_payload_per_batch() {
        let mappings = vec![mapping("id")];
        let mut state = WriterState::new(
            WriteBackend::DirectRawBulk,
            SchemaCheck::Strict,
            PlanOptions::default(),
            mappings,
        )
        .unwrap();
        let mut sink = RecordingRawSink::default();
        let batch = int32_batch("id", &[10, 20]);

        let stats = poll_ready(write_direct_batch_to_sink(&mut state, &mut sink, &batch)).unwrap();

        assert_eq!(
            stats,
            WriteStats {
                rows_written: 2,
                batches_written: 1
            }
        );
        assert_eq!(sink.payloads.len(), 1);
        assert_eq!(sink.payloads[0].row_token_offsets, vec![0, 5]);
        assert_eq!(
            sink.payloads[0].bytes,
            vec![0xD1, 10, 0, 0, 0, 0xD1, 20, 0, 0, 0]
        );
    }

    #[test]
    fn direct_batch_write_success_emits_stats_trace() -> Result<(), String> {
        for backend in [WriteBackend::DirectFramedBulk, WriteBackend::DirectRawBulk] {
            let mappings = vec![mapping("id")];
            let mut state = WriterState::new(
                backend,
                SchemaCheck::Strict,
                PlanOptions::default(),
                mappings,
            )
            .unwrap();
            let mut sink = RecordingRawSink::default();
            let batch = int32_batch("id", &[10, 20]);

            let (stats, traces) = capture_traces(|| {
                poll_ready(write_direct_batch_to_sink(&mut state, &mut sink, &batch))
            });
            let stats = stats.unwrap();

            assert_eq!(
                stats,
                WriteStats {
                    rows_written: 2,
                    batches_written: 1
                }
            );
            let records = traces.records()?;
            let event = trace_event(&records, BATCH_WRITE_COMPLETED_EVENT)?;
            let expected_backend = match backend {
                WriteBackend::DirectFramedBulk => "DirectFramedBulk",
                WriteBackend::DirectRawBulk => "DirectRawBulk",
                WriteBackend::Auto => "Auto",
                WriteBackend::BaselineTokenRow => "BaselineTokenRow",
            };
            assert_trace_field(event, "backend", expected_backend);
            assert_trace_field(event, "batch_row_count", "2");
            assert_trace_field(event, "batch_column_count", "1");
            assert_trace_field(event, "accepted_rows_after", "2");
            assert_trace_field(event, "accepted_batches_after", "1");
        }

        Ok(())
    }

    #[test]
    fn direct_raw_batch_write_emits_encoding_summary_trace() -> Result<(), String> {
        let mappings = vec![mapping("id")];
        let mut state = WriterState::new(
            WriteBackend::DirectRawBulk,
            SchemaCheck::Strict,
            PlanOptions::default(),
            mappings,
        )
        .unwrap();
        let mut sink = RecordingRawSink::default();
        let batch = int32_batch("id", &[10, 20]);

        let (_stats, traces) = capture_traces(|| {
            poll_ready(write_direct_batch_to_sink(&mut state, &mut sink, &batch))
        });

        let records = traces.records()?;
        let measured = trace_event(&records, DIRECT_RAW_MEASURED_EVENT)?;
        assert_trace_field(measured, "phase", DIRECT_ENCODING_PHASE);
        assert_trace_field(measured, "backend", "DirectRawBulk");
        assert_trace_field(measured, "batch_row_count", "2");
        assert_trace_field(measured, "batch_column_count", "1");
        assert_trace_field(measured, "encoded_row_count", "2");
        assert_trace_field(measured, "encoded_byte_count", "10");
        assert!(measured.fields().contains_key("elapsed_us"));

        let ranges = trace_event(&records, DIRECT_RAW_RANGES_PLANNED_EVENT)?;
        assert_trace_field(ranges, "phase", DIRECT_ENCODING_PHASE);
        assert_trace_field(ranges, "encoded_range_count", "1");
        assert_trace_field(ranges, "encoded_byte_count", "10");

        Ok(())
    }

    #[test]
    fn direct_raw_packet_write_emits_sanitized_summary_trace() -> Result<(), String> {
        let mappings = vec![mapping("id")];
        let mut state = WriterState::new(
            WriteBackend::DirectRawBulk,
            SchemaCheck::Strict,
            PlanOptions::default(),
            mappings,
        )
        .unwrap();
        let mut sink = RecordingRawSink::default();
        let batch = int32_batch("id", &[10, 20]);

        let (_stats, traces) = capture_traces(|| {
            poll_ready(write_direct_batch_to_sink(&mut state, &mut sink, &batch))
        });

        let records = traces.records()?;
        let packet = trace_event(&records, DIRECT_RAW_PACKET_WRITE_COMPLETED_EVENT)?;
        assert_trace_field(packet, "phase", PACKET_WRITE_PHASE);
        assert_trace_field(packet, "backend", "DirectRawBulk");
        assert_trace_field(packet, "encoded_row_start", "0");
        assert_trace_field(packet, "encoded_row_count", "2");
        assert_trace_field(packet, "encoded_byte_count", "10");
        assert!(packet.fields().contains_key("elapsed_us"));
        assert!(!packet.fields().contains_key("raw_packet_count"));

        Ok(())
    }

    #[test]
    fn direct_raw_value_conversion_failure_trace_includes_diagnostic_codes() -> Result<(), String> {
        let mappings = vec![schema_mapping_at(
            0,
            "u64_value",
            DataType::UInt64,
            MssqlType::BigInt,
            false,
        )];
        let mut state = WriterState::new(
            WriteBackend::DirectRawBulk,
            SchemaCheck::Strict,
            PlanOptions::default(),
            mappings,
        )
        .unwrap();
        let mut sink = RecordingRawSink::default();
        let batch = uint64_batch("u64_value", &[i64::MAX as u64 + 1]);

        let (err, traces) = capture_traces(|| {
            poll_ready(write_direct_batch_to_sink(&mut state, &mut sink, &batch))
        });
        let err = err.unwrap_err();

        assert_write_phase(&err, WritePhase::ValueConversion);
        let Error::ValueConversion { diagnostics } = inner_error(&err) else {
            panic!("expected value conversion error");
        };
        assert_eq!(
            diagnostics.all()[0].code(),
            DiagnosticCode::IntegerOutOfRange
        );
        let records = traces.records()?;
        let failure = trace_event(&records, BATCH_WRITE_FAILED_EVENT)?;
        assert_eq!(failure.span_name(), Some(BATCH_WRITE_SPAN));
        assert_trace_field(failure, "phase", VALUE_CONVERSION_PHASE);
        assert_trace_field(failure, "backend", "DirectRawBulk");
        assert_trace_field(failure, "batch_row_count", "1");
        assert_trace_field(failure, "batch_column_count", "1");
        assert_trace_field(failure, "diagnostic_codes", "IntegerOutOfRange");
        assert_trace_field(
            failure,
            "error_summary",
            "value conversion failed with diagnostics",
        );

        Ok(())
    }

    #[test]
    fn direct_raw_trace_does_not_emit_values_or_payload_bytes() -> Result<(), String> {
        let mappings = vec![utf8_mapping_at(0, "secret_value")];
        let mut state = WriterState::new(
            WriteBackend::DirectRawBulk,
            SchemaCheck::Strict,
            PlanOptions::default(),
            mappings,
        )
        .unwrap();
        let mut sink = RecordingRawSink::default();
        let batch = utf8_batch("secret_value", &["password=secret"]);

        let (_stats, traces) = capture_traces(|| {
            poll_ready(write_direct_batch_to_sink(&mut state, &mut sink, &batch))
        });

        traces.assert_no_forbidden_text(&["password=secret"])?;

        let mappings = vec![mapping("id")];
        let mut state = WriterState::new(
            WriteBackend::DirectRawBulk,
            SchemaCheck::Strict,
            PlanOptions::default(),
            mappings,
        )
        .unwrap();
        let mut sink = RecordingRawSink::default();
        let batch = int32_batch("id", &[987_654_321]);
        let (_stats, numeric_traces) = capture_traces(|| {
            poll_ready(write_direct_batch_to_sink(&mut state, &mut sink, &batch))
        });

        numeric_traces.assert_no_forbidden_text(&["987654321"])?;

        Ok(())
    }

    #[test]
    fn write_direct_batch_to_sink_accumulates_multi_batch_stats() {
        let mappings = vec![mapping("id")];
        let mut state = WriterState::new(
            WriteBackend::DirectRawBulk,
            SchemaCheck::Strict,
            PlanOptions::default(),
            mappings,
        )
        .unwrap();
        let mut sink = RecordingRawSink::default();

        let first = poll_ready(write_direct_batch_to_sink(
            &mut state,
            &mut sink,
            &int32_batch("id", &[10, 20]),
        ))
        .unwrap();
        let second = poll_ready(write_direct_batch_to_sink(
            &mut state,
            &mut sink,
            &int32_batch("id", &[30]),
        ))
        .unwrap();

        assert_eq!(
            first,
            WriteStats {
                rows_written: 2,
                batches_written: 1
            }
        );
        assert_eq!(
            second,
            WriteStats {
                rows_written: 3,
                batches_written: 2
            }
        );
        assert_eq!(sink.payloads.len(), 2);
        assert_eq!(sink.payloads[1].bytes, vec![0xD1, 30, 0, 0, 0]);
    }

    #[test]
    fn write_direct_batch_to_sink_chunks_measured_payloads_by_byte_limit() {
        let mappings = vec![binary_mapping_at(0, "payload")];
        let mut state = WriterState::new(
            WriteBackend::DirectRawBulk,
            SchemaCheck::Strict,
            PlanOptions::default(),
            mappings,
        )
        .unwrap();
        let mut sink = RecordingRawSink::default();
        let row_bytes = vec![0x5a; DIRECT_RAW_MAX_PAYLOAD_BYTES / 2 + 1];
        let batch = binary_batch("payload", &[row_bytes.as_slice(), row_bytes.as_slice()]);

        let stats = poll_ready(write_direct_batch_to_sink(&mut state, &mut sink, &batch)).unwrap();

        assert_eq!(
            stats,
            WriteStats {
                rows_written: 2,
                batches_written: 1
            }
        );
        assert_eq!(sink.payloads.len(), 2);
        assert_eq!(sink.payloads[0].row_token_offsets, [0]);
        assert_eq!(sink.payloads[1].row_token_offsets, [0]);
    }

    #[test]
    fn write_direct_batch_to_sink_skips_send_for_empty_batch_but_records_stats() {
        let mappings = vec![mapping("id")];
        let mut state = WriterState::new(
            WriteBackend::DirectRawBulk,
            SchemaCheck::Strict,
            PlanOptions::default(),
            mappings,
        )
        .unwrap();
        let mut sink = RecordingRawSink::default();
        let batch = int32_batch("id", &[]);

        let stats = poll_ready(write_direct_batch_to_sink(&mut state, &mut sink, &batch)).unwrap();

        assert_eq!(
            stats,
            WriteStats {
                rows_written: 0,
                batches_written: 1
            }
        );
        assert!(sink.payloads.is_empty());
    }

    #[test]
    fn write_direct_batch_to_sink_rejects_bad_later_row_before_send() {
        let mappings = vec![float_mapping("amount")];
        let mut state = WriterState::new(
            WriteBackend::DirectRawBulk,
            SchemaCheck::Strict,
            PlanOptions::default(),
            mappings,
        )
        .unwrap();
        let mut sink = RecordingRawSink::default();
        let batch = float64_batch("amount", &[Some(1.0), Some(f64::NAN)]);

        let err =
            poll_ready(write_direct_batch_to_sink(&mut state, &mut sink, &batch)).unwrap_err();

        assert_write_phase(&err, WritePhase::ValueConversion);
        let Error::ValueConversion { diagnostics } = inner_error(&err) else {
            panic!("expected value conversion error");
        };
        assert_eq!(diagnostics.all()[0].code(), DiagnosticCode::NonFiniteFloat);
        assert_eq!(diagnostics.all()[0].row(), Some(1));
        assert!(sink.payloads.is_empty());
        assert_eq!(state.stats(), WriteStats::default());
    }

    #[test]
    fn write_direct_batch_to_sink_rejects_uint64_bigint_overflow_before_any_range_send() {
        let mappings = vec![schema_mapping_at(
            0,
            "u64_value",
            DataType::UInt64,
            MssqlType::BigInt,
            false,
        )];
        let mut state = WriterState::new(
            WriteBackend::DirectRawBulk,
            SchemaCheck::Strict,
            PlanOptions::default(),
            mappings,
        )
        .unwrap();
        let mut sink = RecordingRawSink::default();
        let row_count = DIRECT_RAW_MAX_PAYLOAD_BYTES / 9 + 2;
        let mut values = vec![1_u64; row_count];
        values[row_count - 1] = i64::MAX as u64 + 1;
        let batch = uint64_batch("u64_value", &values);

        let err =
            poll_ready(write_direct_batch_to_sink(&mut state, &mut sink, &batch)).unwrap_err();

        assert_write_phase(&err, WritePhase::ValueConversion);
        let Error::ValueConversion { diagnostics } = inner_error(&err) else {
            panic!("expected value conversion error");
        };
        assert_eq!(
            diagnostics.all()[0].code(),
            DiagnosticCode::IntegerOutOfRange
        );
        assert_eq!(diagnostics.all()[0].row(), Some(row_count - 1));
        assert!(sink.payloads.is_empty());
        assert_eq!(state.stats(), WriteStats::default());
    }

    #[test]
    fn write_direct_batch_to_sink_rejects_runtime_type_mismatch_before_send() {
        let mappings = vec![mapping("id")];
        let mut state = WriterState::new(
            WriteBackend::DirectRawBulk,
            SchemaCheck::Strict,
            PlanOptions::default(),
            mappings,
        )
        .unwrap();
        let mut sink = RecordingRawSink::default();
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "id",
                DataType::Float64,
                false,
            )])),
            vec![Arc::new(Float64Array::from(vec![1.0]))],
        )
        .unwrap();

        let err =
            poll_ready(write_direct_batch_to_sink(&mut state, &mut sink, &batch)).unwrap_err();

        assert_write_phase(&err, WritePhase::BatchSchemaValidation);
        let Error::ValueConversion { diagnostics } = inner_error(&err) else {
            panic!("expected value conversion error");
        };
        assert_eq!(diagnostics.all()[0].code(), DiagnosticCode::SchemaMismatch);
        assert!(
            diagnostics.all()[0]
                .message()
                .contains("runtime Arrow type Float64")
        );
        assert!(sink.payloads.is_empty());
        assert_eq!(state.stats(), WriteStats::default());
    }

    #[test]
    fn write_direct_batch_to_sink_send_failure_preserves_error_and_keeps_stats() {
        let mappings = vec![mapping("id")];
        let mut state = WriterState::new(
            WriteBackend::DirectRawBulk,
            SchemaCheck::Strict,
            PlanOptions::default(),
            mappings,
        )
        .unwrap();
        let mut sink = RecordingRawSink {
            fail_on_send: true,
            payloads: Vec::new(),
        };
        let batch = int32_batch("id", &[1, 2, 3]);

        let err =
            poll_ready(write_direct_batch_to_sink(&mut state, &mut sink, &batch)).unwrap_err();

        assert_write_phase(&err, WritePhase::PacketWrite);
        let Error::Tiberius { source } = inner_error(&err) else {
            panic!("expected tiberius error");
        };
        assert_eq!(
            source.to_string(),
            "BULK UPLOAD input failure: fake raw send failure"
        );
        assert!(sink.payloads.is_empty());
        assert_eq!(state.stats(), WriteStats::default());
    }

    #[test]
    fn writer_types_are_exported_from_crate_root() {
        assert_eq!(crate::WriteBackend::default(), WriteBackend::Auto);
        assert_eq!(crate::WriteOptions::default(), WriteOptions::default());
        assert_eq!(crate::WriteStats::default(), WriteStats::default());
        assert_eq!(crate::WritePhase::PacketWrite.as_str(), "packet_write");
        let _ = std::any::type_name::<crate::BulkWriter<'static, DummyStream>>();
    }

    #[test]
    fn tiberius_alias_exposes_client_type() {
        let name = std::any::type_name::<tiberius::Client<DummyStream>>();

        assert!(name.contains("tiberius"));
    }

    fn trace_event<'a>(
        records: &'a [CapturedTrace],
        telemetry_event: &str,
    ) -> Result<&'a CapturedTrace, String> {
        records
            .iter()
            .find(|record| {
                record.kind() == CapturedTraceKind::Event
                    && record
                        .fields()
                        .get("telemetry_event")
                        .is_some_and(|value| value == telemetry_event)
            })
            .ok_or_else(|| format!("missing trace event {telemetry_event}: {records:#?}"))
    }

    fn assert_trace_field(record: &CapturedTrace, field: &str, expected: &str) {
        assert_eq!(
            record.fields().get(field).map(String::as_str),
            Some(expected),
            "trace record: {record:#?}"
        );
    }

    fn assert_write_phase(error: &Error, expected: WritePhase) {
        assert_eq!(error.write_phase(), Some(expected));
    }

    fn inner_error(error: &Error) -> &Error {
        error.without_write_phase()
    }

    fn mapping(name: &str) -> SchemaMapping {
        SchemaMapping::new(
            ArrowFieldRef::new(0, name.to_owned(), false, DataType::Int32),
            MssqlColumn::new(Identifier::new(name).unwrap(), MssqlType::Int, false),
        )
    }

    fn schema_mapping_at(
        index: usize,
        name: &str,
        arrow_type: DataType,
        mssql_type: MssqlType,
        nullable: bool,
    ) -> SchemaMapping {
        SchemaMapping::new(
            ArrowFieldRef::new(index, name.to_owned(), nullable, arrow_type),
            MssqlColumn::new(Identifier::new(name).unwrap(), mssql_type, nullable),
        )
    }

    fn float_mapping(name: &str) -> SchemaMapping {
        float_mapping_at(0, name)
    }

    fn float_mapping_at(index: usize, name: &str) -> SchemaMapping {
        SchemaMapping::new(
            ArrowFieldRef::new(index, name.to_owned(), false, DataType::Float64),
            MssqlColumn::new(
                Identifier::new(name).unwrap(),
                MssqlType::Float { precision: 53 },
                false,
            ),
        )
    }

    fn utf8_mapping_at(index: usize, name: &str) -> SchemaMapping {
        SchemaMapping::new(
            ArrowFieldRef::new(index, name.to_owned(), false, DataType::Utf8),
            MssqlColumn::new(
                Identifier::new(name).unwrap(),
                MssqlType::NVarChar(MssqlTypeLength::Max),
                false,
            ),
        )
    }

    fn binary_mapping_at(index: usize, name: &str) -> SchemaMapping {
        SchemaMapping::new(
            ArrowFieldRef::new(index, name.to_owned(), false, DataType::Binary),
            MssqlColumn::new(
                Identifier::new(name).unwrap(),
                MssqlType::VarBinary(MssqlTypeLength::Max),
                false,
            ),
        )
    }

    fn fixed_size_binary_mapping_at(index: usize, name: &str, length: usize) -> SchemaMapping {
        SchemaMapping::new(
            ArrowFieldRef::new(
                index,
                name.to_owned(),
                false,
                DataType::FixedSizeBinary(i32::try_from(length).unwrap()),
            ),
            MssqlColumn::new(
                Identifier::new(name).unwrap(),
                MssqlType::Binary(length),
                false,
            ),
        )
    }

    fn int32_batch(name: &str, values: &[i32]) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new(name, DataType::Int32, false)]));
        let array = Arc::new(Int32Array::from(values.to_vec()));

        RecordBatch::try_new(schema, vec![array]).unwrap()
    }

    fn uint64_batch(name: &str, values: &[u64]) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new(name, DataType::UInt64, false)]));
        let array = Arc::new(UInt64Array::from(values.to_vec()));

        RecordBatch::try_new(schema, vec![array]).unwrap()
    }

    fn binary_batch(name: &str, values: &[&[u8]]) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new(name, DataType::Binary, false)]));
        let array = Arc::new(BinaryArray::from_iter_values(values.iter().copied()));

        RecordBatch::try_new(schema, vec![array]).unwrap()
    }

    fn utf8_batch(name: &str, values: &[&str]) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new(name, DataType::Utf8, false)]));
        let array = Arc::new(StringArray::from(values.to_vec()));

        RecordBatch::try_new(schema, vec![array]).unwrap()
    }

    fn bulk_target_column(ordinal: usize, name: &str, nullable: bool) -> FakeBulkTargetColumn {
        bulk_target_column_with_type(ordinal, name, nullable, tiberius::ColumnType::Int4)
    }

    fn bulk_target_column_with_type(
        ordinal: usize,
        name: &str,
        nullable: bool,
        column_type: tiberius::ColumnType,
    ) -> FakeBulkTargetColumn {
        FakeBulkTargetColumn {
            ordinal,
            name: name.to_owned(),
            nullable,
            column_type,
            decimal_precision_scale: None,
        }
    }

    fn bulk_target_decimal_column(
        ordinal: usize,
        name: &str,
        nullable: bool,
        precision: u8,
        scale: u8,
    ) -> FakeBulkTargetColumn {
        FakeBulkTargetColumn {
            ordinal,
            name: name.to_owned(),
            nullable,
            column_type: tiberius::ColumnType::Decimaln,
            decimal_precision_scale: Some((precision, scale)),
        }
    }

    fn float64_batch(name: &str, values: &[Option<f64>]) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new(
            name,
            DataType::Float64,
            false,
        )]));
        let array = Arc::new(Float64Array::from(values.to_vec()));

        RecordBatch::try_new(schema, vec![array]).unwrap()
    }

    fn poll_ready<F>(future: F) -> F::Output
    where
        F: Future,
    {
        let mut context = Context::from_waker(Waker::noop());
        let mut future = Box::pin(future);

        match future.as_mut().poll(&mut context) {
            Poll::Ready(output) => output,
            Poll::Pending => panic!("future unexpectedly returned pending"),
        }
    }

    #[derive(Debug, Default)]
    struct RecordingSink {
        fail_on_send: Option<usize>,
        rows: Vec<tiberius::TokenRow<'static>>,
    }

    #[derive(Debug, Default)]
    struct RecordingRawSink {
        fail_on_send: bool,
        payloads: Vec<RecordedRawPayload>,
    }

    #[derive(Debug, Default)]
    struct RecordingFinishSink {
        fail_message: Option<&'static str>,
    }

    #[derive(Debug, PartialEq, Eq)]
    struct RecordedRawPayload {
        bytes: Vec<u8>,
        row_token_offsets: Vec<usize>,
    }

    impl RawRowsSink for RecordingRawSink {
        async fn send_measured_raw_rows(
            &mut self,
            backend: WriteBackend,
            encoder: &DirectEncoder,
            batch: &RecordBatch,
            measured: &MeasuredDirectBatch,
            range: MeasuredRowRange,
        ) -> crate::Result<()> {
            let payload =
                encoder.encode_measured_batch_range(batch, measured, range.start, range.len)?;

            if self.fail_on_send {
                return Err(Error::Tiberius {
                    source: tiberius::error::Error::BulkInput(Cow::Borrowed(
                        "fake raw send failure",
                    )),
                });
            }

            self.payloads.push(RecordedRawPayload {
                bytes: payload.bytes().to_vec(),
                row_token_offsets: payload.row_token_offsets().to_vec(),
            });
            emit_direct_raw_packet_write_completed(
                backend,
                range,
                payload.row_count(),
                payload.bytes().len(),
                std::time::Duration::ZERO,
            );
            Ok(())
        }
    }

    impl FinishSink for RecordingFinishSink {
        async fn finalize_bulk_load(self) -> crate::Result<()> {
            match self.fail_message {
                Some(message) => Err(Error::Tiberius {
                    source: tiberius::error::Error::BulkInput(Cow::Borrowed(message)),
                }),
                None => Ok(()),
            }
        }
    }

    impl TokenRowSink for RecordingSink {
        async fn send_token_row(&mut self, row: tiberius::TokenRow<'static>) -> crate::Result<()> {
            if self.fail_on_send == Some(self.rows.len()) {
                return Err(Error::Tiberius {
                    source: tiberius::error::Error::BulkInput(Cow::Borrowed("fake send failure")),
                });
            }

            self.rows.push(row);
            Ok(())
        }
    }

    #[derive(Debug)]
    struct FakeBulkTargetColumn {
        ordinal: usize,
        name: String,
        nullable: bool,
        column_type: tiberius::ColumnType,
        decimal_precision_scale: Option<(u8, u8)>,
    }

    impl BulkTargetColumnMetadata for FakeBulkTargetColumn {
        fn ordinal(&self) -> usize {
            self.ordinal
        }

        fn name(&self) -> &str {
            &self.name
        }

        fn is_nullable(&self) -> bool {
            self.nullable
        }

        fn column_type(&self) -> tiberius::ColumnType {
            self.column_type
        }

        fn decimal_precision_scale(&self) -> Option<(u8, u8)> {
            self.decimal_precision_scale
        }
    }

    #[derive(Debug)]
    struct DummyStream;

    impl AsyncRead for DummyStream {
        fn poll_read(
            self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
            _buf: &mut [u8],
        ) -> Poll<std::io::Result<usize>> {
            Poll::Ready(Ok(0))
        }
    }

    impl AsyncWrite for DummyStream {
        fn poll_write(
            self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
            buf: &[u8],
        ) -> Poll<std::io::Result<usize>> {
            Poll::Ready(Ok(buf.len()))
        }

        fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
            Poll::Ready(Ok(()))
        }

        fn poll_close(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
            Poll::Ready(Ok(()))
        }
    }
}

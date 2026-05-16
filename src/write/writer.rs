//! Baseline bulk writer public API skeleton.

use arrow_array::RecordBatch;
use futures_util::io::{AsyncRead, AsyncWrite};

use crate::{
    Diagnostic, DiagnosticCode, DiagnosticSet, FieldRef, PlanOptions, Result, SchemaMapping,
    TableName,
};

use super::{SchemaCheck, convert::RecordBatchView};

/// Write backend selection.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub enum WriteBackend {
    /// Select the best available backend for the current crate build and plan.
    #[default]
    Auto,
    /// Use Tiberius' row-oriented `TokenRow` bulk-load path.
    BaselineTokenRow,
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
struct WriterState {
    backend: WriteBackend,
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

        Ok(Self {
            backend,
            schema_check,
            plan_options,
            mappings,
            stats: WriteStats::default(),
        })
    }

    fn backend(&self) -> WriteBackend {
        self.backend
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
        let state = WriterState::new(
            options.backend,
            options.schema_check,
            options.plan_options,
            mappings,
        )?;
        let request = match state.backend() {
            WriteBackend::BaselineTokenRow => {
                let table_sql = bulk_insert_table_sql(&table);
                let columns = client
                    .bulk_insert_columns(&table_sql)
                    .await
                    .map_err(|source| crate::Error::Tiberius { source })?;
                validate_bulk_target_columns(columns.iter(), state.mappings())?;
                client
                    .bulk_insert_with_columns(&table_sql, columns)
                    .await
                    .map_err(|source| crate::Error::Tiberius { source })?
            }
            WriteBackend::Auto | WriteBackend::DirectRawBulk => {
                return Err(execution_unavailable(state.backend()));
            }
        };

        Ok(Self { state, request })
    }

    /// Writes one Arrow record batch.
    pub async fn write_batch(&mut self, batch: &RecordBatch) -> Result<WriteStats> {
        write_batch_to_sink(&mut self.state, &mut self.request, batch).await
    }

    /// Finalizes the bulk writer and returns cumulative write statistics.
    pub async fn finish(self) -> Result<WriteStats> {
        let Self { state, request } = self;
        let stats = state.stats();

        request
            .finalize()
            .await
            .map_err(|source| crate::Error::Tiberius { source })?;

        Ok(stats)
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
}

async fn write_batch_to_sink<Sink>(
    state: &mut WriterState,
    sink: &mut Sink,
    batch: &RecordBatch,
) -> Result<WriteStats>
where
    Sink: TokenRowSink,
{
    let view = record_batch_view(
        batch,
        state.mappings(),
        state.schema_check(),
        state.plan_options(),
    )?;
    validate_batch_rows(&view)?;
    let rows_written = usize_to_u64_saturating(view.row_count());

    for row_index in 0..view.row_count() {
        let row = view.tiberius_row_owned(row_index)?;
        sink.send_token_row(row).await?;
    }

    Ok(state.record_accepted_batch(rows_written))
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

fn usize_to_u64_saturating(value: usize) -> u64 {
    u64::try_from(value).unwrap_or(u64::MAX)
}

fn resolve_backend(requested_backend: WriteBackend) -> Result<WriteBackend> {
    match requested_backend {
        WriteBackend::Auto | WriteBackend::BaselineTokenRow => Ok(WriteBackend::BaselineTokenRow),
        WriteBackend::DirectRawBulk => Err(crate::Error::BackendUnavailable {
            backend: WriteBackend::DirectRawBulk,
            reason: "direct raw bulk backend is not implemented yet".to_owned(),
        }),
    }
}

fn execution_unavailable(backend: WriteBackend) -> crate::Error {
    crate::Error::BackendUnavailable {
        backend,
        reason: "bulk writer execution is not implemented yet".to_owned(),
    }
}

#[cfg(test)]
mod tests {
    use std::{
        borrow::Cow,
        future::Future,
        pin::Pin,
        sync::Arc,
        task::{Context, Poll, Wake, Waker},
    };

    use arrow_array::{Float64Array, Int32Array, RecordBatch};
    use arrow_schema::{DataType, Field, Schema};
    use futures_util::io::{AsyncRead, AsyncWrite};

    use super::{
        BulkTargetColumnMetadata, TokenRowSink, WriteBackend, WriteOptions, WriteStats,
        WriterState, bulk_insert_table_sql, record_batch_view, resolve_backend,
        validate_batch_rows, validate_bulk_target_columns, write_batch_to_sink,
    };
    use crate::{
        ArrowFieldRef, DiagnosticCode, Error, Identifier, MssqlColumn, MssqlType, PlanOptions,
        SchemaCheck, SchemaMapping, TableName,
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
    fn auto_backend_resolves_to_baseline_token_row() {
        assert_eq!(
            resolve_backend(WriteBackend::Auto).unwrap(),
            WriteBackend::BaselineTokenRow
        );
        assert_eq!(
            resolve_backend(WriteBackend::BaselineTokenRow).unwrap(),
            WriteBackend::BaselineTokenRow
        );
    }

    #[test]
    fn direct_raw_bulk_resolution_fails_until_direct_backend_exists() {
        let result = resolve_backend(WriteBackend::DirectRawBulk);

        assert_backend_unavailable_reason(
            result,
            WriteBackend::DirectRawBulk,
            "direct raw bulk backend is not implemented yet",
        );
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

        assert_eq!(state.backend(), WriteBackend::BaselineTokenRow);
        assert_eq!(state.schema_check(), SchemaCheck::Strict);
        assert_eq!(state.mappings(), mappings.as_slice());
        assert_eq!(state.stats(), WriteStats::default());
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

        let row = view.tiberius_row_owned(1).unwrap();
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

        let Error::ValueConversion { diagnostics } = err else {
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

        let Error::Tiberius { source } = err else {
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
    fn writer_types_are_exported_from_crate_root() {
        assert_eq!(crate::WriteBackend::default(), WriteBackend::Auto);
        assert_eq!(crate::WriteOptions::default(), WriteOptions::default());
        assert_eq!(crate::WriteStats::default(), WriteStats::default());
        let _ = std::any::type_name::<crate::BulkWriter<'static, DummyStream>>();
    }

    #[test]
    fn tiberius_alias_exposes_client_type() {
        let name = std::any::type_name::<tiberius::Client<DummyStream>>();

        assert!(name.contains("tiberius"));
    }

    fn mapping(name: &str) -> SchemaMapping {
        SchemaMapping::new(
            ArrowFieldRef::new(0, name.to_owned(), false, DataType::Int32),
            MssqlColumn::new(Identifier::new(name).unwrap(), MssqlType::Int, false),
        )
    }

    fn float_mapping(name: &str) -> SchemaMapping {
        SchemaMapping::new(
            ArrowFieldRef::new(0, name.to_owned(), false, DataType::Float64),
            MssqlColumn::new(
                Identifier::new(name).unwrap(),
                MssqlType::Float { precision: 53 },
                false,
            ),
        )
    }

    fn int32_batch(name: &str, values: &[i32]) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new(name, DataType::Int32, false)]));
        let array = Arc::new(Int32Array::from(values.to_vec()));

        RecordBatch::try_new(schema, vec![array]).unwrap()
    }

    fn bulk_target_column(ordinal: usize, name: &str, nullable: bool) -> FakeBulkTargetColumn {
        FakeBulkTargetColumn {
            ordinal,
            name: name.to_owned(),
            nullable,
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

    fn assert_backend_unavailable_reason<T: std::fmt::Debug>(
        result: crate::Result<T>,
        expected: WriteBackend,
        expected_reason: &str,
    ) {
        match result {
            Err(crate::Error::BackendUnavailable { backend, reason }) => {
                assert_eq!(backend, expected);
                assert_eq!(reason, expected_reason);
            }
            other => panic!("expected backend-unavailable error, got {other:?}"),
        }
    }

    fn poll_ready<F>(future: F) -> F::Output
    where
        F: Future,
    {
        let waker = Waker::from(Arc::new(NoopWake));
        let mut context = Context::from_waker(&waker);
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
    }

    struct NoopWake;

    impl Wake for NoopWake {
        fn wake(self: Arc<Self>) {}
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

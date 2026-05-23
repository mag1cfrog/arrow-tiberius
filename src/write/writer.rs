//! Baseline bulk writer public API skeleton.

use std::borrow::Cow;

use arrow_array::RecordBatch;
use futures_util::io::{AsyncRead, AsyncWrite};

use crate::{
    Diagnostic, DiagnosticCode, DiagnosticSet, FieldRef, PlanOptions, Result, SchemaMapping,
    TableName,
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
    primitive::PrimitiveArrowToMssql, variable_width::VariableWidthArrowToMssql,
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
                Some(DirectEncoder::new(&mappings)?)
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
        let state = WriterState::new(
            options.backend,
            options.schema_check,
            options.plan_options,
            mappings,
        )?;
        let mut request = match state.backend() {
            WriteBackend::BaselineTokenRow
            | WriteBackend::DirectFramedBulk
            | WriteBackend::DirectRawBulk => {
                let table_sql = bulk_insert_table_sql(&table);
                let columns = client
                    .bulk_insert_columns(&table_sql)
                    .await
                    .map_err(|source| crate::Error::Tiberius { source })?;
                validate_bulk_target_columns(columns.iter(), state.mappings())?;
                if matches!(
                    state.backend(),
                    WriteBackend::DirectFramedBulk | WriteBackend::DirectRawBulk
                ) {
                    let encoder =
                        state
                            .direct_encoder()
                            .ok_or_else(|| crate::Error::BackendUnavailable {
                                backend: state.backend(),
                                reason: "direct bulk encoder is not available for this writer"
                                    .to_owned(),
                            })?;
                    validate_direct_bulk_target_column_types(columns.iter(), encoder.plan())?;
                }
                client
                    .bulk_insert_with_columns(&table_sql, columns)
                    .await
                    .map_err(|source| crate::Error::Tiberius { source })?
            }
            WriteBackend::Auto => {
                return Err(execution_unavailable(state.backend()));
            }
        };

        if state.backend() == WriteBackend::DirectRawBulk {
            request.enable_direct_packet_writes();
        }

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
        let stats = state.stats();

        #[cfg(feature = "bench-profile")]
        {
            let (_result, stats) = request
                .finalize_with_stats()
                .await
                .map_err(|source| crate::Error::Tiberius { source })?;
            profile::record_bulk_load_stats(stats);
        }

        #[cfg(not(feature = "bench-profile"))]
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
        DirectColumnEncoding::Primitive(PrimitiveArrowToMssql::Float32ToReal) => {
            Some(tiberius::ColumnType::Float4)
        }
        DirectColumnEncoding::Primitive(PrimitiveArrowToMssql::Float64ToFloat) => {
            Some(tiberius::ColumnType::Float8)
        }
        DirectColumnEncoding::UInt64Decimal20_0 => Some(tiberius::ColumnType::Decimaln),
        DirectColumnEncoding::VariableWidth(VariableWidthArrowToMssql::Utf8ToNVarChar {
            ..
        }) => Some(tiberius::ColumnType::NVarchar),
        DirectColumnEncoding::VariableWidth(VariableWidthArrowToMssql::BinaryToVarBinary {
            ..
        }) => Some(tiberius::ColumnType::BigVarBin),
        DirectColumnEncoding::VariableWidth(_) => None,
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
        let row = tiberius_row_owned(&view, row_index)?;
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

async fn write_direct_batch_to_sink<Sink>(
    state: &mut WriterState,
    sink: &mut Sink,
    batch: &RecordBatch,
) -> Result<WriteStats>
where
    Sink: RawRowsSink,
{
    let encoder = state
        .direct_encoder()
        .ok_or_else(|| crate::Error::BackendUnavailable {
            backend: state.backend(),
            reason: "direct bulk encoder is not available for this writer".to_owned(),
        })?;
    let measure_start = std::time::Instant::now();
    let measured = encoder.measure_batch(batch);
    let measured = profile::record_elapsed(measure_start, profile::record_measure_batch, measured)?;
    let rows_written = usize_to_u64_saturating(measured.row_count());

    let split_start = std::time::Instant::now();
    let ranges = measured.row_ranges(DIRECT_RAW_MAX_PAYLOAD_BYTES);
    let ranges = profile::record_elapsed(split_start, profile::record_row_range_split, ranges)?;

    for range in ranges {
        sink.send_measured_raw_rows(encoder, batch, &measured, range)
            .await?;
    }

    profile::record_accepted_batch(measured.row_count());
    Ok(state.record_accepted_batch(rows_written))
}

trait RawRowsSink {
    async fn send_measured_raw_rows(
        &mut self,
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

        send_result.map_err(|source| crate::Error::Tiberius { source })
    }
}

fn usize_to_u64_saturating(value: usize) -> u64 {
    u64::try_from(value).unwrap_or(u64::MAX)
}

fn resolve_backend(requested_backend: WriteBackend) -> Result<WriteBackend> {
    match requested_backend {
        WriteBackend::Auto | WriteBackend::BaselineTokenRow => Ok(WriteBackend::BaselineTokenRow),
        WriteBackend::DirectFramedBulk => Ok(WriteBackend::DirectFramedBulk),
        WriteBackend::DirectRawBulk => Ok(WriteBackend::DirectRawBulk),
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

    use arrow_array::{BinaryArray, Float64Array, Int32Array, RecordBatch};
    use arrow_schema::{DataType, Field, Schema};
    use futures_util::io::{AsyncRead, AsyncWrite};

    use super::{
        BulkTargetColumnMetadata, DIRECT_RAW_MAX_PAYLOAD_BYTES, DirectEncoder, MeasuredDirectBatch,
        MeasuredRowRange, RawRowsSink, TokenRowSink, WriteBackend, WriteOptions, WriteStats,
        WriterState, bulk_insert_table_sql, record_batch_view, resolve_backend, tiberius_row_owned,
        validate_batch_rows, validate_bulk_target_columns,
        validate_direct_bulk_target_column_types, write_batch_to_sink, write_direct_batch_to_sink,
    };
    use crate::{
        ArrowFieldRef, DiagnosticCode, Error, Identifier, MssqlColumn, MssqlType, MssqlTypeLength,
        PlanOptions, SchemaCheck, SchemaMapping, TableName,
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
    fn direct_bulk_backends_resolve_to_requested_backend() {
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
        assert!(state.direct_encoder().is_none());
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
            ArrowFieldRef::new(0, "created_on".to_owned(), true, DataType::Date32),
            MssqlColumn::new(
                Identifier::new("created_on").unwrap(),
                MssqlType::Date,
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

        let Error::ValueConversion { diagnostics } = err else {
            panic!("expected value conversion error");
        };
        assert_eq!(diagnostics.all()[0].code(), DiagnosticCode::NonFiniteFloat);
        assert_eq!(diagnostics.all()[0].row(), Some(1));
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

        let Error::ValueConversion { diagnostics } = err else {
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

    fn int32_batch(name: &str, values: &[i32]) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new(name, DataType::Int32, false)]));
        let array = Arc::new(Int32Array::from(values.to_vec()));

        RecordBatch::try_new(schema, vec![array]).unwrap()
    }

    fn binary_batch(name: &str, values: &[&[u8]]) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new(name, DataType::Binary, false)]));
        let array = Arc::new(BinaryArray::from_iter_values(values.iter().copied()));

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

    #[derive(Debug, Default)]
    struct RecordingRawSink {
        fail_on_send: bool,
        payloads: Vec<RecordedRawPayload>,
    }

    #[derive(Debug, PartialEq, Eq)]
    struct RecordedRawPayload {
        bytes: Vec<u8>,
        row_token_offsets: Vec<usize>,
    }

    impl RawRowsSink for RecordingRawSink {
        async fn send_measured_raw_rows(
            &mut self,
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
            Ok(())
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

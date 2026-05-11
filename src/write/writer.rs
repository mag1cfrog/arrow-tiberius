//! Baseline bulk writer public API skeleton.

use std::marker::PhantomData;

use arrow_array::RecordBatch;
use futures_util::io::{AsyncRead, AsyncWrite};

use crate::{Result, SchemaMapping, TableName};

use super::SchemaCheck;

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
    mappings: Vec<SchemaMapping>,
    stats: WriteStats,
}

impl WriterState {
    fn new(requested_backend: WriteBackend, mappings: Vec<SchemaMapping>) -> Result<Self> {
        let backend = resolve_backend(requested_backend)?;

        Ok(Self {
            backend,
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

    fn stats(&self) -> WriteStats {
        self.stats
    }

    #[cfg(test)]
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
    _client: PhantomData<&'client mut tiberius::Client<S>>,
}

impl<'client, S> BulkWriter<'client, S>
where
    S: AsyncRead + AsyncWrite + Unpin + Send,
{
    /// Starts a bulk writer for a planned SQL Server table target.
    pub async fn new(
        _client: &'client mut tiberius::Client<S>,
        _table: TableName,
        mappings: Vec<SchemaMapping>,
        options: WriteOptions,
    ) -> Result<Self> {
        let state = WriterState::new(options.backend, mappings)?;

        Err(execution_unavailable(state.backend()))
    }

    /// Writes one Arrow record batch.
    pub async fn write_batch(&mut self, _batch: &RecordBatch) -> Result<WriteStats> {
        let _planned_column_count = self.state.mappings().len();

        Err(execution_unavailable(self.state.backend()))
    }

    /// Finalizes the bulk writer and returns cumulative write statistics.
    pub async fn finish(self) -> Result<WriteStats> {
        let _stats = self.state.stats();

        Err(execution_unavailable(self.state.backend()))
    }
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
        future::Future,
        marker::PhantomData,
        pin::Pin,
        sync::Arc,
        task::{Context, Poll, Wake, Waker},
    };

    use arrow_array::RecordBatch;
    use arrow_schema::{DataType, Schema};
    use futures_util::io::{AsyncRead, AsyncWrite};

    use super::{BulkWriter, WriteBackend, WriteOptions, WriteStats, WriterState, resolve_backend};
    use crate::{ArrowFieldRef, Identifier, MssqlColumn, MssqlType, SchemaCheck, SchemaMapping};

    #[test]
    fn write_backend_defaults_to_auto() {
        assert_eq!(WriteBackend::default(), WriteBackend::Auto);
    }

    #[test]
    fn write_options_default_to_auto_backend_and_strict_schema_check() {
        let options = WriteOptions::default();

        assert_eq!(options.backend, WriteBackend::Auto);
        assert_eq!(options.schema_check, SchemaCheck::Strict);
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

        let state = WriterState::new(WriteBackend::Auto, mappings.clone()).unwrap();

        assert_eq!(state.backend(), WriteBackend::BaselineTokenRow);
        assert_eq!(state.mappings(), mappings.as_slice());
        assert_eq!(state.stats(), WriteStats::default());
    }

    #[test]
    fn writer_state_accumulates_accepted_batch_stats() {
        let mut state = WriterState::new(WriteBackend::BaselineTokenRow, Vec::new()).unwrap();

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

    #[test]
    fn write_batch_rejects_baseline_until_execution_is_implemented() {
        let batch = empty_batch();
        let mut writer = skeleton_writer(WriteBackend::BaselineTokenRow);
        let result = poll_ready(writer.write_batch(&batch));

        assert_execution_unavailable(result, WriteBackend::BaselineTokenRow);
    }

    #[test]
    fn finish_rejects_baseline_until_execution_is_implemented() {
        let writer = skeleton_writer(WriteBackend::BaselineTokenRow);
        let result = poll_ready(writer.finish());

        assert_execution_unavailable(result, WriteBackend::BaselineTokenRow);
    }

    fn skeleton_writer(backend: WriteBackend) -> BulkWriter<'static, DummyStream> {
        BulkWriter {
            state: WriterState {
                backend,
                mappings: Vec::new(),
                stats: WriteStats::default(),
            },
            _client: PhantomData,
        }
    }

    fn empty_batch() -> RecordBatch {
        RecordBatch::new_empty(Arc::new(Schema::empty()))
    }

    fn mapping(name: &str) -> SchemaMapping {
        SchemaMapping::new(
            ArrowFieldRef::new(0, name.to_owned(), false, DataType::Int32),
            MssqlColumn::new(Identifier::new(name).unwrap(), MssqlType::Int, false),
        )
    }

    fn assert_execution_unavailable(result: crate::Result<WriteStats>, expected: WriteBackend) {
        assert_backend_unavailable_reason(
            result,
            expected,
            "bulk writer execution is not implemented yet",
        );
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

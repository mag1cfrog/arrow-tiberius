//! Baseline bulk writer public API skeleton.

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
        let state = WriterState::new(options.backend, mappings)?;
        let request = match state.backend() {
            WriteBackend::BaselineTokenRow => {
                let table_sql = bulk_insert_table_sql(&table);
                client
                    .bulk_insert(&table_sql)
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
    pub async fn write_batch(&mut self, _batch: &RecordBatch) -> Result<WriteStats> {
        let _planned_column_count = self.state.mappings().len();
        let _request = &mut self.request;

        Err(execution_unavailable(self.state.backend()))
    }

    /// Finalizes the bulk writer and returns cumulative write statistics.
    pub async fn finish(self) -> Result<WriteStats> {
        let Self {
            state,
            request: _request,
        } = self;
        let _stats = state.stats();

        Err(execution_unavailable(state.backend()))
    }
}

fn bulk_insert_table_sql(table: &TableName) -> String {
    table.quoted_sql()
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
        pin::Pin,
        task::{Context, Poll},
    };

    use arrow_schema::DataType;
    use futures_util::io::{AsyncRead, AsyncWrite};

    use super::{
        WriteBackend, WriteOptions, WriteStats, WriterState, bulk_insert_table_sql, resolve_backend,
    };
    use crate::{
        ArrowFieldRef, Identifier, MssqlColumn, MssqlType, SchemaCheck, SchemaMapping, TableName,
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
    fn bulk_insert_table_sql_uses_quoted_table_name() {
        let table = TableName::new("dbo]x", "target.table").unwrap();

        assert_eq!(bulk_insert_table_sql(&table), "[dbo]]x].[target.table]");
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

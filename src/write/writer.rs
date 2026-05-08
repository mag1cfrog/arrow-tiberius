//! Baseline bulk writer public API skeleton.

use std::marker::PhantomData;

use arrow_array::RecordBatch;
use futures_util::io::{AsyncRead, AsyncWrite};

use crate::{MssqlTablePlan, Result, TableName};

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

/// SQL Server bulk writer for Arrow record batches.
#[derive(Debug)]
pub struct BulkWriter<'client, S>
where
    S: AsyncRead + AsyncWrite + Unpin + Send,
{
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
        _plan: MssqlTablePlan,
        options: WriteOptions,
    ) -> Result<Self> {
        Err(crate::Error::BackendUnavailable {
            backend: options.backend,
            reason: "bulk writer execution is not implemented yet".to_owned(),
        })
    }

    /// Writes one Arrow record batch.
    pub async fn write_batch(&mut self, _batch: &RecordBatch) -> Result<WriteStats> {
        Err(crate::Error::BackendUnavailable {
            backend: WriteBackend::Auto,
            reason: "bulk writer execution is not implemented yet".to_owned(),
        })
    }

    /// Finalizes the bulk writer and returns cumulative write statistics.
    pub async fn finish(self) -> Result<WriteStats> {
        Err(crate::Error::BackendUnavailable {
            backend: WriteBackend::Auto,
            reason: "bulk writer execution is not implemented yet".to_owned(),
        })
    }
}

#[cfg(test)]
mod tests {
    use std::{
        pin::Pin,
        task::{Context, Poll},
    };

    use futures_util::io::{AsyncRead, AsyncWrite};

    use super::{WriteBackend, WriteOptions, WriteStats};
    use crate::{BulkWriter, SchemaCheck};

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
    fn write_stats_default_to_zero() {
        let stats = WriteStats::default();

        assert_eq!(stats.rows_written, 0);
        assert_eq!(stats.batches_written, 0);
    }

    #[test]
    fn writer_types_are_exported_from_crate_root() {
        assert_eq!(crate::WriteBackend::default(), WriteBackend::Auto);
        assert_eq!(crate::WriteOptions::default(), WriteOptions::default());
        assert_eq!(crate::WriteStats::default(), WriteStats::default());
        let _ = std::any::type_name::<BulkWriter<'static, DummyStream>>();
    }

    #[test]
    fn tiberius_alias_exposes_client_type() {
        let name = std::any::type_name::<tiberius::Client<DummyStream>>();

        assert!(name.contains("tiberius"));
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

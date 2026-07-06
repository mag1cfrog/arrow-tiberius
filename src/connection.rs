//! SQL Server connection helpers.

use std::fmt;

use arrow_array::RecordBatch;
use tokio::net::TcpStream;
use tokio_util::compat::{Compat, TokioAsyncWriteCompatExt};

use crate::{BulkWriter, Error, PlannedSchema, Result, TableName, WriteOptions, WriteStats};

type CompatibleMssqlTransport = Compat<TcpStream>;

/// Opaque SQL Server client constructed with this crate's compatible Tiberius dependency.
///
/// Use [`connect_mssql_client_from_ado_string`] to create this type. Its
/// concrete Tiberius client and async transport types are intentionally hidden
/// so downstream crates do not have to name or match `tiberius-raw-bulk`
/// directly.
pub struct ConnectedMssqlClient {
    client: tiberius::Client<CompatibleMssqlTransport>,
}

/// Bulk writer created from a [`ConnectedMssqlClient`].
///
/// This wrapper keeps the compatible Tiberius client and transport types out of
/// downstream signatures while exposing the same write and finish operations as
/// [`BulkWriter`].
pub struct ConnectedBulkWriter<'client> {
    writer: BulkWriter<'client, CompatibleMssqlTransport>,
}

impl fmt::Debug for ConnectedMssqlClient {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ConnectedMssqlClient")
            .finish_non_exhaustive()
    }
}

impl fmt::Debug for ConnectedBulkWriter<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ConnectedBulkWriter")
            .finish_non_exhaustive()
    }
}

/// Metadata returned after executing SQL through a connected client.
///
/// This type is part of the narrow lifecycle SQL API. Statement execution is
/// added separately from connection construction so connection setup can remain
/// independently reviewable.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct SqlExecutionOutcome {
    /// Row counts reported by SQL Server DONE tokens, in server result order.
    pub rows_affected: Vec<u64>,
}

impl SqlExecutionOutcome {
    /// Returns the sum of all reported affected-row counts.
    pub fn total_rows_affected(&self) -> u64 {
        self.rows_affected.iter().copied().sum()
    }
}

impl ConnectedMssqlClient {
    /// Returns whether the target table exists in SQL Server metadata.
    ///
    /// This is a narrow metadata probe, not a generic query API. For
    /// schema-qualified names it checks the exact schema and table. For
    /// unqualified names it checks whether any table with that name exists in
    /// the current database.
    pub async fn table_exists(&mut self, table: &TableName) -> Result<bool> {
        let query = table_exists_query(table);
        let row = self
            .client
            .simple_query(query)
            .await
            .map_err(|source| Error::TableExistsQuery { source })?
            .into_row()
            .await
            .map_err(|source| Error::TableExistsQuery { source })?
            .ok_or_else(|| Error::TableExistsUnexpectedResult {
                reason: "metadata query returned no rows".to_owned(),
            })?;

        row.try_get("exists")
            .map_err(|source| Error::TableExistsQuery { source })?
            .ok_or_else(|| Error::TableExistsUnexpectedResult {
                reason: "metadata query returned NULL".to_owned(),
            })
    }

    /// Returns `COUNT_BIG(*)` for a target table.
    ///
    /// The query uses this crate's bracket-quoted [`TableName`] rendering and
    /// returns only a checked `u64` count. It does not expose raw SQL text,
    /// result rows, or the underlying Tiberius client type.
    pub async fn target_row_count(&mut self, table: &TableName) -> Result<u64> {
        let query = target_row_count_query(table);
        let row = self
            .client
            .simple_query(query)
            .await
            .map_err(|source| Error::TargetRowCountQuery { source })?
            .into_row()
            .await
            .map_err(|source| Error::TargetRowCountQuery { source })?
            .ok_or_else(|| Error::TargetRowCountUnexpectedResult {
                reason: "target row count query returned no rows".to_owned(),
            })?;
        let count = row
            .try_get::<i64, _>("row_count")
            .map_err(|source| Error::TargetRowCountQuery { source })?
            .ok_or_else(|| Error::TargetRowCountUnexpectedResult {
                reason: "target row count query returned NULL".to_owned(),
            })?;

        count_big_i64_to_u64(count)
    }

    /// Executes a prepared lifecycle SQL statement.
    ///
    /// This method accepts statement text but intentionally returns only
    /// affected-row metadata. It does not expose a generic result-row mapping
    /// API.
    pub async fn execute_statement(&mut self, sql: &str) -> Result<SqlExecutionOutcome> {
        let result = self
            .client
            .execute(sql, &[])
            .await
            .map_err(|source| Error::SqlExecution { source })?;

        Ok(SqlExecutionOutcome {
            rows_affected: result.rows_affected().to_vec(),
        })
    }

    /// Starts a bulk writer on this same SQL Server connection.
    ///
    /// The returned writer borrows the connected client, so lifecycle SQL and
    /// bulk loading cannot accidentally use two different connections through
    /// this API.
    pub async fn bulk_writer(
        &mut self,
        table: TableName,
        planned_schema: PlannedSchema,
        options: WriteOptions,
    ) -> Result<ConnectedBulkWriter<'_>> {
        let writer = BulkWriter::new(&mut self.client, table, planned_schema, options).await?;

        Ok(ConnectedBulkWriter { writer })
    }
}

impl ConnectedBulkWriter<'_> {
    /// Writes one Arrow record batch.
    pub async fn write_batch(&mut self, batch: &RecordBatch) -> Result<WriteStats> {
        self.writer.write_batch(batch).await
    }

    /// Finalizes the bulk writer and returns cumulative write statistics.
    pub async fn finish(self) -> Result<WriteStats> {
        self.writer.finish().await
    }
}

/// Connects to SQL Server from an ADO-style connection string.
///
/// The connection uses this crate's `tiberius-raw-bulk` dependency identity and
/// Tokio TCP transport internally. The returned wrapper hides those concrete
/// types from downstream crates.
///
/// The raw connection string is not stored in the returned client or in errors.
pub async fn connect_mssql_client_from_ado_string(
    connection_string: &str,
) -> Result<ConnectedMssqlClient> {
    let config = tiberius::Config::from_ado_string(connection_string)
        .map_err(|_source| Error::InvalidConnectionString)?;
    let tcp = TcpStream::connect(config.get_addr())
        .await
        .map_err(|source| Error::ConnectionTcpConnect { source })?;
    tcp.set_nodelay(true)
        .map_err(|source| Error::ConnectionTcpConnect { source })?;

    let client = tiberius::Client::connect(config, tcp.compat_write())
        .await
        .map_err(|source| Error::ConnectionClientSetup { source })?;

    Ok(ConnectedMssqlClient { client })
}

fn table_exists_query(table: &TableName) -> String {
    let mut conditions = vec![format!(
        "t.name = {}",
        sql_string_literal(table.table().as_str())
    )];
    if let Some(schema) = table.schema() {
        conditions.push(format!("s.name = {}", sql_string_literal(schema.as_str())));
    }

    format!(
        "SELECT CASE WHEN EXISTS (SELECT 1 FROM sys.tables AS t \
         INNER JOIN sys.schemas AS s ON s.schema_id = t.schema_id \
         WHERE {}) THEN CAST(1 AS bit) ELSE CAST(0 AS bit) END AS [exists]",
        conditions.join(" AND ")
    )
}

fn target_row_count_query(table: &TableName) -> String {
    format!(
        "SELECT COUNT_BIG(*) AS [row_count] FROM {}",
        table.quoted_sql()
    )
}

fn count_big_i64_to_u64(count: i64) -> Result<u64> {
    u64::try_from(count).map_err(|_| Error::TargetRowCountUnexpectedResult {
        reason: "target row count was outside the supported range".to_owned(),
    })
}

fn sql_string_literal(value: &str) -> String {
    format!("N'{}'", value.replace('\'', "''"))
}

#[cfg(test)]
mod tests {
    use crate::{Error, connect_mssql_client_from_ado_string};

    #[test]
    fn sql_execution_outcome_records_rows_affected_in_order() {
        let outcome = crate::SqlExecutionOutcome {
            rows_affected: vec![2, 3, 5],
        };

        assert_eq!(outcome.rows_affected, vec![2, 3, 5]);
        assert_eq!(outcome.total_rows_affected(), 10);
    }

    #[test]
    fn table_exists_query_filters_schema_and_table() -> crate::Result<()> {
        let table = crate::TableName::new("tenant", "people")?;
        let query = super::table_exists_query(&table);

        assert!(query.contains("FROM sys.tables AS t"));
        assert!(query.contains("INNER JOIN sys.schemas AS s"));
        assert!(query.contains("t.name = N'people'"));
        assert!(query.contains("s.name = N'tenant'"));
        Ok(())
    }

    #[test]
    fn table_exists_query_escapes_string_literals() -> crate::Result<()> {
        let table = crate::TableName::new("tenant's", "people's")?;
        let query = super::table_exists_query(&table);

        assert!(query.contains("t.name = N'people''s'"));
        assert!(query.contains("s.name = N'tenant''s'"));
        Ok(())
    }

    #[test]
    fn unqualified_table_exists_query_filters_only_table_name() -> crate::Result<()> {
        let table = crate::TableName::unqualified("people")?;
        let query = super::table_exists_query(&table);

        assert!(query.contains("t.name = N'people'"));
        assert!(!query.contains("s.name ="));
        Ok(())
    }

    #[test]
    fn target_row_count_query_uses_quoted_table_name() -> crate::Result<()> {
        let table = crate::TableName::new("tenant.schema", "people]2026")?;
        let query = super::target_row_count_query(&table);

        assert_eq!(
            query,
            "SELECT COUNT_BIG(*) AS [row_count] FROM [tenant.schema].[people]]2026]"
        );
        Ok(())
    }

    #[test]
    fn count_big_conversion_rejects_negative_values_without_panicking() {
        let error = super::count_big_i64_to_u64(-1).err().unwrap_or_else(|| {
            Error::TargetRowCountUnexpectedResult {
                reason: "expected negative count to fail".to_owned(),
            }
        });

        assert!(matches!(
            error,
            Error::TargetRowCountUnexpectedResult { .. }
        ));
    }

    #[test]
    fn connected_client_type_is_public_without_raw_client_signature() {
        let type_name = std::any::type_name::<crate::ConnectedMssqlClient>();

        assert!(type_name.contains("ConnectedMssqlClient"));
        assert!(!type_name.contains("tiberius::Client"));
    }

    #[test]
    fn connected_writer_type_is_public_without_raw_transport_signature() {
        let type_name = std::any::type_name::<crate::ConnectedBulkWriter<'static>>();

        assert!(type_name.contains("ConnectedBulkWriter"));
        assert!(!type_name.contains("tiberius::Client"));
        assert!(!type_name.contains("tokio::net::TcpStream"));
    }

    #[tokio::test]
    async fn invalid_connection_string_error_is_redacted() -> crate::Result<()> {
        let connection_string =
            "Server=tcp:localhost,notaport;Password=secret-token-123;Access Token=token-456";
        let result = connect_mssql_client_from_ado_string(connection_string).await;
        let Err(error) = result else {
            return Err(Error::InvalidConnectionString);
        };

        assert!(matches!(error, Error::InvalidConnectionString));
        let display = error.to_string();
        let debug = format!("{error:?}");

        for secret in ["secret-token-123", "token-456", connection_string] {
            assert!(!display.contains(secret));
            assert!(!debug.contains(secret));
        }

        Ok(())
    }

    #[tokio::test]
    async fn tcp_connect_error_is_structured_and_redacted() -> crate::Result<()> {
        let connection_string =
            "Server=tcp:127.0.0.1,1;User Id=sa;Password=secret-token-123;Encrypt=false";
        let result = connect_mssql_client_from_ado_string(connection_string).await;
        let Err(error) = result else {
            return Err(Error::InvalidConnectionString);
        };

        assert!(matches!(error, Error::ConnectionTcpConnect { .. }));
        let display = error.to_string();
        let debug = format!("{error:?}");

        for secret in ["secret-token-123", connection_string] {
            assert!(!display.contains(secret));
            assert!(!debug.contains(secret));
        }

        Ok(())
    }
}

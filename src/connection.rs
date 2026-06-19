//! SQL Server connection helpers.

use std::fmt;

use tokio::net::TcpStream;
use tokio_util::compat::{Compat, TokioAsyncWriteCompatExt};

use crate::{Error, Result};

type CompatibleMssqlTransport = Compat<TcpStream>;

/// Opaque SQL Server client constructed with this crate's compatible Tiberius dependency.
///
/// Use [`connect_mssql_client_from_ado_string`] to create this type. Its
/// concrete Tiberius client and async transport types are intentionally hidden
/// so downstream crates do not have to name or match `tiberius-raw-bulk`
/// directly.
pub struct ConnectedMssqlClient {
    _client: tiberius::Client<CompatibleMssqlTransport>,
}

impl fmt::Debug for ConnectedMssqlClient {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ConnectedMssqlClient")
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

    Ok(ConnectedMssqlClient { _client: client })
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
    fn connected_client_type_is_public_without_raw_client_signature() {
        let type_name = std::any::type_name::<crate::ConnectedMssqlClient>();

        assert!(type_name.contains("ConnectedMssqlClient"));
        assert!(!type_name.contains("tiberius::Client"));
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

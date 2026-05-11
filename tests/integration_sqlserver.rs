//! SQL Server integration harness smoke tests.

#![cfg(feature = "integration-tests")]

use std::env;

use tokio::net::TcpStream;
use tokio_util::compat::{Compat, TokioAsyncWriteCompatExt};

const CONNECTION_STRING_ENV: &str = "ARROW_TIBERIUS_TEST_MSSQL_URL";
const TEST_DATABASE_ENV: &str = "ARROW_TIBERIUS_TEST_MSSQL_DATABASE";

#[test]
fn sqlserver_integration_harness_is_configured() {
    let Some(connection_string) = env::var_os(CONNECTION_STRING_ENV) else {
        eprintln!(
            "skipping SQL Server integration harness smoke test: {CONNECTION_STRING_ENV} is not set"
        );
        return;
    };

    let Some(database) = env::var_os(TEST_DATABASE_ENV) else {
        eprintln!(
            "skipping SQL Server integration harness smoke test: {TEST_DATABASE_ENV} is not set"
        );
        return;
    };

    assert!(!connection_string.is_empty());
    assert!(!database.is_empty());
}

#[tokio::test]
async fn sqlserver_integration_harness_opens_tiberius_client() -> tiberius::Result<()> {
    let Some((connection_string, database)) = integration_config() else {
        eprintln!(
            "skipping SQL Server integration connection smoke test: {CONNECTION_STRING_ENV} or {TEST_DATABASE_ENV} is not set"
        );
        return Ok(());
    };

    let mut client = connect(&connection_string, &database).await?;
    let row = client
        .simple_query("SELECT DB_NAME()")
        .await?
        .into_row()
        .await?
        .expect("SELECT DB_NAME() should return one row");
    let actual_database = row
        .get::<&str, _>(0)
        .expect("SELECT DB_NAME() should return a database name");

    assert_eq!(actual_database, database);

    Ok(())
}

type TestClient = tiberius::Client<Compat<TcpStream>>;

async fn connect(connection_string: &str, database: &str) -> tiberius::Result<TestClient> {
    let connection_string = format!("{connection_string};database={database}");
    let config = tiberius::Config::from_ado_string(&connection_string)?;
    let tcp = TcpStream::connect(config.get_addr()).await?;

    tiberius::Client::connect(config, tcp.compat_write()).await
}

fn integration_config() -> Option<(String, String)> {
    let connection_string = env::var(CONNECTION_STRING_ENV).ok()?;
    let database = env::var(TEST_DATABASE_ENV).ok()?;

    Some((connection_string, database))
}

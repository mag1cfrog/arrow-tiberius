//! SQL Server integration harness smoke tests.

#![cfg(feature = "integration-tests")]

use std::env;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use arrow_array::{ArrayRef, Int32Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use arrow_tiberius::{
    BulkWriter, MssqlProfile, PlanOptions, TableName, WriteBackend, WriteOptions,
    create_table_sql_from_mappings, plan_arrow_schema_to_mssql_mappings,
};
use tokio::net::TcpStream;
use tokio_util::compat::{Compat, TokioAsyncWriteCompatExt};

const CONNECTION_STRING_ENV: &str = "ARROW_TIBERIUS_TEST_MSSQL_URL";
const TEST_DATABASE_ENV: &str = "ARROW_TIBERIUS_TEST_MSSQL_DATABASE";
static TABLE_COUNTER: AtomicU64 = AtomicU64::new(0);

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

#[tokio::test]
async fn baseline_writer_inserts_int32_and_utf8_batch() -> TestResult<()> {
    let Some((connection_string, database)) = integration_config() else {
        eprintln!(
            "skipping SQL Server baseline writer integration test: {CONNECTION_STRING_ENV} or {TEST_DATABASE_ENV} is not set"
        );
        return Ok(());
    };

    let mut client = connect(&connection_string, &database).await?;
    let table = unique_table_name()?;
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("label", DataType::Utf8, true),
    ]));
    let (mappings, _diagnostics) = plan_arrow_schema_to_mssql_mappings(
        Arc::clone(&schema),
        MssqlProfile::sql_server_2016_compat_100(),
        PlanOptions::default(),
    )?
    .into_parts();
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int32Array::from(vec![1_i32, 2, 3])) as ArrayRef,
            Arc::new(StringArray::from(vec![Some("alpha"), Some("東京"), None])),
        ],
    )?;

    execute_sql(
        &mut client,
        create_table_sql_from_mappings(&table, &mappings),
    )
    .await?;

    let result = async {
        let mut writer = BulkWriter::new(
            &mut client,
            table.clone(),
            mappings,
            WriteOptions {
                backend: WriteBackend::BaselineTokenRow,
                ..WriteOptions::default()
            },
        )
        .await?;
        let stats = writer.write_batch(&batch).await?;

        assert_eq!(stats.rows_written, 3);
        assert_eq!(stats.batches_written, 1);
        assert_eq!(writer.finish().await?, stats);

        let rows = client
            .simple_query(format!(
                "SELECT [id], [label] FROM {} ORDER BY [id]",
                table.quoted_sql()
            ))
            .await?
            .into_first_result()
            .await?;

        assert_eq!(rows.len(), 3);
        assert_eq!(rows[0].get::<i32, _>(0), Some(1));
        assert_eq!(rows[0].get::<&str, _>(1), Some("alpha"));
        assert_eq!(rows[1].get::<i32, _>(0), Some(2));
        assert_eq!(rows[1].get::<&str, _>(1), Some("東京"));
        assert_eq!(rows[2].get::<i32, _>(0), Some(3));
        assert_eq!(rows[2].get::<&str, _>(1), None);

        Ok::<(), Box<dyn std::error::Error>>(())
    }
    .await;

    let drop_result = drop_table(&mut client, &table).await;
    result?;
    drop_result?;

    Ok(())
}

type TestClient = tiberius::Client<Compat<TcpStream>>;
type TestResult<T> = std::result::Result<T, Box<dyn std::error::Error>>;

async fn connect(connection_string: &str, database: &str) -> tiberius::Result<TestClient> {
    let connection_string = format!("{connection_string};database={database}");
    let config = tiberius::Config::from_ado_string(&connection_string)?;
    let tcp = TcpStream::connect(config.get_addr()).await?;

    tiberius::Client::connect(config, tcp.compat_write()).await
}

async fn execute_sql(client: &mut TestClient, sql: String) -> tiberius::Result<()> {
    client.simple_query(sql).await?.into_results().await?;

    Ok(())
}

async fn drop_table(client: &mut TestClient, table: &TableName) -> tiberius::Result<()> {
    execute_sql(
        client,
        format!("DROP TABLE IF EXISTS {}", table.quoted_sql()),
    )
    .await
}

fn unique_table_name() -> arrow_tiberius::Result<TableName> {
    let counter = TABLE_COUNTER.fetch_add(1, Ordering::Relaxed);
    let table = format!("arrow_tiberius_{}_{}", std::process::id(), counter);

    TableName::new("dbo", table)
}

fn integration_config() -> Option<(String, String)> {
    let connection_string = env::var(CONNECTION_STRING_ENV).ok()?;
    let database = env::var(TEST_DATABASE_ENV).ok()?;

    Some((connection_string, database))
}

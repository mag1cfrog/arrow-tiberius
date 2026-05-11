//! SQL Server integration harness smoke tests.

#![cfg(feature = "integration-tests")]

use std::env;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use arrow_array::{
    ArrayRef, BinaryArray, BooleanArray, Float64Array, Int32Array, Int64Array, RecordBatch,
    StringArray,
};
use arrow_schema::{DataType, Field, Schema};
use arrow_tiberius::{
    BulkWriter, DiagnosticCode, Error, MssqlProfile, PlanOptions, TableName, WriteBackend,
    WriteOptions, create_table_sql_from_mappings, plan_arrow_schema_to_mssql_mappings,
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

#[tokio::test]
async fn baseline_writer_round_trips_supported_value_matrix() -> TestResult<()> {
    let Some((connection_string, database)) = integration_config() else {
        eprintln!(
            "skipping SQL Server baseline writer matrix integration test: {CONNECTION_STRING_ENV} or {TEST_DATABASE_ENV} is not set"
        );
        return Ok(());
    };

    let mut client = connect(&connection_string, &database).await?;
    let table = unique_table_name()?;
    let schema = Arc::new(Schema::new(vec![
        Field::new("row_id", DataType::Int32, false),
        Field::new("flag", DataType::Boolean, true),
        Field::new("i32_value", DataType::Int32, true),
        Field::new("i64_value", DataType::Int64, true),
        Field::new("f64_value", DataType::Float64, true),
        Field::new("text_value", DataType::Utf8, true),
        Field::new("bytes_value", DataType::Binary, true),
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
            Arc::new(Int32Array::from(vec![1_i32, 2, 3, 4])) as ArrayRef,
            Arc::new(BooleanArray::from(vec![
                Some(true),
                Some(false),
                None,
                Some(true),
            ])),
            Arc::new(Int32Array::from(vec![
                Some(i32::MIN),
                Some(0),
                Some(i32::MAX),
                None,
            ])),
            Arc::new(Int64Array::from(vec![
                Some(i64::MIN),
                Some(0),
                Some(i64::MAX),
                None,
            ])),
            Arc::new(Float64Array::from(vec![
                Some(-123.5),
                Some(0.0),
                Some(42.25),
                None,
            ])),
            Arc::new(StringArray::from(vec![
                Some(""),
                Some("ascii"),
                Some("東京"),
                None,
            ])),
            Arc::new(BinaryArray::from_iter(vec![
                Some(&b""[..]),
                Some(&b"\x00\x01\xfe\xff"[..]),
                Some(&b"abc"[..]),
                None,
            ])),
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

        assert_eq!(stats.rows_written, 4);
        assert_eq!(stats.batches_written, 1);
        assert_eq!(writer.finish().await?, stats);

        let rows = client
            .simple_query(format!(
                "SELECT [row_id], [flag], [i32_value], [i64_value], [f64_value], [text_value], [bytes_value] FROM {} ORDER BY [row_id]",
                table.quoted_sql()
            ))
            .await?
            .into_first_result()
            .await?;

        assert_eq!(rows.len(), 4);

        assert_eq!(rows[0].get::<i32, _>(0), Some(1));
        assert_eq!(rows[0].get::<bool, _>(1), Some(true));
        assert_eq!(rows[0].get::<i32, _>(2), Some(i32::MIN));
        assert_eq!(rows[0].get::<i64, _>(3), Some(i64::MIN));
        assert_eq!(rows[0].get::<f64, _>(4), Some(-123.5));
        assert_eq!(rows[0].get::<&str, _>(5), Some(""));
        assert_eq!(rows[0].get::<&[u8], _>(6), Some(&b""[..]));

        assert_eq!(rows[1].get::<i32, _>(0), Some(2));
        assert_eq!(rows[1].get::<bool, _>(1), Some(false));
        assert_eq!(rows[1].get::<i32, _>(2), Some(0));
        assert_eq!(rows[1].get::<i64, _>(3), Some(0));
        assert_eq!(rows[1].get::<f64, _>(4), Some(0.0));
        assert_eq!(rows[1].get::<&str, _>(5), Some("ascii"));
        assert_eq!(rows[1].get::<&[u8], _>(6), Some(&b"\x00\x01\xfe\xff"[..]));

        assert_eq!(rows[2].get::<i32, _>(0), Some(3));
        assert_eq!(rows[2].get::<bool, _>(1), None);
        assert_eq!(rows[2].get::<i32, _>(2), Some(i32::MAX));
        assert_eq!(rows[2].get::<i64, _>(3), Some(i64::MAX));
        assert_eq!(rows[2].get::<f64, _>(4), Some(42.25));
        assert_eq!(rows[2].get::<&str, _>(5), Some("東京"));
        assert_eq!(rows[2].get::<&[u8], _>(6), Some(&b"abc"[..]));

        assert_eq!(rows[3].get::<i32, _>(0), Some(4));
        assert_eq!(rows[3].get::<bool, _>(1), Some(true));
        assert_eq!(rows[3].get::<i32, _>(2), None);
        assert_eq!(rows[3].get::<i64, _>(3), None);
        assert_eq!(rows[3].get::<f64, _>(4), None);
        assert_eq!(rows[3].get::<&str, _>(5), None);
        assert_eq!(rows[3].get::<&[u8], _>(6), None);

        Ok::<(), Box<dyn std::error::Error>>(())
    }
    .await;

    let drop_result = drop_table(&mut client, &table).await;
    result?;
    drop_result?;

    Ok(())
}

#[tokio::test]
async fn baseline_writer_rejects_target_table_schema_drift() -> TestResult<()> {
    let Some((connection_string, database)) = integration_config() else {
        eprintln!(
            "skipping SQL Server baseline writer schema-drift integration test: {CONNECTION_STRING_ENV} or {TEST_DATABASE_ENV} is not set"
        );
        return Ok(());
    };

    let mut client = connect(&connection_string, &database).await?;
    let table = unique_table_name()?;
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
    let (mappings, _diagnostics) = plan_arrow_schema_to_mssql_mappings(
        Arc::clone(&schema),
        MssqlProfile::sql_server_2016_compat_100(),
        PlanOptions::default(),
    )?
    .into_parts();
    let batch = RecordBatch::try_new(
        schema,
        vec![Arc::new(Int32Array::from(vec![1_i32, 2])) as ArrayRef],
    )?;

    execute_sql(
        &mut client,
        format!(
            "CREATE TABLE {} ([renamed_id] int NOT NULL)",
            table.quoted_sql()
        ),
    )
    .await?;

    let result = async {
        let err = BulkWriter::new(
            &mut client,
            table.clone(),
            mappings,
            WriteOptions {
                backend: WriteBackend::BaselineTokenRow,
                ..WriteOptions::default()
            },
        )
        .await
        .expect_err("target-table schema drift should be rejected");

        let Error::ValueConversion { diagnostics } = err else {
            panic!("expected value conversion error");
        };

        assert!(diagnostics.all().iter().any(|diagnostic| diagnostic.code()
            == DiagnosticCode::SchemaMismatch
            && diagnostic.message().contains("renamed_id")
            && diagnostic.message().contains("id")));

        let row_count = select_count(&mut client, &table).await?;
        assert_eq!(row_count, 0);

        let mut writer = BulkWriter::new(
            &mut client,
            table.clone(),
            vec![arrow_tiberius::SchemaMapping::new(
                arrow_tiberius::ArrowFieldRef::new(
                    0,
                    "renamed_id".to_owned(),
                    false,
                    DataType::Int32,
                ),
                arrow_tiberius::MssqlColumn::new(
                    arrow_tiberius::Identifier::new("renamed_id")?,
                    arrow_tiberius::MssqlType::Int,
                    false,
                ),
            )],
            WriteOptions {
                backend: WriteBackend::BaselineTokenRow,
                ..WriteOptions::default()
            },
        )
        .await?;
        let err = writer.write_batch(&batch).await.expect_err(
            "runtime Arrow field drift should still be rejected after failed writer construction",
        );

        let Error::ValueConversion { diagnostics } = err else {
            panic!("expected value conversion error");
        };
        assert!(diagnostics.all().iter().any(|diagnostic| {
            diagnostic.code() == DiagnosticCode::SchemaMismatch
                && diagnostic.message().contains("runtime Arrow field name id")
                && diagnostic
                    .message()
                    .contains("planned Arrow field name renamed_id")
        }));
        assert_eq!(writer.finish().await?.rows_written, 0);

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

async fn select_count(client: &mut TestClient, table: &TableName) -> TestResult<i32> {
    let row = client
        .simple_query(format!("SELECT COUNT(*) FROM {}", table.quoted_sql()))
        .await?
        .into_row()
        .await?
        .ok_or_else(|| std::io::Error::other("SELECT COUNT(*) returned no rows"))?;

    Ok(row
        .get::<i32, _>(0)
        .ok_or_else(|| std::io::Error::other("SELECT COUNT(*) did not return an int"))?)
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

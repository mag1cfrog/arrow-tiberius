//! SQL Server integration harness smoke tests.

#![cfg(feature = "integration-tests")]

use std::env;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use arrow_array::{
    ArrayRef, BinaryArray, BooleanArray, Float64Array, Int32Array, Int64Array, RecordBatch,
    StringArray, UInt64Array,
};
use arrow_schema::{DataType, Field, Schema};
use arrow_tiberius::{
    BulkWriter, DiagnosticCode, Error, MssqlProfile, PlanOptions, TableName, UInt64Policy,
    WriteBackend, WriteOptions, create_table_sql_from_mappings,
    plan_arrow_schema_to_mssql_mappings,
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

        ensure_eq(stats.rows_written, 3, "rows_written")?;
        ensure_eq(stats.batches_written, 1, "batches_written")?;
        ensure_eq(writer.finish().await?, stats, "finish stats")?;

        let rows = client
            .simple_query(format!(
                "SELECT [id], [label] FROM {} ORDER BY [id]",
                table.quoted_sql()
            ))
            .await?
            .into_first_result()
            .await?;

        ensure_eq(rows.len(), 3, "row count")?;
        ensure_eq(rows[0].get::<i32, _>(0), Some(1), "row 0 id")?;
        ensure_eq(rows[0].get::<&str, _>(1), Some("alpha"), "row 0 label")?;
        ensure_eq(rows[1].get::<i32, _>(0), Some(2), "row 1 id")?;
        ensure_eq(rows[1].get::<&str, _>(1), Some("東京"), "row 1 label")?;
        ensure_eq(rows[2].get::<i32, _>(0), Some(3), "row 2 id")?;
        ensure_eq(rows[2].get::<&str, _>(1), None, "row 2 label")?;

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

        ensure_eq(stats.rows_written, 4, "rows_written")?;
        ensure_eq(stats.batches_written, 1, "batches_written")?;
        ensure_eq(writer.finish().await?, stats, "finish stats")?;

        let rows = client
            .simple_query(format!(
                "SELECT [row_id], [flag], [i32_value], [i64_value], [f64_value], [text_value], [bytes_value] FROM {} ORDER BY [row_id]",
                table.quoted_sql()
            ))
            .await?
            .into_first_result()
            .await?;

        ensure_eq(rows.len(), 4, "row count")?;

        ensure_eq(rows[0].get::<i32, _>(0), Some(1), "row 0 row_id")?;
        ensure_eq(rows[0].get::<bool, _>(1), Some(true), "row 0 flag")?;
        ensure_eq(
            rows[0].get::<i32, _>(2),
            Some(i32::MIN),
            "row 0 i32_value",
        )?;
        ensure_eq(
            rows[0].get::<i64, _>(3),
            Some(i64::MIN),
            "row 0 i64_value",
        )?;
        ensure_eq(
            rows[0].get::<f64, _>(4),
            Some(-123.5),
            "row 0 f64_value",
        )?;
        ensure_eq(rows[0].get::<&str, _>(5), Some(""), "row 0 text_value")?;
        ensure_eq(
            rows[0].get::<&[u8], _>(6),
            Some(&b""[..]),
            "row 0 bytes_value",
        )?;

        ensure_eq(rows[1].get::<i32, _>(0), Some(2), "row 1 row_id")?;
        ensure_eq(rows[1].get::<bool, _>(1), Some(false), "row 1 flag")?;
        ensure_eq(rows[1].get::<i32, _>(2), Some(0), "row 1 i32_value")?;
        ensure_eq(rows[1].get::<i64, _>(3), Some(0), "row 1 i64_value")?;
        ensure_eq(rows[1].get::<f64, _>(4), Some(0.0), "row 1 f64_value")?;
        ensure_eq(
            rows[1].get::<&str, _>(5),
            Some("ascii"),
            "row 1 text_value",
        )?;
        ensure_eq(
            rows[1].get::<&[u8], _>(6),
            Some(&b"\x00\x01\xfe\xff"[..]),
            "row 1 bytes_value",
        )?;

        ensure_eq(rows[2].get::<i32, _>(0), Some(3), "row 2 row_id")?;
        ensure_eq(rows[2].get::<bool, _>(1), None, "row 2 flag")?;
        ensure_eq(
            rows[2].get::<i32, _>(2),
            Some(i32::MAX),
            "row 2 i32_value",
        )?;
        ensure_eq(
            rows[2].get::<i64, _>(3),
            Some(i64::MAX),
            "row 2 i64_value",
        )?;
        ensure_eq(
            rows[2].get::<f64, _>(4),
            Some(42.25),
            "row 2 f64_value",
        )?;
        ensure_eq(
            rows[2].get::<&str, _>(5),
            Some("東京"),
            "row 2 text_value",
        )?;
        ensure_eq(
            rows[2].get::<&[u8], _>(6),
            Some(&b"abc"[..]),
            "row 2 bytes_value",
        )?;

        ensure_eq(rows[3].get::<i32, _>(0), Some(4), "row 3 row_id")?;
        ensure_eq(rows[3].get::<bool, _>(1), Some(true), "row 3 flag")?;
        ensure_eq(rows[3].get::<i32, _>(2), None, "row 3 i32_value")?;
        ensure_eq(rows[3].get::<i64, _>(3), None, "row 3 i64_value")?;
        ensure_eq(rows[3].get::<f64, _>(4), None, "row 3 f64_value")?;
        ensure_eq(rows[3].get::<&str, _>(5), None, "row 3 text_value")?;
        ensure_eq(rows[3].get::<&[u8], _>(6), None, "row 3 bytes_value")?;

        Ok::<(), Box<dyn std::error::Error>>(())
    }
    .await;

    let drop_result = drop_table(&mut client, &table).await;
    result?;
    drop_result?;

    Ok(())
}

#[tokio::test]
async fn baseline_writer_round_trips_uint64_policy_values() -> TestResult<()> {
    let Some((connection_string, database)) = integration_config() else {
        eprintln!(
            "skipping SQL Server UInt64 policy integration test: {CONNECTION_STRING_ENV} or {TEST_DATABASE_ENV} is not set"
        );
        return Ok(());
    };

    let mut client = connect(&connection_string, &database).await?;
    let decimal_table = unique_table_name()?;
    let bigint_table = unique_table_name()?;
    let decimal_schema = Arc::new(Schema::new(vec![
        Field::new("row_id", DataType::Int32, false),
        Field::new("u64_value", DataType::UInt64, true),
    ]));
    let bigint_schema = Arc::new(Schema::new(vec![
        Field::new("row_id", DataType::Int32, false),
        Field::new("u64_value", DataType::UInt64, true),
    ]));
    let (decimal_mappings, _diagnostics) = plan_arrow_schema_to_mssql_mappings(
        Arc::clone(&decimal_schema),
        MssqlProfile::sql_server_2016_compat_100(),
        PlanOptions {
            uint64_policy: UInt64Policy::Decimal20_0,
            ..PlanOptions::default()
        },
    )?
    .into_parts();
    let (bigint_mappings, _diagnostics) = plan_arrow_schema_to_mssql_mappings(
        Arc::clone(&bigint_schema),
        MssqlProfile::sql_server_2016_compat_100(),
        PlanOptions {
            uint64_policy: UInt64Policy::CheckedBigInt,
            ..PlanOptions::default()
        },
    )?
    .into_parts();
    let decimal_batch = RecordBatch::try_new(
        decimal_schema,
        vec![
            Arc::new(Int32Array::from(vec![1_i32, 2, 3, 4])) as ArrayRef,
            Arc::new(UInt64Array::from(vec![
                Some(0_u64),
                Some((i64::MAX as u64) + 1),
                Some(u64::MAX),
                None,
            ])),
        ],
    )?;
    let bigint_batch = RecordBatch::try_new(
        bigint_schema,
        vec![
            Arc::new(Int32Array::from(vec![1_i32, 2, 3])) as ArrayRef,
            Arc::new(UInt64Array::from(vec![
                Some(0_u64),
                Some(i64::MAX as u64),
                None,
            ])),
        ],
    )?;

    execute_sql(
        &mut client,
        create_table_sql_from_mappings(&decimal_table, &decimal_mappings),
    )
    .await?;
    execute_sql(
        &mut client,
        create_table_sql_from_mappings(&bigint_table, &bigint_mappings),
    )
    .await?;

    let result = async {
        let mut decimal_writer = BulkWriter::new(
            &mut client,
            decimal_table.clone(),
            decimal_mappings,
            WriteOptions {
                backend: WriteBackend::BaselineTokenRow,
                ..WriteOptions::default()
            },
        )
        .await?;
        let decimal_stats = decimal_writer.write_batch(&decimal_batch).await?;
        ensure_eq(decimal_stats.rows_written, 4, "decimal rows_written")?;
        ensure_eq(
            decimal_writer.finish().await?,
            decimal_stats,
            "decimal finish stats",
        )?;

        let mut bigint_writer = BulkWriter::new(
            &mut client,
            bigint_table.clone(),
            bigint_mappings,
            WriteOptions {
                backend: WriteBackend::BaselineTokenRow,
                ..WriteOptions::default()
            },
        )
        .await?;
        let bigint_stats = bigint_writer.write_batch(&bigint_batch).await?;
        ensure_eq(bigint_stats.rows_written, 3, "bigint rows_written")?;
        ensure_eq(
            bigint_writer.finish().await?,
            bigint_stats,
            "bigint finish stats",
        )?;

        let decimal_rows = client
            .simple_query(format!(
                "SELECT [row_id], CONVERT(varchar(40), [u64_value]) FROM {} ORDER BY [row_id]",
                decimal_table.quoted_sql()
            ))
            .await?
            .into_first_result()
            .await?;

        ensure_eq(decimal_rows.len(), 4, "decimal row count")?;
        ensure_eq(
            decimal_rows[0].get::<i32, _>(0),
            Some(1),
            "decimal row 0 id",
        )?;
        ensure_eq(
            decimal_rows[0].get::<&str, _>(1),
            Some("0"),
            "decimal row 0 value",
        )?;
        ensure_eq(
            decimal_rows[1].get::<i32, _>(0),
            Some(2),
            "decimal row 1 id",
        )?;
        ensure_eq(
            decimal_rows[1].get::<&str, _>(1),
            Some("9223372036854775808"),
            "decimal row 1 value",
        )?;
        ensure_eq(
            decimal_rows[2].get::<i32, _>(0),
            Some(3),
            "decimal row 2 id",
        )?;
        ensure_eq(
            decimal_rows[2].get::<&str, _>(1),
            Some("18446744073709551615"),
            "decimal row 2 value",
        )?;
        ensure_eq(
            decimal_rows[3].get::<i32, _>(0),
            Some(4),
            "decimal row 3 id",
        )?;
        ensure_eq(
            decimal_rows[3].get::<&str, _>(1),
            None,
            "decimal row 3 value",
        )?;

        let bigint_rows = client
            .simple_query(format!(
                "SELECT [row_id], [u64_value] FROM {} ORDER BY [row_id]",
                bigint_table.quoted_sql()
            ))
            .await?
            .into_first_result()
            .await?;

        ensure_eq(bigint_rows.len(), 3, "bigint row count")?;
        ensure_eq(bigint_rows[0].get::<i32, _>(0), Some(1), "bigint row 0 id")?;
        ensure_eq(
            bigint_rows[0].get::<i64, _>(1),
            Some(0),
            "bigint row 0 value",
        )?;
        ensure_eq(bigint_rows[1].get::<i32, _>(0), Some(2), "bigint row 1 id")?;
        ensure_eq(
            bigint_rows[1].get::<i64, _>(1),
            Some(i64::MAX),
            "bigint row 1 value",
        )?;
        ensure_eq(bigint_rows[2].get::<i32, _>(0), Some(3), "bigint row 2 id")?;
        ensure_eq(bigint_rows[2].get::<i64, _>(1), None, "bigint row 2 value")?;

        Ok::<(), Box<dyn std::error::Error>>(())
    }
    .await;

    let decimal_drop_result = drop_table(&mut client, &decimal_table).await;
    let bigint_drop_result = drop_table(&mut client, &bigint_table).await;
    result?;
    decimal_drop_result?;
    bigint_drop_result?;

    Ok(())
}

#[tokio::test]
async fn baseline_writer_rejects_uint64_checked_bigint_overflow_without_partial_insert()
-> TestResult<()> {
    let Some((connection_string, database)) = integration_config() else {
        eprintln!(
            "skipping SQL Server UInt64 overflow integration test: {CONNECTION_STRING_ENV} or {TEST_DATABASE_ENV} is not set"
        );
        return Ok(());
    };

    let mut client = connect(&connection_string, &database).await?;
    let table = unique_table_name()?;
    let schema = Arc::new(Schema::new(vec![
        Field::new("row_id", DataType::Int32, false),
        Field::new("u64_value", DataType::UInt64, false),
    ]));
    let (mappings, _diagnostics) = plan_arrow_schema_to_mssql_mappings(
        Arc::clone(&schema),
        MssqlProfile::sql_server_2016_compat_100(),
        PlanOptions {
            uint64_policy: UInt64Policy::CheckedBigInt,
            ..PlanOptions::default()
        },
    )?
    .into_parts();
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int32Array::from(vec![1_i32, 2])) as ArrayRef,
            Arc::new(UInt64Array::from(vec![0_u64, (i64::MAX as u64) + 1])),
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
        let err = match writer.write_batch(&batch).await {
            Ok(_stats) => {
                let _stats = writer.finish().await?;
                return Err(test_error("UInt64 bigint overflow was accepted"));
            }
            Err(err) => err,
        };

        let diagnostics = match err {
            Error::ValueConversion { diagnostics } => diagnostics,
            other => {
                return Err(test_error(format!(
                    "expected value conversion error, got {other}"
                )));
            }
        };
        ensure(
            diagnostics.all().iter().any(|diagnostic| {
                diagnostic.code() == DiagnosticCode::IntegerOutOfRange
                    && diagnostic.row() == Some(1)
                    && diagnostic
                        .field()
                        .is_some_and(|field| field.name() == "u64_value")
            }),
            "UInt64 bigint overflow diagnostic should include row and field",
        )?;
        ensure_eq(
            writer.finish().await?.rows_written,
            0,
            "finish rows_written",
        )?;
        ensure_eq(
            select_count(&mut client, &table).await?,
            0,
            "row count after overflow rejection",
        )?;

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
        let err = match BulkWriter::new(
            &mut client,
            table.clone(),
            mappings,
            WriteOptions {
                backend: WriteBackend::BaselineTokenRow,
                ..WriteOptions::default()
            },
        )
        .await
        {
            Ok(writer) => {
                let _stats = writer.finish().await?;
                return Err(test_error("target-table schema drift was accepted"));
            }
            Err(err) => err,
        };

        let diagnostics = match err {
            Error::ValueConversion { diagnostics } => diagnostics,
            other => {
                return Err(test_error(format!(
                    "expected value conversion error, got {other}"
                )));
            }
        };

        ensure(
            diagnostics.all().iter().any(|diagnostic| {
                diagnostic.code() == DiagnosticCode::SchemaMismatch
                    && diagnostic.message().contains("renamed_id")
                    && diagnostic.message().contains("id")
            }),
            "target schema drift diagnostic should mention renamed_id and id",
        )?;

        let row_count = select_count(&mut client, &table).await?;
        ensure_eq(row_count, 0, "row count after rejected writer creation")?;

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
        let err = match writer.write_batch(&batch).await {
            Ok(_stats) => {
                let _stats = writer.finish().await?;
                return Err(test_error("runtime Arrow field drift was accepted"));
            }
            Err(err) => err,
        };

        let diagnostics = match err {
            Error::ValueConversion { diagnostics } => diagnostics,
            other => {
                return Err(test_error(format!(
                    "expected value conversion error, got {other}"
                )));
            }
        };
        ensure(
            diagnostics.all().iter().any(|diagnostic| {
                diagnostic.code() == DiagnosticCode::SchemaMismatch
                    && diagnostic.message().contains("runtime Arrow field name id")
                    && diagnostic
                        .message()
                        .contains("planned Arrow field name renamed_id")
            }),
            "runtime schema drift diagnostic should mention id and renamed_id",
        )?;
        ensure_eq(
            writer.finish().await?.rows_written,
            0,
            "finish rows_written",
        )?;

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

fn ensure(condition: bool, message: impl Into<String>) -> TestResult<()> {
    if condition {
        return Ok(());
    }

    Err(test_error(message))
}

fn ensure_eq<T>(actual: T, expected: T, context: &str) -> TestResult<()>
where
    T: std::fmt::Debug + PartialEq,
{
    ensure(
        actual == expected,
        format!("{context}: expected {expected:?}, got {actual:?}"),
    )
}

fn test_error(message: impl Into<String>) -> Box<dyn std::error::Error> {
    Box::new(std::io::Error::other(message.into()))
}

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

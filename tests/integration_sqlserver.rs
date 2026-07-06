//! SQL Server integration harness smoke tests.

#![cfg(feature = "integration-tests")]

use std::env;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use arrow_array::{
    Array, ArrayRef, BinaryArray, BooleanArray, Date32Array, Date64Array, Decimal32Array,
    Decimal64Array, Decimal128Array, Decimal256Array, FixedSizeBinaryArray, Float16Array,
    Float32Array, Float64Array, Int8Array, Int16Array, Int32Array, Int64Array, LargeBinaryArray,
    LargeStringArray, RecordBatch, StringArray, Time32MillisecondArray, Time32SecondArray,
    Time64MicrosecondArray, Time64NanosecondArray, TimestampMicrosecondArray,
    TimestampMillisecondArray, TimestampNanosecondArray, TimestampSecondArray, UInt8Array,
    UInt16Array, UInt32Array, UInt64Array,
    types::{ArrowPrimitiveType, Float16Type},
};
use arrow_buffer::{MutableBuffer, NullBuffer, OffsetBuffer, ScalarBuffer, i256};
use arrow_data::ArrayData;
use arrow_schema::{DataType, Field, Schema, TimeUnit};
use arrow_tiberius::{
    ArrowFieldRef, BulkWriter, Date64Policy, DecimalPolicy, DiagnosticCode, DiagnosticSet, Error,
    Identifier, MssqlColumn, MssqlProfile, MssqlType, MssqlTypeLength, NanosecondPolicy,
    PlanOptions, SchemaMapping, TableName, TimestampPolicy, TimezonePolicy, UInt64Policy,
    WriteBackend, WriteOptions, WritePhase, create_table_sql_from_mappings,
    plan_arrow_schema_to_mssql_mappings,
};
use tokio::net::TcpStream;
use tokio_util::compat::{Compat, TokioAsyncWriteCompatExt};

const CONNECTION_STRING_ENV: &str = "ARROW_TIBERIUS_TEST_MSSQL_URL";
const TEST_DATABASE_ENV: &str = "ARROW_TIBERIUS_TEST_MSSQL_DATABASE";
static TABLE_COUNTER: AtomicU64 = AtomicU64::new(0);

type F16 = <Float16Type as ArrowPrimitiveType>::Native;

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
async fn writer_round_trips_empty_and_multi_batch_values_across_supported_backends()
-> TestResult<()> {
    let Some((connection_string, database)) = integration_config() else {
        eprintln!(
            "skipping SQL Server writer empty and multi-batch parity test: {CONNECTION_STRING_ENV} or {TEST_DATABASE_ENV} is not set"
        );
        return Ok(());
    };

    let mut client = connect(&connection_string, &database).await?;
    let schema = Arc::new(Schema::new(vec![
        Field::new("row_id", DataType::Int32, false),
        Field::new("label", DataType::Utf8, true),
        Field::new("payload", DataType::Binary, true),
    ]));
    let (mappings, _diagnostics) = plan_arrow_schema_to_mssql_mappings(
        Arc::clone(&schema),
        MssqlProfile::sql_server_2016_compat_100(),
        PlanOptions::default(),
    )?
    .into_parts();
    let empty_batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int32Array::from(Vec::<i32>::new())) as ArrayRef,
            Arc::new(StringArray::from(Vec::<Option<&str>>::new())),
            Arc::new(BinaryArray::from_iter(Vec::<Option<&[u8]>>::new())),
        ],
    )?;
    let first_batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int32Array::from(vec![1_i32, 2])) as ArrayRef,
            Arc::new(StringArray::from(vec![Some(""), Some("東京")])),
            Arc::new(BinaryArray::from_iter(vec![
                Some(&b""[..]),
                Some(&b"\x00\xff"[..]),
            ])),
        ],
    )?;
    let second_batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int32Array::from(vec![3_i32, 4])) as ArrayRef,
            Arc::new(StringArray::from(vec![Some("emoji 😀"), None])),
            Arc::new(BinaryArray::from_iter(vec![Some(&b"abc"[..]), None])),
        ],
    )?;

    for backend in [WriteBackend::BaselineTokenRow, WriteBackend::DirectRawBulk] {
        let table = unique_table_name()?;

        execute_sql(
            &mut client,
            create_table_sql_from_mappings(&table, &mappings),
        )
        .await?;

        let result = async {
            let mut writer = BulkWriter::new(
                &mut client,
                table.clone(),
                mappings.clone(),
                WriteOptions {
                    backend,
                    ..WriteOptions::default()
                },
            )
            .await?;

            let empty_stats = writer.write_batch(&empty_batch).await?;
            ensure_eq(empty_stats.rows_written, 0, "empty rows_written")?;
            ensure_eq(empty_stats.batches_written, 1, "empty batches_written")?;

            let first_stats = writer.write_batch(&first_batch).await?;
            ensure_eq(first_stats.rows_written, 2, "first rows_written")?;
            ensure_eq(first_stats.batches_written, 2, "first batches_written")?;

            let second_stats = writer.write_batch(&second_batch).await?;
            ensure_eq(second_stats.rows_written, 4, "second rows_written")?;
            ensure_eq(second_stats.batches_written, 3, "second batches_written")?;
            ensure_eq(writer.finish().await?, second_stats, "finish stats")?;

            let rows = client
                .simple_query(format!(
                    "SELECT [row_id], [label], [payload] FROM {} ORDER BY [row_id]",
                    table.quoted_sql()
                ))
                .await?
                .into_first_result()
                .await?;

            ensure_eq(rows.len(), 4, "row count")?;
            ensure_eq(rows[0].get::<i32, _>(0), Some(1), "row 0 row_id")?;
            ensure_eq(rows[0].get::<&str, _>(1), Some(""), "row 0 label")?;
            ensure_eq(rows[0].get::<&[u8], _>(2), Some(&b""[..]), "row 0 payload")?;
            ensure_eq(rows[1].get::<i32, _>(0), Some(2), "row 1 row_id")?;
            ensure_eq(rows[1].get::<&str, _>(1), Some("東京"), "row 1 label")?;
            ensure_eq(
                rows[1].get::<&[u8], _>(2),
                Some(&b"\x00\xff"[..]),
                "row 1 payload",
            )?;
            ensure_eq(rows[2].get::<i32, _>(0), Some(3), "row 2 row_id")?;
            ensure_eq(rows[2].get::<&str, _>(1), Some("emoji 😀"), "row 2 label")?;
            ensure_eq(
                rows[2].get::<&[u8], _>(2),
                Some(&b"abc"[..]),
                "row 2 payload",
            )?;
            ensure_eq(rows[3].get::<i32, _>(0), Some(4), "row 3 row_id")?;
            ensure_eq(rows[3].get::<&str, _>(1), None, "row 3 label")?;
            ensure_eq(rows[3].get::<&[u8], _>(2), None, "row 3 payload")?;

            Ok::<(), Box<dyn std::error::Error>>(())
        }
        .await;

        let drop_result = drop_table(&mut client, &table).await;
        result?;
        drop_result?;
    }

    Ok(())
}

#[tokio::test]
async fn baseline_writer_round_trips_fixed_size_binary_values() -> TestResult<()> {
    round_trip_fixed_size_binary_values(
        WriteBackend::BaselineTokenRow,
        "SQL Server baseline fixed-size binary integration test",
    )
    .await
}

#[tokio::test]
async fn direct_raw_writer_round_trips_fixed_size_binary_values() -> TestResult<()> {
    round_trip_fixed_size_binary_values(
        WriteBackend::DirectRawBulk,
        "SQL Server direct raw fixed-size binary integration test",
    )
    .await
}

#[tokio::test]
async fn direct_raw_writer_round_trips_fixed_size_binary_with_variable_width_values()
-> TestResult<()> {
    let Some((connection_string, database)) = integration_config() else {
        eprintln!(
            "skipping SQL Server direct raw fixed-size binary mixed integration test: {CONNECTION_STRING_ENV} or {TEST_DATABASE_ENV} is not set"
        );
        return Ok(());
    };

    let mut client = connect(&connection_string, &database).await?;
    let table = unique_table_name()?;
    let schema = Arc::new(Schema::new(vec![
        Field::new("row_id", DataType::Int32, false),
        Field::new("label", DataType::Utf8, true),
        Field::new("digest", DataType::FixedSizeBinary(3), true),
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
            Arc::new(StringArray::from(vec![Some("alpha"), None, Some("Tokyo")])),
            Arc::new(FixedSizeBinaryArray::try_from_sparse_iter_with_size(
                [Some(&b"abc"[..]), None, Some(&b"\x00\xff\x7f"[..])].into_iter(),
                3,
            )?),
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
                backend: WriteBackend::DirectRawBulk,
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
                "SELECT [row_id], [label], [digest] FROM {} ORDER BY [row_id]",
                table.quoted_sql()
            ))
            .await?
            .into_first_result()
            .await?;

        ensure_eq(rows.len(), 3, "row count")?;
        ensure_eq(rows[0].get::<i32, _>(0), Some(1), "row 0 row_id")?;
        ensure_eq(rows[0].get::<&str, _>(1), Some("alpha"), "row 0 label")?;
        ensure_eq(
            rows[0].get::<&[u8], _>(2),
            Some(&b"abc"[..]),
            "row 0 digest",
        )?;
        ensure_eq(rows[1].get::<i32, _>(0), Some(2), "row 1 row_id")?;
        ensure_eq(rows[1].get::<&str, _>(1), None, "row 1 label")?;
        ensure_eq(rows[1].get::<&[u8], _>(2), None, "row 1 digest")?;
        ensure_eq(rows[2].get::<i32, _>(0), Some(3), "row 2 row_id")?;
        ensure_eq(rows[2].get::<&str, _>(1), Some("Tokyo"), "row 2 label")?;
        ensure_eq(
            rows[2].get::<&[u8], _>(2),
            Some(&b"\x00\xff\x7f"[..]),
            "row 2 digest",
        )?;

        Ok::<(), Box<dyn std::error::Error>>(())
    }
    .await;

    let drop_result = drop_table(&mut client, &table).await;
    result?;
    drop_result?;

    Ok(())
}

async fn round_trip_fixed_size_binary_values(
    backend: WriteBackend,
    skip_context: &str,
) -> TestResult<()> {
    let Some((connection_string, database)) = integration_config() else {
        eprintln!(
            "skipping {skip_context}: {CONNECTION_STRING_ENV} or {TEST_DATABASE_ENV} is not set"
        );
        return Ok(());
    };

    let mut client = connect(&connection_string, &database).await?;
    let table = unique_table_name()?;
    let schema = Arc::new(Schema::new(vec![
        Field::new("row_id", DataType::Int32, false),
        Field::new("one_byte", DataType::FixedSizeBinary(1), true),
        Field::new("four_bytes", DataType::FixedSizeBinary(4), true),
        Field::new("max_bytes", DataType::FixedSizeBinary(8000), true),
    ]));
    let (mappings, _diagnostics) = plan_arrow_schema_to_mssql_mappings(
        Arc::clone(&schema),
        MssqlProfile::sql_server_2016_compat_100(),
        PlanOptions::default(),
    )?
    .into_parts();
    let max_zero = vec![0_u8; 8000];
    let max_ff = vec![0xff_u8; 8000];
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int32Array::from(vec![1_i32, 2, 3])) as ArrayRef,
            Arc::new(FixedSizeBinaryArray::try_from_sparse_iter_with_size(
                [Some(&b"\x00"[..]), Some(&b"\xff"[..]), None].into_iter(),
                1,
            )?),
            Arc::new(FixedSizeBinaryArray::try_from_sparse_iter_with_size(
                [Some(&b"\x00\x01\xfe\xff"[..]), Some(&b"abcd"[..]), None].into_iter(),
                4,
            )?),
            Arc::new(FixedSizeBinaryArray::try_from_sparse_iter_with_size(
                [Some(max_zero.as_slice()), Some(max_ff.as_slice()), None].into_iter(),
                8000,
            )?),
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
                backend,
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
                "SELECT [row_id], [one_byte], [four_bytes], [max_bytes] FROM {} ORDER BY [row_id]",
                table.quoted_sql()
            ))
            .await?
            .into_first_result()
            .await?;

        ensure_eq(rows.len(), 3, "row count")?;

        ensure_eq(rows[0].get::<i32, _>(0), Some(1), "row 0 row_id")?;
        ensure_eq(
            rows[0].get::<&[u8], _>(1),
            Some(&b"\x00"[..]),
            "row 0 one_byte",
        )?;
        ensure_eq(
            rows[0].get::<&[u8], _>(2),
            Some(&b"\x00\x01\xfe\xff"[..]),
            "row 0 four_bytes",
        )?;
        ensure_eq(
            rows[0].get::<&[u8], _>(3),
            Some(max_zero.as_slice()),
            "row 0 max_bytes",
        )?;

        ensure_eq(rows[1].get::<i32, _>(0), Some(2), "row 1 row_id")?;
        ensure_eq(
            rows[1].get::<&[u8], _>(1),
            Some(&b"\xff"[..]),
            "row 1 one_byte",
        )?;
        ensure_eq(
            rows[1].get::<&[u8], _>(2),
            Some(&b"abcd"[..]),
            "row 1 four_bytes",
        )?;
        ensure_eq(
            rows[1].get::<&[u8], _>(3),
            Some(max_ff.as_slice()),
            "row 1 max_bytes",
        )?;

        ensure_eq(rows[2].get::<i32, _>(0), Some(3), "row 2 row_id")?;
        ensure_eq(rows[2].get::<&[u8], _>(1), None, "row 2 one_byte")?;
        ensure_eq(rows[2].get::<&[u8], _>(2), None, "row 2 four_bytes")?;
        ensure_eq(rows[2].get::<&[u8], _>(3), None, "row 2 max_bytes")?;

        Ok::<(), Box<dyn std::error::Error>>(())
    }
    .await;

    let drop_result = drop_table(&mut client, &table).await;
    result?;
    drop_result?;

    Ok(())
}

#[tokio::test]
async fn direct_raw_writer_round_trips_fast_path_primitive_matrix() -> TestResult<()> {
    let Some((connection_string, database)) = integration_config() else {
        eprintln!(
            "skipping SQL Server direct raw primitive matrix integration test: {CONNECTION_STRING_ENV} or {TEST_DATABASE_ENV} is not set"
        );
        return Ok(());
    };

    let mut client = connect(&connection_string, &database).await?;
    let table = unique_table_name()?;
    let schema = Arc::new(Schema::new(vec![
        Field::new("row_id", DataType::Int32, false),
        Field::new("flag_nn", DataType::Boolean, false),
        Field::new("flag_null", DataType::Boolean, true),
        Field::new("u8_nn", DataType::UInt8, false),
        Field::new("u8_null", DataType::UInt8, true),
        Field::new("i8_nn", DataType::Int8, false),
        Field::new("i8_null", DataType::Int8, true),
        Field::new("i16_nn", DataType::Int16, false),
        Field::new("i16_null", DataType::Int16, true),
        Field::new("i32_nn", DataType::Int32, false),
        Field::new("i32_null", DataType::Int32, true),
        Field::new("u16_nn", DataType::UInt16, false),
        Field::new("u16_null", DataType::UInt16, true),
        Field::new("i64_nn", DataType::Int64, false),
        Field::new("i64_null", DataType::Int64, true),
        Field::new("u32_nn", DataType::UInt32, false),
        Field::new("u32_null", DataType::UInt32, true),
        Field::new("f32_nn", DataType::Float32, false),
        Field::new("f32_null", DataType::Float32, true),
        Field::new("f64_nn", DataType::Float64, false),
        Field::new("f64_null", DataType::Float64, true),
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
            Arc::new(BooleanArray::from(vec![true, false, true, false])),
            Arc::new(BooleanArray::from(vec![
                Some(true),
                None,
                Some(false),
                None,
            ])),
            Arc::new(UInt8Array::from(vec![u8::MIN, 1, 42, u8::MAX])),
            Arc::new(UInt8Array::from(vec![
                Some(u8::MIN),
                None,
                Some(u8::MAX),
                Some(0),
            ])),
            Arc::new(Int8Array::from(vec![i8::MIN, -1, 0, i8::MAX])),
            Arc::new(Int8Array::from(vec![
                Some(i8::MIN),
                None,
                Some(i8::MAX),
                Some(0),
            ])),
            Arc::new(Int16Array::from(vec![i16::MIN, -1, 0, i16::MAX])),
            Arc::new(Int16Array::from(vec![
                Some(i16::MIN),
                None,
                Some(i16::MAX),
                Some(0),
            ])),
            Arc::new(Int32Array::from(vec![i32::MIN, -1, 0, i32::MAX])),
            Arc::new(Int32Array::from(vec![
                Some(i32::MIN),
                None,
                Some(i32::MAX),
                Some(0),
            ])),
            Arc::new(UInt16Array::from(vec![u16::MIN, 1, 42, u16::MAX])),
            Arc::new(UInt16Array::from(vec![
                Some(u16::MIN),
                None,
                Some(u16::MAX),
                Some(0),
            ])),
            Arc::new(Int64Array::from(vec![i64::MIN, -1, 0, i64::MAX])),
            Arc::new(Int64Array::from(vec![
                Some(i64::MIN),
                None,
                Some(i64::MAX),
                Some(0),
            ])),
            Arc::new(UInt32Array::from(vec![u32::MIN, 1, 42, u32::MAX])),
            Arc::new(UInt32Array::from(vec![
                Some(u32::MIN),
                None,
                Some(u32::MAX),
                Some(0),
            ])),
            Arc::new(Float32Array::from(vec![-123.5, -0.0, 0.0, 42.25])),
            Arc::new(Float32Array::from(vec![
                Some(-123.5),
                None,
                Some(42.25),
                Some(0.0),
            ])),
            Arc::new(Float64Array::from(vec![-123.5, -0.0, 0.0, 42.25])),
            Arc::new(Float64Array::from(vec![
                Some(-123.5),
                None,
                Some(42.25),
                Some(0.0),
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
                backend: WriteBackend::DirectRawBulk,
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
                "SELECT [row_id], [flag_nn], [flag_null], [u8_nn], [u8_null], [i8_nn], [i8_null], [i16_nn], [i16_null], [i32_nn], [i32_null], [u16_nn], [u16_null], [i64_nn], [i64_null], [u32_nn], [u32_null], [f32_nn], [f32_null], [f64_nn], [f64_null] FROM {} ORDER BY [row_id]",
                table.quoted_sql()
            ))
            .await?
            .into_first_result()
            .await?;

        ensure_eq(rows.len(), 4, "row count")?;

        ensure_eq(rows[0].get::<i32, _>(0), Some(1), "row 0 row_id")?;
        ensure_eq(rows[0].get::<bool, _>(1), Some(true), "row 0 flag_nn")?;
        ensure_eq(rows[0].get::<bool, _>(2), Some(true), "row 0 flag_null")?;
        ensure_eq(rows[0].get::<u8, _>(3), Some(u8::MIN), "row 0 u8_nn")?;
        ensure_eq(
            rows[0].get::<u8, _>(4),
            Some(u8::MIN),
            "row 0 u8_null",
        )?;
        ensure_eq(rows[0].get::<i16, _>(5), Some(i8::MIN as i16), "row 0 i8_nn")?;
        ensure_eq(
            rows[0].get::<i16, _>(6),
            Some(i8::MIN as i16),
            "row 0 i8_null",
        )?;
        ensure_eq(rows[0].get::<i16, _>(7), Some(i16::MIN), "row 0 i16_nn")?;
        ensure_eq(
            rows[0].get::<i16, _>(8),
            Some(i16::MIN),
            "row 0 i16_null",
        )?;
        ensure_eq(rows[0].get::<i32, _>(9), Some(i32::MIN), "row 0 i32_nn")?;
        ensure_eq(
            rows[0].get::<i32, _>(10),
            Some(i32::MIN),
            "row 0 i32_null",
        )?;
        ensure_eq(
            rows[0].get::<i32, _>(11),
            Some(u16::MIN as i32),
            "row 0 u16_nn",
        )?;
        ensure_eq(
            rows[0].get::<i32, _>(12),
            Some(u16::MIN as i32),
            "row 0 u16_null",
        )?;
        ensure_eq(rows[0].get::<i64, _>(13), Some(i64::MIN), "row 0 i64_nn")?;
        ensure_eq(
            rows[0].get::<i64, _>(14),
            Some(i64::MIN),
            "row 0 i64_null",
        )?;
        ensure_eq(
            rows[0].get::<i64, _>(15),
            Some(u32::MIN as i64),
            "row 0 u32_nn",
        )?;
        ensure_eq(
            rows[0].get::<i64, _>(16),
            Some(u32::MIN as i64),
            "row 0 u32_null",
        )?;
        ensure_eq(rows[0].get::<f32, _>(17), Some(-123.5), "row 0 f32_nn")?;
        ensure_eq(
            rows[0].get::<f32, _>(18),
            Some(-123.5),
            "row 0 f32_null",
        )?;
        ensure_eq(rows[0].get::<f64, _>(19), Some(-123.5), "row 0 f64_nn")?;
        ensure_eq(
            rows[0].get::<f64, _>(20),
            Some(-123.5),
            "row 0 f64_null",
        )?;

        ensure_eq(rows[1].get::<i32, _>(0), Some(2), "row 1 row_id")?;
        ensure_eq(rows[1].get::<bool, _>(1), Some(false), "row 1 flag_nn")?;
        ensure_eq(rows[1].get::<bool, _>(2), None, "row 1 flag_null")?;
        ensure_eq(rows[1].get::<u8, _>(3), Some(1), "row 1 u8_nn")?;
        ensure_eq(rows[1].get::<u8, _>(4), None, "row 1 u8_null")?;
        ensure_eq(rows[1].get::<i16, _>(5), Some(-1), "row 1 i8_nn")?;
        ensure_eq(rows[1].get::<i16, _>(6), None, "row 1 i8_null")?;
        ensure_eq(rows[1].get::<i16, _>(7), Some(-1), "row 1 i16_nn")?;
        ensure_eq(rows[1].get::<i16, _>(8), None, "row 1 i16_null")?;
        ensure_eq(rows[1].get::<i32, _>(9), Some(-1), "row 1 i32_nn")?;
        ensure_eq(rows[1].get::<i32, _>(10), None, "row 1 i32_null")?;
        ensure_eq(rows[1].get::<i32, _>(11), Some(1), "row 1 u16_nn")?;
        ensure_eq(rows[1].get::<i32, _>(12), None, "row 1 u16_null")?;
        ensure_eq(rows[1].get::<i64, _>(13), Some(-1), "row 1 i64_nn")?;
        ensure_eq(rows[1].get::<i64, _>(14), None, "row 1 i64_null")?;
        ensure_eq(rows[1].get::<i64, _>(15), Some(1), "row 1 u32_nn")?;
        ensure_eq(rows[1].get::<i64, _>(16), None, "row 1 u32_null")?;
        ensure_eq(rows[1].get::<f32, _>(17), Some(-0.0), "row 1 f32_nn")?;
        ensure_eq(rows[1].get::<f32, _>(18), None, "row 1 f32_null")?;
        ensure_eq(rows[1].get::<f64, _>(19), Some(-0.0), "row 1 f64_nn")?;
        ensure_eq(rows[1].get::<f64, _>(20), None, "row 1 f64_null")?;

        ensure_eq(rows[2].get::<i32, _>(0), Some(3), "row 2 row_id")?;
        ensure_eq(rows[2].get::<bool, _>(1), Some(true), "row 2 flag_nn")?;
        ensure_eq(rows[2].get::<bool, _>(2), Some(false), "row 2 flag_null")?;
        ensure_eq(rows[2].get::<u8, _>(3), Some(42), "row 2 u8_nn")?;
        ensure_eq(rows[2].get::<u8, _>(4), Some(u8::MAX), "row 2 u8_null")?;
        ensure_eq(rows[2].get::<i16, _>(5), Some(0), "row 2 i8_nn")?;
        ensure_eq(
            rows[2].get::<i16, _>(6),
            Some(i8::MAX as i16),
            "row 2 i8_null",
        )?;
        ensure_eq(rows[2].get::<i16, _>(7), Some(0), "row 2 i16_nn")?;
        ensure_eq(
            rows[2].get::<i16, _>(8),
            Some(i16::MAX),
            "row 2 i16_null",
        )?;
        ensure_eq(rows[2].get::<i32, _>(9), Some(0), "row 2 i32_nn")?;
        ensure_eq(
            rows[2].get::<i32, _>(10),
            Some(i32::MAX),
            "row 2 i32_null",
        )?;
        ensure_eq(
            rows[2].get::<i32, _>(11),
            Some(42),
            "row 2 u16_nn",
        )?;
        ensure_eq(
            rows[2].get::<i32, _>(12),
            Some(u16::MAX as i32),
            "row 2 u16_null",
        )?;
        ensure_eq(rows[2].get::<i64, _>(13), Some(0), "row 2 i64_nn")?;
        ensure_eq(
            rows[2].get::<i64, _>(14),
            Some(i64::MAX),
            "row 2 i64_null",
        )?;
        ensure_eq(rows[2].get::<i64, _>(15), Some(42), "row 2 u32_nn")?;
        ensure_eq(
            rows[2].get::<i64, _>(16),
            Some(u32::MAX as i64),
            "row 2 u32_null",
        )?;
        ensure_eq(rows[2].get::<f32, _>(17), Some(0.0), "row 2 f32_nn")?;
        ensure_eq(rows[2].get::<f32, _>(18), Some(42.25), "row 2 f32_null")?;
        ensure_eq(rows[2].get::<f64, _>(19), Some(0.0), "row 2 f64_nn")?;
        ensure_eq(rows[2].get::<f64, _>(20), Some(42.25), "row 2 f64_null")?;

        ensure_eq(rows[3].get::<i32, _>(0), Some(4), "row 3 row_id")?;
        ensure_eq(rows[3].get::<bool, _>(1), Some(false), "row 3 flag_nn")?;
        ensure_eq(rows[3].get::<bool, _>(2), None, "row 3 flag_null")?;
        ensure_eq(rows[3].get::<u8, _>(3), Some(u8::MAX), "row 3 u8_nn")?;
        ensure_eq(rows[3].get::<u8, _>(4), Some(0), "row 3 u8_null")?;
        ensure_eq(
            rows[3].get::<i16, _>(5),
            Some(i8::MAX as i16),
            "row 3 i8_nn",
        )?;
        ensure_eq(rows[3].get::<i16, _>(6), Some(0), "row 3 i8_null")?;
        ensure_eq(
            rows[3].get::<i16, _>(7),
            Some(i16::MAX),
            "row 3 i16_nn",
        )?;
        ensure_eq(rows[3].get::<i16, _>(8), Some(0), "row 3 i16_null")?;
        ensure_eq(rows[3].get::<i32, _>(9), Some(i32::MAX), "row 3 i32_nn")?;
        ensure_eq(rows[3].get::<i32, _>(10), Some(0), "row 3 i32_null")?;
        ensure_eq(
            rows[3].get::<i32, _>(11),
            Some(u16::MAX as i32),
            "row 3 u16_nn",
        )?;
        ensure_eq(rows[3].get::<i32, _>(12), Some(0), "row 3 u16_null")?;
        ensure_eq(rows[3].get::<i64, _>(13), Some(i64::MAX), "row 3 i64_nn")?;
        ensure_eq(rows[3].get::<i64, _>(14), Some(0), "row 3 i64_null")?;
        ensure_eq(
            rows[3].get::<i64, _>(15),
            Some(u32::MAX as i64),
            "row 3 u32_nn",
        )?;
        ensure_eq(rows[3].get::<i64, _>(16), Some(0), "row 3 u32_null")?;
        ensure_eq(rows[3].get::<f32, _>(17), Some(42.25), "row 3 f32_nn")?;
        ensure_eq(rows[3].get::<f32, _>(18), Some(0.0), "row 3 f32_null")?;
        ensure_eq(rows[3].get::<f64, _>(19), Some(42.25), "row 3 f64_nn")?;
        ensure_eq(rows[3].get::<f64, _>(20), Some(0.0), "row 3 f64_null")?;

        Ok::<(), Box<dyn std::error::Error>>(())
    }
    .await;

    let drop_result = drop_table(&mut client, &table).await;
    result?;
    drop_result?;

    Ok(())
}

#[tokio::test]
async fn writer_round_trips_float16_real_values_across_supported_backends() -> TestResult<()> {
    let Some((connection_string, database)) = integration_config() else {
        eprintln!(
            "skipping SQL Server Float16 real integration test: {CONNECTION_STRING_ENV} or {TEST_DATABASE_ENV} is not set"
        );
        return Ok(());
    };

    for backend in [WriteBackend::BaselineTokenRow, WriteBackend::DirectRawBulk] {
        let mut client = connect(&connection_string, &database).await?;
        let table = unique_table_name()?;
        let schema = Arc::new(Schema::new(vec![
            Field::new("row_id", DataType::Int32, false),
            Field::new("half_nn", DataType::Float16, false),
            Field::new("half_null", DataType::Float16, true),
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
                Arc::new(Float16Array::from(vec![
                    F16::from_f32(1.5),
                    F16::from_f32(-0.0),
                    F16::from_f32(-2.25),
                ])),
                Arc::new(Float16Array::from(vec![
                    Some(F16::from_f32(3.5)),
                    None,
                    Some(F16::from_f32(0.0)),
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
                    backend,
                    ..WriteOptions::default()
                },
            )
            .await?;
            let stats = writer.write_batch(&batch).await?;

            ensure_eq(stats.rows_written, 3, "rows_written")?;
            ensure_eq(writer.finish().await?, stats, "finish stats")?;

            let rows = client
                .simple_query(format!(
                    "SELECT [row_id], [half_nn], [half_null] FROM {} ORDER BY [row_id]",
                    table.quoted_sql()
                ))
                .await?
                .into_first_result()
                .await?;

            ensure_eq(rows.len(), 3, "row count")?;
            ensure_eq(rows[0].get::<i32, _>(0), Some(1), "row 0 row_id")?;
            ensure_eq(rows[0].get::<f32, _>(1), Some(1.5), "row 0 half_nn")?;
            ensure_eq(rows[0].get::<f32, _>(2), Some(3.5), "row 0 half_null")?;
            ensure_eq(rows[1].get::<i32, _>(0), Some(2), "row 1 row_id")?;
            ensure_eq(rows[1].get::<f32, _>(1), Some(-0.0), "row 1 half_nn")?;
            ensure_eq(rows[1].get::<f32, _>(2), None, "row 1 half_null")?;
            ensure_eq(rows[2].get::<i32, _>(0), Some(3), "row 2 row_id")?;
            ensure_eq(rows[2].get::<f32, _>(1), Some(-2.25), "row 2 half_nn")?;
            ensure_eq(rows[2].get::<f32, _>(2), Some(0.0), "row 2 half_null")?;

            Ok::<(), Box<dyn std::error::Error>>(())
        }
        .await;

        let drop_result = drop_table(&mut client, &table).await;
        result?;
        drop_result?;
    }

    Ok(())
}

#[tokio::test]
async fn direct_raw_writer_round_trips_variable_width_matrix() -> TestResult<()> {
    let Some((connection_string, database)) = integration_config() else {
        eprintln!(
            "skipping SQL Server direct raw variable-width integration test: {CONNECTION_STRING_ENV} or {TEST_DATABASE_ENV} is not set"
        );
        return Ok(());
    };

    let mut client = connect(&connection_string, &database).await?;
    let table = unique_table_name()?;
    let schema = Arc::new(Schema::new(vec![
        Field::new("row_id", DataType::Int32, false),
        Field::new("tiny_value", DataType::UInt8, true),
        Field::new("signed_tiny_value", DataType::Int8, true),
        Field::new("small_value", DataType::Int16, true),
        Field::new("unsigned_medium_value", DataType::UInt16, true),
        Field::new("unsigned_total_value", DataType::UInt32, true),
        Field::new("real_value", DataType::Float32, true),
        Field::new("text_value", DataType::Utf8, true),
        Field::new("bytes_value", DataType::Binary, true),
    ]));
    let (mappings, _diagnostics) = plan_arrow_schema_to_mssql_mappings(
        Arc::clone(&schema),
        MssqlProfile::sql_server_2016_compat_100(),
        PlanOptions::default(),
    )?
    .into_parts();
    let large_text = "x".repeat(5000);
    let large_bytes = vec![0xab; 9000];
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int32Array::from(vec![1_i32, 2, 3, 4])) as ArrayRef,
            Arc::new(UInt8Array::from(vec![
                Some(u8::MIN),
                None,
                Some(42),
                Some(u8::MAX),
            ])),
            Arc::new(Int8Array::from(vec![
                Some(i8::MIN),
                None,
                Some(0),
                Some(i8::MAX),
            ])),
            Arc::new(Int16Array::from(vec![
                Some(i16::MIN),
                None,
                Some(0),
                Some(i16::MAX),
            ])),
            Arc::new(UInt16Array::from(vec![
                Some(u16::MIN),
                None,
                Some(42),
                Some(u16::MAX),
            ])),
            Arc::new(UInt32Array::from(vec![
                Some(u32::MIN),
                None,
                Some(42),
                Some(u32::MAX),
            ])),
            Arc::new(Float32Array::from(vec![
                Some(-123.5),
                None,
                Some(0.0),
                Some(42.25),
            ])),
            Arc::new(StringArray::from(vec![
                Some(""),
                Some("ascii"),
                Some("Tokyo 東京"),
                Some(large_text.as_str()),
            ])),
            Arc::new(BinaryArray::from_iter(vec![
                Some(&b""[..]),
                Some(&b"\x00\x01\xfe"[..]),
                None,
                Some(large_bytes.as_slice()),
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
                backend: WriteBackend::DirectRawBulk,
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
                "SELECT [row_id], [tiny_value], [signed_tiny_value], [small_value], [unsigned_medium_value], [unsigned_total_value], [real_value], [text_value], [bytes_value] FROM {} ORDER BY [row_id]",
                table.quoted_sql()
            ))
            .await?
            .into_first_result()
            .await?;

        ensure_eq(rows.len(), 4, "row count")?;

        ensure_eq(rows[0].get::<i32, _>(0), Some(1), "row 0 row_id")?;
        ensure_eq(rows[0].get::<u8, _>(1), Some(u8::MIN), "row 0 tiny")?;
        ensure_eq(
            rows[0].get::<i16, _>(2),
            Some(i8::MIN as i16),
            "row 0 signed_tiny",
        )?;
        ensure_eq(
            rows[0].get::<i16, _>(3),
            Some(i16::MIN),
            "row 0 small",
        )?;
        ensure_eq(
            rows[0].get::<i32, _>(4),
            Some(u16::MIN as i32),
            "row 0 unsigned_medium",
        )?;
        ensure_eq(
            rows[0].get::<i64, _>(5),
            Some(u32::MIN as i64),
            "row 0 unsigned_total",
        )?;
        ensure_eq(rows[0].get::<f32, _>(6), Some(-123.5), "row 0 real")?;
        ensure_eq(rows[0].get::<&str, _>(7), Some(""), "row 0 text_value")?;
        ensure_eq(
            rows[0].get::<&[u8], _>(8),
            Some(&b""[..]),
            "row 0 bytes_value",
        )?;

        ensure_eq(rows[1].get::<i32, _>(0), Some(2), "row 1 row_id")?;
        ensure_eq(rows[1].get::<u8, _>(1), None, "row 1 tiny")?;
        ensure_eq(rows[1].get::<i16, _>(2), None, "row 1 signed_tiny")?;
        ensure_eq(rows[1].get::<i16, _>(3), None, "row 1 small")?;
        ensure_eq(rows[1].get::<i32, _>(4), None, "row 1 unsigned_medium")?;
        ensure_eq(rows[1].get::<i64, _>(5), None, "row 1 unsigned_total")?;
        ensure_eq(rows[1].get::<f32, _>(6), None, "row 1 real")?;
        ensure_eq(
            rows[1].get::<&str, _>(7),
            Some("ascii"),
            "row 1 text_value",
        )?;
        ensure_eq(
            rows[1].get::<&[u8], _>(8),
            Some(&b"\x00\x01\xfe"[..]),
            "row 1 bytes_value",
        )?;

        ensure_eq(rows[2].get::<i32, _>(0), Some(3), "row 2 row_id")?;
        ensure_eq(rows[2].get::<u8, _>(1), Some(42), "row 2 tiny")?;
        ensure_eq(rows[2].get::<i16, _>(2), Some(0), "row 2 signed_tiny")?;
        ensure_eq(rows[2].get::<i16, _>(3), Some(0), "row 2 small")?;
        ensure_eq(
            rows[2].get::<i32, _>(4),
            Some(42),
            "row 2 unsigned_medium",
        )?;
        ensure_eq(
            rows[2].get::<i64, _>(5),
            Some(42),
            "row 2 unsigned_total",
        )?;
        ensure_eq(rows[2].get::<f32, _>(6), Some(0.0), "row 2 real")?;
        ensure_eq(
            rows[2].get::<&str, _>(7),
            Some("Tokyo 東京"),
            "row 2 text_value",
        )?;
        ensure_eq(rows[2].get::<&[u8], _>(8), None, "row 2 bytes_value")?;

        ensure_eq(rows[3].get::<i32, _>(0), Some(4), "row 3 row_id")?;
        ensure_eq(rows[3].get::<u8, _>(1), Some(u8::MAX), "row 3 tiny")?;
        ensure_eq(
            rows[3].get::<i16, _>(2),
            Some(i8::MAX as i16),
            "row 3 signed_tiny",
        )?;
        ensure_eq(
            rows[3].get::<i16, _>(3),
            Some(i16::MAX),
            "row 3 small",
        )?;
        ensure_eq(
            rows[3].get::<i32, _>(4),
            Some(u16::MAX as i32),
            "row 3 unsigned_medium",
        )?;
        ensure_eq(
            rows[3].get::<i64, _>(5),
            Some(u32::MAX as i64),
            "row 3 unsigned_total",
        )?;
        ensure_eq(rows[3].get::<f32, _>(6), Some(42.25), "row 3 real")?;
        ensure_eq(
            rows[3].get::<&str, _>(7),
            Some(large_text.as_str()),
            "row 3 text_value",
        )?;
        ensure_eq(
            rows[3].get::<&[u8], _>(8),
            Some(large_bytes.as_slice()),
            "row 3 bytes_value",
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
async fn direct_raw_writer_round_trips_large_variable_width_values() -> TestResult<()> {
    let Some((connection_string, database)) = integration_config() else {
        eprintln!(
            "skipping SQL Server direct raw large variable-width integration test: {CONNECTION_STRING_ENV} or {TEST_DATABASE_ENV} is not set"
        );
        return Ok(());
    };

    let mut client = connect(&connection_string, &database).await?;
    let table = unique_table_name()?;
    let schema = Arc::new(Schema::new(vec![
        Field::new("row_id", DataType::Int32, false),
        Field::new("large_text", DataType::LargeUtf8, true),
        Field::new("large_bytes", DataType::LargeBinary, true),
    ]));
    let (mappings, _diagnostics) = plan_arrow_schema_to_mssql_mappings(
        Arc::clone(&schema),
        MssqlProfile::sql_server_2016_compat_100(),
        PlanOptions::default(),
    )?
    .into_parts();
    let long_text = "x".repeat(5000);
    let long_bytes = vec![0xab; 9000];
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int32Array::from(vec![1_i32, 2, 3, 4])) as ArrayRef,
            Arc::new(LargeStringArray::from(vec![
                Some(""),
                Some("ascii"),
                Some("Tokyo 東京 🙂"),
                Some(long_text.as_str()),
            ])),
            Arc::new(LargeBinaryArray::from_iter(vec![
                Some(&b""[..]),
                Some(&b"\x00\x01\xfe\xff"[..]),
                None,
                Some(long_bytes.as_slice()),
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
                backend: WriteBackend::DirectRawBulk,
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
                "SELECT [row_id], [large_text], [large_bytes] FROM {} ORDER BY [row_id]",
                table.quoted_sql()
            ))
            .await?
            .into_first_result()
            .await?;

        ensure_eq(rows.len(), 4, "row count")?;

        ensure_eq(rows[0].get::<i32, _>(0), Some(1), "row 0 row_id")?;
        ensure_eq(rows[0].get::<&str, _>(1), Some(""), "row 0 large_text")?;
        ensure_eq(
            rows[0].get::<&[u8], _>(2),
            Some(&b""[..]),
            "row 0 large_bytes",
        )?;

        ensure_eq(rows[1].get::<i32, _>(0), Some(2), "row 1 row_id")?;
        ensure_eq(rows[1].get::<&str, _>(1), Some("ascii"), "row 1 large_text")?;
        ensure_eq(
            rows[1].get::<&[u8], _>(2),
            Some(&b"\x00\x01\xfe\xff"[..]),
            "row 1 large_bytes",
        )?;

        ensure_eq(rows[2].get::<i32, _>(0), Some(3), "row 2 row_id")?;
        ensure_eq(
            rows[2].get::<&str, _>(1),
            Some("Tokyo 東京 🙂"),
            "row 2 large_text",
        )?;
        ensure_eq(rows[2].get::<&[u8], _>(2), None, "row 2 large_bytes")?;

        ensure_eq(rows[3].get::<i32, _>(0), Some(4), "row 3 row_id")?;
        ensure_eq(
            rows[3].get::<&str, _>(1),
            Some(long_text.as_str()),
            "row 3 large_text",
        )?;
        ensure_eq(
            rows[3].get::<&[u8], _>(2),
            Some(long_bytes.as_slice()),
            "row 3 large_bytes",
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
#[ignore = "manual multi-GB allocation stress test for LargeBinary offsets above i32::MAX"]
async fn direct_raw_writer_round_trips_large_binary_offsets_above_i32_boundary() -> TestResult<()> {
    let Some((connection_string, database)) = integration_config() else {
        eprintln!(
            "skipping SQL Server direct raw LargeBinary offset stress test: {CONNECTION_STRING_ENV} or {TEST_DATABASE_ENV} is not set"
        );
        return Ok(());
    };

    let mut client = connect(&connection_string, &database).await?;
    let table = unique_table_name()?;
    let schema = Arc::new(Schema::new(vec![
        Field::new("row_id", DataType::Int32, false),
        Field::new("large_bytes", DataType::LargeBinary, true),
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
            large_binary_array_crossing_i32_offset_boundary()?,
        ],
    )?;
    let large_bytes = batch
        .column(1)
        .as_any()
        .downcast_ref::<LargeBinaryArray>()
        .ok_or_else(|| std::io::Error::other("large_bytes should be LargeBinaryArray"))?;
    ensure_eq(large_bytes.is_null(0), true, "row 0 local null")?;
    ensure_eq(
        &large_bytes.value(1)[..4],
        &[1, 2, 3, 4],
        "row 1 local prefix",
    )?;
    ensure_eq(
        &large_bytes.value(1)[12..16],
        &[5, 6, 7, 8],
        "row 1 local suffix",
    )?;
    ensure_eq(
        &large_bytes.value(2)[..4],
        &[9, 10, 11, 12],
        "row 2 local prefix",
    )?;
    ensure_eq(
        &large_bytes.value(2)[4..8],
        &[13, 14, 15, 16],
        "row 2 local suffix",
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
                backend: WriteBackend::DirectRawBulk,
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
                "SELECT [row_id], CONVERT(bigint, DATALENGTH([large_bytes])), \
                 SUBSTRING([large_bytes], 1, 4), \
                 SUBSTRING([large_bytes], DATALENGTH([large_bytes]) - 3, 4) \
                 FROM {} ORDER BY [row_id]",
                table.quoted_sql()
            ))
            .await?
            .into_first_result()
            .await?;

        ensure_eq(rows.len(), 3, "row count")?;
        ensure_eq(rows[0].get::<i32, _>(0), Some(1), "row 0 row_id")?;
        ensure_eq(rows[0].get::<i64, _>(1), None, "row 0 byte length")?;
        ensure_eq(rows[0].get::<&[u8], _>(2), None, "row 0 prefix")?;
        ensure_eq(rows[0].get::<&[u8], _>(3), None, "row 0 suffix")?;

        ensure_eq(rows[1].get::<i32, _>(0), Some(2), "row 1 row_id")?;
        ensure_eq(rows[1].get::<i64, _>(1), Some(16), "row 1 byte length")?;
        ensure_eq(
            rows[1].get::<&[u8], _>(2),
            Some(&[1, 2, 3, 4][..]),
            "row 1 prefix",
        )?;
        ensure_eq(
            rows[1].get::<&[u8], _>(3),
            Some(&[5, 6, 7, 8][..]),
            "row 1 suffix",
        )?;

        ensure_eq(rows[2].get::<i32, _>(0), Some(3), "row 2 row_id")?;
        ensure_eq(rows[2].get::<i64, _>(1), Some(8), "row 2 byte length")?;
        ensure_eq(
            rows[2].get::<&[u8], _>(2),
            Some(&[9, 10, 11, 12][..]),
            "row 2 prefix",
        )?;
        ensure_eq(
            rows[2].get::<&[u8], _>(3),
            Some(&[13, 14, 15, 16][..]),
            "row 2 suffix",
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
async fn writer_round_trips_uint64_policy_values_across_supported_backends() -> TestResult<()> {
    let Some((connection_string, database)) = integration_config() else {
        eprintln!(
            "skipping SQL Server UInt64 policy integration test: {CONNECTION_STRING_ENV} or {TEST_DATABASE_ENV} is not set"
        );
        return Ok(());
    };

    let mut client = connect(&connection_string, &database).await?;
    let decimal_schema = Arc::new(Schema::new(vec![
        Field::new("row_id", DataType::Int32, false),
        Field::new("u64_value", DataType::UInt64, true),
        Field::new("label", DataType::Utf8, true),
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
            Arc::new(StringArray::from(vec![
                Some("zero"),
                Some("over_bigint"),
                Some("max_u64"),
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

    for backend in [
        WriteBackend::BaselineTokenRow,
        WriteBackend::DirectFramedBulk,
        WriteBackend::DirectRawBulk,
    ] {
        let decimal_table = unique_table_name()?;
        let bigint_table = unique_table_name()?;

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
                decimal_mappings.clone(),
                WriteOptions {
                    backend,
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
                bigint_mappings.clone(),
                WriteOptions {
                    backend,
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
                    "SELECT [row_id], CONVERT(varchar(40), [u64_value]), [label] FROM {} ORDER BY [row_id]",
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
                decimal_rows[0].get::<&str, _>(2),
                Some("zero"),
                "decimal row 0 label",
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
                decimal_rows[1].get::<&str, _>(2),
                Some("over_bigint"),
                "decimal row 1 label",
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
                decimal_rows[2].get::<&str, _>(2),
                Some("max_u64"),
                "decimal row 2 label",
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
            ensure_eq(
                decimal_rows[3].get::<&str, _>(2),
                None,
                "decimal row 3 label",
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
    }

    Ok(())
}

#[tokio::test]
async fn writer_rejects_uint64_checked_bigint_overflow_without_partial_insert() -> TestResult<()> {
    let Some((connection_string, database)) = integration_config() else {
        eprintln!(
            "skipping SQL Server UInt64 overflow integration test: {CONNECTION_STRING_ENV} or {TEST_DATABASE_ENV} is not set"
        );
        return Ok(());
    };

    let mut client = connect(&connection_string, &database).await?;
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

    for backend in [
        WriteBackend::BaselineTokenRow,
        WriteBackend::DirectFramedBulk,
        WriteBackend::DirectRawBulk,
    ] {
        let table = unique_table_name()?;

        execute_sql(
            &mut client,
            create_table_sql_from_mappings(&table, &mappings),
        )
        .await?;

        let result = async {
            let mut writer = BulkWriter::new(
                &mut client,
                table.clone(),
                mappings.clone(),
                WriteOptions {
                    backend,
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

            let diagnostics = value_conversion_diagnostics(&err, WritePhase::ValueConversion)?;
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
    }

    Ok(())
}

#[tokio::test]
async fn baseline_writer_round_trips_decimal_policy_values() -> TestResult<()> {
    round_trip_decimal_policy_values(
        WriteBackend::BaselineTokenRow,
        "SQL Server decimal policy integration test",
    )
    .await
}

#[tokio::test]
async fn direct_raw_writer_round_trips_decimal_policy_values() -> TestResult<()> {
    round_trip_decimal_policy_values(
        WriteBackend::DirectRawBulk,
        "SQL Server direct raw decimal policy integration test",
    )
    .await
}

async fn round_trip_decimal_policy_values(
    backend: WriteBackend,
    skip_context: &str,
) -> TestResult<()> {
    let Some((connection_string, database)) = integration_config() else {
        eprintln!(
            "skipping {skip_context}: {CONNECTION_STRING_ENV} or {TEST_DATABASE_ENV} is not set"
        );
        return Ok(());
    };

    let mut client = connect(&connection_string, &database).await?;
    let table = unique_table_name()?;
    let schema = Arc::new(Schema::new(vec![
        Field::new("row_id", DataType::Int32, false),
        Field::new("d32_value", DataType::Decimal32(9, 2), true),
        Field::new("d64_value", DataType::Decimal64(18, 4), true),
        Field::new("d128_value", DataType::Decimal128(30, 6), true),
        Field::new("d256_value", DataType::Decimal256(30, 0), true),
        Field::new("negative_scale_value", DataType::Decimal128(3, -2), true),
    ]));
    let (mappings, _diagnostics) = plan_arrow_schema_to_mssql_mappings(
        Arc::clone(&schema),
        MssqlProfile::sql_server_2016_compat_100(),
        PlanOptions {
            decimal_policy: DecimalPolicy::NormalizeNegativeScale,
            ..PlanOptions::default()
        },
    )?
    .into_parts();
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int32Array::from(vec![1_i32, 2, 3, 4])) as ArrayRef,
            Arc::new(
                Decimal32Array::from(vec![Some(12_345_i32), Some(-12_345), Some(0), None])
                    .with_precision_and_scale(9, 2)?,
            ),
            Arc::new(
                Decimal64Array::from(vec![
                    Some(1_234_567_890_i64),
                    Some(-1_234_567_890),
                    Some(0),
                    None,
                ])
                .with_precision_and_scale(18, 4)?,
            ),
            Arc::new(
                Decimal128Array::from(vec![
                    Some(123_456_789_012_345_678_901_234_567_890_i128),
                    Some(-123_456_789_012_345_678_901_234_567_890_i128),
                    Some(0),
                    None,
                ])
                .with_precision_and_scale(30, 6)?,
            ),
            Arc::new(
                Decimal256Array::from(vec![
                    Some(i256::from_i128(
                        123_456_789_012_345_678_901_234_567_890_i128,
                    )),
                    Some(i256::from_i128(
                        -123_456_789_012_345_678_901_234_567_890_i128,
                    )),
                    Some(i256::ZERO),
                    None,
                ])
                .with_precision_and_scale(30, 0)?,
            ),
            Arc::new(
                Decimal128Array::from(vec![Some(123_i128), Some(-123), Some(0), None])
                    .with_precision_and_scale(3, -2)?,
            ),
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
                backend,
                ..WriteOptions::default()
            },
        )
        .await?;
        let stats = writer.write_batch(&batch).await?;
        ensure_eq(stats.rows_written, 4, "decimal rows_written")?;
        ensure_eq(stats.batches_written, 1, "decimal batches_written")?;
        ensure_eq(writer.finish().await?, stats, "decimal finish stats")?;

        let rows = client
            .simple_query(format!(
                "SELECT [row_id], CONVERT(varchar(80), [d32_value]), CONVERT(varchar(80), [d64_value]), CONVERT(varchar(80), [d128_value]), CONVERT(varchar(80), [d256_value]), CONVERT(varchar(80), [negative_scale_value]) FROM {} ORDER BY [row_id]",
                table.quoted_sql()
            ))
            .await?
            .into_first_result()
            .await?;

        ensure_eq(rows.len(), 4, "decimal row count")?;

        ensure_eq(rows[0].get::<i32, _>(0), Some(1), "row 0 row_id")?;
        ensure_eq(rows[0].get::<&str, _>(1), Some("123.45"), "row 0 d32")?;
        ensure_eq(
            rows[0].get::<&str, _>(2),
            Some("123456.7890"),
            "row 0 d64",
        )?;
        ensure_eq(
            rows[0].get::<&str, _>(3),
            Some("123456789012345678901234.567890"),
            "row 0 d128",
        )?;
        ensure_eq(
            rows[0].get::<&str, _>(4),
            Some("123456789012345678901234567890"),
            "row 0 d256",
        )?;
        ensure_eq(
            rows[0].get::<&str, _>(5),
            Some("12300"),
            "row 0 negative scale",
        )?;

        ensure_eq(rows[1].get::<i32, _>(0), Some(2), "row 1 row_id")?;
        ensure_eq(rows[1].get::<&str, _>(1), Some("-123.45"), "row 1 d32")?;
        ensure_eq(
            rows[1].get::<&str, _>(2),
            Some("-123456.7890"),
            "row 1 d64",
        )?;
        ensure_eq(
            rows[1].get::<&str, _>(3),
            Some("-123456789012345678901234.567890"),
            "row 1 d128",
        )?;
        ensure_eq(
            rows[1].get::<&str, _>(4),
            Some("-123456789012345678901234567890"),
            "row 1 d256",
        )?;
        ensure_eq(
            rows[1].get::<&str, _>(5),
            Some("-12300"),
            "row 1 negative scale",
        )?;

        ensure_eq(rows[2].get::<i32, _>(0), Some(3), "row 2 row_id")?;
        ensure_eq(rows[2].get::<&str, _>(1), Some("0.00"), "row 2 d32")?;
        ensure_eq(rows[2].get::<&str, _>(2), Some("0.0000"), "row 2 d64")?;
        ensure_eq(
            rows[2].get::<&str, _>(3),
            Some("0.000000"),
            "row 2 d128",
        )?;
        ensure_eq(rows[2].get::<&str, _>(4), Some("0"), "row 2 d256")?;
        ensure_eq(
            rows[2].get::<&str, _>(5),
            Some("0"),
            "row 2 negative scale",
        )?;

        ensure_eq(rows[3].get::<i32, _>(0), Some(4), "row 3 row_id")?;
        ensure_eq(rows[3].get::<&str, _>(1), None, "row 3 d32")?;
        ensure_eq(rows[3].get::<&str, _>(2), None, "row 3 d64")?;
        ensure_eq(rows[3].get::<&str, _>(3), None, "row 3 d128")?;
        ensure_eq(rows[3].get::<&str, _>(4), None, "row 3 d256")?;
        ensure_eq(rows[3].get::<&str, _>(5), None, "row 3 negative scale")?;

        Ok::<(), Box<dyn std::error::Error>>(())
    }
    .await;

    let drop_result = drop_table(&mut client, &table).await;
    result?;
    drop_result?;

    Ok(())
}

#[tokio::test]
async fn baseline_writer_round_trips_date32_values() -> TestResult<()> {
    round_trip_date32_values(
        WriteBackend::BaselineTokenRow,
        "SQL Server Date32 round-trip integration test",
    )
    .await
}

#[tokio::test]
async fn direct_raw_writer_round_trips_date32_values() -> TestResult<()> {
    round_trip_date32_values(
        WriteBackend::DirectRawBulk,
        "SQL Server direct raw Date32 round-trip integration test",
    )
    .await
}

async fn round_trip_date32_values(backend: WriteBackend, test_name: &str) -> TestResult<()> {
    let Some((connection_string, database)) = integration_config() else {
        eprintln!(
            "skipping {test_name}: {CONNECTION_STRING_ENV} or {TEST_DATABASE_ENV} is not set"
        );
        return Ok(());
    };

    let mut client = connect(&connection_string, &database).await?;
    let table = unique_table_name()?;
    let schema = Arc::new(Schema::new(vec![
        Field::new("row_id", DataType::Int32, false),
        Field::new("date32_value", DataType::Date32, true),
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
            Arc::new(Date32Array::from(vec![
                Some(-1_i32),
                Some(0_i32),
                Some(1_i32),
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
                backend,
                ..WriteOptions::default()
            },
        )
        .await?;
        let stats = writer.write_batch(&batch).await?;

        ensure_eq(stats.rows_written, 4, "Date32 rows_written")?;
        ensure_eq(stats.batches_written, 1, "Date32 batches_written")?;
        ensure_eq(writer.finish().await?, stats, "Date32 finish stats")?;

        let rows = client
            .simple_query(format!(
                "SELECT [row_id], CONVERT(varchar(20), [date32_value], 126) FROM {} ORDER BY [row_id]",
                table.quoted_sql()
            ))
            .await?
            .into_first_result()
            .await?;

        ensure_eq(rows.len(), 4, "Date32 row count")?;
        ensure_eq(rows[0].get::<i32, _>(0), Some(1), "Date32 row 0 id")?;
        ensure_eq(
            rows[0].get::<&str, _>(1),
            Some("1969-12-31"),
            "Date32 row 0 value",
        )?;
        ensure_eq(rows[1].get::<i32, _>(0), Some(2), "Date32 row 1 id")?;
        ensure_eq(
            rows[1].get::<&str, _>(1),
            Some("1970-01-01"),
            "Date32 row 1 value",
        )?;
        ensure_eq(rows[2].get::<i32, _>(0), Some(3), "Date32 row 2 id")?;
        ensure_eq(
            rows[2].get::<&str, _>(1),
            Some("1970-01-02"),
            "Date32 row 2 value",
        )?;
        ensure_eq(rows[3].get::<i32, _>(0), Some(4), "Date32 row 3 id")?;
        ensure_eq(rows[3].get::<&str, _>(1), None, "Date32 row 3 value")?;

        Ok::<(), Box<dyn std::error::Error>>(())
    }
    .await;

    let drop_result = drop_table(&mut client, &table).await;
    result?;
    drop_result?;

    Ok(())
}

#[tokio::test]
async fn baseline_writer_round_trips_date64_datetime2_values() -> TestResult<()> {
    round_trip_date64_datetime2_values(
        WriteBackend::BaselineTokenRow,
        "SQL Server Date64 datetime2 round-trip integration test",
    )
    .await
}

#[tokio::test]
async fn direct_raw_writer_round_trips_date64_datetime2_values() -> TestResult<()> {
    round_trip_date64_datetime2_values(
        WriteBackend::DirectRawBulk,
        "SQL Server direct raw Date64 datetime2 round-trip integration test",
    )
    .await
}

async fn round_trip_date64_datetime2_values(
    backend: WriteBackend,
    test_name: &str,
) -> TestResult<()> {
    let Some((connection_string, database)) = integration_config() else {
        eprintln!(
            "skipping {test_name}: {CONNECTION_STRING_ENV} or {TEST_DATABASE_ENV} is not set"
        );
        return Ok(());
    };

    let mut client = connect(&connection_string, &database).await?;
    let table = unique_table_name()?;
    let schema = Arc::new(Schema::new(vec![
        Field::new("row_id", DataType::Int32, false),
        Field::new("date64_value", DataType::Date64, true),
    ]));
    let (mappings, _diagnostics) = plan_arrow_schema_to_mssql_mappings(
        Arc::clone(&schema),
        MssqlProfile::sql_server_2016_compat_100(),
        PlanOptions {
            date64_policy: Date64Policy::TimestampDateTime2,
            ..PlanOptions::default()
        },
    )?
    .into_parts();
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int32Array::from(vec![1_i32, 2, 3, 4])) as ArrayRef,
            Arc::new(Date64Array::from(vec![
                Some(-1_i64),
                Some(0_i64),
                Some(86_400_123_i64),
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
                backend,
                ..WriteOptions::default()
            },
        )
        .await?;
        let stats = writer.write_batch(&batch).await?;

        ensure_eq(stats.rows_written, 4, "Date64 rows_written")?;
        ensure_eq(stats.batches_written, 1, "Date64 batches_written")?;
        ensure_eq(writer.finish().await?, stats, "Date64 finish stats")?;

        let rows = client
            .simple_query(format!(
                "SELECT [row_id], CONVERT(varchar(30), [date64_value], 126) FROM {} ORDER BY [row_id]",
                table.quoted_sql()
            ))
            .await?
            .into_first_result()
            .await?;

        ensure_eq(rows.len(), 4, "Date64 row count")?;
        ensure_eq(rows[0].get::<i32, _>(0), Some(1), "Date64 row 0 id")?;
        ensure_eq(
            rows[0].get::<&str, _>(1),
            Some("1969-12-31T23:59:59.999"),
            "Date64 row 0 value",
        )?;
        ensure_eq(rows[1].get::<i32, _>(0), Some(2), "Date64 row 1 id")?;
        ensure_eq(
            rows[1].get::<&str, _>(1),
            Some("1970-01-01T00:00:00"),
            "Date64 row 1 value",
        )?;
        ensure_eq(rows[2].get::<i32, _>(0), Some(3), "Date64 row 2 id")?;
        ensure_eq(
            rows[2].get::<&str, _>(1),
            Some("1970-01-02T00:00:00.123"),
            "Date64 row 2 value",
        )?;
        ensure_eq(rows[3].get::<i32, _>(0), Some(4), "Date64 row 3 id")?;
        ensure_eq(rows[3].get::<&str, _>(1), None, "Date64 row 3 value")?;

        Ok::<(), Box<dyn std::error::Error>>(())
    }
    .await;

    let drop_result = drop_table(&mut client, &table).await;
    result?;
    drop_result?;

    Ok(())
}

#[tokio::test]
async fn baseline_writer_round_trips_timezone_free_timestamp_datetime2_values() -> TestResult<()> {
    round_trip_timezone_free_timestamp_datetime2_values(
        WriteBackend::BaselineTokenRow,
        "SQL Server timezone-free timestamp round-trip integration test",
    )
    .await
}

#[tokio::test]
async fn direct_raw_writer_round_trips_timezone_free_timestamp_datetime2_values() -> TestResult<()>
{
    round_trip_timezone_free_timestamp_datetime2_values(
        WriteBackend::DirectRawBulk,
        "SQL Server direct raw timezone-free timestamp round-trip integration test",
    )
    .await
}

async fn round_trip_timezone_free_timestamp_datetime2_values(
    backend: WriteBackend,
    test_name: &str,
) -> TestResult<()> {
    let Some((connection_string, database)) = integration_config() else {
        eprintln!(
            "skipping {test_name}: {CONNECTION_STRING_ENV} or {TEST_DATABASE_ENV} is not set"
        );
        return Ok(());
    };

    let mut client = connect(&connection_string, &database).await?;
    let table = unique_table_name()?;
    let plan_options = PlanOptions {
        nanosecond_policy: NanosecondPolicy::TruncateTo100ns,
        ..PlanOptions::default()
    };
    let schema = Arc::new(Schema::new(vec![
        Field::new("row_id", DataType::Int32, false),
        Field::new("ts_s", DataType::Timestamp(TimeUnit::Second, None), true),
        Field::new(
            "ts_ms",
            DataType::Timestamp(TimeUnit::Millisecond, None),
            true,
        ),
        Field::new(
            "ts_us",
            DataType::Timestamp(TimeUnit::Microsecond, None),
            true,
        ),
        Field::new(
            "ts_ns",
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            true,
        ),
    ]));
    let (mappings, _diagnostics) = plan_arrow_schema_to_mssql_mappings(
        Arc::clone(&schema),
        MssqlProfile::sql_server_2016_compat_100(),
        plan_options,
    )?
    .into_parts();
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int32Array::from(vec![1_i32, 2, 3])) as ArrayRef,
            Arc::new(TimestampSecondArray::from(vec![
                Some(0_i64),
                Some(-1_i64),
                None,
            ])),
            Arc::new(TimestampMillisecondArray::from(vec![
                Some(-1_i64),
                Some(86_400_123_i64),
                None,
            ])),
            Arc::new(TimestampMicrosecondArray::from(vec![
                Some(1_234_567_i64),
                Some(0_i64),
                None,
            ])),
            Arc::new(TimestampNanosecondArray::from(vec![
                Some(123_456_789_i64),
                Some(-149_i64),
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
                backend,
                plan_options,
                ..WriteOptions::default()
            },
        )
        .await?;
        let stats = writer.write_batch(&batch).await?;

        ensure_eq(stats.rows_written, 3, "timestamp rows_written")?;
        ensure_eq(stats.batches_written, 1, "timestamp batches_written")?;
        ensure_eq(writer.finish().await?, stats, "timestamp finish stats")?;

        let rows = client
            .simple_query(format!(
                "SELECT [row_id], CONVERT(varchar(40), [ts_s], 126), CONVERT(varchar(40), [ts_ms], 126), CONVERT(varchar(40), [ts_us], 126), CONVERT(varchar(40), [ts_ns], 126) FROM {} ORDER BY [row_id]",
                table.quoted_sql()
            ))
            .await?
            .into_first_result()
            .await?;

        ensure_eq(rows.len(), 3, "timestamp row count")?;
        ensure_eq(rows[0].get::<i32, _>(0), Some(1), "timestamp row 0 id")?;
        ensure_eq(
            rows[0].get::<&str, _>(1),
            Some("1970-01-01T00:00:00"),
            "timestamp row 0 second",
        )?;
        ensure_eq(
            rows[0].get::<&str, _>(2),
            Some("1969-12-31T23:59:59.9990000"),
            "timestamp row 0 millisecond",
        )?;
        ensure_eq(
            rows[0].get::<&str, _>(3),
            Some("1970-01-01T00:00:01.2345670"),
            "timestamp row 0 microsecond",
        )?;
        ensure_eq(
            rows[0].get::<&str, _>(4),
            Some("1970-01-01T00:00:00.1234567"),
            "timestamp row 0 nanosecond",
        )?;

        ensure_eq(rows[1].get::<i32, _>(0), Some(2), "timestamp row 1 id")?;
        ensure_eq(
            rows[1].get::<&str, _>(1),
            Some("1969-12-31T23:59:59"),
            "timestamp row 1 second",
        )?;
        ensure_eq(
            rows[1].get::<&str, _>(2),
            Some("1970-01-02T00:00:00.1230000"),
            "timestamp row 1 millisecond",
        )?;
        ensure_eq(
            rows[1].get::<&str, _>(3),
            Some("1970-01-01T00:00:00"),
            "timestamp row 1 microsecond",
        )?;
        ensure_eq(
            rows[1].get::<&str, _>(4),
            Some("1969-12-31T23:59:59.9999998"),
            "timestamp row 1 nanosecond",
        )?;

        ensure_eq(rows[2].get::<i32, _>(0), Some(3), "timestamp row 2 id")?;
        ensure_eq(rows[2].get::<&str, _>(1), None, "timestamp row 2 second")?;
        ensure_eq(
            rows[2].get::<&str, _>(2),
            None,
            "timestamp row 2 millisecond",
        )?;
        ensure_eq(
            rows[2].get::<&str, _>(3),
            None,
            "timestamp row 2 microsecond",
        )?;
        ensure_eq(
            rows[2].get::<&str, _>(4),
            None,
            "timestamp row 2 nanosecond",
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
async fn writer_round_trips_timezone_free_timestamp_datetime2_0_values_across_supported_backends()
-> TestResult<()> {
    let Some((connection_string, database)) = integration_config() else {
        eprintln!(
            "skipping SQL Server timestamp datetime2(0) round-trip integration test: {CONNECTION_STRING_ENV} or {TEST_DATABASE_ENV} is not set"
        );
        return Ok(());
    };

    for backend in [WriteBackend::BaselineTokenRow, WriteBackend::DirectRawBulk] {
        let mut client = connect(&connection_string, &database).await?;
        let table = unique_table_name()?;
        let plan_options = PlanOptions {
            timestamp_policy: TimestampPolicy::DateTime2 { precision: 0 },
            ..PlanOptions::default()
        };
        let schema = Arc::new(Schema::new(vec![
            Field::new("row_id", DataType::Int32, false),
            Field::new(
                "created_at",
                DataType::Timestamp(TimeUnit::Microsecond, None),
                true,
            ),
        ]));
        let (mappings, _diagnostics) = plan_arrow_schema_to_mssql_mappings(
            Arc::clone(&schema),
            MssqlProfile::sql_server_2016_compat_100(),
            plan_options,
        )?
        .into_parts();
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int32Array::from(vec![1_i32, 2, 3])) as ArrayRef,
                Arc::new(TimestampMicrosecondArray::from(vec![
                    Some(1_600_000_i64),
                    Some(86_399_600_000_i64),
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
                    backend,
                    plan_options,
                    ..WriteOptions::default()
                },
            )
            .await?;
            let stats = writer.write_batch(&batch).await?;

            ensure_eq(stats.rows_written, 3, "timestamp datetime2(0) rows_written")?;
            ensure_eq(
                stats.batches_written,
                1,
                "timestamp datetime2(0) batches_written",
            )?;
            ensure_eq(
                writer.finish().await?,
                stats,
                "timestamp datetime2(0) finish stats",
            )?;

            let rows = client
                .simple_query(format!(
                    "SELECT [row_id], CONVERT(varchar(40), [created_at], 126) FROM {} ORDER BY [row_id]",
                    table.quoted_sql()
                ))
                .await?
                .into_first_result()
                .await?;

            ensure_eq(rows.len(), 3, "timestamp datetime2(0) row count")?;
            ensure_eq(
                rows[0].get::<i32, _>(0),
                Some(1),
                "timestamp datetime2(0) row 0 id",
            )?;
            ensure_eq(
                rows[0].get::<&str, _>(1),
                Some("1970-01-01T00:00:02"),
                "timestamp datetime2(0) row 0 value",
            )?;
            ensure_eq(
                rows[1].get::<i32, _>(0),
                Some(2),
                "timestamp datetime2(0) row 1 id",
            )?;
            ensure_eq(
                rows[1].get::<&str, _>(1),
                Some("1970-01-02T00:00:00"),
                "timestamp datetime2(0) row 1 value",
            )?;
            ensure_eq(
                rows[2].get::<i32, _>(0),
                Some(3),
                "timestamp datetime2(0) row 2 id",
            )?;
            ensure_eq(
                rows[2].get::<&str, _>(1),
                None,
                "timestamp datetime2(0) row 2 value",
            )?;

            Ok::<(), Box<dyn std::error::Error>>(())
        }
        .await;

        let drop_result = drop_table(&mut client, &table).await;
        result?;
        drop_result?;
    }

    Ok(())
}

#[tokio::test]
async fn writer_round_trips_timezone_free_timestamp_datetime2_3_values_across_supported_backends()
-> TestResult<()> {
    let Some((connection_string, database)) = integration_config() else {
        eprintln!(
            "skipping SQL Server timestamp datetime2(3) round-trip integration test: {CONNECTION_STRING_ENV} or {TEST_DATABASE_ENV} is not set"
        );
        return Ok(());
    };

    for backend in [WriteBackend::BaselineTokenRow, WriteBackend::DirectRawBulk] {
        let mut client = connect(&connection_string, &database).await?;
        let table = unique_table_name()?;
        let plan_options = PlanOptions {
            timestamp_policy: TimestampPolicy::DateTime2 { precision: 3 },
            ..PlanOptions::default()
        };
        let schema = Arc::new(Schema::new(vec![
            Field::new("row_id", DataType::Int32, false),
            Field::new(
                "created_at",
                DataType::Timestamp(TimeUnit::Microsecond, None),
                true,
            ),
        ]));
        let (mappings, _diagnostics) = plan_arrow_schema_to_mssql_mappings(
            Arc::clone(&schema),
            MssqlProfile::sql_server_2016_compat_100(),
            plan_options,
        )?
        .into_parts();
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int32Array::from(vec![1_i32, 2, 3])) as ArrayRef,
                Arc::new(TimestampMicrosecondArray::from(vec![
                    Some(1_234_567_i64),
                    Some(86_399_999_500_i64),
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
                    backend,
                    plan_options,
                    ..WriteOptions::default()
                },
            )
            .await?;
            let stats = writer.write_batch(&batch).await?;

            ensure_eq(stats.rows_written, 3, "timestamp datetime2(3) rows_written")?;
            ensure_eq(
                stats.batches_written,
                1,
                "timestamp datetime2(3) batches_written",
            )?;
            ensure_eq(
                writer.finish().await?,
                stats,
                "timestamp datetime2(3) finish stats",
            )?;

            let rows = client
                .simple_query(format!(
                    "SELECT [row_id], CONVERT(varchar(40), [created_at], 126) FROM {} ORDER BY [row_id]",
                    table.quoted_sql()
                ))
                .await?
                .into_first_result()
                .await?;

            ensure_eq(rows.len(), 3, "timestamp datetime2(3) row count")?;
            ensure_eq(
                rows[0].get::<i32, _>(0),
                Some(1),
                "timestamp datetime2(3) row 0 id",
            )?;
            ensure_eq(
                rows[0].get::<&str, _>(1),
                Some("1970-01-01T00:00:01.235"),
                "timestamp datetime2(3) row 0 value",
            )?;
            ensure_eq(
                rows[1].get::<i32, _>(0),
                Some(2),
                "timestamp datetime2(3) row 1 id",
            )?;
            ensure_eq(
                rows[1].get::<&str, _>(1),
                Some("1970-01-02T00:00:00"),
                "timestamp datetime2(3) row 1 value",
            )?;
            ensure_eq(
                rows[2].get::<i32, _>(0),
                Some(3),
                "timestamp datetime2(3) row 2 id",
            )?;
            ensure_eq(
                rows[2].get::<&str, _>(1),
                None,
                "timestamp datetime2(3) row 2 value",
            )?;

            Ok::<(), Box<dyn std::error::Error>>(())
        }
        .await;

        let drop_result = drop_table(&mut client, &table).await;
        result?;
        drop_result?;
    }

    Ok(())
}

#[tokio::test]
async fn writer_round_trips_timezone_free_timestamp_datetime2_6_values_across_supported_backends()
-> TestResult<()> {
    let Some((connection_string, database)) = integration_config() else {
        eprintln!(
            "skipping SQL Server timestamp datetime2(6) round-trip integration test: {CONNECTION_STRING_ENV} or {TEST_DATABASE_ENV} is not set"
        );
        return Ok(());
    };

    for backend in [WriteBackend::BaselineTokenRow, WriteBackend::DirectRawBulk] {
        let mut client = connect(&connection_string, &database).await?;
        let table = unique_table_name()?;
        let plan_options = PlanOptions {
            timestamp_policy: TimestampPolicy::DateTime2 { precision: 6 },
            nanosecond_policy: NanosecondPolicy::RoundTo100ns,
            ..PlanOptions::default()
        };
        let schema = Arc::new(Schema::new(vec![
            Field::new("row_id", DataType::Int32, false),
            Field::new(
                "created_at",
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                true,
            ),
        ]));
        let (mappings, _diagnostics) = plan_arrow_schema_to_mssql_mappings(
            Arc::clone(&schema),
            MssqlProfile::sql_server_2016_compat_100(),
            plan_options,
        )?
        .into_parts();
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int32Array::from(vec![1_i32, 2, 3])) as ArrayRef,
                Arc::new(TimestampNanosecondArray::from(vec![
                    Some(1_234_567_500_i64),
                    Some(86_399_999_999_500_i64),
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
                    backend,
                    plan_options,
                    ..WriteOptions::default()
                },
            )
            .await?;
            let stats = writer.write_batch(&batch).await?;

            ensure_eq(stats.rows_written, 3, "timestamp datetime2(6) rows_written")?;
            ensure_eq(
                stats.batches_written,
                1,
                "timestamp datetime2(6) batches_written",
            )?;
            ensure_eq(
                writer.finish().await?,
                stats,
                "timestamp datetime2(6) finish stats",
            )?;

            let rows = client
                .simple_query(format!(
                    "SELECT [row_id], CONVERT(varchar(40), [created_at], 126) FROM {} ORDER BY [row_id]",
                    table.quoted_sql()
                ))
                .await?
                .into_first_result()
                .await?;

            ensure_eq(rows.len(), 3, "timestamp datetime2(6) row count")?;
            ensure_eq(
                rows[0].get::<i32, _>(0),
                Some(1),
                "timestamp datetime2(6) row 0 id",
            )?;
            ensure_eq(
                rows[0].get::<&str, _>(1),
                Some("1970-01-01T00:00:01.234568"),
                "timestamp datetime2(6) row 0 value",
            )?;
            ensure_eq(
                rows[1].get::<i32, _>(0),
                Some(2),
                "timestamp datetime2(6) row 1 id",
            )?;
            ensure_eq(
                rows[1].get::<&str, _>(1),
                Some("1970-01-02T00:00:00"),
                "timestamp datetime2(6) row 1 value",
            )?;
            ensure_eq(
                rows[2].get::<i32, _>(0),
                Some(3),
                "timestamp datetime2(6) row 2 id",
            )?;
            ensure_eq(
                rows[2].get::<&str, _>(1),
                None,
                "timestamp datetime2(6) row 2 value",
            )?;

            Ok::<(), Box<dyn std::error::Error>>(())
        }
        .await;

        let drop_result = drop_table(&mut client, &table).await;
        result?;
        drop_result?;
    }

    Ok(())
}

#[tokio::test]
async fn writer_round_trips_timezone_free_timestamp_datetime_values_across_supported_backends()
-> TestResult<()> {
    let Some((connection_string, database)) = integration_config() else {
        eprintln!(
            "skipping SQL Server timestamp datetime round-trip integration test: {CONNECTION_STRING_ENV} or {TEST_DATABASE_ENV} is not set"
        );
        return Ok(());
    };

    for backend in [WriteBackend::BaselineTokenRow, WriteBackend::DirectRawBulk] {
        let mut client = connect(&connection_string, &database).await?;
        let table = unique_table_name()?;
        let plan_options = PlanOptions {
            timestamp_policy: TimestampPolicy::DateTime,
            ..PlanOptions::default()
        };
        let schema = Arc::new(Schema::new(vec![
            Field::new("row_id", DataType::Int32, false),
            Field::new(
                "created_at",
                DataType::Timestamp(TimeUnit::Microsecond, None),
                true,
            ),
        ]));
        let (mappings, _diagnostics) = plan_arrow_schema_to_mssql_mappings(
            Arc::clone(&schema),
            MssqlProfile::sql_server_2016_compat_100(),
            plan_options,
        )?
        .into_parts();
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int32Array::from(vec![1_i32, 2, 3, 4])) as ArrayRef,
                Arc::new(TimestampMicrosecondArray::from(vec![
                    Some(-315_619_200_000_000_i64),
                    Some(1_700_i64),
                    Some(86_399_999_000_i64),
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
                    backend,
                    plan_options,
                    ..WriteOptions::default()
                },
            )
            .await?;
            let stats = writer.write_batch(&batch).await?;

            ensure_eq(stats.rows_written, 4, "timestamp datetime rows_written")?;
            ensure_eq(stats.batches_written, 1, "timestamp datetime batches_written")?;
            ensure_eq(
                writer.finish().await?,
                stats,
                "timestamp datetime finish stats",
            )?;

            let rows = client
                .simple_query(format!(
                    "SELECT [row_id], CONVERT(varchar(40), [created_at], 126) FROM {} ORDER BY [row_id]",
                    table.quoted_sql()
                ))
                .await?
                .into_first_result()
                .await?;

            ensure_eq(rows.len(), 4, "timestamp datetime row count")?;
            ensure_eq(
                rows[0].get::<i32, _>(0),
                Some(1),
                "timestamp datetime row 0 id",
            )?;
            ensure_eq(
                rows[0].get::<&str, _>(1),
                Some("1960-01-01T00:00:00"),
                "timestamp datetime row 0 value",
            )?;
            ensure_eq(
                rows[1].get::<i32, _>(0),
                Some(2),
                "timestamp datetime row 1 id",
            )?;
            ensure_eq(
                rows[1].get::<&str, _>(1),
                Some("1970-01-01T00:00:00.003"),
                "timestamp datetime row 1 value",
            )?;
            ensure_eq(
                rows[2].get::<i32, _>(0),
                Some(3),
                "timestamp datetime row 2 id",
            )?;
            ensure_eq(
                rows[2].get::<&str, _>(1),
                Some("1970-01-02T00:00:00"),
                "timestamp datetime row 2 value",
            )?;
            ensure_eq(
                rows[3].get::<i32, _>(0),
                Some(4),
                "timestamp datetime row 3 id",
            )?;
            ensure_eq(
                rows[3].get::<&str, _>(1),
                None,
                "timestamp datetime row 3 value",
            )?;

            Ok::<(), Box<dyn std::error::Error>>(())
        }
        .await;

        let drop_result = drop_table(&mut client, &table).await;
        result?;
        drop_result?;
    }

    Ok(())
}

#[tokio::test]
async fn writer_round_trips_non_nullable_timestamp_ns_datetime_issue_171() -> TestResult<()> {
    let Some((connection_string, database)) = integration_config() else {
        eprintln!(
            "skipping SQL Server issue 171 timestamp datetime round-trip integration test: {CONNECTION_STRING_ENV} or {TEST_DATABASE_ENV} is not set"
        );
        return Ok(());
    };

    let plan_options = PlanOptions {
        timestamp_policy: TimestampPolicy::DateTime,
        ..PlanOptions::default()
    };
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new(
            "event_time",
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            false,
        ),
    ]));
    let (mappings, _diagnostics) = plan_arrow_schema_to_mssql_mappings(
        Arc::clone(&schema),
        MssqlProfile::sql_server_2016_compat_100(),
        plan_options,
    )?
    .into_parts();
    let cases = [
        (
            1_i32,
            1_780_529_793_687_000_000_i64,
            "2026-06-03T23:36:33.687000",
        ),
        (2, 1_778_615_767_493_000_000, "2026-05-12T19:56:07.493000"),
        (3, 1_774_840_482_427_000_000, "2026-03-30T03:14:42.427000"),
    ];
    let expected_values_sql = cases
        .iter()
        .map(|(row_id, _nanos, literal)| {
            format!("({row_id}, CAST(CAST(N'{literal}' AS datetime2(6)) AS datetime))")
        })
        .collect::<Vec<_>>()
        .join(", ");
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int32Array::from(
                cases
                    .iter()
                    .map(|(row_id, _nanos, _literal)| *row_id)
                    .collect::<Vec<_>>(),
            )) as ArrayRef,
            Arc::new(TimestampNanosecondArray::from(
                cases
                    .iter()
                    .map(|(_row_id, nanos, _literal)| *nanos)
                    .collect::<Vec<_>>(),
            )),
        ],
    )?;

    for backend in [WriteBackend::BaselineTokenRow, WriteBackend::DirectRawBulk] {
        let mut client = connect(&connection_string, &database).await?;
        let table = unique_table_name()?;

        execute_sql(
            &mut client,
            create_table_sql_from_mappings(&table, &mappings),
        )
        .await?;

        let result = async {
            let mut writer = BulkWriter::new(
                &mut client,
                table.clone(),
                mappings.clone(),
                WriteOptions {
                    backend,
                    plan_options,
                    ..WriteOptions::default()
                },
            )
            .await?;
            let stats = writer.write_batch(&batch).await?;

            ensure_eq(
                stats.rows_written,
                cases.len() as u64,
                "issue 171 timestamp datetime rows_written",
            )?;
            ensure_eq(
                writer.finish().await?,
                stats,
                "issue 171 timestamp datetime finish stats",
            )?;

            let actual_rows = client
                .simple_query(format!(
                    "SELECT [id], CONVERT(varchar(40), [event_time], 126) FROM {} ORDER BY [id]",
                    table.quoted_sql()
                ))
                .await?
                .into_first_result()
                .await?;
            let expected_rows = client
                .simple_query(format!(
                    "SELECT [id], CONVERT(varchar(40), [expected_at], 126) FROM (VALUES {expected_values_sql}) AS v([id], [expected_at]) ORDER BY [id]"
                ))
                .await?
                .into_first_result()
                .await?;

            ensure_eq(
                actual_rows.len(),
                expected_rows.len(),
                "issue 171 timestamp datetime row count",
            )?;
            for (index, (actual, expected)) in
                actual_rows.iter().zip(expected_rows.iter()).enumerate()
            {
                ensure_eq(
                    actual.get::<i32, _>(0),
                    expected.get::<i32, _>(0),
                    &format!("issue 171 timestamp datetime row {index} id"),
                )?;
                ensure_eq(
                    actual.get::<&str, _>(1),
                    expected.get::<&str, _>(1),
                    &format!("issue 171 timestamp datetime row {index} value"),
                )?;
            }

            Ok::<(), Box<dyn std::error::Error>>(())
        }
        .await;

        let drop_result = drop_table(&mut client, &table).await;
        result?;
        drop_result?;
    }

    Ok(())
}

#[tokio::test]
async fn writer_rejects_datetime_timestamp_out_of_range_without_partial_insert() -> TestResult<()> {
    let Some((connection_string, database)) = integration_config() else {
        eprintln!(
            "skipping SQL Server timestamp datetime out-of-range integration test: {CONNECTION_STRING_ENV} or {TEST_DATABASE_ENV} is not set"
        );
        return Ok(());
    };

    let mut client = connect(&connection_string, &database).await?;
    let plan_options = PlanOptions {
        timestamp_policy: TimestampPolicy::DateTime,
        ..PlanOptions::default()
    };
    let schema = Arc::new(Schema::new(vec![
        Field::new("row_id", DataType::Int32, false),
        Field::new(
            "created_at",
            DataType::Timestamp(TimeUnit::Microsecond, None),
            false,
        ),
    ]));
    let (mappings, _diagnostics) = plan_arrow_schema_to_mssql_mappings(
        Arc::clone(&schema),
        MssqlProfile::sql_server_2016_compat_100(),
        plan_options,
    )?
    .into_parts();
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int32Array::from(vec![1_i32])) as ArrayRef,
            Arc::new(TimestampMicrosecondArray::from(vec![
                -6_847_804_800_001_000_i64,
            ])),
        ],
    )?;

    for backend in [WriteBackend::BaselineTokenRow, WriteBackend::DirectRawBulk] {
        let table = unique_table_name()?;

        execute_sql(
            &mut client,
            create_table_sql_from_mappings(&table, &mappings),
        )
        .await?;

        let result = async {
            let mut writer = BulkWriter::new(
                &mut client,
                table.clone(),
                mappings.clone(),
                WriteOptions {
                    backend,
                    plan_options,
                    ..WriteOptions::default()
                },
            )
            .await?;
            let err = match writer.write_batch(&batch).await {
                Ok(_stats) => {
                    let _stats = writer.finish().await?;
                    return Err(test_error("datetime out-of-range timestamp was accepted"));
                }
                Err(err) => err,
            };

            let diagnostics = value_conversion_diagnostics(&err, WritePhase::ValueConversion)?;
            ensure(
                diagnostics.all().iter().any(|diagnostic| {
                    diagnostic.code() == DiagnosticCode::TimestampOutOfRange
                        && diagnostic.row() == Some(0)
                        && diagnostic
                            .field()
                            .is_some_and(|field| field.name() == "created_at")
                }),
                "datetime out-of-range diagnostic should include row and field",
            )?;
            ensure_eq(
                writer.finish().await?.rows_written,
                0,
                "finish rows_written",
            )?;
            ensure_eq(
                select_count(&mut client, &table).await?,
                0,
                "row count after datetime out-of-range rejection",
            )?;

            Ok::<(), Box<dyn std::error::Error>>(())
        }
        .await;

        let drop_result = drop_table(&mut client, &table).await;
        result?;
        drop_result?;
    }

    Ok(())
}

#[tokio::test]
async fn baseline_writer_round_trips_timezone_aware_timestamp_normalized_datetime2_values()
-> TestResult<()> {
    round_trip_timezone_aware_timestamp_normalized_datetime2_values(
        WriteBackend::BaselineTokenRow,
        "SQL Server timezone-aware normalized datetime2 integration test",
    )
    .await
}

#[tokio::test]
async fn direct_raw_writer_round_trips_timezone_aware_timestamp_normalized_datetime2_values()
-> TestResult<()> {
    round_trip_timezone_aware_timestamp_normalized_datetime2_values(
        WriteBackend::DirectRawBulk,
        "SQL Server direct raw timezone-aware normalized datetime2 integration test",
    )
    .await
}

async fn round_trip_timezone_aware_timestamp_normalized_datetime2_values(
    backend: WriteBackend,
    test_name: &str,
) -> TestResult<()> {
    let Some((connection_string, database)) = integration_config() else {
        eprintln!(
            "skipping {test_name}: {CONNECTION_STRING_ENV} or {TEST_DATABASE_ENV} is not set"
        );
        return Ok(());
    };

    let mut client = connect(&connection_string, &database).await?;
    let table = unique_table_name()?;
    let plan_options = PlanOptions {
        timezone_policy: TimezonePolicy::NormalizeUtcDateTime2,
        ..PlanOptions::default()
    };
    let schema = Arc::new(Schema::new(vec![
        Field::new("row_id", DataType::Int32, false),
        Field::new(
            "ts",
            DataType::Timestamp(TimeUnit::Second, Some("America/New_York".into())),
            true,
        ),
    ]));
    let (mappings, _diagnostics) = plan_arrow_schema_to_mssql_mappings(
        Arc::clone(&schema),
        MssqlProfile::sql_server_2016_compat_100(),
        plan_options,
    )?
    .into_parts();
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int32Array::from(vec![1_i32, 2])) as ArrayRef,
            Arc::new(
                TimestampSecondArray::from(vec![Some(0_i64), None])
                    .with_timezone("America/New_York"),
            ),
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
                backend,
                plan_options,
                ..WriteOptions::default()
            },
        )
        .await?;
        let stats = writer.write_batch(&batch).await?;

        ensure_eq(
            stats.rows_written,
            2,
            "timezone-aware datetime2 rows_written",
        )?;
        ensure_eq(
            stats.batches_written,
            1,
            "timezone-aware datetime2 batches_written",
        )?;
        ensure_eq(
            writer.finish().await?,
            stats,
            "timezone-aware datetime2 finish stats",
        )?;

        let rows = client
            .simple_query(format!(
                "SELECT [row_id], CONVERT(varchar(40), [ts], 126) FROM {} ORDER BY [row_id]",
                table.quoted_sql()
            ))
            .await?
            .into_first_result()
            .await?;

        ensure_eq(rows.len(), 2, "timezone-aware datetime2 row count")?;
        ensure_eq(
            rows[0].get::<i32, _>(0),
            Some(1),
            "timezone-aware datetime2 row 0 id",
        )?;
        ensure_eq(
            rows[0].get::<&str, _>(1),
            Some("1970-01-01T00:00:00"),
            "timezone-aware datetime2 row 0 value",
        )?;
        ensure_eq(
            rows[1].get::<i32, _>(0),
            Some(2),
            "timezone-aware datetime2 row 1 id",
        )?;
        ensure_eq(
            rows[1].get::<&str, _>(1),
            None,
            "timezone-aware datetime2 row 1 value",
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
async fn writer_round_trips_timezone_aware_timestamp_normalized_datetime2_3_values_across_supported_backends()
-> TestResult<()> {
    let Some((connection_string, database)) = integration_config() else {
        eprintln!(
            "skipping SQL Server timezone-aware normalized datetime2(3) integration test: {CONNECTION_STRING_ENV} or {TEST_DATABASE_ENV} is not set"
        );
        return Ok(());
    };

    for backend in [WriteBackend::BaselineTokenRow, WriteBackend::DirectRawBulk] {
        let mut client = connect(&connection_string, &database).await?;
        let table = unique_table_name()?;
        let plan_options = PlanOptions {
            timezone_policy: TimezonePolicy::NormalizeUtcDateTime2,
            timestamp_policy: TimestampPolicy::DateTime2 { precision: 3 },
            ..PlanOptions::default()
        };
        let schema = Arc::new(Schema::new(vec![
            Field::new("row_id", DataType::Int32, false),
            Field::new(
                "ts",
                DataType::Timestamp(TimeUnit::Microsecond, Some("America/New_York".into())),
                true,
            ),
        ]));
        let (mappings, _diagnostics) = plan_arrow_schema_to_mssql_mappings(
            Arc::clone(&schema),
            MssqlProfile::sql_server_2016_compat_100(),
            plan_options,
        )?
        .into_parts();
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int32Array::from(vec![1_i32, 2, 3])) as ArrayRef,
                Arc::new(
                    TimestampMicrosecondArray::from(vec![
                        Some(1_234_567_i64),
                        Some(86_399_999_500_i64),
                        None,
                    ])
                    .with_timezone("America/New_York"),
                ),
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
                    backend,
                    plan_options,
                    ..WriteOptions::default()
                },
            )
            .await?;
            let stats = writer.write_batch(&batch).await?;

            ensure_eq(
                stats.rows_written,
                3,
                "timezone-aware normalized datetime2(3) rows_written",
            )?;
            ensure_eq(
                stats.batches_written,
                1,
                "timezone-aware normalized datetime2(3) batches_written",
            )?;
            ensure_eq(
                writer.finish().await?,
                stats,
                "timezone-aware normalized datetime2(3) finish stats",
            )?;

            let rows = client
                .simple_query(format!(
                    "SELECT [row_id], CONVERT(varchar(40), [ts], 126) FROM {} ORDER BY [row_id]",
                    table.quoted_sql()
                ))
                .await?
                .into_first_result()
                .await?;

            ensure_eq(
                rows.len(),
                3,
                "timezone-aware normalized datetime2(3) row count",
            )?;
            ensure_eq(
                rows[0].get::<i32, _>(0),
                Some(1),
                "timezone-aware normalized datetime2(3) row 0 id",
            )?;
            ensure_eq(
                rows[0].get::<&str, _>(1),
                Some("1970-01-01T00:00:01.235"),
                "timezone-aware normalized datetime2(3) row 0 value",
            )?;
            ensure_eq(
                rows[1].get::<i32, _>(0),
                Some(2),
                "timezone-aware normalized datetime2(3) row 1 id",
            )?;
            ensure_eq(
                rows[1].get::<&str, _>(1),
                Some("1970-01-02T00:00:00"),
                "timezone-aware normalized datetime2(3) row 1 value",
            )?;
            ensure_eq(
                rows[2].get::<i32, _>(0),
                Some(3),
                "timezone-aware normalized datetime2(3) row 2 id",
            )?;
            ensure_eq(
                rows[2].get::<&str, _>(1),
                None,
                "timezone-aware normalized datetime2(3) row 2 value",
            )?;

            Ok::<(), Box<dyn std::error::Error>>(())
        }
        .await;

        let drop_result = drop_table(&mut client, &table).await;
        result?;
        drop_result?;
    }

    Ok(())
}

#[tokio::test]
async fn writer_round_trips_timezone_aware_timestamp_normalized_datetime_values_across_supported_backends()
-> TestResult<()> {
    let Some((connection_string, database)) = integration_config() else {
        eprintln!(
            "skipping SQL Server timezone-aware normalized datetime integration test: {CONNECTION_STRING_ENV} or {TEST_DATABASE_ENV} is not set"
        );
        return Ok(());
    };

    for backend in [WriteBackend::BaselineTokenRow, WriteBackend::DirectRawBulk] {
        let mut client = connect(&connection_string, &database).await?;
        let table = unique_table_name()?;
        let plan_options = PlanOptions {
            timezone_policy: TimezonePolicy::NormalizeUtcDateTime2,
            timestamp_policy: TimestampPolicy::DateTime,
            ..PlanOptions::default()
        };
        let schema = Arc::new(Schema::new(vec![
            Field::new("row_id", DataType::Int32, false),
            Field::new(
                "ts",
                DataType::Timestamp(TimeUnit::Microsecond, Some("America/New_York".into())),
                true,
            ),
        ]));
        let (mappings, _diagnostics) = plan_arrow_schema_to_mssql_mappings(
            Arc::clone(&schema),
            MssqlProfile::sql_server_2016_compat_100(),
            plan_options,
        )?
        .into_parts();
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int32Array::from(vec![1_i32, 2, 3])) as ArrayRef,
                Arc::new(
                    TimestampMicrosecondArray::from(vec![
                        Some(1_700_i64),
                        Some(86_399_999_000_i64),
                        None,
                    ])
                    .with_timezone("America/New_York"),
                ),
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
                    backend,
                    plan_options,
                    ..WriteOptions::default()
                },
            )
            .await?;
            let stats = writer.write_batch(&batch).await?;

            ensure_eq(
                stats.rows_written,
                3,
                "timezone-aware normalized datetime rows_written",
            )?;
            ensure_eq(
                stats.batches_written,
                1,
                "timezone-aware normalized datetime batches_written",
            )?;
            ensure_eq(
                writer.finish().await?,
                stats,
                "timezone-aware normalized datetime finish stats",
            )?;

            let rows = client
                .simple_query(format!(
                    "SELECT [row_id], CONVERT(varchar(40), [ts], 126) FROM {} ORDER BY [row_id]",
                    table.quoted_sql()
                ))
                .await?
                .into_first_result()
                .await?;

            ensure_eq(
                rows.len(),
                3,
                "timezone-aware normalized datetime row count",
            )?;
            ensure_eq(
                rows[0].get::<i32, _>(0),
                Some(1),
                "timezone-aware normalized datetime row 0 id",
            )?;
            ensure_eq(
                rows[0].get::<&str, _>(1),
                Some("1970-01-01T00:00:00.003"),
                "timezone-aware normalized datetime row 0 value",
            )?;
            ensure_eq(
                rows[1].get::<i32, _>(0),
                Some(2),
                "timezone-aware normalized datetime row 1 id",
            )?;
            ensure_eq(
                rows[1].get::<&str, _>(1),
                Some("1970-01-02T00:00:00"),
                "timezone-aware normalized datetime row 1 value",
            )?;
            ensure_eq(
                rows[2].get::<i32, _>(0),
                Some(3),
                "timezone-aware normalized datetime row 2 id",
            )?;
            ensure_eq(
                rows[2].get::<&str, _>(1),
                None,
                "timezone-aware normalized datetime row 2 value",
            )?;

            Ok::<(), Box<dyn std::error::Error>>(())
        }
        .await;

        let drop_result = drop_table(&mut client, &table).await;
        result?;
        drop_result?;
    }

    Ok(())
}

#[tokio::test]
async fn baseline_writer_round_trips_timezone_aware_timestamp_datetimeoffset_values()
-> TestResult<()> {
    round_trip_timezone_aware_timestamp_datetimeoffset_values(
        WriteBackend::BaselineTokenRow,
        "SQL Server timezone-aware datetimeoffset integration test",
    )
    .await
}

#[tokio::test]
async fn direct_raw_writer_round_trips_timezone_aware_timestamp_datetimeoffset_values()
-> TestResult<()> {
    round_trip_timezone_aware_timestamp_datetimeoffset_values(
        WriteBackend::DirectRawBulk,
        "SQL Server direct raw timezone-aware datetimeoffset integration test",
    )
    .await
}

async fn round_trip_timezone_aware_timestamp_datetimeoffset_values(
    backend: WriteBackend,
    test_name: &str,
) -> TestResult<()> {
    let Some((connection_string, database)) = integration_config() else {
        eprintln!(
            "skipping {test_name}: {CONNECTION_STRING_ENV} or {TEST_DATABASE_ENV} is not set"
        );
        return Ok(());
    };

    let mut client = connect(&connection_string, &database).await?;
    let table = unique_table_name()?;
    let plan_options = PlanOptions {
        timezone_policy: TimezonePolicy::DateTimeOffset,
        timestamp_policy: TimestampPolicy::DateTime,
        ..PlanOptions::default()
    };
    let schema = Arc::new(Schema::new(vec![
        Field::new("row_id", DataType::Int32, false),
        Field::new(
            "ts_named",
            DataType::Timestamp(TimeUnit::Second, Some("America/New_York".into())),
            true,
        ),
        Field::new(
            "ts_fixed",
            DataType::Timestamp(TimeUnit::Second, Some("+02:30".into())),
            true,
        ),
    ]));
    let (mappings, _diagnostics) = plan_arrow_schema_to_mssql_mappings(
        Arc::clone(&schema),
        MssqlProfile::sql_server_2016_compat_100(),
        plan_options,
    )?
    .into_parts();
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int32Array::from(vec![1_i32, 2])) as ArrayRef,
            Arc::new(
                TimestampSecondArray::from(vec![Some(1_738_411_200_i64), Some(1_750_593_600_i64)])
                    .with_timezone("America/New_York"),
            ),
            Arc::new(TimestampSecondArray::from(vec![Some(0_i64), None]).with_timezone("+02:30")),
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
                backend,
                plan_options,
                ..WriteOptions::default()
            },
        )
        .await?;
        let stats = writer.write_batch(&batch).await?;

        ensure_eq(
            stats.rows_written,
            2,
            "timezone-aware datetimeoffset rows_written",
        )?;
        ensure_eq(
            stats.batches_written,
            1,
            "timezone-aware datetimeoffset batches_written",
        )?;
        ensure_eq(
            writer.finish().await?,
            stats,
            "timezone-aware datetimeoffset finish stats",
        )?;

        let rows = client
            .simple_query(format!(
                "SELECT [row_id], CONVERT(varchar(50), [ts_named], 126), CONVERT(varchar(50), [ts_fixed], 126) FROM {} ORDER BY [row_id]",
                table.quoted_sql()
            ))
            .await?
            .into_first_result()
            .await?;

        ensure_eq(rows.len(), 2, "timezone-aware datetimeoffset row count")?;
        ensure_eq(
            rows[0].get::<i32, _>(0),
            Some(1),
            "timezone-aware datetimeoffset row 0 id",
        )?;
        ensure_eq(
            rows[0].get::<&str, _>(1),
            Some("2025-02-01T07:00:00-05:00"),
            "timezone-aware datetimeoffset row 0 named",
        )?;
        ensure_eq(
            rows[0].get::<&str, _>(2),
            Some("1970-01-01T02:30:00+02:30"),
            "timezone-aware datetimeoffset row 0 fixed",
        )?;

        ensure_eq(
            rows[1].get::<i32, _>(0),
            Some(2),
            "timezone-aware datetimeoffset row 1 id",
        )?;
        ensure_eq(
            rows[1].get::<&str, _>(1),
            Some("2025-06-22T08:00:00-04:00"),
            "timezone-aware datetimeoffset row 1 named",
        )?;
        ensure_eq(
            rows[1].get::<&str, _>(2),
            None,
            "timezone-aware datetimeoffset row 1 fixed",
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
async fn baseline_writer_round_trips_time_only_values() -> TestResult<()> {
    round_trip_time_only_values(
        WriteBackend::BaselineTokenRow,
        "SQL Server time-only round-trip integration test",
    )
    .await
}

#[tokio::test]
async fn direct_raw_writer_round_trips_time_only_values() -> TestResult<()> {
    round_trip_time_only_values(
        WriteBackend::DirectRawBulk,
        "SQL Server direct raw time-only round-trip integration test",
    )
    .await
}

async fn round_trip_time_only_values(backend: WriteBackend, test_name: &str) -> TestResult<()> {
    let Some((connection_string, database)) = integration_config() else {
        eprintln!(
            "skipping {test_name}: {CONNECTION_STRING_ENV} or {TEST_DATABASE_ENV} is not set"
        );
        return Ok(());
    };

    let mut client = connect(&connection_string, &database).await?;
    let table = unique_table_name()?;
    let schema = Arc::new(Schema::new(vec![
        Field::new("row_id", DataType::Int32, false),
        Field::new("time_s", DataType::Time32(TimeUnit::Second), false),
        Field::new("time_ms", DataType::Time32(TimeUnit::Millisecond), true),
        Field::new("time_us", DataType::Time64(TimeUnit::Microsecond), false),
        Field::new("time_ns", DataType::Time64(TimeUnit::Nanosecond), true),
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
            Arc::new(Int32Array::from(vec![1_i32, 2])) as ArrayRef,
            Arc::new(Time32SecondArray::from(vec![Some(11_111_i32), Some(0_i32)])),
            Arc::new(Time32MillisecondArray::from(vec![
                Some(11_111_111_i32),
                None,
            ])),
            Arc::new(Time64MicrosecondArray::from(vec![
                Some(11_111_111_111_i64),
                Some(86_399_999_999_i64),
            ])),
            Arc::new(Time64NanosecondArray::from(vec![
                Some(11_111_111_111_100_i64),
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
                backend,
                ..WriteOptions::default()
            },
        )
        .await?;
        let stats = writer.write_batch(&batch).await?;

        ensure_eq(stats.rows_written, 2, "time-only rows_written")?;
        ensure_eq(stats.batches_written, 1, "time-only batches_written")?;
        ensure_eq(writer.finish().await?, stats, "time-only finish stats")?;

        let rows = client
            .simple_query(format!(
                "SELECT [row_id], CONVERT(varchar(30), [time_s]), CONVERT(varchar(30), [time_ms]), CONVERT(varchar(30), [time_us]), CONVERT(varchar(30), [time_ns]) FROM {} ORDER BY [row_id]",
                table.quoted_sql()
            ))
            .await?
            .into_first_result()
            .await?;

        ensure_eq(rows.len(), 2, "time-only row count")?;
        ensure_eq(rows[0].get::<i32, _>(0), Some(1), "time-only row 0 id")?;
        ensure_eq(
            rows[0].get::<&str, _>(1),
            Some("03:05:11"),
            "time-only row 0 second",
        )?;
        ensure_eq(
            rows[0].get::<&str, _>(2),
            Some("03:05:11.111"),
            "time-only row 0 millisecond",
        )?;
        ensure_eq(
            rows[0].get::<&str, _>(3),
            Some("03:05:11.111111"),
            "time-only row 0 microsecond",
        )?;
        ensure_eq(
            rows[0].get::<&str, _>(4),
            Some("03:05:11.1111111"),
            "time-only row 0 nanosecond",
        )?;

        ensure_eq(rows[1].get::<i32, _>(0), Some(2), "time-only row 1 id")?;
        ensure_eq(
            rows[1].get::<&str, _>(1),
            Some("00:00:00"),
            "time-only row 1 second",
        )?;
        ensure_eq(
            rows[1].get::<&str, _>(2),
            None,
            "time-only row 1 millisecond",
        )?;
        ensure_eq(
            rows[1].get::<&str, _>(3),
            Some("23:59:59.999999"),
            "time-only row 1 microsecond",
        )?;
        ensure_eq(
            rows[1].get::<&str, _>(4),
            None,
            "time-only row 1 nanosecond",
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
async fn baseline_writer_rejects_decimal_precision_overflow_without_partial_insert()
-> TestResult<()> {
    let Some((connection_string, database)) = integration_config() else {
        eprintln!(
            "skipping SQL Server decimal overflow integration test: {CONNECTION_STRING_ENV} or {TEST_DATABASE_ENV} is not set"
        );
        return Ok(());
    };

    let mut client = connect(&connection_string, &database).await?;
    let table = unique_table_name()?;
    let schema = Arc::new(Schema::new(vec![
        Field::new("row_id", DataType::Int32, false),
        Field::new("amount", DataType::Decimal128(5, 2), false),
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
            Arc::new(Int32Array::from(vec![1_i32, 2])) as ArrayRef,
            malicious_decimal128_array(DataType::Decimal128(5, 2), &[12_345, 100_000])?,
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
                return Err(test_error("decimal precision overflow was accepted"));
            }
            Err(err) => err,
        };

        let diagnostics = value_conversion_diagnostics(&err, WritePhase::ValueConversion)?;
        ensure(
            diagnostics.all().iter().any(|diagnostic| {
                diagnostic.code() == DiagnosticCode::DecimalOutOfRange
                    && diagnostic.row() == Some(1)
                    && diagnostic
                        .field()
                        .is_some_and(|field| field.name() == "amount")
            }),
            "decimal overflow diagnostic should include row and field",
        )?;
        ensure_eq(
            writer.finish().await?.rows_written,
            0,
            "finish rows_written",
        )?;
        ensure_eq(
            select_count(&mut client, &table).await?,
            0,
            "row count after decimal overflow rejection",
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

        let diagnostics = value_conversion_diagnostics(&err, WritePhase::TargetMetadataValidation)?;

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
            vec![SchemaMapping::new(
                ArrowFieldRef::new(0, "renamed_id".to_owned(), false, DataType::Int32),
                MssqlColumn::new(Identifier::new("renamed_id")?, MssqlType::Int, false),
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

        let diagnostics = value_conversion_diagnostics(&err, WritePhase::BatchSchemaValidation)?;
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

#[tokio::test]
async fn direct_raw_writer_rejects_unsupported_schema_without_partial_insert() -> TestResult<()> {
    let Some((connection_string, database)) = integration_config() else {
        eprintln!(
            "skipping SQL Server direct raw unsupported schema integration test: {CONNECTION_STRING_ENV} or {TEST_DATABASE_ENV} is not set"
        );
        return Ok(());
    };

    let mut client = connect(&connection_string, &database).await?;
    let table = unique_table_name()?;
    let mappings = vec![SchemaMapping::new(
        ArrowFieldRef::new(
            0,
            "list_value".to_owned(),
            true,
            DataType::List(Arc::new(Field::new("item", DataType::Int32, true))),
        ),
        MssqlColumn::new(
            Identifier::new("list_value")?,
            MssqlType::NVarChar(MssqlTypeLength::Max),
            true,
        ),
    )];

    execute_sql(
        &mut client,
        create_table_sql_from_mappings(&table, &mappings),
    )
    .await?;

    let result = async {
        let err = match BulkWriter::new(
            &mut client,
            table.clone(),
            mappings,
            WriteOptions {
                backend: WriteBackend::DirectRawBulk,
                ..WriteOptions::default()
            },
        )
        .await
        {
            Ok(writer) => {
                let _stats = writer.finish().await?;
                return Err(test_error("unsupported direct raw schema was accepted"));
            }
            Err(err) => err,
        };

        let diagnostics = direct_encoding_diagnostics(&err, WritePhase::WriterInitialization)?;

        ensure(
            diagnostics.all().iter().any(|diagnostic| {
                diagnostic.code() == DiagnosticCode::DirectEncodingUnsupportedMapping
                    && diagnostic.field().map(|field| field.name()) == Some("list_value")
            }),
            "unsupported direct schema diagnostic should mention list_value",
        )?;

        ensure_eq(
            select_count(&mut client, &table).await?,
            0,
            "row count after rejected direct writer creation",
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

fn ensure_write_phase(error: &Error, expected: WritePhase) -> TestResult<()> {
    ensure_eq(error.write_phase(), Some(expected), "write phase")
}

fn value_conversion_diagnostics(
    error: &Error,
    expected_phase: WritePhase,
) -> TestResult<&DiagnosticSet> {
    ensure_write_phase(error, expected_phase)?;

    match error.without_write_phase() {
        Error::ValueConversion { diagnostics } => Ok(diagnostics),
        other => Err(test_error(format!(
            "expected value conversion error, got {other}"
        ))),
    }
}

fn direct_encoding_diagnostics(
    error: &Error,
    expected_phase: WritePhase,
) -> TestResult<&DiagnosticSet> {
    ensure_write_phase(error, expected_phase)?;

    match error.without_write_phase() {
        Error::DirectEncoding { diagnostics } => Ok(diagnostics),
        other => Err(test_error(format!(
            "expected direct encoding error, got {other}"
        ))),
    }
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

fn malicious_decimal128_array(data_type: DataType, values: &[i128]) -> TestResult<ArrayRef> {
    let data = ArrayData::builder(data_type)
        .len(values.len())
        .add_buffer(values.to_vec().into())
        .build()?;

    Ok(Arc::new(Decimal128Array::from(data)))
}

fn large_binary_array_crossing_i32_offset_boundary() -> TestResult<ArrayRef> {
    let boundary = i64::from(i32::MAX);
    let offsets = vec![0_i64, boundary - 8, boundary + 8, boundary + 16];
    let mut values = MutableBuffer::from_len_zeroed(usize::try_from(boundary + 16)?);
    values.as_slice_mut().fill(0xab);

    values.as_slice_mut()[usize::try_from(boundary - 8)?..usize::try_from(boundary - 4)?]
        .copy_from_slice(&[1, 2, 3, 4]);
    values.as_slice_mut()[usize::try_from(boundary + 4)?..usize::try_from(boundary + 8)?]
        .copy_from_slice(&[5, 6, 7, 8]);
    values.as_slice_mut()[usize::try_from(boundary + 8)?..usize::try_from(boundary + 12)?]
        .copy_from_slice(&[9, 10, 11, 12]);
    values.as_slice_mut()[usize::try_from(boundary + 12)?..usize::try_from(boundary + 16)?]
        .copy_from_slice(&[13, 14, 15, 16]);
    ensure_eq(
        &values.as_slice()[usize::try_from(boundary - 8)?..usize::try_from(boundary - 4)?],
        &[1, 2, 3, 4],
        "raw values row 1 prefix",
    )?;
    ensure_eq(
        &values.as_slice()[usize::try_from(boundary + 4)?..usize::try_from(boundary + 8)?],
        &[5, 6, 7, 8],
        "raw values row 1 suffix",
    )?;
    ensure_eq(
        &values.as_slice()[usize::try_from(boundary + 8)?..usize::try_from(boundary + 12)?],
        &[9, 10, 11, 12],
        "raw values row 2 prefix",
    )?;
    ensure_eq(
        &values.as_slice()[usize::try_from(boundary + 12)?..usize::try_from(boundary + 16)?],
        &[13, 14, 15, 16],
        "raw values row 2 suffix",
    )?;

    let offsets = OffsetBuffer::new(ScalarBuffer::from(offsets));
    let values = values.into();
    let array = LargeBinaryArray::try_new(
        offsets,
        values,
        Some(NullBuffer::from(vec![false, true, true])),
    )?;

    Ok(Arc::new(array))
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

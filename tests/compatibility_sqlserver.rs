//! Focused SQL Server compatibility probes.

#![cfg(feature = "integration-tests")]

use std::env;
use std::fmt::Debug;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use arrow_array::{ArrayRef, Int32Array, RecordBatch, TimestampMicrosecondArray};
use arrow_schema::{DataType, Field, Schema, TimeUnit};
use arrow_tiberius::{
    BulkWriter, CompatibilityLevel, MssqlProfile, MssqlVersion, PlanOptions, TableName,
    TimestampPolicy, WriteBackend, WriteOptions, create_table_sql_from_mappings,
};
use tokio::net::TcpStream;
use tokio_util::compat::{Compat, TokioAsyncWriteCompatExt};

const CONNECTION_STRING_ENV: &str = "ARROW_TIBERIUS_TEST_MSSQL_URL";
const TEST_DATABASE_ENV: &str = "ARROW_TIBERIUS_TEST_MSSQL_DATABASE";
const COMPATIBILITY_LEVEL_ENV: &str = "ARROW_TIBERIUS_TEST_MSSQL_COMPATIBILITY_LEVEL";
static TABLE_COUNTER: AtomicU64 = AtomicU64::new(0);

type TestClient = tiberius::Client<Compat<TcpStream>>;
type TestResult<T> = Result<T, Box<dyn std::error::Error>>;

#[tokio::test]
async fn datetime_rounding_matches_sql_server_casts() -> TestResult<()> {
    let Some((connection_string, database)) = integration_config() else {
        eprintln!(
            "skipping SQL Server datetime compatibility probe: {CONNECTION_STRING_ENV} or {TEST_DATABASE_ENV} is not set"
        );
        return Ok(());
    };

    let cases = [
        (
            1_i32,
            1_780_529_793_684_400_i64,
            "2026-06-03T23:36:33.684400",
        ),
        (2, 1_780_529_793_684_582, "2026-06-03T23:36:33.684582"),
        (3, 1_780_529_793_685_000, "2026-06-03T23:36:33.685000"),
        (4, 1_778_615_767_491_000, "2026-05-12T19:56:07.491000"),
        (5, 1_778_615_767_492_000, "2026-05-12T19:56:07.492000"),
        (6, 1_774_840_482_425_000, "2026-03-30T03:14:42.425000"),
        (7, 1_774_840_482_426_000, "2026-03-30T03:14:42.426000"),
        (8, 1_767_311_999_999_500, "2026-01-01T23:59:59.999500"),
    ];
    let expected_values_sql = cases
        .iter()
        .map(|(row_id, _micros, literal)| {
            format!("({row_id}, CAST(CAST(N'{literal}' AS datetime2(6)) AS datetime))")
        })
        .collect::<Vec<_>>()
        .join(", ");

    let profile = compatibility_profile()?;
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
    let (planned_schema, _diagnostics) = profile
        .plan_arrow_schema(Arc::clone(&schema), plan_options)?
        .into_parts();
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int32Array::from(
                cases
                    .iter()
                    .map(|(row_id, _micros, _literal)| *row_id)
                    .collect::<Vec<_>>(),
            )) as ArrayRef,
            Arc::new(TimestampMicrosecondArray::from(
                cases
                    .iter()
                    .map(|(_row_id, micros, _literal)| *micros)
                    .collect::<Vec<_>>(),
            )),
        ],
    )?;

    for backend in [WriteBackend::BaselineTokenRow, WriteBackend::DirectRawBulk] {
        let mut client = connect(&connection_string, &database).await?;
        ensure_database_compatibility_matches_profile(
            &mut client,
            profile,
            "datetime compatibility probe database compatibility level",
        )
        .await?;
        let table = unique_table_name()?;

        execute_sql(
            &mut client,
            create_table_sql_from_mappings(&table, &planned_schema),
        )
        .await?;

        let result = async {
            let mut writer = BulkWriter::new(
                &mut client,
                table.clone(),
                planned_schema.clone(),
                WriteOptions {
                    backend,
                    ..WriteOptions::default()
                },
            )
            .await?;
            let stats = writer.write_batch(&batch).await?;

            ensure_eq(
                stats.rows_written,
                cases.len() as u64,
                "datetime compatibility probe rows_written",
            )?;
            ensure_eq(
                writer.finish().await?,
                stats,
                "datetime compatibility probe finish stats",
            )?;

            let actual_rows = client
                .simple_query(format!(
                    "SELECT [row_id], CONVERT(varchar(40), [created_at], 126) FROM {} ORDER BY [row_id]",
                    table.quoted_sql()
                ))
                .await?
                .into_first_result()
                .await?;
            let expected_rows = client
                .simple_query(format!(
                    "SELECT [row_id], CONVERT(varchar(40), [expected_at], 126) FROM (VALUES {expected_values_sql}) AS v([row_id], [expected_at]) ORDER BY [row_id]"
                ))
                .await?
                .into_first_result()
                .await?;

            ensure_eq(
                actual_rows.len(),
                expected_rows.len(),
                "datetime compatibility probe row count",
            )?;
            for (index, (actual, expected)) in
                actual_rows.iter().zip(expected_rows.iter()).enumerate()
            {
                ensure_eq(
                    actual.get::<i32, _>(0),
                    expected.get::<i32, _>(0),
                    &format!("datetime compatibility probe row {index} id"),
                )?;
                ensure_eq(
                    actual.get::<&str, _>(1),
                    expected.get::<&str, _>(1),
                    &format!("datetime compatibility probe row {index} value"),
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

fn compatibility_profile() -> TestResult<MssqlProfile> {
    let level = env::var(COMPATIBILITY_LEVEL_ENV)
        .unwrap_or_else(|_| "100".to_owned())
        .parse::<u16>()
        .map_err(|err| test_error(format!("invalid {COMPATIBILITY_LEVEL_ENV}: {err}")))?;

    let compatibility_level = CompatibilityLevel::new(level)?;

    Ok(MssqlProfile::new(
        MssqlVersion::SqlServer2017,
        compatibility_level,
    )?)
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

async fn ensure_database_compatibility_matches_profile(
    client: &mut TestClient,
    profile: MssqlProfile,
    context: &str,
) -> TestResult<()> {
    let row = client
        .simple_query(
            "SELECT CAST(compatibility_level AS int) FROM sys.databases WHERE name = DB_NAME()",
        )
        .await?
        .into_row()
        .await?
        .ok_or_else(|| std::io::Error::other("compatibility level query returned no rows"))?;
    let actual = row
        .get::<i32, _>(0)
        .ok_or_else(|| std::io::Error::other("compatibility level query returned NULL"))?;
    let expected = i32::from(profile.compatibility_level().as_u16());

    ensure_eq(actual, expected, context)
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
    let table = format!("arrow_tiberius_compat_{}_{}", std::process::id(), counter);

    TableName::new("dbo", table)
}

fn integration_config() -> Option<(String, String)> {
    let connection_string = env::var(CONNECTION_STRING_ENV).ok()?;
    let database = env::var(TEST_DATABASE_ENV).ok()?;

    Some((connection_string, database))
}

fn ensure_eq<T>(actual: T, expected: T, context: &str) -> TestResult<()>
where
    T: Debug + PartialEq,
{
    if actual == expected {
        Ok(())
    } else {
        Err(test_error(format!(
            "{context}: expected {expected:?}, got {actual:?}"
        )))
    }
}

fn test_error(message: impl Into<String>) -> Box<dyn std::error::Error> {
    Box::new(std::io::Error::other(message.into()))
}

//! Write multiple Arrow record batches to SQL Server through `BulkWriter`.

use std::{env, error::Error, io, sync::Arc};

use arrow_array::{ArrayRef, BooleanArray, Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use arrow_tiberius::{
    BulkWriter, MssqlProfile, PlanOptions, TableName, WriteBackend, WriteOptions,
    create_table_sql_from_mappings,
};
use tokio::net::TcpStream;
use tokio_util::compat::{Compat, TokioAsyncWriteCompatExt};

const URL_ENV: &str = "ARROW_TIBERIUS_EXAMPLE_MSSQL_URL";
const SCHEMA_ENV: &str = "ARROW_TIBERIUS_EXAMPLE_MSSQL_SCHEMA";
const TABLE_ENV: &str = "ARROW_TIBERIUS_EXAMPLE_MSSQL_TABLE";
const KEEP_TABLE_ENV: &str = "ARROW_TIBERIUS_EXAMPLE_KEEP_TABLE";
const EXISTING_TABLE_ENV: &str = "ARROW_TIBERIUS_EXAMPLE_EXISTING_TABLE";

const DEFAULT_SCHEMA: &str = "dbo";
const DEFAULT_TABLE: &str = "arrow_tiberius_example_write";
const EXPECTED_ROWS: u64 = 5;
const EXPECTED_BATCHES: u64 = 2;

type ExampleClient = tiberius::Client<Compat<TcpStream>>;
type ExampleResult<T> = Result<T, Box<dyn Error>>;

#[tokio::main(flavor = "current_thread")]
async fn main() -> ExampleResult<()> {
    let Some(connection_string) = env::var(URL_ENV).ok().filter(|value| !value.is_empty()) else {
        println!("skipping SQL Server example: set {URL_ENV} to run it");
        return Ok(());
    };

    let config = ExampleConfig::from_env()?;
    let schema = example_schema();
    let batches = example_batches(schema.clone())?;

    let profile = MssqlProfile::sql_server_2016_compat_100();
    let planned_schema = profile
        .plan_arrow_schema(schema.as_ref(), PlanOptions::default())?
        .into_value();
    let create_table_sql = create_table_sql_from_mappings(&config.table, &planned_schema);

    let mut client = connect(&connection_string).await?;

    if config.create_disposable_table {
        execute_sql(
            &mut client,
            format!("DROP TABLE IF EXISTS {}", config.table.quoted_sql()),
        )
        .await?;
        execute_sql(&mut client, create_table_sql).await?;
        println!("created disposable table {}", config.table.quoted_sql());
    } else {
        println!("using existing table {}", config.table.quoted_sql());
    }

    let mut writer = BulkWriter::new(
        &mut client,
        config.table.clone(),
        planned_schema,
        WriteOptions {
            backend: WriteBackend::DirectRawBulk,
            ..WriteOptions::default()
        },
    )
    .await?;

    let first_stats = writer.write_batch(&batches[0]).await?;
    ensure_eq(first_stats.rows_written, 2, "rows after first batch")?;
    ensure_eq(first_stats.batches_written, 1, "batches after first batch")?;

    let second_stats = writer.write_batch(&batches[1]).await?;
    ensure_eq(
        second_stats.rows_written,
        EXPECTED_ROWS,
        "rows after second batch",
    )?;
    ensure_eq(
        second_stats.batches_written,
        EXPECTED_BATCHES,
        "batches after second batch",
    )?;

    let final_stats = writer.finish().await?;
    ensure_eq(final_stats.rows_written, EXPECTED_ROWS, "final row count")?;
    ensure_eq(
        final_stats.batches_written,
        EXPECTED_BATCHES,
        "final batch count",
    )?;

    println!(
        "wrote {} rows across {} batches with {:?}",
        final_stats.rows_written,
        final_stats.batches_written,
        WriteBackend::DirectRawBulk
    );

    if config.create_disposable_table && !config.keep_table {
        execute_sql(
            &mut client,
            format!("DROP TABLE IF EXISTS {}", config.table.quoted_sql()),
        )
        .await?;
        println!("dropped disposable table {}", config.table.quoted_sql());
    } else if config.keep_table {
        println!("kept table {}", config.table.quoted_sql());
    }

    Ok(())
}

#[derive(Debug)]
struct ExampleConfig {
    table: TableName,
    keep_table: bool,
    create_disposable_table: bool,
}

impl ExampleConfig {
    fn from_env() -> ExampleResult<Self> {
        let schema = env::var(SCHEMA_ENV).unwrap_or_else(|_| DEFAULT_SCHEMA.to_owned());
        let table = env::var(TABLE_ENV).unwrap_or_else(|_| DEFAULT_TABLE.to_owned());
        let keep_table = env_flag(KEEP_TABLE_ENV);
        let use_existing_table = env_flag(EXISTING_TABLE_ENV);

        Ok(Self {
            table: TableName::new(schema, table)?,
            keep_table,
            create_disposable_table: !use_existing_table,
        })
    }
}

async fn connect(connection_string: &str) -> tiberius::Result<ExampleClient> {
    let config = tiberius::Config::from_ado_string(connection_string)?;
    let tcp = TcpStream::connect(config.get_addr()).await?;

    tiberius::Client::connect(config, tcp.compat_write()).await
}

async fn execute_sql(client: &mut ExampleClient, sql: String) -> tiberius::Result<()> {
    client.simple_query(sql).await?.into_results().await?;

    Ok(())
}

fn example_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, true),
        Field::new("is_active", DataType::Boolean, false),
    ]))
}

fn example_batches(schema: Arc<Schema>) -> Result<Vec<RecordBatch>, arrow_schema::ArrowError> {
    let batch_one = RecordBatch::try_new(
        schema.clone(),
        vec![
            array(Int64Array::from(vec![1_i64, 2])),
            array(StringArray::from(vec![Some("alpha"), Some("beta")])),
            array(BooleanArray::from(vec![true, false])),
        ],
    )?;
    let batch_two = RecordBatch::try_new(
        schema,
        vec![
            array(Int64Array::from(vec![3_i64, 4, 5])),
            array(StringArray::from(vec![
                Some("gamma"),
                None,
                Some("epsilon"),
            ])),
            array(BooleanArray::from(vec![true, true, false])),
        ],
    )?;

    Ok(vec![batch_one, batch_two])
}

fn array<Array>(array: Array) -> ArrayRef
where
    Array: arrow_array::Array + 'static,
{
    Arc::new(array)
}

fn env_flag(name: &str) -> bool {
    env::var(name).is_ok_and(|value| {
        matches!(
            value.trim().to_ascii_lowercase().as_str(),
            "1" | "true" | "yes" | "on"
        )
    })
}

fn ensure_eq<T>(actual: T, expected: T, context: &str) -> ExampleResult<()>
where
    T: std::fmt::Debug + PartialEq,
{
    if actual == expected {
        return Ok(());
    }

    Err(Box::new(io::Error::other(format!(
        "{context}: expected {expected:?}, got {actual:?}"
    ))))
}

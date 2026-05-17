use std::env;
use std::error::Error;
use std::fs::File;
use std::path::Path;
use std::time::Instant;

use arrow_ipc::reader::FileReader;
use arrow_odbc::arrow::array::{Float64Array, Int32Array, Int64Array, StringArray};
use arrow_odbc::arrow::datatypes::{DataType, Field, Schema};
use arrow_odbc::arrow::record_batch::{RecordBatch, RecordBatchIterator};
use arrow_odbc::insert_into_table;
use arrow_odbc::odbc_api::buffers::TextRowSet;
use arrow_odbc::odbc_api::{Connection, Cursor, Environment};

const CONNECTION_STRING_ENV: &str = "ARROW_TIBERIUS_BENCH_ODBC_CONNECTION_STRING";
const DATABASE_ENV: &str = "ARROW_TIBERIUS_BENCH_DATABASE";

fn main() -> Result<(), Box<dyn Error>> {
    let command = env::args().nth(1);

    match command.as_deref() {
        Some("validate") => validate(),
        Some("bench") => bench(env::args().skip(2).collect()),
        Some(command) => Err(format!("unknown arrow-odbc runner command `{command}`").into()),
        None => Err("missing arrow-odbc runner command".into()),
    }
}

fn validate() -> Result<(), Box<dyn Error>> {
    let connection_string = required_env(CONNECTION_STRING_ENV)?;
    let database = required_env(DATABASE_ENV)?;
    let environment = Environment::new()?;
    let _connection =
        environment.connect_with_connection_string(&connection_string, Default::default())?;

    println!("arrow-odbc runner validated database {database}");
    Ok(())
}

fn bench(args: Vec<String>) -> Result<(), Box<dyn Error>> {
    let options = BenchOptions::parse(args)?;
    if !is_supported_scenario(&options.scenario) {
        return Err(format!(
            "arrow-odbc runner only supports narrow_numeric and mixed_nullable, got {}",
            options.scenario
        )
        .into());
    }

    let connection_string = required_env(CONNECTION_STRING_ENV)?;
    let database = required_env(DATABASE_ENV)?;
    let environment = Environment::new()?;
    let connection =
        environment.connect_with_connection_string(&connection_string, Default::default())?;

    let mut total_rows = 0_u64;
    let write_start = Instant::now();

    for repeat in 0..options.repeat {
        let table = format!(
            "dbo.arrow_tiberius_odbc_bench_{}_{}",
            std::process::id(),
            repeat
        );
        let repeat_result = run_repeat(&connection, &table, &options);
        let cleanup_result = execute_sql(&connection, &format!("DROP TABLE IF EXISTS {table}"));

        if let Err(error) = cleanup_result {
            if repeat_result.is_err() {
                eprintln!(
                    "warning: failed to clean up arrow-odbc benchmark table {table}: {error}"
                );
            } else {
                return Err(error);
            }
        }

        total_rows = total_rows.saturating_add(repeat_result?);
    }

    let write_elapsed = write_start.elapsed();

    println!("arrow-odbc runner");
    println!("  database: {database}");
    println!("  scenario: {}", options.scenario);
    println!("  repeat: {}", options.repeat);
    println!("  rows written: {total_rows}");
    println!("  write seconds: {:.3}", write_elapsed.as_secs_f64());
    println!(
        "  write rows/sec: {:.2}",
        rows_per_second(total_rows, write_elapsed)
    );

    Ok(())
}

fn run_repeat(
    connection: &Connection<'_>,
    table: &str,
    options: &BenchOptions,
) -> Result<u64, Box<dyn Error>> {
    execute_sql(connection, &format!("DROP TABLE IF EXISTS {table}"))?;
    execute_sql(connection, &create_table_sql(table, &options.scenario)?)?;

    let input = batches_for_options(options)?;
    let mut reader = RecordBatchIterator::new(input.batches.into_iter().map(Ok), input.schema);
    insert_into_table(connection, &mut reader, table, options.batch_size)?;

    let actual = select_count(connection, table)?;
    let expected = u64::try_from(input.rows)?;
    if actual != expected {
        return Err(format!(
            "arrow-odbc row-count validation failed: expected {expected}, got {actual}"
        )
        .into());
    }

    Ok(actual)
}

#[derive(Debug)]
struct InputBatches {
    schema: std::sync::Arc<Schema>,
    batches: Vec<RecordBatch>,
    rows: usize,
}

fn batches_for_options(options: &BenchOptions) -> Result<InputBatches, Box<dyn Error>> {
    if let Some(path) = &options.input_ipc {
        return ipc_batches_for_scenario(path, &options.scenario, options.rows);
    }

    let schema = schema_for_scenario(&options.scenario)?;
    let batches = batches_for_scenario(
        &options.scenario,
        schema.clone(),
        options.rows,
        options.batch_size,
    )?;

    Ok(InputBatches {
        schema,
        batches,
        rows: options.rows,
    })
}

fn ipc_batches_for_scenario(
    path: &Path,
    scenario: &str,
    expected_rows: usize,
) -> Result<InputBatches, Box<dyn Error>> {
    let expected_schema = schema_for_scenario(scenario)?;
    let file = File::open(path)?;
    let reader = FileReader::try_new(file, None)?;
    let schema = reader.schema();

    if schema.as_ref() != expected_schema.as_ref() {
        return Err(format!(
            "Arrow IPC schema does not match scenario `{scenario}`: expected {:?}, got {:?}",
            expected_schema.fields(),
            schema.fields()
        )
        .into());
    }

    let batches = reader.collect::<Result<Vec<_>, _>>()?;
    let rows = batches
        .iter()
        .map(arrow_odbc::arrow::record_batch::RecordBatch::num_rows)
        .sum::<usize>();

    if rows != expected_rows {
        return Err(format!(
            "Arrow IPC row count does not match --rows: expected {expected_rows}, got {rows}"
        )
        .into());
    }

    Ok(InputBatches {
        schema,
        batches,
        rows,
    })
}

fn is_supported_scenario(scenario: &str) -> bool {
    matches!(scenario, "narrow_numeric" | "mixed_nullable")
}

fn create_table_sql(table: &str, scenario: &str) -> Result<String, Box<dyn Error>> {
    match scenario {
        "narrow_numeric" => Ok(format!(
            "CREATE TABLE {table} (id32 int NOT NULL, id64 bigint NOT NULL, score float(53) NOT NULL)"
        )),
        "mixed_nullable" => Ok(format!(
            "CREATE TABLE {table} (id32 int NOT NULL, maybe_id64 bigint NULL, maybe_score float(53) NULL, category nvarchar(max) NULL)"
        )),
        other => Err(format!("unsupported scenario `{other}`").into()),
    }
}

fn execute_sql(connection: &Connection<'_>, sql: &str) -> Result<(), Box<dyn Error>> {
    connection.execute(sql, (), None)?;
    Ok(())
}

fn select_count(connection: &Connection<'_>, table: &str) -> Result<u64, Box<dyn Error>> {
    let sql = format!("SELECT COUNT_BIG(*) FROM {table}");
    let cursor = connection
        .execute(&sql, (), None)?
        .ok_or("SELECT COUNT_BIG(*) did not return a cursor")?;
    let text = cursor_to_string(cursor)?;
    let count = text.trim().parse::<u64>()?;

    Ok(count)
}

fn cursor_to_string(mut cursor: impl Cursor) -> Result<String, Box<dyn Error>> {
    let mut buffer = TextRowSet::for_cursor(1, &mut cursor, Some(64))?;
    let mut row_set_cursor = cursor.bind_buffer(&mut buffer)?;
    let row_set = row_set_cursor.fetch()?.ok_or("cursor returned no rows")?;
    let value = row_set
        .at_as_str(0, 0)?
        .ok_or("cursor count cell was NULL")?
        .to_owned();

    Ok(value)
}

fn schema_for_scenario(scenario: &str) -> Result<std::sync::Arc<Schema>, Box<dyn Error>> {
    match scenario {
        "narrow_numeric" => Ok(narrow_numeric_schema()),
        "mixed_nullable" => Ok(mixed_nullable_schema()),
        other => Err(format!("unsupported scenario `{other}`").into()),
    }
}

fn narrow_numeric_schema() -> std::sync::Arc<Schema> {
    std::sync::Arc::new(Schema::new(vec![
        Field::new("id32", DataType::Int32, false),
        Field::new("id64", DataType::Int64, false),
        Field::new("score", DataType::Float64, false),
    ]))
}

fn mixed_nullable_schema() -> std::sync::Arc<Schema> {
    std::sync::Arc::new(Schema::new(vec![
        Field::new("id32", DataType::Int32, false),
        Field::new("maybe_id64", DataType::Int64, true),
        Field::new("maybe_score", DataType::Float64, true),
        Field::new("category", DataType::Utf8, true),
    ]))
}

fn batches_for_scenario(
    scenario: &str,
    schema: std::sync::Arc<Schema>,
    rows: usize,
    batch_size: usize,
) -> Result<Vec<RecordBatch>, Box<dyn Error>> {
    match scenario {
        "narrow_numeric" => narrow_numeric_batches(schema, rows, batch_size),
        "mixed_nullable" => mixed_nullable_batches(schema, rows, batch_size),
        other => Err(format!("unsupported scenario `{other}`").into()),
    }
}

fn narrow_numeric_batches(
    schema: std::sync::Arc<Schema>,
    rows: usize,
    batch_size: usize,
) -> Result<Vec<RecordBatch>, Box<dyn Error>> {
    let mut batches = Vec::new();
    let mut offset = 0;

    while offset < rows {
        let len = batch_size.min(rows - offset);
        let id32 = (offset..offset + len)
            .map(deterministic_i32)
            .collect::<Int32Array>();
        let id64 = (offset..offset + len)
            .map(|row| i64::from(deterministic_i32(row)) * 1_000)
            .collect::<Int64Array>();
        let score = (offset..offset + len)
            .map(deterministic_score)
            .collect::<Float64Array>();
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                std::sync::Arc::new(id32),
                std::sync::Arc::new(id64),
                std::sync::Arc::new(score),
            ],
        )?;

        batches.push(batch);
        offset += len;
    }

    Ok(batches)
}

fn mixed_nullable_batches(
    schema: std::sync::Arc<Schema>,
    rows: usize,
    batch_size: usize,
) -> Result<Vec<RecordBatch>, Box<dyn Error>> {
    let mut batches = Vec::new();
    let mut offset = 0;

    while offset < rows {
        let len = batch_size.min(rows - offset);
        let id32 = (offset..offset + len)
            .map(deterministic_i32)
            .collect::<Int32Array>();
        let maybe_id64 = (offset..offset + len)
            .map(|row| {
                if row % 7 == 0 {
                    None
                } else {
                    Some(i64::from(deterministic_i32(row)) * 10)
                }
            })
            .collect::<Int64Array>();
        let maybe_score = (offset..offset + len)
            .map(|row| {
                if row % 11 == 0 {
                    None
                } else {
                    Some(deterministic_score(row))
                }
            })
            .collect::<Float64Array>();
        let category_values = ["alpha", "beta", "gamma", "delta", "epsilon"];
        let category = (offset..offset + len)
            .map(|row| {
                if row % 5 == 0 {
                    None
                } else {
                    Some(category_values[row % category_values.len()])
                }
            })
            .collect::<StringArray>();
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                std::sync::Arc::new(id32),
                std::sync::Arc::new(maybe_id64),
                std::sync::Arc::new(maybe_score),
                std::sync::Arc::new(category),
            ],
        )?;

        batches.push(batch);
        offset += len;
    }

    Ok(batches)
}

fn deterministic_i32(row: usize) -> i32 {
    (row as i32)
        .wrapping_mul(1_103_515_245)
        .wrapping_add(12_345)
}

fn deterministic_score(row: usize) -> f64 {
    let value = row.wrapping_mul(48_271) % 1_000_003;
    value as f64 / 97.0
}

fn rows_per_second(rows: u64, elapsed: std::time::Duration) -> f64 {
    if elapsed.is_zero() {
        return 0.0;
    }

    rows as f64 / elapsed.as_secs_f64()
}

fn required_env(name: &str) -> Result<String, Box<dyn Error>> {
    let value =
        env::var(name).map_err(|_| format!("missing required environment variable {name}"))?;

    if value.is_empty() {
        return Err(format!("required environment variable {name} is empty").into());
    }

    Ok(value)
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct BenchOptions {
    rows: usize,
    batch_size: usize,
    scenario: String,
    repeat: usize,
    input_ipc: Option<std::path::PathBuf>,
}

impl BenchOptions {
    fn parse(args: Vec<String>) -> Result<Self, Box<dyn Error>> {
        let mut options = Self {
            rows: 100_000,
            batch_size: 8_192,
            scenario: "narrow_numeric".to_owned(),
            repeat: 1,
            input_ipc: None,
        };
        let mut index = 0;

        while index < args.len() {
            match args[index].as_str() {
                "--rows" => {
                    index += 1;
                    options.rows = parse_positive_usize("--rows", args.get(index))?;
                }
                "--batch-size" => {
                    index += 1;
                    options.batch_size = parse_positive_usize("--batch-size", args.get(index))?;
                }
                "--scenario" => {
                    index += 1;
                    options.scenario = required_arg("--scenario", args.get(index))?.to_owned();
                }
                "--repeat" => {
                    index += 1;
                    options.repeat = parse_positive_usize("--repeat", args.get(index))?;
                }
                "--input-ipc" => {
                    index += 1;
                    options.input_ipc = Some(std::path::PathBuf::from(required_arg(
                        "--input-ipc",
                        args.get(index),
                    )?));
                }
                other => return Err(format!("unknown arrow-odbc runner option `{other}`").into()),
            }

            index += 1;
        }

        Ok(options)
    }
}

fn parse_positive_usize(option: &str, value: Option<&String>) -> Result<usize, Box<dyn Error>> {
    let parsed = required_arg(option, value)?.parse::<usize>()?;

    if parsed == 0 {
        return Err(format!("{option} must be greater than zero").into());
    }

    Ok(parsed)
}

fn required_arg<'a>(option: &str, value: Option<&'a String>) -> Result<&'a str, Box<dyn Error>> {
    value
        .map(String::as_str)
        .ok_or_else(|| format!("missing value for {option}").into())
}

#[cfg(test)]
mod tests {
    use super::{
        BenchOptions, batches_for_options, create_table_sql, ipc_batches_for_scenario,
        is_supported_scenario, mixed_nullable_batches, mixed_nullable_schema,
        narrow_numeric_batches, narrow_numeric_schema, rows_per_second,
    };
    use arrow_ipc::writer::FileWriter;
    use arrow_odbc::arrow::array::Array;
    use arrow_odbc::arrow::datatypes::Schema;
    use arrow_odbc::arrow::record_batch::RecordBatch;
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicU64, Ordering};

    static TEST_FILE_COUNTER: AtomicU64 = AtomicU64::new(0);

    #[test]
    fn parses_bench_options() {
        let options = BenchOptions::parse(vec![
            "--rows".to_owned(),
            "25".to_owned(),
            "--batch-size".to_owned(),
            "8".to_owned(),
            "--scenario".to_owned(),
            "narrow_numeric".to_owned(),
            "--repeat".to_owned(),
            "3".to_owned(),
        ])
        .expect("valid options should parse");

        assert_eq!(options.rows, 25);
        assert_eq!(options.batch_size, 8);
        assert_eq!(options.scenario, "narrow_numeric");
        assert_eq!(options.repeat, 3);
        assert!(options.input_ipc.is_none());
    }

    #[test]
    fn parses_input_ipc_option() {
        let options = BenchOptions::parse(vec![
            "--rows".to_owned(),
            "25".to_owned(),
            "--input-ipc".to_owned(),
            "/workspace/bench.arrow".to_owned(),
        ])
        .expect("valid options should parse");

        assert_eq!(options.rows, 25);
        assert_eq!(
            options.input_ipc.as_deref(),
            Some(std::path::Path::new("/workspace/bench.arrow"))
        );
    }

    #[test]
    fn rejects_zero_rows() {
        let err = BenchOptions::parse(vec!["--rows".to_owned(), "0".to_owned()])
            .expect_err("zero rows should be rejected");

        assert!(err.to_string().contains("--rows"));
    }

    #[test]
    fn rejects_missing_option_value() {
        let err = BenchOptions::parse(vec!["--batch-size".to_owned()])
            .expect_err("missing value should be rejected");

        assert!(err.to_string().contains("--batch-size"));
    }

    #[test]
    fn rejects_unknown_option() {
        let err = BenchOptions::parse(vec!["--unexpected".to_owned()])
            .expect_err("unknown option should be rejected");

        assert!(err.to_string().contains("--unexpected"));
    }

    #[test]
    fn recognizes_supported_scenarios() {
        assert!(is_supported_scenario("narrow_numeric"));
        assert!(is_supported_scenario("mixed_nullable"));
        assert!(!is_supported_scenario("decimal_temporal"));
    }

    #[test]
    fn renders_mixed_nullable_table_sql() {
        let sql = create_table_sql("dbo.target", "mixed_nullable")
            .expect("mixed nullable DDL should render");

        assert_eq!(
            sql,
            "CREATE TABLE dbo.target (id32 int NOT NULL, maybe_id64 bigint NULL, maybe_score float(53) NULL, category nvarchar(max) NULL)"
        );
    }

    #[test]
    fn narrow_numeric_batches_cover_tail_rows() {
        let schema = narrow_numeric_schema();
        let batches = narrow_numeric_batches(schema, 25, 8).expect("batches should build");
        let lengths = batches
            .iter()
            .map(arrow_odbc::arrow::record_batch::RecordBatch::num_rows)
            .collect::<Vec<_>>();

        assert_eq!(lengths, vec![8, 8, 8, 1]);
    }

    #[test]
    fn mixed_nullable_batches_cover_tail_rows_and_nulls() {
        let schema = mixed_nullable_schema();
        let batches = mixed_nullable_batches(schema, 25, 8).expect("batches should build");
        let lengths = batches
            .iter()
            .map(arrow_odbc::arrow::record_batch::RecordBatch::num_rows)
            .collect::<Vec<_>>();
        let first = &batches[0];

        assert_eq!(lengths, vec![8, 8, 8, 1]);
        assert!(first.column(1).is_null(0));
        assert!(first.column(1).is_valid(1));
        assert!(first.column(2).is_null(0));
        assert!(first.column(2).is_valid(1));
        assert!(first.column(3).is_null(0));
        assert!(first.column(3).is_valid(1));
    }

    #[test]
    fn reads_input_ipc_batches_for_matching_scenario() {
        let schema = mixed_nullable_schema();
        let batches = mixed_nullable_batches(schema.clone(), 25, 8).expect("batches should build");
        let path = temp_ipc_path("mixed");
        write_ipc_file(&path, schema.as_ref(), &batches);

        let input = ipc_batches_for_scenario(&path, "mixed_nullable", 25)
            .expect("matching IPC file should load");

        assert_eq!(input.schema.as_ref(), schema.as_ref());
        assert_eq!(input.batches, batches);
        assert_eq!(input.rows, 25);

        std::fs::remove_file(path).expect("test IPC file should be removed");
    }

    #[test]
    fn rejects_input_ipc_schema_mismatch() {
        let schema = narrow_numeric_schema();
        let batches = narrow_numeric_batches(schema.clone(), 4, 4).expect("batches should build");
        let path = temp_ipc_path("schema-mismatch");
        write_ipc_file(&path, schema.as_ref(), &batches);

        let err = ipc_batches_for_scenario(&path, "mixed_nullable", 4)
            .expect_err("schema mismatch should be rejected");

        assert!(err.to_string().contains("schema"));

        std::fs::remove_file(path).expect("test IPC file should be removed");
    }

    #[test]
    fn rejects_input_ipc_row_count_mismatch() {
        let schema = mixed_nullable_schema();
        let batches = mixed_nullable_batches(schema.clone(), 25, 8).expect("batches should build");
        let path = temp_ipc_path("row-mismatch");
        write_ipc_file(&path, schema.as_ref(), &batches);

        let err = ipc_batches_for_scenario(&path, "mixed_nullable", 24)
            .expect_err("row count mismatch should be rejected");

        assert!(err.to_string().contains("row count"));

        std::fs::remove_file(path).expect("test IPC file should be removed");
    }

    #[test]
    fn input_ipc_options_use_file_batches_instead_of_generating() {
        let schema = mixed_nullable_schema();
        let batches = mixed_nullable_batches(schema.clone(), 25, 8).expect("batches should build");
        let path = temp_ipc_path("options");
        write_ipc_file(&path, schema.as_ref(), &batches);
        let options = BenchOptions {
            rows: 25,
            batch_size: 2,
            scenario: "mixed_nullable".to_owned(),
            repeat: 1,
            input_ipc: Some(path.clone()),
        };

        let input = batches_for_options(&options).expect("input IPC batches should load");
        let lengths = input
            .batches
            .iter()
            .map(arrow_odbc::arrow::record_batch::RecordBatch::num_rows)
            .collect::<Vec<_>>();

        assert_eq!(input.rows, 25);
        assert_eq!(lengths, vec![8, 8, 8, 1]);

        std::fs::remove_file(path).expect("test IPC file should be removed");
    }

    #[test]
    fn rows_per_second_handles_zero_elapsed() {
        assert_eq!(rows_per_second(25, std::time::Duration::ZERO), 0.0);
    }

    fn write_ipc_file(path: &std::path::Path, schema: &Schema, batches: &[RecordBatch]) {
        let file = std::fs::File::create(path).expect("test IPC file should be created");
        let mut writer = FileWriter::try_new(file, schema).expect("IPC writer should be created");

        for batch in batches {
            writer.write(batch).expect("batch should be written");
        }

        writer.finish().expect("IPC file should finish");
    }

    fn temp_ipc_path(name: &str) -> PathBuf {
        let counter = TEST_FILE_COUNTER.fetch_add(1, Ordering::Relaxed);
        std::env::temp_dir().join(format!(
            "arrow-tiberius-odbc-runner-{name}-{}-{counter}.arrow",
            std::process::id()
        ))
    }
}

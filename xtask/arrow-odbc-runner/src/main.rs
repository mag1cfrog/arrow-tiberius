use std::env;
use std::error::Error;
use std::fs::File;
use std::path::Path;
use std::time::Instant;

use arrow_ipc::reader::FileReader;
use arrow_odbc::arrow::record_batch::{RecordBatch, RecordBatchIterator};
use arrow_odbc::insert_into_table;
use arrow_odbc::odbc_api::buffers::TextRowSet;
use arrow_odbc::odbc_api::{Connection, Cursor, Environment};

const CONNECTION_STRING_ENV: &str = "ARROW_TIBERIUS_BENCH_ODBC_CONNECTION_STRING";
const DATABASE_ENV: &str = "ARROW_TIBERIUS_BENCH_DATABASE";
const TABLE_PLACEHOLDER: &str = "__ARROW_TIBERIUS_ODBC_TABLE__";

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

    let connection_string = required_env(CONNECTION_STRING_ENV)?;
    let database = required_env(DATABASE_ENV)?;
    let environment = Environment::new()?;
    let connection =
        environment.connect_with_connection_string(&connection_string, Default::default())?;
    connection.set_autocommit(false)?;

    let write_start = Instant::now();
    let total_rows_result = run_repeats(&connection, &options);
    if total_rows_result.is_ok() {
        connection.commit()?;
    } else if let Err(error) = connection.rollback() {
        eprintln!("warning: failed to roll back arrow-odbc benchmark transaction: {error}");
    }
    let write_elapsed = write_start.elapsed();
    let total_rows = total_rows_result?;

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

fn run_repeats(
    connection: &Connection<'_>,
    options: &BenchOptions,
) -> Result<u64, Box<dyn Error>> {
    let mut total_rows = 0_u64;

    for repeat in 0..options.repeat {
        let table = format!(
            "[dbo].[arrow_tiberius_odbc_bench_{}_{}]",
            std::process::id(),
            repeat
        );
        let repeat_result = run_repeat(connection, &table, options);
        let cleanup_result = execute_sql(connection, &format!("DROP TABLE IF EXISTS {table}"));

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

    Ok(total_rows)
}

fn run_repeat(
    connection: &Connection<'_>,
    table: &str,
    options: &BenchOptions,
) -> Result<u64, Box<dyn Error>> {
    execute_sql(connection, &format!("DROP TABLE IF EXISTS {table}"))?;
    execute_sql(connection, &options.create_table_sql(&table)?)?;

    let input = ipc_batches(&options.input_ipc, options.rows)?;
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
    schema: std::sync::Arc<arrow_odbc::arrow::datatypes::Schema>,
    batches: Vec<RecordBatch>,
    rows: usize,
}

fn ipc_batches(path: &Path, expected_rows: usize) -> Result<InputBatches, Box<dyn Error>> {
    let file = File::open(path)?;
    let reader = FileReader::try_new(file, None)?;
    let schema = reader.schema();

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
    input_ipc: std::path::PathBuf,
    create_table_sql_template: String,
}

impl BenchOptions {
    fn parse(args: Vec<String>) -> Result<Self, Box<dyn Error>> {
        let mut options = Self {
            rows: 100_000,
            batch_size: 8_192,
            scenario: "narrow_numeric".to_owned(),
            repeat: 1,
            input_ipc: std::path::PathBuf::new(),
            create_table_sql_template: String::new(),
        };
        let mut input_ipc = None;
        let mut create_table_sql_template = None;
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
                    input_ipc = Some(std::path::PathBuf::from(required_arg(
                        "--input-ipc",
                        args.get(index),
                    )?));
                }
                "--create-table-sql-template" => {
                    index += 1;
                    create_table_sql_template = Some(
                        required_arg("--create-table-sql-template", args.get(index))?.to_owned(),
                    );
                }
                other => return Err(format!("unknown arrow-odbc runner option `{other}`").into()),
            }

            index += 1;
        }

        options.input_ipc =
            input_ipc.ok_or("missing required arrow-odbc runner option `--input-ipc <FILE>`")?;
        options.create_table_sql_template = create_table_sql_template.ok_or(
            "missing required arrow-odbc runner option `--create-table-sql-template <SQL>`",
        )?;
        if !options.create_table_sql_template.contains(TABLE_PLACEHOLDER) {
            return Err(format!(
                "--create-table-sql-template must contain `{TABLE_PLACEHOLDER}`"
            )
            .into());
        }

        Ok(options)
    }

    fn create_table_sql(&self, table: &str) -> Result<String, Box<dyn Error>> {
        if table.contains(TABLE_PLACEHOLDER) {
            return Err("benchmark table name unexpectedly contains template placeholder".into());
        }

        Ok(self.create_table_sql_template.replace(TABLE_PLACEHOLDER, table))
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
    use super::{BenchOptions, TABLE_PLACEHOLDER, ipc_batches, rows_per_second};
    use arrow_ipc::writer::FileWriter;
    use arrow_odbc::arrow::array::{Float64Array, Int32Array, Int64Array, StringArray};
    use arrow_odbc::arrow::datatypes::{DataType, Field, Schema};
    use arrow_odbc::arrow::record_batch::RecordBatch;
    use std::path::PathBuf;
    use std::sync::Arc;
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
            "--input-ipc".to_owned(),
            "/workspace/bench.arrow".to_owned(),
            "--create-table-sql-template".to_owned(),
            create_table_sql_template(),
        ])
        .expect("valid options should parse");

        assert_eq!(options.rows, 25);
        assert_eq!(options.batch_size, 8);
        assert_eq!(options.scenario, "narrow_numeric");
        assert_eq!(options.repeat, 3);
        assert_eq!(
            options.input_ipc.as_path(),
            std::path::Path::new("/workspace/bench.arrow")
        );
        assert_eq!(options.create_table_sql_template, create_table_sql_template());
    }

    #[test]
    fn parses_input_ipc_option() {
        let options = BenchOptions::parse(vec![
            "--rows".to_owned(),
            "25".to_owned(),
            "--input-ipc".to_owned(),
            "/workspace/bench.arrow".to_owned(),
            "--create-table-sql-template".to_owned(),
            create_table_sql_template(),
        ])
        .expect("valid options should parse");

        assert_eq!(options.rows, 25);
        assert_eq!(
            options.input_ipc.as_path(),
            std::path::Path::new("/workspace/bench.arrow")
        );
    }

    #[test]
    fn rejects_missing_input_ipc() {
        let err = BenchOptions::parse(vec![
            "--rows".to_owned(),
            "25".to_owned(),
            "--scenario".to_owned(),
            "mixed_nullable".to_owned(),
            "--create-table-sql-template".to_owned(),
            create_table_sql_template(),
        ])
        .expect_err("input IPC should be required");

        assert!(err.to_string().contains("--input-ipc"));
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
    fn renders_table_sql_from_template() {
        let options = BenchOptions::parse(vec![
            "--input-ipc".to_owned(),
            "/workspace/bench.arrow".to_owned(),
            "--create-table-sql-template".to_owned(),
            create_table_sql_template(),
        ])
        .expect("valid options should parse");
        let sql = options
            .create_table_sql("[dbo].[target]")
            .expect("template should render");

        assert_eq!(
            sql,
            "CREATE TABLE [dbo].[target] ([id32] int NOT NULL)"
        );
    }

    #[test]
    fn rejects_table_sql_template_without_placeholder() {
        let err = BenchOptions::parse(vec![
            "--input-ipc".to_owned(),
            "/workspace/bench.arrow".to_owned(),
            "--create-table-sql-template".to_owned(),
            "CREATE TABLE [dbo].[target] ([id32] int NOT NULL)".to_owned(),
        ])
        .expect_err("template without placeholder should be rejected");

        assert!(err.to_string().contains(TABLE_PLACEHOLDER));
    }

    #[test]
    fn reads_input_ipc_batches_for_matching_scenario() {
        let schema = mixed_nullable_schema();
        let batches = mixed_nullable_test_batches(schema.clone(), &[8, 8, 8, 1]);
        let path = temp_ipc_path("mixed");
        write_ipc_file(&path, schema.as_ref(), &batches);

        let input = ipc_batches(&path, 25).expect("matching IPC file should load");

        assert_eq!(input.schema.as_ref(), schema.as_ref());
        assert_eq!(input.batches, batches);
        assert_eq!(input.rows, 25);

        std::fs::remove_file(path).expect("test IPC file should be removed");
    }

    #[test]
    fn rejects_input_ipc_row_count_mismatch() {
        let schema = mixed_nullable_schema();
        let batches = mixed_nullable_test_batches(schema.clone(), &[8, 8, 8, 1]);
        let path = temp_ipc_path("row-mismatch");
        write_ipc_file(&path, schema.as_ref(), &batches);

        let err = ipc_batches(&path, 24).expect_err("row count mismatch should be rejected");

        assert!(err.to_string().contains("row count"));

        std::fs::remove_file(path).expect("test IPC file should be removed");
    }

    #[test]
    fn input_ipc_preserves_file_batch_boundaries() {
        let schema = mixed_nullable_schema();
        let batches = mixed_nullable_test_batches(schema.clone(), &[8, 8, 8, 1]);
        let path = temp_ipc_path("options");
        write_ipc_file(&path, schema.as_ref(), &batches);

        let input = ipc_batches(&path, 25).expect("input IPC batches should load");
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

    fn mixed_nullable_test_batches(schema: Arc<Schema>, lengths: &[usize]) -> Vec<RecordBatch> {
        let mut offset = 0;
        let mut batches = Vec::with_capacity(lengths.len());

        for &len in lengths {
            let start = offset;
            offset += len;
            let batch = RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(
                        (start..offset)
                            .map(|row| row as i32)
                            .collect::<Int32Array>(),
                    ),
                    Arc::new(
                        (start..offset)
                            .map(|row| {
                                if row % 7 == 0 {
                                    None
                                } else {
                                    Some(row as i64 * 10)
                                }
                            })
                            .collect::<Int64Array>(),
                    ),
                    Arc::new(
                        (start..offset)
                            .map(|row| {
                                if row % 11 == 0 {
                                    None
                                } else {
                                    Some(row as f64 / 10.0)
                                }
                            })
                            .collect::<Float64Array>(),
                    ),
                    Arc::new(
                        (start..offset)
                            .map(|row| {
                                if row % 5 == 0 {
                                    None
                                } else {
                                    Some(format!("category-{}", row % 3))
                                }
                            })
                            .collect::<StringArray>(),
                    ),
                ],
            )
            .expect("mixed nullable test batch should build");

            batches.push(batch);
        }

        batches
    }

    fn temp_ipc_path(name: &str) -> PathBuf {
        let counter = TEST_FILE_COUNTER.fetch_add(1, Ordering::Relaxed);
        std::env::temp_dir().join(format!(
            "arrow-tiberius-odbc-runner-{name}-{}-{counter}.arrow",
            std::process::id()
        ))
    }

    fn create_table_sql_template() -> String {
        format!("CREATE TABLE {TABLE_PLACEHOLDER} ([id32] int NOT NULL)")
    }

    fn mixed_nullable_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("id32", DataType::Int32, false),
            Field::new("maybe_id64", DataType::Int64, true),
            Field::new("maybe_score", DataType::Float64, true),
            Field::new("category", DataType::Utf8, true),
        ]))
    }
}

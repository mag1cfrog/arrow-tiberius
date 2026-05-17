use std::ffi::OsString;
use std::fmt;
use std::path::PathBuf;
use std::str::FromStr;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use crate::{odbc_runner, sqlserver};
use arrow_array::{
    ArrayRef, BinaryArray, BooleanArray, Date32Array, Decimal128Array, Float64Array, Int32Array,
    Int64Array, RecordBatch, StringArray, TimestampMillisecondArray,
};
use arrow_schema::{DataType, Field, Schema, SchemaRef, TimeUnit};
use arrow_tiberius::{
    BulkWriter, MssqlProfile, PlanOptions, SchemaMapping, TableName, WriteBackend, WriteOptions,
    create_table_sql_from_mappings, plan_arrow_schema_to_mssql_mappings,
};
use tokio::net::TcpStream;
use tokio_util::compat::{Compat, TokioAsyncWriteCompatExt};

static BENCH_TABLE_COUNTER: AtomicU64 = AtomicU64::new(0);

pub(super) fn run(args: &[OsString]) -> Result<(), WriterBenchError> {
    if args.is_empty()
        || args
            .first()
            .is_some_and(|arg| arg == "-h" || arg == "--help")
    {
        print_help();
        return Ok(());
    }

    if let Some(command) = args.first().and_then(|arg| arg.to_str()) {
        if command == "baseline" {
            return run_baseline(&args[1..]);
        }

        if command == "arrow-odbc" {
            return run_arrow_odbc(&args[1..]);
        }

        if !command.starts_with('-') {
            return Err(WriterBenchError::UnknownCommand(command.to_owned()));
        }
    }

    run_generated_summary(args)
}

fn run_generated_summary(args: &[OsString]) -> Result<(), WriterBenchError> {
    let options = WriterBenchOptions::parse(args)?;
    let summary = summarize_generated_batches(&options)?;
    print_summary(&options, &summary);
    Ok(())
}

fn run_baseline(args: &[OsString]) -> Result<(), WriterBenchError> {
    if args.iter().any(|arg| arg == "-h" || arg == "--help") {
        print_baseline_help();
        return Ok(());
    }

    let options = BaselineBenchOptions::parse(args)?;
    let connection = options
        .sql_server
        .connect_or_start()
        .map_err(WriterBenchError::SqlServer)?;
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_io()
        .build()
        .map_err(WriterBenchError::Io)?;
    let report = runtime.block_on(run_baseline_async(&options, &connection))?;

    print_baseline_summary(&options, &report, &connection);
    Ok(())
}

fn run_arrow_odbc(args: &[OsString]) -> Result<(), WriterBenchError> {
    if args.iter().any(|arg| arg == "-h" || arg == "--help") {
        print_arrow_odbc_help();
        return Ok(());
    }

    let options = ArrowOdbcBenchOptions::parse(args)?;

    if options.build_runner_image {
        build_arrow_odbc_runner_image(&options)?;
        return Ok(());
    }

    Err(WriterBenchError::ArrowOdbcUnavailable {
        reason: "arrow-odbc runner container is not wired yet".to_owned(),
    })
}

fn print_help() {
    println!(
        "Usage:\n  cargo xtask writer-bench [OPTIONS]\n  cargo xtask writer-bench baseline [OPTIONS]\n  cargo xtask writer-bench arrow-odbc [OPTIONS]\n\nCommands:\n  baseline      Run the baseline TokenRow SQL Server writer benchmark\n  arrow-odbc    Run the optional arrow-odbc SQL Server writer benchmark\n\nOptions:\n  --rows <COUNT>          Total rows to generate [default: 100000]\n  --batch-size <COUNT>    Maximum rows per generated RecordBatch [default: 8192]\n  --scenario <NAME>       Benchmark scenario [default: narrow_numeric]\n  --repeat <COUNT>        Number of benchmark repeats [default: 1]\n  --output <FORMAT>       Output format: human [default: human]\n  -h, --help              Print help\n\nScenarios:"
    );
    for scenario in SCENARIOS {
        println!("  {:<16}  {}", scenario.name, scenario.description);
    }
}

fn print_baseline_help() {
    println!(
        "Usage:\n  cargo xtask writer-bench baseline [OPTIONS]\n\nData Options:\n  --rows <COUNT>              Total rows to generate [default: 100000]\n  --batch-size <COUNT>        Maximum rows per generated RecordBatch [default: 8192]\n  --scenario <NAME>           Benchmark scenario [default: narrow_numeric]\n  --repeat <COUNT>            Number of benchmark repeats [default: 1]\n  --output <FORMAT>           Output format: human [default: human]\n\nSQL Server Options:\n  --container-runtime <PATH>  Container runtime executable, such as docker or podman\n  --connection-string <URL>   Use an existing SQL Server instead of a local container\n  --image <IMAGE>             SQL Server container image\n  --database <NAME>           Benchmark database name\n  --keep-container            Keep the container after the task exits\n  -h, --help                  Print help"
    );
}

fn print_arrow_odbc_help() {
    println!(
        "Usage:\n  cargo xtask writer-bench arrow-odbc [OPTIONS]\n\nData Options:\n  --rows <COUNT>              Total rows to generate [default: 100000]\n  --batch-size <COUNT>        Maximum rows per generated RecordBatch [default: 8192]\n  --scenario <NAME>           Supported scenarios: narrow_numeric, mixed_nullable [default: narrow_numeric]\n  --repeat <COUNT>            Number of benchmark repeats [default: 1]\n  --output <FORMAT>           Output format: human [default: human]\n\nSQL Server Options:\n  --container-runtime <PATH>  Container runtime executable, such as docker or podman\n  --connection-string <URL>   Use an existing SQL Server instead of a local container\n  --image <IMAGE>             SQL Server container image\n  --database <NAME>           Benchmark database name\n  --keep-container            Keep managed containers after the task exits\n\nODBC Runner Options:\n  --build-runner-image        Build the managed arrow-odbc runner image and exit\n  --runner-image <IMAGE>      Managed arrow-odbc runner image tag\n  -h, --help                  Print help\n\nThis is a SQL Server write-path comparison only. The arrow-odbc runner image contains unixODBC, Microsoft ODBC Driver 18 for SQL Server, and Rust."
    );
}

fn print_summary(options: &WriterBenchOptions, summary: &GeneratedBatchSummary) {
    println!("writer-bench");
    println!("  rows per repeat: {}", options.rows);
    println!("  batch size: {}", options.batch_size);
    println!("  scenario: {}", options.scenario);
    println!("  repeat: {}", options.repeat);
    println!("  output: {}", options.output);
    println!("  batches per repeat: {}", summary.batches);
    println!("  generated rows per repeat: {}", summary.rows);
}

fn print_baseline_summary(
    options: &BaselineBenchOptions,
    report: &BaselineBenchReport,
    connection: &sqlserver::SqlServerConnection,
) {
    println!("writer-bench baseline");
    println!("  backend: baseline_token_row");
    println!("  rows per repeat: {}", options.benchmark.rows);
    println!("  batch size: {}", options.benchmark.batch_size);
    println!("  scenario: {}", options.benchmark.scenario);
    println!("  repeat: {}", options.benchmark.repeat);
    println!("  output: {}", options.benchmark.output);
    println!("  batches written: {}", report.stats.batches_written);
    println!("  rows written: {}", report.stats.rows_written);
    println!(
        "  write rows/sec: {}",
        format_rows_per_second(
            report.stats.rows_written,
            report.timings.write + report.timings.finish
        )
    );
    println!("  validated rows: {}", report.validated_rows);
    println!(
        "  existing connection: {}",
        options.sql_server.connection_string.is_some()
    );
    println!("  database: {}", connection.database);
    if let Some(runtime) = &options.sql_server.container_runtime {
        println!("  container runtime: {}", runtime.display());
    } else if options.sql_server.connection_string.is_some() {
        println!("  container runtime: <not used>");
    } else {
        println!("  container runtime: <auto>");
    }
    println!("  image: {}", options.sql_server.image);
    println!("  keep container: {}", options.sql_server.keep_container);
    println!("  setup: {}", format_duration(report.timings.setup));
    println!("  write: {}", format_duration(report.timings.write));
    println!("  finish: {}", format_duration(report.timings.finish));
    println!("  validate: {}", format_duration(report.timings.validate));
    println!("  cleanup: {}", format_duration(report.timings.cleanup));
    println!("  total: {}", format_duration(report.timings.total));
}

#[derive(Debug, Clone)]
struct WriterBenchOptions {
    rows: usize,
    batch_size: usize,
    scenario: &'static BenchmarkScenarioDefinition,
    repeat: usize,
    output: BenchmarkOutput,
}

impl Default for WriterBenchOptions {
    fn default() -> Self {
        Self {
            rows: 100_000,
            batch_size: 8_192,
            scenario: scenario_by_name("narrow_numeric").unwrap_or(&NARROW_NUMERIC_SCENARIO),
            repeat: 1,
            output: BenchmarkOutput::Human,
        }
    }
}

impl WriterBenchOptions {
    fn parse(args: &[OsString]) -> Result<Self, WriterBenchError> {
        let mut options = Self::default();
        let mut index = 0;

        while index < args.len() {
            let arg = args[index]
                .to_str()
                .ok_or_else(|| WriterBenchError::InvalidUtf8Argument(args[index].clone()))?;

            match arg {
                "-h" | "--help" => {
                    print_help();
                    return Ok(options);
                }
                "--rows" => {
                    options.rows = parse_positive_usize("--rows", &required_value(args, index)?)?;
                    index += 1;
                }
                "--batch-size" => {
                    options.batch_size =
                        parse_positive_usize("--batch-size", &required_value(args, index)?)?;
                    index += 1;
                }
                "--scenario" => {
                    options.scenario = parse_scenario(&required_value(args, index)?)?;
                    index += 1;
                }
                "--repeat" => {
                    options.repeat =
                        parse_positive_usize("--repeat", &required_value(args, index)?)?;
                    index += 1;
                }
                "--output" => {
                    options.output = required_value(args, index)?.parse()?;
                    index += 1;
                }
                other => return Err(WriterBenchError::UnknownOption(other.to_owned())),
            }

            index += 1;
        }

        Ok(options)
    }
}

#[derive(Debug, Clone)]
struct BaselineBenchOptions {
    benchmark: WriterBenchOptions,
    sql_server: sqlserver::SqlServerConnectionOptions,
}

impl BaselineBenchOptions {
    fn parse(args: &[OsString]) -> Result<Self, WriterBenchError> {
        let mut options = Self {
            benchmark: WriterBenchOptions::default(),
            sql_server: sqlserver::SqlServerConnectionOptions::benchmark_default(),
        };
        let mut index = 0;

        while index < args.len() {
            let arg = args[index]
                .to_str()
                .ok_or_else(|| WriterBenchError::InvalidUtf8Argument(args[index].clone()))?;

            match arg {
                "-h" | "--help" => {
                    print_baseline_help();
                    return Ok(options);
                }
                "--rows" => {
                    options.benchmark.rows =
                        parse_positive_usize("--rows", &required_value(args, index)?)?;
                    index += 1;
                }
                "--batch-size" => {
                    options.benchmark.batch_size =
                        parse_positive_usize("--batch-size", &required_value(args, index)?)?;
                    index += 1;
                }
                "--scenario" => {
                    options.benchmark.scenario = parse_scenario(&required_value(args, index)?)?;
                    index += 1;
                }
                "--repeat" => {
                    options.benchmark.repeat =
                        parse_positive_usize("--repeat", &required_value(args, index)?)?;
                    index += 1;
                }
                "--output" => {
                    options.benchmark.output = required_value(args, index)?.parse()?;
                    index += 1;
                }
                "--container-runtime" => {
                    options.sql_server.container_runtime =
                        Some(PathBuf::from(required_value(args, index)?));
                    index += 1;
                }
                "--connection-string" => {
                    options.sql_server.connection_string = Some(required_value(args, index)?);
                    index += 1;
                }
                "--image" => {
                    options.sql_server.image = required_value(args, index)?;
                    index += 1;
                }
                "--database" => {
                    options.sql_server.database = required_value(args, index)?;
                    index += 1;
                }
                "--keep-container" => {
                    options.sql_server.keep_container = true;
                }
                other => return Err(WriterBenchError::UnknownOption(other.to_owned())),
            }

            index += 1;
        }

        Ok(options)
    }
}

#[derive(Debug, Clone)]
struct ArrowOdbcBenchOptions {
    benchmark: WriterBenchOptions,
    sql_server: sqlserver::SqlServerConnectionOptions,
    build_runner_image: bool,
    runner_image: String,
}

impl ArrowOdbcBenchOptions {
    fn parse(args: &[OsString]) -> Result<Self, WriterBenchError> {
        let options = parse_writer_sqlserver_options(args, print_arrow_odbc_help)?;

        if !is_arrow_odbc_supported_scenario(options.benchmark.scenario) {
            return Err(WriterBenchError::UnsupportedArrowOdbcScenario {
                scenario: options.benchmark.scenario.name.to_owned(),
            });
        }

        Ok(options)
    }
}

fn parse_writer_sqlserver_options(
    args: &[OsString],
    print_command_help: fn(),
) -> Result<ArrowOdbcBenchOptions, WriterBenchError> {
    let mut options = ArrowOdbcBenchOptions {
        benchmark: WriterBenchOptions::default(),
        sql_server: sqlserver::SqlServerConnectionOptions::benchmark_default(),
        build_runner_image: false,
        runner_image: odbc_runner::DEFAULT_RUNNER_IMAGE_TAG.to_owned(),
    };
    let mut index = 0;

    while index < args.len() {
        let arg = args[index]
            .to_str()
            .ok_or_else(|| WriterBenchError::InvalidUtf8Argument(args[index].clone()))?;

        match arg {
            "-h" | "--help" => {
                print_command_help();
                return Ok(options);
            }
            "--rows" => {
                options.benchmark.rows =
                    parse_positive_usize("--rows", &required_value(args, index)?)?;
                index += 1;
            }
            "--batch-size" => {
                options.benchmark.batch_size =
                    parse_positive_usize("--batch-size", &required_value(args, index)?)?;
                index += 1;
            }
            "--scenario" => {
                options.benchmark.scenario = parse_scenario(&required_value(args, index)?)?;
                index += 1;
            }
            "--repeat" => {
                options.benchmark.repeat =
                    parse_positive_usize("--repeat", &required_value(args, index)?)?;
                index += 1;
            }
            "--output" => {
                options.benchmark.output = required_value(args, index)?.parse()?;
                index += 1;
            }
            "--container-runtime" => {
                options.sql_server.container_runtime =
                    Some(PathBuf::from(required_value(args, index)?));
                index += 1;
            }
            "--connection-string" => {
                options.sql_server.connection_string = Some(required_value(args, index)?);
                index += 1;
            }
            "--image" => {
                options.sql_server.image = required_value(args, index)?;
                index += 1;
            }
            "--database" => {
                options.sql_server.database = required_value(args, index)?;
                index += 1;
            }
            "--keep-container" => {
                options.sql_server.keep_container = true;
            }
            "--build-runner-image" => {
                options.build_runner_image = true;
            }
            "--runner-image" => {
                options.runner_image = required_value(args, index)?;
                index += 1;
            }
            other => return Err(WriterBenchError::UnknownOption(other.to_owned())),
        }

        index += 1;
    }

    Ok(options)
}

fn is_arrow_odbc_supported_scenario(scenario: &BenchmarkScenarioDefinition) -> bool {
    matches!(scenario.name, "narrow_numeric" | "mixed_nullable")
}

fn build_arrow_odbc_runner_image(options: &ArrowOdbcBenchOptions) -> Result<(), WriterBenchError> {
    let container_runtime = options
        .sql_server
        .resolve_runtime()
        .map_err(WriterBenchError::SqlServer)?;
    let image_options = odbc_runner::RunnerImageOptions {
        container_runtime,
        image_tag: options.runner_image.clone(),
        manifest_dir: repository_root()?,
    };

    println!("writer-bench arrow-odbc");
    println!("  action: build_runner_image");
    println!("  image: {}", image_options.image_tag);
    println!("  dockerfile: {}", image_options.dockerfile().display());

    odbc_runner::build_runner_image(&image_options).map_err(WriterBenchError::OdbcRunner)
}

fn repository_root() -> Result<PathBuf, WriterBenchError> {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .map(PathBuf::from)
        .ok_or_else(|| {
            WriterBenchError::Validation("xtask manifest directory has no parent".to_owned())
        })
}

#[derive(Debug, Clone, Copy)]
struct BenchmarkScenarioDefinition {
    name: &'static str,
    description: &'static str,
    schema: fn() -> SchemaRef,
    columns: fn(offset: usize, len: usize) -> Result<Vec<ArrayRef>, WriterBenchError>,
}

impl fmt::Display for BenchmarkScenarioDefinition {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.name)
    }
}

const NARROW_NUMERIC_SCENARIO: BenchmarkScenarioDefinition = BenchmarkScenarioDefinition {
    name: "narrow_numeric",
    description: "Primitive numeric throughput",
    schema: narrow_numeric_schema,
    columns: narrow_numeric_columns,
};

const MIXED_NULLABLE_SCENARIO: BenchmarkScenarioDefinition = BenchmarkScenarioDefinition {
    name: "mixed_nullable",
    description: "Nullable primitives and short strings",
    schema: mixed_nullable_schema,
    columns: mixed_nullable_columns,
};

const WIDE_MIXED_SCENARIO: BenchmarkScenarioDefinition = BenchmarkScenarioDefinition {
    name: "wide_mixed",
    description: "Ingestion-style ids, event time, categories, text, and binary payloads",
    schema: wide_mixed_schema,
    columns: wide_mixed_columns,
};

const DECIMAL_TEMPORAL_SCENARIO: BenchmarkScenarioDefinition = BenchmarkScenarioDefinition {
    name: "decimal_temporal",
    description: "Finance-style decimals, dates, and timestamps",
    schema: decimal_temporal_schema,
    columns: decimal_temporal_columns,
};

const STRING_HEAVY_SCENARIO: BenchmarkScenarioDefinition = BenchmarkScenarioDefinition {
    name: "string_heavy",
    description: "Large variable text and binary payload rows",
    schema: string_heavy_schema,
    columns: string_heavy_columns,
};

const WIDE_SPARSE_SCENARIO: BenchmarkScenarioDefinition = BenchmarkScenarioDefinition {
    name: "wide_sparse",
    description: "Thirty-two mixed columns with sparse nullable values",
    schema: wide_sparse_schema,
    columns: wide_sparse_columns,
};

const TPCH_LINEITEM_LIKE_SCENARIO: BenchmarkScenarioDefinition = BenchmarkScenarioDefinition {
    name: "tpch_lineitem_like",
    description: "TPC-H lineitem-inspired transport workload without external dbgen",
    schema: tpch_lineitem_like_schema,
    columns: tpch_lineitem_like_columns,
};

const SCENARIOS: &[BenchmarkScenarioDefinition] = &[
    NARROW_NUMERIC_SCENARIO,
    MIXED_NULLABLE_SCENARIO,
    WIDE_MIXED_SCENARIO,
    DECIMAL_TEMPORAL_SCENARIO,
    STRING_HEAVY_SCENARIO,
    WIDE_SPARSE_SCENARIO,
    TPCH_LINEITEM_LIKE_SCENARIO,
];

fn scenario_by_name(name: &str) -> Option<&'static BenchmarkScenarioDefinition> {
    SCENARIOS.iter().find(|scenario| scenario.name == name)
}

fn parse_scenario(value: &str) -> Result<&'static BenchmarkScenarioDefinition, WriterBenchError> {
    scenario_by_name(value).ok_or_else(|| WriterBenchError::InvalidScenario(value.to_owned()))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum BenchmarkOutput {
    Human,
}

impl fmt::Display for BenchmarkOutput {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Human => f.write_str("human"),
        }
    }
}

impl FromStr for BenchmarkOutput {
    type Err = WriterBenchError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "human" => Ok(Self::Human),
            other => Err(WriterBenchError::InvalidOutput(other.to_owned())),
        }
    }
}

#[derive(Debug, Clone)]
struct GeneratedBatchReader {
    scenario: &'static BenchmarkScenarioDefinition,
    schema: SchemaRef,
    rows: usize,
    batch_size: usize,
    next_offset: usize,
}

impl GeneratedBatchReader {
    fn new(options: &WriterBenchOptions) -> Self {
        Self::new_with_schema(options, (options.scenario.schema)())
    }

    fn new_with_schema(options: &WriterBenchOptions, schema: SchemaRef) -> Self {
        Self {
            scenario: options.scenario,
            schema,
            rows: options.rows,
            batch_size: options.batch_size,
            next_offset: 0,
        }
    }
}

impl Iterator for GeneratedBatchReader {
    type Item = Result<RecordBatch, WriterBenchError>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.next_offset == self.rows {
            return None;
        }

        let offset = self.next_offset;
        let len = self.batch_size.min(self.rows - offset);
        self.next_offset += len;

        Some(generate_batch(
            self.scenario,
            self.schema.clone(),
            offset,
            len,
        ))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct GeneratedBatchSummary {
    batches: usize,
    rows: usize,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
struct BaselineBenchReport {
    stats: arrow_tiberius::WriteStats,
    validated_rows: u64,
    timings: BaselineBenchTimings,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
struct BaselineBenchTimings {
    setup: Duration,
    write: Duration,
    finish: Duration,
    validate: Duration,
    cleanup: Duration,
    total: Duration,
}

type BenchClient = tiberius::Client<Compat<TcpStream>>;

async fn run_baseline_async(
    options: &BaselineBenchOptions,
    connection: &sqlserver::SqlServerConnection,
) -> Result<BaselineBenchReport, WriterBenchError> {
    let total_start = Instant::now();
    let mut report = BaselineBenchReport::default();

    let setup_start = Instant::now();
    let mut client = connect(&connection.connection_string, &connection.database).await?;
    let schema = (options.benchmark.scenario.schema)();
    let mappings = benchmark_mappings_for_schema(Arc::clone(&schema))?;
    report.timings.setup += setup_start.elapsed();

    for _repeat_index in 0..options.benchmark.repeat {
        let table = unique_benchmark_table_name()?;
        let repeat_report =
            run_baseline_repeat(&mut client, options, &schema, &mappings, &table).await;
        let cleanup_start = Instant::now();
        let cleanup_result = drop_table(&mut client, &table).await;
        report.timings.cleanup += cleanup_start.elapsed();

        if let Err(source) = cleanup_result {
            if repeat_report.is_err() {
                eprintln!(
                    "warning: failed to clean up benchmark table {} after benchmark failure: {source}",
                    table.quoted_sql()
                );
            } else {
                return Err(WriterBenchError::Tiberius(source));
            }
        }

        let repeat_report = repeat_report?;
        report.stats.rows_written = report
            .stats
            .rows_written
            .saturating_add(repeat_report.stats.rows_written);
        report.stats.batches_written = report
            .stats
            .batches_written
            .saturating_add(repeat_report.stats.batches_written);
        report.validated_rows = report
            .validated_rows
            .saturating_add(repeat_report.validated_rows);
        report.timings.setup += repeat_report.timings.setup;
        report.timings.write += repeat_report.timings.write;
        report.timings.finish += repeat_report.timings.finish;
        report.timings.validate += repeat_report.timings.validate;
    }

    report.timings.total = total_start.elapsed();
    Ok(report)
}

async fn run_baseline_repeat(
    client: &mut BenchClient,
    options: &BaselineBenchOptions,
    schema: &SchemaRef,
    mappings: &[arrow_tiberius::SchemaMapping],
    table: &TableName,
) -> Result<BaselineBenchReport, WriterBenchError> {
    let mut report = BaselineBenchReport::default();
    let setup_start = Instant::now();

    execute_sql(
        client,
        format!("DROP TABLE IF EXISTS {}", table.quoted_sql()),
    )
    .await?;
    execute_sql(client, benchmark_table_sql(table, mappings)).await?;
    let mut writer = BulkWriter::new(
        client,
        table.clone(),
        mappings.to_vec(),
        WriteOptions {
            backend: WriteBackend::BaselineTokenRow,
            ..WriteOptions::default()
        },
    )
    .await
    .map_err(WriterBenchError::ArrowTiberius)?;
    report.timings.setup += setup_start.elapsed();

    let write_start = Instant::now();
    for batch in GeneratedBatchReader::new_with_schema(&options.benchmark, Arc::clone(schema)) {
        let batch = batch?;
        report.stats = writer
            .write_batch(&batch)
            .await
            .map_err(WriterBenchError::ArrowTiberius)?;
    }
    report.timings.write += write_start.elapsed();

    let finish_start = Instant::now();
    report.stats = writer
        .finish()
        .await
        .map_err(WriterBenchError::ArrowTiberius)?;
    report.timings.finish += finish_start.elapsed();

    let validate_start = Instant::now();
    report.validated_rows = select_count(client, table).await?;
    report.timings.validate += validate_start.elapsed();

    if report.validated_rows != report.stats.rows_written {
        return Err(WriterBenchError::RowCountMismatch {
            expected: report.stats.rows_written,
            actual: report.validated_rows,
        });
    }

    Ok(report)
}

async fn connect(connection_string: &str, database: &str) -> Result<BenchClient, WriterBenchError> {
    let connection_string = format!("{connection_string};database={database}");
    let config = tiberius::Config::from_ado_string(&connection_string)
        .map_err(WriterBenchError::Tiberius)?;
    let tcp = TcpStream::connect(config.get_addr())
        .await
        .map_err(WriterBenchError::Io)?;

    tiberius::Client::connect(config, tcp.compat_write())
        .await
        .map_err(WriterBenchError::Tiberius)
}

async fn execute_sql(client: &mut BenchClient, sql: String) -> Result<(), WriterBenchError> {
    client
        .simple_query(sql)
        .await
        .map_err(WriterBenchError::Tiberius)?
        .into_results()
        .await
        .map_err(WriterBenchError::Tiberius)?;

    Ok(())
}

async fn drop_table(client: &mut BenchClient, table: &TableName) -> tiberius::Result<()> {
    client
        .simple_query(format!("DROP TABLE IF EXISTS {}", table.quoted_sql()))
        .await?
        .into_results()
        .await?;

    Ok(())
}

async fn select_count(
    client: &mut BenchClient,
    table: &TableName,
) -> Result<u64, WriterBenchError> {
    let row = client
        .simple_query(format!("SELECT COUNT_BIG(*) FROM {}", table.quoted_sql()))
        .await
        .map_err(WriterBenchError::Tiberius)?
        .into_row()
        .await
        .map_err(WriterBenchError::Tiberius)?
        .ok_or_else(|| {
            WriterBenchError::Validation("SELECT COUNT_BIG(*) returned no row".to_owned())
        })?;
    let count = row.get::<i64, _>(0).ok_or_else(|| {
        WriterBenchError::Validation("SELECT COUNT_BIG(*) did not return bigint".to_owned())
    })?;

    u64::try_from(count).map_err(|_| {
        WriterBenchError::Validation(format!(
            "SELECT COUNT_BIG(*) returned negative count {count}"
        ))
    })
}

fn unique_benchmark_table_name() -> Result<TableName, WriterBenchError> {
    let counter = BENCH_TABLE_COUNTER.fetch_add(1, Ordering::Relaxed);
    let table = format!("arrow_tiberius_bench_{}_{}", std::process::id(), counter);

    TableName::new("dbo", table).map_err(WriterBenchError::ArrowTiberius)
}

fn benchmark_table_sql(table: &TableName, mappings: &[SchemaMapping]) -> String {
    create_table_sql_from_mappings(table, mappings)
}

fn benchmark_mappings_for_schema(
    schema: SchemaRef,
) -> Result<Vec<SchemaMapping>, WriterBenchError> {
    let (mappings, _diagnostics) = plan_arrow_schema_to_mssql_mappings(
        schema,
        MssqlProfile::sql_server_2016_compat_100(),
        PlanOptions::default(),
    )
    .map_err(WriterBenchError::ArrowTiberius)?
    .into_parts();

    Ok(mappings)
}

fn format_duration(duration: Duration) -> String {
    format!("{:.3}s", duration.as_secs_f64())
}

fn format_rows_per_second(rows: u64, elapsed: Duration) -> String {
    if elapsed.is_zero() {
        return "n/a".to_owned();
    }

    format!("{:.2}", rows as f64 / elapsed.as_secs_f64())
}

fn summarize_generated_batches(
    options: &WriterBenchOptions,
) -> Result<GeneratedBatchSummary, WriterBenchError> {
    let mut summary = GeneratedBatchSummary {
        batches: 0,
        rows: 0,
    };

    for batch in GeneratedBatchReader::new(options) {
        let batch = batch?;
        summary.batches += 1;
        summary.rows += batch.num_rows();
    }

    Ok(summary)
}

fn generate_batch(
    scenario: &BenchmarkScenarioDefinition,
    schema: SchemaRef,
    offset: usize,
    len: usize,
) -> Result<RecordBatch, WriterBenchError> {
    let columns = (scenario.columns)(offset, len)?;

    RecordBatch::try_new(schema, columns).map_err(WriterBenchError::Arrow)
}

fn narrow_numeric_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id32", DataType::Int32, false),
        Field::new("id64", DataType::Int64, false),
        Field::new("score", DataType::Float64, false),
    ]))
}

fn narrow_numeric_columns(offset: usize, len: usize) -> Result<Vec<ArrayRef>, WriterBenchError> {
    let id32 = (offset..offset + len)
        .map(deterministic_i32)
        .collect::<Int32Array>();
    let id64 = (offset..offset + len)
        .map(|row| i64::from(deterministic_i32(row)) * 1_000)
        .collect::<Int64Array>();
    let score = (offset..offset + len)
        .map(deterministic_score)
        .collect::<Float64Array>();

    Ok(vec![Arc::new(id32), Arc::new(id64), Arc::new(score)])
}

fn mixed_nullable_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id32", DataType::Int32, false),
        Field::new("maybe_id64", DataType::Int64, true),
        Field::new("maybe_score", DataType::Float64, true),
        Field::new("category", DataType::Utf8, true),
    ]))
}

fn mixed_nullable_columns(offset: usize, len: usize) -> Result<Vec<ArrayRef>, WriterBenchError> {
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

    Ok(vec![
        Arc::new(id32),
        Arc::new(maybe_id64),
        Arc::new(maybe_score),
        Arc::new(category),
    ])
}

fn wide_mixed_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("account_id", DataType::Int32, false),
        Field::new(
            "event_time_ms",
            DataType::Timestamp(TimeUnit::Millisecond, None),
            false,
        ),
        Field::new("amount", DataType::Float64, true),
        Field::new("status", DataType::Utf8, true),
        Field::new("region", DataType::Utf8, true),
        Field::new("description", DataType::Utf8, true),
        Field::new("payload", DataType::Binary, true),
    ]))
}

fn wide_mixed_columns(offset: usize, len: usize) -> Result<Vec<ArrayRef>, WriterBenchError> {
    let status_values = ["queued", "active", "settled", "failed", "cancelled"];
    let region_values = ["us-west", "us-east", "eu-central", "ap-south"];
    let id = (offset..offset + len)
        .map(|row| 10_000_000_i64 + row as i64)
        .collect::<Int64Array>();
    let account_id = (offset..offset + len)
        .map(|row| 1_000 + (row % 50_000) as i32)
        .collect::<Int32Array>();
    let event_time_ms = TimestampMillisecondArray::from_iter_values(
        (offset..offset + len).map(|row| 1_735_689_600_000_i64 + (row as i64 * 1_000)),
    );
    let amount = (offset..offset + len)
        .map(|row| {
            if row % 17 == 0 {
                None
            } else {
                Some(((row % 1_000_000) as f64 + 25.0) / 100.0)
            }
        })
        .collect::<Float64Array>();
    let status = (offset..offset + len)
        .map(|row| {
            if row % 19 == 0 {
                None
            } else {
                Some(status_values[row % status_values.len()])
            }
        })
        .collect::<StringArray>();
    let region = (offset..offset + len)
        .map(|row| {
            if row % 23 == 0 {
                None
            } else {
                Some(region_values[(row / 3) % region_values.len()])
            }
        })
        .collect::<StringArray>();
    let description = (offset..offset + len)
        .map(|row| {
            if row % 29 == 0 {
                None
            } else {
                Some(deterministic_description(row))
            }
        })
        .collect::<StringArray>();
    let payload = (offset..offset + len)
        .map(|row| {
            if row % 31 == 0 {
                None
            } else {
                Some(deterministic_payload(row))
            }
        })
        .collect::<BinaryArray>();

    Ok(vec![
        Arc::new(id),
        Arc::new(account_id),
        Arc::new(event_time_ms),
        Arc::new(amount),
        Arc::new(status),
        Arc::new(region),
        Arc::new(description),
        Arc::new(payload),
    ])
}

fn deterministic_description(row: usize) -> String {
    let words = [
        "batch",
        "transfer",
        "ledger",
        "route",
        "validated",
        "checkpoint",
    ];
    let repeats = 1 + row % 7;
    let mut value = format!("event-{row:012}");

    for index in 0..repeats {
        value.push('-');
        value.push_str(words[(row + index) % words.len()]);
    }

    value
}

fn deterministic_payload(row: usize) -> Vec<u8> {
    let len = 8 + row % 57;
    (0..len)
        .map(|index| ((row.wrapping_mul(31) + index.wrapping_mul(17)) % 251) as u8)
        .collect()
}

fn decimal_temporal_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("transaction_id", DataType::Int64, false),
        Field::new("account_id", DataType::Int32, false),
        Field::new("amount", DataType::Decimal128(18, 4), false),
        Field::new("fee", DataType::Decimal128(12, 4), true),
        Field::new("trade_date", DataType::Date32, false),
        Field::new(
            "posted_at_ms",
            DataType::Timestamp(TimeUnit::Millisecond, None),
            false,
        ),
        Field::new("approved", DataType::Boolean, true),
    ]))
}

fn decimal_temporal_columns(offset: usize, len: usize) -> Result<Vec<ArrayRef>, WriterBenchError> {
    let transaction_id = (offset..offset + len)
        .map(|row| 500_000_000_i64 + row as i64)
        .collect::<Int64Array>();
    let account_id = (offset..offset + len)
        .map(|row| 10_000 + (row % 200_000) as i32)
        .collect::<Int32Array>();
    let amount = decimal128_array(
        (offset..offset + len).map(|row| {
            let sign = if row % 13 == 0 { -1_i128 } else { 1_i128 };
            Some(sign * (1_000_000_i128 + (row % 50_000_000) as i128))
        }),
        18,
        4,
    )?;
    let fee = decimal128_array(
        (offset..offset + len).map(|row| {
            if row % 11 == 0 {
                None
            } else {
                Some(25_i128 + (row % 10_000) as i128)
            }
        }),
        12,
        4,
    )?;
    let trade_date = Date32Array::from_iter_values(
        (offset..offset + len).map(|row| 19_723_i32 + (row % 365) as i32),
    );
    let posted_at_ms = TimestampMillisecondArray::from_iter_values(
        (offset..offset + len).map(|row| 1_735_689_600_000_i64 + (row as i64 * 17_000)),
    );
    let approved = (offset..offset + len)
        .map(|row| {
            if row % 17 == 0 {
                None
            } else {
                Some(row % 3 != 0)
            }
        })
        .collect::<BooleanArray>();

    Ok(vec![
        Arc::new(transaction_id),
        Arc::new(account_id),
        Arc::new(amount),
        Arc::new(fee),
        Arc::new(trade_date),
        Arc::new(posted_at_ms),
        Arc::new(approved),
    ])
}

fn string_heavy_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("tenant", DataType::Utf8, false),
        Field::new("document_type", DataType::Utf8, true),
        Field::new("title", DataType::Utf8, true),
        Field::new("body", DataType::Utf8, true),
        Field::new("metadata", DataType::Utf8, true),
        Field::new("payload", DataType::Binary, true),
    ]))
}

fn string_heavy_columns(offset: usize, len: usize) -> Result<Vec<ArrayRef>, WriterBenchError> {
    let document_types = ["invoice", "event", "profile", "message", "audit"];
    let id = (offset..offset + len)
        .map(|row| 900_000_000_i64 + row as i64)
        .collect::<Int64Array>();
    let tenant = (offset..offset + len)
        .map(|row| Some(format!("tenant-{:04}", row % 512)))
        .collect::<StringArray>();
    let document_type = (offset..offset + len)
        .map(|row| {
            if row % 37 == 0 {
                None
            } else {
                Some(document_types[row % document_types.len()].to_owned())
            }
        })
        .collect::<StringArray>();
    let title = (offset..offset + len)
        .map(|row| {
            if row % 41 == 0 {
                None
            } else {
                Some(format!("document title {row:012}"))
            }
        })
        .collect::<StringArray>();
    let body = (offset..offset + len)
        .map(|row| {
            if row % 43 == 0 {
                None
            } else {
                Some(deterministic_text(row, 512 + row % 2_048))
            }
        })
        .collect::<StringArray>();
    let metadata = (offset..offset + len)
        .map(|row| {
            if row % 47 == 0 {
                None
            } else {
                Some(format!(
                    "{{\"tenant\":{},\"source\":{},\"sequence\":{row}}}",
                    row % 512,
                    row % 17
                ))
            }
        })
        .collect::<StringArray>();
    let payload = (offset..offset + len)
        .map(|row| {
            if row % 53 == 0 {
                None
            } else {
                Some(deterministic_payload_with_len(row, 1_024 + row % 4_096))
            }
        })
        .collect::<BinaryArray>();

    Ok(vec![
        Arc::new(id),
        Arc::new(tenant),
        Arc::new(document_type),
        Arc::new(title),
        Arc::new(body),
        Arc::new(metadata),
        Arc::new(payload),
    ])
}

fn wide_sparse_schema() -> SchemaRef {
    let mut fields = Vec::with_capacity(32);

    for index in 0..8 {
        fields.push(Field::new(
            format!("metric_i32_{index:02}"),
            DataType::Int32,
            index != 0,
        ));
    }
    for index in 0..8 {
        fields.push(Field::new(
            format!("metric_f64_{index:02}"),
            DataType::Float64,
            true,
        ));
    }
    for index in 0..8 {
        fields.push(Field::new(format!("tag_{index:02}"), DataType::Utf8, true));
    }
    for index in 0..8 {
        fields.push(Field::new(
            format!("flag_{index:02}"),
            DataType::Boolean,
            true,
        ));
    }

    Arc::new(Schema::new(fields))
}

fn wide_sparse_columns(offset: usize, len: usize) -> Result<Vec<ArrayRef>, WriterBenchError> {
    let mut columns = Vec::with_capacity(32);

    for column in 0..8 {
        columns.push(Arc::new(wide_sparse_i32_column(offset, len, column)) as ArrayRef);
    }
    for column in 0..8 {
        columns.push(Arc::new(wide_sparse_f64_column(offset, len, column)) as ArrayRef);
    }
    for column in 0..8 {
        columns.push(Arc::new(wide_sparse_string_column(offset, len, column)) as ArrayRef);
    }
    for column in 0..8 {
        columns.push(Arc::new(wide_sparse_bool_column(offset, len, column)) as ArrayRef);
    }

    Ok(columns)
}

fn tpch_lineitem_like_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("l_orderkey", DataType::Int64, false),
        Field::new("l_partkey", DataType::Int64, false),
        Field::new("l_suppkey", DataType::Int64, false),
        Field::new("l_linenumber", DataType::Int32, false),
        Field::new("l_quantity", DataType::Decimal128(15, 2), false),
        Field::new("l_extendedprice", DataType::Decimal128(18, 2), false),
        Field::new("l_discount", DataType::Decimal128(10, 4), false),
        Field::new("l_tax", DataType::Decimal128(10, 4), false),
        Field::new("l_returnflag", DataType::Utf8, false),
        Field::new("l_linestatus", DataType::Utf8, false),
        Field::new("l_shipdate", DataType::Date32, false),
        Field::new("l_commitdate", DataType::Date32, false),
        Field::new("l_receiptdate", DataType::Date32, false),
        Field::new("l_shipinstruct", DataType::Utf8, false),
        Field::new("l_shipmode", DataType::Utf8, false),
        Field::new("l_comment", DataType::Utf8, true),
    ]))
}

fn tpch_lineitem_like_columns(
    offset: usize,
    len: usize,
) -> Result<Vec<ArrayRef>, WriterBenchError> {
    let return_flags = ["A", "N", "R"];
    let line_statuses = ["F", "O"];
    let ship_instructs = [
        "DELIVER IN PERSON",
        "COLLECT COD",
        "NONE",
        "TAKE BACK RETURN",
    ];
    let ship_modes = ["AIR", "FOB", "MAIL", "RAIL", "REG AIR", "SHIP", "TRUCK"];

    let orderkey = (offset..offset + len)
        .map(|row| 1_i64 + (row / 4) as i64)
        .collect::<Int64Array>();
    let partkey = (offset..offset + len)
        .map(|row| 1_i64 + (row % 200_000) as i64)
        .collect::<Int64Array>();
    let suppkey = (offset..offset + len)
        .map(|row| 1_i64 + (row % 10_000) as i64)
        .collect::<Int64Array>();
    let linenumber = (offset..offset + len)
        .map(|row| 1 + (row % 7) as i32)
        .collect::<Int32Array>();
    let quantity = decimal128_array(
        (offset..offset + len).map(|row| Some(100_i128 + (row % 5_000) as i128)),
        15,
        2,
    )?;
    let extendedprice = decimal128_array(
        (offset..offset + len).map(|row| Some(10_000_i128 + (row % 900_000) as i128)),
        18,
        2,
    )?;
    let discount = decimal128_array(
        (offset..offset + len).map(|row| Some((row % 1_000) as i128)),
        10,
        4,
    )?;
    let tax = decimal128_array(
        (offset..offset + len).map(|row| Some((row % 800) as i128)),
        10,
        4,
    )?;
    let returnflag = (offset..offset + len)
        .map(|row| Some(return_flags[row % return_flags.len()]))
        .collect::<StringArray>();
    let linestatus = (offset..offset + len)
        .map(|row| Some(line_statuses[(row / 5) % line_statuses.len()]))
        .collect::<StringArray>();
    let shipdate = Date32Array::from_iter_values(
        (offset..offset + len).map(|row| 8_400_i32 + (row % 2_500) as i32),
    );
    let commitdate = Date32Array::from_iter_values(
        (offset..offset + len).map(|row| 8_430_i32 + (row % 2_500) as i32),
    );
    let receiptdate = Date32Array::from_iter_values(
        (offset..offset + len).map(|row| 8_460_i32 + (row % 2_500) as i32),
    );
    let shipinstruct = (offset..offset + len)
        .map(|row| Some(ship_instructs[row % ship_instructs.len()]))
        .collect::<StringArray>();
    let shipmode = (offset..offset + len)
        .map(|row| Some(ship_modes[row % ship_modes.len()]))
        .collect::<StringArray>();
    let comment = (offset..offset + len)
        .map(|row| {
            if row % 101 == 0 {
                None
            } else {
                Some(deterministic_text(row, 24 + row % 96))
            }
        })
        .collect::<StringArray>();

    Ok(vec![
        Arc::new(orderkey),
        Arc::new(partkey),
        Arc::new(suppkey),
        Arc::new(linenumber),
        Arc::new(quantity),
        Arc::new(extendedprice),
        Arc::new(discount),
        Arc::new(tax),
        Arc::new(returnflag),
        Arc::new(linestatus),
        Arc::new(shipdate),
        Arc::new(commitdate),
        Arc::new(receiptdate),
        Arc::new(shipinstruct),
        Arc::new(shipmode),
        Arc::new(comment),
    ])
}

fn decimal128_array(
    values: impl IntoIterator<Item = Option<i128>>,
    precision: u8,
    scale: i8,
) -> Result<Decimal128Array, WriterBenchError> {
    Decimal128Array::from(values.into_iter().collect::<Vec<_>>())
        .with_precision_and_scale(precision, scale)
        .map_err(WriterBenchError::Arrow)
}

fn wide_sparse_i32_column(offset: usize, len: usize, column: usize) -> Int32Array {
    (offset..offset + len)
        .map(|row| {
            if column != 0 && row % (column + 11) == 0 {
                None
            } else {
                Some((row.wrapping_mul(column + 3) % 1_000_000) as i32)
            }
        })
        .collect()
}

fn wide_sparse_f64_column(offset: usize, len: usize, column: usize) -> Float64Array {
    (offset..offset + len)
        .map(|row| {
            if row % (column + 13) == 0 {
                None
            } else {
                Some((row.wrapping_mul(column + 19) % 10_000) as f64 / 10.0)
            }
        })
        .collect()
}

fn wide_sparse_string_column(offset: usize, len: usize, column: usize) -> StringArray {
    (offset..offset + len)
        .map(|row| {
            if row % (column + 17) == 0 {
                None
            } else {
                Some(format!("tag-{column:02}-{}", row % 128))
            }
        })
        .collect()
}

fn wide_sparse_bool_column(offset: usize, len: usize, column: usize) -> BooleanArray {
    (offset..offset + len)
        .map(|row| {
            if row % (column + 23) == 0 {
                None
            } else {
                Some((row + column).is_multiple_of(2))
            }
        })
        .collect()
}

fn deterministic_text(row: usize, len: usize) -> String {
    const ALPHABET: &[u8] = b"abcdefghijklmnopqrstuvwxyz0123456789 ";
    let mut value = String::with_capacity(len);

    for index in 0..len {
        let byte = ALPHABET[(row.wrapping_mul(31) + index.wrapping_mul(7)) % ALPHABET.len()];
        value.push(char::from(byte));
    }

    value
}

fn deterministic_payload_with_len(row: usize, len: usize) -> Vec<u8> {
    (0..len)
        .map(|index| ((row.wrapping_mul(131) + index.wrapping_mul(29)) % 251) as u8)
        .collect()
}

fn deterministic_i32(row: usize) -> i32 {
    let mixed = row.wrapping_mul(1_103_515_245).wrapping_add(12_345);
    (mixed % 1_000_000) as i32
}

fn deterministic_score(row: usize) -> f64 {
    let bucket = row.wrapping_mul(37).wrapping_add(17) % 100_000;
    bucket as f64 / 100.0
}

fn parse_positive_usize(option: &'static str, value: &str) -> Result<usize, WriterBenchError> {
    let parsed = value
        .parse::<usize>()
        .map_err(|_| WriterBenchError::InvalidPositiveInteger {
            option,
            value: value.to_owned(),
        })?;

    if parsed == 0 {
        return Err(WriterBenchError::InvalidPositiveInteger {
            option,
            value: value.to_owned(),
        });
    }

    Ok(parsed)
}

fn required_value(args: &[OsString], index: usize) -> Result<String, WriterBenchError> {
    let value = args
        .get(index + 1)
        .ok_or_else(|| WriterBenchError::MissingOptionValue(option_name(args, index)))?;

    value
        .to_str()
        .map(ToOwned::to_owned)
        .ok_or_else(|| WriterBenchError::InvalidUtf8Argument(value.clone()))
}

fn option_name(args: &[OsString], index: usize) -> String {
    args.get(index)
        .and_then(|arg| arg.to_str())
        .unwrap_or("<unknown>")
        .to_owned()
}

#[derive(Debug)]
pub(super) enum WriterBenchError {
    UnknownCommand(String),
    UnknownOption(String),
    MissingOptionValue(String),
    InvalidUtf8Argument(OsString),
    InvalidPositiveInteger { option: &'static str, value: String },
    InvalidScenario(String),
    InvalidOutput(String),
    Arrow(arrow_schema::ArrowError),
    ArrowTiberius(arrow_tiberius::Error),
    Tiberius(tiberius::error::Error),
    SqlServer(sqlserver::SqlServerError),
    OdbcRunner(odbc_runner::OdbcRunnerError),
    Io(std::io::Error),
    Validation(String),
    RowCountMismatch { expected: u64, actual: u64 },
    UnsupportedArrowOdbcScenario { scenario: String },
    ArrowOdbcUnavailable { reason: String },
}

impl fmt::Display for WriterBenchError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnknownCommand(command) => write!(f, "unknown writer-bench command `{command}`"),
            Self::UnknownOption(option) => write!(f, "unknown writer-bench option `{option}`"),
            Self::MissingOptionValue(option) => write!(f, "missing value for `{option}`"),
            Self::InvalidUtf8Argument(arg) => write!(f, "argument is not valid UTF-8: {arg:?}"),
            Self::InvalidPositiveInteger { option, value } => {
                write!(f, "{option} must be a positive integer, got `{value}`")
            }
            Self::InvalidScenario(value) => {
                write!(
                    f,
                    "unknown writer-bench scenario `{value}`; expected one of: {}",
                    scenario_names()
                )
            }
            Self::InvalidOutput(value) => {
                write!(f, "unknown writer-bench output `{value}`; expected human")
            }
            Self::Arrow(source) => write!(f, "failed to generate Arrow benchmark data: {source}"),
            Self::ArrowTiberius(source) => write!(f, "arrow-tiberius benchmark failed: {source}"),
            Self::Tiberius(source) => write!(f, "SQL Server benchmark operation failed: {source}"),
            Self::SqlServer(source) => write!(f, "{source}"),
            Self::OdbcRunner(source) => write!(f, "{source}"),
            Self::Io(source) => write!(f, "benchmark I/O failed: {source}"),
            Self::Validation(reason) => write!(f, "benchmark validation failed: {reason}"),
            Self::RowCountMismatch { expected, actual } => write!(
                f,
                "benchmark row-count validation failed: expected {expected}, got {actual}"
            ),
            Self::UnsupportedArrowOdbcScenario { scenario } => write!(
                f,
                "arrow-odbc benchmark scenario `{scenario}` is not supported yet; expected narrow_numeric or mixed_nullable"
            ),
            Self::ArrowOdbcUnavailable { reason } => {
                write!(f, "arrow-odbc benchmark is unavailable: {reason}")
            }
        }
    }
}

fn scenario_names() -> String {
    SCENARIOS
        .iter()
        .map(|scenario| scenario.name)
        .collect::<Vec<_>>()
        .join(", ")
}

#[cfg(test)]
mod tests {
    use super::{BenchmarkOutput, WriterBenchError, WriterBenchOptions};
    use arrow_array::{
        Array, BinaryArray, BooleanArray, Date32Array, Decimal128Array, Float64Array, Int32Array,
        Int64Array, RecordBatch, StringArray, TimestampMillisecondArray,
    };
    use arrow_schema::{DataType, TimeUnit};
    use std::ffi::OsString;
    use std::path::PathBuf;

    #[test]
    fn parses_writer_bench_defaults() {
        let options = WriterBenchOptions::parse(&[]).unwrap();

        assert_eq!(options.rows, 100_000);
        assert_eq!(options.batch_size, 8_192);
        assert_eq!(options.scenario.name, "narrow_numeric");
        assert_eq!(options.repeat, 1);
        assert_eq!(options.output, BenchmarkOutput::Human);
    }

    #[test]
    fn parses_writer_bench_options() {
        let args = [
            OsString::from("--rows"),
            OsString::from("17"),
            OsString::from("--batch-size"),
            OsString::from("4"),
            OsString::from("--scenario"),
            OsString::from("mixed_nullable"),
            OsString::from("--repeat"),
            OsString::from("3"),
            OsString::from("--output"),
            OsString::from("human"),
        ];

        let options = WriterBenchOptions::parse(&args).unwrap();

        assert_eq!(options.rows, 17);
        assert_eq!(options.batch_size, 4);
        assert_eq!(options.scenario.name, "mixed_nullable");
        assert_eq!(options.repeat, 3);
        assert_eq!(options.output, BenchmarkOutput::Human);
    }

    #[test]
    fn rejects_zero_rows() {
        let args = [OsString::from("--rows"), OsString::from("0")];
        let err = WriterBenchOptions::parse(&args).unwrap_err();

        assert!(matches!(
            err,
            WriterBenchError::InvalidPositiveInteger {
                option: "--rows",
                value
            } if value == "0"
        ));
    }

    #[test]
    fn rejects_invalid_batch_size() {
        let args = [OsString::from("--batch-size"), OsString::from("nope")];
        let err = WriterBenchOptions::parse(&args).unwrap_err();

        assert!(matches!(
            err,
            WriterBenchError::InvalidPositiveInteger {
                option: "--batch-size",
                value
            } if value == "nope"
        ));
    }

    #[test]
    fn rejects_unknown_scenario() {
        let args = [OsString::from("--scenario"), OsString::from("tpch")];
        let err = WriterBenchOptions::parse(&args).unwrap_err();

        assert!(matches!(err, WriterBenchError::InvalidScenario(value) if value == "tpch"));
    }

    #[test]
    fn rejects_unknown_output() {
        let args = [OsString::from("--output"), OsString::from("json")];
        let err = WriterBenchOptions::parse(&args).unwrap_err();

        assert!(matches!(err, WriterBenchError::InvalidOutput(value) if value == "json"));
    }

    #[test]
    fn parses_baseline_command_with_shared_generation_options() {
        let args = [
            OsString::from("--rows"),
            OsString::from("10"),
            OsString::from("--batch-size"),
            OsString::from("4"),
            OsString::from("--scenario"),
            OsString::from("mixed_nullable"),
            OsString::from("--connection-string"),
            OsString::from("server=tcp:127.0.0.1,1433;password=secret"),
            OsString::from("--database"),
            OsString::from("bench_db"),
        ];

        let options = super::BaselineBenchOptions::parse(&args).unwrap();

        assert_eq!(options.benchmark.rows, 10);
        assert_eq!(options.benchmark.batch_size, 4);
        assert_eq!(options.benchmark.scenario.name, "mixed_nullable");
        assert_eq!(options.sql_server.database, "bench_db");
        assert!(options.sql_server.connection_string.is_some());
    }

    #[test]
    fn parses_arrow_odbc_runner_image_build_options() {
        let args = [
            OsString::from("--build-runner-image"),
            OsString::from("--runner-image"),
            OsString::from("custom-arrow-odbc-runner:test"),
            OsString::from("--container-runtime"),
            OsString::from("podman"),
        ];

        let options = super::ArrowOdbcBenchOptions::parse(&args).unwrap();

        assert!(options.build_runner_image);
        assert_eq!(options.runner_image, "custom-arrow-odbc-runner:test");
        assert_eq!(
            options.sql_server.container_runtime,
            Some(PathBuf::from("podman"))
        );
    }

    #[test]
    fn parses_baseline_sql_server_options_without_leaking_connection_string() {
        let args = [
            OsString::from("--rows"),
            OsString::from("17"),
            OsString::from("--container-runtime"),
            OsString::from("podman"),
            OsString::from("--connection-string"),
            OsString::from("server=tcp:127.0.0.1,1433;password=secret"),
            OsString::from("--image"),
            OsString::from("custom-sqlserver"),
            OsString::from("--database"),
            OsString::from("bench_db"),
            OsString::from("--keep-container"),
        ];

        let options = super::BaselineBenchOptions::parse(&args).unwrap();

        assert_eq!(options.benchmark.rows, 17);
        assert_eq!(
            options.sql_server.container_runtime,
            Some(PathBuf::from("podman"))
        );
        assert_eq!(
            options.sql_server.connection_string.as_deref(),
            Some("server=tcp:127.0.0.1,1433;password=secret")
        );
        assert_eq!(options.sql_server.image, "custom-sqlserver");
        assert_eq!(options.sql_server.database, "bench_db");
        assert!(options.sql_server.keep_container);
    }

    #[test]
    fn renders_narrow_numeric_benchmark_table_ddl() {
        let table = arrow_tiberius::TableName::new("dbo", "bench").unwrap();
        let schema = (super::scenario_by_name("narrow_numeric").unwrap().schema)();
        let mappings = super::benchmark_mappings_for_schema(schema).unwrap();
        let sql = super::benchmark_table_sql(&table, &mappings);

        assert_eq!(
            sql,
            "CREATE TABLE [dbo].[bench] (\n    [id32] int NOT NULL,\n    [id64] bigint NOT NULL,\n    [score] float(53) NOT NULL\n);"
        );
    }

    #[test]
    fn renders_mixed_nullable_benchmark_table_ddl() {
        let table = arrow_tiberius::TableName::new("dbo", "bench").unwrap();
        let schema = (super::scenario_by_name("mixed_nullable").unwrap().schema)();
        let mappings = super::benchmark_mappings_for_schema(schema).unwrap();
        let sql = super::benchmark_table_sql(&table, &mappings);

        assert_eq!(
            sql,
            "CREATE TABLE [dbo].[bench] (\n    [id32] int NOT NULL,\n    [maybe_id64] bigint NULL,\n    [maybe_score] float(53) NULL,\n    [category] nvarchar(max) NULL\n);"
        );
    }

    #[test]
    fn formats_rows_per_second_for_report_output() {
        assert_eq!(
            super::format_rows_per_second(2_500, std::time::Duration::from_millis(500)),
            "5000.00"
        );
    }

    #[test]
    fn formats_zero_elapsed_rows_per_second_without_panicking() {
        assert_eq!(
            super::format_rows_per_second(2_500, std::time::Duration::ZERO),
            "n/a"
        );
    }

    #[test]
    fn rejects_missing_baseline_sql_server_option_value() {
        let args = [OsString::from("--connection-string")];
        let err = super::BaselineBenchOptions::parse(&args).unwrap_err();

        assert!(matches!(
            err,
            WriterBenchError::MissingOptionValue(option) if option == "--connection-string"
        ));
    }

    #[test]
    fn baseline_help_does_not_parse_generation_options() {
        let args = [OsString::from("baseline"), OsString::from("--help")];

        super::run(&args).unwrap();
    }

    #[test]
    fn rejects_unknown_writer_bench_command() {
        let args = [OsString::from("direct")];
        let err = super::run(&args).unwrap_err();

        assert!(matches!(err, WriterBenchError::UnknownCommand(command) if command == "direct"));
    }

    #[test]
    fn parses_arrow_odbc_command_with_shared_generation_options() {
        let args = [
            OsString::from("--rows"),
            OsString::from("25"),
            OsString::from("--batch-size"),
            OsString::from("5"),
            OsString::from("--scenario"),
            OsString::from("mixed_nullable"),
            OsString::from("--connection-string"),
            OsString::from("server=tcp:127.0.0.1,1433;password=secret"),
            OsString::from("--database"),
            OsString::from("bench_db"),
        ];

        let options = super::ArrowOdbcBenchOptions::parse(&args).unwrap();

        assert_eq!(options.benchmark.rows, 25);
        assert_eq!(options.benchmark.batch_size, 5);
        assert_eq!(options.benchmark.scenario.name, "mixed_nullable");
        assert_eq!(options.sql_server.database, "bench_db");
        assert!(options.sql_server.connection_string.is_some());
        assert!(!options.build_runner_image);
        assert_eq!(
            options.runner_image,
            crate::odbc_runner::DEFAULT_RUNNER_IMAGE_TAG
        );
    }

    #[test]
    fn arrow_odbc_command_rejects_non_overlap_scenarios() {
        let args = [
            OsString::from("--scenario"),
            OsString::from("decimal_temporal"),
        ];

        let err = super::ArrowOdbcBenchOptions::parse(&args).unwrap_err();

        assert!(matches!(
            err,
            WriterBenchError::UnsupportedArrowOdbcScenario { scenario }
                if scenario == "decimal_temporal"
        ));
    }

    #[test]
    fn arrow_odbc_command_reports_disabled_runner_before_odbc_dependency_exists() {
        let args = [
            OsString::from("arrow-odbc"),
            OsString::from("--rows"),
            OsString::from("10"),
            OsString::from("--scenario"),
            OsString::from("narrow_numeric"),
        ];

        let err = super::run(&args).unwrap_err();

        assert!(
            matches!(err, WriterBenchError::ArrowOdbcUnavailable { reason } if reason.contains("runner container"))
        );
    }

    #[test]
    fn arrow_odbc_help_is_command_specific() {
        let args = [OsString::from("arrow-odbc"), OsString::from("--help")];

        super::run(&args).unwrap();
    }

    #[test]
    fn generates_requested_row_count_across_batches() {
        let options = WriterBenchOptions {
            rows: 10,
            batch_size: 4,
            scenario: super::scenario_by_name("narrow_numeric").unwrap(),
            repeat: 1,
            output: BenchmarkOutput::Human,
        };

        let batches = generated_batches(&options);

        assert_eq!(batches.len(), 3);
        assert_eq!(batches[0].num_rows(), 4);
        assert_eq!(batches[1].num_rows(), 4);
        assert_eq!(batches[2].num_rows(), 2);
        assert_eq!(
            batches.iter().map(|batch| batch.num_rows()).sum::<usize>(),
            10
        );
    }

    #[test]
    fn narrow_numeric_schema_matches_definition() {
        let options = WriterBenchOptions {
            rows: 1,
            batch_size: 1,
            scenario: super::scenario_by_name("narrow_numeric").unwrap(),
            repeat: 1,
            output: BenchmarkOutput::Human,
        };

        let batches = generated_batches(&options);
        let schema = batches[0].schema();

        assert_eq!(schema.field(0).name(), "id32");
        assert_eq!(schema.field(0).data_type(), &DataType::Int32);
        assert!(!schema.field(0).is_nullable());
        assert_eq!(schema.field(1).name(), "id64");
        assert_eq!(schema.field(1).data_type(), &DataType::Int64);
        assert!(!schema.field(1).is_nullable());
        assert_eq!(schema.field(2).name(), "score");
        assert_eq!(schema.field(2).data_type(), &DataType::Float64);
        assert!(!schema.field(2).is_nullable());
    }

    #[test]
    fn mixed_nullable_schema_matches_definition() {
        let options = WriterBenchOptions {
            rows: 1,
            batch_size: 1,
            scenario: super::scenario_by_name("mixed_nullable").unwrap(),
            repeat: 1,
            output: BenchmarkOutput::Human,
        };

        let batches = generated_batches(&options);
        let schema = batches[0].schema();

        assert_eq!(schema.field(0).data_type(), &DataType::Int32);
        assert!(!schema.field(0).is_nullable());
        assert_eq!(schema.field(1).data_type(), &DataType::Int64);
        assert!(schema.field(1).is_nullable());
        assert_eq!(schema.field(2).data_type(), &DataType::Float64);
        assert!(schema.field(2).is_nullable());
        assert_eq!(schema.field(3).data_type(), &DataType::Utf8);
        assert!(schema.field(3).is_nullable());
    }

    #[test]
    fn nullable_scenario_contains_nulls() {
        let options = WriterBenchOptions {
            rows: 32,
            batch_size: 32,
            scenario: super::scenario_by_name("mixed_nullable").unwrap(),
            repeat: 1,
            output: BenchmarkOutput::Human,
        };

        let batches = generated_batches(&options);
        let batch = &batches[0];

        assert!(batch.column(1).null_count() > 0);
        assert!(batch.column(2).null_count() > 0);
        assert!(batch.column(3).null_count() > 0);
    }

    #[test]
    fn generated_values_are_deterministic() {
        let options = WriterBenchOptions {
            rows: 17,
            batch_size: 5,
            scenario: super::scenario_by_name("mixed_nullable").unwrap(),
            repeat: 1,
            output: BenchmarkOutput::Human,
        };

        let first = generated_batches(&options);
        let second = generated_batches(&options);

        assert_eq!(first, second);
    }

    #[test]
    fn generated_values_continue_across_batch_boundaries() {
        let options = WriterBenchOptions {
            rows: 6,
            batch_size: 4,
            scenario: super::scenario_by_name("narrow_numeric").unwrap(),
            repeat: 1,
            output: BenchmarkOutput::Human,
        };

        let batches = generated_batches(&options);
        let first_batch_id32 = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        let second_batch_id32 = batches[1]
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();

        assert_eq!(first_batch_id32.value(3), super::deterministic_i32(3));
        assert_eq!(second_batch_id32.value(0), super::deterministic_i32(4));
        assert_ne!(first_batch_id32.value(3), second_batch_id32.value(0));
    }

    #[test]
    fn generated_columns_have_expected_runtime_array_types() {
        let options = WriterBenchOptions {
            rows: 3,
            batch_size: 3,
            scenario: super::scenario_by_name("mixed_nullable").unwrap(),
            repeat: 1,
            output: BenchmarkOutput::Human,
        };

        let batches = generated_batches(&options);
        let batch = &batches[0];

        assert!(batch.column(0).as_any().is::<Int32Array>());
        assert!(batch.column(1).as_any().is::<Int64Array>());
        assert!(batch.column(2).as_any().is::<Float64Array>());
        assert!(batch.column(3).as_any().is::<StringArray>());
    }

    #[test]
    fn wide_mixed_schema_matches_definition() {
        let options = WriterBenchOptions {
            rows: 1,
            batch_size: 1,
            scenario: super::scenario_by_name("wide_mixed").unwrap(),
            repeat: 1,
            output: BenchmarkOutput::Human,
        };

        let batches = generated_batches(&options);
        let schema = batches[0].schema();

        assert_eq!(schema.fields().len(), 8);
        assert_eq!(schema.field(0).name(), "id");
        assert_eq!(schema.field(0).data_type(), &DataType::Int64);
        assert!(!schema.field(0).is_nullable());
        assert_eq!(schema.field(1).name(), "account_id");
        assert_eq!(schema.field(1).data_type(), &DataType::Int32);
        assert!(!schema.field(1).is_nullable());
        assert_eq!(schema.field(2).name(), "event_time_ms");
        assert_eq!(
            schema.field(2).data_type(),
            &DataType::Timestamp(TimeUnit::Millisecond, None)
        );
        assert!(!schema.field(2).is_nullable());
        assert_eq!(schema.field(3).name(), "amount");
        assert_eq!(schema.field(3).data_type(), &DataType::Float64);
        assert!(schema.field(3).is_nullable());
        assert_eq!(schema.field(4).name(), "status");
        assert_eq!(schema.field(4).data_type(), &DataType::Utf8);
        assert!(schema.field(4).is_nullable());
        assert_eq!(schema.field(5).name(), "region");
        assert_eq!(schema.field(5).data_type(), &DataType::Utf8);
        assert!(schema.field(5).is_nullable());
        assert_eq!(schema.field(6).name(), "description");
        assert_eq!(schema.field(6).data_type(), &DataType::Utf8);
        assert!(schema.field(6).is_nullable());
        assert_eq!(schema.field(7).name(), "payload");
        assert_eq!(schema.field(7).data_type(), &DataType::Binary);
        assert!(schema.field(7).is_nullable());
    }

    #[test]
    fn wide_mixed_contains_variable_text_binary_and_nulls() {
        let options = WriterBenchOptions {
            rows: 128,
            batch_size: 128,
            scenario: super::scenario_by_name("wide_mixed").unwrap(),
            repeat: 1,
            output: BenchmarkOutput::Human,
        };

        let batches = generated_batches(&options);
        let batch = &batches[0];
        let description = batch
            .column(6)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let payload = batch
            .column(7)
            .as_any()
            .downcast_ref::<BinaryArray>()
            .unwrap();

        assert!(batch.column(3).null_count() > 0);
        assert!(batch.column(4).null_count() > 0);
        assert!(batch.column(5).null_count() > 0);
        assert!(description.null_count() > 0);
        assert!(payload.null_count() > 0);
        assert_ne!(description.value(1).len(), description.value(2).len());
        assert_ne!(payload.value(1).len(), payload.value(2).len());
    }

    #[test]
    fn wide_mixed_runtime_array_types_match_schema() {
        let options = WriterBenchOptions {
            rows: 3,
            batch_size: 3,
            scenario: super::scenario_by_name("wide_mixed").unwrap(),
            repeat: 1,
            output: BenchmarkOutput::Human,
        };

        let batches = generated_batches(&options);
        let batch = &batches[0];

        assert!(batch.column(0).as_any().is::<Int64Array>());
        assert!(batch.column(1).as_any().is::<Int32Array>());
        assert!(batch.column(2).as_any().is::<TimestampMillisecondArray>());
        assert!(batch.column(3).as_any().is::<Float64Array>());
        assert!(batch.column(4).as_any().is::<StringArray>());
        assert!(batch.column(5).as_any().is::<StringArray>());
        assert!(batch.column(6).as_any().is::<StringArray>());
        assert!(batch.column(7).as_any().is::<BinaryArray>());
    }

    #[test]
    fn wide_mixed_is_deterministic_across_readers() {
        let options = WriterBenchOptions {
            rows: 257,
            batch_size: 31,
            scenario: super::scenario_by_name("wide_mixed").unwrap(),
            repeat: 1,
            output: BenchmarkOutput::Human,
        };

        let first = generated_batches(&options);
        let second = generated_batches(&options);

        assert_eq!(first, second);
    }

    #[test]
    fn decimal_temporal_covers_finance_and_time_types() {
        let options = WriterBenchOptions {
            rows: 64,
            batch_size: 64,
            scenario: super::scenario_by_name("decimal_temporal").unwrap(),
            repeat: 1,
            output: BenchmarkOutput::Human,
        };

        let batches = generated_batches(&options);
        let batch = &batches[0];
        let schema = batch.schema();

        assert_eq!(schema.field(2).data_type(), &DataType::Decimal128(18, 4));
        assert_eq!(schema.field(3).data_type(), &DataType::Decimal128(12, 4));
        assert_eq!(schema.field(4).data_type(), &DataType::Date32);
        assert_eq!(
            schema.field(5).data_type(),
            &DataType::Timestamp(TimeUnit::Millisecond, None)
        );
        assert_eq!(schema.field(6).data_type(), &DataType::Boolean);
        assert!(batch.column(2).as_any().is::<Decimal128Array>());
        assert!(batch.column(3).as_any().is::<Decimal128Array>());
        assert!(batch.column(4).as_any().is::<Date32Array>());
        assert!(batch.column(5).as_any().is::<TimestampMillisecondArray>());
        assert!(batch.column(6).as_any().is::<BooleanArray>());
        assert!(batch.column(3).null_count() > 0);
        assert!(batch.column(6).null_count() > 0);
    }

    #[test]
    fn string_heavy_has_kb_scale_variable_payloads() {
        let options = WriterBenchOptions {
            rows: 128,
            batch_size: 128,
            scenario: super::scenario_by_name("string_heavy").unwrap(),
            repeat: 1,
            output: BenchmarkOutput::Human,
        };

        let batches = generated_batches(&options);
        let batch = &batches[0];
        let body = batch
            .column(4)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let payload = batch
            .column(6)
            .as_any()
            .downcast_ref::<BinaryArray>()
            .unwrap();

        assert!(body.null_count() > 0);
        assert!(payload.null_count() > 0);
        assert!(body.value(1).len() >= 512);
        assert!(payload.value(1).len() >= 1_024);
        assert_ne!(body.value(1).len(), body.value(2).len());
        assert_ne!(payload.value(1).len(), payload.value(2).len());
    }

    #[test]
    fn wide_sparse_has_many_columns_and_sparse_nulls() {
        let options = WriterBenchOptions {
            rows: 256,
            batch_size: 256,
            scenario: super::scenario_by_name("wide_sparse").unwrap(),
            repeat: 1,
            output: BenchmarkOutput::Human,
        };

        let batches = generated_batches(&options);
        let batch = &batches[0];
        let schema = batch.schema();

        assert_eq!(schema.fields().len(), 32);
        assert_eq!(schema.field(0).data_type(), &DataType::Int32);
        assert_eq!(schema.field(8).data_type(), &DataType::Float64);
        assert_eq!(schema.field(16).data_type(), &DataType::Utf8);
        assert_eq!(schema.field(24).data_type(), &DataType::Boolean);
        assert!(!schema.field(0).is_nullable());
        assert!(schema.field(1).is_nullable());
        assert!(batch.column(0).null_count() == 0);
        assert!(batch.column(1).null_count() > 0);
        assert!(batch.column(8).null_count() > 0);
        assert!(batch.column(16).null_count() > 0);
        assert!(batch.column(24).null_count() > 0);
    }

    #[test]
    fn tpch_lineitem_like_covers_order_line_transport_shape() {
        let options = WriterBenchOptions {
            rows: 128,
            batch_size: 128,
            scenario: super::scenario_by_name("tpch_lineitem_like").unwrap(),
            repeat: 1,
            output: BenchmarkOutput::Human,
        };

        let batches = generated_batches(&options);
        let batch = &batches[0];
        let schema = batch.schema();

        assert_eq!(schema.fields().len(), 16);
        assert_eq!(schema.field(0).name(), "l_orderkey");
        assert_eq!(schema.field(4).data_type(), &DataType::Decimal128(15, 2));
        assert_eq!(schema.field(5).data_type(), &DataType::Decimal128(18, 2));
        assert_eq!(schema.field(10).data_type(), &DataType::Date32);
        assert_eq!(schema.field(15).data_type(), &DataType::Utf8);
        assert!(batch.column(4).as_any().is::<Decimal128Array>());
        assert!(batch.column(10).as_any().is::<Date32Array>());
        assert!(batch.column(15).null_count() > 0);
    }

    #[test]
    fn realistic_scenarios_stream_multiple_batches() {
        for scenario_name in [
            "wide_mixed",
            "decimal_temporal",
            "string_heavy",
            "wide_sparse",
            "tpch_lineitem_like",
        ] {
            let options = WriterBenchOptions {
                rows: 4_097,
                batch_size: 1_024,
                scenario: super::scenario_by_name(scenario_name).unwrap(),
                repeat: 1,
                output: BenchmarkOutput::Human,
            };

            let summary = super::summarize_generated_batches(&options).unwrap();

            assert_eq!(summary.rows, 4_097);
            assert_eq!(summary.batches, 5);
        }
    }

    #[test]
    fn lazy_reader_emits_no_empty_batch_for_exact_multiple() {
        let options = WriterBenchOptions {
            rows: 8,
            batch_size: 4,
            scenario: super::scenario_by_name("narrow_numeric").unwrap(),
            repeat: 1,
            output: BenchmarkOutput::Human,
        };

        let batches = super::GeneratedBatchReader::new(&options)
            .collect::<Result<Vec<_>, _>>()
            .unwrap();

        assert_eq!(batches.len(), 2);
        assert!(batches.iter().all(|batch| batch.num_rows() == 4));
    }

    #[test]
    fn lazy_reader_emits_partial_tail_batch() {
        let options = WriterBenchOptions {
            rows: 9,
            batch_size: 4,
            scenario: super::scenario_by_name("mixed_nullable").unwrap(),
            repeat: 1,
            output: BenchmarkOutput::Human,
        };

        let batches = super::GeneratedBatchReader::new(&options)
            .collect::<Result<Vec<_>, _>>()
            .unwrap();

        assert_eq!(batches.len(), 3);
        assert_eq!(batches[0].num_rows(), 4);
        assert_eq!(batches[1].num_rows(), 4);
        assert_eq!(batches[2].num_rows(), 1);
    }

    #[test]
    fn lazy_readers_are_repeatable_without_shared_state() {
        let options = WriterBenchOptions {
            rows: 17,
            batch_size: 6,
            scenario: super::scenario_by_name("mixed_nullable").unwrap(),
            repeat: 1,
            output: BenchmarkOutput::Human,
        };

        let first = super::GeneratedBatchReader::new(&options)
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        let second = super::GeneratedBatchReader::new(&options)
            .collect::<Result<Vec<_>, _>>()
            .unwrap();

        assert_eq!(first, second);
    }

    #[test]
    fn lazy_reader_summarizes_large_volume_without_collecting_batches() {
        let options = WriterBenchOptions {
            rows: 1_000_001,
            batch_size: 8_192,
            scenario: super::scenario_by_name("narrow_numeric").unwrap(),
            repeat: 1,
            output: BenchmarkOutput::Human,
        };

        let summary = super::summarize_generated_batches(&options).unwrap();

        assert_eq!(summary.rows, 1_000_001);
        assert_eq!(summary.batches, 123);
    }

    #[test]
    fn scenario_registry_lists_supported_scenarios() {
        let names = super::SCENARIOS
            .iter()
            .map(|scenario| scenario.name)
            .collect::<Vec<_>>();

        assert_eq!(
            names,
            [
                "narrow_numeric",
                "mixed_nullable",
                "wide_mixed",
                "decimal_temporal",
                "string_heavy",
                "wide_sparse",
                "tpch_lineitem_like"
            ]
        );
        assert!(
            super::SCENARIOS
                .iter()
                .all(|scenario| !scenario.description.is_empty())
        );
    }

    fn generated_batches(options: &WriterBenchOptions) -> Vec<RecordBatch> {
        super::GeneratedBatchReader::new(options)
            .collect::<Result<Vec<_>, _>>()
            .unwrap()
    }
}

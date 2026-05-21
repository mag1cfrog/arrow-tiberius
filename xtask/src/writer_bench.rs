use std::collections::BTreeMap;
use std::ffi::OsString;
use std::fmt;
use std::future::Future;
use std::path::{Path, PathBuf};
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
    write::profile::DirectWriteProfile,
};
use tokio::net::TcpStream;
use tokio_util::compat::{Compat, TokioAsyncWriteCompatExt};

mod dataset;
mod sqlserver_profile;

static BENCH_TABLE_COUNTER: AtomicU64 = AtomicU64::new(0);
static BENCH_IPC_COUNTER: AtomicU64 = AtomicU64::new(0);
const ODBC_TABLE_PLACEHOLDER: &str = "__ARROW_TIBERIUS_ODBC_TABLE__";
const DEFAULT_SQLSERVER_PROFILE_SAMPLE_MS: u64 = 250;

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

        if command == "compare" {
            return run_compare(&args[1..]);
        }

        if command == "ipc" {
            return run_ipc_dataset(&args[1..]);
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
    let network = create_arrow_odbc_network(&options)?;
    let connection = options
        .sql_server
        .connect_or_start_with_network(network.as_ref())
        .map_err(WriterBenchError::SqlServer)?;
    let mut runner_image = build_arrow_odbc_runner_image(&options)?;
    let runner_image_tag = runner_image.image_tag().to_owned();
    let ipc_dataset = prepare_arrow_odbc_ipc_dataset(&options)?;
    let runner_result = run_arrow_odbc_runner(
        &options,
        &runner_image,
        network.as_ref(),
        &connection,
        &ipc_dataset,
    );
    let dataset_cleanup_result = ipc_dataset.cleanup();
    runner_image
        .cleanup()
        .map_err(WriterBenchError::OdbcRunner)?;
    runner_result?;
    dataset_cleanup_result?;

    println!("writer-bench arrow-odbc");
    println!("  backend: arrow_odbc");
    println!("  runner image: {}", runner_image_tag);
    println!("  database: {}", connection.database);
    Ok(())
}

fn run_compare(args: &[OsString]) -> Result<(), WriterBenchError> {
    if args.iter().any(|arg| arg == "-h" || arg == "--help") {
        print_compare_help();
        return Ok(());
    }

    let options = CompareBenchOptions::parse(args)?;
    let report = run_compare_benchmark(&options)?;

    print_compare_summary(&options, &report);
    Ok(())
}

#[derive(Debug)]
struct ManagedIpcDataset {
    host_path: PathBuf,
    container_path: Option<String>,
}

impl ManagedIpcDataset {
    fn cleanup(&self) -> Result<(), WriterBenchError> {
        if self.host_path.exists() {
            std::fs::remove_file(&self.host_path).map_err(WriterBenchError::Io)?;
        }

        Ok(())
    }
}

fn prepare_arrow_odbc_ipc_dataset(
    options: &ArrowOdbcBenchOptions,
) -> Result<ManagedIpcDataset, WriterBenchError> {
    let root = repository_root()?;
    let dataset_dir = root.join("target").join("arrow-tiberius-writer-bench");
    std::fs::create_dir_all(&dataset_dir).map_err(WriterBenchError::Io)?;

    let counter = BENCH_IPC_COUNTER.fetch_add(1, Ordering::Relaxed);
    let filename = format!(
        "arrow-odbc-{}-{}-{counter}.arrow",
        std::process::id(),
        options.benchmark.scenario.name
    );
    let host_path = dataset_dir.join(&filename);
    let container_path = format!("/workspace/target/arrow-tiberius-writer-bench/{filename}");
    let summary = dataset::write_ipc_dataset(&options.benchmark, &host_path)?;

    println!("writer-bench arrow-odbc");
    println!("  action: prepare_ipc_dataset");
    println!("  path: {}", host_path.display());
    println!("  container path: {container_path}");
    println!("  rows: {}", summary.rows);
    println!("  batches: {}", summary.batches);

    Ok(ManagedIpcDataset {
        host_path,
        container_path: Some(container_path),
    })
}

fn prepare_compare_ipc_dataset(
    options: &CompareBenchOptions,
) -> Result<ManagedIpcDataset, WriterBenchError> {
    let root = repository_root()?;
    let dataset_dir = root.join("target").join("arrow-tiberius-writer-bench");
    std::fs::create_dir_all(&dataset_dir).map_err(WriterBenchError::Io)?;

    let counter = BENCH_IPC_COUNTER.fetch_add(1, Ordering::Relaxed);
    let filename = format!(
        "compare-{}-{}-{counter}.arrow",
        std::process::id(),
        options.benchmark.scenario.name
    );
    let host_path = dataset_dir.join(&filename);
    let container_path = format!("/workspace/target/arrow-tiberius-writer-bench/{filename}");
    let summary = dataset::write_ipc_dataset(&options.benchmark, &host_path)?;

    println!("writer-bench compare");
    println!("  action: prepare_ipc_dataset");
    println!("  path: {}", host_path.display());
    println!("  container path: {container_path}");
    println!("  rows: {}", summary.rows);
    println!("  batches: {}", summary.batches);

    Ok(ManagedIpcDataset {
        host_path,
        container_path: Some(container_path),
    })
}

fn run_ipc_dataset(args: &[OsString]) -> Result<(), WriterBenchError> {
    if args.iter().any(|arg| arg == "-h" || arg == "--help") {
        print_ipc_help();
        return Ok(());
    }

    let options = IpcDatasetOptions::parse(args)?;
    let written = dataset::write_ipc_dataset(&options.benchmark, &options.path)?;
    let replayed = dataset::summarize_ipc_dataset(&options.path)?;

    if written != replayed {
        return Err(WriterBenchError::Validation(format!(
            "IPC dataset replay summary mismatch: wrote {written:?}, replayed {replayed:?}"
        )));
    }

    println!("writer-bench ipc");
    println!("  path: {}", options.path.display());
    println!("  scenario: {}", options.benchmark.scenario);
    println!("  rows: {}", written.rows);
    println!("  batches: {}", written.batches);
    println!("  batch size: {}", options.benchmark.batch_size);
    Ok(())
}

fn print_help() {
    println!(
        "Usage:\n  cargo xtask writer-bench [OPTIONS]\n  cargo xtask writer-bench baseline [OPTIONS]\n  cargo xtask writer-bench arrow-odbc [OPTIONS]\n  cargo xtask writer-bench compare [OPTIONS]\n  cargo xtask writer-bench ipc [OPTIONS]\n\nCommands:\n  baseline      Run the baseline TokenRow SQL Server writer benchmark\n  arrow-odbc    Run the optional arrow-odbc SQL Server writer benchmark\n  compare       Compare writer backends over one shared Arrow IPC dataset\n  ipc           Generate a benchmark Arrow IPC dataset file\n\nOptions:\n  --rows <COUNT>          Total rows to generate [default: 100000]\n  --batch-size <COUNT>    Maximum rows per generated RecordBatch [default: 8192]\n  --scenario <NAME>       Benchmark scenario [default: narrow_numeric]\n  --repeat <COUNT>        Number of benchmark repeats [default: 1]\n  --output <FORMAT>       Output format: human [default: human]\n  -h, --help              Print help\n\nScenarios:"
    );
    for scenario in SCENARIOS {
        println!("  {:<16}  {}", scenario.name, scenario.description);
    }
}

fn print_baseline_help() {
    println!(
        "Usage:\n  cargo xtask writer-bench baseline [OPTIONS]\n\nData Options:\n  --rows <COUNT>              Total rows to generate [default: 100000]\n  --batch-size <COUNT>        Maximum rows per generated RecordBatch [default: 8192]\n  --scenario <NAME>           Benchmark scenario [default: narrow_numeric]\n  --repeat <COUNT>            Number of benchmark repeats [default: 1]\n  --output <FORMAT>           Output format: human [default: human]\n\nSQL Server Options:\n  --container-runtime <PATH>  Container runtime executable, such as docker or podman\n  --connection-string <URL>   Use an existing SQL Server instead of a local container\n  --image <IMAGE>             SQL Server container image\n  --database <NAME>           Benchmark database name\n  --tds-packet-size <BYTES>   Requested TDS packet size for Tiberius writers\n  --keep-container            Keep the container after the task exits\n  -h, --help                  Print help"
    );
}

fn print_arrow_odbc_help() {
    println!(
        "Usage:\n  cargo xtask writer-bench arrow-odbc [OPTIONS]\n\nData Options:\n  --rows <COUNT>              Total rows to generate [default: 100000]\n  --batch-size <COUNT>        Maximum rows per generated RecordBatch [default: 8192]\n  --scenario <NAME>           Benchmark scenario [default: narrow_numeric]\n  --repeat <COUNT>            Number of benchmark repeats [default: 1]\n  --output <FORMAT>           Output format: human [default: human]\n\nSQL Server Options:\n  --container-runtime <PATH>  Container runtime executable, such as docker or podman\n  --connection-string <URL>   Use an existing SQL Server instead of a local container\n  --image <IMAGE>             SQL Server container image\n  --database <NAME>           Benchmark database name\n  --keep-container            Keep managed containers after the task exits\n\nODBC Runner Options:\n  --runner-image <IMAGE>      Managed ODBC runner image tag\n  --keep-runner-image         Keep the managed ODBC runner image after the task exits\n  -h, --help                  Print help\n\nThis is a SQL Server write-path comparison only. The ODBC runner image contains unixODBC, Microsoft ODBC Driver 18 for SQL Server, and Rust."
    );
}

fn print_compare_help() {
    println!(
        "Usage:\n  cargo xtask writer-bench compare [OPTIONS]\n\nData Options:\n  --rows <COUNT>                    Total rows to generate [default: 100000]\n  --batch-size <COUNT>              Maximum rows per generated RecordBatch [default: 8192]\n  --scenario <NAME>                 Benchmark scenario [default: narrow_numeric]\n  --repeat <COUNT>                  Number of benchmark repeats [default: 1]\n  --backends <LIST>                 Comma-separated backends: baseline,direct-raw,arrow-odbc,odbc-bcp [default: baseline,arrow-odbc]\n  --output <FORMAT>                 Output format: human [default: human]\n  --profile-direct                  Include direct-raw phase timings and counters\n\nSQL Server Options:\n  --container-runtime <PATH>        Container runtime executable, such as docker or podman\n  --connection-string <URL>         Use an existing SQL Server instead of a local container\n  --image <IMAGE>                   SQL Server container image\n  --database <NAME>                 Benchmark database name\n  --tds-packet-size <BYTES>         Requested TDS packet size for Tiberius writers\n  --profile-sqlserver               Profile the SQL Server writer session during compare writes\n  --sqlserver-profile-sample-ms <MILLIS>\n                                    SQL Server profile sample interval [default: 250]\n  --keep-container                  Keep managed containers after the task exits\n\nODBC Runner Options:\n  --runner-image <IMAGE>            Managed ODBC runner image tag\n  --keep-runner-image               Keep the managed ODBC runner image after the task exits\n  -h, --help                        Print help\n\nCompare runs use one shared Arrow IPC dataset as the fairness boundary."
    );
}

fn print_ipc_help() {
    println!(
        "Usage:\n  cargo xtask writer-bench ipc --path <FILE> [OPTIONS]\n\nData Options:\n  --path <FILE>               Arrow IPC file to create\n  --rows <COUNT>              Total rows to generate [default: 100000]\n  --batch-size <COUNT>        Maximum rows per generated RecordBatch [default: 8192]\n  --scenario <NAME>           Benchmark scenario [default: narrow_numeric]\n  -h, --help                  Print help\n\nThe IPC file is the shared benchmark dataset boundary used by compare backends."
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
    report: &TiberiusBenchReport,
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
    print_peak_rss("  ", report.peak_rss_kib);
}

fn print_tiberius_backend_summary(report: &TiberiusBenchReport) {
    println!("    batches written: {}", report.stats.batches_written);
    println!("    rows written: {}", report.stats.rows_written);
    println!(
        "    write rows/sec: {}",
        format_rows_per_second(
            report.stats.rows_written,
            report.timings.write + report.timings.finish
        )
    );
    println!("    validated rows: {}", report.validated_rows);
    println!("    setup: {}", format_duration(report.timings.setup));
    println!("    write: {}", format_duration(report.timings.write));
    println!("    finish: {}", format_duration(report.timings.finish));
    println!("    validate: {}", format_duration(report.timings.validate));
    println!("    cleanup: {}", format_duration(report.timings.cleanup));
    println!("    total: {}", format_duration(report.timings.total));
    print_peak_rss("    ", report.peak_rss_kib);
    print_sql_server_profile_target("    ", report.sql_server_profile_target.as_ref());
    if let Some(profile) = report.direct_profile {
        print_direct_profile("    ", profile);
    }
}

fn print_direct_profile(prefix: &str, profile: DirectWriteProfile) {
    println!("{prefix}direct profile:");
    println!(
        "{prefix}  measure_batch: {}",
        format_duration(profile.measure_batch)
    );
    println!(
        "{prefix}  row_range_split: {}",
        format_duration(profile.row_range_split)
    );
    println!(
        "{prefix}  append_encode: {}",
        format_duration(profile.append_encode)
    );
    println!(
        "{prefix}  send_total: {}",
        format_duration(profile.send_total)
    );
    println!(
        "{prefix}  send_without_append_encode: {}",
        format_duration(profile.send_without_append_encode())
    );
    println!("{prefix}  rows: {}", profile.rows);
    println!("{prefix}  batches: {}", profile.batches);
    println!("{prefix}  row_ranges: {}", profile.row_ranges);
    println!("{prefix}  encoded bytes: {}", profile.encoded_bytes);
    println!(
        "{prefix}  max row range bytes: {}",
        profile.max_row_range_bytes
    );
    println!(
        "{prefix}  nvarchar utf16 bytes: {}",
        profile.nvarchar_utf16_bytes
    );
    println!("{prefix}  varbinary bytes: {}", profile.varbinary_bytes);
    println!("{prefix}  null cells: {}", profile.null_cells);
    println!(
        "{prefix}  packet write calls: {}",
        profile.packet_write_calls
    );
    println!("{prefix}  packets written: {}", profile.packets_written);
    println!(
        "{prefix}  packet payload bytes: {}",
        profile.packet_payload_bytes
    );
    println!(
        "{prefix}  max packet payload bytes: {}",
        profile.max_packet_payload_bytes
    );
    println!(
        "{prefix}  max buffered bytes before write: {}",
        profile.max_buffered_bytes_before_write
    );
    println!(
        "{prefix}  buffered bytes after last write: {}",
        profile.buffered_bytes_after_last_write
    );
    println!(
        "{prefix}  finalized packet payload bytes: {}",
        profile.finalized_packet_payload_bytes
    );
    println!(
        "{prefix}  bulk write_packets elapsed: {}",
        format_duration(profile.bulk_write_packets_elapsed)
    );
    println!(
        "{prefix}  bulk write_to_wire calls: {}",
        profile.bulk_write_to_wire_calls
    );
    println!(
        "{prefix}  bulk write_to_wire elapsed: {}",
        format_duration(profile.bulk_write_to_wire_elapsed)
    );
    println!(
        "{prefix}  bulk write_to_wire payload bytes: {}",
        profile.bulk_write_to_wire_payload_bytes
    );
    println!(
        "{prefix}  bulk max write_to_wire elapsed: {}",
        format_duration(profile.bulk_max_write_to_wire_elapsed)
    );
    println!(
        "{prefix}  bulk max write_to_wire payload bytes: {}",
        profile.bulk_max_write_to_wire_payload_bytes
    );
    println!("{prefix}  bulk flush calls: {}", profile.bulk_flush_calls);
    println!(
        "{prefix}  bulk flush elapsed: {}",
        format_duration(profile.bulk_flush_elapsed)
    );
    println!(
        "{prefix}  bulk max flush elapsed: {}",
        format_duration(profile.bulk_max_flush_elapsed)
    );
    println!(
        "{prefix}  bulk finalize elapsed: {}",
        format_duration(profile.bulk_finalize_elapsed)
    );
    println!(
        "{prefix}  bulk finalize write_to_wire elapsed: {}",
        format_duration(profile.bulk_finalize_write_to_wire_elapsed)
    );
    println!(
        "{prefix}  bulk finalize flush elapsed: {}",
        format_duration(profile.bulk_finalize_flush_elapsed)
    );
    println!(
        "{prefix}  bulk finalize result elapsed: {}",
        format_duration(profile.bulk_finalize_result_elapsed)
    );
    println!(
        "{prefix}  bulk connection write calls: {}",
        profile.bulk_connection_write_calls
    );
    println!(
        "{prefix}  bulk connection write payload bytes: {}",
        profile.bulk_connection_write_payload_bytes
    );
    println!(
        "{prefix}  bulk connection write ready elapsed: {}",
        format_duration(profile.bulk_connection_write_ready_elapsed)
    );
    println!(
        "{prefix}  bulk connection write encode elapsed: {}",
        format_duration(profile.bulk_connection_write_encode_elapsed)
    );
    println!(
        "{prefix}  bulk connection write flush elapsed: {}",
        format_duration(profile.bulk_connection_write_flush_elapsed)
    );
    println!(
        "{prefix}  bulk connection write max ready elapsed: {}",
        format_duration(profile.bulk_connection_write_max_ready_elapsed)
    );
    println!(
        "{prefix}  bulk connection write max encode elapsed: {}",
        format_duration(profile.bulk_connection_write_max_encode_elapsed)
    );
    println!(
        "{prefix}  bulk connection write max flush elapsed: {}",
        format_duration(profile.bulk_connection_write_max_flush_elapsed)
    );
    println!(
        "{prefix}  bulk connection write max payload bytes: {}",
        profile.bulk_connection_write_max_payload_bytes
    );
    println!(
        "{prefix}  bulk direct packet write calls: {}",
        profile.bulk_direct_packet_write_calls
    );
    println!(
        "{prefix}  bulk direct packet payload bytes: {}",
        profile.bulk_direct_packet_payload_bytes
    );
    println!(
        "{prefix}  bulk direct packet header bytes: {}",
        profile.bulk_direct_packet_header_bytes
    );
    println!(
        "{prefix}  bulk direct packet max payload bytes: {}",
        profile.bulk_direct_packet_max_payload_bytes
    );
    println!(
        "{prefix}  bulk direct packet final calls: {}",
        profile.bulk_direct_packet_final_calls
    );
    println!(
        "{prefix}  bulk direct packet final payload bytes: {}",
        profile.bulk_direct_packet_final_payload_bytes
    );
    println!(
        "{prefix}  bulk direct packet final header bytes: {}",
        profile.bulk_direct_packet_final_header_bytes
    );
    println!(
        "{prefix}  bulk direct packet raw stream calls: {}",
        profile.bulk_direct_packet_raw_stream_calls
    );
    println!(
        "{prefix}  bulk direct packet TLS stream calls: {}",
        profile.bulk_direct_packet_tls_stream_calls
    );
    println!(
        "{prefix}  bulk direct packet low-level write calls: {}",
        profile.bulk_direct_packet_low_level_write_calls
    );
    println!(
        "{prefix}  bulk direct packet low-level write bytes: {}",
        profile.bulk_direct_packet_low_level_write_bytes
    );
    println!(
        "{prefix}  bulk direct packet max low-level write bytes: {}",
        profile.bulk_direct_packet_max_low_level_write_bytes
    );
    println!(
        "{prefix}  bulk direct packet write elapsed: {}",
        format_duration(profile.bulk_direct_packet_write_elapsed)
    );
    println!(
        "{prefix}  bulk direct packet max write elapsed: {}",
        format_duration(profile.bulk_direct_packet_max_write_elapsed)
    );
    println!(
        "{prefix}  bulk direct packet header write calls: {}",
        profile.bulk_direct_packet_header_write_calls
    );
    println!(
        "{prefix}  bulk direct packet header write bytes: {}",
        profile.bulk_direct_packet_header_write_bytes
    );
    println!(
        "{prefix}  bulk direct packet header max write bytes: {}",
        profile.bulk_direct_packet_header_max_write_bytes
    );
    println!(
        "{prefix}  bulk direct packet header write elapsed: {}",
        format_duration(profile.bulk_direct_packet_header_write_elapsed)
    );
    println!(
        "{prefix}  bulk direct packet header max write elapsed: {}",
        format_duration(profile.bulk_direct_packet_header_max_write_elapsed)
    );
    println!(
        "{prefix}  bulk direct packet header partial writes: {}",
        profile.bulk_direct_packet_header_partial_writes
    );
    println!(
        "{prefix}  bulk direct packet payload write calls: {}",
        profile.bulk_direct_packet_payload_write_calls
    );
    println!(
        "{prefix}  bulk direct packet payload write bytes: {}",
        profile.bulk_direct_packet_payload_write_bytes
    );
    println!(
        "{prefix}  bulk direct packet payload max write bytes: {}",
        profile.bulk_direct_packet_payload_max_write_bytes
    );
    println!(
        "{prefix}  bulk direct packet payload write elapsed: {}",
        format_duration(profile.bulk_direct_packet_payload_write_elapsed)
    );
    println!(
        "{prefix}  bulk direct packet payload max write elapsed: {}",
        format_duration(profile.bulk_direct_packet_payload_max_write_elapsed)
    );
    println!(
        "{prefix}  bulk direct packet payload partial writes: {}",
        profile.bulk_direct_packet_payload_partial_writes
    );
    println!(
        "{prefix}  bulk direct packet poll_write polls: {}",
        profile.bulk_direct_packet_poll_write_polls
    );
    println!(
        "{prefix}  bulk direct packet poll_write pending count: {}",
        profile.bulk_direct_packet_poll_write_pending_count
    );
    println!(
        "{prefix}  bulk direct packet poll_write pending elapsed: {}",
        format_duration(profile.bulk_direct_packet_poll_write_pending_elapsed)
    );
    println!(
        "{prefix}  bulk direct packet poll_write max pending elapsed: {}",
        format_duration(profile.bulk_direct_packet_poll_write_max_pending_elapsed)
    );
    println!(
        "{prefix}  bulk direct packet poll_write ready count: {}",
        profile.bulk_direct_packet_poll_write_ready_count
    );
    println!(
        "{prefix}  bulk direct packet poll_write ready elapsed: {}",
        format_duration(profile.bulk_direct_packet_poll_write_ready_elapsed)
    );
    println!(
        "{prefix}  bulk direct packet poll_write max ready elapsed: {}",
        format_duration(profile.bulk_direct_packet_poll_write_max_ready_elapsed)
    );
    println!(
        "{prefix}  bulk direct packet flush calls: {}",
        profile.bulk_direct_packet_flush_calls
    );
    println!(
        "{prefix}  bulk direct packet flush elapsed: {}",
        format_duration(profile.bulk_direct_packet_flush_elapsed)
    );
    println!(
        "{prefix}  bulk direct packet max flush elapsed: {}",
        format_duration(profile.bulk_direct_packet_max_flush_elapsed)
    );
    println!(
        "{prefix}  bulk direct packet flush pending count: {}",
        profile.bulk_direct_packet_flush_pending_count
    );
    println!(
        "{prefix}  bulk direct packet flush pending elapsed: {}",
        format_duration(profile.bulk_direct_packet_flush_pending_elapsed)
    );
    println!(
        "{prefix}  bulk direct packet flush max pending elapsed: {}",
        format_duration(profile.bulk_direct_packet_flush_max_pending_elapsed)
    );
}

fn print_compare_summary(options: &CompareBenchOptions, report: &CompareBenchReport) {
    println!("writer-bench compare");
    println!("  rows per repeat: {}", options.benchmark.rows);
    println!("  batch size: {}", options.benchmark.batch_size);
    println!("  scenario: {}", options.benchmark.scenario);
    println!("  repeat: {}", options.benchmark.repeat);
    println!("  output: {}", options.benchmark.output);
    println!("  dataset: {}", report.ipc_dataset.display());
    println!("  database: {}", report.database);

    for backend in &report.backends {
        println!("  backend: {}", backend.backend());
        match backend {
            CompareBackendBenchReport::Baseline { report }
            | CompareBackendBenchReport::DirectRaw { report } => {
                print_tiberius_backend_summary(report);
            }
            CompareBackendBenchReport::ArrowOdbc { report } => {
                println!("    rows written: {}", report.rows_written);
                println!(
                    "    write rows/sec: {}",
                    format_rows_per_second(report.rows_written, report.write_elapsed)
                );
                println!("    validated rows: {}", report.rows_written);
                println!("    write: {}", format_duration(report.write_elapsed));
                print_peak_rss("    ", report.peak_rss_kib);
            }
            CompareBackendBenchReport::OdbcBcp { report } => {
                println!("    rows written: {}", report.rows_written);
                println!(
                    "    write rows/sec: {}",
                    format_rows_per_second(report.rows_written, report.write_elapsed)
                );
                println!("    validated rows: {}", report.rows_written);
                println!("    write: {}", format_duration(report.write_elapsed));
                print_peak_rss("    ", report.peak_rss_kib);
            }
        }
    }
}

fn print_peak_rss(prefix: &str, peak_rss_kib: Option<u64>) {
    if let Some(peak_rss_kib) = peak_rss_kib {
        println!("{prefix}peak rss KiB: {peak_rss_kib}");
    }
}

fn print_sql_server_profile_target(prefix: &str, target: Option<&SqlServerProfileTarget>) {
    if let Some(target) = target {
        println!("{prefix}sql server profile:");
        println!(
            "{prefix}  sample interval: {} ms",
            target.sample_interval.as_millis()
        );
        println!("{prefix}  writer session id: {}", target.writer_session_id);
        println!(
            "{prefix}  observer session id: {}",
            target.observer_session_id
        );
        println!(
            "{prefix}  writer connection: {} {} encrypted={} packet_size={} reads={} writes={}",
            target.initial_activity.connection.net_transport,
            target.initial_activity.connection.protocol_type,
            target.initial_activity.connection.encrypt_option,
            target.initial_activity.connection.net_packet_size,
            target.initial_activity.connection.num_reads,
            target.initial_activity.connection.num_writes
        );
        match &target.initial_activity.request {
            Some(request) => println!(
                "{prefix}  initial request: status={} command={} wait={} wait_ms={} cpu_ms={} elapsed_ms={}",
                request.status,
                request.command,
                request.wait_type.as_deref().unwrap_or("<none>"),
                request.wait_time_ms,
                request.cpu_time_ms,
                request.total_elapsed_time_ms
            ),
            None => println!("{prefix}  initial request: <none>"),
        }
        println!(
            "{prefix}  initial waiting tasks: {}",
            target.initial_activity.waiting_tasks.len()
        );
        println!("{prefix}  write samples: {}", target.write_samples.len());
        print_sql_server_profile_sample_coverage(prefix, &target.write_samples);
        let sample_summary = SqlServerProfileSampleSummary::from_samples(&target.write_samples);
        print_sql_server_profile_sample_distribution(
            prefix,
            "request status samples",
            &sample_summary.request_statuses,
        );
        print_sql_server_profile_sample_distribution(
            prefix,
            "request wait samples",
            &sample_summary.request_waits,
        );
        print_sql_server_profile_sample_distribution(
            prefix,
            "waiting task wait samples",
            &sample_summary.waiting_task_waits,
        );
        print_sql_server_session_wait_deltas(prefix, &target.session_wait_deltas);
        print_sql_server_database_file_io_deltas(prefix, &target.database_file_io_deltas);
    }
}

fn print_sql_server_profile_sample_coverage(prefix: &str, samples: &[SqlServerProfileSample]) {
    let (Some(first), Some(last)) = (samples.first(), samples.last()) else {
        println!("{prefix}  write sample coverage: <none>");
        return;
    };

    println!(
        "{prefix}  write sample coverage: first=repeat {} {}..{} last=repeat {} {}..{}",
        first.repeat_index + 1,
        format_duration(first.write_elapsed_start),
        format_duration(first.write_elapsed_end),
        last.repeat_index + 1,
        format_duration(last.write_elapsed_start),
        format_duration(last.write_elapsed_end),
    );
}

fn print_sql_server_profile_sample_distribution(
    prefix: &str,
    label: &str,
    distribution: &BTreeMap<String, usize>,
) {
    if distribution.is_empty() {
        println!("{prefix}  {label}: <none>");
        return;
    }

    println!("{prefix}  {label}:");
    for (value, count) in distribution {
        println!("{prefix}    {value}: {count}");
    }
}

fn print_sql_server_session_wait_deltas(prefix: &str, waits: &[SqlServerSessionWaitDelta]) {
    if waits.is_empty() {
        println!("{prefix}  session wait deltas: <none>");
        return;
    }

    println!("{prefix}  session wait deltas:");
    for wait in waits {
        println!(
            "{prefix}    {}: wait_ms={} tasks={} max_wait_ms={} signal_wait_ms={}",
            wait.wait_type,
            wait.wait_time_ms,
            wait.waiting_tasks_count,
            wait.max_wait_time_ms,
            wait.signal_wait_time_ms
        );
    }
}

fn print_sql_server_database_file_io_deltas(prefix: &str, files: &[SqlServerDatabaseFileIoDelta]) {
    if files.is_empty() {
        println!("{prefix}  database file IO deltas: <none>");
        return;
    }

    println!("{prefix}  database file IO deltas:");
    for file in files {
        println!(
            "{prefix}    {} {}: reads={} read_bytes={} read_stall_ms={} writes={} write_bytes={} write_stall_ms={}",
            file.file_type,
            file.logical_name,
            file.read_count,
            file.read_bytes,
            file.read_stall_ms,
            file.write_count,
            file.write_bytes,
            file.write_stall_ms
        );
    }
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
struct IpcDatasetOptions {
    benchmark: WriterBenchOptions,
    path: PathBuf,
}

impl IpcDatasetOptions {
    fn parse(args: &[OsString]) -> Result<Self, WriterBenchError> {
        let mut benchmark = WriterBenchOptions::default();
        let mut path = None;
        let mut index = 0;

        while index < args.len() {
            let arg = args[index]
                .to_str()
                .ok_or_else(|| WriterBenchError::InvalidUtf8Argument(args[index].clone()))?;

            match arg {
                "-h" | "--help" => {
                    print_ipc_help();
                    return Ok(Self {
                        benchmark,
                        path: PathBuf::new(),
                    });
                }
                "--path" => {
                    path = Some(PathBuf::from(required_value(args, index)?));
                    index += 1;
                }
                "--rows" => {
                    benchmark.rows = parse_positive_usize("--rows", &required_value(args, index)?)?;
                    index += 1;
                }
                "--batch-size" => {
                    benchmark.batch_size =
                        parse_positive_usize("--batch-size", &required_value(args, index)?)?;
                    index += 1;
                }
                "--scenario" => {
                    benchmark.scenario = parse_scenario(&required_value(args, index)?)?;
                    index += 1;
                }
                other => return Err(WriterBenchError::UnknownOption(other.to_owned())),
            }

            index += 1;
        }

        let path = path.ok_or_else(|| {
            WriterBenchError::Validation("writer-bench ipc requires --path <FILE>".to_owned())
        })?;

        Ok(Self { benchmark, path })
    }
}

#[derive(Debug, Clone)]
struct BaselineBenchOptions {
    benchmark: WriterBenchOptions,
    sql_server: sqlserver::SqlServerConnectionOptions,
    tds_packet_size: Option<u32>,
}

impl BaselineBenchOptions {
    fn parse(args: &[OsString]) -> Result<Self, WriterBenchError> {
        let mut options = Self {
            benchmark: WriterBenchOptions::default(),
            sql_server: sqlserver::SqlServerConnectionOptions::benchmark_default(),
            tds_packet_size: None,
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
                "--tds-packet-size" => {
                    options.tds_packet_size = Some(parse_positive_u32(
                        "--tds-packet-size",
                        &required_value(args, index)?,
                    )?);
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
    runner_image: String,
    keep_runner_image: bool,
}

impl ArrowOdbcBenchOptions {
    fn parse(args: &[OsString]) -> Result<Self, WriterBenchError> {
        parse_writer_sqlserver_options(args, print_arrow_odbc_help)
    }
}

#[derive(Debug, Clone)]
struct CompareBenchOptions {
    benchmark: WriterBenchOptions,
    sql_server: sqlserver::SqlServerConnectionOptions,
    sql_server_profile: Option<SqlServerProfileOptions>,
    backends: Vec<BenchmarkBackend>,
    runner_image: String,
    keep_runner_image: bool,
    profile_direct: bool,
    tds_packet_size: Option<u32>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct SqlServerProfileOptions {
    sample_interval: Duration,
}

impl Default for SqlServerProfileOptions {
    fn default() -> Self {
        Self {
            sample_interval: Duration::from_millis(DEFAULT_SQLSERVER_PROFILE_SAMPLE_MS),
        }
    }
}

impl CompareBenchOptions {
    fn parse(args: &[OsString]) -> Result<Self, WriterBenchError> {
        let mut options = Self {
            benchmark: WriterBenchOptions::default(),
            sql_server: sqlserver::SqlServerConnectionOptions::benchmark_default(),
            sql_server_profile: None,
            backends: vec![BenchmarkBackend::Baseline, BenchmarkBackend::ArrowOdbc],
            runner_image: odbc_runner::DEFAULT_RUNNER_IMAGE_TAG.to_owned(),
            keep_runner_image: false,
            profile_direct: false,
            tds_packet_size: None,
        };
        let mut sql_server_profile_sample_interval = None;
        let mut index = 0;

        while index < args.len() {
            let arg = args[index]
                .to_str()
                .ok_or_else(|| WriterBenchError::InvalidUtf8Argument(args[index].clone()))?;

            match arg {
                "-h" | "--help" => {
                    print_compare_help();
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
                "--backends" => {
                    options.backends = parse_benchmark_backends(&required_value(args, index)?)?;
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
                "--tds-packet-size" => {
                    options.tds_packet_size = Some(parse_positive_u32(
                        "--tds-packet-size",
                        &required_value(args, index)?,
                    )?);
                    index += 1;
                }
                "--profile-sqlserver" => {
                    options
                        .sql_server_profile
                        .get_or_insert_with(SqlServerProfileOptions::default);
                }
                "--sqlserver-profile-sample-ms" => {
                    let sample_ms = parse_positive_u64(
                        "--sqlserver-profile-sample-ms",
                        &required_value(args, index)?,
                    )?;
                    sql_server_profile_sample_interval = Some(Duration::from_millis(sample_ms));
                    index += 1;
                }
                "--keep-container" => {
                    options.sql_server.keep_container = true;
                }
                "--runner-image" => {
                    options.runner_image = required_value(args, index)?;
                    index += 1;
                }
                "--keep-runner-image" => {
                    options.keep_runner_image = true;
                }
                "--profile-direct" => {
                    options.profile_direct = true;
                }
                other => return Err(WriterBenchError::UnknownOption(other.to_owned())),
            }

            index += 1;
        }

        if let Some(sample_interval) = sql_server_profile_sample_interval {
            let profile = options.sql_server_profile.as_mut().ok_or_else(|| {
                WriterBenchError::Validation(
                    "writer-bench compare --sqlserver-profile-sample-ms requires --profile-sqlserver"
                        .to_owned(),
                )
            })?;
            profile.sample_interval = sample_interval;
        }

        Ok(options)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum BenchmarkBackend {
    Baseline,
    DirectRaw,
    ArrowOdbc,
    OdbcBcp,
}

impl BenchmarkBackend {
    fn is_tiberius(&self) -> bool {
        matches!(self, Self::Baseline | Self::DirectRaw)
    }
}

impl fmt::Display for BenchmarkBackend {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Baseline => f.write_str("baseline"),
            Self::DirectRaw => f.write_str("direct-raw"),
            Self::ArrowOdbc => f.write_str("arrow-odbc"),
            Self::OdbcBcp => f.write_str("odbc-bcp"),
        }
    }
}

impl FromStr for BenchmarkBackend {
    type Err = WriterBenchError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "baseline" => Ok(Self::Baseline),
            "direct-raw" => Ok(Self::DirectRaw),
            "arrow-odbc" => Ok(Self::ArrowOdbc),
            "odbc-bcp" => Ok(Self::OdbcBcp),
            other => Err(WriterBenchError::Validation(format!(
                "unknown writer-bench compare backend `{other}`; expected baseline, direct-raw, arrow-odbc, or odbc-bcp"
            ))),
        }
    }
}

fn parse_benchmark_backends(value: &str) -> Result<Vec<BenchmarkBackend>, WriterBenchError> {
    let mut backends = Vec::new();

    for raw_backend in value.split(',') {
        let raw_backend = raw_backend.trim();
        if raw_backend.is_empty() {
            return Err(WriterBenchError::Validation(
                "writer-bench compare --backends contains an empty backend name".to_owned(),
            ));
        }

        let backend = raw_backend.parse::<BenchmarkBackend>()?;
        if backends.contains(&backend) {
            return Err(WriterBenchError::Validation(format!(
                "writer-bench compare backend `{backend}` was provided more than once"
            )));
        }

        backends.push(backend);
    }

    if backends.is_empty() {
        return Err(WriterBenchError::Validation(
            "writer-bench compare requires at least one backend".to_owned(),
        ));
    }

    Ok(backends)
}

fn ensure_direct_raw_supported_scenario(
    benchmark: &WriterBenchOptions,
) -> Result<(), WriterBenchError> {
    if DIRECT_RAW_SUPPORTED_SCENARIOS.contains(&benchmark.scenario.name) {
        return Ok(());
    }

    Err(WriterBenchError::Validation(format!(
        "writer-bench compare backend `direct-raw` currently supports only scenarios {}; scenario `{}` contains column types that are not implemented by the direct TDS encoder yet",
        DIRECT_RAW_SUPPORTED_SCENARIOS.join(", "),
        benchmark.scenario.name
    )))
}

fn parse_writer_sqlserver_options(
    args: &[OsString],
    print_command_help: fn(),
) -> Result<ArrowOdbcBenchOptions, WriterBenchError> {
    let mut options = ArrowOdbcBenchOptions {
        benchmark: WriterBenchOptions::default(),
        sql_server: sqlserver::SqlServerConnectionOptions::benchmark_default(),
        runner_image: odbc_runner::DEFAULT_RUNNER_IMAGE_TAG.to_owned(),
        keep_runner_image: false,
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
            "--runner-image" => {
                options.runner_image = required_value(args, index)?;
                index += 1;
            }
            "--keep-runner-image" => {
                options.keep_runner_image = true;
            }
            other => return Err(WriterBenchError::UnknownOption(other.to_owned())),
        }

        index += 1;
    }

    Ok(options)
}

fn create_arrow_odbc_network(
    options: &ArrowOdbcBenchOptions,
) -> Result<Option<sqlserver::ManagedNetwork>, WriterBenchError> {
    create_odbc_runner_network(&options.sql_server)
}

fn create_compare_network(
    options: &CompareBenchOptions,
) -> Result<Option<sqlserver::ManagedNetwork>, WriterBenchError> {
    if options.backends.contains(&BenchmarkBackend::ArrowOdbc)
        || options.backends.contains(&BenchmarkBackend::OdbcBcp)
    {
        create_odbc_runner_network(&options.sql_server)
    } else {
        Ok(None)
    }
}

fn create_odbc_runner_network(
    sql_server: &sqlserver::SqlServerConnectionOptions,
) -> Result<Option<sqlserver::ManagedNetwork>, WriterBenchError> {
    if sql_server.connection_string.is_some() {
        return Ok(None);
    }

    let container_runtime = sql_server
        .resolve_runtime()
        .map_err(WriterBenchError::SqlServer)?;
    let network = sqlserver::ManagedNetwork::create(container_runtime, sql_server.keep_container)
        .map_err(WriterBenchError::SqlServer)?;

    println!("writer-bench odbc-runner");
    println!("  action: prepare_container_network");
    println!("  network: {}", network.name());
    println!("  keep network: {}", sql_server.keep_container);

    Ok(Some(network))
}

fn build_arrow_odbc_runner_image(
    options: &ArrowOdbcBenchOptions,
) -> Result<odbc_runner::ManagedRunnerImage, WriterBenchError> {
    build_odbc_runner_image(
        &options.sql_server,
        &options.runner_image,
        options.keep_runner_image,
    )
}

fn build_compare_odbc_runner_image(
    options: &CompareBenchOptions,
) -> Result<odbc_runner::ManagedRunnerImage, WriterBenchError> {
    build_odbc_runner_image(
        &options.sql_server,
        &options.runner_image,
        options.keep_runner_image,
    )
}

fn build_odbc_runner_image(
    sql_server: &sqlserver::SqlServerConnectionOptions,
    runner_image: &str,
    keep_runner_image: bool,
) -> Result<odbc_runner::ManagedRunnerImage, WriterBenchError> {
    let container_runtime = sql_server
        .resolve_runtime()
        .map_err(WriterBenchError::SqlServer)?;
    let image_options = odbc_runner::RunnerImageOptions {
        container_runtime,
        image_tag: runner_image.to_owned(),
        manifest_dir: repository_root()?,
    };

    println!("writer-bench odbc-runner");
    println!("  action: prepare_runner_image");
    println!("  image: {}", image_options.image_tag);
    println!("  dockerfile: {}", image_options.dockerfile().display());
    println!("  keep image: {keep_runner_image}");

    odbc_runner::ManagedRunnerImage::build(image_options, keep_runner_image)
        .map_err(WriterBenchError::OdbcRunner)
}

fn run_arrow_odbc_runner(
    options: &ArrowOdbcBenchOptions,
    runner_image: &odbc_runner::ManagedRunnerImage,
    network: Option<&sqlserver::ManagedNetwork>,
    connection: &sqlserver::SqlServerConnection,
    ipc_dataset: &ManagedIpcDataset,
) -> Result<(), WriterBenchError> {
    run_arrow_odbc_runner_for_benchmark(
        &options.benchmark,
        runner_image,
        network,
        connection,
        ipc_dataset,
    )
}

fn run_arrow_odbc_runner_for_benchmark(
    benchmark: &WriterBenchOptions,
    runner_image: &odbc_runner::ManagedRunnerImage,
    network: Option<&sqlserver::ManagedNetwork>,
    connection: &sqlserver::SqlServerConnection,
    ipc_dataset: &ManagedIpcDataset,
) -> Result<(), WriterBenchError> {
    println!("  action: run_arrow_odbc_runner");
    let command_options = arrow_odbc_runner_command_options(
        benchmark,
        runner_image,
        network,
        connection,
        ipc_dataset,
    )?;

    odbc_runner::run_runner_command(&command_options).map_err(WriterBenchError::OdbcRunner)
}

fn run_arrow_odbc_runner_for_benchmark_capture(
    benchmark: &WriterBenchOptions,
    runner_image: &odbc_runner::ManagedRunnerImage,
    network: Option<&sqlserver::ManagedNetwork>,
    connection: &sqlserver::SqlServerConnection,
    ipc_dataset: &ManagedIpcDataset,
) -> Result<OdbcRunnerBenchReport, WriterBenchError> {
    println!("  action: run_arrow_odbc_runner");
    let command_options = arrow_odbc_runner_command_options(
        benchmark,
        runner_image,
        network,
        connection,
        ipc_dataset,
    )?;
    let output = odbc_runner::run_runner_command_capture(&command_options)
        .map_err(WriterBenchError::OdbcRunner)?;

    print!("{}", output.stdout);
    eprint!("{}", output.stderr);

    parse_arrow_odbc_runner_report(&format!("{}\n{}", output.stdout, output.stderr))
}

fn run_odbc_bcp_runner_for_benchmark_capture(
    benchmark: &WriterBenchOptions,
    runner_image: &odbc_runner::ManagedRunnerImage,
    network: Option<&sqlserver::ManagedNetwork>,
    connection: &sqlserver::SqlServerConnection,
    ipc_dataset: &ManagedIpcDataset,
) -> Result<OdbcRunnerBenchReport, WriterBenchError> {
    println!("  action: run_odbc_bcp_runner");
    let command_options =
        odbc_bcp_runner_command_options(benchmark, runner_image, network, connection, ipc_dataset)?;
    let output = odbc_runner::run_runner_command_capture(&command_options)
        .map_err(WriterBenchError::OdbcRunner)?;

    print!("{}", output.stdout);
    eprint!("{}", output.stderr);

    parse_odbc_bcp_runner_report(&format!("{}\n{}", output.stdout, output.stderr))
}

fn arrow_odbc_runner_command_options(
    benchmark: &WriterBenchOptions,
    runner_image: &odbc_runner::ManagedRunnerImage,
    network: Option<&sqlserver::ManagedNetwork>,
    connection: &sqlserver::SqlServerConnection,
    ipc_dataset: &ManagedIpcDataset,
) -> Result<odbc_runner::RunnerCommandOptions, WriterBenchError> {
    let container_path = ipc_dataset.container_path.as_deref().ok_or_else(|| {
        WriterBenchError::Validation(
            "arrow-odbc benchmark requires an IPC dataset container path".to_owned(),
        )
    })?;

    Ok(runner_image.command_options(
        network.map(|network| network.name().to_owned()),
        vec![
            (
                "ARROW_TIBERIUS_BENCH_CONNECTION_STRING".to_owned(),
                connection.connection_string.clone(),
            ),
            (
                "ARROW_TIBERIUS_BENCH_ODBC_CONNECTION_STRING".to_owned(),
                odbc_connection_string(connection)?,
            ),
            (
                "ARROW_TIBERIUS_BENCH_DATABASE".to_owned(),
                connection.database.clone(),
            ),
        ],
        Some(repository_root()?),
        Some("/workspace".to_owned()),
        arrow_odbc_runner_args(benchmark, container_path)?,
    ))
}

fn arrow_odbc_runner_args(
    benchmark: &WriterBenchOptions,
    input_ipc: &str,
) -> Result<Vec<String>, WriterBenchError> {
    let create_table_sql_template = arrow_odbc_create_table_sql_template(benchmark)?;

    Ok(vec![
        "cargo".to_owned(),
        "run".to_owned(),
        "--release".to_owned(),
        "--manifest-path".to_owned(),
        "xtask/arrow-odbc-runner/Cargo.toml".to_owned(),
        "--target-dir".to_owned(),
        "/tmp/arrow-tiberius-odbc-runner-target".to_owned(),
        "--".to_owned(),
        "bench".to_owned(),
        "--rows".to_owned(),
        benchmark.rows.to_string(),
        "--batch-size".to_owned(),
        benchmark.batch_size.to_string(),
        "--scenario".to_owned(),
        benchmark.scenario.name.to_owned(),
        "--repeat".to_owned(),
        benchmark.repeat.to_string(),
        "--input-ipc".to_owned(),
        input_ipc.to_owned(),
        "--create-table-sql-template".to_owned(),
        create_table_sql_template,
    ])
}

fn odbc_bcp_runner_command_options(
    benchmark: &WriterBenchOptions,
    runner_image: &odbc_runner::ManagedRunnerImage,
    network: Option<&sqlserver::ManagedNetwork>,
    connection: &sqlserver::SqlServerConnection,
    ipc_dataset: &ManagedIpcDataset,
) -> Result<odbc_runner::RunnerCommandOptions, WriterBenchError> {
    let container_path = ipc_dataset.container_path.as_deref().ok_or_else(|| {
        WriterBenchError::Validation(
            "odbc-bcp benchmark requires an IPC dataset container path".to_owned(),
        )
    })?;

    Ok(runner_image.command_options(
        network.map(|network| network.name().to_owned()),
        vec![
            (
                "ARROW_TIBERIUS_BENCH_ODBC_CONNECTION_STRING".to_owned(),
                odbc_connection_string(connection)?,
            ),
            (
                "ARROW_TIBERIUS_BENCH_DATABASE".to_owned(),
                connection.database.clone(),
            ),
        ],
        Some(repository_root()?),
        Some("/workspace".to_owned()),
        odbc_bcp_runner_args(benchmark, container_path)?,
    ))
}

fn odbc_bcp_runner_args(
    benchmark: &WriterBenchOptions,
    input_ipc: &str,
) -> Result<Vec<String>, WriterBenchError> {
    let create_table_sql_template = arrow_odbc_create_table_sql_template(benchmark)?;

    Ok(vec![
        "cargo".to_owned(),
        "run".to_owned(),
        "--release".to_owned(),
        "--manifest-path".to_owned(),
        "xtask/odbc-bcp-runner/Cargo.toml".to_owned(),
        "--target-dir".to_owned(),
        "/tmp/arrow-tiberius-odbc-bcp-runner-target".to_owned(),
        "--".to_owned(),
        "bench".to_owned(),
        "--rows".to_owned(),
        benchmark.rows.to_string(),
        "--batch-size".to_owned(),
        benchmark.batch_size.to_string(),
        "--scenario".to_owned(),
        benchmark.scenario.name.to_owned(),
        "--repeat".to_owned(),
        benchmark.repeat.to_string(),
        "--input-ipc".to_owned(),
        input_ipc.to_owned(),
        "--create-table-sql-template".to_owned(),
        create_table_sql_template,
    ])
}

fn arrow_odbc_create_table_sql_template(
    benchmark: &WriterBenchOptions,
) -> Result<String, WriterBenchError> {
    let placeholder_table =
        TableName::new("dbo", ODBC_TABLE_PLACEHOLDER).map_err(WriterBenchError::ArrowTiberius)?;
    let schema = (benchmark.scenario.schema)();
    let mappings = benchmark_mappings_for_schema(schema)?;
    let sql = benchmark_table_sql(&placeholder_table, &mappings);
    let quoted_placeholder = placeholder_table.quoted_sql();

    Ok(sql.replace(&quoted_placeholder, ODBC_TABLE_PLACEHOLDER))
}

fn parse_arrow_odbc_runner_report(output: &str) -> Result<OdbcRunnerBenchReport, WriterBenchError> {
    parse_odbc_runner_report(output, "arrow-odbc")
}

fn parse_odbc_bcp_runner_report(output: &str) -> Result<OdbcRunnerBenchReport, WriterBenchError> {
    parse_odbc_runner_report(output, "odbc-bcp")
}

fn parse_odbc_runner_report(
    output: &str,
    runner_name: &str,
) -> Result<OdbcRunnerBenchReport, WriterBenchError> {
    let rows_written = parse_odbc_runner_u64(output, "rows written", runner_name)?;
    let write_seconds = parse_odbc_runner_f64(output, "write seconds", runner_name)?;
    let peak_rss_kib = parse_odbc_runner_optional_u64(output, "peak rss KiB", runner_name)?;

    if !write_seconds.is_finite() || write_seconds < 0.0 {
        return Err(WriterBenchError::Validation(format!(
            "{runner_name} runner reported invalid write seconds `{write_seconds}`"
        )));
    }

    Ok(OdbcRunnerBenchReport {
        rows_written,
        write_elapsed: Duration::from_secs_f64(write_seconds),
        peak_rss_kib,
    })
}

fn parse_odbc_runner_u64(
    output: &str,
    label: &str,
    runner_name: &str,
) -> Result<u64, WriterBenchError> {
    parse_odbc_runner_value(output, label, runner_name)?
        .parse::<u64>()
        .map_err(|source| {
            WriterBenchError::Validation(format!(
                "{runner_name} runner reported invalid {label}: {source}"
            ))
        })
}

fn parse_odbc_runner_f64(
    output: &str,
    label: &str,
    runner_name: &str,
) -> Result<f64, WriterBenchError> {
    parse_odbc_runner_value(output, label, runner_name)?
        .parse::<f64>()
        .map_err(|source| {
            WriterBenchError::Validation(format!(
                "{runner_name} runner reported invalid {label}: {source}"
            ))
        })
}

fn parse_odbc_runner_optional_u64(
    output: &str,
    label: &str,
    runner_name: &str,
) -> Result<Option<u64>, WriterBenchError> {
    let Some(value) = parse_odbc_runner_optional_value(output, label) else {
        return Ok(None);
    };

    value.parse().map(Some).map_err(|source| {
        WriterBenchError::Validation(format!(
            "{runner_name} runner reported invalid {label}: {source}"
        ))
    })
}

fn parse_odbc_runner_value<'a>(
    output: &'a str,
    label: &str,
    runner_name: &str,
) -> Result<&'a str, WriterBenchError> {
    parse_odbc_runner_optional_value(output, label).ok_or_else(|| {
        WriterBenchError::Validation(format!("{runner_name} runner output is missing `{label}`"))
    })
}

fn parse_odbc_runner_optional_value<'a>(output: &'a str, label: &str) -> Option<&'a str> {
    let prefix = format!("{label}:");
    output
        .lines()
        .find_map(|line| line.trim().strip_prefix(&prefix).map(str::trim))
}

fn current_process_peak_rss_kib() -> Option<u64> {
    let status = std::fs::read_to_string("/proc/self/status").ok()?;
    status.lines().find_map(|line| {
        let value = line.strip_prefix("VmHWM:")?.trim();
        let kib = value.strip_suffix("kB")?.trim();
        kib.parse().ok()
    })
}

fn odbc_connection_string(
    connection: &sqlserver::SqlServerConnection,
) -> Result<String, WriterBenchError> {
    let connection_string = connection
        .container_connection_string
        .as_deref()
        .unwrap_or(&connection.connection_string);
    odbc_connection_string_from_parts(connection_string, &connection.database)
}

fn odbc_connection_string_from_parts(
    connection_string: &str,
    database: &str,
) -> Result<String, WriterBenchError> {
    let server =
        connection_setting(connection_string, &["server", "data source"]).ok_or_else(|| {
            WriterBenchError::Validation(
                "SQL Server connection string is missing server for ODBC runner".to_owned(),
            )
        })?;
    let user = connection_setting(connection_string, &["user id", "uid"]).ok_or_else(|| {
        WriterBenchError::Validation(
            "SQL Server connection string is missing user id for ODBC runner".to_owned(),
        )
    })?;
    let password =
        connection_setting(connection_string, &["password", "pwd"]).ok_or_else(|| {
            WriterBenchError::Validation(
                "SQL Server connection string is missing password for ODBC runner".to_owned(),
            )
        })?;
    let trust_server_certificate =
        connection_setting(connection_string, &["trustservercertificate"]).unwrap_or("yes");

    Ok(format!(
        "Driver={{ODBC Driver 18 for SQL Server}};Server={server};UID={user};PWD={password};Database={database};TrustServerCertificate={};",
        odbc_bool_value(trust_server_certificate)
    ))
}

fn connection_setting<'a>(connection_string: &'a str, names: &[&str]) -> Option<&'a str> {
    connection_string.split(';').find_map(|segment| {
        let (name, value) = segment.split_once('=')?;
        let name = name.trim();
        let value = value.trim();

        if value.is_empty() {
            return None;
        }

        names
            .iter()
            .any(|expected| name.eq_ignore_ascii_case(expected))
            .then_some(value)
    })
}

fn odbc_bool_value(value: &str) -> &'static str {
    if value.eq_ignore_ascii_case("true") || value.eq_ignore_ascii_case("yes") || value == "1" {
        "yes"
    } else {
        "no"
    }
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

const DIRECT_RAW_SUPPORTED_SCENARIOS: &[&str] = &[
    NARROW_NUMERIC_SCENARIO.name,
    MIXED_NULLABLE_SCENARIO.name,
    STRING_HEAVY_SCENARIO.name,
    WIDE_SPARSE_SCENARIO.name,
];

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

#[derive(Debug, Clone, Default, PartialEq, Eq)]
struct TiberiusBenchReport {
    stats: arrow_tiberius::WriteStats,
    validated_rows: u64,
    peak_rss_kib: Option<u64>,
    timings: TiberiusBenchTimings,
    sql_server_profile_target: Option<SqlServerProfileTarget>,
    direct_profile: Option<DirectWriteProfile>,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
struct TiberiusBenchTimings {
    setup: Duration,
    write: Duration,
    finish: Duration,
    validate: Duration,
    cleanup: Duration,
    total: Duration,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct CompareBenchReport {
    ipc_dataset: PathBuf,
    database: String,
    backends: Vec<CompareBackendBenchReport>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum CompareBackendBenchReport {
    Baseline { report: TiberiusBenchReport },
    DirectRaw { report: TiberiusBenchReport },
    ArrowOdbc { report: OdbcRunnerBenchReport },
    OdbcBcp { report: OdbcRunnerBenchReport },
}

impl CompareBackendBenchReport {
    fn backend(&self) -> BenchmarkBackend {
        match self {
            Self::Baseline { .. } => BenchmarkBackend::Baseline,
            Self::DirectRaw { .. } => BenchmarkBackend::DirectRaw,
            Self::ArrowOdbc { .. } => BenchmarkBackend::ArrowOdbc,
            Self::OdbcBcp { .. } => BenchmarkBackend::OdbcBcp,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct OdbcRunnerBenchReport {
    rows_written: u64,
    write_elapsed: Duration,
    peak_rss_kib: Option<u64>,
}

type BenchClient = tiberius::Client<Compat<TcpStream>>;

#[derive(Debug, Clone, PartialEq, Eq)]
struct SqlServerProfileTarget {
    writer_session_id: i32,
    observer_session_id: i32,
    sample_interval: Duration,
    initial_activity: sqlserver_profile::ActivitySnapshot,
    write_samples: Vec<SqlServerProfileSample>,
    session_wait_deltas: Vec<SqlServerSessionWaitDelta>,
    database_file_io_deltas: Vec<SqlServerDatabaseFileIoDelta>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct SqlServerProfileSample {
    repeat_index: usize,
    write_elapsed_start: Duration,
    write_elapsed_end: Duration,
    activity: sqlserver_profile::ActivitySnapshot,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct SqlServerSessionWaitDelta {
    wait_type: String,
    waiting_tasks_count: i64,
    wait_time_ms: i64,
    max_wait_time_ms: i64,
    signal_wait_time_ms: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct SqlServerDatabaseFileIoDelta {
    file_id: i32,
    logical_name: String,
    file_type: String,
    read_count: i64,
    read_bytes: i64,
    read_stall_ms: i64,
    write_count: i64,
    write_bytes: i64,
    write_stall_ms: i64,
}

#[derive(Debug, Default, Clone, PartialEq, Eq)]
struct SqlServerProfileSampleSummary {
    request_statuses: BTreeMap<String, usize>,
    request_waits: BTreeMap<String, usize>,
    waiting_task_waits: BTreeMap<String, usize>,
}

impl SqlServerProfileSampleSummary {
    fn from_samples(samples: &[SqlServerProfileSample]) -> Self {
        let mut summary = Self::default();

        for sample in samples {
            match &sample.activity.request {
                Some(request) => {
                    increment_sample_count(&mut summary.request_statuses, &request.status);
                    increment_sample_count(
                        &mut summary.request_waits,
                        request.wait_type.as_deref().unwrap_or("<none>"),
                    );
                }
                None => {
                    increment_sample_count(&mut summary.request_statuses, "<no request>");
                    increment_sample_count(&mut summary.request_waits, "<no request>");
                }
            }

            for waiting_task in &sample.activity.waiting_tasks {
                increment_sample_count(&mut summary.waiting_task_waits, &waiting_task.wait_type);
            }
        }

        summary
    }
}

fn increment_sample_count(distribution: &mut BTreeMap<String, usize>, value: &str) {
    let count = distribution.entry(value.to_owned()).or_default();
    *count = count.saturating_add(1);
}

fn sql_server_session_wait_deltas(
    initial: &[sqlserver_profile::SessionWaitSnapshot],
    final_waits: &[sqlserver_profile::SessionWaitSnapshot],
) -> Vec<SqlServerSessionWaitDelta> {
    let initial_waits = initial
        .iter()
        .map(|wait| (wait.wait_type.as_str(), wait))
        .collect::<BTreeMap<_, _>>();
    let mut deltas = final_waits
        .iter()
        .filter_map(|final_wait| {
            let initial_wait = initial_waits.get(final_wait.wait_type.as_str()).copied();
            let delta = SqlServerSessionWaitDelta {
                wait_type: final_wait.wait_type.clone(),
                waiting_tasks_count: counter_delta(
                    initial_wait.map_or(0, |wait| wait.waiting_tasks_count),
                    final_wait.waiting_tasks_count,
                ),
                wait_time_ms: counter_delta(
                    initial_wait.map_or(0, |wait| wait.wait_time_ms),
                    final_wait.wait_time_ms,
                ),
                max_wait_time_ms: final_wait.max_wait_time_ms,
                signal_wait_time_ms: counter_delta(
                    initial_wait.map_or(0, |wait| wait.signal_wait_time_ms),
                    final_wait.signal_wait_time_ms,
                ),
            };

            (delta.waiting_tasks_count > 0 || delta.wait_time_ms > 0).then_some(delta)
        })
        .collect::<Vec<_>>();
    deltas.sort_by(|left, right| {
        right
            .wait_time_ms
            .cmp(&left.wait_time_ms)
            .then_with(|| left.wait_type.cmp(&right.wait_type))
    });
    deltas
}

fn counter_delta(initial: i64, final_value: i64) -> i64 {
    final_value.saturating_sub(initial)
}

fn sql_server_database_file_io_deltas(
    initial: &[sqlserver_profile::DatabaseFileIoSnapshot],
    final_files: &[sqlserver_profile::DatabaseFileIoSnapshot],
) -> Vec<SqlServerDatabaseFileIoDelta> {
    let initial_files = initial
        .iter()
        .map(|file| (file.file_id, file))
        .collect::<BTreeMap<_, _>>();
    let mut deltas = final_files
        .iter()
        .map(|final_file| {
            let initial_file = initial_files.get(&final_file.file_id).copied();
            SqlServerDatabaseFileIoDelta {
                file_id: final_file.file_id,
                logical_name: final_file.logical_name.clone(),
                file_type: final_file.file_type.clone(),
                read_count: counter_delta(
                    initial_file.map_or(0, |file| file.read_count),
                    final_file.read_count,
                ),
                read_bytes: counter_delta(
                    initial_file.map_or(0, |file| file.read_bytes),
                    final_file.read_bytes,
                ),
                read_stall_ms: counter_delta(
                    initial_file.map_or(0, |file| file.read_stall_ms),
                    final_file.read_stall_ms,
                ),
                write_count: counter_delta(
                    initial_file.map_or(0, |file| file.write_count),
                    final_file.write_count,
                ),
                write_bytes: counter_delta(
                    initial_file.map_or(0, |file| file.write_bytes),
                    final_file.write_bytes,
                ),
                write_stall_ms: counter_delta(
                    initial_file.map_or(0, |file| file.write_stall_ms),
                    final_file.write_stall_ms,
                ),
            }
        })
        .collect::<Vec<_>>();
    deltas.sort_by(|left, right| {
        left.file_type
            .cmp(&right.file_type)
            .then_with(|| left.logical_name.cmp(&right.logical_name))
            .then_with(|| left.file_id.cmp(&right.file_id))
    });
    deltas
}

struct SqlServerProfileSession {
    target: SqlServerProfileTarget,
    observer: BenchClient,
    next_repeat_index: usize,
    initial_session_waits: Vec<sqlserver_profile::SessionWaitSnapshot>,
    initial_database_file_io: Vec<sqlserver_profile::DatabaseFileIoSnapshot>,
}

impl SqlServerProfileSession {
    async fn start(
        writer: &mut BenchClient,
        connection: &sqlserver::SqlServerConnection,
        options: SqlServerProfileOptions,
    ) -> Result<Self, WriterBenchError> {
        let writer_session_id = select_session_id(writer).await?;
        let mut observer =
            connect(&connection.connection_string, &connection.database, None).await?;
        let observer_session_id = select_session_id(&mut observer).await?;
        let initial_activity =
            sqlserver_profile::current_activity_snapshot(&mut observer, writer_session_id).await?;
        let initial_session_waits =
            sqlserver_profile::session_wait_snapshots(&mut observer, writer_session_id).await?;
        let initial_database_file_io =
            sqlserver_profile::database_file_io_snapshots(&mut observer).await?;

        Ok(Self {
            target: SqlServerProfileTarget {
                writer_session_id,
                observer_session_id,
                sample_interval: options.sample_interval,
                initial_activity,
                write_samples: Vec::new(),
                session_wait_deltas: Vec::new(),
                database_file_io_deltas: Vec::new(),
            },
            observer,
            next_repeat_index: 0,
            initial_session_waits,
            initial_database_file_io,
        })
    }

    fn target(&self) -> SqlServerProfileTarget {
        self.target.clone()
    }

    async fn finish(&mut self) -> Result<(), WriterBenchError> {
        let final_session_waits = sqlserver_profile::session_wait_snapshots(
            &mut self.observer,
            self.target.writer_session_id,
        )
        .await?;
        self.target.session_wait_deltas =
            sql_server_session_wait_deltas(&self.initial_session_waits, &final_session_waits);
        let final_database_file_io =
            sqlserver_profile::database_file_io_snapshots(&mut self.observer).await?;
        self.target.database_file_io_deltas = sql_server_database_file_io_deltas(
            &self.initial_database_file_io,
            &final_database_file_io,
        );
        Ok(())
    }

    async fn sample_write<T, F>(&mut self, write: F) -> Result<T, WriterBenchError>
    where
        F: Future<Output = Result<T, WriterBenchError>>,
    {
        let repeat_index = self.next_repeat_index;
        self.next_repeat_index = self.next_repeat_index.saturating_add(1);
        let started_at = Instant::now();
        let mut sample_interval = tokio::time::interval(self.target.sample_interval);
        sample_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        sample_interval.tick().await;
        tokio::pin!(write);

        loop {
            tokio::select! {
                result = &mut write => return result,
                _ = sample_interval.tick() => {
                    let write_elapsed_start = started_at.elapsed();
                    let activity = sqlserver_profile::current_activity_snapshot(
                        &mut self.observer,
                        self.target.writer_session_id,
                    )
                    .await?;
                    self.target.write_samples.push(SqlServerProfileSample {
                        repeat_index,
                        write_elapsed_start,
                        write_elapsed_end: started_at.elapsed(),
                        activity,
                    });
                }
            }
        }
    }
}

async fn run_baseline_async(
    options: &BaselineBenchOptions,
    connection: &sqlserver::SqlServerConnection,
) -> Result<TiberiusBenchReport, WriterBenchError> {
    let total_start = Instant::now();
    let setup_start = Instant::now();
    let ipc_dataset = prepare_baseline_ipc_dataset(options)?;
    let ipc_setup = setup_start.elapsed();

    let run_result = run_tiberius_benchmark_from_ipc(
        &options.benchmark,
        connection,
        &ipc_dataset.host_path,
        WriteBackend::BaselineTokenRow,
        false,
        options.tds_packet_size,
        None,
    )
    .await;
    let dataset_cleanup_result = ipc_dataset.cleanup();
    let mut report = run_result?;
    dataset_cleanup_result?;
    report.timings.setup += ipc_setup;
    report.timings.total = total_start.elapsed();
    Ok(report)
}

fn run_compare_benchmark(
    options: &CompareBenchOptions,
) -> Result<CompareBenchReport, WriterBenchError> {
    if options.profile_direct && !options.backends.contains(&BenchmarkBackend::DirectRaw) {
        return Err(WriterBenchError::Validation(
            "writer-bench compare --profile-direct requires the direct-raw backend".to_owned(),
        ));
    }

    if options.sql_server_profile.is_some()
        && !options.backends.iter().any(BenchmarkBackend::is_tiberius)
    {
        return Err(WriterBenchError::Validation(
            "writer-bench compare --profile-sqlserver requires the baseline or direct-raw backend"
                .to_owned(),
        ));
    }

    if options.backends.contains(&BenchmarkBackend::DirectRaw) {
        ensure_direct_raw_supported_scenario(&options.benchmark)?;
    }

    let network = create_compare_network(options)?;
    let connection = options
        .sql_server
        .connect_or_start_with_network(network.as_ref())
        .map_err(WriterBenchError::SqlServer)?;
    let mut runner_image = if options.backends.contains(&BenchmarkBackend::ArrowOdbc)
        || options.backends.contains(&BenchmarkBackend::OdbcBcp)
    {
        Some(build_compare_odbc_runner_image(options)?)
    } else {
        None
    };
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_io()
        .enable_time()
        .build()
        .map_err(WriterBenchError::Io)?;
    let ipc_dataset = prepare_compare_ipc_dataset(options)?;
    let run_result = (|| {
        let mut backends = Vec::new();

        if options.backends.contains(&BenchmarkBackend::Baseline) {
            let report = runtime.block_on(async {
                let backend_start = Instant::now();
                let mut report = run_tiberius_benchmark_from_ipc(
                    &options.benchmark,
                    &connection,
                    &ipc_dataset.host_path,
                    WriteBackend::BaselineTokenRow,
                    false,
                    options.tds_packet_size,
                    options.sql_server_profile,
                )
                .await?;
                report.timings.total = backend_start.elapsed();
                Ok::<_, WriterBenchError>(report)
            })?;
            backends.push(CompareBackendBenchReport::Baseline { report });
        }

        if options.backends.contains(&BenchmarkBackend::DirectRaw) {
            let report = runtime.block_on(async {
                let backend_start = Instant::now();
                let mut report = run_tiberius_benchmark_from_ipc(
                    &options.benchmark,
                    &connection,
                    &ipc_dataset.host_path,
                    WriteBackend::DirectRawBulk,
                    options.profile_direct,
                    options.tds_packet_size,
                    options.sql_server_profile,
                )
                .await?;
                report.timings.total = backend_start.elapsed();
                Ok::<_, WriterBenchError>(report)
            })?;
            backends.push(CompareBackendBenchReport::DirectRaw { report });
        }

        if options.backends.contains(&BenchmarkBackend::ArrowOdbc) {
            let runner_image = runner_image.as_ref().ok_or_else(|| {
                WriterBenchError::Validation(
                    "ODBC runner image was not prepared for arrow-odbc compare".to_owned(),
                )
            })?;
            let report = run_arrow_odbc_runner_for_benchmark_capture(
                &options.benchmark,
                runner_image,
                network.as_ref(),
                &connection,
                &ipc_dataset,
            )?;
            backends.push(CompareBackendBenchReport::ArrowOdbc { report });
        }

        if options.backends.contains(&BenchmarkBackend::OdbcBcp) {
            let runner_image = runner_image.as_ref().ok_or_else(|| {
                WriterBenchError::Validation(
                    "ODBC runner image was not prepared for odbc-bcp compare".to_owned(),
                )
            })?;
            let report = run_odbc_bcp_runner_for_benchmark_capture(
                &options.benchmark,
                runner_image,
                network.as_ref(),
                &connection,
                &ipc_dataset,
            )?;
            backends.push(CompareBackendBenchReport::OdbcBcp { report });
        }

        Ok(CompareBenchReport {
            ipc_dataset: ipc_dataset.host_path.clone(),
            database: connection.database.clone(),
            backends,
        })
    })();
    let dataset_cleanup_result = ipc_dataset.cleanup();
    let runner_cleanup_result = if let Some(runner_image) = runner_image.as_mut() {
        runner_image.cleanup().map_err(WriterBenchError::OdbcRunner)
    } else {
        Ok(())
    };
    let report = run_result?;
    dataset_cleanup_result?;
    runner_cleanup_result?;

    Ok(report)
}

async fn run_tiberius_benchmark_from_ipc(
    benchmark: &WriterBenchOptions,
    connection: &sqlserver::SqlServerConnection,
    ipc_path: &Path,
    backend: WriteBackend,
    profile_direct: bool,
    tds_packet_size: Option<u32>,
    sql_server_profile: Option<SqlServerProfileOptions>,
) -> Result<TiberiusBenchReport, WriterBenchError> {
    let mut report = TiberiusBenchReport::default();

    let setup_start = Instant::now();
    let mut client = connect(
        &connection.connection_string,
        &connection.database,
        tds_packet_size,
    )
    .await?;
    let mut sql_server_profile_session = if let Some(options) = sql_server_profile {
        Some(SqlServerProfileSession::start(&mut client, connection, options).await?)
    } else {
        None
    };
    let schema = (benchmark.scenario.schema)();
    let mappings = benchmark_mappings_for_schema(Arc::clone(&schema))?;
    report.timings.setup += setup_start.elapsed();

    for _repeat_index in 0..benchmark.repeat {
        let repeat_result = async {
            let table = unique_benchmark_table_name()?;
            let repeat_report =
                run_tiberius_repeat_from_ipc(
                    &mut client,
                    &mappings,
                    &table,
                    ipc_path,
                    backend,
                    profile_direct,
                    sql_server_profile_session.as_mut(),
                )
                .await;
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
            merge_direct_profile(&mut report.direct_profile, repeat_report.direct_profile);

            Ok(())
        }
        .await;
        repeat_result?;
    }

    report.peak_rss_kib = current_process_peak_rss_kib();
    if let Some(profile_session) = sql_server_profile_session.as_mut() {
        profile_session.finish().await?;
    }
    report.sql_server_profile_target = sql_server_profile_session
        .as_ref()
        .map(SqlServerProfileSession::target);

    Ok(report)
}

fn prepare_baseline_ipc_dataset(
    options: &BaselineBenchOptions,
) -> Result<ManagedIpcDataset, WriterBenchError> {
    let root = repository_root()?;
    let dataset_dir = root.join("target").join("arrow-tiberius-writer-bench");
    std::fs::create_dir_all(&dataset_dir).map_err(WriterBenchError::Io)?;

    let counter = BENCH_IPC_COUNTER.fetch_add(1, Ordering::Relaxed);
    let filename = format!(
        "baseline-{}-{}-{counter}.arrow",
        std::process::id(),
        options.benchmark.scenario.name
    );
    let host_path = dataset_dir.join(&filename);
    let summary = dataset::write_ipc_dataset(&options.benchmark, &host_path)?;

    println!("writer-bench baseline");
    println!("  action: prepare_ipc_dataset");
    println!("  path: {}", host_path.display());
    println!("  rows: {}", summary.rows);
    println!("  batches: {}", summary.batches);

    Ok(ManagedIpcDataset {
        host_path,
        container_path: None,
    })
}

async fn run_tiberius_repeat_from_ipc(
    client: &mut BenchClient,
    mappings: &[arrow_tiberius::SchemaMapping],
    table: &TableName,
    ipc_path: &Path,
    backend: WriteBackend,
    profile_direct: bool,
    sql_server_profile_session: Option<&mut SqlServerProfileSession>,
) -> Result<TiberiusBenchReport, WriterBenchError> {
    let batches =
        dataset::ipc_dataset_reader(ipc_path)?.map(|batch| batch.map_err(WriterBenchError::Arrow));

    run_tiberius_repeat_with_batches(
        client,
        mappings,
        table,
        batches,
        backend,
        profile_direct,
        sql_server_profile_session,
    )
    .await
}

async fn run_tiberius_repeat_with_batches(
    client: &mut BenchClient,
    mappings: &[arrow_tiberius::SchemaMapping],
    table: &TableName,
    batches: impl IntoIterator<Item = Result<RecordBatch, WriterBenchError>>,
    backend: WriteBackend,
    profile_direct: bool,
    mut sql_server_profile_session: Option<&mut SqlServerProfileSession>,
) -> Result<TiberiusBenchReport, WriterBenchError> {
    let mut report = TiberiusBenchReport::default();
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
            backend,
            ..WriteOptions::default()
        },
    )
    .await
    .map_err(WriterBenchError::ArrowTiberius)?;
    report.timings.setup += setup_start.elapsed();

    let profiling_direct = profile_direct && backend == WriteBackend::DirectRawBulk;
    if profiling_direct {
        arrow_tiberius::write::profile::start_direct_write_profile();
    }

    let write_batches = async {
        for batch in batches {
            let batch = batch?;
            report.stats = writer
                .write_batch(&batch)
                .await
                .map_err(WriterBenchError::ArrowTiberius)?;
        }

        Ok(())
    };
    let write_start = Instant::now();
    if let Some(profile_session) = sql_server_profile_session.as_mut() {
        profile_session.sample_write(write_batches).await?;
    } else {
        write_batches.await?;
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

    if profiling_direct {
        report.direct_profile = arrow_tiberius::write::profile::finish_direct_write_profile();
    }

    Ok(report)
}

fn merge_direct_profile(
    target: &mut Option<DirectWriteProfile>,
    source: Option<DirectWriteProfile>,
) {
    let Some(source) = source else {
        return;
    };

    let target = target.get_or_insert_with(DirectWriteProfile::default);
    target.measure_batch += source.measure_batch;
    target.row_range_split += source.row_range_split;
    target.append_encode += source.append_encode;
    target.send_total += source.send_total;
    target.rows = target.rows.saturating_add(source.rows);
    target.batches = target.batches.saturating_add(source.batches);
    target.row_ranges = target.row_ranges.saturating_add(source.row_ranges);
    target.encoded_bytes = target.encoded_bytes.saturating_add(source.encoded_bytes);
    target.max_row_range_bytes = target.max_row_range_bytes.max(source.max_row_range_bytes);
    target.nvarchar_utf16_bytes = target
        .nvarchar_utf16_bytes
        .saturating_add(source.nvarchar_utf16_bytes);
    target.varbinary_bytes = target
        .varbinary_bytes
        .saturating_add(source.varbinary_bytes);
    target.null_cells = target.null_cells.saturating_add(source.null_cells);
    target.packet_write_calls = target
        .packet_write_calls
        .saturating_add(source.packet_write_calls);
    target.packets_written = target
        .packets_written
        .saturating_add(source.packets_written);
    target.packet_payload_bytes = target
        .packet_payload_bytes
        .saturating_add(source.packet_payload_bytes);
    target.max_packet_payload_bytes = target
        .max_packet_payload_bytes
        .max(source.max_packet_payload_bytes);
    target.max_buffered_bytes_before_write = target
        .max_buffered_bytes_before_write
        .max(source.max_buffered_bytes_before_write);
    target.buffered_bytes_after_last_write = source.buffered_bytes_after_last_write;
    target.finalized_packet_payload_bytes = target
        .finalized_packet_payload_bytes
        .saturating_add(source.finalized_packet_payload_bytes);
    target.bulk_write_packets_elapsed += source.bulk_write_packets_elapsed;
    target.bulk_write_to_wire_calls = target
        .bulk_write_to_wire_calls
        .saturating_add(source.bulk_write_to_wire_calls);
    target.bulk_write_to_wire_elapsed += source.bulk_write_to_wire_elapsed;
    target.bulk_write_to_wire_payload_bytes = target
        .bulk_write_to_wire_payload_bytes
        .saturating_add(source.bulk_write_to_wire_payload_bytes);
    target.bulk_max_write_to_wire_elapsed = target
        .bulk_max_write_to_wire_elapsed
        .max(source.bulk_max_write_to_wire_elapsed);
    target.bulk_max_write_to_wire_payload_bytes = target
        .bulk_max_write_to_wire_payload_bytes
        .max(source.bulk_max_write_to_wire_payload_bytes);
    target.bulk_flush_calls = target
        .bulk_flush_calls
        .saturating_add(source.bulk_flush_calls);
    target.bulk_flush_elapsed += source.bulk_flush_elapsed;
    target.bulk_max_flush_elapsed = target
        .bulk_max_flush_elapsed
        .max(source.bulk_max_flush_elapsed);
    target.bulk_finalize_elapsed += source.bulk_finalize_elapsed;
    target.bulk_finalize_write_to_wire_elapsed += source.bulk_finalize_write_to_wire_elapsed;
    target.bulk_finalize_flush_elapsed += source.bulk_finalize_flush_elapsed;
    target.bulk_finalize_result_elapsed += source.bulk_finalize_result_elapsed;
    target.bulk_connection_write_calls = target
        .bulk_connection_write_calls
        .saturating_add(source.bulk_connection_write_calls);
    target.bulk_connection_write_payload_bytes = target
        .bulk_connection_write_payload_bytes
        .saturating_add(source.bulk_connection_write_payload_bytes);
    target.bulk_connection_write_ready_elapsed += source.bulk_connection_write_ready_elapsed;
    target.bulk_connection_write_encode_elapsed += source.bulk_connection_write_encode_elapsed;
    target.bulk_connection_write_flush_elapsed += source.bulk_connection_write_flush_elapsed;
    target.bulk_connection_write_max_ready_elapsed = target
        .bulk_connection_write_max_ready_elapsed
        .max(source.bulk_connection_write_max_ready_elapsed);
    target.bulk_connection_write_max_encode_elapsed = target
        .bulk_connection_write_max_encode_elapsed
        .max(source.bulk_connection_write_max_encode_elapsed);
    target.bulk_connection_write_max_flush_elapsed = target
        .bulk_connection_write_max_flush_elapsed
        .max(source.bulk_connection_write_max_flush_elapsed);
    target.bulk_connection_write_max_payload_bytes = target
        .bulk_connection_write_max_payload_bytes
        .max(source.bulk_connection_write_max_payload_bytes);
    target.bulk_direct_packet_write_calls = target
        .bulk_direct_packet_write_calls
        .saturating_add(source.bulk_direct_packet_write_calls);
    target.bulk_direct_packet_payload_bytes = target
        .bulk_direct_packet_payload_bytes
        .saturating_add(source.bulk_direct_packet_payload_bytes);
    target.bulk_direct_packet_header_bytes = target
        .bulk_direct_packet_header_bytes
        .saturating_add(source.bulk_direct_packet_header_bytes);
    target.bulk_direct_packet_max_payload_bytes = target
        .bulk_direct_packet_max_payload_bytes
        .max(source.bulk_direct_packet_max_payload_bytes);
    target.bulk_direct_packet_final_calls = target
        .bulk_direct_packet_final_calls
        .saturating_add(source.bulk_direct_packet_final_calls);
    target.bulk_direct_packet_final_payload_bytes = target
        .bulk_direct_packet_final_payload_bytes
        .saturating_add(source.bulk_direct_packet_final_payload_bytes);
    target.bulk_direct_packet_final_header_bytes = target
        .bulk_direct_packet_final_header_bytes
        .saturating_add(source.bulk_direct_packet_final_header_bytes);
    target.bulk_direct_packet_raw_stream_calls = target
        .bulk_direct_packet_raw_stream_calls
        .saturating_add(source.bulk_direct_packet_raw_stream_calls);
    target.bulk_direct_packet_tls_stream_calls = target
        .bulk_direct_packet_tls_stream_calls
        .saturating_add(source.bulk_direct_packet_tls_stream_calls);
    target.bulk_direct_packet_low_level_write_calls = target
        .bulk_direct_packet_low_level_write_calls
        .saturating_add(source.bulk_direct_packet_low_level_write_calls);
    target.bulk_direct_packet_low_level_write_bytes = target
        .bulk_direct_packet_low_level_write_bytes
        .saturating_add(source.bulk_direct_packet_low_level_write_bytes);
    target.bulk_direct_packet_max_low_level_write_bytes = target
        .bulk_direct_packet_max_low_level_write_bytes
        .max(source.bulk_direct_packet_max_low_level_write_bytes);
    target.bulk_direct_packet_write_elapsed += source.bulk_direct_packet_write_elapsed;
    target.bulk_direct_packet_max_write_elapsed = target
        .bulk_direct_packet_max_write_elapsed
        .max(source.bulk_direct_packet_max_write_elapsed);
    target.bulk_direct_packet_header_write_calls = target
        .bulk_direct_packet_header_write_calls
        .saturating_add(source.bulk_direct_packet_header_write_calls);
    target.bulk_direct_packet_header_write_bytes = target
        .bulk_direct_packet_header_write_bytes
        .saturating_add(source.bulk_direct_packet_header_write_bytes);
    target.bulk_direct_packet_header_max_write_bytes = target
        .bulk_direct_packet_header_max_write_bytes
        .max(source.bulk_direct_packet_header_max_write_bytes);
    target.bulk_direct_packet_header_write_elapsed +=
        source.bulk_direct_packet_header_write_elapsed;
    target.bulk_direct_packet_header_max_write_elapsed = target
        .bulk_direct_packet_header_max_write_elapsed
        .max(source.bulk_direct_packet_header_max_write_elapsed);
    target.bulk_direct_packet_header_partial_writes = target
        .bulk_direct_packet_header_partial_writes
        .saturating_add(source.bulk_direct_packet_header_partial_writes);
    target.bulk_direct_packet_payload_write_calls = target
        .bulk_direct_packet_payload_write_calls
        .saturating_add(source.bulk_direct_packet_payload_write_calls);
    target.bulk_direct_packet_payload_write_bytes = target
        .bulk_direct_packet_payload_write_bytes
        .saturating_add(source.bulk_direct_packet_payload_write_bytes);
    target.bulk_direct_packet_payload_max_write_bytes = target
        .bulk_direct_packet_payload_max_write_bytes
        .max(source.bulk_direct_packet_payload_max_write_bytes);
    target.bulk_direct_packet_payload_write_elapsed +=
        source.bulk_direct_packet_payload_write_elapsed;
    target.bulk_direct_packet_payload_max_write_elapsed = target
        .bulk_direct_packet_payload_max_write_elapsed
        .max(source.bulk_direct_packet_payload_max_write_elapsed);
    target.bulk_direct_packet_payload_partial_writes = target
        .bulk_direct_packet_payload_partial_writes
        .saturating_add(source.bulk_direct_packet_payload_partial_writes);
    target.bulk_direct_packet_poll_write_polls = target
        .bulk_direct_packet_poll_write_polls
        .saturating_add(source.bulk_direct_packet_poll_write_polls);
    target.bulk_direct_packet_poll_write_pending_count = target
        .bulk_direct_packet_poll_write_pending_count
        .saturating_add(source.bulk_direct_packet_poll_write_pending_count);
    target.bulk_direct_packet_poll_write_pending_elapsed +=
        source.bulk_direct_packet_poll_write_pending_elapsed;
    target.bulk_direct_packet_poll_write_max_pending_elapsed = target
        .bulk_direct_packet_poll_write_max_pending_elapsed
        .max(source.bulk_direct_packet_poll_write_max_pending_elapsed);
    target.bulk_direct_packet_poll_write_ready_count = target
        .bulk_direct_packet_poll_write_ready_count
        .saturating_add(source.bulk_direct_packet_poll_write_ready_count);
    target.bulk_direct_packet_poll_write_ready_elapsed +=
        source.bulk_direct_packet_poll_write_ready_elapsed;
    target.bulk_direct_packet_poll_write_max_ready_elapsed = target
        .bulk_direct_packet_poll_write_max_ready_elapsed
        .max(source.bulk_direct_packet_poll_write_max_ready_elapsed);
    target.bulk_direct_packet_flush_calls = target
        .bulk_direct_packet_flush_calls
        .saturating_add(source.bulk_direct_packet_flush_calls);
    target.bulk_direct_packet_flush_elapsed += source.bulk_direct_packet_flush_elapsed;
    target.bulk_direct_packet_max_flush_elapsed = target
        .bulk_direct_packet_max_flush_elapsed
        .max(source.bulk_direct_packet_max_flush_elapsed);
    target.bulk_direct_packet_flush_pending_count = target
        .bulk_direct_packet_flush_pending_count
        .saturating_add(source.bulk_direct_packet_flush_pending_count);
    target.bulk_direct_packet_flush_pending_elapsed +=
        source.bulk_direct_packet_flush_pending_elapsed;
    target.bulk_direct_packet_flush_max_pending_elapsed = target
        .bulk_direct_packet_flush_max_pending_elapsed
        .max(source.bulk_direct_packet_flush_max_pending_elapsed);
}

async fn connect(
    connection_string: &str,
    database: &str,
    tds_packet_size: Option<u32>,
) -> Result<BenchClient, WriterBenchError> {
    let connection_string =
        tiberius_connection_string(connection_string, database, tds_packet_size);
    let config = tiberius::Config::from_ado_string(&connection_string)
        .map_err(WriterBenchError::Tiberius)?;
    let tcp = TcpStream::connect(config.get_addr())
        .await
        .map_err(WriterBenchError::Io)?;

    tiberius::Client::connect(config, tcp.compat_write())
        .await
        .map_err(WriterBenchError::Tiberius)
}

fn tiberius_connection_string(
    connection_string: &str,
    database: &str,
    tds_packet_size: Option<u32>,
) -> String {
    let mut connection_string = format!("{connection_string};database={database}");
    if let Some(tds_packet_size) = tds_packet_size {
        connection_string.push_str(&format!(";Packet Size={tds_packet_size}"));
    }
    connection_string
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

async fn select_session_id(client: &mut BenchClient) -> Result<i32, WriterBenchError> {
    let row = client
        .simple_query("SELECT CONVERT(int, @@SPID)")
        .await
        .map_err(WriterBenchError::Tiberius)?
        .into_row()
        .await
        .map_err(WriterBenchError::Tiberius)?
        .ok_or_else(|| {
            WriterBenchError::Validation("session id query returned no row".to_owned())
        })?;

    row.try_get::<i32, _>(0)
        .map_err(WriterBenchError::Tiberius)?
        .ok_or_else(|| WriterBenchError::Validation("session id query returned null".to_owned()))
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

fn parse_positive_u32(option: &'static str, value: &str) -> Result<u32, WriterBenchError> {
    let parsed = value
        .parse::<u32>()
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

fn parse_positive_u64(option: &'static str, value: &str) -> Result<u64, WriterBenchError> {
    let parsed = value
        .parse::<u64>()
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
    use super::{
        BenchmarkOutput, DirectWriteProfile, SqlServerProfileSample, SqlServerProfileSampleSummary,
        WriterBenchError, WriterBenchOptions, sqlserver_profile,
    };
    use arrow_array::{
        Array, BinaryArray, BooleanArray, Date32Array, Decimal128Array, Float64Array, Int32Array,
        Int64Array, RecordBatch, StringArray, TimestampMillisecondArray,
    };
    use arrow_schema::{DataType, TimeUnit};
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::{ffi::OsString, time::Duration};

    static TEST_FILE_COUNTER: AtomicU64 = AtomicU64::new(0);

    fn sql_server_profile_sample(
        status_and_wait: Option<(&str, Option<&str>)>,
        waiting_task_waits: &[&str],
    ) -> SqlServerProfileSample {
        SqlServerProfileSample {
            repeat_index: 0,
            write_elapsed_start: Duration::from_millis(10),
            write_elapsed_end: Duration::from_millis(11),
            activity: sqlserver_profile::ActivitySnapshot {
                connection: sqlserver_profile::ConnectionSnapshot {
                    net_transport: "TCP".to_owned(),
                    protocol_type: "TSQL".to_owned(),
                    encrypt_option: "FALSE".to_owned(),
                    net_packet_size: 4096,
                    num_reads: 3,
                    num_writes: 5,
                },
                request: status_and_wait.map(|(status, wait_type)| {
                    sqlserver_profile::RequestSnapshot {
                        status: status.to_owned(),
                        command: "INSERT BULK".to_owned(),
                        wait_type: wait_type.map(ToOwned::to_owned),
                        wait_time_ms: 0,
                        last_wait_type: "SOS_SCHEDULER_YIELD".to_owned(),
                        wait_resource: String::new(),
                        blocking_session_id: 0,
                        cpu_time_ms: 7,
                        total_elapsed_time_ms: 9,
                        reads: 11,
                        writes: 13,
                        logical_reads: 17,
                        open_transaction_count: 1,
                    }
                }),
                waiting_tasks: waiting_task_waits
                    .iter()
                    .enumerate()
                    .map(
                        |(exec_context_id, wait_type)| sqlserver_profile::WaitingTaskSnapshot {
                            exec_context_id: exec_context_id as i32,
                            wait_type: (*wait_type).to_owned(),
                            wait_duration_ms: 19,
                            blocking_session_id: None,
                            resource_description: None,
                        },
                    )
                    .collect(),
            },
        }
    }

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
    fn sql_server_profile_summary_counts_request_and_task_wait_samples() {
        let samples = [
            sql_server_profile_sample(
                Some(("running", Some("ASYNC_NETWORK_IO"))),
                &["ASYNC_NETWORK_IO", "WRITELOG"],
            ),
            sql_server_profile_sample(Some(("running", None)), &["ASYNC_NETWORK_IO"]),
            sql_server_profile_sample(None, &[]),
        ];

        let summary = SqlServerProfileSampleSummary::from_samples(&samples);

        assert_eq!(summary.request_statuses.get("running"), Some(&2));
        assert_eq!(summary.request_statuses.get("<no request>"), Some(&1));
        assert_eq!(summary.request_waits.get("ASYNC_NETWORK_IO"), Some(&1));
        assert_eq!(summary.request_waits.get("<none>"), Some(&1));
        assert_eq!(summary.request_waits.get("<no request>"), Some(&1));
        assert_eq!(summary.waiting_task_waits.get("ASYNC_NETWORK_IO"), Some(&2));
        assert_eq!(summary.waiting_task_waits.get("WRITELOG"), Some(&1));
    }

    #[test]
    fn sql_server_profile_wait_deltas_exclude_initial_session_totals() {
        let initial = [
            sqlserver_profile::SessionWaitSnapshot {
                wait_type: "ASYNC_NETWORK_IO".to_owned(),
                waiting_tasks_count: 3,
                wait_time_ms: 5,
                max_wait_time_ms: 4,
                signal_wait_time_ms: 1,
            },
            sqlserver_profile::SessionWaitSnapshot {
                wait_type: "UNCHANGED".to_owned(),
                waiting_tasks_count: 8,
                wait_time_ms: 13,
                max_wait_time_ms: 9,
                signal_wait_time_ms: 2,
            },
        ];
        let final_waits = [
            sqlserver_profile_wait("WRITELOG", 4, 23, 17, 3),
            sqlserver_profile_wait("ASYNC_NETWORK_IO", 5, 16, 7, 4),
            sqlserver_profile_wait("UNCHANGED", 8, 13, 9, 2),
        ];

        let waits = super::sql_server_session_wait_deltas(&initial, &final_waits);

        assert_eq!(
            waits,
            [
                super::SqlServerSessionWaitDelta {
                    wait_type: "WRITELOG".to_owned(),
                    waiting_tasks_count: 4,
                    wait_time_ms: 23,
                    max_wait_time_ms: 17,
                    signal_wait_time_ms: 3,
                },
                super::SqlServerSessionWaitDelta {
                    wait_type: "ASYNC_NETWORK_IO".to_owned(),
                    waiting_tasks_count: 2,
                    wait_time_ms: 11,
                    max_wait_time_ms: 7,
                    signal_wait_time_ms: 3,
                },
            ]
        );
    }

    fn sqlserver_profile_wait(
        wait_type: &str,
        waiting_tasks_count: i64,
        wait_time_ms: i64,
        max_wait_time_ms: i64,
        signal_wait_time_ms: i64,
    ) -> sqlserver_profile::SessionWaitSnapshot {
        sqlserver_profile::SessionWaitSnapshot {
            wait_type: wait_type.to_owned(),
            waiting_tasks_count,
            wait_time_ms,
            max_wait_time_ms,
            signal_wait_time_ms,
        }
    }

    #[test]
    fn sql_server_profile_file_io_deltas_preserve_file_metadata() {
        let initial = [
            sqlserver_profile_file_io(2, "bench_log", "LOG", 1, 8, 2, 3, 64, 5),
            sqlserver_profile_file_io(1, "bench_data", "ROWS", 5, 40, 3, 7, 96, 11),
        ];
        let final_files = [
            sqlserver_profile_file_io(2, "bench_log", "LOG", 1, 8, 2, 9, 192, 17),
            sqlserver_profile_file_io(1, "bench_data", "ROWS", 8, 88, 7, 11, 160, 19),
        ];

        let files = super::sql_server_database_file_io_deltas(&initial, &final_files);

        assert_eq!(
            files,
            [
                super::SqlServerDatabaseFileIoDelta {
                    file_id: 2,
                    logical_name: "bench_log".to_owned(),
                    file_type: "LOG".to_owned(),
                    read_count: 0,
                    read_bytes: 0,
                    read_stall_ms: 0,
                    write_count: 6,
                    write_bytes: 128,
                    write_stall_ms: 12,
                },
                super::SqlServerDatabaseFileIoDelta {
                    file_id: 1,
                    logical_name: "bench_data".to_owned(),
                    file_type: "ROWS".to_owned(),
                    read_count: 3,
                    read_bytes: 48,
                    read_stall_ms: 4,
                    write_count: 4,
                    write_bytes: 64,
                    write_stall_ms: 8,
                },
            ]
        );
    }

    #[allow(clippy::too_many_arguments)]
    fn sqlserver_profile_file_io(
        file_id: i32,
        logical_name: &str,
        file_type: &str,
        read_count: i64,
        read_bytes: i64,
        read_stall_ms: i64,
        write_count: i64,
        write_bytes: i64,
        write_stall_ms: i64,
    ) -> sqlserver_profile::DatabaseFileIoSnapshot {
        sqlserver_profile::DatabaseFileIoSnapshot {
            file_id,
            logical_name: logical_name.to_owned(),
            file_type: file_type.to_owned(),
            read_count,
            read_bytes,
            read_stall_ms,
            write_count,
            write_bytes,
            write_stall_ms,
        }
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
            OsString::from("--tds-packet-size"),
            OsString::from("32767"),
        ];

        let options = super::BaselineBenchOptions::parse(&args).unwrap();

        assert_eq!(options.benchmark.rows, 10);
        assert_eq!(options.benchmark.batch_size, 4);
        assert_eq!(options.benchmark.scenario.name, "mixed_nullable");
        assert_eq!(options.sql_server.database, "bench_db");
        assert!(options.sql_server.connection_string.is_some());
        assert_eq!(options.tds_packet_size, Some(32767));
    }

    #[test]
    fn parses_arrow_odbc_runner_image_options() {
        let args = [
            OsString::from("--runner-image"),
            OsString::from("custom-odbc-runner:test"),
            OsString::from("--keep-runner-image"),
            OsString::from("--container-runtime"),
            OsString::from("podman"),
        ];

        let options = super::ArrowOdbcBenchOptions::parse(&args).unwrap();

        assert_eq!(options.runner_image, "custom-odbc-runner:test");
        assert!(options.keep_runner_image);
        assert_eq!(
            options.sql_server.container_runtime,
            Some(PathBuf::from("podman"))
        );
    }

    #[test]
    fn parses_compare_command_with_backend_selection() {
        let args = [
            OsString::from("--rows"),
            OsString::from("25"),
            OsString::from("--batch-size"),
            OsString::from("5"),
            OsString::from("--scenario"),
            OsString::from("mixed_nullable"),
            OsString::from("--repeat"),
            OsString::from("3"),
            OsString::from("--backends"),
            OsString::from("baseline,arrow-odbc"),
            OsString::from("--runner-image"),
            OsString::from("custom-odbc-runner:test"),
            OsString::from("--keep-runner-image"),
            OsString::from("--connection-string"),
            OsString::from("server=tcp:127.0.0.1,1433;password=secret"),
            OsString::from("--database"),
            OsString::from("bench_db"),
            OsString::from("--tds-packet-size"),
            OsString::from("32767"),
        ];

        let options = super::CompareBenchOptions::parse(&args).unwrap();

        assert_eq!(options.benchmark.rows, 25);
        assert_eq!(options.benchmark.batch_size, 5);
        assert_eq!(options.benchmark.scenario.name, "mixed_nullable");
        assert_eq!(options.benchmark.repeat, 3);
        assert_eq!(
            options.backends,
            [
                super::BenchmarkBackend::Baseline,
                super::BenchmarkBackend::ArrowOdbc
            ]
        );
        assert_eq!(options.runner_image, "custom-odbc-runner:test");
        assert!(options.keep_runner_image);
        assert_eq!(options.sql_server.database, "bench_db");
        assert!(options.sql_server.connection_string.is_some());
        assert_eq!(options.tds_packet_size, Some(32767));
    }

    #[test]
    fn rejects_invalid_tds_packet_size() {
        let args = [
            OsString::from("--backends"),
            OsString::from("direct-raw"),
            OsString::from("--tds-packet-size"),
            OsString::from("0"),
        ];

        let err = super::CompareBenchOptions::parse(&args).unwrap_err();

        assert!(matches!(
            err,
            WriterBenchError::InvalidPositiveInteger {
                option: "--tds-packet-size",
                value
            } if value == "0"
        ));
    }

    #[test]
    fn parses_compare_command_with_odbc_bcp_backend() {
        let args = [
            OsString::from("--scenario"),
            OsString::from("narrow_numeric"),
            OsString::from("--backends"),
            OsString::from("baseline,direct-raw,arrow-odbc,odbc-bcp"),
        ];

        let options = super::CompareBenchOptions::parse(&args).unwrap();

        assert_eq!(
            options.backends,
            [
                super::BenchmarkBackend::Baseline,
                super::BenchmarkBackend::DirectRaw,
                super::BenchmarkBackend::ArrowOdbc,
                super::BenchmarkBackend::OdbcBcp
            ]
        );
    }

    #[test]
    fn parses_compare_direct_profile_flag() {
        let args = [
            OsString::from("--backends"),
            OsString::from("direct-raw"),
            OsString::from("--profile-direct"),
        ];

        let options = super::CompareBenchOptions::parse(&args).unwrap();

        assert!(options.profile_direct);
    }

    #[test]
    fn merge_direct_profile_preserves_direct_packet_stats() {
        let mut target = Some(DirectWriteProfile {
            bulk_direct_packet_write_calls: 2,
            bulk_direct_packet_payload_bytes: 3,
            bulk_direct_packet_header_bytes: 5,
            bulk_direct_packet_max_payload_bytes: 7,
            bulk_direct_packet_final_calls: 11,
            bulk_direct_packet_final_payload_bytes: 13,
            bulk_direct_packet_final_header_bytes: 17,
            bulk_direct_packet_raw_stream_calls: 19,
            bulk_direct_packet_tls_stream_calls: 23,
            bulk_direct_packet_low_level_write_calls: 7,
            bulk_direct_packet_low_level_write_bytes: 11,
            bulk_direct_packet_max_low_level_write_bytes: 13,
            bulk_direct_packet_write_elapsed: Duration::from_millis(17),
            bulk_direct_packet_max_write_elapsed: Duration::from_millis(19),
            bulk_direct_packet_header_write_calls: 29,
            bulk_direct_packet_header_write_bytes: 31,
            bulk_direct_packet_header_max_write_bytes: 37,
            bulk_direct_packet_header_write_elapsed: Duration::from_millis(41),
            bulk_direct_packet_header_max_write_elapsed: Duration::from_millis(43),
            bulk_direct_packet_header_partial_writes: 47,
            bulk_direct_packet_payload_write_calls: 53,
            bulk_direct_packet_payload_write_bytes: 59,
            bulk_direct_packet_payload_max_write_bytes: 61,
            bulk_direct_packet_payload_write_elapsed: Duration::from_millis(67),
            bulk_direct_packet_payload_max_write_elapsed: Duration::from_millis(71),
            bulk_direct_packet_payload_partial_writes: 73,
            bulk_direct_packet_poll_write_polls: 79,
            bulk_direct_packet_poll_write_pending_count: 83,
            bulk_direct_packet_poll_write_pending_elapsed: Duration::from_millis(89),
            bulk_direct_packet_poll_write_max_pending_elapsed: Duration::from_millis(97),
            bulk_direct_packet_poll_write_ready_count: 101,
            bulk_direct_packet_poll_write_ready_elapsed: Duration::from_millis(103),
            bulk_direct_packet_poll_write_max_ready_elapsed: Duration::from_millis(107),
            bulk_direct_packet_flush_calls: 23,
            bulk_direct_packet_flush_elapsed: Duration::from_millis(29),
            bulk_direct_packet_max_flush_elapsed: Duration::from_millis(31),
            bulk_direct_packet_flush_pending_count: 109,
            bulk_direct_packet_flush_pending_elapsed: Duration::from_millis(113),
            bulk_direct_packet_flush_max_pending_elapsed: Duration::from_millis(127),
            ..DirectWriteProfile::default()
        });
        let source = DirectWriteProfile {
            bulk_direct_packet_write_calls: 37,
            bulk_direct_packet_payload_bytes: 41,
            bulk_direct_packet_header_bytes: 43,
            bulk_direct_packet_max_payload_bytes: 47,
            bulk_direct_packet_final_calls: 53,
            bulk_direct_packet_final_payload_bytes: 59,
            bulk_direct_packet_final_header_bytes: 61,
            bulk_direct_packet_raw_stream_calls: 67,
            bulk_direct_packet_tls_stream_calls: 71,
            bulk_direct_packet_low_level_write_calls: 47,
            bulk_direct_packet_low_level_write_bytes: 53,
            bulk_direct_packet_max_low_level_write_bytes: 59,
            bulk_direct_packet_write_elapsed: Duration::from_millis(61),
            bulk_direct_packet_max_write_elapsed: Duration::from_millis(67),
            bulk_direct_packet_header_write_calls: 73,
            bulk_direct_packet_header_write_bytes: 79,
            bulk_direct_packet_header_max_write_bytes: 83,
            bulk_direct_packet_header_write_elapsed: Duration::from_millis(89),
            bulk_direct_packet_header_max_write_elapsed: Duration::from_millis(97),
            bulk_direct_packet_header_partial_writes: 101,
            bulk_direct_packet_payload_write_calls: 103,
            bulk_direct_packet_payload_write_bytes: 107,
            bulk_direct_packet_payload_max_write_bytes: 109,
            bulk_direct_packet_payload_write_elapsed: Duration::from_millis(113),
            bulk_direct_packet_payload_max_write_elapsed: Duration::from_millis(127),
            bulk_direct_packet_payload_partial_writes: 131,
            bulk_direct_packet_poll_write_polls: 137,
            bulk_direct_packet_poll_write_pending_count: 139,
            bulk_direct_packet_poll_write_pending_elapsed: Duration::from_millis(149),
            bulk_direct_packet_poll_write_max_pending_elapsed: Duration::from_millis(151),
            bulk_direct_packet_poll_write_ready_count: 157,
            bulk_direct_packet_poll_write_ready_elapsed: Duration::from_millis(163),
            bulk_direct_packet_poll_write_max_ready_elapsed: Duration::from_millis(167),
            bulk_direct_packet_flush_calls: 71,
            bulk_direct_packet_flush_elapsed: Duration::from_millis(73),
            bulk_direct_packet_max_flush_elapsed: Duration::from_millis(79),
            bulk_direct_packet_flush_pending_count: 173,
            bulk_direct_packet_flush_pending_elapsed: Duration::from_millis(179),
            bulk_direct_packet_flush_max_pending_elapsed: Duration::from_millis(181),
            ..DirectWriteProfile::default()
        };

        super::merge_direct_profile(&mut target, Some(source));

        let profile = target.unwrap();
        assert_eq!(profile.bulk_direct_packet_write_calls, 39);
        assert_eq!(profile.bulk_direct_packet_payload_bytes, 44);
        assert_eq!(profile.bulk_direct_packet_header_bytes, 48);
        assert_eq!(profile.bulk_direct_packet_max_payload_bytes, 47);
        assert_eq!(profile.bulk_direct_packet_final_calls, 64);
        assert_eq!(profile.bulk_direct_packet_final_payload_bytes, 72);
        assert_eq!(profile.bulk_direct_packet_final_header_bytes, 78);
        assert_eq!(profile.bulk_direct_packet_raw_stream_calls, 86);
        assert_eq!(profile.bulk_direct_packet_tls_stream_calls, 94);
        assert_eq!(profile.bulk_direct_packet_low_level_write_calls, 54);
        assert_eq!(profile.bulk_direct_packet_low_level_write_bytes, 64);
        assert_eq!(profile.bulk_direct_packet_max_low_level_write_bytes, 59);
        assert_eq!(
            profile.bulk_direct_packet_write_elapsed,
            Duration::from_millis(78)
        );
        assert_eq!(
            profile.bulk_direct_packet_max_write_elapsed,
            Duration::from_millis(67)
        );
        assert_eq!(profile.bulk_direct_packet_header_write_calls, 102);
        assert_eq!(profile.bulk_direct_packet_header_write_bytes, 110);
        assert_eq!(profile.bulk_direct_packet_header_max_write_bytes, 83);
        assert_eq!(
            profile.bulk_direct_packet_header_write_elapsed,
            Duration::from_millis(130)
        );
        assert_eq!(
            profile.bulk_direct_packet_header_max_write_elapsed,
            Duration::from_millis(97)
        );
        assert_eq!(profile.bulk_direct_packet_header_partial_writes, 148);
        assert_eq!(profile.bulk_direct_packet_payload_write_calls, 156);
        assert_eq!(profile.bulk_direct_packet_payload_write_bytes, 166);
        assert_eq!(profile.bulk_direct_packet_payload_max_write_bytes, 109);
        assert_eq!(
            profile.bulk_direct_packet_payload_write_elapsed,
            Duration::from_millis(180)
        );
        assert_eq!(
            profile.bulk_direct_packet_payload_max_write_elapsed,
            Duration::from_millis(127)
        );
        assert_eq!(profile.bulk_direct_packet_payload_partial_writes, 204);
        assert_eq!(profile.bulk_direct_packet_poll_write_polls, 216);
        assert_eq!(profile.bulk_direct_packet_poll_write_pending_count, 222);
        assert_eq!(
            profile.bulk_direct_packet_poll_write_pending_elapsed,
            Duration::from_millis(238)
        );
        assert_eq!(
            profile.bulk_direct_packet_poll_write_max_pending_elapsed,
            Duration::from_millis(151)
        );
        assert_eq!(profile.bulk_direct_packet_poll_write_ready_count, 258);
        assert_eq!(
            profile.bulk_direct_packet_poll_write_ready_elapsed,
            Duration::from_millis(266)
        );
        assert_eq!(
            profile.bulk_direct_packet_poll_write_max_ready_elapsed,
            Duration::from_millis(167)
        );
        assert_eq!(profile.bulk_direct_packet_flush_calls, 94);
        assert_eq!(
            profile.bulk_direct_packet_flush_elapsed,
            Duration::from_millis(102)
        );
        assert_eq!(
            profile.bulk_direct_packet_max_flush_elapsed,
            Duration::from_millis(79)
        );
        assert_eq!(profile.bulk_direct_packet_flush_pending_count, 282);
        assert_eq!(
            profile.bulk_direct_packet_flush_pending_elapsed,
            Duration::from_millis(292)
        );
        assert_eq!(
            profile.bulk_direct_packet_flush_max_pending_elapsed,
            Duration::from_millis(181)
        );
    }

    #[test]
    fn compare_defaults_to_both_initial_backends() {
        let options = super::CompareBenchOptions::parse(&[]).unwrap();

        assert_eq!(
            options.backends,
            [
                super::BenchmarkBackend::Baseline,
                super::BenchmarkBackend::ArrowOdbc
            ]
        );
        assert_eq!(options.benchmark.scenario.name, "narrow_numeric");
        assert!(!options.profile_direct);
        assert!(options.sql_server_profile.is_none());
    }

    #[test]
    fn parses_compare_sql_server_profile_options() {
        let args = [
            OsString::from("--profile-sqlserver"),
            OsString::from("--sqlserver-profile-sample-ms"),
            OsString::from("125"),
        ];

        let options = super::CompareBenchOptions::parse(&args).unwrap();
        let profile = options.sql_server_profile.unwrap();

        assert_eq!(
            profile.sample_interval,
            std::time::Duration::from_millis(125)
        );
    }

    #[test]
    fn compare_rejects_sql_server_sample_interval_without_profile_flag() {
        let args = [
            OsString::from("--sqlserver-profile-sample-ms"),
            OsString::from("500"),
        ];

        let err = super::CompareBenchOptions::parse(&args).unwrap_err();

        assert!(matches!(
            err,
            WriterBenchError::Validation(message)
                if message.contains("--sqlserver-profile-sample-ms")
                    && message.contains("--profile-sqlserver")
        ));
    }

    #[test]
    fn compare_sql_server_profile_uses_default_sample_interval() {
        let args = [OsString::from("--profile-sqlserver")];

        let options = super::CompareBenchOptions::parse(&args).unwrap();
        let profile = options.sql_server_profile.unwrap();

        assert_eq!(
            profile.sample_interval,
            std::time::Duration::from_millis(super::DEFAULT_SQLSERVER_PROFILE_SAMPLE_MS)
        );
    }

    #[test]
    fn compare_rejects_zero_sql_server_profile_sample_interval() {
        let args = [
            OsString::from("--sqlserver-profile-sample-ms"),
            OsString::from("0"),
        ];

        let err = super::CompareBenchOptions::parse(&args).unwrap_err();

        assert!(matches!(
            err,
            WriterBenchError::InvalidPositiveInteger {
                option: "--sqlserver-profile-sample-ms",
                ..
            }
        ));
    }

    #[test]
    fn compare_allows_external_backends_for_decimal_temporal() {
        let args = [
            OsString::from("--scenario"),
            OsString::from("decimal_temporal"),
            OsString::from("--backends"),
            OsString::from("baseline,arrow-odbc,odbc-bcp"),
        ];

        let options = super::CompareBenchOptions::parse(&args).unwrap();

        assert_eq!(
            options.backends,
            [
                super::BenchmarkBackend::Baseline,
                super::BenchmarkBackend::ArrowOdbc,
                super::BenchmarkBackend::OdbcBcp
            ]
        );
        assert_eq!(options.benchmark.scenario.name, "decimal_temporal");
    }

    #[test]
    fn compare_allows_direct_raw_for_narrow_numeric() {
        let args = [
            OsString::from("--scenario"),
            OsString::from("narrow_numeric"),
            OsString::from("--backends"),
            OsString::from("direct-raw"),
        ];

        let options = super::CompareBenchOptions::parse(&args).unwrap();

        assert_eq!(options.backends, [super::BenchmarkBackend::DirectRaw]);
        super::ensure_direct_raw_supported_scenario(&options.benchmark).unwrap();
    }

    #[test]
    fn compare_allows_direct_raw_for_variable_width_supported_scenarios() {
        for scenario in ["mixed_nullable", "string_heavy", "wide_sparse"] {
            let args = [
                OsString::from("--scenario"),
                OsString::from(scenario),
                OsString::from("--backends"),
                OsString::from("direct-raw"),
            ];

            let options = super::CompareBenchOptions::parse(&args).unwrap();

            assert_eq!(options.backends, [super::BenchmarkBackend::DirectRaw]);
            super::ensure_direct_raw_supported_scenario(&options.benchmark).unwrap();
        }
    }

    #[test]
    fn compare_rejects_direct_raw_for_unsupported_scenarios() {
        for scenario in ["wide_mixed", "decimal_temporal", "tpch_lineitem_like"] {
            let args = [
                OsString::from("--scenario"),
                OsString::from(scenario),
                OsString::from("--backends"),
                OsString::from("direct-raw"),
            ];

            let options = super::CompareBenchOptions::parse(&args).unwrap();
            let err = super::ensure_direct_raw_supported_scenario(&options.benchmark).unwrap_err();

            assert!(matches!(
                err,
                WriterBenchError::Validation(message)
                    if message.contains("direct-raw")
                        && message.contains("narrow_numeric")
                        && message.contains("mixed_nullable")
                        && message.contains("string_heavy")
                        && message.contains("wide_sparse")
                        && message.contains(scenario)
            ));
        }
    }

    #[test]
    fn compare_rejects_duplicate_backend_names() {
        let args = [
            OsString::from("--backends"),
            OsString::from("baseline,baseline"),
        ];

        let err = super::CompareBenchOptions::parse(&args).unwrap_err();

        assert!(matches!(
            err,
            WriterBenchError::Validation(message) if message.contains("more than once")
        ));
    }

    #[test]
    fn compare_rejects_empty_backend_names() {
        let args = [OsString::from("--backends"), OsString::from("baseline,")];

        let err = super::CompareBenchOptions::parse(&args).unwrap_err();

        assert!(matches!(
            err,
            WriterBenchError::Validation(message) if message.contains("empty backend")
        ));
    }

    #[test]
    fn compare_rejects_unknown_backend_names() {
        let args = [OsString::from("--backends"), OsString::from("baseline,raw")];

        let err = super::CompareBenchOptions::parse(&args).unwrap_err();

        assert!(matches!(
            err,
            WriterBenchError::Validation(message) if message.contains("unknown writer-bench compare backend `raw`")
        ));
    }

    #[test]
    fn compare_rejects_direct_profile_without_direct_backend() {
        let args = [
            OsString::from("--backends"),
            OsString::from("baseline"),
            OsString::from("--profile-direct"),
        ];

        let options = super::CompareBenchOptions::parse(&args).unwrap();
        let err = super::run_compare_benchmark(&options).unwrap_err();

        assert!(matches!(
            err,
            WriterBenchError::Validation(message)
                if message.contains("--profile-direct")
                    && message.contains("direct-raw")
        ));
    }

    #[test]
    fn compare_rejects_sql_server_profile_for_external_backends_only() {
        let args = [
            OsString::from("--backends"),
            OsString::from("arrow-odbc,odbc-bcp"),
            OsString::from("--profile-sqlserver"),
        ];

        let options = super::CompareBenchOptions::parse(&args).unwrap();
        let err = super::run_compare_benchmark(&options).unwrap_err();

        assert!(matches!(
            err,
            WriterBenchError::Validation(message)
                if message.contains("--profile-sqlserver")
                    && message.contains("baseline")
                    && message.contains("direct-raw")
        ));
    }

    #[test]
    fn parses_ipc_dataset_command_options() {
        let args = [
            OsString::from("--path"),
            OsString::from("/tmp/bench.arrow"),
            OsString::from("--rows"),
            OsString::from("25"),
            OsString::from("--batch-size"),
            OsString::from("5"),
            OsString::from("--scenario"),
            OsString::from("mixed_nullable"),
        ];

        let options = super::IpcDatasetOptions::parse(&args).unwrap();

        assert_eq!(options.path, PathBuf::from("/tmp/bench.arrow"));
        assert_eq!(options.benchmark.rows, 25);
        assert_eq!(options.benchmark.batch_size, 5);
        assert_eq!(options.benchmark.scenario.name, "mixed_nullable");
    }

    #[test]
    fn ipc_dataset_command_requires_path() {
        let args = [OsString::from("--rows"), OsString::from("25")];
        let err = super::IpcDatasetOptions::parse(&args).unwrap_err();

        assert!(matches!(err, WriterBenchError::Validation(message) if message.contains("--path")));
    }

    #[test]
    fn ipc_dataset_command_writes_replayable_file() {
        let path = temp_test_file("writer-bench-ipc");
        let args = [
            OsString::from("ipc"),
            OsString::from("--path"),
            OsString::from(&path),
            OsString::from("--rows"),
            OsString::from("17"),
            OsString::from("--batch-size"),
            OsString::from("6"),
            OsString::from("--scenario"),
            OsString::from("mixed_nullable"),
        ];

        super::run(&args).unwrap();
        let summary = super::dataset::summarize_ipc_dataset(&path).unwrap();

        assert_eq!(summary.rows, 17);
        assert_eq!(summary.batches, 3);

        std::fs::remove_file(path).unwrap();
    }

    #[test]
    fn baseline_prepares_replayable_ipc_dataset_and_cleans_it_up() {
        let options = super::BaselineBenchOptions {
            benchmark: WriterBenchOptions {
                rows: 17,
                batch_size: 6,
                scenario: super::scenario_by_name("mixed_nullable").unwrap(),
                repeat: 2,
                output: BenchmarkOutput::Human,
            },
            sql_server: crate::sqlserver::SqlServerConnectionOptions::benchmark_default(),
            tds_packet_size: None,
        };

        let dataset = super::prepare_baseline_ipc_dataset(&options).unwrap();

        assert!(dataset.container_path.is_none());
        assert!(dataset.host_path.exists());
        let summary = super::dataset::summarize_ipc_dataset(&dataset.host_path).unwrap();
        assert_eq!(summary.rows, 17);
        assert_eq!(summary.batches, 3);

        dataset.cleanup().unwrap();
        assert!(!dataset.host_path.exists());
    }

    #[test]
    fn compare_prepares_replayable_ipc_dataset_and_cleans_it_up() {
        let options = super::CompareBenchOptions {
            benchmark: WriterBenchOptions {
                rows: 19,
                batch_size: 7,
                scenario: super::scenario_by_name("mixed_nullable").unwrap(),
                repeat: 2,
                output: BenchmarkOutput::Human,
            },
            sql_server: crate::sqlserver::SqlServerConnectionOptions::benchmark_default(),
            sql_server_profile: None,
            backends: vec![super::BenchmarkBackend::Baseline],
            runner_image: crate::odbc_runner::DEFAULT_RUNNER_IMAGE_TAG.to_owned(),
            keep_runner_image: false,
            profile_direct: false,
            tds_packet_size: None,
        };

        let dataset = super::prepare_compare_ipc_dataset(&options).unwrap();

        assert!(dataset.container_path.is_some());
        assert!(dataset.host_path.exists());
        let summary = super::dataset::summarize_ipc_dataset(&dataset.host_path).unwrap();
        assert_eq!(summary.rows, 19);
        assert_eq!(summary.batches, 3);

        dataset.cleanup().unwrap();
        assert!(!dataset.host_path.exists());
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
    fn compare_help_does_not_parse_generation_options() {
        let args = [OsString::from("compare"), OsString::from("--help")];

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
        assert!(!options.keep_runner_image);
        assert_eq!(
            options.runner_image,
            crate::odbc_runner::DEFAULT_RUNNER_IMAGE_TAG
        );
    }

    #[test]
    fn arrow_odbc_runner_args_include_input_ipc_dataset() {
        let options = super::ArrowOdbcBenchOptions {
            benchmark: WriterBenchOptions {
                rows: 25,
                batch_size: 5,
                scenario: super::scenario_by_name("mixed_nullable").unwrap(),
                repeat: 3,
                output: BenchmarkOutput::Human,
            },
            sql_server: crate::sqlserver::SqlServerConnectionOptions::benchmark_default(),
            runner_image: crate::odbc_runner::DEFAULT_RUNNER_IMAGE_TAG.to_owned(),
            keep_runner_image: false,
        };

        let args =
            super::arrow_odbc_runner_args(&options.benchmark, "/workspace/target/bench.arrow")
                .unwrap();

        assert!(args.iter().any(|arg| arg == "--release"));
        assert!(args.windows(2).any(|pair| pair == ["--rows", "25"]));
        assert!(args.windows(2).any(|pair| pair == ["--batch-size", "5"]));
        assert!(
            args.windows(2)
                .any(|pair| pair == ["--scenario", "mixed_nullable"])
        );
        assert!(args.windows(2).any(|pair| pair == ["--repeat", "3"]));
        assert!(
            args.windows(2)
                .any(|pair| pair == ["--input-ipc", "/workspace/target/bench.arrow"])
        );
        assert!(
            args.windows(2)
                .any(|pair| pair[0] == "--create-table-sql-template"
                    && pair[1].contains(super::ODBC_TABLE_PLACEHOLDER)
                    && pair[1].contains("[category] nvarchar(max) NULL"))
        );
    }

    #[test]
    fn odbc_bcp_runner_args_include_shared_ipc_dataset() {
        let benchmark = WriterBenchOptions {
            rows: 25,
            batch_size: 5,
            scenario: super::scenario_by_name("narrow_numeric").unwrap(),
            repeat: 3,
            output: BenchmarkOutput::Human,
        };

        let args =
            super::odbc_bcp_runner_args(&benchmark, "/workspace/target/bench.arrow").unwrap();

        assert!(args.iter().any(|arg| arg == "--release"));
        assert!(
            args.windows(2)
                .any(|pair| pair == ["--manifest-path", "xtask/odbc-bcp-runner/Cargo.toml"])
        );
        assert!(args.windows(2).any(|pair| pair == ["--rows", "25"]));
        assert!(args.windows(2).any(|pair| pair == ["--batch-size", "5"]));
        assert!(
            args.windows(2)
                .any(|pair| pair == ["--scenario", "narrow_numeric"])
        );
        assert!(args.windows(2).any(|pair| pair == ["--repeat", "3"]));
        assert!(
            args.windows(2)
                .any(|pair| pair == ["--input-ipc", "/workspace/target/bench.arrow"])
        );
    }

    #[test]
    fn odbc_bcp_runner_args_accept_mixed_nullable_scenario() {
        let benchmark = WriterBenchOptions {
            rows: 25,
            batch_size: 5,
            scenario: super::scenario_by_name("mixed_nullable").unwrap(),
            repeat: 3,
            output: BenchmarkOutput::Human,
        };

        let args =
            super::odbc_bcp_runner_args(&benchmark, "/workspace/target/bench.arrow").unwrap();

        assert!(
            args.windows(2)
                .any(|pair| pair == ["--scenario", "mixed_nullable"])
        );
    }

    #[test]
    fn parses_arrow_odbc_runner_report_from_noisy_output() {
        let output = "\
Compiling unrelated crate
arrow-odbc runner
  database: arrow_tiberius_benchmark
  rows written: 25
  write seconds: 0.067
  write rows/sec: 375.43
  peak rss KiB: 123456
";

        let report = super::parse_arrow_odbc_runner_report(output).unwrap();

        assert_eq!(report.rows_written, 25);
        assert_eq!(report.write_elapsed, std::time::Duration::from_millis(67));
        assert_eq!(report.peak_rss_kib, Some(123456));
    }

    #[test]
    fn parses_odbc_bcp_runner_report_from_noisy_output() {
        let output = "\
Compiling unrelated crate
odbc-bcp runner
  database: arrow_tiberius_benchmark
  rows written: 25
  write seconds: 0.067
  write rows/sec: 375.43
  peak rss KiB: 654321
";

        let report = super::parse_odbc_bcp_runner_report(output).unwrap();

        assert_eq!(report.rows_written, 25);
        assert_eq!(report.write_elapsed, std::time::Duration::from_millis(67));
        assert_eq!(report.peak_rss_kib, Some(654321));
    }

    #[test]
    fn parses_odbc_runner_report_without_peak_rss_for_backward_compatibility() {
        let output = "rows written: 25\nwrite seconds: 0.067";

        let report = super::parse_arrow_odbc_runner_report(output).unwrap();

        assert_eq!(report.rows_written, 25);
        assert_eq!(report.write_elapsed, std::time::Duration::from_millis(67));
        assert_eq!(report.peak_rss_kib, None);
    }

    #[test]
    fn rejects_odbc_bcp_runner_report_negative_seconds() {
        let output = "rows written: 25\nwrite seconds: -1";

        let err = super::parse_odbc_bcp_runner_report(output).unwrap_err();

        assert!(matches!(
            err,
            WriterBenchError::Validation(message) if message.contains("invalid write seconds")
        ));
    }

    #[test]
    fn rejects_odbc_bcp_runner_report_missing_rows_with_bcp_label() {
        let err = super::parse_odbc_bcp_runner_report("write seconds: 0.1").unwrap_err();

        assert!(matches!(
            err,
            WriterBenchError::Validation(message)
                if message.contains("odbc-bcp")
                    && message.contains("missing `rows written`")
                    && !message.contains("arrow-odbc")
        ));
    }

    #[test]
    fn rejects_odbc_bcp_runner_report_invalid_rows_with_bcp_label() {
        let output = "rows written: not-a-number\nwrite seconds: 0.1";

        let err = super::parse_odbc_bcp_runner_report(output).unwrap_err();

        assert!(matches!(
            err,
            WriterBenchError::Validation(message)
                if message.contains("odbc-bcp")
                    && message.contains("invalid rows written")
                    && !message.contains("arrow-odbc")
        ));
    }

    #[test]
    fn rejects_arrow_odbc_runner_report_missing_rows() {
        let err = super::parse_arrow_odbc_runner_report("write seconds: 0.1").unwrap_err();

        assert!(matches!(
            err,
            WriterBenchError::Validation(message) if message.contains("missing `rows written`")
        ));
    }

    #[test]
    fn rejects_arrow_odbc_runner_report_negative_seconds() {
        let output = "rows written: 25\nwrite seconds: -1";

        let err = super::parse_arrow_odbc_runner_report(output).unwrap_err();

        assert!(matches!(
            err,
            WriterBenchError::Validation(message) if message.contains("invalid write seconds")
        ));
    }

    #[test]
    fn arrow_odbc_network_is_not_created_for_existing_connection_string() {
        let args = [
            OsString::from("--connection-string"),
            OsString::from("server=tcp:127.0.0.1,1433;password=secret"),
        ];
        let options = super::ArrowOdbcBenchOptions::parse(&args).unwrap();

        let network = super::create_arrow_odbc_network(&options).unwrap();

        assert!(network.is_none());
    }

    #[test]
    fn arrow_odbc_connection_string_is_derived_from_sql_server_connection() {
        let odbc = super::odbc_connection_string_from_parts(
            "server=tcp:sqlserver,1433;user id=sa;password=secret;TrustServerCertificate=true",
            "bench_db",
        )
        .unwrap();

        assert_eq!(
            odbc,
            "Driver={ODBC Driver 18 for SQL Server};Server=tcp:sqlserver,1433;UID=sa;PWD=secret;Database=bench_db;TrustServerCertificate=yes;"
        );
    }

    #[test]
    fn arrow_odbc_connection_string_rejects_missing_password() {
        let err = super::odbc_connection_string_from_parts(
            "server=tcp:sqlserver,1433;user id=sa",
            "bench_db",
        )
        .unwrap_err();

        assert!(
            matches!(err, WriterBenchError::Validation(message) if message.contains("password"))
        );
    }

    #[test]
    fn tiberius_connection_string_appends_database_and_optional_packet_size() {
        let connection_string = super::tiberius_connection_string(
            "server=tcp:127.0.0.1,1433;User ID=sa;Password=secret",
            "bench",
            Some(32767),
        );

        assert_eq!(
            connection_string,
            "server=tcp:127.0.0.1,1433;User ID=sa;Password=secret;database=bench;Packet Size=32767"
        );

        let connection_string = super::tiberius_connection_string(
            "server=tcp:127.0.0.1,1433;User ID=sa;Password=secret",
            "bench",
            None,
        );

        assert_eq!(
            connection_string,
            "server=tcp:127.0.0.1,1433;User ID=sa;Password=secret;database=bench"
        );
    }

    #[test]
    fn arrow_odbc_command_accepts_decimal_temporal() {
        let args = [
            OsString::from("--scenario"),
            OsString::from("decimal_temporal"),
        ];

        let options = super::ArrowOdbcBenchOptions::parse(&args).unwrap();

        assert_eq!(options.benchmark.scenario.name, "decimal_temporal");
    }

    #[test]
    fn arrow_odbc_command_rejects_standalone_runner_image_build_flag() {
        let args = [OsString::from("--build-runner-image")];
        let err = super::ArrowOdbcBenchOptions::parse(&args).unwrap_err();

        assert!(
            matches!(err, WriterBenchError::UnknownOption(option) if option == "--build-runner-image")
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

    fn temp_test_file(name: &str) -> PathBuf {
        let counter = TEST_FILE_COUNTER.fetch_add(1, Ordering::Relaxed);
        std::env::temp_dir().join(format!(
            "arrow-tiberius-{name}-{}-{counter}.arrow",
            std::process::id()
        ))
    }
}

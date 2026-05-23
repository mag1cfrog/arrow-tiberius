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
    ArrayRef, BinaryArray, BooleanArray, Date32Array, Date64Array, Decimal128Array, Float32Array,
    Float64Array, Int8Array, Int16Array, Int32Array, Int64Array, RecordBatch, StringArray,
    TimestampMillisecondArray, UInt8Array, UInt16Array, UInt32Array, UInt64Array,
};
use arrow_schema::{DataType, Field, Schema, SchemaRef, TimeUnit};
use arrow_tiberius::{
    BulkWriter, Date64Policy, MssqlProfile, PlanOptions, SchemaMapping, TableName, UInt64Policy,
    WriteBackend, WriteOptions, create_table_sql_from_mappings,
    plan_arrow_schema_to_mssql_mappings, write::profile::DirectWriteProfile,
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
    ensure_arrow_odbc_supported_scenario(&options.benchmark)?;
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
        "Usage:\n  cargo xtask writer-bench compare [OPTIONS]\n\nData Options:\n  --rows <COUNT>                    Total rows to generate [default: 100000]\n  --batch-size <COUNT>              Maximum rows per generated RecordBatch [default: 8192]\n  --scenario <NAME>                 Benchmark scenario [default: narrow_numeric]\n  --repeat <COUNT>                  Number of benchmark repeats [default: 1]\n  --backends <LIST>                 Comma-separated backends: baseline,direct-framed,direct-raw,direct-raw-no-date-fast-path,direct-raw-no-fixed-fast-path,arrow-odbc,odbc-bcp [default: baseline,arrow-odbc]\n  --output <FORMAT>                 Output format: human [default: human]\n  --profile-direct                  Include direct backend phase timings and counters\n\nSQL Server Options:\n  --container-runtime <PATH>        Container runtime executable, such as docker or podman\n  --connection-string <URL>         Use an existing SQL Server instead of a local container\n  --image <IMAGE>                   SQL Server container image\n  --database <NAME>                 Benchmark database name\n  --tds-packet-size <BYTES>         Requested TDS packet size for Tiberius writers\n  --sqlserver-recovery-model <MODEL>\n                                    Set compare database recovery model: full, bulk-logged, or simple\n  --sqlserver-bulk-table-lock       Enable table lock on bulk load for compare benchmark tables\n  --profile-sqlserver               Profile the SQL Server writer session during compare writes\n  --sqlserver-profile-sample-ms <MILLIS>\n                                    SQL Server profile sample interval [default: 250]\n  --keep-container                  Keep managed containers after the task exits\n\nODBC Runner Options:\n  --arrow-odbc-autocommit           Use ODBC autocommit for arrow-odbc compares\n  --odbc-bcp-defer-batches          Defer odbc-bcp commits to bcp_done\n  --runner-image <IMAGE>            Managed ODBC runner image tag\n  --keep-runner-image               Keep the managed ODBC runner image after the task exits\n  -h, --help                        Print help\n\nCompare runs use one shared Arrow IPC dataset as the fairness boundary."
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
            | CompareBackendBenchReport::DirectFramed { report }
            | CompareBackendBenchReport::DirectRaw { report }
            | CompareBackendBenchReport::DirectRawNoDateFastPath { report }
            | CompareBackendBenchReport::DirectRawNoFixedFastPath { report } => {
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
        println!("{prefix}  recovery model: {}", target.recovery_model);
        println!(
            "{prefix}  transaction policy: bulk writer request without explicit benchmark transaction"
        );
        println!(
            "{prefix}  writer connection: {} {} encrypted={} packet_size={} reads={} writes={} last_read={} last_write={}",
            target.initial_activity.connection.net_transport,
            target.initial_activity.connection.protocol_type,
            target.initial_activity.connection.encrypt_option,
            target.initial_activity.connection.net_packet_size,
            target.initial_activity.connection.num_reads,
            target.initial_activity.connection.num_writes,
            target
                .initial_activity
                .connection
                .last_read
                .as_deref()
                .unwrap_or("<none>"),
            target
                .initial_activity
                .connection
                .last_write
                .as_deref()
                .unwrap_or("<none>")
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
        print_sql_server_connection_deltas(prefix, &target.connection_deltas);
        print_sql_server_profile_phase_deltas(prefix, &target.phase_deltas);
        print_sql_server_table_page_snapshots(prefix, &target.table_page_snapshots);
    }
}

fn print_sql_server_profile_sample_coverage(prefix: &str, samples: &[SqlServerProfileSample]) {
    let coverages = sql_server_profile_sample_coverages(samples);
    if coverages.is_empty() {
        println!("{prefix}  write sample coverage: <none>");
        return;
    }

    println!("{prefix}  write sample coverage:");
    for coverage in coverages {
        println!(
            "{prefix}    {}: first=repeat {} {}..{} last=repeat {} {}..{}",
            coverage.phase,
            coverage.first_repeat_index + 1,
            format_duration(coverage.first_elapsed_start),
            format_duration(coverage.first_elapsed_end),
            coverage.last_repeat_index + 1,
            format_duration(coverage.last_elapsed_start),
            format_duration(coverage.last_elapsed_end),
        );
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct SqlServerProfileSampleCoverage {
    phase: String,
    first_repeat_index: usize,
    first_elapsed_start: Duration,
    first_elapsed_end: Duration,
    last_repeat_index: usize,
    last_elapsed_start: Duration,
    last_elapsed_end: Duration,
}

fn sql_server_profile_sample_coverages(
    samples: &[SqlServerProfileSample],
) -> Vec<SqlServerProfileSampleCoverage> {
    let mut by_phase: BTreeMap<&str, SqlServerProfileSampleCoverage> = BTreeMap::new();

    for sample in samples {
        by_phase
            .entry(sample.phase.as_str())
            .and_modify(|coverage| {
                coverage.last_repeat_index = sample.repeat_index;
                coverage.last_elapsed_start = sample.write_elapsed_start;
                coverage.last_elapsed_end = sample.write_elapsed_end;
            })
            .or_insert_with(|| SqlServerProfileSampleCoverage {
                phase: sample.phase.clone(),
                first_repeat_index: sample.repeat_index,
                first_elapsed_start: sample.write_elapsed_start,
                first_elapsed_end: sample.write_elapsed_end,
                last_repeat_index: sample.repeat_index,
                last_elapsed_start: sample.write_elapsed_start,
                last_elapsed_end: sample.write_elapsed_end,
            });
    }

    by_phase.into_values().collect()
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
            "{prefix}    {}: wait_ms={} tasks={} signal_wait_ms={}",
            wait.wait_type, wait.wait_time_ms, wait.waiting_tasks_count, wait.signal_wait_time_ms
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

fn print_sql_server_connection_deltas(prefix: &str, connections: &[SqlServerConnectionDelta]) {
    if connections.is_empty() {
        println!("{prefix}  connection deltas: <none>");
        return;
    }

    println!("{prefix}  connection deltas:");
    for connection in connections {
        println!(
            "{prefix}    {} {} encrypted={} packet_size={} reads={} writes={} last_read={} last_write={}",
            connection.net_transport,
            connection.protocol_type,
            connection.encrypt_option,
            connection.net_packet_size,
            connection.num_reads,
            connection.num_writes,
            connection.last_read.as_deref().unwrap_or("<none>"),
            connection.last_write.as_deref().unwrap_or("<none>")
        );
    }
}

fn print_sql_server_profile_phase_deltas(prefix: &str, phases: &[SqlServerProfilePhaseDelta]) {
    if phases.is_empty() {
        println!("{prefix}  phase deltas: <none>");
        return;
    }

    println!("{prefix}  phase deltas:");
    for phase in phases {
        println!("{prefix}    {}:", phase.phase);
        print_sql_server_session_wait_deltas(&format!("{prefix}    "), &phase.session_wait_deltas);
        print_sql_server_database_file_io_deltas(
            &format!("{prefix}    "),
            &phase.database_file_io_deltas,
        );
        print_sql_server_connection_deltas(&format!("{prefix}    "), &phase.connection_deltas);
    }
}

fn print_sql_server_table_page_snapshots(
    prefix: &str,
    tables: &[sqlserver_profile::TablePageSnapshot],
) {
    if tables.is_empty() {
        println!("{prefix}  table page snapshots: <none>");
        return;
    }

    println!("{prefix}  table page snapshots:");
    for table in tables {
        println!(
            "{prefix}    {}: rows={} used_pages={} in_row_used_pages={} lob_used_pages={} row_overflow_used_pages={}",
            table.table,
            table.row_count,
            table.used_page_count,
            table.in_row_used_page_count,
            table.lob_used_page_count,
            table.row_overflow_used_page_count
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
    sql_server_recovery_model: Option<SqlServerRecoveryModel>,
    sql_server_bulk_table_lock: bool,
    backends: Vec<BenchmarkBackend>,
    runner_image: String,
    keep_runner_image: bool,
    profile_direct: bool,
    tds_packet_size: Option<u32>,
    arrow_odbc_autocommit: bool,
    odbc_bcp_defer_batches: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct SqlServerProfileOptions {
    sample_interval: Duration,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SqlServerRecoveryModel {
    Full,
    BulkLogged,
    Simple,
}

impl fmt::Display for SqlServerRecoveryModel {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Full => f.write_str("FULL"),
            Self::BulkLogged => f.write_str("BULK_LOGGED"),
            Self::Simple => f.write_str("SIMPLE"),
        }
    }
}

impl FromStr for SqlServerRecoveryModel {
    type Err = WriterBenchError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "full" => Ok(Self::Full),
            "bulk-logged" => Ok(Self::BulkLogged),
            "simple" => Ok(Self::Simple),
            _ => Err(WriterBenchError::Validation(format!(
                "unknown writer-bench compare SQL Server recovery model `{value}`; expected full, bulk-logged, or simple"
            ))),
        }
    }
}

impl SqlServerRecoveryModel {
    fn from_sql_server_desc(value: &str) -> Result<Self, WriterBenchError> {
        match value {
            "FULL" => Ok(Self::Full),
            "BULK_LOGGED" => Ok(Self::BulkLogged),
            "SIMPLE" => Ok(Self::Simple),
            _ => Err(WriterBenchError::Validation(format!(
                "unknown SQL Server recovery model reported by server `{value}`"
            ))),
        }
    }
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
            sql_server_recovery_model: None,
            sql_server_bulk_table_lock: false,
            backends: vec![BenchmarkBackend::Baseline, BenchmarkBackend::ArrowOdbc],
            runner_image: odbc_runner::DEFAULT_RUNNER_IMAGE_TAG.to_owned(),
            keep_runner_image: false,
            profile_direct: false,
            tds_packet_size: None,
            arrow_odbc_autocommit: false,
            odbc_bcp_defer_batches: false,
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
                "--sqlserver-recovery-model" => {
                    options.sql_server_recovery_model = Some(required_value(args, index)?.parse()?);
                    index += 1;
                }
                "--sqlserver-bulk-table-lock" => {
                    options.sql_server_bulk_table_lock = true;
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
                "--arrow-odbc-autocommit" => {
                    options.arrow_odbc_autocommit = true;
                }
                "--odbc-bcp-defer-batches" => {
                    options.odbc_bcp_defer_batches = true;
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
    DirectFramed,
    DirectRaw,
    DirectRawNoDateFastPath,
    DirectRawNoFixedFastPath,
    ArrowOdbc,
    OdbcBcp,
}

impl BenchmarkBackend {
    fn is_tiberius(&self) -> bool {
        matches!(
            self,
            Self::Baseline
                | Self::DirectFramed
                | Self::DirectRaw
                | Self::DirectRawNoDateFastPath
                | Self::DirectRawNoFixedFastPath
        )
    }

    fn is_direct(&self) -> bool {
        matches!(
            self,
            Self::DirectFramed
                | Self::DirectRaw
                | Self::DirectRawNoDateFastPath
                | Self::DirectRawNoFixedFastPath
        )
    }

    fn supports_sql_server_profile(&self) -> bool {
        self.is_tiberius() || matches!(self, Self::ArrowOdbc | Self::OdbcBcp)
    }
}

impl fmt::Display for BenchmarkBackend {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Baseline => f.write_str("baseline"),
            Self::DirectFramed => f.write_str("direct-framed"),
            Self::DirectRaw => f.write_str("direct-raw"),
            Self::DirectRawNoDateFastPath => f.write_str("direct-raw-no-date-fast-path"),
            Self::DirectRawNoFixedFastPath => f.write_str("direct-raw-no-fixed-fast-path"),
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
            "direct-framed" => Ok(Self::DirectFramed),
            "direct-raw" => Ok(Self::DirectRaw),
            "direct-raw-no-date-fast-path" => Ok(Self::DirectRawNoDateFastPath),
            "direct-raw-no-fixed-fast-path" => Ok(Self::DirectRawNoFixedFastPath),
            "arrow-odbc" => Ok(Self::ArrowOdbc),
            "odbc-bcp" => Ok(Self::OdbcBcp),
            other => Err(WriterBenchError::Validation(format!(
                "unknown writer-bench compare backend `{other}`; expected baseline, direct-framed, direct-raw, direct-raw-no-date-fast-path, direct-raw-no-fixed-fast-path, arrow-odbc, or odbc-bcp"
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
        "writer-bench compare direct backends currently support only scenarios {}; scenario `{}` contains column types that are not implemented by the direct TDS encoder yet",
        DIRECT_RAW_SUPPORTED_SCENARIOS.join(", "),
        benchmark.scenario.name
    )))
}

fn ensure_direct_date_fast_path_ab_scenario(
    benchmark: &WriterBenchOptions,
) -> Result<(), WriterBenchError> {
    if benchmark.scenario.name == DATE_FAST_PATH_SCENARIO.name {
        return Ok(());
    }

    Err(WriterBenchError::Validation(format!(
        "writer-bench compare backend direct-raw-no-date-fast-path is an exact A/B for scenario `{}` only; scenario `{}` would not isolate the date fast path",
        DATE_FAST_PATH_SCENARIO.name, benchmark.scenario.name
    )))
}

fn ensure_arrow_odbc_supported_scenario(
    benchmark: &WriterBenchOptions,
) -> Result<(), WriterBenchError> {
    if !ARROW_ODBC_UNSUPPORTED_SCENARIOS.contains(&benchmark.scenario.name) {
        return Ok(());
    }

    Err(WriterBenchError::Validation(format!(
        "writer-bench arrow-odbc does not support scenario `{}` because this benchmark path does not support the unsigned integer mappings used by that scenario; choose a backend that supports this scenario",
        benchmark.scenario.name
    )))
}

fn ensure_odbc_bcp_supported_scenario(
    benchmark: &WriterBenchOptions,
) -> Result<(), WriterBenchError> {
    if !ODBC_BCP_UNSUPPORTED_SCENARIOS.contains(&benchmark.scenario.name) {
        return Ok(());
    }

    Err(WriterBenchError::Validation(format!(
        "writer-bench odbc-bcp does not support scenario `{}` because the ODBC BCP runner does not support UInt64 columns yet; use baseline, direct-framed, or direct-raw",
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
        ArrowOdbcRunnerBenchOptions {
            profile_sql_server: false,
            autocommit: false,
            bulk_table_lock: false,
        },
    )?;

    odbc_runner::run_runner_command(&command_options).map_err(WriterBenchError::OdbcRunner)
}

fn run_arrow_odbc_runner_for_benchmark_capture(
    benchmark: &WriterBenchOptions,
    runner_image: &odbc_runner::ManagedRunnerImage,
    network: Option<&sqlserver::ManagedNetwork>,
    connection: &sqlserver::SqlServerConnection,
    ipc_dataset: &ManagedIpcDataset,
    options: ArrowOdbcRunnerBenchOptions,
) -> Result<OdbcRunnerBenchReport, WriterBenchError> {
    println!("  action: run_arrow_odbc_runner");
    let command_options = arrow_odbc_runner_command_options(
        benchmark,
        runner_image,
        network,
        connection,
        ipc_dataset,
        options,
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
    options: OdbcBcpRunnerBenchOptions,
) -> Result<OdbcRunnerBenchReport, WriterBenchError> {
    println!("  action: run_odbc_bcp_runner");
    let command_options = odbc_bcp_runner_command_options(
        benchmark,
        runner_image,
        network,
        connection,
        ipc_dataset,
        options,
    )?;
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
    options: ArrowOdbcRunnerBenchOptions,
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
        arrow_odbc_runner_args(
            benchmark,
            container_path,
            options.profile_sql_server,
            options.autocommit,
            options.bulk_table_lock,
        )?,
    ))
}

fn arrow_odbc_runner_args(
    benchmark: &WriterBenchOptions,
    input_ipc: &str,
    profile_sql_server: bool,
    autocommit: bool,
    bulk_table_lock: bool,
) -> Result<Vec<String>, WriterBenchError> {
    let create_table_sql_template =
        arrow_odbc_create_table_sql_template(benchmark, bulk_table_lock)?;

    let mut args = vec![
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
    ];
    if profile_sql_server {
        args.push("--profile-sqlserver".to_owned());
    }
    if autocommit {
        args.push("--autocommit".to_owned());
    }

    Ok(args)
}

fn odbc_bcp_runner_command_options(
    benchmark: &WriterBenchOptions,
    runner_image: &odbc_runner::ManagedRunnerImage,
    network: Option<&sqlserver::ManagedNetwork>,
    connection: &sqlserver::SqlServerConnection,
    ipc_dataset: &ManagedIpcDataset,
    options: OdbcBcpRunnerBenchOptions,
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
        odbc_bcp_runner_args(
            benchmark,
            container_path,
            options.profile_sql_server,
            options.defer_batches,
            options.bulk_table_lock,
        )?,
    ))
}

fn odbc_bcp_runner_args(
    benchmark: &WriterBenchOptions,
    input_ipc: &str,
    profile_sql_server: bool,
    defer_batches: bool,
    bulk_table_lock: bool,
) -> Result<Vec<String>, WriterBenchError> {
    let create_table_sql_template =
        arrow_odbc_create_table_sql_template(benchmark, bulk_table_lock)?;

    let mut args = vec![
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
    ];
    if profile_sql_server {
        args.push("--profile-sqlserver".to_owned());
    }
    if defer_batches {
        args.push("--defer-batches".to_owned());
    }

    Ok(args)
}

fn arrow_odbc_create_table_sql_template(
    benchmark: &WriterBenchOptions,
    bulk_table_lock: bool,
) -> Result<String, WriterBenchError> {
    let placeholder_table =
        TableName::new("dbo", ODBC_TABLE_PLACEHOLDER).map_err(WriterBenchError::ArrowTiberius)?;
    let mappings = benchmark_mappings_for_scenario(benchmark.scenario)?;
    let sql = benchmark_table_sql(&placeholder_table, &mappings);
    let quoted_placeholder = placeholder_table.quoted_sql();
    let sql = if bulk_table_lock {
        format!(
            "{sql}\n{}",
            benchmark_bulk_table_lock_sql(&placeholder_table)
        )
    } else {
        sql
    };

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

const EXTENDED_PRIMITIVE_SCENARIO: BenchmarkScenarioDefinition = BenchmarkScenarioDefinition {
    name: "extended_primitive",
    description: "Small integer and real primitive throughput",
    schema: extended_primitive_schema,
    columns: extended_primitive_columns,
};

const UINT64_POLICY_SCENARIO: BenchmarkScenarioDefinition = BenchmarkScenarioDefinition {
    name: "uint64_policy",
    description: "UInt64 decimal(20,0) policy throughput",
    schema: uint64_policy_schema,
    columns: uint64_policy_columns,
};

const DATE_FAST_PATH_SCENARIO: BenchmarkScenarioDefinition = BenchmarkScenarioDefinition {
    name: "date_fast_path",
    description: "Date32 date and Date64 datetime2 fixed-width direct rows",
    schema: date_fast_path_schema,
    columns: date_fast_path_columns,
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

const STRING_HEAVY_TEXT_ONLY_SCENARIO: BenchmarkScenarioDefinition = BenchmarkScenarioDefinition {
    name: "string_heavy_text_only",
    description: "Large variable text rows with minimal binary payloads",
    schema: string_heavy_schema,
    columns: string_heavy_text_only_columns,
};

const STRING_HEAVY_BINARY_ONLY_SCENARIO: BenchmarkScenarioDefinition =
    BenchmarkScenarioDefinition {
        name: "string_heavy_binary_only",
        description: "Large variable binary rows with minimal text payloads",
        schema: string_heavy_schema,
        columns: string_heavy_binary_only_columns,
    };

const STRING_HEAVY_INLINE_4K_SCENARIO: BenchmarkScenarioDefinition = BenchmarkScenarioDefinition {
    name: "string_heavy_inline_4k",
    description: "Fixed string-heavy rows with about 4 KiB of SQL variable payload bytes",
    schema: string_heavy_schema,
    columns: string_heavy_inline_4k_columns,
};

const STRING_HEAVY_EDGE_7K_SCENARIO: BenchmarkScenarioDefinition = BenchmarkScenarioDefinition {
    name: "string_heavy_edge_7k",
    description: "Fixed string-heavy rows near the SQL Server in-row payload boundary",
    schema: string_heavy_schema,
    columns: string_heavy_edge_7k_columns,
};

const STRING_HEAVY_LOB_9K_SCENARIO: BenchmarkScenarioDefinition = BenchmarkScenarioDefinition {
    name: "string_heavy_lob_9k",
    description: "Fixed string-heavy rows above the SQL Server in-row payload boundary",
    schema: string_heavy_schema,
    columns: string_heavy_lob_9k_columns,
};

const STRING_HEAVY_UNICODE_SCENARIO: BenchmarkScenarioDefinition = BenchmarkScenarioDefinition {
    name: "string_heavy_unicode",
    description: "Large BMP Unicode text and binary payload rows",
    schema: string_heavy_schema,
    columns: string_heavy_unicode_columns,
};
const STRING_HEAVY_UNICODE_TENANT_FIRST_CODEPOINT: u32 = 0x79df;
const STRING_HEAVY_UNICODE_TENANT_SECOND_CODEPOINT: u32 = 0x6237;

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
    EXTENDED_PRIMITIVE_SCENARIO.name,
    UINT64_POLICY_SCENARIO.name,
    DATE_FAST_PATH_SCENARIO.name,
    MIXED_NULLABLE_SCENARIO.name,
    STRING_HEAVY_SCENARIO.name,
    STRING_HEAVY_TEXT_ONLY_SCENARIO.name,
    STRING_HEAVY_BINARY_ONLY_SCENARIO.name,
    STRING_HEAVY_INLINE_4K_SCENARIO.name,
    STRING_HEAVY_EDGE_7K_SCENARIO.name,
    STRING_HEAVY_LOB_9K_SCENARIO.name,
    STRING_HEAVY_UNICODE_SCENARIO.name,
    WIDE_SPARSE_SCENARIO.name,
];

const ARROW_ODBC_UNSUPPORTED_SCENARIOS: &[&str] = &[
    EXTENDED_PRIMITIVE_SCENARIO.name,
    UINT64_POLICY_SCENARIO.name,
];
const ODBC_BCP_UNSUPPORTED_SCENARIOS: &[&str] = &[UINT64_POLICY_SCENARIO.name];

const SCENARIOS: &[BenchmarkScenarioDefinition] = &[
    NARROW_NUMERIC_SCENARIO,
    EXTENDED_PRIMITIVE_SCENARIO,
    UINT64_POLICY_SCENARIO,
    DATE_FAST_PATH_SCENARIO,
    MIXED_NULLABLE_SCENARIO,
    WIDE_MIXED_SCENARIO,
    DECIMAL_TEMPORAL_SCENARIO,
    STRING_HEAVY_SCENARIO,
    STRING_HEAVY_TEXT_ONLY_SCENARIO,
    STRING_HEAVY_BINARY_ONLY_SCENARIO,
    STRING_HEAVY_INLINE_4K_SCENARIO,
    STRING_HEAVY_EDGE_7K_SCENARIO,
    STRING_HEAVY_LOB_9K_SCENARIO,
    STRING_HEAVY_UNICODE_SCENARIO,
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

#[derive(Debug, Clone, Copy)]
struct TiberiusRepeatConfig {
    scenario: &'static BenchmarkScenarioDefinition,
    backend: WriteBackend,
    profile_direct: bool,
    disable_date_fast_path: bool,
    disable_fixed_width_fast_path: bool,
    bulk_table_lock: bool,
}

#[derive(Debug, Clone, Copy)]
struct TiberiusBenchmarkConfig {
    backend: WriteBackend,
    profile_direct: bool,
    disable_date_fast_path: bool,
    disable_fixed_width_fast_path: bool,
    tds_packet_size: Option<u32>,
    sql_server_profile: Option<SqlServerProfileOptions>,
    bulk_table_lock: bool,
}

#[derive(Debug, Clone, Copy)]
struct ArrowOdbcRunnerBenchOptions {
    profile_sql_server: bool,
    autocommit: bool,
    bulk_table_lock: bool,
}

#[derive(Debug, Clone, Copy)]
struct OdbcBcpRunnerBenchOptions {
    profile_sql_server: bool,
    defer_batches: bool,
    bulk_table_lock: bool,
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
    DirectFramed { report: TiberiusBenchReport },
    DirectRaw { report: TiberiusBenchReport },
    DirectRawNoDateFastPath { report: TiberiusBenchReport },
    DirectRawNoFixedFastPath { report: TiberiusBenchReport },
    ArrowOdbc { report: OdbcRunnerBenchReport },
    OdbcBcp { report: OdbcRunnerBenchReport },
}

impl CompareBackendBenchReport {
    fn backend(&self) -> BenchmarkBackend {
        match self {
            Self::Baseline { .. } => BenchmarkBackend::Baseline,
            Self::DirectFramed { .. } => BenchmarkBackend::DirectFramed,
            Self::DirectRaw { .. } => BenchmarkBackend::DirectRaw,
            Self::DirectRawNoDateFastPath { .. } => BenchmarkBackend::DirectRawNoDateFastPath,
            Self::DirectRawNoFixedFastPath { .. } => BenchmarkBackend::DirectRawNoFixedFastPath,
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
    recovery_model: String,
    sample_interval: Duration,
    initial_activity: sqlserver_profile::ActivitySnapshot,
    write_samples: Vec<SqlServerProfileSample>,
    session_wait_deltas: Vec<SqlServerSessionWaitDelta>,
    database_file_io_deltas: Vec<SqlServerDatabaseFileIoDelta>,
    connection_deltas: Vec<SqlServerConnectionDelta>,
    phase_deltas: Vec<SqlServerProfilePhaseDelta>,
    table_page_snapshots: Vec<sqlserver_profile::TablePageSnapshot>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct SqlServerProfileSample {
    phase: String,
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

#[derive(Debug, Clone, PartialEq, Eq)]
struct SqlServerConnectionDelta {
    net_transport: String,
    protocol_type: String,
    encrypt_option: String,
    net_packet_size: i32,
    num_reads: i64,
    num_writes: i64,
    last_read: Option<String>,
    last_write: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct SqlServerProfilePhaseDelta {
    phase: String,
    session_wait_deltas: Vec<SqlServerSessionWaitDelta>,
    database_file_io_deltas: Vec<SqlServerDatabaseFileIoDelta>,
    connection_deltas: Vec<SqlServerConnectionDelta>,
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

fn sql_server_connection_delta(
    initial: &sqlserver_profile::ConnectionSnapshot,
    final_connection: &sqlserver_profile::ConnectionSnapshot,
) -> SqlServerConnectionDelta {
    SqlServerConnectionDelta {
        net_transport: final_connection.net_transport.clone(),
        protocol_type: final_connection.protocol_type.clone(),
        encrypt_option: final_connection.encrypt_option.clone(),
        net_packet_size: final_connection.net_packet_size,
        num_reads: counter_delta(initial.num_reads, final_connection.num_reads),
        num_writes: counter_delta(initial.num_writes, final_connection.num_writes),
        last_read: final_connection.last_read.clone(),
        last_write: final_connection.last_write.clone(),
    }
}

fn merge_sql_server_session_wait_deltas(
    target: &mut Vec<SqlServerSessionWaitDelta>,
    source: Vec<SqlServerSessionWaitDelta>,
) {
    let mut merged = target
        .drain(..)
        .map(|wait| (wait.wait_type.clone(), wait))
        .collect::<BTreeMap<_, _>>();

    for wait in source {
        let merged_wait =
            merged
                .entry(wait.wait_type.clone())
                .or_insert(SqlServerSessionWaitDelta {
                    wait_type: wait.wait_type,
                    waiting_tasks_count: 0,
                    wait_time_ms: 0,
                    signal_wait_time_ms: 0,
                });
        merged_wait.waiting_tasks_count = merged_wait
            .waiting_tasks_count
            .saturating_add(wait.waiting_tasks_count);
        merged_wait.wait_time_ms = merged_wait.wait_time_ms.saturating_add(wait.wait_time_ms);
        merged_wait.signal_wait_time_ms = merged_wait
            .signal_wait_time_ms
            .saturating_add(wait.signal_wait_time_ms);
    }

    *target = merged.into_values().collect();
    target.sort_by(|left, right| {
        right
            .wait_time_ms
            .cmp(&left.wait_time_ms)
            .then_with(|| left.wait_type.cmp(&right.wait_type))
    });
}

fn merge_sql_server_database_file_io_deltas(
    target: &mut Vec<SqlServerDatabaseFileIoDelta>,
    source: Vec<SqlServerDatabaseFileIoDelta>,
) {
    let mut merged = target
        .drain(..)
        .map(|file| (file.file_id, file))
        .collect::<BTreeMap<_, _>>();

    for file in source {
        let merged_file = merged
            .entry(file.file_id)
            .or_insert(SqlServerDatabaseFileIoDelta {
                file_id: file.file_id,
                logical_name: file.logical_name,
                file_type: file.file_type,
                read_count: 0,
                read_bytes: 0,
                read_stall_ms: 0,
                write_count: 0,
                write_bytes: 0,
                write_stall_ms: 0,
            });
        merged_file.read_count = merged_file.read_count.saturating_add(file.read_count);
        merged_file.read_bytes = merged_file.read_bytes.saturating_add(file.read_bytes);
        merged_file.read_stall_ms = merged_file.read_stall_ms.saturating_add(file.read_stall_ms);
        merged_file.write_count = merged_file.write_count.saturating_add(file.write_count);
        merged_file.write_bytes = merged_file.write_bytes.saturating_add(file.write_bytes);
        merged_file.write_stall_ms = merged_file
            .write_stall_ms
            .saturating_add(file.write_stall_ms);
    }

    *target = merged.into_values().collect();
    target.sort_by(|left, right| {
        left.file_type
            .cmp(&right.file_type)
            .then_with(|| left.logical_name.cmp(&right.logical_name))
            .then_with(|| left.file_id.cmp(&right.file_id))
    });
}

fn merge_sql_server_connection_deltas(
    target: &mut Vec<SqlServerConnectionDelta>,
    source: Vec<SqlServerConnectionDelta>,
) {
    let mut merged = target
        .drain(..)
        .map(|connection| {
            (
                (
                    connection.net_transport.clone(),
                    connection.protocol_type.clone(),
                    connection.encrypt_option.clone(),
                    connection.net_packet_size,
                ),
                connection,
            )
        })
        .collect::<BTreeMap<_, _>>();

    for connection in source {
        let merged_connection = merged
            .entry((
                connection.net_transport.clone(),
                connection.protocol_type.clone(),
                connection.encrypt_option.clone(),
                connection.net_packet_size,
            ))
            .or_insert(SqlServerConnectionDelta {
                net_transport: connection.net_transport,
                protocol_type: connection.protocol_type,
                encrypt_option: connection.encrypt_option,
                net_packet_size: connection.net_packet_size,
                num_reads: 0,
                num_writes: 0,
                last_read: None,
                last_write: None,
            });
        merged_connection.num_reads = merged_connection
            .num_reads
            .saturating_add(connection.num_reads);
        merged_connection.num_writes = merged_connection
            .num_writes
            .saturating_add(connection.num_writes);
        merged_connection.last_read = connection.last_read;
        merged_connection.last_write = connection.last_write;
    }

    *target = merged.into_values().collect();
}

fn merge_sql_server_profile_phase_delta(
    target: &mut Vec<SqlServerProfilePhaseDelta>,
    phase: &str,
    session_wait_deltas: Vec<SqlServerSessionWaitDelta>,
    database_file_io_deltas: Vec<SqlServerDatabaseFileIoDelta>,
    connection_deltas: Vec<SqlServerConnectionDelta>,
) {
    if let Some(phase_delta) = target.iter_mut().find(|delta| delta.phase == phase) {
        merge_sql_server_session_wait_deltas(
            &mut phase_delta.session_wait_deltas,
            session_wait_deltas,
        );
        merge_sql_server_database_file_io_deltas(
            &mut phase_delta.database_file_io_deltas,
            database_file_io_deltas,
        );
        merge_sql_server_connection_deltas(&mut phase_delta.connection_deltas, connection_deltas);
        return;
    }

    target.push(SqlServerProfilePhaseDelta {
        phase: phase.to_owned(),
        session_wait_deltas,
        database_file_io_deltas,
        connection_deltas,
    });
}

struct SqlServerProfileSession {
    target: SqlServerProfileTarget,
    observer: BenchClient,
    next_repeat_index: usize,
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
        let recovery_model = sqlserver_profile::recovery_model(&mut observer).await?;
        let initial_activity =
            sqlserver_profile::current_activity_snapshot(&mut observer, writer_session_id).await?;

        Ok(Self {
            target: SqlServerProfileTarget {
                writer_session_id,
                observer_session_id,
                recovery_model,
                sample_interval: options.sample_interval,
                initial_activity,
                write_samples: Vec::new(),
                session_wait_deltas: Vec::new(),
                database_file_io_deltas: Vec::new(),
                connection_deltas: Vec::new(),
                phase_deltas: Vec::new(),
                table_page_snapshots: Vec::new(),
            },
            observer,
            next_repeat_index: 0,
        })
    }

    fn target(&self) -> SqlServerProfileTarget {
        self.target.clone()
    }

    async fn snapshot_table_pages(&mut self, table: &TableName) -> Result<(), WriterBenchError> {
        self.target.table_page_snapshots.push(
            sqlserver_profile::table_page_snapshot(&mut self.observer, &table.quoted_sql()).await?,
        );
        Ok(())
    }

    fn next_repeat_index(&mut self) -> usize {
        let repeat_index = self.next_repeat_index;
        self.next_repeat_index = self.next_repeat_index.saturating_add(1);
        repeat_index
    }

    async fn sample_phase<T, F>(
        &mut self,
        repeat_index: usize,
        phase: &str,
        write: F,
    ) -> Result<T, WriterBenchError>
    where
        F: Future<Output = Result<T, WriterBenchError>>,
    {
        let initial_session_waits = sqlserver_profile::session_wait_snapshots(
            &mut self.observer,
            self.target.writer_session_id,
        )
        .await?;
        let initial_database_file_io =
            sqlserver_profile::database_file_io_snapshots(&mut self.observer).await?;
        let initial_connection = sqlserver_profile::connection_snapshot(
            &mut self.observer,
            self.target.writer_session_id,
        )
        .await?;
        let started_at = Instant::now();

        let write_result = {
            let sample_activity = self.sample_write_activity(repeat_index, phase, started_at);
            tokio::pin!(sample_activity);
            tokio::pin!(write);

            tokio::select! {
                result = &mut write => result,
                result = &mut sample_activity => {
                    result?;
                    Err(WriterBenchError::Validation(
                        "SQL Server write sampler stopped before the measured write finished"
                            .to_owned(),
                    ))
                }
            }
        };

        let final_session_waits = sqlserver_profile::session_wait_snapshots(
            &mut self.observer,
            self.target.writer_session_id,
        )
        .await?;
        merge_sql_server_session_wait_deltas(
            &mut self.target.session_wait_deltas,
            sql_server_session_wait_deltas(&initial_session_waits, &final_session_waits),
        );
        let final_database_file_io =
            sqlserver_profile::database_file_io_snapshots(&mut self.observer).await?;
        merge_sql_server_database_file_io_deltas(
            &mut self.target.database_file_io_deltas,
            sql_server_database_file_io_deltas(&initial_database_file_io, &final_database_file_io),
        );
        let final_connection = sqlserver_profile::connection_snapshot(
            &mut self.observer,
            self.target.writer_session_id,
        )
        .await?;
        let connection_delta = sql_server_connection_delta(&initial_connection, &final_connection);
        merge_sql_server_connection_deltas(
            &mut self.target.connection_deltas,
            vec![connection_delta.clone()],
        );
        merge_sql_server_profile_phase_delta(
            &mut self.target.phase_deltas,
            phase,
            sql_server_session_wait_deltas(&initial_session_waits, &final_session_waits),
            sql_server_database_file_io_deltas(&initial_database_file_io, &final_database_file_io),
            vec![connection_delta],
        );

        write_result
    }

    async fn sample_write_activity(
        &mut self,
        repeat_index: usize,
        phase: &str,
        started_at: Instant,
    ) -> Result<(), WriterBenchError> {
        let mut sample_interval = tokio::time::interval(self.target.sample_interval);
        sample_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        sample_interval.tick().await;

        loop {
            sample_interval.tick().await;
            let write_elapsed_start = started_at.elapsed();
            let activity = sqlserver_profile::current_activity_snapshot(
                &mut self.observer,
                self.target.writer_session_id,
            )
            .await?;
            self.target.write_samples.push(SqlServerProfileSample {
                phase: phase.to_owned(),
                repeat_index,
                write_elapsed_start,
                write_elapsed_end: started_at.elapsed(),
                activity,
            });
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
        TiberiusBenchmarkConfig {
            backend: WriteBackend::BaselineTokenRow,
            profile_direct: false,
            disable_date_fast_path: false,
            disable_fixed_width_fast_path: false,
            tds_packet_size: options.tds_packet_size,
            sql_server_profile: None,
            bulk_table_lock: false,
        },
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
    if options.profile_direct && !options.backends.iter().any(BenchmarkBackend::is_direct) {
        return Err(WriterBenchError::Validation(
            "writer-bench compare --profile-direct requires direct-framed or direct-raw".to_owned(),
        ));
    }

    if options.arrow_odbc_autocommit && !options.backends.contains(&BenchmarkBackend::ArrowOdbc) {
        return Err(WriterBenchError::Validation(
            "writer-bench compare --arrow-odbc-autocommit requires the arrow-odbc backend"
                .to_owned(),
        ));
    }

    if options.odbc_bcp_defer_batches && !options.backends.contains(&BenchmarkBackend::OdbcBcp) {
        return Err(WriterBenchError::Validation(
            "writer-bench compare --odbc-bcp-defer-batches requires the odbc-bcp backend"
                .to_owned(),
        ));
    }

    if options.sql_server_profile.is_some()
        && !options
            .backends
            .iter()
            .any(BenchmarkBackend::supports_sql_server_profile)
    {
        return Err(WriterBenchError::Validation(
            "writer-bench compare --profile-sqlserver requires the baseline, direct-framed, direct-raw, arrow-odbc, or odbc-bcp backend"
                .to_owned(),
        ));
    }

    if options.backends.iter().any(BenchmarkBackend::is_direct) {
        ensure_direct_raw_supported_scenario(&options.benchmark)?;
    }
    if options
        .backends
        .contains(&BenchmarkBackend::DirectRawNoDateFastPath)
    {
        ensure_direct_date_fast_path_ab_scenario(&options.benchmark)?;
    }
    if options.backends.contains(&BenchmarkBackend::ArrowOdbc) {
        ensure_arrow_odbc_supported_scenario(&options.benchmark)?;
    }
    if options.backends.contains(&BenchmarkBackend::OdbcBcp) {
        ensure_odbc_bcp_supported_scenario(&options.benchmark)?;
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
    let original_recovery_model = if let Some(recovery_model) = options.sql_server_recovery_model {
        match runtime.block_on(current_sql_server_recovery_model(&connection)) {
            Ok(original_recovery_model) => {
                if let Err(error) =
                    runtime.block_on(set_sql_server_recovery_model(&connection, recovery_model))
                {
                    let dataset_cleanup_result = ipc_dataset.cleanup();
                    return match dataset_cleanup_result {
                        Ok(()) => Err(error),
                        Err(cleanup_error) => Err(WriterBenchError::Validation(format!(
                            "SQL Server recovery model setup failed and IPC dataset cleanup also failed; setup error: {error}; cleanup error: {cleanup_error}"
                        ))),
                    };
                }
                Some(original_recovery_model)
            }
            Err(error) => {
                let dataset_cleanup_result = ipc_dataset.cleanup();
                return match dataset_cleanup_result {
                    Ok(()) => Err(error),
                    Err(cleanup_error) => Err(WriterBenchError::Validation(format!(
                        "SQL Server recovery model snapshot failed and IPC dataset cleanup also failed; snapshot error: {error}; cleanup error: {cleanup_error}"
                    ))),
                };
            }
        }
    } else {
        None
    };
    let run_result = (|| {
        let mut backends = Vec::new();

        for backend in &options.backends {
            match backend {
                BenchmarkBackend::Baseline => {
                    let report = runtime.block_on(async {
                        let backend_start = Instant::now();
                        let mut report = run_tiberius_benchmark_from_ipc(
                            &options.benchmark,
                            &connection,
                            &ipc_dataset.host_path,
                            TiberiusBenchmarkConfig {
                                backend: WriteBackend::BaselineTokenRow,
                                profile_direct: false,
                                disable_date_fast_path: false,
                                disable_fixed_width_fast_path: false,
                                tds_packet_size: options.tds_packet_size,
                                sql_server_profile: options.sql_server_profile,
                                bulk_table_lock: options.sql_server_bulk_table_lock,
                            },
                        )
                        .await?;
                        report.timings.total = backend_start.elapsed();
                        Ok::<_, WriterBenchError>(report)
                    })?;
                    backends.push(CompareBackendBenchReport::Baseline { report });
                }
                BenchmarkBackend::DirectFramed => {
                    let report = runtime.block_on(async {
                        let backend_start = Instant::now();
                        let mut report = run_tiberius_benchmark_from_ipc(
                            &options.benchmark,
                            &connection,
                            &ipc_dataset.host_path,
                            TiberiusBenchmarkConfig {
                                backend: WriteBackend::DirectFramedBulk,
                                profile_direct: options.profile_direct,
                                disable_date_fast_path: false,
                                disable_fixed_width_fast_path: false,
                                tds_packet_size: options.tds_packet_size,
                                sql_server_profile: options.sql_server_profile,
                                bulk_table_lock: options.sql_server_bulk_table_lock,
                            },
                        )
                        .await?;
                        report.timings.total = backend_start.elapsed();
                        Ok::<_, WriterBenchError>(report)
                    })?;
                    backends.push(CompareBackendBenchReport::DirectFramed { report });
                }
                BenchmarkBackend::DirectRaw => {
                    let report = runtime.block_on(async {
                        let backend_start = Instant::now();
                        let mut report = run_tiberius_benchmark_from_ipc(
                            &options.benchmark,
                            &connection,
                            &ipc_dataset.host_path,
                            TiberiusBenchmarkConfig {
                                backend: WriteBackend::DirectRawBulk,
                                profile_direct: options.profile_direct,
                                disable_date_fast_path: false,
                                disable_fixed_width_fast_path: false,
                                tds_packet_size: options.tds_packet_size,
                                sql_server_profile: options.sql_server_profile,
                                bulk_table_lock: options.sql_server_bulk_table_lock,
                            },
                        )
                        .await?;
                        report.timings.total = backend_start.elapsed();
                        Ok::<_, WriterBenchError>(report)
                    })?;
                    backends.push(CompareBackendBenchReport::DirectRaw { report });
                }
                BenchmarkBackend::DirectRawNoDateFastPath => {
                    let report = runtime.block_on(async {
                        let backend_start = Instant::now();
                        let mut report = run_tiberius_benchmark_from_ipc(
                            &options.benchmark,
                            &connection,
                            &ipc_dataset.host_path,
                            TiberiusBenchmarkConfig {
                                backend: WriteBackend::DirectRawBulk,
                                profile_direct: options.profile_direct,
                                disable_date_fast_path: true,
                                disable_fixed_width_fast_path: false,
                                tds_packet_size: options.tds_packet_size,
                                sql_server_profile: options.sql_server_profile,
                                bulk_table_lock: options.sql_server_bulk_table_lock,
                            },
                        )
                        .await?;
                        report.timings.total = backend_start.elapsed();
                        Ok::<_, WriterBenchError>(report)
                    })?;
                    backends.push(CompareBackendBenchReport::DirectRawNoDateFastPath { report });
                }
                BenchmarkBackend::DirectRawNoFixedFastPath => {
                    let report = runtime.block_on(async {
                        let backend_start = Instant::now();
                        let mut report = run_tiberius_benchmark_from_ipc(
                            &options.benchmark,
                            &connection,
                            &ipc_dataset.host_path,
                            TiberiusBenchmarkConfig {
                                backend: WriteBackend::DirectRawBulk,
                                profile_direct: options.profile_direct,
                                disable_date_fast_path: false,
                                disable_fixed_width_fast_path: true,
                                tds_packet_size: options.tds_packet_size,
                                sql_server_profile: options.sql_server_profile,
                                bulk_table_lock: options.sql_server_bulk_table_lock,
                            },
                        )
                        .await?;
                        report.timings.total = backend_start.elapsed();
                        Ok::<_, WriterBenchError>(report)
                    })?;
                    backends.push(CompareBackendBenchReport::DirectRawNoFixedFastPath { report });
                }
                BenchmarkBackend::ArrowOdbc => {
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
                        ArrowOdbcRunnerBenchOptions {
                            profile_sql_server: options.sql_server_profile.is_some(),
                            autocommit: options.arrow_odbc_autocommit,
                            bulk_table_lock: options.sql_server_bulk_table_lock,
                        },
                    )?;
                    backends.push(CompareBackendBenchReport::ArrowOdbc { report });
                }
                BenchmarkBackend::OdbcBcp => {
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
                        OdbcBcpRunnerBenchOptions {
                            profile_sql_server: options.sql_server_profile.is_some(),
                            defer_batches: options.odbc_bcp_defer_batches,
                            bulk_table_lock: options.sql_server_bulk_table_lock,
                        },
                    )?;
                    backends.push(CompareBackendBenchReport::OdbcBcp { report });
                }
            }
        }

        Ok(CompareBenchReport {
            ipc_dataset: ipc_dataset.host_path.clone(),
            database: connection.database.clone(),
            backends,
        })
    })();
    let recovery_restore_result = if let Some(recovery_model) = original_recovery_model {
        runtime.block_on(set_sql_server_recovery_model(&connection, recovery_model))
    } else {
        Ok(())
    };
    let dataset_cleanup_result = ipc_dataset.cleanup();
    let runner_cleanup_result = if let Some(runner_image) = runner_image.as_mut() {
        runner_image.cleanup().map_err(WriterBenchError::OdbcRunner)
    } else {
        Ok(())
    };
    let report = match (run_result, recovery_restore_result) {
        (Ok(report), Ok(())) => report,
        (Ok(_), Err(restore_error)) => return Err(restore_error),
        (Err(run_error), Ok(())) => return Err(run_error),
        (Err(run_error), Err(restore_error)) => {
            return Err(WriterBenchError::Validation(format!(
                "writer-bench compare failed and SQL Server recovery model restore also failed; benchmark error: {run_error}; restore error: {restore_error}"
            )));
        }
    };
    dataset_cleanup_result?;
    runner_cleanup_result?;

    Ok(report)
}

async fn run_tiberius_benchmark_from_ipc(
    benchmark: &WriterBenchOptions,
    connection: &sqlserver::SqlServerConnection,
    ipc_path: &Path,
    config: TiberiusBenchmarkConfig,
) -> Result<TiberiusBenchReport, WriterBenchError> {
    let mut report = TiberiusBenchReport::default();

    let setup_start = Instant::now();
    let mut client = connect(
        &connection.connection_string,
        &connection.database,
        config.tds_packet_size,
    )
    .await?;
    let mut sql_server_profile_session = if let Some(options) = config.sql_server_profile {
        Some(SqlServerProfileSession::start(&mut client, connection, options).await?)
    } else {
        None
    };
    let mappings = benchmark_mappings_for_scenario(benchmark.scenario)?;
    report.timings.setup += setup_start.elapsed();

    for _repeat_index in 0..benchmark.repeat {
        let repeat_result = async {
            let table = unique_benchmark_table_name()?;
            let repeat_report =
                run_tiberius_repeat_from_ipc(
                    &mut client,
                    TiberiusRepeatConfig {
                        scenario: benchmark.scenario,
                        backend: config.backend,
                        profile_direct: config.profile_direct,
                        disable_date_fast_path: config.disable_date_fast_path,
                        disable_fixed_width_fast_path: config.disable_fixed_width_fast_path,
                        bulk_table_lock: config.bulk_table_lock,
                    },
                    &mappings,
                    &table,
                    ipc_path,
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
    config: TiberiusRepeatConfig,
    mappings: &[arrow_tiberius::SchemaMapping],
    table: &TableName,
    ipc_path: &Path,
    sql_server_profile_session: Option<&mut SqlServerProfileSession>,
) -> Result<TiberiusBenchReport, WriterBenchError> {
    let batches =
        dataset::ipc_dataset_reader(ipc_path)?.map(|batch| batch.map_err(WriterBenchError::Arrow));

    run_tiberius_repeat_with_batches(
        client,
        config,
        mappings,
        table,
        batches,
        sql_server_profile_session,
    )
    .await
}

async fn run_tiberius_repeat_with_batches(
    client: &mut BenchClient,
    config: TiberiusRepeatConfig,
    mappings: &[arrow_tiberius::SchemaMapping],
    table: &TableName,
    batches: impl IntoIterator<Item = Result<RecordBatch, WriterBenchError>>,
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
    if config.bulk_table_lock {
        execute_sql(client, benchmark_bulk_table_lock_sql(table)).await?;
    }
    let mut writer = BulkWriter::new(
        client,
        table.clone(),
        mappings.to_vec(),
        WriteOptions {
            backend: config.backend,
            ..WriteOptions::default()
        },
    )
    .await
    .map_err(WriterBenchError::ArrowTiberius)?;
    report.timings.setup += setup_start.elapsed();

    let profiling_direct = config.profile_direct
        && matches!(
            config.backend,
            WriteBackend::DirectFramedBulk | WriteBackend::DirectRawBulk
        );
    let _date_fast_path_override = config
        .disable_date_fast_path
        .then(arrow_tiberius::write::profile::disable_direct_date_fast_path_for_scope);
    let _fixed_width_fast_path_override = config
        .disable_fixed_width_fast_path
        .then(arrow_tiberius::write::profile::disable_direct_fixed_width_fast_path_for_scope);
    if profiling_direct {
        arrow_tiberius::write::profile::start_direct_write_profile();
    }

    let write_batches = async {
        let write_start = Instant::now();
        for batch in batches {
            let batch = batch?;
            report.stats = writer
                .write_batch(&batch)
                .await
                .map_err(WriterBenchError::ArrowTiberius)?;
        }
        report.timings.write += write_start.elapsed();
        Ok(())
    };
    let repeat_index = sql_server_profile_session
        .as_mut()
        .map(|profile_session| profile_session.next_repeat_index());
    if let (Some(profile_session), Some(repeat_index)) =
        (sql_server_profile_session.as_mut(), repeat_index)
    {
        profile_session
            .sample_phase(repeat_index, "write_batch", write_batches)
            .await?;
    } else {
        write_batches.await?;
    }

    let finish_writer = async {
        let finish_start = Instant::now();
        report.stats = writer
            .finish()
            .await
            .map_err(WriterBenchError::ArrowTiberius)?;
        report.timings.finish += finish_start.elapsed();
        Ok(())
    };
    if let (Some(profile_session), Some(repeat_index)) =
        (sql_server_profile_session.as_mut(), repeat_index)
    {
        profile_session
            .sample_phase(repeat_index, "finish", finish_writer)
            .await?;
    } else {
        finish_writer.await?;
    }

    let validate_start = Instant::now();
    report.validated_rows = select_count(client, table).await?;
    report.timings.validate += validate_start.elapsed();

    if report.validated_rows != report.stats.rows_written {
        return Err(WriterBenchError::RowCountMismatch {
            expected: report.stats.rows_written,
            actual: report.validated_rows,
        });
    }
    validate_scenario_contents(client, table, config.scenario, report.validated_rows).await?;

    if let Some(profile_session) = sql_server_profile_session.as_mut() {
        profile_session.snapshot_table_pages(table).await?;
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

async fn set_sql_server_recovery_model(
    connection: &sqlserver::SqlServerConnection,
    recovery_model: SqlServerRecoveryModel,
) -> Result<(), WriterBenchError> {
    let mut client = connect(&connection.connection_string, &connection.database, None).await?;
    execute_sql(
        &mut client,
        format!(
            "ALTER DATABASE [{}] SET RECOVERY {recovery_model}",
            connection.database.replace(']', "]]")
        ),
    )
    .await
}

async fn current_sql_server_recovery_model(
    connection: &sqlserver::SqlServerConnection,
) -> Result<SqlServerRecoveryModel, WriterBenchError> {
    let mut client = connect(&connection.connection_string, &connection.database, None).await?;
    let recovery_model = sqlserver_profile::recovery_model(&mut client).await?;
    SqlServerRecoveryModel::from_sql_server_desc(&recovery_model)
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
    select_count_query(
        client,
        format!("SELECT COUNT_BIG(*) FROM {}", table.quoted_sql()),
        "SELECT COUNT_BIG(*)",
    )
    .await
}

async fn validate_scenario_contents(
    client: &mut BenchClient,
    table: &TableName,
    scenario: &BenchmarkScenarioDefinition,
    expected_rows: u64,
) -> Result<(), WriterBenchError> {
    if scenario.name == STRING_HEAVY_UNICODE_SCENARIO.name {
        validate_string_heavy_unicode_contents(client, table, expected_rows).await?;
    }

    Ok(())
}

async fn validate_string_heavy_unicode_contents(
    client: &mut BenchClient,
    table: &TableName,
    expected_rows: u64,
) -> Result<(), WriterBenchError> {
    let actual = select_count_query(
        client,
        string_heavy_unicode_tenant_sentinel_count_sql(&table.quoted_sql()),
        "string_heavy_unicode tenant sentinel count",
    )
    .await?;

    if actual != expected_rows {
        return Err(WriterBenchError::Validation(format!(
            "string_heavy_unicode tenant sentinel validation failed: expected {expected_rows}, got {actual}"
        )));
    }

    Ok(())
}

fn string_heavy_unicode_tenant_sentinel_count_sql(table: &str) -> String {
    format!(
        "SELECT COUNT_BIG(*) FROM {table} \
         WHERE UNICODE(SUBSTRING([tenant], 1, 1)) = {STRING_HEAVY_UNICODE_TENANT_FIRST_CODEPOINT} \
         AND UNICODE(SUBSTRING([tenant], 2, 1)) = {STRING_HEAVY_UNICODE_TENANT_SECOND_CODEPOINT}"
    )
}

async fn select_count_query(
    client: &mut BenchClient,
    sql: String,
    label: &'static str,
) -> Result<u64, WriterBenchError> {
    let row = client
        .simple_query(sql)
        .await
        .map_err(WriterBenchError::Tiberius)?
        .into_row()
        .await
        .map_err(WriterBenchError::Tiberius)?
        .ok_or_else(|| WriterBenchError::Validation(format!("{label} returned no row")))?;
    let count = row
        .get::<i64, _>(0)
        .ok_or_else(|| WriterBenchError::Validation(format!("{label} did not return bigint")))?;

    u64::try_from(count).map_err(|_| {
        WriterBenchError::Validation(format!("{label} returned negative count {count}"))
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

fn benchmark_bulk_table_lock_sql(table: &TableName) -> String {
    format!(
        "EXEC sys.sp_tableoption N'{}', 'table lock on bulk load', 'ON';",
        escape_sql_string_literal(&table.quoted_sql())
    )
}

fn escape_sql_string_literal(value: &str) -> String {
    value.replace('\'', "''")
}

#[cfg(test)]
fn benchmark_mappings_for_schema(
    schema: SchemaRef,
) -> Result<Vec<SchemaMapping>, WriterBenchError> {
    benchmark_mappings_for_schema_with_options(schema, PlanOptions::default())
}

fn benchmark_mappings_for_scenario(
    scenario: &BenchmarkScenarioDefinition,
) -> Result<Vec<SchemaMapping>, WriterBenchError> {
    benchmark_mappings_for_schema_with_options(
        (scenario.schema)(),
        benchmark_plan_options(scenario),
    )
}

fn benchmark_plan_options(scenario: &BenchmarkScenarioDefinition) -> PlanOptions {
    if scenario.name == UINT64_POLICY_SCENARIO.name {
        return PlanOptions {
            uint64_policy: UInt64Policy::Decimal20_0,
            ..PlanOptions::default()
        };
    }

    if scenario.name == DATE_FAST_PATH_SCENARIO.name {
        return PlanOptions {
            date64_policy: Date64Policy::TimestampDateTime2,
            ..PlanOptions::default()
        };
    }

    PlanOptions::default()
}

fn benchmark_mappings_for_schema_with_options(
    schema: SchemaRef,
    plan_options: PlanOptions,
) -> Result<Vec<SchemaMapping>, WriterBenchError> {
    let (mappings, _diagnostics) = plan_arrow_schema_to_mssql_mappings(
        schema,
        MssqlProfile::sql_server_2016_compat_100(),
        plan_options,
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

fn extended_primitive_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("u8_value", DataType::UInt8, false),
        Field::new("maybe_u8", DataType::UInt8, true),
        Field::new("i8_value", DataType::Int8, false),
        Field::new("maybe_i8", DataType::Int8, true),
        Field::new("i16_value", DataType::Int16, false),
        Field::new("maybe_i16", DataType::Int16, true),
        Field::new("u16_value", DataType::UInt16, false),
        Field::new("maybe_u16", DataType::UInt16, true),
        Field::new("u32_value", DataType::UInt32, false),
        Field::new("maybe_u32", DataType::UInt32, true),
        Field::new("f32_value", DataType::Float32, false),
        Field::new("maybe_f32", DataType::Float32, true),
    ]))
}

fn extended_primitive_columns(
    offset: usize,
    len: usize,
) -> Result<Vec<ArrayRef>, WriterBenchError> {
    let u8_value = (offset..offset + len)
        .map(|row| (row % 256) as u8)
        .collect::<UInt8Array>();
    let maybe_u8 = (offset..offset + len)
        .map(|row| (row % 7 != 0).then_some((row % 256) as u8))
        .collect::<UInt8Array>();
    let i8_value = (offset..offset + len)
        .map(|row| ((row % 256) as i16 - 128) as i8)
        .collect::<Int8Array>();
    let maybe_i8 = (offset..offset + len)
        .map(|row| (row % 11 != 0).then_some(((row % 256) as i16 - 128) as i8))
        .collect::<Int8Array>();
    let i16_value = (offset..offset + len)
        .map(|row| ((row % 65_536) as i32 - 32_768) as i16)
        .collect::<Int16Array>();
    let maybe_i16 = (offset..offset + len)
        .map(|row| (row % 13 != 0).then_some(((row % 65_536) as i32 - 32_768) as i16))
        .collect::<Int16Array>();
    let u16_value = (offset..offset + len)
        .map(|row| (row % 65_536) as u16)
        .collect::<UInt16Array>();
    let maybe_u16 = (offset..offset + len)
        .map(|row| (row % 17 != 0).then_some((row % 65_536) as u16))
        .collect::<UInt16Array>();
    let u32_value = (offset..offset + len)
        .map(|row| {
            let value = (row as u64).wrapping_mul(1_103_515_245) & u64::from(u32::MAX);
            value as u32
        })
        .collect::<UInt32Array>();
    let maybe_u32 = (offset..offset + len)
        .map(|row| {
            (row % 19 != 0).then_some({
                let value = (row as u64).wrapping_mul(2_654_435_761) & u64::from(u32::MAX);
                value as u32
            })
        })
        .collect::<UInt32Array>();
    let f32_value = (offset..offset + len)
        .map(|row| ((row % 10_000) as f32 - 5_000.0) / 8.0)
        .collect::<Float32Array>();
    let maybe_f32 = (offset..offset + len)
        .map(|row| (row % 23 != 0).then_some(((row % 10_000) as f32 - 5_000.0) / 16.0))
        .collect::<Float32Array>();

    Ok(vec![
        Arc::new(u8_value),
        Arc::new(maybe_u8),
        Arc::new(i8_value),
        Arc::new(maybe_i8),
        Arc::new(i16_value),
        Arc::new(maybe_i16),
        Arc::new(u16_value),
        Arc::new(maybe_u16),
        Arc::new(u32_value),
        Arc::new(maybe_u32),
        Arc::new(f32_value),
        Arc::new(maybe_f32),
    ])
}

fn uint64_policy_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("row_id", DataType::Int32, false),
        Field::new("u64_small", DataType::UInt64, false),
        Field::new("u64_mid", DataType::UInt64, false),
        Field::new("u64_full", DataType::UInt64, false),
        Field::new("maybe_u64_full", DataType::UInt64, true),
    ]))
}

fn uint64_policy_columns(offset: usize, len: usize) -> Result<Vec<ArrayRef>, WriterBenchError> {
    let row_id = (offset..offset + len)
        .map(deterministic_i32)
        .collect::<Int32Array>();
    let u64_small = (offset..offset + len)
        .map(|row| (row % 1_000_000_000) as u64)
        .collect::<UInt64Array>();
    let u64_mid = (offset..offset + len)
        .map(|row| i64::MAX as u64 - (row % 1_000_000) as u64)
        .collect::<UInt64Array>();
    let u64_full = (offset..offset + len)
        .map(|row| u64::MAX - (row % 1_000_000) as u64)
        .collect::<UInt64Array>();
    let maybe_u64_full = (offset..offset + len)
        .map(|row| (row % 13 != 0).then_some(u64::MAX - (row % 1_000_000) as u64))
        .collect::<UInt64Array>();

    Ok(vec![
        Arc::new(row_id),
        Arc::new(u64_small),
        Arc::new(u64_mid),
        Arc::new(u64_full),
        Arc::new(maybe_u64_full),
    ])
}

fn date_fast_path_schema() -> SchemaRef {
    let mut fields = vec![Field::new("row_id", DataType::Int32, false)];

    for group in 0..8 {
        fields.push(Field::new(
            format!("trade_date_{group}"),
            DataType::Date32,
            false,
        ));
        fields.push(Field::new(
            format!("maybe_trade_date_{group}"),
            DataType::Date32,
            true,
        ));
        fields.push(Field::new(
            format!("posted_at_{group}"),
            DataType::Date64,
            false,
        ));
        fields.push(Field::new(
            format!("maybe_posted_at_{group}"),
            DataType::Date64,
            true,
        ));
    }

    Arc::new(Schema::new(fields))
}

fn date_fast_path_columns(offset: usize, len: usize) -> Result<Vec<ArrayRef>, WriterBenchError> {
    let mut columns: Vec<ArrayRef> = Vec::with_capacity(33);
    columns.push(Arc::new(
        (offset..offset + len)
            .map(deterministic_i32)
            .collect::<Int32Array>(),
    ));

    for group in 0..8 {
        columns.push(Arc::new(Date32Array::from_iter_values(
            (offset..offset + len).map(|row| 19_723_i32 + ((row + group * 17) % 365) as i32),
        )));
        columns.push(Arc::new(
            (offset..offset + len)
                .map(|row| {
                    (row % (11 + group) != 0)
                        .then_some(19_723_i32 + ((row + group * 19) % 365) as i32)
                })
                .collect::<Date32Array>(),
        ));
        columns.push(Arc::new(Date64Array::from_iter_values(
            (offset..offset + len)
                .map(|row| 1_735_689_600_000_i64 + ((row + group) as i64 * 3_701)),
        )));
        columns.push(Arc::new(
            (offset..offset + len)
                .map(|row| {
                    (row % (13 + group) != 0)
                        .then_some(1_735_689_600_000_i64 + ((row + group * 3) as i64 * 9_001))
                })
                .collect::<Date64Array>(),
        ));
    }

    Ok(columns)
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
    string_heavy_columns_with_shape(offset, len, StringHeavyText::Ascii, StringHeavyShape::Full)
}

fn string_heavy_text_only_columns(
    offset: usize,
    len: usize,
) -> Result<Vec<ArrayRef>, WriterBenchError> {
    string_heavy_columns_with_shape(
        offset,
        len,
        StringHeavyText::Ascii,
        StringHeavyShape::TextOnly,
    )
}

fn string_heavy_binary_only_columns(
    offset: usize,
    len: usize,
) -> Result<Vec<ArrayRef>, WriterBenchError> {
    string_heavy_columns_with_shape(
        offset,
        len,
        StringHeavyText::Ascii,
        StringHeavyShape::BinaryOnly,
    )
}

fn string_heavy_inline_4k_columns(
    offset: usize,
    len: usize,
) -> Result<Vec<ArrayRef>, WriterBenchError> {
    string_heavy_columns_with_shape(
        offset,
        len,
        StringHeavyText::Ascii,
        StringHeavyShape::Fixed {
            body_chars: 1_024,
            payload_bytes: 2_048,
        },
    )
}

fn string_heavy_edge_7k_columns(
    offset: usize,
    len: usize,
) -> Result<Vec<ArrayRef>, WriterBenchError> {
    string_heavy_columns_with_shape(
        offset,
        len,
        StringHeavyText::Ascii,
        StringHeavyShape::Fixed {
            body_chars: 1_536,
            payload_bytes: 4_096,
        },
    )
}

fn string_heavy_lob_9k_columns(
    offset: usize,
    len: usize,
) -> Result<Vec<ArrayRef>, WriterBenchError> {
    string_heavy_columns_with_shape(
        offset,
        len,
        StringHeavyText::Ascii,
        StringHeavyShape::Fixed {
            body_chars: 2_048,
            payload_bytes: 5_120,
        },
    )
}

fn string_heavy_unicode_columns(
    offset: usize,
    len: usize,
) -> Result<Vec<ArrayRef>, WriterBenchError> {
    string_heavy_columns_with_shape(
        offset,
        len,
        StringHeavyText::Unicode,
        StringHeavyShape::Full,
    )
}

#[derive(Clone, Copy)]
enum StringHeavyText {
    Ascii,
    Unicode,
}

#[derive(Clone, Copy)]
enum StringHeavyShape {
    Full,
    TextOnly,
    BinaryOnly,
    Fixed {
        body_chars: usize,
        payload_bytes: usize,
    },
}

fn string_heavy_columns_with_shape(
    offset: usize,
    len: usize,
    text: StringHeavyText,
    shape: StringHeavyShape,
) -> Result<Vec<ArrayRef>, WriterBenchError> {
    let id = (offset..offset + len)
        .map(|row| 900_000_000_i64 + row as i64)
        .collect::<Int64Array>();
    let tenant = (offset..offset + len)
        .map(|row| Some(text.tenant(row)))
        .collect::<StringArray>();
    let document_type = (offset..offset + len)
        .map(|row| {
            if row % 37 == 0 {
                None
            } else {
                Some(text.document_type(row))
            }
        })
        .collect::<StringArray>();
    let title = (offset..offset + len)
        .map(|row| {
            if row % 41 == 0 {
                None
            } else {
                Some(text.title(row))
            }
        })
        .collect::<StringArray>();
    let body = (offset..offset + len)
        .map(|row| {
            if row % 43 == 0 {
                None
            } else {
                Some(text.body(row, shape.body_len(row)))
            }
        })
        .collect::<StringArray>();
    let metadata = (offset..offset + len)
        .map(|row| {
            if row % 47 == 0 {
                None
            } else {
                Some(text.metadata(row))
            }
        })
        .collect::<StringArray>();
    let payload = (offset..offset + len)
        .map(|row| {
            if row % 53 == 0 {
                None
            } else {
                Some(deterministic_payload_with_len(row, shape.payload_len(row)))
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

impl StringHeavyShape {
    fn body_len(self, row: usize) -> usize {
        match self {
            Self::Full | Self::TextOnly => 512 + row % 2_048,
            Self::BinaryOnly => 24 + row % 96,
            Self::Fixed { body_chars, .. } => body_chars,
        }
    }

    fn payload_len(self, row: usize) -> usize {
        match self {
            Self::Full | Self::BinaryOnly => 1_024 + row % 4_096,
            Self::TextOnly => 8 + row % 32,
            Self::Fixed { payload_bytes, .. } => payload_bytes,
        }
    }
}

impl StringHeavyText {
    fn tenant(self, row: usize) -> String {
        match self {
            Self::Ascii => format!("tenant-{:04}", row % 512),
            Self::Unicode => format!("\u{79df}\u{6237}-{:04}", row % 512),
        }
    }

    fn document_type(self, row: usize) -> String {
        const ASCII_TYPES: &[&str] = &["invoice", "event", "profile", "message", "audit"];
        const UNICODE_TYPES: &[&str] = &[
            "\u{53d1}\u{7968}",
            "\u{4e8b}\u{4ef6}",
            "\u{6863}\u{6848}",
            "\u{6d88}\u{606f}",
            "\u{5ba1}\u{8ba1}",
        ];

        let values = match self {
            Self::Ascii => ASCII_TYPES,
            Self::Unicode => UNICODE_TYPES,
        };
        values[row % values.len()].to_owned()
    }

    fn title(self, row: usize) -> String {
        match self {
            Self::Ascii => format!("document title {row:012}"),
            Self::Unicode => format!("\u{6587}\u{6863} title {row:012}"),
        }
    }

    fn body(self, row: usize, len: usize) -> String {
        match self {
            Self::Ascii => deterministic_text(row, len),
            Self::Unicode => deterministic_unicode_text(row, len),
        }
    }

    fn metadata(self, row: usize) -> String {
        match self {
            Self::Ascii => format!(
                "{{\"tenant\":{},\"source\":{},\"sequence\":{row}}}",
                row % 512,
                row % 17
            ),
            Self::Unicode => format!(
                "{{\"tenant\":\"\u{79df}\u{6237}-{:04}\",\"source\":\"\u{6765}\u{6e90}-{}\",\"sequence\":{row}}}",
                row % 512,
                row % 17
            ),
        }
    }
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

fn deterministic_unicode_text(row: usize, len: usize) -> String {
    const ALPHABET: &[char] = &[
        '\u{6570}', '\u{636e}', '\u{5199}', '\u{5165}', '\u{6d4b}', '\u{8bd5}', '\u{00e9}',
        '\u{00f1}', '\u{03bb}', '\u{03a9}', '0', '1', '2', '3', ' ',
    ];
    let mut value = String::with_capacity(len);

    for index in 0..len {
        value.push(ALPHABET[(row.wrapping_mul(31) + index.wrapping_mul(7)) % ALPHABET.len()]);
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
        Array, BinaryArray, BooleanArray, Date32Array, Date64Array, Decimal128Array, Float32Array,
        Float64Array, Int8Array, Int16Array, Int32Array, Int64Array, RecordBatch, StringArray,
        TimestampMillisecondArray, UInt8Array, UInt16Array, UInt32Array, UInt64Array,
    };
    use arrow_schema::{DataType, TimeUnit};
    use arrow_tiberius::MssqlType;
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::{ffi::OsString, time::Duration};

    static TEST_FILE_COUNTER: AtomicU64 = AtomicU64::new(0);

    fn sql_server_profile_sample(
        status_and_wait: Option<(&str, Option<&str>)>,
        waiting_task_waits: &[&str],
    ) -> SqlServerProfileSample {
        sql_server_profile_sample_with_phase(
            "write_batch",
            10,
            11,
            status_and_wait,
            waiting_task_waits,
        )
    }

    fn sql_server_profile_sample_with_phase(
        phase: &str,
        write_elapsed_start_ms: u64,
        write_elapsed_end_ms: u64,
        status_and_wait: Option<(&str, Option<&str>)>,
        waiting_task_waits: &[&str],
    ) -> SqlServerProfileSample {
        SqlServerProfileSample {
            phase: phase.to_owned(),
            repeat_index: 0,
            write_elapsed_start: Duration::from_millis(write_elapsed_start_ms),
            write_elapsed_end: Duration::from_millis(write_elapsed_end_ms),
            activity: sqlserver_profile::ActivitySnapshot {
                connection: sqlserver_profile::ConnectionSnapshot {
                    net_transport: "TCP".to_owned(),
                    protocol_type: "TSQL".to_owned(),
                    encrypt_option: "FALSE".to_owned(),
                    net_packet_size: 4096,
                    num_reads: 3,
                    num_writes: 5,
                    last_read: Some("2026-05-21T12:00:00".to_owned()),
                    last_write: Some("2026-05-21T12:00:01".to_owned()),
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

    fn connection_snapshot(
        num_reads: i64,
        num_writes: i64,
        last_read: Option<&str>,
        last_write: Option<&str>,
    ) -> sqlserver_profile::ConnectionSnapshot {
        sqlserver_profile::ConnectionSnapshot {
            net_transport: "TCP".to_owned(),
            protocol_type: "TSQL".to_owned(),
            encrypt_option: "FALSE".to_owned(),
            net_packet_size: 4096,
            num_reads,
            num_writes,
            last_read: last_read.map(ToOwned::to_owned),
            last_write: last_write.map(ToOwned::to_owned),
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
    fn sql_server_profile_sample_coverage_is_grouped_by_phase() {
        let samples = [
            sql_server_profile_sample_with_phase("write_batch", 10, 11, None, &[]),
            sql_server_profile_sample_with_phase("finish", 1, 2, None, &[]),
            sql_server_profile_sample_with_phase("write_batch", 20, 21, None, &[]),
        ];

        let coverages = super::sql_server_profile_sample_coverages(&samples);

        assert_eq!(coverages.len(), 2);
        assert_eq!(coverages[0].phase, "finish");
        assert_eq!(coverages[0].first_elapsed_start, Duration::from_millis(1));
        assert_eq!(coverages[0].last_elapsed_end, Duration::from_millis(2));
        assert_eq!(coverages[1].phase, "write_batch");
        assert_eq!(coverages[1].first_elapsed_start, Duration::from_millis(10));
        assert_eq!(coverages[1].last_elapsed_end, Duration::from_millis(21));
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
                    signal_wait_time_ms: 3,
                },
                super::SqlServerSessionWaitDelta {
                    wait_type: "ASYNC_NETWORK_IO".to_owned(),
                    waiting_tasks_count: 2,
                    wait_time_ms: 11,
                    signal_wait_time_ms: 3,
                },
            ]
        );
    }

    #[test]
    fn sql_server_profile_connection_delta_counts_packet_counters() {
        let initial = connection_snapshot(3, 5, Some("2026-05-21T12:00:00"), None);
        let final_connection = connection_snapshot(
            23,
            11,
            Some("2026-05-21T12:00:03"),
            Some("2026-05-21T12:00:04"),
        );

        let delta = super::sql_server_connection_delta(&initial, &final_connection);

        assert_eq!(delta.num_reads, 20);
        assert_eq!(delta.num_writes, 6);
        assert_eq!(delta.net_packet_size, 4096);
        assert_eq!(delta.last_read.as_deref(), Some("2026-05-21T12:00:03"));
        assert_eq!(delta.last_write.as_deref(), Some("2026-05-21T12:00:04"));
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
            OsString::from("--sqlserver-recovery-model"),
            OsString::from("simple"),
            OsString::from("--sqlserver-bulk-table-lock"),
            OsString::from("--tds-packet-size"),
            OsString::from("32767"),
            OsString::from("--arrow-odbc-autocommit"),
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
        assert_eq!(
            options.sql_server_recovery_model,
            Some(super::SqlServerRecoveryModel::Simple)
        );
        assert!(options.sql_server_bulk_table_lock);
        assert_eq!(options.tds_packet_size, Some(32767));
        assert!(options.arrow_odbc_autocommit);
    }

    #[test]
    fn compare_rejects_unknown_sql_server_recovery_model() {
        let args = [
            OsString::from("--sqlserver-recovery-model"),
            OsString::from("minimal"),
        ];

        let err = super::CompareBenchOptions::parse(&args).unwrap_err();

        assert!(matches!(
            err,
            WriterBenchError::Validation(message)
                if message.contains("recovery model `minimal`")
                    && message.contains("bulk-logged")
        ));
    }

    #[test]
    fn parses_sql_server_reported_recovery_models() {
        assert_eq!(
            super::SqlServerRecoveryModel::from_sql_server_desc("FULL").unwrap(),
            super::SqlServerRecoveryModel::Full
        );
        assert_eq!(
            super::SqlServerRecoveryModel::from_sql_server_desc("BULK_LOGGED").unwrap(),
            super::SqlServerRecoveryModel::BulkLogged
        );
        assert_eq!(
            super::SqlServerRecoveryModel::from_sql_server_desc("SIMPLE").unwrap(),
            super::SqlServerRecoveryModel::Simple
        );

        let err = super::SqlServerRecoveryModel::from_sql_server_desc("UNKNOWN").unwrap_err();
        assert!(matches!(
            err,
            WriterBenchError::Validation(message)
                if message.contains("recovery model reported by server `UNKNOWN`")
        ));
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
            OsString::from(
                "baseline,direct-framed,direct-raw,direct-raw-no-date-fast-path,direct-raw-no-fixed-fast-path,arrow-odbc,odbc-bcp",
            ),
            OsString::from("--odbc-bcp-defer-batches"),
        ];

        let options = super::CompareBenchOptions::parse(&args).unwrap();

        assert_eq!(
            options.backends,
            [
                super::BenchmarkBackend::Baseline,
                super::BenchmarkBackend::DirectFramed,
                super::BenchmarkBackend::DirectRaw,
                super::BenchmarkBackend::DirectRawNoDateFastPath,
                super::BenchmarkBackend::DirectRawNoFixedFastPath,
                super::BenchmarkBackend::ArrowOdbc,
                super::BenchmarkBackend::OdbcBcp
            ]
        );
        assert!(options.odbc_bcp_defer_batches);
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
    fn compare_allows_direct_framed_for_narrow_numeric() {
        let args = [
            OsString::from("--scenario"),
            OsString::from("narrow_numeric"),
            OsString::from("--backends"),
            OsString::from("direct-framed"),
        ];

        let options = super::CompareBenchOptions::parse(&args).unwrap();

        assert_eq!(options.backends, [super::BenchmarkBackend::DirectFramed]);
        super::ensure_direct_raw_supported_scenario(&options.benchmark).unwrap();
    }

    #[test]
    fn compare_allows_date_fast_path_ab_backend_for_date_fast_path_scenario() {
        let args = [
            OsString::from("--scenario"),
            OsString::from("date_fast_path"),
            OsString::from("--backends"),
            OsString::from("direct-raw-no-date-fast-path,direct-raw"),
        ];

        let options = super::CompareBenchOptions::parse(&args).unwrap();

        assert_eq!(
            options.backends,
            [
                super::BenchmarkBackend::DirectRawNoDateFastPath,
                super::BenchmarkBackend::DirectRaw
            ]
        );
        super::ensure_direct_date_fast_path_ab_scenario(&options.benchmark).unwrap();
    }

    #[test]
    fn compare_allows_fixed_width_fast_path_ab_backend_for_narrow_numeric() {
        let args = [
            OsString::from("--scenario"),
            OsString::from("narrow_numeric"),
            OsString::from("--backends"),
            OsString::from("direct-raw-no-fixed-fast-path,direct-raw"),
        ];

        let options = super::CompareBenchOptions::parse(&args).unwrap();

        assert_eq!(
            options.backends,
            [
                super::BenchmarkBackend::DirectRawNoFixedFastPath,
                super::BenchmarkBackend::DirectRaw
            ]
        );
        super::ensure_direct_raw_supported_scenario(&options.benchmark).unwrap();
    }

    #[test]
    fn compare_rejects_date_fast_path_ab_backend_for_other_scenarios() {
        let args = [
            OsString::from("--scenario"),
            OsString::from("narrow_numeric"),
            OsString::from("--backends"),
            OsString::from("direct-raw-no-date-fast-path"),
        ];

        let options = super::CompareBenchOptions::parse(&args).unwrap();
        let err = super::ensure_direct_date_fast_path_ab_scenario(&options.benchmark).unwrap_err();

        assert!(matches!(
            err,
            WriterBenchError::Validation(message)
                if message.contains("direct-raw-no-date-fast-path")
                    && message.contains("date_fast_path")
                    && message.contains("narrow_numeric")
        ));
    }

    #[test]
    fn compare_allows_direct_raw_for_additional_supported_scenarios() {
        for scenario in [
            "extended_primitive",
            "uint64_policy",
            "date_fast_path",
            "mixed_nullable",
            "string_heavy",
            "string_heavy_text_only",
            "string_heavy_binary_only",
            "string_heavy_inline_4k",
            "string_heavy_edge_7k",
            "string_heavy_lob_9k",
            "string_heavy_unicode",
            "wide_sparse",
        ] {
            let args = [
                OsString::from("--scenario"),
                OsString::from(scenario),
                OsString::from("--backends"),
                OsString::from("direct-framed,direct-raw"),
            ];

            let options = super::CompareBenchOptions::parse(&args).unwrap();

            assert_eq!(
                options.backends,
                [
                    super::BenchmarkBackend::DirectFramed,
                    super::BenchmarkBackend::DirectRaw
                ]
            );
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
                    if message.contains("direct backends")
                        && message.contains("narrow_numeric")
                        && message.contains("extended_primitive")
                        && message.contains("uint64_policy")
                        && message.contains("date_fast_path")
                        && message.contains("mixed_nullable")
                        && message.contains("string_heavy")
                        && message.contains("string_heavy_text_only")
                        && message.contains("string_heavy_binary_only")
                        && message.contains("string_heavy_unicode")
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
                    && message.contains("direct-framed")
                    && message.contains("direct-raw")
        ));
    }

    #[test]
    fn compare_rejects_bcp_deferred_batches_without_bcp_backend() {
        let args = [
            OsString::from("--backends"),
            OsString::from("baseline"),
            OsString::from("--odbc-bcp-defer-batches"),
        ];

        let options = super::CompareBenchOptions::parse(&args).unwrap();
        let err = super::run_compare_benchmark(&options).unwrap_err();

        assert!(matches!(
            err,
            WriterBenchError::Validation(message)
                if message.contains("--odbc-bcp-defer-batches")
                    && message.contains("odbc-bcp")
        ));
    }

    #[test]
    fn compare_rejects_arrow_odbc_autocommit_without_arrow_odbc_backend() {
        let args = [
            OsString::from("--backends"),
            OsString::from("baseline"),
            OsString::from("--arrow-odbc-autocommit"),
        ];

        let options = super::CompareBenchOptions::parse(&args).unwrap();
        let err = super::run_compare_benchmark(&options).unwrap_err();

        assert!(matches!(
            err,
            WriterBenchError::Validation(message)
                if message.contains("--arrow-odbc-autocommit")
                    && message.contains("arrow-odbc")
        ));
    }

    #[test]
    fn compare_rejects_extended_primitive_for_arrow_odbc() {
        let args = [
            OsString::from("--scenario"),
            OsString::from("extended_primitive"),
            OsString::from("--backends"),
            OsString::from("baseline,arrow-odbc"),
        ];

        let options = super::CompareBenchOptions::parse(&args).unwrap();
        let err = super::run_compare_benchmark(&options).unwrap_err();

        assert!(matches!(
            err,
            WriterBenchError::Validation(message)
                if message.contains("arrow-odbc")
                    && message.contains("extended_primitive")
                    && message.contains("unsigned integer mappings")
        ));
    }

    #[test]
    fn compare_rejects_uint64_policy_for_odbc_bcp() {
        let args = [
            OsString::from("--scenario"),
            OsString::from("uint64_policy"),
            OsString::from("--backends"),
            OsString::from("baseline,odbc-bcp"),
        ];

        let options = super::CompareBenchOptions::parse(&args).unwrap();
        let err = super::run_compare_benchmark(&options).unwrap_err();

        assert!(matches!(
            err,
            WriterBenchError::Validation(message)
                if message.contains("odbc-bcp")
                    && message.contains("uint64_policy")
                    && message.contains("UInt64")
        ));
    }

    #[test]
    fn odbc_bcp_supports_sql_server_profile() {
        assert!(super::BenchmarkBackend::OdbcBcp.supports_sql_server_profile());
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
            sql_server_recovery_model: None,
            sql_server_bulk_table_lock: false,
            backends: vec![super::BenchmarkBackend::Baseline],
            runner_image: crate::odbc_runner::DEFAULT_RUNNER_IMAGE_TAG.to_owned(),
            keep_runner_image: false,
            profile_direct: false,
            tds_packet_size: None,
            arrow_odbc_autocommit: false,
            odbc_bcp_defer_batches: false,
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
    fn renders_uint64_policy_benchmark_table_ddl() {
        let table = arrow_tiberius::TableName::new("dbo", "bench").unwrap();
        let scenario = super::scenario_by_name("uint64_policy").unwrap();
        let mappings = super::benchmark_mappings_for_scenario(scenario).unwrap();
        let sql = super::benchmark_table_sql(&table, &mappings);

        assert_eq!(
            sql,
            "CREATE TABLE [dbo].[bench] (\n    [row_id] int NOT NULL,\n    [u64_small] decimal(20,0) NOT NULL,\n    [u64_mid] decimal(20,0) NOT NULL,\n    [u64_full] decimal(20,0) NOT NULL,\n    [maybe_u64_full] decimal(20,0) NULL\n);"
        );
    }

    #[test]
    fn renders_bulk_table_lock_sql_for_benchmark_table() {
        let table = arrow_tiberius::TableName::new("dbo", "bench'o").unwrap();

        assert_eq!(
            super::benchmark_bulk_table_lock_sql(&table),
            "EXEC sys.sp_tableoption N'[dbo].[bench''o]', 'table lock on bulk load', 'ON';"
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

        let args = super::arrow_odbc_runner_args(
            &options.benchmark,
            "/workspace/target/bench.arrow",
            false,
            false,
            false,
        )
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
    fn arrow_odbc_runner_args_pass_sql_server_profile_flag() {
        let benchmark = WriterBenchOptions {
            rows: 25,
            batch_size: 5,
            scenario: super::scenario_by_name("mixed_nullable").unwrap(),
            repeat: 3,
            output: BenchmarkOutput::Human,
        };

        let args = super::arrow_odbc_runner_args(
            &benchmark,
            "/workspace/target/bench.arrow",
            true,
            false,
            false,
        )
        .unwrap();

        assert!(args.iter().any(|arg| arg == "--profile-sqlserver"));
    }

    #[test]
    fn arrow_odbc_runner_args_pass_autocommit_flag() {
        let benchmark = WriterBenchOptions {
            rows: 25,
            batch_size: 5,
            scenario: super::scenario_by_name("mixed_nullable").unwrap(),
            repeat: 3,
            output: BenchmarkOutput::Human,
        };

        let args = super::arrow_odbc_runner_args(
            &benchmark,
            "/workspace/target/bench.arrow",
            false,
            true,
            false,
        )
        .unwrap();

        assert!(args.iter().any(|arg| arg == "--autocommit"));
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

        let args = super::odbc_bcp_runner_args(
            &benchmark,
            "/workspace/target/bench.arrow",
            false,
            false,
            false,
        )
        .unwrap();

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

        let args = super::odbc_bcp_runner_args(
            &benchmark,
            "/workspace/target/bench.arrow",
            false,
            false,
            false,
        )
        .unwrap();

        assert!(
            args.windows(2)
                .any(|pair| pair == ["--scenario", "mixed_nullable"])
        );
    }

    #[test]
    fn odbc_bcp_runner_args_pass_sql_server_profile_flag() {
        let benchmark = WriterBenchOptions {
            rows: 25,
            batch_size: 5,
            scenario: super::scenario_by_name("narrow_numeric").unwrap(),
            repeat: 3,
            output: BenchmarkOutput::Human,
        };

        let args = super::odbc_bcp_runner_args(
            &benchmark,
            "/workspace/target/bench.arrow",
            true,
            false,
            false,
        )
        .unwrap();

        assert!(args.iter().any(|arg| arg == "--profile-sqlserver"));
    }

    #[test]
    fn odbc_bcp_runner_args_pass_deferred_batch_flag() {
        let benchmark = WriterBenchOptions {
            rows: 25,
            batch_size: 5,
            scenario: super::scenario_by_name("narrow_numeric").unwrap(),
            repeat: 3,
            output: BenchmarkOutput::Human,
        };

        let args = super::odbc_bcp_runner_args(
            &benchmark,
            "/workspace/target/bench.arrow",
            false,
            true,
            false,
        )
        .unwrap();

        assert!(args.iter().any(|arg| arg == "--defer-batches"));
    }

    #[test]
    fn odbc_bcp_runner_args_pass_bulk_table_lock_table_option() {
        let benchmark = WriterBenchOptions {
            rows: 25,
            batch_size: 5,
            scenario: super::scenario_by_name("narrow_numeric").unwrap(),
            repeat: 3,
            output: BenchmarkOutput::Human,
        };

        let args = super::odbc_bcp_runner_args(
            &benchmark,
            "/workspace/target/bench.arrow",
            false,
            false,
            true,
        )
        .unwrap();

        assert!(args.windows(2).any(|pair| {
            pair[0] == "--create-table-sql-template"
                && pair[1].contains("table lock on bulk load")
                && pair[1].contains(super::ODBC_TABLE_PLACEHOLDER)
        }));
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
    fn arrow_odbc_command_rejects_extended_primitive() {
        let args = [
            OsString::from("--scenario"),
            OsString::from("extended_primitive"),
        ];

        let options = super::ArrowOdbcBenchOptions::parse(&args).unwrap();
        let err = super::ensure_arrow_odbc_supported_scenario(&options.benchmark).unwrap_err();

        assert!(matches!(
            err,
            WriterBenchError::Validation(message)
                if message.contains("arrow-odbc")
                    && message.contains("extended_primitive")
                    && message.contains("unsigned integer mappings")
        ));
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
    fn extended_primitive_schema_matches_definition() {
        let options = WriterBenchOptions {
            rows: 1,
            batch_size: 1,
            scenario: super::scenario_by_name("extended_primitive").unwrap(),
            repeat: 1,
            output: BenchmarkOutput::Human,
        };

        let batches = generated_batches(&options);
        let schema = batches[0].schema();

        assert_eq!(schema.fields().len(), 12);
        assert_eq!(schema.field(0).data_type(), &DataType::UInt8);
        assert!(!schema.field(0).is_nullable());
        assert_eq!(schema.field(1).data_type(), &DataType::UInt8);
        assert!(schema.field(1).is_nullable());
        assert_eq!(schema.field(2).data_type(), &DataType::Int8);
        assert!(!schema.field(2).is_nullable());
        assert_eq!(schema.field(3).data_type(), &DataType::Int8);
        assert!(schema.field(3).is_nullable());
        assert_eq!(schema.field(4).data_type(), &DataType::Int16);
        assert!(!schema.field(4).is_nullable());
        assert_eq!(schema.field(5).data_type(), &DataType::Int16);
        assert!(schema.field(5).is_nullable());
        assert_eq!(schema.field(6).data_type(), &DataType::UInt16);
        assert!(!schema.field(6).is_nullable());
        assert_eq!(schema.field(7).data_type(), &DataType::UInt16);
        assert!(schema.field(7).is_nullable());
        assert_eq!(schema.field(8).data_type(), &DataType::UInt32);
        assert!(!schema.field(8).is_nullable());
        assert_eq!(schema.field(9).data_type(), &DataType::UInt32);
        assert!(schema.field(9).is_nullable());
        assert_eq!(schema.field(10).data_type(), &DataType::Float32);
        assert!(!schema.field(10).is_nullable());
        assert_eq!(schema.field(11).data_type(), &DataType::Float32);
        assert!(schema.field(11).is_nullable());
    }

    #[test]
    fn uint64_policy_schema_maps_to_decimal20() {
        let scenario = super::scenario_by_name("uint64_policy").unwrap();
        let options = WriterBenchOptions {
            rows: 64,
            batch_size: 64,
            scenario,
            repeat: 1,
            output: BenchmarkOutput::Human,
        };

        let batches = generated_batches(&options);
        let batch = &batches[0];
        let schema = batch.schema();
        let mappings = super::benchmark_mappings_for_scenario(scenario).unwrap();

        assert_eq!(schema.fields().len(), 5);
        assert_eq!(schema.field(0).data_type(), &DataType::Int32);
        assert_eq!(schema.field(1).data_type(), &DataType::UInt64);
        assert_eq!(schema.field(2).data_type(), &DataType::UInt64);
        assert_eq!(schema.field(3).data_type(), &DataType::UInt64);
        assert_eq!(schema.field(4).data_type(), &DataType::UInt64);
        assert!(schema.field(4).is_nullable());
        assert!(batch.column(1).as_any().is::<UInt64Array>());
        assert!(batch.column(4).as_any().is::<UInt64Array>());
        assert!(batch.column(4).null_count() > 0);
        assert_eq!(
            mappings
                .iter()
                .map(|mapping| mapping.mssql().ty())
                .collect::<Vec<_>>(),
            [
                &MssqlType::Int,
                &MssqlType::Decimal {
                    precision: 20,
                    scale: 0
                },
                &MssqlType::Decimal {
                    precision: 20,
                    scale: 0
                },
                &MssqlType::Decimal {
                    precision: 20,
                    scale: 0
                },
                &MssqlType::Decimal {
                    precision: 20,
                    scale: 0
                }
            ]
        );
    }

    #[test]
    fn date_fast_path_schema_maps_date64_to_datetime2() {
        let scenario = super::scenario_by_name("date_fast_path").unwrap();
        let mappings = super::benchmark_mappings_for_scenario(scenario).unwrap();
        let mssql_types = mappings
            .iter()
            .map(|mapping| mapping.mssql().ty())
            .collect::<Vec<_>>();

        assert_eq!(mssql_types[0], &MssqlType::Int);
        assert_eq!(mssql_types.len(), 33);
        assert_eq!(
            mssql_types
                .iter()
                .filter(|ty| matches!(ty, &&MssqlType::Date))
                .count(),
            16
        );
        assert_eq!(
            mssql_types
                .iter()
                .filter(|ty| matches!(ty, &&MssqlType::DateTime2 { precision: 3 }))
                .count(),
            16
        );
    }

    #[test]
    fn extended_primitive_runtime_array_types_and_nulls_match_schema() {
        let options = WriterBenchOptions {
            rows: 64,
            batch_size: 64,
            scenario: super::scenario_by_name("extended_primitive").unwrap(),
            repeat: 1,
            output: BenchmarkOutput::Human,
        };

        let batches = generated_batches(&options);
        let batch = &batches[0];

        assert!(batch.column(0).as_any().is::<UInt8Array>());
        assert_eq!(batch.column(0).null_count(), 0);
        assert!(batch.column(1).as_any().is::<UInt8Array>());
        assert!(batch.column(1).null_count() > 0);
        assert!(batch.column(2).as_any().is::<Int8Array>());
        assert_eq!(batch.column(2).null_count(), 0);
        assert!(batch.column(3).as_any().is::<Int8Array>());
        assert!(batch.column(3).null_count() > 0);
        assert!(batch.column(4).as_any().is::<Int16Array>());
        assert_eq!(batch.column(4).null_count(), 0);
        assert!(batch.column(5).as_any().is::<Int16Array>());
        assert!(batch.column(5).null_count() > 0);
        assert!(batch.column(6).as_any().is::<UInt16Array>());
        assert_eq!(batch.column(6).null_count(), 0);
        assert!(batch.column(7).as_any().is::<UInt16Array>());
        assert!(batch.column(7).null_count() > 0);
        assert!(batch.column(8).as_any().is::<UInt32Array>());
        assert_eq!(batch.column(8).null_count(), 0);
        assert!(batch.column(9).as_any().is::<UInt32Array>());
        assert!(batch.column(9).null_count() > 0);
        assert!(batch.column(10).as_any().is::<Float32Array>());
        assert_eq!(batch.column(10).null_count(), 0);
        assert!(batch.column(11).as_any().is::<Float32Array>());
        assert!(batch.column(11).null_count() > 0);
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
    fn date_fast_path_covers_nullable_and_non_nullable_date_columns() {
        let options = WriterBenchOptions {
            rows: 64,
            batch_size: 64,
            scenario: super::scenario_by_name("date_fast_path").unwrap(),
            repeat: 1,
            output: BenchmarkOutput::Human,
        };

        let batches = generated_batches(&options);
        let batch = &batches[0];
        let schema = batch.schema();

        assert_eq!(schema.fields().len(), 33);
        assert_eq!(schema.field(0).data_type(), &DataType::Int32);
        assert_eq!(batch.column(0).null_count(), 0);

        for group in 0..8 {
            let base = 1 + group * 4;
            assert_eq!(schema.field(base).data_type(), &DataType::Date32);
            assert_eq!(schema.field(base + 1).data_type(), &DataType::Date32);
            assert_eq!(schema.field(base + 2).data_type(), &DataType::Date64);
            assert_eq!(schema.field(base + 3).data_type(), &DataType::Date64);
            assert!(!schema.field(base).is_nullable());
            assert!(schema.field(base + 1).is_nullable());
            assert!(!schema.field(base + 2).is_nullable());
            assert!(schema.field(base + 3).is_nullable());
            assert!(batch.column(base).as_any().is::<Date32Array>());
            assert!(batch.column(base + 1).as_any().is::<Date32Array>());
            assert!(batch.column(base + 2).as_any().is::<Date64Array>());
            assert!(batch.column(base + 3).as_any().is::<Date64Array>());
            assert_eq!(batch.column(base).null_count(), 0);
            assert!(batch.column(base + 1).null_count() > 0);
            assert_eq!(batch.column(base + 2).null_count(), 0);
            assert!(batch.column(base + 3).null_count() > 0);
        }
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
    fn string_heavy_unicode_keeps_string_heavy_shape_with_bmp_text() {
        let string_heavy = WriterBenchOptions {
            rows: 128,
            batch_size: 128,
            scenario: super::scenario_by_name("string_heavy").unwrap(),
            repeat: 1,
            output: BenchmarkOutput::Human,
        };
        let unicode = WriterBenchOptions {
            scenario: super::scenario_by_name("string_heavy_unicode").unwrap(),
            ..string_heavy.clone()
        };

        let ascii_batch = &generated_batches(&string_heavy)[0];
        let unicode_batch = &generated_batches(&unicode)[0];
        let ascii_body = ascii_batch
            .column(4)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let unicode_tenant = unicode_batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let unicode_body = unicode_batch
            .column(4)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let unicode_payload = unicode_batch
            .column(6)
            .as_any()
            .downcast_ref::<BinaryArray>()
            .unwrap();

        assert_eq!(unicode_batch.schema(), ascii_batch.schema());
        assert_eq!(unicode_body.null_count(), ascii_body.null_count());
        assert_eq!(
            unicode_body.value(1).chars().count(),
            ascii_body.value(1).chars().count()
        );
        assert!(!unicode_tenant.value(0).is_ascii());
        assert!(!unicode_body.value(1).is_ascii());
        assert!(
            unicode_body
                .value(1)
                .chars()
                .all(|ch| (ch as u32) <= 0xffff)
        );
        assert_eq!(
            unicode_payload.value(1),
            ascii_batch
                .column(6)
                .as_any()
                .downcast_ref::<BinaryArray>()
                .unwrap()
                .value(1)
        );
    }

    #[test]
    fn string_heavy_text_and_binary_variants_split_large_payload_shape() {
        let full = WriterBenchOptions {
            rows: 128,
            batch_size: 128,
            scenario: super::scenario_by_name("string_heavy").unwrap(),
            repeat: 1,
            output: BenchmarkOutput::Human,
        };
        let text_only = WriterBenchOptions {
            scenario: super::scenario_by_name("string_heavy_text_only").unwrap(),
            ..full.clone()
        };
        let binary_only = WriterBenchOptions {
            scenario: super::scenario_by_name("string_heavy_binary_only").unwrap(),
            ..full.clone()
        };

        let full_batch = &generated_batches(&full)[0];
        let text_batch = &generated_batches(&text_only)[0];
        let binary_batch = &generated_batches(&binary_only)[0];

        let full_body = string_column(full_batch, 4);
        let full_payload = binary_column(full_batch, 6);
        let text_body = string_column(text_batch, 4);
        let text_payload = binary_column(text_batch, 6);
        let binary_body = string_column(binary_batch, 4);
        let binary_payload = binary_column(binary_batch, 6);

        assert_eq!(text_batch.schema(), full_batch.schema());
        assert_eq!(binary_batch.schema(), full_batch.schema());
        assert_eq!(text_body.null_count(), full_body.null_count());
        assert_eq!(text_payload.null_count(), full_payload.null_count());
        assert_eq!(binary_body.null_count(), full_body.null_count());
        assert_eq!(binary_payload.null_count(), full_payload.null_count());

        assert_eq!(text_body.value(1).len(), full_body.value(1).len());
        assert_eq!(binary_payload.value(1).len(), full_payload.value(1).len());
        assert!(text_payload.value(1).len() < full_payload.value(1).len() / 16);
        assert!(binary_body.value(1).len() < full_body.value(1).len() / 4);
    }

    #[test]
    fn string_heavy_threshold_variants_hold_fixed_combined_payload_sizes() {
        for (scenario_name, body_chars, payload_bytes, nominal_sql_bytes) in [
            ("string_heavy_inline_4k", 1_024, 2_048, 4_096),
            ("string_heavy_edge_7k", 1_536, 4_096, 7_168),
            ("string_heavy_lob_9k", 2_048, 5_120, 9_216),
        ] {
            let options = WriterBenchOptions {
                rows: 128,
                batch_size: 128,
                scenario: super::scenario_by_name(scenario_name).unwrap(),
                repeat: 1,
                output: BenchmarkOutput::Human,
            };

            let batch = &generated_batches(&options)[0];
            let body = string_column(batch, 4);
            let payload = binary_column(batch, 6);

            assert_eq!(batch.schema(), (super::string_heavy_schema)());
            assert_eq!(body.null_count(), 3);
            assert_eq!(payload.null_count(), 3);
            assert_eq!(body.value(1).len(), body_chars);
            assert_eq!(body.value(2).len(), body_chars);
            assert_eq!(payload.value(1).len(), payload_bytes);
            assert_eq!(payload.value(2).len(), payload_bytes);
            assert_eq!(
                body.value(1).len() * 2 + payload.value(1).len(),
                nominal_sql_bytes
            );
        }
    }

    #[test]
    fn string_heavy_unicode_sentinel_query_checks_tenant_codepoints() {
        let sql = super::string_heavy_unicode_tenant_sentinel_count_sql("[dbo].[target]");

        assert!(sql.contains("COUNT_BIG(*) FROM [dbo].[target]"));
        assert!(sql.contains("UNICODE(SUBSTRING([tenant], 1, 1)) = 31199"));
        assert!(sql.contains("UNICODE(SUBSTRING([tenant], 2, 1)) = 25143"));
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
            "date_fast_path",
            "wide_mixed",
            "decimal_temporal",
            "string_heavy",
            "string_heavy_text_only",
            "string_heavy_binary_only",
            "string_heavy_inline_4k",
            "string_heavy_edge_7k",
            "string_heavy_lob_9k",
            "string_heavy_unicode",
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
                "extended_primitive",
                "uint64_policy",
                "date_fast_path",
                "mixed_nullable",
                "wide_mixed",
                "decimal_temporal",
                "string_heavy",
                "string_heavy_text_only",
                "string_heavy_binary_only",
                "string_heavy_inline_4k",
                "string_heavy_edge_7k",
                "string_heavy_lob_9k",
                "string_heavy_unicode",
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

    fn string_column(batch: &RecordBatch, index: usize) -> &StringArray {
        batch
            .column(index)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
    }

    fn binary_column(batch: &RecordBatch, index: usize) -> &BinaryArray {
        batch
            .column(index)
            .as_any()
            .downcast_ref::<BinaryArray>()
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

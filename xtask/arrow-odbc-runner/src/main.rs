use std::collections::BTreeMap;
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
    if !options.autocommit {
        connection.set_autocommit(false)?;
    }
    let mut sql_server_profile = if options.profile_sqlserver {
        Some(SqlServerProfile::start(&connection)?)
    } else {
        None
    };

    let write_start = Instant::now();
    let total_rows_result = run_repeats(&connection, &options, sql_server_profile.as_mut());
    if options.autocommit {
        // ODBC already completed each execute under its connection autocommit policy.
    } else if total_rows_result.is_ok() {
        if let Some(profile) = sql_server_profile.as_mut() {
            profile.capture_phase(&connection, "commit", || {
                connection.commit()?;
                Ok(())
            })?;
        } else {
            connection.commit()?;
        }
    } else if let Err(error) = connection.rollback() {
        eprintln!("warning: failed to roll back arrow-odbc benchmark transaction: {error}");
    }
    let write_elapsed = write_start.elapsed();
    if let Some(profile) = sql_server_profile.as_mut() {
        profile.finish(&connection)?;
    }
    let total_rows = total_rows_result?;

    println!("arrow-odbc runner");
    println!("  database: {database}");
    println!("  scenario: {}", options.scenario);
    println!("  repeat: {}", options.repeat);
    println!("  transaction policy: {}", options.transaction_policy());
    println!("  rows written: {total_rows}");
    println!("  write seconds: {:.3}", write_elapsed.as_secs_f64());
    println!(
        "  write rows/sec: {:.2}",
        rows_per_second(total_rows, write_elapsed)
    );
    if let Some(peak_rss_kib) = current_process_peak_rss_kib() {
        println!("  peak rss KiB: {peak_rss_kib}");
    }
    if let Some(profile) = sql_server_profile {
        profile.print("  ");
    }

    Ok(())
}

fn run_repeats(
    connection: &Connection<'_>,
    options: &BenchOptions,
    mut sql_server_profile: Option<&mut SqlServerProfile>,
) -> Result<u64, Box<dyn Error>> {
    let mut total_rows = 0_u64;

    for repeat in 0..options.repeat {
        let table = format!(
            "[dbo].[arrow_tiberius_odbc_bench_{}_{}]",
            std::process::id(),
            repeat
        );
        let repeat_result = run_repeat(
            connection,
            &table,
            options,
            sql_server_profile.as_deref_mut(),
        );
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
    mut sql_server_profile: Option<&mut SqlServerProfile>,
) -> Result<u64, Box<dyn Error>> {
    execute_sql(connection, &format!("DROP TABLE IF EXISTS {table}"))?;
    execute_sql(connection, &options.create_table_sql(table)?)?;

    let input = ipc_batches(&options.input_ipc, options.rows)?;
    let mut reader = RecordBatchIterator::new(input.batches.into_iter().map(Ok), input.schema);
    if let Some(profile) = sql_server_profile.as_mut() {
        profile.capture_phase(connection, "insert", || {
            insert_into_table(connection, &mut reader, table, options.batch_size)?;
            Ok(())
        })?;
    } else {
        insert_into_table(connection, &mut reader, table, options.batch_size)?;
    }

    let actual = select_count(connection, table)?;
    let expected = u64::try_from(input.rows)?;
    if actual != expected {
        return Err(format!(
            "arrow-odbc row-count validation failed: expected {expected}, got {actual}"
        )
        .into());
    }

    if let Some(profile) = sql_server_profile {
        profile.snapshot_table_pages(connection, table)?;
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

fn current_process_peak_rss_kib() -> Option<u64> {
    let status = std::fs::read_to_string("/proc/self/status").ok()?;
    status.lines().find_map(|line| {
        let value = line.strip_prefix("VmHWM:")?.trim();
        let kib = value.strip_suffix("kB")?.trim();
        kib.parse().ok()
    })
}

fn required_env(name: &str) -> Result<String, Box<dyn Error>> {
    let value =
        env::var(name).map_err(|_| format!("missing required environment variable {name}"))?;

    if value.is_empty() {
        return Err(format!("required environment variable {name} is empty").into());
    }

    Ok(value)
}

#[derive(Debug)]
struct SqlServerProfile {
    writer_session_id: i32,
    recovery_model: String,
    initial_session_waits: Vec<SessionWaitSnapshot>,
    initial_database_file_io: Vec<DatabaseFileIoSnapshot>,
    session_wait_deltas: Vec<SessionWaitDelta>,
    database_file_io_deltas: Vec<DatabaseFileIoDelta>,
    phase_deltas: Vec<SqlServerProfilePhaseDelta>,
    table_page_snapshots: Vec<TablePageSnapshot>,
}

impl SqlServerProfile {
    fn start(connection: &Connection<'_>) -> Result<Self, Box<dyn Error>> {
        Ok(Self {
            writer_session_id: select_session_id(connection)?,
            recovery_model: recovery_model(connection)?,
            initial_session_waits: session_wait_snapshots(connection)?,
            initial_database_file_io: database_file_io_snapshots(connection)?,
            session_wait_deltas: Vec::new(),
            database_file_io_deltas: Vec::new(),
            phase_deltas: Vec::new(),
            table_page_snapshots: Vec::new(),
        })
    }

    fn finish(&mut self, connection: &Connection<'_>) -> Result<(), Box<dyn Error>> {
        self.session_wait_deltas = session_wait_deltas(
            &self.initial_session_waits,
            &session_wait_snapshots(connection)?,
        );
        self.database_file_io_deltas = database_file_io_deltas(
            &self.initial_database_file_io,
            &database_file_io_snapshots(connection)?,
        );
        Ok(())
    }

    fn print(&self, prefix: &str) {
        println!("{prefix}sql server profile:");
        println!("{prefix}  writer session id: {}", self.writer_session_id);
        println!("{prefix}  recovery model: {}", self.recovery_model);
        print_session_wait_deltas(prefix, &self.session_wait_deltas);
        print_database_file_io_deltas(prefix, &self.database_file_io_deltas);
        print_phase_deltas(prefix, &self.phase_deltas);
        print_table_page_snapshots(prefix, &self.table_page_snapshots);
    }

    fn capture_phase<T>(
        &mut self,
        connection: &Connection<'_>,
        phase: &str,
        work: impl FnOnce() -> Result<T, Box<dyn Error>>,
    ) -> Result<T, Box<dyn Error>> {
        let initial_session_waits = session_wait_snapshots(connection)?;
        let initial_database_file_io = database_file_io_snapshots(connection)?;
        let result = work();
        self.phase_deltas.push(SqlServerProfilePhaseDelta {
            phase: phase.to_owned(),
            session_wait_deltas: session_wait_deltas(
                &initial_session_waits,
                &session_wait_snapshots(connection)?,
            ),
            database_file_io_deltas: database_file_io_deltas(
                &initial_database_file_io,
                &database_file_io_snapshots(connection)?,
            ),
        });
        result
    }

    fn snapshot_table_pages(
        &mut self,
        connection: &Connection<'_>,
        table: &str,
    ) -> Result<(), Box<dyn Error>> {
        self.table_page_snapshots
            .push(table_page_snapshot(connection, table)?);
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct SessionWaitSnapshot {
    wait_type: String,
    waiting_tasks_count: i64,
    wait_time_ms: i64,
    signal_wait_time_ms: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct SessionWaitDelta {
    wait_type: String,
    waiting_tasks_count: i64,
    wait_time_ms: i64,
    signal_wait_time_ms: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct DatabaseFileIoSnapshot {
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
struct DatabaseFileIoDelta {
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
struct SqlServerProfilePhaseDelta {
    phase: String,
    session_wait_deltas: Vec<SessionWaitDelta>,
    database_file_io_deltas: Vec<DatabaseFileIoDelta>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct TablePageSnapshot {
    table: String,
    row_count: i64,
    in_row_used_page_count: i64,
    lob_used_page_count: i64,
    row_overflow_used_page_count: i64,
    used_page_count: i64,
}

fn print_session_wait_deltas(prefix: &str, waits: &[SessionWaitDelta]) {
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

fn print_database_file_io_deltas(prefix: &str, files: &[DatabaseFileIoDelta]) {
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

fn print_phase_deltas(prefix: &str, phases: &[SqlServerProfilePhaseDelta]) {
    if phases.is_empty() {
        println!("{prefix}  phase deltas: <none>");
        return;
    }

    println!("{prefix}  phase deltas:");
    for phase in phases {
        println!("{prefix}    {}:", phase.phase);
        print_session_wait_deltas(&format!("{prefix}    "), &phase.session_wait_deltas);
        print_database_file_io_deltas(&format!("{prefix}    "), &phase.database_file_io_deltas);
    }
}

fn print_table_page_snapshots(prefix: &str, tables: &[TablePageSnapshot]) {
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

fn select_session_id(connection: &Connection<'_>) -> Result<i32, Box<dyn Error>> {
    let cursor = connection
        .execute("SELECT CONVERT(nvarchar(16), @@SPID)", (), None)?
        .ok_or("SELECT @@SPID did not return a cursor")?;
    Ok(cursor_to_string(cursor)?.trim().parse()?)
}

fn recovery_model(connection: &Connection<'_>) -> Result<String, Box<dyn Error>> {
    let rows = text_rows(
        connection,
        "SELECT CONVERT(nvarchar(60), d.recovery_model_desc) \
        FROM sys.databases AS d \
        WHERE d.database_id = DB_ID()",
        1,
    )?;
    let row = rows
        .first()
        .ok_or("SQL Server recovery model snapshot found no current database")?;
    Ok(required_profile_column(row, 0, "recovery_model")?.to_owned())
}

fn table_page_snapshot(
    connection: &Connection<'_>,
    table: &str,
) -> Result<TablePageSnapshot, Box<dyn Error>> {
    let table_literal = table.replace('\'', "''");
    let rows = text_rows(
        connection,
        &format!(
            "SELECT \
                CONVERT(nvarchar(32), COALESCE(SUM(s.row_count), 0)), \
                CONVERT(nvarchar(32), COALESCE(SUM(s.in_row_used_page_count), 0)), \
                CONVERT(nvarchar(32), COALESCE(SUM(s.lob_used_page_count), 0)), \
                CONVERT(nvarchar(32), COALESCE(SUM(s.row_overflow_used_page_count), 0)), \
                CONVERT(nvarchar(32), COALESCE(SUM(s.used_page_count), 0)) \
            FROM sys.dm_db_partition_stats AS s \
            WHERE s.object_id = OBJECT_ID(N'{table_literal}')"
        ),
        5,
    )?;
    let row = rows
        .first()
        .ok_or_else(|| format!("SQL Server table page snapshot found no row for {table}"))?;

    Ok(TablePageSnapshot {
        table: table.to_owned(),
        row_count: parse_profile_column(row, 0, "row_count")?,
        in_row_used_page_count: parse_profile_column(row, 1, "in_row_used_page_count")?,
        lob_used_page_count: parse_profile_column(row, 2, "lob_used_page_count")?,
        row_overflow_used_page_count: parse_profile_column(row, 3, "row_overflow_used_page_count")?,
        used_page_count: parse_profile_column(row, 4, "used_page_count")?,
    })
}

fn session_wait_snapshots(
    connection: &Connection<'_>,
) -> Result<Vec<SessionWaitSnapshot>, Box<dyn Error>> {
    text_rows(
        connection,
        "SELECT \
            CONVERT(nvarchar(60), s.wait_type), \
            CONVERT(nvarchar(32), CONVERT(bigint, s.waiting_tasks_count)), \
            CONVERT(nvarchar(32), CONVERT(bigint, s.wait_time_ms)), \
            CONVERT(nvarchar(32), CONVERT(bigint, s.signal_wait_time_ms)) \
        FROM sys.dm_exec_session_wait_stats AS s \
        WHERE CONVERT(int, s.session_id) = @@SPID \
        ORDER BY s.wait_time_ms DESC, s.wait_type",
        4,
    )?
    .into_iter()
    .map(|row| {
        Ok(SessionWaitSnapshot {
            wait_type: required_profile_column(&row, 0, "wait_type")?.to_owned(),
            waiting_tasks_count: parse_profile_column(&row, 1, "waiting_tasks_count")?,
            wait_time_ms: parse_profile_column(&row, 2, "wait_time_ms")?,
            signal_wait_time_ms: parse_profile_column(&row, 3, "signal_wait_time_ms")?,
        })
    })
    .collect()
}

fn database_file_io_snapshots(
    connection: &Connection<'_>,
) -> Result<Vec<DatabaseFileIoSnapshot>, Box<dyn Error>> {
    text_rows(
        connection,
        "SELECT \
            CONVERT(nvarchar(16), CONVERT(int, f.file_id)), \
            CONVERT(nvarchar(128), f.name), \
            CONVERT(nvarchar(60), f.type_desc), \
            CONVERT(nvarchar(32), CONVERT(bigint, vfs.num_of_reads)), \
            CONVERT(nvarchar(32), CONVERT(bigint, vfs.num_of_bytes_read)), \
            CONVERT(nvarchar(32), CONVERT(bigint, vfs.io_stall_read_ms)), \
            CONVERT(nvarchar(32), CONVERT(bigint, vfs.num_of_writes)), \
            CONVERT(nvarchar(32), CONVERT(bigint, vfs.num_of_bytes_written)), \
            CONVERT(nvarchar(32), CONVERT(bigint, vfs.io_stall_write_ms)) \
        FROM sys.dm_io_virtual_file_stats(DB_ID(), NULL) AS vfs \
        INNER JOIN sys.database_files AS f \
            ON f.file_id = vfs.file_id \
        ORDER BY f.type, f.file_id",
        9,
    )?
    .into_iter()
    .map(|row| {
        Ok(DatabaseFileIoSnapshot {
            file_id: parse_profile_column(&row, 0, "file_id")?,
            logical_name: required_profile_column(&row, 1, "logical_name")?.to_owned(),
            file_type: required_profile_column(&row, 2, "file_type")?.to_owned(),
            read_count: parse_profile_column(&row, 3, "read_count")?,
            read_bytes: parse_profile_column(&row, 4, "read_bytes")?,
            read_stall_ms: parse_profile_column(&row, 5, "read_stall_ms")?,
            write_count: parse_profile_column(&row, 6, "write_count")?,
            write_bytes: parse_profile_column(&row, 7, "write_bytes")?,
            write_stall_ms: parse_profile_column(&row, 8, "write_stall_ms")?,
        })
    })
    .collect()
}

fn text_rows(
    connection: &Connection<'_>,
    sql: &str,
    columns: usize,
) -> Result<Vec<Vec<Option<String>>>, Box<dyn Error>> {
    let cursor = connection
        .execute(sql, (), None)?
        .ok_or("SQL Server profile query did not return a cursor")?;
    cursor_to_text_rows(cursor, columns)
}

fn cursor_to_text_rows(
    mut cursor: impl Cursor,
    columns: usize,
) -> Result<Vec<Vec<Option<String>>>, Box<dyn Error>> {
    let mut buffer = TextRowSet::for_cursor(128, &mut cursor, Some(3072))?;
    let mut row_set_cursor = cursor.bind_buffer(&mut buffer)?;
    let mut rows = Vec::new();

    while let Some(row_set) = row_set_cursor.fetch()? {
        for row_index in 0..row_set.num_rows() {
            let mut row = Vec::with_capacity(columns);
            for column_index in 0..columns {
                row.push(
                    row_set
                        .at_as_str(column_index, row_index)?
                        .map(ToOwned::to_owned),
                );
            }
            rows.push(row);
        }
    }

    Ok(rows)
}

fn required_profile_column<'a>(
    row: &'a [Option<String>],
    index: usize,
    column: &str,
) -> Result<&'a str, Box<dyn Error>> {
    row.get(index)
        .and_then(Option::as_deref)
        .ok_or_else(|| format!("SQL Server profile column `{column}` was null").into())
}

fn parse_profile_column<T>(
    row: &[Option<String>],
    index: usize,
    column: &str,
) -> Result<T, Box<dyn Error>>
where
    T: std::str::FromStr,
    T::Err: Error + 'static,
{
    Ok(required_profile_column(row, index, column)?.parse()?)
}

fn session_wait_deltas(
    initial: &[SessionWaitSnapshot],
    final_waits: &[SessionWaitSnapshot],
) -> Vec<SessionWaitDelta> {
    let initial_waits = initial
        .iter()
        .map(|wait| (wait.wait_type.as_str(), wait))
        .collect::<BTreeMap<_, _>>();
    let mut deltas = final_waits
        .iter()
        .filter_map(|final_wait| {
            let initial_wait = initial_waits.get(final_wait.wait_type.as_str()).copied();
            let delta = SessionWaitDelta {
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

fn database_file_io_deltas(
    initial: &[DatabaseFileIoSnapshot],
    final_files: &[DatabaseFileIoSnapshot],
) -> Vec<DatabaseFileIoDelta> {
    let initial_files = initial
        .iter()
        .map(|file| (file.file_id, file))
        .collect::<BTreeMap<_, _>>();
    let mut deltas = final_files
        .iter()
        .map(|final_file| {
            let initial_file = initial_files.get(&final_file.file_id).copied();
            DatabaseFileIoDelta {
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

fn counter_delta(initial: i64, final_value: i64) -> i64 {
    final_value.saturating_sub(initial)
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct BenchOptions {
    rows: usize,
    batch_size: usize,
    scenario: String,
    repeat: usize,
    input_ipc: std::path::PathBuf,
    create_table_sql_template: String,
    profile_sqlserver: bool,
    autocommit: bool,
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
            profile_sqlserver: false,
            autocommit: false,
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
                "--profile-sqlserver" => {
                    options.profile_sqlserver = true;
                }
                "--autocommit" => {
                    options.autocommit = true;
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
        if !options
            .create_table_sql_template
            .contains(TABLE_PLACEHOLDER)
        {
            return Err(
                format!("--create-table-sql-template must contain `{TABLE_PLACEHOLDER}`").into(),
            );
        }

        Ok(options)
    }

    fn create_table_sql(&self, table: &str) -> Result<String, Box<dyn Error>> {
        if table.contains(TABLE_PLACEHOLDER) {
            return Err("benchmark table name unexpectedly contains template placeholder".into());
        }

        Ok(self
            .create_table_sql_template
            .replace(TABLE_PLACEHOLDER, table))
    }

    fn transaction_policy(&self) -> &'static str {
        if self.autocommit {
            "ODBC autocommit"
        } else {
            "manual commit after repeats"
        }
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
        BenchOptions, DatabaseFileIoDelta, DatabaseFileIoSnapshot, SessionWaitDelta,
        SessionWaitSnapshot, TABLE_PLACEHOLDER, database_file_io_deltas, ipc_batches,
        rows_per_second, session_wait_deltas,
    };
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
        assert!(!options.profile_sqlserver);
        assert!(!options.autocommit);
        assert_eq!(
            options.input_ipc.as_path(),
            std::path::Path::new("/workspace/bench.arrow")
        );
        assert_eq!(
            options.create_table_sql_template,
            create_table_sql_template()
        );
    }

    #[test]
    fn parses_sql_server_profile_option() {
        let options = BenchOptions::parse(vec![
            "--input-ipc".to_owned(),
            "/workspace/bench.arrow".to_owned(),
            "--create-table-sql-template".to_owned(),
            create_table_sql_template(),
            "--profile-sqlserver".to_owned(),
        ])
        .expect("profile options should parse");

        assert!(options.profile_sqlserver);
    }

    #[test]
    fn parses_autocommit_policy() {
        let options = BenchOptions::parse(vec![
            "--input-ipc".to_owned(),
            "/workspace/bench.arrow".to_owned(),
            "--create-table-sql-template".to_owned(),
            create_table_sql_template(),
            "--autocommit".to_owned(),
        ])
        .expect("autocommit option should parse");

        assert!(options.autocommit);
        assert_eq!(options.transaction_policy(), "ODBC autocommit");
    }

    #[test]
    fn sql_server_session_wait_deltas_exclude_initial_totals() {
        let waits = session_wait_deltas(
            &[
                session_wait("ASYNC_NETWORK_IO", 3, 5, 1),
                session_wait("UNCHANGED", 8, 13, 2),
            ],
            &[
                session_wait("WRITELOG", 4, 23, 3),
                session_wait("ASYNC_NETWORK_IO", 5, 16, 4),
                session_wait("UNCHANGED", 8, 13, 2),
            ],
        );

        assert_eq!(
            waits,
            [
                SessionWaitDelta {
                    wait_type: "WRITELOG".to_owned(),
                    waiting_tasks_count: 4,
                    wait_time_ms: 23,
                    signal_wait_time_ms: 3,
                },
                SessionWaitDelta {
                    wait_type: "ASYNC_NETWORK_IO".to_owned(),
                    waiting_tasks_count: 2,
                    wait_time_ms: 11,
                    signal_wait_time_ms: 3,
                },
            ]
        );
    }

    fn session_wait(
        wait_type: &str,
        waiting_tasks_count: i64,
        wait_time_ms: i64,
        signal_wait_time_ms: i64,
    ) -> SessionWaitSnapshot {
        SessionWaitSnapshot {
            wait_type: wait_type.to_owned(),
            waiting_tasks_count,
            wait_time_ms,
            signal_wait_time_ms,
        }
    }

    #[test]
    fn sql_server_database_file_io_deltas_preserve_metadata() {
        let files = database_file_io_deltas(
            &[
                database_file_io(2, "bench_log", "LOG", 1, 8, 2, 3, 64, 5),
                database_file_io(1, "bench_data", "ROWS", 5, 40, 3, 7, 96, 11),
            ],
            &[
                database_file_io(2, "bench_log", "LOG", 1, 8, 2, 9, 192, 17),
                database_file_io(1, "bench_data", "ROWS", 8, 88, 7, 11, 160, 19),
            ],
        );

        assert_eq!(
            files,
            [
                DatabaseFileIoDelta {
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
                DatabaseFileIoDelta {
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
    fn database_file_io(
        file_id: i32,
        logical_name: &str,
        file_type: &str,
        read_count: i64,
        read_bytes: i64,
        read_stall_ms: i64,
        write_count: i64,
        write_bytes: i64,
        write_stall_ms: i64,
    ) -> DatabaseFileIoSnapshot {
        DatabaseFileIoSnapshot {
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

        assert_eq!(sql, "CREATE TABLE [dbo].[target] ([id32] int NOT NULL)");
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

use std::env;
use std::error::Error;
use std::ffi::{CString, c_char, c_void};
use std::fs::File;
use std::path::{Path, PathBuf};
use std::ptr::{null, null_mut};
use std::time::{Duration, Instant};

use arrow_array::{Array, Float64Array, Int32Array, Int64Array, RecordBatch};
use arrow_ipc::reader::FileReader;
use arrow_schema::DataType;
use libloading::Library;

const CONNECTION_STRING_ENV: &str = "ARROW_TIBERIUS_BENCH_ODBC_CONNECTION_STRING";
const DATABASE_ENV: &str = "ARROW_TIBERIUS_BENCH_DATABASE";
const ODBC_LIBRARY_ENV: &str = "ARROW_TIBERIUS_BENCH_ODBC_LIBRARY";
const BCP_LIBRARY_ENV: &str = "ARROW_TIBERIUS_BENCH_BCP_LIBRARY";
const DEFAULT_ODBC_LIBRARY: &str = "libodbc.so.2";
const DEFAULT_BCP_LIBRARY: &str = "/opt/microsoft/msodbcsql18/lib64/libmsodbcsql-18.6.so.2.1";
const TABLE_PLACEHOLDER: &str = "__ARROW_TIBERIUS_ODBC_TABLE__";

const SQL_SUCCESS: SqlReturn = 0;
const SQL_SUCCESS_WITH_INFO: SqlReturn = 1;
const SQL_NO_DATA: SqlReturn = 100;
const SQL_HANDLE_ENV: i16 = 1;
const SQL_HANDLE_DBC: i16 = 2;
const SQL_HANDLE_STMT: i16 = 3;
const SQL_ATTR_ODBC_VERSION: i32 = 200;
const SQL_OV_ODBC3: isize = 3;
const SQL_DRIVER_NOPROMPT: u16 = 0;
const SQL_NTS: i16 = -3;
const SQL_IS_INTEGER: i32 = -6;
const SQL_C_CHAR: i16 = 1;
const SQL_COPT_SS_BCP: i32 = 1219;
const SQL_BCP_ON: isize = 1;
const DB_IN: i32 = 1;
const SQLINT4: i32 = 0x38;
const SQLFLT8: i32 = 0x3e;
const SQLINT8: i32 = 0x7f;
const BCP_SUCCEED: i16 = 1;

type SqlReturn = i16;
type Handle = *mut c_void;
type HEnv = Handle;
type HDbc = Handle;
type HStmt = Handle;
type Pointer = *mut c_void;
type SqlLen = isize;
type DbInt = i32;

type SqlAllocHandle = unsafe extern "C" fn(i16, Handle, *mut Handle) -> SqlReturn;
type SqlDisconnect = unsafe extern "C" fn(HDbc) -> SqlReturn;
type SqlDriverConnect =
    unsafe extern "C" fn(HDbc, Pointer, *const u8, i16, *mut u8, i16, *mut i16, u16) -> SqlReturn;
type SqlExecDirect = unsafe extern "C" fn(HStmt, *const u8, i32) -> SqlReturn;
type SqlFetch = unsafe extern "C" fn(HStmt) -> SqlReturn;
type SqlFreeHandle = unsafe extern "C" fn(i16, Handle) -> SqlReturn;
type SqlGetData = unsafe extern "C" fn(HStmt, u16, i16, Pointer, SqlLen, *mut SqlLen) -> SqlReturn;
type SqlSetConnectAttr = unsafe extern "C" fn(HDbc, i32, Pointer, i32) -> SqlReturn;
type SqlSetEnvAttr = unsafe extern "C" fn(HEnv, i32, Pointer, i32) -> SqlReturn;

type BcpBind =
    unsafe extern "C" fn(HDbc, *const u8, i32, DbInt, *const u8, i32, i32, i32) -> i16;
type BcpBatch = unsafe extern "C" fn(HDbc) -> DbInt;
type BcpDone = unsafe extern "C" fn(HDbc) -> DbInt;
type BcpInitA =
    unsafe extern "C" fn(HDbc, *const c_char, *const c_char, *const c_char, i32) -> i16;
type BcpSendRow = unsafe extern "C" fn(HDbc) -> i16;

fn main() -> Result<(), Box<dyn Error>> {
    let command = env::args().nth(1);

    match command.as_deref() {
        Some("validate") => validate(),
        Some("bench") => bench(env::args().skip(2).collect()),
        Some(command) => Err(format!("unknown odbc-bcp runner command `{command}`").into()),
        None => Err("missing odbc-bcp runner command".into()),
    }
}

fn validate() -> Result<(), Box<dyn Error>> {
    let connection_string = required_env(CONNECTION_STRING_ENV)?;
    let database = required_env(DATABASE_ENV)?;
    let apis = NativeApis::load_from_env()?;
    let _connection = RawBcpConnection::connect(&apis.odbc, &connection_string)?;

    println!("odbc-bcp runner validated database {database}");
    Ok(())
}

fn bench(args: Vec<String>) -> Result<(), Box<dyn Error>> {
    let options = BenchOptions::parse(args)?;
    if options.scenario != "narrow_numeric" {
        return Err(format!(
            "odbc-bcp runner currently supports only `narrow_numeric`, got `{}`",
            options.scenario
        )
        .into());
    }

    let connection_string = required_env(CONNECTION_STRING_ENV)?;
    let database = required_env(DATABASE_ENV)?;
    let apis = NativeApis::load_from_env()?;
    let input = narrow_numeric_ipc_rows(&options.input_ipc, options.rows)?;
    let connection = RawBcpConnection::connect(&apis.odbc, &connection_string)?;

    let write_start = Instant::now();
    let total_rows = run_repeats(&connection, &apis.bcp, &input, &options)?;
    let write_elapsed = write_start.elapsed();

    println!("odbc-bcp runner");
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
    connection: &RawBcpConnection<'_>,
    bcp: &BcpApi,
    input: &NarrowNumericInput,
    options: &BenchOptions,
) -> Result<u64, Box<dyn Error>> {
    let mut total_rows = 0_u64;

    for repeat in 0..options.repeat {
        let table = format!(
            "[dbo].[arrow_tiberius_odbc_bcp_bench_{}_{}]",
            std::process::id(),
            repeat
        );
        let repeat_result = run_repeat(connection, bcp, input, &table, options);
        let cleanup_result = execute_sql(connection, &format!("DROP TABLE IF EXISTS {table}"));

        if let Err(error) = cleanup_result {
            if repeat_result.is_err() {
                eprintln!("warning: failed to clean up odbc-bcp benchmark table {table}: {error}");
            } else {
                return Err(error);
            }
        }

        total_rows = total_rows.saturating_add(repeat_result?);
    }

    Ok(total_rows)
}

fn run_repeat(
    connection: &RawBcpConnection<'_>,
    bcp: &BcpApi,
    input: &NarrowNumericInput,
    table: &str,
    options: &BenchOptions,
) -> Result<u64, Box<dyn Error>> {
    execute_sql(connection, &format!("DROP TABLE IF EXISTS {table}"))?;
    execute_sql(connection, &options.create_table_sql(table)?)?;

    let rows_written = bcp.copy_narrow_numeric(connection, table, input, options.batch_size)?;

    let actual = select_count(connection, table)?;
    if actual != rows_written {
        return Err(format!(
            "odbc-bcp row-count validation failed: expected {rows_written}, got {actual}"
        )
        .into());
    }

    Ok(actual)
}

#[derive(Debug)]
struct NarrowNumericInput {
    rows: Vec<NarrowNumericRow>,
}

impl NarrowNumericInput {
    fn len(&self) -> usize {
        self.rows.len()
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
struct NarrowNumericRow {
    id32: i32,
    id64: i64,
    score: f64,
}

fn narrow_numeric_ipc_rows(
    path: &Path,
    expected_rows: usize,
) -> Result<NarrowNumericInput, Box<dyn Error>> {
    let file = File::open(path)?;
    let reader = FileReader::try_new(file, None)?;
    let mut rows = Vec::with_capacity(expected_rows);

    for batch in reader {
        let batch = batch?;
        append_narrow_numeric_batch(&batch, &mut rows)?;
    }

    if rows.len() != expected_rows {
        return Err(format!(
            "Arrow IPC row count does not match --rows: expected {expected_rows}, got {}",
            rows.len()
        )
        .into());
    }

    Ok(NarrowNumericInput { rows })
}

fn append_narrow_numeric_batch(
    batch: &RecordBatch,
    rows: &mut Vec<NarrowNumericRow>,
) -> Result<(), Box<dyn Error>> {
    if batch.num_columns() != 3 {
        return Err(format!(
            "narrow_numeric expects 3 columns, got {}",
            batch.num_columns()
        )
        .into());
    }

    let id32 = required_primitive_column::<Int32Array>(batch, 0, "id32", &DataType::Int32)?;
    let id64 = required_primitive_column::<Int64Array>(batch, 1, "id64", &DataType::Int64)?;
    let score = required_primitive_column::<Float64Array>(batch, 2, "score", &DataType::Float64)?;

    for row_index in 0..batch.num_rows() {
        if id32.is_null(row_index) || id64.is_null(row_index) || score.is_null(row_index) {
            return Err(format!(
                "narrow_numeric does not support NULL at row {row_index}"
            )
            .into());
        }

        rows.push(NarrowNumericRow {
            id32: id32.value(row_index),
            id64: id64.value(row_index),
            score: score.value(row_index),
        });
    }

    Ok(())
}

fn required_primitive_column<'a, T: 'static>(
    batch: &'a RecordBatch,
    index: usize,
    name: &str,
    data_type: &DataType,
) -> Result<&'a T, Box<dyn Error>> {
    let schema = batch.schema();
    let field = schema.field(index);
    if field.name() != name {
        return Err(format!(
            "narrow_numeric column {index} expected `{name}`, got `{}`",
            field.name()
        )
        .into());
    }
    if field.data_type() != data_type {
        return Err(format!(
            "narrow_numeric column `{name}` expected {data_type:?}, got {:?}",
            field.data_type()
        )
        .into());
    }
    if field.is_nullable() {
        return Err(format!("narrow_numeric column `{name}` must be non-nullable").into());
    }

    batch
        .column(index)
        .as_any()
        .downcast_ref::<T>()
        .ok_or_else(|| format!("narrow_numeric column `{name}` has mismatched runtime array").into())
}

struct NativeApis {
    odbc: OdbcApi,
    bcp: BcpApi,
}

impl NativeApis {
    fn load_from_env() -> Result<Self, Box<dyn Error>> {
        Ok(Self {
            odbc: OdbcApi::load_from_env()?,
            bcp: BcpApi::load_from_env()?,
        })
    }
}

struct OdbcApi {
    _library: Library,
    sql_alloc_handle: SqlAllocHandle,
    sql_disconnect: SqlDisconnect,
    sql_driver_connect: SqlDriverConnect,
    sql_exec_direct: SqlExecDirect,
    sql_fetch: SqlFetch,
    sql_free_handle: SqlFreeHandle,
    sql_get_data: SqlGetData,
    sql_set_connect_attr: SqlSetConnectAttr,
    sql_set_env_attr: SqlSetEnvAttr,
}

impl OdbcApi {
    fn load_from_env() -> Result<Self, Box<dyn Error>> {
        let library_path =
            env::var(ODBC_LIBRARY_ENV).unwrap_or_else(|_| DEFAULT_ODBC_LIBRARY.to_owned());
        Self::load(&library_path)
    }

    fn load(path: impl AsRef<Path>) -> Result<Self, Box<dyn Error>> {
        let library = unsafe { Library::new(path.as_ref())? };
        let sql_alloc_handle =
            unsafe { *library.get::<SqlAllocHandle>(b"SQLAllocHandle\0")? };
        let sql_disconnect = unsafe { *library.get::<SqlDisconnect>(b"SQLDisconnect\0")? };
        let sql_driver_connect =
            unsafe { *library.get::<SqlDriverConnect>(b"SQLDriverConnect\0")? };
        let sql_exec_direct = unsafe { *library.get::<SqlExecDirect>(b"SQLExecDirect\0")? };
        let sql_fetch = unsafe { *library.get::<SqlFetch>(b"SQLFetch\0")? };
        let sql_free_handle = unsafe { *library.get::<SqlFreeHandle>(b"SQLFreeHandle\0")? };
        let sql_get_data = unsafe { *library.get::<SqlGetData>(b"SQLGetData\0")? };
        let sql_set_connect_attr =
            unsafe { *library.get::<SqlSetConnectAttr>(b"SQLSetConnectAttr\0")? };
        let sql_set_env_attr = unsafe { *library.get::<SqlSetEnvAttr>(b"SQLSetEnvAttr\0")? };

        Ok(Self {
            _library: library,
            sql_alloc_handle,
            sql_disconnect,
            sql_driver_connect,
            sql_exec_direct,
            sql_fetch,
            sql_free_handle,
            sql_get_data,
            sql_set_connect_attr,
            sql_set_env_attr,
        })
    }
}

struct BcpApi {
    _library: Library,
    bcp_batch: BcpBatch,
    bcp_bind: BcpBind,
    bcp_done: BcpDone,
    bcp_init_a: BcpInitA,
    bcp_sendrow: BcpSendRow,
}

impl BcpApi {
    fn load_from_env() -> Result<Self, Box<dyn Error>> {
        let library_path =
            env::var(BCP_LIBRARY_ENV).unwrap_or_else(|_| DEFAULT_BCP_LIBRARY.to_owned());
        Self::load(&library_path)
    }

    fn load(path: impl AsRef<Path>) -> Result<Self, Box<dyn Error>> {
        let library = unsafe { Library::new(path.as_ref())? };
        let bcp_batch = unsafe { *library.get::<BcpBatch>(b"bcp_batch\0")? };
        let bcp_bind = unsafe { *library.get::<BcpBind>(b"bcp_bind\0")? };
        let bcp_done = unsafe { *library.get::<BcpDone>(b"bcp_done\0")? };
        let bcp_init_a = unsafe { *library.get::<BcpInitA>(b"bcp_initA\0")? };
        let bcp_sendrow = unsafe { *library.get::<BcpSendRow>(b"bcp_sendrow\0")? };

        Ok(Self {
            _library: library,
            bcp_batch,
            bcp_bind,
            bcp_done,
            bcp_init_a,
            bcp_sendrow,
        })
    }

    fn copy_narrow_numeric(
        &self,
        connection: &RawBcpConnection,
        table: &str,
        input: &NarrowNumericInput,
        batch_size: usize,
    ) -> Result<u64, Box<dyn Error>> {
        let table = c_string("table", table)?;
        let init_result =
            unsafe { (self.bcp_init_a)(connection.hdbc, table.as_ptr(), null(), null(), DB_IN) };
        require_bcp_success("bcp_initA", init_result)?;

        let mut bound = BoundNarrowNumericRow::default();
        self.bind_narrow_numeric_columns(connection, &mut bound)?;

        let mut sent_since_batch = 0_usize;
        let mut rows_reported = 0_u64;
        for row in &input.rows {
            bound.set(*row);
            let send_result = unsafe { (self.bcp_sendrow)(connection.hdbc) };
            require_bcp_success("bcp_sendrow", send_result)?;
            sent_since_batch += 1;

            if sent_since_batch == batch_size {
                rows_reported = rows_reported.saturating_add(self.flush_batch(connection)?);
                sent_since_batch = 0;
            }
        }

        if sent_since_batch > 0 {
            rows_reported = rows_reported.saturating_add(self.flush_batch(connection)?);
        }

        let done_rows = unsafe { (self.bcp_done)(connection.hdbc) };
        if done_rows < 0 {
            return Err("bcp_done failed".into());
        }
        rows_reported = rows_reported.saturating_add(u64::try_from(done_rows)?);

        let expected = u64::try_from(input.len())?;
        if rows_reported != expected {
            return Err(format!(
                "BCP reported {rows_reported} rows across bcp_batch and bcp_done, expected {expected}"
            )
            .into());
        }

        Ok(rows_reported)
    }

    fn bind_narrow_numeric_columns(
        &self,
        connection: &RawBcpConnection,
        row: &mut BoundNarrowNumericRow,
    ) -> Result<(), Box<dyn Error>> {
        self.bind_fixed(
            connection,
            "id32",
            row.id32_ptr(),
            std::mem::size_of::<i32>(),
            SQLINT4,
            1,
        )?;
        self.bind_fixed(
            connection,
            "id64",
            row.id64_ptr(),
            std::mem::size_of::<i64>(),
            SQLINT8,
            2,
        )?;
        self.bind_fixed(
            connection,
            "score",
            row.score_ptr(),
            std::mem::size_of::<f64>(),
            SQLFLT8,
            3,
        )
    }

    fn bind_fixed(
        &self,
        connection: &RawBcpConnection,
        column: &str,
        value_ptr: *const u8,
        value_len: usize,
        server_type: i32,
        server_column: i32,
    ) -> Result<(), Box<dyn Error>> {
        let value_len = DbInt::try_from(value_len)?;
        let result = unsafe {
            (self.bcp_bind)(
                connection.hdbc,
                value_ptr,
                0,
                value_len,
                null(),
                0,
                server_type,
                server_column,
            )
        };
        require_bcp_success(&format!("bcp_bind {column}"), result)
    }

    fn flush_batch(&self, connection: &RawBcpConnection) -> Result<u64, Box<dyn Error>> {
        let rows = unsafe { (self.bcp_batch)(connection.hdbc) };
        if rows < 0 {
            Err("bcp_batch failed".into())
        } else {
            Ok(u64::try_from(rows)?)
        }
    }
}

#[derive(Debug, Default)]
struct BoundNarrowNumericRow {
    id32: i32,
    id64: i64,
    score: f64,
}

impl BoundNarrowNumericRow {
    fn set(&mut self, row: NarrowNumericRow) {
        self.id32 = row.id32;
        self.id64 = row.id64;
        self.score = row.score;
    }

    fn id32_ptr(&self) -> *const u8 {
        std::ptr::from_ref(&self.id32).cast::<u8>()
    }

    fn id64_ptr(&self) -> *const u8 {
        std::ptr::from_ref(&self.id64).cast::<u8>()
    }

    fn score_ptr(&self) -> *const u8 {
        std::ptr::from_ref(&self.score).cast::<u8>()
    }
}

struct RawBcpConnection<'a> {
    odbc: &'a OdbcApi,
    henv: HEnv,
    hdbc: HDbc,
    connected: bool,
}

impl<'a> RawBcpConnection<'a> {
    fn connect(odbc: &'a OdbcApi, connection_string: &str) -> Result<Self, Box<dyn Error>> {
        let mut connection = Self::allocate(odbc)?;
        connection.enable_bcp()?;
        connection.driver_connect(connection_string)?;
        Ok(connection)
    }

    fn allocate(odbc: &'a OdbcApi) -> Result<Self, Box<dyn Error>> {
        let mut henv = null_mut();
        let env_result =
            unsafe { (odbc.sql_alloc_handle)(SQL_HANDLE_ENV, null_mut(), &mut henv) };
        require_odbc_success("SQLAllocHandle ENV", env_result)?;

        let version_result = unsafe {
            (odbc.sql_set_env_attr)(
                henv,
                SQL_ATTR_ODBC_VERSION,
                SQL_OV_ODBC3 as Pointer,
                0,
            )
        };
        if let Err(error) = require_odbc_success("SQLSetEnvAttr ODBC version", version_result) {
            unsafe {
                let _ = (odbc.sql_free_handle)(SQL_HANDLE_ENV, henv);
            }
            return Err(error);
        }

        let mut hdbc = null_mut();
        let dbc_result = unsafe { (odbc.sql_alloc_handle)(SQL_HANDLE_DBC, henv, &mut hdbc) };
        if let Err(error) = require_odbc_success("SQLAllocHandle DBC", dbc_result) {
            unsafe {
                let _ = (odbc.sql_free_handle)(SQL_HANDLE_ENV, henv);
            }
            return Err(error);
        }

        Ok(Self {
            odbc,
            henv,
            hdbc,
            connected: false,
        })
    }

    fn enable_bcp(&mut self) -> Result<(), Box<dyn Error>> {
        let result = unsafe {
            (self.odbc.sql_set_connect_attr)(
                self.hdbc,
                SQL_COPT_SS_BCP,
                SQL_BCP_ON as Pointer,
                SQL_IS_INTEGER,
            )
        };
        require_odbc_success("SQLSetConnectAttr SQL_COPT_SS_BCP", result)
    }

    fn driver_connect(&mut self, connection_string: &str) -> Result<(), Box<dyn Error>> {
        let connection_string = c_string("connection string", connection_string)?;
        let result = unsafe {
            (self.odbc.sql_driver_connect)(
                self.hdbc,
                null_mut(),
                connection_string.as_ptr().cast::<u8>(),
                SQL_NTS,
                null_mut(),
                0,
                null_mut(),
                SQL_DRIVER_NOPROMPT,
            )
        };
        require_odbc_success("SQLDriverConnect", result)?;
        self.connected = true;
        Ok(())
    }

    fn allocate_statement(&self) -> Result<RawStatement<'_>, Box<dyn Error>> {
        let mut hstmt = null_mut();
        let result =
            unsafe { (self.odbc.sql_alloc_handle)(SQL_HANDLE_STMT, self.hdbc, &mut hstmt) };
        require_odbc_success("SQLAllocHandle STMT", result)?;

        Ok(RawStatement {
            odbc: self.odbc,
            hstmt,
        })
    }
}

impl Drop for RawBcpConnection<'_> {
    fn drop(&mut self) {
        unsafe {
            if self.connected {
                let _ = (self.odbc.sql_disconnect)(self.hdbc);
            }
            let _ = (self.odbc.sql_free_handle)(SQL_HANDLE_DBC, self.hdbc);
            let _ = (self.odbc.sql_free_handle)(SQL_HANDLE_ENV, self.henv);
        }
    }
}

struct RawStatement<'a> {
    odbc: &'a OdbcApi,
    hstmt: HStmt,
}

impl Drop for RawStatement<'_> {
    fn drop(&mut self) {
        unsafe {
            let _ = (self.odbc.sql_free_handle)(SQL_HANDLE_STMT, self.hstmt);
        }
    }
}

fn require_bcp_success(operation: &str, code: i16) -> Result<(), Box<dyn Error>> {
    if code == BCP_SUCCEED {
        Ok(())
    } else {
        Err(format!("{operation} failed with return code {code}").into())
    }
}

fn require_odbc_success(operation: &str, code: SqlReturn) -> Result<(), Box<dyn Error>> {
    if code == SQL_SUCCESS || code == SQL_SUCCESS_WITH_INFO {
        Ok(())
    } else {
        Err(format!("{operation} failed with return code {code:?}").into())
    }
}

fn execute_sql(connection: &RawBcpConnection<'_>, sql: &str) -> Result<(), Box<dyn Error>> {
    let sql = c_string("SQL statement", sql)?;
    let statement = connection.allocate_statement()?;
    let result = unsafe {
        (connection.odbc.sql_exec_direct)(statement.hstmt, sql.as_ptr().cast::<u8>(), SQL_NTS.into())
    };
    require_odbc_success("SQLExecDirect", result)
}

fn select_count(connection: &RawBcpConnection<'_>, table: &str) -> Result<u64, Box<dyn Error>> {
    let sql = c_string("count SQL statement", &format!("SELECT COUNT_BIG(*) FROM {table}"))?;
    let statement = connection.allocate_statement()?;
    let exec_result = unsafe {
        (connection.odbc.sql_exec_direct)(statement.hstmt, sql.as_ptr().cast::<u8>(), SQL_NTS.into())
    };
    require_odbc_success("SQLExecDirect count", exec_result)?;

    let fetch_result = unsafe { (connection.odbc.sql_fetch)(statement.hstmt) };
    require_odbc_success("SQLFetch count", fetch_result)?;

    let mut buffer = [0_u8; 64];
    let mut indicator = 0;
    let get_result = unsafe {
        (connection.odbc.sql_get_data)(
            statement.hstmt,
            1,
            SQL_C_CHAR,
            buffer.as_mut_ptr().cast::<c_void>(),
            SqlLen::try_from(buffer.len())?,
            &mut indicator,
        )
    };
    require_odbc_success("SQLGetData count", get_result)?;

    if indicator < 0 {
        return Err("COUNT_BIG returned NULL".into());
    }
    let nul_position = buffer
        .iter()
        .position(|byte| *byte == 0)
        .unwrap_or(buffer.len());
    let text = std::str::from_utf8(&buffer[..nul_position])?;

    let no_more_rows = unsafe { (connection.odbc.sql_fetch)(statement.hstmt) };
    if no_more_rows != SQL_NO_DATA {
        return Err("COUNT_BIG returned more than one row".into());
    }

    Ok(text.trim().parse::<u64>()?)
}

fn rows_per_second(rows: u64, elapsed: Duration) -> f64 {
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

fn c_string(label: &str, value: &str) -> Result<CString, Box<dyn Error>> {
    CString::new(value).map_err(|_| format!("{label} contains an interior NUL byte").into())
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct BenchOptions {
    rows: usize,
    batch_size: usize,
    scenario: String,
    repeat: usize,
    input_ipc: PathBuf,
    create_table_sql_template: String,
}

impl BenchOptions {
    fn parse(args: Vec<String>) -> Result<Self, Box<dyn Error>> {
        let mut options = Self {
            rows: 100_000,
            batch_size: 8_192,
            scenario: "narrow_numeric".to_owned(),
            repeat: 1,
            input_ipc: PathBuf::new(),
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
                    input_ipc = Some(PathBuf::from(required_arg("--input-ipc", args.get(index))?));
                }
                "--create-table-sql-template" => {
                    index += 1;
                    create_table_sql_template = Some(
                        required_arg("--create-table-sql-template", args.get(index))?.to_owned(),
                    );
                }
                other => return Err(format!("unknown odbc-bcp runner option `{other}`").into()),
            }

            index += 1;
        }

        options.input_ipc =
            input_ipc.ok_or("missing required odbc-bcp runner option `--input-ipc <FILE>`")?;
        options.create_table_sql_template = create_table_sql_template
            .ok_or("missing required odbc-bcp runner option `--create-table-sql-template <SQL>`")?;
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
    use super::{
        BenchOptions, NarrowNumericRow, TABLE_PLACEHOLDER, append_narrow_numeric_batch, c_string,
        narrow_numeric_ipc_rows, rows_per_second,
    };
    use arrow_array::{ArrayRef, Float64Array, Int32Array, Int64Array, RecordBatch, StringArray};
    use arrow_ipc::writer::FileWriter;
    use arrow_schema::{DataType, Field, Schema};
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
    fn rejects_missing_input_ipc() {
        let err = BenchOptions::parse(vec![
            "--rows".to_owned(),
            "25".to_owned(),
            "--create-table-sql-template".to_owned(),
            create_table_sql_template(),
        ])
        .expect_err("input IPC should be required");

        assert!(err.to_string().contains("--input-ipc"));
    }

    #[test]
    fn rejects_template_without_placeholder() {
        let err = BenchOptions::parse(vec![
            "--input-ipc".to_owned(),
            "/workspace/bench.arrow".to_owned(),
            "--create-table-sql-template".to_owned(),
            "CREATE TABLE [dbo].[fixed] ([id32] int NOT NULL);".to_owned(),
        ])
        .expect_err("placeholder should be required");

        assert!(err.to_string().contains(TABLE_PLACEHOLDER));
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
            "CREATE TABLE [dbo].[target] ([id32] int NOT NULL);"
        );
    }

    #[test]
    fn extracts_narrow_numeric_rows_from_ipc() {
        let path = temp_test_file("odbc-bcp-narrow");
        write_narrow_numeric_ipc(
            &path,
            vec![
                NarrowNumericRow {
                    id32: 1,
                    id64: 11,
                    score: 1.5,
                },
                NarrowNumericRow {
                    id32: -2,
                    id64: 22,
                    score: -2.25,
                },
            ],
        );

        let input = narrow_numeric_ipc_rows(&path, 2).expect("IPC should parse");

        assert_eq!(
            input.rows,
            vec![
                NarrowNumericRow {
                    id32: 1,
                    id64: 11,
                    score: 1.5
                },
                NarrowNumericRow {
                    id32: -2,
                    id64: 22,
                    score: -2.25
                }
            ]
        );

        std::fs::remove_file(path).expect("test IPC cleanup should succeed");
    }

    #[test]
    fn rejects_ipc_row_count_mismatch() {
        let path = temp_test_file("odbc-bcp-row-count");
        write_narrow_numeric_ipc(
            &path,
            vec![NarrowNumericRow {
                id32: 1,
                id64: 11,
                score: 1.5,
            }],
        );

        let err = narrow_numeric_ipc_rows(&path, 2).expect_err("row count should be checked");

        assert!(err.to_string().contains("row count"));
        std::fs::remove_file(path).expect("test IPC cleanup should succeed");
    }

    #[test]
    fn rejects_narrow_numeric_schema_drift() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id32", DataType::Int32, false),
            Field::new("id64", DataType::Int64, false),
            Field::new("score", DataType::Utf8, false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int32Array::from(vec![1])) as ArrayRef,
                Arc::new(Int64Array::from(vec![2])) as ArrayRef,
                Arc::new(StringArray::from(vec!["not a float"])) as ArrayRef,
            ],
        )
        .expect("batch should build");
        let mut rows = Vec::new();

        let err = append_narrow_numeric_batch(&batch, &mut rows)
            .expect_err("schema drift should be rejected");

        assert!(err.to_string().contains("score"));
        assert!(rows.is_empty());
    }

    #[test]
    fn rejects_nullable_narrow_numeric_column() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id32", DataType::Int32, true),
            Field::new("id64", DataType::Int64, false),
            Field::new("score", DataType::Float64, false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int32Array::from(vec![Some(1)])) as ArrayRef,
                Arc::new(Int64Array::from(vec![2])) as ArrayRef,
                Arc::new(Float64Array::from(vec![3.0])) as ArrayRef,
            ],
        )
        .expect("batch should build");
        let mut rows = Vec::new();

        let err = append_narrow_numeric_batch(&batch, &mut rows)
            .expect_err("nullable schema should be rejected");

        assert!(err.to_string().contains("non-nullable"));
        assert!(rows.is_empty());
    }

    #[test]
    fn rejects_interior_nul_in_c_string_inputs() {
        let err = c_string("table", "abc\0def").expect_err("interior NUL should be rejected");

        assert!(err.to_string().contains("NUL"));
    }

    #[test]
    fn formats_zero_elapsed_rows_per_second_without_panicking() {
        assert_eq!(rows_per_second(25, std::time::Duration::ZERO), 0.0);
    }

    fn create_table_sql_template() -> String {
        format!("CREATE TABLE {TABLE_PLACEHOLDER} ([id32] int NOT NULL);")
    }

    fn write_narrow_numeric_ipc(path: impl AsRef<std::path::Path>, rows: Vec<NarrowNumericRow>) {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id32", DataType::Int32, false),
            Field::new("id64", DataType::Int64, false),
            Field::new("score", DataType::Float64, false),
        ]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int32Array::from(
                    rows.iter().map(|row| row.id32).collect::<Vec<_>>(),
                )) as ArrayRef,
                Arc::new(Int64Array::from(
                    rows.iter().map(|row| row.id64).collect::<Vec<_>>(),
                )) as ArrayRef,
                Arc::new(Float64Array::from(
                    rows.iter().map(|row| row.score).collect::<Vec<_>>(),
                )) as ArrayRef,
            ],
        )
        .expect("test batch should build");

        let mut file = std::fs::File::create(path).expect("test IPC file should be created");
        let mut writer =
            FileWriter::try_new(&mut file, &schema).expect("test IPC writer should be created");
        writer.write(&batch).expect("test batch should be written");
        writer.finish().expect("test IPC writer should finish");
    }

    fn temp_test_file(label: &str) -> PathBuf {
        let counter = TEST_FILE_COUNTER.fetch_add(1, Ordering::Relaxed);
        std::env::temp_dir().join(format!("{label}-{}-{counter}.arrow", std::process::id()))
    }
}

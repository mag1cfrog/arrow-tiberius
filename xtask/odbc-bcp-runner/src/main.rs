use std::env;
use std::error::Error;
use std::ffi::{CString, c_char, c_void};
use std::fs::File;
use std::path::{Path, PathBuf};
use std::ptr::{null, null_mut};
use std::time::{Duration, Instant};

use arrow_array::{
    Array, BinaryArray, BooleanArray, Date32Array, Decimal128Array, Float64Array, Int32Array,
    Int64Array, RecordBatch, StringArray, TimestampMillisecondArray,
};
use arrow_ipc::reader::FileReader;
use arrow_schema::DataType;
use chrono::{DateTime, Datelike, NaiveDate, Timelike, Utc};
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
const SQL_NULL_DATA: DbInt = -1;
const SQLVARBINARY: i32 = 0x25;
const SQLVARCHAR: i32 = 0x27;
const SQLBIT: i32 = 0x32;
const SQLINT4: i32 = 0x38;
const SQLFLT8: i32 = 0x3e;
const SQLINT8: i32 = 0x7f;
const SQLNVARCHAR: i32 = 0xe7;
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
type BcpCollen = unsafe extern "C" fn(HDbc, DbInt, i32) -> i16;
type BcpColptr = unsafe extern "C" fn(HDbc, *const u8, i32) -> i16;
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
    let connection_string = required_env(CONNECTION_STRING_ENV)?;
    let database = required_env(DATABASE_ENV)?;
    let apis = NativeApis::load_from_env()?;
    let connection = RawBcpConnection::connect(&apis.odbc, &connection_string)?;

    let write_start = Instant::now();
    let total_rows = run_repeats(&connection, &apis.bcp, &options)?;
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
    if let Some(peak_rss_kib) = current_process_peak_rss_kib() {
        println!("  peak rss KiB: {peak_rss_kib}");
    }

    Ok(())
}

fn run_repeats(
    connection: &RawBcpConnection<'_>,
    bcp: &BcpApi,
    options: &BenchOptions,
) -> Result<u64, Box<dyn Error>> {
    let mut total_rows = 0_u64;

    for repeat in 0..options.repeat {
        let table = format!(
            "[dbo].[arrow_tiberius_odbc_bcp_bench_{}_{}]",
            std::process::id(),
            repeat
        );
        let repeat_result = run_repeat(connection, bcp, &table, options);
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
    table: &str,
    options: &BenchOptions,
) -> Result<u64, Box<dyn Error>> {
    execute_sql(connection, &format!("DROP TABLE IF EXISTS {table}"))?;
    execute_sql(connection, &options.create_table_sql(table)?)?;

    let rows_written = bcp.copy_ipc_dataset(connection, table, options)?;

    let actual = select_count(connection, table)?;
    if actual != rows_written {
        return Err(format!(
            "odbc-bcp row-count validation failed: expected {rows_written}, got {actual}"
        )
        .into());
    }

    Ok(actual)
}

#[cfg(test)]
fn validate_ipc_schema_and_count(
    path: &Path,
    expected_rows: usize,
) -> Result<usize, Box<dyn Error>> {
    let file = File::open(path)?;
    let reader = FileReader::try_new(file, None)?;
    let mut rows = 0_usize;

    for batch in reader {
        let batch = batch?;
        let _ = BcpColumnBindings::new(&batch)?;
        rows = rows.saturating_add(batch.num_rows());
    }

    if rows != expected_rows {
        return Err(format!(
            "Arrow IPC row count does not match --rows: expected {expected_rows}, got {}",
            rows
        )
        .into());
    }

    Ok(rows)
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
    bcp_collen: BcpCollen,
    bcp_colptr: BcpColptr,
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
        let bcp_collen = unsafe { *library.get::<BcpCollen>(b"bcp_collen\0")? };
        let bcp_colptr = unsafe { *library.get::<BcpColptr>(b"bcp_colptr\0")? };
        let bcp_done = unsafe { *library.get::<BcpDone>(b"bcp_done\0")? };
        let bcp_init_a = unsafe { *library.get::<BcpInitA>(b"bcp_initA\0")? };
        let bcp_sendrow = unsafe { *library.get::<BcpSendRow>(b"bcp_sendrow\0")? };

        Ok(Self {
            _library: library,
            bcp_batch,
            bcp_bind,
            bcp_collen,
            bcp_colptr,
            bcp_done,
            bcp_init_a,
            bcp_sendrow,
        })
    }

    fn copy_ipc_dataset(
        &self,
        connection: &RawBcpConnection,
        table: &str,
        options: &BenchOptions,
    ) -> Result<u64, Box<dyn Error>> {
        let table = c_string("table", table)?;
        let init_result =
            unsafe { (self.bcp_init_a)(connection.hdbc, table.as_ptr(), null(), null(), DB_IN) };
        require_bcp_success("bcp_initA", init_result)?;

        let file = File::open(&options.input_ipc)?;
        let reader = FileReader::try_new(file, None)?;
        let mut rows_seen = 0_usize;
        let mut sent_since_batch = 0_usize;
        let mut rows_reported = 0_u64;
        let mut bindings: Option<BcpColumnBindings> = None;

        for batch in reader {
            let batch = batch?;
            if bindings.is_none() {
                let mut next_bindings = BcpColumnBindings::new(&batch)?;
                next_bindings.bind(connection, self)?;
                bindings = Some(next_bindings);
            }
            let bindings = bindings
                .as_mut()
                .ok_or("BCP column bindings were not initialized")?;
            bindings.validate_batch(&batch)?;

            for row_index in 0..batch.num_rows() {
                bindings.set_row(connection, self, &batch, row_index)?;
                let send_result = unsafe { (self.bcp_sendrow)(connection.hdbc) };
                require_bcp_success("bcp_sendrow", send_result)?;
                sent_since_batch += 1;
                rows_seen += 1;

                if sent_since_batch == options.batch_size {
                    rows_reported = rows_reported.saturating_add(self.flush_batch(connection)?);
                    sent_since_batch = 0;
                }
            }
        }

        if bindings.is_none() {
            return Err("Arrow IPC file did not contain any record batches".into());
        }

        if sent_since_batch > 0 {
            rows_reported = rows_reported.saturating_add(self.flush_batch(connection)?);
        }

        let done_rows = unsafe { (self.bcp_done)(connection.hdbc) };
        if done_rows < 0 {
            return Err("bcp_done failed".into());
        }
        rows_reported = rows_reported.saturating_add(u64::try_from(done_rows)?);

        if rows_seen != options.rows {
            return Err(format!(
                "Arrow IPC row count does not match --rows: expected {}, got {rows_seen}",
                options.rows
            )
            .into());
        }

        let expected = u64::try_from(options.rows)?;
        if rows_reported != expected {
            return Err(format!(
                "BCP reported {rows_reported} rows across bcp_batch and bcp_done, expected {expected}"
            )
            .into());
        }

        Ok(rows_reported)
    }

    fn bind_column(
        &self,
        connection: &RawBcpConnection,
        column: &str,
        value_ptr: *const u8,
        value_len: DbInt,
        server_type: i32,
        server_column: i32,
    ) -> Result<(), Box<dyn Error>> {
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

    fn set_column_len(
        &self,
        connection: &RawBcpConnection,
        column: &str,
        value_len: DbInt,
        server_column: i32,
    ) -> Result<(), Box<dyn Error>> {
        let result = unsafe { (self.bcp_collen)(connection.hdbc, value_len, server_column) };
        require_bcp_success(&format!("bcp_collen {column}"), result)
    }

    fn set_column_ptr(
        &self,
        connection: &RawBcpConnection,
        column: &str,
        value_ptr: *const u8,
        server_column: i32,
    ) -> Result<(), Box<dyn Error>> {
        let result = unsafe { (self.bcp_colptr)(connection.hdbc, value_ptr, server_column) };
        require_bcp_success(&format!("bcp_colptr {column}"), result)
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

#[derive(Debug)]
struct BcpColumnBindings {
    columns: Vec<BcpColumnBinding>,
}

impl BcpColumnBindings {
    fn new(batch: &RecordBatch) -> Result<Self, Box<dyn Error>> {
        let schema = batch.schema();
        let columns = schema
            .fields()
            .iter()
            .enumerate()
            .map(|(index, field)| BcpColumnBinding::new(index, field.name(), field.data_type(), field.is_nullable()))
            .collect::<Result<Vec<_>, _>>()?;

        Ok(Self { columns })
    }

    fn bind(&mut self, connection: &RawBcpConnection, bcp: &BcpApi) -> Result<(), Box<dyn Error>> {
        for column in &mut self.columns {
            column.bind(connection, bcp)?;
        }

        Ok(())
    }

    fn validate_batch(&self, batch: &RecordBatch) -> Result<(), Box<dyn Error>> {
        if batch.num_columns() != self.columns.len() {
            return Err(format!(
                "Arrow IPC batch column count changed: expected {}, got {}",
                self.columns.len(),
                batch.num_columns()
            )
            .into());
        }

        for column in &self.columns {
            column.validate_batch(batch)?;
        }

        Ok(())
    }

    fn set_row(
        &mut self,
        connection: &RawBcpConnection,
        bcp: &BcpApi,
        batch: &RecordBatch,
        row_index: usize,
    ) -> Result<(), Box<dyn Error>> {
        for column in &mut self.columns {
            column.set_row(connection, bcp, batch, row_index)?;
        }

        Ok(())
    }
}

#[derive(Debug)]
struct BcpColumnBinding {
    index: usize,
    server_column: i32,
    name: String,
    data_type: DataType,
    nullable: bool,
    buffer: BcpColumnBuffer,
}

impl BcpColumnBinding {
    fn new(
        index: usize,
        name: &str,
        data_type: &DataType,
        nullable: bool,
    ) -> Result<Self, Box<dyn Error>> {
        let server_column = i32::try_from(index + 1)?;
        let buffer = BcpColumnBuffer::new(data_type)?;

        Ok(Self {
            index,
            server_column,
            name: name.to_owned(),
            data_type: data_type.clone(),
            nullable,
            buffer,
        })
    }

    fn bind(&mut self, connection: &RawBcpConnection, bcp: &BcpApi) -> Result<(), Box<dyn Error>> {
        bcp.bind_column(
            connection,
            &self.name,
            self.buffer.ptr(),
            self.buffer.bind_len()?,
            self.buffer.server_type(),
            self.server_column,
        )
    }

    fn validate_batch(&self, batch: &RecordBatch) -> Result<(), Box<dyn Error>> {
        let schema = batch.schema();
        let field = schema.field(self.index);

        if field.name() != &self.name {
            return Err(format!(
                "Arrow IPC column {} expected `{}`, got `{}`",
                self.index,
                self.name,
                field.name()
            )
            .into());
        }
        if field.data_type() != &self.data_type {
            return Err(format!(
                "Arrow IPC column `{}` expected {:?}, got {:?}",
                self.name,
                self.data_type,
                field.data_type()
            )
            .into());
        }
        if field.is_nullable() != self.nullable {
            return Err(format!(
                "Arrow IPC column `{}` nullable flag changed: expected {}, got {}",
                self.name,
                self.nullable,
                field.is_nullable()
            )
            .into());
        }

        Ok(())
    }

    fn set_row(
        &mut self,
        connection: &RawBcpConnection,
        bcp: &BcpApi,
        batch: &RecordBatch,
        row_index: usize,
    ) -> Result<(), Box<dyn Error>> {
        if batch.column(self.index).is_null(row_index) {
            if !self.nullable {
                return Err(format!(
                    "Arrow IPC column `{}` is non-nullable but row {row_index} is NULL",
                    self.name
                )
                .into());
            }
            return bcp.set_column_len(connection, &self.name, SQL_NULL_DATA, self.server_column);
        }

        self.buffer.set_from_batch(batch, self.index, row_index, &self.name)?;

        if self.buffer.is_variable_len() {
            bcp.set_column_ptr(connection, &self.name, self.buffer.ptr(), self.server_column)?;
        }

        bcp.set_column_len(
            connection,
            &self.name,
            self.buffer.current_len()?,
            self.server_column,
        )
    }
}

#[derive(Debug)]
enum BcpColumnBuffer {
    I32(i32),
    I64(i64),
    F64(f64),
    Bit(u8),
    Text(Vec<u8>),
    WideText(Vec<u8>),
    Binary(Vec<u8>),
}

impl BcpColumnBuffer {
    fn new(data_type: &DataType) -> Result<Self, Box<dyn Error>> {
        match data_type {
            DataType::Int32 => Ok(Self::I32(0)),
            DataType::Int64 => Ok(Self::I64(0)),
            DataType::Float64 => Ok(Self::F64(0.0)),
            DataType::Boolean => Ok(Self::Bit(0)),
            DataType::Utf8 => Ok(Self::WideText(vec![0])),
            DataType::Decimal128(_, _) | DataType::Date32 | DataType::Timestamp(_, _) => {
                Ok(Self::Text(vec![0]))
            }
            DataType::Binary => Ok(Self::Binary(vec![0])),
            other => Err(format!("odbc-bcp runner does not support Arrow type {other:?}").into()),
        }
    }

    fn server_type(&self) -> i32 {
        match self {
            Self::I32(_) => SQLINT4,
            Self::I64(_) => SQLINT8,
            Self::F64(_) => SQLFLT8,
            Self::Bit(_) => SQLBIT,
            Self::Text(_) => SQLVARCHAR,
            Self::WideText(_) => SQLNVARCHAR,
            Self::Binary(_) => SQLVARBINARY,
        }
    }

    fn bind_len(&self) -> Result<DbInt, Box<dyn Error>> {
        match self {
            Self::I32(_) => Ok(DbInt::try_from(std::mem::size_of::<i32>())?),
            Self::I64(_) => Ok(DbInt::try_from(std::mem::size_of::<i64>())?),
            Self::F64(_) => Ok(DbInt::try_from(std::mem::size_of::<f64>())?),
            Self::Bit(_) => Ok(DbInt::try_from(std::mem::size_of::<u8>())?),
            Self::Text(bytes) | Self::WideText(bytes) | Self::Binary(bytes) => {
                Ok(DbInt::try_from(bytes.len())?)
            }
        }
    }

    fn current_len(&self) -> Result<DbInt, Box<dyn Error>> {
        match self {
            Self::I32(_) => Ok(DbInt::try_from(std::mem::size_of::<i32>())?),
            Self::I64(_) => Ok(DbInt::try_from(std::mem::size_of::<i64>())?),
            Self::F64(_) => Ok(DbInt::try_from(std::mem::size_of::<f64>())?),
            Self::Bit(_) => Ok(DbInt::try_from(std::mem::size_of::<u8>())?),
            Self::Text(bytes) | Self::WideText(bytes) | Self::Binary(bytes) => {
                Ok(DbInt::try_from(bytes.len())?)
            }
        }
    }

    fn is_variable_len(&self) -> bool {
        matches!(self, Self::Text(_) | Self::WideText(_) | Self::Binary(_))
    }

    fn ptr(&self) -> *const u8 {
        match self {
            Self::I32(value) => std::ptr::from_ref(value).cast::<u8>(),
            Self::I64(value) => std::ptr::from_ref(value).cast::<u8>(),
            Self::F64(value) => std::ptr::from_ref(value).cast::<u8>(),
            Self::Bit(value) => std::ptr::from_ref(value).cast::<u8>(),
            Self::Text(bytes) | Self::WideText(bytes) | Self::Binary(bytes) => bytes.as_ptr(),
        }
    }

    fn set_from_batch(
        &mut self,
        batch: &RecordBatch,
        index: usize,
        row_index: usize,
        name: &str,
    ) -> Result<(), Box<dyn Error>> {
        match self {
            Self::I32(value) => {
                let array = required_column::<Int32Array>(batch, index, name)?;
                *value = array.value(row_index);
            }
            Self::I64(value) => match batch.schema().field(index).data_type() {
                DataType::Int64 => {
                    let array = required_column::<Int64Array>(batch, index, name)?;
                    *value = array.value(row_index);
                }
                other => {
                    return Err(format!(
                        "odbc-bcp column `{name}` expected Int64 array, got {other:?}"
                    )
                    .into());
                }
            },
            Self::F64(value) => {
                let array = required_column::<Float64Array>(batch, index, name)?;
                *value = array.value(row_index);
            }
            Self::Bit(value) => {
                let array = required_column::<BooleanArray>(batch, index, name)?;
                *value = u8::from(array.value(row_index));
            }
            Self::Text(bytes) => {
                bytes.clear();
                match batch.schema().field(index).data_type() {
                    DataType::Decimal128(_, scale) => {
                        let array = required_column::<Decimal128Array>(batch, index, name)?;
                        bytes.extend_from_slice(
                            format_decimal(array.value(row_index), *scale)?.as_bytes(),
                        );
                    }
                    DataType::Date32 => {
                        let array = required_column::<Date32Array>(batch, index, name)?;
                        bytes.extend_from_slice(format_date32(array.value(row_index))?.as_bytes());
                    }
                    DataType::Timestamp(unit, timezone) => {
                        if timezone.is_some() {
                            return Err(format!(
                                "odbc-bcp runner supports only timezone-free timestamps for `{name}`"
                            )
                            .into());
                        }
                        if !matches!(unit, arrow_schema::TimeUnit::Millisecond) {
                            return Err(format!(
                                "odbc-bcp runner supports only millisecond timestamps for `{name}`"
                            )
                            .into());
                        }
                        let array = required_column::<TimestampMillisecondArray>(batch, index, name)?;
                        bytes.extend_from_slice(
                            format_timestamp_millis(array.value(row_index))?.as_bytes(),
                        );
                    }
                    other => {
                        return Err(format!(
                            "odbc-bcp text column `{name}` does not support Arrow type {other:?}"
                        )
                        .into());
                    }
                }
            }
            Self::WideText(bytes) => {
                bytes.clear();
                let array = required_column::<StringArray>(batch, index, name)?;
                push_utf16le(bytes, array.value(row_index));
            }
            Self::Binary(bytes) => {
                bytes.clear();
                let array = required_column::<BinaryArray>(batch, index, name)?;
                bytes.extend_from_slice(array.value(row_index));
            }
        }

        Ok(())
    }
}

fn required_column<'a, T: 'static>(
    batch: &'a RecordBatch,
    index: usize,
    name: &str,
) -> Result<&'a T, Box<dyn Error>> {
    batch
        .column(index)
        .as_any()
        .downcast_ref::<T>()
        .ok_or_else(|| format!("Arrow IPC column `{name}` has mismatched runtime array").into())
}

fn format_decimal(unscaled: i128, scale: i8) -> Result<String, Box<dyn Error>> {
    if scale < 0 {
        return Err(format!("odbc-bcp runner does not support negative decimal scale {scale}").into());
    }

    let scale = usize::try_from(scale)?;
    let negative = unscaled < 0;
    let magnitude = if negative {
        unscaled
            .checked_neg()
            .ok_or("decimal value cannot be formatted because it is i128::MIN")? as u128
    } else {
        unscaled as u128
    };
    let mut digits = magnitude.to_string();

    if scale == 0 {
        if negative {
            digits.insert(0, '-');
        }
        return Ok(digits);
    }

    if digits.len() <= scale {
        let mut padded = String::with_capacity(scale + 1);
        for _ in 0..=scale - digits.len() {
            padded.push('0');
        }
        padded.push_str(&digits);
        digits = padded;
    }

    let split = digits.len() - scale;
    let mut value = format!("{}.{}", &digits[..split], &digits[split..]);
    if negative {
        value.insert(0, '-');
    }

    Ok(value)
}

fn push_utf16le(output: &mut Vec<u8>, value: &str) {
    for unit in value.encode_utf16() {
        output.extend_from_slice(&unit.to_le_bytes());
    }
}

fn format_date32(days_since_epoch: i32) -> Result<String, Box<dyn Error>> {
    let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).ok_or("invalid Unix epoch date")?;
    let date = epoch
        .checked_add_signed(chrono::Duration::days(i64::from(days_since_epoch)))
        .ok_or_else(|| format!("Date32 value {days_since_epoch} is out of range"))?;

    Ok(format!(
        "{:04}-{:02}-{:02}",
        date.year(),
        date.month(),
        date.day()
    ))
}

fn format_timestamp_millis(milliseconds: i64) -> Result<String, Box<dyn Error>> {
    let datetime = DateTime::<Utc>::from_timestamp_millis(milliseconds)
        .ok_or_else(|| format!("timestamp millisecond value {milliseconds} is out of range"))?;

    Ok(format!(
        "{:04}-{:02}-{:02} {:02}:{:02}:{:02}.{:03}",
        datetime.year(),
        datetime.month(),
        datetime.day(),
        datetime.hour(),
        datetime.minute(),
        datetime.second(),
        datetime.timestamp_subsec_millis()
    ))
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
        BcpColumnBindings, BenchOptions, TABLE_PLACEHOLDER, c_string, format_date32,
        format_decimal, format_timestamp_millis, push_utf16le, rows_per_second,
        validate_ipc_schema_and_count, BcpColumnBuffer,
    };
    use arrow_array::{
        ArrayRef, BinaryArray, BooleanArray, Date32Array, Decimal128Array, Float32Array,
        Float64Array, Int32Array, Int64Array, RecordBatch, StringArray,
        TimestampMillisecondArray, TimestampNanosecondArray,
    };
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
    fn validates_supported_ipc_schema_and_count() {
        let path = temp_test_file("odbc-bcp-supported");
        write_supported_ipc(&path, 2);

        let rows = validate_ipc_schema_and_count(&path, 2).expect("IPC should validate");

        assert_eq!(rows, 2);
        std::fs::remove_file(path).expect("test IPC cleanup should succeed");
    }

    #[test]
    fn rejects_ipc_row_count_mismatch() {
        let path = temp_test_file("odbc-bcp-row-count");
        write_supported_ipc(&path, 1);

        let err = validate_ipc_schema_and_count(&path, 2).expect_err("row count should be checked");

        assert!(err.to_string().contains("row count"));
        std::fs::remove_file(path).expect("test IPC cleanup should succeed");
    }

    #[test]
    fn rejects_unsupported_arrow_type() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id32", DataType::Int32, false),
            Field::new("unsupported", DataType::Float32, false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int32Array::from(vec![1])) as ArrayRef,
                Arc::new(Float32Array::from(vec![2.5])) as ArrayRef,
            ],
        )
        .expect("batch should build");

        let err = BcpColumnBindings::new(&batch).expect_err("unsupported type should be rejected");

        assert!(err.to_string().contains("Float32"));
    }

    #[test]
    fn rejects_batch_schema_drift_after_binding_plan_is_built() {
        let first = supported_batch(1);
        let bindings = BcpColumnBindings::new(&first).expect("supported schema should bind");
        let drifted_schema = Arc::new(Schema::new(vec![Field::new(
            "id32",
            DataType::Int64,
            false,
        )]));
        let drifted = RecordBatch::try_new(
            drifted_schema,
            vec![Arc::new(Int64Array::from(vec![1])) as ArrayRef],
        )
        .expect("drifted batch should build");

        let err = bindings
            .validate_batch(&drifted)
            .expect_err("schema drift should fail");

        assert!(err.to_string().contains("column count"));
    }

    #[test]
    fn formats_decimal_text_for_bcp_conversion() {
        assert_eq!(format_decimal(12345, 2).unwrap(), "123.45");
        assert_eq!(format_decimal(-12, 4).unwrap(), "-0.0012");
        assert_eq!(format_decimal(7, 0).unwrap(), "7");
    }

    #[test]
    fn rejects_negative_decimal_scale_for_bcp_conversion() {
        let err = format_decimal(1, -1).expect_err("negative scale should fail");

        assert!(err.to_string().contains("negative decimal scale"));
    }

    #[test]
    fn encodes_utf8_as_utf16le_for_nvarchar_bcp_binding() {
        let mut bytes = Vec::new();

        push_utf16le(&mut bytes, "A\u{00e9}");

        assert_eq!(bytes, [0x41, 0x00, 0xe9, 0x00]);
    }

    #[test]
    fn rejects_non_millisecond_timestamp_for_bcp_conversion() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "ts",
            DataType::Timestamp(arrow_schema::TimeUnit::Nanosecond, None),
            false,
        )]));
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(TimestampNanosecondArray::from_iter_values([1])) as ArrayRef],
        )
        .expect("batch should build");
        let mut buffer = BcpColumnBuffer::new(batch.schema().field(0).data_type())
            .expect("timestamp buffer should be created");

        let err = buffer
            .set_from_batch(&batch, 0, 0, "ts")
            .expect_err("nanosecond timestamp should fail");

        assert!(err.to_string().contains("millisecond timestamps"));
    }

    #[test]
    fn formats_temporal_text_for_bcp_conversion() {
        assert_eq!(format_date32(0).unwrap(), "1970-01-01");
        assert_eq!(
            format_timestamp_millis(1_735_689_600_123).unwrap(),
            "2025-01-01 00:00:00.123"
        );
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

    fn write_supported_ipc(path: impl AsRef<std::path::Path>, rows: usize) {
        let batch = supported_batch(rows);
        let schema = batch.schema();
        let mut file = std::fs::File::create(path).expect("test IPC file should be created");
        let mut writer =
            FileWriter::try_new(&mut file, &schema).expect("test IPC writer should be created");
        writer.write(&batch).expect("test batch should be written");
        writer.finish().expect("test IPC writer should finish");
    }

    fn supported_batch(rows: usize) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id32", DataType::Int32, false),
            Field::new("id64", DataType::Int64, false),
            Field::new("score", DataType::Float64, false),
            Field::new("flag", DataType::Boolean, true),
            Field::new("label", DataType::Utf8, true),
            Field::new("payload", DataType::Binary, true),
            Field::new("amount", DataType::Decimal128(18, 4), false),
            Field::new("trade_date", DataType::Date32, false),
            Field::new(
                "posted_at_ms",
                DataType::Timestamp(arrow_schema::TimeUnit::Millisecond, None),
                false,
            ),
        ]));
        RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int32Array::from_iter_values(0..rows as i32)) as ArrayRef,
                Arc::new(Int64Array::from_iter_values(
                    (0..rows).map(|row| row as i64 * 10),
                )) as ArrayRef,
                Arc::new(Float64Array::from_iter_values(
                    (0..rows).map(|row| row as f64 + 0.5),
                )) as ArrayRef,
                Arc::new(BooleanArray::from(
                    (0..rows)
                        .map(|row| if row % 2 == 0 { Some(true) } else { None })
                        .collect::<Vec<_>>(),
                )) as ArrayRef,
                Arc::new(StringArray::from(
                    (0..rows)
                        .map(|row| Some(format!("label-{row}")))
                        .collect::<Vec<_>>(),
                )) as ArrayRef,
                Arc::new(BinaryArray::from_iter(
                    (0..rows).map(|row| Some(vec![u8::try_from(row % 251).unwrap()])),
                )) as ArrayRef,
                Arc::new(
                    Decimal128Array::from_iter_values((0..rows).map(|row| row as i128 + 1))
                        .with_precision_and_scale(18, 4)
                        .expect("decimal metadata should be valid"),
                ) as ArrayRef,
                Arc::new(Date32Array::from_iter_values(
                    (0..rows).map(|row| row as i32),
                )) as ArrayRef,
                Arc::new(TimestampMillisecondArray::from_iter_values(
                    (0..rows).map(|row| 1_735_689_600_000_i64 + row as i64),
                )) as ArrayRef,
            ],
        )
        .expect("test batch should build")
    }

    fn temp_test_file(label: &str) -> PathBuf {
        let counter = TEST_FILE_COUNTER.fetch_add(1, Ordering::Relaxed);
        std::env::temp_dir().join(format!("{label}-{}-{counter}.arrow", std::process::id()))
    }
}

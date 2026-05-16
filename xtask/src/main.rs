//! Repository maintenance tasks for `arrow-tiberius`.

use std::env;
use std::ffi::OsString;
use std::fmt;
use std::net::{Ipv4Addr, SocketAddrV4, TcpListener};
use std::path::PathBuf;
use std::process::{Command, ExitCode, Stdio};
use std::thread::sleep;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

mod writer_bench;

fn main() -> ExitCode {
    match run(env::args_os().skip(1)) {
        Ok(()) => ExitCode::SUCCESS,
        Err(err) => {
            eprintln!("error: {err}");
            ExitCode::FAILURE
        }
    }
}

fn run(args: impl IntoIterator<Item = OsString>) -> Result<(), XtaskError> {
    let args = args.into_iter().collect::<Vec<_>>();

    match args.first().and_then(|arg| arg.to_str()) {
        None | Some("-h" | "--help") => {
            print_help();
            Ok(())
        }
        Some("sqlserver-test") => {
            if args[1..].iter().any(|arg| arg == "-h" || arg == "--help") {
                print_help();
                return Ok(());
            }

            let options = SqlServerTestOptions::parse(&args[1..])?;
            run_sqlserver_tests(&options)
        }
        Some("writer-bench") => writer_bench::run(&args[1..]).map_err(XtaskError::WriterBench),
        Some(command) => Err(XtaskError::UnknownCommand(command.to_owned())),
    }
}

fn run_sqlserver_tests(options: &SqlServerTestOptions) -> Result<(), XtaskError> {
    println!("sqlserver-test");

    let connection = if let Some(connection_string) = &options.connection_string {
        println!("  container runtime: <not used>");
        println!("  existing connection: true");
        SqlServerConnection {
            connection_string: connection_string.clone(),
            database: options.database.clone(),
            _container: None,
        }
    } else {
        println!("  existing connection: false");
        let runtime = options.resolve_container_runtime()?;
        println!("  container runtime: {}", runtime.display());
        println!("  image: {}", options.image);

        let container = SqlServerContainer::start(options, runtime)?;
        let connection = container.connection();
        container.wait_until_ready()?;
        container.initialize_database(&options.database)?;

        SqlServerConnection {
            connection_string: connection,
            database: options.database.clone(),
            _container: Some(container),
        }
    };

    println!("  keep container: {}", options.keep_container);
    println!("  test database: {}", connection.database);

    let mut command = Command::new("cargo");
    command
        .arg("test")
        .arg("--features")
        .arg("integration-tests")
        .env(CONNECTION_STRING_ENV, &connection.connection_string)
        .env(TEST_DATABASE_ENV, &connection.database);

    run_command(&mut command, "cargo test --features integration-tests")?;
    Ok(())
}

fn print_help() {
    println!(
        "Usage:\n  cargo xtask <COMMAND> [OPTIONS]\n\nCommands:\n  sqlserver-test    Run SQL Server integration tests\n  writer-bench      Generate writer benchmark inputs and summaries\n\nRun `cargo xtask <COMMAND> --help` for command-specific options."
    );
}

#[derive(Debug)]
struct SqlServerConnection {
    connection_string: String,
    database: String,
    _container: Option<SqlServerContainer>,
}

#[derive(Debug)]
struct SqlServerContainer {
    runtime: PathBuf,
    name: String,
    password: String,
    host_port: u16,
    keep_container: bool,
}

impl SqlServerContainer {
    fn start(options: &SqlServerTestOptions, runtime: PathBuf) -> Result<Self, XtaskError> {
        let host_port = find_free_port()?;
        let name = format!("arrow-tiberius-sqlserver-{}", unique_suffix());
        let password = generate_password();

        let mut command = Command::new(&runtime);
        command
            .arg("run")
            .arg("--detach")
            .arg("--name")
            .arg(&name)
            .arg("--label")
            .arg("org.arrow-tiberius.xtask=sqlserver-test")
            .arg("--env")
            .arg("ACCEPT_EULA=Y")
            .arg("--env")
            .arg(format!("MSSQL_SA_PASSWORD={password}"))
            .arg("--publish")
            .arg(format!("127.0.0.1:{host_port}:1433"))
            .arg(&options.image);

        run_command(&mut command, "start SQL Server container")?;

        Ok(Self {
            runtime,
            name,
            password,
            host_port,
            keep_container: options.keep_container,
        })
    }

    fn connection(&self) -> String {
        format!(
            "server=tcp:127.0.0.1,{};user id=sa;password={};TrustServerCertificate=true",
            self.host_port, self.password
        )
    }

    fn wait_until_ready(&self) -> Result<(), XtaskError> {
        let deadline = Instant::now() + Duration::from_secs(SQLSERVER_READY_TIMEOUT_SECS);
        let mut last_error = None;

        while Instant::now() < deadline {
            match self.sqlcmd("SELECT 1") {
                Ok(()) => return Ok(()),
                Err(err) => {
                    last_error = Some(err.to_string());
                    sleep(Duration::from_secs(2));
                }
            }
        }

        Err(XtaskError::SqlServerReadinessTimeout {
            seconds: SQLSERVER_READY_TIMEOUT_SECS,
            last_error,
        })
    }

    fn initialize_database(&self, database: &str) -> Result<(), XtaskError> {
        validate_database_name(database)?;
        let escaped_database = database.replace(']', "]]");
        let sql = format!(
            "IF DB_ID(N'{database}') IS NULL CREATE DATABASE [{escaped_database}]; ALTER DATABASE [{escaped_database}] SET COMPATIBILITY_LEVEL = 100;"
        );

        self.sqlcmd(&sql)
    }

    fn sqlcmd(&self, query: &str) -> Result<(), XtaskError> {
        let commands = [
            SqlCmd {
                path: "/opt/mssql-tools18/bin/sqlcmd",
                trust_server_certificate: true,
            },
            SqlCmd {
                path: "/opt/mssql-tools/bin/sqlcmd",
                trust_server_certificate: false,
            },
        ];

        let mut last_error = None;
        for sqlcmd in commands {
            let mut command = Command::new(&self.runtime);
            command
                .arg("exec")
                .arg(&self.name)
                .arg(sqlcmd.path)
                .arg("-S")
                .arg("localhost")
                .arg("-U")
                .arg("sa")
                .arg("-P")
                .arg(&self.password)
                .arg("-Q")
                .arg(query);

            if sqlcmd.trust_server_certificate {
                command.arg("-C");
            }

            match run_command_capture(&mut command) {
                Ok(()) => return Ok(()),
                Err(err) => last_error = Some(err),
            }
        }

        Err(last_error.unwrap_or_else(|| {
            XtaskError::CommandFailed(
                "sqlcmd".to_owned(),
                "no sqlcmd command was attempted".to_owned(),
            )
        }))
    }
}

impl Drop for SqlServerContainer {
    fn drop(&mut self) {
        if self.keep_container {
            eprintln!("keeping SQL Server container `{}`", self.name);
            return;
        }

        let status = Command::new(&self.runtime)
            .arg("rm")
            .arg("--force")
            .arg("--volumes")
            .arg(&self.name)
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status();

        if let Err(err) = status {
            eprintln!(
                "failed to clean up SQL Server container `{}`: {err}",
                self.name
            );
        }
    }
}

#[derive(Debug, Clone, Copy)]
struct SqlCmd {
    path: &'static str,
    trust_server_certificate: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct SqlServerTestOptions {
    container_runtime: Option<PathBuf>,
    connection_string: Option<String>,
    image: String,
    database: String,
    keep_container: bool,
}

impl Default for SqlServerTestOptions {
    fn default() -> Self {
        Self {
            container_runtime: None,
            connection_string: None,
            image: "mcr.microsoft.com/mssql/server:2017-latest".to_owned(),
            database: "arrow_tiberius_integration".to_owned(),
            keep_container: false,
        }
    }
}

impl SqlServerTestOptions {
    fn parse(args: &[OsString]) -> Result<Self, XtaskError> {
        let mut options = Self::default();
        let mut index = 0;

        while index < args.len() {
            let arg = args[index]
                .to_str()
                .ok_or_else(|| XtaskError::InvalidUtf8Argument(args[index].clone()))?;

            match arg {
                "-h" | "--help" => {
                    print_help();
                    return Ok(options);
                }
                "--container-runtime" => {
                    options.container_runtime = Some(PathBuf::from(required_value(args, index)?));
                    index += 1;
                }
                "--connection-string" => {
                    options.connection_string = Some(required_value(args, index)?);
                    index += 1;
                }
                "--image" => {
                    options.image = required_value(args, index)?;
                    index += 1;
                }
                "--database" => {
                    options.database = required_value(args, index)?;
                    index += 1;
                }
                "--keep-container" => {
                    options.keep_container = true;
                }
                other => return Err(XtaskError::UnknownOption(other.to_owned())),
            }

            index += 1;
        }

        Ok(options)
    }

    fn resolve_container_runtime(&self) -> Result<PathBuf, XtaskError> {
        if let Some(runtime) = &self.container_runtime {
            return Ok(runtime.clone());
        }

        if let Some(runtime) = env::var_os(CONTAINER_RUNTIME_ENV) {
            return Ok(PathBuf::from(runtime));
        }

        find_on_path("docker")
            .or_else(|| find_on_path("podman"))
            .ok_or(XtaskError::ContainerRuntimeNotFound)
    }
}

fn required_value(args: &[OsString], index: usize) -> Result<String, XtaskError> {
    let value = args
        .get(index + 1)
        .ok_or_else(|| XtaskError::MissingOptionValue(option_name(args, index)))?;

    value
        .to_str()
        .map(ToOwned::to_owned)
        .ok_or_else(|| XtaskError::InvalidUtf8Argument(value.clone()))
}

fn option_name(args: &[OsString], index: usize) -> String {
    args.get(index)
        .and_then(|arg| arg.to_str())
        .unwrap_or("<unknown>")
        .to_owned()
}

fn find_on_path(executable: &str) -> Option<PathBuf> {
    let path = env::var_os("PATH")?;

    env::split_paths(&path)
        .map(|dir| dir.join(executable))
        .find(|candidate| candidate.is_file())
}

fn run_command(command: &mut Command, description: &'static str) -> Result<(), XtaskError> {
    let status = command
        .status()
        .map_err(|source| XtaskError::CommandSpawn {
            description,
            source,
        })?;

    if status.success() {
        Ok(())
    } else {
        Err(XtaskError::CommandStatus {
            description,
            status,
        })
    }
}

fn run_command_capture(command: &mut Command) -> Result<(), XtaskError> {
    let output = command
        .output()
        .map_err(|source| XtaskError::CommandSpawn {
            description: "run command",
            source,
        })?;

    if output.status.success() {
        return Ok(());
    }

    let mut message = String::new();
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    if !stdout.trim().is_empty() {
        message.push_str(stdout.trim());
    }
    if !stderr.trim().is_empty() {
        if !message.is_empty() {
            message.push('\n');
        }
        message.push_str(stderr.trim());
    }

    if message.is_empty() {
        message = format!("command exited with {}", output.status);
    }

    Err(XtaskError::CommandFailed("command".to_owned(), message))
}

fn find_free_port() -> Result<u16, XtaskError> {
    let listener = TcpListener::bind(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 0))
        .map_err(XtaskError::PortBind)?;
    let port = listener.local_addr().map_err(XtaskError::PortBind)?.port();
    Ok(port)
}

fn unique_suffix() -> String {
    let millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis();
    format!("{}-{millis}", std::process::id())
}

fn generate_password() -> String {
    format!("ArrowTiberius_{}!aA9", unique_suffix().replace('-', ""))
}

fn validate_database_name(database: &str) -> Result<(), XtaskError> {
    if database.is_empty() {
        return Err(XtaskError::InvalidDatabaseName(
            "database name cannot be empty".to_owned(),
        ));
    }

    if database.len() > 128 {
        return Err(XtaskError::InvalidDatabaseName(
            "database name cannot exceed 128 bytes".to_owned(),
        ));
    }

    if !database
        .chars()
        .all(|ch| ch.is_ascii_alphanumeric() || ch == '_')
    {
        return Err(XtaskError::InvalidDatabaseName(
            "database name can only contain ASCII letters, digits, and underscores".to_owned(),
        ));
    }

    Ok(())
}

#[derive(Debug)]
enum XtaskError {
    UnknownCommand(String),
    UnknownOption(String),
    MissingOptionValue(String),
    InvalidUtf8Argument(OsString),
    ContainerRuntimeNotFound,
    CommandSpawn {
        description: &'static str,
        source: std::io::Error,
    },
    CommandStatus {
        description: &'static str,
        status: std::process::ExitStatus,
    },
    CommandFailed(String, String),
    PortBind(std::io::Error),
    InvalidDatabaseName(String),
    SqlServerReadinessTimeout {
        seconds: u64,
        last_error: Option<String>,
    },
    WriterBench(writer_bench::WriterBenchError),
}

impl fmt::Display for XtaskError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnknownCommand(command) => write!(f, "unknown command `{command}`"),
            Self::UnknownOption(option) => write!(f, "unknown option `{option}`"),
            Self::MissingOptionValue(option) => write!(f, "missing value for `{option}`"),
            Self::InvalidUtf8Argument(arg) => write!(f, "argument is not valid UTF-8: {arg:?}"),
            Self::ContainerRuntimeNotFound => write!(
                f,
                "container runtime not found; set {CONTAINER_RUNTIME_ENV} or pass --container-runtime"
            ),
            Self::CommandSpawn {
                description,
                source,
            } => {
                write!(f, "failed to {description}: {source}")
            }
            Self::CommandStatus {
                description,
                status,
            } => {
                write!(f, "{description} failed with {status}")
            }
            Self::CommandFailed(command, message) => write!(f, "{command} failed: {message}"),
            Self::PortBind(source) => write!(f, "failed to reserve a local port: {source}"),
            Self::InvalidDatabaseName(reason) => write!(f, "invalid database name: {reason}"),
            Self::SqlServerReadinessTimeout {
                seconds,
                last_error,
            } => {
                write!(f, "SQL Server did not become ready within {seconds}s")?;
                if let Some(last_error) = last_error {
                    write!(f, "; last error: {last_error}")?;
                }
                Ok(())
            }
            Self::WriterBench(source) => write!(f, "{source}"),
        }
    }
}

const CONTAINER_RUNTIME_ENV: &str = "ARROW_TIBERIUS_CONTAINER_RUNTIME";
const CONNECTION_STRING_ENV: &str = "ARROW_TIBERIUS_TEST_MSSQL_URL";
const TEST_DATABASE_ENV: &str = "ARROW_TIBERIUS_TEST_MSSQL_DATABASE";
const SQLSERVER_READY_TIMEOUT_SECS: u64 = 120;

#[cfg(test)]
mod tests {
    use super::{SqlServerTestOptions, XtaskError};
    use std::ffi::OsString;
    use std::path::PathBuf;

    #[test]
    fn parses_sqlserver_test_options() {
        let args = [
            OsString::from("--container-runtime"),
            OsString::from("podman"),
            OsString::from("--connection-string"),
            OsString::from("server=tcp:127.0.0.1,1433"),
            OsString::from("--image"),
            OsString::from("custom-sqlserver"),
            OsString::from("--database"),
            OsString::from("custom_db"),
            OsString::from("--keep-container"),
        ];

        let options = SqlServerTestOptions::parse(&args).unwrap();

        assert_eq!(options.container_runtime, Some(PathBuf::from("podman")));
        assert_eq!(
            options.connection_string.as_deref(),
            Some("server=tcp:127.0.0.1,1433")
        );
        assert_eq!(options.image, "custom-sqlserver");
        assert_eq!(options.database, "custom_db");
        assert!(options.keep_container);
    }

    #[test]
    fn rejects_missing_option_value() {
        let args = [OsString::from("--image")];
        let err = SqlServerTestOptions::parse(&args).unwrap_err();

        assert!(matches!(err, XtaskError::MissingOptionValue(option) if option == "--image"));
    }

    #[test]
    fn rejects_unknown_options() {
        let args = [OsString::from("--wat")];
        let err = SqlServerTestOptions::parse(&args).unwrap_err();

        assert!(matches!(err, XtaskError::UnknownOption(option) if option == "--wat"));
    }
}

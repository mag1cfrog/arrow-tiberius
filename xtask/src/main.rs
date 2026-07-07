//! Repository maintenance tasks for `arrow-tiberius`.

use std::env;
use std::ffi::OsString;
use std::fmt;
use std::path::PathBuf;
use std::process::{Command, ExitCode};

mod odbc_runner;
mod sqlserver;
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
            print_top_level_help();
            Ok(())
        }
        Some("sqlserver-test") => {
            if args[1..].iter().any(|arg| arg == "-h" || arg == "--help") {
                print_sqlserver_test_help();
                return Ok(());
            }

            let options = SqlServerTestOptions::parse(&args[1..])?;
            run_sqlserver_tests(&options)
        }
        Some("sqlserver-compat-probe") => {
            if args[1..].iter().any(|arg| arg == "-h" || arg == "--help") {
                print_sqlserver_compat_probe_help();
                return Ok(());
            }

            let options = SqlServerCompatProbeOptions::parse(&args[1..])?;
            run_sqlserver_compat_probe(&options)
        }
        Some("writer-bench") => writer_bench::run(&args[1..]).map_err(XtaskError::WriterBench),
        Some(command) => Err(XtaskError::UnknownCommand(command.to_owned())),
    }
}

fn run_sqlserver_tests(options: &SqlServerTestOptions) -> Result<(), XtaskError> {
    println!("sqlserver-test");

    if options.connection.connection_string.is_some() {
        println!("  existing connection: true");
        println!("  container runtime: <not used>");
    } else {
        println!("  existing connection: false");
        if let Some(runtime) = &options.connection.container_runtime {
            println!("  container runtime: {}", runtime.display());
        } else {
            println!("  container runtime: <auto>");
        }
        println!("  image: {}", options.connection.image);
    }

    println!("  keep container: {}", options.connection.keep_container);

    let connection = options
        .connection
        .connect_or_start()
        .map_err(XtaskError::SqlServer)?;

    println!("  test database: {}", connection.database);

    let mut command = Command::new("cargo");
    command
        .arg("test")
        .arg("--features")
        .arg("integration-tests")
        .env(
            sqlserver::CONNECTION_STRING_ENV,
            &connection.connection_string,
        )
        .env(sqlserver::TEST_DATABASE_ENV, &connection.database);

    run_command(&mut command, "cargo test --features integration-tests")?;
    Ok(())
}

fn run_sqlserver_compat_probe(options: &SqlServerCompatProbeOptions) -> Result<(), XtaskError> {
    println!("sqlserver-compat-probe");

    if options.connection.connection_string.is_some() {
        println!("  existing connection: true");
        println!("  container runtime: <not used>");
    } else {
        println!("  existing connection: false");
        if let Some(runtime) = &options.connection.container_runtime {
            println!("  container runtime: {}", runtime.display());
        } else {
            println!("  container runtime: <auto>");
        }
        println!("  image: {}", options.connection.image);
    }

    println!("  compatibility level: {}", options.compatibility_level);
    println!("  SQL Server version: {}", options.server_version);
    println!("  keep container: {}", options.connection.keep_container);

    let connection = options
        .connection
        .connect_or_start_with_compatibility_level(options.compatibility_level)
        .map_err(XtaskError::SqlServer)?;

    println!("  test database: {}", connection.database);

    let mut command = Command::new("cargo");
    command
        .arg("test")
        .arg("--features")
        .arg("integration-tests")
        .arg("--test")
        .arg("compatibility_sqlserver")
        .env(
            sqlserver::CONNECTION_STRING_ENV,
            &connection.connection_string,
        )
        .env(sqlserver::TEST_DATABASE_ENV, &connection.database)
        .env(
            sqlserver::COMPATIBILITY_LEVEL_ENV,
            options.compatibility_level.to_string(),
        )
        .env(
            sqlserver::SERVER_VERSION_ENV,
            options.server_version.as_str(),
        );

    run_command(
        &mut command,
        "cargo test --features integration-tests --test compatibility_sqlserver",
    )?;
    Ok(())
}

fn print_top_level_help() {
    println!(
        "Usage:\n  cargo xtask <COMMAND> [OPTIONS]\n\nCommands:\n  sqlserver-test          Run SQL Server integration tests\n  sqlserver-compat-probe  Run focused SQL Server compatibility probes\n  writer-bench            Generate writer benchmark inputs and summaries\n\nRun `cargo xtask <COMMAND> --help` for command-specific options."
    );
}

fn print_sqlserver_test_help() {
    println!(
        "Usage:\n  cargo xtask sqlserver-test [OPTIONS]\n\nOptions:\n  --container-runtime <PATH>  Container runtime executable, such as docker or podman\n  --connection-string <URL>   Use an existing SQL Server instead of a local container\n  --image <IMAGE>             SQL Server container image\n  --database <NAME>           Test database name\n  --keep-container            Keep the container after the task exits\n  -h, --help                  Print help"
    );
}

fn print_sqlserver_compat_probe_help() {
    println!(
        "Usage:\n  cargo xtask sqlserver-compat-probe [OPTIONS]\n\nOptions:\n  --container-runtime <PATH>     Container runtime executable, such as docker or podman\n  --connection-string <URL>      Use an existing SQL Server instead of a local container\n  --image <IMAGE>                SQL Server container image\n  --database <NAME>              Test database name\n  --version <YEAR>               SQL Server version: 2017, 2019, 2022, or 2025\n  --compatibility-level <LEVEL>  SQL Server database compatibility level\n  --keep-container               Keep the container after the task exits\n  -h, --help                     Print help"
    );
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct SqlServerTestOptions {
    connection: sqlserver::SqlServerConnectionOptions,
}

impl Default for SqlServerTestOptions {
    fn default() -> Self {
        Self {
            connection: sqlserver::SqlServerConnectionOptions::integration_default(),
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
                    print_sqlserver_test_help();
                    return Ok(options);
                }
                "--container-runtime" => {
                    options.connection.container_runtime =
                        Some(PathBuf::from(required_value(args, index)?));
                    index += 1;
                }
                "--connection-string" => {
                    options.connection.connection_string = Some(required_value(args, index)?);
                    index += 1;
                }
                "--image" => {
                    options.connection.image = required_value(args, index)?;
                    index += 1;
                }
                "--database" => {
                    options.connection.database = required_value(args, index)?;
                    index += 1;
                }
                "--keep-container" => {
                    options.connection.keep_container = true;
                }
                other => return Err(XtaskError::UnknownOption(other.to_owned())),
            }

            index += 1;
        }

        Ok(options)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct SqlServerCompatProbeOptions {
    connection: sqlserver::SqlServerConnectionOptions,
    server_version: String,
    compatibility_level: u16,
}

impl Default for SqlServerCompatProbeOptions {
    fn default() -> Self {
        let mut connection = sqlserver::SqlServerConnectionOptions::integration_default();
        connection.database = "arrow_tiberius_compat_probe".to_owned();

        Self {
            connection,
            server_version: "2017".to_owned(),
            compatibility_level: 100,
        }
    }
}

impl SqlServerCompatProbeOptions {
    fn parse(args: &[OsString]) -> Result<Self, XtaskError> {
        let mut options = Self::default();
        let mut index = 0;

        while index < args.len() {
            let arg = args[index]
                .to_str()
                .ok_or_else(|| XtaskError::InvalidUtf8Argument(args[index].clone()))?;

            match arg {
                "-h" | "--help" => {
                    print_sqlserver_compat_probe_help();
                    return Ok(options);
                }
                "--container-runtime" => {
                    options.connection.container_runtime =
                        Some(PathBuf::from(required_value(args, index)?));
                    index += 1;
                }
                "--connection-string" => {
                    options.connection.connection_string = Some(required_value(args, index)?);
                    index += 1;
                }
                "--image" => {
                    options.connection.image = required_value(args, index)?;
                    index += 1;
                }
                "--database" => {
                    options.connection.database = required_value(args, index)?;
                    index += 1;
                }
                "--version" => {
                    let value = required_value(args, index)?;
                    if !is_supported_sqlserver_version(&value) {
                        return Err(XtaskError::InvalidSqlServerVersion(value));
                    }
                    options.server_version = value;
                    index += 1;
                }
                "--compatibility-level" => {
                    let value = required_value(args, index)?;
                    options.compatibility_level = value
                        .parse::<u16>()
                        .map_err(|_| XtaskError::InvalidCompatibilityLevel(value))?;
                    index += 1;
                }
                "--keep-container" => {
                    options.connection.keep_container = true;
                }
                other => return Err(XtaskError::UnknownOption(other.to_owned())),
            }

            index += 1;
        }

        Ok(options)
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

fn is_supported_sqlserver_version(value: &str) -> bool {
    matches!(value, "2017" | "2019" | "2022" | "2025")
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

#[derive(Debug)]
enum XtaskError {
    UnknownCommand(String),
    UnknownOption(String),
    MissingOptionValue(String),
    InvalidCompatibilityLevel(String),
    InvalidSqlServerVersion(String),
    InvalidUtf8Argument(OsString),
    CommandSpawn {
        description: &'static str,
        source: std::io::Error,
    },
    CommandStatus {
        description: &'static str,
        status: std::process::ExitStatus,
    },
    SqlServer(sqlserver::SqlServerError),
    WriterBench(writer_bench::WriterBenchError),
}

impl fmt::Display for XtaskError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnknownCommand(command) => write!(f, "unknown command `{command}`"),
            Self::UnknownOption(option) => write!(f, "unknown option `{option}`"),
            Self::MissingOptionValue(option) => write!(f, "missing value for `{option}`"),
            Self::InvalidCompatibilityLevel(value) => {
                write!(f, "invalid SQL Server compatibility level `{value}`")
            }
            Self::InvalidSqlServerVersion(value) => {
                write!(f, "invalid SQL Server version `{value}`")
            }
            Self::InvalidUtf8Argument(arg) => write!(f, "argument is not valid UTF-8: {arg:?}"),
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
            Self::SqlServer(source) => write!(f, "{source}"),
            Self::WriterBench(source) => write!(f, "{source}"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{SqlServerCompatProbeOptions, SqlServerTestOptions, XtaskError};
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

        assert_eq!(
            options.connection.container_runtime,
            Some(PathBuf::from("podman"))
        );
        assert_eq!(
            options.connection.connection_string.as_deref(),
            Some("server=tcp:127.0.0.1,1433")
        );
        assert_eq!(options.connection.image, "custom-sqlserver");
        assert_eq!(options.connection.database, "custom_db");
        assert!(options.connection.keep_container);
    }

    #[test]
    fn parses_sqlserver_compat_probe_options() {
        let args = [
            OsString::from("--container-runtime"),
            OsString::from("podman"),
            OsString::from("--connection-string"),
            OsString::from("server=tcp:127.0.0.1,1433"),
            OsString::from("--image"),
            OsString::from("custom-sqlserver"),
            OsString::from("--database"),
            OsString::from("custom_db"),
            OsString::from("--version"),
            OsString::from("2022"),
            OsString::from("--compatibility-level"),
            OsString::from("140"),
            OsString::from("--keep-container"),
        ];

        let options = SqlServerCompatProbeOptions::parse(&args).unwrap();

        assert_eq!(
            options.connection.container_runtime,
            Some(PathBuf::from("podman"))
        );
        assert_eq!(
            options.connection.connection_string.as_deref(),
            Some("server=tcp:127.0.0.1,1433")
        );
        assert_eq!(options.connection.image, "custom-sqlserver");
        assert_eq!(options.connection.database, "custom_db");
        assert_eq!(options.server_version, "2022");
        assert_eq!(options.compatibility_level, 140);
        assert!(options.connection.keep_container);
    }

    #[test]
    fn sqlserver_compat_probe_uses_separate_database_by_default() {
        let options = SqlServerCompatProbeOptions::default();

        assert_eq!(options.connection.database, "arrow_tiberius_compat_probe");
        assert_eq!(options.server_version, "2017");
        assert_eq!(options.compatibility_level, 100);
    }

    #[test]
    fn rejects_invalid_compatibility_level() {
        let args = [
            OsString::from("--compatibility-level"),
            OsString::from("not-a-level"),
        ];
        let err = SqlServerCompatProbeOptions::parse(&args).unwrap_err();

        assert!(
            matches!(err, XtaskError::InvalidCompatibilityLevel(value) if value == "not-a-level")
        );
    }

    #[test]
    fn rejects_invalid_sqlserver_version() {
        let args = [OsString::from("--version"), OsString::from("2014")];
        let err = SqlServerCompatProbeOptions::parse(&args).unwrap_err();

        assert!(matches!(err, XtaskError::InvalidSqlServerVersion(value) if value == "2014"));
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

    #[test]
    fn sqlserver_help_is_command_specific() {
        let result = super::run([OsString::from("sqlserver-test"), OsString::from("--help")]);

        assert!(result.is_ok());
    }

    #[test]
    fn sqlserver_compat_probe_help_is_command_specific() {
        let result = super::run([
            OsString::from("sqlserver-compat-probe"),
            OsString::from("--help"),
        ]);

        assert!(result.is_ok());
    }
}

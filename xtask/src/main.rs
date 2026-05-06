//! Repository maintenance tasks for `arrow-tiberius`.

use std::env;
use std::ffi::OsString;
use std::fmt;
use std::path::PathBuf;
use std::process::ExitCode;

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
            println!("sqlserver-test");
            if options.connection_string.is_some() {
                println!("  container runtime: <not used>");
            } else {
                let runtime = options.resolve_container_runtime()?;
                println!("  container runtime: {}", runtime.display());
            }
            println!("  keep container: {}", options.keep_container);
            println!(
                "  existing connection: {}",
                options.connection_string.is_some()
            );
            println!("  image: {}", options.image);
            println!("  test database: {}", options.database);
            println!("  status: runner execution will be added in the next step");
            Ok(())
        }
        Some(command) => Err(XtaskError::UnknownCommand(command.to_owned())),
    }
}

fn print_help() {
    println!(
        "Usage:\n  cargo xtask sqlserver-test [OPTIONS]\n\nCommands:\n  sqlserver-test    Run SQL Server integration tests\n\nOptions:\n  --container-runtime <PATH>  Container runtime executable, such as docker or podman\n  --connection-string <URL>   Use an existing SQL Server instead of a local container\n  --image <IMAGE>             SQL Server container image\n  --database <NAME>           Test database name\n  --keep-container            Keep the container after the task exits\n  -h, --help                  Print help"
    );
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

#[derive(Debug)]
enum XtaskError {
    UnknownCommand(String),
    UnknownOption(String),
    MissingOptionValue(String),
    InvalidUtf8Argument(OsString),
    ContainerRuntimeNotFound,
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
        }
    }
}

const CONTAINER_RUNTIME_ENV: &str = "ARROW_TIBERIUS_CONTAINER_RUNTIME";

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

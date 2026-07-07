use std::env;
use std::fmt;
use std::net::{Ipv4Addr, SocketAddrV4, TcpListener};
use std::path::PathBuf;
use std::process::{Command, Stdio};
use std::thread::sleep;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

#[derive(Debug)]
pub(crate) struct SqlServerConnection {
    pub(crate) connection_string: String,
    pub(crate) container_connection_string: Option<String>,
    pub(crate) database: String,
    _container: Option<SqlServerContainer>,
}

#[derive(Debug)]
pub(crate) struct ManagedNetwork {
    runtime: PathBuf,
    name: String,
    keep_network: bool,
}

impl ManagedNetwork {
    pub(crate) fn create(runtime: PathBuf, keep_network: bool) -> Result<Self, SqlServerError> {
        let name = format!("arrow-tiberius-bench-network-{}", unique_suffix());
        let mut command = Command::new(&runtime);
        command
            .arg("network")
            .arg("create")
            .arg("--label")
            .arg("org.arrow-tiberius.xtask=sqlserver")
            .arg(&name);

        run_command_capture(&mut command)?;

        Ok(Self {
            runtime,
            name,
            keep_network,
        })
    }

    pub(crate) fn name(&self) -> &str {
        &self.name
    }
}

impl Drop for ManagedNetwork {
    fn drop(&mut self) {
        if self.keep_network {
            eprintln!("keeping benchmark container network `{}`", self.name);
            return;
        }

        let status = Command::new(&self.runtime)
            .arg("network")
            .arg("rm")
            .arg(&self.name)
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status();

        if let Err(err) = status {
            eprintln!(
                "failed to clean up benchmark container network `{}`: {err}",
                self.name
            );
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct SqlServerConnectionOptions {
    pub(crate) container_runtime: Option<PathBuf>,
    pub(crate) connection_string: Option<String>,
    pub(crate) image: String,
    pub(crate) database: String,
    pub(crate) keep_container: bool,
}

impl SqlServerConnectionOptions {
    pub(crate) fn integration_default() -> Self {
        Self {
            container_runtime: None,
            connection_string: None,
            image: "mcr.microsoft.com/mssql/server:2017-latest".to_owned(),
            database: "arrow_tiberius_integration".to_owned(),
            keep_container: false,
        }
    }

    pub(crate) fn benchmark_default() -> Self {
        Self {
            database: "arrow_tiberius_benchmark".to_owned(),
            ..Self::integration_default()
        }
    }

    pub(crate) fn connect_or_start(&self) -> Result<SqlServerConnection, SqlServerError> {
        self.connect_or_start_with_compatibility_level(100)
    }

    pub(crate) fn connect_or_start_with_compatibility_level(
        &self,
        compatibility_level: u16,
    ) -> Result<SqlServerConnection, SqlServerError> {
        self.connect_or_start_with_network_and_compatibility_level(None, compatibility_level)
    }

    pub(crate) fn connect_or_start_with_network(
        &self,
        network: Option<&ManagedNetwork>,
    ) -> Result<SqlServerConnection, SqlServerError> {
        self.connect_or_start_with_network_and_compatibility_level(network, 100)
    }

    fn connect_or_start_with_network_and_compatibility_level(
        &self,
        network: Option<&ManagedNetwork>,
        compatibility_level: u16,
    ) -> Result<SqlServerConnection, SqlServerError> {
        if let Some(connection_string) = &self.connection_string {
            return Ok(SqlServerConnection {
                connection_string: connection_string.clone(),
                container_connection_string: None,
                database: self.database.clone(),
                _container: None,
            });
        }

        let runtime = self.resolve_container_runtime()?;
        let container = SqlServerContainer::start(self, runtime, network)?;
        let connection = container.host_connection();
        let container_connection = network.is_some().then(|| container.container_connection());
        container.wait_until_ready()?;
        container.initialize_database(&self.database, compatibility_level)?;

        Ok(SqlServerConnection {
            connection_string: connection,
            container_connection_string: container_connection,
            database: self.database.clone(),
            _container: Some(container),
        })
    }

    fn resolve_container_runtime(&self) -> Result<PathBuf, SqlServerError> {
        if let Some(runtime) = &self.container_runtime {
            return Ok(runtime.clone());
        }

        if let Some(runtime) = env::var_os(CONTAINER_RUNTIME_ENV) {
            return Ok(PathBuf::from(runtime));
        }

        find_on_path("docker")
            .or_else(|| find_on_path("podman"))
            .ok_or(SqlServerError::ContainerRuntimeNotFound)
    }

    pub(crate) fn resolve_runtime(&self) -> Result<PathBuf, SqlServerError> {
        self.resolve_container_runtime()
    }
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
    fn start(
        options: &SqlServerConnectionOptions,
        runtime: PathBuf,
        network: Option<&ManagedNetwork>,
    ) -> Result<Self, SqlServerError> {
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
            .arg("org.arrow-tiberius.xtask=sqlserver")
            .arg("--env")
            .arg("ACCEPT_EULA=Y")
            .arg("--env")
            .arg(format!("MSSQL_SA_PASSWORD={password}"))
            .arg("--publish")
            .arg(format!("127.0.0.1:{host_port}:1433"));

        if let Some(network) = network {
            command.arg("--network").arg(network.name());
        }

        command.arg(&options.image);

        run_command_capture(&mut command)?;

        Ok(Self {
            runtime,
            name,
            password,
            host_port,
            keep_container: options.keep_container,
        })
    }

    fn host_connection(&self) -> String {
        format!(
            "server=tcp:127.0.0.1,{};user id=sa;password={};TrustServerCertificate=true",
            self.host_port, self.password
        )
    }

    pub(crate) fn container_connection(&self) -> String {
        format!(
            "server=tcp:{},1433;user id=sa;password={};TrustServerCertificate=true",
            self.name, self.password
        )
    }

    fn wait_until_ready(&self) -> Result<(), SqlServerError> {
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

        Err(SqlServerError::ReadinessTimeout {
            seconds: SQLSERVER_READY_TIMEOUT_SECS,
            last_error,
        })
    }

    fn initialize_database(
        &self,
        database: &str,
        compatibility_level: u16,
    ) -> Result<(), SqlServerError> {
        validate_database_name(database)?;
        let escaped_database = database.replace(']', "]]");
        let sql = format!(
            "IF DB_ID(N'{database}') IS NULL CREATE DATABASE [{escaped_database}]; ALTER DATABASE [{escaped_database}] SET COMPATIBILITY_LEVEL = {compatibility_level};"
        );

        self.sqlcmd(&sql)
    }

    fn sqlcmd(&self, query: &str) -> Result<(), SqlServerError> {
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
            SqlServerError::CommandFailed(
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

fn find_on_path(executable: &str) -> Option<PathBuf> {
    let path = env::var_os("PATH")?;

    env::split_paths(&path)
        .map(|dir| dir.join(executable))
        .find(|candidate| candidate.is_file())
}

fn run_command_capture(command: &mut Command) -> Result<(), SqlServerError> {
    let output = command
        .output()
        .map_err(|source| SqlServerError::CommandSpawn {
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

    Err(SqlServerError::CommandFailed("command".to_owned(), message))
}

fn find_free_port() -> Result<u16, SqlServerError> {
    let listener = TcpListener::bind(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 0))
        .map_err(SqlServerError::PortBind)?;
    let port = listener
        .local_addr()
        .map_err(SqlServerError::PortBind)?
        .port();
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

fn validate_database_name(database: &str) -> Result<(), SqlServerError> {
    if database.is_empty() {
        return Err(SqlServerError::InvalidDatabaseName(
            "database name cannot be empty".to_owned(),
        ));
    }

    if database.len() > 128 {
        return Err(SqlServerError::InvalidDatabaseName(
            "database name cannot exceed 128 bytes".to_owned(),
        ));
    }

    if !database
        .chars()
        .all(|ch| ch.is_ascii_alphanumeric() || ch == '_')
    {
        return Err(SqlServerError::InvalidDatabaseName(
            "database name can only contain ASCII letters, digits, and underscores".to_owned(),
        ));
    }

    Ok(())
}

#[derive(Debug)]
pub(crate) enum SqlServerError {
    ContainerRuntimeNotFound,
    CommandSpawn {
        description: &'static str,
        source: std::io::Error,
    },
    CommandFailed(String, String),
    PortBind(std::io::Error),
    InvalidDatabaseName(String),
    ReadinessTimeout {
        seconds: u64,
        last_error: Option<String>,
    },
}

impl fmt::Display for SqlServerError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
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
            Self::CommandFailed(command, message) => write!(f, "{command} failed: {message}"),
            Self::PortBind(source) => write!(f, "failed to reserve a local port: {source}"),
            Self::InvalidDatabaseName(reason) => write!(f, "invalid database name: {reason}"),
            Self::ReadinessTimeout {
                seconds,
                last_error,
            } => {
                write!(f, "SQL Server did not become ready within {seconds}s")?;
                if let Some(last_error) = last_error {
                    write!(f, "; last error: {last_error}")?;
                }
                Ok(())
            }
        }
    }
}

pub(crate) const CONNECTION_STRING_ENV: &str = "ARROW_TIBERIUS_TEST_MSSQL_URL";
pub(crate) const TEST_DATABASE_ENV: &str = "ARROW_TIBERIUS_TEST_MSSQL_DATABASE";
pub(crate) const COMPATIBILITY_LEVEL_ENV: &str = "ARROW_TIBERIUS_TEST_MSSQL_COMPATIBILITY_LEVEL";
pub(crate) const CONTAINER_RUNTIME_ENV: &str = "ARROW_TIBERIUS_CONTAINER_RUNTIME";
const SQLSERVER_READY_TIMEOUT_SECS: u64 = 120;

#[cfg(test)]
mod tests {
    use super::{ManagedNetwork, SqlServerConnectionOptions, SqlServerContainer, SqlServerError};

    #[test]
    fn benchmark_default_uses_separate_database_from_integration_default() {
        let integration = SqlServerConnectionOptions::integration_default();
        let benchmark = SqlServerConnectionOptions::benchmark_default();

        assert_eq!(integration.database, "arrow_tiberius_integration");
        assert_eq!(benchmark.database, "arrow_tiberius_benchmark");
        assert_eq!(benchmark.image, integration.image);
        assert_eq!(benchmark.container_runtime, integration.container_runtime);
        assert_eq!(benchmark.connection_string, integration.connection_string);
        assert_eq!(benchmark.keep_container, integration.keep_container);
    }

    #[test]
    fn existing_connection_string_path_does_not_require_container_runtime() {
        let options = SqlServerConnectionOptions {
            container_runtime: Some("definitely-not-used".into()),
            connection_string: Some("server=tcp:127.0.0.1,1433;password=secret".to_owned()),
            image: "definitely-not-used".to_owned(),
            database: "bench_db".to_owned(),
            keep_container: true,
        };

        let connection = options.connect_or_start().unwrap();

        assert_eq!(connection.database, "bench_db");
        assert_eq!(
            connection.connection_string,
            "server=tcp:127.0.0.1,1433;password=secret"
        );
        assert_eq!(connection.container_connection_string, None);
    }

    #[test]
    fn managed_network_exposes_container_network_name() {
        let network = ManagedNetwork {
            runtime: "podman".into(),
            name: "arrow-tiberius-bench-network-test".to_owned(),
            keep_network: true,
        };

        assert_eq!(network.name(), "arrow-tiberius-bench-network-test");
    }

    #[test]
    fn container_connection_uses_container_name_and_default_sqlserver_port() {
        let container = SqlServerContainer {
            runtime: "podman".into(),
            name: "arrow-tiberius-sqlserver-test".to_owned(),
            password: "secret".to_owned(),
            host_port: 14331,
            keep_container: true,
        };

        assert_eq!(
            container.host_connection(),
            "server=tcp:127.0.0.1,14331;user id=sa;password=secret;TrustServerCertificate=true"
        );
        assert_eq!(
            container.container_connection(),
            "server=tcp:arrow-tiberius-sqlserver-test,1433;user id=sa;password=secret;TrustServerCertificate=true"
        );
    }

    #[test]
    fn rejects_database_names_that_cannot_be_safely_rendered() {
        for database in ["", "bad-name", "bad name", "bad]name"] {
            let err = super::validate_database_name(database).unwrap_err();

            assert!(matches!(err, SqlServerError::InvalidDatabaseName(_)));
        }
    }

    #[test]
    fn rejects_database_names_over_sql_server_identifier_limit() {
        let database = "a".repeat(129);
        let err = super::validate_database_name(&database).unwrap_err();

        assert!(matches!(err, SqlServerError::InvalidDatabaseName(_)));
    }

    #[test]
    fn accepts_database_names_that_can_be_used_for_managed_containers() {
        super::validate_database_name("arrow_tiberius_benchmark_2026").unwrap();
    }
}

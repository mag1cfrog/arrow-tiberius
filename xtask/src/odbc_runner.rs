use std::fmt;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};

pub(crate) const DEFAULT_RUNNER_IMAGE_TAG: &str = "arrow-tiberius-odbc-runner:local";
const RUNNER_DOCKERFILE: &str = "xtask/containers/odbc-runner/Dockerfile";

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct RunnerImageOptions {
    pub(crate) container_runtime: PathBuf,
    pub(crate) image_tag: String,
    pub(crate) manifest_dir: PathBuf,
}

impl RunnerImageOptions {
    pub(crate) fn dockerfile(&self) -> PathBuf {
        self.manifest_dir.join(RUNNER_DOCKERFILE)
    }

    pub(crate) fn build_context(&self) -> &Path {
        &self.manifest_dir
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct RunnerCommandOptions {
    pub(crate) container_runtime: PathBuf,
    pub(crate) image_tag: String,
    pub(crate) network: Option<String>,
    pub(crate) env: Vec<(String, String)>,
    pub(crate) workspace: Option<PathBuf>,
    pub(crate) workdir: Option<String>,
    pub(crate) args: Vec<String>,
}

impl RunnerCommandOptions {
    fn container_args(&self) -> Vec<String> {
        let mut args = vec![
            "run".to_owned(),
            "--rm".to_owned(),
            "--label".to_owned(),
            "org.arrow-tiberius.xtask=odbc-runner".to_owned(),
        ];

        if let Some(network) = &self.network {
            args.push("--network".to_owned());
            args.push(network.clone());
        }

        for (name, value) in &self.env {
            args.push("--env".to_owned());
            args.push(format!("{name}={value}"));
        }

        if let Some(workspace) = &self.workspace {
            args.push("--volume".to_owned());
            args.push(format!("{}:/workspace:Z", workspace.display()));
        }

        if let Some(workdir) = &self.workdir {
            args.push("--workdir".to_owned());
            args.push(workdir.clone());
        }

        args.push(self.image_tag.clone());
        args.extend(self.args.iter().cloned());
        args
    }
}

pub(crate) fn build_runner_image(options: &RunnerImageOptions) -> Result<(), OdbcRunnerError> {
    let mut command = Command::new(&options.container_runtime);
    command
        .arg("build")
        .arg("--file")
        .arg(options.dockerfile())
        .arg("--tag")
        .arg(&options.image_tag)
        .arg(options.build_context())
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit());

    let status = command
        .status()
        .map_err(|source| OdbcRunnerError::CommandSpawn {
            description: "build ODBC runner image",
            source,
        })?;

    if status.success() {
        Ok(())
    } else {
        Err(OdbcRunnerError::CommandStatus {
            description: "build ODBC runner image",
            status,
        })
    }
}

pub(crate) fn run_runner_command(options: &RunnerCommandOptions) -> Result<(), OdbcRunnerError> {
    let mut command = Command::new(&options.container_runtime);
    command
        .args(options.container_args())
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit());

    let status = command
        .status()
        .map_err(|source| OdbcRunnerError::CommandSpawn {
            description: "run ODBC runner command",
            source,
        })?;

    if status.success() {
        Ok(())
    } else {
        Err(OdbcRunnerError::CommandStatus {
            description: "run ODBC runner command",
            status,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct RunnerCommandOutput {
    pub(crate) stdout: String,
    pub(crate) stderr: String,
}

pub(crate) fn run_runner_command_capture(
    options: &RunnerCommandOptions,
) -> Result<RunnerCommandOutput, OdbcRunnerError> {
    let output = Command::new(&options.container_runtime)
        .args(options.container_args())
        .output()
        .map_err(|source| OdbcRunnerError::CommandSpawn {
            description: "run ODBC runner command",
            source,
        })?;

    let stdout = String::from_utf8_lossy(&output.stdout).into_owned();
    let stderr = String::from_utf8_lossy(&output.stderr).into_owned();

    if output.status.success() {
        Ok(RunnerCommandOutput { stdout, stderr })
    } else {
        print!("{stdout}");
        eprint!("{stderr}");
        Err(OdbcRunnerError::CommandStatus {
            description: "run ODBC runner command",
            status: output.status,
        })
    }
}

pub(crate) fn remove_runner_image(options: &RunnerImageOptions) -> Result<(), OdbcRunnerError> {
    let mut command = Command::new(&options.container_runtime);
    command
        .arg("image")
        .arg("rm")
        .arg("--force")
        .arg(&options.image_tag)
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit());

    let status = command
        .status()
        .map_err(|source| OdbcRunnerError::CommandSpawn {
            description: "remove ODBC runner image",
            source,
        })?;

    if status.success() {
        Ok(())
    } else {
        Err(OdbcRunnerError::CommandStatus {
            description: "remove ODBC runner image",
            status,
        })
    }
}

#[derive(Debug)]
pub(crate) struct ManagedRunnerImage {
    options: RunnerImageOptions,
    keep: bool,
    built: bool,
}

impl ManagedRunnerImage {
    pub(crate) fn build(options: RunnerImageOptions, keep: bool) -> Result<Self, OdbcRunnerError> {
        build_runner_image(&options)?;
        Ok(Self {
            options,
            keep,
            built: true,
        })
    }

    pub(crate) fn image_tag(&self) -> &str {
        &self.options.image_tag
    }

    pub(crate) fn command_options(
        &self,
        network: Option<String>,
        env: Vec<(String, String)>,
        workspace: Option<PathBuf>,
        workdir: Option<String>,
        args: Vec<String>,
    ) -> RunnerCommandOptions {
        RunnerCommandOptions {
            container_runtime: self.options.container_runtime.clone(),
            image_tag: self.options.image_tag.clone(),
            network,
            env,
            workspace,
            workdir,
            args,
        }
    }

    pub(crate) fn cleanup(&mut self) -> Result<(), OdbcRunnerError> {
        if self.keep || !self.built {
            return Ok(());
        }

        remove_runner_image(&self.options)?;
        self.built = false;
        Ok(())
    }
}

impl Drop for ManagedRunnerImage {
    fn drop(&mut self) {
        if let Err(error) = self.cleanup() {
            eprintln!("warning: failed to clean up ODBC runner image: {error}");
        }
    }
}

#[derive(Debug)]
pub(crate) enum OdbcRunnerError {
    CommandSpawn {
        description: &'static str,
        source: std::io::Error,
    },
    CommandStatus {
        description: &'static str,
        status: std::process::ExitStatus,
    },
}

impl fmt::Display for OdbcRunnerError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::CommandSpawn {
                description,
                source,
            } => write!(f, "failed to {description}: {source}"),
            Self::CommandStatus {
                description,
                status,
            } => write!(f, "{description} failed with {status}"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{
        DEFAULT_RUNNER_IMAGE_TAG, RUNNER_DOCKERFILE, RunnerCommandOptions, RunnerImageOptions,
    };

    #[test]
    fn runner_image_options_resolve_dockerfile_and_context() {
        let options = RunnerImageOptions {
            container_runtime: "podman".into(),
            image_tag: DEFAULT_RUNNER_IMAGE_TAG.to_owned(),
            manifest_dir: "/workspace/arrow-tiberius".into(),
        };

        assert_eq!(
            options.dockerfile(),
            std::path::PathBuf::from("/workspace/arrow-tiberius").join(RUNNER_DOCKERFILE)
        );
        assert_eq!(
            options.build_context(),
            std::path::Path::new("/workspace/arrow-tiberius")
        );
    }

    #[test]
    fn runner_command_options_build_container_args_without_network() {
        let options = RunnerCommandOptions {
            container_runtime: "podman".into(),
            image_tag: DEFAULT_RUNNER_IMAGE_TAG.to_owned(),
            network: None,
            env: Vec::new(),
            workspace: None,
            workdir: None,
            args: vec!["odbcinst".to_owned(), "-q".to_owned(), "-d".to_owned()],
        };

        assert_eq!(
            options.container_args(),
            [
                "run",
                "--rm",
                "--label",
                "org.arrow-tiberius.xtask=odbc-runner",
                DEFAULT_RUNNER_IMAGE_TAG,
                "odbcinst",
                "-q",
                "-d",
            ]
        );
    }

    #[test]
    fn runner_command_options_build_container_args_with_network() {
        let options = RunnerCommandOptions {
            container_runtime: "podman".into(),
            image_tag: DEFAULT_RUNNER_IMAGE_TAG.to_owned(),
            network: Some("bench-network".to_owned()),
            env: Vec::new(),
            workspace: None,
            workdir: None,
            args: vec!["cargo".to_owned(), "test".to_owned()],
        };

        assert_eq!(
            options.container_args(),
            [
                "run",
                "--rm",
                "--label",
                "org.arrow-tiberius.xtask=odbc-runner",
                "--network",
                "bench-network",
                DEFAULT_RUNNER_IMAGE_TAG,
                "cargo",
                "test",
            ]
        );
    }

    #[test]
    fn runner_command_options_build_container_args_with_env_and_workspace() {
        let options = RunnerCommandOptions {
            container_runtime: "podman".into(),
            image_tag: DEFAULT_RUNNER_IMAGE_TAG.to_owned(),
            network: None,
            env: vec![
                (
                    "ARROW_TIBERIUS_BENCH_DATABASE".to_owned(),
                    "bench".to_owned(),
                ),
                (
                    "ARROW_TIBERIUS_BENCH_CONNECTION_STRING".to_owned(),
                    "server=tcp:sqlserver,1433".to_owned(),
                ),
            ],
            workspace: Some("/home/hanbo/repo/arrow-tiberius".into()),
            workdir: Some("/workspace".to_owned()),
            args: vec!["cargo".to_owned(), "metadata".to_owned()],
        };

        assert_eq!(
            options.container_args(),
            [
                "run",
                "--rm",
                "--label",
                "org.arrow-tiberius.xtask=odbc-runner",
                "--env",
                "ARROW_TIBERIUS_BENCH_DATABASE=bench",
                "--env",
                "ARROW_TIBERIUS_BENCH_CONNECTION_STRING=server=tcp:sqlserver,1433",
                "--volume",
                "/home/hanbo/repo/arrow-tiberius:/workspace:Z",
                "--workdir",
                "/workspace",
                DEFAULT_RUNNER_IMAGE_TAG,
                "cargo",
                "metadata",
            ]
        );
    }
}

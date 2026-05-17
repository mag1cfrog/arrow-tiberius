use std::fmt;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};

pub(crate) const DEFAULT_RUNNER_IMAGE_TAG: &str = "arrow-tiberius-arrow-odbc-runner:local";
const RUNNER_DOCKERFILE: &str = "xtask/containers/arrow-odbc-runner/Dockerfile";

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
            description: "build arrow-odbc runner image",
            source,
        })?;

    if status.success() {
        Ok(())
    } else {
        Err(OdbcRunnerError::CommandStatus {
            description: "build arrow-odbc runner image",
            status,
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
            description: "remove arrow-odbc runner image",
            source,
        })?;

    if status.success() {
        Ok(())
    } else {
        Err(OdbcRunnerError::CommandStatus {
            description: "remove arrow-odbc runner image",
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
            eprintln!("warning: failed to clean up arrow-odbc runner image: {error}");
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
    use super::{DEFAULT_RUNNER_IMAGE_TAG, RUNNER_DOCKERFILE, RunnerImageOptions};

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
}

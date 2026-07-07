# Integration Tests

Default validation does not require SQL Server:

```bash
cargo fmt --check
cargo clippy --all-targets --all-features -- -D warnings
cargo test
```

SQL Server validation is opt-in through the xtask runner:

```bash
cargo xtask sqlserver-test
cargo xtask sqlserver-compat-probe
```

`sqlserver-test` is the normal broad integration test path. The xtask runner
starts a local SQL Server container, waits for readiness, creates the test
database, sets compatibility level 100, runs the feature-gated integration
tests, and removes the container when the command exits.

`sqlserver-compat-probe` is the focused compatibility probe path. It runs a
small SQL Server behavior probe for one engine version and one database
compatibility level, using SQL Server itself to derive expected values for
behavior that can vary by compatibility level.

Plain `cargo test` must remain independent of SQL Server.

## Container Runtime

The runner supports Docker-compatible runtimes such as Docker and Podman. Runtime selection uses:

1. `--container-runtime`
2. `ARROW_TIBERIUS_CONTAINER_RUNTIME`
3. `docker` on `PATH`
4. `podman` on `PATH`

Examples:

```bash
cargo xtask sqlserver-test --container-runtime podman
ARROW_TIBERIUS_CONTAINER_RUNTIME=podman cargo xtask sqlserver-test
```

Shell aliases such as `alias docker=podman` are not used by the Rust process. Use the flag or environment variable when you need a specific runtime.

## Defaults

The default image is:

```text
mcr.microsoft.com/mssql/server:2017-latest
```

The default database is:

```text
arrow_tiberius_integration
```

SQL Server is configured with:

```sql
ALTER DATABASE [arrow_tiberius_integration] SET COMPATIBILITY_LEVEL = 100;
```

This gives local coverage for SQL Server 2017 running at database compatibility
level 100, matching the profile used by the broad integration harness.

## Compatibility Probe

Run a focused probe for a specific SQL Server image, engine version, and
database compatibility level:

```bash
cargo xtask sqlserver-compat-probe \
  --image mcr.microsoft.com/mssql/server:2022-latest \
  --version 2022 \
  --compatibility-level 160
```

Supported `--version` values are `2017`, `2019`, `2022`, and `2025`.
Use `--compatibility-level` to select the database compatibility level to set
before the probe runs.

## Existing SQL Server

To use an existing SQL Server instead of a local container:

```bash
cargo xtask sqlserver-test \
  --connection-string 'server=tcp:127.0.0.1,1433;user id=sa;password=...;TrustServerCertificate=true' \
  --database arrow_tiberius_integration
```

The xtask runner passes these environment variables to the feature-gated integration tests:

```text
ARROW_TIBERIUS_TEST_MSSQL_URL
ARROW_TIBERIUS_TEST_MSSQL_DATABASE
ARROW_TIBERIUS_TEST_MSSQL_COMPATIBILITY_LEVEL
ARROW_TIBERIUS_TEST_MSSQL_VERSION
```

The lower-level command is only for CI/debugging when SQL Server is already configured and those environment variables are set:

```bash
cargo test --features integration-tests
```

Prefer `cargo xtask sqlserver-test` for local development.

## CI

GitHub Actions runs the same xtask entrypoints in `.github/workflows/ci.yml`.
The fast Rust job runs formatting, clippy, and normal workspace tests first.
The broad SQL Server job then runs:

```bash
cargo xtask sqlserver-test
```

The compatibility probe job runs one CI job per SQL Server version and loops
over the selected compatibility levels for that version:

```bash
cargo xtask sqlserver-compat-probe \
  --image <version image> \
  --version <year> \
  --compatibility-level <level>
```

CI currently probes SQL Server 2017, 2019, 2022, and 2025 images. The SQL
Server jobs assume a Linux runner with Docker available and network access to
pull the configured `mcr.microsoft.com/mssql/server` images. Container startup,
readiness, database creation, compatibility-level setup, test execution, and
cleanup remain owned by the xtask harness.

## Debugging

Keep the container after a failed run:

```bash
cargo xtask sqlserver-test --keep-container
```

Change the SQL Server image:

```bash
cargo xtask sqlserver-test --image mcr.microsoft.com/mssql/server:2017-latest
cargo xtask sqlserver-compat-probe --image mcr.microsoft.com/mssql/server:2022-latest --version 2022 --compatibility-level 160
```

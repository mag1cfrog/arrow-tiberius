# arrow-tiberius

[![Crates.io](https://img.shields.io/crates/v/arrow-tiberius.svg)](https://crates.io/crates/arrow-tiberius)
[![Docs.rs](https://docs.rs/arrow-tiberius/badge.svg)](https://docs.rs/arrow-tiberius)
[![License](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](LICENSE)

`arrow-tiberius` bridges Apache Arrow and Microsoft SQL Server through the
Tiberius TDS driver.

The v0.1 API focuses on Arrow-to-SQL Server writes:

- plan SQL Server-compatible schemas from Arrow schemas,
- render deterministic `CREATE TABLE` SQL,
- report unsupported mappings as structured diagnostics,
- write Arrow `RecordBatch` values with a selectable SQL Server bulk writer,
- emit sanitized writer and protocol tracing through `tracing`.

SQL Server-to-Arrow reads are reserved for a later release.

## Install

```toml
[dependencies]
arrow-tiberius = "0.1"
```

## Quick Start

Plan an Arrow schema and render SQL Server DDL:

```rust
use arrow_schema::{DataType, Field, Schema};
use arrow_tiberius::{
    MssqlProfile, PlanOptions, TableName, create_table_sql_from_mappings,
};

fn main() -> arrow_tiberius::Result<()> {
    let schema = Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, true),
    ]);

    let profile = MssqlProfile::sql_server_2016_compat_100();
    let outcome = profile.plan_arrow_schema(&schema, PlanOptions::default())?;

    let table = TableName::new("dbo", "people")?;
    let ddl = create_table_sql_from_mappings(&table, outcome.mappings());

    assert!(ddl.contains("CREATE TABLE [dbo].[people]"));
    Ok(())
}
```

Write a batch to an existing SQL Server table:

```rust
use arrow_array::RecordBatch;
use arrow_tiberius::{
    MssqlProfile, PlanOptions, TableName, WriteBackend, WriteOptions,
    connect_mssql_client_from_ado_string,
};

async fn write_batch(
    connection_string: &str,
    batch: &RecordBatch,
) -> arrow_tiberius::Result<()> {
    let mut client = connect_mssql_client_from_ado_string(connection_string).await?;
    let profile = MssqlProfile::sql_server_2016_compat_100();
    let planned_schema = profile
        .plan_arrow_schema(batch.schema().as_ref(), PlanOptions::default())?
        .into_value();

    let table = TableName::new("dbo", "people")?;
    let mut writer = client
        .bulk_writer(
            table,
            planned_schema,
            WriteOptions {
                backend: WriteBackend::DirectRawBulk,
                ..WriteOptions::default()
            },
        )
        .await?;

    writer.write_batch(batch).await?;
    writer.finish().await?;
    Ok(())
}
```

The connected writer validates target table metadata before sending rows. It
does not create the target table automatically; callers can use the DDL helper
when they want this crate to produce the table definition.

## Diagnostics

Planning and write failures return structured diagnostics instead of requiring
string parsing. Diagnostics include severity, machine-readable code, field
context, row context when available, and message text.

For user-facing write failure reports, use `Error::safe_error_info()`. It
exposes the write phase, inner error kind, sanitized summary, diagnostic codes,
and structured diagnostics when available while keeping dependency source text
out of default reports.

For the complete planning surface, see
[Arrow to SQL Server Type Mapping](docs/type-mapping.md).

## Writer Backends

`WriteBackend` controls how planned Arrow rows are sent to SQL Server:

| Backend | Purpose |
| --- | --- |
| `Auto` | Default selection. Currently resolves to `DirectRawBulk`. |
| `BaselineTokenRow` | Compatibility path using Tiberius `TokenRow` bulk load. |
| `DirectFramedBulk` | Direct Arrow-to-TDS row encoding through Tiberius framed writes. |
| `DirectRawBulk` | Optimized direct encoder plus raw bulk packet writes from the Tiberius fork. |

The direct raw backend is the optimized production path for currently supported
mappings. The baseline backend remains useful for compatibility checks and
parity tests.

## Observability

`arrow-tiberius` emits structured spans and events through `tracing` for schema
planning, writer initialization, batch writes, direct raw backend summaries,
and writer finish. It never installs a subscriber.

Its `tiberius-raw-bulk` dependency also emits sanitized protocol tracing under
the `tiberius_raw_bulk::protocol` target. Those protocol events are emitted
inside active `arrow-tiberius` writer spans during connect, bulk-load, and
finish operations.

See [Observability](docs/observability.md) for subscriber setup, span and event
names, safe field categories, redaction guarantees, and workflow integration.

## Examples

Compile-checked examples that do not require SQL Server:

```bash
cargo run --example schema_to_ddl
cargo run --example planning_diagnostics
cargo run --example backend_selection
cargo run --example policy_dependent_planning
```

SQL Server write example:

```bash
ARROW_TIBERIUS_EXAMPLE_MSSQL_URL='server=tcp:localhost,1433;user=sa;password=...;TrustServerCertificate=true' \
  cargo run --example sqlserver_batch_write
```

By default, the SQL Server example creates, writes to, and drops
`[dbo].[arrow_tiberius_example_write]`.

## Compatibility

Choose the `MssqlProfile` that matches the SQL Server version and database
compatibility level you plan to write against. The original v0.1 profile remains
available:

```rust
use arrow_tiberius::MssqlProfile;

let profile = MssqlProfile::sql_server_2016_compat_100();
```

This release also models SQL Server 2017 database compatibility levels 100,
110, 120, 130, and 140, including the SQL Server 2017 compatibility-level-100
target used by the integration harness.

`arrow-tiberius` depends on the published `tiberius-raw-bulk` package as the
crate name `tiberius` and owns that compatibility boundary internally:

```toml
tiberius = { package = "tiberius-raw-bulk", version = "=0.12.3-raw-bulk.14", default-features = false, features = [
    "tds73",
    "winauth",
    "native-tls",
] }
```

Downstream crates should normally depend only on `arrow-tiberius` and construct
SQL Server clients through `connect_mssql_client_from_ado_string` or
`ConnectedMssqlClient`.

## Feature Flags

| Feature | Default | Purpose |
| --- | --- | --- |
| `bench-profile` | no | Enables benchmark-only direct write profiling hooks and forwards to `tiberius/bulk-load-profile`. |
| `integration-tests` | no | Enables SQL Server integration tests that require explicit environment setup or the xtask runner. |

## Validation

Default local validation does not require SQL Server:

```bash
cargo fmt --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace
```

Run SQL Server integration tests through the xtask harness:

```bash
cargo xtask sqlserver-test
```

## Documentation

See [Documentation Index](docs/README.md) for the maintained user and maintainer
docs.

# arrow-tiberius

[![Crates.io](https://img.shields.io/crates/v/arrow-tiberius.svg)](https://crates.io/crates/arrow-tiberius)
[![Docs.rs](https://docs.rs/arrow-tiberius/badge.svg)](https://docs.rs/arrow-tiberius)
[![License](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](LICENSE)

`arrow-tiberius` is a Rust library for bridging Apache Arrow and Microsoft SQL
Server through the [Tiberius TDS driver](https://github.com/prisma/tiberius).

The crate is designed around a bidirectional boundary:

```text
Arrow Schema + RecordBatch values
    -> SQL Server write plan and DDL
    -> SQL Server bulk load through Tiberius

SQL Server metadata and rows through Tiberius
    -> Arrow schema and RecordBatch values
```

The v0.1 release implements the Arrow-to-SQL Server write path first. The public
API is still intentionally shaped around Arrow, SQL Server profiles, structured
diagnostics, and directional modules so a SQL Server-to-Arrow read path can be
added without renaming the crate or replacing the core model.

> [!NOTE]
> v0.1 implements the Arrow-to-SQL Server direction only. SQL Server-to-Arrow
> reading is reserved for a later release.

## Scope

In v0.1, `arrow-tiberius` provides:

- Arrow-to-SQL Server schema planning.
- SQL Server identifiers, type metadata, compatibility profile, and DDL helpers.
- Structured planning and runtime diagnostics.
- Arrow `RecordBatch` bulk writing through Tiberius.
- Baseline and optimized writer backend selection.
- SQL Server integration tests and writer benchmark harnesses.

It does not provide SQL Server-to-Arrow reads yet.

## Quick Start

Add the crate:

```toml
[dependencies]
arrow-tiberius = "0.1"
```

Plan an Arrow schema and render deterministic `CREATE TABLE` SQL:

```rust
use arrow_schema::{DataType, Field, Schema};
use arrow_tiberius::{
    MssqlProfile, PlanOptions, TableName, create_table_sql_from_mappings,
    plan_arrow_schema_to_mssql_mappings,
};

fn main() -> arrow_tiberius::Result<()> {
    let schema = Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, true),
    ]);

    let outcome = plan_arrow_schema_to_mssql_mappings(
        &schema,
        MssqlProfile::sql_server_2016_compat_100(),
        PlanOptions::default(),
    )?;

    let table = TableName::new("dbo", "people")?;
    let ddl = create_table_sql_from_mappings(&table, outcome.value());

    assert!(ddl.contains("CREATE TABLE [dbo].[people]"));
    Ok(())
}
```

Write batches to an existing SQL Server table with a crate-owned compatible
SQL Server client:

```rust
use arrow_array::RecordBatch;
use arrow_tiberius::{
    MssqlProfile, PlanOptions, TableName, WriteBackend, WriteOptions,
    connect_mssql_client_from_ado_string, plan_arrow_schema_to_mssql_mappings,
};

async fn write_batch(
    connection_string: &str,
    batch: &RecordBatch,
) -> arrow_tiberius::Result<()> {
    let mut client = connect_mssql_client_from_ado_string(connection_string).await?;
    let outcome = plan_arrow_schema_to_mssql_mappings(
        batch.schema().as_ref(),
        MssqlProfile::sql_server_2016_compat_100(),
        PlanOptions::default(),
    )?;

    let table = TableName::new("dbo", "people")?;
    let mut writer = client
        .bulk_writer(
            table,
            outcome.value().to_vec(),
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

The connected writer validates the target table metadata before sending rows.
It does not create the target table automatically; callers can use the DDL
helpers when they want this crate to produce the table definition.

## Diagnostics

Planning and write failures return structured diagnostics instead of relying on
string parsing. Callers can inspect severity, machine-readable code, field, row,
and message.

```rust
use arrow_schema::{DataType, Field, Schema};
use arrow_tiberius::{
    Error, MssqlProfile, PlanOptions, plan_arrow_schema_to_mssql_mappings,
};

let schema = Schema::new(vec![Field::new("raw", DataType::UInt64, false)]);
let err = plan_arrow_schema_to_mssql_mappings(
    &schema,
    MssqlProfile::sql_server_2016_compat_100(),
    PlanOptions::default(),
)
.expect_err("UInt64 requires an explicit policy by default");

if let Error::Planning { diagnostics } = err {
    for diagnostic in diagnostics.all() {
        println!("{:?}: {}", diagnostic.code(), diagnostic.message());
    }
}
```

See [Arrow to SQL Server Type Mapping](docs/type-mapping.md) for the full
supported and unsupported mapping surface.

## Observability

`arrow-tiberius` emits structured spans and events through `tracing` for schema
planning, writer initialization, batch writes, direct raw backend summaries,
and writer finish. It does not install a global subscriber; applications decide
how tracing is filtered, formatted, exported, or attached to workflow-level
spans.

Its `tiberius-raw-bulk` dependency also emits sanitized protocol tracing under
the `tiberius_raw_bulk::protocol` target. Those protocol events are emitted
inside the active `arrow-tiberius` writer spans during connect, bulk-load, and
finish operations, so applications can see writer lifecycle telemetry and
low-level SQL Server client/TDS telemetry with one subscriber.

The tracing contract is intentionally sanitized. It includes backend names,
validated identifiers, counts, phase names, diagnostic codes, and elapsed
durations. It does not emit connection strings, passwords, row values, raw
packet bytes, or arbitrary SQL text.

See [Observability](docs/observability.md) for subscriber setup, span and event
names, safe field categories, redaction guarantees, and downstream workflow
integration guidance.

## Writer Backends

`WriteBackend` controls how planned Arrow rows are sent to SQL Server:

| Backend | Purpose |
| --- | --- |
| `Auto` | Default selection. Currently resolves to `DirectRawBulk`. |
| `BaselineTokenRow` | Compatibility and reference path using Tiberius `TokenRow` bulk load. |
| `DirectFramedBulk` | Direct Arrow-to-TDS row encoding through Tiberius framed writes. |
| `DirectRawBulk` | Optimized direct encoder plus raw bulk packet writes from the Tiberius fork. |

The direct raw backend is the optimized production path for currently supported
mappings. The baseline backend remains useful for compatibility checks,
debugging, and parity tests.

## Examples

Compile-checked examples are available under `examples/` and do not require SQL
Server:

```bash
cargo run --example schema_to_ddl
cargo run --example planning_diagnostics
cargo run --example backend_selection
cargo run --example policy_dependent_planning
```

The examples cover [schema to DDL](examples/schema_to_ddl.rs),
[planning diagnostics](examples/planning_diagnostics.rs),
[backend selection](examples/backend_selection.rs), and
[policy-dependent planning](examples/policy_dependent_planning.rs).

An environment-gated SQL Server write example is also available:

```bash
ARROW_TIBERIUS_EXAMPLE_MSSQL_URL='server=tcp:localhost,1433;user=sa;password=...;TrustServerCertificate=true' \
  cargo run --example sqlserver_batch_write
```

By default it creates, writes to, and drops `[dbo].[arrow_tiberius_example_write]`.
Set `ARROW_TIBERIUS_EXAMPLE_KEEP_TABLE=1` to keep the disposable table, or set
`ARROW_TIBERIUS_EXAMPLE_MSSQL_SCHEMA`, `ARROW_TIBERIUS_EXAMPLE_MSSQL_TABLE`,
and `ARROW_TIBERIUS_EXAMPLE_EXISTING_TABLE=1` to write to an existing table
explicitly.

## SQL Server Compatibility

The v0.1 profile targets SQL Server 2016 with database compatibility level 100:

```rust
use arrow_tiberius::MssqlProfile;

let profile = MssqlProfile::sql_server_2016_compat_100();
```

See [Integration Tests](docs/integration-tests.md) for the SQL Server validation
path used by this repository.

## Tiberius Dependency Model

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
SQL Server clients through the connected-client API:

```toml
[dependencies]
arrow-tiberius = "0.1"
```

Use `connect_mssql_client_from_ado_string`, `ConnectedMssqlClient`, and
`ConnectedBulkWriter` when lifecycle SQL and bulk loading must run on the same
connection. Depending on upstream `tiberius` separately creates a distinct crate
type. A client from upstream `tiberius` is not the same type as a client from
`tiberius-raw-bulk` and will not match this crate's writer internals.

The fork exists because upstream Tiberius does not expose the raw bulk packet
APIs needed by the optimized direct writer. The baseline writer and direct
writer use the same forked package dependency; only the optimized backend calls
the raw-row APIs.

## Feature Flags

| Feature | Default | Purpose |
| --- | --- | --- |
| `bench-profile` | no | Enables benchmark-only direct write profiling hooks and forwards to `tiberius/bulk-load-profile`. |
| `integration-tests` | no | Enables SQL Server integration tests that require explicit environment setup or the xtask runner. |

Docs.rs is configured to build with all features so feature-gated public items
are documented. Normal library use does not require either feature.

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

The harness starts SQL Server when possible, configures compatibility level 100,
runs feature-gated integration tests, and cleans up managed resources. See
[Integration Tests](docs/integration-tests.md) for container runtime and
existing-server options.

Writer benchmark commands and interpretation guidance are in
[Writer Benchmarks](docs/benchmarks.md). The curated direct raw benchmark
summary is in [Direct Raw Benchmark Comparison](docs/direct-raw-benchmark-comparison.md).

## Related Crates

[`arrow-odbc`](https://docs.rs/arrow-odbc/) is the broader Arrow/ODBC crate. It
targets ODBC data sources generally and supports reading and writing Arrow
arrays through ODBC drivers. Use it when you need a database-agnostic ODBC path
or SQL-to-Arrow reads today.

`arrow-tiberius` is narrower: it targets Microsoft SQL Server through Tiberius
and focuses v0.1 on Arrow-to-SQL Server bulk writes. That narrower scope lets
the direct raw backend use SQL Server-specific TDS bulk-load encoding instead of
going through ODBC.

For the SQL Server write workloads this crate is built around, the local
benchmark data generally favors `DirectRawBulk`: it is much faster than
`arrow-odbc` on primitive and mixed nullable rows while using far less memory.
Representative runs show about 3.05x throughput on primitive numeric rows
with about 20 MiB peak RSS versus about 998 MiB, and about 1.66x throughput on
mixed nullable rows with about 21 MiB peak RSS versus about 157 MiB. The main
exception is some large variable-width text/binary workloads, where `arrow-odbc`
can write about 1.28x to 1.37x faster but with roughly 1.4 GiB peak RSS versus
about 100 MiB for `DirectRawBulk`. See
[primitive direct raw comparison](docs/benchmark-results/2026-05-19-primitive-direct-raw-compare.md)
and
[variable-width direct raw comparison](docs/benchmark-results/2026-05-19-variable-width-direct-raw-compare.md)
for the measured numbers and setup.

## Project Status

`arrow-tiberius` is preparing its first v0.1 release. The v0.1 release focus is
Arrow-to-SQL Server writing. SQL Server-to-Arrow reading is reserved for a later
release.

See [v0.1 Release Boundary](docs/release-v0.1.md) for the maintainer release
scope, gates, and publication checklist.

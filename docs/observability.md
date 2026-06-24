# Observability

This document describes the `arrow-tiberius` tracing contract for applications
that plan Arrow schemas, write Arrow `RecordBatch` values to SQL Server, or wrap
those writes inside a higher-level workflow.

`arrow-tiberius` emits spans and events through the [`tracing`] crate. It does
not install a global subscriber, choose an output sink, configure filters, or
send telemetry to a vendor. Applications own subscriber setup and decide where
events go.

[`tracing`]: https://docs.rs/tracing/

## Subscriber Ownership

Library code only emits telemetry. It is silent unless the application has a
subscriber installed by the time `arrow-tiberius` code runs.

For a small application or test, one possible setup is:

```toml
[dependencies]
tracing = "0.1"
tracing-subscriber = { version = "0.3", features = ["env-filter", "fmt"] }
```

```rust
use tracing_subscriber::EnvFilter;

fn init_tracing_for_example() {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::new(
            "info,arrow_tiberius=debug,tiberius_raw_bulk::protocol=info",
        ))
        .init();
}
```

Production applications should choose their own subscriber, filter policy, and
export pipeline. For example, a service might send `tracing` data through an
OpenTelemetry layer to Grafana, Datadog, or another backend. That setup belongs
to the application, not to `arrow-tiberius`.

## Target And Levels

All crate-owned instrumentation uses the tracing target:

```text
arrow_tiberius
```

The `tiberius-raw-bulk` dependency emits sanitized SQL Server client and TDS
protocol telemetry under:

```text
tiberius_raw_bulk::protocol
```

That protocol target covers lower-level phases such as connection setup, TLS
negotiation, login, bulk-load protocol operations, packet write summaries,
server token summaries, and SQL Browser named-instance resolution. It is emitted
by `tiberius-raw-bulk`, not re-wrapped by `arrow-tiberius`.

Recommended filters:

| Level | Meaning |
| --- | --- |
| `info` | Lifecycle summaries such as schema planning, writer initialization, batch writes, and finish. |
| `debug` | Direct raw backend summaries such as measured encoded bytes and planned row ranges. |
| `error` | Sanitized failures with phase, summary, and diagnostic codes. |
| `trace` | Reserved for future high-volume details. The current writer path does not require trace-level events. |

The crate intentionally avoids per-row logs and raw payload logs.

## Span And Event Contract

The table below lists the current tracing vocabulary emitted by the
Arrow-to-SQL Server write path. Names are useful for downstream filters,
dashboards, and tests, but they are not exposed as public Rust constants.

| Area | Span | Events | Level |
| --- | --- | --- | --- |
| Schema planning | `arrow_tiberius.schema_planning` | `arrow_tiberius.schema_planning.started`, `arrow_tiberius.schema_planning.completed`, `arrow_tiberius.schema_planning.failed` | `info`, `error` |
| Writer initialization | `arrow_tiberius.writer_initialization` | `arrow_tiberius.writer_initialization.started`, `arrow_tiberius.writer_initialization.completed`, `arrow_tiberius.writer_initialization.failed` | `info`, `error` |
| Target metadata validation | `arrow_tiberius.writer_initialization` | `arrow_tiberius.target_metadata_validation.started`, `arrow_tiberius.target_metadata_validation.completed`, `arrow_tiberius.target_metadata_validation.failed` | `info`, `error` |
| Batch write | `arrow_tiberius.batch_write` | `arrow_tiberius.batch_write.started`, `arrow_tiberius.batch_write.completed`, `arrow_tiberius.batch_write.failed` | `info`, `error` |
| Direct raw encoding | `arrow_tiberius.batch_write` | `arrow_tiberius.direct_raw.measured`, `arrow_tiberius.direct_raw.ranges_planned`, `arrow_tiberius.direct_raw.failed` | `debug`, `error` |
| Direct raw packet write | `arrow_tiberius.batch_write` | `arrow_tiberius.direct_raw.packet_write.completed`, `arrow_tiberius.direct_raw.failed` | `debug`, `error` |
| Finish | `arrow_tiberius.finish` | `arrow_tiberius.finish.started`, `arrow_tiberius.finish.completed`, `arrow_tiberius.finish.failed` | `info`, `error` |

Direct raw encoding and packet write events are emitted only for
`WriteBackend::DirectRawBulk`. Baseline and framed direct writes still emit the
shared writer initialization, batch write, and finish spans.

## Field Contract

Fields are structured for filtering and aggregation. The exact set depends on
the phase and event.

Safe field categories:

| Category | Examples |
| --- | --- |
| Phase and event identity | `phase`, `telemetry_event` |
| Backend names | `requested_backend`, `resolved_backend`, `backend` |
| Validated SQL Server identifiers | `target_schema`, `target_table` |
| Counts | `arrow_field_count`, `planned_column_count`, `batch_row_count`, `batch_column_count`, `accepted_rows_after`, `batches_written` |
| Direct raw summaries | `encoded_row_start`, `encoded_row_count`, `encoded_byte_count`, `encoded_range_count` |
| Diagnostic summaries | `diagnostic_count`, `error_diagnostic_count`, `warning_diagnostic_count`, `diagnostic_codes`, `error_summary` |
| Timing summaries | `elapsed_us` |
| Planning policy names | `string_policy`, `binary_policy`, `timezone_policy`, `nanosecond_policy`, `uint64_policy`, `decimal_policy`, `decimal256_policy`, `float_policy`, `date64_policy` |

Schema planning also emits data type family summaries such as
`arrow_data_type_families` and `mssql_type_families`. Diagnostic summaries may
include `diagnostic_field_names`. Field and table names are identifiers, not
row values, but they are log-visible. Do not use secret material as schema,
table, or column names.

## Redaction Contract

`arrow-tiberius` tracing does not emit:

- SQL Server connection strings.
- Passwords, access tokens, or authentication material.
- Row values.
- Raw packet bytes or full encoded row payload bytes.
- Raw dependency debug output.
- Arbitrary SQL text.
- Diagnostic messages that may include detailed source text.

Failure events use sanitized `error_summary` values and machine-readable
`diagnostic_codes`. Detailed diagnostics remain available from returned
`Error` values for callers that need to inspect them in process.

## Downstream Workflow Spans

Applications should add workflow, source, and output context outside
`arrow-tiberius`. The crate cannot know job ids, source aliases, retry ids,
or orchestrator output names.

Use a parent span around writer calls:

```rust
use tracing::Instrument as _;

let output_span = tracing::info_span!(
    target: "my_app",
    "my_app.output_write",
    workflow_name = "daily_load",
    output_name = "people",
);

async {
    writer.write_batch(batch).await?;
    writer.finish().await
}
.instrument(output_span)
.await?;
```

With a subscriber installed, the `arrow-tiberius` spans are emitted inside the
application span. This lets downstream systems group crate-owned writer details
under the application's workflow or output context without duplicating writer
internals.

During writer operations, `tiberius-raw-bulk` protocol events are emitted under
the active `arrow-tiberius` writer spans. A typical write trace can therefore
look like:

```text
my_app.output_write
  -> arrow_tiberius.writer_initialization
    -> protocol.connection.connect
    -> protocol.tls.negotiation
    -> protocol.login.flow
  -> arrow_tiberius.batch_write
    -> protocol.bulk_load.request
    -> protocol.bulk_load.packet.written
  -> arrow_tiberius.finish
    -> protocol.token.done
```

Collectors should keep the targets distinct: `arrow_tiberius` describes Arrow
schema planning and writer lifecycle semantics, while
`tiberius_raw_bulk::protocol` describes SQL Server client and TDS protocol
semantics.

When a workflow writes multiple outputs, create one parent span per output or
per logical workflow step. Keep source-specific context in the application span
or its fields rather than expecting `arrow-tiberius` to infer it.

## Known Gaps

- `arrow-tiberius` does not install a subscriber or exporter.
- `arrow-tiberius` does not emit row-level telemetry.
- `arrow-tiberius` does not emit raw packet bytes, raw row payload bytes, or
  arbitrary SQL text.
- Direct raw writer events report safe row and byte summaries. Lower-level
  packet summaries are emitted by `tiberius-raw-bulk` protocol tracing.
- SQL Server engine behavior is outside both client libraries. Server-side
  execution, waits, locks, IO, and query plans require SQL Server DMVs,
  Extended Events, Query Store, or separate profiling queries.
- Workflow ids, output names, source aliases, retries, transactions, and
  orchestration status must be supplied by downstream applications.
- The `bench-profile` feature exposes benchmark-only profiling hooks. It is
  separate from the normal tracing contract and is not required for production
  observability.

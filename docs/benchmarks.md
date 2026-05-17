# Writer Benchmarks

The writer benchmark harness lives under `cargo xtask writer-bench`. It is for
SQL Server write-path comparisons only. It does not benchmark reads, exports,
object storage, or general database query performance.

Benchmark results are local to the machine, container runtime, SQL Server image,
network path, row count, batch size, and scenario used for the run. Do not check
machine-specific result files into the repo, and do not treat one local run as a
general claim that one backend is always faster than another.

## Prerequisites

- Rust toolchain for this workspace.
- A container runtime such as `podman` or `docker`, or an existing SQL Server
  connection string.
- For `arrow-odbc`, the managed runner image contains unixODBC, Microsoft ODBC
  Driver 18 for SQL Server, and Rust. The normal xtask path does not require the
  host to have unixODBC development libraries installed.

The examples below use `podman`. Replace it with `docker` when needed.

## Scenarios

Run `cargo xtask writer-bench --help` to see the authoritative scenario list.
Current scenarios are:

- `narrow_numeric`: primitive numeric throughput.
- `mixed_nullable`: nullable primitives and short strings.
- `wide_mixed`: ingestion-style ids, event time, categories, text, and binary
  payloads.
- `decimal_temporal`: finance-style decimals, dates, and timestamps.
- `string_heavy`: large variable text and binary payload rows.
- `wide_sparse`: thirty-two mixed columns with sparse nullable values.
- `tpch_lineitem_like`: TPC-H lineitem-inspired transport workload without
  external dbgen.

Rows per second is only directly comparable for the same scenario and data
volume. A narrow numeric row and a string-heavy row have very different payload
sizes and conversion costs.

## Baseline Writer

Use `baseline` to benchmark this crate's current TokenRow SQL Server writer:

```sh
cargo xtask writer-bench baseline \
  --container-runtime podman \
  --scenario narrow_numeric \
  --rows 100000 \
  --batch-size 8192 \
  --repeat 3
```

The harness starts a SQL Server container unless `--connection-string` is
provided. It creates a benchmark database and table, writes the generated Arrow
batches, validates the number of inserted rows, and cleans up managed resources
after the run.

## Arrow ODBC Backend

Use `arrow-odbc` to benchmark the optional `arrow-odbc` SQL Server write path:

```sh
cargo xtask writer-bench arrow-odbc \
  --container-runtime podman \
  --scenario narrow_numeric \
  --rows 10000 \
  --batch-size 8192 \
  --repeat 3
```

This path builds and runs a managed runner image. The runner reads an Arrow IPC
dataset and writes it through `arrow-odbc`. Use `--keep-runner-image` only when
you want to keep that image for repeated local experiments.

## Backend Compare

Use `compare` for the fairest backend comparison. The command generates one
Arrow IPC dataset and has each selected backend write that same file:

```sh
cargo xtask writer-bench compare \
  --container-runtime podman \
  --backends baseline,arrow-odbc \
  --scenario narrow_numeric \
  --rows 10000 \
  --batch-size 8192 \
  --repeat 3
```

The shared IPC file is the fairness boundary. It keeps data generation outside
the backend timing and ensures every backend sees the same rows, null pattern,
string values, binary values, and temporal values.

For stable comparisons, prefer runs long enough that setup noise and timer
resolution do not dominate the result. Very short runs are useful as smoke
tests, but they are not enough for performance decisions.

## IPC Dataset Generation

Use `ipc` when you want to inspect or reuse a generated dataset:

```sh
cargo xtask writer-bench ipc \
  --path target/bench.arrow \
  --scenario mixed_nullable \
  --rows 100000 \
  --batch-size 8192
```

Generated benchmark IPC files should stay under `target/` or another ignored
local path.

## Existing SQL Server

Pass `--connection-string` to use an existing SQL Server instead of a managed
container:

```sh
cargo xtask writer-bench baseline \
  --connection-string 'server=tcp:127.0.0.1,1433;user id=sa;password=REDACTED;TrustServerCertificate=true' \
  --database arrow_tiberius_benchmark \
  --scenario mixed_nullable \
  --rows 100000 \
  --batch-size 8192
```

Avoid sharing command output that contains secrets. Prefer temporary benchmark
credentials and a disposable database.

## Metrics

Human output includes:

- backend name.
- scenario name.
- rows per repeat.
- batch size.
- repeat count.
- rows written.
- batches written.
- write rows per second.
- validated rows.
- setup, write, finish, validate, cleanup, and total timings.

For backend comparison, focus first on write time and validated rows. Setup time
includes container startup, image build, database creation, table creation, and
other harness work. Cleanup time can include container and image removal.

## Cleanup

Managed containers, networks, runner containers, generated IPC files, and the
runner image are cleaned up by default. The flags below intentionally keep local
resources:

- `--keep-container`
- `--keep-runner-image`

If a process is interrupted, inspect and remove leftover local resources with
your container runtime, for example:

```sh
podman ps -a
podman rm -f <container>
podman network ls
podman network rm <network>
podman images
podman rmi <image>
```

Generated IPC files use `target/arrow-tiberius-writer-bench/` during managed
compare runs.

## Current Backend Scope

The current production writer backend is the baseline TokenRow path. The direct
raw TDS encoder is not benchmarked until that backend exists. When it is added,
the compare command should be extended so baseline, `arrow-odbc`, and direct TDS
can all consume the same IPC dataset.

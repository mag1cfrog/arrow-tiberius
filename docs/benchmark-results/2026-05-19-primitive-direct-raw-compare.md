# Local Writer Benchmark: Primitive Direct Raw vs Existing Writers

Date: 2026-05-19

This document records one local benchmark run on one Linux machine. Treat it as
local evidence for this development environment, not as a universal performance
claim. Results can change with hardware, SQL Server version, container runtime,
driver version, network path, row count, batch size, schema shape, data
distribution, Rust version, SQL Server configuration, and benchmark runner
implementation details.

This run compares four writer backends on the primitive `narrow_numeric`
scenario:

- `baseline`: this crate's current TokenRow writer.
- `direct-raw`: this branch's primitive direct raw TDS row encoder.
- `arrow-odbc`: generic ODBC parameter-array writer through the `arrow-odbc`
  crate and Microsoft ODBC Driver 18 for SQL Server.
- `odbc-bcp`: benchmark-only native SQL Server ODBC BCP runner using Microsoft
  ODBC Driver 18 BCP extension functions.

The goal of this run was to make every backend spend more than one minute in
the measured write path, so the result is less sensitive to short-run noise than
the smaller smoke comparisons.

## Environment

- Host: `gmktec`
- OS: Fedora Linux 43 (Server Edition)
- Kernel: `Linux gmktec 6.19.13-200.fc43.x86_64 #1 SMP PREEMPT_DYNAMIC Sat Apr 18 20:20:44 UTC 2026 x86_64 GNU/Linux`
- CPU: AMD Ryzen 7 8845HS w/ Radeon 780M Graphics
- CPU topology: 1 socket, 8 cores, 16 logical CPUs, 2 threads per core
- CPU frequency range reported by `lscpu`: 419.4210 MHz to 5137.9038 MHz
- CPU caches: L1d 256 KiB, L1i 256 KiB, L2 8 MiB, L3 16 MiB
- Memory from `free -h`: 27 GiB total, 15 GiB available during collection
- Swap from `free -h`: 39 GiB total, 618 MiB used during collection
- Container runtime: `podman version 5.8.2`
- SQL Server image: `mcr.microsoft.com/mssql/server:2017-latest`
- ODBC runner base image: `rust:1-bookworm`
- ODBC driver package in runner image: `msodbcsql18` 18.6.2.1-1
- Rust: `rustc 1.93.0 (254b59607 2026-01-19)`
- Cargo: `cargo 1.93.0 (083ac5135 2025-12-15)`

## Command

The benchmark used a shared Arrow IPC dataset through `writer-bench compare`.
All four backends wrote the same generated IPC file to the same managed SQL
Server container.

```sh
cargo xtask writer-bench compare \
  --scenario narrow_numeric \
  --rows 50000000 \
  --batch-size 8192 \
  --repeat 1 \
  --backends baseline,direct-raw,arrow-odbc,odbc-bcp
```

After the fixed-width primitive fast path was added, `direct-raw` was rerun with
the same data shape:

```sh
cargo xtask writer-bench compare \
  --scenario narrow_numeric \
  --rows 50000000 \
  --batch-size 8192 \
  --repeat 1 \
  --backends direct-raw
```

The managed SQL Server container and container network were cleaned up by
`xtask` after the run. The ODBC runner image was also removed after the run.
Runner compile time is outside the measured write window.

## Scenario

- Scenario: `narrow_numeric`
- Rows: 50,000,000
- Batch size: 8,192
- Batches: 6,104
- Repeat: 1

Schema:

| Column | Arrow type | SQL Server type | Nullable |
| --- | --- | --- | --- |
| `id32` | `Int32` | `int` | no |
| `id64` | `Int64` | `bigint` | no |
| `score` | `Float64` | `float(53)` | no |

## Results

For the Tiberius backends, rows/sec is computed from write plus finish time,
matching the `writer-bench` report. For the ODBC runners, rows/sec is computed
from the runner write window.

The `direct-raw` row below uses the final-code rerun after the fixed-width
primitive fast path. The other backend rows are from the same local benchmark
series on the same machine and SQL Server image.

| Backend | Rows | Write | Finish | Validate | Total | Rows/sec | Peak RSS KiB | Peak RSS MiB |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| `baseline` | 50,000,000 | 123.764s | 0.044s | 0.198s | 124.049s | 403,852.88 | 19,012 | 18.57 |
| `direct-raw` | 50,000,000 | 55.850s | 0.046s | 0.197s | 56.129s | 894,526.36 | 20,308 | 19.83 |
| `arrow-odbc` | 50,000,000 | 170.225s | n/a | n/a | n/a | 293,728.89 | 1,022,080 | 998.13 |
| `odbc-bcp` | 50,000,000 | 90.104s | n/a | n/a | n/a | 554,914.32 | 16,264 | 15.88 |

Relative to this run's `direct-raw` result:

| Comparison | Ratio |
| --- | ---: |
| `direct-raw` / `baseline` | 2.22x |
| `direct-raw` / `arrow-odbc` | 3.05x |
| `direct-raw` / `odbc-bcp` | 1.61x |

## Direct Raw Profiling

A temporary env-gated phase profiler was used to compare the direct encoder
before and after the fixed-width primitive fast path. The profiler was removed
after collecting these numbers.

Before the optimization:

| Phase | Time | Percent |
| --- | ---: | ---: |
| outer validation | 0.016751s | 0.02% |
| inner validation | 0.003712s | 0.01% |
| measure lengths | 1.720852s | 2.47% |
| build layout | 6.542528s | 9.38% |
| allocate payload | 0.602737s | 0.86% |
| fill columns | 12.289070s | 17.62% |
| finalize payload | 0.189752s | 0.27% |
| send raw payload | 48.399348s | 69.38% |
| profiled total | 69.764750s | 100.00% |

After the optimization:

| Phase | Time | Percent |
| --- | ---: | ---: |
| validation | 0.013861s | 0.03% |
| fast layout | 0.006788s | 0.01% |
| fast allocate/tokens | 1.206924s | 2.20% |
| fast fill | 5.761178s | 10.49% |
| fast finalize | 0.170686s | 0.31% |
| general layout | 0.000000s | 0.00% |
| general allocate/tokens | 0.000000s | 0.00% |
| general fill | 0.000000s | 0.00% |
| general finalize | 0.000000s | 0.00% |
| send raw payload | 47.756388s | 86.96% |
| profiled total | 54.915824s | 100.00% |

## Notes

- `direct-raw` was the fastest backend on this primitive numeric workload in
  this local run.
- The optimized `direct-raw` path reduced the measured write window from
  123.808s for `baseline` to 55.896s for the same 50,000,000 rows.
- The fast path avoids the per-batch `cell_lengths` matrix, `RowLayout`,
  `CellPosition` vector, and generic per-cell position lookup for non-null
  fixed-width primitive mappings.
- This result is limited to the primitive `narrow_numeric` scenario. At the time
  of this run, the benchmark `direct-raw` backend only supports this scenario.
- `direct-raw` still emits row-shaped TDS payloads. It is not the future full
  columnar/native batch layout.
- Peak RSS for `baseline` and `direct-raw` is read from the xtask process
  high-water mark after each backend finishes. Because they run in the same
  process, this is useful as a rough local signal, but it is not a fully
  isolated per-backend memory profile.
- Peak RSS for `arrow-odbc` and `odbc-bcp` is measured inside each runner
  process.
- This run used local container networking. It did not test remote SQL Server
  latency, TLS overhead to a remote server, server-side indexes, triggers,
  constraints beyond nullability, or concurrent writers.

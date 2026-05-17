# Local Writer Benchmark: Baseline TokenRow vs arrow-odbc

Date: 2026-05-17

This document records one local benchmark run on one Linux machine. Treat it as
local evidence for this development environment, not as a universal performance
claim. Results can change with hardware, SQL Server version, container runtime,
driver version, network path, row count, batch size, schema shape, data
distribution, Rust version, and SQL Server configuration.

The `arrow-odbc` runner used manual transaction control for this run:
autocommit was disabled before writing, the measured write window included the
final commit, and failures would roll back. Older local `arrow-odbc` runs with
per-execute autocommit are superseded by this result.

## Environment

- Host: `gmktec`
- OS: Fedora Linux 43 (Server Edition)
- Kernel: `Linux gmktec 6.19.13-200.fc43.x86_64 #1 SMP PREEMPT_DYNAMIC Sat Apr 18 20:20:44 UTC 2026 x86_64 GNU/Linux`
- CPU: AMD Ryzen 7 8845HS w/ Radeon 780M Graphics
- CPU topology: 1 socket, 8 cores, 16 logical CPUs, 2 threads per core
- CPU frequency range reported by `lscpu`: 419.4210 MHz to 5137.9038 MHz
- CPU caches: L1d 256 KiB, L1i 256 KiB, L2 8 MiB, L3 16 MiB
- Memory from `free -h`: 27 GiB total, 15 GiB available during collection
- Swap from `free -h`: 39 GiB total, 689 MiB used during collection
- Container runtime: `podman version 5.8.2`
- SQL Server image: `mcr.microsoft.com/mssql/server:2017-latest`
- ODBC runner base image: `rust:1-bookworm`
- ODBC driver package in runner image: `msodbcsql18` 18.6.2.1-1
- Rust: `rustc 1.93.0 (254b59607 2026-01-19)`
- Cargo: `cargo 1.93.0 (083ac5135 2025-12-15)`

## Command

The benchmark used shared Arrow IPC datasets through `writer-bench compare`.
Each scenario generated one IPC file, then both backends wrote the same file to
the same managed SQL Server container.

```sh
mkdir -p target/bench-results
log="target/bench-results/writer-compare-optimized-arrow-odbc-calibrated-2026-05-17.raw.log"
: > "$log"

run_case() {
  scenario="$1"
  rows="$2"
  repeat="$3"
  batch_size="8192"

  cargo xtask writer-bench compare \
    --container-runtime podman \
    --backends baseline,arrow-odbc \
    --scenario "$scenario" \
    --rows "$rows" \
    --batch-size "$batch_size" \
    --repeat "$repeat" \
    --keep-runner-image 2>&1 | tee -a "$log"
}

run_case narrow_numeric 10000000 3
run_case mixed_nullable 5000000 3
run_case decimal_temporal 4000000 3
run_case wide_mixed 1000000 3
run_case wide_sparse 750000 3
run_case tpch_lineitem_like 1000000 3
run_case string_heavy 50000 3
```

The runner image was kept between scenarios to avoid rebuilding it for every
scenario, then removed after the run.

## Results

`baseline` is this crate's current TokenRow writer. `arrow-odbc` is the managed
container runner using `arrow-odbc` and Microsoft ODBC Driver 18 for SQL Server.

| Scenario | Rows per repeat | Repeat | Total rows | Baseline rows/sec | Baseline write | Baseline total | arrow-odbc rows/sec | arrow-odbc write | arrow-odbc / baseline |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| `narrow_numeric` | 10,000,000 | 3 | 30,000,000 | 402,966.36 | 74.313s | 74.623s | 286,338.78 | 104.771s | 0.71x |
| `mixed_nullable` | 5,000,000 | 3 | 15,000,000 | 203,542.69 | 73.560s | 73.813s | 181,481.62 | 82.653s | 0.89x |
| `decimal_temporal` | 4,000,000 | 3 | 12,000,000 | 165,634.44 | 72.295s | 72.552s | 201,457.21 | 59.566s | 1.22x |
| `wide_mixed` | 1,000,000 | 3 | 3,000,000 | 40,437.95 | 74.059s | 74.270s | 123,056.73 | 24.379s | 3.04x |
| `wide_sparse` | 750,000 | 3 | 2,250,000 | 24,075.63 | 93.325s | 93.539s | 78,206.47 | 28.770s | 3.25x |
| `tpch_lineitem_like` | 1,000,000 | 3 | 3,000,000 | 29,283.15 | 102.317s | 102.532s | 89,490.80 | 33.523s | 3.06x |
| `string_heavy` | 50,000 | 3 | 150,000 | 1,878.44 | 79.846s | 79.927s | 3,666.85 | 40.907s | 1.95x |

## Notes

- The baseline writer was faster on `narrow_numeric` and `mixed_nullable`.
- The `arrow-odbc` path was faster on the decimal, wide, TPC-H-like, and
  string-heavy scenarios.
- The widest and string-heavy cases are the most relevant warning signal for
  the current baseline writer. They suggest the row-oriented TokenRow path is
  likely doing materially more per-cell work than a columnar ODBC parameter
  array path for wide or payload-heavy data.
- These numbers do not compare against a future direct TDS batch encoder. That
  backend should be measured with the same shared IPC comparison command once it
  exists.
- This run used one managed SQL Server container and local container networking.
  It did not test remote SQL Server latency, TLS overhead to a remote server,
  server-side indexes, triggers, constraints beyond nullability, or concurrent
  writers.

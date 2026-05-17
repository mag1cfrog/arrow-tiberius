# Local Writer Benchmark: Baseline TokenRow vs arrow-odbc vs ODBC BCP

Date: 2026-05-17

This document records one local benchmark run on one Linux machine. Treat it as
local evidence for this development environment, not as a universal performance
claim. Results can change with hardware, SQL Server version, container runtime,
driver version, network path, row count, batch size, schema shape, data
distribution, Rust version, SQL Server configuration, and benchmark runner
implementation details.

This run repeats the calibrated workload from
`2026-05-17-local-writer-compare.md`, but adds a third backend:

- `baseline`: this crate's current TokenRow writer.
- `arrow-odbc`: generic ODBC parameter-array writer through the `arrow-odbc`
  crate and Microsoft ODBC Driver 18 for SQL Server.
- `odbc-bcp`: benchmark-only native SQL Server ODBC BCP runner using Microsoft
  ODBC Driver 18 BCP extension functions.

The `arrow-odbc` runner used manual transaction control for this run:
autocommit was disabled before writing, the measured write window included the
final commit, and failures would roll back.

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
Each scenario generated one IPC file, then all three backends wrote the same
file to the same managed SQL Server container.

```sh
mkdir -p target/bench-results
log="target/bench-results/writer-compare-three-backends-calibrated-2026-05-17.raw.log"
: > "$log"

run_case() {
  scenario="$1"
  rows="$2"
  repeat="$3"
  batch_size="8192"

  cargo xtask writer-bench compare \
    --container-runtime podman \
    --backends baseline,arrow-odbc,odbc-bcp \
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

The runner image was kept between scenarios to avoid rebuilding the base image
from scratch for every scenario. Each runner still compiled inside a fresh
container, and that compile time is outside the measured write window. The
runner image was removed after the run.

## Results

| Scenario | Rows per repeat | Repeat | Total rows | Baseline rows/sec | Baseline write | arrow-odbc rows/sec | arrow-odbc write | ODBC BCP rows/sec | ODBC BCP write | BCP / baseline | BCP / arrow-odbc |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| `narrow_numeric` | 10,000,000 | 3 | 30,000,000 | 410,191.24 | 73.004s | 282,438.00 | 106.218s | 587,682.18 | 51.048s | 1.43x | 2.08x |
| `mixed_nullable` | 5,000,000 | 3 | 15,000,000 | 201,626.59 | 74.258s | 178,518.30 | 84.025s | 369,148.99 | 40.634s | 1.83x | 2.07x |
| `decimal_temporal` | 4,000,000 | 3 | 12,000,000 | 165,785.20 | 72.235s | 199,966.67 | 60.010s | 255,683.63 | 46.933s | 1.54x | 1.28x |
| `wide_mixed` | 1,000,000 | 3 | 3,000,000 | 40,939.46 | 73.149s | 116,031.72 | 25.855s | 134,904.22 | 22.238s | 3.30x | 1.16x |
| `wide_sparse` | 750,000 | 3 | 2,250,000 | 23,533.06 | 95.482s | 75,913.49 | 29.639s | 80,159.61 | 28.069s | 3.41x | 1.06x |
| `tpch_lineitem_like` | 1,000,000 | 3 | 3,000,000 | 29,222.05 | 102.530s | 90,003.60 | 33.332s | 78,995.18 | 37.977s | 2.70x | 0.88x |
| `string_heavy` | 50,000 | 3 | 150,000 | 1,839.50 | 81.537s | 3,749.63 | 40.004s | 8,223.68 | 18.240s | 4.47x | 2.19x |

## Notes

- Native ODBC BCP was faster than the current baseline writer on every tested
  scenario.
- Native ODBC BCP was faster than `arrow-odbc` on six of seven tested
  scenarios.
- `tpch_lineitem_like` was the one scenario where `arrow-odbc` beat the current
  native BCP runner. The BCP runner currently formats decimal, date, and
  timestamp values as text for SQL Server conversion, and this scenario has
  several decimal and date columns. That likely leaves optimization room in the
  benchmark-only BCP runner itself.
- `wide_sparse` was close between `arrow-odbc` and ODBC BCP. Both were much
  faster than the row-oriented baseline path.
- `string_heavy` showed the largest gain for ODBC BCP over both existing
  backends in this run.
- These numbers do not compare against a future direct TDS batch encoder. That
  backend should be measured with the same shared IPC comparison command once it
  exists.
- This run used one managed SQL Server container per scenario and local
  container networking. It did not test remote SQL Server latency, TLS overhead
  to a remote server, server-side indexes, triggers, constraints beyond
  nullability, or concurrent writers.

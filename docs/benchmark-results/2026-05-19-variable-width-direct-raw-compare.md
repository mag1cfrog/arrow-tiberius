# Local Writer Benchmark: Variable-Width Direct Raw TDS Compare

## Summary

This run compares four writer backends after direct raw TDS support was extended
to Arrow `Utf8` and `Binary` mappings:

- `baseline`: this crate's Tiberius `TokenRow` writer.
- `direct-raw`: this crate's direct raw TDS row payload writer.
- `arrow-odbc`: generic ODBC parameter-array writer through the `arrow-odbc`
  crate.
- `odbc-bcp`: benchmark-only SQL Server native ODBC BCP runner using Microsoft
  ODBC Driver 18.

The goal was to test whether #67's variable-width direct encoder is effective
on:

- `string_heavy`: large `nvarchar(max)` and `varbinary(max)` payload rows.
- `mixed_nullable`: nullable primitive columns plus short `nvarchar(max)`
  values.

These are local benchmark results from one machine. They are useful development
evidence, not a universal performance claim.

## Environment

- Date: 2026-05-19 local time.
- Commit: `8194437`.
- OS: Fedora Linux 43 Server Edition.
- Kernel: `Linux gmktec 6.19.13-200.fc43.x86_64 #1 SMP PREEMPT_DYNAMIC Sat Apr 18 20:20:44 UTC 2026 x86_64 GNU/Linux`.
- CPU: AMD Ryzen 7 8845HS w/ Radeon 780M Graphics.
- CPU topology: 8 cores, 16 threads.
- Memory: 27 GiB total, 17 GiB available at collection time.
- Rust: `rustc 1.93.0 (254b59607 2026-01-19)`.
- Cargo: `cargo 1.93.0 (083ac5135 2025-12-15)`.
- Container runtime: `podman version 5.8.2`.
- SQL Server image: `mcr.microsoft.com/mssql/server:2017-latest`.
- ODBC runner image: `arrow-tiberius-odbc-runner:local`, built by xtask.
- ODBC driver package in runner image: `msodbcsql18` 18.6.2.1-1.

## Commands

Smoke run:

```sh
cargo xtask writer-bench compare \
  --container-runtime podman \
  --scenario string_heavy \
  --rows 1000 \
  --batch-size 512 \
  --repeat 1 \
  --backends baseline,direct-raw,arrow-odbc,odbc-bcp
```

Calibrated `string_heavy` run:

```sh
cargo xtask writer-bench compare \
  --container-runtime podman \
  --scenario string_heavy \
  --rows 300000 \
  --batch-size 8192 \
  --repeat 1 \
  --backends baseline,direct-raw,arrow-odbc,odbc-bcp
```

Calibrated `mixed_nullable` run:

```sh
cargo xtask writer-bench compare \
  --container-runtime podman \
  --scenario mixed_nullable \
  --rows 5000000 \
  --batch-size 8192 \
  --repeat 1 \
  --backends baseline,direct-raw,arrow-odbc,odbc-bcp
```

## Results: string_heavy

- Rows per repeat: 300,000.
- Batch size: 8,192.
- Repeat: 1.
- Generated IPC batches: 37.

| Backend | Rows | Write | Finish | Total | Rows/sec | Peak RSS KiB | Peak RSS MiB |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| `baseline` | 300,000 | 169.308s | 0.003s | 169.388s | 1,771.89 | 106,440 | 103.95 |
| `direct-raw` | 300,000 | 136.610s | 0.009s | 136.692s | 2,195.88 | 208,292 | 203.41 |
| `arrow-odbc` | 300,000 | 26.466s | n/a | n/a | 11,335.30 | 1,455,744 | 1,421.63 |
| `odbc-bcp` | 300,000 | 41.249s | n/a | n/a | 7,272.90 | 52,776 | 51.54 |

Relative rows/sec:

| Comparison | Ratio |
| --- | ---: |
| `direct-raw` / `baseline` | 1.24x |
| `arrow-odbc` / `direct-raw` | 5.16x |
| `odbc-bcp` / `direct-raw` | 3.31x |

Interpretation:

- `direct-raw` is faster than `baseline` on large variable-width rows, but only
  by about 24 percent in this run.
- `direct-raw` uses about twice the peak RSS of `baseline` on this scenario.
- `arrow-odbc` is much faster on this large-payload workload, but with much
  higher memory use.
- `odbc-bcp` is also much faster than `direct-raw` and uses the least memory of
  the four backends.

## Results: mixed_nullable

- Rows per repeat: 5,000,000.
- Batch size: 8,192.
- Repeat: 1.
- Generated IPC batches: 611.

| Backend | Rows | Write | Finish | Total | Rows/sec | Peak RSS KiB | Peak RSS MiB |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| `baseline` | 5,000,000 | 25.212s | 0.044s | 25.308s | 197,974.23 | 19,312 | 18.86 |
| `direct-raw` | 5,000,000 | 16.565s | 0.047s | 16.654s | 300,986.29 | 21,148 | 20.65 |
| `arrow-odbc` | 5,000,000 | 27.539s | n/a | n/a | 181,560.70 | 160,488 | 156.73 |
| `odbc-bcp` | 5,000,000 | 14.878s | n/a | n/a | 336,066.68 | 16,388 | 16.00 |

Relative rows/sec:

| Comparison | Ratio |
| --- | ---: |
| `direct-raw` / `baseline` | 1.52x |
| `direct-raw` / `arrow-odbc` | 1.66x |
| `odbc-bcp` / `direct-raw` | 1.12x |

Interpretation:

- `direct-raw` is effective on the short-string nullable workload.
- It is about 52 percent faster than `baseline` and about 66 percent faster than
  `arrow-odbc` in this run.
- It is still slightly slower than SQL Server native ODBC BCP.
- Peak RSS is close to baseline and far below `arrow-odbc`.

## Development Notes

The #67 direct variable-width encoder is beneficial for short strings and mixed
nullable rows, but the large-payload result shows a clear optimization target.
The current direct path still builds full row payload buffers and performs
UTF-16 encoding into row-major positions. For large `nvarchar(max)` and
`varbinary(max)` payloads, future work should investigate:

- avoiding full-batch row payload materialization for very large payloads;
- row-range bounded encoding to cap peak memory;
- more efficient UTF-16 staging for `nvarchar(max)`;
- whether PLP chunking strategy affects SQL Server bulk-load throughput;
- profiling direct `string_heavy` to find whether time is dominated by UTF-16
  conversion, payload allocation, SQL Server send, or raw bulk API behavior.

This benchmark supports keeping the direct raw path, but it also shows that
large variable-width payloads need targeted optimization before claiming broad
superiority over ODBC-backed writers.

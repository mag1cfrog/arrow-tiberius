# Direct Raw Benchmark Comparison

This document records curated writer benchmark evidence for issue 59. It is the
summary source of truth for direct raw performance comparisons. Raw command
logs may live under `target/bench-results/` during local work, but those files
are intentionally not committed.

These numbers are local evidence only. They are not a general performance
guarantee. Compare rows/sec only within the same scenario, row count, batch
size, SQL Server image, machine, and benchmark command shape.

## Method

- Build mode: `cargo run --release -p xtask -- writer-bench compare`.
- SQL Server: managed `mcr.microsoft.com/mssql/server:2017-latest` container.
- Container runtime: `podman`.
- Dataset boundary: one shared Arrow IPC file per compare run.
- Batch size: `8192`.
- Repeat count: `3`.
- Formal rows/repeat are calibrated so each participating backend spends about
  one minute or more in the measured write path.
- Reported throughput uses each backend's `write rows/sec` from the benchmark
  output. For Tiberius backends this uses write plus finish time. For ODBC
  runners this uses their reported write time.
- Setup, runner image build, IPC generation, validation, and cleanup are not
  part of the rows/sec number.

## Backend Support Notes

- `fixed_size_binary` is supported by `baseline`, `direct-framed`, and
  `direct-raw`.
- `fixed_size_binary` is intentionally excluded from `arrow-odbc` and
  `odbc-bcp` comparisons. `arrow-odbc` 23.2.0 panics on this scenario through
  its variadic binary writer path, and the benchmark BCP runner reports that it
  does not support `FixedSizeBinary`.
- `date_fast_path` is intentionally excluded from `arrow-odbc` and `odbc-bcp`
  comparisons. `arrow-odbc` 23.2.0 fails this scenario with an integer
  conversion panic, and the benchmark BCP runner reports that it does not
  support `Date64`.
- `fixed_width_128` and `decimal_temporal_128` are Tiberius-only encoder
  isolation scenarios. They are intentionally wide so local per-cell encoding
  cost is large enough to inspect with `--profile-direct`.
- `decimal_temporal_128` is intentionally excluded from `arrow-odbc` and
  `odbc-bcp` comparisons because this scenario includes time mappings that the
  benchmark reference runners do not support consistently.
- `string_heavy_unicode` is intentionally excluded from `arrow-odbc`
  comparisons. The runner writes this scenario without preserving the BMP
  Unicode tenant sentinel, so it is not a lossless reference backend for this
  workload.

## Results

### narrow_numeric

Command:

```sh
cargo run --release -p xtask -- writer-bench compare \
  --container-runtime podman \
  --backends baseline,direct-framed,direct-raw,arrow-odbc,odbc-bcp \
  --rows 25000000 \
  --batch-size 8192 \
  --repeat 3 \
  --scenario narrow_numeric \
  --keep-runner-image
```

Rows written per backend: 75,000,000.

| Backend | Rows/sec | Relative to baseline | Measured write time |
| --- | ---: | ---: | ---: |
| `baseline` | 1,145,315.80 | 1.00x | 65.484s |
| `direct-framed` | 1,194,552.35 | 1.04x | 62.785s |
| `direct-raw` | 1,115,472.68 | 0.97x | 67.236s |
| `arrow-odbc` | 286,151.42 | 0.25x | 262.099s |
| `odbc-bcp` | 617,563.51 | 0.54x | 121.445s |

Observation: on this release run, `direct-framed` is modestly faster than the
baseline, while `direct-raw` is slightly slower than the baseline. Both direct
Tiberius paths are much faster than the two ODBC reference paths for this
scenario.

Investigation note: a Tiberius-only rerun with `--profile-direct` using the same
row count, batch size, and repeat count reversed the small `direct-raw` loss:
`baseline` wrote 1,139,484.42 rows/sec, `direct-framed` wrote 1,167,037.39
rows/sec, and `direct-raw` wrote 1,196,613.44 rows/sec. The direct profile
showed most time in packet writes to SQL Server rather than local row encoding.
This suggests the original 3 percent `direct-raw` loss was run-to-run I/O or SQL
Server variance, not an encoder correctness problem.

### extended_primitive

Command:

```sh
cargo run --release -p xtask -- writer-bench compare \
  --container-runtime podman \
  --backends baseline,direct-framed,direct-raw,odbc-bcp \
  --rows 20000000 \
  --batch-size 8192 \
  --repeat 3 \
  --scenario extended_primitive \
  --keep-runner-image
```

Rows written per backend: 60,000,000.

`arrow-odbc` is excluded because this benchmark runner does not support this
scenario's unsigned integer mappings.

| Backend | Rows/sec | Relative to baseline | Measured write time |
| --- | ---: | ---: | ---: |
| `baseline` | 670,798.00 | 1.00x | 89.446s |
| `direct-framed` | 705,719.14 | 1.05x | 85.020s |
| `direct-raw` | 710,502.03 | 1.06x | 84.448s |
| `odbc-bcp` | 426,451.36 | 0.64x | 140.696s |

Observation: for extended primitive rows, both direct paths are modestly faster
than the baseline. `direct-raw` is slightly ahead of `direct-framed` on this
run, and both are materially faster than the benchmark BCP reference path.

### uint64_policy

Command:

```sh
cargo run --release -p xtask -- writer-bench compare \
  --container-runtime podman \
  --backends baseline,direct-framed,direct-raw \
  --rows 20000000 \
  --batch-size 8192 \
  --repeat 3 \
  --scenario uint64_policy
```

Rows written per backend: 60,000,000.

`arrow-odbc` and `odbc-bcp` are excluded because the benchmark reference
runners do not support this scenario's UInt64 policy mappings.

| Backend | Rows/sec | Relative to baseline | Measured write time |
| --- | ---: | ---: | ---: |
| `baseline` | 633,006.96 | 1.00x | 94.786s |
| `direct-framed` | 665,160.73 | 1.05x | 90.204s |
| `direct-raw` | 749,817.63 | 1.18x | 80.020s |

Observation: for UInt64 values planned as `decimal(20,0)`, `direct-raw` is the
clear fastest backend in this run. `direct-framed` is also faster than the
baseline, but by a smaller margin.

### fixed_size_binary

Command:

```sh
cargo run --release -p xtask -- writer-bench compare \
  --container-runtime podman \
  --backends baseline,direct-framed,direct-raw \
  --rows 20000000 \
  --batch-size 8192 \
  --repeat 3 \
  --scenario fixed_size_binary
```

Rows written per backend: 60,000,000.

| Backend | Rows/sec | Relative to baseline | Measured write time |
| --- | ---: | ---: | ---: |
| `baseline` | 693,272.36 | 1.00x | 86.546s |
| `direct-framed` | 841,563.01 | 1.21x | 71.296s |
| `direct-raw` | 842,702.94 | 1.22x | 71.199s |

Observation: for fixed-size binary rows, both direct paths are about 21 percent
to 22 percent faster than the baseline. `direct-framed` and `direct-raw` are
effectively tied on this run.

### date_fast_path

Command:

```sh
cargo run --release -p xtask -- writer-bench compare \
  --container-runtime podman \
  --backends baseline,direct-framed,direct-raw \
  --rows 20000000 \
  --batch-size 8192 \
  --repeat 3 \
  --scenario date_fast_path
```

Rows written per backend: 60,000,000.

| Backend | Rows/sec | Relative to baseline | Measured write time |
| --- | ---: | ---: | ---: |
| `baseline` | 240,247.69 | 1.00x | 249.742s |
| `direct-framed` | 319,075.31 | 1.33x | 188.043s |
| `direct-raw` | 323,710.03 | 1.35x | 185.352s |

Observation: for date rows, both direct paths are materially faster than the
baseline. `direct-raw` is slightly faster than `direct-framed` on this run.

### mixed_nullable

Command:

```sh
cargo run --release -p xtask -- writer-bench compare \
  --container-runtime podman \
  --backends baseline,direct-framed,direct-raw,arrow-odbc,odbc-bcp \
  --rows 12000000 \
  --batch-size 8192 \
  --repeat 3 \
  --scenario mixed_nullable \
  --keep-runner-image
```

Rows written per backend: 36,000,000.

| Backend | Rows/sec | Relative to baseline | Measured write time |
| --- | ---: | ---: | ---: |
| `baseline` | 539,741.68 | 1.00x | 66.698s |
| `direct-framed` | 551,817.78 | 1.02x | 65.239s |
| `direct-raw` | 553,962.60 | 1.03x | 64.986s |
| `arrow-odbc` | 184,804.93 | 0.34x | 194.800s |
| `odbc-bcp` | 352,071.35 | 0.65x | 102.252s |

Observation: for mixed nullable primitive and string rows, both direct paths are
slightly faster than the baseline. The direct paths are also faster than both
ODBC reference paths in this run.

### decimal_temporal

Command:

```sh
cargo run --release -p xtask -- writer-bench compare \
  --container-runtime podman \
  --backends baseline,direct-framed,direct-raw,arrow-odbc,odbc-bcp \
  --rows 18000000 \
  --batch-size 8192 \
  --repeat 3 \
  --scenario decimal_temporal \
  --keep-runner-image
```

Rows written per backend: 54,000,000.

| Backend | Rows/sec | Relative to baseline | Measured write time |
| --- | ---: | ---: | ---: |
| `baseline` | 747,764.20 | 1.00x | 72.215s |
| `direct-framed` | 812,354.11 | 1.09x | 66.474s |
| `direct-raw` | 741,876.87 | 0.99x | 72.789s |
| `arrow-odbc` | 224,025.49 | 0.30x | 241.044s |
| `odbc-bcp` | 366,255.65 | 0.49x | 147.438s |

Observation: for decimal and temporal rows, `direct-framed` is faster than the
baseline on this run, while `direct-raw` is effectively tied with the baseline.
Both Tiberius direct paths are faster than the ODBC reference paths.

Investigation note: a Tiberius-only rerun with `--profile-direct` using the same
row count, batch size, and repeat count also reversed the small `direct-raw`
loss: `baseline` wrote 669,509.91 rows/sec, `direct-framed` wrote 658,160.92
rows/sec, and `direct-raw` wrote 693,769.13 rows/sec. The `direct-framed` and
`direct-raw` profiles had similar local encoding cost, and most measured time
was spent waiting on packet writes to SQL Server. This points to write-path
variance rather than a decimal or temporal encoding regression.

### fixed_width_128

Command:

```sh
cargo run --release -p xtask -- writer-bench compare \
  --container-runtime podman \
  --backends baseline,direct-framed,direct-raw,direct-raw-no-fixed-fast-path \
  --rows 2500000 \
  --batch-size 8192 \
  --repeat 3 \
  --scenario fixed_width_128 \
  --profile-direct
```

Rows written per backend: 7,500,000.

| Backend | Rows/sec | Relative to baseline | Measured write time |
| --- | ---: | ---: | ---: |
| `baseline` | 63,867.11 | 1.00x | 117.432s |
| `direct-framed` | 101,926.19 | 1.60x | 73.583s |
| `direct-raw` | 99,756.90 | 1.56x | 75.183s |
| `direct-raw-no-fixed-fast-path` | 108,298.92 | 1.70x | 69.253s |

Observation: the 128-column fixed-width scenario clearly separates direct
encoding from the baseline. `direct-raw` is 1.56x the baseline, and
`direct-framed` is 1.60x the baseline on this run.

Profile note: this wide scenario amplifies local encoding cost. `direct-raw`
spent 19.168s in `append_encode`, compared with 4.782s in the
`narrow_numeric` profile rerun. Disabling the fixed-width fast path increased
`append_encode` to 34.470s, but its packet write wait was lower in this run, so
`direct-raw-no-fixed-fast-path` was the fastest end-to-end backend. This means
the scenario is useful for exposing encoder cost, but end-to-end ordering still
reflects SQL Server and socket backpressure.

### decimal_temporal_128

Command:

```sh
cargo run --release -p xtask -- writer-bench compare \
  --container-runtime podman \
  --backends baseline,direct-framed,direct-raw,direct-raw-no-fixed-fast-path \
  --rows 2000000 \
  --batch-size 8192 \
  --repeat 3 \
  --scenario decimal_temporal_128 \
  --profile-direct
```

Rows written per backend: 6,000,000.

| Backend | Rows/sec | Relative to baseline | Measured write time |
| --- | ---: | ---: | ---: |
| `baseline` | 57,136.34 | 1.00x | 105.012s |
| `direct-framed` | 79,020.58 | 1.38x | 75.929s |
| `direct-raw` | 89,500.69 | 1.57x | 67.039s |
| `direct-raw-no-fixed-fast-path` | 87,396.89 | 1.53x | 68.652s |

Observation: the 128-column decimal and temporal scenario also separates
direct encoding from the baseline. `direct-raw` is the fastest backend on this
run at 1.57x the baseline, and it is slightly faster than the no-fixed-fast-path
A/B backend.

Profile note: this wide scenario amplifies decimal and temporal local encoding
cost. `direct-raw` spent 38.701s in `append_encode`, compared with 9.909s in
the `decimal_temporal` profile rerun. Unlike `fixed_width_128`, the
fixed-width fast path is also modestly faster end-to-end here.

### string_heavy_binary_only

Command:

```sh
cargo run --release -p xtask -- writer-bench compare \
  --container-runtime podman \
  --backends baseline,direct-framed,direct-raw,arrow-odbc,odbc-bcp \
  --rows 1000000 \
  --batch-size 8192 \
  --repeat 3 \
  --scenario string_heavy_binary_only \
  --keep-runner-image
```

Rows written per backend: 3,000,000.

| Backend | Rows/sec | Relative to baseline | Measured write time |
| --- | ---: | ---: | ---: |
| `baseline` | 23,798.27 | 1.00x | 126.060s |
| `direct-framed` | 30,510.24 | 1.28x | 98.328s |
| `direct-raw` | 29,612.96 | 1.24x | 101.307s |
| `arrow-odbc` | 15,677.26 | 0.66x | 191.360s |
| `odbc-bcp` | 21,315.14 | 0.90x | 140.745s |

Observation: for large binary-heavy `varbinary(max)` rows, both direct Tiberius
paths are faster than the baseline and both ODBC reference paths. This scenario
is a payload-heavy end-to-end comparison rather than an encoder isolation
benchmark, so the result mostly reflects the full write path for large binary
values.

### string_heavy_unicode

Command:

```sh
cargo run --release -p xtask -- writer-bench compare \
  --container-runtime podman \
  --backends baseline,direct-framed,direct-raw,odbc-bcp \
  --rows 250000 \
  --batch-size 8192 \
  --repeat 3 \
  --scenario string_heavy_unicode \
  --keep-runner-image
```

Rows written per backend: 750,000.

| Backend | Rows/sec | Relative to baseline | Measured write time |
| --- | ---: | ---: | ---: |
| `baseline` | 8,765.53 | 1.00x | 85.562s |
| `direct-framed` | 9,304.07 | 1.06x | 80.609s |
| `direct-raw` | 9,201.36 | 1.05x | 81.509s |
| `odbc-bcp` | 7,815.92 | 0.89x | 95.958s |

Observation: for large BMP Unicode `nvarchar(max)` rows, both direct Tiberius
paths are modestly faster than the baseline and the BCP reference path. The
improvement is smaller than `string_heavy_binary_only`, which is expected for a
Unicode text workload dominated by payload encoding and SQL Server write costs.

### wide_sparse

Command:

```sh
cargo run --release -p xtask -- writer-bench compare \
  --container-runtime podman \
  --backends baseline,direct-framed,direct-raw,arrow-odbc,odbc-bcp \
  --rows 3000000 \
  --batch-size 8192 \
  --repeat 3 \
  --scenario wide_sparse \
  --keep-runner-image
```

Rows written per backend: 9,000,000.

| Backend | Rows/sec | Relative to baseline | Measured write time |
| --- | ---: | ---: | ---: |
| `baseline` | 117,177.04 | 1.00x | 76.807s |
| `direct-framed` | 117,992.18 | 1.01x | 76.276s |
| `direct-raw` | 119,461.34 | 1.02x | 75.338s |
| `arrow-odbc` | 86,279.62 | 0.74x | 104.312s |
| `odbc-bcp` | 95,192.77 | 0.81x | 94.545s |

Observation: for thirty-two sparse mixed columns with short nullable strings,
the Tiberius backends are close together. `direct-raw` is slightly faster than
the baseline and `direct-framed`, while both direct paths remain faster than the
ODBC reference paths.

## Pending Scenarios

All issue 59 comparison scenarios currently selected for this document have
formal rows/sec records.

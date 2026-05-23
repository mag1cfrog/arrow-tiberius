# Issue 75 extended primitive release benchmark

This records local release benchmark results from 2026-05-22 for the
`extended_primitive` writer benchmark scenario added for issue 75. Treat this
as development evidence from this machine, not a portable performance claim.

The scenario covers the new direct primitive mappings:

- `UInt8` to `tinyint`.
- `Int8` to `smallint`.
- `Int16` to `smallint`.
- `UInt16` to `int`.
- `UInt32` to `bigint`.
- `Float32` to `real`.

Each type is present as both a nullable and non-nullable column, so each row has
12 primitive columns. `arrow-odbc` is intentionally not included because it
rejects this schema at `UInt16`/`UInt32`. The supported backends for this
scenario are `baseline`, `direct-framed`, `direct-raw`, and `odbc-bcp`.

## Environment

- Local managed SQL Server through Podman.
- `cargo run --release -p xtask` for the Tiberius benchmark harness.
- ODBC runner binaries built with `cargo run --release` inside the managed
  runner container.
- Batch size: 8192 rows.
- SQL Server recovery model and table locking were left at the benchmark
  defaults.

Raw logs:

```text
target/bench-results/issue-75-extended-primitive-release-50m-repeat1-20260522.log
target/bench-results/issue-75-extended-primitive-release-50m-repeat3-20260522.log
```

## Calibration Run

The first run matched the previous primitive-heavy row count from the
`narrow_numeric` benchmark and used one repeat.

```bash
cargo run --release -p xtask -- writer-bench compare \
  --container-runtime podman \
  --scenario extended_primitive \
  --rows 50000000 \
  --batch-size 8192 \
  --repeat 1 \
  --backends baseline,direct-framed,direct-raw,odbc-bcp
```

| Backend | Rows | Write | Finish | Total | Rows/sec | Peak RSS KiB |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| `baseline` | 50,000,000 | 73.712s | 0.430s | 74.379s | 674,385.95 | 15,464 |
| `direct-framed` | 50,000,000 | 69.257s | 0.214s | 69.699s | 719,721.27 | 20,956 |
| `direct-raw` | 50,000,000 | 68.845s | 4.295s | 73.367s | 683,629.28 | 20,956 |
| `odbc-bcp` | 50,000,000 | 117.111s | n/a | n/a | 426,945.38 | 15,196 |

The calibration run was useful as a sanity check, but the single-repeat
`direct-raw` finish time was unusually high compared with the repeated run
below.

## Repeated Run

The stability run kept the same 50,000,000 rows per repeat and used three
repeats. Each backend wrote 150,000,000 rows total.

```bash
cargo run --release -p xtask -- writer-bench compare \
  --container-runtime podman \
  --scenario extended_primitive \
  --rows 50000000 \
  --batch-size 8192 \
  --repeat 3 \
  --backends baseline,direct-framed,direct-raw,odbc-bcp \
  --keep-runner-image
```

The runner image was removed manually after the run.

| Backend | Rows | Write | Finish | Total | Rows/sec | Peak RSS KiB |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| `baseline` | 150,000,000 | 220.760s | 5.187s | 226.664s | 663,872.99 | 15,428 |
| `direct-framed` | 150,000,000 | 215.796s | 0.631s | 217.206s | 693,075.99 | 20,936 |
| `direct-raw` | 150,000,000 | 219.036s | 0.536s | 220.234s | 683,148.97 | 20,936 |
| `odbc-bcp` | 150,000,000 | 347.591s | n/a | n/a | 431,541.67 | 15,352 |

Relative to `baseline` by rows/sec:

| Backend | Relative Throughput |
| --- | ---: |
| `direct-framed` | 1.04x |
| `direct-raw` | 1.03x |
| `odbc-bcp` | 0.65x |

Relative to `direct-raw` by rows/sec:

| Backend | Relative Throughput |
| --- | ---: |
| `direct-framed` | 1.01x |
| `baseline` | 0.97x |
| `odbc-bcp` | 0.63x |

## Interpretation

For this all-primitive extended schema, the direct paths are only modestly
faster than the baseline TokenRow path. `direct-framed` and `direct-raw` are
effectively close on the repeated run, with `direct-framed` about 1.5 percent
ahead by rows/sec. This is not a meaningful enough gap to justify tuning the
raw transport path specifically for this scenario.

The notable result is that `odbc-bcp` is slower here, unlike some earlier
large string or binary cases. That suggests the issue 75 direct primitive path
is competitive for the newly supported scalar types, and the remaining
performance investigation should stay focused on payload-heavy variable-width
or SQL Server LOB behavior rather than these primitive mappings.

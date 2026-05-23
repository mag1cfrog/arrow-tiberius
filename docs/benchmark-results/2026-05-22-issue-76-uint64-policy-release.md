# Issue 76 UInt64 policy release benchmark

This records local release benchmark results from 2026-05-22 for the
`uint64_policy` writer benchmark scenario added for issue 76. Treat this as
development evidence from this machine, not a portable performance claim.

The scenario covers `UInt64` values planned with
`UInt64Policy::Decimal20_0`, so the SQL Server table uses `decimal(20,0)` for
the unsigned 64-bit columns. Each row has:

- One `Int32` row id column.
- Three non-null `UInt64` columns.
- One nullable `UInt64` column.

`arrow-odbc` and `odbc-bcp` are intentionally not included because the current
benchmark ODBC paths do not support this UInt64 scenario.

## Environment

- Local managed SQL Server through Podman.
- `cargo run --release -p xtask` for the benchmark harness.
- Batch size: 8192 rows.
- TDS packet size: 32767 bytes.
- SQL Server recovery model and table locking were left at the benchmark
  defaults.

## Calibration Run

```bash
cargo xtask writer-bench compare \
  --container-runtime podman \
  --scenario uint64_policy \
  --rows 1000000 \
  --batch-size 8192 \
  --repeat 1 \
  --backends baseline,direct-framed,direct-raw \
  --profile-direct \
  --tds-packet-size 32767
```

| Backend | Rows | Write | Finish | Total | Rows/sec | Peak RSS KiB |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| `baseline` | 1,000,000 | 6.532s | 0.044s | 6.611s | 152,073.35 | 20,748 |
| `direct-framed` | 1,000,000 | 3.901s | 0.051s | 3.976s | 253,072.92 | 23,764 |
| `direct-raw` | 1,000,000 | 1.842s | 0.049s | 1.918s | 528,626.19 | 23,764 |

This calibration run was useful for sizing. It was a short debug-harness run,
so the release repeated run below is the result to use for comparison.

## Repeated Run

```bash
cargo run --release -p xtask -- writer-bench compare \
  --container-runtime podman \
  --scenario uint64_policy \
  --rows 40000000 \
  --batch-size 8192 \
  --repeat 2 \
  --backends baseline,direct-framed,direct-raw \
  --profile-direct \
  --tds-packet-size 32767
```

Each backend wrote 80,000,000 rows total.

| Backend | Rows | Write | Finish | Total | Rows/sec | Peak RSS KiB |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| `baseline` | 80,000,000 | 115.054s | 0.401s | 115.831s | 692,907.05 | 15,712 |
| `direct-framed` | 80,000,000 | 106.420s | 0.611s | 107.558s | 747,449.61 | 18,624 |
| `direct-raw` | 80,000,000 | 109.285s | 0.539s | 110.201s | 728,437.74 | 18,624 |

Relative to `baseline` by rows/sec:

| Backend | Relative Throughput |
| --- | ---: |
| `direct-framed` | 1.08x |
| `direct-raw` | 1.05x |

Relative to `direct-raw` by rows/sec:

| Backend | Relative Throughput |
| --- | ---: |
| `direct-framed` | 1.03x |
| `baseline` | 0.95x |

## Direct Profile Notes

For the repeated run, both direct paths encoded the same payload shape:

- Rows: 80,000,000.
- Encoded bytes: 3,839,999,976.
- Packets written: 117,906.
- Max packet payload bytes: 32,568.

The direct-framed path spent 9.289s in `append_encode` and 93.460s in
`send_total`. The direct-raw path spent 9.573s in `append_encode` and 95.439s
in `send_total`.

## Interpretation

On the long release run, the direct paths are modestly faster than the baseline
TokenRow path, but the difference is small. `direct-framed` is about 8 percent
faster than baseline and about 3 percent faster than `direct-raw` on this run.

The useful signal is that the new UInt64 decimal direct encoding is not an
obvious bottleneck. Most time in both direct paths is send/backpressure time,
not local encoding time. For issue 76, this supports treating the UInt64 direct
writer implementation as performance-acceptable and moving on unless a later
profile shows a type-specific regression.

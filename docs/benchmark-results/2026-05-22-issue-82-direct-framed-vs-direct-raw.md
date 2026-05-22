# Issue 82 direct framed vs direct raw benchmark

This records one local release benchmark from 2026-05-22. Treat it as
development evidence, not a portable performance claim. The goal was to isolate
transport overhead from Arrow-to-TDS row encoding.

## Question

We wanted a clean A/B split between these paths:

- `baseline`: Tiberius `TokenRow` encoding plus Tiberius framed packet sink.
- `direct-framed`: arrow-tiberius direct row encoding plus Tiberius framed packet
  sink.
- `direct-raw`: arrow-tiberius direct row encoding plus the forked direct packet
  writer.
- `odbc-bcp`: Microsoft ODBC BCP control path.

The key comparison is `direct-framed` vs `direct-raw`. Those two use the same
Arrow-side direct encoder and the same generated IPC dataset. The intended
difference is the final packet transport path.

## Code State

- arrow-tiberius branch: `feat/82-sqlserver-writer-profile`
- arrow-tiberius base commit during the run: `e503269`
- tiberius-raw-bulk base commit during the run: `9e17479`
- tiberius-raw-bulk had local uncommitted contiguous direct packet changes in:
  - `src/client/connection.rs`
  - `src/tds/codec/bulk_load.rs`
  - `tests/bulk.rs`

For the run only, `Cargo.toml` was temporarily patched with:

```toml
[patch.crates-io]
tiberius-raw-bulk = { path = "../tiberius-raw-bulk" }
```

That patch was removed after collecting the log.

Full raw log:

```text
target/bench-results/issue-82-clean-ab-edge-7k-release-300k-20260522.log
```

## Command

```bash
cargo run --release -p xtask -- writer-bench compare \
  --container-runtime podman \
  --scenario string_heavy_edge_7k \
  --rows 300000 \
  --batch-size 8192 \
  --repeat 1 \
  --backends baseline,direct-framed,direct-raw,odbc-bcp \
  --profile-direct \
  --profile-sqlserver \
  --sqlserver-profile-sample-ms 10 \
  --keep-runner-image
```

## Result Summary

| Backend | Write Time | Rows/Sec | Total Time | Peak RSS KiB |
|---|---:|---:|---:|---:|
| `baseline` | 22.941s | 13,053.45 | 23.145s | 114,948 |
| `direct-framed` | 18.620s | 16,072.54 | 18.759s | 120,940 |
| `direct-raw` | 6.230s | 47,917.87 | 6.345s | 120,940 |
| `odbc-bcp` | 8.710s | 34,443.17 | n/a | 58,168 |

Speed ratios from write time:

| Comparison | Ratio |
|---|---:|
| `direct-raw` vs `direct-framed` | 2.99x faster |
| `direct-raw` vs `baseline` | 3.68x faster |
| `direct-raw` vs `odbc-bcp` | 1.40x faster |
| `direct-framed` vs `baseline` | 1.23x faster |

## Direct Profile Split

Both direct paths encoded the same payload shape:

- rows: 300,000
- batches: 37
- row ranges: 293
- encoded bytes: 2,188,177,100
- packets written: 535,268
- packet payload bytes: 2,188,175,584
- max packet payload bytes: 4,088

The important split is below.

| Metric | `direct-framed` | `direct-raw` |
|---|---:|---:|
| `measure_batch` | 0.201s | 0.206s |
| `append_encode` | 2.520s | 2.511s |
| `send_total` | 17.632s | 5.183s |
| `send_without_append_encode` | 15.112s | 2.672s |
| `bulk write_packets elapsed` | 15.110s | 2.670s |
| `bulk write_to_wire elapsed` | 15.087s | 2.634s |
| framed connection write calls | 535,269 | 0 |
| framed connection encode elapsed | 10.089s | 0.000s |
| framed connection flush elapsed | 4.934s | 0.000s |
| direct packet write calls | 0 | 535,269 |
| direct packet low-level write calls | 0 | 535,702 |
| direct packet write elapsed | 0.000s | 2.557s |

Interpretation:

- Direct encoding itself is not the remaining gap. `measure_batch` and
  `append_encode` are effectively identical for `direct-framed` and `direct-raw`.
- The framed sink path is the slow leg in this benchmark. It spends about
  15.087s in `write_to_wire`, split mostly between framed packet encoding
  10.089s and framed sink flush 4.934s.
- The coalesced direct packet path reduces the same transport region to 2.634s.
  That is the main reason `direct-raw` beats both `direct-framed` and `odbc-bcp`
  in this run.

## SQL Server Profile

Top wait deltas:

| Backend | `ASYNC_NETWORK_IO` | `PREEMPTIVE_OS_FLUSHFILEBUFFERS` | Other notable waits |
|---|---:|---:|---|
| `baseline` | 10,550 ms | 3,719 ms | `MEMORY_ALLOCATION_EXT` 1,228 ms |
| `direct-framed` | 8,879 ms | 3,260 ms | `WRITELOG` 228 ms |
| `direct-raw` | 400 ms | 26 ms | `PAGELATCH_UP` 170 ms, `PREEMPTIVE_OS_WRITEFILEGATHER` 163 ms |
| `odbc-bcp` | 17 ms | 161 ms | `LATCH_EX` 630 ms |

Database file IO deltas:

| Backend | Log Writes | Log Bytes | Row Writes | Row Bytes | Table Used Pages |
|---|---:|---:|---:|---:|---:|
| `baseline` | 44,042 | 2,440,060,928 | 4,445 | 1,628,413,952 | 296,977 |
| `direct-framed` | 43,851 | 2,447,212,544 | 4,479 | 1,809,620,992 | 296,977 |
| `direct-raw` | 42,180 | 2,443,030,528 | 2,000 | 1,945,534,464 | 296,977 |
| `odbc-bcp` | 42,021 | 2,435,538,944 | 2,021 | 2,083,823,616 | 296,977 |

Connection notes:

- Tiberius backends used `encrypted=FALSE` and `packet_size=4096`.
- `odbc-bcp` used `encrypted=TRUE` and reported `packet_size=4266`.
- All four produced the same final table page count, 296,977 used pages.

Interpretation:

- In this run, high flush waits are associated with the framed Tiberius paths,
  not with the coalesced direct packet path.
- `direct-framed` and `baseline` both show large `ASYNC_NETWORK_IO` and
  `PREEMPTIVE_OS_FLUSHFILEBUFFERS` waits. That fits a client-side transport path
  that feeds SQL Server more slowly and leaves the server frequently waiting on
  incoming packets.
- `direct-raw` has much lower SQL Server wait time while writing the same data
  shape and roughly the same log bytes. This supports focusing on transport
  shape first, not schema narrowing or row encoder semantics.

## Conclusion

The clean A/B result supports making the contiguous direct packet writer the
main optimization target. The direct encoder is already fast enough for this
case; the large difference is between sending its output through the framed sink
and sending it through the direct packet path.

The immediate next engineering decision is whether to make the fork's direct
packet coalescing official and then rerun this benchmark after the fork changes
are committed. A repeat run is still useful because this was one local run, but
the effect size here is large enough that it is not just noise.

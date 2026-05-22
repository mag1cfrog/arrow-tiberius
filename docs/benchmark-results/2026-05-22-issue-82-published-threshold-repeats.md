# Issue 82 published threshold repeat benchmark

This records local release benchmark repeats from 2026-05-22. Treat it as
development evidence, not a portable performance claim. The goal was to repeat
the string payload threshold comparison after publishing the contiguous direct
packet writer in `tiberius-raw-bulk` `0.12.3-raw-bulk.11`.

## Question

The prior direct packet transport A/B showed a large difference between:

- `direct-framed`: arrow-tiberius direct row encoding through the Tiberius
  framed packet sink.
- `direct-raw`: the same direct row encoding through the forked direct packet
  writer.

These repeats check that conclusion across string-heavy rows below and near the
SQL Server in-row boundary, then compare it with rows that cross into LOB
storage.

## Code State

- arrow-tiberius branch: `feat/82-sqlserver-writer-profile`
- arrow-tiberius base commit during the runs: `7009181`
- tiberius-raw-bulk dependency: published `0.12.3-raw-bulk.11`
- Container runtime: `podman version 5.8.2`
- SQL Server image: `mcr.microsoft.com/mssql/server:2017-latest`
- ODBC runner image: `arrow-tiberius-odbc-runner:local`
- Rust: `rustc 1.93.0 (254b59607 2026-01-19)`

Full raw logs:

```text
target/bench-results/issue-82-published-raw-bulk-11-inline-4k-release-300k-repeat3-20260522.log
target/bench-results/issue-82-published-raw-bulk-11-edge-7k-release-300k-repeat3-20260522.log
target/bench-results/issue-82-published-raw-bulk-11-lob-9k-release-100k-repeat3-20260522.log
```

The managed SQL Server containers and container networks were cleaned up after
the completed runs. The ODBC runner image was kept between runs with
`--keep-runner-image`. An initial `string_heavy_lob_9k` run with `300000` rows
per repeat was stopped after more than 20 minutes without a final compare
summary; its SQL Server container and network were removed manually.

## Commands

The non-LOB scenarios used this command shape:

```bash
cargo run --release -p xtask -- writer-bench compare \
  --container-runtime podman \
  --scenario string_heavy_inline_4k \
  --rows 300000 \
  --batch-size 8192 \
  --repeat 3 \
  --backends baseline,direct-framed,direct-raw,odbc-bcp \
  --profile-direct \
  --profile-sqlserver \
  --sqlserver-profile-sample-ms 10 \
  --keep-runner-image
```

`string_heavy_edge_7k` replaced the scenario name for the second non-LOB run.
The completed LOB run used `string_heavy_lob_9k` and reduced `--rows` to
`100000`.

## Write Results

Each write time below covers three repeats through one backend. The total row
count reflects the reduced LOB run size.

| Scenario | Total Rows | `baseline` | `direct-framed` | `direct-raw` | `odbc-bcp` |
|---|---:|---:|---:|---:|---:|
| `string_heavy_inline_4k` | 900,000 | 44.282s | 34.507s | 14.205s | 22.684s |
| `string_heavy_edge_7k` | 900,000 | 71.744s | 61.068s | 21.635s | 42.350s |
| `string_heavy_lob_9k` | 300,000 | 112.065s | 111.233s | 111.287s | 115.792s |

The transport split repeats for the two non-LOB cases:

| Scenario | `direct-raw` vs `direct-framed` | `direct-raw` vs `odbc-bcp` |
|---|---:|---:|
| `string_heavy_inline_4k` | 2.43x faster | 1.60x faster |
| `string_heavy_edge_7k` | 2.82x faster | 1.96x faster |

That split disappears at `string_heavy_lob_9k`. All four bulk paths take about
111s to 116s for 300,000 rows in this run.

## Direct Profile Split

For the two non-LOB scenarios, the direct encoders remain close while the send
path separates sharply.

| Scenario | Backend | `append_encode` | `send_total` | `send_without_append_encode` | Dominant transport detail |
|---|---|---:|---:|---:|---|
| `inline_4k` | `direct-framed` | 4.918s | 33.659s | 28.741s | framed connection encode 18.945s, flush 9.640s |
| `inline_4k` | `direct-raw` | 4.976s | 13.304s | 8.327s | direct packet `poll_write` pending 6.428s |
| `edge_7k` | `direct-framed` | 7.321s | 58.208s | 50.887s | framed connection encode 32.044s, flush 18.572s |
| `edge_7k` | `direct-raw` | 7.371s | 18.600s | 11.229s | direct packet `poll_write` pending 8.052s |

For `lob_9k`, direct encoding is still a small slice of the measured send time:

| Backend | `append_encode` | `send_total` | `send_without_append_encode` | Dominant transport detail |
|---|---:|---:|---:|---|
| `direct-framed` | 3.393s | 109.941s | 106.548s | framed connection flush 90.008s |
| `direct-raw` | 3.948s | 109.921s | 105.973s | direct packet `poll_write` pending 104.180s |

## LOB SQL Server Evidence

The LOB run converges on the same SQL Server storage and wait shape.

| Backend | `PREEMPTIVE_OS_FLUSHFILEBUFFERS` wait | Table page shape per repeat |
|---|---:|---|
| `baseline` | 90,916 ms | 99,957 in-row pages, 95,833 LOB pages |
| `direct-framed` | 91,247 ms | 99,957 in-row pages, 95,833 LOB pages |
| `direct-raw` | 90,763 ms | 99,957 in-row pages, 95,833 LOB pages |
| `odbc-bcp` | 87,930 ms | 99,957 in-row pages, 95,833 LOB pages |

Each LOB backend wrote three tables with 100,000 rows per table. Every table
snapshot reported:

```text
used_pages=195790
in_row_used_pages=99957
lob_used_pages=95833
row_overflow_used_pages=0
```

The profiled SQL Server database reported recovery model `FULL` for each run.
The Tiberius backends reported transaction policy `bulk writer request without
explicit benchmark transaction`. The ODBC BCP runner reported
`bcp_batch every 8192 rows plus bcp_done`.

## Conclusion

The repeated non-LOB data is enough to treat the transport question as answered
for now. For inline string-heavy rows below the LOB transition, the published
direct raw packet path remains materially faster than the framed path while the
direct encoding cost stays similar.

The next issue 82 investigation should move to SQL Server LOB bulk behavior.
At `lob_9k`, baseline, direct framed, direct raw, and BCP all show nearly the
same write time, the same table page shape, and very large
`PREEMPTIVE_OS_FLUSHFILEBUFFERS` waits. The next evidence should separate
recovery model and logging mode, transaction and batch boundaries, bulk options
such as table locking, and whether the LOB transition itself is the expected
throughput cliff for this table shape under the current managed SQL Server
setup.

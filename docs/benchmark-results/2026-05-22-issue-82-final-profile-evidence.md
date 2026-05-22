# Issue 82 final SQL Server profiling evidence

This records local release benchmark evidence collected near the end of issue
82. Treat it as development evidence, not a portable performance claim.

## Issue Scope

Issue 82 added opt-in SQL Server side profiling to `writer-bench compare`.
The profiler observes the active writer SQL Server session through a separate
connection while the measured write is running. The output includes request
state, waiting tasks, connection counters, session wait deltas, database file IO
deltas, and table page snapshots.

Raw machine-readable sample output was mentioned as optional future scope in
the issue text. It is not implemented in this slice.

## Code State

- arrow-tiberius branch: `feat/82-sqlserver-writer-profile`
- tiberius-raw-bulk dependency: published `0.12.3-raw-bulk.11`
- Container runtime: `podman version 5.8.2`
- SQL Server image: `mcr.microsoft.com/mssql/server:2017-latest`
- ODBC runner image: `arrow-tiberius-odbc-runner:local`
- Rust: `rustc 1.93.0 (254b59607 2026-01-19)`

## Normal Variable-Width Compare

These are normal high-level writer compares. They did not enable SQL Server
profiling, recovery-model overrides, or table-lock overrides.

Command shape:

```bash
cargo run --release -p xtask -- writer-bench compare \
  --container-runtime podman \
  --scenario string_heavy \
  --rows 300000 \
  --batch-size 8192 \
  --repeat 1 \
  --backends baseline,direct-raw,arrow-odbc,odbc-bcp \
  --keep-runner-image
```

Full raw logs:

```text
target/bench-results/issue-82-current-string-heavy-release-300k-repeat1-20260522.log
target/bench-results/issue-82-current-string-heavy-text-only-release-300k-repeat1-20260522.log
target/bench-results/issue-82-current-string-heavy-binary-only-release-300k-repeat1-20260522.log
```

Results with the current default TDS packet size:

| Scenario | `baseline` | `direct-raw` | `arrow-odbc` | `odbc-bcp` | Interpretation |
|---|---:|---:|---:|---:|---|
| `string_heavy` | 35.079s | 32.947s | 21.740s | 32.484s | `arrow-odbc` remains faster |
| `string_heavy_text_only` | 9.464s | 7.782s | 14.172s | 9.357s | `direct-raw` is fastest |
| `string_heavy_binary_only` | 10.076s | 6.864s | 22.541s | 8.598s | `direct-raw` is fastest |

The issue 67 release comparison used `--tds-packet-size 32767`, so the same
normal runs were repeated with that setting:

```text
target/bench-results/issue-82-current-string-heavy-release-300k-repeat1-packet32767-20260522.log
target/bench-results/issue-82-current-string-heavy-text-only-release-300k-repeat1-packet32767-20260522.log
target/bench-results/issue-82-current-string-heavy-binary-only-release-300k-repeat1-packet32767-20260522.log
```

| Scenario | `baseline` | `direct-raw` | `arrow-odbc` | `odbc-bcp` | Interpretation |
|---|---:|---:|---:|---:|---|
| `string_heavy` | 34.399s | 28.589s | 24.682s | 35.832s | `arrow-odbc` remains faster, but the gap is smaller |
| `string_heavy_text_only` | 8.775s | 7.197s | 14.189s | 9.173s | `direct-raw` is fastest |
| `string_heavy_binary_only` | 9.923s | 10.510s | 22.235s | 8.619s | `odbc-bcp` is fastest; this packet size hurts `direct-raw` here |

The old issue 67 `string_heavy` run with `--tds-packet-size 32767` had
`direct-raw` at 28.483s and `arrow-odbc` at 22.375s. The current combined
large string/binary result is still not reversed, but the gap is small enough
that it should not block moving to the next type implementation. The split
scenarios show the direct raw path is now strong for text-heavy and normal
binary-heavy cases.

## LOB Logging Diagnosis

The SQL Server profiler showed that `string_heavy_lob_9k` converged across
`baseline`, `direct-framed`, `direct-raw`, and `odbc-bcp` under the default
managed SQL Server setup. The shared profile shape was:

- recovery model: `FULL`
- table page shape: about 99,957 in-row pages and 95,833 LOB pages per 100,000
  rows
- dominant wait: `PREEMPTIVE_OS_FLUSHFILEBUFFERS`
- log bytes: about 1.09 GB for one 100,000-row BCP run

BCP transaction boundaries alone did not explain the cliff:

| BCP Mode | Write | Flush Wait | Log Bytes |
|---|---:|---:|---:|
| `bcp_batch` every 8192 rows | 44.277s | 29.678s | 1.094 GB |
| Defer all rows to `bcp_done` | 41.254s | 29.440s | 1.094 GB |

Table locking alone under `FULL` recovery also did not remove the logging
pressure:

| Mode | Write | Flush Wait | Log Bytes |
|---|---:|---:|---:|
| `FULL`, no table lock | 44.277s | 29.678s | 1.094 GB |
| `FULL`, table lock | 58.826s | 45.500s | 1.036 GB |

`SIMPLE` recovery plus the SQL Server table option `table lock on bulk load`
changed the LOB case sharply:

| Mode | Write | Flush Wait | Log Bytes |
|---|---:|---:|---:|
| `FULL`, no table lock | 44.277s | 29.678s | 1.094 GB |
| `SIMPLE`, table lock | 5.115s | 0.164s | 30.7 MB |

The table still had the same LOB page shape:

```text
in_row_used_pages=99957
lob_used_pages=95833
row_overflow_used_pages=0
```

Under `SIMPLE` recovery plus table lock, all four backends improved:

| Backend | Write |
|---|---:|
| `direct-raw` | 2.774s |
| `odbc-bcp` | 3.812s |
| `direct-framed` | 6.304s |
| `baseline` | 7.716s |

Full raw logs:

```text
target/bench-results/issue-82-lob-bcp-batch-release-100k-repeat1-20260522.log
target/bench-results/issue-82-lob-bcp-defer-release-100k-repeat1-20260522.log
target/bench-results/issue-82-lob-bcp-full-table-lock-release-100k-repeat1-20260522.log
target/bench-results/issue-82-lob-bcp-simple-table-lock-release-100k-repeat1-20260522.log
target/bench-results/issue-82-lob-all-simple-table-lock-release-100k-repeat1-20260522.log
```

## Conclusion

The issue 82 profiler answered the intended question. For non-LOB
variable-width payloads, the remaining direct raw performance gap is no longer
a broad reason to block type coverage. For LOB payloads, the original cliff was
primarily SQL Server bulk logging and flush behavior under `FULL` recovery,
not a direct encoder failure.

The next type implementation can proceed. Remaining performance work should be
tracked as narrower follow-up work, especially:

- combined large text and binary rows where `arrow-odbc` is still faster;
- packet-size behavior for binary-heavy rows;
- user-facing guidance for bulk-load logging conditions when users control SQL
  Server recovery model and table options.

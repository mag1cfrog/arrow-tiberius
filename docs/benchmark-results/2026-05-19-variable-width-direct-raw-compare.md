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

Performance-sensitive runs should use a release-built xtask:

```sh
cargo run --release -p xtask -- writer-bench compare ...
```

Using `cargo xtask` directly runs the benchmark harness in debug mode. For this
benchmark family, debug mode materially distorts the internal Rust writer
timings.

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

Additional #81 append-buffer rerun:

After publishing `tiberius-raw-bulk` `0.12.3-raw-bulk.4`, the direct writer
was rerun with `BulkLoadRequest::send_raw_rows_with`, so encoded row ranges are
appended directly into the Tiberius bulk-load request buffer for variable-width
direct plans instead of first building a separate range payload and then copying
it into Tiberius.

```sh
cargo xtask writer-bench compare \
  --container-runtime podman \
  --scenario string_heavy \
  --rows 300000 \
  --batch-size 8192 \
  --repeat 1 \
  --backends direct-raw
```

| Backend | Rows | Write | Finish | Total | Rows/sec | Peak RSS KiB | Peak RSS MiB |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| `direct-raw` append-buffer rerun | 300,000 | 120.400s | 0.005s | 120.476s | 2,491.59 | 104,996 | 102.54 |

Compared with the original direct row in this document, the append-buffer path
improved `string_heavy` throughput by about 13.5 percent and reduced peak RSS by
about 49.6 percent on this local rerun. Since this was not a full four-backend
rerun, the original table remains the fair same-run comparison across backends.

Profiled #81 append-buffer rerun:

After adding the optional direct writer profiler, the same direct-only benchmark
shape was rerun to separate direct encoding time from time spent below the
encoder in Tiberius packet writes, network I/O, and SQL Server ingestion.

```sh
cargo xtask writer-bench compare \
  --container-runtime podman \
  --scenario string_heavy \
  --rows 300000 \
  --batch-size 8192 \
  --repeat 1 \
  --backends direct-raw \
  --profile-direct
```

| Backend | Rows | Write | Finish | Total | Rows/sec | Peak RSS KiB | Peak RSS MiB |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| `direct-raw` profiled append-buffer rerun | 300,000 | 118.161s | 0.005s | 118.232s | 2,538.81 | 105,516 | 103.04 |

Direct profile:

| Phase or counter | Value |
| --- | ---: |
| `measure_batch` | 8.855s |
| `row_range_split` | 0.004s |
| `append_encode` | 22.094s |
| `send_total` | 108.583s |
| `send_without_append_encode` | 86.490s |
| rows | 300,000 |
| batches | 37 |
| row ranges | 256 |
| encoded bytes | 1,883,800,622 |
| max row range bytes | 8,388,567 |
| nvarchar UTF-16 bytes | 950,022,348 |
| varbinary bytes | 902,553,858 |
| null cells | 34,448 |

The profiler shows that the largest measured cost is below the direct encoder:
`send_without_append_encode` was about 73 percent of the total write window.
The next `string_heavy` optimization should therefore focus on Tiberius raw bulk
packet write behavior, PLP chunking shape, packet sizing, or SQL Server ingestion
behavior before spending more time on local UTF-16 encoding micro-optimizations.

After publishing `tiberius-raw-bulk` `0.12.3-raw-bulk.5`, the same direct-only
profile was rerun with packet statistics from `BulkLoadRequest`.

```sh
cargo xtask writer-bench compare \
  --container-runtime podman \
  --scenario string_heavy \
  --rows 300000 \
  --batch-size 8192 \
  --repeat 1 \
  --backends direct-raw \
  --profile-direct
```

| Backend | Rows | Write | Finish | Total | Rows/sec | Peak RSS KiB | Peak RSS MiB |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| `direct-raw` packet-profile rerun | 300,000 | 121.155s | 0.005s | 121.229s | 2,476.07 | 105,756 | 103.28 |

Direct packet profile:

| Phase or counter | Value |
| --- | ---: |
| `measure_batch` | 9.147s |
| `row_range_split` | 0.005s |
| `append_encode` | 22.404s |
| `send_total` | 111.260s |
| `send_without_append_encode` | 88.855s |
| rows | 300,000 |
| batches | 37 |
| row ranges | 256 |
| encoded bytes | 1,883,800,622 |
| max row range bytes | 8,388,567 |
| nvarchar UTF-16 bytes | 950,022,348 |
| varbinary bytes | 902,553,858 |
| null cells | 34,448 |
| packet write calls | 257 |
| packets written before finalization | 460,812 |
| packet payload bytes before finalization | 1,883,799,456 |
| max packet payload bytes | 4,088 |
| max buffered bytes before write | 8,392,264 |
| buffered bytes after last write | 1,365 |
| finalized packet payload bytes | 1,365 |

The packet profile explains the large `send_without_append_encode` window more
directly: this path writes 460,812 bulk-load packets with a maximum payload of
4,088 bytes. The next optimization should therefore investigate packet-size
negotiation or packet coalescing in the Tiberius fork before changing the Arrow
encoder again.

After publishing `tiberius-raw-bulk` `0.12.3-raw-bulk.6`, the same profile was
rerun with a larger requested TDS packet size.

```sh
cargo xtask writer-bench compare \
  --container-runtime podman \
  --scenario string_heavy \
  --rows 300000 \
  --batch-size 8192 \
  --repeat 1 \
  --backends direct-raw \
  --profile-direct \
  --tds-packet-size 32767
```

| Backend | Rows | Write | Finish | Total | Rows/sec | Peak RSS KiB | Peak RSS MiB |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| `direct-raw` packet-size rerun | 300,000 | 117.129s | 0.006s | 117.207s | 2,561.14 | 105,368 | 102.90 |

Direct packet-size profile:

| Phase or counter | Value |
| --- | ---: |
| `measure_batch` | 9.119s |
| `row_range_split` | 0.004s |
| `append_encode` | 22.092s |
| `send_total` | 107.171s |
| `send_without_append_encode` | 85.079s |
| rows | 300,000 |
| batches | 37 |
| row ranges | 256 |
| encoded bytes | 1,883,800,622 |
| max row range bytes | 8,388,567 |
| nvarchar UTF-16 bytes | 950,022,348 |
| varbinary bytes | 902,553,858 |
| null cells | 34,448 |
| packet write calls | 257 |
| packets written before finalization | 57,842 |
| packet payload bytes before finalization | 1,883,798,256 |
| max packet payload bytes | 32,568 |
| max buffered bytes before write | 8,420,360 |
| buffered bytes after last write | 2,565 |
| finalized packet payload bytes | 2,565 |

Requesting a larger TDS packet size reduced complete packet writes by about
87.4 percent and raised max packet payload from 4,088 bytes to 32,568 bytes.
However, write time only improved by about 3.3 percent compared with the
packet-profile rerun above. That suggests packet count was part of the cost, but
not the dominant remaining gap. The next investigation should focus on the
server-ingestion side of `send_without_append_encode`, Tiberius flush/write
behavior, or PLP/max-type encoding shape rather than assuming packet size alone
can close the gap.

After publishing `tiberius-raw-bulk` `0.12.3-raw-bulk.7`, the same packet-size
profile was rerun with lower-level bulk write timing statistics.

```sh
cargo xtask writer-bench compare \
  --container-runtime podman \
  --scenario string_heavy \
  --rows 300000 \
  --batch-size 8192 \
  --repeat 1 \
  --backends direct-raw \
  --profile-direct \
  --tds-packet-size 32767
```

| Backend | Rows | Write | Finish | Total | Rows/sec | Peak RSS KiB | Peak RSS MiB |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| `direct-raw` write-timing rerun | 300,000 | 119.703s | 0.006s | 119.795s | 2,506.08 | 105,096 | 102.63 |

Direct write timing profile:

| Phase or counter | Value |
| --- | ---: |
| `measure_batch` | 9.282s |
| `row_range_split` | 0.005s |
| `append_encode` | 22.622s |
| `send_total` | 109.494s |
| `send_without_append_encode` | 86.873s |
| rows | 300,000 |
| batches | 37 |
| row ranges | 256 |
| encoded bytes | 1,883,800,622 |
| max row range bytes | 8,388,567 |
| nvarchar UTF-16 bytes | 950,022,348 |
| varbinary bytes | 902,553,858 |
| null cells | 34,448 |
| packet write calls | 257 |
| packets written before finalization | 57,842 |
| packet payload bytes before finalization | 1,883,798,256 |
| max packet payload bytes | 32,568 |
| max buffered bytes before write | 8,420,360 |
| buffered bytes after last write | 2,565 |
| finalized packet payload bytes | 2,565 |
| bulk `write_packets` elapsed | 86.861s |
| bulk `write_to_wire` calls | 57,843 |
| bulk `write_to_wire` elapsed | 86.824s |
| bulk `write_to_wire` payload bytes | 1,883,800,821 |
| bulk max `write_to_wire` elapsed | 0.004s |
| bulk max `write_to_wire` payload bytes | 32,568 |
| bulk flush calls | 1 |
| bulk flush elapsed | 0.000s |
| bulk max flush elapsed | 0.000s |
| bulk finalize elapsed | 0.004s |
| bulk finalize `write_to_wire` elapsed | 0.000s |
| bulk finalize flush elapsed | 0.000s |
| bulk finalize result elapsed | 0.004s |

The lower-level timing shows that nearly all remaining
`send_without_append_encode` time is spent awaiting `write_to_wire` calls.
Finalization and explicit flush time are effectively noise in this run. That
makes the next useful investigation narrower: packet payloads are already large,
but this path still performs 57,843 awaited writes for 1.88 GB of row payload.
Future optimization should measure whether Tiberius can safely reduce awaited
write calls, change PLP/max-type chunking, or stream larger contiguous writes to
SQL Server without disturbing packet framing.

After publishing `tiberius-raw-bulk` `0.12.3-raw-bulk.8`, the same packet-size
profile was rerun with connection write timing split below `write_to_wire`.

```sh
cargo xtask writer-bench compare \
  --container-runtime podman \
  --scenario string_heavy \
  --rows 300000 \
  --batch-size 8192 \
  --repeat 1 \
  --backends direct-raw \
  --profile-direct \
  --tds-packet-size 32767
```

| Backend | Rows | Write | Finish | Total | Rows/sec | Peak RSS KiB | Peak RSS MiB |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| `direct-raw` connection-write profile rerun | 300,000 | 117.940s | 0.006s | 118.026s | 2,543.53 | 105,272 | 102.80 |

Connection write timing profile:

| Phase or counter | Value |
| --- | ---: |
| `measure_batch` | 9.208s |
| `row_range_split` | 0.005s |
| `append_encode` | 22.510s |
| `send_total` | 107.724s |
| `send_without_append_encode` | 85.214s |
| rows | 300,000 |
| batches | 37 |
| row ranges | 256 |
| encoded bytes | 1,883,800,622 |
| max row range bytes | 8,388,567 |
| nvarchar UTF-16 bytes | 950,022,348 |
| varbinary bytes | 902,553,858 |
| null cells | 34,448 |
| packet write calls | 257 |
| packets written before finalization | 57,842 |
| packet payload bytes before finalization | 1,883,798,256 |
| max packet payload bytes | 32,568 |
| max buffered bytes before write | 8,420,360 |
| buffered bytes after last write | 2,565 |
| finalized packet payload bytes | 2,565 |
| bulk `write_packets` elapsed | 85.204s |
| bulk `write_to_wire` calls | 57,843 |
| bulk `write_to_wire` elapsed | 85.176s |
| bulk `write_to_wire` payload bytes | 1,883,800,821 |
| bulk max `write_to_wire` elapsed | 0.003s |
| bulk max `write_to_wire` payload bytes | 32,568 |
| bulk flush calls | 1 |
| bulk flush elapsed | 0.000s |
| bulk finalize elapsed | 0.004s |
| bulk connection write calls | 57,843 |
| bulk connection write payload bytes | 1,883,800,821 |
| bulk connection write ready elapsed | 0.005s |
| bulk connection write encode elapsed | 84.413s |
| bulk connection write flush elapsed | 0.743s |
| bulk connection write max encode elapsed | 0.003s |
| bulk connection write max payload bytes | 32,568 |

The deeper split shows that the framed sink is not primarily waiting for
readiness or flush. The dominant measured cost is packet encode/start-send into
the framed sink: 84.413s of the 85.176s `write_to_wire` window. The next useful
optimization target is therefore the Tiberius packet serialization path itself,
especially avoiding or reducing the packet payload copy from the bulk-load
buffer into the framed sink buffer. A direct bulk packet write path that writes
header and payload bytes to the underlying stream without re-copying every
packet should be investigated before more Arrow-side encoding changes.

The same profile was then rerun through a release-built xtask to avoid making
optimization decisions from debug-mode timings.

```sh
cargo run --release -p xtask -- writer-bench compare \
  --container-runtime podman \
  --scenario string_heavy \
  --rows 300000 \
  --batch-size 8192 \
  --repeat 1 \
  --backends direct-raw \
  --profile-direct \
  --tds-packet-size 32767
```

| Backend | Rows | Write | Finish | Total | Rows/sec | Peak RSS KiB | Peak RSS MiB |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| `direct-raw` release connection-write profile rerun | 300,000 | 35.382s | 0.339s | 35.793s | 8,398.43 | 100,912 | 98.55 |

Release connection write timing profile:

| Phase or counter | Value |
| --- | ---: |
| `measure_batch` | 0.155s |
| `row_range_split` | 0.000s |
| `append_encode` | 2.479s |
| `send_total` | 34.496s |
| `send_without_append_encode` | 32.017s |
| bulk `write_packets` elapsed | 32.015s |
| bulk `write_to_wire` elapsed | 32.009s |
| bulk max `write_to_wire` elapsed | 1.679s |
| bulk finalize elapsed | 0.336s |
| bulk finalize result elapsed | 0.336s |
| bulk connection write ready elapsed | 0.001s |
| bulk connection write encode elapsed | 9.083s |
| bulk connection write flush elapsed | 22.917s |
| bulk connection write max flush elapsed | 1.679s |

Release mode changes the interpretation. Packet encode/start-send is still
meaningful, but it is not the dominant phase under optimization. The largest
measured cost is now framed sink flush: 22.917s of the 32.009s
`write_to_wire` window. The next fork experiment should therefore focus on
changing bulk packet flushing/write behavior with release-mode benchmarks,
rather than assuming the debug-mode packet encode cost is representative.

After updating the ODBC runner commands to use `cargo run --release` inside the
runner container as well, the four-backend `string_heavy` compare was rerun with
optimized internal and external benchmark binaries.

```sh
cargo run --release -p xtask -- writer-bench compare \
  --container-runtime podman \
  --scenario string_heavy \
  --rows 300000 \
  --batch-size 8192 \
  --repeat 1 \
  --backends baseline,direct-raw,arrow-odbc,odbc-bcp \
  --tds-packet-size 32767
```

| Backend | Rows | Write | Finish | Total | Rows/sec | Peak RSS KiB | Peak RSS MiB |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| `baseline` | 300,000 | 34.158s | 0.293s | 34.523s | 8,708.13 | 96,696 | 94.43 |
| `direct-raw` | 300,000 | 28.259s | 0.275s | 28.597s | 10,513.81 | 100,916 | 98.55 |
| `arrow-odbc` | 300,000 | 20.886s | n/a | n/a | 14,363.69 | 1,453,492 | 1,419.43 |
| `odbc-bcp` | 300,000 | 36.375s | n/a | n/a | 8,247.42 | 50,524 | 49.34 |

Release relative rows/sec:

| Comparison | Ratio |
| --- | ---: |
| `direct-raw` / `baseline` | 1.21x |
| `arrow-odbc` / `direct-raw` | 1.37x |
| `direct-raw` / `odbc-bcp` | 1.27x |

In optimized builds, `direct-raw` is materially faster than the baseline
TokenRow writer and the benchmark-only ODBC BCP runner for this workload. It is
still slower than `arrow-odbc` on large string/binary payloads, while using far
less memory than `arrow-odbc`.

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
The #81 follow-up reduced one large-copy source by appending variable-width
encoded row ranges directly into the Tiberius request buffer. For large
`nvarchar(max)` and `varbinary(max)` payloads, future work should still
investigate:

- Tiberius raw bulk packet write behavior and packet sizing;
- whether PLP chunking strategy affects SQL Server bulk-load throughput;
- SQL Server ingestion behavior for PLP-heavy raw bulk rows;
- more efficient UTF-16 staging for `nvarchar(max)` after lower-level send costs
  are better understood.

This benchmark supports keeping the direct raw path, but it also shows that
large variable-width payloads need targeted optimization before claiming broad
superiority over ODBC-backed writers.

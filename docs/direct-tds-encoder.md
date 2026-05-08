# Direct TDS Bulk Encoder Design

## Scope

This document records the v0.1 route for an optimized direct
Arrow-to-TDS bulk writer backend.

It is a design document for issue #8. It does not implement the encoder, the
baseline writer, the forked Tiberius package, or benchmark code. Later issues
should be able to implement #9 and #13 from this document without re-opening the
driver-boundary decision.

The decision is:

- use Tiberius for connection ownership, authentication, TLS, metadata queries,
  packet framing, flushing, finalization, and server result handling;
- publish and depend on one minimal forked Tiberius package for v0.1;
- add a narrow raw bulk-row extension point to that fork;
- keep `arrow-tiberius` focused on Arrow schema planning, value conversion,
  direct bulk-row encoding, diagnostics, and backend selection;
- do not implement a local SQL Server/TDS driver in `arrow-tiberius` v0.1.

## Source Inventory

Project sources:

- Issue #8: <https://github.com/mag1cfrog/arrow-tiberius/issues/8>
- Issue #9: <https://github.com/mag1cfrog/arrow-tiberius/issues/9>
- Issue #10: <https://github.com/mag1cfrog/arrow-tiberius/issues/10>
- Issue #13: <https://github.com/mag1cfrog/arrow-tiberius/issues/13>
- Evidence matrix: `docs/evidence-matrix.md`
- Public API boundary: `docs/api-boundary.md`
- Integration test harness: `docs/integration-tests.md`

External sources already captured in the evidence matrix:

- Tiberius 0.12.3 docs:
  <https://docs.rs/tiberius/0.12.3/tiberius/>
- `Client::bulk_insert`:
  <https://docs.rs/tiberius/0.12.3/tiberius/struct.Client.html#method.bulk_insert>
- `BulkLoadRequest`:
  <https://docs.rs/tiberius/0.12.3/tiberius/struct.BulkLoadRequest.html>
- `TokenRow`:
  <https://docs.rs/tiberius/0.12.3/tiberius/struct.TokenRow.html>
- Tiberius issue #397:
  <https://github.com/prisma/tiberius/issues/397>

## Baseline Bulk Path

The baseline writer should use the existing public Tiberius bulk-load API:

```rust
let mut bulk = client.bulk_insert(table).await?;
bulk.send(row).await?;
bulk.finalize().await?;
```

That path is enough for a correctness baseline because `TokenRow` and
`ColumnData` are public. `arrow-tiberius` can convert one Arrow row at a time
into `ColumnData` values and send each row through `BulkLoadRequest::send`.

The baseline path is intentionally row-oriented:

1. inspect an Arrow `RecordBatch`;
2. validate the batch against the planned schema;
3. for each row, build a `TokenRow`;
4. for each planned column, extract the Arrow value and convert it to the
   matching Tiberius `ColumnData`;
5. call `BulkLoadRequest::send(TokenRow)`;
6. call `BulkLoadRequest::finalize()` exactly once.

This path should be the correctness reference for direct encoding, but it is
not expected to be the final high-throughput path.

## Current Public API Gap

The direct backend wants to encode Arrow arrays into complete TDS bulk row
payloads and pass those payloads to Tiberius for packet handling.

Current upstream Tiberius public APIs are not sufficient for that:

- `BulkLoadRequest::send` only accepts `TokenRow`.
- `BulkLoadRequest` does not expose destination bulk column metadata.
- The packet buffer and packet-size flushing path are private.
- The connection write path is private.
- Internal metadata types such as detailed type information, precision, scale,
  length, and collation are not exposed as a stable direct-encoding surface.

Public `ColumnType` is too coarse for direct row encoding. A direct encoder
needs enough metadata to match the server destination columns exactly, including
length, precision, scale, variable-length handling, nullability behavior, and
the TDS type chosen by Tiberius' `bulk_insert` setup.

Therefore #9 should not try to implement direct raw bulk encoding against
upstream Tiberius 0.12.3 alone.

## Decision

`arrow-tiberius` should use a single published forked Tiberius package for both
baseline and direct writer backends.

The fork should be minimal and bulk-load specific. It should expose only the
extension points needed by an Arrow-aware raw row encoder:

```rust
let mut bulk = client.bulk_insert(table).await?;
let columns = bulk.columns();
bulk.send_raw_row(encoded_row).await?;
bulk.finalize().await?;
```

The exact names are owned by #13. The required semantics are:

- `columns()` or equivalent returns a read-only destination metadata view after
  `bulk_insert` has initialized the bulk request.
- `send_raw_row(...)` accepts one already-encoded bulk row payload, not an
  arbitrary SQL packet.
- `send_raw_row(...)` writes through the same packet buffer, packet splitting,
  flushing, and connection state machinery that `send(TokenRow)` uses.
- `finalize()` remains owned by Tiberius and is still required exactly once.

The fork should not expose raw connections, packet ids, token stream internals,
or general-purpose TDS buffer machinery unless implementation proves that the
narrow bulk-row API cannot work.

## Fork Package Requirements For #13

#13 owns the package name, versioning, publication, and maintenance workflow.
This issue defines the API constraints that #13 must satisfy.

The fork package should:

- be registry-published before `arrow-tiberius` itself is publishable;
- preserve Tiberius license and attribution;
- remain close to upstream Tiberius;
- document its upstream base version and patch set;
- expose one Tiberius client family used by both baseline and direct backends;
- support dependency aliasing so `arrow-tiberius` can import it as `tiberius`
  if the final package name differs.

Illustrative dependency shape:

```toml
[dependencies]
tiberius = { package = "tiberius-arrow", version = "0.12.3-arrow.1" }
```

The package name and version above are placeholders. The important requirement
is that `arrow-tiberius` does not expose both upstream and forked Tiberius
client types.

## Direct Encoder Inputs

The direct backend should consume already-planned schema decisions. It should
not remap Arrow types or SQL Server types internally.

Inputs needed by the direct encoder:

- Arrow `RecordBatch` or batch stream.
- The shared schema/write planning model from #6:
  - `MssqlTablePlan`;
  - `SchemaMapping`;
  - `ArrowFieldPlan`;
  - `MssqlColumnPlan`;
  - `MssqlType`;
  - write policy options that affect value-level validation.
- Tiberius destination bulk metadata from the forked `BulkLoadRequest`.
- Backend options controlling direct-only, baseline-only, or auto behavior.

The direct encoder must compare planned MSSQL columns against destination bulk
metadata before encoding. If the destination metadata does not match the plan,
the backend should fail with structured diagnostics rather than trying to encode
against unexpected server metadata.

## First Direct Type Set

#9 should start with a deliberately narrow type set. The goal is to prove the
architecture and parity strategy, not to support every planned type immediately.

Recommended first direct set:

| Arrow source | Planned SQL Server target | Notes |
| --- | --- | --- |
| `Boolean` | `bit` | Include null handling. |
| `Int32` | `int` | Exact, common, simple fixed-width encoding. |
| `Int64` | `bigint` | Exact fixed-width encoding. |
| `Float64` | `float(53)` | Finite values only under current policy. |
| `Utf8` | `nvarchar(max)` or `nvarchar(n)` | Include empty, ASCII, non-ASCII, and null values. |
| `Binary` | `varbinary(max)` or `varbinary(n)` | Include empty, short, and null values. |

Types to defer from the first direct prototype unless implementation evidence
makes them low-risk:

- decimals;
- dates and timestamps;
- `UInt64` policy-dependent targets;
- `Float32`;
- observed-length string/binary policies;
- `LargeUtf8` and `LargeBinary` if large-value chunking needs additional work.

The baseline writer may support a wider type set earlier. The direct backend
should clearly report unsupported planned columns or fall back only when backend
policy explicitly allows fallback.

## Backend Selection And Fallback

The public writer workflow should not change based on backend choice. Backend
selection should be an option, not a separate user workflow.

Conceptual backend policy:

```rust
pub enum WriteBackend {
    Auto,
    BaselineTokenRow,
    DirectRawBulk,
}
```

Behavior:

- `BaselineTokenRow`: always use the row-oriented Tiberius `TokenRow` path.
- `DirectRawBulk`: require the direct backend; fail if any planned column is
  unsupported by the direct encoder.
- `Auto`: use direct encoding only when the full plan is supported by the
  direct backend; otherwise fall back to baseline with an explicit diagnostic.

Fallback must not be silent. If `Auto` falls back, callers should be able to
see why, because otherwise benchmark and production behavior can diverge
without explanation.

## Parity Strategy

The baseline writer is the correctness reference. The direct backend must prove
parity before performance claims are meaningful.

Parity tests should use the #10 harness:

1. create the same target table shape;
2. write the same Arrow batch through the baseline backend;
3. write the same Arrow batch through the direct backend;
4. read back row counts and values through SQL Server;
5. compare results for every supported direct type;
6. include null cases for every supported direct type.

Initial parity cases:

- booleans: `true`, `false`, `NULL`;
- integers: min, max, zero, `NULL`;
- float64: finite values and `NULL`;
- strings: empty, ASCII, non-ASCII, long-enough-to-check-length, `NULL`;
- binary: empty, short bytes, `NULL`.

Unsupported direct-backend types should have tests that verify clear
diagnostics or explicit baseline fallback according to selected backend policy.

## Publication Boundary

A crates.io-published `arrow-tiberius` cannot depend on a Git-only fork. The
forked Tiberius package must be a real dependency with a versioned registry
release before `arrow-tiberius` is publishable.

The fork should be treated as an owned dependency, not a hidden patch:

- document package name and version;
- document upstream base commit or version;
- preserve upstream license notices;
- document local patch purpose;
- track which upstream fixes are included;
- keep the raw-row API narrow enough that upstreaming remains possible later.

Upstreaming can be considered later, but v0.1 should not block on upstream
review.

## Rejected Option: Upstream Tiberius Only

Using upstream Tiberius alone is rejected for the direct backend because the
public API does not expose destination bulk metadata or raw row sending.

The upstream-only path remains valid for the baseline writer.

## Rejected Option: Local SQL Server Driver

Implementing a local TDS bulk driver inside `arrow-tiberius` is rejected for
v0.1.

That route would make this crate responsible for connection lifecycle,
authentication, TLS, packet sizing, token stream behavior, metadata discovery,
bulk-load setup, finalization, and server response handling. Those are driver
responsibilities already owned by Tiberius.

The direct encoder should own Arrow-aware row payload encoding only.

## Risks

- The fork API may expose too much Tiberius internals if #13 is not kept narrow.
- Tiberius internal metadata may change across upstream releases.
- Direct encoding can be faster but wrong if metadata parity checks are weak.
- Silent fallback can hide performance regressions.
- Carrying a fork creates maintenance work even if the patch is small.
- Decimal and temporal encoding edge cases are easy to corrupt silently; they
  should not be included in the first direct prototype without focused tests.

## Follow-Up Issue Boundaries

- #7 implements the baseline `TokenRow` writer and value conversion API.
- #9 implements the first direct raw-bulk encoder prototype.
- #10 owns the integration harness and parity tests as implementations become
  available.
- #13 publishes and maintains the forked Tiberius package API required here.

This document should be updated only when those later issues discover that the
API shape described here is insufficient.

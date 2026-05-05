# v0.1 Public API Boundary

## Scope

This document designs the public API boundary for `arrow-tiberius` v0.1.
The v0.1 crate writes caller-supplied Arrow batches into SQL Server through a
caller-supplied Tiberius client. The design should keep enough namespace and
conceptual room for future SQL Server-to-Arrow reads without implementing reads
in v0.1.

This is a design document, not an implementation. It should be detailed enough
for later issues to add public types, planner logic, DDL rendering, baseline
writing, and direct raw-bulk encoding without revisiting the crate boundary.

## Goals

- Define public module names and responsibilities.
- Define the intended public type surface for SQL Server profiles, identifiers,
  planning, policies, DDL rendering, writing, diagnostics, and errors.
- Keep the crate identity broader than a writer-only package.
- Make the write path usable with caller-owned Tiberius clients and
  caller-owned Arrow data.
- Reserve future read-side placement without implementing read behavior.
- Document how `arrow-tiberius` should depend on the forked Tiberius package as
  its single SQL Server/TDS driver dependency.

## Non-Goals

- Implementing Arrow-to-SQL Server type mappings.
- Implementing DDL rendering.
- Implementing Tiberius writes.
- Implementing direct TDS bulk encoding.
- Implementing SQL Server-to-Arrow reads.
- Adding Arrow, Tiberius, futures, or runtime dependencies.
- Adding connection pooling, CLI, config, secrets, Delta, S3, scheduling, or
  job-level retry behavior.

## Source Inventory

Primary project sources:

- Issue #4, public API boundary for write path and future read path:
  <https://github.com/mag1cfrog/arrow-tiberius/issues/4>
- Issue #3, evidence matrix for Arrow, SQL Server/TDS, Tiberius, and ODBC:
  <https://github.com/mag1cfrog/arrow-tiberius/issues/3>
- Issue #13, published Tiberius fork package for raw bulk support:
  <https://github.com/mag1cfrog/arrow-tiberius/issues/13>
- Evidence matrix in this repository: `docs/evidence-matrix.md`
- Crate root documentation: `src/lib.rs`
- Workspace/package metadata: `Cargo.toml`

Relevant decisions already captured in project sources:

- `arrow-tiberius` is a reusable library bridge, not a CLI or exporter runtime.
- The caller owns Tiberius client construction, connection lifecycle, optional
  pooling, configuration, secrets, source data acquisition, and job orchestration.
- The crate owns Arrow schema planning for SQL Server, DDL rendering, Arrow batch
  writing, backend selection, diagnostics, and conversion policy.
- v0.1 should not support two incompatible Tiberius client types.
- The forked Tiberius package should expose bulk metadata and raw-row sending
  while keeping Tiberius responsible for connection state, packet framing, auth,
  TLS, metadata querying, packet flushing, and finalization.

## Document Structure

The remaining sections should be filled in small reviewed steps:

1. Public module map.
2. SQL Server profile API.
3. Table name and identifier API.
4. Planning and DDL API.
5. Conversion policy API.
6. Writer API.
7. Batch and stream writing API.
8. Backend selection API.
9. Diagnostics and error API.
10. Write-time schema compatibility.
11. Forked Tiberius dependency strategy.
12. Feature flag posture.
13. Future read-side placement.
14. Deferred decisions.

## Candidate User Flow

The final design should support a flow equivalent to:

```rust
let profile = MssqlProfile::sql_server_2016_compat_100();
let plan = WritePlan::from_arrow_schema(schema, profile, PlanOptions::default())?;
let ddl = plan.create_table_sql(TableName::new("dbo", "target_table"))?;

let mut writer = BulkWriter::new(
    &mut client,
    TableName::new("dbo", "target_table"),
    plan,
    WriteOptions::default(),
)
.await?;

writer.write_batch(&batch).await?;
writer.finish().await?;
```

The design should also decide the shape of a stream convenience function
equivalent to:

```rust
write_record_batch_stream(&mut client, table, plan, stream, options).await?;
```

## Initial Design Constraints

- Prefer a design document over compiled public stubs in this issue.
- Do not add dependencies until an implementation issue needs them.
- Public plans should be immutable after successful construction.
- Planning may return warnings with a usable plan.
- Unsupported mappings and lossy mappings without an explicit policy should be
  planning errors.
- Write-time batch schema checks should be strict by default.
- The intended stream shape should be compatible with `futures_core::Stream`,
  but this issue should not add the dependency.
- Backend selection should include baseline, direct raw-bulk, and automatic
  selection concepts.
- The public API should expose one Tiberius-compatible client type, not both
  upstream and forked client types.
- Future read support should have reserved module placement without v0.1 read
  implementation.

## Public Module Map

The public module layout should separate concepts that may be shared by future
read support from write-only behavior:

```text
arrow_tiberius
├── diagnostic
├── error
├── identifier
├── profile
├── read
└── write
```

Recommended responsibilities:

- `diagnostic`: structured warnings and validation messages shared by planning,
  DDL, write, and future read APIs.
- `error`: crate-level `Error` and `Result` aliases.
- `identifier`: SQL Server identifier and table-name types, including quoting
  behavior.
- `profile`: SQL Server capability/profile types such as SQL Server 2016 with
  database compatibility level 100.
- `write`: v0.1 write planning, DDL rendering, writer options, writer types, and
  backend selection.
- `read`: reserved module for future SQL Server-to-Arrow APIs. It should be
  present only when read design or implementation starts; v0.1 does not need to
  expose an empty module.

Crate-level re-exports should favor the common write flow while keeping module
ownership clear:

```rust
pub use error::{Error, Result};
pub use identifier::TableName;
pub use profile::MssqlProfile;
pub use write::{BulkWriter, PlanOptions, WriteOptions, WritePlan};
```

Implementation-only code should stay under private modules inside `write` or
crate-private modules such as `ddl`, `encode`, or `tds` once implementation
issues need them. Those internal module names should not become public API by
accident.

## SQL Server Profile API

`MssqlProfile` should describe target SQL Server behavior that affects planning
and DDL generation. It is not a connection string, server probe result, or
runtime environment object.

Candidate public shape:

```rust
pub struct MssqlProfile {
    // private fields
}

impl MssqlProfile {
    pub fn sql_server_2016_compat_100() -> Self;
}
```

The initial profile should capture:

- SQL Server engine family: SQL Server 2016.
- Database compatibility level: 100.
- Default Unicode string posture: prefer `nvarchar` for Arrow UTF-8 strings.
- Temporal precision limits: `datetime2` and `datetimeoffset` precision 0
  through 7.
- Decimal limits: precision 1 through 38 and scale 0 through precision.
- Identifier limits: regular and delimited identifier length constraints
  relevant to SQL Server.

Planning implications:

- `MssqlProfile` should be copied into or referenced by `WritePlan` so the plan
  remains stable after construction.
- Profiles should be value objects with no network access.
- Future profiles may add newer SQL Server behavior, but v0.1 should not require
  runtime feature detection.

## Table Name And Identifier API

SQL Server identifiers need explicit handling because table names may be
schema-qualified, user data may contain reserved words or special characters,
and dot handling must not accidentally change object identity.

Candidate public types:

```rust
pub struct Identifier {
    // private fields
}

pub struct TableName {
    // private fields
}
```

Candidate constructors:

```rust
impl Identifier {
    pub fn new(name: impl Into<String>) -> Result<Self>;
    pub fn quoted_sql(&self) -> String;
    pub fn as_str(&self) -> &str;
}

impl TableName {
    pub fn new(schema: impl Into<String>, table: impl Into<String>) -> Result<Self>;
    pub fn unqualified(table: impl Into<String>) -> Result<Self>;
    pub fn quoted_sql(&self) -> String;
}
```

Design decisions:

- `TableName::new("dbo", "target_table")` should mean exactly two identifier
  parts: schema and table.
- Dots inside one identifier string should be treated as literal identifier
  characters and bracket-quoted, not parsed as multipart names.
- Bracket quoting should be the default rendering strategy for generated SQL.
- `]` inside an identifier should be escaped by doubling it when rendered inside
  brackets.
- Empty identifiers should be rejected.
- Over-length identifiers should be rejected during construction or planning
  with a structured diagnostic.
- v0.1 should not accept raw SQL table-name strings in the primary API. A later
  issue may add an unsafe/raw escape hatch if there is a concrete need.

Column names should use the same `Identifier` validation and quoting rules.
The Arrow field name remains the source identity, while the planned SQL column
identifier is the target identity after policy is applied.

## Planning And DDL API

Planning converts an Arrow schema plus SQL Server profile and policy options
into an immutable `WritePlan`. The plan should be reusable across batches with
the same expected schema.

Candidate public shape:

```rust
pub struct WritePlan {
    // private fields
}

pub struct PlannedColumn {
    // private fields
}

pub struct PlanOptions {
    // public builder or private fields with accessors
}

pub struct PlanOutcome {
    pub plan: WritePlan,
    pub diagnostics: Vec<Diagnostic>,
}
```

Candidate constructors and accessors:

```rust
impl WritePlan {
    pub fn from_arrow_schema(
        schema: impl Into<Arc<Schema>>,
        profile: MssqlProfile,
        options: PlanOptions,
    ) -> Result<PlanOutcome>;

    pub fn arrow_schema(&self) -> &Schema;
    pub fn profile(&self) -> &MssqlProfile;
    pub fn columns(&self) -> &[PlannedColumn];
    pub fn create_table_sql(&self, table: &TableName) -> Result<String>;
}
```

Design decisions:

- `WritePlan` should be immutable after construction.
- Planning should preserve Arrow field order as SQL column order unless a future
  issue adds explicit reordering.
- Planning should preserve source Arrow field identity for diagnostics.
- `PlanOutcome` should allow a usable plan with warnings.
- Unsupported Arrow types, invalid identifiers, incompatible precision/scale,
  and lossy conversions without explicit policy should be planning errors.
- DDL rendering should be a method on `WritePlan` or a small `DdlRenderer`
  wrapper over `WritePlan`; v0.1 can start with `WritePlan::create_table_sql`.
- DDL rendering should produce deterministic SQL text for reviewable tests.
- v0.1 should only design `CREATE TABLE` rendering. `DROP`, `MERGE`, `TRUNCATE`,
  migration, and schema-diff APIs are out of scope.

`PlannedColumn` should expose read-only information needed by users and later
writer implementations:

```rust
impl PlannedColumn {
    pub fn source_index(&self) -> usize;
    pub fn source_name(&self) -> &str;
    pub fn target_name(&self) -> &Identifier;
    pub fn nullable(&self) -> bool;
    pub fn sql_type(&self) -> &SqlType;
}
```

`SqlType` can be public if users need to inspect plans or test DDL decisions.
It should describe supported SQL Server target types structurally instead of as
pre-rendered SQL strings, for example `NVarChar(Max)`, `Decimal { precision,
scale }`, or `DateTime2 { precision }`. Exact enum design belongs to the later
implementation issue, but the API boundary should reserve this structural type.

## Conversion Policy API

`PlanOptions` should contain the policy decisions from the evidence matrix.
Defaults should be conservative and lossless. More permissive, lossy, or
semantic-changing behavior should require explicit user choice.

Candidate public shape:

```rust
pub struct PlanOptions {
    pub string_policy: StringPolicy,
    pub binary_policy: BinaryPolicy,
    pub timezone_policy: TimezonePolicy,
    pub nanosecond_policy: NanosecondPolicy,
    pub uint64_policy: UInt64Policy,
    pub decimal_policy: DecimalPolicy,
    pub decimal256_policy: Decimal256Policy,
    pub float_policy: FloatPolicy,
    pub date64_policy: Date64Policy,
}
```

The exact field visibility can be decided during implementation. Public fields
are simple and ergonomic; builder methods allow better forward compatibility.
If public fields are used, `#[non_exhaustive]` should be considered so new
policy fields can be added later without a semver break.

Recommended defaults:

- `StringPolicy::NVarCharMax`
- `BinaryPolicy::VarBinaryMax`
- `TimezonePolicy::Reject`
- `NanosecondPolicy::RejectNon100ns`
- `UInt64Policy::Reject`
- `DecimalPolicy::RejectNegativeScale`
- `Decimal256Policy::CheckedDowncast`
- `FloatPolicy::RejectNonFinite`
- `Date64Policy::RejectNonMidnight`

Policy enum variants should mirror the evidence matrix:

```rust
pub enum StringPolicy {
    NVarCharMax,
    NVarChar(usize),
    ObservedNVarChar,
}

pub enum BinaryPolicy {
    VarBinaryMax,
    VarBinary(usize),
    ObservedVarBinary,
}

pub enum TimezonePolicy {
    Reject,
    DateTimeOffset,
    NormalizeUtcDateTime2,
}

pub enum NanosecondPolicy {
    RejectNon100ns,
    RoundTo100ns,
    TruncateTo100ns,
}

pub enum UInt64Policy {
    Reject,
    Decimal20_0,
    CheckedBigInt,
}

pub enum DecimalPolicy {
    RejectNegativeScale,
    NormalizeNegativeScale,
}

pub enum Decimal256Policy {
    CheckedDowncast,
    Reject,
}

pub enum FloatPolicy {
    RejectNonFinite,
}

pub enum Date64Policy {
    RejectNonMidnight,
    TimestampDateTime2,
}
```

Planning implications:

- Batch-sensitive options such as `ObservedNVarChar` and `ObservedVarBinary`
  need access to observed values, not only schema. They may require a later
  planning API that accepts a sample batch or statistics. Until then, schema-only
  planning should reject these policies or produce a diagnostic explaining that
  observed planning needs data.
- Lossy or semantic-changing options such as timestamp normalization,
  nanosecond rounding/truncation, negative-scale normalization, and `Date64` to
  timestamp remapping should produce warnings naming the selected policy.
- The policy types should live in `write`, not globally, unless future read APIs
  reuse them.

## Writer API

The writer API should execute a previously constructed `WritePlan` against a
caller-supplied Tiberius client. The caller remains responsible for creating,
configuring, authenticating, pooling, and dropping the client.

Candidate public shape:

```rust
pub struct BulkWriter<'client, C> {
    // private fields
}

pub struct WriteOptions {
    pub backend: WriteBackend,
    pub schema_check: SchemaCheck,
}
```

`SchemaCheck` is defined in the write-time schema compatibility section. It
belongs in `WriteOptions` because it controls execution-time validation rather
than schema planning.

Candidate constructor and lifecycle:

```rust
impl<'client, C> BulkWriter<'client, C> {
    pub async fn new(
        client: &'client mut C,
        table: TableName,
        plan: WritePlan,
        options: WriteOptions,
    ) -> Result<Self>;

    pub async fn write_batch(&mut self, batch: &RecordBatch) -> Result<WriteStats>;
    pub async fn finish(self) -> Result<WriteStats>;
}
```

Design decisions:

- `BulkWriter` should borrow the client mutably for the writer lifetime. This
  matches Tiberius operations that need exclusive mutable access to the client.
- `BulkWriter::new` should start the selected bulk-load path and validate that
  the destination metadata is compatible with the plan when the backend can
  observe metadata.
- `write_batch` should validate the batch schema before writing rows.
- `finish` should finalize the underlying Tiberius bulk request and consume the
  writer so double-finalize is impossible in normal use.
- Dropping an unfinished writer should not silently report success. The exact
  cleanup behavior belongs to implementation, but users should be guided to call
  `finish`.
- Empty batches should be accepted if their schema matches the plan and should
  not force special-case user code.
- `WritePlan` should be owned by the writer unless implementation proves that
  borrowing is materially better. Ownership avoids lifetime coupling between
  planning and async writing.

`WriteStats` should be a small public value object:

```rust
pub struct WriteStats {
    pub rows_written: u64,
    pub batches_written: u64,
}
```

Implementation may need more internal counters, but v0.1 should keep the public
stats stable and conservative.

## Batch And Stream Writing API

The primary low-level API should be `BulkWriter::write_batch`, because it gives
callers direct control over batching, transactions, retries, and lifecycle.

The convenience stream API should be layered on top of `BulkWriter`:

```rust
pub async fn write_record_batch_stream<C, S>(
    client: &mut C,
    table: TableName,
    plan: WritePlan,
    stream: S,
    options: WriteOptions,
) -> Result<WriteStats>
where
    S: Stream<Item = Result<RecordBatch>>;
```

Design decisions:

- The intended stream trait is `futures_core::Stream`, but this issue should not
  add the dependency.
- The stream item should be `Result<RecordBatch>` so upstream stream failures can
  be propagated through the crate error type.
- The convenience function should call `finish` after the stream ends.
- If a stream item errors, the function should return that error and should not
  pretend the bulk operation finished successfully.
- Transaction ownership should stay with the caller. v0.1 should not start,
  commit, or roll back SQL transactions implicitly.
- Retry behavior is out of scope. Retrying bulk writes safely is job-level
  behavior because partial writes and transaction boundaries are application
  decisions.

The stream convenience function should be optional in implementation if it would
force premature dependency choices. The API boundary should reserve the shape so
the crate can add it without disturbing `BulkWriter`.

## Backend Selection API

The public API should let users choose the write backend without changing their
overall write flow.

Candidate public enum:

```rust
#[derive(Default)]
pub enum WriteBackend {
    #[default]
    Auto,
    BaselineTokenRow,
    DirectRawBulk,
}
```

The default variant should be declared on the enum instead of using a separate
manual `Default` implementation.

Backend semantics:

- `Auto`: choose the best available supported backend for the build and plan.
  Early v0.1 implementation may resolve this to `BaselineTokenRow` until direct
  raw bulk support is available.
- `BaselineTokenRow`: use Tiberius' existing `TokenRow` bulk path. This is the
  compatibility backend and should be useful for correctness-first tests.
- `DirectRawBulk`: use Arrow-aware direct TDS bulk row encoding through the
  forked Tiberius raw-row API. If unavailable or unsupported for a plan, this
  should return a clear error instead of silently falling back.

Design decisions:

- Explicit `DirectRawBulk` should fail loudly when direct encoding is unavailable
  or unsupported. Silent fallback would make benchmarks and performance
  expectations misleading.
- `Auto` may fall back, but diagnostics should expose which backend was selected
  when practical.
- Backend selection should live in `WriteOptions`, not `PlanOptions`, because it
  controls execution rather than schema planning.
- The selected backend must not require a different public client type.

`WriteOptions` should be execution-focused. It should not duplicate conversion
policies already captured in `PlanOptions` and frozen into `WritePlan`.

Candidate default:

```rust
impl Default for WriteOptions {
    fn default() -> Self {
        Self {
            backend: WriteBackend::Auto,
            schema_check: SchemaCheck::Strict,
        }
    }
}
```

## Diagnostics And Error API

Diagnostics should represent actionable planning or execution findings that are
useful to users even when an operation succeeds. Errors should represent failed
operations. The API should keep these concepts separate.

Candidate public diagnostic types:

```rust
pub struct Diagnostic {
    pub severity: DiagnosticSeverity,
    pub code: DiagnosticCode,
    pub message: String,
    pub field: Option<FieldRef>,
}

pub enum DiagnosticSeverity {
    Warning,
    Error,
}

pub struct FieldRef {
    pub index: usize,
    pub name: String,
}
```

Design decisions:

- Planning should return warnings through `PlanOutcome::diagnostics` when a plan
  is still usable.
- Planning errors may also be represented internally as diagnostics, but the
  public failure path should be `Result::Err`.
- Diagnostics should include field index and source field name whenever the
  finding is column-specific.
- Diagnostic codes should be stable enough for tests and callers to match
  without parsing message text.
- Message text should remain human-readable and can evolve more freely than
  codes.

Candidate diagnostic code shape:

```rust
pub enum DiagnosticCode {
    UnsupportedArrowType,
    LossyConversionRequiresPolicy,
    PolicyApplied,
    IdentifierInvalid,
    IdentifierTooLong,
    DecimalOutOfRange,
    TimestampOutOfRange,
    SchemaMismatch,
    BackendUnavailable,
}
```

The exact code set should be filled by implementation issues as mappings and
writer behavior become concrete. This issue should establish that codes exist
and are not plain strings.

Candidate crate error shape:

```rust
use snafu::Snafu;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("planning failed"))]
    Planning {
        diagnostics: Vec<Diagnostic>,
    },
    #[snafu(display("DDL rendering failed: {message}"))]
    Ddl {
        message: String,
    },
    #[snafu(display("record batch schema does not match the write plan"))]
    SchemaMismatch {
        diagnostics: Vec<Diagnostic>,
    },
    #[snafu(display("write backend {backend:?} is unavailable: {message}"))]
    BackendUnavailable {
        backend: WriteBackend,
        message: String,
    },
    #[snafu(display("Tiberius operation failed"))]
    Tiberius {
        #[snafu(source)]
        source: tiberius::error::Error,
    },
}
```

Design decisions:

- The crate should expose one `Error` type and one `Result<T>` alias.
- Error variants should preserve enough structured context for callers to make
  decisions without string matching.
- Tiberius errors should be wrapped without erasing the source error.
- Error implementation should use `snafu` when errors are implemented. This
  design issue should not add the dependency yet. The snippet above is intended
  shape, not compiled code in this PR.
- Public errors should still expose stable structured context; `snafu` context
  selectors should be treated as construction helpers, not as the caller-facing
  diagnostic model.
- Errors from upstream batch streams should be convertible into `Error` only
  once the stream API chooses exact bounds.

## Write-Time Schema Compatibility

`WritePlan` is created from an Arrow schema. Every batch written through a
writer should be checked against that planned schema before rows are encoded.

Candidate public shape:

```rust
#[derive(Default)]
pub enum SchemaCheck {
    #[default]
    Strict,
}
```

Recommended v0.1 behavior:

- `SchemaCheck::Strict` should require exact schema equality with the planned
  Arrow schema.
- Strict equality should include field count, field order, field names, data
  types, and nullability.
- Metadata should be treated conservatively. If Arrow schema equality includes
  metadata, v0.1 should follow that behavior rather than invent a looser rule.
- A batch with zero rows should still be schema-checked.
- A schema mismatch should return a structured `Error::SchemaMismatch`.

Future compatibility options may be added later, for example:

```rust
pub enum SchemaCheck {
    Strict,
    IgnoreMetadata,
    AllowNullableRelaxation,
}
```

Those variants should not be included in v0.1 unless implementation issues prove
they are needed. Starting with one strict mode keeps writer behavior predictable
and leaves room for explicit compatibility policies later.

Planning and writing should remain distinct:

- `PlanOptions` controls how an Arrow schema is mapped into a SQL Server plan.
- `WriteOptions::schema_check` controls how incoming batches are validated
  against an already-built plan.
- Runtime batches should not trigger implicit re-planning.

## Forked Tiberius Dependency Strategy

`arrow-tiberius` should expose exactly one Tiberius-compatible public client
type. It should not support both upstream Tiberius and a forked Tiberius package
at the same time because Rust treats those as different concrete types even when
their APIs are similar.

Preferred dependency posture after issue #13 exists:

```toml
[dependencies]
tiberius = { package = "tiberius-arrow", version = "0.12.3-arrow.1" }
```

The package name above is illustrative. Issue #13 owns the final crates.io
package name and versioning policy. The API requirement from this issue is that
`arrow-tiberius` imports and exposes one crate name for the client type.

Design decisions:

- Alias the fork package as `tiberius` in `Cargo.toml` if crates.io naming
  allows it. This keeps public examples and type signatures recognizable.
- Do not expose both `upstream_tiberius::Client` and `forked_tiberius::Client`
  in public APIs.
- Do not make the baseline backend depend on upstream Tiberius while the direct
  backend depends on the fork. Both backends should share the same client type.
- If the fork package is not yet published, implementation issues should either
  block publication of `arrow-tiberius` or use a temporary local development
  dependency that is removed before publishing.
- Public APIs should accept the concrete forked Tiberius client type once the
  dependency exists, unless implementation finds a narrow trait abstraction that
  does not hide important Tiberius behavior.

Conceptual writer signature after the dependency exists:

```rust
impl<'client> BulkWriter<'client> {
    pub async fn new(
        client: &'client mut tiberius::Client<CompatStream>,
        table: TableName,
        plan: WritePlan,
        options: WriteOptions,
    ) -> Result<Self>;
}
```

The exact stream type parameter should follow Tiberius' real `Client` generic
shape. This document should not invent that bound. The key requirement is that
there is one public Tiberius client family.

## Feature Flag Posture

v0.1 should keep feature flags minimal. Feature flags should not create two
incompatible public client types or make examples compile against a different
driver package.

Recommended posture:

- No feature flag for choosing upstream versus forked Tiberius.
- No feature flag for enabling Delta, S3, CLI, pooling, or runtime behavior.
- No feature flag for ODBC in the core write path.
- Optional future feature flags may be acceptable for benchmarks, integration
  tests, or runtime-specific examples if they do not alter the public client
  type.

Backend choice should be a runtime `WriteBackend` option, not a Cargo feature,
unless direct raw-bulk support requires compile-time gating for unavoidable
dependency reasons. Even then, the public client type must stay the same.

## Future Read-Side Placement

The crate name should remain `arrow-tiberius`, not `arrow-tiberius-writer`, so
future SQL Server-to-Arrow reads fit naturally.

Reserved future module:

```text
arrow_tiberius::read
```

Expected future responsibilities:

- planning SQL Server query result schemas into Arrow schemas;
- mapping Tiberius row metadata to Arrow fields;
- building Arrow arrays or record batches from query results;
- read-side diagnostics using the shared `diagnostic` module;
- read-side errors using the shared `error` module.

Design decisions for v0.1:

- Do not expose an empty `read` module just to reserve the name.
- Keep shared concepts such as `MssqlProfile`, identifiers, diagnostics, and
  errors outside `write` when they are likely to apply to reads later.
- Keep write-specific policies under `write` unless a future read issue proves
  they should be shared.
- Do not name crate-level types in a writer-only way if they are conceptually
  shared. For example, prefer `MssqlProfile` over `WriteProfile`.

This placement lets v0.1 ship a write path without closing the door on a future
read API.

## Deferred Until Later Steps

- Exact fork package name and version.
- Exact Tiberius client generic bounds.
- Exact stream item and error bounds for the convenience stream API.
- Exact future read API shape.

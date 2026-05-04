# Evidence Matrix

This document records source-backed facts that inform `arrow-tiberius` v0.1
schema planning, SQL Server DDL choices, Tiberius write integration, and
benchmark boundaries.

The matrix is intentionally evidence-first. When the sources do not justify a
mapping decision, the entry should stay marked as an open question rather than
becoming an implicit implementation choice.

## Scope

`arrow-tiberius` v0.1 is focused on writing Apache Arrow `RecordBatch` values
to Microsoft SQL Server through Tiberius. The first target profile is SQL
Server 2016 with database compatibility level 100.

This document covers:

- Arrow Rust schema and array model evidence.
- SQL Server type, length, precision, scale, nullability, identifier, and
  compatibility evidence.
- Tiberius public APIs and private bulk-load gaps relevant to baseline and
  direct writers.
- Cargo/crates.io publication constraints for a Tiberius fork package.
- `arrow-odbc` evidence for benchmark and fallback comparison.
- Candidate v0.1 Arrow-to-SQL Server mapping status.

This document does not implement mappings, benchmarks, direct TDS encoding, or
SQL Server integration tests.

## Inspected Versions

Arrow Rust evidence in this document is pinned to version 58.2.0.

Tiberius evidence in this document is pinned to version 0.12.3, the current
crates.io release inspected during this issue.

Later implementation issues should prefer split Arrow crates such as
`arrow-schema = "58.2.0"` and `arrow-array = "58.2.0"` over the umbrella
`arrow` crate unless the umbrella crate is specifically needed. Cargo caret
semantics may allow compatible 58.x updates, but v0.1 should not attempt to
support multiple Arrow major versions at once because Arrow Rust public types
such as `RecordBatch` are version-specific Rust types.

If evidence from a newer Arrow major is considered later, update this document
or create a follow-up issue before changing implementation dependencies.

## Status Labels

Use one of these labels for each candidate mapping:

- `supported-exact`: expected to be representable without lossy conversion
  under the selected profile.
- `supported-policy-dependent`: viable only after an explicit policy choice,
  such as string length, binary length, timestamp unit, or timezone behavior.
- `lossy-requires-policy`: may lose range, precision, scale, timezone, or
  representation detail and must require explicit policy before implementation.
- `unsupported-v0.1`: should produce diagnostics in v0.1 rather than silently
  map.
- `open-question`: evidence is not sufficient yet to make a planning decision.

## Inspected Sources

### Arrow Rust

Primary sources:

- `arrow` 58.2.0 crate page: <https://docs.rs/crate/arrow/58.2.0>
- `arrow-schema` 58.2.0 crate page: <https://docs.rs/crate/arrow-schema/58.2.0>
- `arrow-array` 58.2.0 crate page: <https://docs.rs/crate/arrow-array/58.2.0>
- `arrow_schema::DataType`: <https://docs.rs/arrow-schema/58.2.0/arrow_schema/enum.DataType.html>
- `arrow_schema::Field`: <https://docs.rs/arrow-schema/58.2.0/arrow_schema/struct.Field.html>
- `arrow_schema::Schema`: <https://docs.rs/arrow-schema/58.2.0/arrow_schema/struct.Schema.html>
- `arrow_array` crate overview: <https://docs.rs/arrow-array/58.2.0/arrow_array/>
- `arrow_array::RecordBatch`: <https://docs.rs/arrow-array/58.2.0/arrow_array/struct.RecordBatch.html>

Facts to capture in later sections:

- Logical Arrow data type variants relevant to v0.1.
- Field name, data type, nullability, and metadata behavior.
- Schema field ordering and metadata behavior.
- Record batch schema and column-length invariants.
- Array variants needed for primitive, string, binary, decimal, date, and
  timestamp values.

### SQL Server

Primary sources:

- Data types overview: <https://learn.microsoft.com/en-us/sql/t-sql/data-types/data-types-transact-sql>
- Precision, scale, and length: <https://learn.microsoft.com/en-us/sql/t-sql/data-types/precision-scale-and-length-transact-sql>
- `decimal` and `numeric`: <https://learn.microsoft.com/en-us/sql/t-sql/data-types/decimal-and-numeric-transact-sql>
- `char` and `varchar`: <https://learn.microsoft.com/en-us/sql/t-sql/data-types/char-and-varchar-transact-sql>
- `nchar` and `nvarchar`: <https://learn.microsoft.com/en-us/sql/t-sql/data-types/nchar-and-nvarchar-transact-sql>
- `binary` and `varbinary`: <https://learn.microsoft.com/en-us/sql/t-sql/data-types/binary-and-varbinary-transact-sql>
- Date and time types overview: <https://learn.microsoft.com/en-us/sql/t-sql/data-types/date-and-time-types>
- `date`: <https://learn.microsoft.com/en-us/sql/t-sql/data-types/date-transact-sql>
- `time`: <https://learn.microsoft.com/en-us/sql/t-sql/data-types/time-transact-sql>
- `datetime`: <https://learn.microsoft.com/en-us/sql/t-sql/data-types/datetime-transact-sql>
- `datetime2`: <https://learn.microsoft.com/en-us/sql/t-sql/data-types/datetime2-transact-sql>
- `datetimeoffset`: <https://learn.microsoft.com/en-us/sql/t-sql/data-types/datetimeoffset-transact-sql>
- Database compatibility level: <https://learn.microsoft.com/en-us/sql/t-sql/statements/alter-database-transact-sql-compatibility-level>
- Database identifiers: <https://learn.microsoft.com/en-us/sql/relational-databases/databases/database-identifiers>
- Delimited identifiers: <https://learn.microsoft.com/en-us/sql/t-sql/statements/set-quoted-identifier-transact-sql>

Facts to capture in later sections:

- Supported SQL Server v0.1 target types and their exact limits.
- `decimal`/`numeric` precision and scale limits.
- Character and Unicode length semantics.
- Binary length semantics.
- Temporal ranges, precision, and timezone behavior.
- Identifier length, delimiter, and escaping rules.
- Precise meaning of SQL Server 2016 with compatibility level 100.

### Tiberius

Primary sources:

- Tiberius 0.12.3 crate page: <https://docs.rs/crate/tiberius/0.12.3>
- Tiberius crate docs: <https://docs.rs/tiberius/0.12.3/tiberius/>
- `Client::bulk_insert`: <https://docs.rs/tiberius/0.12.3/tiberius/struct.Client.html#method.bulk_insert>
- `BulkLoadRequest`: <https://docs.rs/tiberius/0.12.3/tiberius/struct.BulkLoadRequest.html>
- `TokenRow`: <https://docs.rs/tiberius/0.12.3/tiberius/struct.TokenRow.html>
- `ColumnData`: <https://docs.rs/tiberius/0.12.3/tiberius/enum.ColumnData.html>
- `ColumnType`: <https://docs.rs/tiberius/0.12.3/tiberius/enum.ColumnType.html>
- `TypeLength`: <https://docs.rs/tiberius/0.12.3/tiberius/enum.TypeLength.html>
- Tiberius GitHub repository: <https://github.com/prisma/tiberius>
- Local source references inspected from the crates.io package:
  - `src/client.rs`
  - `src/tds/codec/bulk_load.rs`
  - `src/tds/codec/token/token_row.rs`
  - `src/tds/codec/token/token_col_metadata.rs`
  - `src/tds/codec/type_info.rs`
  - `src/tds/codec/column_data.rs`

Facts to capture in later sections:

- Baseline public bulk insert path through `BulkLoadRequest::send(TokenRow)`.
- Finalization behavior and buffering requirements.
- Public value and metadata representations available to external crates.
- Private APIs that block a clean external direct Arrow-to-TDS bulk encoder.
- Upstream issues or PRs relevant to bulk-load metadata, column ordering,
  raw bulk row APIs, and maintenance expectations.

### Cargo And crates.io

Primary sources:

- Cargo dependency specification: <https://doc.rust-lang.org/cargo/reference/specifying-dependencies.html>
- Cargo publishing reference: <https://doc.rust-lang.org/cargo/reference/publishing.html>
- Cargo registries reference: <https://doc.rust-lang.org/cargo/reference/registries.html>
- crates.io package policies: <https://crates.io/policies>

Facts to capture in later sections:

- Whether crates.io packages may depend on Git or path-only dependencies.
- How `version` interacts with `git` or `path` dependencies during local
  development and publication.
- Why a registry-published Tiberius fork package is needed if `arrow-tiberius`
  itself will be publishable.
- Naming, dependency aliasing, and maintenance implications for the fork.

### arrow-odbc

Primary sources:

- `arrow-odbc` crate docs: <https://docs.rs/arrow-odbc/latest/arrow_odbc/>
- `arrow-odbc` package page: <https://docs.rs/crate/arrow-odbc/latest>
- `arrow-odbc` repository: <https://github.com/pacman82/arrow-odbc>

Facts to capture in later sections:

- Existing Arrow read support from ODBC data sources.
- Existing Arrow record batch insertion support into database tables.
- ODBC driver/runtime deployment implications.
- Why `arrow-odbc` is a benchmark and fallback reference rather than the
  initial `arrow-tiberius` runtime dependency.

## Arrow Rust Evidence

Evidence is pinned to Arrow Rust 58.2.0.

### Schema And Field Model

Sources:

- `arrow_schema::Field`: <https://docs.rs/arrow-schema/58.2.0/arrow_schema/struct.Field.html>
- `arrow_schema::Schema`: <https://docs.rs/arrow-schema/58.2.0/arrow_schema/struct.Schema.html>

Evidence:

- A `Field` carries at least a field name, `DataType`, nullability, and optional
  metadata. Planning must preserve source field order and source field names
  until an explicit SQL Server identifier policy maps them.
- A `Schema` is an ordered collection of fields plus schema-level metadata.
  SQL Server DDL rendering should use the field order from the Arrow schema
  unless a later issue introduces an explicit reordering policy.
- Field nullability is the Arrow-side source for SQL Server `NULL` versus
  `NOT NULL` planning. Nullable arrays and null bitmaps are runtime value
  behavior; schema nullability is the planning signal.

Planning implications:

- `WritePlan` should hold enough source-field identity to report diagnostics by
  field name and column index.
- Schema metadata should not silently affect SQL Server DDL in v0.1. If later
  issues want metadata-driven overrides, they should define an explicit policy.

### DataType Coverage

Source:

- `arrow_schema::DataType`: <https://docs.rs/arrow-schema/58.2.0/arrow_schema/enum.DataType.html>

Evidence:

- Arrow Rust 58.2.0 exposes primitive fixed-width types relevant to v0.1:
  `Boolean`, signed integers `Int8` through `Int64`, unsigned integers `UInt8`
  through `UInt64`, and floats `Float16`, `Float32`, and `Float64`.
- Timestamp values are represented as `Timestamp(TimeUnit, Option<Arc<str>>)`.
  Time is measured relative to the Unix epoch as a signed 64-bit integer. The
  timezone is optional and, when present, is metadata that affects timestamp
  interpretation.
- `TimeUnit` variants are `Second`, `Millisecond`, `Microsecond`, and
  `Nanosecond`.
- `Date32` is a signed 32-bit date as elapsed days since Unix epoch.
- `Date64` is a signed 64-bit date as elapsed milliseconds since Unix epoch.
  The Arrow documentation recommends `Date32` for clean date-only values and
  timestamp variants when time of day matters.
- `Binary` stores variable-length opaque bytes with 32-bit offsets, while
  `LargeBinary` uses 64-bit offsets. `Utf8` and `LargeUtf8` follow the same
  offset distinction for UTF-8 strings.
- `Decimal128` and `Decimal256` carry precision and scale. Arrow Rust permits
  negative scale in some situations, where negative scale represents zero
  padding to the right of the digits.
- Nested and encoded types include `List`, `LargeList`, `FixedSizeList`,
  `Struct`, `Union`, `Map`, `Dictionary`, and `RunEndEncoded`.

Planning implications:

- `Float16`, Arrow decimal negative scales, and nested/encoded types need
  explicit diagnostics unless later issues choose support.
- `Utf8` is already Unicode UTF-8 in Arrow. SQL Server string planning must not
  map it to non-Unicode `varchar` without an explicit collation/encoding policy.
- `LargeUtf8` and `LargeBinary` describe Arrow offset capacity, not observed
  per-value length. SQL Server length decisions still need a declared or
  inferred target length policy.
- Timestamp planning must distinguish no-timezone timestamps from timezone-aware
  timestamps. SQL Server `datetime2` and `datetimeoffset` have different
  semantics, so v0.1 should require an explicit policy for timezone handling.

### RecordBatch Model

Source:

- `arrow_array::RecordBatch`: <https://docs.rs/arrow-array/58.2.0/arrow_array/struct.RecordBatch.html>

Evidence:

- A `RecordBatch` is a two-dimensional, column-oriented batch with a defined
  schema.
- All arrays in a `RecordBatch` have the same row count, and the arrays'
  datatypes must match the batch schema.
- Record batches are intended as an incremental unit of work, which fits the
  v0.1 streaming writer goal.

Planning implications:

- The writer should validate each incoming batch schema against the `WritePlan`.
- The writer should process batches incrementally rather than requiring the full
  source table in memory.
- Unit tests should include schema mismatch cases even before SQL Server
  integration tests exist.

## SQL Server Evidence

### Data Type Categories

Source:

- Data types overview: <https://learn.microsoft.com/en-us/sql/t-sql/data-types/data-types-transact-sql>

Evidence:

- SQL Server system data types include exact numerics, approximate numerics,
  date and time types, character strings, Unicode character strings, binary
  strings, and other database-specific types.
- Exact numerics include `tinyint`, `smallint`, `int`, `bigint`, `bit`,
  `decimal`, and `numeric`. Microsoft documents `decimal` and `numeric` as
  functionally equivalent.
- Approximate numerics include `float` and `real`.
- Date and time types include `date`, `time`, `datetime2`, `datetimeoffset`,
  `datetime`, and `smalldatetime`.
- Character string types include `char`, `varchar`, and deprecated `text`.
  Unicode character string types include `nchar`, `nvarchar`, and deprecated
  `ntext`. Binary string types include `binary`, `varbinary`, and deprecated
  `image`.

Planning implications:

- v0.1 should avoid deprecated `text`, `ntext`, and `image`; use
  `varchar(max)`, `nvarchar(max)`, or `varbinary(max)` when a large-value type
  is selected by policy.
- Because Arrow `Utf8` is Unicode, `nvarchar` is the conservative SQL Server
  string family for SQL Server 2016 unless an explicit `varchar` policy and
  collation/encoding decision exists.

### Boolean And Integers

Sources:

- `bit`: <https://learn.microsoft.com/en-us/sql/t-sql/data-types/bit-transact-sql>
- Integer types: <https://learn.microsoft.com/en-us/sql/t-sql/data-types/int-bigint-smallint-and-tinyint-transact-sql>

Evidence:

- SQL Server `bit` can store `1`, `0`, or `NULL` and is documented for Boolean
  values.
- Integer ranges and storage are:
  - `tinyint`: `0` through `255`, 1 byte.
  - `smallint`: `-32,768` through `32,767`, 2 bytes.
  - `int`: `-2,147,483,648` through `2,147,483,647`, 4 bytes.
  - `bigint`: `-9,223,372,036,854,775,808` through
    `9,223,372,036,854,775,807`, 8 bytes.
- SQL Server does not have general unsigned integer types beyond `tinyint`.

Planning implications:

- Arrow `Boolean` can target SQL Server `bit`.
- Arrow `Int8`, `Int16`, `Int32`, and `Int64` can map exactly to signed SQL
  Server integer types with equal or wider range.
- Arrow `UInt8` can map exactly to `tinyint`.
- Arrow `UInt16`, `UInt32`, and `UInt64` require range-aware policy. `UInt16`
  can fit into `int`; `UInt32` can fit into `bigint`; `UInt64` can exceed
  `bigint` and should not silently map.

### Floating Point

Source:

- `float` and `real`: <https://learn.microsoft.com/en-us/sql/t-sql/data-types/float-and-real-transact-sql>

Evidence:

- SQL Server `real` is an approximate 4-byte floating point type with about
  seven digits of precision.
- SQL Server `float(n)` uses `n` from 1 through 53, but SQL Server treats
  `1..=24` as 24 and `25..=53` as 53. `float(53)` is the double precision
  synonym.
- Microsoft documents `float` and `real` as approximate numeric types, not
  exact numeric types.

Planning implications:

- Arrow `Float32` can target SQL Server `real`.
- Arrow `Float64` can target SQL Server `float(53)`.
- Diagnostics and docs should avoid describing floating mappings as exact
  decimal-preserving conversions.
- Arrow `Float16` is not directly represented by a SQL Server float type and
  should be unsupported or explicitly promoted by policy.

### Decimal And Numeric

Sources:

- `decimal` and `numeric`: <https://learn.microsoft.com/en-us/sql/t-sql/data-types/decimal-and-numeric-transact-sql>
- Precision, scale, and length: <https://learn.microsoft.com/en-us/sql/t-sql/data-types/precision-scale-and-length-transact-sql>

Evidence:

- SQL Server `decimal(p, s)` and `numeric(p, s)` are fixed precision and scale
  numeric types.
- Precision is the total number of stored decimal digits. Scale is the number
  of digits to the right of the decimal point.
- SQL Server decimal precision must be 1 through 38. Scale must be between 0
  and precision.
- SQL Server treats each precision/scale combination as a distinct type.
- Reducing precision or scale can round values or overflow depending on the
  conversion and session settings.

Planning implications:

- Arrow `Decimal128(p, s)` can target SQL Server `decimal(p, s)` only when
  `1 <= p <= 38` and `0 <= s <= p`.
- Arrow `Decimal256` can exceed SQL Server decimal capacity and should be
  unsupported unless the Arrow precision is within SQL Server limits and the
  implementation proves value conversion.
- Arrow negative decimal scale is not directly compatible with SQL Server's
  nonnegative scale requirement and needs explicit diagnostics or policy.

### Strings

Sources:

- `char` and `varchar`: <https://learn.microsoft.com/en-us/sql/t-sql/data-types/char-and-varchar-transact-sql>
- `nchar` and `nvarchar`: <https://learn.microsoft.com/en-us/sql/t-sql/data-types/nchar-and-nvarchar-transact-sql>

Evidence:

- `char(n)` and `varchar(n)` use byte length semantics. `n` can be 1 through
  8,000; `varchar(max)` can store up to 2 GB.
- `nchar(n)` and `nvarchar(n)` use byte-pair semantics. `n` can be 1 through
  4,000; `nvarchar(max)` can store up to 2 GB.
- SQL Server 2016 predates UTF-8 collations for `varchar`; Microsoft recommends
  Unicode `nchar`/`nvarchar` on older engine versions to reduce character
  conversion issues.
- With supplementary-character collations, `nchar`/`nvarchar` use UTF-16 and a
  supplementary character can consume two byte-pairs. Therefore `nvarchar(n)`
  is not a guaranteed count of Unicode scalar values.

Planning implications:

- Default Arrow `Utf8` planning should prefer `nvarchar` under the SQL Server
  2016 profile.
- Choosing between `nvarchar(n)` and `nvarchar(max)` requires a length policy.
- `LargeUtf8` does not automatically imply `nvarchar(max)`, but it is a strong
  signal that unbounded or large-value policy may be needed.
- `varchar` should be policy-controlled because it depends on collation and
  code page behavior in SQL Server 2016.

### Binary

Source:

- `binary` and `varbinary`: <https://learn.microsoft.com/en-us/sql/t-sql/data-types/binary-and-varbinary-transact-sql>

Evidence:

- `binary(n)` is fixed-length binary data for `n` from 1 through 8,000 bytes.
- `varbinary(n)` is variable-length binary data for `n` from 1 through 8,000
  bytes.
- `varbinary(max)` can store up to 2 GB.
- Microsoft documents `image` as deprecated and recommends `varbinary(max)`
  instead for large binary values.

Planning implications:

- Arrow `Binary` should target `varbinary(n)` or `varbinary(max)` according to
  an explicit length policy.
- Arrow `LargeBinary` does not automatically imply `varbinary(max)`, but it is
  a strong signal that unbounded or large-value policy may be needed.
- `binary(n)` should not be the default for Arrow variable-length binary data.

### Date And Time

Sources:

- Date and time types overview: <https://learn.microsoft.com/en-us/sql/t-sql/data-types/date-and-time-types>
- `date`: <https://learn.microsoft.com/en-us/sql/t-sql/data-types/date-transact-sql>
- `datetime2`: <https://learn.microsoft.com/en-us/sql/t-sql/data-types/datetime2-transact-sql>
- `datetimeoffset`: <https://learn.microsoft.com/en-us/sql/t-sql/data-types/datetimeoffset-transact-sql>

Evidence:

- SQL Server `date` stores a date without time or timezone. Its range is
  `0001-01-01` through `9999-12-31`.
- SQL Server `datetime2(p)` stores date and time without timezone. Fractional
  seconds precision is 0 through 7, with 100 ns accuracy at precision 7.
- SQL Server `datetimeoffset(p)` stores date and time with an offset. Its
  offset range is `-14:00` through `+14:00`, and the stored offset is preserved.
- `datetime2` and `datetimeoffset` were introduced before SQL Server 2016 and
  are available on the first target engine. Down-level client behavior may
  expose them as strings, but this crate writes through Tiberius/TDS rather
  than relying on generic down-level clients.

Planning implications:

- Arrow `Date32` can target SQL Server `date` subject to value range checks.
- Arrow `Date64` can target SQL Server `date` only when values represent exact
  days and are in SQL Server's date range. Otherwise it should be treated as a
  timestamp or rejected according to policy.
- Arrow timestamps without timezone can target `datetime2(p)` with precision
  selected from the Arrow `TimeUnit`.
- Arrow nanosecond timestamps exceed SQL Server's 100 ns precision and require
  explicit truncation/rounding/fail policy.
- Arrow timestamps with timezone should not silently map to `datetime2` because
  that loses timezone semantics. They should require policy or target
  `datetimeoffset` where the timezone can be represented as an offset.

### Identifiers

Sources:

- Database identifiers: <https://learn.microsoft.com/en-us/sql/relational-databases/databases/database-identifiers>
- Maximum capacity specifications: <https://learn.microsoft.com/en-us/sql/sql-server/maximum-capacity-specifications-for-sql-server>
- `SET QUOTED_IDENTIFIER`: <https://learn.microsoft.com/en-us/sql/t-sql/statements/set-quoted-identifier-transact-sql>

Evidence:

- SQL Server identifiers have a 128-character limit for regular database
  objects.
- SQL Server supports regular identifiers and delimited identifiers.
- Bracket-delimited identifiers are available independently of
  `QUOTED_IDENTIFIER`; double-quoted identifiers depend on that setting.

Planning implications:

- v0.1 DDL rendering should prefer bracket quoting and escape closing brackets
  inside names.
- Identifier policy must reject or diagnose names that exceed SQL Server limits
  after any normalization policy.
- Dots in Arrow field names should not be treated as multipart SQL identifiers
  unless the API explicitly asks for multipart interpretation.

### SQL Server 2016 With Compatibility Level 100

Sources:

- ALTER DATABASE compatibility level: <https://learn.microsoft.com/en-us/sql/t-sql/statements/alter-database-transact-sql-compatibility-level>
- Change database compatibility level: <https://learn.microsoft.com/en-us/sql/database-engine/install-windows/change-the-database-compatibility-mode-and-use-the-query-store>

Evidence:

- SQL Server 2016 is engine version 13.x. Its default compatibility level is
  130, but databases can run at lower compatibility levels such as 100.
- Microsoft separates engine upgrade from database compatibility level. Newer
  engine features and query optimizer behaviors can be gated by compatibility
  level.
- Compatibility level primarily affects T-SQL and query optimization behavior.
  It does not make a SQL Server 2016 instance literally become SQL Server 2008.

Planning implications:

- The first profile should be named precisely, for example
  `sql_server_2016_compat_100`, not `sql_server_2008`.
- Conservative DDL choices are appropriate, but the project should not assume
  SQL Server 2008 wire-protocol behavior when connected to a SQL Server 2016
  engine through Tiberius.
- Tests and docs should distinguish engine version from database compatibility
  level.

## Tiberius Evidence

Evidence is pinned to Tiberius 0.12.3.

### Public Driver Boundary

Sources:

- Tiberius crate docs: <https://docs.rs/tiberius/0.12.3/tiberius/>
- Tiberius crate metadata from crates.io: <https://docs.rs/crate/tiberius/0.12.3>

Evidence:

- Tiberius is a Rust TDS driver for Microsoft SQL Server.
- Tiberius 0.12.3 is dual licensed MIT/Apache-2.0.
- Tiberius exposes `Client`, `Config`, `Query`, `QueryStream`, `Row`,
  `BulkLoadRequest`, `TokenRow`, `ColumnData`, `ColumnType`, `IntoSql`,
  `ToSql`, `FromSql`, and related SQL Server value types.
- The default feature set includes `tds73`, `winauth`, and `native-tls`.
  The `tds73` feature gates newer date/time variants such as `date`,
  `time`, `datetime2`, and `datetimeoffset`.

Planning implications:

- `arrow-tiberius` should not implement connection setup, authentication, TLS,
  packet sizing, query execution, or token stream handling locally for v0.1.
- Date/time support through Tiberius should keep `tds73` enabled unless later
  dependency work proves a reason to disable it.
- If `arrow-tiberius` publishes with a Tiberius fork package, it should preserve
  upstream license and attribution.

### Baseline Bulk Insert Path

Sources:

- `Client::bulk_insert`: <https://docs.rs/tiberius/0.12.3/tiberius/struct.Client.html#method.bulk_insert>
- `BulkLoadRequest`: <https://docs.rs/tiberius/0.12.3/tiberius/struct.BulkLoadRequest.html>
- Tiberius source `src/client.rs`.
- Tiberius source `src/tds/codec/bulk_load.rs`.
- Tiberius source `tests/bulk.rs`.

Evidence:

- The public baseline API is:

  ```rust
  let mut req = client.bulk_insert(table).await?;
  req.send(row).await?;
  let result = req.finalize().await?;
  ```

- `Client::bulk_insert` first flushes the current stream, queries destination
  metadata with `SELECT TOP 0 * FROM {table}`, filters updateable columns, then
  sends an `INSERT BULK {table} (...)` batch before constructing
  `BulkLoadRequest`.
- `BulkLoadRequest` stores the connection, packet id, internal buffer, and
  destination metadata columns privately.
- `BulkLoadRequest::send` accepts a `TokenRow`, encodes it with the destination
  metadata, and writes packets only when the buffer exceeds packet size.
- `BulkLoadRequest::finalize` appends a done token, flushes pending data,
  writes the final end-of-message bulk-load packet, flushes the sink, and reads
  the server `ExecuteResult`.
- Tiberius tests cover `TokenRow` bulk inserts for several scalar SQL Server
  types.

Planning implications:

- The baseline `arrow-tiberius` writer can be implemented through
  `TokenRow` without private Tiberius APIs.
- The writer must call `finalize` exactly once after sending rows. Dropping a
  request without finalization is not enough to make rows available.
- Baseline writing should use a shared `WritePlan` for conversion decisions,
  but destination metadata still comes from Tiberius' `bulk_insert` flow.

### TokenRow And ColumnData

Sources:

- `TokenRow`: <https://docs.rs/tiberius/0.12.3/tiberius/struct.TokenRow.html>
- `ColumnData`: <https://docs.rs/tiberius/0.12.3/tiberius/enum.ColumnData.html>
- Tiberius source `src/tds/codec/token/token_row.rs`.
- Tiberius source `src/tds/codec/column_data.rs`.
- Tiberius source `src/to_sql.rs`.

Evidence:

- `TokenRow` is a row-oriented container around a vector of `ColumnData`.
- `TokenRow` validates that its value count matches destination metadata column
  count during encoding.
- `TokenRow::push` appends one `ColumnData` value.
- `ColumnData` variants relevant to v0.1 include `Bit`, `U8`, `I16`, `I32`,
  `I64`, `F32`, `F64`, `String`, `Binary`, `Numeric`, `Date`, `Time`,
  `DateTime2`, and `DateTimeOffset`.
- Each nullable value is represented as an `Option` inside the matching
  `ColumnData` variant.
- Tiberius implements `IntoSql`/`ToSql` for common Rust scalar types, but Arrow
  arrays will need explicit extraction/conversion logic in `arrow-tiberius`.

Planning implications:

- Baseline conversion should produce `ColumnData` values directly or through
  `IntoSql` only where that does not hide Arrow-specific policy decisions.
- Null handling can use the `None` variant of the matching `ColumnData` type.
- Baseline conversion remains row-oriented and per-cell, which is useful for
  correctness but may be the performance ceiling that motivates direct encoding.

### Metadata And Type Representation

Sources:

- `ColumnType`: <https://docs.rs/tiberius/0.12.3/tiberius/enum.ColumnType.html>
- Tiberius source `src/row.rs`.
- Tiberius source `src/tds/codec/type_info.rs`.
- Tiberius source `src/tds/codec/token/token_col_metadata.rs`.
- Issue #397: <https://github.com/prisma/tiberius/issues/397>
- PR #398: <https://github.com/prisma/tiberius/pull/398>

Evidence:

- Public `ColumnType` is a coarse type enum exposed through query result
  `Column` values.
- Internal `TypeInfo`, `VarLenContext`, `MetaDataColumn`, and
  `BaseMetaDataColumn` carry the detailed TDS metadata needed to encode values
  against destination SQL Server columns.
- `BulkLoadRequest` already owns `Vec<MetaDataColumn>` internally, but the
  public API does not expose the columns or a read-only metadata view.
- The Tiberius crate re-exports `BulkLoadRequest`, `ColumnData`, `ColumnFlag`,
  `IntoRow`, `TokenRow`, and `TypeLength`, but not `TypeInfo`,
  `MetaDataColumn`, or `BaseMetaDataColumn`.
- Upstream issue #397 requests exposing `BaseMetaDataColumn` and `TypeInfo`, or
  exposing the metadata from `BulkLoadRequest`, because user code otherwise has
  to reimplement those types and metadata queries.
- Upstream PR #398 proposes adding a method to query column metadata.

Planning implications:

- `ColumnType` alone is not enough for a direct Arrow-to-TDS encoder because it
  loses length, precision, scale, collation, and exact TDS type details.
- The forked Tiberius package should expose a narrow read-only metadata view
  from `BulkLoadRequest` or a metadata query API sufficient for bulk encoding.
- Exposing raw internal structs wholesale is not required if the fork can offer
  a stable metadata view tailored to bulk-load encoding.

### Direct Encoder API Gaps

Sources:

- Tiberius source `src/tds/codec/bulk_load.rs`.
- Tiberius source `src/tds/codec/token/token_row/bytes_mut_with_data_columns.rs`.
- Tiberius source `src/tds/codec/column_data/bytes_mut_with_type_info.rs`.
- Issue #397: <https://github.com/prisma/tiberius/issues/397>

Evidence:

- `BulkLoadRequest::send` accepts only `TokenRow`.
- The bulk-load packet buffer, packet splitting, destination metadata, and
  connection write/flush operations are private.
- Row encoding relies on internal wrappers that attach destination metadata to
  the byte buffer before encoding each `ColumnData`.
- There is no public method on `BulkLoadRequest` to append already-encoded TDS
  row bytes.

Planning implications:

- A direct Arrow-to-TDS backend cannot be cleanly implemented against upstream
  Tiberius 0.12.3 public APIs alone.
- The minimal fork API should add:
  - read-only access to destination bulk metadata, or an equivalent stable
    metadata view.
  - a method to send already-encoded bulk row bytes through the existing
    `BulkLoadRequest` packet buffer and packet flush path.
- The fork should keep Tiberius responsible for connection state, packet
  splitting, finalization, server result handling, TLS, and authentication.

### Upstream Bulk-Load Threads

Sources:

- Issue #311: <https://github.com/prisma/tiberius/issues/311>
- PR #359: <https://github.com/prisma/tiberius/pull/359>
- Issue #397: <https://github.com/prisma/tiberius/issues/397>
- PR #398: <https://github.com/prisma/tiberius/pull/398>
- Issue #410: <https://github.com/prisma/tiberius/issues/410>

Evidence:

- Issue #311 requests passing explicit column names to `bulk_insert`, mainly to
  control column order.
- PR #359 proposes a `bulk_insert_columns` API for explicit column lists and
  subset inserts.
- Issue #397 requests metadata and type exposure needed by external code.
- PR #398 proposes a column metadata query method.
- Issue #410 reports a BCP failure when a `DATE` column precedes a `TIME`
  column, which is relevant to direct encoder parity and temporal tests.

Planning implications:

- `arrow-tiberius` should not assume destination column order is always safe
  when using `SELECT TOP 0 *`; explicit write-plan column order and eventual
  column-list support matter.
- Direct encoder work should include temporal ordering/parity tests, especially
  around `date`, `time`, and `datetime2`.
- Upstream review should not block v0.1, but these threads support keeping the
  fork patch narrow and aligned with known Tiberius API gaps.

## Cargo And crates.io Evidence

Sources:

- Cargo dependency specification: <https://doc.rust-lang.org/cargo/reference/specifying-dependencies.html>
- Cargo publishing reference: <https://doc.rust-lang.org/cargo/reference/publishing.html>
- Cargo registries reference: <https://doc.rust-lang.org/cargo/reference/registries.html>

Evidence:

- Cargo supports dependencies from registries, Git repositories, and local
  paths during development.
- Crates published to crates.io cannot depend on code outside crates.io except
  for dev-dependencies.
- Local `path` dependencies alone are not permitted for crates.io publication.
- Cargo supports specifying multiple locations, such as `path` plus `version`
  or `git` plus `version`; the local path or Git source is used locally, while
  the registry version is used when packaged for publication.
- A dependency package can be renamed in `Cargo.toml` using the `package` key.

Planning implications:

- A publishable `arrow-tiberius` crate cannot depend on an unpublished Git-only
  Tiberius fork as a normal dependency.
- If the direct encoder requires forked Tiberius APIs, the fork must itself be
  published to crates.io or publication must be explicitly deferred.
- `arrow-tiberius` should depend on one Tiberius package in its normal
  dependency graph. Carrying both upstream Tiberius and a fork would expose two
  incompatible `Client` types.
- If the fork's package name differs from the Rust import name wanted by
  downstream users, `Cargo.toml` dependency aliasing can preserve the desired
  import path, but the public API still needs one concrete client type.

## arrow-odbc Evidence

TODO: Capture existing Arrow read/write support and benchmark boundary.

## Candidate v0.1 Mapping Matrix

| Arrow source type | Candidate SQL Server target | Tiberius representation | Null handling | Constraints | Status | SQL Server 2016 compat-100 note | Test fixture idea | Sources | Open question |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| Boolean | TODO | TODO | TODO | TODO | `open-question` | TODO | TODO | TODO | TODO |
| Int8 | TODO | TODO | TODO | TODO | `open-question` | TODO | TODO | TODO | TODO |
| Int16 | TODO | TODO | TODO | TODO | `open-question` | TODO | TODO | TODO | TODO |
| Int32 | TODO | TODO | TODO | TODO | `open-question` | TODO | TODO | TODO | TODO |
| Int64 | TODO | TODO | TODO | TODO | `open-question` | TODO | TODO | TODO | TODO |
| UInt8 | TODO | TODO | TODO | TODO | `open-question` | TODO | TODO | TODO | TODO |
| UInt16 | TODO | TODO | TODO | TODO | `open-question` | TODO | TODO | TODO | TODO |
| UInt32 | TODO | TODO | TODO | TODO | `open-question` | TODO | TODO | TODO | TODO |
| UInt64 | TODO | TODO | TODO | TODO | `open-question` | TODO | TODO | TODO | TODO |
| Float32 | TODO | TODO | TODO | TODO | `open-question` | TODO | TODO | TODO | TODO |
| Float64 | TODO | TODO | TODO | TODO | `open-question` | TODO | TODO | TODO | TODO |
| Utf8 | TODO | TODO | TODO | TODO | `open-question` | TODO | TODO | TODO | TODO |
| LargeUtf8 | TODO | TODO | TODO | TODO | `open-question` | TODO | TODO | TODO | TODO |
| Binary | TODO | TODO | TODO | TODO | `open-question` | TODO | TODO | TODO | TODO |
| LargeBinary | TODO | TODO | TODO | TODO | `open-question` | TODO | TODO | TODO | TODO |
| Decimal128 | TODO | TODO | TODO | TODO | `open-question` | TODO | TODO | TODO | TODO |
| Decimal256 | TODO | TODO | TODO | TODO | `open-question` | TODO | TODO | TODO | TODO |
| Date32 | TODO | TODO | TODO | TODO | `open-question` | TODO | TODO | TODO | TODO |
| Date64 | TODO | TODO | TODO | TODO | `open-question` | TODO | TODO | TODO | TODO |
| Timestamp second | TODO | TODO | TODO | TODO | `open-question` | TODO | TODO | TODO | TODO |
| Timestamp millisecond | TODO | TODO | TODO | TODO | `open-question` | TODO | TODO | TODO | TODO |
| Timestamp microsecond | TODO | TODO | TODO | TODO | `open-question` | TODO | TODO | TODO | TODO |
| Timestamp nanosecond | TODO | TODO | TODO | TODO | `open-question` | TODO | TODO | TODO | TODO |
| List | TODO | TODO | TODO | TODO | `open-question` | TODO | TODO | TODO | TODO |
| Struct | TODO | TODO | TODO | TODO | `open-question` | TODO | TODO | TODO | TODO |
| Map | TODO | TODO | TODO | TODO | `open-question` | TODO | TODO | TODO | TODO |
| Dictionary | TODO | TODO | TODO | TODO | `open-question` | TODO | TODO | TODO | TODO |
| Union | TODO | TODO | TODO | TODO | `open-question` | TODO | TODO | TODO | TODO |

## Open Questions

- Which SQL Server string target should be the default for Arrow `Utf8`:
  `nvarchar(n)`, `nvarchar(max)`, `varchar(n)`, or a policy-selected type?
- Should SQL Server `datetime2` be the default timestamp target for timezone-free
  Arrow timestamps?
- Should timezone-aware Arrow timestamps target `datetimeoffset`, fail by
  default, or require an explicit normalization policy?
- How should Arrow `UInt64` be handled when values exceed SQL Server `bigint`?
- Should dictionary arrays be rejected in v0.1 or decoded according to their
  value type before planning?
- What exact public API should the Tiberius fork expose for raw bulk row bytes?

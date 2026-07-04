# Arrow to SQL Server Type Mapping

This page documents the current Arrow-to-SQL Server planning surface for
`arrow-tiberius`.

The direct raw writer is the primary optimized writer. The baseline writer is
still supported as a compatibility and reference path. The tables below show
whether each planned mapping is supported by both writers.

## Status Legend

- `yes`: supported for planned schema and write-time values.
- `runtime check`: planned by schema, but individual values can still be
  rejected at write time.
- `schema-only reject`: the schema-only planner cannot select the mapping
  without observed values or a different policy.
- `no`: not supported.

## Default Mappings

These are the mappings selected by `PlanOptions::default()` for schema-only
planning.

| Arrow type | SQL Server type | Baseline writer | Direct raw writer | Notes |
| --- | --- | --- | --- | --- |
| `Boolean` | `bit` | yes | yes | Arrow nulls become SQL `NULL`. |
| `Int8` | `smallint` | yes | yes | SQL Server has no signed 8-bit integer type. |
| `Int16` | `smallint` | yes | yes | - |
| `Int32` | `int` | yes | yes | - |
| `Int64` | `bigint` | yes | yes | - |
| `UInt8` | `tinyint` | yes | yes | SQL Server `tinyint` is unsigned 8-bit. |
| `UInt16` | `int` | yes | yes | Lossless widening. |
| `UInt32` | `bigint` | yes | yes | Lossless widening. |
| `Float16` | `real` | runtime check | runtime check | Values are widened to SQL Server `real`; non-finite values are rejected by `FloatPolicy::RejectNonFinite`. |
| `Float32` | `real` | runtime check | runtime check | Non-finite values are rejected by `FloatPolicy::RejectNonFinite`. |
| `Float64` | `float(53)` | runtime check | runtime check | Non-finite values are rejected by `FloatPolicy::RejectNonFinite`. |
| `Utf8` | `nvarchar(max)` | yes | yes | Uses UTF-16 length checks for bounded targets when selected by policy. |
| `LargeUtf8` | `nvarchar(max)` | yes | yes | - |
| `Binary` | `varbinary(max)` | yes | yes | - |
| `LargeBinary` | `varbinary(max)` | yes | yes | - |
| `FixedSizeBinary(n)` | `binary(n)` | yes | yes | `n` must be in SQL Server `binary(n)` range `1..=8000`. |
| `Decimal32(p, s)` | `decimal(p, s)` | runtime check | runtime check | `1 <= p <= 38`, `0 <= s <= p`. |
| `Decimal64(p, s)` | `decimal(p, s)` | runtime check | runtime check | `1 <= p <= 38`, `0 <= s <= p`. |
| `Decimal128(p, s)` | `decimal(p, s)` | runtime check | runtime check | `1 <= p <= 38`, `0 <= s <= p`. |
| `Decimal256(p, s)` | `decimal(p, s)` | runtime check | runtime check | Default `Decimal256Policy::CheckedDowncast`; shape must fit SQL Server decimal and values must fit the selected decimal payload. |
| `Date32` | `date` | yes | yes | Requires TDS 7.3 date support. |
| `Time32(Second)` | `time(0)` | yes | yes | Requires TDS 7.3 time support. |
| `Time32(Millisecond)` | `time(3)` | yes | yes | Requires TDS 7.3 time support. |
| `Time64(Microsecond)` | `time(6)` | yes | yes | Requires TDS 7.3 time support. |
| `Time64(Nanosecond)` | `time(7)` | runtime check | runtime check | SQL Server stores 100ns ticks; `NanosecondPolicy` controls non-100ns values. |
| `Timestamp(Second, None)` | `datetime2(7)` | yes | yes | Default `TimestampPolicy::DateTime2 { precision: 7 }`. Requires TDS 7.3 `datetime2` support. |
| `Timestamp(Millisecond, None)` | `datetime2(7)` | yes | yes | Default `TimestampPolicy::DateTime2 { precision: 7 }`. |
| `Timestamp(Microsecond, None)` | `datetime2(7)` | yes | yes | Default `TimestampPolicy::DateTime2 { precision: 7 }`. |
| `Timestamp(Nanosecond, None)` | `datetime2(7)` | runtime check | runtime check | Default `TimestampPolicy::DateTime2 { precision: 7 }`. SQL Server stores 100ns ticks; `NanosecondPolicy` controls non-100ns values. |

## Policy-Dependent Mappings

These Arrow types or target shapes require a non-default policy or policy-specific
runtime checks.

| Arrow type | Policy | SQL Server type | Baseline writer | Direct raw writer | Notes |
| --- | --- | --- | --- | --- | --- |
| `UInt64` | `UInt64Policy::Reject` | none | no | no | Default. Schema planning rejects `UInt64`. |
| `UInt64` | `UInt64Policy::Decimal20_0` | `decimal(20,0)` | yes | yes | Lossless for all `UInt64` values. |
| `UInt64` | `UInt64Policy::CheckedBigInt` | `bigint` | runtime check | runtime check | Values greater than `i64::MAX` are rejected. |
| `Utf8`, `LargeUtf8` | `StringPolicy::NVarCharMax` | `nvarchar(max)` | yes | yes | Default. |
| `Utf8`, `LargeUtf8` | `StringPolicy::NVarChar(n)` | `nvarchar(n)` | runtime check | runtime check | Runtime rejects values whose UTF-16 length exceeds `n`. |
| `Utf8`, `LargeUtf8` | `StringPolicy::ObservedNVarChar` | inferred `nvarchar(n)` | schema-only reject | schema-only reject | Requires observed values or statistics; schema-only planning currently rejects it. |
| `Binary`, `LargeBinary` | `BinaryPolicy::VarBinaryMax` | `varbinary(max)` | yes | yes | Default. |
| `Binary`, `LargeBinary` | `BinaryPolicy::VarBinary(n)` | `varbinary(n)` | runtime check | runtime check | Runtime rejects values whose byte length exceeds `n`. |
| `Binary`, `LargeBinary` | `BinaryPolicy::ObservedVarBinary` | inferred `varbinary(n)` | schema-only reject | schema-only reject | Requires observed values or statistics; schema-only planning currently rejects it. |
| `Decimal32`, `Decimal64`, `Decimal128` with negative scale | `DecimalPolicy::RejectNegativeScale` | none | no | no | Default. |
| `Decimal32`, `Decimal64`, `Decimal128` with negative scale | `DecimalPolicy::NormalizeNegativeScale` | `decimal(p + abs(s), 0)` | runtime check | runtime check | Normalized precision must stay `<= 38`. |
| `Decimal256` | `Decimal256Policy::CheckedDowncast` | `decimal(p, s)` | runtime check | runtime check | Default; SQL Server decimal precision is limited to 38. |
| `Decimal256` | `Decimal256Policy::Reject` | none | no | no | Rejects all `Decimal256` columns. |
| `Date64` | `Date64Policy::RejectNonMidnight` | none | schema-only reject | schema-only reject | Default. The schema-only planner cannot prove every value is midnight. |
| `Date64` | `Date64Policy::TimestampDateTime2` | `datetime2(3)` | yes | yes | Preserves millisecond timestamp information instead of forcing date-only values. |
| Timezone-free `Timestamp(_, None)` | `TimestampPolicy::DateTime2 { precision: p }` | `datetime2(p)` | runtime check | runtime check | Default is `p = 7`; `p` must be in `0..=7`. Values finer than the selected precision are rounded. |
| Timezone-free `Timestamp(_, None)` | `TimestampPolicy::DateTime` | `datetime` | runtime check | runtime check | Uses SQL Server legacy `datetime` range and 1/300 second rounding. Values before 1753 are rejected. |
| Time or timestamp nanosecond values | `NanosecondPolicy::RejectNon100ns` | selected temporal type | runtime check | runtime check | Default. Rejects values not divisible by 100ns. |
| Time or timestamp nanosecond values | `NanosecondPolicy::RoundTo100ns` | selected temporal type | runtime check | runtime check | Rounds to the nearest SQL Server 100ns tick. |
| Time or timestamp nanosecond values | `NanosecondPolicy::TruncateTo100ns` | selected temporal type | runtime check | runtime check | Truncates toward the lower SQL Server 100ns tick. |
| Timezone-aware `Timestamp(_, Some(tz))` | `TimezonePolicy::Reject` | none | no | no | Default when timezone string is non-empty. |
| Timezone-aware `Timestamp(_, Some(tz))` | `TimezonePolicy::NormalizeUtcDateTime2` | selected by `TimestampPolicy` | runtime check | runtime check | Normalizes the Arrow instant to UTC and writes the timezone-free target. The default remains `datetime2(7)`. |
| Timezone-aware `Timestamp(_, Some(tz))` | `TimezonePolicy::DateTimeOffset` | `datetimeoffset(7)` | runtime check | runtime check | Preserves the represented instant with a SQL Server offset. |

An empty Arrow timestamp timezone string is treated as timezone-free and maps to
the target selected by `TimestampPolicy`. The default target is `datetime2(7)`.

## Unsupported Arrow Types

The schema planner rejects these Arrow type families:

| Arrow type family | Status | Notes |
| --- | --- | --- |
| `Null` | no | No SQL Server target type is inferred. |
| `Duration` | no | No SQL Server duration target is currently defined. |
| `Interval` | no | No SQL Server interval target is currently defined. |
| Unsupported time-unit combinations | no | Only `Time32(Second)`, `Time32(Millisecond)`, `Time64(Microsecond)`, and `Time64(Nanosecond)` are mapped. |
| `Utf8View`, `BinaryView` | no | View arrays are not currently mapped. |
| `List`, `LargeList`, `FixedSizeList`, `ListView`, `LargeListView` | no | Nested values are not serialized to SQL Server scalar columns. |
| `Struct`, `Map`, `Union` | no | Nested values are not serialized to SQL Server scalar columns. |
| `Dictionary`, `RunEndEncoded` | no | Encoded arrays are not currently decoded during planning. |

## Writer Backend Notes

`WriteBackend::DirectRawBulk` is the optimized path and supports the mappings
listed above when the selected policy permits them. `WriteBackend::BaselineTokenRow`
is retained as a compatibility and reference path. It should not be treated as
deprecated.

`WriteBackend::DirectFramedBulk` uses the direct Arrow-to-TDS row encoder but
keeps Tiberius' framed packet writer. `WriteBackend::DirectRawBulk` uses the
same row encoder plus the raw bulk direct packet writer.

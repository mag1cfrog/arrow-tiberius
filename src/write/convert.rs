//! Runtime record batch conversion scaffolding.

#![allow(dead_code)]

use std::borrow::Cow;

use arrow_array::{
    Array, BinaryArray, BooleanArray, Date32Array, Date64Array, Decimal32Array, Decimal64Array,
    Decimal128Array, Decimal256Array, Float32Array, Float64Array, Int8Array, Int16Array,
    Int32Array, Int64Array, LargeBinaryArray, LargeStringArray, RecordBatch, StringArray,
    TimestampMicrosecondArray, TimestampMillisecondArray, TimestampNanosecondArray,
    TimestampSecondArray, UInt8Array, UInt16Array, UInt32Array, UInt64Array, timezone::Tz,
};
use arrow_buffer::i256;
use arrow_schema::{DataType, TimeUnit};
use chrono::{Offset, TimeZone};

use crate::{
    Diagnostic, DiagnosticCode, DiagnosticSet, FieldRef, MssqlType, MssqlTypeLength,
    NanosecondPolicy, PlanOptions, Result, SchemaMapping,
};

const SQL_SERVER_DATE_UNIX_EPOCH_DAYS: i64 = 719_162;
const SQL_SERVER_DATE_MAX_DAYS: i64 = 3_652_058;
const MILLISECONDS_PER_DAY: i64 = 86_400_000;
const SQL_SERVER_DATETIME2_DATE64_SCALE: u8 = 3;
const SQL_SERVER_DATETIME2_TIMESTAMP_SCALE: u8 = 7;
const TICKS_100NS_PER_SECOND: i128 = 10_000_000;
const TICKS_100NS_PER_MILLISECOND: i128 = 10_000;
const TICKS_100NS_PER_MICROSECOND: i128 = 10;
const TICKS_100NS_PER_DAY: i128 = 864_000_000_000;
const NANOSECONDS_PER_100NS_TICK: i64 = 100;
/// SQL Server accepts datetimeoffset offsets from -14:00 through +14:00.
const SQL_SERVER_DATETIMEOFFSET_MAX_OFFSET_MINUTES: i16 = 14 * 60;

/// Direction-specific runtime context for Arrow-to-MSSQL value conversion.
#[derive(Debug, Clone, Copy)]
pub(crate) struct ArrowToMssqlRuntimeMapping<'a> {
    mapping: &'a SchemaMapping,
    nanosecond_policy: NanosecondPolicy,
}

impl<'a> ArrowToMssqlRuntimeMapping<'a> {
    /// Creates runtime conversion context from structural mapping and write options.
    pub(crate) const fn new(mapping: &'a SchemaMapping, options: &PlanOptions) -> Self {
        Self {
            mapping,
            nanosecond_policy: options.nanosecond_policy,
        }
    }

    /// Returns the structural Arrow/MSSQL mapping.
    pub(crate) const fn mapping(self) -> &'a SchemaMapping {
        self.mapping
    }

    /// Returns the nanosecond timestamp policy selected for write conversion.
    pub(crate) const fn nanosecond_policy(self) -> NanosecondPolicy {
        self.nanosecond_policy
    }
}

/// Borrowed value extracted from one Arrow array cell.
#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) enum ArrowCell<'a> {
    /// Arrow null value.
    Null,
    /// Arrow boolean value.
    Boolean(bool),
    /// Arrow signed 8-bit integer value.
    Int8(i8),
    /// Arrow signed 16-bit integer value.
    Int16(i16),
    /// Arrow signed 32-bit integer value.
    Int32(i32),
    /// Arrow signed 64-bit integer value.
    Int64(i64),
    /// Arrow unsigned 8-bit integer value.
    UInt8(u8),
    /// Arrow unsigned 16-bit integer value.
    UInt16(u16),
    /// Arrow unsigned 32-bit integer value.
    UInt32(u32),
    /// Arrow unsigned 64-bit integer value.
    UInt64(u64),
    /// Arrow 32-bit decimal value.
    Decimal32(i32),
    /// Arrow 64-bit decimal value.
    Decimal64(i64),
    /// Arrow 128-bit decimal value.
    Decimal128(i128),
    /// Arrow 256-bit decimal value.
    Decimal256(i256),
    /// Arrow Date32 day offset from Unix epoch.
    Date32(i32),
    /// Arrow Date64 millisecond timestamp from Unix epoch.
    Date64(i64),
    /// Arrow timestamp second offset from Unix epoch.
    TimestampSecond(i64),
    /// Arrow timestamp millisecond offset from Unix epoch.
    TimestampMillisecond(i64),
    /// Arrow timestamp microsecond offset from Unix epoch.
    TimestampMicrosecond(i64),
    /// Arrow timestamp nanosecond offset from Unix epoch.
    TimestampNanosecond(i64),
    /// Arrow 32-bit floating point value.
    Float32(f32),
    /// Arrow 64-bit floating point value.
    Float64(f64),
    /// Arrow UTF-8 string value.
    Utf8(&'a str),
    /// Arrow binary value.
    Binary(&'a [u8]),
}

/// Semantic SQL Server value for one planned cell.
#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) enum MssqlCell<'a> {
    /// SQL Server `bit` cell.
    Bit(Option<bool>),
    /// SQL Server `tinyint` cell.
    TinyInt(Option<u8>),
    /// SQL Server `smallint` cell.
    SmallInt(Option<i16>),
    /// SQL Server `int` cell.
    Int(Option<i32>),
    /// SQL Server `bigint` cell.
    BigInt(Option<i64>),
    /// SQL Server `decimal` cell.
    Decimal(Option<MssqlDecimal>),
    /// SQL Server `date` cell.
    Date(Option<MssqlDate>),
    /// SQL Server `datetime2` cell.
    DateTime2(Option<MssqlDateTime2>),
    /// SQL Server `datetimeoffset` cell.
    DateTimeOffset(Option<MssqlDateTimeOffset>),
    /// SQL Server `real` cell.
    Real(Option<f32>),
    /// SQL Server `float` cell.
    Float(Option<f64>),
    /// SQL Server `nvarchar` cell.
    NVarChar(Option<&'a str>),
    /// SQL Server `varbinary` cell.
    VarBinary(Option<&'a [u8]>),
}

/// Semantic SQL Server decimal value.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct MssqlDecimal {
    unscaled: i128,
    scale: u8,
}

impl MssqlDecimal {
    /// Creates a semantic decimal value from its unscaled integer and scale.
    const fn new(unscaled: i128, scale: u8) -> Self {
        Self { unscaled, scale }
    }

    /// Returns the unscaled integer value.
    pub(crate) const fn unscaled(self) -> i128 {
        self.unscaled
    }

    /// Returns the decimal scale.
    pub(crate) const fn scale(self) -> u8 {
        self.scale
    }

    fn to_tiberius_numeric(self) -> tiberius::numeric::Numeric {
        tiberius::numeric::Numeric::new_with_scale(self.unscaled, self.scale)
    }
}

/// Semantic SQL Server date value.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct MssqlDate {
    days: u32,
}

impl MssqlDate {
    /// Creates a semantic date value from SQL Server's day count.
    const fn new(days: u32) -> Self {
        Self { days }
    }

    /// Returns the number of days from 0001-01-01.
    pub(crate) const fn days(self) -> u32 {
        self.days
    }

    fn to_tiberius_date(self) -> tiberius::time::Date {
        tiberius::time::Date::new(self.days)
    }
}

/// Semantic SQL Server datetime2 value.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct MssqlDateTime2 {
    date: MssqlDate,
    time: MssqlTime,
}

impl MssqlDateTime2 {
    /// Creates a semantic datetime2 value from date and time components.
    const fn new(date: MssqlDate, time: MssqlTime) -> Self {
        Self { date, time }
    }

    /// Returns the date component.
    pub(crate) const fn date(self) -> MssqlDate {
        self.date
    }

    /// Returns the time component.
    pub(crate) const fn time(self) -> MssqlTime {
        self.time
    }

    fn to_tiberius_datetime2(self) -> tiberius::time::DateTime2 {
        tiberius::time::DateTime2::new(self.date.to_tiberius_date(), self.time.to_tiberius_time())
    }
}

/// Semantic SQL Server datetimeoffset value.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct MssqlDateTimeOffset {
    datetime2: MssqlDateTime2,
    offset_minutes: i16,
}

impl MssqlDateTimeOffset {
    /// Creates a semantic datetimeoffset value from local date/time and offset.
    ///
    /// The offset is expressed as minutes from UTC, matching SQL Server and
    /// Tiberius `datetimeoffset` encoding.
    const fn new(datetime2: MssqlDateTime2, offset_minutes: i16) -> Self {
        Self {
            datetime2,
            offset_minutes,
        }
    }

    /// Returns the local date/time component.
    pub(crate) const fn datetime2(self) -> MssqlDateTime2 {
        self.datetime2
    }

    /// Returns the number of minutes from UTC.
    pub(crate) const fn offset_minutes(self) -> i16 {
        self.offset_minutes
    }

    /// Converts this crate-owned semantic value into Tiberius' backend value.
    fn to_tiberius_datetimeoffset(self) -> tiberius::time::DateTimeOffset {
        tiberius::time::DateTimeOffset::new(
            self.datetime2.to_tiberius_datetime2(),
            self.offset_minutes,
        )
    }
}

/// Semantic SQL Server time-of-day value.
///
/// SQL Server stores `time`/`datetime2` time-of-day as an integer count of
/// fractional-second increments since midnight. The `scale` says how fine each
/// increment is: scale 0 means whole seconds, scale 3 means milliseconds, and
/// scale 7 means 100ns ticks.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct MssqlTime {
    /// Number of `10^-scale` second increments since midnight.
    ///
    /// For example, at scale 3 this is milliseconds since midnight, so
    /// 12:00:00.123 is `43_200_123`.
    increments: u64,
    /// Fractional-second precision used by `increments`.
    ///
    /// `datetime2(3)` uses scale 3, so one increment is one millisecond.
    scale: u8,
}

impl MssqlTime {
    /// Creates a semantic time value from SQL Server increments and scale.
    ///
    /// This constructor assumes the caller has already validated that
    /// `increments` fits inside one day for the selected `scale`.
    const fn new(increments: u64, scale: u8) -> Self {
        Self { increments, scale }
    }

    /// Returns the number of `10^-scale` second increments since midnight.
    pub(crate) const fn increments(self) -> u64 {
        self.increments
    }

    /// Returns the fractional-second precision used by the increments.
    pub(crate) const fn scale(self) -> u8 {
        self.scale
    }

    fn to_tiberius_time(self) -> tiberius::time::Time {
        tiberius::time::Time::new(self.increments, self.scale)
    }
}

/// Borrowed conversion view over one Arrow record batch and schema mappings.
#[derive(Debug)]
pub(crate) struct RecordBatchView<'a> {
    batch: &'a RecordBatch,
    mappings: &'a [SchemaMapping],
    plan_options: PlanOptions,
}

impl<'a> RecordBatchView<'a> {
    /// Creates a conversion view after validating batch columns against mappings.
    pub(crate) fn new(batch: &'a RecordBatch, mappings: &'a [SchemaMapping]) -> Result<Self> {
        Self::new_with_options(batch, mappings, &PlanOptions::default())
    }

    /// Creates a conversion view with explicit write conversion policies.
    pub(crate) fn new_with_options(
        batch: &'a RecordBatch,
        mappings: &'a [SchemaMapping],
        plan_options: &PlanOptions,
    ) -> Result<Self> {
        validate_runtime_columns(batch, mappings)?;

        Ok(Self {
            batch,
            mappings,
            plan_options: *plan_options,
        })
    }

    /// Returns the number of rows in the runtime batch.
    pub(crate) fn row_count(&self) -> usize {
        self.batch.num_rows()
    }

    /// Returns the planned mappings in conversion order.
    pub(crate) const fn mappings(&self) -> &[SchemaMapping] {
        self.mappings
    }

    /// Checks that a row index is inside the runtime batch.
    pub(crate) fn check_row_index(&self, row_index: usize) -> Result<()> {
        if row_index < self.row_count() {
            return Ok(());
        }

        let message = format!(
            "row index {row_index} is outside runtime batch with {} row(s)",
            self.row_count()
        );
        Err(value_conversion_error(
            Diagnostic::error(DiagnosticCode::RowIndexOutOfBounds, message).with_row(row_index),
        ))
    }

    /// Extracts one borrowed Arrow cell from a planned mapping and row index.
    fn arrow_cell(&self, mapping: &SchemaMapping, row_index: usize) -> Result<ArrowCell<'_>> {
        self.check_row_index(row_index)?;

        let Some(array) = self
            .batch
            .columns()
            .get(mapping.arrow().index())
            .map(AsRef::as_ref)
        else {
            return Err(value_conversion_error(row_mapping_diagnostic(
                mapping,
                row_index,
                DiagnosticCode::ValueTypeMismatch,
                "planned column index is outside the runtime batch",
            )));
        };

        extract_arrow_cell(array, mapping, row_index)
    }

    /// Converts one planned cell into a semantic SQL Server cell.
    fn mssql_cell(&self, mapping: &SchemaMapping, row_index: usize) -> Result<MssqlCell<'_>> {
        let cell = self.arrow_cell(mapping, row_index)?;
        let runtime_mapping = ArrowToMssqlRuntimeMapping::new(mapping, &self.plan_options);
        mssql_cell_from_arrow_cell(runtime_mapping, cell, row_index)
    }

    /// Converts one runtime row into semantic SQL Server cells in mapping order.
    pub(crate) fn mssql_row(&self, row_index: usize) -> Result<Vec<MssqlCell<'_>>> {
        self.check_row_index(row_index)?;

        let mut cells = Vec::with_capacity(self.mappings.len());
        for mapping in self.mappings {
            cells.push(self.mssql_cell(mapping, row_index)?);
        }

        Ok(cells)
    }

    /// Converts one runtime row into an owned Tiberius token row.
    pub(crate) fn tiberius_row_owned(
        &self,
        row_index: usize,
    ) -> Result<tiberius::TokenRow<'static>> {
        let cells = self.mssql_row(row_index)?;
        let mut row = tiberius::TokenRow::with_capacity(cells.len());

        for cell in cells {
            row.push(mssql_cell_to_tiberius_owned(cell));
        }

        Ok(row)
    }
}

/// Converts a semantic SQL Server cell into borrowed Tiberius column data.
pub(crate) fn mssql_cell_to_tiberius_borrowed(cell: MssqlCell<'_>) -> tiberius::ColumnData<'_> {
    match cell {
        MssqlCell::Bit(value) => tiberius::ColumnData::Bit(value),
        MssqlCell::TinyInt(value) => tiberius::ColumnData::U8(value),
        MssqlCell::SmallInt(value) => tiberius::ColumnData::I16(value),
        MssqlCell::Int(value) => tiberius::ColumnData::I32(value),
        MssqlCell::BigInt(value) => tiberius::ColumnData::I64(value),
        MssqlCell::Decimal(value) => {
            tiberius::ColumnData::Numeric(value.map(MssqlDecimal::to_tiberius_numeric))
        }
        MssqlCell::Date(value) => {
            tiberius::ColumnData::Date(value.map(MssqlDate::to_tiberius_date))
        }
        MssqlCell::DateTime2(value) => {
            tiberius::ColumnData::DateTime2(value.map(MssqlDateTime2::to_tiberius_datetime2))
        }
        MssqlCell::DateTimeOffset(value) => tiberius::ColumnData::DateTimeOffset(
            value.map(MssqlDateTimeOffset::to_tiberius_datetimeoffset),
        ),
        MssqlCell::Real(value) => tiberius::ColumnData::F32(value),
        MssqlCell::Float(value) => tiberius::ColumnData::F64(value),
        MssqlCell::NVarChar(value) => tiberius::ColumnData::String(value.map(Cow::Borrowed)),
        MssqlCell::VarBinary(value) => tiberius::ColumnData::Binary(value.map(Cow::Borrowed)),
    }
}

/// Converts a semantic SQL Server cell into owned Tiberius column data.
pub(crate) fn mssql_cell_to_tiberius_owned(cell: MssqlCell<'_>) -> tiberius::ColumnData<'static> {
    match cell {
        MssqlCell::Bit(value) => tiberius::ColumnData::Bit(value),
        MssqlCell::TinyInt(value) => tiberius::ColumnData::U8(value),
        MssqlCell::SmallInt(value) => tiberius::ColumnData::I16(value),
        MssqlCell::Int(value) => tiberius::ColumnData::I32(value),
        MssqlCell::BigInt(value) => tiberius::ColumnData::I64(value),
        MssqlCell::Decimal(value) => {
            tiberius::ColumnData::Numeric(value.map(MssqlDecimal::to_tiberius_numeric))
        }
        MssqlCell::Date(value) => {
            tiberius::ColumnData::Date(value.map(MssqlDate::to_tiberius_date))
        }
        MssqlCell::DateTime2(value) => {
            tiberius::ColumnData::DateTime2(value.map(MssqlDateTime2::to_tiberius_datetime2))
        }
        MssqlCell::DateTimeOffset(value) => tiberius::ColumnData::DateTimeOffset(
            value.map(MssqlDateTimeOffset::to_tiberius_datetimeoffset),
        ),
        MssqlCell::Real(value) => tiberius::ColumnData::F32(value),
        MssqlCell::Float(value) => tiberius::ColumnData::F64(value),
        MssqlCell::NVarChar(value) => {
            tiberius::ColumnData::String(value.map(|value| Cow::Owned(value.to_owned())))
        }
        MssqlCell::VarBinary(value) => {
            tiberius::ColumnData::Binary(value.map(|value| Cow::Owned(value.to_vec())))
        }
    }
}

fn extract_arrow_cell<'a>(
    array: &'a dyn Array,
    mapping: &SchemaMapping,
    row_index: usize,
) -> Result<ArrowCell<'a>> {
    if array.is_null(row_index) {
        return Ok(ArrowCell::Null);
    }

    match mapping.arrow().data_type() {
        DataType::Boolean => {
            let array = downcast_array::<BooleanArray>(array, mapping, row_index)?;
            Ok(ArrowCell::Boolean(array.value(row_index)))
        }
        DataType::Int8 => {
            let array = downcast_array::<Int8Array>(array, mapping, row_index)?;
            Ok(ArrowCell::Int8(array.value(row_index)))
        }
        DataType::Int16 => {
            let array = downcast_array::<Int16Array>(array, mapping, row_index)?;
            Ok(ArrowCell::Int16(array.value(row_index)))
        }
        DataType::Int32 => {
            let array = downcast_array::<Int32Array>(array, mapping, row_index)?;
            Ok(ArrowCell::Int32(array.value(row_index)))
        }
        DataType::Int64 => {
            let array = downcast_array::<Int64Array>(array, mapping, row_index)?;
            Ok(ArrowCell::Int64(array.value(row_index)))
        }
        DataType::UInt8 => {
            let array = downcast_array::<UInt8Array>(array, mapping, row_index)?;
            Ok(ArrowCell::UInt8(array.value(row_index)))
        }
        DataType::UInt16 => {
            let array = downcast_array::<UInt16Array>(array, mapping, row_index)?;
            Ok(ArrowCell::UInt16(array.value(row_index)))
        }
        DataType::UInt32 => {
            let array = downcast_array::<UInt32Array>(array, mapping, row_index)?;
            Ok(ArrowCell::UInt32(array.value(row_index)))
        }
        DataType::UInt64 => {
            let array = downcast_array::<UInt64Array>(array, mapping, row_index)?;
            Ok(ArrowCell::UInt64(array.value(row_index)))
        }
        DataType::Decimal32(_, _) => {
            let array = downcast_array::<Decimal32Array>(array, mapping, row_index)?;
            Ok(ArrowCell::Decimal32(array.value(row_index)))
        }
        DataType::Decimal64(_, _) => {
            let array = downcast_array::<Decimal64Array>(array, mapping, row_index)?;
            Ok(ArrowCell::Decimal64(array.value(row_index)))
        }
        DataType::Decimal128(_, _) => {
            let array = downcast_array::<Decimal128Array>(array, mapping, row_index)?;
            Ok(ArrowCell::Decimal128(array.value(row_index)))
        }
        DataType::Decimal256(_, _) => {
            let array = downcast_array::<Decimal256Array>(array, mapping, row_index)?;
            Ok(ArrowCell::Decimal256(array.value(row_index)))
        }
        DataType::Date32 => {
            let array = downcast_array::<Date32Array>(array, mapping, row_index)?;
            Ok(ArrowCell::Date32(array.value(row_index)))
        }
        DataType::Date64 => {
            let array = downcast_array::<Date64Array>(array, mapping, row_index)?;
            Ok(ArrowCell::Date64(array.value(row_index)))
        }
        DataType::Timestamp(time_unit, _) => match time_unit {
            TimeUnit::Second => {
                let array = downcast_array::<TimestampSecondArray>(array, mapping, row_index)?;
                Ok(ArrowCell::TimestampSecond(array.value(row_index)))
            }
            TimeUnit::Millisecond => {
                let array = downcast_array::<TimestampMillisecondArray>(array, mapping, row_index)?;
                Ok(ArrowCell::TimestampMillisecond(array.value(row_index)))
            }
            TimeUnit::Microsecond => {
                let array = downcast_array::<TimestampMicrosecondArray>(array, mapping, row_index)?;
                Ok(ArrowCell::TimestampMicrosecond(array.value(row_index)))
            }
            TimeUnit::Nanosecond => {
                let array = downcast_array::<TimestampNanosecondArray>(array, mapping, row_index)?;
                Ok(ArrowCell::TimestampNanosecond(array.value(row_index)))
            }
        },
        DataType::Float32 => {
            let array = downcast_array::<Float32Array>(array, mapping, row_index)?;
            Ok(ArrowCell::Float32(array.value(row_index)))
        }
        DataType::Float64 => {
            let array = downcast_array::<Float64Array>(array, mapping, row_index)?;
            Ok(ArrowCell::Float64(array.value(row_index)))
        }
        DataType::Utf8 => {
            let array = downcast_array::<StringArray>(array, mapping, row_index)?;
            Ok(ArrowCell::Utf8(array.value(row_index)))
        }
        DataType::LargeUtf8 => {
            let array = downcast_array::<LargeStringArray>(array, mapping, row_index)?;
            Ok(ArrowCell::Utf8(array.value(row_index)))
        }
        DataType::Binary => {
            let array = downcast_array::<BinaryArray>(array, mapping, row_index)?;
            Ok(ArrowCell::Binary(array.value(row_index)))
        }
        DataType::LargeBinary => {
            let array = downcast_array::<LargeBinaryArray>(array, mapping, row_index)?;
            Ok(ArrowCell::Binary(array.value(row_index)))
        }
        other => Err(unsupported_value_conversion(
            mapping,
            row_index,
            format!("Arrow value extraction for {other} is not supported yet"),
        )),
    }
}

fn mssql_bit_value(mapping: &SchemaMapping, row_index: usize, cell: ArrowCell<'_>) -> Result<bool> {
    match cell {
        ArrowCell::Boolean(value) => Ok(value),
        other => Err(value_conversion_error(row_mapping_diagnostic(
            mapping,
            row_index,
            DiagnosticCode::ValueTypeMismatch,
            format!("expected Arrow boolean payload, got {other:?}"),
        ))),
    }
}

fn mssql_tinyint_value(
    mapping: &SchemaMapping,
    row_index: usize,
    cell: ArrowCell<'_>,
) -> Result<u8> {
    match cell {
        ArrowCell::UInt8(value) => Ok(value),
        other => Err(value_conversion_error(row_mapping_diagnostic(
            mapping,
            row_index,
            DiagnosticCode::ValueTypeMismatch,
            format!("expected Arrow UInt8 payload, got {other:?}"),
        ))),
    }
}

fn mssql_smallint_value(
    mapping: &SchemaMapping,
    row_index: usize,
    cell: ArrowCell<'_>,
) -> Result<i16> {
    match cell {
        ArrowCell::Int8(value) => Ok(i16::from(value)),
        ArrowCell::Int16(value) => Ok(value),
        other => Err(value_conversion_error(row_mapping_diagnostic(
            mapping,
            row_index,
            DiagnosticCode::ValueTypeMismatch,
            format!("expected Arrow Int8 or Int16 payload, got {other:?}"),
        ))),
    }
}

fn mssql_int_value(mapping: &SchemaMapping, row_index: usize, cell: ArrowCell<'_>) -> Result<i32> {
    match cell {
        ArrowCell::Int32(value) => Ok(value),
        ArrowCell::UInt16(value) => Ok(i32::from(value)),
        other => Err(value_conversion_error(row_mapping_diagnostic(
            mapping,
            row_index,
            DiagnosticCode::ValueTypeMismatch,
            format!("expected Arrow Int32 or UInt16 payload, got {other:?}"),
        ))),
    }
}

fn mssql_bigint_value(
    mapping: &SchemaMapping,
    row_index: usize,
    cell: ArrowCell<'_>,
) -> Result<i64> {
    match cell {
        ArrowCell::Int64(value) => Ok(value),
        ArrowCell::UInt32(value) => Ok(i64::from(value)),
        ArrowCell::UInt64(value) => i64::try_from(value).map_err(|_| {
            value_conversion_error(row_mapping_diagnostic(
                mapping,
                row_index,
                DiagnosticCode::IntegerOutOfRange,
                format!("Arrow UInt64 value {value} does not fit planned SQL Server bigint"),
            ))
        }),
        other => Err(value_conversion_error(row_mapping_diagnostic(
            mapping,
            row_index,
            DiagnosticCode::ValueTypeMismatch,
            format!("expected Arrow Int64, UInt32, or UInt64 payload, got {other:?}"),
        ))),
    }
}

fn mssql_decimal_value(
    mapping: &SchemaMapping,
    row_index: usize,
    cell: ArrowCell<'_>,
) -> Result<MssqlDecimal> {
    let scale = decimal_scale(mapping, row_index)?;
    validate_decimal_scale_compatibility(mapping, row_index, scale)?;

    match (cell, mapping.arrow().data_type()) {
        (ArrowCell::UInt64(value), DataType::UInt64) if is_uint64_decimal20_0_mapping(mapping) => {
            mssql_decimal(mapping, row_index, i128::from(value), scale)
        }
        (ArrowCell::Decimal32(value), DataType::Decimal32(_, arrow_scale)) if *arrow_scale >= 0 => {
            mssql_decimal(mapping, row_index, i128::from(value), scale)
        }
        (ArrowCell::Decimal32(value), DataType::Decimal32(_, arrow_scale)) => {
            let value =
                normalize_negative_scale(mapping, row_index, i128::from(value), *arrow_scale)?;
            mssql_decimal(mapping, row_index, value, scale)
        }
        (ArrowCell::Decimal64(value), DataType::Decimal64(_, arrow_scale)) if *arrow_scale >= 0 => {
            mssql_decimal(mapping, row_index, i128::from(value), scale)
        }
        (ArrowCell::Decimal64(value), DataType::Decimal64(_, arrow_scale)) => {
            let value =
                normalize_negative_scale(mapping, row_index, i128::from(value), *arrow_scale)?;
            mssql_decimal(mapping, row_index, value, scale)
        }
        (ArrowCell::Decimal128(value), DataType::Decimal128(_, arrow_scale))
            if *arrow_scale >= 0 =>
        {
            mssql_decimal(mapping, row_index, value, scale)
        }
        (ArrowCell::Decimal128(value), DataType::Decimal128(_, arrow_scale)) => {
            let value = normalize_negative_scale(mapping, row_index, value, *arrow_scale)?;
            mssql_decimal(mapping, row_index, value, scale)
        }
        (ArrowCell::Decimal256(value), DataType::Decimal256(_, arrow_scale))
            if *arrow_scale >= 0 =>
        {
            let value = value.to_i128().ok_or_else(|| {
                value_conversion_error(row_mapping_diagnostic(
                    mapping,
                    row_index,
                    DiagnosticCode::DecimalOutOfRange,
                    "Arrow Decimal256 value does not fit runtime i128 decimal representation",
                ))
            })?;
            mssql_decimal(mapping, row_index, value, scale)
        }
        (ArrowCell::Decimal256(value), DataType::Decimal256(_, arrow_scale)) => {
            let value = value.to_i128().ok_or_else(|| {
                value_conversion_error(row_mapping_diagnostic(
                    mapping,
                    row_index,
                    DiagnosticCode::DecimalOutOfRange,
                    "Arrow Decimal256 value does not fit runtime i128 decimal representation",
                ))
            })?;
            let value = normalize_negative_scale(mapping, row_index, value, *arrow_scale)?;
            mssql_decimal(mapping, row_index, value, scale)
        }
        other => Err(value_conversion_error(row_mapping_diagnostic(
            mapping,
            row_index,
            DiagnosticCode::ValueTypeMismatch,
            format!("expected Arrow decimal-compatible payload, got {other:?}"),
        ))),
    }
}

fn mssql_date_value(
    mapping: &SchemaMapping,
    row_index: usize,
    cell: ArrowCell<'_>,
) -> Result<MssqlDate> {
    match (cell, mapping.arrow().data_type()) {
        (ArrowCell::Date32(value), DataType::Date32) => {
            mssql_date_from_arrow_date32(mapping, row_index, value)
        }
        other => Err(value_conversion_error(row_mapping_diagnostic(
            mapping,
            row_index,
            DiagnosticCode::ValueTypeMismatch,
            format!("expected Arrow Date32 payload, got {other:?}"),
        ))),
    }
}

fn mssql_datetime2_value(
    runtime_mapping: ArrowToMssqlRuntimeMapping<'_>,
    row_index: usize,
    cell: ArrowCell<'_>,
) -> Result<MssqlDateTime2> {
    let mapping = runtime_mapping.mapping();

    match (cell, mapping.arrow().data_type(), mapping.mssql().ty()) {
        (
            ArrowCell::Date64(value),
            DataType::Date64,
            MssqlType::DateTime2 {
                precision: SQL_SERVER_DATETIME2_DATE64_SCALE,
            },
        ) => mssql_datetime2_from_arrow_date64(mapping, row_index, value),
        (
            ArrowCell::TimestampSecond(value),
            DataType::Timestamp(TimeUnit::Second, timezone),
            MssqlType::DateTime2 {
                precision: SQL_SERVER_DATETIME2_TIMESTAMP_SCALE,
            },
        ) => {
            validate_timestamp_timezone_metadata(mapping, row_index, timezone.as_deref())?;
            mssql_datetime2_from_arrow_timestamp_second(mapping, row_index, value)
        }
        (
            ArrowCell::TimestampMillisecond(value),
            DataType::Timestamp(TimeUnit::Millisecond, timezone),
            MssqlType::DateTime2 {
                precision: SQL_SERVER_DATETIME2_TIMESTAMP_SCALE,
            },
        ) => {
            validate_timestamp_timezone_metadata(mapping, row_index, timezone.as_deref())?;
            mssql_datetime2_from_arrow_timestamp_millisecond(mapping, row_index, value)
        }
        (
            ArrowCell::TimestampMicrosecond(value),
            DataType::Timestamp(TimeUnit::Microsecond, timezone),
            MssqlType::DateTime2 {
                precision: SQL_SERVER_DATETIME2_TIMESTAMP_SCALE,
            },
        ) => {
            validate_timestamp_timezone_metadata(mapping, row_index, timezone.as_deref())?;
            mssql_datetime2_from_arrow_timestamp_microsecond(mapping, row_index, value)
        }
        (
            ArrowCell::TimestampNanosecond(value),
            DataType::Timestamp(TimeUnit::Nanosecond, timezone),
            MssqlType::DateTime2 {
                precision: SQL_SERVER_DATETIME2_TIMESTAMP_SCALE,
            },
        ) => {
            validate_timestamp_timezone_metadata(mapping, row_index, timezone.as_deref())?;
            mssql_datetime2_from_arrow_timestamp_nanosecond(
                mapping,
                row_index,
                value,
                runtime_mapping.nanosecond_policy(),
            )
        }
        other => Err(value_conversion_error(row_mapping_diagnostic(
            mapping,
            row_index,
            DiagnosticCode::ValueTypeMismatch,
            format!(
                "expected Arrow Date64 or timestamp payload planned as datetime2, got {other:?}"
            ),
        ))),
    }
}

fn mssql_datetimeoffset_value(
    runtime_mapping: ArrowToMssqlRuntimeMapping<'_>,
    row_index: usize,
    cell: ArrowCell<'_>,
) -> Result<MssqlDateTimeOffset> {
    let mapping = runtime_mapping.mapping();

    match (cell, mapping.arrow().data_type(), mapping.mssql().ty()) {
        (
            ArrowCell::TimestampSecond(value),
            DataType::Timestamp(TimeUnit::Second, Some(timezone)),
            MssqlType::DateTimeOffset {
                precision: SQL_SERVER_DATETIME2_TIMESTAMP_SCALE,
            },
        ) => {
            let resolution = timezone_resolution_from_metadata(mapping, row_index, timezone)?;
            let offset_minutes = resolution.offset_for_instant(mapping, row_index, value, 0)?;
            let utc_ticks = i128::from(value) * TICKS_100NS_PER_SECOND;
            mssql_datetimeoffset_from_utc_100ns_ticks(
                mapping,
                row_index,
                utc_ticks,
                offset_minutes,
                "second",
                value,
            )
        }
        (
            ArrowCell::TimestampMillisecond(value),
            DataType::Timestamp(TimeUnit::Millisecond, Some(timezone)),
            MssqlType::DateTimeOffset {
                precision: SQL_SERVER_DATETIME2_TIMESTAMP_SCALE,
            },
        ) => {
            let (seconds, nanoseconds) = epoch_parts_from_milliseconds(mapping, row_index, value)?;
            let resolution = timezone_resolution_from_metadata(mapping, row_index, timezone)?;
            let offset_minutes =
                resolution.offset_for_instant(mapping, row_index, seconds, nanoseconds)?;
            let utc_ticks = i128::from(value) * TICKS_100NS_PER_MILLISECOND;
            mssql_datetimeoffset_from_utc_100ns_ticks(
                mapping,
                row_index,
                utc_ticks,
                offset_minutes,
                "millisecond",
                value,
            )
        }
        (
            ArrowCell::TimestampMicrosecond(value),
            DataType::Timestamp(TimeUnit::Microsecond, Some(timezone)),
            MssqlType::DateTimeOffset {
                precision: SQL_SERVER_DATETIME2_TIMESTAMP_SCALE,
            },
        ) => {
            let (seconds, nanoseconds) = epoch_parts_from_microseconds(mapping, row_index, value)?;
            let resolution = timezone_resolution_from_metadata(mapping, row_index, timezone)?;
            let offset_minutes =
                resolution.offset_for_instant(mapping, row_index, seconds, nanoseconds)?;
            let utc_ticks = i128::from(value) * TICKS_100NS_PER_MICROSECOND;
            mssql_datetimeoffset_from_utc_100ns_ticks(
                mapping,
                row_index,
                utc_ticks,
                offset_minutes,
                "microsecond",
                value,
            )
        }
        (
            ArrowCell::TimestampNanosecond(value),
            DataType::Timestamp(TimeUnit::Nanosecond, Some(timezone)),
            MssqlType::DateTimeOffset {
                precision: SQL_SERVER_DATETIME2_TIMESTAMP_SCALE,
            },
        ) => {
            let (seconds, nanoseconds) = epoch_parts_from_nanoseconds(mapping, row_index, value)?;
            let resolution = timezone_resolution_from_metadata(mapping, row_index, timezone)?;
            let offset_minutes =
                resolution.offset_for_instant(mapping, row_index, seconds, nanoseconds)?;
            let utc_ticks = nanoseconds_to_100ns_ticks(
                mapping,
                row_index,
                value,
                runtime_mapping.nanosecond_policy(),
            )?;
            mssql_datetimeoffset_from_utc_100ns_ticks(
                mapping,
                row_index,
                utc_ticks,
                offset_minutes,
                "nanosecond",
                value,
            )
        }
        other => Err(value_conversion_error(row_mapping_diagnostic(
            mapping,
            row_index,
            DiagnosticCode::ValueTypeMismatch,
            format!(
                "expected timezone-aware Arrow timestamp payload planned as datetimeoffset, got {other:?}"
            ),
        ))),
    }
}

fn mssql_real_value(mapping: &SchemaMapping, row_index: usize, cell: ArrowCell<'_>) -> Result<f32> {
    match cell {
        ArrowCell::Float32(value) if value.is_finite() => Ok(value),
        ArrowCell::Float32(value) => Err(non_finite_float_error(mapping, row_index, value)),
        other => Err(value_conversion_error(row_mapping_diagnostic(
            mapping,
            row_index,
            DiagnosticCode::ValueTypeMismatch,
            format!("expected Arrow Float32 payload, got {other:?}"),
        ))),
    }
}

fn mssql_float_value(
    mapping: &SchemaMapping,
    row_index: usize,
    cell: ArrowCell<'_>,
) -> Result<f64> {
    match cell {
        ArrowCell::Float64(value) if value.is_finite() => Ok(value),
        ArrowCell::Float64(value) => Err(non_finite_float_error(mapping, row_index, value)),
        other => Err(value_conversion_error(row_mapping_diagnostic(
            mapping,
            row_index,
            DiagnosticCode::ValueTypeMismatch,
            format!("expected Arrow Float64 payload, got {other:?}"),
        ))),
    }
}

fn mssql_nvarchar_value<'a>(
    mapping: &SchemaMapping,
    row_index: usize,
    cell: ArrowCell<'a>,
) -> Result<&'a str> {
    match cell {
        ArrowCell::Utf8(value) => Ok(value),
        other => Err(value_conversion_error(row_mapping_diagnostic(
            mapping,
            row_index,
            DiagnosticCode::ValueTypeMismatch,
            format!("expected Arrow UTF-8 payload, got {other:?}"),
        ))),
    }
}

fn mssql_varbinary_value<'a>(
    mapping: &SchemaMapping,
    row_index: usize,
    cell: ArrowCell<'a>,
) -> Result<&'a [u8]> {
    match cell {
        ArrowCell::Binary(value) => Ok(value),
        other => Err(value_conversion_error(row_mapping_diagnostic(
            mapping,
            row_index,
            DiagnosticCode::ValueTypeMismatch,
            format!("expected Arrow binary payload, got {other:?}"),
        ))),
    }
}

fn mssql_cell_from_arrow_cell<'a>(
    runtime_mapping: ArrowToMssqlRuntimeMapping<'_>,
    cell: ArrowCell<'a>,
    row_index: usize,
) -> Result<MssqlCell<'a>> {
    let mapping = runtime_mapping.mapping();

    if matches!(cell, ArrowCell::Null) {
        if !mapping.mssql().nullable() {
            return Err(value_conversion_error(row_mapping_diagnostic(
                mapping,
                row_index,
                DiagnosticCode::NullInNonNullableColumn,
                "null value in non-nullable planned column",
            )));
        }

        return null_mssql_cell(mapping, row_index);
    }

    match mapping.mssql().ty() {
        MssqlType::Bit => Ok(MssqlCell::Bit(Some(mssql_bit_value(
            mapping, row_index, cell,
        )?))),
        MssqlType::TinyInt => Ok(MssqlCell::TinyInt(Some(mssql_tinyint_value(
            mapping, row_index, cell,
        )?))),
        MssqlType::SmallInt => Ok(MssqlCell::SmallInt(Some(mssql_smallint_value(
            mapping, row_index, cell,
        )?))),
        MssqlType::Int => Ok(MssqlCell::Int(Some(mssql_int_value(
            mapping, row_index, cell,
        )?))),
        MssqlType::BigInt => Ok(MssqlCell::BigInt(Some(mssql_bigint_value(
            mapping, row_index, cell,
        )?))),
        MssqlType::Decimal { .. } => Ok(MssqlCell::Decimal(Some(mssql_decimal_value(
            mapping, row_index, cell,
        )?))),
        MssqlType::Date => Ok(MssqlCell::Date(Some(mssql_date_value(
            mapping, row_index, cell,
        )?))),
        MssqlType::DateTime2 { .. } => Ok(MssqlCell::DateTime2(Some(mssql_datetime2_value(
            runtime_mapping,
            row_index,
            cell,
        )?))),
        MssqlType::DateTimeOffset { .. } => Ok(MssqlCell::DateTimeOffset(Some(
            mssql_datetimeoffset_value(runtime_mapping, row_index, cell)?,
        ))),
        MssqlType::Real => Ok(MssqlCell::Real(Some(mssql_real_value(
            mapping, row_index, cell,
        )?))),
        MssqlType::Float { .. } => Ok(MssqlCell::Float(Some(mssql_float_value(
            mapping, row_index, cell,
        )?))),
        MssqlType::NVarChar(length) => nvar_char_cell(mapping, row_index, *length, cell),
        MssqlType::VarBinary(length) => var_binary_cell(mapping, row_index, *length, cell),
    }
}

fn null_mssql_cell<'a>(mapping: &SchemaMapping, row_index: usize) -> Result<MssqlCell<'a>> {
    match mapping.mssql().ty() {
        MssqlType::Bit => Ok(MssqlCell::Bit(None)),
        MssqlType::TinyInt => Ok(MssqlCell::TinyInt(None)),
        MssqlType::SmallInt => Ok(MssqlCell::SmallInt(None)),
        MssqlType::Int => Ok(MssqlCell::Int(None)),
        MssqlType::BigInt => Ok(MssqlCell::BigInt(None)),
        MssqlType::Decimal { .. } if supports_null_decimal_cell(mapping) => {
            Ok(MssqlCell::Decimal(None))
        }
        MssqlType::Date => Ok(MssqlCell::Date(None)),
        MssqlType::DateTime2 { .. } if supports_null_datetime2_cell(mapping) => {
            Ok(MssqlCell::DateTime2(None))
        }
        MssqlType::DateTimeOffset { .. } if supports_null_datetimeoffset_cell(mapping) => {
            Ok(MssqlCell::DateTimeOffset(None))
        }
        MssqlType::Real => Ok(MssqlCell::Real(None)),
        MssqlType::Float { .. } => Ok(MssqlCell::Float(None)),
        MssqlType::NVarChar(_) => Ok(MssqlCell::NVarChar(None)),
        MssqlType::VarBinary(_) => Ok(MssqlCell::VarBinary(None)),
        ty => Err(unsupported_value_conversion(
            mapping,
            row_index,
            format!(
                "planned SQL Server type {} is not supported yet",
                ty.to_sql()
            ),
        )),
    }
}

fn is_uint64_decimal20_0_mapping(mapping: &SchemaMapping) -> bool {
    matches!(
        (mapping.arrow().data_type(), mapping.mssql().ty()),
        (
            DataType::UInt64,
            MssqlType::Decimal {
                precision: 20,
                scale: 0
            }
        )
    )
}

fn supports_null_decimal_cell(mapping: &SchemaMapping) -> bool {
    matches!(
        mapping.arrow().data_type(),
        DataType::UInt64
            | DataType::Decimal32(_, _)
            | DataType::Decimal64(_, _)
            | DataType::Decimal128(_, _)
            | DataType::Decimal256(_, _)
    ) && matches!(mapping.mssql().ty(), MssqlType::Decimal { .. })
}

fn supports_null_datetime2_cell(mapping: &SchemaMapping) -> bool {
    match (mapping.arrow().data_type(), mapping.mssql().ty()) {
        (
            DataType::Date64,
            MssqlType::DateTime2 {
                precision: SQL_SERVER_DATETIME2_DATE64_SCALE,
            },
        ) => true,
        (
            DataType::Timestamp(
                TimeUnit::Second
                | TimeUnit::Millisecond
                | TimeUnit::Microsecond
                | TimeUnit::Nanosecond,
                _,
            ),
            MssqlType::DateTime2 {
                precision: SQL_SERVER_DATETIME2_TIMESTAMP_SCALE,
            },
        ) => true,
        _ => false,
    }
}

fn supports_null_datetimeoffset_cell(mapping: &SchemaMapping) -> bool {
    matches!(
        (mapping.arrow().data_type(), mapping.mssql().ty()),
        (
            DataType::Timestamp(
                TimeUnit::Second
                    | TimeUnit::Millisecond
                    | TimeUnit::Microsecond
                    | TimeUnit::Nanosecond,
                Some(_)
            ),
            MssqlType::DateTimeOffset {
                precision: SQL_SERVER_DATETIME2_TIMESTAMP_SCALE,
            }
        )
    )
}

fn decimal_scale(mapping: &SchemaMapping, row_index: usize) -> Result<u8> {
    let MssqlType::Decimal { scale, .. } = mapping.mssql().ty() else {
        return Err(value_conversion_error(row_mapping_diagnostic(
            mapping,
            row_index,
            DiagnosticCode::ValueTypeMismatch,
            "planned SQL Server type is not decimal",
        )));
    };

    let scale = u8::try_from(*scale).map_err(|_| {
        value_conversion_error(row_mapping_diagnostic(
            mapping,
            row_index,
            DiagnosticCode::DecimalOutOfRange,
            format!(
                "planned SQL Server decimal scale {scale} cannot be represented by Tiberius Numeric"
            ),
        ))
    })?;

    if scale >= 38 {
        return Err(value_conversion_error(row_mapping_diagnostic(
            mapping,
            row_index,
            DiagnosticCode::DecimalOutOfRange,
            format!(
                "planned SQL Server decimal scale {scale} cannot be represented by Tiberius Numeric"
            ),
        )));
    }

    Ok(scale)
}

fn validate_decimal_scale_compatibility(
    mapping: &SchemaMapping,
    row_index: usize,
    planned_scale: u8,
) -> Result<()> {
    let expected_scale = match mapping.arrow().data_type() {
        DataType::UInt64 if is_uint64_decimal20_0_mapping(mapping) => 0,
        DataType::Decimal32(_, arrow_scale)
        | DataType::Decimal64(_, arrow_scale)
        | DataType::Decimal128(_, arrow_scale)
        | DataType::Decimal256(_, arrow_scale)
            if *arrow_scale < 0 =>
        {
            0
        }
        DataType::Decimal32(_, arrow_scale)
        | DataType::Decimal64(_, arrow_scale)
        | DataType::Decimal128(_, arrow_scale)
        | DataType::Decimal256(_, arrow_scale) => u8::try_from(*arrow_scale).map_err(|_| {
            value_conversion_error(row_mapping_diagnostic(
                mapping,
                row_index,
                DiagnosticCode::DecimalOutOfRange,
                format!("Arrow decimal scale {arrow_scale} cannot be represented at runtime"),
            ))
        })?,
        _ => return Ok(()),
    };

    if planned_scale == expected_scale {
        return Ok(());
    }

    Err(value_conversion_error(row_mapping_diagnostic(
        mapping,
        row_index,
        DiagnosticCode::SchemaMismatch,
        format!(
            "planned SQL Server decimal scale {planned_scale} is incompatible with Arrow decimal scale {expected_scale}"
        ),
    )))
}

fn mssql_decimal(
    mapping: &SchemaMapping,
    row_index: usize,
    unscaled: i128,
    scale: u8,
) -> Result<MssqlDecimal> {
    let MssqlType::Decimal { precision, .. } = mapping.mssql().ty() else {
        return Err(value_conversion_error(row_mapping_diagnostic(
            mapping,
            row_index,
            DiagnosticCode::ValueTypeMismatch,
            "planned SQL Server type is not decimal",
        )));
    };

    if decimal_unscaled_fits_precision(unscaled, *precision) {
        return Ok(MssqlDecimal::new(unscaled, scale));
    }

    Err(value_conversion_error(row_mapping_diagnostic(
        mapping,
        row_index,
        DiagnosticCode::DecimalOutOfRange,
        format!("decimal value {unscaled} does not fit planned precision {precision}"),
    )))
}

fn normalize_negative_scale(
    mapping: &SchemaMapping,
    row_index: usize,
    unscaled: i128,
    arrow_scale: i8,
) -> Result<i128> {
    if arrow_scale >= 0 {
        return Ok(unscaled);
    }

    let factor = 10_i128
        .checked_pow(u32::from(arrow_scale.unsigned_abs()))
        .ok_or_else(|| {
            value_conversion_error(row_mapping_diagnostic(
                mapping,
                row_index,
                DiagnosticCode::DecimalOutOfRange,
                format!("negative decimal scale {arrow_scale} normalization factor overflows"),
            ))
        })?;

    unscaled.checked_mul(factor).ok_or_else(|| {
        value_conversion_error(row_mapping_diagnostic(
            mapping,
            row_index,
            DiagnosticCode::DecimalOutOfRange,
            format!("decimal value {unscaled} overflows while normalizing scale {arrow_scale}"),
        ))
    })
}

fn decimal_unscaled_fits_precision(value: i128, precision: u8) -> bool {
    if precision == 0 {
        return false;
    }

    let Some(max) = decimal_max_unscaled(precision) else {
        return false;
    };

    value <= max && value >= -max
}

fn decimal_max_unscaled(precision: u8) -> Option<i128> {
    10_i128.checked_pow(u32::from(precision))?.checked_sub(1)
}

fn mssql_date_from_arrow_date32(
    mapping: &SchemaMapping,
    row_index: usize,
    days_from_unix_epoch: i32,
) -> Result<MssqlDate> {
    let days = i64::from(days_from_unix_epoch) + SQL_SERVER_DATE_UNIX_EPOCH_DAYS;

    if (0..=SQL_SERVER_DATE_MAX_DAYS).contains(&days) {
        return Ok(MssqlDate::new(days as u32));
    }

    Err(value_conversion_error(row_mapping_diagnostic(
        mapping,
        row_index,
        DiagnosticCode::TimestampOutOfRange,
        format!("Arrow Date32 day offset {days_from_unix_epoch} is outside SQL Server date range"),
    )))
}

fn mssql_datetime2_from_arrow_date64(
    mapping: &SchemaMapping,
    row_index: usize,
    milliseconds_from_unix_epoch: i64,
) -> Result<MssqlDateTime2> {
    let days_from_unix_epoch = milliseconds_from_unix_epoch.div_euclid(MILLISECONDS_PER_DAY);
    let milliseconds_since_midnight = milliseconds_from_unix_epoch.rem_euclid(MILLISECONDS_PER_DAY);
    let days = days_from_unix_epoch + SQL_SERVER_DATE_UNIX_EPOCH_DAYS;

    if !(0..=SQL_SERVER_DATE_MAX_DAYS).contains(&days) {
        return Err(value_conversion_error(row_mapping_diagnostic(
            mapping,
            row_index,
            DiagnosticCode::TimestampOutOfRange,
            format!(
                "Arrow Date64 millisecond value {milliseconds_from_unix_epoch} is outside SQL Server datetime2 range"
            ),
        )));
    }

    Ok(MssqlDateTime2::new(
        MssqlDate::new(days as u32),
        MssqlTime::new(
            milliseconds_since_midnight as u64,
            SQL_SERVER_DATETIME2_DATE64_SCALE,
        ),
    ))
}

fn nanoseconds_to_100ns_ticks(
    mapping: &SchemaMapping,
    row_index: usize,
    nanoseconds_from_unix_epoch: i64,
    policy: NanosecondPolicy,
) -> Result<i128> {
    let base_ticks = nanoseconds_from_unix_epoch.div_euclid(NANOSECONDS_PER_100NS_TICK);
    let remainder = nanoseconds_from_unix_epoch.rem_euclid(NANOSECONDS_PER_100NS_TICK);

    match policy {
        NanosecondPolicy::RejectNon100ns if remainder == 0 => Ok(i128::from(base_ticks)),
        NanosecondPolicy::RejectNon100ns => Err(value_conversion_error(row_mapping_diagnostic(
            mapping,
            row_index,
            DiagnosticCode::LossyConversionRequiresPolicy,
            format!(
                "Arrow timestamp nanosecond value {nanoseconds_from_unix_epoch} is not divisible by 100ns"
            ),
        ))),
        NanosecondPolicy::TruncateTo100ns => Ok(i128::from(base_ticks)),
        NanosecondPolicy::RoundTo100ns => {
            let rounded_ticks = if remainder >= 50 {
                base_ticks.checked_add(1).ok_or_else(|| {
                    value_conversion_error(row_mapping_diagnostic(
                        mapping,
                        row_index,
                        DiagnosticCode::TimestampOutOfRange,
                        format!(
                            "Arrow timestamp nanosecond value {nanoseconds_from_unix_epoch} overflows while rounding to 100ns"
                        ),
                    ))
                })?
            } else {
                base_ticks
            };
            Ok(i128::from(rounded_ticks))
        }
    }
}

fn validate_timestamp_timezone_metadata(
    mapping: &SchemaMapping,
    row_index: usize,
    timezone: Option<&str>,
) -> Result<()> {
    let Some(timezone) = timezone.filter(|timezone| !timezone.is_empty()) else {
        return Ok(());
    };

    timezone_resolution_from_metadata(mapping, row_index, timezone).map(|_| ())
}

/// Resolved timezone metadata for a planned Arrow timestamp column.
///
/// Arrow timestamp timezone metadata can contain either a fixed offset or a
/// timezone database name. Fixed offsets are row-independent, while named
/// timezones need the row timestamp instant to account for historical and DST
/// offset rules.
#[derive(Debug, Clone, Copy)]
enum TimezoneResolution {
    FixedOffset { offset_minutes: i16 },
    Named { timezone: Tz },
}

impl TimezoneResolution {
    /// Returns the SQL Server offset for one timestamp instant.
    fn offset_for_instant(
        self,
        mapping: &SchemaMapping,
        row_index: usize,
        seconds_from_unix_epoch: i64,
        nanoseconds: u32,
    ) -> Result<i16> {
        match self {
            Self::FixedOffset { offset_minutes } => Ok(offset_minutes),
            Self::Named { timezone } => {
                let datetime = timezone
                    .timestamp_opt(seconds_from_unix_epoch, nanoseconds)
                    .single()
                    .ok_or_else(|| {
                        timezone_instant_error(mapping, row_index, seconds_from_unix_epoch)
                    })?;
                let offset_seconds = datetime.offset().fix().local_minus_utc();
                sql_server_offset_minutes(mapping, row_index, offset_seconds)
            }
        }
    }
}

/// Resolves Arrow timestamp timezone metadata once for a planned column.
fn timezone_resolution_from_metadata(
    mapping: &SchemaMapping,
    row_index: usize,
    timezone: &str,
) -> Result<TimezoneResolution> {
    if timezone.eq_ignore_ascii_case("Z") || timezone.eq_ignore_ascii_case("UTC") {
        return Ok(TimezoneResolution::FixedOffset { offset_minutes: 0 });
    }

    if let Some(offset) = parse_sql_server_fixed_timezone_offset(mapping, row_index, timezone) {
        return offset.map(|offset_minutes| TimezoneResolution::FixedOffset { offset_minutes });
    }

    let timezone = timezone
        .parse::<Tz>()
        .map_err(|_| unsupported_timezone_error(mapping, row_index, timezone))?;

    Ok(TimezoneResolution::Named { timezone })
}

fn sql_server_offset_minutes(
    mapping: &SchemaMapping,
    row_index: usize,
    offset_seconds: i32,
) -> Result<i16> {
    if offset_seconds % 60 != 0 {
        return Err(unsupported_timezone_offset_error(
            mapping,
            row_index,
            offset_seconds,
        ));
    }

    let offset_minutes = i16::try_from(offset_seconds / 60)
        .map_err(|_| unsupported_timezone_offset_error(mapping, row_index, offset_seconds))?;

    if offset_minutes.unsigned_abs() > SQL_SERVER_DATETIMEOFFSET_MAX_OFFSET_MINUTES as u16 {
        return Err(unsupported_timezone_offset_error(
            mapping,
            row_index,
            offset_seconds,
        ));
    }

    Ok(offset_minutes)
}

fn parse_sql_server_fixed_timezone_offset(
    mapping: &SchemaMapping,
    row_index: usize,
    timezone: &str,
) -> Option<Result<i16>> {
    let timezone_bytes = timezone.as_bytes();
    if !matches!(timezone_bytes.first(), Some(b'+' | b'-')) {
        return None;
    }

    // Arrow accepts some offset spellings that SQL Server would not accept as
    // written, such as `+12:60`. Validate fixed offsets ourselves before
    // falling back to the Arrow timezone database parser for named zones.
    let digits = match timezone_bytes.len() {
        3 => [timezone_bytes[1], timezone_bytes[2], b'0', b'0'],
        5 => [
            timezone_bytes[1],
            timezone_bytes[2],
            timezone_bytes[3],
            timezone_bytes[4],
        ],
        6 if timezone_bytes[3] == b':' => [
            timezone_bytes[1],
            timezone_bytes[2],
            timezone_bytes[4],
            timezone_bytes[5],
        ],
        _ => {
            return Some(Err(unsupported_timezone_error(
                mapping, row_index, timezone,
            )));
        }
    };

    if digits.iter().any(|digit| !digit.is_ascii_digit()) {
        return Some(Err(unsupported_timezone_error(
            mapping, row_index, timezone,
        )));
    }

    let hours = i16::from((digits[0] - b'0') * 10 + (digits[1] - b'0'));
    let minutes = i16::from((digits[2] - b'0') * 10 + (digits[3] - b'0'));

    if minutes >= 60 {
        return Some(Err(unsupported_timezone_error(
            mapping, row_index, timezone,
        )));
    }

    let Some(total_minutes) = hours
        .checked_mul(60)
        .and_then(|value| value.checked_add(minutes))
    else {
        return Some(Err(unsupported_timezone_error(
            mapping, row_index, timezone,
        )));
    };

    if total_minutes > SQL_SERVER_DATETIMEOFFSET_MAX_OFFSET_MINUTES {
        return Some(Err(unsupported_timezone_error(
            mapping, row_index, timezone,
        )));
    }

    if timezone_bytes[0] == b'-' {
        Some(Ok(-total_minutes))
    } else {
        Some(Ok(total_minutes))
    }
}

fn mssql_datetime2_from_unix_epoch_100ns_ticks(
    mapping: &SchemaMapping,
    row_index: usize,
    ticks_from_unix_epoch: i128,
    unit_name: &str,
    source_value: i64,
) -> Result<MssqlDateTime2> {
    let days_from_unix_epoch = ticks_from_unix_epoch.div_euclid(TICKS_100NS_PER_DAY);
    let ticks_since_midnight = ticks_from_unix_epoch.rem_euclid(TICKS_100NS_PER_DAY);
    let days = days_from_unix_epoch + i128::from(SQL_SERVER_DATE_UNIX_EPOCH_DAYS);

    if !(0..=i128::from(SQL_SERVER_DATE_MAX_DAYS)).contains(&days) {
        return Err(value_conversion_error(row_mapping_diagnostic(
            mapping,
            row_index,
            DiagnosticCode::TimestampOutOfRange,
            format!(
                "Arrow timestamp {unit_name} value {source_value} is outside SQL Server datetime2 range"
            ),
        )));
    }

    let days = u32::try_from(days).map_err(|_| {
        value_conversion_error(row_mapping_diagnostic(
            mapping,
            row_index,
            DiagnosticCode::TimestampOutOfRange,
            format!(
                "Arrow timestamp {unit_name} value {source_value} has an invalid SQL Server date component"
            ),
        ))
    })?;
    let ticks_since_midnight = u64::try_from(ticks_since_midnight).map_err(|_| {
        value_conversion_error(row_mapping_diagnostic(
            mapping,
            row_index,
            DiagnosticCode::TimestampOutOfRange,
            format!(
                "Arrow timestamp {unit_name} value {source_value} has an invalid SQL Server time component"
            ),
        ))
    })?;

    Ok(MssqlDateTime2::new(
        MssqlDate::new(days),
        MssqlTime::new(ticks_since_midnight, SQL_SERVER_DATETIME2_TIMESTAMP_SCALE),
    ))
}

fn mssql_datetime2_from_arrow_timestamp_second(
    mapping: &SchemaMapping,
    row_index: usize,
    seconds_from_unix_epoch: i64,
) -> Result<MssqlDateTime2> {
    let ticks = i128::from(seconds_from_unix_epoch) * TICKS_100NS_PER_SECOND;
    mssql_datetime2_from_unix_epoch_100ns_ticks(
        mapping,
        row_index,
        ticks,
        "second",
        seconds_from_unix_epoch,
    )
}

fn mssql_datetime2_from_arrow_timestamp_millisecond(
    mapping: &SchemaMapping,
    row_index: usize,
    milliseconds_from_unix_epoch: i64,
) -> Result<MssqlDateTime2> {
    let ticks = i128::from(milliseconds_from_unix_epoch) * TICKS_100NS_PER_MILLISECOND;
    mssql_datetime2_from_unix_epoch_100ns_ticks(
        mapping,
        row_index,
        ticks,
        "millisecond",
        milliseconds_from_unix_epoch,
    )
}

fn mssql_datetime2_from_arrow_timestamp_microsecond(
    mapping: &SchemaMapping,
    row_index: usize,
    microseconds_from_unix_epoch: i64,
) -> Result<MssqlDateTime2> {
    let ticks = i128::from(microseconds_from_unix_epoch) * TICKS_100NS_PER_MICROSECOND;
    mssql_datetime2_from_unix_epoch_100ns_ticks(
        mapping,
        row_index,
        ticks,
        "microsecond",
        microseconds_from_unix_epoch,
    )
}

fn mssql_datetime2_from_arrow_timestamp_nanosecond(
    mapping: &SchemaMapping,
    row_index: usize,
    nanoseconds_from_unix_epoch: i64,
    policy: NanosecondPolicy,
) -> Result<MssqlDateTime2> {
    let ticks =
        nanoseconds_to_100ns_ticks(mapping, row_index, nanoseconds_from_unix_epoch, policy)?;
    mssql_datetime2_from_unix_epoch_100ns_ticks(
        mapping,
        row_index,
        ticks,
        "nanosecond",
        nanoseconds_from_unix_epoch,
    )
}

fn mssql_datetimeoffset_from_utc_100ns_ticks(
    mapping: &SchemaMapping,
    row_index: usize,
    utc_ticks_from_unix_epoch: i128,
    offset_minutes: i16,
    unit_name: &str,
    source_value: i64,
) -> Result<MssqlDateTimeOffset> {
    let offset_ticks = i128::from(offset_minutes) * 60 * TICKS_100NS_PER_SECOND;
    let local_ticks = utc_ticks_from_unix_epoch
        .checked_add(offset_ticks)
        .ok_or_else(|| {
            value_conversion_error(row_mapping_diagnostic(
                mapping,
                row_index,
                DiagnosticCode::TimestampOutOfRange,
                format!(
                    "Arrow timestamp {unit_name} value {source_value} overflows while applying timezone offset {offset_minutes} minute(s)"
                ),
            ))
        })?;
    let datetime2 = mssql_datetime2_from_unix_epoch_100ns_ticks(
        mapping,
        row_index,
        local_ticks,
        unit_name,
        source_value,
    )?;

    Ok(MssqlDateTimeOffset::new(datetime2, offset_minutes))
}

fn epoch_parts_from_milliseconds(
    mapping: &SchemaMapping,
    row_index: usize,
    milliseconds_from_unix_epoch: i64,
) -> Result<(i64, u32)> {
    let seconds = milliseconds_from_unix_epoch.div_euclid(1_000);
    let nanoseconds = milliseconds_from_unix_epoch.rem_euclid(1_000) * 1_000_000;
    epoch_parts(mapping, row_index, seconds, nanoseconds)
}

fn epoch_parts_from_microseconds(
    mapping: &SchemaMapping,
    row_index: usize,
    microseconds_from_unix_epoch: i64,
) -> Result<(i64, u32)> {
    let seconds = microseconds_from_unix_epoch.div_euclid(1_000_000);
    let nanoseconds = microseconds_from_unix_epoch.rem_euclid(1_000_000) * 1_000;
    epoch_parts(mapping, row_index, seconds, nanoseconds)
}

fn epoch_parts_from_nanoseconds(
    mapping: &SchemaMapping,
    row_index: usize,
    nanoseconds_from_unix_epoch: i64,
) -> Result<(i64, u32)> {
    let seconds = nanoseconds_from_unix_epoch.div_euclid(1_000_000_000);
    let nanoseconds = nanoseconds_from_unix_epoch.rem_euclid(1_000_000_000);
    epoch_parts(mapping, row_index, seconds, nanoseconds)
}

fn epoch_parts(
    mapping: &SchemaMapping,
    row_index: usize,
    seconds_from_unix_epoch: i64,
    nanoseconds: i64,
) -> Result<(i64, u32)> {
    let nanoseconds = u32::try_from(nanoseconds).map_err(|_| {
        value_conversion_error(row_mapping_diagnostic(
            mapping,
            row_index,
            DiagnosticCode::TimestampOutOfRange,
            format!("timestamp nanosecond component {nanoseconds} is outside valid range"),
        ))
    })?;

    Ok((seconds_from_unix_epoch, nanoseconds))
}

fn nvar_char_cell<'a>(
    mapping: &SchemaMapping,
    row_index: usize,
    length: MssqlTypeLength,
    cell: ArrowCell<'a>,
) -> Result<MssqlCell<'a>> {
    let value = mssql_nvarchar_value(mapping, row_index, cell)?;
    let code_units = value.encode_utf16().count();

    if exceeds_length(length, code_units) {
        return Err(value_too_long_error(
            mapping,
            row_index,
            format!(
                "string value has {code_units} UTF-16 code unit(s), exceeding planned {}",
                mapping.mssql().ty().to_sql()
            ),
        ));
    }

    Ok(MssqlCell::NVarChar(Some(value)))
}

fn var_binary_cell<'a>(
    mapping: &SchemaMapping,
    row_index: usize,
    length: MssqlTypeLength,
    cell: ArrowCell<'a>,
) -> Result<MssqlCell<'a>> {
    let value = mssql_varbinary_value(mapping, row_index, cell)?;
    let bytes = value.len();

    if exceeds_length(length, bytes) {
        return Err(value_too_long_error(
            mapping,
            row_index,
            format!(
                "binary value has {bytes} byte(s), exceeding planned {}",
                mapping.mssql().ty().to_sql()
            ),
        ));
    }

    Ok(MssqlCell::VarBinary(Some(value)))
}

fn exceeds_length(length: MssqlTypeLength, actual: usize) -> bool {
    match length {
        MssqlTypeLength::Bounded(limit) => actual > limit,
        MssqlTypeLength::Max => false,
    }
}

fn downcast_array<'a, T: Array + 'static>(
    array: &'a dyn Array,
    mapping: &SchemaMapping,
    row_index: usize,
) -> Result<&'a T> {
    array.as_any().downcast_ref::<T>().ok_or_else(|| {
        value_conversion_error(row_mapping_diagnostic(
            mapping,
            row_index,
            DiagnosticCode::ValueTypeMismatch,
            format!(
                "runtime Arrow type {} does not match planned Arrow type {}",
                array.data_type(),
                mapping.arrow().data_type()
            ),
        ))
    })
}

fn unsupported_timezone_error(
    mapping: &SchemaMapping,
    row_index: usize,
    timezone: &str,
) -> crate::Error {
    value_conversion_error(row_mapping_diagnostic(
        mapping,
        row_index,
        DiagnosticCode::TimezoneUnsupported,
        format!(
            "Arrow timestamp timezone {timezone:?} is not a valid Arrow timezone name or fixed offset"
        ),
    ))
}

fn timezone_instant_error(
    mapping: &SchemaMapping,
    row_index: usize,
    seconds_from_unix_epoch: i64,
) -> crate::Error {
    value_conversion_error(row_mapping_diagnostic(
        mapping,
        row_index,
        DiagnosticCode::TimestampOutOfRange,
        format!(
            "Arrow timestamp second value {seconds_from_unix_epoch} cannot be represented in the planned timezone"
        ),
    ))
}

fn unsupported_timezone_offset_error(
    mapping: &SchemaMapping,
    row_index: usize,
    offset_seconds: i32,
) -> crate::Error {
    value_conversion_error(row_mapping_diagnostic(
        mapping,
        row_index,
        DiagnosticCode::TimezoneUnsupported,
        format!(
            "resolved timezone offset {offset_seconds} second(s) cannot be represented as a SQL Server datetimeoffset minute offset"
        ),
    ))
}

fn unsupported_value_conversion(
    mapping: &SchemaMapping,
    row_index: usize,
    message: impl Into<String>,
) -> crate::Error {
    value_conversion_error(row_mapping_diagnostic(
        mapping,
        row_index,
        DiagnosticCode::ValueConversionUnsupported,
        message,
    ))
}

fn non_finite_float_error(
    mapping: &SchemaMapping,
    row_index: usize,
    value: impl std::fmt::Display,
) -> crate::Error {
    value_conversion_error(row_mapping_diagnostic(
        mapping,
        row_index,
        DiagnosticCode::NonFiniteFloat,
        format!("non-finite floating point value {value} is not supported"),
    ))
}

fn value_too_long_error(
    mapping: &SchemaMapping,
    row_index: usize,
    message: impl Into<String>,
) -> crate::Error {
    value_conversion_error(row_mapping_diagnostic(
        mapping,
        row_index,
        DiagnosticCode::ValueTooLong,
        message,
    ))
}

fn validate_runtime_columns(batch: &RecordBatch, mappings: &[SchemaMapping]) -> Result<()> {
    if batch.num_columns() < mappings.len() {
        let mapping = &mappings[batch.num_columns()];
        return Err(value_conversion_error(mapping_diagnostic(
            mapping,
            DiagnosticCode::SchemaMismatch,
            format!(
                "planned column index {} is outside runtime batch with {} column(s)",
                mapping.arrow().index(),
                batch.num_columns()
            ),
        )));
    }

    if batch.num_columns() > mappings.len() {
        return Err(value_conversion_error(Diagnostic::error(
            DiagnosticCode::SchemaMismatch,
            format!(
                "runtime batch has {} column(s) but mappings contain {} column(s)",
                batch.num_columns(),
                mappings.len()
            ),
        )));
    }

    for (position, (field, (array, mapping))) in batch
        .schema()
        .fields()
        .iter()
        .zip(batch.columns().iter().zip(mappings))
        .enumerate()
    {
        if mapping.arrow().index() != position {
            return Err(value_conversion_error(mapping_diagnostic(
                mapping,
                DiagnosticCode::SchemaMismatch,
                format!(
                    "mapping position {position} does not match planned Arrow field index {}",
                    mapping.arrow().index()
                ),
            )));
        }

        if field.name() != mapping.arrow().name() {
            return Err(value_conversion_error(mapping_diagnostic(
                mapping,
                DiagnosticCode::SchemaMismatch,
                format!(
                    "runtime Arrow field name {} does not match planned Arrow field name {}",
                    field.name(),
                    mapping.arrow().name()
                ),
            )));
        }

        validate_runtime_column(array.as_ref(), mapping)?;
    }

    Ok(())
}

fn validate_runtime_column(array: &dyn Array, mapping: &SchemaMapping) -> Result<()> {
    if array.data_type() != mapping.arrow().data_type() {
        return Err(value_conversion_error(mapping_diagnostic(
            mapping,
            DiagnosticCode::SchemaMismatch,
            format!(
                "runtime Arrow type {} does not match planned Arrow type {}",
                array.data_type(),
                mapping.arrow().data_type()
            ),
        )));
    }

    Ok(())
}

fn mapping_diagnostic(
    mapping: &SchemaMapping,
    code: DiagnosticCode,
    message: impl Into<String>,
) -> Diagnostic {
    Diagnostic::error(code, message).with_field(FieldRef::new(
        mapping.arrow().index(),
        mapping.arrow().name(),
    ))
}

fn row_mapping_diagnostic(
    mapping: &SchemaMapping,
    row_index: usize,
    code: DiagnosticCode,
    message: impl Into<String>,
) -> Diagnostic {
    mapping_diagnostic(mapping, code, message).with_row(row_index)
}

fn value_conversion_error(diagnostic: Diagnostic) -> crate::Error {
    crate::Error::ValueConversion {
        diagnostics: DiagnosticSet::from(vec![diagnostic]),
    }
}

#[cfg(test)]
mod tests {
    use std::borrow::Cow;
    use std::sync::Arc;

    use arrow_array::{
        ArrayRef, BinaryArray, BooleanArray, Date32Array, Date64Array, Decimal32Array,
        Decimal64Array, Decimal128Array, Decimal256Array, Float32Array, Float64Array, Int8Array,
        Int16Array, Int32Array, Int64Array, LargeBinaryArray, LargeStringArray, RecordBatch,
        StringArray, TimestampMicrosecondArray, TimestampMillisecondArray,
        TimestampNanosecondArray, TimestampSecondArray, UInt8Array, UInt16Array, UInt32Array,
        UInt64Array, new_null_array,
    };
    use arrow_buffer::i256;
    use arrow_data::ArrayData;
    use arrow_schema::{DataType, Field, Schema, TimeUnit};

    use super::{
        ArrowCell, ArrowToMssqlRuntimeMapping, MssqlCell, MssqlDate, MssqlDateTime2,
        MssqlDateTimeOffset, MssqlDecimal, MssqlTime, RecordBatchView,
        mssql_cell_to_tiberius_borrowed, mssql_cell_to_tiberius_owned,
        timezone_resolution_from_metadata,
    };
    use crate::{
        ArrowFieldRef, BinaryPolicy, Date64Policy, DecimalPolicy, DiagnosticCode, Error,
        Identifier, MssqlColumn, MssqlProfile, MssqlType, NanosecondPolicy, PlanOptions,
        SchemaMapping, StringPolicy, TimezonePolicy, UInt64Policy,
        plan_arrow_schema_to_mssql_mappings,
    };

    #[test]
    fn accepts_matching_batch_and_mappings() {
        let mappings = mappings_for_schema(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("active", DataType::Boolean, true),
        ]));
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("id", DataType::Int32, false),
                Field::new("active", DataType::Boolean, true),
            ])),
            vec![
                Arc::new(Int32Array::from(vec![1_i32, 2])) as ArrayRef,
                Arc::new(BooleanArray::from(vec![Some(true), None])),
            ],
        )
        .unwrap();

        let view = RecordBatchView::new(&batch, &mappings).unwrap();

        assert_eq!(view.row_count(), 2);
        assert_eq!(view.mappings().len(), 2);
        view.check_row_index(1).unwrap();
    }

    #[test]
    fn runtime_mapping_keeps_write_policy_out_of_schema_mapping() {
        let options = PlanOptions {
            nanosecond_policy: NanosecondPolicy::TruncateTo100ns,
            ..PlanOptions::default()
        };
        let mappings = mappings_for_schema_with_options(
            Schema::new(vec![Field::new(
                "created_at",
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                true,
            )]),
            options,
        );

        let runtime_mapping = ArrowToMssqlRuntimeMapping::new(&mappings[0], &options);

        assert_eq!(runtime_mapping.mapping(), &mappings[0]);
        assert_eq!(
            runtime_mapping.nanosecond_policy(),
            NanosecondPolicy::TruncateTo100ns
        );
        assert_eq!(
            mappings[0].mssql().ty(),
            &MssqlType::DateTime2 { precision: 7 }
        );
    }

    #[test]
    fn extracts_arrow_cells_for_supported_initial_primitives() {
        let mappings = mappings_for_schema(Schema::new(vec![
            Field::new("active", DataType::Boolean, true),
            Field::new("tiny", DataType::Int8, true),
            Field::new("small", DataType::Int16, true),
            Field::new("quantity", DataType::Int32, true),
            Field::new("total", DataType::Int64, true),
            Field::new("unsigned_tiny", DataType::UInt8, true),
            Field::new("unsigned_medium", DataType::UInt16, true),
            Field::new("unsigned_large", DataType::UInt32, true),
            Field::new("real_value", DataType::Float32, true),
            Field::new("float_value", DataType::Float64, true),
            Field::new("text", DataType::Utf8, true),
            Field::new("large_text", DataType::LargeUtf8, true),
            Field::new("bytes", DataType::Binary, true),
            Field::new("large_bytes", DataType::LargeBinary, true),
        ]));
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("active", DataType::Boolean, true),
                Field::new("tiny", DataType::Int8, true),
                Field::new("small", DataType::Int16, true),
                Field::new("quantity", DataType::Int32, true),
                Field::new("total", DataType::Int64, true),
                Field::new("unsigned_tiny", DataType::UInt8, true),
                Field::new("unsigned_medium", DataType::UInt16, true),
                Field::new("unsigned_large", DataType::UInt32, true),
                Field::new("real_value", DataType::Float32, true),
                Field::new("float_value", DataType::Float64, true),
                Field::new("text", DataType::Utf8, true),
                Field::new("large_text", DataType::LargeUtf8, true),
                Field::new("bytes", DataType::Binary, true),
                Field::new("large_bytes", DataType::LargeBinary, true),
            ])),
            vec![
                Arc::new(BooleanArray::from(vec![Some(true), None])) as ArrayRef,
                Arc::new(Int8Array::from(vec![Some(-8_i8), None])),
                Arc::new(Int16Array::from(vec![Some(-16_i16), None])),
                Arc::new(Int32Array::from(vec![Some(12_i32), None])),
                Arc::new(Int64Array::from(vec![Some(34_i64), None])),
                Arc::new(UInt8Array::from(vec![Some(8_u8), None])),
                Arc::new(UInt16Array::from(vec![Some(16_u16), None])),
                Arc::new(UInt32Array::from(vec![Some(32_u32), None])),
                Arc::new(Float32Array::from(vec![Some(1.25_f32), None])),
                Arc::new(Float64Array::from(vec![Some(2.5_f64), None])),
                Arc::new(StringArray::from(vec![Some("hello"), None])),
                Arc::new(LargeStringArray::from(vec![Some("東京"), None])),
                Arc::new(BinaryArray::from(vec![Some(&b"abc"[..]), None])),
                Arc::new(LargeBinaryArray::from(vec![Some(&b"large"[..]), None])),
            ],
        )
        .unwrap();
        let view = RecordBatchView::new(&batch, &mappings).unwrap();

        assert_eq!(
            view.arrow_cell(&mappings[0], 0).unwrap(),
            ArrowCell::Boolean(true)
        );
        assert_eq!(view.arrow_cell(&mappings[0], 1).unwrap(), ArrowCell::Null);
        assert_eq!(
            view.arrow_cell(&mappings[1], 0).unwrap(),
            ArrowCell::Int8(-8)
        );
        assert_eq!(view.arrow_cell(&mappings[1], 1).unwrap(), ArrowCell::Null);
        assert_eq!(
            view.arrow_cell(&mappings[2], 0).unwrap(),
            ArrowCell::Int16(-16)
        );
        assert_eq!(view.arrow_cell(&mappings[2], 1).unwrap(), ArrowCell::Null);
        assert_eq!(
            view.arrow_cell(&mappings[3], 0).unwrap(),
            ArrowCell::Int32(12)
        );
        assert_eq!(view.arrow_cell(&mappings[3], 1).unwrap(), ArrowCell::Null);
        assert_eq!(
            view.arrow_cell(&mappings[4], 0).unwrap(),
            ArrowCell::Int64(34)
        );
        assert_eq!(view.arrow_cell(&mappings[4], 1).unwrap(), ArrowCell::Null);
        assert_eq!(
            view.arrow_cell(&mappings[5], 0).unwrap(),
            ArrowCell::UInt8(8)
        );
        assert_eq!(view.arrow_cell(&mappings[5], 1).unwrap(), ArrowCell::Null);
        assert_eq!(
            view.arrow_cell(&mappings[6], 0).unwrap(),
            ArrowCell::UInt16(16)
        );
        assert_eq!(view.arrow_cell(&mappings[6], 1).unwrap(), ArrowCell::Null);
        assert_eq!(
            view.arrow_cell(&mappings[7], 0).unwrap(),
            ArrowCell::UInt32(32)
        );
        assert_eq!(view.arrow_cell(&mappings[7], 1).unwrap(), ArrowCell::Null);
        assert_eq!(
            view.arrow_cell(&mappings[8], 0).unwrap(),
            ArrowCell::Float32(1.25)
        );
        assert_eq!(view.arrow_cell(&mappings[8], 1).unwrap(), ArrowCell::Null);
        assert_eq!(
            view.arrow_cell(&mappings[9], 0).unwrap(),
            ArrowCell::Float64(2.5)
        );
        assert_eq!(view.arrow_cell(&mappings[9], 1).unwrap(), ArrowCell::Null);
        assert_eq!(
            view.arrow_cell(&mappings[10], 0).unwrap(),
            ArrowCell::Utf8("hello")
        );
        assert_eq!(view.arrow_cell(&mappings[10], 1).unwrap(), ArrowCell::Null);
        assert_eq!(
            view.arrow_cell(&mappings[11], 0).unwrap(),
            ArrowCell::Utf8("東京")
        );
        assert_eq!(view.arrow_cell(&mappings[11], 1).unwrap(), ArrowCell::Null);
        assert_eq!(
            view.arrow_cell(&mappings[12], 0).unwrap(),
            ArrowCell::Binary(b"abc")
        );
        assert_eq!(view.arrow_cell(&mappings[12], 1).unwrap(), ArrowCell::Null);
        assert_eq!(
            view.arrow_cell(&mappings[13], 0).unwrap(),
            ArrowCell::Binary(b"large")
        );
        assert_eq!(view.arrow_cell(&mappings[13], 1).unwrap(), ArrowCell::Null);
    }

    #[test]
    fn extracts_uint64_arrow_cells_at_policy_boundaries() {
        let mappings = mappings_for_schema_with_options(
            Schema::new(vec![Field::new("unsigned_huge", DataType::UInt64, true)]),
            PlanOptions {
                uint64_policy: UInt64Policy::Decimal20_0,
                ..PlanOptions::default()
            },
        );
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "unsigned_huge",
                DataType::UInt64,
                true,
            )])),
            vec![Arc::new(UInt64Array::from(vec![
                Some(0_u64),
                Some(i64::MAX as u64),
                Some((i64::MAX as u64) + 1),
                Some(u64::MAX),
                None,
            ]))],
        )
        .unwrap();
        let view = RecordBatchView::new(&batch, &mappings).unwrap();

        assert_eq!(
            view.arrow_cell(&mappings[0], 0).unwrap(),
            ArrowCell::UInt64(0)
        );
        assert_eq!(
            view.arrow_cell(&mappings[0], 1).unwrap(),
            ArrowCell::UInt64(i64::MAX as u64)
        );
        assert_eq!(
            view.arrow_cell(&mappings[0], 2).unwrap(),
            ArrowCell::UInt64((i64::MAX as u64) + 1)
        );
        assert_eq!(
            view.arrow_cell(&mappings[0], 3).unwrap(),
            ArrowCell::UInt64(u64::MAX)
        );
        assert_eq!(view.arrow_cell(&mappings[0], 4).unwrap(), ArrowCell::Null);
    }

    #[test]
    fn extracts_timezone_free_timestamp_arrow_cells_at_i64_boundaries() {
        let mappings = mappings_for_schema(Schema::new(vec![
            Field::new("ts_s", DataType::Timestamp(TimeUnit::Second, None), true),
            Field::new(
                "ts_ms",
                DataType::Timestamp(TimeUnit::Millisecond, None),
                true,
            ),
            Field::new(
                "ts_us",
                DataType::Timestamp(TimeUnit::Microsecond, None),
                true,
            ),
            Field::new(
                "ts_ns",
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                true,
            ),
        ]));
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("ts_s", DataType::Timestamp(TimeUnit::Second, None), true),
                Field::new(
                    "ts_ms",
                    DataType::Timestamp(TimeUnit::Millisecond, None),
                    true,
                ),
                Field::new(
                    "ts_us",
                    DataType::Timestamp(TimeUnit::Microsecond, None),
                    true,
                ),
                Field::new(
                    "ts_ns",
                    DataType::Timestamp(TimeUnit::Nanosecond, None),
                    true,
                ),
            ])),
            vec![
                Arc::new(TimestampSecondArray::from(vec![
                    Some(i64::MIN),
                    Some(0),
                    Some(i64::MAX),
                    None,
                ])) as ArrayRef,
                Arc::new(TimestampMillisecondArray::from(vec![
                    Some(i64::MIN),
                    Some(0),
                    Some(i64::MAX),
                    None,
                ])),
                Arc::new(TimestampMicrosecondArray::from(vec![
                    Some(i64::MIN),
                    Some(0),
                    Some(i64::MAX),
                    None,
                ])),
                Arc::new(TimestampNanosecondArray::from(vec![
                    Some(i64::MIN),
                    Some(0),
                    Some(i64::MAX),
                    None,
                ])),
            ],
        )
        .unwrap();
        let view = RecordBatchView::new(&batch, &mappings).unwrap();

        assert_eq!(
            view.arrow_cell(&mappings[0], 0).unwrap(),
            ArrowCell::TimestampSecond(i64::MIN)
        );
        assert_eq!(
            view.arrow_cell(&mappings[0], 2).unwrap(),
            ArrowCell::TimestampSecond(i64::MAX)
        );
        assert_eq!(view.arrow_cell(&mappings[0], 3).unwrap(), ArrowCell::Null);
        assert_eq!(
            view.arrow_cell(&mappings[1], 0).unwrap(),
            ArrowCell::TimestampMillisecond(i64::MIN)
        );
        assert_eq!(
            view.arrow_cell(&mappings[1], 2).unwrap(),
            ArrowCell::TimestampMillisecond(i64::MAX)
        );
        assert_eq!(view.arrow_cell(&mappings[1], 3).unwrap(), ArrowCell::Null);
        assert_eq!(
            view.arrow_cell(&mappings[2], 0).unwrap(),
            ArrowCell::TimestampMicrosecond(i64::MIN)
        );
        assert_eq!(
            view.arrow_cell(&mappings[2], 2).unwrap(),
            ArrowCell::TimestampMicrosecond(i64::MAX)
        );
        assert_eq!(view.arrow_cell(&mappings[2], 3).unwrap(), ArrowCell::Null);
        assert_eq!(
            view.arrow_cell(&mappings[3], 0).unwrap(),
            ArrowCell::TimestampNanosecond(i64::MIN)
        );
        assert_eq!(
            view.arrow_cell(&mappings[3], 2).unwrap(),
            ArrowCell::TimestampNanosecond(i64::MAX)
        );
        assert_eq!(view.arrow_cell(&mappings[3], 3).unwrap(), ArrowCell::Null);
    }

    #[test]
    fn extracts_timezone_aware_timestamp_arrow_cells_without_losing_epoch_values() {
        let options = PlanOptions {
            timezone_policy: TimezonePolicy::DateTimeOffset,
            ..PlanOptions::default()
        };
        let schema = Schema::new(vec![
            Field::new(
                "ts_s",
                DataType::Timestamp(TimeUnit::Second, Some("America/New_York".into())),
                true,
            ),
            Field::new(
                "ts_ms",
                DataType::Timestamp(TimeUnit::Millisecond, Some("+02:30".into())),
                true,
            ),
            Field::new(
                "ts_us",
                DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
                true,
            ),
            Field::new(
                "ts_ns",
                DataType::Timestamp(TimeUnit::Nanosecond, Some("-07".into())),
                true,
            ),
        ]);
        let mappings = mappings_for_schema_with_options(schema.clone(), options);
        let batch = RecordBatch::try_new(
            Arc::new(schema),
            vec![
                Arc::new(
                    TimestampSecondArray::from(vec![Some(1_i64), None])
                        .with_timezone("America/New_York"),
                ) as ArrayRef,
                Arc::new(
                    TimestampMillisecondArray::from(vec![Some(2_i64), None])
                        .with_timezone("+02:30"),
                ),
                Arc::new(
                    TimestampMicrosecondArray::from(vec![Some(3_i64), None]).with_timezone("UTC"),
                ),
                Arc::new(
                    TimestampNanosecondArray::from(vec![Some(4_i64), None]).with_timezone("-07"),
                ),
            ],
        )
        .unwrap();
        let view = RecordBatchView::new(&batch, &mappings).unwrap();

        assert_eq!(
            view.arrow_cell(&mappings[0], 0).unwrap(),
            ArrowCell::TimestampSecond(1)
        );
        assert_eq!(
            view.arrow_cell(&mappings[1], 0).unwrap(),
            ArrowCell::TimestampMillisecond(2)
        );
        assert_eq!(
            view.arrow_cell(&mappings[2], 0).unwrap(),
            ArrowCell::TimestampMicrosecond(3)
        );
        assert_eq!(
            view.arrow_cell(&mappings[3], 0).unwrap(),
            ArrowCell::TimestampNanosecond(4)
        );

        for mapping in &mappings {
            assert_eq!(view.arrow_cell(mapping, 1).unwrap(), ArrowCell::Null);
        }
    }

    #[test]
    fn extracts_decimal_arrow_cells_for_all_widths() {
        let fields = vec![
            Field::new("decimal32", DataType::Decimal32(9, 2), true),
            Field::new("decimal64", DataType::Decimal64(18, 4), true),
            Field::new("decimal128", DataType::Decimal128(38, 9), true),
            Field::new("decimal256", DataType::Decimal256(38, 0), true),
        ];
        let mappings = mappings_for_schema(Schema::new(fields.clone()));
        let schema = Arc::new(Schema::new(fields));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(
                    Decimal32Array::from(vec![
                        Some(12_345_i32),
                        Some(-12_345_i32),
                        Some(0_i32),
                        None,
                    ])
                    .with_precision_and_scale(9, 2)
                    .unwrap(),
                ) as ArrayRef,
                Arc::new(
                    Decimal64Array::from(vec![
                        Some(1_234_567_890_i64),
                        Some(-1_234_567_890_i64),
                        Some(0_i64),
                        None,
                    ])
                    .with_precision_and_scale(18, 4)
                    .unwrap(),
                ),
                Arc::new(
                    Decimal128Array::from(vec![
                        Some(123_456_789_012_345_678_901_234_567_890_i128),
                        Some(-123_456_789_012_345_678_901_234_567_890_i128),
                        Some(0_i128),
                        None,
                    ])
                    .with_precision_and_scale(38, 9)
                    .unwrap(),
                ),
                Arc::new(
                    Decimal256Array::from(vec![
                        Some(i256::from_i128(
                            123_456_789_012_345_678_901_234_567_890_i128,
                        )),
                        Some(i256::from_i128(
                            -123_456_789_012_345_678_901_234_567_890_i128,
                        )),
                        Some(i256::ZERO),
                        None,
                    ])
                    .with_precision_and_scale(38, 0)
                    .unwrap(),
                ),
            ],
        )
        .unwrap();
        let view = RecordBatchView::new(&batch, &mappings).unwrap();

        assert_eq!(
            view.arrow_cell(&mappings[0], 0).unwrap(),
            ArrowCell::Decimal32(12_345)
        );
        assert_eq!(
            view.arrow_cell(&mappings[0], 1).unwrap(),
            ArrowCell::Decimal32(-12_345)
        );
        assert_eq!(
            view.arrow_cell(&mappings[0], 2).unwrap(),
            ArrowCell::Decimal32(0)
        );
        assert_eq!(view.arrow_cell(&mappings[0], 3).unwrap(), ArrowCell::Null);

        assert_eq!(
            view.arrow_cell(&mappings[1], 0).unwrap(),
            ArrowCell::Decimal64(1_234_567_890)
        );
        assert_eq!(
            view.arrow_cell(&mappings[1], 1).unwrap(),
            ArrowCell::Decimal64(-1_234_567_890)
        );
        assert_eq!(
            view.arrow_cell(&mappings[1], 2).unwrap(),
            ArrowCell::Decimal64(0)
        );
        assert_eq!(view.arrow_cell(&mappings[1], 3).unwrap(), ArrowCell::Null);

        assert_eq!(
            view.arrow_cell(&mappings[2], 0).unwrap(),
            ArrowCell::Decimal128(123_456_789_012_345_678_901_234_567_890)
        );
        assert_eq!(
            view.arrow_cell(&mappings[2], 1).unwrap(),
            ArrowCell::Decimal128(-123_456_789_012_345_678_901_234_567_890)
        );
        assert_eq!(
            view.arrow_cell(&mappings[2], 2).unwrap(),
            ArrowCell::Decimal128(0)
        );
        assert_eq!(view.arrow_cell(&mappings[2], 3).unwrap(), ArrowCell::Null);

        assert_eq!(
            view.arrow_cell(&mappings[3], 0).unwrap(),
            ArrowCell::Decimal256(i256::from_i128(123_456_789_012_345_678_901_234_567_890))
        );
        assert_eq!(
            view.arrow_cell(&mappings[3], 1).unwrap(),
            ArrowCell::Decimal256(i256::from_i128(-123_456_789_012_345_678_901_234_567_890))
        );
        assert_eq!(
            view.arrow_cell(&mappings[3], 2).unwrap(),
            ArrowCell::Decimal256(i256::ZERO)
        );
        assert_eq!(view.arrow_cell(&mappings[3], 3).unwrap(), ArrowCell::Null);
    }

    #[test]
    fn extracts_date_arrow_cells() {
        let fields = vec![
            Field::new("date32", DataType::Date32, true),
            Field::new("date64", DataType::Date64, true),
        ];
        let mappings = mappings_for_schema_with_options(
            Schema::new(fields.clone()),
            PlanOptions {
                date64_policy: Date64Policy::TimestampDateTime2,
                ..PlanOptions::default()
            },
        );
        let schema = Arc::new(Schema::new(fields));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Date32Array::from(vec![
                    Some(0_i32),
                    Some(-1_i32),
                    Some(1_i32),
                    None,
                ])) as ArrayRef,
                Arc::new(Date64Array::from(vec![
                    Some(0_i64),
                    Some(-1_i64),
                    Some(86_400_123_i64),
                    None,
                ])),
            ],
        )
        .unwrap();
        let view = RecordBatchView::new(&batch, &mappings).unwrap();

        assert_eq!(
            view.arrow_cell(&mappings[0], 0).unwrap(),
            ArrowCell::Date32(0)
        );
        assert_eq!(
            view.arrow_cell(&mappings[0], 1).unwrap(),
            ArrowCell::Date32(-1)
        );
        assert_eq!(
            view.arrow_cell(&mappings[0], 2).unwrap(),
            ArrowCell::Date32(1)
        );
        assert_eq!(view.arrow_cell(&mappings[0], 3).unwrap(), ArrowCell::Null);

        assert_eq!(
            view.arrow_cell(&mappings[1], 0).unwrap(),
            ArrowCell::Date64(0)
        );
        assert_eq!(
            view.arrow_cell(&mappings[1], 1).unwrap(),
            ArrowCell::Date64(-1)
        );
        assert_eq!(
            view.arrow_cell(&mappings[1], 2).unwrap(),
            ArrowCell::Date64(86_400_123)
        );
        assert_eq!(view.arrow_cell(&mappings[1], 3).unwrap(), ArrowCell::Null);
    }

    #[test]
    fn converts_supported_initial_primitives_to_mssql_cells() {
        let mappings = mappings_for_schema(Schema::new(vec![
            Field::new("active", DataType::Boolean, true),
            Field::new("tiny", DataType::Int8, true),
            Field::new("small", DataType::Int16, true),
            Field::new("quantity", DataType::Int32, true),
            Field::new("total", DataType::Int64, true),
            Field::new("unsigned_tiny", DataType::UInt8, true),
            Field::new("unsigned_medium", DataType::UInt16, true),
            Field::new("unsigned_large", DataType::UInt32, true),
            Field::new("real_value", DataType::Float32, true),
            Field::new("float_value", DataType::Float64, true),
            Field::new("text", DataType::Utf8, true),
            Field::new("large_text", DataType::LargeUtf8, true),
            Field::new("bytes", DataType::Binary, true),
            Field::new("large_bytes", DataType::LargeBinary, true),
        ]));
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("active", DataType::Boolean, true),
                Field::new("tiny", DataType::Int8, true),
                Field::new("small", DataType::Int16, true),
                Field::new("quantity", DataType::Int32, true),
                Field::new("total", DataType::Int64, true),
                Field::new("unsigned_tiny", DataType::UInt8, true),
                Field::new("unsigned_medium", DataType::UInt16, true),
                Field::new("unsigned_large", DataType::UInt32, true),
                Field::new("real_value", DataType::Float32, true),
                Field::new("float_value", DataType::Float64, true),
                Field::new("text", DataType::Utf8, true),
                Field::new("large_text", DataType::LargeUtf8, true),
                Field::new("bytes", DataType::Binary, true),
                Field::new("large_bytes", DataType::LargeBinary, true),
            ])),
            vec![
                Arc::new(BooleanArray::from(vec![Some(true), None])) as ArrayRef,
                Arc::new(Int8Array::from(vec![Some(-8_i8), None])),
                Arc::new(Int16Array::from(vec![Some(-16_i16), None])),
                Arc::new(Int32Array::from(vec![Some(12_i32), None])),
                Arc::new(Int64Array::from(vec![Some(34_i64), None])),
                Arc::new(UInt8Array::from(vec![Some(8_u8), None])),
                Arc::new(UInt16Array::from(vec![Some(16_u16), None])),
                Arc::new(UInt32Array::from(vec![Some(32_u32), None])),
                Arc::new(Float32Array::from(vec![Some(1.25_f32), None])),
                Arc::new(Float64Array::from(vec![Some(2.5_f64), None])),
                Arc::new(StringArray::from(vec![Some("hello"), None])),
                Arc::new(LargeStringArray::from(vec![Some("東京"), None])),
                Arc::new(BinaryArray::from(vec![Some(&b"abc"[..]), None])),
                Arc::new(LargeBinaryArray::from(vec![Some(&b"large"[..]), None])),
            ],
        )
        .unwrap();
        let view = RecordBatchView::new(&batch, &mappings).unwrap();

        assert_eq!(
            view.mssql_cell(&mappings[0], 0).unwrap(),
            MssqlCell::Bit(Some(true))
        );
        assert_eq!(
            view.mssql_cell(&mappings[0], 1).unwrap(),
            MssqlCell::Bit(None)
        );
        assert_eq!(
            view.mssql_cell(&mappings[1], 0).unwrap(),
            MssqlCell::SmallInt(Some(-8))
        );
        assert_eq!(
            view.mssql_cell(&mappings[1], 1).unwrap(),
            MssqlCell::SmallInt(None)
        );
        assert_eq!(
            view.mssql_cell(&mappings[2], 0).unwrap(),
            MssqlCell::SmallInt(Some(-16))
        );
        assert_eq!(
            view.mssql_cell(&mappings[2], 1).unwrap(),
            MssqlCell::SmallInt(None)
        );
        assert_eq!(
            view.mssql_cell(&mappings[3], 0).unwrap(),
            MssqlCell::Int(Some(12))
        );
        assert_eq!(
            view.mssql_cell(&mappings[3], 1).unwrap(),
            MssqlCell::Int(None)
        );
        assert_eq!(
            view.mssql_cell(&mappings[4], 0).unwrap(),
            MssqlCell::BigInt(Some(34))
        );
        assert_eq!(
            view.mssql_cell(&mappings[4], 1).unwrap(),
            MssqlCell::BigInt(None)
        );
        assert_eq!(
            view.mssql_cell(&mappings[5], 0).unwrap(),
            MssqlCell::TinyInt(Some(8))
        );
        assert_eq!(
            view.mssql_cell(&mappings[5], 1).unwrap(),
            MssqlCell::TinyInt(None)
        );
        assert_eq!(
            view.mssql_cell(&mappings[6], 0).unwrap(),
            MssqlCell::Int(Some(16))
        );
        assert_eq!(
            view.mssql_cell(&mappings[6], 1).unwrap(),
            MssqlCell::Int(None)
        );
        assert_eq!(
            view.mssql_cell(&mappings[7], 0).unwrap(),
            MssqlCell::BigInt(Some(32))
        );
        assert_eq!(
            view.mssql_cell(&mappings[7], 1).unwrap(),
            MssqlCell::BigInt(None)
        );
        assert_eq!(
            view.mssql_cell(&mappings[8], 0).unwrap(),
            MssqlCell::Real(Some(1.25))
        );
        assert_eq!(
            view.mssql_cell(&mappings[8], 1).unwrap(),
            MssqlCell::Real(None)
        );
        assert_eq!(
            view.mssql_cell(&mappings[9], 0).unwrap(),
            MssqlCell::Float(Some(2.5))
        );
        assert_eq!(
            view.mssql_cell(&mappings[9], 1).unwrap(),
            MssqlCell::Float(None)
        );
        assert_eq!(
            view.mssql_cell(&mappings[10], 0).unwrap(),
            MssqlCell::NVarChar(Some("hello"))
        );
        assert_eq!(
            view.mssql_cell(&mappings[10], 1).unwrap(),
            MssqlCell::NVarChar(None)
        );
        assert_eq!(
            view.mssql_cell(&mappings[11], 0).unwrap(),
            MssqlCell::NVarChar(Some("東京"))
        );
        assert_eq!(
            view.mssql_cell(&mappings[11], 1).unwrap(),
            MssqlCell::NVarChar(None)
        );
        assert_eq!(
            view.mssql_cell(&mappings[12], 0).unwrap(),
            MssqlCell::VarBinary(Some(b"abc"))
        );
        assert_eq!(
            view.mssql_cell(&mappings[12], 1).unwrap(),
            MssqlCell::VarBinary(None)
        );
        assert_eq!(
            view.mssql_cell(&mappings[13], 0).unwrap(),
            MssqlCell::VarBinary(Some(b"large"))
        );
        assert_eq!(
            view.mssql_cell(&mappings[13], 1).unwrap(),
            MssqlCell::VarBinary(None)
        );
    }

    #[test]
    fn converts_runtime_row_to_mssql_cells_in_mapping_order() {
        let mappings = mappings_for_schema(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("active", DataType::Boolean, true),
            Field::new("name", DataType::Utf8, true),
            Field::new("payload", DataType::Binary, true),
        ]));
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("id", DataType::Int32, false),
                Field::new("active", DataType::Boolean, true),
                Field::new("name", DataType::Utf8, true),
                Field::new("payload", DataType::Binary, true),
            ])),
            vec![
                Arc::new(Int32Array::from(vec![1_i32, 2])) as ArrayRef,
                Arc::new(BooleanArray::from(vec![Some(true), None])),
                Arc::new(StringArray::from(vec![Some("first"), Some("second")])),
                Arc::new(BinaryArray::from(vec![Some(&b"abc"[..]), None])),
            ],
        )
        .unwrap();
        let view = RecordBatchView::new(&batch, &mappings).unwrap();

        let first_row = view.mssql_row(0).unwrap();
        assert_eq!(
            first_row,
            vec![
                MssqlCell::Int(Some(1)),
                MssqlCell::Bit(Some(true)),
                MssqlCell::NVarChar(Some("first")),
                MssqlCell::VarBinary(Some(b"abc")),
            ]
        );

        let second_row = view.mssql_row(1).unwrap();
        assert_eq!(
            second_row,
            vec![
                MssqlCell::Int(Some(2)),
                MssqlCell::Bit(None),
                MssqlCell::NVarChar(Some("second")),
                MssqlCell::VarBinary(None),
            ]
        );
    }

    #[test]
    fn converts_runtime_row_to_owned_tiberius_token_row() {
        let mappings = mappings_for_schema(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("active", DataType::Boolean, true),
            Field::new("name", DataType::Utf8, true),
            Field::new("payload", DataType::Binary, true),
        ]));
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("id", DataType::Int32, false),
                Field::new("active", DataType::Boolean, true),
                Field::new("name", DataType::Utf8, true),
                Field::new("payload", DataType::Binary, true),
            ])),
            vec![
                Arc::new(Int32Array::from(vec![1_i32])) as ArrayRef,
                Arc::new(BooleanArray::from(vec![Some(true)])),
                Arc::new(StringArray::from(vec![Some("first")])),
                Arc::new(BinaryArray::from(vec![Some(&b"abc"[..])])),
            ],
        )
        .unwrap();
        let view = RecordBatchView::new(&batch, &mappings).unwrap();

        let row = view.tiberius_row_owned(0).unwrap();

        assert_eq!(row.len(), 4);
        assert_eq!(row.get(0), Some(&tiberius::ColumnData::I32(Some(1))));
        assert_eq!(row.get(1), Some(&tiberius::ColumnData::Bit(Some(true))));

        let Some(tiberius::ColumnData::String(Some(Cow::Owned(value)))) = row.get(2) else {
            panic!("expected owned string column data");
        };
        assert_eq!(value, "first");

        let Some(tiberius::ColumnData::Binary(Some(Cow::Owned(value)))) = row.get(3) else {
            panic!("expected owned binary column data");
        };
        assert_eq!(value, b"abc");
    }

    #[test]
    fn row_helpers_reject_row_index_out_of_bounds() {
        let mappings =
            mappings_for_schema(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)])),
            vec![Arc::new(Int32Array::from(vec![1_i32]))],
        )
        .unwrap();
        let view = RecordBatchView::new(&batch, &mappings).unwrap();

        let err = view.mssql_row(1).unwrap_err();
        assert_single_diagnostic(err, DiagnosticCode::RowIndexOutOfBounds, Some(1), None);

        let err = view.tiberius_row_owned(1).unwrap_err();
        assert_single_diagnostic(err, DiagnosticCode::RowIndexOutOfBounds, Some(1), None);
    }

    #[test]
    fn row_helpers_preserve_conversion_diagnostics() {
        let mappings = mappings_for_schema(Schema::new(vec![Field::new(
            "ratio",
            DataType::Float64,
            true,
        )]));
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "ratio",
                DataType::Float64,
                true,
            )])),
            vec![Arc::new(Float64Array::from(vec![f64::NAN]))],
        )
        .unwrap();
        let view = RecordBatchView::new(&batch, &mappings).unwrap();

        let err = view.mssql_row(0).unwrap_err();
        assert_single_diagnostic(
            err,
            DiagnosticCode::NonFiniteFloat,
            Some(0),
            Some((0, "ratio")),
        );

        let err = view.tiberius_row_owned(0).unwrap_err();
        assert_single_diagnostic(
            err,
            DiagnosticCode::NonFiniteFloat,
            Some(0),
            Some((0, "ratio")),
        );
    }

    #[test]
    fn converts_mssql_cells_to_borrowed_tiberius_column_data() {
        assert_eq!(
            mssql_cell_to_tiberius_borrowed(MssqlCell::Bit(Some(true))),
            tiberius::ColumnData::Bit(Some(true))
        );
        assert_eq!(
            mssql_cell_to_tiberius_borrowed(MssqlCell::Bit(None)),
            tiberius::ColumnData::Bit(None)
        );
        assert_eq!(
            mssql_cell_to_tiberius_borrowed(MssqlCell::TinyInt(Some(8))),
            tiberius::ColumnData::U8(Some(8))
        );
        assert_eq!(
            mssql_cell_to_tiberius_borrowed(MssqlCell::TinyInt(None)),
            tiberius::ColumnData::U8(None)
        );
        assert_eq!(
            mssql_cell_to_tiberius_borrowed(MssqlCell::SmallInt(Some(-16))),
            tiberius::ColumnData::I16(Some(-16))
        );
        assert_eq!(
            mssql_cell_to_tiberius_borrowed(MssqlCell::SmallInt(None)),
            tiberius::ColumnData::I16(None)
        );
        assert_eq!(
            mssql_cell_to_tiberius_borrowed(MssqlCell::Int(Some(32))),
            tiberius::ColumnData::I32(Some(32))
        );
        assert_eq!(
            mssql_cell_to_tiberius_borrowed(MssqlCell::Int(None)),
            tiberius::ColumnData::I32(None)
        );
        assert_eq!(
            mssql_cell_to_tiberius_borrowed(MssqlCell::BigInt(Some(64))),
            tiberius::ColumnData::I64(Some(64))
        );
        assert_eq!(
            mssql_cell_to_tiberius_borrowed(MssqlCell::BigInt(None)),
            tiberius::ColumnData::I64(None)
        );
        assert_eq!(
            mssql_cell_to_tiberius_borrowed(MssqlCell::Decimal(Some(MssqlDecimal::new(12345, 2)))),
            tiberius::ColumnData::Numeric(Some(tiberius::numeric::Numeric::new_with_scale(
                12345, 2
            )))
        );
        assert_eq!(
            mssql_cell_to_tiberius_borrowed(MssqlCell::Decimal(None)),
            tiberius::ColumnData::Numeric(None)
        );
        assert_eq!(
            mssql_cell_to_tiberius_borrowed(MssqlCell::Date(Some(MssqlDate::new(719_163)))),
            tiberius::ColumnData::Date(Some(tiberius::time::Date::new(719_163)))
        );
        assert_eq!(
            mssql_cell_to_tiberius_borrowed(MssqlCell::Date(None)),
            tiberius::ColumnData::Date(None)
        );
        assert_eq!(
            mssql_cell_to_tiberius_borrowed(MssqlCell::DateTime2(Some(MssqlDateTime2::new(
                MssqlDate::new(719_163),
                MssqlTime::new(43_200_123, 3),
            )))),
            tiberius::ColumnData::DateTime2(Some(tiberius::time::DateTime2::new(
                tiberius::time::Date::new(719_163),
                tiberius::time::Time::new(43_200_123, 3),
            )))
        );
        assert_eq!(
            mssql_cell_to_tiberius_borrowed(MssqlCell::DateTime2(None)),
            tiberius::ColumnData::DateTime2(None)
        );
        assert_eq!(
            mssql_cell_to_tiberius_borrowed(MssqlCell::DateTimeOffset(Some(
                MssqlDateTimeOffset::new(
                    MssqlDateTime2::new(MssqlDate::new(719_163), MssqlTime::new(43_200_123, 3)),
                    -420,
                ),
            ))),
            tiberius::ColumnData::DateTimeOffset(Some(tiberius::time::DateTimeOffset::new(
                tiberius::time::DateTime2::new(
                    tiberius::time::Date::new(719_163),
                    tiberius::time::Time::new(43_200_123, 3),
                ),
                -420,
            )))
        );
        assert_eq!(
            mssql_cell_to_tiberius_borrowed(MssqlCell::DateTimeOffset(None)),
            tiberius::ColumnData::DateTimeOffset(None)
        );
        assert_eq!(
            mssql_cell_to_tiberius_borrowed(MssqlCell::Real(Some(1.25))),
            tiberius::ColumnData::F32(Some(1.25))
        );
        assert_eq!(
            mssql_cell_to_tiberius_borrowed(MssqlCell::Real(None)),
            tiberius::ColumnData::F32(None)
        );
        assert_eq!(
            mssql_cell_to_tiberius_borrowed(MssqlCell::Float(Some(2.5))),
            tiberius::ColumnData::F64(Some(2.5))
        );
        assert_eq!(
            mssql_cell_to_tiberius_borrowed(MssqlCell::Float(None)),
            tiberius::ColumnData::F64(None)
        );

        let text = "hello";
        let bytes = b"abc".as_slice();

        let text_data = mssql_cell_to_tiberius_borrowed(MssqlCell::NVarChar(Some(text)));
        let tiberius::ColumnData::String(Some(Cow::Borrowed(value))) = text_data else {
            panic!("expected borrowed string column data");
        };
        assert_eq!(value, text);

        assert_eq!(
            mssql_cell_to_tiberius_borrowed(MssqlCell::NVarChar(None)),
            tiberius::ColumnData::String(None)
        );

        let binary_data = mssql_cell_to_tiberius_borrowed(MssqlCell::VarBinary(Some(bytes)));
        let tiberius::ColumnData::Binary(Some(Cow::Borrowed(value))) = binary_data else {
            panic!("expected borrowed binary column data");
        };
        assert_eq!(value, bytes);

        assert_eq!(
            mssql_cell_to_tiberius_borrowed(MssqlCell::VarBinary(None)),
            tiberius::ColumnData::Binary(None)
        );
    }

    #[test]
    fn converts_mssql_cells_to_owned_tiberius_column_data() {
        assert_eq!(
            mssql_cell_to_tiberius_owned(MssqlCell::Bit(Some(true))),
            tiberius::ColumnData::Bit(Some(true))
        );
        assert_eq!(
            mssql_cell_to_tiberius_owned(MssqlCell::TinyInt(Some(8))),
            tiberius::ColumnData::U8(Some(8))
        );
        assert_eq!(
            mssql_cell_to_tiberius_owned(MssqlCell::SmallInt(Some(-16))),
            tiberius::ColumnData::I16(Some(-16))
        );
        assert_eq!(
            mssql_cell_to_tiberius_owned(MssqlCell::Int(Some(32))),
            tiberius::ColumnData::I32(Some(32))
        );
        assert_eq!(
            mssql_cell_to_tiberius_owned(MssqlCell::BigInt(Some(64))),
            tiberius::ColumnData::I64(Some(64))
        );
        assert_eq!(
            mssql_cell_to_tiberius_owned(MssqlCell::Decimal(Some(MssqlDecimal::new(12345, 2)))),
            tiberius::ColumnData::Numeric(Some(tiberius::numeric::Numeric::new_with_scale(
                12345, 2
            )))
        );
        assert_eq!(
            mssql_cell_to_tiberius_owned(MssqlCell::Decimal(None)),
            tiberius::ColumnData::Numeric(None)
        );
        assert_eq!(
            mssql_cell_to_tiberius_owned(MssqlCell::Date(Some(MssqlDate::new(719_163)))),
            tiberius::ColumnData::Date(Some(tiberius::time::Date::new(719_163)))
        );
        assert_eq!(
            mssql_cell_to_tiberius_owned(MssqlCell::Date(None)),
            tiberius::ColumnData::Date(None)
        );
        assert_eq!(
            mssql_cell_to_tiberius_owned(MssqlCell::DateTime2(Some(MssqlDateTime2::new(
                MssqlDate::new(719_163),
                MssqlTime::new(43_200_123, 3),
            )))),
            tiberius::ColumnData::DateTime2(Some(tiberius::time::DateTime2::new(
                tiberius::time::Date::new(719_163),
                tiberius::time::Time::new(43_200_123, 3),
            )))
        );
        assert_eq!(
            mssql_cell_to_tiberius_owned(MssqlCell::DateTime2(None)),
            tiberius::ColumnData::DateTime2(None)
        );
        assert_eq!(
            mssql_cell_to_tiberius_owned(MssqlCell::DateTimeOffset(Some(
                MssqlDateTimeOffset::new(
                    MssqlDateTime2::new(MssqlDate::new(719_163), MssqlTime::new(43_200_123, 3)),
                    330,
                ),
            ))),
            tiberius::ColumnData::DateTimeOffset(Some(tiberius::time::DateTimeOffset::new(
                tiberius::time::DateTime2::new(
                    tiberius::time::Date::new(719_163),
                    tiberius::time::Time::new(43_200_123, 3),
                ),
                330,
            )))
        );
        assert_eq!(
            mssql_cell_to_tiberius_owned(MssqlCell::DateTimeOffset(None)),
            tiberius::ColumnData::DateTimeOffset(None)
        );
        assert_eq!(
            mssql_cell_to_tiberius_owned(MssqlCell::Real(Some(1.25))),
            tiberius::ColumnData::F32(Some(1.25))
        );
        assert_eq!(
            mssql_cell_to_tiberius_owned(MssqlCell::Float(Some(2.5))),
            tiberius::ColumnData::F64(Some(2.5))
        );

        let text_data = mssql_cell_to_tiberius_owned(MssqlCell::NVarChar(Some("hello")));
        let tiberius::ColumnData::String(Some(Cow::Owned(value))) = text_data else {
            panic!("expected owned string column data");
        };
        assert_eq!(value, "hello");

        assert_eq!(
            mssql_cell_to_tiberius_owned(MssqlCell::NVarChar(None)),
            tiberius::ColumnData::String(None)
        );

        let binary_data = mssql_cell_to_tiberius_owned(MssqlCell::VarBinary(Some(b"abc")));
        let tiberius::ColumnData::Binary(Some(Cow::Owned(value))) = binary_data else {
            panic!("expected owned binary column data");
        };
        assert_eq!(value, b"abc");

        assert_eq!(
            mssql_cell_to_tiberius_owned(MssqlCell::VarBinary(None)),
            tiberius::ColumnData::Binary(None)
        );
    }

    #[test]
    fn mssql_datetimeoffset_exposes_datetime_and_offset_components() {
        let datetime2 = MssqlDateTime2::new(MssqlDate::new(719_163), MssqlTime::new(1, 7));
        let datetimeoffset = MssqlDateTimeOffset::new(datetime2, -840);

        assert_eq!(datetimeoffset.datetime2(), datetime2);
        assert_eq!(datetimeoffset.offset_minutes(), -840);
    }

    #[test]
    fn resolves_fixed_timezone_offsets_for_datetimeoffset() {
        let mapping = timezone_timestamp_mapping("+00:00", TimezonePolicy::DateTimeOffset);

        for (timezone, expected_minutes) in [
            ("UTC", 0),
            ("+00:00", 0),
            ("-00:00", 0),
            ("+02:30", 150),
            ("+0230", 150),
            ("-07", -420),
            ("-07:45", -465),
            ("+14:00", 840),
            ("-14:00", -840),
        ] {
            let resolution = timezone_resolution_from_metadata(&mapping, 7, timezone).unwrap();

            assert_eq!(
                resolution.offset_for_instant(&mapping, 7, 0, 0).unwrap(),
                expected_minutes
            );
            assert_eq!(
                resolution
                    .offset_for_instant(&mapping, 7, 1_750_594_400, 0)
                    .unwrap(),
                expected_minutes
            );
        }
    }

    #[test]
    fn resolves_named_timezone_offsets_for_each_instant() {
        let mapping =
            timezone_timestamp_mapping("America/New_York", TimezonePolicy::DateTimeOffset);
        let resolution =
            timezone_resolution_from_metadata(&mapping, 0, "America/New_York").unwrap();

        let winter_epoch = 1_738_411_200;
        let summer_epoch = 1_750_594_400;

        assert_eq!(
            resolution
                .offset_for_instant(&mapping, 0, winter_epoch, 0)
                .unwrap(),
            -300
        );
        assert_eq!(
            resolution
                .offset_for_instant(&mapping, 1, summer_epoch, 0)
                .unwrap(),
            -240
        );
    }

    #[test]
    fn rejects_invalid_timezone_names_and_unrepresentable_offsets() {
        let mapping = timezone_timestamp_mapping("+00:00", TimezonePolicy::DateTimeOffset);

        for timezone in ["", " ", "Foobar", "+1:00", "+ab:cd", "+02:3x", "+12:60"] {
            let err = timezone_resolution_from_metadata(&mapping, 7, timezone).unwrap_err();
            assert_single_diagnostic(
                err,
                DiagnosticCode::TimezoneUnsupported,
                Some(7),
                Some((0, "ts")),
            );
        }

        let err = timezone_resolution_from_metadata(&mapping, 7, "+14:01").unwrap_err();
        assert_single_diagnostic(
            err,
            DiagnosticCode::TimezoneUnsupported,
            Some(7),
            Some((0, "ts")),
        );
    }

    #[test]
    fn converts_empty_ascii_and_non_ascii_strings() {
        let mappings = mappings_for_schema(Schema::new(vec![
            Field::new("text", DataType::Utf8, true),
            Field::new("large_text", DataType::LargeUtf8, true),
        ]));
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("text", DataType::Utf8, true),
                Field::new("large_text", DataType::LargeUtf8, true),
            ])),
            vec![
                Arc::new(StringArray::from(vec!["", "ascii", "東京"])) as ArrayRef,
                Arc::new(LargeStringArray::from(vec!["", "ascii", "🙂"])),
            ],
        )
        .unwrap();
        let view = RecordBatchView::new(&batch, &mappings).unwrap();

        assert_eq!(
            view.mssql_cell(&mappings[0], 0).unwrap(),
            MssqlCell::NVarChar(Some(""))
        );
        assert_eq!(
            view.mssql_cell(&mappings[0], 1).unwrap(),
            MssqlCell::NVarChar(Some("ascii"))
        );
        assert_eq!(
            view.mssql_cell(&mappings[0], 2).unwrap(),
            MssqlCell::NVarChar(Some("東京"))
        );
        assert_eq!(
            view.mssql_cell(&mappings[1], 0).unwrap(),
            MssqlCell::NVarChar(Some(""))
        );
        assert_eq!(
            view.mssql_cell(&mappings[1], 1).unwrap(),
            MssqlCell::NVarChar(Some("ascii"))
        );
        assert_eq!(
            view.mssql_cell(&mappings[1], 2).unwrap(),
            MssqlCell::NVarChar(Some("🙂"))
        );
    }

    #[test]
    fn converts_empty_and_non_empty_binary_values() {
        let mappings = mappings_for_schema(Schema::new(vec![
            Field::new("bytes", DataType::Binary, true),
            Field::new("large_bytes", DataType::LargeBinary, true),
        ]));
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("bytes", DataType::Binary, true),
                Field::new("large_bytes", DataType::LargeBinary, true),
            ])),
            vec![
                Arc::new(BinaryArray::from(vec![Some(&b""[..]), Some(&b"abc"[..])])) as ArrayRef,
                Arc::new(LargeBinaryArray::from(vec![
                    Some(&b""[..]),
                    Some(&b"large"[..]),
                ])),
            ],
        )
        .unwrap();
        let view = RecordBatchView::new(&batch, &mappings).unwrap();

        assert_eq!(
            view.mssql_cell(&mappings[0], 0).unwrap(),
            MssqlCell::VarBinary(Some(b""))
        );
        assert_eq!(
            view.mssql_cell(&mappings[0], 1).unwrap(),
            MssqlCell::VarBinary(Some(b"abc"))
        );
        assert_eq!(
            view.mssql_cell(&mappings[1], 0).unwrap(),
            MssqlCell::VarBinary(Some(b""))
        );
        assert_eq!(
            view.mssql_cell(&mappings[1], 1).unwrap(),
            MssqlCell::VarBinary(Some(b"large"))
        );
    }

    #[test]
    fn rejects_bounded_nvarchar_by_utf16_code_units() {
        let mappings = mappings_for_schema_with_options(
            Schema::new(vec![Field::new("text", DataType::Utf8, true)]),
            PlanOptions {
                string_policy: StringPolicy::NVarChar(2),
                ..PlanOptions::default()
            },
        );
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("text", DataType::Utf8, true)])),
            vec![Arc::new(StringArray::from(vec!["ab", "🙂", "abc"]))],
        )
        .unwrap();
        let view = RecordBatchView::new(&batch, &mappings).unwrap();

        assert_eq!(
            view.mssql_cell(&mappings[0], 0).unwrap(),
            MssqlCell::NVarChar(Some("ab"))
        );
        assert_eq!(
            view.mssql_cell(&mappings[0], 1).unwrap(),
            MssqlCell::NVarChar(Some("🙂"))
        );
        let err = view.mssql_cell(&mappings[0], 2).unwrap_err();

        assert_single_diagnostic(
            err,
            DiagnosticCode::ValueTooLong,
            Some(2),
            Some((0, "text")),
        );
    }

    #[test]
    fn rejects_bounded_varbinary_by_byte_count() {
        let mappings = mappings_for_schema_with_options(
            Schema::new(vec![Field::new("bytes", DataType::Binary, true)]),
            PlanOptions {
                binary_policy: BinaryPolicy::VarBinary(2),
                ..PlanOptions::default()
            },
        );
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "bytes",
                DataType::Binary,
                true,
            )])),
            vec![Arc::new(BinaryArray::from(vec![
                Some(&b""[..]),
                Some(&b"ab"[..]),
                Some(&b"abc"[..]),
            ]))],
        )
        .unwrap();
        let view = RecordBatchView::new(&batch, &mappings).unwrap();

        assert_eq!(
            view.mssql_cell(&mappings[0], 0).unwrap(),
            MssqlCell::VarBinary(Some(b""))
        );
        assert_eq!(
            view.mssql_cell(&mappings[0], 1).unwrap(),
            MssqlCell::VarBinary(Some(b"ab"))
        );
        let err = view.mssql_cell(&mappings[0], 2).unwrap_err();

        assert_single_diagnostic(
            err,
            DiagnosticCode::ValueTooLong,
            Some(2),
            Some((0, "bytes")),
        );
    }

    #[test]
    fn converts_uint64_decimal20_0_boundary_values() {
        let mappings = mappings_for_schema_with_options(
            Schema::new(vec![Field::new(
                "unsigned_as_decimal",
                DataType::UInt64,
                true,
            )]),
            PlanOptions {
                uint64_policy: UInt64Policy::Decimal20_0,
                ..PlanOptions::default()
            },
        );
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "unsigned_as_decimal",
                DataType::UInt64,
                true,
            )])),
            vec![Arc::new(UInt64Array::from(vec![
                Some(0_u64),
                Some(i64::MAX as u64),
                Some((i64::MAX as u64) + 1),
                Some(u64::MAX),
                None,
            ]))],
        )
        .unwrap();
        let view = RecordBatchView::new(&batch, &mappings).unwrap();

        assert_eq!(
            view.mssql_cell(&mappings[0], 0).unwrap(),
            MssqlCell::Decimal(Some(MssqlDecimal::new(0, 0)))
        );
        assert_eq!(
            view.mssql_cell(&mappings[0], 1).unwrap(),
            MssqlCell::Decimal(Some(MssqlDecimal::new(i128::from(i64::MAX), 0)))
        );
        assert_eq!(
            view.mssql_cell(&mappings[0], 2).unwrap(),
            MssqlCell::Decimal(Some(MssqlDecimal::new(i128::from(i64::MAX) + 1, 0)))
        );
        assert_eq!(
            view.mssql_cell(&mappings[0], 3).unwrap(),
            MssqlCell::Decimal(Some(MssqlDecimal::new(i128::from(u64::MAX), 0)))
        );
        assert_eq!(
            view.mssql_cell(&mappings[0], 4).unwrap(),
            MssqlCell::Decimal(None)
        );
    }

    #[test]
    fn converts_uint64_decimal20_0_to_owned_tiberius_numeric() {
        let mappings = mappings_for_schema_with_options(
            Schema::new(vec![Field::new(
                "unsigned_as_decimal",
                DataType::UInt64,
                true,
            )]),
            PlanOptions {
                uint64_policy: UInt64Policy::Decimal20_0,
                ..PlanOptions::default()
            },
        );
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "unsigned_as_decimal",
                DataType::UInt64,
                true,
            )])),
            vec![Arc::new(UInt64Array::from(vec![Some(u64::MAX)]))],
        )
        .unwrap();
        let view = RecordBatchView::new(&batch, &mappings).unwrap();

        let row = view.tiberius_row_owned(0).unwrap();

        assert_eq!(
            row.get(0),
            Some(&tiberius::ColumnData::Numeric(Some(
                tiberius::numeric::Numeric::new_with_scale(i128::from(u64::MAX), 0)
            )))
        );
    }

    #[test]
    fn converts_decimal32_64_128_cells_with_sign_zero_scale_and_null() {
        let fields = vec![
            Field::new("decimal32", DataType::Decimal32(9, 2), true),
            Field::new("decimal64", DataType::Decimal64(18, 4), true),
            Field::new("decimal128", DataType::Decimal128(38, 9), true),
        ];
        let mappings = mappings_for_schema(Schema::new(fields.clone()));
        let schema = Arc::new(Schema::new(fields));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(
                    Decimal32Array::from(vec![
                        Some(12_345_i32),
                        Some(-12_345_i32),
                        Some(0_i32),
                        None,
                    ])
                    .with_precision_and_scale(9, 2)
                    .unwrap(),
                ) as ArrayRef,
                Arc::new(
                    Decimal64Array::from(vec![
                        Some(1_234_567_890_i64),
                        Some(-1_234_567_890_i64),
                        Some(0_i64),
                        None,
                    ])
                    .with_precision_and_scale(18, 4)
                    .unwrap(),
                ),
                Arc::new(
                    Decimal128Array::from(vec![
                        Some(123_456_789_012_345_678_901_234_567_890_i128),
                        Some(-123_456_789_012_345_678_901_234_567_890_i128),
                        Some(0_i128),
                        None,
                    ])
                    .with_precision_and_scale(38, 9)
                    .unwrap(),
                ),
            ],
        )
        .unwrap();
        let view = RecordBatchView::new(&batch, &mappings).unwrap();

        assert_eq!(
            view.mssql_cell(&mappings[0], 0).unwrap(),
            MssqlCell::Decimal(Some(MssqlDecimal::new(12_345, 2)))
        );
        assert_eq!(
            view.mssql_cell(&mappings[0], 1).unwrap(),
            MssqlCell::Decimal(Some(MssqlDecimal::new(-12_345, 2)))
        );
        assert_eq!(
            view.mssql_cell(&mappings[0], 2).unwrap(),
            MssqlCell::Decimal(Some(MssqlDecimal::new(0, 2)))
        );
        assert_eq!(
            view.mssql_cell(&mappings[0], 3).unwrap(),
            MssqlCell::Decimal(None)
        );

        assert_eq!(
            view.mssql_cell(&mappings[1], 0).unwrap(),
            MssqlCell::Decimal(Some(MssqlDecimal::new(1_234_567_890, 4)))
        );
        assert_eq!(
            view.mssql_cell(&mappings[1], 1).unwrap(),
            MssqlCell::Decimal(Some(MssqlDecimal::new(-1_234_567_890, 4)))
        );
        assert_eq!(
            view.mssql_cell(&mappings[1], 2).unwrap(),
            MssqlCell::Decimal(Some(MssqlDecimal::new(0, 4)))
        );
        assert_eq!(
            view.mssql_cell(&mappings[1], 3).unwrap(),
            MssqlCell::Decimal(None)
        );

        assert_eq!(
            view.mssql_cell(&mappings[2], 0).unwrap(),
            MssqlCell::Decimal(Some(MssqlDecimal::new(
                123_456_789_012_345_678_901_234_567_890,
                9,
            )))
        );
        assert_eq!(
            view.mssql_cell(&mappings[2], 1).unwrap(),
            MssqlCell::Decimal(Some(MssqlDecimal::new(
                -123_456_789_012_345_678_901_234_567_890,
                9,
            )))
        );
        assert_eq!(
            view.mssql_cell(&mappings[2], 2).unwrap(),
            MssqlCell::Decimal(Some(MssqlDecimal::new(0, 9)))
        );
        assert_eq!(
            view.mssql_cell(&mappings[2], 3).unwrap(),
            MssqlCell::Decimal(None)
        );
    }

    #[test]
    fn normalizes_negative_decimal_scale_at_runtime() {
        let mappings = mappings_for_schema_with_options(
            Schema::new(vec![Field::new(
                "amount",
                DataType::Decimal128(3, -2),
                true,
            )]),
            PlanOptions {
                decimal_policy: DecimalPolicy::NormalizeNegativeScale,
                ..PlanOptions::default()
            },
        );
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "amount",
                DataType::Decimal128(3, -2),
                true,
            )])),
            vec![Arc::new(
                Decimal128Array::from(vec![Some(123_i128), Some(-123_i128), Some(0), None])
                    .with_precision_and_scale(3, -2)
                    .unwrap(),
            )],
        )
        .unwrap();
        let view = RecordBatchView::new(&batch, &mappings).unwrap();

        assert_eq!(
            view.mssql_cell(&mappings[0], 0).unwrap(),
            MssqlCell::Decimal(Some(MssqlDecimal::new(12_300, 0)))
        );
        assert_eq!(
            view.mssql_cell(&mappings[0], 1).unwrap(),
            MssqlCell::Decimal(Some(MssqlDecimal::new(-12_300, 0)))
        );
        assert_eq!(
            view.mssql_cell(&mappings[0], 2).unwrap(),
            MssqlCell::Decimal(Some(MssqlDecimal::new(0, 0)))
        );
        assert_eq!(
            view.mssql_cell(&mappings[0], 3).unwrap(),
            MssqlCell::Decimal(None)
        );
    }

    #[test]
    fn rejects_negative_decimal_scale_normalization_overflow() {
        let mappings = mappings_for_schema_with_options(
            Schema::new(vec![Field::new(
                "amount",
                DataType::Decimal128(37, -1),
                false,
            )]),
            PlanOptions {
                decimal_policy: DecimalPolicy::NormalizeNegativeScale,
                ..PlanOptions::default()
            },
        );
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "amount",
                DataType::Decimal128(37, -1),
                false,
            )])),
            vec![malicious_decimal128_array(
                DataType::Decimal128(37, -1),
                &[i128::MAX],
            )],
        )
        .unwrap();
        let view = RecordBatchView::new(&batch, &mappings).unwrap();

        let err = view.mssql_cell(&mappings[0], 0).unwrap_err();

        assert_single_diagnostic(
            err,
            DiagnosticCode::DecimalOutOfRange,
            Some(0),
            Some((0, "amount")),
        );
    }

    #[test]
    fn converts_decimal128_to_owned_tiberius_numeric_with_scale() {
        let mappings = mappings_for_schema(Schema::new(vec![Field::new(
            "amount",
            DataType::Decimal128(10, 3),
            true,
        )]));
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "amount",
                DataType::Decimal128(10, 3),
                true,
            )])),
            vec![Arc::new(
                Decimal128Array::from(vec![Some(-123_456_i128)])
                    .with_precision_and_scale(10, 3)
                    .unwrap(),
            )],
        )
        .unwrap();
        let view = RecordBatchView::new(&batch, &mappings).unwrap();

        let row = view.tiberius_row_owned(0).unwrap();

        assert_eq!(
            row.get(0),
            Some(&tiberius::ColumnData::Numeric(Some(
                tiberius::numeric::Numeric::new_with_scale(-123_456, 3)
            )))
        );
    }

    #[test]
    fn rejects_decimal_scale_that_tiberius_numeric_cannot_represent() {
        let mappings = mappings_for_schema(Schema::new(vec![Field::new(
            "amount",
            DataType::Decimal128(38, 38),
            true,
        )]));
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "amount",
                DataType::Decimal128(38, 38),
                true,
            )])),
            vec![Arc::new(
                Decimal128Array::from(vec![Some(1_i128)])
                    .with_precision_and_scale(38, 38)
                    .unwrap(),
            )],
        )
        .unwrap();
        let view = RecordBatchView::new(&batch, &mappings).unwrap();

        let err = view.tiberius_row_owned(0).unwrap_err();

        assert_single_diagnostic(
            err,
            DiagnosticCode::DecimalOutOfRange,
            Some(0),
            Some((0, "amount")),
        );
    }

    #[test]
    fn accepts_decimal_values_at_planned_precision_boundaries() {
        let mappings = mappings_for_schema(Schema::new(vec![Field::new(
            "amount",
            DataType::Decimal128(5, 2),
            false,
        )]));
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "amount",
                DataType::Decimal128(5, 2),
                false,
            )])),
            vec![Arc::new(
                Decimal128Array::from(vec![99_999_i128, -99_999_i128])
                    .with_precision_and_scale(5, 2)
                    .unwrap(),
            )],
        )
        .unwrap();
        let view = RecordBatchView::new(&batch, &mappings).unwrap();

        assert_eq!(
            view.mssql_cell(&mappings[0], 0).unwrap(),
            MssqlCell::Decimal(Some(MssqlDecimal::new(99_999, 2)))
        );
        assert_eq!(
            view.mssql_cell(&mappings[0], 1).unwrap(),
            MssqlCell::Decimal(Some(MssqlDecimal::new(-99_999, 2)))
        );
    }

    #[test]
    fn rejects_decimal_values_outside_planned_precision() {
        let mappings = mappings_for_schema(Schema::new(vec![Field::new(
            "amount",
            DataType::Decimal128(5, 2),
            false,
        )]));
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "amount",
                DataType::Decimal128(5, 2),
                false,
            )])),
            vec![malicious_decimal128_array(
                DataType::Decimal128(5, 2),
                &[100_000_i128, -100_000_i128],
            )],
        )
        .unwrap();
        let view = RecordBatchView::new(&batch, &mappings).unwrap();

        let positive = view.mssql_cell(&mappings[0], 0).unwrap_err();
        assert_single_diagnostic(
            positive,
            DiagnosticCode::DecimalOutOfRange,
            Some(0),
            Some((0, "amount")),
        );

        let negative = view.mssql_cell(&mappings[0], 1).unwrap_err();
        assert_single_diagnostic(
            negative,
            DiagnosticCode::DecimalOutOfRange,
            Some(1),
            Some((0, "amount")),
        );
    }

    #[test]
    fn converts_uint64_checked_bigint_boundary_values() {
        let mappings = mappings_for_schema_with_options(
            Schema::new(vec![Field::new(
                "unsigned_as_bigint",
                DataType::UInt64,
                true,
            )]),
            PlanOptions {
                uint64_policy: UInt64Policy::CheckedBigInt,
                ..PlanOptions::default()
            },
        );
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "unsigned_as_bigint",
                DataType::UInt64,
                true,
            )])),
            vec![Arc::new(UInt64Array::from(vec![
                Some(0_u64),
                Some(i64::MAX as u64),
                None,
            ]))],
        )
        .unwrap();
        let view = RecordBatchView::new(&batch, &mappings).unwrap();

        assert_eq!(
            view.mssql_cell(&mappings[0], 0).unwrap(),
            MssqlCell::BigInt(Some(0))
        );
        assert_eq!(
            view.mssql_cell(&mappings[0], 1).unwrap(),
            MssqlCell::BigInt(Some(i64::MAX))
        );
        assert_eq!(
            view.mssql_cell(&mappings[0], 2).unwrap(),
            MssqlCell::BigInt(None)
        );
    }

    #[test]
    fn rejects_uint64_checked_bigint_overflow_without_wrapping() {
        let mappings = mappings_for_schema_with_options(
            Schema::new(vec![Field::new(
                "unsigned_as_bigint",
                DataType::UInt64,
                false,
            )]),
            PlanOptions {
                uint64_policy: UInt64Policy::CheckedBigInt,
                ..PlanOptions::default()
            },
        );
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "unsigned_as_bigint",
                DataType::UInt64,
                false,
            )])),
            vec![Arc::new(UInt64Array::from(vec![
                (i64::MAX as u64) + 1,
                u64::MAX,
            ]))],
        )
        .unwrap();
        let view = RecordBatchView::new(&batch, &mappings).unwrap();

        let just_over = view.mssql_cell(&mappings[0], 0).unwrap_err();
        assert_single_diagnostic(
            just_over,
            DiagnosticCode::IntegerOutOfRange,
            Some(0),
            Some((0, "unsigned_as_bigint")),
        );

        let max = view.mssql_cell(&mappings[0], 1).unwrap_err();
        assert_single_diagnostic(
            max,
            DiagnosticCode::IntegerOutOfRange,
            Some(1),
            Some((0, "unsigned_as_bigint")),
        );
    }

    #[test]
    fn converts_decimal256_checked_downcast_values() {
        let mappings = mappings_for_schema(Schema::new(vec![Field::new(
            "amount",
            DataType::Decimal256(38, 4),
            true,
        )]));
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "amount",
                DataType::Decimal256(38, 4),
                true,
            )])),
            vec![Arc::new(
                Decimal256Array::from(vec![
                    Some(i256::from_i128(123_456_789_012_345_678_901_234_567_890)),
                    Some(i256::from_i128(-123_456_789_012_345_678_901_234_567_890)),
                    Some(i256::ZERO),
                    None,
                ])
                .with_precision_and_scale(38, 4)
                .unwrap(),
            )],
        )
        .unwrap();
        let view = RecordBatchView::new(&batch, &mappings).unwrap();

        assert_eq!(
            view.mssql_cell(&mappings[0], 0).unwrap(),
            MssqlCell::Decimal(Some(MssqlDecimal::new(
                123_456_789_012_345_678_901_234_567_890,
                4,
            )))
        );
        assert_eq!(
            view.mssql_cell(&mappings[0], 1).unwrap(),
            MssqlCell::Decimal(Some(MssqlDecimal::new(
                -123_456_789_012_345_678_901_234_567_890,
                4,
            )))
        );
        assert_eq!(
            view.mssql_cell(&mappings[0], 2).unwrap(),
            MssqlCell::Decimal(Some(MssqlDecimal::new(0, 4)))
        );
        assert_eq!(
            view.mssql_cell(&mappings[0], 3).unwrap(),
            MssqlCell::Decimal(None)
        );
    }

    #[test]
    fn rejects_decimal256_values_that_do_not_fit_i128_runtime_representation() {
        let mappings = mappings_for_schema(Schema::new(vec![Field::new(
            "amount",
            DataType::Decimal256(38, 0),
            false,
        )]));
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "amount",
                DataType::Decimal256(38, 0),
                false,
            )])),
            vec![Arc::new(
                Decimal256Array::from(vec![i256::from_i128(i128::MAX) + i256::ONE])
                    .with_precision_and_scale(38, 0)
                    .unwrap(),
            )],
        )
        .unwrap();
        let view = RecordBatchView::new(&batch, &mappings).unwrap();

        let err = view.mssql_cell(&mappings[0], 0).unwrap_err();

        assert_single_diagnostic(
            err,
            DiagnosticCode::DecimalOutOfRange,
            Some(0),
            Some((0, "amount")),
        );
    }

    #[test]
    fn rejects_decimal256_checked_downcast_values_outside_planned_precision() {
        let mappings = mappings_for_schema(Schema::new(vec![Field::new(
            "amount",
            DataType::Decimal256(5, 2),
            false,
        )]));
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "amount",
                DataType::Decimal256(5, 2),
                false,
            )])),
            vec![Arc::new(
                Decimal256Array::from(vec![i256::from_i128(100_000)])
                    .with_precision_and_scale(5, 2)
                    .unwrap(),
            )],
        )
        .unwrap();
        let view = RecordBatchView::new(&batch, &mappings).unwrap();

        let err = view.mssql_cell(&mappings[0], 0).unwrap_err();

        assert_single_diagnostic(
            err,
            DiagnosticCode::DecimalOutOfRange,
            Some(0),
            Some((0, "amount")),
        );
    }

    #[test]
    fn converts_date32_cells_to_mssql_date_with_boundaries_and_null() {
        let mappings = mappings_for_schema(Schema::new(vec![Field::new(
            "date_value",
            DataType::Date32,
            true,
        )]));
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "date_value",
                DataType::Date32,
                true,
            )])),
            vec![Arc::new(Date32Array::from(vec![
                Some(0_i32),
                Some(-1_i32),
                Some(1_i32),
                Some(-719_162_i32),
                Some(2_932_896_i32),
                None,
            ]))],
        )
        .unwrap();
        let view = RecordBatchView::new(&batch, &mappings).unwrap();

        assert_eq!(
            view.mssql_cell(&mappings[0], 0).unwrap(),
            MssqlCell::Date(Some(MssqlDate::new(719_162)))
        );
        assert_eq!(
            view.mssql_cell(&mappings[0], 1).unwrap(),
            MssqlCell::Date(Some(MssqlDate::new(719_161)))
        );
        assert_eq!(
            view.mssql_cell(&mappings[0], 2).unwrap(),
            MssqlCell::Date(Some(MssqlDate::new(719_163)))
        );
        assert_eq!(
            view.mssql_cell(&mappings[0], 3).unwrap(),
            MssqlCell::Date(Some(MssqlDate::new(0)))
        );
        assert_eq!(
            view.mssql_cell(&mappings[0], 4).unwrap(),
            MssqlCell::Date(Some(MssqlDate::new(3_652_058)))
        );
        assert_eq!(
            view.mssql_cell(&mappings[0], 5).unwrap(),
            MssqlCell::Date(None)
        );
    }

    #[test]
    fn rejects_date32_null_in_non_nullable_column() {
        let mappings = mappings_for_schema(Schema::new(vec![Field::new(
            "date_value",
            DataType::Date32,
            false,
        )]));
        let batch = unsafe_batch_for_field(
            "date_value",
            DataType::Date32,
            Arc::new(Date32Array::from(vec![None::<i32>])),
            false,
        );
        let view = RecordBatchView::new(&batch, &mappings).unwrap();

        let err = view.mssql_cell(&mappings[0], 0).unwrap_err();

        assert_single_diagnostic(
            err,
            DiagnosticCode::NullInNonNullableColumn,
            Some(0),
            Some((0, "date_value")),
        );
    }

    #[test]
    fn rejects_date32_values_outside_sql_server_date_range() {
        let mappings = mappings_for_schema(Schema::new(vec![Field::new(
            "date_value",
            DataType::Date32,
            false,
        )]));
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "date_value",
                DataType::Date32,
                false,
            )])),
            vec![Arc::new(Date32Array::from(vec![
                -719_163_i32,
                2_932_897_i32,
            ]))],
        )
        .unwrap();
        let view = RecordBatchView::new(&batch, &mappings).unwrap();

        let below = view.mssql_cell(&mappings[0], 0).unwrap_err();
        assert_single_diagnostic(
            below,
            DiagnosticCode::TimestampOutOfRange,
            Some(0),
            Some((0, "date_value")),
        );

        let above = view.mssql_cell(&mappings[0], 1).unwrap_err();
        assert_single_diagnostic(
            above,
            DiagnosticCode::TimestampOutOfRange,
            Some(1),
            Some((0, "date_value")),
        );
    }

    #[test]
    fn converts_date64_cells_to_mssql_datetime2_with_boundaries_and_null() {
        let mappings = mappings_for_schema_with_options(
            Schema::new(vec![Field::new("date_value", DataType::Date64, true)]),
            PlanOptions {
                date64_policy: Date64Policy::TimestampDateTime2,
                ..PlanOptions::default()
            },
        );
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "date_value",
                DataType::Date64,
                true,
            )])),
            vec![Arc::new(Date64Array::from(vec![
                Some(0_i64),
                Some(-1_i64),
                Some(86_400_123_i64),
                Some(-62_135_596_800_000_i64),
                Some(253_402_300_799_999_i64),
                None,
            ]))],
        )
        .unwrap();
        let view = RecordBatchView::new(&batch, &mappings).unwrap();

        assert_eq!(
            view.mssql_cell(&mappings[0], 0).unwrap(),
            MssqlCell::DateTime2(Some(MssqlDateTime2::new(
                MssqlDate::new(719_162),
                MssqlTime::new(0, 3),
            )))
        );
        assert_eq!(
            view.mssql_cell(&mappings[0], 1).unwrap(),
            MssqlCell::DateTime2(Some(MssqlDateTime2::new(
                MssqlDate::new(719_161),
                MssqlTime::new(86_399_999, 3),
            )))
        );
        assert_eq!(
            view.mssql_cell(&mappings[0], 2).unwrap(),
            MssqlCell::DateTime2(Some(MssqlDateTime2::new(
                MssqlDate::new(719_163),
                MssqlTime::new(123, 3),
            )))
        );
        assert_eq!(
            view.mssql_cell(&mappings[0], 3).unwrap(),
            MssqlCell::DateTime2(Some(MssqlDateTime2::new(
                MssqlDate::new(0),
                MssqlTime::new(0, 3),
            )))
        );
        assert_eq!(
            view.mssql_cell(&mappings[0], 4).unwrap(),
            MssqlCell::DateTime2(Some(MssqlDateTime2::new(
                MssqlDate::new(3_652_058),
                MssqlTime::new(86_399_999, 3),
            )))
        );
        assert_eq!(
            view.mssql_cell(&mappings[0], 5).unwrap(),
            MssqlCell::DateTime2(None)
        );
    }

    #[test]
    fn rejects_date64_null_in_non_nullable_column() {
        let mappings = mappings_for_schema_with_options(
            Schema::new(vec![Field::new("date_value", DataType::Date64, false)]),
            PlanOptions {
                date64_policy: Date64Policy::TimestampDateTime2,
                ..PlanOptions::default()
            },
        );
        let batch = unsafe_batch_for_field(
            "date_value",
            DataType::Date64,
            Arc::new(Date64Array::from(vec![None::<i64>])),
            false,
        );
        let view = RecordBatchView::new(&batch, &mappings).unwrap();

        let err = view.mssql_cell(&mappings[0], 0).unwrap_err();

        assert_single_diagnostic(
            err,
            DiagnosticCode::NullInNonNullableColumn,
            Some(0),
            Some((0, "date_value")),
        );
    }

    #[test]
    fn rejects_date64_values_outside_sql_server_datetime2_range() {
        let mappings = mappings_for_schema_with_options(
            Schema::new(vec![Field::new("date_value", DataType::Date64, false)]),
            PlanOptions {
                date64_policy: Date64Policy::TimestampDateTime2,
                ..PlanOptions::default()
            },
        );
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "date_value",
                DataType::Date64,
                false,
            )])),
            vec![Arc::new(Date64Array::from(vec![
                -62_135_596_800_001_i64,
                253_402_300_800_000_i64,
            ]))],
        )
        .unwrap();
        let view = RecordBatchView::new(&batch, &mappings).unwrap();

        let below = view.mssql_cell(&mappings[0], 0).unwrap_err();
        assert_single_diagnostic(
            below,
            DiagnosticCode::TimestampOutOfRange,
            Some(0),
            Some((0, "date_value")),
        );

        let above = view.mssql_cell(&mappings[0], 1).unwrap_err();
        assert_single_diagnostic(
            above,
            DiagnosticCode::TimestampOutOfRange,
            Some(1),
            Some((0, "date_value")),
        );
    }

    #[test]
    fn rejects_forged_date64_mapping_with_unsupported_datetime2_precision() {
        let mapping = SchemaMapping::new(
            ArrowFieldRef::new(0, "date_value".to_owned(), false, DataType::Date64),
            MssqlColumn::new(
                Identifier::new("date_value").unwrap(),
                MssqlType::DateTime2 { precision: 7 },
                false,
            ),
        );
        let mappings = vec![mapping];
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "date_value",
                DataType::Date64,
                false,
            )])),
            vec![Arc::new(Date64Array::from(vec![0_i64]))],
        )
        .unwrap();
        let view = RecordBatchView::new(&batch, &mappings).unwrap();

        let err = view.mssql_cell(&mappings[0], 0).unwrap_err();

        assert_single_diagnostic(
            err,
            DiagnosticCode::ValueTypeMismatch,
            Some(0),
            Some((0, "date_value")),
        );
    }

    #[test]
    fn converts_timezone_free_timestamp_cells_to_datetime2_7_with_boundaries_and_nulls() {
        let mappings = mappings_for_schema(Schema::new(vec![
            Field::new("ts_s", DataType::Timestamp(TimeUnit::Second, None), true),
            Field::new(
                "ts_ms",
                DataType::Timestamp(TimeUnit::Millisecond, None),
                true,
            ),
            Field::new(
                "ts_us",
                DataType::Timestamp(TimeUnit::Microsecond, None),
                true,
            ),
            Field::new(
                "ts_ns",
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                true,
            ),
        ]));
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("ts_s", DataType::Timestamp(TimeUnit::Second, None), true),
                Field::new(
                    "ts_ms",
                    DataType::Timestamp(TimeUnit::Millisecond, None),
                    true,
                ),
                Field::new(
                    "ts_us",
                    DataType::Timestamp(TimeUnit::Microsecond, None),
                    true,
                ),
                Field::new(
                    "ts_ns",
                    DataType::Timestamp(TimeUnit::Nanosecond, None),
                    true,
                ),
            ])),
            vec![
                Arc::new(TimestampSecondArray::from(vec![
                    Some(0_i64),
                    Some(-1_i64),
                    None,
                ])) as ArrayRef,
                Arc::new(TimestampMillisecondArray::from(vec![
                    Some(0_i64),
                    Some(-1_i64),
                    None,
                ])),
                Arc::new(TimestampMicrosecondArray::from(vec![
                    Some(1_234_567_i64),
                    Some(-1_i64),
                    None,
                ])),
                Arc::new(TimestampNanosecondArray::from(vec![
                    Some(123_456_700_i64),
                    Some(-100_i64),
                    None,
                ])),
            ],
        )
        .unwrap();
        let view = RecordBatchView::new(&batch, &mappings).unwrap();

        assert_eq!(
            view.mssql_cell(&mappings[0], 0).unwrap(),
            MssqlCell::DateTime2(Some(MssqlDateTime2::new(
                MssqlDate::new(719_162),
                MssqlTime::new(0, 7),
            )))
        );
        assert_eq!(
            view.mssql_cell(&mappings[0], 1).unwrap(),
            MssqlCell::DateTime2(Some(MssqlDateTime2::new(
                MssqlDate::new(719_161),
                MssqlTime::new(863_990_000_000, 7),
            )))
        );
        assert_eq!(
            view.mssql_cell(&mappings[0], 2).unwrap(),
            MssqlCell::DateTime2(None)
        );
        assert_eq!(
            view.mssql_cell(&mappings[1], 1).unwrap(),
            MssqlCell::DateTime2(Some(MssqlDateTime2::new(
                MssqlDate::new(719_161),
                MssqlTime::new(863_999_990_000, 7),
            )))
        );
        assert_eq!(
            view.mssql_cell(&mappings[2], 0).unwrap(),
            MssqlCell::DateTime2(Some(MssqlDateTime2::new(
                MssqlDate::new(719_162),
                MssqlTime::new(12_345_670, 7),
            )))
        );
        assert_eq!(
            view.mssql_cell(&mappings[2], 1).unwrap(),
            MssqlCell::DateTime2(Some(MssqlDateTime2::new(
                MssqlDate::new(719_161),
                MssqlTime::new(863_999_999_990, 7),
            )))
        );
        assert_eq!(
            view.mssql_cell(&mappings[3], 0).unwrap(),
            MssqlCell::DateTime2(Some(MssqlDateTime2::new(
                MssqlDate::new(719_162),
                MssqlTime::new(1_234_567, 7),
            )))
        );
        assert_eq!(
            view.mssql_cell(&mappings[3], 1).unwrap(),
            MssqlCell::DateTime2(Some(MssqlDateTime2::new(
                MssqlDate::new(719_161),
                MssqlTime::new(863_999_999_999, 7),
            )))
        );
    }

    #[test]
    fn converts_timezone_aware_timestamp_cells_to_normalized_utc_datetime2() {
        let options = PlanOptions {
            timezone_policy: TimezonePolicy::NormalizeUtcDateTime2,
            ..PlanOptions::default()
        };
        let schema = Schema::new(vec![
            Field::new(
                "new_york",
                DataType::Timestamp(TimeUnit::Second, Some("America/New_York".into())),
                true,
            ),
            Field::new(
                "offset",
                DataType::Timestamp(TimeUnit::Millisecond, Some("+02:30".into())),
                true,
            ),
            Field::new(
                "utc",
                DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
                true,
            ),
        ]);
        let mappings = mappings_for_schema_with_options(schema.clone(), options);
        let batch = RecordBatch::try_new(
            Arc::new(schema),
            vec![
                Arc::new(
                    TimestampSecondArray::from(vec![Some(0_i64), None])
                        .with_timezone("America/New_York"),
                ) as ArrayRef,
                Arc::new(
                    TimestampMillisecondArray::from(vec![Some(0_i64), None])
                        .with_timezone("+02:30"),
                ),
                Arc::new(
                    TimestampMicrosecondArray::from(vec![Some(1_234_567_i64), None])
                        .with_timezone("UTC"),
                ),
            ],
        )
        .unwrap();
        let view = RecordBatchView::new(&batch, &mappings).unwrap();

        assert_eq!(
            view.mssql_cell(&mappings[0], 0).unwrap(),
            MssqlCell::DateTime2(Some(MssqlDateTime2::new(
                MssqlDate::new(719_162),
                MssqlTime::new(0, 7),
            )))
        );
        assert_eq!(
            view.mssql_cell(&mappings[0], 1).unwrap(),
            MssqlCell::DateTime2(None)
        );
        assert_eq!(
            view.mssql_cell(&mappings[1], 0).unwrap(),
            MssqlCell::DateTime2(Some(MssqlDateTime2::new(
                MssqlDate::new(719_162),
                MssqlTime::new(0, 7),
            )))
        );
        assert_eq!(
            view.mssql_cell(&mappings[1], 1).unwrap(),
            MssqlCell::DateTime2(None)
        );
        assert_eq!(
            view.mssql_cell(&mappings[2], 0).unwrap(),
            MssqlCell::DateTime2(Some(MssqlDateTime2::new(
                MssqlDate::new(719_162),
                MssqlTime::new(12_345_670, 7),
            )))
        );
        assert_eq!(
            view.mssql_cell(&mappings[2], 1).unwrap(),
            MssqlCell::DateTime2(None)
        );
    }

    #[test]
    fn rejects_invalid_timezone_metadata_for_normalized_utc_datetime2() {
        let options = PlanOptions {
            timezone_policy: TimezonePolicy::NormalizeUtcDateTime2,
            ..PlanOptions::default()
        };
        let schema = Schema::new(vec![Field::new(
            "ts",
            DataType::Timestamp(TimeUnit::Second, Some("Foobar".into())),
            false,
        )]);
        let mappings = mappings_for_schema_with_options(schema.clone(), options);
        let batch = RecordBatch::try_new(
            Arc::new(schema),
            vec![Arc::new(
                TimestampSecondArray::from(vec![0_i64]).with_timezone("Foobar"),
            )],
        )
        .unwrap();
        let view = RecordBatchView::new(&batch, &mappings).unwrap();

        let err = view.mssql_cell(&mappings[0], 0).unwrap_err();

        assert_single_diagnostic(
            err,
            DiagnosticCode::TimezoneUnsupported,
            Some(0),
            Some((0, "ts")),
        );
    }

    #[test]
    fn applies_nanosecond_policy_to_timezone_aware_normalized_utc_datetime2() {
        let options = PlanOptions {
            timezone_policy: TimezonePolicy::NormalizeUtcDateTime2,
            nanosecond_policy: NanosecondPolicy::RoundTo100ns,
            ..PlanOptions::default()
        };
        let schema = Schema::new(vec![Field::new(
            "ts_ns",
            DataType::Timestamp(TimeUnit::Nanosecond, Some("America/New_York".into())),
            false,
        )]);
        let mappings = mappings_for_schema_with_options(schema.clone(), options);
        let batch = RecordBatch::try_new(
            Arc::new(schema),
            vec![Arc::new(
                TimestampNanosecondArray::from(vec![150_i64]).with_timezone("America/New_York"),
            )],
        )
        .unwrap();
        let view = RecordBatchView::new_with_options(&batch, &mappings, &options).unwrap();

        assert_eq!(
            view.mssql_cell(&mappings[0], 0).unwrap(),
            MssqlCell::DateTime2(Some(MssqlDateTime2::new(
                MssqlDate::new(719_162),
                MssqlTime::new(2, 7),
            )))
        );
    }

    #[test]
    fn converts_timezone_aware_timestamp_cells_to_datetimeoffset() {
        let options = PlanOptions {
            timezone_policy: TimezonePolicy::DateTimeOffset,
            ..PlanOptions::default()
        };
        let schema = Schema::new(vec![
            Field::new(
                "fixed_positive",
                DataType::Timestamp(TimeUnit::Millisecond, Some("+02:30".into())),
                true,
            ),
            Field::new(
                "fixed_negative",
                DataType::Timestamp(TimeUnit::Nanosecond, Some("-07".into())),
                true,
            ),
            Field::new(
                "utc",
                DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
                true,
            ),
        ]);
        let mappings = mappings_for_schema_with_options(schema.clone(), options);
        let batch = RecordBatch::try_new(
            Arc::new(schema),
            vec![
                Arc::new(
                    TimestampMillisecondArray::from(vec![Some(0_i64), None])
                        .with_timezone("+02:30"),
                ) as ArrayRef,
                Arc::new(
                    TimestampNanosecondArray::from(vec![Some(0_i64), None]).with_timezone("-07"),
                ),
                Arc::new(
                    TimestampMicrosecondArray::from(vec![Some(1_234_567_i64), None])
                        .with_timezone("UTC"),
                ),
            ],
        )
        .unwrap();
        let view = RecordBatchView::new(&batch, &mappings).unwrap();

        assert_eq!(
            view.mssql_cell(&mappings[0], 0).unwrap(),
            MssqlCell::DateTimeOffset(Some(MssqlDateTimeOffset::new(
                MssqlDateTime2::new(MssqlDate::new(719_162), MssqlTime::new(90_000_000_000, 7)),
                150,
            )))
        );
        assert_eq!(
            view.mssql_cell(&mappings[0], 1).unwrap(),
            MssqlCell::DateTimeOffset(None)
        );
        assert_eq!(
            view.mssql_cell(&mappings[1], 0).unwrap(),
            MssqlCell::DateTimeOffset(Some(MssqlDateTimeOffset::new(
                MssqlDateTime2::new(MssqlDate::new(719_161), MssqlTime::new(612_000_000_000, 7)),
                -420,
            )))
        );
        assert_eq!(
            view.mssql_cell(&mappings[1], 1).unwrap(),
            MssqlCell::DateTimeOffset(None)
        );
        assert_eq!(
            view.mssql_cell(&mappings[2], 0).unwrap(),
            MssqlCell::DateTimeOffset(Some(MssqlDateTimeOffset::new(
                MssqlDateTime2::new(MssqlDate::new(719_162), MssqlTime::new(12_345_670, 7)),
                0,
            )))
        );
    }

    #[test]
    fn resolves_named_timezone_datetimeoffset_per_timestamp_instant() {
        let options = PlanOptions {
            timezone_policy: TimezonePolicy::DateTimeOffset,
            ..PlanOptions::default()
        };
        let schema = Schema::new(vec![Field::new(
            "new_york",
            DataType::Timestamp(TimeUnit::Second, Some("America/New_York".into())),
            false,
        )]);
        let mappings = mappings_for_schema_with_options(schema.clone(), options);
        let batch = RecordBatch::try_new(
            Arc::new(schema),
            vec![Arc::new(
                TimestampSecondArray::from(vec![1_738_411_200_i64, 1_750_593_600_i64])
                    .with_timezone("America/New_York"),
            )],
        )
        .unwrap();
        let view = RecordBatchView::new(&batch, &mappings).unwrap();

        assert_eq!(
            view.mssql_cell(&mappings[0], 0).unwrap(),
            MssqlCell::DateTimeOffset(Some(MssqlDateTimeOffset::new(
                MssqlDateTime2::new(MssqlDate::new(739_282), MssqlTime::new(252_000_000_000, 7)),
                -300,
            )))
        );
        assert_eq!(
            view.mssql_cell(&mappings[0], 1).unwrap(),
            MssqlCell::DateTimeOffset(Some(MssqlDateTimeOffset::new(
                MssqlDateTime2::new(MssqlDate::new(739_423), MssqlTime::new(288_000_000_000, 7)),
                -240,
            )))
        );
    }

    #[test]
    fn rejects_invalid_timezone_metadata_for_datetimeoffset() {
        let options = PlanOptions {
            timezone_policy: TimezonePolicy::DateTimeOffset,
            ..PlanOptions::default()
        };
        let schema = Schema::new(vec![Field::new(
            "ts",
            DataType::Timestamp(TimeUnit::Second, Some("Foobar".into())),
            false,
        )]);
        let mappings = mappings_for_schema_with_options(schema.clone(), options);
        let batch = RecordBatch::try_new(
            Arc::new(schema),
            vec![Arc::new(
                TimestampSecondArray::from(vec![0_i64]).with_timezone("Foobar"),
            )],
        )
        .unwrap();
        let view = RecordBatchView::new(&batch, &mappings).unwrap();

        let err = view.mssql_cell(&mappings[0], 0).unwrap_err();

        assert_single_diagnostic(
            err,
            DiagnosticCode::TimezoneUnsupported,
            Some(0),
            Some((0, "ts")),
        );
    }

    #[test]
    fn applies_nanosecond_policy_to_datetimeoffset() {
        let options = PlanOptions {
            timezone_policy: TimezonePolicy::DateTimeOffset,
            nanosecond_policy: NanosecondPolicy::RoundTo100ns,
            ..PlanOptions::default()
        };
        let schema = Schema::new(vec![Field::new(
            "ts_ns",
            DataType::Timestamp(TimeUnit::Nanosecond, Some("+00:00".into())),
            false,
        )]);
        let mappings = mappings_for_schema_with_options(schema.clone(), options);
        let batch = RecordBatch::try_new(
            Arc::new(schema),
            vec![Arc::new(
                TimestampNanosecondArray::from(vec![150_i64]).with_timezone("+00:00"),
            )],
        )
        .unwrap();
        let view = RecordBatchView::new_with_options(&batch, &mappings, &options).unwrap();

        assert_eq!(
            view.mssql_cell(&mappings[0], 0).unwrap(),
            MssqlCell::DateTimeOffset(Some(MssqlDateTimeOffset::new(
                MssqlDateTime2::new(MssqlDate::new(719_162), MssqlTime::new(2, 7)),
                0,
            )))
        );
    }

    #[test]
    fn rejects_nanosecond_timestamp_precision_loss_by_default() {
        let mappings = mappings_for_schema(Schema::new(vec![Field::new(
            "ts_ns",
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            false,
        )]));
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "ts_ns",
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                false,
            )])),
            vec![Arc::new(TimestampNanosecondArray::from(vec![101_i64]))],
        )
        .unwrap();
        let view = RecordBatchView::new(&batch, &mappings).unwrap();

        let err = view.mssql_cell(&mappings[0], 0).unwrap_err();

        assert_single_diagnostic(
            err,
            DiagnosticCode::LossyConversionRequiresPolicy,
            Some(0),
            Some((0, "ts_ns")),
        );
    }

    #[test]
    fn applies_nanosecond_round_and_truncate_policies_at_runtime() {
        let options = PlanOptions {
            nanosecond_policy: NanosecondPolicy::RoundTo100ns,
            ..PlanOptions::default()
        };
        let mappings = mappings_for_schema_with_options(
            Schema::new(vec![Field::new(
                "ts_ns",
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                false,
            )]),
            options,
        );
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "ts_ns",
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                false,
            )])),
            vec![Arc::new(TimestampNanosecondArray::from(vec![
                149_i64, 150_i64, -149_i64,
            ]))],
        )
        .unwrap();
        let round_view = RecordBatchView::new_with_options(&batch, &mappings, &options).unwrap();
        let truncate_view = RecordBatchView::new_with_options(
            &batch,
            &mappings,
            &PlanOptions {
                nanosecond_policy: NanosecondPolicy::TruncateTo100ns,
                ..PlanOptions::default()
            },
        )
        .unwrap();

        assert_eq!(
            round_view.mssql_cell(&mappings[0], 0).unwrap(),
            MssqlCell::DateTime2(Some(MssqlDateTime2::new(
                MssqlDate::new(719_162),
                MssqlTime::new(1, 7),
            )))
        );
        assert_eq!(
            round_view.mssql_cell(&mappings[0], 1).unwrap(),
            MssqlCell::DateTime2(Some(MssqlDateTime2::new(
                MssqlDate::new(719_162),
                MssqlTime::new(2, 7),
            )))
        );
        assert_eq!(
            round_view.mssql_cell(&mappings[0], 2).unwrap(),
            MssqlCell::DateTime2(Some(MssqlDateTime2::new(
                MssqlDate::new(719_161),
                MssqlTime::new(863_999_999_999, 7),
            )))
        );
        assert_eq!(
            truncate_view.mssql_cell(&mappings[0], 0).unwrap(),
            MssqlCell::DateTime2(Some(MssqlDateTime2::new(
                MssqlDate::new(719_162),
                MssqlTime::new(1, 7),
            )))
        );
        assert_eq!(
            truncate_view.mssql_cell(&mappings[0], 1).unwrap(),
            MssqlCell::DateTime2(Some(MssqlDateTime2::new(
                MssqlDate::new(719_162),
                MssqlTime::new(1, 7),
            )))
        );
        assert_eq!(
            truncate_view.mssql_cell(&mappings[0], 2).unwrap(),
            MssqlCell::DateTime2(Some(MssqlDateTime2::new(
                MssqlDate::new(719_161),
                MssqlTime::new(863_999_999_998, 7),
            )))
        );
    }

    #[test]
    fn rejects_timestamp_values_outside_sql_server_datetime2_range() {
        let mappings = mappings_for_schema(Schema::new(vec![Field::new(
            "ts_s",
            DataType::Timestamp(TimeUnit::Second, None),
            false,
        )]));
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "ts_s",
                DataType::Timestamp(TimeUnit::Second, None),
                false,
            )])),
            vec![Arc::new(TimestampSecondArray::from(vec![
                i64::MIN,
                i64::MAX,
            ]))],
        )
        .unwrap();
        let view = RecordBatchView::new(&batch, &mappings).unwrap();

        let below = view.mssql_cell(&mappings[0], 0).unwrap_err();
        assert_single_diagnostic(
            below,
            DiagnosticCode::TimestampOutOfRange,
            Some(0),
            Some((0, "ts_s")),
        );

        let above = view.mssql_cell(&mappings[0], 1).unwrap_err();
        assert_single_diagnostic(
            above,
            DiagnosticCode::TimestampOutOfRange,
            Some(1),
            Some((0, "ts_s")),
        );
    }

    #[test]
    fn preserves_integer_boundaries_during_widening() {
        let mappings = mappings_for_schema(Schema::new(vec![
            Field::new("tiny", DataType::Int8, false),
            Field::new("small", DataType::Int16, false),
            Field::new("unsigned_tiny", DataType::UInt8, false),
            Field::new("unsigned_medium", DataType::UInt16, false),
            Field::new("unsigned_large", DataType::UInt32, false),
        ]));
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("tiny", DataType::Int8, false),
                Field::new("small", DataType::Int16, false),
                Field::new("unsigned_tiny", DataType::UInt8, false),
                Field::new("unsigned_medium", DataType::UInt16, false),
                Field::new("unsigned_large", DataType::UInt32, false),
            ])),
            vec![
                Arc::new(Int8Array::from(vec![i8::MIN, i8::MAX])) as ArrayRef,
                Arc::new(Int16Array::from(vec![i16::MIN, i16::MAX])),
                Arc::new(UInt8Array::from(vec![u8::MIN, u8::MAX])),
                Arc::new(UInt16Array::from(vec![u16::MIN, u16::MAX])),
                Arc::new(UInt32Array::from(vec![u32::MIN, u32::MAX])),
            ],
        )
        .unwrap();
        let view = RecordBatchView::new(&batch, &mappings).unwrap();

        assert_eq!(
            view.mssql_cell(&mappings[0], 0).unwrap(),
            MssqlCell::SmallInt(Some(i16::from(i8::MIN)))
        );
        assert_eq!(
            view.mssql_cell(&mappings[0], 1).unwrap(),
            MssqlCell::SmallInt(Some(i16::from(i8::MAX)))
        );
        assert_eq!(
            view.mssql_cell(&mappings[1], 0).unwrap(),
            MssqlCell::SmallInt(Some(i16::MIN))
        );
        assert_eq!(
            view.mssql_cell(&mappings[1], 1).unwrap(),
            MssqlCell::SmallInt(Some(i16::MAX))
        );
        assert_eq!(
            view.mssql_cell(&mappings[2], 0).unwrap(),
            MssqlCell::TinyInt(Some(u8::MIN))
        );
        assert_eq!(
            view.mssql_cell(&mappings[2], 1).unwrap(),
            MssqlCell::TinyInt(Some(u8::MAX))
        );
        assert_eq!(
            view.mssql_cell(&mappings[3], 0).unwrap(),
            MssqlCell::Int(Some(i32::from(u16::MIN)))
        );
        assert_eq!(
            view.mssql_cell(&mappings[3], 1).unwrap(),
            MssqlCell::Int(Some(i32::from(u16::MAX)))
        );
        assert_eq!(
            view.mssql_cell(&mappings[4], 0).unwrap(),
            MssqlCell::BigInt(Some(i64::from(u32::MIN)))
        );
        assert_eq!(
            view.mssql_cell(&mappings[4], 1).unwrap(),
            MssqlCell::BigInt(Some(i64::from(u32::MAX)))
        );
    }

    #[test]
    fn rejects_null_in_non_nullable_planned_column() {
        let mappings = mappings_for_schema(Schema::new(vec![Field::new(
            "active",
            DataType::Boolean,
            false,
        )]));
        let batch = unsafe_batch_for_field(
            "active",
            DataType::Boolean,
            Arc::new(BooleanArray::from(vec![None::<bool>])),
            false,
        );
        let view = RecordBatchView::new(&batch, &mappings).unwrap();

        let err = view.mssql_cell(&mappings[0], 0).unwrap_err();

        assert_single_diagnostic(
            err,
            DiagnosticCode::NullInNonNullableColumn,
            Some(0),
            Some((0, "active")),
        );
    }

    #[test]
    fn rejects_non_finite_float32_values() {
        let mappings = mappings_for_schema(Schema::new(vec![Field::new(
            "ratio",
            DataType::Float32,
            true,
        )]));
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "ratio",
                DataType::Float32,
                true,
            )])),
            vec![Arc::new(Float32Array::from(vec![
                f32::NAN,
                f32::INFINITY,
                f32::NEG_INFINITY,
            ]))],
        )
        .unwrap();
        let view = RecordBatchView::new(&batch, &mappings).unwrap();

        for row_index in 0..3 {
            let err = view.mssql_cell(&mappings[0], row_index).unwrap_err();

            assert_single_diagnostic(
                err,
                DiagnosticCode::NonFiniteFloat,
                Some(row_index),
                Some((0, "ratio")),
            );
        }
    }

    #[test]
    fn rejects_non_finite_float64_values() {
        let mappings = mappings_for_schema(Schema::new(vec![Field::new(
            "ratio",
            DataType::Float64,
            true,
        )]));
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "ratio",
                DataType::Float64,
                true,
            )])),
            vec![Arc::new(Float64Array::from(vec![
                f64::NAN,
                f64::INFINITY,
                f64::NEG_INFINITY,
            ]))],
        )
        .unwrap();
        let view = RecordBatchView::new(&batch, &mappings).unwrap();

        for row_index in 0..3 {
            let err = view.mssql_cell(&mappings[0], row_index).unwrap_err();

            assert_single_diagnostic(
                err,
                DiagnosticCode::NonFiniteFloat,
                Some(row_index),
                Some((0, "ratio")),
            );
        }
    }

    #[test]
    fn rejects_payload_that_does_not_fit_planned_mssql_type() {
        let mappings = vec![SchemaMapping::new(
            ArrowFieldRef::new(0, "id".to_owned(), false, DataType::Int32),
            MssqlColumn::new(Identifier::new("id").unwrap(), MssqlType::BigInt, false),
        )];
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)])),
            vec![Arc::new(Int32Array::from(vec![7_i32]))],
        )
        .unwrap();
        let view = RecordBatchView::new(&batch, &mappings).unwrap();

        let err = view.mssql_cell(&mappings[0], 0).unwrap_err();

        assert_single_diagnostic(
            err,
            DiagnosticCode::ValueTypeMismatch,
            Some(0),
            Some((0, "id")),
        );
    }

    #[test]
    fn rejects_decimal_mapping_scale_mismatch_before_value_corruption() {
        let mappings = vec![SchemaMapping::new(
            ArrowFieldRef::new(0, "amount".to_owned(), false, DataType::Decimal128(5, 2)),
            MssqlColumn::new(
                Identifier::new("amount").unwrap(),
                MssqlType::Decimal {
                    precision: 5,
                    scale: 0,
                },
                false,
            ),
        )];
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "amount",
                DataType::Decimal128(5, 2),
                false,
            )])),
            vec![Arc::new(
                Decimal128Array::from(vec![123_i128])
                    .with_precision_and_scale(5, 2)
                    .unwrap(),
            )],
        )
        .unwrap();
        let view = RecordBatchView::new(&batch, &mappings).unwrap();

        let err = view.mssql_cell(&mappings[0], 0).unwrap_err();

        assert_single_diagnostic(
            err,
            DiagnosticCode::SchemaMismatch,
            Some(0),
            Some((0, "amount")),
        );
    }

    #[test]
    fn rejects_planned_column_index_outside_runtime_batch() {
        let mappings = mappings_for_schema(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("active", DataType::Boolean, true),
        ]));
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)])),
            vec![Arc::new(Int32Array::from(vec![1_i32]))],
        )
        .unwrap();

        let err = RecordBatchView::new(&batch, &mappings).unwrap_err();

        assert_single_diagnostic(
            err,
            DiagnosticCode::SchemaMismatch,
            None,
            Some((1, "active")),
        );
    }

    #[test]
    fn rejects_extra_runtime_columns_without_mappings() {
        let mappings =
            mappings_for_schema(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("id", DataType::Int32, false),
                Field::new("extra", DataType::Boolean, true),
            ])),
            vec![
                Arc::new(Int32Array::from(vec![1_i32])) as ArrayRef,
                Arc::new(BooleanArray::from(vec![Some(true)])),
            ],
        )
        .unwrap();

        let err = RecordBatchView::new(&batch, &mappings).unwrap_err();

        assert_single_diagnostic(err, DiagnosticCode::SchemaMismatch, None, None);
    }

    #[test]
    fn rejects_mapping_position_that_disagrees_with_arrow_index() {
        let mappings = vec![SchemaMapping::new(
            ArrowFieldRef::new(1, "id".to_owned(), false, DataType::Int32),
            MssqlColumn::new(Identifier::new("id").unwrap(), MssqlType::Int, false),
        )];
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)])),
            vec![Arc::new(Int32Array::from(vec![1_i32]))],
        )
        .unwrap();

        let err = RecordBatchView::new(&batch, &mappings).unwrap_err();

        assert_single_diagnostic(err, DiagnosticCode::SchemaMismatch, None, Some((1, "id")));
    }

    #[test]
    fn rejects_runtime_field_name_mismatch_even_when_type_matches() {
        let mappings = mappings_for_schema(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("amount", DataType::Int32, false),
        ]));
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("amount", DataType::Int32, false),
                Field::new("id", DataType::Int32, false),
            ])),
            vec![
                Arc::new(Int32Array::from(vec![100_i32])) as ArrayRef,
                Arc::new(Int32Array::from(vec![1_i32])),
            ],
        )
        .unwrap();

        let err = RecordBatchView::new(&batch, &mappings).unwrap_err();

        assert_single_diagnostic(err, DiagnosticCode::SchemaMismatch, None, Some((0, "id")));
    }

    #[test]
    fn rejects_runtime_field_rename_even_when_position_and_type_match() {
        let mappings =
            mappings_for_schema(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "renamed_id",
                DataType::Int32,
                false,
            )])),
            vec![Arc::new(Int32Array::from(vec![1_i32]))],
        )
        .unwrap();

        let err = RecordBatchView::new(&batch, &mappings).unwrap_err();

        assert_single_diagnostic(err, DiagnosticCode::SchemaMismatch, None, Some((0, "id")));
    }

    #[test]
    fn rejects_runtime_arrow_type_mismatch() {
        let mappings = mappings_for_schema(Schema::new(vec![Field::new(
            "number",
            DataType::Int32,
            true,
        )]));
        let batch = unsafe_batch_for_field(
            "number",
            DataType::Int32,
            Arc::new(Int64Array::from(vec![1_i64])),
            true,
        );

        let err = RecordBatchView::new(&batch, &mappings).unwrap_err();

        assert_single_diagnostic(
            err,
            DiagnosticCode::SchemaMismatch,
            None,
            Some((0, "number")),
        );
    }

    #[test]
    fn rejects_row_index_out_of_bounds() {
        let mappings = mappings_for_schema(Schema::new(vec![Field::new(
            "number",
            DataType::Int32,
            true,
        )]));
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "number",
                DataType::Int32,
                true,
            )])),
            vec![Arc::new(Int32Array::from(vec![1_i32]))],
        )
        .unwrap();
        let view = RecordBatchView::new(&batch, &mappings).unwrap();

        let err = view.check_row_index(1).unwrap_err();

        assert_single_diagnostic(err, DiagnosticCode::RowIndexOutOfBounds, Some(1), None);
    }

    fn mappings_for_schema(schema: Schema) -> Vec<SchemaMapping> {
        mappings_for_schema_with_options(schema, PlanOptions::default())
    }

    fn mappings_for_schema_with_options(
        schema: Schema,
        options: PlanOptions,
    ) -> Vec<SchemaMapping> {
        plan_arrow_schema_to_mssql_mappings(
            Arc::new(schema),
            MssqlProfile::sql_server_2016_compat_100(),
            options,
        )
        .unwrap()
        .into_parts()
        .0
    }

    fn timezone_timestamp_mapping(
        timezone: &str,
        timezone_policy: TimezonePolicy,
    ) -> SchemaMapping {
        mappings_for_schema_with_options(
            Schema::new(vec![Field::new(
                "ts",
                DataType::Timestamp(TimeUnit::Second, Some(timezone.into())),
                true,
            )]),
            PlanOptions {
                timezone_policy,
                ..PlanOptions::default()
            },
        )
        .remove(0)
    }

    fn assert_policy_planned_null_runtime_unsupported(
        name: &str,
        data_type: DataType,
        options: PlanOptions,
    ) {
        let mappings = mappings_for_schema_with_options(
            Schema::new(vec![Field::new(name, data_type.clone(), true)]),
            options,
        );
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(name, data_type.clone(), true)])),
            vec![new_null_array(&data_type, 1)],
        )
        .unwrap();
        let view = RecordBatchView::new(&batch, &mappings).unwrap();

        let err = view.mssql_cell(&mappings[0], 0).unwrap_err();

        assert_single_diagnostic(
            err,
            DiagnosticCode::ValueConversionUnsupported,
            Some(0),
            Some((0, name)),
        );
    }

    fn unsafe_batch_for_field(
        name: &str,
        data_type: DataType,
        array: ArrayRef,
        nullable: bool,
    ) -> RecordBatch {
        // SAFETY: this deliberately constructs a mismatched batch for converter
        // validation tests. The test only inspects metadata and never reads the
        // mismatched array through the declared schema type.
        unsafe {
            RecordBatch::new_unchecked(
                Arc::new(Schema::new(vec![Field::new(name, data_type, nullable)])),
                vec![array],
                1,
            )
        }
    }

    fn malicious_decimal128_array(data_type: DataType, values: &[i128]) -> ArrayRef {
        let data = ArrayData::builder(data_type)
            .len(values.len())
            .add_buffer(values.to_vec().into())
            .build()
            .unwrap();

        Arc::new(Decimal128Array::from(data))
    }

    fn assert_single_diagnostic(
        err: Error,
        expected_code: DiagnosticCode,
        expected_row: Option<usize>,
        expected_field: Option<(usize, &str)>,
    ) {
        let Error::ValueConversion { diagnostics } = err else {
            panic!("expected value conversion error");
        };

        assert_eq!(diagnostics.len(), 1);
        let diagnostic = &diagnostics.all()[0];
        assert_eq!(diagnostic.code(), expected_code);
        assert_eq!(diagnostic.row(), expected_row);
        assert_eq!(
            diagnostic
                .field()
                .map(|field| (field.index(), field.name())),
            expected_field
        );
    }
}

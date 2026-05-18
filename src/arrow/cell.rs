//! Arrow runtime cell values.

use arrow_buffer::i256;

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

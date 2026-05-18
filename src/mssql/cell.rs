//! MSSQL runtime cell values.

/// Arrow-to-MSSQL runtime cell conversion.
pub(crate) mod from_arrow;

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
    pub(crate) const fn new(unscaled: i128, scale: u8) -> Self {
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
}

/// Semantic SQL Server date value.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct MssqlDate {
    days: u32,
}

impl MssqlDate {
    /// Creates a semantic date value from SQL Server's day count.
    pub(crate) const fn new(days: u32) -> Self {
        Self { days }
    }

    /// Returns the number of days from 0001-01-01.
    pub(crate) const fn days(self) -> u32 {
        self.days
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
    pub(crate) const fn new(date: MssqlDate, time: MssqlTime) -> Self {
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
}

/// Semantic SQL Server datetimeoffset value.
///
/// TDS encodes `datetimeoffset` as a UTC `datetime2` component plus an offset.
/// SQL Server displays that instant as local time by applying the offset.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct MssqlDateTimeOffset {
    datetime2: MssqlDateTime2,
    offset_minutes: i16,
}

impl MssqlDateTimeOffset {
    /// Creates a semantic datetimeoffset value from UTC date/time and offset.
    ///
    /// The offset is expressed as minutes from UTC, matching SQL Server and
    /// Tiberius `datetimeoffset` encoding.
    pub(crate) const fn new(datetime2: MssqlDateTime2, offset_minutes: i16) -> Self {
        Self {
            datetime2,
            offset_minutes,
        }
    }

    /// Returns the UTC date/time component used by TDS encoding.
    pub(crate) const fn datetime2(self) -> MssqlDateTime2 {
        self.datetime2
    }

    /// Returns the number of minutes from UTC.
    pub(crate) const fn offset_minutes(self) -> i16 {
        self.offset_minutes
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
    pub(crate) const fn new(increments: u64, scale: u8) -> Self {
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
}

#[cfg(test)]
mod tests {
    use super::{MssqlDate, MssqlDateTime2, MssqlDateTimeOffset, MssqlTime};

    #[test]
    fn datetimeoffset_exposes_datetime_and_offset_components() {
        let datetime2 = MssqlDateTime2::new(MssqlDate::new(719_163), MssqlTime::new(1, 7));
        let datetimeoffset = MssqlDateTimeOffset::new(datetime2, -840);

        assert_eq!(datetimeoffset.datetime2(), datetime2);
        assert_eq!(datetimeoffset.offset_minutes(), -840);
    }
}

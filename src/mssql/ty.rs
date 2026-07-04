//! SQL Server type model used by write planning.

/// SQL Server variable-length type length.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum MssqlTypeLength {
    /// Bounded length.
    Bounded(usize),
    /// SQL Server `max` length.
    Max,
}

/// SQL Server `time(p)` precision.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct MssqlTimePrecision(u8);

impl MssqlTimePrecision {
    /// SQL Server `time(0)` precision.
    pub const ZERO: Self = Self(0);
    /// SQL Server `time(3)` precision.
    pub const THREE: Self = Self(3);
    /// SQL Server `time(6)` precision.
    pub const SIX: Self = Self(6);
    /// SQL Server `time(7)` precision.
    pub const SEVEN: Self = Self(7);

    /// Creates a SQL Server `time(p)` precision when `p` is valid.
    pub const fn new(precision: u8) -> Option<Self> {
        if precision <= 7 {
            Some(Self(precision))
        } else {
            None
        }
    }

    /// Returns the raw SQL Server precision value.
    pub const fn get(self) -> u8 {
        self.0
    }
}

impl MssqlTypeLength {
    fn render(self) -> String {
        match self {
            Self::Bounded(length) => length.to_string(),
            Self::Max => "max".to_owned(),
        }
    }
}

/// SQL Server target type for a planned column.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum MssqlType {
    /// SQL Server `bit`.
    Bit,
    /// SQL Server `tinyint`.
    TinyInt,
    /// SQL Server `smallint`.
    SmallInt,
    /// SQL Server `int`.
    Int,
    /// SQL Server `bigint`.
    BigInt,
    /// SQL Server `real`.
    Real,
    /// SQL Server `float(n)`.
    Float {
        /// SQL Server floating-point precision.
        precision: u8,
    },
    /// SQL Server `nvarchar(n|max)`.
    NVarChar(MssqlTypeLength),
    /// SQL Server `varbinary(n|max)`.
    VarBinary(MssqlTypeLength),
    /// SQL Server `binary(n)`.
    Binary(usize),
    /// SQL Server `decimal(p,s)`.
    Decimal {
        /// SQL Server decimal precision.
        precision: u8,
        /// SQL Server decimal scale.
        scale: i8,
    },
    /// SQL Server `date`.
    Date,
    /// SQL Server `time(p)`.
    Time(MssqlTimePrecision),
    /// SQL Server `datetime`.
    DateTime,
    /// SQL Server `datetime2(p)`.
    DateTime2 {
        /// Fractional seconds precision.
        precision: u8,
    },
    /// SQL Server `datetimeoffset(p)`.
    DateTimeOffset {
        /// Fractional seconds precision.
        precision: u8,
    },
}

impl MssqlType {
    /// Renders this type as deterministic SQL.
    pub fn to_sql(&self) -> String {
        match self {
            Self::Bit => "bit".to_owned(),
            Self::TinyInt => "tinyint".to_owned(),
            Self::SmallInt => "smallint".to_owned(),
            Self::Int => "int".to_owned(),
            Self::BigInt => "bigint".to_owned(),
            Self::Real => "real".to_owned(),
            Self::Float { precision } => format!("float({precision})"),
            Self::NVarChar(length) => format!("nvarchar({})", length.render()),
            Self::VarBinary(length) => format!("varbinary({})", length.render()),
            Self::Binary(length) => format!("binary({length})"),
            Self::Decimal { precision, scale } => format!("decimal({precision},{scale})"),
            Self::Date => "date".to_owned(),
            Self::Time(precision) => format!("time({})", precision.get()),
            Self::DateTime => "datetime".to_owned(),
            Self::DateTime2 { precision } => format!("datetime2({precision})"),
            Self::DateTimeOffset { precision } => format!("datetimeoffset({precision})"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{MssqlTimePrecision, MssqlType, MssqlTypeLength};

    #[test]
    fn renders_primitive_types() {
        assert_eq!(MssqlType::Bit.to_sql(), "bit");
        assert_eq!(MssqlType::TinyInt.to_sql(), "tinyint");
        assert_eq!(MssqlType::SmallInt.to_sql(), "smallint");
        assert_eq!(MssqlType::Int.to_sql(), "int");
        assert_eq!(MssqlType::BigInt.to_sql(), "bigint");
        assert_eq!(MssqlType::Real.to_sql(), "real");
        assert_eq!(MssqlType::Float { precision: 53 }.to_sql(), "float(53)");
    }

    #[test]
    fn renders_variable_length_types() {
        assert_eq!(
            MssqlType::NVarChar(MssqlTypeLength::Max).to_sql(),
            "nvarchar(max)"
        );
        assert_eq!(
            MssqlType::NVarChar(MssqlTypeLength::Bounded(128)).to_sql(),
            "nvarchar(128)"
        );
        assert_eq!(
            MssqlType::VarBinary(MssqlTypeLength::Max).to_sql(),
            "varbinary(max)"
        );
        assert_eq!(
            MssqlType::VarBinary(MssqlTypeLength::Bounded(8000)).to_sql(),
            "varbinary(8000)"
        );
        assert_eq!(MssqlType::Binary(16).to_sql(), "binary(16)");
    }

    #[test]
    fn renders_decimal_and_temporal_types() {
        assert_eq!(
            MssqlType::Decimal {
                precision: 38,
                scale: 9
            }
            .to_sql(),
            "decimal(38,9)"
        );
        assert_eq!(MssqlType::Date.to_sql(), "date");
        assert_eq!(
            MssqlType::Time(MssqlTimePrecision::ZERO).to_sql(),
            "time(0)"
        );
        assert_eq!(
            MssqlType::Time(MssqlTimePrecision::THREE).to_sql(),
            "time(3)"
        );
        assert_eq!(MssqlType::Time(MssqlTimePrecision::SIX).to_sql(), "time(6)");
        assert_eq!(
            MssqlType::Time(MssqlTimePrecision::SEVEN).to_sql(),
            "time(7)"
        );
        assert_eq!(
            MssqlType::Time(MssqlTimePrecision::new(1).unwrap()).to_sql(),
            "time(1)"
        );
        assert_eq!(
            MssqlType::Time(MssqlTimePrecision::new(2).unwrap()).to_sql(),
            "time(2)"
        );
        assert_eq!(
            MssqlType::Time(MssqlTimePrecision::new(4).unwrap()).to_sql(),
            "time(4)"
        );
        assert_eq!(
            MssqlType::Time(MssqlTimePrecision::new(5).unwrap()).to_sql(),
            "time(5)"
        );
        assert_eq!(MssqlType::DateTime.to_sql(), "datetime");
        assert_eq!(
            MssqlType::DateTime2 { precision: 7 }.to_sql(),
            "datetime2(7)"
        );
        assert_eq!(
            MssqlType::DateTimeOffset { precision: 7 }.to_sql(),
            "datetimeoffset(7)"
        );
    }

    #[test]
    fn rejects_invalid_time_precision() {
        for precision in 0..=7 {
            assert_eq!(MssqlTimePrecision::new(precision).unwrap().get(), precision);
        }
        assert_eq!(MssqlTimePrecision::new(8), None);
        assert_eq!(MssqlTimePrecision::new(u8::MAX), None);
    }
}

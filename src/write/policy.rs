//! Write-path options and conversion policies.

/// Planning options for Arrow-to-SQL Server conversion.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash)]
pub struct PlanOptions {
    /// SQL Server text target policy.
    pub string_policy: StringPolicy,
    /// SQL Server binary target policy.
    pub binary_policy: BinaryPolicy,
    /// Timezone-aware timestamp policy.
    pub timezone_policy: TimezonePolicy,
    /// SQL Server timezone-free timestamp target policy.
    pub timestamp_policy: TimestampPolicy,
    /// Nanosecond timestamp precision policy.
    pub nanosecond_policy: NanosecondPolicy,
    /// Unsigned 64-bit integer policy.
    pub uint64_policy: UInt64Policy,
    /// Decimal policy shared by decimal widths.
    pub decimal_policy: DecimalPolicy,
    /// Decimal256-specific policy.
    pub decimal256_policy: Decimal256Policy,
    /// Floating-point policy.
    pub float_policy: FloatPolicy,
    /// Date64-specific policy.
    pub date64_policy: Date64Policy,
}

/// String conversion policy.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub enum StringPolicy {
    /// Use `nvarchar(max)`.
    #[default]
    NVarCharMax,
    /// Use bounded `nvarchar(n)`.
    NVarChar(usize),
    /// Infer bounded `nvarchar(n)` from observed values.
    ObservedNVarChar,
}

/// Binary conversion policy.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub enum BinaryPolicy {
    /// Use `varbinary(max)`.
    #[default]
    VarBinaryMax,
    /// Use bounded `varbinary(n)`.
    VarBinary(usize),
    /// Infer bounded `varbinary(n)` from observed values.
    ObservedVarBinary,
}

/// Timezone-free timestamp target policy.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum TimestampPolicy {
    /// Use SQL Server `datetime2(p)`.
    DateTime2 {
        /// Fractional seconds precision.
        precision: u8,
    },
    /// Use SQL Server legacy `datetime`.
    DateTime,
}

impl Default for TimestampPolicy {
    fn default() -> Self {
        Self::DateTime2 { precision: 7 }
    }
}

/// Timezone-aware timestamp conversion policy.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub enum TimezonePolicy {
    /// Reject timezone-aware timestamps.
    #[default]
    Reject,
    /// Target SQL Server `datetimeoffset`.
    DateTimeOffset,
    /// Normalize to UTC and target timezone-free `datetime2`.
    NormalizeUtcDateTime2,
}

/// Nanosecond timestamp precision policy.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub enum NanosecondPolicy {
    /// Reject nanosecond timestamps not divisible by 100.
    #[default]
    RejectNon100ns,
    /// Round to SQL Server 100ns precision.
    RoundTo100ns,
    /// Truncate to SQL Server 100ns precision.
    TruncateTo100ns,
}

/// Unsigned 64-bit integer conversion policy.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub enum UInt64Policy {
    /// Reject `UInt64` columns.
    #[default]
    Reject,
    /// Target SQL Server `decimal(20,0)`.
    Decimal20_0,
    /// Target `bigint` after checking values fit signed 64-bit range.
    CheckedBigInt,
}

/// Decimal conversion policy.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub enum DecimalPolicy {
    /// Reject Arrow decimals with negative scale.
    #[default]
    RejectNegativeScale,
    /// Normalize Arrow decimals with negative scale.
    NormalizeNegativeScale,
}

/// Decimal256 conversion policy.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub enum Decimal256Policy {
    /// Checked downcast when precision, scale, and value fit SQL Server decimal.
    #[default]
    CheckedDowncast,
    /// Reject all `Decimal256` columns.
    Reject,
}

/// Floating-point conversion policy.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub enum FloatPolicy {
    /// Reject NaN and infinity values.
    #[default]
    RejectNonFinite,
}

/// Date64 conversion policy.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub enum Date64Policy {
    /// Reject `Date64` values that are not midnight dates.
    #[default]
    RejectNonMidnight,
    /// Remap `Date64` to SQL Server `datetime2`.
    TimestampDateTime2,
}

/// Write-time batch schema compatibility policy.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub enum SchemaCheck {
    /// Require exact schema equality with the planned schema.
    #[default]
    Strict,
}

#[cfg(test)]
mod tests {
    use super::{
        BinaryPolicy, Date64Policy, Decimal256Policy, DecimalPolicy, FloatPolicy, NanosecondPolicy,
        PlanOptions, SchemaCheck, StringPolicy, TimestampPolicy, TimezonePolicy, UInt64Policy,
    };

    #[test]
    fn defaults_match_v0_1_policy_decisions() {
        let options = PlanOptions::default();

        assert_eq!(options.string_policy, StringPolicy::NVarCharMax);
        assert_eq!(options.binary_policy, BinaryPolicy::VarBinaryMax);
        assert_eq!(options.timezone_policy, TimezonePolicy::Reject);
        assert_eq!(
            options.timestamp_policy,
            TimestampPolicy::DateTime2 { precision: 7 }
        );
        assert_eq!(options.nanosecond_policy, NanosecondPolicy::RejectNon100ns);
        assert_eq!(options.uint64_policy, UInt64Policy::Reject);
        assert_eq!(options.decimal_policy, DecimalPolicy::RejectNegativeScale);
        assert_eq!(options.decimal256_policy, Decimal256Policy::CheckedDowncast);
        assert_eq!(options.float_policy, FloatPolicy::RejectNonFinite);
        assert_eq!(options.date64_policy, Date64Policy::RejectNonMidnight);
    }

    #[test]
    fn individual_policy_defaults_match_plan_options() {
        assert_eq!(StringPolicy::default(), StringPolicy::NVarCharMax);
        assert_eq!(BinaryPolicy::default(), BinaryPolicy::VarBinaryMax);
        assert_eq!(TimezonePolicy::default(), TimezonePolicy::Reject);
        assert_eq!(
            TimestampPolicy::default(),
            TimestampPolicy::DateTime2 { precision: 7 }
        );
        assert_eq!(
            NanosecondPolicy::default(),
            NanosecondPolicy::RejectNon100ns
        );
        assert_eq!(UInt64Policy::default(), UInt64Policy::Reject);
        assert_eq!(DecimalPolicy::default(), DecimalPolicy::RejectNegativeScale);
        assert_eq!(
            Decimal256Policy::default(),
            Decimal256Policy::CheckedDowncast
        );
        assert_eq!(FloatPolicy::default(), FloatPolicy::RejectNonFinite);
        assert_eq!(Date64Policy::default(), Date64Policy::RejectNonMidnight);
        assert_eq!(SchemaCheck::default(), SchemaCheck::Strict);
    }

    #[test]
    fn supports_explicit_non_default_policy_overrides() {
        let options = PlanOptions {
            string_policy: StringPolicy::NVarChar(128),
            binary_policy: BinaryPolicy::VarBinary(256),
            timezone_policy: TimezonePolicy::DateTimeOffset,
            timestamp_policy: TimestampPolicy::DateTime,
            nanosecond_policy: NanosecondPolicy::RoundTo100ns,
            uint64_policy: UInt64Policy::Decimal20_0,
            decimal_policy: DecimalPolicy::NormalizeNegativeScale,
            decimal256_policy: Decimal256Policy::Reject,
            float_policy: FloatPolicy::RejectNonFinite,
            date64_policy: Date64Policy::TimestampDateTime2,
        };

        assert_eq!(options.string_policy, StringPolicy::NVarChar(128));
        assert_eq!(options.binary_policy, BinaryPolicy::VarBinary(256));
        assert_eq!(options.timezone_policy, TimezonePolicy::DateTimeOffset);
        assert_eq!(options.timestamp_policy, TimestampPolicy::DateTime);
        assert_eq!(options.nanosecond_policy, NanosecondPolicy::RoundTo100ns);
        assert_eq!(options.uint64_policy, UInt64Policy::Decimal20_0);
        assert_eq!(
            options.decimal_policy,
            DecimalPolicy::NormalizeNegativeScale
        );
        assert_eq!(options.decimal256_policy, Decimal256Policy::Reject);
        assert_eq!(options.float_policy, FloatPolicy::RejectNonFinite);
        assert_eq!(options.date64_policy, Date64Policy::TimestampDateTime2);
    }
}

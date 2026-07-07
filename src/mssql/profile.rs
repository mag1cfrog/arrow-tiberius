//! SQL Server profile types.

use arrow_schema::Schema;

use crate::schema::{PlannedSchema, plan_arrow_schema_to_mssql_schema};
use crate::write::PlanOptions;
use crate::{PlanOutcome, Result};

/// SQL Server engine version targeted by planning.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum MssqlVersion {
    /// SQL Server 2016.
    SqlServer2016,
    /// SQL Server 2017.
    SqlServer2017,
    /// SQL Server 2019.
    SqlServer2019,
    /// SQL Server 2022.
    SqlServer2022,
    /// SQL Server 2025.
    SqlServer2025,
}

impl MssqlVersion {
    /// Returns the default database compatibility level for this SQL Server
    /// engine version.
    pub const fn default_compatibility_level(self) -> CompatibilityLevel {
        match self {
            Self::SqlServer2016 => CompatibilityLevel::SQL_SERVER_2016,
            Self::SqlServer2017 => CompatibilityLevel::SQL_SERVER_2017,
            Self::SqlServer2019 => CompatibilityLevel::SQL_SERVER_2019,
            Self::SqlServer2022 => CompatibilityLevel::SQL_SERVER_2022,
            Self::SqlServer2025 => CompatibilityLevel::SQL_SERVER_2025,
        }
    }

    /// Returns whether this SQL Server engine version supports the database
    /// compatibility level.
    pub const fn supports_compatibility_level(
        self,
        compatibility_level: CompatibilityLevel,
    ) -> bool {
        match self {
            Self::SqlServer2016 => compatibility_level.as_u16() <= 130,
            Self::SqlServer2017 => compatibility_level.as_u16() <= 140,
            Self::SqlServer2019 => compatibility_level.as_u16() <= 150,
            Self::SqlServer2022 => compatibility_level.as_u16() <= 160,
            Self::SqlServer2025 => compatibility_level.as_u16() <= 170,
        }
    }
}

/// SQL Server database compatibility level.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct CompatibilityLevel(u16);

impl CompatibilityLevel {
    /// SQL Server 2008 / 2008 R2 compatibility level.
    pub const SQL_SERVER_2008: Self = Self(100);
    /// SQL Server 2012 compatibility level.
    pub const SQL_SERVER_2012: Self = Self(110);
    /// SQL Server 2014 compatibility level.
    pub const SQL_SERVER_2014: Self = Self(120);
    /// SQL Server 2016 compatibility level.
    pub const SQL_SERVER_2016: Self = Self(130);
    /// SQL Server 2017 compatibility level.
    pub const SQL_SERVER_2017: Self = Self(140);
    /// SQL Server 2019 compatibility level.
    pub const SQL_SERVER_2019: Self = Self(150);
    /// SQL Server 2022 compatibility level.
    pub const SQL_SERVER_2022: Self = Self(160);
    /// SQL Server 2025 compatibility level.
    pub const SQL_SERVER_2025: Self = Self(170);

    /// Creates a validated compatibility level.
    pub fn new(level: u16) -> Result<Self> {
        if !Self::is_supported(level) {
            return Err(crate::Error::InvalidCompatibilityLevel { level });
        }

        Ok(Self(level))
    }

    /// Returns the numeric compatibility level.
    pub const fn as_u16(self) -> u16 {
        self.0
    }

    const fn is_supported(level: u16) -> bool {
        matches!(level, 100 | 110 | 120 | 130 | 140 | 150 | 160 | 170)
    }
}

impl TryFrom<u16> for CompatibilityLevel {
    type Error = crate::Error;

    fn try_from(value: u16) -> Result<Self> {
        Self::new(value)
    }
}

/// SQL Server planning profile.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct MssqlProfile {
    version: MssqlVersion,
    compatibility_level: CompatibilityLevel,
}

/// Profile-selected strategy for converting Arrow timestamps to SQL Server
/// `datetime` fragments.
///
/// SQL Server can round `datetime` casts differently by database
/// compatibility level. Writers should ask the profile for this semantic
/// behavior instead of checking raw compatibility-level numbers.
///
/// The important boundary is database compatibility level 130. Older
/// compatibility levels keep the legacy precision-loss step used by SQL Server
/// casts from high-precision temporal values to `datetime`; level 130 and newer
/// use the improved direct rounding behavior.
///
/// Legacy mode does not preserve more source precision. It can store a larger
/// displayed `datetime` value for some inputs because the source is rounded to
/// milliseconds before SQL Server chooses the final 1/300-second fragment.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[allow(dead_code)]
pub(crate) enum DateTimeRounding {
    /// Use legacy pre-130 `datetime` cast semantics.
    ///
    /// This first rounds the source instant to whole milliseconds, then rounds
    /// that millisecond value to SQL Server's 1/300-second `datetime`
    /// fragments. For example, `2026-06-03T23:36:33.684582` stores as `.687`
    /// after losing the sub-millisecond portion.
    LegacyPre130,
    /// Use compatibility-level 130 and later nearest-fragment semantics.
    ///
    /// This rounds the original high-precision source instant directly to the
    /// nearest 1/300-second `datetime` fragment. For example,
    /// `2026-06-03T23:36:33.684582` stores as `.683`.
    Compat130Plus,
}

impl MssqlProfile {
    /// Creates a SQL Server profile after validating that the engine version
    /// supports the requested database compatibility level.
    pub fn new(version: MssqlVersion, compatibility_level: CompatibilityLevel) -> Result<Self> {
        if !version.supports_compatibility_level(compatibility_level) {
            return Err(crate::Error::UnsupportedCompatibilityLevel {
                version,
                compatibility_level: compatibility_level.as_u16(),
            });
        }

        Ok(Self {
            version,
            compatibility_level,
        })
    }

    /// Creates the v0.1 SQL Server 2016 profile with database compatibility
    /// level 100.
    pub const fn sql_server_2016_compat_100() -> Self {
        Self {
            version: MssqlVersion::SqlServer2016,
            compatibility_level: CompatibilityLevel::SQL_SERVER_2008,
        }
    }

    /// Creates the SQL Server 2017 profile with database compatibility
    /// level 100.
    pub const fn sql_server_2017_compat_100() -> Self {
        Self {
            version: MssqlVersion::SqlServer2017,
            compatibility_level: CompatibilityLevel::SQL_SERVER_2008,
        }
    }

    /// Creates the SQL Server 2017 profile with database compatibility
    /// level 110.
    pub const fn sql_server_2017_compat_110() -> Self {
        Self {
            version: MssqlVersion::SqlServer2017,
            compatibility_level: CompatibilityLevel::SQL_SERVER_2012,
        }
    }

    /// Creates the SQL Server 2017 profile with database compatibility
    /// level 120.
    pub const fn sql_server_2017_compat_120() -> Self {
        Self {
            version: MssqlVersion::SqlServer2017,
            compatibility_level: CompatibilityLevel::SQL_SERVER_2014,
        }
    }

    /// Creates the SQL Server 2017 profile with database compatibility
    /// level 130.
    pub const fn sql_server_2017_compat_130() -> Self {
        Self {
            version: MssqlVersion::SqlServer2017,
            compatibility_level: CompatibilityLevel::SQL_SERVER_2016,
        }
    }

    /// Creates the SQL Server 2017 profile with database compatibility
    /// level 140.
    pub const fn sql_server_2017_compat_140() -> Self {
        Self {
            version: MssqlVersion::SqlServer2017,
            compatibility_level: CompatibilityLevel::SQL_SERVER_2017,
        }
    }

    /// Plans an Arrow schema using this SQL Server profile.
    pub fn plan_arrow_schema(
        self,
        schema: impl AsRef<Schema>,
        options: PlanOptions,
    ) -> Result<PlanOutcome<PlannedSchema>> {
        plan_arrow_schema_to_mssql_schema(schema, self, options)
    }

    /// Returns the SQL Server engine version.
    pub const fn version(self) -> MssqlVersion {
        self.version
    }

    /// Returns the database compatibility level.
    pub const fn compatibility_level(self) -> CompatibilityLevel {
        self.compatibility_level
    }

    /// Returns the `datetime` rounding behavior selected by compatibility level.
    ///
    /// This is the single place that maps raw SQL Server compatibility levels
    /// to runtime timestamp-conversion behavior.
    #[allow(dead_code)]
    pub(crate) const fn datetime_rounding(self) -> DateTimeRounding {
        if self.compatibility_level.as_u16() < 130 {
            DateTimeRounding::LegacyPre130
        } else {
            DateTimeRounding::Compat130Plus
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{CompatibilityLevel, DateTimeRounding, MssqlProfile, MssqlVersion};

    #[test]
    fn constructs_sql_server_2016_compat_100_profile() {
        let profile = MssqlProfile::sql_server_2016_compat_100();

        assert_eq!(profile.version(), MssqlVersion::SqlServer2016);
        assert_eq!(profile.compatibility_level().as_u16(), 100);
    }

    #[test]
    fn constructs_sql_server_2017_profiles() {
        let cases = [
            (MssqlProfile::sql_server_2017_compat_100(), 100),
            (MssqlProfile::sql_server_2017_compat_110(), 110),
            (MssqlProfile::sql_server_2017_compat_120(), 120),
            (MssqlProfile::sql_server_2017_compat_130(), 130),
            (MssqlProfile::sql_server_2017_compat_140(), 140),
        ];

        for (profile, compatibility_level) in cases {
            assert_eq!(profile.version(), MssqlVersion::SqlServer2017);
            assert_eq!(profile.compatibility_level().as_u16(), compatibility_level);
        }
    }

    #[test]
    fn reports_default_compatibility_level_by_version() {
        let cases = [
            (MssqlVersion::SqlServer2016, 130),
            (MssqlVersion::SqlServer2017, 140),
            (MssqlVersion::SqlServer2019, 150),
            (MssqlVersion::SqlServer2022, 160),
            (MssqlVersion::SqlServer2025, 170),
        ];

        for (version, compatibility_level) in cases {
            assert_eq!(
                version.default_compatibility_level().as_u16(),
                compatibility_level
            );
        }
    }

    #[test]
    fn creates_profiles_for_supported_version_compatibility_pairs() {
        let cases: &[(MssqlVersion, &[u16])] = &[
            (MssqlVersion::SqlServer2016, &[100, 110, 120, 130]),
            (MssqlVersion::SqlServer2017, &[100, 110, 120, 130, 140]),
            (MssqlVersion::SqlServer2019, &[100, 110, 120, 130, 140, 150]),
            (
                MssqlVersion::SqlServer2022,
                &[100, 110, 120, 130, 140, 150, 160],
            ),
            (
                MssqlVersion::SqlServer2025,
                &[100, 110, 120, 130, 140, 150, 160, 170],
            ),
        ];

        for (version, compatibility_levels) in cases {
            for compatibility_level in *compatibility_levels {
                let compatibility_level = CompatibilityLevel::new(*compatibility_level).unwrap();
                let profile = MssqlProfile::new(*version, compatibility_level).unwrap();

                assert_eq!(profile.version(), *version);
                assert_eq!(profile.compatibility_level(), compatibility_level);
            }
        }
    }

    #[test]
    fn rejects_version_compatibility_pairs_not_supported_by_sql_server() {
        let cases = [
            (MssqlVersion::SqlServer2016, 140),
            (MssqlVersion::SqlServer2017, 150),
            (MssqlVersion::SqlServer2019, 160),
            (MssqlVersion::SqlServer2022, 170),
        ];

        for (version, compatibility_level) in cases {
            let level = CompatibilityLevel::new(compatibility_level).unwrap();
            let err = MssqlProfile::new(version, level).expect_err("profile should be rejected");

            assert!(
                err.to_string().contains(&format!(
                    "does not support database compatibility level {compatibility_level}"
                )),
                "unexpected error: {err}"
            );
        }
    }

    #[test]
    fn accepts_supported_compatibility_levels() {
        let cases = [
            (100, CompatibilityLevel::SQL_SERVER_2008),
            (110, CompatibilityLevel::SQL_SERVER_2012),
            (120, CompatibilityLevel::SQL_SERVER_2014),
            (130, CompatibilityLevel::SQL_SERVER_2016),
            (140, CompatibilityLevel::SQL_SERVER_2017),
            (150, CompatibilityLevel::SQL_SERVER_2019),
            (160, CompatibilityLevel::SQL_SERVER_2022),
            (170, CompatibilityLevel::SQL_SERVER_2025),
        ];

        for (value, expected) in cases {
            let level = CompatibilityLevel::new(value).unwrap();

            assert_eq!(level, expected);
            assert_eq!(level.as_u16(), value);
        }
    }

    #[test]
    fn try_from_accepts_supported_compatibility_levels() {
        for value in [100, 110, 120, 130, 140, 150, 160, 170] {
            let level = CompatibilityLevel::try_from(value).unwrap();

            assert_eq!(level.as_u16(), value);
        }
    }

    #[test]
    fn rejects_unsupported_compatibility_level() {
        let err = CompatibilityLevel::new(90).expect_err("level should be rejected");

        assert!(err.to_string().contains("invalid compatibility level 90"));
    }

    #[test]
    fn rejects_nearby_and_extreme_compatibility_levels() {
        for level in [
            0,
            99,
            101,
            109,
            111,
            119,
            121,
            129,
            131,
            139,
            141,
            149,
            151,
            159,
            161,
            169,
            171,
            u16::MAX,
        ] {
            let err = CompatibilityLevel::new(level).expect_err("level should be rejected");

            assert!(
                err.to_string()
                    .contains(&format!("invalid compatibility level {level}")),
                "unexpected error: {err}"
            );
        }
    }

    #[test]
    fn selects_datetime_rounding_by_compatibility_level() {
        let cases = [
            (
                MssqlProfile::sql_server_2016_compat_100(),
                DateTimeRounding::LegacyPre130,
            ),
            (
                MssqlProfile::sql_server_2017_compat_100(),
                DateTimeRounding::LegacyPre130,
            ),
            (
                MssqlProfile::sql_server_2017_compat_110(),
                DateTimeRounding::LegacyPre130,
            ),
            (
                MssqlProfile::sql_server_2017_compat_120(),
                DateTimeRounding::LegacyPre130,
            ),
            (
                MssqlProfile::sql_server_2017_compat_130(),
                DateTimeRounding::Compat130Plus,
            ),
            (
                MssqlProfile::sql_server_2017_compat_140(),
                DateTimeRounding::Compat130Plus,
            ),
            (
                MssqlProfile::new(
                    MssqlVersion::SqlServer2019,
                    CompatibilityLevel::SQL_SERVER_2019,
                )
                .unwrap(),
                DateTimeRounding::Compat130Plus,
            ),
            (
                MssqlProfile::new(
                    MssqlVersion::SqlServer2022,
                    CompatibilityLevel::SQL_SERVER_2022,
                )
                .unwrap(),
                DateTimeRounding::Compat130Plus,
            ),
            (
                MssqlProfile::new(
                    MssqlVersion::SqlServer2025,
                    CompatibilityLevel::SQL_SERVER_2025,
                )
                .unwrap(),
                DateTimeRounding::Compat130Plus,
            ),
        ];

        for (profile, expected) in cases {
            assert_eq!(profile.datetime_rounding(), expected);
        }
    }
}

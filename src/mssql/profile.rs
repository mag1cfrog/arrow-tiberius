//! SQL Server profile types.

use crate::error::Result;

/// SQL Server engine version targeted by planning.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum MssqlVersion {
    /// SQL Server 2016.
    SqlServer2016,
}

/// SQL Server database compatibility level.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct CompatibilityLevel(u16);

impl CompatibilityLevel {
    /// SQL Server 2008 / 2008 R2 compatibility level.
    pub const SQL_SERVER_2008: Self = Self(100);

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
        matches!(level, 100)
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

impl MssqlProfile {
    /// Creates the v0.1 SQL Server 2016 profile with database compatibility
    /// level 100.
    pub const fn sql_server_2016_compat_100() -> Self {
        Self {
            version: MssqlVersion::SqlServer2016,
            compatibility_level: CompatibilityLevel::SQL_SERVER_2008,
        }
    }

    /// Returns the SQL Server engine version.
    pub const fn version(self) -> MssqlVersion {
        self.version
    }

    /// Returns the database compatibility level.
    pub const fn compatibility_level(self) -> CompatibilityLevel {
        self.compatibility_level
    }
}

#[cfg(test)]
mod tests {
    use super::{CompatibilityLevel, MssqlProfile, MssqlVersion};

    #[test]
    fn constructs_sql_server_2016_compat_100_profile() {
        let profile = MssqlProfile::sql_server_2016_compat_100();

        assert_eq!(profile.version(), MssqlVersion::SqlServer2016);
        assert_eq!(profile.compatibility_level().as_u16(), 100);
    }

    #[test]
    fn accepts_supported_compatibility_level() {
        let level = CompatibilityLevel::new(100).unwrap();

        assert_eq!(level.as_u16(), 100);
    }

    #[test]
    fn try_from_accepts_supported_compatibility_level() {
        let level = CompatibilityLevel::try_from(100).unwrap();

        assert_eq!(level, CompatibilityLevel::SQL_SERVER_2008);
    }

    #[test]
    fn rejects_unsupported_compatibility_level() {
        let err = CompatibilityLevel::new(90).expect_err("level should be rejected");

        assert!(err.to_string().contains("invalid compatibility level 90"));
    }

    #[test]
    fn rejects_nearby_and_extreme_compatibility_levels() {
        for level in [0, 99, 101, 150, u16::MAX] {
            let err = CompatibilityLevel::new(level).expect_err("level should be rejected");

            assert!(
                err.to_string()
                    .contains(&format!("invalid compatibility level {level}")),
                "unexpected error: {err}"
            );
        }
    }
}

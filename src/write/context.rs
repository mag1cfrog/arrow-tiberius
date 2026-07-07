//! Shared write-runtime conversion context.

use crate::{MssqlProfile, NanosecondPolicy, PlanOptions, mssql::profile::DateTimeRounding};

/// Runtime conversion context derived from a planned schema.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) struct RuntimeConversionContext {
    profile: MssqlProfile,
    plan_options: PlanOptions,
}

impl RuntimeConversionContext {
    /// Creates a runtime conversion context.
    pub(crate) const fn new(profile: MssqlProfile, plan_options: PlanOptions) -> Self {
        Self {
            profile,
            plan_options,
        }
    }

    /// Returns the SQL Server profile selected during planning.
    #[allow(dead_code)]
    pub(crate) const fn profile(self) -> MssqlProfile {
        self.profile
    }

    /// Returns the conversion policies selected during planning.
    #[allow(dead_code)]
    pub(crate) const fn plan_options(self) -> PlanOptions {
        self.plan_options
    }

    /// Returns the nanosecond conversion policy.
    #[allow(dead_code)]
    pub(crate) const fn nanosecond_policy(self) -> NanosecondPolicy {
        self.plan_options.nanosecond_policy
    }

    /// Returns the SQL Server datetime rounding behavior selected by the profile.
    #[allow(dead_code)]
    pub(crate) const fn datetime_rounding(self) -> DateTimeRounding {
        self.profile.datetime_rounding()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn selects_datetime_rounding_from_profile() {
        let options = PlanOptions::default();

        assert_eq!(
            RuntimeConversionContext::new(MssqlProfile::sql_server_2016_compat_100(), options)
                .datetime_rounding(),
            DateTimeRounding::LegacyPre130
        );
        assert_eq!(
            RuntimeConversionContext::new(MssqlProfile::sql_server_2017_compat_140(), options)
                .datetime_rounding(),
            DateTimeRounding::Compat130Plus
        );
    }
}

//! Shared write-runtime conversion context.

use crate::{MssqlProfile, NanosecondPolicy, PlanOptions};

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
}

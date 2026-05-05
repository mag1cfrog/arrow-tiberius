//! Structured diagnostics for planning and writing.

/// Diagnostic severity.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum DiagnosticSeverity {
    /// The operation can continue, but callers may want to surface the finding.
    Warning,
    /// The operation cannot continue successfully.
    Error,
}

/// Machine-readable diagnostic code.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum DiagnosticCode {
    /// An Arrow type is not supported by the selected operation.
    UnsupportedArrowType,
    /// A conversion may lose information and requires explicit policy.
    LossyConversionRequiresPolicy,
    /// An explicit conversion policy was applied.
    PolicyApplied,
    /// A SQL Server identifier is invalid.
    IdentifierInvalid,
    /// A SQL Server identifier exceeds the supported length.
    IdentifierTooLong,
    /// A decimal value, precision, or scale is outside the supported range.
    DecimalOutOfRange,
    /// A timestamp value is outside the supported range.
    TimestampOutOfRange,
    /// A runtime batch schema does not match the planned schema.
    SchemaMismatch,
    /// A requested write backend is unavailable.
    BackendUnavailable,
    /// A mapping depends on explicit user policy.
    ProfileDependentConversion,
}

/// Field location for a diagnostic.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct FieldRef {
    index: usize,
    name: String,
}

impl FieldRef {
    /// Creates a field reference.
    pub fn new(index: usize, name: impl Into<String>) -> Self {
        Self {
            index,
            name: name.into(),
        }
    }

    /// Returns the field index.
    pub const fn index(&self) -> usize {
        self.index
    }

    /// Returns the field name.
    pub fn name(&self) -> &str {
        &self.name
    }
}

/// Structured diagnostic.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Diagnostic {
    severity: DiagnosticSeverity,
    code: DiagnosticCode,
    message: String,
    field: Option<FieldRef>,
}

impl Diagnostic {
    /// Creates a diagnostic.
    pub fn new(
        severity: DiagnosticSeverity,
        code: DiagnosticCode,
        message: impl Into<String>,
    ) -> Self {
        Self {
            severity,
            code,
            message: message.into(),
            field: None,
        }
    }

    /// Creates a warning diagnostic.
    pub fn warning(code: DiagnosticCode, message: impl Into<String>) -> Self {
        Self::new(DiagnosticSeverity::Warning, code, message)
    }

    /// Creates an error diagnostic.
    pub fn error(code: DiagnosticCode, message: impl Into<String>) -> Self {
        Self::new(DiagnosticSeverity::Error, code, message)
    }

    /// Attaches field location to this diagnostic.
    #[must_use]
    pub fn with_field(mut self, field: FieldRef) -> Self {
        self.field = Some(field);
        self
    }

    /// Returns the diagnostic severity.
    pub const fn severity(&self) -> DiagnosticSeverity {
        self.severity
    }

    /// Returns the diagnostic code.
    pub const fn code(&self) -> DiagnosticCode {
        self.code
    }

    /// Returns the diagnostic message.
    pub fn message(&self) -> &str {
        &self.message
    }

    /// Returns the optional field location.
    pub fn field(&self) -> Option<&FieldRef> {
        self.field.as_ref()
    }

    /// Returns true if this diagnostic is an error.
    pub const fn is_error(&self) -> bool {
        matches!(self.severity, DiagnosticSeverity::Error)
    }
}

/// Collection of diagnostics.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct DiagnosticSet {
    diagnostics: Vec<Diagnostic>,
}

impl DiagnosticSet {
    /// Creates an empty diagnostic set.
    pub const fn new() -> Self {
        Self {
            diagnostics: Vec::new(),
        }
    }

    /// Adds a diagnostic.
    pub fn push(&mut self, diagnostic: Diagnostic) {
        self.diagnostics.push(diagnostic);
    }

    /// Returns all diagnostics.
    pub fn all(&self) -> &[Diagnostic] {
        &self.diagnostics
    }

    /// Returns true if no diagnostics are present.
    pub fn is_empty(&self) -> bool {
        self.diagnostics.is_empty()
    }

    /// Returns true if at least one error diagnostic is present.
    pub fn has_errors(&self) -> bool {
        self.diagnostics.iter().any(Diagnostic::is_error)
    }

    /// Returns the number of diagnostics.
    pub fn len(&self) -> usize {
        self.diagnostics.len()
    }
}

impl From<Vec<Diagnostic>> for DiagnosticSet {
    fn from(diagnostics: Vec<Diagnostic>) -> Self {
        Self { diagnostics }
    }
}

impl IntoIterator for DiagnosticSet {
    type Item = Diagnostic;
    type IntoIter = std::vec::IntoIter<Diagnostic>;

    fn into_iter(self) -> Self::IntoIter {
        self.diagnostics.into_iter()
    }
}

/// Successful planning result plus diagnostics.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlanOutcome<T> {
    value: T,
    diagnostics: DiagnosticSet,
}

impl<T> PlanOutcome<T> {
    /// Creates a planning outcome.
    pub const fn new(value: T, diagnostics: DiagnosticSet) -> Self {
        Self { value, diagnostics }
    }

    /// Returns the planned value.
    pub const fn value(&self) -> &T {
        &self.value
    }

    /// Returns the diagnostics.
    pub const fn diagnostics(&self) -> &DiagnosticSet {
        &self.diagnostics
    }

    /// Consumes this outcome into its parts.
    pub fn into_parts(self) -> (T, DiagnosticSet) {
        (self.value, self.diagnostics)
    }
}

#[cfg(test)]
mod tests {
    use super::{
        Diagnostic, DiagnosticCode, DiagnosticSet, DiagnosticSeverity, FieldRef, PlanOutcome,
    };

    #[test]
    fn creates_field_diagnostic() {
        let diagnostic = Diagnostic::warning(DiagnosticCode::PolicyApplied, "policy applied")
            .with_field(FieldRef::new(2, "amount"));

        assert_eq!(diagnostic.severity(), DiagnosticSeverity::Warning);
        assert_eq!(diagnostic.code(), DiagnosticCode::PolicyApplied);
        assert_eq!(diagnostic.message(), "policy applied");

        let field = diagnostic.field().unwrap();
        assert_eq!(field.index(), 2);
        assert_eq!(field.name(), "amount");
    }

    #[test]
    fn detects_error_diagnostics() {
        let mut diagnostics = DiagnosticSet::new();
        diagnostics.push(Diagnostic::warning(
            DiagnosticCode::PolicyApplied,
            "policy applied",
        ));

        assert!(!diagnostics.has_errors());

        diagnostics.push(Diagnostic::error(
            DiagnosticCode::UnsupportedArrowType,
            "unsupported",
        ));

        assert!(diagnostics.has_errors());
        assert_eq!(diagnostics.len(), 2);
    }

    #[test]
    fn converts_from_vec() {
        let diagnostics = DiagnosticSet::from(vec![Diagnostic::error(
            DiagnosticCode::IdentifierInvalid,
            "invalid",
        )]);

        assert_eq!(diagnostics.len(), 1);
        assert!(!diagnostics.is_empty());
    }

    #[test]
    fn plan_outcome_exposes_value_and_diagnostics() {
        let diagnostics = DiagnosticSet::from(vec![Diagnostic::warning(
            DiagnosticCode::ProfileDependentConversion,
            "policy needed",
        )]);
        let outcome = PlanOutcome::new("plan", diagnostics);

        assert_eq!(outcome.value(), &"plan");
        assert_eq!(outcome.diagnostics().len(), 1);

        let (value, diagnostics) = outcome.into_parts();
        assert_eq!(value, "plan");
        assert_eq!(diagnostics.len(), 1);
    }
}

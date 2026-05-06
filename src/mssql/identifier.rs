//! SQL Server identifier types and quoting helpers.

use crate::Result;

const MAX_IDENTIFIER_CHARS: usize = 128;

/// SQL Server identifier policy.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum IdentifierPolicy {
    /// Reject empty and over-length identifiers, then render with bracket
    /// quoting.
    BracketQuoted,
}

/// A single SQL Server identifier part.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Identifier {
    value: String,
}

impl Identifier {
    /// Creates a SQL Server identifier using the default identifier policy.
    pub fn new(value: impl Into<String>) -> Result<Self> {
        Self::with_policy(value, IdentifierPolicy::BracketQuoted)
    }

    /// Creates a SQL Server identifier using an explicit identifier policy.
    pub fn with_policy(value: impl Into<String>, policy: IdentifierPolicy) -> Result<Self> {
        let value = value.into();
        validate_identifier(&value, policy)?;
        Ok(Self { value })
    }

    /// Returns the unquoted identifier text.
    pub fn as_str(&self) -> &str {
        &self.value
    }

    /// Renders this identifier with SQL Server bracket quoting.
    pub fn quoted_sql(&self) -> String {
        bracket_quote(&self.value)
    }
}

/// SQL Server table name.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct TableName {
    schema: Option<Identifier>,
    table: Identifier,
}

impl TableName {
    /// Creates a schema-qualified table name.
    pub fn new(schema: impl Into<String>, table: impl Into<String>) -> Result<Self> {
        Ok(Self {
            schema: Some(Identifier::new(schema)?),
            table: Identifier::new(table)?,
        })
    }

    /// Creates an unqualified table name.
    pub fn unqualified(table: impl Into<String>) -> Result<Self> {
        Ok(Self {
            schema: None,
            table: Identifier::new(table)?,
        })
    }

    /// Returns the optional schema identifier.
    pub fn schema(&self) -> Option<&Identifier> {
        self.schema.as_ref()
    }

    /// Returns the table identifier.
    pub fn table(&self) -> &Identifier {
        &self.table
    }

    /// Renders the table name with bracket-quoted identifier parts.
    pub fn quoted_sql(&self) -> String {
        match &self.schema {
            Some(schema) => format!("{}.{}", schema.quoted_sql(), self.table.quoted_sql()),
            None => self.table.quoted_sql(),
        }
    }
}

fn validate_identifier(value: &str, policy: IdentifierPolicy) -> Result<()> {
    match policy {
        IdentifierPolicy::BracketQuoted => {
            if value.is_empty() {
                return Err(crate::Error::InvalidIdentifier {
                    reason: "identifier cannot be empty".to_owned(),
                });
            }

            if value.chars().any(char::is_control) {
                return Err(crate::Error::InvalidIdentifier {
                    reason: "identifier cannot contain control characters".to_owned(),
                });
            }

            let len = value.chars().count();
            if len > MAX_IDENTIFIER_CHARS {
                return Err(crate::Error::InvalidIdentifier {
                    reason: format!(
                        "identifier is {len} characters; maximum is {MAX_IDENTIFIER_CHARS}"
                    ),
                });
            }

            Ok(())
        }
    }
}

fn bracket_quote(value: &str) -> String {
    let escaped = value.replace(']', "]]");
    format!("[{escaped}]")
}

#[cfg(test)]
mod tests {
    use super::{Identifier, TableName};

    #[test]
    fn quotes_ordinary_identifier() {
        let ident = Identifier::new("target_table").unwrap();

        assert_eq!(ident.as_str(), "target_table");
        assert_eq!(ident.quoted_sql(), "[target_table]");
    }

    #[test]
    fn quotes_identifier_with_spaces() {
        let ident = Identifier::new("target table").unwrap();

        assert_eq!(ident.quoted_sql(), "[target table]");
    }

    #[test]
    fn quotes_reserved_like_identifier() {
        let ident = Identifier::new("select").unwrap();

        assert_eq!(ident.quoted_sql(), "[select]");
    }

    #[test]
    fn treats_dot_as_literal_identifier_content() {
        let ident = Identifier::new("dbo.target").unwrap();

        assert_eq!(ident.quoted_sql(), "[dbo.target]");
    }

    #[test]
    fn escapes_brackets() {
        let ident = Identifier::new("a]b").unwrap();

        assert_eq!(ident.quoted_sql(), "[a]]b]");
    }

    #[test]
    fn quotes_injection_shaped_identifier_as_one_identifier() {
        let ident = Identifier::new("dbo].[target]; DROP TABLE [prod];--").unwrap();

        assert_eq!(
            ident.quoted_sql(),
            "[dbo]].[target]]; DROP TABLE [prod]];--]"
        );
    }

    #[test]
    fn accepts_exactly_128_unicode_scalar_values() {
        let value = "表".repeat(128);
        let ident = Identifier::new(value.clone()).unwrap();

        assert_eq!(ident.as_str(), value);
    }

    #[test]
    fn rejects_empty_identifier() {
        let err = Identifier::new("").expect_err("empty identifiers should be rejected");

        assert!(
            err.to_string().contains("identifier cannot be empty"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn rejects_control_characters() {
        for value in ["line\nbreak", "tab\tname", "nul\0name"] {
            let err = Identifier::new(value).expect_err("control characters should be rejected");

            assert!(
                err.to_string().contains("control characters"),
                "unexpected error: {err}"
            );
        }
    }

    #[test]
    fn rejects_over_length_identifier() {
        let value = "x".repeat(129);
        let err = Identifier::new(value).expect_err("over-length identifiers should be rejected");

        assert!(
            err.to_string().contains("maximum is 128"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn rejects_over_length_unicode_identifier_by_character_count() {
        let value = "表".repeat(129);
        let err = Identifier::new(value).expect_err("over-length identifiers should be rejected");

        assert!(
            err.to_string().contains("identifier is 129 characters"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn renders_schema_qualified_table_name() {
        let table = TableName::new("dbo", "target").unwrap();

        assert_eq!(table.quoted_sql(), "[dbo].[target]");
        assert_eq!(table.schema().unwrap().as_str(), "dbo");
        assert_eq!(table.table().as_str(), "target");
    }

    #[test]
    fn renders_unqualified_table_name() {
        let table = TableName::unqualified("target").unwrap();

        assert_eq!(table.quoted_sql(), "[target]");
        assert!(table.schema().is_none());
        assert_eq!(table.table().as_str(), "target");
    }

    #[test]
    fn table_name_does_not_split_dots_inside_parts() {
        let table = TableName::new("dbo.part", "target.part").unwrap();

        assert_eq!(table.quoted_sql(), "[dbo.part].[target.part]");
    }

    #[test]
    fn rejects_invalid_schema_or_table_part() {
        let err = TableName::new("", "target").expect_err("empty schema should be rejected");
        assert!(
            err.to_string().contains("identifier cannot be empty"),
            "unexpected error: {err}"
        );

        let err = TableName::new("dbo", "").expect_err("empty table should be rejected");
        assert!(
            err.to_string().contains("identifier cannot be empty"),
            "unexpected error: {err}"
        );

        let err = TableName::unqualified("").expect_err("empty table should be rejected");
        assert!(
            err.to_string().contains("identifier cannot be empty"),
            "unexpected error: {err}"
        );
    }
}

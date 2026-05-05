//! Deterministic SQL Server DDL rendering helpers.

use crate::{Identifier, MssqlType, TableName};

/// SQL Server column definition for `CREATE TABLE` rendering.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColumnDefinition {
    name: Identifier,
    ty: MssqlType,
    nullable: bool,
}

impl ColumnDefinition {
    /// Creates a column definition.
    pub const fn new(name: Identifier, ty: MssqlType, nullable: bool) -> Self {
        Self { name, ty, nullable }
    }

    /// Returns the column name.
    pub const fn name(&self) -> &Identifier {
        &self.name
    }

    /// Returns the SQL Server column type.
    pub const fn ty(&self) -> &MssqlType {
        &self.ty
    }

    /// Returns true when the column allows `NULL`.
    pub const fn nullable(&self) -> bool {
        self.nullable
    }

    fn to_sql(&self) -> String {
        let nullability = if self.nullable { "NULL" } else { "NOT NULL" };
        format!(
            "{} {} {nullability}",
            self.name.quoted_sql(),
            self.ty.to_sql()
        )
    }
}

/// Options for `CREATE TABLE` rendering.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash)]
pub struct CreateTableOptions;

/// Renders deterministic SQL Server `CREATE TABLE` DDL.
pub fn create_table_sql(
    table: &TableName,
    columns: &[ColumnDefinition],
    _options: CreateTableOptions,
) -> String {
    let mut sql = format!("CREATE TABLE {} (", table.quoted_sql());

    if columns.is_empty() {
        sql.push_str("\n);");
        return sql;
    }

    for (index, column) in columns.iter().enumerate() {
        let suffix = if index + 1 == columns.len() { "" } else { "," };
        sql.push_str("\n    ");
        sql.push_str(&column.to_sql());
        sql.push_str(suffix);
    }

    sql.push_str("\n);");
    sql
}

#[cfg(test)]
mod tests {
    use crate::{
        ColumnDefinition, CreateTableOptions, Identifier, MssqlType, MssqlTypeLength, TableName,
        create_table_sql,
    };

    #[test]
    fn renders_create_table_with_deterministic_formatting() {
        let table = TableName::new("dbo", "target").unwrap();
        let columns = vec![
            ColumnDefinition::new(Identifier::new("id").unwrap(), MssqlType::Int, false),
            ColumnDefinition::new(
                Identifier::new("name").unwrap(),
                MssqlType::NVarChar(MssqlTypeLength::Max),
                true,
            ),
        ];

        let sql = create_table_sql(&table, &columns, CreateTableOptions);

        assert_eq!(
            sql,
            "CREATE TABLE [dbo].[target] (\n    [id] int NOT NULL,\n    [name] nvarchar(max) NULL\n);"
        );
    }

    #[test]
    fn quotes_table_and_column_identifiers() {
        let table = TableName::new("dbo.part", "target]part").unwrap();
        let columns = vec![ColumnDefinition::new(
            Identifier::new("select]from").unwrap(),
            MssqlType::Bit,
            false,
        )];

        let sql = create_table_sql(&table, &columns, CreateTableOptions);

        assert_eq!(
            sql,
            "CREATE TABLE [dbo.part].[target]]part] (\n    [select]]from] bit NOT NULL\n);"
        );
    }

    #[test]
    fn renders_empty_column_list_without_panicking() {
        let table = TableName::unqualified("empty").unwrap();

        let sql = create_table_sql(&table, &[], CreateTableOptions);

        assert_eq!(sql, "CREATE TABLE [empty] (\n);");
    }
}

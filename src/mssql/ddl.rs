//! Deterministic SQL Server DDL rendering helpers.

use super::{MssqlColumnPlan, TableName};

/// Options for `CREATE TABLE` rendering.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash)]
pub struct CreateTableOptions;

/// Renders deterministic SQL Server `CREATE TABLE` DDL.
pub fn create_table_sql(
    table: &TableName,
    columns: &[MssqlColumnPlan],
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
        CreateTableOptions, Identifier, MssqlColumnPlan, MssqlType, MssqlTypeLength, TableName,
        create_table_sql,
    };

    #[test]
    fn renders_create_table_with_deterministic_formatting() {
        let table = TableName::new("dbo", "target").unwrap();
        let columns = vec![
            MssqlColumnPlan::new(Identifier::new("id").unwrap(), MssqlType::Int, false),
            MssqlColumnPlan::new(
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
        let columns = vec![MssqlColumnPlan::new(
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

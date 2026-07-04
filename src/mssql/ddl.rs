//! Deterministic SQL Server DDL rendering helpers.

use super::{MssqlColumn, TableName};

/// Options for `CREATE TABLE` rendering.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash)]
pub struct CreateTableOptions;

/// Renders deterministic SQL Server `CREATE TABLE` DDL.
pub fn create_table_sql(
    table: &TableName,
    columns: &[MssqlColumn],
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
        CreateTableOptions, Identifier, MssqlColumn, MssqlTimePrecision, MssqlType,
        MssqlTypeLength, TableName, create_table_sql,
    };

    #[test]
    fn renders_create_table_with_deterministic_formatting() {
        let table = TableName::new("dbo", "target").unwrap();
        let columns = vec![
            MssqlColumn::new(Identifier::new("id").unwrap(), MssqlType::Int, false),
            MssqlColumn::new(
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
        let columns = vec![MssqlColumn::new(
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

    #[test]
    fn renders_decimal_and_temporal_columns() {
        let table = TableName::new("dbo", "events").unwrap();
        let columns = vec![
            MssqlColumn::new(
                Identifier::new("amount").unwrap(),
                MssqlType::Decimal {
                    precision: 38,
                    scale: 9,
                },
                false,
            ),
            MssqlColumn::new(
                Identifier::new("event_date").unwrap(),
                MssqlType::Date,
                true,
            ),
            MssqlColumn::new(
                Identifier::new("event_time").unwrap(),
                MssqlType::Time(MssqlTimePrecision::SEVEN),
                true,
            ),
            MssqlColumn::new(
                Identifier::new("created_at").unwrap(),
                MssqlType::DateTime,
                false,
            ),
            MssqlColumn::new(
                Identifier::new("updated_at").unwrap(),
                MssqlType::DateTime2 { precision: 7 },
                false,
            ),
            MssqlColumn::new(
                Identifier::new("source_offset").unwrap(),
                MssqlType::DateTimeOffset { precision: 7 },
                true,
            ),
        ];

        let sql = create_table_sql(&table, &columns, CreateTableOptions);

        assert_eq!(
            sql,
            "CREATE TABLE [dbo].[events] (\n    [amount] decimal(38,9) NOT NULL,\n    [event_date] date NULL,\n    [event_time] time(7) NULL,\n    [created_at] datetime NOT NULL,\n    [updated_at] datetime2(7) NOT NULL,\n    [source_offset] datetimeoffset(7) NULL\n);"
        );
    }

    #[test]
    fn renders_fixed_binary_columns() {
        let table = TableName::new("dbo", "binary_values").unwrap();
        let columns = vec![MssqlColumn::new(
            Identifier::new("digest").unwrap(),
            MssqlType::Binary(32),
            false,
        )];

        let sql = create_table_sql(&table, &columns, CreateTableOptions);

        assert_eq!(
            sql,
            "CREATE TABLE [dbo].[binary_values] (\n    [digest] binary(32) NOT NULL\n);"
        );
    }
}

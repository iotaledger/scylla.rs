// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use super::*;
use crate::PrimaryKey;

#[derive(ParseFromStr, Clone, Debug, TryInto, From, ToTokens, PartialEq)]
pub enum MaterializedViewStatement {
    Create(CreateMaterializedViewStatement),
    Alter(AlterMaterializedViewStatement),
    Drop(DropMaterializedViewStatement),
}

impl Parse for MaterializedViewStatement {
    type Output = Self;
    fn parse(s: &mut StatementStream<'_>) -> anyhow::Result<Self::Output> {
        Ok(
            if let Some(stmt) = s.parse::<Option<CreateMaterializedViewStatement>>()? {
                Self::Create(stmt)
            } else if let Some(stmt) = s.parse::<Option<AlterMaterializedViewStatement>>()? {
                Self::Alter(stmt)
            } else if let Some(stmt) = s.parse::<Option<DropMaterializedViewStatement>>()? {
                Self::Drop(stmt)
            } else {
                anyhow::bail!("Invalid MATERIALIZED VIEW statement!")
            },
        )
    }
}

impl Peek for MaterializedViewStatement {
    fn peek(s: StatementStream<'_>) -> bool {
        s.check::<CreateMaterializedViewStatement>()
            || s.check::<AlterMaterializedViewStatement>()
            || s.check::<DropMaterializedViewStatement>()
    }
}

impl Display for MaterializedViewStatement {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Create(stmt) => stmt.fmt(f),
            Self::Alter(stmt) => stmt.fmt(f),
            Self::Drop(stmt) => stmt.fmt(f),
        }
    }
}

#[derive(ParseFromStr, Builder, Clone, Debug, ToTokens, PartialEq)]
pub struct CreateMaterializedViewStatement {
    #[builder(setter(name = "set_if_not_exists"), default)]
    pub if_not_exists: bool,
    #[builder(setter(into))]
    pub name: Name,
    pub select_statement: SelectStatement,
    #[builder(setter(into))]
    pub primary_key: PrimaryKey,
    pub table_opts: TableOpts,
}

impl CreateMaterializedViewStatementBuilder {
    /// Set IF NOT EXISTS on the statement.
    /// To undo this, use `set_if_not_exists(false)`.
    pub fn if_not_exists(&mut self) -> &mut Self {
        self.if_not_exists.replace(true);
        self
    }
}

impl Parse for CreateMaterializedViewStatement {
    type Output = Self;
    fn parse(s: &mut StatementStream<'_>) -> anyhow::Result<Self::Output> {
        s.parse::<(CREATE, MATERIALIZED, VIEW)>()?;
        let mut res = CreateMaterializedViewStatementBuilder::default();
        res.set_if_not_exists(s.parse::<Option<(IF, NOT, EXISTS)>>()?.is_some())
            .name(s.parse::<Name>()?)
            .select_statement(s.parse::<(AS, _)>()?.1)
            .primary_key(s.parse_from::<((PRIMARY, KEY), Parens<PrimaryKey>)>()?.1)
            .table_opts(s.parse_from::<(WITH, TableOpts)>()?.1);
        s.parse::<Option<Semicolon>>()?;
        Ok(res
            .build()
            .map_err(|e| anyhow::anyhow!("Invalid CREATE MATERIALIZED VIEW statement: {}", e))?)
    }
}

impl Peek for CreateMaterializedViewStatement {
    fn peek(s: StatementStream<'_>) -> bool {
        s.check::<(CREATE, MATERIALIZED, VIEW)>()
    }
}

impl Display for CreateMaterializedViewStatement {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "CREATE MATERIALIZED VIEW{} {} AS {} PRIMARY KEY ({}) WITH {}",
            if self.if_not_exists { " IF NOT EXISTS" } else { "" },
            self.name,
            self.select_statement,
            self.primary_key,
            self.table_opts
        )
    }
}

#[derive(ParseFromStr, Builder, Clone, Debug, ToTokens, PartialEq)]
pub struct AlterMaterializedViewStatement {
    #[builder(setter(into))]
    pub name: Name,
    pub table_opts: TableOpts,
}

impl Parse for AlterMaterializedViewStatement {
    type Output = Self;
    fn parse(s: &mut StatementStream<'_>) -> anyhow::Result<Self::Output> {
        s.parse::<(ALTER, MATERIALIZED, VIEW)>()?;
        let mut res = AlterMaterializedViewStatementBuilder::default();
        res.name(s.parse::<Name>()?)
            .table_opts(s.parse_from::<(WITH, TableOpts)>()?.1);
        s.parse::<Option<Semicolon>>()?;
        Ok(res
            .build()
            .map_err(|e| anyhow::anyhow!("Invalid ALTER MATERIALIZED VIEW statement: {}", e))?)
    }
}

impl Peek for AlterMaterializedViewStatement {
    fn peek(s: StatementStream<'_>) -> bool {
        s.check::<(ALTER, MATERIALIZED, VIEW)>()
    }
}

impl Display for AlterMaterializedViewStatement {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "ALTER MATERIALIZED VIEW {} WITH {}", self.name, self.table_opts)
    }
}

#[derive(ParseFromStr, Builder, Clone, Debug, ToTokens, PartialEq, Eq)]
pub struct DropMaterializedViewStatement {
    #[builder(setter(name = "set_if_exists"), default)]
    pub if_exists: bool,
    #[builder(setter(into))]
    pub name: Name,
}

impl DropMaterializedViewStatementBuilder {
    /// Set IF EXISTS on the statement.
    /// To undo this, use `set_if_exists(false)`.
    pub fn if_exists(&mut self) -> &mut Self {
        self.if_exists.replace(true);
        self
    }
}

impl Parse for DropMaterializedViewStatement {
    type Output = Self;
    fn parse(s: &mut StatementStream<'_>) -> anyhow::Result<Self::Output> {
        s.parse::<(DROP, MATERIALIZED, VIEW)>()?;
        let mut res = DropMaterializedViewStatementBuilder::default();
        res.set_if_exists(s.parse::<Option<(IF, EXISTS)>>()?.is_some())
            .name(s.parse::<Name>()?);
        s.parse::<Option<Semicolon>>()?;
        Ok(res
            .build()
            .map_err(|e| anyhow::anyhow!("Invalid DROP MATERIALIZED VIEW statement: {}", e))?)
    }
}

impl Peek for DropMaterializedViewStatement {
    fn peek(s: StatementStream<'_>) -> bool {
        s.check::<(DROP, MATERIALIZED, VIEW)>()
    }
}

impl Display for DropMaterializedViewStatement {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "DROP MATERIALIZED VIEW{} {}",
            if self.if_exists { " IF EXISTS" } else { "" },
            self.name
        )
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_parse_create_mv() {
        let mut builder = CreateMaterializedViewStatementBuilder::default();
        builder.name("test_mv");
        assert!(builder.build().is_err());
        builder.select_statement(
            SelectStatementBuilder::default()
                .select_clause(SelectClause::All)
                .from("test_table")
                .where_clause(vec![
                    Relation::is_not_null("column_1"),
                    Relation::is_not_null("column 2"),
                ])
                .build()
                .unwrap(),
        );
        assert!(builder.build().is_err());
        builder.primary_key(vec!["column_1", "column 2"]);
        assert!(builder.build().is_err());
        builder.table_opts(
            crate::TableOptsBuilder::default()
                .comment(r#"test comment " "#)
                .build()
                .unwrap(),
        );
        let statement = builder.build().unwrap().to_string();
        assert_eq!(builder.build().unwrap(), statement.parse().unwrap());
        builder.primary_key(PrimaryKey::partition_key("column_1").clustering_columns(vec!["column 2", "column_3"]));
        let statement = builder.build().unwrap().to_string();
        assert_eq!(builder.build().unwrap(), statement.parse().unwrap());
    }

    #[test]
    fn test_parse_alter_mv() {
        let mut builder = AlterMaterializedViewStatementBuilder::default();
        builder.name("test mv");
        assert!(builder.build().is_err());
        builder.table_opts(
            crate::TableOptsBuilder::default()
                .default_time_to_live(100)
                .build()
                .unwrap(),
        );
        let statement = builder.build().unwrap().to_string();
        assert_eq!(builder.build().unwrap(), statement.parse().unwrap());
    }

    #[test]
    fn test_parse_drop_mv() {
        let mut builder = DropMaterializedViewStatementBuilder::default();
        builder.name("test_mv");
        let statement = builder.build().unwrap().to_string();
        assert_eq!(builder.build().unwrap(), statement.parse().unwrap());
        builder.if_exists();
        let statement = builder.build().unwrap().to_string();
        assert_eq!(builder.build().unwrap(), statement.parse().unwrap());
    }
}

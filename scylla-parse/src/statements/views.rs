// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use super::*;
use crate::PrimaryKey;

#[derive(ParseFromStr, Clone, Debug, TryInto, From, ToTokens, PartialEq)]
#[parse_via(TaggedMaterializedViewStatement)]
pub enum MaterializedViewStatement {
    Create(CreateMaterializedViewStatement),
    Alter(AlterMaterializedViewStatement),
    Drop(DropMaterializedViewStatement),
}

impl TryFrom<TaggedMaterializedViewStatement> for MaterializedViewStatement {
    type Error = anyhow::Error;
    fn try_from(value: TaggedMaterializedViewStatement) -> Result<Self, Self::Error> {
        Ok(match value {
            TaggedMaterializedViewStatement::Create(s) => MaterializedViewStatement::Create(s.try_into()?),
            TaggedMaterializedViewStatement::Alter(s) => MaterializedViewStatement::Alter(s.try_into()?),
            TaggedMaterializedViewStatement::Drop(s) => MaterializedViewStatement::Drop(s.try_into()?),
        })
    }
}

#[derive(ParseFromStr, Clone, Debug, TryInto, From, ToTokens, PartialEq)]
#[tokenize_as(MaterializedViewStatement)]
pub enum TaggedMaterializedViewStatement {
    Create(TaggedCreateMaterializedViewStatement),
    Alter(TaggedAlterMaterializedViewStatement),
    Drop(TaggedDropMaterializedViewStatement),
}

impl Parse for TaggedMaterializedViewStatement {
    type Output = Self;
    fn parse(s: &mut StatementStream<'_>) -> anyhow::Result<Self::Output> {
        Ok(if s.check::<CREATE>() {
            Self::Create(s.parse()?)
        } else if s.check::<ALTER>() {
            Self::Alter(s.parse()?)
        } else if s.check::<DROP>() {
            Self::Drop(s.parse()?)
        } else {
            anyhow::bail!("Expected a materialized view statement, found {}", s.info())
        })
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
#[parse_via(TaggedCreateMaterializedViewStatement)]
#[builder(setter(strip_option))]
pub struct CreateMaterializedViewStatement {
    #[builder(setter(name = "set_if_not_exists"), default)]
    pub if_not_exists: bool,
    #[builder(setter(into))]
    pub name: KeyspaceQualifiedName,
    pub select_statement: SelectStatement,
    #[builder(setter(into))]
    pub primary_key: PrimaryKey,
    #[builder(default)]
    pub table_opts: Option<TableOpts>,
}

impl TryFrom<TaggedCreateMaterializedViewStatement> for CreateMaterializedViewStatement {
    type Error = anyhow::Error;
    fn try_from(value: TaggedCreateMaterializedViewStatement) -> Result<Self, Self::Error> {
        Ok(Self {
            if_not_exists: value.if_not_exists,
            name: value.name.try_into()?,
            select_statement: value.select_statement.into_value()?.try_into()?,
            primary_key: value.primary_key.into_value()?,
            table_opts: value.table_opts.map(|v| v.into_value()).transpose()?,
        })
    }
}

#[derive(ParseFromStr, Builder, Clone, Debug, ToTokens, PartialEq)]
#[tokenize_as(CreateMaterializedViewStatement)]
#[builder(setter(strip_option))]
pub struct TaggedCreateMaterializedViewStatement {
    #[builder(setter(name = "set_if_not_exists"), default)]
    pub if_not_exists: bool,
    pub name: TaggedKeyspaceQualifiedName,
    pub select_statement: Tag<TaggedSelectStatement>,
    pub primary_key: Tag<PrimaryKey>,
    #[builder(default)]
    pub table_opts: Option<Tag<TableOpts>>,
}

impl CreateMaterializedViewStatementBuilder {
    /// Set IF NOT EXISTS on the statement.
    /// To undo this, use `set_if_not_exists(false)`.
    pub fn if_not_exists(&mut self) -> &mut Self {
        self.if_not_exists.replace(true);
        self
    }
}

impl Parse for TaggedCreateMaterializedViewStatement {
    type Output = Self;
    fn parse(s: &mut StatementStream<'_>) -> anyhow::Result<Self::Output> {
        s.parse::<(CREATE, MATERIALIZED, VIEW)>()?;
        let mut res = TaggedCreateMaterializedViewStatementBuilder::default();
        res.set_if_not_exists(s.parse::<Option<(IF, NOT, EXISTS)>>()?.is_some())
            .name(s.parse()?)
            .select_statement(s.parse::<(AS, _)>()?.1)
            .primary_key(s.parse_from::<((PRIMARY, KEY), Parens<Tag<PrimaryKey>>)>()?.1);
        if let Some(p) = s.parse_from::<If<WITH, Tag<TableOpts>>>()? {
            res.table_opts(p);
        }
        s.parse::<Option<Semicolon>>()?;
        Ok(res
            .build()
            .map_err(|e| anyhow::anyhow!("Invalid CREATE MATERIALIZED VIEW statement: {}", e))?)
    }
}

impl Display for CreateMaterializedViewStatement {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "CREATE MATERIALIZED VIEW{} {} AS {} PRIMARY KEY ({}){}",
            if self.if_not_exists { " IF NOT EXISTS" } else { "" },
            self.name,
            self.select_statement,
            self.primary_key,
            match self.table_opts.as_ref() {
                Some(opts) => format!(" WITH {}", opts),
                None => "".to_string(),
            }
        )
    }
}

#[derive(ParseFromStr, Builder, Clone, Debug, ToTokens, PartialEq)]
#[parse_via(TaggedAlterMaterializedViewStatement)]
pub struct AlterMaterializedViewStatement {
    #[builder(setter(into))]
    pub name: KeyspaceQualifiedName,
    pub table_opts: TableOpts,
}

impl TryFrom<TaggedAlterMaterializedViewStatement> for AlterMaterializedViewStatement {
    type Error = anyhow::Error;
    fn try_from(value: TaggedAlterMaterializedViewStatement) -> Result<Self, Self::Error> {
        Ok(Self {
            name: value.name.try_into()?,
            table_opts: value.table_opts.into_value()?,
        })
    }
}

#[derive(ParseFromStr, Builder, Clone, Debug, ToTokens, PartialEq)]
#[tokenize_as(AlterMaterializedViewStatement)]
pub struct TaggedAlterMaterializedViewStatement {
    pub name: TaggedKeyspaceQualifiedName,
    pub table_opts: Tag<TableOpts>,
}

impl Parse for TaggedAlterMaterializedViewStatement {
    type Output = Self;
    fn parse(s: &mut StatementStream<'_>) -> anyhow::Result<Self::Output> {
        s.parse::<(ALTER, MATERIALIZED, VIEW)>()?;
        let mut res = TaggedAlterMaterializedViewStatementBuilder::default();
        res.name(s.parse()?).table_opts(s.parse::<(WITH, _)>()?.1);
        s.parse::<Option<Semicolon>>()?;
        Ok(res
            .build()
            .map_err(|e| anyhow::anyhow!("Invalid ALTER MATERIALIZED VIEW statement: {}", e))?)
    }
}

impl Display for AlterMaterializedViewStatement {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "ALTER MATERIALIZED VIEW {} WITH {}", self.name, self.table_opts)
    }
}

#[derive(ParseFromStr, Builder, Clone, Debug, ToTokens, PartialEq, Eq)]
#[parse_via(TaggedDropMaterializedViewStatement)]
pub struct DropMaterializedViewStatement {
    #[builder(setter(name = "set_if_exists"), default)]
    pub if_exists: bool,
    #[builder(setter(into))]
    pub name: KeyspaceQualifiedName,
}

impl TryFrom<TaggedDropMaterializedViewStatement> for DropMaterializedViewStatement {
    type Error = anyhow::Error;
    fn try_from(value: TaggedDropMaterializedViewStatement) -> Result<Self, Self::Error> {
        Ok(Self {
            if_exists: value.if_exists,
            name: value.name.try_into()?,
        })
    }
}

#[derive(ParseFromStr, Builder, Clone, Debug, ToTokens, PartialEq, Eq)]
#[tokenize_as(DropMaterializedViewStatement)]
pub struct TaggedDropMaterializedViewStatement {
    #[builder(setter(name = "set_if_exists"), default)]
    pub if_exists: bool,
    pub name: TaggedKeyspaceQualifiedName,
}

impl DropMaterializedViewStatementBuilder {
    /// Set IF EXISTS on the statement.
    /// To undo this, use `set_if_exists(false)`.
    pub fn if_exists(&mut self) -> &mut Self {
        self.if_exists.replace(true);
        self
    }
}

impl Parse for TaggedDropMaterializedViewStatement {
    type Output = Self;
    fn parse(s: &mut StatementStream<'_>) -> anyhow::Result<Self::Output> {
        s.parse::<(DROP, MATERIALIZED, VIEW)>()?;
        let mut res = TaggedDropMaterializedViewStatementBuilder::default();
        res.set_if_exists(s.parse::<Option<(IF, EXISTS)>>()?.is_some())
            .name(s.parse()?);
        s.parse::<Option<Semicolon>>()?;
        Ok(res
            .build()
            .map_err(|e| anyhow::anyhow!("Invalid DROP MATERIALIZED VIEW statement: {}", e))?)
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
    use crate::KeyspaceQualifyExt;

    #[test]
    fn test_parse_create_mv() {
        let mut builder = CreateMaterializedViewStatementBuilder::default();
        builder.name("my_keyspace".dot("test_mv"));
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
        let statement = builder.build().unwrap().to_string();
        assert_eq!(builder.build().unwrap(), statement.parse().unwrap());
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

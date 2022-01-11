// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use super::*;
use crate::{
    ColumnDefinition,
    Constant,
    PrimaryKey,
    StatementOptValue,
    TerminatingList,
};

#[derive(ParseFromStr, Clone, Debug, TryInto, From, ToTokens, PartialEq)]
#[parse_via(TaggedDataDefinitionStatement)]
pub enum DataDefinitionStatement {
    Use(UseStatement),
    CreateKeyspace(CreateKeyspaceStatement),
    AlterKeyspace(AlterKeyspaceStatement),
    DropKeyspace(DropKeyspaceStatement),
    CreateTable(CreateTableStatement),
    AlterTable(AlterTableStatement),
    DropTable(DropTableStatement),
    Truncate(TruncateStatement),
}

impl TryFrom<TaggedDataDefinitionStatement> for DataDefinitionStatement {
    type Error = anyhow::Error;
    fn try_from(value: TaggedDataDefinitionStatement) -> Result<Self, Self::Error> {
        Ok(match value {
            TaggedDataDefinitionStatement::Use(value) => DataDefinitionStatement::Use(value.try_into()?),
            TaggedDataDefinitionStatement::CreateKeyspace(value) => {
                DataDefinitionStatement::CreateKeyspace(value.try_into()?)
            }
            TaggedDataDefinitionStatement::AlterKeyspace(value) => {
                DataDefinitionStatement::AlterKeyspace(value.try_into()?)
            }
            TaggedDataDefinitionStatement::DropKeyspace(value) => {
                DataDefinitionStatement::DropKeyspace(value.try_into()?)
            }
            TaggedDataDefinitionStatement::CreateTable(value) => {
                DataDefinitionStatement::CreateTable(value.try_into()?)
            }
            TaggedDataDefinitionStatement::AlterTable(value) => DataDefinitionStatement::AlterTable(value.try_into()?),
            TaggedDataDefinitionStatement::DropTable(value) => DataDefinitionStatement::DropTable(value.try_into()?),
            TaggedDataDefinitionStatement::Truncate(value) => DataDefinitionStatement::Truncate(value.try_into()?),
        })
    }
}

#[derive(ParseFromStr, Clone, Debug, TryInto, From, ToTokens, PartialEq)]
#[tokenize_as(DataDefinitionStatement)]
pub enum TaggedDataDefinitionStatement {
    Use(TaggedUseStatement),
    CreateKeyspace(TaggedCreateKeyspaceStatement),
    AlterKeyspace(TaggedAlterKeyspaceStatement),
    DropKeyspace(TaggedDropKeyspaceStatement),
    CreateTable(TaggedCreateTableStatement),
    AlterTable(TaggedAlterTableStatement),
    DropTable(TaggedDropTableStatement),
    Truncate(TaggedTruncateStatement),
}

impl Parse for TaggedDataDefinitionStatement {
    type Output = Self;
    fn parse(s: &mut StatementStream<'_>) -> anyhow::Result<Self::Output> {
        Ok(if let Some(keyword) = s.find::<ReservedKeyword>() {
            match keyword {
                ReservedKeyword::USE => Self::Use(s.parse()?),
                ReservedKeyword::CREATE | ReservedKeyword::ALTER | ReservedKeyword::DROP => {
                    if let Some((_, keyword2)) = s.find::<(ReservedKeyword, ReservedKeyword)>() {
                        match (keyword, keyword2) {
                            (ReservedKeyword::CREATE, ReservedKeyword::KEYSPACE) => Self::CreateKeyspace(s.parse()?),
                            (ReservedKeyword::CREATE, ReservedKeyword::TABLE) => Self::CreateTable(s.parse()?),
                            (ReservedKeyword::ALTER, ReservedKeyword::KEYSPACE) => Self::AlterKeyspace(s.parse()?),
                            (ReservedKeyword::ALTER, ReservedKeyword::TABLE) => Self::AlterTable(s.parse()?),
                            (ReservedKeyword::DROP, ReservedKeyword::KEYSPACE) => Self::DropKeyspace(s.parse()?),
                            (ReservedKeyword::DROP, ReservedKeyword::TABLE) => Self::DropTable(s.parse()?),
                            _ => anyhow::bail!("Unexpected keyword following {}: {}", keyword, keyword2),
                        }
                    } else {
                        anyhow::bail!("Unexpected token following {}: {}", keyword, s.info())
                    }
                }
                ReservedKeyword::TRUNCATE => Self::Truncate(s.parse()?),
                _ => anyhow::bail!("Expected a data definition statement, found {}", s.info()),
            }
        } else {
            anyhow::bail!("Expected a data definition statement, found {}", s.info())
        })
    }
}

impl Display for DataDefinitionStatement {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Use(s) => s.fmt(f),
            Self::CreateKeyspace(s) => s.fmt(f),
            Self::AlterKeyspace(s) => s.fmt(f),
            Self::DropKeyspace(s) => s.fmt(f),
            Self::CreateTable(s) => s.fmt(f),
            Self::AlterTable(s) => s.fmt(f),
            Self::DropTable(s) => s.fmt(f),
            Self::Truncate(s) => s.fmt(f),
        }
    }
}

#[derive(ParseFromStr, Clone, Debug, ToTokens, PartialEq, Eq)]
#[parse_via(TaggedUseStatement)]
pub struct UseStatement {
    pub keyspace: Name,
}

impl TryFrom<TaggedUseStatement> for UseStatement {
    type Error = anyhow::Error;
    fn try_from(value: TaggedUseStatement) -> Result<Self, Self::Error> {
        Ok(Self {
            keyspace: value.keyspace.into_value()?,
        })
    }
}

#[derive(ParseFromStr, Clone, Debug, ToTokens, PartialEq, Eq)]
#[tokenize_as(UseStatement)]
pub struct TaggedUseStatement {
    pub keyspace: Tag<Name>,
}

impl Parse for TaggedUseStatement {
    type Output = Self;
    fn parse(s: &mut StatementStream<'_>) -> anyhow::Result<Self::Output> {
        s.parse::<USE>()?;
        let keyspace = s.parse()?;
        s.parse::<Option<Semicolon>>()?;
        Ok(Self { keyspace })
    }
}

impl Display for UseStatement {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "USE {}", self.keyspace)
    }
}

impl<T: Into<Name>> From<T> for UseStatement {
    fn from(name: T) -> Self {
        Self { keyspace: name.into() }
    }
}

#[derive(Builder, Clone, Debug, ToTokens, PartialEq, Eq, Default)]
pub struct KeyspaceOpts {
    #[builder(setter(into))]
    pub replication: Replication,
    #[builder(setter(strip_option), default)]
    pub durable_writes: Option<bool>,
}

impl Parse for KeyspaceOpts {
    type Output = Self;
    fn parse(s: &mut StatementStream<'_>) -> anyhow::Result<Self::Output> {
        let mut res = KeyspaceOptsBuilder::default();

        for StatementOpt { name, value } in s.parse_from::<List<StatementOpt, AND>>()? {
            let (Name::Quoted(n) | Name::Unquoted(n)) = &name;
            match n.as_str() {
                "replication" => {
                    if res.replication.is_some() {
                        anyhow::bail!("Duplicate replication option");
                    } else if let StatementOptValue::Map(m) = value {
                        res.replication(Replication::try_from(m)?);
                    } else {
                        anyhow::bail!("Invalid replication value: {}", value);
                    }
                }
                "durable_writes" => {
                    if res.durable_writes.is_some() {
                        anyhow::bail!("Duplicate durable writes option");
                    } else if let StatementOptValue::Constant(Constant::Boolean(b)) = value {
                        res.durable_writes(b);
                    } else {
                        anyhow::bail!("Invalid durable writes value: {}", value);
                    }
                }
                _ => anyhow::bail!("Invalid table option: {}", name),
            }
        }
        Ok(res
            .build()
            .map_err(|e| anyhow::anyhow!("Invalid Keyspace Options: {}", e))?)
    }
}

impl Display for KeyspaceOpts {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "replication = {}", self.replication)?;
        if let Some(d) = self.durable_writes {
            write!(f, " AND durable_writes = {}", d)?;
        }
        Ok(())
    }
}

#[derive(ParseFromStr, Builder, Clone, Debug, ToTokens, PartialEq, Eq)]
#[parse_via(TaggedCreateKeyspaceStatement)]
pub struct CreateKeyspaceStatement {
    #[builder(setter(name = "set_if_not_exists"), default)]
    pub if_not_exists: bool,
    #[builder(setter(into))]
    pub keyspace: Name,
    pub options: KeyspaceOpts,
}

impl TryFrom<TaggedCreateKeyspaceStatement> for CreateKeyspaceStatement {
    type Error = anyhow::Error;
    fn try_from(value: TaggedCreateKeyspaceStatement) -> Result<Self, Self::Error> {
        Ok(Self {
            if_not_exists: value.if_not_exists,
            keyspace: value.keyspace.into_value()?,
            options: value.options,
        })
    }
}

#[derive(ParseFromStr, Builder, Clone, Debug, ToTokens, PartialEq, Eq)]
#[tokenize_as(CreateKeyspaceStatement)]
pub struct TaggedCreateKeyspaceStatement {
    #[builder(setter(name = "set_if_not_exists"), default)]
    pub if_not_exists: bool,
    pub keyspace: Tag<Name>,
    pub options: KeyspaceOpts,
}

impl CreateKeyspaceStatementBuilder {
    /// Set IF NOT EXISTS on the statement.
    /// To undo this, use `set_if_not_exists(false)`.
    pub fn if_not_exists(&mut self) -> &mut Self {
        self.if_not_exists.replace(true);
        self
    }
}

impl Parse for TaggedCreateKeyspaceStatement {
    type Output = Self;
    fn parse(s: &mut StatementStream<'_>) -> anyhow::Result<Self::Output> {
        s.parse::<(CREATE, KEYSPACE)>()?;
        let mut res = TaggedCreateKeyspaceStatementBuilder::default();
        res.set_if_not_exists(s.parse::<Option<(IF, NOT, EXISTS)>>()?.is_some())
            .keyspace(s.parse()?);
        s.parse::<WITH>()?;
        res.options(s.parse()?);
        s.parse::<Option<Semicolon>>()?;
        Ok(res
            .build()
            .map_err(|e| anyhow::anyhow!("Invalid CREATE KEYSPACE statement: {}", e))?)
    }
}

impl Display for CreateKeyspaceStatement {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "CREATE KEYSPACE{} {} WITH {}",
            if self.if_not_exists { " IF NOT EXISTS" } else { "" },
            self.keyspace,
            self.options
        )
    }
}

impl KeyspaceOptionsExt for CreateKeyspaceStatement {
    fn keyspace_opts(&self) -> &KeyspaceOpts {
        &self.options
    }

    fn keyspace_opts_mut(&mut self) -> &mut KeyspaceOpts {
        &mut self.options
    }
}

#[derive(ParseFromStr, Builder, Clone, Debug, ToTokens, PartialEq, Eq)]
#[parse_via(TaggedAlterKeyspaceStatement)]
pub struct AlterKeyspaceStatement {
    #[builder(setter(into))]
    pub keyspace: Name,
    pub options: KeyspaceOpts,
}

impl TryFrom<TaggedAlterKeyspaceStatement> for AlterKeyspaceStatement {
    type Error = anyhow::Error;
    fn try_from(value: TaggedAlterKeyspaceStatement) -> Result<Self, Self::Error> {
        Ok(Self {
            keyspace: value.keyspace.into_value()?,
            options: value.options,
        })
    }
}

#[derive(ParseFromStr, Builder, Clone, Debug, ToTokens, PartialEq, Eq)]
#[tokenize_as(AlterKeyspaceStatement)]
pub struct TaggedAlterKeyspaceStatement {
    pub keyspace: Tag<Name>,
    pub options: KeyspaceOpts,
}

impl Parse for TaggedAlterKeyspaceStatement {
    type Output = Self;
    fn parse(s: &mut StatementStream<'_>) -> anyhow::Result<Self::Output> {
        s.parse::<(ALTER, KEYSPACE)>()?;
        let mut res = TaggedAlterKeyspaceStatementBuilder::default();
        res.keyspace(s.parse()?);
        s.parse::<WITH>()?;
        res.options(s.parse()?);
        s.parse::<Option<Semicolon>>()?;
        Ok(res
            .build()
            .map_err(|e| anyhow::anyhow!("Invalid ALTER KEYSPACE statement: {}", e))?)
    }
}

impl Display for AlterKeyspaceStatement {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "ALTER KEYSPACE {} WITH {}", self.keyspace, self.options)
    }
}

#[derive(ParseFromStr, Builder, Clone, Debug, ToTokens, PartialEq, Eq)]
#[parse_via(TaggedDropKeyspaceStatement)]
pub struct DropKeyspaceStatement {
    #[builder(setter(name = "set_if_exists"), default)]
    pub if_exists: bool,
    #[builder(setter(into))]
    pub keyspace: Name,
}

impl TryFrom<TaggedDropKeyspaceStatement> for DropKeyspaceStatement {
    type Error = anyhow::Error;
    fn try_from(value: TaggedDropKeyspaceStatement) -> Result<Self, Self::Error> {
        Ok(Self {
            if_exists: value.if_exists,
            keyspace: value.keyspace.into_value()?,
        })
    }
}

#[derive(ParseFromStr, Builder, Clone, Debug, ToTokens, PartialEq, Eq)]
#[tokenize_as(DropKeyspaceStatement)]
pub struct TaggedDropKeyspaceStatement {
    #[builder(setter(name = "set_if_exists"), default)]
    pub if_exists: bool,
    pub keyspace: Tag<Name>,
}

impl DropKeyspaceStatementBuilder {
    /// Set IF EXISTS on the statement.
    /// To undo this, use `set_if_exists(false)`.
    pub fn if_exists(&mut self) -> &mut Self {
        self.if_exists.replace(true);
        self
    }
}

impl Parse for TaggedDropKeyspaceStatement {
    type Output = Self;
    fn parse(s: &mut StatementStream<'_>) -> anyhow::Result<Self::Output> {
        s.parse::<(DROP, KEYSPACE)>()?;
        let mut res = TaggedDropKeyspaceStatementBuilder::default();
        res.set_if_exists(s.parse::<Option<(IF, EXISTS)>>()?.is_some())
            .keyspace(s.parse()?);
        s.parse::<Option<Semicolon>>()?;
        Ok(res
            .build()
            .map_err(|e| anyhow::anyhow!("Invalid DROP KEYSPACE statement: {}", e))?)
    }
}

impl Display for DropKeyspaceStatement {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "DROP KEYSPACE{} {}",
            if self.if_exists { " IF EXISTS" } else { "" },
            self.keyspace
        )
    }
}

impl<T: Into<Name>> From<T> for DropKeyspaceStatement {
    fn from(name: T) -> Self {
        Self {
            if_exists: Default::default(),
            keyspace: name.into(),
        }
    }
}

#[derive(ParseFromStr, Builder, Clone, Debug, ToTokens, PartialEq)]
#[builder(setter(strip_option), build_fn(validate = "Self::validate"))]
#[parse_via(TaggedCreateTableStatement)]
pub struct CreateTableStatement {
    #[builder(setter(name = "set_if_not_exists"), default)]
    pub if_not_exists: bool,
    #[builder(setter(into))]
    pub table: KeyspaceQualifiedName,
    pub columns: Vec<ColumnDefinition>,
    #[builder(setter(into), default)]
    pub primary_key: Option<PrimaryKey>,
    #[builder(default)]
    pub options: Option<TableOpts>,
}

impl TryFrom<TaggedCreateTableStatement> for CreateTableStatement {
    type Error = anyhow::Error;
    fn try_from(value: TaggedCreateTableStatement) -> Result<Self, Self::Error> {
        Ok(Self {
            if_not_exists: value.if_not_exists,
            table: value.table.try_into()?,
            columns: value.columns,
            primary_key: value.primary_key,
            options: value.options,
        })
    }
}

#[derive(ParseFromStr, Builder, Clone, Debug, ToTokens, PartialEq)]
#[builder(setter(strip_option), build_fn(validate = "Self::validate"))]
#[tokenize_as(CreateTableStatement)]
pub struct TaggedCreateTableStatement {
    #[builder(setter(name = "set_if_not_exists"), default)]
    pub if_not_exists: bool,
    pub table: TaggedKeyspaceQualifiedName,
    pub columns: Vec<ColumnDefinition>,
    #[builder(default)]
    pub primary_key: Option<PrimaryKey>,
    #[builder(default)]
    pub options: Option<TableOpts>,
}

impl CreateTableStatementBuilder {
    /// Set IF NOT EXISTS on the statement.
    /// To undo this, use `set_if_not_exists(false)`.
    pub fn if_not_exists(&mut self) -> &mut Self {
        self.if_not_exists.replace(true);
        self
    }

    fn validate(&self) -> Result<(), String> {
        if self.columns.as_ref().map(|s| s.is_empty()).unwrap_or(false) {
            return Err("Column definitions cannot be empty".to_string());
        }
        Ok(())
    }
}

impl TaggedCreateTableStatementBuilder {
    fn validate(&self) -> Result<(), String> {
        if self.columns.as_ref().map(|s| s.is_empty()).unwrap_or(false) {
            return Err("Column definitions cannot be empty".to_string());
        }
        Ok(())
    }
}

impl Parse for TaggedCreateTableStatement {
    type Output = Self;
    fn parse(s: &mut StatementStream<'_>) -> anyhow::Result<Self::Output> {
        s.parse::<(CREATE, TABLE)>()?;
        let mut res = TaggedCreateTableStatementBuilder::default();
        res.set_if_not_exists(s.parse::<Option<(IF, NOT, EXISTS)>>()?.is_some())
            .table(s.parse()?);
        s.parse::<LeftParen>()?;
        res.columns(s.parse_from::<TerminatingList<ColumnDefinition, Comma, (PRIMARY, KEY)>>()?);
        if let Some(p) = s.parse_from::<If<(PRIMARY, KEY), Parens<PrimaryKey>>>()? {
            res.primary_key(p);
        }
        s.parse::<RightParen>()?;
        if let Some(p) = s.parse_from::<If<WITH, TableOpts>>()? {
            res.options(p);
        }
        s.parse::<Option<Semicolon>>()?;
        Ok(res
            .build()
            .map_err(|e| anyhow::anyhow!("Invalid CREATE TABLE statement: {}", e))?)
    }
}

impl Display for CreateTableStatement {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "CREATE TABLE{} {} ({}",
            if self.if_not_exists { " IF NOT EXISTS" } else { "" },
            self.table,
            self.columns
                .iter()
                .map(|i| i.to_string())
                .collect::<Vec<_>>()
                .join(", "),
        )?;
        if let Some(ref pk) = self.primary_key {
            write!(f, ", PRIMARY KEY ({})", pk)?;
        }
        write!(f, ")")?;
        if let Some(ref options) = self.options {
            write!(f, " WITH {}", options)?;
        }
        Ok(())
    }
}

impl TableOptionsExt for CreateTableStatement {
    fn table_opts(&self) -> &Option<TableOpts> {
        &self.options
    }

    fn table_opts_mut(&mut self) -> &mut Option<TableOpts> {
        &mut self.options
    }
}

#[derive(ParseFromStr, Builder, Clone, Debug, ToTokens, PartialEq)]
#[parse_via(TaggedAlterTableStatement)]
pub struct AlterTableStatement {
    #[builder(setter(into))]
    pub table: KeyspaceQualifiedName,
    pub instruction: AlterTableInstruction,
}

impl TryFrom<TaggedAlterTableStatement> for AlterTableStatement {
    type Error = anyhow::Error;
    fn try_from(value: TaggedAlterTableStatement) -> Result<Self, Self::Error> {
        Ok(Self {
            table: value.table.try_into()?,
            instruction: value.instruction,
        })
    }
}

#[derive(ParseFromStr, Builder, Clone, Debug, ToTokens, PartialEq)]
#[tokenize_as(AlterTableStatement)]
pub struct TaggedAlterTableStatement {
    pub table: TaggedKeyspaceQualifiedName,
    pub instruction: AlterTableInstruction,
}

impl Parse for TaggedAlterTableStatement {
    type Output = Self;
    fn parse(s: &mut StatementStream<'_>) -> anyhow::Result<Self::Output> {
        s.parse::<(ALTER, TABLE)>()?;
        let mut res = TaggedAlterTableStatementBuilder::default();
        res.table(s.parse()?).instruction(s.parse()?);
        s.parse::<Option<Semicolon>>()?;
        Ok(res
            .build()
            .map_err(|e| anyhow::anyhow!("Invalid ALTER TABLE statement: {}", e))?)
    }
}

impl Display for AlterTableStatement {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "ALTER TABLE {} {}", self.table, self.instruction)
    }
}

#[derive(ParseFromStr, Clone, Debug, ToTokens, PartialEq)]
pub enum AlterTableInstruction {
    Add(Vec<ColumnDefinition>),
    Drop(Vec<Name>),
    Alter(Name, CqlType),
    With(TableOpts),
}

impl AlterTableInstruction {
    pub fn add<T: Into<ColumnDefinition>>(defs: Vec<T>) -> anyhow::Result<Self> {
        if defs.is_empty() {
            anyhow::bail!("Column definitions cannot be empty");
        }
        Ok(AlterTableInstruction::Add(defs.into_iter().map(|i| i.into()).collect()))
    }

    pub fn drop<T: Into<Name>>(names: Vec<T>) -> anyhow::Result<Self> {
        if names.is_empty() {
            anyhow::bail!("Column names cannot be empty");
        }
        Ok(AlterTableInstruction::Drop(
            names.into_iter().map(|i| i.into()).collect(),
        ))
    }

    pub fn alter<N: Into<Name>, T: Into<CqlType>>(name: N, cql_type: T) -> Self {
        AlterTableInstruction::Alter(name.into(), cql_type.into())
    }

    pub fn with(opts: TableOpts) -> Self {
        AlterTableInstruction::With(opts)
    }
}

impl Parse for AlterTableInstruction {
    type Output = Self;
    fn parse(s: &mut StatementStream<'_>) -> anyhow::Result<Self::Output> {
        Ok(
            if s.parse::<Option<ADD>>()?.is_some() || s.check::<ColumnDefinition>() {
                Self::Add(s.parse_from::<List<ColumnDefinition, Comma>>()?)
            } else if s.parse::<Option<DROP>>()?.is_some() {
                if let Some(columns) = s.parse_from::<Option<Parens<List<Name, Comma>>>>()? {
                    Self::Drop(columns)
                } else {
                    Self::Drop(vec![s.parse()?])
                }
            } else if s.parse::<Option<ALTER>>()?.is_some() {
                let (col, _, ty) = s.parse::<(_, TYPE, _)>()?;
                Self::Alter(col, ty)
            } else if s.parse::<Option<WITH>>()?.is_some() {
                Self::With(s.parse_from::<TableOpts>()?)
            } else {
                anyhow::bail!("Invalid ALTER TABLE instruction: {}", s.info());
            },
        )
    }
}

impl Display for AlterTableInstruction {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Add(cols) => write!(
                f,
                "ADD {}",
                cols.iter().map(|i| i.to_string()).collect::<Vec<_>>().join(", ")
            ),
            Self::Drop(cols) => write!(
                f,
                "DROP ({})",
                cols.iter().map(|i| i.to_string()).collect::<Vec<_>>().join(", ")
            ),
            Self::Alter(col, ty) => write!(f, "ALTER {} TYPE {}", col, ty),
            Self::With(options) => write!(f, "WITH {}", options),
        }
    }
}

#[derive(ParseFromStr, Builder, Clone, Debug, ToTokens, PartialEq, Eq)]
#[parse_via(TaggedDropTableStatement)]
pub struct DropTableStatement {
    #[builder(setter(name = "set_if_exists"), default)]
    pub if_exists: bool,
    #[builder(setter(into))]
    pub table: KeyspaceQualifiedName,
}

impl TryFrom<TaggedDropTableStatement> for DropTableStatement {
    type Error = anyhow::Error;
    fn try_from(value: TaggedDropTableStatement) -> Result<Self, Self::Error> {
        Ok(Self {
            if_exists: value.if_exists,
            table: value.table.try_into()?,
        })
    }
}

#[derive(ParseFromStr, Builder, Clone, Debug, ToTokens, PartialEq, Eq)]
#[tokenize_as(DropTableStatement)]
pub struct TaggedDropTableStatement {
    #[builder(setter(name = "set_if_exists"), default)]
    pub if_exists: bool,
    pub table: TaggedKeyspaceQualifiedName,
}

impl DropTableStatementBuilder {
    /// Set IF EXISTS on the statement.
    /// To undo this, use `set_if_exists(false)`.
    pub fn if_exists(&mut self) -> &mut Self {
        self.if_exists.replace(true);
        self
    }
}

impl Parse for TaggedDropTableStatement {
    type Output = Self;
    fn parse(s: &mut StatementStream<'_>) -> anyhow::Result<Self::Output> {
        s.parse::<(DROP, TABLE)>()?;
        let mut res = TaggedDropTableStatementBuilder::default();
        res.set_if_exists(s.parse::<Option<(IF, EXISTS)>>()?.is_some())
            .table(s.parse()?);
        s.parse::<Option<Semicolon>>()?;
        Ok(res
            .build()
            .map_err(|e| anyhow::anyhow!("Invalid DROP TABLE statement: {}", e))?)
    }
}

impl Display for DropTableStatement {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "DROP TABLE{} {}",
            if self.if_exists { " IF EXISTS" } else { "" },
            self.table,
        )
    }
}

impl<T: Into<KeyspaceQualifiedName>> From<T> for DropTableStatement {
    fn from(name: T) -> Self {
        Self {
            if_exists: Default::default(),
            table: name.into(),
        }
    }
}

#[derive(ParseFromStr, Builder, Clone, Debug, ToTokens, PartialEq, Eq)]
#[parse_via(TaggedTruncateStatement)]
pub struct TruncateStatement {
    #[builder(setter(into))]
    pub table: KeyspaceQualifiedName,
}

impl TryFrom<TaggedTruncateStatement> for TruncateStatement {
    type Error = anyhow::Error;
    fn try_from(value: TaggedTruncateStatement) -> Result<Self, Self::Error> {
        Ok(Self {
            table: value.table.try_into()?,
        })
    }
}

#[derive(ParseFromStr, Builder, Clone, Debug, ToTokens, PartialEq, Eq)]
#[tokenize_as(TruncateStatement)]
pub struct TaggedTruncateStatement {
    pub table: TaggedKeyspaceQualifiedName,
}

impl Parse for TaggedTruncateStatement {
    type Output = Self;
    fn parse(s: &mut StatementStream<'_>) -> anyhow::Result<Self::Output> {
        s.parse::<TRUNCATE>()?;
        let mut res = TaggedTruncateStatementBuilder::default();
        res.table(s.parse()?);
        s.parse::<Option<Semicolon>>()?;
        Ok(res
            .build()
            .map_err(|e| anyhow::anyhow!("Invalid TRUNCATE statement: {}", e))?)
    }
}

impl Display for TruncateStatement {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "TRUNCATE {}", self.table)
    }
}

impl<T: Into<KeyspaceQualifiedName>> From<T> for TruncateStatement {
    fn from(name: T) -> Self {
        Self { table: name.into() }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::{
        Compaction,
        Compression,
        JavaTimeUnit,
        KeyspaceQualifyExt,
        NativeType,
        SpeculativeRetry,
    };

    #[test]
    fn test_parse_create_table() {
        let mut builder = CreateTableStatementBuilder::default();
        assert!(builder.build().is_err());
        builder.table("test");
        assert!(builder.build().is_err());
        builder.columns(vec![
            ("ascii", NativeType::Ascii).into(),
            ("bigint", NativeType::Bigint).into(),
            ("blob", NativeType::Blob).into(),
            ("boolean", NativeType::Boolean).into(),
            ("counter", NativeType::Counter).into(),
            ("decimal", NativeType::Decimal).into(),
            ("double", NativeType::Double).into(),
            ("duration", NativeType::Duration).into(),
            ("float", NativeType::Float).into(),
            ("inet", NativeType::Inet).into(),
            ("int", NativeType::Int).into(),
            ("smallint", NativeType::Smallint).into(),
            ("text", NativeType::Text).into(),
            ("time", NativeType::Time).into(),
            ("timestamp", NativeType::Timestamp).into(),
            ("timeuuid", NativeType::Timeuuid).into(),
            ("tinyint", NativeType::Tinyint).into(),
            ("uuid", NativeType::Uuid).into(),
            ("varchar", NativeType::Varchar).into(),
            ("varint", NativeType::Varint).into(),
        ]);
        let statement = builder.build().unwrap().to_string();
        assert_eq!(builder.build().unwrap(), statement.parse().unwrap());
        builder.table("test".dot("test"));
        let statement = builder.build().unwrap().to_string();
        assert_eq!(builder.build().unwrap(), statement.parse().unwrap());
        builder.primary_key(vec!["tinyint", "int", "bigint"]);
        let statement = builder.build().unwrap().to_string();
        assert_eq!(builder.build().unwrap(), statement.parse().unwrap());
        builder.if_not_exists();
        let statement = builder.build().unwrap().to_string();
        assert_eq!(builder.build().unwrap(), statement.parse().unwrap());
        let mut opts_builder = crate::TableOptsBuilder::default();
        assert!(opts_builder.build().is_err());
        opts_builder.comment("test");
        builder.options(opts_builder.build().unwrap());
        let statement = builder.build().unwrap().to_string();
        assert_eq!(builder.build().unwrap(), statement.parse().unwrap());
        opts_builder.compaction(Compaction::size_tiered().enabled(false).build().unwrap().into());
        builder.options(opts_builder.build().unwrap());
        let statement = builder.build().unwrap().to_string();
        assert_eq!(builder.build().unwrap(), statement.parse().unwrap());
        opts_builder.compression(Compression::build().class("LZ4Compressor").build().unwrap());
        builder.options(opts_builder.build().unwrap());
        let statement = builder.build().unwrap().to_string();
        assert_eq!(builder.build().unwrap(), statement.parse().unwrap());
        opts_builder.default_time_to_live(0);
        builder.options(opts_builder.build().unwrap());
        let statement = builder.build().unwrap().to_string();
        assert_eq!(builder.build().unwrap(), statement.parse().unwrap());
        opts_builder.gc_grace_seconds(99999);
        builder.options(opts_builder.build().unwrap());
        let statement = builder.build().unwrap().to_string();
        assert_eq!(builder.build().unwrap(), statement.parse().unwrap());
        opts_builder.memtable_flush_period_in_ms(100);
        builder.options(opts_builder.build().unwrap());
        let statement = builder.build().unwrap().to_string();
        assert_eq!(builder.build().unwrap(), statement.parse().unwrap());
        opts_builder.read_repair(true);
        builder.options(opts_builder.build().unwrap());
        let statement = builder.build().unwrap().to_string();
        assert_eq!(builder.build().unwrap(), statement.parse().unwrap());
        opts_builder.read_repair(false);
        builder.options(opts_builder.build().unwrap());
        let statement = builder.build().unwrap().to_string();
        assert_eq!(builder.build().unwrap(), statement.parse().unwrap());
        opts_builder.speculative_retry(SpeculativeRetry::Percentile(99.0));
        builder.options(opts_builder.build().unwrap());
        let statement = builder.build().unwrap().to_string();
        assert_eq!(builder.build().unwrap(), statement.parse().unwrap());
    }

    #[test]
    fn test_parse_alter_table() {
        let mut builder = AlterTableStatementBuilder::default();
        builder.table("test");
        assert!(builder.build().is_err());
        builder.instruction(
            AlterTableInstruction::add(vec![
                ("ascii", NativeType::Ascii),
                ("bigint", NativeType::Bigint),
                ("blob", NativeType::Blob),
                ("boolean", NativeType::Boolean),
                ("counter", NativeType::Counter),
                ("decimal", NativeType::Decimal),
                ("double", NativeType::Double),
                ("duration", NativeType::Duration),
                ("float", NativeType::Float),
                ("inet", NativeType::Inet),
                ("int", NativeType::Int),
                ("smallint", NativeType::Smallint),
                ("text", NativeType::Text),
                ("time", NativeType::Time),
                ("timestamp", NativeType::Timestamp),
                ("timeuuid", NativeType::Timeuuid),
                ("tinyint", NativeType::Tinyint),
                ("uuid", NativeType::Uuid),
                ("varchar", NativeType::Varchar),
                ("varint", NativeType::Varint),
            ])
            .unwrap(),
        );
        let statement = builder.build().unwrap().to_string();
        assert_eq!(builder.build().unwrap(), statement.parse().unwrap());
        builder.table("test".dot("test"));
        let statement = builder.build().unwrap().to_string();
        assert_eq!(builder.build().unwrap(), statement.parse().unwrap());
        builder.instruction(AlterTableInstruction::alter("ascii", NativeType::Blob));
        let statement = builder.build().unwrap().to_string();
        assert_eq!(builder.build().unwrap(), statement.parse().unwrap());
        builder.instruction(AlterTableInstruction::drop(vec!["ascii", "timestamp", "varint"]).unwrap());
        let statement = builder.build().unwrap().to_string();
        assert_eq!(builder.build().unwrap(), statement.parse().unwrap());
        let mut opts_builder = crate::TableOptsBuilder::default();
        assert!(opts_builder.build().is_err());
        opts_builder
            .compaction(
                Compaction::leveled()
                    .enabled(false)
                    .tombstone_threshold(0.99)
                    .tombstone_compaction_interval(10)
                    .sstable_size_in_mb(2)
                    .fanout_size(4)
                    .log_all(true)
                    .unchecked_tombstone_compaction(false)
                    .only_purge_repaired_tombstone(true)
                    .min_threshold(1)
                    .max_threshold(10)
                    .build()
                    .unwrap()
                    .into(),
            )
            .compression(
                Compression::build()
                    .class("java.org.something.MyCompressorClass")
                    .chunk_length_in_kb(10)
                    .crc_check_chance(0.5)
                    .compression_level(1)
                    .enabled(true)
                    .build()
                    .unwrap(),
            )
            .default_time_to_live(0)
            .gc_grace_seconds(99999)
            .memtable_flush_period_in_ms(100)
            .read_repair(true)
            .speculative_retry(SpeculativeRetry::custom("3h30m"));
        builder.instruction(AlterTableInstruction::with(opts_builder.build().unwrap()));
        let statement = builder.build().unwrap().to_string();
        assert_eq!(builder.build().unwrap(), statement.parse().unwrap());
        opts_builder
            .compaction(
                Compaction::time_window()
                    .enabled(true)
                    .tombstone_threshold(0.05)
                    .tombstone_compaction_interval(2)
                    .compaction_window_unit(JavaTimeUnit::Days)
                    .compaction_window_size(2)
                    .unsafe_aggressive_sstable_expiration(true)
                    .log_all(false)
                    .unchecked_tombstone_compaction(true)
                    .only_purge_repaired_tombstone(false)
                    .min_threshold(1)
                    .max_threshold(10)
                    .build()
                    .unwrap()
                    .into(),
            )
            .read_repair(false)
            .speculative_retry(SpeculativeRetry::Always);
        builder.instruction(AlterTableInstruction::with(opts_builder.build().unwrap()));
        let statement = builder.build().unwrap().to_string();
        assert_eq!(builder.build().unwrap(), statement.parse().unwrap());
    }

    #[test]
    fn test_parse_drop_table() {
        let mut builder = DropTableStatementBuilder::default();
        builder.table("test");
        let statement = builder.build().unwrap().to_string();
        assert_eq!(builder.build().unwrap(), statement.parse().unwrap());
        builder.if_exists().table("test".dot("test"));
        let statement = builder.build().unwrap().to_string();
        assert_eq!(builder.build().unwrap(), statement.parse().unwrap());
    }

    #[test]
    fn test_parse_create_keyspace() {
        let mut builder = CreateKeyspaceStatementBuilder::default();
        builder.keyspace("test");
        assert!(builder.build().is_err());
        builder.if_not_exists();
        assert!(builder.build().is_err());
        builder.options(
            KeyspaceOptsBuilder::default()
                .replication(1)
                .durable_writes(true)
                .build()
                .unwrap(),
        );
        let statement = builder.build().unwrap().to_string();
        println!("{}", statement);
        assert_eq!(builder.build().unwrap(), statement.parse().unwrap());
        builder.options(
            KeyspaceOptsBuilder::default()
                .replication(Replication::network_topology(maplit::btreemap! {
                    "dc1" => 1,
                    "dc2" => 2,
                }))
                .durable_writes(false)
                .build()
                .unwrap(),
        );
        let statement = builder.build().unwrap().to_string();
        assert_eq!(builder.build().unwrap(), statement.parse().unwrap());
    }

    #[test]
    fn test_parse_alter_keyspace() {
        let mut builder = AlterKeyspaceStatementBuilder::default();
        builder.keyspace("test whitespace");
        assert!(builder.build().is_err());
        builder.options(KeyspaceOptsBuilder::default().replication(2).build().unwrap());
        let statement = builder.build().unwrap().to_string();
        assert_eq!(builder.build().unwrap(), statement.parse().unwrap());
        builder.options(
            KeyspaceOptsBuilder::default()
                .replication(Replication::network_topology(maplit::btreemap! {
                    "dc1" => 1,
                    "dc2" => 2,
                }))
                .durable_writes(true)
                .build()
                .unwrap(),
        );
        let statement = builder.build().unwrap().to_string();
        assert_eq!(builder.build().unwrap(), statement.parse().unwrap());
    }

    #[test]
    fn test_parse_drop_keyspace() {
        let mut builder = DropKeyspaceStatementBuilder::default();
        builder.keyspace("test");
        let statement = builder.build().unwrap().to_string();
        assert_eq!(builder.build().unwrap(), statement.parse().unwrap());
        builder.if_exists();
        let statement = builder.build().unwrap().to_string();
        assert_eq!(builder.build().unwrap(), statement.parse().unwrap());
    }
}

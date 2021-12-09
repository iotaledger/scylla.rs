// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use super::*;

#[derive(ParseFromStr, Clone, Debug, TryInto, From, ToTokens, PartialEq, Eq)]
#[parse_via(TaggedTriggerStatement)]
pub enum TriggerStatement {
    Create(CreateTriggerStatement),
    Drop(DropTriggerStatement),
}

impl TryFrom<TaggedTriggerStatement> for TriggerStatement {
    type Error = anyhow::Error;

    fn try_from(value: TaggedTriggerStatement) -> Result<Self, Self::Error> {
        Ok(match value {
            TaggedTriggerStatement::Create(value) => TriggerStatement::Create(value.try_into()?),
            TaggedTriggerStatement::Drop(value) => TriggerStatement::Drop(value.try_into()?),
        })
    }
}

#[derive(ParseFromStr, Clone, Debug, TryInto, From, ToTokens, PartialEq, Eq)]
#[tokenize_as(TriggerStatement)]
pub enum TaggedTriggerStatement {
    Create(TaggedCreateTriggerStatement),
    Drop(TaggedDropTriggerStatement),
}

impl Parse for TaggedTriggerStatement {
    type Output = Self;
    fn parse(s: &mut StatementStream<'_>) -> anyhow::Result<Self::Output> {
        Ok(if let Some(stmt) = s.parse::<Option<TaggedCreateTriggerStatement>>()? {
            Self::Create(stmt)
        } else if let Some(stmt) = s.parse::<Option<TaggedDropTriggerStatement>>()? {
            Self::Drop(stmt)
        } else {
            anyhow::bail!("Invalid TRIGGER statement!")
        })
    }
}

impl Display for TriggerStatement {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Create(stmt) => stmt.fmt(f),
            Self::Drop(stmt) => stmt.fmt(f),
        }
    }
}

#[derive(ParseFromStr, Builder, Clone, Debug, ToTokens, PartialEq, Eq)]
#[parse_via(TaggedCreateTriggerStatement)]
pub struct CreateTriggerStatement {
    #[builder(setter(name = "set_if_not_exists"), default)]
    pub if_not_exists: bool,
    #[builder(setter(into))]
    pub name: Name,
    #[builder(setter(into))]
    pub table: KeyspaceQualifiedName,
    #[builder(setter(into))]
    pub using: LitStr,
}

impl TryFrom<TaggedCreateTriggerStatement> for CreateTriggerStatement {
    type Error = anyhow::Error;

    fn try_from(value: TaggedCreateTriggerStatement) -> Result<Self, Self::Error> {
        Ok(Self {
            if_not_exists: value.if_not_exists,
            name: value.name.into_value()?,
            table: value.table.try_into()?,
            using: value.using.into_value()?,
        })
    }
}

#[derive(ParseFromStr, Builder, Clone, Debug, ToTokens, PartialEq, Eq)]
#[tokenize_as(CreateTriggerStatement)]
pub struct TaggedCreateTriggerStatement {
    #[builder(setter(name = "set_if_not_exists"), default)]
    pub if_not_exists: bool,
    pub name: Tag<Name>,
    pub table: TaggedKeyspaceQualifiedName,
    pub using: Tag<LitStr>,
}

impl CreateTriggerStatementBuilder {
    /// Set IF NOT EXISTS on the statement.
    /// To undo this, use `set_if_not_exists(false)`.
    pub fn if_not_exists(&mut self) -> &mut Self {
        self.if_not_exists.replace(true);
        self
    }
}

impl Parse for TaggedCreateTriggerStatement {
    type Output = Self;
    fn parse(s: &mut StatementStream<'_>) -> anyhow::Result<Self::Output> {
        s.parse::<(CREATE, TRIGGER)>()?;
        let mut res = TaggedCreateTriggerStatementBuilder::default();
        res.set_if_not_exists(s.parse::<Option<(IF, NOT, EXISTS)>>()?.is_some())
            .name(s.parse()?)
            .table(s.parse::<(ON, _)>()?.1)
            .using(s.parse::<(USING, _)>()?.1);
        s.parse::<Option<Semicolon>>()?;
        Ok(res
            .build()
            .map_err(|e| anyhow::anyhow!("Invalid CREATE TRIGGER statement: {}", e))?)
    }
}

impl Display for CreateTriggerStatement {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "CREATE TRIGGER{} {} ON {} USING {}",
            if self.if_not_exists { " IF NOT EXISTS" } else { "" },
            self.name,
            self.table,
            self.using
        )
    }
}

#[derive(ParseFromStr, Builder, Clone, Debug, ToTokens, PartialEq, Eq)]
#[parse_via(TaggedDropTriggerStatement)]
pub struct DropTriggerStatement {
    #[builder(setter(name = "set_if_exists"), default)]
    pub if_exists: bool,
    #[builder(setter(into))]
    pub name: Name,
    #[builder(setter(into))]
    pub table: KeyspaceQualifiedName,
}

impl TryFrom<TaggedDropTriggerStatement> for DropTriggerStatement {
    type Error = anyhow::Error;
    fn try_from(value: TaggedDropTriggerStatement) -> Result<Self, Self::Error> {
        Ok(Self {
            if_exists: value.if_exists,
            name: value.name.into_value()?,
            table: value.table.try_into()?,
        })
    }
}

#[derive(ParseFromStr, Builder, Clone, Debug, ToTokens, PartialEq, Eq)]
#[tokenize_as(DropTriggerStatement)]
pub struct TaggedDropTriggerStatement {
    #[builder(setter(name = "set_if_exists"), default)]
    pub if_exists: bool,
    pub name: Tag<Name>,
    pub table: TaggedKeyspaceQualifiedName,
}

impl DropTriggerStatementBuilder {
    /// Set IF EXISTS on the statement.
    /// To undo this, use `set_if_exists(false)`.
    pub fn if_exists(&mut self) -> &mut Self {
        self.if_exists.replace(true);
        self
    }
}

impl Parse for TaggedDropTriggerStatement {
    type Output = Self;
    fn parse(s: &mut StatementStream<'_>) -> anyhow::Result<Self::Output> {
        s.parse::<(DROP, TRIGGER)>()?;
        let mut res = TaggedDropTriggerStatementBuilder::default();
        res.set_if_exists(s.parse::<Option<(IF, EXISTS)>>()?.is_some())
            .name(s.parse()?)
            .table(s.parse::<(ON, _)>()?.1);
        s.parse::<Option<Semicolon>>()?;
        Ok(res
            .build()
            .map_err(|e| anyhow::anyhow!("Invalid DROP TRIGGER statement: {}", e))?)
    }
}

impl Display for DropTriggerStatement {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "DROP TRIGGER{} {} ON {}",
            if self.if_exists { " IF EXISTS" } else { "" },
            self.name,
            self.table
        )
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::KeyspaceQualifyExt;

    #[test]
    fn test_parse_create_trigger() {
        let mut builder = CreateTriggerStatementBuilder::default();
        builder.name("test_trigger");
        assert!(builder.build().is_err());
        builder.table("test_keyspace".dot("my_table"));
        assert!(builder.build().is_err());
        builder.using("test_trigger_function");
        let statement = builder.build().unwrap().to_string();
        assert_eq!(builder.build().unwrap(), statement.parse().unwrap());
        builder.if_not_exists();
        let statement = builder.build().unwrap().to_string();
        assert_eq!(builder.build().unwrap(), statement.parse().unwrap());
    }

    #[test]
    fn test_parse_drop_trigger() {
        let mut builder = DropTriggerStatementBuilder::default();
        builder.name("test_trigger");
        assert!(builder.build().is_err());
        builder.table("test_keyspace".dot("my_table"));
        let statement = builder.build().unwrap().to_string();
        assert_eq!(builder.build().unwrap(), statement.parse().unwrap());
        builder.if_exists();
        let statement = builder.build().unwrap().to_string();
        assert_eq!(builder.build().unwrap(), statement.parse().unwrap());
    }
}

// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use super::*;

#[derive(ParseFromStr, Clone, Debug, TryInto, From, ToTokens, PartialEq, Eq)]
#[parse_via(TaggedSecondaryIndexStatement)]
pub enum SecondaryIndexStatement {
    Create(CreateIndexStatement),
    Drop(DropIndexStatement),
}

impl TryFrom<TaggedSecondaryIndexStatement> for SecondaryIndexStatement {
    type Error = anyhow::Error;
    fn try_from(statement: TaggedSecondaryIndexStatement) -> Result<Self, Self::Error> {
        match statement {
            TaggedSecondaryIndexStatement::Create(statement) => {
                Ok(SecondaryIndexStatement::Create(statement.try_into()?))
            }
            TaggedSecondaryIndexStatement::Drop(statement) => Ok(SecondaryIndexStatement::Drop(statement.try_into()?)),
        }
    }
}

#[derive(ParseFromStr, Clone, Debug, TryInto, From, ToTokens, PartialEq, Eq)]
#[tokenize_as(SecondaryIndexStatement)]
pub enum TaggedSecondaryIndexStatement {
    Create(TaggedCreateIndexStatement),
    Drop(TaggedDropIndexStatement),
}

impl Parse for TaggedSecondaryIndexStatement {
    type Output = Self;
    fn parse(s: &mut StatementStream<'_>) -> anyhow::Result<Self::Output> {
        Ok(if s.check::<CREATE>() {
            Self::Create(s.parse()?)
        } else if s.check::<DROP>() {
            Self::Drop(s.parse()?)
        } else {
            anyhow::bail!("Expected a secondary index statement, found {}", s.info())
        })
    }
}

impl Display for SecondaryIndexStatement {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Create(stmt) => stmt.fmt(f),
            Self::Drop(stmt) => stmt.fmt(f),
        }
    }
}

#[derive(ParseFromStr, Builder, Clone, Debug, ToTokens, PartialEq, Eq)]
#[builder(setter(strip_option))]
#[parse_via(TaggedCreateIndexStatement)]
pub struct CreateIndexStatement {
    #[builder(setter(name = "set_custom"), default)]
    pub custom: bool,
    #[builder(setter(name = "set_if_not_exists"), default)]
    pub if_not_exists: bool,
    #[builder(setter(into), default)]
    pub name: Option<Name>,
    #[builder(setter(into))]
    pub table: KeyspaceQualifiedName,
    #[builder(setter(into))]
    pub index_id: IndexIdentifier,
    #[builder(default)]
    pub using: Option<IndexClass>,
}

impl TryFrom<TaggedCreateIndexStatement> for CreateIndexStatement {
    type Error = anyhow::Error;
    fn try_from(statement: TaggedCreateIndexStatement) -> Result<Self, Self::Error> {
        Ok(Self {
            custom: statement.custom,
            if_not_exists: statement.if_not_exists,
            name: statement.name.map(|v| v.into_value()).transpose()?,
            table: statement.table.try_into()?,
            index_id: statement.index_id.into_value()?,
            using: statement.using.map(|v| v.into_value()).transpose()?,
        })
    }
}

#[derive(ParseFromStr, Builder, Clone, Debug, ToTokens, PartialEq, Eq)]
#[builder(setter(strip_option))]
#[tokenize_as(CreateIndexStatement)]
pub struct TaggedCreateIndexStatement {
    #[builder(setter(name = "set_custom"), default)]
    pub custom: bool,
    #[builder(setter(name = "set_if_not_exists"), default)]
    pub if_not_exists: bool,
    #[builder(default)]
    pub name: Option<Tag<Name>>,
    pub table: TaggedKeyspaceQualifiedName,
    pub index_id: Tag<IndexIdentifier>,
    #[builder(default)]
    pub using: Option<Tag<IndexClass>>,
}

impl CreateIndexStatementBuilder {
    /// Set IF NOT EXISTS on the statement.
    /// To undo this, use `set_if_not_exists(false)`.
    pub fn if_not_exists(&mut self) -> &mut Self {
        self.if_not_exists.replace(true);
        self
    }

    // Set CUSTOM on the statement
    // To undo this, use `set_custom(false)`.
    pub fn custom(&mut self) -> &mut Self {
        self.custom.replace(true);
        self
    }
}

impl Parse for TaggedCreateIndexStatement {
    type Output = Self;
    fn parse(s: &mut StatementStream<'_>) -> anyhow::Result<Self::Output> {
        s.parse::<CREATE>()?;
        let mut res = TaggedCreateIndexStatementBuilder::default();
        res.set_custom(s.parse::<Option<CUSTOM>>()?.is_some());
        s.parse::<INDEX>()?;
        res.set_if_not_exists(s.parse::<Option<(IF, NOT, EXISTS)>>()?.is_some());
        if let Some(n) = s.parse()? {
            res.name(n);
        }
        s.parse::<ON>()?;
        res.table(s.parse()?)
            .index_id(s.parse_from::<Parens<Tag<IndexIdentifier>>>()?);
        if let Some(u) = s.parse()? {
            res.using(u);
        }
        s.parse::<Option<Semicolon>>()?;
        Ok(res
            .build()
            .map_err(|e| anyhow::anyhow!("Invalid CREATE INDEX statement: {}", e))?)
    }
}

impl Display for CreateIndexStatement {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "CREATE{} INDEX{}{} ON {}({})",
            if self.custom { " CUSTOM" } else { "" },
            if self.if_not_exists { " IF NOT EXISTS" } else { "" },
            if let Some(ref name) = self.name {
                format!(" {}", name)
            } else {
                String::new()
            },
            self.table,
            self.index_id
        )?;
        if let Some(ref using) = self.using {
            write!(f, " USING {}", using)?;
        }
        Ok(())
    }
}

#[derive(ParseFromStr, Clone, Debug, ToTokens, PartialEq, Eq)]
pub enum IndexIdentifier {
    Column(Name),
    Qualified(IndexQualifier, Name),
}

impl IndexIdentifier {
    pub fn keys(name: impl Into<Name>) -> Self {
        Self::Qualified(IndexQualifier::Keys, name.into())
    }

    pub fn values(name: impl Into<Name>) -> Self {
        Self::Qualified(IndexQualifier::Values, name.into())
    }

    pub fn entries(name: impl Into<Name>) -> Self {
        Self::Qualified(IndexQualifier::Entries, name.into())
    }

    pub fn full(name: impl Into<Name>) -> Self {
        Self::Qualified(IndexQualifier::Full, name.into())
    }
}

impl Parse for IndexIdentifier {
    type Output = Self;
    fn parse(s: &mut StatementStream<'_>) -> anyhow::Result<Self::Output> {
        Ok(if let Some(name) = s.parse()? {
            IndexIdentifier::Column(name)
        } else {
            IndexIdentifier::Qualified(s.parse()?, s.parse_from::<Parens<Name>>()?)
        })
    }
}

impl Display for IndexIdentifier {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            IndexIdentifier::Column(name) => name.fmt(f),
            IndexIdentifier::Qualified(qualifier, name) => write!(f, "{} ({})", qualifier, name),
        }
    }
}

impl<N: Into<Name>> From<N> for IndexIdentifier {
    fn from(name: N) -> Self {
        IndexIdentifier::Column(name.into())
    }
}

#[derive(ParseFromStr, Clone, Debug, ToTokens, PartialEq, Eq)]
pub enum IndexQualifier {
    Keys,
    Values,
    Entries,
    Full,
}

impl Parse for IndexQualifier {
    type Output = Self;
    fn parse(s: &mut StatementStream<'_>) -> anyhow::Result<Self::Output> {
        Ok(if s.parse::<Option<KEYS>>()?.is_some() {
            IndexQualifier::Keys
        } else if s.parse::<Option<VALUES>>()?.is_some() {
            IndexQualifier::Values
        } else if s.parse::<Option<ENTRIES>>()?.is_some() {
            IndexQualifier::Entries
        } else if s.parse::<Option<FULL>>()?.is_some() {
            IndexQualifier::Full
        } else {
            anyhow::bail!("Expected an index qualifier, found {}", s.info())
        })
    }
}

impl Display for IndexQualifier {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            IndexQualifier::Keys => write!(f, "KEYS"),
            IndexQualifier::Values => write!(f, "VALUES"),
            IndexQualifier::Entries => write!(f, "ENTRIES"),
            IndexQualifier::Full => write!(f, "FULL"),
        }
    }
}

#[derive(ParseFromStr, Clone, Debug, ToTokens, PartialEq, Eq)]
pub struct IndexClass {
    pub path: LitStr,
    pub options: Option<MapLiteral>,
}

impl IndexClass {
    pub fn new(path: impl Into<LitStr>) -> Self {
        Self {
            path: path.into(),
            options: None,
        }
    }

    pub fn options(mut self, options: impl Into<MapLiteral>) -> Self {
        self.options = Some(options.into());
        self
    }
}

impl Parse for IndexClass {
    type Output = Self;
    fn parse(s: &mut StatementStream<'_>) -> anyhow::Result<Self::Output> {
        s.parse::<USING>()?;
        Ok(IndexClass {
            path: s.parse()?,
            options: s.parse::<Option<(WITH, OPTIONS, Equals, _)>>()?.map(|i| i.3),
        })
    }
}

impl Display for IndexClass {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.path)?;
        if let Some(ref options) = self.options {
            write!(f, " WITH OPTIONS = {}", options)?;
        }
        Ok(())
    }
}

#[derive(ParseFromStr, Builder, Clone, Debug, ToTokens, PartialEq, Eq)]
#[parse_via(TaggedDropIndexStatement)]
pub struct DropIndexStatement {
    #[builder(setter(name = "set_if_exists"), default)]
    pub if_exists: bool,
    #[builder(setter(into))]
    pub name: Name,
}

impl TryFrom<TaggedDropIndexStatement> for DropIndexStatement {
    type Error = anyhow::Error;

    fn try_from(value: TaggedDropIndexStatement) -> anyhow::Result<Self> {
        Ok(Self {
            if_exists: value.if_exists,
            name: value.name.into_value()?,
        })
    }
}

#[derive(ParseFromStr, Builder, Clone, Debug, ToTokens, PartialEq, Eq)]
#[tokenize_as(DropIndexStatement)]
pub struct TaggedDropIndexStatement {
    #[builder(setter(name = "set_if_exists"), default)]
    pub if_exists: bool,
    pub name: Tag<Name>,
}

impl DropIndexStatementBuilder {
    /// Set IF EXISTS on the statement.
    /// To undo this, use `set_if_exists(false)`.
    pub fn if_exists(&mut self) -> &mut Self {
        self.if_exists.replace(true);
        self
    }
}

impl Parse for TaggedDropIndexStatement {
    type Output = Self;
    fn parse(s: &mut StatementStream<'_>) -> anyhow::Result<Self::Output> {
        s.parse::<(DROP, INDEX)>()?;
        let mut res = TaggedDropIndexStatementBuilder::default();
        res.set_if_exists(s.parse::<Option<(IF, EXISTS)>>()?.is_some());
        if let Some(n) = s.parse()? {
            res.name(n);
        }
        s.parse::<Option<Semicolon>>()?;
        Ok(res
            .build()
            .map_err(|e| anyhow::anyhow!("Invalid DROP INDEX statement: {}", e))?)
    }
}

impl Display for DropIndexStatement {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "DROP INDEX{} {}",
            if self.if_exists { " IF EXISTS" } else { "" },
            self.name
        )?;
        Ok(())
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_parse_create_index() {
        let mut builder = CreateIndexStatementBuilder::default();
        builder.table("test");
        assert!(builder.build().is_err());
        builder.index_id("my_index_id");
        let statement = builder.build().unwrap().to_string();
        assert_eq!(builder.build().unwrap(), statement.parse().unwrap());
        builder.name("my_index_name");
        let statement = builder.build().unwrap().to_string();
        assert_eq!(builder.build().unwrap(), statement.parse().unwrap());
        builder.custom();
        let statement = builder.build().unwrap().to_string();
        assert_eq!(builder.build().unwrap(), statement.parse().unwrap());
        builder.if_not_exists();
        let statement = builder.build().unwrap().to_string();
        assert_eq!(builder.build().unwrap(), statement.parse().unwrap());
        builder.using(IndexClass::new("path.to.the.IndexClass").options(maplit::hashmap! {
            LitStr::from("storage") => LitStr::from("/mnt/ssd/indexes/")
        }));
        let statement = builder.build().unwrap().to_string();
        assert_eq!(builder.build().unwrap(), statement.parse().unwrap());
    }

    #[test]
    fn test_parse_drop_index() {
        let mut builder = DropIndexStatementBuilder::default();
        builder.name("my_index_name");
        let statement = builder.build().unwrap().to_string();
        assert_eq!(builder.build().unwrap(), statement.parse().unwrap());
        builder.if_exists();
        let statement = builder.build().unwrap().to_string();
        assert_eq!(builder.build().unwrap(), statement.parse().unwrap());
    }
}

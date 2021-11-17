use super::*;

#[derive(ParseFromStr, Clone, Debug, TryInto, From)]
pub enum SecondaryIndexStatement {
    Create(CreateIndexStatement),
    Drop(DropIndexStatement),
}

impl Parse for SecondaryIndexStatement {
    type Output = Self;
    fn parse(s: &mut StatementStream<'_>) -> anyhow::Result<Self::Output> {
        Ok(if let Some(stmt) = s.parse_if::<CreateIndexStatement>() {
            Self::Create(stmt?)
        } else if let Some(stmt) = s.parse_if::<DropIndexStatement>() {
            Self::Drop(stmt?)
        } else {
            anyhow::bail!("Expected a data manipulation statement!")
        })
    }
}

impl Peek for SecondaryIndexStatement {
    fn peek(s: StatementStream<'_>) -> bool {
        s.check::<CreateIndexStatement>() || s.check::<DropIndexStatement>()
    }
}

#[derive(ParseFromStr, Builder, Clone, Debug)]
pub struct CreateIndexStatement {
    #[builder(default)]
    pub custom: bool,
    #[builder(default)]
    pub if_not_exists: bool,
    #[builder(default)]
    pub name: Option<Name>,
    pub table: KeyspaceQualifiedName,
    pub index_id: IndexIdentifier,
    #[builder(default)]
    pub using: Option<IndexClass>,
}

impl Parse for CreateIndexStatement {
    type Output = Self;
    fn parse(s: &mut StatementStream<'_>) -> anyhow::Result<Self::Output> {
        s.parse::<CREATE>()?;
        let mut res = CreateIndexStatementBuilder::default();
        res.custom(s.parse::<Option<CUSTOM>>()?.is_some());
        s.parse::<INDEX>()?;
        res.if_not_exists(s.parse::<Option<(IF, NOT, EXISTS)>>()?.is_some());
        res.name(s.parse::<Option<Name>>()?);
        s.parse::<ON>()?;
        res.table(s.parse::<KeyspaceQualifiedName>()?)
            .index_id(s.parse_from::<Parens<IndexIdentifier>>()?);
        res.using(s.parse::<Option<IndexClass>>()?);
        Ok(res
            .build()
            .map_err(|e| anyhow::anyhow!("Invalid CREATE INDEX statement: {}", e))?)
    }
}

impl Peek for CreateIndexStatement {
    fn peek(s: StatementStream<'_>) -> bool {
        s.check::<(CREATE, Option<CUSTOM>, INDEX)>()
    }
}

#[derive(ParseFromStr, Clone, Debug)]
pub enum IndexIdentifier {
    Column(Name),
    Qualified(IndexQualifier, Name),
}

impl Parse for IndexIdentifier {
    type Output = Self;
    fn parse(s: &mut StatementStream<'_>) -> anyhow::Result<Self::Output> {
        Ok(if let Some(name) = s.parse_if::<Name>() {
            IndexIdentifier::Column(name?)
        } else {
            IndexIdentifier::Qualified(s.parse()?, s.parse_from::<Parens<Name>>()?)
        })
    }
}

#[derive(ParseFromStr, Clone, Debug)]
pub enum IndexQualifier {
    Keys,
    Values,
    Entries,
    Full,
}

impl Parse for IndexQualifier {
    type Output = Self;
    fn parse(s: &mut StatementStream<'_>) -> anyhow::Result<Self::Output> {
        Ok(if s.parse_if::<KEYS>().is_some() {
            IndexQualifier::Keys
        } else if s.parse_if::<VALUES>().is_some() {
            IndexQualifier::Values
        } else if s.parse_if::<ENTRIES>().is_some() {
            IndexQualifier::Entries
        } else if s.parse_if::<FULL>().is_some() {
            IndexQualifier::Full
        } else {
            anyhow::bail!("Expected an index qualifier!")
        })
    }
}

#[derive(ParseFromStr, Clone, Debug)]
pub struct IndexClass {
    pub path: String,
    pub options: Option<MapLiteral>,
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

impl Peek for IndexClass {
    fn peek(s: StatementStream<'_>) -> bool {
        s.check::<USING>()
    }
}

#[derive(ParseFromStr, Builder, Clone, Debug)]
pub struct DropIndexStatement {
    #[builder(default)]
    pub if_exists: bool,
    pub name: Name,
}

impl Parse for DropIndexStatement {
    type Output = Self;
    fn parse(s: &mut StatementStream<'_>) -> anyhow::Result<Self::Output> {
        s.parse::<(DROP, INDEX)>()?;
        let mut res = DropIndexStatementBuilder::default();
        res.if_exists(s.parse::<Option<(IF, EXISTS)>>()?.is_some())
            .name(s.parse()?);
        Ok(res
            .build()
            .map_err(|e| anyhow::anyhow!("Invalid DROP INDEX statement: {}", e))?)
    }
}

impl Peek for DropIndexStatement {
    fn peek(s: StatementStream<'_>) -> bool {
        s.check::<(DROP, INDEX)>()
    }
}

use super::*;

#[derive(ParseFromStr, Clone, Debug, TryInto, From)]
pub enum SecondaryIndexStatement {
    Create(CreateIndexStatement),
    Drop(DropIndexStatement),
}

impl Parse for SecondaryIndexStatement {
    type Output = Self;
    fn parse(s: &mut StatementStream<'_>) -> anyhow::Result<Self::Output> {
        Ok(if let Some(stmt) = s.parse::<Option<CreateIndexStatement>>()? {
            Self::Create(stmt)
        } else if let Some(stmt) = s.parse::<Option<DropIndexStatement>>()? {
            Self::Drop(stmt)
        } else {
            anyhow::bail!("Expected a data manipulation statement, found {}", s.info())
        })
    }
}

impl Peek for SecondaryIndexStatement {
    fn peek(s: StatementStream<'_>) -> bool {
        s.check::<CreateIndexStatement>() || s.check::<DropIndexStatement>()
    }
}

#[derive(ParseFromStr, Builder, Clone, Debug)]
#[builder(setter(strip_option))]
pub struct CreateIndexStatement {
    #[builder(default)]
    pub custom: bool,
    #[builder(default)]
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

impl Parse for CreateIndexStatement {
    type Output = Self;
    fn parse(s: &mut StatementStream<'_>) -> anyhow::Result<Self::Output> {
        s.parse::<CREATE>()?;
        let mut res = CreateIndexStatementBuilder::default();
        res.custom(s.parse::<Option<CUSTOM>>()?.is_some());
        s.parse::<INDEX>()?;
        res.if_not_exists(s.parse::<Option<(IF, NOT, EXISTS)>>()?.is_some());
        if let Some(n) = s.parse::<Option<Name>>()? {
            res.name(n);
        }
        s.parse::<ON>()?;
        res.table(s.parse::<KeyspaceQualifiedName>()?)
            .index_id(s.parse_from::<Parens<IndexIdentifier>>()?);
        if let Some(u) = s.parse::<Option<IndexClass>>()? {
            res.using(u);
        }
        s.parse::<Option<Semicolon>>()?;
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

#[derive(ParseFromStr, Clone, Debug)]
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

#[derive(ParseFromStr, Clone, Debug)]
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

impl Peek for IndexClass {
    fn peek(s: StatementStream<'_>) -> bool {
        s.check::<USING>()
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

#[derive(ParseFromStr, Builder, Clone, Debug)]
pub struct DropIndexStatement {
    #[builder(default)]
    pub if_exists: bool,
    #[builder(setter(into))]
    pub name: Name,
}

impl Parse for DropIndexStatement {
    type Output = Self;
    fn parse(s: &mut StatementStream<'_>) -> anyhow::Result<Self::Output> {
        s.parse::<(DROP, INDEX)>()?;
        let mut res = DropIndexStatementBuilder::default();
        res.if_exists(s.parse::<Option<(IF, EXISTS)>>()?.is_some());
        if let Some(n) = s.parse::<Option<Name>>()? {
            res.name(n);
        }
        s.parse::<Option<Semicolon>>()?;
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
        let stmt = "
            CREATE CUSTOM INDEX userIndex ON users (email)
            USING 'path.to.the.IndexClass'
            WITH OPTIONS = {'storage': '/mnt/ssd/indexes/'};";
        let parsed = stmt.parse::<CreateIndexStatement>().unwrap();
        let test = CreateIndexStatementBuilder::default()
            .custom(true)
            .name("userIndex")
            .table("users")
            .index_id("email")
            .using(IndexClass::new("path.to.the.IndexClass").options(maplit::hashmap! {
                LitStr::from("storage") => LitStr::from("/mnt/ssd/indexes/")
            }))
            .build()
            .unwrap();
        assert_eq!(parsed.to_string(), test.to_string());
    }

    #[test]
    fn test_parse_drop_index() {
        let stmt = "DROP INDEX IF EXISTS userIndex;";
        let parsed = stmt.parse::<DropIndexStatement>().unwrap();
        let test = DropIndexStatementBuilder::default()
            .if_exists(true)
            .name("userIndex")
            .build()
            .unwrap();
        assert_eq!(parsed.to_string(), test.to_string());
    }
}

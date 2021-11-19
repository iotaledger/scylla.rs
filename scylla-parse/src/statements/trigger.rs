use super::*;

#[derive(ParseFromStr, Clone, Debug, TryInto, From)]
pub enum TriggerStatement {
    Create(CreateTriggerStatement),
    Drop(DropTriggerStatement),
}

impl Parse for TriggerStatement {
    type Output = Self;
    fn parse(s: &mut StatementStream<'_>) -> anyhow::Result<Self::Output> {
        Ok(if let Some(stmt) = s.parse::<Option<CreateTriggerStatement>>()? {
            Self::Create(stmt)
        } else if let Some(stmt) = s.parse::<Option<DropTriggerStatement>>()? {
            Self::Drop(stmt)
        } else {
            anyhow::bail!("Invalid TRIGGER statement!")
        })
    }
}

impl Peek for TriggerStatement {
    fn peek(s: StatementStream<'_>) -> bool {
        s.check::<CreateTriggerStatement>() || s.check::<DropTriggerStatement>()
    }
}

#[derive(ParseFromStr, Builder, Clone, Debug)]
pub struct CreateTriggerStatement {
    #[builder(default)]
    pub if_not_exists: bool,
    #[builder(setter(into))]
    pub name: Name,
    #[builder(setter(into))]
    pub table: KeyspaceQualifiedName,
    #[builder(setter(into))]
    pub using: LitStr,
}

impl Parse for CreateTriggerStatement {
    type Output = Self;
    fn parse(s: &mut StatementStream<'_>) -> anyhow::Result<Self::Output> {
        s.parse::<(CREATE, TRIGGER)>()?;
        let mut res = CreateTriggerStatementBuilder::default();
        res.if_not_exists(s.parse::<Option<(IF, NOT, EXISTS)>>()?.is_some())
            .name(s.parse::<Name>()?)
            .table(s.parse::<(ON, KeyspaceQualifiedName)>()?.1)
            .using(s.parse::<(USING, LitStr)>()?.1);
        s.parse::<Option<Semicolon>>()?;
        Ok(res
            .build()
            .map_err(|e| anyhow::anyhow!("Invalid CREATE TRIGGER statement: {}", e))?)
    }
}

impl Peek for CreateTriggerStatement {
    fn peek(s: StatementStream<'_>) -> bool {
        s.check::<(CREATE, TRIGGER)>()
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

#[derive(ParseFromStr, Builder, Clone, Debug)]
pub struct DropTriggerStatement {
    #[builder(default)]
    pub if_exists: bool,
    #[builder(setter(into))]
    pub name: Name,
    #[builder(setter(into))]
    pub table: KeyspaceQualifiedName,
}

impl Parse for DropTriggerStatement {
    type Output = Self;
    fn parse(s: &mut StatementStream<'_>) -> anyhow::Result<Self::Output> {
        s.parse::<(DROP, TRIGGER)>()?;
        let mut res = DropTriggerStatementBuilder::default();
        res.if_exists(s.parse::<Option<(IF, EXISTS)>>()?.is_some())
            .name(s.parse::<Name>()?)
            .table(s.parse::<(ON, KeyspaceQualifiedName)>()?.1);
        s.parse::<Option<Semicolon>>()?;
        Ok(res
            .build()
            .map_err(|e| anyhow::anyhow!("Invalid DROP TRIGGER statement: {}", e))?)
    }
}

impl Peek for DropTriggerStatement {
    fn peek(s: StatementStream<'_>) -> bool {
        s.check::<(DROP, TRIGGER)>()
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

    #[test]
    fn test_parse_create_trigger() {
        let stmt = "CREATE TRIGGER my_trigger ON my_table USING 'my_function';";
        let parsed = stmt.parse::<CreateTriggerStatement>().unwrap();
        let test = CreateTriggerStatementBuilder::default()
            .if_not_exists(false)
            .name("my_trigger")
            .table("my_table")
            .using("my_function")
            .build()
            .unwrap();
        assert_eq!(parsed.to_string(), test.to_string());
    }

    #[test]
    fn test_parse_drop_trigger() {
        let stmt = "DROP TRIGGER my_trigger ON my_table;";
        let parsed = stmt.parse::<DropTriggerStatement>().unwrap();
        let test = DropTriggerStatementBuilder::default()
            .if_exists(false)
            .name("my_trigger")
            .table("my_table")
            .build()
            .unwrap();
        assert_eq!(parsed.to_string(), test.to_string());
    }
}

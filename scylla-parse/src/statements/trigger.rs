use super::*;

#[derive(ParseFromStr, Clone, Debug, TryInto, From)]
pub enum TriggerStatement {
    Create(CreateTriggerStatement),
    Drop(DropTriggerStatement),
}

impl Parse for TriggerStatement {
    type Output = Self;
    fn parse(s: &mut StatementStream<'_>) -> anyhow::Result<Self::Output> {
        Ok(if let Some(stmt) = s.parse_if::<CreateTriggerStatement>() {
            Self::Create(stmt?)
        } else if let Some(stmt) = s.parse_if::<DropTriggerStatement>() {
            Self::Drop(stmt?)
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
    pub name: Name,
    pub table: KeyspaceQualifiedName,
    pub using: String,
}

impl Parse for CreateTriggerStatement {
    type Output = Self;
    fn parse(s: &mut StatementStream<'_>) -> anyhow::Result<Self::Output> {
        s.parse::<(CREATE, TRIGGER)>()?;
        let mut res = CreateTriggerStatementBuilder::default();
        res.if_not_exists(s.parse::<Option<(IF, NOT, EXISTS)>>()?.is_some())
            .name(s.parse()?)
            .table(s.parse::<(ON, _)>()?.1)
            .using(s.parse::<(USING, _)>()?.1);
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

#[derive(ParseFromStr, Builder, Clone, Debug)]
pub struct DropTriggerStatement {
    #[builder(default)]
    pub if_exists: bool,
    pub name: Name,
    pub table: KeyspaceQualifiedName,
}

impl Parse for DropTriggerStatement {
    type Output = Self;
    fn parse(s: &mut StatementStream<'_>) -> anyhow::Result<Self::Output> {
        s.parse::<(DROP, TRIGGER)>()?;
        let mut res = DropTriggerStatementBuilder::default();
        res.if_exists(s.parse::<Option<(IF, EXISTS)>>()?.is_some())
            .name(s.parse()?)
            .table(s.parse::<(ON, _)>()?.1);
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

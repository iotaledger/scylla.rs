use crate::PrimaryKey;

use super::*;

#[derive(ParseFromStr, Clone, Debug, TryInto, From)]
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

#[derive(ParseFromStr, Builder, Clone, Debug)]
pub struct CreateMaterializedViewStatement {
    #[builder(default)]
    pub if_not_exists: bool,
    #[builder(setter(into))]
    pub name: Name,
    pub select_statement: SelectStatement,
    pub primary_key: PrimaryKey,
    pub table_opts: TableOpts,
}

impl Parse for CreateMaterializedViewStatement {
    type Output = Self;
    fn parse(s: &mut StatementStream<'_>) -> anyhow::Result<Self::Output> {
        s.parse::<(CREATE, MATERIALIZED, VIEW)>()?;
        let mut res = CreateMaterializedViewStatementBuilder::default();
        res.if_not_exists(s.parse::<Option<(IF, NOT, EXISTS)>>()?.is_some())
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

#[derive(ParseFromStr, Builder, Clone, Debug)]
pub struct AlterMaterializedViewStatement {
    pub name: Name,
    pub table_opts: TableOpts,
}

impl Parse for AlterMaterializedViewStatement {
    type Output = Self;
    fn parse(s: &mut StatementStream<'_>) -> anyhow::Result<Self::Output> {
        s.parse::<(ALTER, MATERIALIZED, VIEW)>()?;
        let mut res = AlterMaterializedViewStatementBuilder::default();
        res.name(s.parse()?).table_opts(s.parse_from::<(WITH, TableOpts)>()?.1);
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

#[derive(ParseFromStr, Builder, Clone, Debug)]
pub struct DropMaterializedViewStatement {
    pub if_exists: bool,
    pub name: Name,
}

impl Parse for DropMaterializedViewStatement {
    type Output = Self;
    fn parse(s: &mut StatementStream<'_>) -> anyhow::Result<Self::Output> {
        s.parse::<(DROP, MATERIALIZED, VIEW)>()?;
        let mut res = DropMaterializedViewStatementBuilder::default();
        res.if_exists(s.parse::<Option<(IF, EXISTS)>>()?.is_some())
            .name(s.parse()?);
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

use crate::PrimaryKey;

use super::*;

#[derive(ParseFromStr, Clone, Debug, TryInto, From, ToTokens)]
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

#[derive(ParseFromStr, Builder, Clone, Debug, ToTokens)]
pub struct CreateMaterializedViewStatement {
    #[builder(default)]
    pub if_not_exists: bool,
    #[builder(setter(into))]
    pub name: Name,
    pub select_statement: SelectStatement,
    #[builder(setter(into))]
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

#[derive(ParseFromStr, Builder, Clone, Debug, ToTokens)]
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

#[derive(ParseFromStr, Builder, Clone, Debug, ToTokens)]
pub struct DropMaterializedViewStatement {
    pub if_exists: bool,
    #[builder(setter(into))]
    pub name: Name,
}

impl Parse for DropMaterializedViewStatement {
    type Output = Self;
    fn parse(s: &mut StatementStream<'_>) -> anyhow::Result<Self::Output> {
        s.parse::<(DROP, MATERIALIZED, VIEW)>()?;
        let mut res = DropMaterializedViewStatementBuilder::default();
        res.if_exists(s.parse::<Option<(IF, EXISTS)>>()?.is_some())
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
        let stmt = "
            CREATE MATERIALIZED VIEW monkeySpecies_by_population AS
            SELECT * FROM monkeySpecies
            WHERE population IS NOT NULL AND species IS NOT NULL
            PRIMARY KEY (population, species)
            WITH comment='Allow query by population instead of species';";
        let parsed = stmt.parse::<CreateMaterializedViewStatement>().unwrap();
        let test = CreateMaterializedViewStatementBuilder::default()
            .name("monkeySpecies_by_population")
            .select_statement(
                SelectStatementBuilder::default()
                    .select_clause(SelectClauseKind::All)
                    .from("monkeySpecies")
                    .where_clause(vec![
                        Relation::is_not_null("population"),
                        Relation::is_not_null("species"),
                    ])
                    .build()
                    .unwrap(),
            )
            .primary_key(vec!["population", "species"])
            .table_opts(
                crate::TableOptsBuilder::default()
                    .comment("Allow query by population instead of species")
                    .build()
                    .unwrap(),
            )
            .build()
            .unwrap();
        assert_eq!(parsed.to_string(), test.to_string());
    }

    #[test]
    fn test_parse_alter_mv() {
        let stmt = "
            ALTER MATERIALIZED VIEW monkeySpecies_by_population WITH default_time_to_live=100;";
        let parsed = stmt.parse::<AlterMaterializedViewStatement>().unwrap();
        let test = AlterMaterializedViewStatementBuilder::default()
            .name("monkeySpecies_by_population")
            .table_opts(
                crate::TableOptsBuilder::default()
                    .default_time_to_live(100)
                    .build()
                    .unwrap(),
            )
            .build()
            .unwrap();
        assert_eq!(parsed.to_string(), test.to_string());
    }

    #[test]
    fn test_parse_drop_mv() {
        let stmt = "DROP MATERIALIZED VIEW IF EXISTS monkeySpecies_by_population;";
        let parsed = stmt.parse::<DropMaterializedViewStatement>().unwrap();
        let test = DropMaterializedViewStatementBuilder::default()
            .if_exists(true)
            .name("monkeySpecies_by_population")
            .build()
            .unwrap();
        assert_eq!(parsed.to_string(), test.to_string());
    }
}

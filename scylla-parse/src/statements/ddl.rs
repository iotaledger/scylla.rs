use std::convert::TryInto;

use super::*;
use crate::{
    ColumnDefinition,
    Constant,
    PrimaryKey,
    StatementOptValue,
};

#[derive(ParseFromStr, Clone, Debug, TryInto, From)]
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

impl Parse for DataDefinitionStatement {
    type Output = Self;
    fn parse(s: &mut StatementStream<'_>) -> anyhow::Result<Self::Output> {
        Ok(if let Some(stmt) = s.parse::<Option<UseStatement>>()? {
            Self::Use(stmt)
        } else if let Some(stmt) = s.parse::<Option<CreateKeyspaceStatement>>()? {
            Self::CreateKeyspace(stmt)
        } else if let Some(stmt) = s.parse::<Option<AlterKeyspaceStatement>>()? {
            Self::AlterKeyspace(stmt)
        } else if let Some(stmt) = s.parse::<Option<DropKeyspaceStatement>>()? {
            Self::DropKeyspace(stmt)
        } else if let Some(stmt) = s.parse::<Option<CreateTableStatement>>()? {
            Self::CreateTable(stmt)
        } else if let Some(stmt) = s.parse::<Option<AlterTableStatement>>()? {
            Self::AlterTable(stmt)
        } else if let Some(stmt) = s.parse::<Option<DropTableStatement>>()? {
            Self::DropTable(stmt)
        } else if let Some(stmt) = s.parse::<Option<TruncateStatement>>()? {
            Self::Truncate(stmt)
        } else {
            anyhow::bail!("Expected data definition statement!")
        })
    }
}

impl Peek for DataDefinitionStatement {
    fn peek(s: StatementStream<'_>) -> bool {
        s.check::<UseStatement>()
            || s.check::<CreateKeyspaceStatement>()
            || s.check::<AlterKeyspaceStatement>()
            || s.check::<DropKeyspaceStatement>()
            || s.check::<CreateTableStatement>()
            || s.check::<AlterTableStatement>()
            || s.check::<DropTableStatement>()
            || s.check::<TruncateStatement>()
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

#[derive(ParseFromStr, Clone, Debug)]
pub struct UseStatement {
    pub keyspace: Name,
}

impl Parse for UseStatement {
    type Output = Self;
    fn parse(s: &mut StatementStream<'_>) -> anyhow::Result<Self::Output> {
        s.parse::<USE>()?;
        let keyspace = s.parse()?;
        s.parse::<Option<Semicolon>>()?;
        Ok(Self { keyspace })
    }
}

impl Peek for UseStatement {
    fn peek(s: StatementStream<'_>) -> bool {
        s.check::<USE>()
    }
}

impl Display for UseStatement {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "USE {}", self.keyspace)
    }
}

#[derive(Builder, Clone, Debug)]
pub struct KeyspaceOpts {
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
                        res.replication(m.try_into()?);
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

#[derive(ParseFromStr, Builder, Clone, Debug)]
pub struct CreateKeyspaceStatement {
    #[builder(default)]
    pub if_not_exists: bool,
    pub keyspace: Name,
    pub options: KeyspaceOpts,
}

impl Parse for CreateKeyspaceStatement {
    type Output = Self;
    fn parse(s: &mut StatementStream<'_>) -> anyhow::Result<Self::Output> {
        s.parse::<(CREATE, KEYSPACE)>()?;
        let mut res = CreateKeyspaceStatementBuilder::default();
        res.if_not_exists(s.parse::<Option<(IF, NOT, EXISTS)>>()?.is_some())
            .keyspace(s.parse()?);
        s.parse::<WITH>()?;
        res.options(s.parse()?);
        s.parse::<Option<Semicolon>>()?;
        Ok(res
            .build()
            .map_err(|e| anyhow::anyhow!("Invalid CREATE KEYSPACE statement: {}", e))?)
    }
}

impl Peek for CreateKeyspaceStatement {
    fn peek(s: StatementStream<'_>) -> bool {
        s.check::<(CREATE, KEYSPACE)>()
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

#[derive(ParseFromStr, Builder, Clone, Debug)]
pub struct AlterKeyspaceStatement {
    pub keyspace: Name,
    pub options: KeyspaceOpts,
}

impl Parse for AlterKeyspaceStatement {
    type Output = Self;
    fn parse(s: &mut StatementStream<'_>) -> anyhow::Result<Self::Output> {
        s.parse::<(ALTER, KEYSPACE)>()?;
        let mut res = AlterKeyspaceStatementBuilder::default();
        res.keyspace(s.parse()?);
        s.parse::<WITH>()?;
        res.options(s.parse()?);
        s.parse::<Option<Semicolon>>()?;
        Ok(res
            .build()
            .map_err(|e| anyhow::anyhow!("Invalid ALTER KEYSPACE statement: {}", e))?)
    }
}

impl Peek for AlterKeyspaceStatement {
    fn peek(s: StatementStream<'_>) -> bool {
        s.check::<(ALTER, KEYSPACE)>()
    }
}

impl Display for AlterKeyspaceStatement {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "ALTER KEYSPACE {} WITH {}", self.keyspace, self.options)
    }
}

#[derive(ParseFromStr, Builder, Clone, Debug)]
pub struct DropKeyspaceStatement {
    #[builder(default)]
    pub if_exists: bool,
    pub keyspace: Name,
}

impl Parse for DropKeyspaceStatement {
    type Output = Self;
    fn parse(s: &mut StatementStream<'_>) -> anyhow::Result<Self::Output> {
        s.parse::<(DROP, KEYSPACE)>()?;
        let mut res = DropKeyspaceStatementBuilder::default();
        res.if_exists(s.parse::<Option<(IF, EXISTS)>>()?.is_some())
            .keyspace(s.parse()?);
        s.parse::<Option<Semicolon>>()?;
        Ok(res
            .build()
            .map_err(|e| anyhow::anyhow!("Invalid DROP KEYSPACE statement: {}", e))?)
    }
}

impl Peek for DropKeyspaceStatement {
    fn peek(s: StatementStream<'_>) -> bool {
        s.check::<(DROP, KEYSPACE)>()
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

#[derive(ParseFromStr, Builder, Clone, Debug)]
#[builder(setter(strip_option))]
pub struct CreateTableStatement {
    #[builder(default)]
    pub if_not_exists: bool,
    pub table: KeyspaceQualifiedName,
    pub columns: Vec<ColumnDefinition>,
    #[builder(default)]
    pub primary_key: Option<PrimaryKey>,
    #[builder(default)]
    pub options: Option<TableOpts>,
}

impl Parse for CreateTableStatement {
    type Output = Self;
    fn parse(s: &mut StatementStream<'_>) -> anyhow::Result<Self::Output> {
        s.parse::<(CREATE, TABLE)>()?;
        let mut res = CreateTableStatementBuilder::default();
        res.if_not_exists(s.parse::<Option<(IF, NOT, EXISTS)>>()?.is_some())
            .table(s.parse()?);
        s.parse::<LeftParen>()?;
        res.columns(s.parse_from::<List<ColumnDefinition, Comma>>()?);
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

impl Peek for CreateTableStatement {
    fn peek(s: StatementStream<'_>) -> bool {
        s.check::<(CREATE, TABLE)>()
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

#[derive(ParseFromStr, Builder, Clone, Debug)]
pub struct AlterTableStatement {
    pub table: KeyspaceQualifiedName,
    pub instruction: AlterTableInstruction,
}

impl Parse for AlterTableStatement {
    type Output = Self;
    fn parse(s: &mut StatementStream<'_>) -> anyhow::Result<Self::Output> {
        s.parse::<(ALTER, TABLE)>()?;
        let mut res = AlterTableStatementBuilder::default();
        res.table(s.parse()?).instruction(s.parse()?);
        s.parse::<Option<Semicolon>>()?;
        Ok(res
            .build()
            .map_err(|e| anyhow::anyhow!("Invalid ALTER TABLE statement: {}", e))?)
    }
}

impl Peek for AlterTableStatement {
    fn peek(s: StatementStream<'_>) -> bool {
        s.check::<(ALTER, TABLE)>()
    }
}

impl Display for AlterTableStatement {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "ALTER TABLE {} {}", self.table, self.instruction)
    }
}

#[derive(ParseFromStr, Clone, Debug)]
pub enum AlterTableInstruction {
    Add(Vec<ColumnDefinition>),
    Drop(Vec<Name>),
    Alter(Name, CqlType),
    With(Vec<StatementOpt>),
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
                Self::With(s.parse_from::<List<StatementOpt, AND>>()?)
            } else {
                anyhow::bail!("Invalid ALTER TABLE instruction: {}", s.parse_from::<Token>()?);
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
                "DROP {}",
                cols.iter().map(|i| i.to_string()).collect::<Vec<_>>().join(" ")
            ),
            Self::Alter(col, ty) => write!(f, "ALTER {} TYPE {}", col, ty),
            Self::With(options) => write!(
                f,
                "WITH {}",
                options.iter().map(|i| i.to_string()).collect::<Vec<_>>().join(" AND ")
            ),
        }
    }
}

#[derive(ParseFromStr, Builder, Clone, Debug)]
pub struct DropTableStatement {
    #[builder(default)]
    pub if_exists: bool,
    pub table: KeyspaceQualifiedName,
}

impl Parse for DropTableStatement {
    type Output = Self;
    fn parse(s: &mut StatementStream<'_>) -> anyhow::Result<Self::Output> {
        s.parse::<(DROP, TABLE)>()?;
        let mut res = DropTableStatementBuilder::default();
        res.if_exists(s.parse::<Option<(IF, EXISTS)>>()?.is_some())
            .table(s.parse()?);
        s.parse::<Option<Semicolon>>()?;
        Ok(res
            .build()
            .map_err(|e| anyhow::anyhow!("Invalid DROP TABLE statement: {}", e))?)
    }
}

impl Peek for DropTableStatement {
    fn peek(s: StatementStream<'_>) -> bool {
        s.check::<(DROP, TABLE)>()
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

#[derive(ParseFromStr, Builder, Clone, Debug)]
pub struct TruncateStatement {
    pub table: KeyspaceQualifiedName,
}

impl Parse for TruncateStatement {
    type Output = Self;
    fn parse(s: &mut StatementStream<'_>) -> anyhow::Result<Self::Output> {
        s.parse::<TRUNCATE>()?;
        let mut res = TruncateStatementBuilder::default();
        res.table(s.parse()?);
        s.parse::<Option<Semicolon>>()?;
        Ok(res
            .build()
            .map_err(|e| anyhow::anyhow!("Invalid TRUNCATE statement: {}", e))?)
    }
}

impl Peek for TruncateStatement {
    fn peek(s: StatementStream<'_>) -> bool {
        s.check::<TRUNCATE>()
    }
}

impl Display for TruncateStatement {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "TRUNCATE {}", self.table)
    }
}

mod test {
    #[allow(unused)]
    use super::*;
    #[allow(unused)]
    use crate::{
        ColumnDefinition,
        Compaction,
        Compression,
        CqlType,
        NativeType,
        SpeculativeRetry,
    };

    #[test]
    fn test_parse_create_table() {
        let statement = "
            CREATE TABLE IF NOT EXISTS test.test (
                id INT PRIMARY KEY, 
                name TEXT, 
                age INT, 
                created_at TIMESTAMP
            ) 
            WITH comment = 'test' 
            AND speculative_retry = '99.0PERCENTILE'
            AND compression = {
                'class': 'LZ4Compressor'
            } 
            AND default_time_to_live = 0 
            AND read_repair = 'BLOCKING' 
            AND compaction = {
                'class': 'SizeTieredCompactionStrategy', 
                'enabled': FALSE
            } 
            AND memtable_flush_period_in_ms = 0 
            AND gc_grace_seconds = 864000";
        let res = StatementStream::new(statement).parse::<CreateTableStatement>().unwrap();
        let test = CreateTableStatementBuilder::default()
            .if_not_exists(true)
            .table("test.test".parse().unwrap())
            .columns(vec![
                ColumnDefinition::build()
                    .name(Name::Unquoted("id".to_string()))
                    .data_type("int".parse().unwrap())
                    .primary_key(true)
                    .build()
                    .unwrap(),
                ColumnDefinition::build()
                    .name(Name::Unquoted("name".to_string()))
                    .data_type("text".parse().unwrap())
                    .build()
                    .unwrap(),
                ColumnDefinition::build()
                    .name(Name::Unquoted("age".to_string()))
                    .data_type("int".parse().unwrap())
                    .build()
                    .unwrap(),
                ColumnDefinition::build()
                    .name(Name::Unquoted("created_at".to_string()))
                    .data_type("timestamp".parse().unwrap())
                    .build()
                    .unwrap(),
            ])
            .build()
            .unwrap()
            .with_comment("test")
            .with_compaction(Compaction::size_tiered().enabled(false).build().unwrap())
            .with_compression(Compression::build().class("LZ4Compressor").build().unwrap())
            .with_default_time_to_live(0)
            .with_gc_grace_seconds(864000)
            .with_memtable_flush_period_in_ms(0)
            .with_read_repair(true)
            .with_speculative_retry(SpeculativeRetry::Percentile(99.0));
        assert_eq!(res.to_string(), test.to_string());
    }

    #[test]
    fn test_parse_alter_table() {
        let statement = "
            ALTER TABLE test.test ADD
                new_id INT, 
                new_name TEXT, 
                new_age INT, 
                new_created_at TIMESTAMP
            ";
        let res = StatementStream::new(statement).parse::<AlterTableStatement>().unwrap();
        let test = AlterTableStatementBuilder::default()
            .table("test.test".parse().unwrap())
            .instruction(AlterTableInstruction::Add(vec![
                ColumnDefinition::build()
                    .name(Name::Unquoted("new_id".to_string()))
                    .data_type("int".parse().unwrap())
                    .build()
                    .unwrap(),
                ColumnDefinition::build()
                    .name(Name::Unquoted("new_name".to_string()))
                    .data_type("text".parse().unwrap())
                    .build()
                    .unwrap(),
                ColumnDefinition::build()
                    .name(Name::Unquoted("new_age".to_string()))
                    .data_type("int".parse().unwrap())
                    .build()
                    .unwrap(),
                ColumnDefinition::build()
                    .name(Name::Unquoted("new_created_at".to_string()))
                    .data_type("timestamp".parse().unwrap())
                    .build()
                    .unwrap(),
            ]))
            .build()
            .unwrap();
        assert_eq!(res.to_string(), test.to_string());
    }

    #[test]
    fn test_parse_drop_table() {
        let statement = "DROP TABLE test.test;";
        let res = StatementStream::new(statement).parse::<DropTableStatement>().unwrap();
        let test = DropTableStatementBuilder::default()
            .if_exists(false)
            .table("test.test".parse().unwrap())
            .build()
            .unwrap();
        assert_eq!(res.to_string(), test.to_string());
    }

    #[test]
    fn test_parse_create_keyspace() {
        let statement = "
            CREATE KEYSPACE IF NOT EXISTS test
            WITH replication = {
                'class': 'SimpleStrategy',
                'replication_factor': 1
            }
            AND durable_writes = true";
        let res = StatementStream::new(statement)
            .parse::<CreateKeyspaceStatement>()
            .unwrap();
        let test = CreateKeyspaceStatementBuilder::default()
            .if_not_exists(true)
            .keyspace("test".parse().unwrap())
            .options(
                KeyspaceOptsBuilder::default()
                    .replication(Replication::simple(1))
                    .durable_writes(true)
                    .build()
                    .unwrap(),
            )
            .build()
            .unwrap();
        assert_eq!(res.to_string(), test.to_string());
    }

    #[test]
    fn test_parse_alter_keyspace() {
        let statement = "
            ALTER KEYSPACE test
            WITH replication = {
                'class': 'NetworkTopologyStrategy',
                'DC2': 3
            }
            AND durable_writes = false;";
        let res = StatementStream::new(statement)
            .parse::<AlterKeyspaceStatement>()
            .unwrap();
        let test = AlterKeyspaceStatementBuilder::default()
            .keyspace("test".parse().unwrap())
            .options(
                KeyspaceOptsBuilder::default()
                    .replication(Replication::NetworkTopologyStrategy(maplit::hashmap! {
                        "DC2".to_string() => 3
                    }))
                    .durable_writes(false)
                    .build()
                    .unwrap(),
            )
            .build()
            .unwrap();
        assert_eq!(res.to_string(), test.to_string());
    }

    #[test]
    fn test_parse_drop_keyspace() {
        let statement = "DROP KEYSPACE test;";
        let res = StatementStream::new(statement)
            .parse::<DropKeyspaceStatement>()
            .unwrap();
        let test = DropKeyspaceStatementBuilder::default()
            .if_exists(false)
            .keyspace("test".parse().unwrap())
            .build()
            .unwrap();
        assert_eq!(res.to_string(), test.to_string());
    }
}

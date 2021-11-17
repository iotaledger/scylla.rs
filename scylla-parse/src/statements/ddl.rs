use super::*;
use crate::{
    ColumnDefinition,
    PrimaryKey,
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
        Ok(if let Some(stmt) = s.parse_if::<UseStatement>() {
            Self::Use(stmt?)
        } else if let Some(stmt) = s.parse_if::<CreateKeyspaceStatement>() {
            Self::CreateKeyspace(stmt?)
        } else if let Some(stmt) = s.parse_if::<AlterKeyspaceStatement>() {
            Self::AlterKeyspace(stmt?)
        } else if let Some(stmt) = s.parse_if::<DropKeyspaceStatement>() {
            Self::DropKeyspace(stmt?)
        } else if let Some(stmt) = s.parse_if::<CreateTableStatement>() {
            Self::CreateTable(stmt?)
        } else if let Some(stmt) = s.parse_if::<AlterTableStatement>() {
            Self::AlterTable(stmt?)
        } else if let Some(stmt) = s.parse_if::<DropTableStatement>() {
            Self::DropTable(stmt?)
        } else if let Some(stmt) = s.parse_if::<TruncateStatement>() {
            Self::Truncate(stmt?)
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
        Ok(Self { keyspace: s.parse()? })
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

#[derive(ParseFromStr, Builder, Clone, Debug)]
pub struct CreateKeyspaceStatement {
    #[builder(default)]
    pub if_not_exists: bool,
    pub keyspace: Name,
    pub options: Vec<StatementOpt>,
}

impl Parse for CreateKeyspaceStatement {
    type Output = Self;
    fn parse(s: &mut StatementStream<'_>) -> anyhow::Result<Self::Output> {
        s.parse::<(CREATE, KEYSPACE)>()?;
        let mut res = CreateKeyspaceStatementBuilder::default();
        res.if_not_exists(s.parse_if::<(IF, NOT, EXISTS)>().is_some())
            .keyspace(s.parse()?);
        s.parse::<WITH>()?;
        res.options(s.parse_from::<List<StatementOpt, AND>>()?);
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
                .iter()
                .map(|i| i.to_string())
                .collect::<Vec<_>>()
                .join(" AND ")
        )
    }
}

#[derive(ParseFromStr, Builder, Clone, Debug)]
pub struct AlterKeyspaceStatement {
    pub keyspace: Name,
    pub options: Vec<StatementOpt>,
}

impl Parse for AlterKeyspaceStatement {
    type Output = Self;
    fn parse(s: &mut StatementStream<'_>) -> anyhow::Result<Self::Output> {
        s.parse::<(ALTER, KEYSPACE)>()?;
        let mut res = AlterKeyspaceStatementBuilder::default();
        res.keyspace(s.parse()?);
        s.parse::<WITH>()?;
        res.options(s.parse_from::<List<StatementOpt, AND>>()?);
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
        write!(
            f,
            "ALTER KEYSPACE {} WITH {}",
            self.keyspace,
            self.options
                .iter()
                .map(|i| i.to_string())
                .collect::<Vec<_>>()
                .join(" AND ")
        )
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
        res.if_exists(s.parse_if::<(IF, EXISTS)>().is_some())
            .keyspace(s.parse()?);
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
        res.if_not_exists(s.parse_if::<(IF, NOT, EXISTS)>().is_some())
            .table(s.parse()?);
        s.parse::<LeftParen>()?;
        res.columns(s.parse_from::<List<ColumnDefinition, Comma>>()?);
        res.primary_key(s.parse_from::<If<(PRIMARY, KEY), Parens<PrimaryKey>>>()?);
        s.parse::<RightParen>()?;
        res.options(s.parse_from::<If<WITH, TableOpts>>()?);
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
        Ok(if s.parse_if::<ADD>().is_some() || s.check::<ColumnDefinition>() {
            Self::Add(s.parse_from::<List<ColumnDefinition, Comma>>()?)
        } else if s.parse_if::<DROP>().is_some() {
            if let Some(columns) = s.parse_from_if::<Parens<List<Name, Comma>>>() {
                Self::Drop(columns?)
            } else {
                Self::Drop(vec![s.parse()?])
            }
        } else if s.parse_if::<ALTER>().is_some() {
            let (col, _, ty) = s.parse::<(_, TYPE, _)>()?;
            Self::Alter(col, ty)
        } else if s.parse_if::<WITH>().is_some() {
            Self::With(s.parse_from::<List<StatementOpt, AND>>()?)
        } else {
            anyhow::bail!("Invalid ALTER TABLE instruction: {}", s.parse_from::<Token>()?);
        })
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
        res.if_exists(s.parse_if::<(IF, EXISTS)>().is_some()).table(s.parse()?);
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
                'class': 'LZ4Compressor', 
                'enabled': TRUE, 
                'chunk_length_in_kb': 64, 
                'crc_check_chance': 1.0, 
                'compression_level': 3
            } 
            AND default_time_to_live = 0 
            AND read_repair = 'BLOCKING' 
            AND compaction = {
                'class': 'SizeTieredCompactionStrategy', 
                'enabled': FALSE, 
                'tombstone_threshold': 0.2, 
                'tombstone_compaction_interval': 86400, 
                'log_all': FALSE, 
                'unchecked_tombstone_compaction': FALSE, 
                'only_purge_repaired_tombstone': FALSE, 
                'min_threshold': 4, 
                'max_threshold': 32, 
                'min_sstable_size': 50, 
                'bucket_low': 0.5, 
                'bucket_high': 1.5
            } 
            AND memtable_flush_period_in_ms = 0 
            AND gc_grace_seconds = 864000";
        let res = StatementStream::new(statement).parse::<CreateTableStatement>().unwrap();
        let test = CreateTableStatementBuilder::default()
            .if_not_exists(true)
            .table("test.test".parse().unwrap())
            .columns(vec![
                ColumnDefinition::build()
                    .name("id".parse().unwrap())
                    .data_type("int".parse().unwrap())
                    .primary_key(true)
                    .build()
                    .unwrap(),
                ColumnDefinition::build()
                    .name("name".parse().unwrap())
                    .data_type("text".parse().unwrap())
                    .build()
                    .unwrap(),
                ColumnDefinition::build()
                    .name("age".parse().unwrap())
                    .data_type("int".parse().unwrap())
                    .build()
                    .unwrap(),
                ColumnDefinition::build()
                    .name("created_at".parse().unwrap())
                    .data_type("timestamp".parse().unwrap())
                    .build()
                    .unwrap(),
            ])
            .build()
            .unwrap()
            .with_comment("test")
            .with_compaction(Compaction::size_tiered().enabled(false).build().unwrap())
            .with_compression(Compression::default())
            .with_default_time_to_live(0)
            .with_gc_grace_seconds(864000)
            .with_memtable_flush_period_in_ms(0)
            .with_read_repair(true)
            .with_speculative_retry(SpeculativeRetry::Percentile(99.0));
        assert_eq!(res.to_string(), test.to_string());
    }
}

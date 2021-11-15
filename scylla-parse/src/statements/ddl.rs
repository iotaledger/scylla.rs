use derive_more::{
    From,
    TryInto,
};

use crate::{
    ColumnDefinition,
    Name,
    PrimaryKey,
    StatementOpt,
    TableName,
    TableOpt,
};

#[derive(TryInto, From)]
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

pub struct UseStatement {
    pub keyspace: Name,
}

pub struct CreateKeyspaceStatement {
    pub if_not_exists: bool,
    pub keyspace: Name,
    pub options: Vec<StatementOpt>,
}

pub struct AlterKeyspaceStatement {
    pub keyspace: Name,
    pub options: Vec<StatementOpt>,
}

pub struct DropKeyspaceStatement {
    pub if_exists: bool,
    pub keyspace: Name,
}

pub struct CreateTableStatement {
    pub if_not_exists: bool,
    pub table: TableName,
    pub columns: Vec<ColumnDefinition>,
    pub primary_key: Option<PrimaryKey>,
    pub options: Vec<TableOpt>,
}

pub struct AlterTableStatement {
    pub table: TableName,
    pub instruction: AlterTableInstruction,
}

pub enum AlterTableInstruction {
    Add(Vec<ColumnDefinition>),
    Drop(Vec<Name>),
    With(Vec<StatementOpt>),
}

pub struct DropTableStatement {
    pub if_exists: bool,
    pub table: TableName,
}

pub struct TruncateStatement {
    pub table: TableName,
}

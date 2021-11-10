use crate::parser::{
    Identifier,
    MapLiteral,
    TableName,
};

pub struct IndexName(String);

pub enum SecondaryIndexStatement {
    Create(CreateIndexStatement),
    Drop(DropIndexStatement),
}

pub struct CreateIndexStatement {
    pub custom: bool,
    pub if_not_exists: bool,
    pub name: IndexName,
    pub table: TableName,
    pub index_id: IndexIdentifier,
    pub using: Option<IndexClass>,
}

pub enum IndexIdentifier {
    Column(Identifier),
    Qualified(IndexQualifier, Identifier),
}

pub enum IndexQualifier {
    Keys,
    Values,
    Entries,
    Full,
}

pub struct IndexClass {
    pub path: String,
    pub options: Option<MapLiteral>,
}

pub struct DropIndexStatement {
    pub if_exists: bool,
    pub name: IndexName,
}

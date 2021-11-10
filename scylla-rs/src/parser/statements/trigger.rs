use crate::parser::{
    Identifier,
    TableName,
};

pub enum TriggerStatement {
    Create(CreateTriggerStatement),
    Drop(DropTriggerStatement),
}

pub struct CreateTriggerStatement {
    pub if_not_exists: bool,
    pub name: Identifier,
    pub table: TableName,
    pub using: String,
}

pub struct DropTriggerStatement {
    pub if_exists: bool,
    pub name: Identifier,
    pub table: TableName,
}

use crate::parser::{Name, TableName};

pub enum TriggerStatement {
    Create(CreateTriggerStatement),
    Drop(DropTriggerStatement),
}

pub struct CreateTriggerStatement {
    pub if_not_exists: bool,
    pub name: Name,
    pub table: TableName,
    pub using: String,
}

pub struct DropTriggerStatement {
    pub if_exists: bool,
    pub name: Name,
    pub table: TableName,
}

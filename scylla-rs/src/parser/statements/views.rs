use crate::parser::{
    PrimaryKey,
    TableOpt,
};

use super::SelectStatement;

pub enum MaterializedViewStatement {
    Create(CreateMaterializedViewStatement),
    Alter(AlterMaterializedViewStatement),
    Drop(DropMaterializedViewStatement),
}

pub struct ViewName(String);

pub struct CreateMaterializedViewStatement {
    pub if_not_exists: bool,
    pub name: ViewName,
    pub select_statement: SelectStatement,
    pub primary_key: PrimaryKey,
    pub table_opts: Vec<TableOpt>,
}

pub struct AlterMaterializedViewStatement {
    pub name: ViewName,
    pub table_opts: Vec<TableOpt>,
}

pub struct DropMaterializedViewStatement {
    pub if_exists: bool,
    pub name: ViewName,
}

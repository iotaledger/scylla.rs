mod ddl;
use derive_more::{
    From,
    TryInto,
};
use std::convert::TryFrom;

pub use ddl::*;

mod dml;
pub use dml::*;

mod index;
pub use index::*;

mod views;
pub use views::*;

mod function;
pub use function::*;

mod security;
pub use security::*;

mod trigger;
pub use trigger::*;

use crate::Parse;

#[derive(TryInto, From)]
pub enum Statement {
    DataDefinition(DataDefinitionStatement),
    DataManipulation(DataManipulationStatement),
    SecondaryIndex(SecondaryIndexStatement),
    MaterializedView(MaterializedViewStatement),
    Role(RoleStatement),
    Permission(PermissionStatement),
    User(UserStatement),
    UserDefinedFunction(UserDefinedFunctionStatement),
    UserDefinedType(UserDefinedTypeStatement),
    Trigger(TriggerStatement),
}

macro_rules! impl_try_into_statements {
    ($($via:ty => {$($stmt:ty),*}),*) => {
        $($(
            impl std::convert::TryInto<$stmt> for Statement {
                type Error = anyhow::Error;

                fn try_into(self) -> Result<$stmt, Self::Error> {
                    match <$via>::try_from(self) {
                        Ok(v) => v.try_into().map_err(|e: &str| anyhow::anyhow!(e)),
                        Err(err) => Err(anyhow::anyhow!(
                            "Could not convert Statement to {}: {}",
                            std::any::type_name::<$stmt>(),
                            err
                        )),
                    }
                }
            }
        )*)*
    };
}

impl_try_into_statements!(
    DataDefinitionStatement => {UseStatement, CreateKeyspaceStatement, AlterKeyspaceStatement, DropKeyspaceStatement, CreateTableStatement, AlterTableStatement, DropTableStatement, TruncateStatement},
    DataManipulationStatement => {InsertStatement, UpdateStatement, DeleteStatement, SelectStatement, BatchStatement}
);

impl Parse for Statement {
    type Output = Statement;

    fn parse(s: &mut crate::StatementStream<'_>) -> anyhow::Result<Self::Output> {
        Ok(if let Some(dml) = s.parse_if() {
            Self::DataManipulation(dml?)
        } else {
            anyhow::bail!("Invalid statement!")
        })
    }
}

pub trait KeyspaceExt {
    fn keyspace(&self) -> String;

    fn set_keyspace(&mut self, keyspace: &str);
}

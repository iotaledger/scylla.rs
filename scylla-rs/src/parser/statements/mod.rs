mod ddl;
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

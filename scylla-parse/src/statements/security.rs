use super::FunctionReference;
use crate::{
    MapLiteral,
    Name,
    SetLiteral,
    TableName,
};

pub enum RoleStatement {
    Create(CreateRoleStatement),
    Alter(AlterRoleStatement),
    Drop(DropRoleStatement),
    Grant(GrantRoleStatement),
    Revoke(RevokeRoleStatement),
    List(ListRolesStatement),
}

pub enum RoleOpt {
    Password(String),
    Login(bool),
    Superuser(bool),
    Options(MapLiteral),
    AccessToDatacenters(SetLiteral),
    AccessToAllDatacenters,
}

pub struct CreateRoleStatement {
    pub if_not_exists: bool,
    pub name: Name,
    pub options: Vec<RoleOpt>,
}

pub struct AlterRoleStatement {
    pub name: Name,
    pub options: Vec<RoleOpt>,
}

pub struct DropRoleStatement {
    pub if_exists: bool,
    pub name: Name,
}

pub struct GrantRoleStatement {
    pub name: Name,
    pub to: Name,
}

pub struct RevokeRoleStatement {
    pub name: Name,
    pub from: Name,
}

pub struct ListRolesStatement {
    pub name: Option<Name>,
    pub no_recursive: bool,
}

pub enum Permission {
    Create,
    Alter,
    Drop,
    Select,
    Modify,
    Authorize,
    Describe,
    Execute,
}

pub enum PermissionKind {
    All,
    One(Permission),
}

pub enum Resource {
    AllKeyspaces,
    Keyspace(Name),
    Table(TableName),
    AllRoles,
    Role(Name),
    AllFunctions { keyspace: Option<Name> },
    Function(FunctionReference),
    AllMBeans,
    MBean(String),
}

pub enum PermissionStatement {
    Grant(GrantPermissionStatement),
    Revoke(RevokePermissionStatement),
    List(ListPermissionsStatement),
}

pub struct GrantPermissionStatement {
    pub permissions: Vec<PermissionKind>,
    pub resource: Resource,
    pub to: Name,
}

pub struct RevokePermissionStatement {
    pub permissions: Vec<PermissionKind>,
    pub resource: Resource,
    pub from: Name,
}

pub struct ListPermissionsStatement {
    pub permissions: Vec<PermissionKind>,
    pub resource: Option<Resource>,
    pub role: Option<Name>,
    pub no_recursive: bool,
}

pub enum UserStatement {
    Create,
    Alter,
    Drop,
    List,
}

pub struct CreateUserStatement {
    pub if_not_exists: bool,
    pub name: Name,
    pub with_password: String,
    pub superuser: bool,
}

pub struct AlterUserStatement {
    pub name: Name,
    pub with_password: Option<String>,
    pub superuser: Option<bool>,
}

pub struct DropUserStatement {
    pub if_exists: bool,
    pub name: Name,
}

pub struct ListUsersStatement;

pub enum UserDefinedTypeStatement {
    Create,
    Alter,
    Drop,
}

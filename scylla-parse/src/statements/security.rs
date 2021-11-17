use super::*;

#[derive(ParseFromStr, Clone, Debug, TryInto, From)]
pub enum RoleStatement {
    Create(CreateRoleStatement),
    Alter(AlterRoleStatement),
    Drop(DropRoleStatement),
    Grant(GrantRoleStatement),
    Revoke(RevokeRoleStatement),
    List(ListRolesStatement),
}

impl Parse for RoleStatement {
    type Output = Self;
    fn parse(s: &mut StatementStream<'_>) -> anyhow::Result<Self::Output> {
        Ok(if let Some(stmt) = s.parse::<Option<CreateRoleStatement>>()? {
            Self::Create(stmt)
        } else if let Some(stmt) = s.parse::<Option<AlterRoleStatement>>()? {
            Self::Alter(stmt)
        } else if let Some(stmt) = s.parse::<Option<DropRoleStatement>>()? {
            Self::Drop(stmt)
        } else if let Some(stmt) = s.parse::<Option<GrantRoleStatement>>()? {
            Self::Grant(stmt)
        } else if let Some(stmt) = s.parse::<Option<RevokeRoleStatement>>()? {
            Self::Revoke(stmt)
        } else if let Some(stmt) = s.parse::<Option<ListRolesStatement>>()? {
            Self::List(stmt)
        } else {
            anyhow::bail!("Expected a role statement!")
        })
    }
}

impl Peek for RoleStatement {
    fn peek(s: StatementStream<'_>) -> bool {
        s.check::<CreateRoleStatement>()
            || s.check::<AlterRoleStatement>()
            || s.check::<DropRoleStatement>()
            || s.check::<GrantRoleStatement>()
            || s.check::<RevokeRoleStatement>()
            || s.check::<ListRolesStatement>()
    }
}

#[derive(ParseFromStr, Clone, Debug)]
pub enum RoleOpt {
    Password(LitStr),
    Login(bool),
    Superuser(bool),
    Options(MapLiteral),
    AccessToDatacenters(SetLiteral),
    AccessToAllDatacenters,
}

impl Parse for RoleOpt {
    type Output = Self;
    fn parse(s: &mut StatementStream<'_>) -> anyhow::Result<Self::Output> {
        Ok(if s.parse::<Option<(ACCESS, TO, ALL, DATACENTERS)>>()?.is_some() {
            Self::AccessToAllDatacenters
        } else if s.parse::<Option<(ACCESS, TO, DATACENTERS)>>()?.is_some() {
            Self::AccessToDatacenters(s.parse()?)
        } else if s.parse::<Option<OPTIONS>>()?.is_some() {
            Self::Options(s.parse()?)
        } else if s.parse::<Option<LOGIN>>()?.is_some() {
            Self::Login(s.parse()?)
        } else if s.parse::<Option<SUPERUSER>>()?.is_some() {
            Self::Superuser(s.parse()?)
        } else if s.parse::<Option<PASSWORD>>()?.is_some() {
            Self::Password(s.parse()?)
        } else {
            anyhow::bail!("Expected a role option!")
        })
    }
}

#[derive(ParseFromStr, Builder, Clone, Debug)]
#[builder(setter(strip_option))]
pub struct CreateRoleStatement {
    #[builder(default)]
    pub if_not_exists: bool,
    #[builder(setter(into))]
    pub name: Name,
    #[builder(default)]
    pub options: Option<Vec<RoleOpt>>,
}

impl Parse for CreateRoleStatement {
    type Output = Self;
    fn parse(s: &mut StatementStream<'_>) -> anyhow::Result<Self::Output> {
        s.parse::<(CREATE, ROLE)>()?;
        let mut res = CreateRoleStatementBuilder::default();
        res.if_not_exists(s.parse::<Option<(IF, NOT, EXISTS)>>()?.is_some())
            .name(s.parse::<Name>()?);
        if let Some(o) = s.parse_from::<If<WITH, List<RoleOpt, AND>>>()? {
            res.options(o);
        }
        s.parse::<Option<Semicolon>>()?;
        Ok(res
            .build()
            .map_err(|e| anyhow::anyhow!("Invalid CREATE ROLE statement: {}", e))?)
    }
}

impl Peek for CreateRoleStatement {
    fn peek(s: StatementStream<'_>) -> bool {
        s.check::<(CREATE, ROLE)>()
    }
}

#[derive(ParseFromStr, Builder, Clone, Debug)]
pub struct AlterRoleStatement {
    pub name: Name,
    pub options: Vec<RoleOpt>,
}

impl Parse for AlterRoleStatement {
    type Output = Self;
    fn parse(s: &mut StatementStream<'_>) -> anyhow::Result<Self::Output> {
        s.parse::<(ALTER, ROLE)>()?;
        let mut res = AlterRoleStatementBuilder::default();
        res.name(s.parse()?)
            .options(s.parse_from::<(WITH, List<RoleOpt, AND>)>()?.1);
        s.parse::<Option<Semicolon>>()?;
        Ok(res
            .build()
            .map_err(|e| anyhow::anyhow!("Invalid ALTER ROLE statement: {}", e))?)
    }
}

impl Peek for AlterRoleStatement {
    fn peek(s: StatementStream<'_>) -> bool {
        s.check::<(ALTER, ROLE)>()
    }
}

#[derive(ParseFromStr, Builder, Clone, Debug)]
pub struct DropRoleStatement {
    #[builder(default)]
    pub if_exists: bool,
    #[builder(setter(into))]
    pub name: Name,
}

impl Parse for DropRoleStatement {
    type Output = Self;
    fn parse(s: &mut StatementStream<'_>) -> anyhow::Result<Self::Output> {
        s.parse::<(DROP, ROLE)>()?;
        let mut res = DropRoleStatementBuilder::default();
        res.if_exists(s.parse::<Option<(IF, EXISTS)>>()?.is_some())
            .name(s.parse::<Name>()?);
        s.parse::<Option<Semicolon>>()?;
        Ok(res
            .build()
            .map_err(|e| anyhow::anyhow!("Invalid DROP ROLE statement: {}", e))?)
    }
}

impl Peek for DropRoleStatement {
    fn peek(s: StatementStream<'_>) -> bool {
        s.check::<(DROP, ROLE)>()
    }
}

#[derive(ParseFromStr, Builder, Clone, Debug)]
pub struct GrantRoleStatement {
    pub name: Name,
    pub to: Name,
}

impl Parse for GrantRoleStatement {
    type Output = Self;
    fn parse(s: &mut StatementStream<'_>) -> anyhow::Result<Self::Output> {
        s.parse::<GRANT>()?;
        let mut res = GrantRoleStatementBuilder::default();
        res.name(s.parse()?);
        s.parse::<TO>()?;
        res.to(s.parse()?);
        s.parse::<Option<Semicolon>>()?;
        Ok(res
            .build()
            .map_err(|e| anyhow::anyhow!("Invalid GRANT ROLE statement: {}", e))?)
    }
}

impl Peek for GrantRoleStatement {
    fn peek(s: StatementStream<'_>) -> bool {
        s.check::<(GRANT, Name, TO)>()
    }
}

#[derive(ParseFromStr, Builder, Clone, Debug)]
pub struct RevokeRoleStatement {
    pub name: Name,
    pub from: Name,
}

impl Parse for RevokeRoleStatement {
    type Output = Self;
    fn parse(s: &mut StatementStream<'_>) -> anyhow::Result<Self::Output> {
        s.parse::<REVOKE>()?;
        let mut res = RevokeRoleStatementBuilder::default();
        res.name(s.parse()?);
        s.parse::<FROM>()?;
        res.from(s.parse()?);
        s.parse::<Option<Semicolon>>()?;
        Ok(res
            .build()
            .map_err(|e| anyhow::anyhow!("Invalid REVOKE ROLE statement: {}", e))?)
    }
}

impl Peek for RevokeRoleStatement {
    fn peek(s: StatementStream<'_>) -> bool {
        s.check::<(REVOKE, Name, FROM)>()
    }
}

#[derive(ParseFromStr, Builder, Clone, Debug)]
#[builder(setter(strip_option))]
pub struct ListRolesStatement {
    #[builder(setter(into), default)]
    pub name: Option<Name>,
    #[builder(default)]
    pub no_recursive: bool,
}

impl Parse for ListRolesStatement {
    type Output = Self;
    fn parse(s: &mut StatementStream<'_>) -> anyhow::Result<Self::Output> {
        s.parse::<(LIST, ROLES)>()?;
        let mut res = ListRolesStatementBuilder::default();
        if let Some(n) = s.parse_from::<If<OF, Name>>()? {
            res.name(n);
        }
        res.no_recursive(s.parse::<Option<NORECURSIVE>>()?.is_some());
        s.parse::<Option<Semicolon>>()?;
        Ok(res
            .build()
            .map_err(|e| anyhow::anyhow!("Invalid LIST ROLES statement: {}", e))?)
    }
}

impl Peek for ListRolesStatement {
    fn peek(s: StatementStream<'_>) -> bool {
        s.check::<(LIST, ROLES)>()
    }
}

#[derive(ParseFromStr, Clone, Debug)]
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

impl Parse for Permission {
    type Output = Self;
    fn parse(s: &mut StatementStream<'_>) -> anyhow::Result<Self::Output> {
        Ok(match s.parse::<ReservedKeyword>()? {
            ReservedKeyword::CREATE => Permission::Create,
            ReservedKeyword::ALTER => Permission::Alter,
            ReservedKeyword::DROP => Permission::Drop,
            ReservedKeyword::SELECT => Permission::Select,
            ReservedKeyword::MODIFY => Permission::Modify,
            ReservedKeyword::AUTHORIZE => Permission::Authorize,
            ReservedKeyword::DESCRIBE => Permission::Describe,
            ReservedKeyword::EXECUTE => Permission::Execute,
            p @ _ => anyhow::bail!("Invalid permission: {}", p),
        })
    }
}

#[derive(ParseFromStr, Clone, Debug)]
pub enum PermissionKind {
    All,
    One(Permission),
}

impl Parse for PermissionKind {
    type Output = Self;
    fn parse(s: &mut StatementStream<'_>) -> anyhow::Result<Self::Output> {
        Ok(if s.parse::<Option<(ALL, Option<PERMISSIONS>)>>()?.is_some() {
            PermissionKind::All
        } else {
            PermissionKind::One(s.parse()?)
        })
    }
}

impl Peek for PermissionKind {
    fn peek(mut s: StatementStream<'_>) -> bool {
        s.parse::<Self>().is_ok()
    }
}

#[derive(ParseFromStr, Clone, Debug)]
pub enum Resource {
    AllKeyspaces,
    Keyspace(Name),
    Table(KeyspaceQualifiedName),
    AllRoles,
    Role(Name),
    AllFunctions { keyspace: Option<Name> },
    Function(FunctionReference),
    AllMBeans,
    MBean(LitStr),
}

impl Parse for Resource {
    type Output = Self;
    fn parse(s: &mut StatementStream<'_>) -> anyhow::Result<Self::Output> {
        Ok(if s.parse::<Option<(ALL, KEYSPACES)>>()?.is_some() {
            Self::AllKeyspaces
        } else if s.parse::<Option<KEYSPACE>>()?.is_some() {
            Self::Keyspace(s.parse()?)
        } else if s.parse::<Option<(ALL, ROLES)>>()?.is_some() {
            Self::AllRoles
        } else if s.parse::<Option<ROLE>>()?.is_some() {
            Self::Role(s.parse()?)
        } else if s.parse::<Option<(ALL, FUNCTIONS)>>()?.is_some() {
            Self::AllFunctions {
                keyspace: s.parse::<Option<(IN, KEYSPACE, _)>>()?.map(|i| i.2),
            }
        } else if s.parse::<Option<FUNCTION>>()?.is_some() {
            Self::Function(s.parse()?)
        } else if s.parse::<Option<(ALL, MBEANS)>>()?.is_some() {
            Self::AllMBeans
        } else if s.parse::<Option<MBEAN>>()?.is_some() {
            Self::MBean(s.parse()?)
        } else if s.parse::<Option<MBEANS>>()?.is_some() {
            Self::MBean(s.parse()?)
        } else if let Some(name) = s.parse::<Option<(Option<TABLE>, _)>>()? {
            Self::Table(name.1)
        } else {
            anyhow::bail!("Invalid resource: {}", s.parse_from::<Token>()?)
        })
    }
}

impl Peek for Resource {
    fn peek(s: StatementStream<'_>) -> bool {
        s.check::<(ALL, KEYSPACES)>()
            || s.check::<KEYSPACE>()
            || s.check::<(ALL, ROLES)>()
            || s.check::<ROLE>()
            || s.check::<(ALL, FUNCTIONS)>()
            || s.check::<FUNCTION>()
            || s.check::<(ALL, MBEANS)>()
            || s.check::<MBEAN>()
            || s.check::<MBEANS>()
            || s.check::<(Option<TABLE>, KeyspaceQualifiedName)>()
    }
}

#[derive(ParseFromStr, Clone, Debug, TryInto, From)]
pub enum PermissionStatement {
    Grant(GrantPermissionStatement),
    Revoke(RevokePermissionStatement),
    List(ListPermissionsStatement),
}

impl Parse for PermissionStatement {
    type Output = Self;
    fn parse(s: &mut StatementStream<'_>) -> anyhow::Result<Self::Output> {
        Ok(if let Some(stmt) = s.parse::<Option<GrantPermissionStatement>>()? {
            Self::Grant(stmt)
        } else if let Some(stmt) = s.parse::<Option<RevokePermissionStatement>>()? {
            Self::Revoke(stmt)
        } else if let Some(stmt) = s.parse::<Option<ListPermissionsStatement>>()? {
            Self::List(stmt)
        } else {
            anyhow::bail!("Expected a permission statement!")
        })
    }
}

impl Peek for PermissionStatement {
    fn peek(s: StatementStream<'_>) -> bool {
        s.check::<GrantPermissionStatement>()
            || s.check::<RevokePermissionStatement>()
            || s.check::<ListPermissionsStatement>()
    }
}

#[derive(ParseFromStr, Builder, Clone, Debug)]
pub struct GrantPermissionStatement {
    pub permissions: PermissionKind,
    pub resource: Resource,
    pub to: Name,
}

impl Parse for GrantPermissionStatement {
    type Output = Self;
    fn parse(s: &mut StatementStream<'_>) -> anyhow::Result<Self::Output> {
        s.parse::<GRANT>()?;
        let mut res = GrantPermissionStatementBuilder::default();
        res.permissions(s.parse()?);
        s.parse::<ON>()?;
        res.resource(s.parse()?);
        s.parse::<TO>()?;
        res.to(s.parse()?);
        s.parse::<Option<Semicolon>>()?;
        Ok(res
            .build()
            .map_err(|e| anyhow::anyhow!("Invalid GRANT PERMISSION statement: {}", e))?)
    }
}

impl Peek for GrantPermissionStatement {
    fn peek(s: StatementStream<'_>) -> bool {
        s.check::<(GRANT, PermissionKind, ON)>()
    }
}

#[derive(ParseFromStr, Builder, Clone, Debug)]
pub struct RevokePermissionStatement {
    pub permissions: PermissionKind,
    pub resource: Resource,
    pub from: Name,
}

impl Parse for RevokePermissionStatement {
    type Output = Self;
    fn parse(s: &mut StatementStream<'_>) -> anyhow::Result<Self::Output> {
        s.parse::<REVOKE>()?;
        let mut res = RevokePermissionStatementBuilder::default();
        res.permissions(s.parse()?);
        s.parse::<ON>()?;
        res.resource(s.parse()?);
        s.parse::<FROM>()?;
        res.from(s.parse()?);
        s.parse::<Option<Semicolon>>()?;
        Ok(res
            .build()
            .map_err(|e| anyhow::anyhow!("Invalid REVOKE PERMISSION statement: {}", e))?)
    }
}

impl Peek for RevokePermissionStatement {
    fn peek(s: StatementStream<'_>) -> bool {
        s.check::<(REVOKE, PermissionKind, ON)>()
    }
}

#[derive(ParseFromStr, Builder, Clone, Debug)]
#[builder(setter(strip_option))]
pub struct ListPermissionsStatement {
    pub permissions: PermissionKind,
    #[builder(default)]
    pub resource: Option<Resource>,
    #[builder(setter(into), default)]
    pub role: Option<Name>,
    #[builder(default)]
    pub no_recursive: bool,
}

impl Parse for ListPermissionsStatement {
    type Output = Self;
    fn parse(s: &mut StatementStream<'_>) -> anyhow::Result<Self::Output> {
        s.parse::<LIST>()?;
        let mut res = ListPermissionsStatementBuilder::default();
        res.permissions(s.parse()?);
        loop {
            if let Some(resource) = s.parse_from::<If<ON, Resource>>()? {
                if res.resource.is_some() {
                    anyhow::bail!("Duplicate ON RESOURCE clause!");
                }
                res.resource(resource);
            } else if let Some(role) = s.parse_from::<If<OF, Name>>()? {
                if res.role.is_some() {
                    anyhow::bail!("Duplicate OF ROLE clause!");
                }
                res.role(role).no_recursive(s.parse::<Option<NORECURSIVE>>()?.is_some());
            } else {
                break;
            }
        }
        s.parse::<Option<Semicolon>>()?;
        Ok(res
            .build()
            .map_err(|e| anyhow::anyhow!("Invalid LIST PERMISSION statement: {}", e))?)
    }
}

impl Peek for ListPermissionsStatement {
    fn peek(s: StatementStream<'_>) -> bool {
        s.check::<(LIST, PermissionKind)>()
    }
}

#[derive(ParseFromStr, Clone, Debug, TryInto, From)]
pub enum UserStatement {
    Create(CreateUserStatement),
    Alter(AlterUserStatement),
    Drop(DropUserStatement),
    List(ListUsersStatement),
}

impl Parse for UserStatement {
    type Output = Self;
    fn parse(s: &mut StatementStream<'_>) -> anyhow::Result<Self::Output> {
        Ok(if let Some(stmt) = s.parse::<Option<CreateUserStatement>>()? {
            Self::Create(stmt)
        } else if let Some(stmt) = s.parse::<Option<AlterUserStatement>>()? {
            Self::Alter(stmt)
        } else if let Some(stmt) = s.parse::<Option<DropUserStatement>>()? {
            Self::Drop(stmt)
        } else if let Some(stmt) = s.parse::<Option<ListUsersStatement>>()? {
            Self::List(stmt)
        } else {
            anyhow::bail!("Expected a user statement!")
        })
    }
}

impl Peek for UserStatement {
    fn peek(s: StatementStream<'_>) -> bool {
        s.check::<CreateUserStatement>()
            || s.check::<AlterUserStatement>()
            || s.check::<DropUserStatement>()
            || s.check::<ListUsersStatement>()
    }
}

#[derive(ParseFromStr, Builder, Clone, Debug)]
#[builder(setter(strip_option))]
pub struct CreateUserStatement {
    #[builder(default)]
    pub if_not_exists: bool,
    #[builder(setter(into))]
    pub name: Name,
    #[builder(setter(into), default)]
    pub with_password: Option<LitStr>,
    #[builder(default)]
    pub superuser: bool,
}

impl Parse for CreateUserStatement {
    type Output = Self;
    fn parse(s: &mut StatementStream<'_>) -> anyhow::Result<Self::Output> {
        s.parse::<(CREATE, USER)>()?;
        let mut res = CreateUserStatementBuilder::default();
        res.if_not_exists(s.parse::<Option<(IF, NOT, EXISTS)>>()?.is_some())
            .name(s.parse::<Name>()?);
        loop {
            if let Some(password) = s.parse_from::<If<(WITH, PASSWORD), LitStr>>()? {
                if res.with_password.is_some() {
                    anyhow::bail!("Duplicate WITH PASSWORD clause!");
                }
                res.with_password(password);
            } else if s.parse::<Option<SUPERUSER>>()?.is_some() {
                if res.superuser.is_some() {
                    anyhow::bail!("Duplicate SUPERUSER option definition!");
                }
                res.superuser(true);
            } else if s.parse::<Option<NOSUPERUSER>>()?.is_some() {
                if res.superuser.is_some() {
                    anyhow::bail!("Duplicate SUPERUSER option definition!");
                }
                res.superuser(false);
            } else {
                break;
            }
        }
        s.parse::<Option<Semicolon>>()?;
        Ok(res
            .build()
            .map_err(|e| anyhow::anyhow!("Invalid CREATE USER statement: {}", e))?)
    }
}

impl Peek for CreateUserStatement {
    fn peek(s: StatementStream<'_>) -> bool {
        s.check::<(CREATE, USER)>()
    }
}

#[derive(ParseFromStr, Builder, Clone, Debug)]
#[builder(setter(strip_option))]
pub struct AlterUserStatement {
    #[builder(setter(into))]
    pub name: Name,
    #[builder(setter(into), default)]
    pub with_password: Option<LitStr>,
    #[builder(default)]
    pub superuser: Option<bool>,
}

impl Parse for AlterUserStatement {
    type Output = Self;
    fn parse(s: &mut StatementStream<'_>) -> anyhow::Result<Self::Output> {
        s.parse::<(ALTER, USER)>()?;
        let mut res = AlterUserStatementBuilder::default();
        res.name(s.parse::<Name>()?);
        loop {
            if let Some(password) = s.parse_from::<If<(WITH, PASSWORD), LitStr>>()? {
                if res.with_password.is_some() {
                    anyhow::bail!("Duplicate WITH PASSWORD clause!");
                }
                res.with_password(password);
            } else if s.parse::<Option<SUPERUSER>>()?.is_some() {
                if res.superuser.is_some() {
                    anyhow::bail!("Duplicate SUPERUSER option definition!");
                }
                res.superuser(true);
            } else if s.parse::<Option<NOSUPERUSER>>()?.is_some() {
                if res.superuser.is_some() {
                    anyhow::bail!("Duplicate SUPERUSER option definition!");
                }
                res.superuser(false);
            } else {
                break;
            }
        }
        s.parse::<Option<Semicolon>>()?;
        Ok(res
            .build()
            .map_err(|e| anyhow::anyhow!("Invalid ALTER USER statement: {}", e))?)
    }
}

impl Peek for AlterUserStatement {
    fn peek(s: StatementStream<'_>) -> bool {
        s.check::<(ALTER, USER)>()
    }
}

#[derive(ParseFromStr, Builder, Clone, Debug)]
pub struct DropUserStatement {
    #[builder(default)]
    pub if_exists: bool,
    #[builder(setter(into))]
    pub name: Name,
}

impl Parse for DropUserStatement {
    type Output = Self;
    fn parse(s: &mut StatementStream<'_>) -> anyhow::Result<Self::Output> {
        s.parse::<(DROP, USER)>()?;
        let mut res = DropUserStatementBuilder::default();
        res.if_exists(s.parse::<Option<(IF, EXISTS)>>()?.is_some())
            .name(s.parse::<Name>()?);
        s.parse::<Option<Semicolon>>()?;
        Ok(res
            .build()
            .map_err(|e| anyhow::anyhow!("Invalid DROP USER statement: {}", e))?)
    }
}

impl Peek for DropUserStatement {
    fn peek(s: StatementStream<'_>) -> bool {
        s.check::<(DROP, USER)>()
    }
}

#[derive(Copy, Clone, Debug)]
pub struct ListUsersStatement;

impl Parse for ListUsersStatement {
    type Output = Self;
    fn parse(s: &mut StatementStream<'_>) -> anyhow::Result<Self::Output> {
        s.parse::<(LIST, USERS, Option<Semicolon>)>()?;
        Ok(ListUsersStatement)
    }
}

impl Peek for ListUsersStatement {
    fn peek(s: StatementStream<'_>) -> bool {
        s.check::<(LIST, USERS)>()
    }
}

#[derive(ParseFromStr, Clone, Debug, TryInto, From)]
pub enum UserDefinedTypeStatement {
    Create(CreateUserDefinedTypeStatement),
    Alter(AlterUserDefinedTypeStatement),
    Drop(DropUserDefinedTypeStatement),
}

impl Parse for UserDefinedTypeStatement {
    type Output = Self;
    fn parse(s: &mut StatementStream<'_>) -> anyhow::Result<Self::Output> {
        Ok(
            if let Some(stmt) = s.parse::<Option<CreateUserDefinedTypeStatement>>()? {
                Self::Create(stmt)
            } else if let Some(stmt) = s.parse::<Option<AlterUserDefinedTypeStatement>>()? {
                Self::Alter(stmt)
            } else if let Some(stmt) = s.parse::<Option<DropUserDefinedTypeStatement>>()? {
                Self::Drop(stmt)
            } else {
                anyhow::bail!("Invalid user defined type statement!")
            },
        )
    }
}

impl Peek for UserDefinedTypeStatement {
    fn peek(s: StatementStream<'_>) -> bool {
        s.check::<CreateUserDefinedTypeStatement>()
            || s.check::<AlterUserDefinedTypeStatement>()
            || s.check::<DropUserDefinedTypeStatement>()
    }
}

#[derive(ParseFromStr, Builder, Clone, Debug)]
pub struct CreateUserDefinedTypeStatement {
    #[builder(default)]
    pub if_not_exists: bool,
    pub name: KeyspaceQualifiedName,
    pub fields: Vec<FieldDefinition>,
}

impl Parse for CreateUserDefinedTypeStatement {
    type Output = Self;
    fn parse(s: &mut StatementStream<'_>) -> anyhow::Result<Self::Output> {
        s.parse::<(CREATE, TYPE)>()?;
        let mut res = CreateUserDefinedTypeStatementBuilder::default();
        res.if_not_exists(s.parse::<Option<(IF, NOT, EXISTS)>>()?.is_some())
            .name(s.parse()?)
            .fields(s.parse_from::<Parens<List<FieldDefinition, Comma>>>()?);
        s.parse::<Option<Semicolon>>()?;
        Ok(res
            .build()
            .map_err(|e| anyhow::anyhow!("Invalid CREATE TYPE statement: {}", e))?)
    }
}

impl Peek for CreateUserDefinedTypeStatement {
    fn peek(s: StatementStream<'_>) -> bool {
        s.check::<(CREATE, TYPE)>()
    }
}

#[derive(ParseFromStr, Clone, Debug)]
pub struct FieldDefinition {
    pub name: Name,
    pub data_type: CqlType,
}

impl Parse for FieldDefinition {
    type Output = Self;
    fn parse(s: &mut StatementStream<'_>) -> anyhow::Result<Self> {
        Ok(Self {
            name: s.parse()?,
            data_type: s.parse()?,
        })
    }
}

impl Peek for FieldDefinition {
    fn peek(s: StatementStream<'_>) -> bool {
        s.check::<(Name, CqlType)>()
    }
}

#[derive(ParseFromStr, Builder, Clone, Debug)]
pub struct AlterUserDefinedTypeStatement {
    pub name: KeyspaceQualifiedName,
    pub instruction: AlterTypeInstruction,
}

impl Parse for AlterUserDefinedTypeStatement {
    type Output = Self;
    fn parse(s: &mut StatementStream<'_>) -> anyhow::Result<Self::Output> {
        s.parse::<(ALTER, TYPE)>()?;
        let mut res = AlterUserDefinedTypeStatementBuilder::default();
        res.name(s.parse()?).instruction(s.parse()?);
        s.parse::<Option<Semicolon>>()?;
        Ok(res
            .build()
            .map_err(|e| anyhow::anyhow!("Invalid ALTER TYPE statement: {}", e))?)
    }
}

impl Peek for AlterUserDefinedTypeStatement {
    fn peek(s: StatementStream<'_>) -> bool {
        s.check::<(ALTER, TYPE)>()
    }
}

#[derive(ParseFromStr, Clone, Debug)]
pub enum AlterTypeInstruction {
    Add(FieldDefinition),
    Rename(Vec<(Name, Name)>),
}

impl Parse for AlterTypeInstruction {
    type Output = Self;
    fn parse(s: &mut StatementStream<'_>) -> anyhow::Result<Self::Output> {
        Ok(if s.parse::<Option<ADD>>()?.is_some() {
            Self::Add(s.parse()?)
        } else if s.parse::<Option<RENAME>>()?.is_some() {
            Self::Rename(
                s.parse_from::<List<(Name, TO, Name), Nothing>>()?
                    .into_iter()
                    .map(|(a, _, b)| (a, b))
                    .collect(),
            )
        } else {
            anyhow::bail!("Invalid ALTER TYPE instruction!");
        })
    }
}

#[derive(ParseFromStr, Builder, Clone, Debug)]
pub struct DropUserDefinedTypeStatement {
    #[builder(default)]
    pub if_exists: bool,
    pub name: KeyspaceQualifiedName,
}

impl Parse for DropUserDefinedTypeStatement {
    type Output = Self;
    fn parse(s: &mut StatementStream<'_>) -> anyhow::Result<Self::Output> {
        s.parse::<(DROP, TYPE)>()?;
        let mut res = DropUserDefinedTypeStatementBuilder::default();
        res.if_exists(s.parse::<Option<(IF, EXISTS)>>()?.is_some())
            .name(s.parse()?);
        s.parse::<Option<Semicolon>>()?;
        Ok(res
            .build()
            .map_err(|e| anyhow::anyhow!("Invalid DROP TYPE statement: {}", e))?)
    }
}

impl Peek for DropUserDefinedTypeStatement {
    fn peek(s: StatementStream<'_>) -> bool {
        s.check::<(DROP, TYPE)>()
    }
}

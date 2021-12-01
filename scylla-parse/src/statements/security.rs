// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use super::*;
use std::collections::BTreeSet;

#[derive(ParseFromStr, Clone, Debug, TryInto, From, ToTokens, PartialEq, Eq)]
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
            anyhow::bail!("Expected a role statement, found {}", s.info())
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

#[derive(ParseFromStr, Clone, Debug, Ord, PartialOrd, Eq, ToTokens)]
pub enum RoleOpt {
    Password(LitStr),
    Login(bool),
    Superuser(bool),
    Options(MapLiteral),
    AccessToDatacenters(SetLiteral),
    AccessToAllDatacenters,
}

impl RoleOpt {
    pub fn password<T: Into<LitStr>>(password: T) -> Self {
        Self::Password(password.into())
    }

    pub fn login(login: bool) -> Self {
        Self::Login(login)
    }

    pub fn superuser(superuser: bool) -> Self {
        Self::Superuser(superuser)
    }

    pub fn options<T: Into<MapLiteral>>(options: T) -> Self {
        Self::Options(options.into())
    }

    pub fn access_to_datacenters<T: Into<SetLiteral>>(datacenters: T) -> Self {
        Self::AccessToDatacenters(datacenters.into())
    }

    pub fn access_to_all_datacenters() -> Self {
        Self::AccessToAllDatacenters
    }
}

impl PartialEq for RoleOpt {
    fn eq(&self, other: &Self) -> bool {
        core::mem::discriminant(self) == core::mem::discriminant(other)
    }
}

impl Parse for RoleOpt {
    type Output = Self;
    fn parse(s: &mut StatementStream<'_>) -> anyhow::Result<Self::Output> {
        Ok(if s.parse::<Option<(ACCESS, TO, ALL, DATACENTERS)>>()?.is_some() {
            Self::AccessToAllDatacenters
        } else if s.parse::<Option<(ACCESS, TO, DATACENTERS)>>()?.is_some() {
            Self::AccessToDatacenters(s.parse()?)
        } else if let Some(m) = s.parse_from::<If<(OPTIONS, Equals), MapLiteral>>()? {
            Self::Options(m)
        } else if let Some(b) = s.parse_from::<If<(LOGIN, Equals), bool>>()? {
            Self::Login(b)
        } else if let Some(b) = s.parse_from::<If<(SUPERUSER, Equals), bool>>()? {
            Self::Superuser(b)
        } else if let Some(p) = s.parse_from::<If<(PASSWORD, Equals), LitStr>>()? {
            Self::Password(p)
        } else {
            anyhow::bail!("Expected a role option, found {}", s.info())
        })
    }
}

impl Display for RoleOpt {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Password(s) => write!(f, "PASSWORD = {}", s),
            Self::Login(b) => write!(f, "LOGIN = {}", b),
            Self::Superuser(b) => write!(f, "SUPERUSER = {}", b),
            Self::Options(m) => write!(f, "OPTIONS = {}", m),
            Self::AccessToDatacenters(s) => write!(f, "ACCESS TO DATACENTERS {}", s),
            Self::AccessToAllDatacenters => write!(f, "ACCESS TO ALL DATACENTERS"),
        }
    }
}

pub trait RoleOptBuilderExt {
    fn role_opts(&mut self) -> &mut Option<BTreeSet<RoleOpt>>;

    fn password(&mut self, p: impl Into<LitStr>) -> &mut Self {
        self.role_opts()
            .get_or_insert_with(Default::default)
            .insert(RoleOpt::Password(p.into()));
        self
    }

    fn login(&mut self, b: bool) -> &mut Self {
        self.role_opts()
            .get_or_insert_with(Default::default)
            .insert(RoleOpt::Login(b));
        self
    }

    fn superuser(&mut self, b: bool) -> &mut Self {
        self.role_opts()
            .get_or_insert_with(Default::default)
            .insert(RoleOpt::Superuser(b));
        self
    }

    fn role_options(&mut self, m: impl Into<MapLiteral>) -> &mut Self {
        self.role_opts()
            .get_or_insert_with(Default::default)
            .insert(RoleOpt::Options(m.into()));
        self
    }

    fn access_to_datacenters(&mut self, s: impl Into<SetLiteral>) -> &mut Self {
        self.role_opts()
            .get_or_insert_with(Default::default)
            .insert(RoleOpt::AccessToDatacenters(s.into()));
        self
    }

    fn access_to_all_datacenters(&mut self) -> &mut Self {
        self.role_opts()
            .get_or_insert_with(Default::default)
            .insert(RoleOpt::AccessToAllDatacenters);
        self
    }
}

#[derive(ParseFromStr, Builder, Clone, Debug, ToTokens, PartialEq, Eq)]
#[builder(setter(strip_option))]
pub struct CreateRoleStatement {
    #[builder(setter(name = "set_if_not_exists"), default)]
    pub if_not_exists: bool,
    #[builder(setter(into))]
    pub name: Name,
    #[builder(default)]
    pub options: Option<BTreeSet<RoleOpt>>,
}

impl CreateRoleStatementBuilder {
    /// Set IF NOT EXISTS on the statement.
    /// To undo this, use `set_if_not_exists(false)`.
    pub fn if_not_exists(&mut self) -> &mut Self {
        self.if_not_exists.replace(true);
        self
    }
}

impl Parse for CreateRoleStatement {
    type Output = Self;
    fn parse(s: &mut StatementStream<'_>) -> anyhow::Result<Self::Output> {
        s.parse::<(CREATE, ROLE)>()?;
        let mut res = CreateRoleStatementBuilder::default();
        res.set_if_not_exists(s.parse::<Option<(IF, NOT, EXISTS)>>()?.is_some())
            .name(s.parse::<Name>()?);
        if let Some(o) = s.parse_from::<If<WITH, List<RoleOpt, AND>>>()? {
            let mut opts = BTreeSet::new();
            for opt in o {
                if opts.contains(&opt) {
                    anyhow::bail!("Duplicate option: {}", opt);
                } else {
                    opts.insert(opt);
                }
            }
            res.options(opts);
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

impl Display for CreateRoleStatement {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "CREATE ROLE{} {}{}",
            if self.if_not_exists { "IF NOT EXISTS" } else { "" },
            self.name,
            if let Some(ref opts) = self.options {
                if !opts.is_empty() {
                    format!(
                        " WITH {}",
                        opts.iter().map(|i| i.to_string()).collect::<Vec<_>>().join(" AND ")
                    )
                } else {
                    String::new()
                }
            } else {
                String::new()
            }
        )
    }
}

impl RoleOptBuilderExt for CreateRoleStatementBuilder {
    fn role_opts(&mut self) -> &mut Option<BTreeSet<RoleOpt>> {
        match self.options {
            Some(ref mut opts) => opts,
            None => {
                self.options = Some(Some(BTreeSet::new()));
                self.options.as_mut().unwrap()
            }
        }
    }
}

#[derive(ParseFromStr, Builder, Clone, Debug, ToTokens, PartialEq, Eq)]
#[builder(build_fn(validate = "Self::validate"))]
pub struct AlterRoleStatement {
    #[builder(setter(into))]
    pub name: Name,
    pub options: BTreeSet<RoleOpt>,
}

impl AlterRoleStatementBuilder {
    fn validate(&self) -> Result<(), String> {
        if self.options.as_ref().map(|s| s.is_empty()).unwrap_or(false) {
            return Err("Role options cannot be empty".to_string());
        }
        Ok(())
    }
}

impl Parse for AlterRoleStatement {
    type Output = Self;
    fn parse(s: &mut StatementStream<'_>) -> anyhow::Result<Self::Output> {
        s.parse::<(ALTER, ROLE)>()?;
        let mut res = AlterRoleStatementBuilder::default();
        res.name(s.parse::<Name>()?);
        let o = s.parse_from::<(WITH, List<RoleOpt, AND>)>()?.1;
        let mut opts = BTreeSet::new();
        for opt in o {
            if opts.contains(&opt) {
                anyhow::bail!("Duplicate option: {}", opt);
            } else {
                opts.insert(opt);
            }
        }
        res.options(opts);
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

impl Display for AlterRoleStatement {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "ALTER ROLE {} WITH {}",
            self.name,
            self.options
                .iter()
                .map(|i| i.to_string())
                .collect::<Vec<_>>()
                .join(" AND ")
        )
    }
}

impl RoleOptBuilderExt for AlterRoleStatementBuilder {
    fn role_opts(&mut self) -> &mut Option<BTreeSet<RoleOpt>> {
        &mut self.options
    }
}

#[derive(ParseFromStr, Builder, Clone, Debug, ToTokens, PartialEq, Eq)]
pub struct DropRoleStatement {
    #[builder(setter(name = "set_if_exists"), default)]
    pub if_exists: bool,
    #[builder(setter(into))]
    pub name: Name,
}

impl DropRoleStatementBuilder {
    /// Set IF EXISTS on the statement.
    /// To undo this, use `set_if_exists(false)`.
    pub fn if_exists(&mut self) -> &mut Self {
        self.if_exists.replace(true);
        self
    }
}

impl Parse for DropRoleStatement {
    type Output = Self;
    fn parse(s: &mut StatementStream<'_>) -> anyhow::Result<Self::Output> {
        s.parse::<(DROP, ROLE)>()?;
        let mut res = DropRoleStatementBuilder::default();
        loop {
            if s.remaining() == 0 || s.parse::<Option<Semicolon>>()?.is_some() {
                break;
            }
            if s.parse::<Option<(IF, EXISTS)>>()?.is_some() {
                res.if_exists();
            } else if let Some(n) = s.parse::<Option<Name>>()? {
                res.name(n);
            } else {
                return Ok(res
                    .build()
                    .map_err(|_| anyhow::anyhow!("Invalid tokens in DROP ROLE statement: {}", s.info()))?);
            }
        }
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

impl Display for DropRoleStatement {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "DROP ROLE{} {}",
            if self.if_exists { " IF EXISTS" } else { "" },
            self.name
        )
    }
}

#[derive(ParseFromStr, Builder, Clone, Debug, ToTokens, PartialEq, Eq)]
#[builder(setter(into))]
pub struct GrantRoleStatement {
    pub name: Name,
    pub to: Name,
}

impl Parse for GrantRoleStatement {
    type Output = Self;
    fn parse(s: &mut StatementStream<'_>) -> anyhow::Result<Self::Output> {
        s.parse::<GRANT>()?;
        let mut res = GrantRoleStatementBuilder::default();
        res.name(s.parse::<Name>()?);
        s.parse::<TO>()?;
        res.to(s.parse::<Name>()?);
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

impl Display for GrantRoleStatement {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "GRANT {} TO {}", self.name, self.to)
    }
}

#[derive(ParseFromStr, Builder, Clone, Debug, ToTokens, PartialEq, Eq)]
#[builder(setter(into))]
pub struct RevokeRoleStatement {
    pub name: Name,
    pub from: Name,
}

impl Parse for RevokeRoleStatement {
    type Output = Self;
    fn parse(s: &mut StatementStream<'_>) -> anyhow::Result<Self::Output> {
        s.parse::<REVOKE>()?;
        let mut res = RevokeRoleStatementBuilder::default();
        res.name(s.parse::<Name>()?);
        s.parse::<FROM>()?;
        res.from(s.parse::<Name>()?);
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

impl Display for RevokeRoleStatement {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "REVOKE {} FROM {}", self.name, self.from)
    }
}

#[derive(ParseFromStr, Builder, Clone, Debug, ToTokens, PartialEq, Eq)]
#[builder(setter(strip_option))]
pub struct ListRolesStatement {
    #[builder(setter(into), default)]
    pub of: Option<Name>,
    #[builder(setter(name = "set_no_recursive"), default)]
    pub no_recursive: bool,
}

impl ListRolesStatementBuilder {
    /// Set NO RECURSIVE on the statement.
    /// To undo this, use `set_no_recursive(false)`.
    pub fn no_recursive(&mut self) -> &mut Self {
        self.no_recursive.replace(true);
        self
    }
}

impl Parse for ListRolesStatement {
    type Output = Self;
    fn parse(s: &mut StatementStream<'_>) -> anyhow::Result<Self::Output> {
        s.parse::<(LIST, ROLES)>()?;
        let mut res = ListRolesStatementBuilder::default();
        if let Some(n) = s.parse_from::<If<OF, Name>>()? {
            res.of(n);
        }
        res.set_no_recursive(s.parse::<Option<NORECURSIVE>>()?.is_some());
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

impl Display for ListRolesStatement {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "LIST ROLES{}{}",
            if let Some(n) = &self.of {
                format!(" OF {}", n)
            } else {
                String::new()
            },
            if self.no_recursive { " NORECURSIVE" } else { "" },
        )
    }
}

#[derive(ParseFromStr, Clone, Debug, ToTokens, PartialEq, Eq)]
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
        Ok(match s.parse::<(ReservedKeyword, Option<PERMISSION>)>()?.0 {
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

impl Display for Permission {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}",
            match self {
                Permission::Create => "CREATE",
                Permission::Alter => "ALTER",
                Permission::Drop => "DROP",
                Permission::Select => "SELECT",
                Permission::Modify => "MODIFY",
                Permission::Authorize => "AUTHORIZE",
                Permission::Describe => "DESCRIBE",
                Permission::Execute => "EXECUTE",
            }
        )
    }
}

#[derive(ParseFromStr, Clone, Debug, ToTokens, PartialEq, Eq)]
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

impl Display for PermissionKind {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            PermissionKind::All => write!(f, "ALL PERMISSIONS"),
            PermissionKind::One(p) => write!(f, "{} PERMISSION", p),
        }
    }
}

impl From<Permission> for PermissionKind {
    fn from(p: Permission) -> Self {
        PermissionKind::One(p)
    }
}

#[derive(ParseFromStr, Clone, Debug, ToTokens, PartialEq, Eq)]
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

impl Resource {
    pub fn all_keyspaces() -> Self {
        Resource::AllKeyspaces
    }

    pub fn keyspace(name: impl Into<Name>) -> Self {
        Resource::Keyspace(name.into())
    }

    pub fn table(name: impl Into<KeyspaceQualifiedName>) -> Self {
        Resource::Table(name.into())
    }

    pub fn all_roles() -> Self {
        Resource::AllRoles
    }

    pub fn role(name: impl Into<Name>) -> Self {
        Resource::Role(name.into())
    }

    pub fn all_functions() -> Self {
        Resource::AllFunctions { keyspace: None }
    }

    pub fn all_functions_in_keyspace(keyspace: impl Into<Name>) -> Self {
        Resource::AllFunctions {
            keyspace: Some(keyspace.into()),
        }
    }

    pub fn function(name: impl Into<FunctionReference>) -> Self {
        Resource::Function(name.into())
    }

    pub fn all_mbeans() -> Self {
        Resource::AllMBeans
    }

    pub fn mbean(name: impl Into<LitStr>) -> Self {
        Resource::MBean(name.into())
    }
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
        } else if let Some(t) = s.parse_from::<If<TABLE, KeyspaceQualifiedName>>()? {
            Self::Table(t)
        } else if let Some(name) = s.parse::<Option<KeyspaceQualifiedName>>()? {
            Self::Table(name)
        } else {
            anyhow::bail!("Invalid resource: {}", s.info())
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

impl Display for Resource {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Resource::AllKeyspaces => write!(f, "ALL KEYSPACES"),
            Resource::Keyspace(n) => write!(f, "KEYSPACE {}", n),
            Resource::Table(n) => write!(f, "TABLE {}", n),
            Resource::AllRoles => write!(f, "ALL ROLES"),
            Resource::Role(n) => write!(f, "ROLE {}", n),
            Resource::AllFunctions { keyspace } => {
                write!(f, "ALL FUNCTIONS")?;
                if let Some(k) = keyspace {
                    write!(f, " IN KEYSPACE {}", k)?;
                }
                Ok(())
            }
            Resource::Function(r) => write!(f, "FUNCTION {}", r),
            Resource::AllMBeans => write!(f, "ALL MBEANS"),
            Resource::MBean(n) => write!(f, "MBEAN {}", n),
        }
    }
}

#[derive(ParseFromStr, Clone, Debug, TryInto, From, ToTokens, PartialEq, Eq)]
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
            anyhow::bail!("Expected a permission statement, found {}", s.info())
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

#[derive(ParseFromStr, Builder, Clone, Debug, ToTokens, PartialEq, Eq)]
#[builder(setter(into))]
pub struct GrantPermissionStatement {
    pub permission: PermissionKind,
    pub resource: Resource,
    pub to: Name,
}

impl Parse for GrantPermissionStatement {
    type Output = Self;
    fn parse(s: &mut StatementStream<'_>) -> anyhow::Result<Self::Output> {
        s.parse::<GRANT>()?;
        let mut res = GrantPermissionStatementBuilder::default();
        res.permission(s.parse::<PermissionKind>()?);
        s.parse::<ON>()?;
        res.resource(s.parse::<Resource>()?);
        s.parse::<TO>()?;
        res.to(s.parse::<Name>()?);
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

impl Display for GrantPermissionStatement {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "GRANT {} ON {} TO {}", self.permission, self.resource, self.to)
    }
}

#[derive(ParseFromStr, Builder, Clone, Debug, ToTokens, PartialEq, Eq)]
#[builder(setter(into))]
pub struct RevokePermissionStatement {
    pub permission: PermissionKind,
    pub resource: Resource,
    pub from: Name,
}

impl Parse for RevokePermissionStatement {
    type Output = Self;
    fn parse(s: &mut StatementStream<'_>) -> anyhow::Result<Self::Output> {
        s.parse::<REVOKE>()?;
        let mut res = RevokePermissionStatementBuilder::default();
        res.permission(s.parse::<PermissionKind>()?);
        s.parse::<ON>()?;
        res.resource(s.parse::<Resource>()?);
        s.parse::<FROM>()?;
        res.from(s.parse::<Name>()?);
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

impl Display for RevokePermissionStatement {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "REVOKE {} ON {} FROM {}", self.permission, self.resource, self.from)
    }
}

#[derive(ParseFromStr, Builder, Clone, Debug, ToTokens, PartialEq, Eq)]
#[builder(setter(strip_option))]
pub struct ListPermissionsStatement {
    #[builder(setter(into))]
    pub permission: PermissionKind,
    #[builder(setter(into), default)]
    pub resource: Option<Resource>,
    #[builder(setter(into), default)]
    pub of: Option<Name>,
    #[builder(setter(name = "set_no_recursive"), default)]
    pub no_recursive: bool,
}

impl ListPermissionsStatementBuilder {
    /// Set NO RECURSIVE on the statement.
    /// To undo this, use `set_no_recursive(false)`.
    pub fn no_recursive(&mut self) -> &mut Self {
        self.no_recursive.replace(true);
        self
    }
}

impl Parse for ListPermissionsStatement {
    type Output = Self;
    fn parse(s: &mut StatementStream<'_>) -> anyhow::Result<Self::Output> {
        s.parse::<LIST>()?;
        let mut res = ListPermissionsStatementBuilder::default();
        res.permission(s.parse::<PermissionKind>()?);
        loop {
            if s.remaining() == 0 || s.parse::<Option<Semicolon>>()?.is_some() {
                break;
            }
            if let Some(resource) = s.parse_from::<If<ON, Resource>>()? {
                if res.resource.is_some() {
                    anyhow::bail!("Duplicate ON RESOURCE clause!");
                }
                res.resource(resource);
            } else if let Some(role) = s.parse_from::<If<OF, Name>>()? {
                if res.of.is_some() {
                    anyhow::bail!("Duplicate OF ROLE clause!");
                }
                res.of(role)
                    .set_no_recursive(s.parse::<Option<NORECURSIVE>>()?.is_some());
            } else {
                return Ok(res
                    .build()
                    .map_err(|_| anyhow::anyhow!("Invalid tokens in LIST PERMISSION statement: {}", s.info()))?);
            }
        }
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

impl Display for ListPermissionsStatement {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "LIST {}{}",
            self.permission,
            if let Some(resource) = &self.resource {
                format!(" ON {}", resource)
            } else {
                String::new()
            }
        )?;
        if let Some(role) = &self.of {
            write!(f, " OF {}", role)?;
            if self.no_recursive {
                write!(f, " NORECURSIVE")?;
            }
        }
        Ok(())
    }
}

#[derive(ParseFromStr, Clone, Debug, TryInto, From, ToTokens, PartialEq, Eq)]
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
            anyhow::bail!("Expected a user statement, found {}", s.info())
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

#[derive(ParseFromStr, Builder, Clone, Debug, ToTokens, PartialEq, Eq)]
#[builder(setter(strip_option))]
pub struct CreateUserStatement {
    #[builder(setter(name = "set_if_not_exists"), default)]
    pub if_not_exists: bool,
    #[builder(setter(into))]
    pub name: Name,
    #[builder(setter(into), default)]
    pub with_password: Option<LitStr>,
    #[builder(default)]
    pub superuser: Option<bool>,
}

impl CreateUserStatementBuilder {
    /// Set IF NOT EXISTS on the statement.
    /// To undo this, use `set_if_not_exists(false)`.
    pub fn if_not_exists(&mut self) -> &mut Self {
        self.if_not_exists.replace(true);
        self
    }
}

impl Parse for CreateUserStatement {
    type Output = Self;
    fn parse(s: &mut StatementStream<'_>) -> anyhow::Result<Self::Output> {
        s.parse::<(CREATE, USER)>()?;
        let mut res = CreateUserStatementBuilder::default();
        res.set_if_not_exists(s.parse::<Option<(IF, NOT, EXISTS)>>()?.is_some())
            .name(s.parse::<Name>()?);
        loop {
            if s.remaining() == 0 || s.parse::<Option<Semicolon>>()?.is_some() {
                break;
            }
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
                return Ok(res
                    .build()
                    .map_err(|_| anyhow::anyhow!("Invalid tokens in CREATE USER statement: {}", s.info()))?);
            }
        }
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

impl Display for CreateUserStatement {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "CREATE USER{} {}",
            if self.if_not_exists { " IF NOT EXISTS" } else { "" },
            self.name
        )?;
        if let Some(password) = &self.with_password {
            write!(f, " WITH PASSWORD {}", password)?;
        }
        if let Some(superuser) = self.superuser {
            write!(f, " {}", if superuser { "SUPERUSER" } else { "NOSUPERUSER" })?;
        }
        Ok(())
    }
}

#[derive(ParseFromStr, Builder, Clone, Debug, ToTokens, PartialEq, Eq)]
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
            if s.remaining() == 0 || s.parse::<Option<Semicolon>>()?.is_some() {
                break;
            }
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
                return Ok(res
                    .build()
                    .map_err(|_| anyhow::anyhow!("Invalid tokens in ALTER USER statement: {}", s.info()))?);
            }
        }
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

impl Display for AlterUserStatement {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "ALTER USER {}", self.name)?;
        if let Some(password) = &self.with_password {
            write!(f, " WITH PASSWORD {}", password)?;
        }
        if let Some(superuser) = self.superuser {
            write!(f, " {}", if superuser { "SUPERUSER" } else { "NOSUPERUSER" })?;
        }
        Ok(())
    }
}

#[derive(ParseFromStr, Builder, Clone, Debug, ToTokens, PartialEq, Eq)]
pub struct DropUserStatement {
    #[builder(setter(name = "set_if_exists"), default)]
    pub if_exists: bool,
    #[builder(setter(into))]
    pub name: Name,
}

impl DropUserStatementBuilder {
    /// Set IF EXISTS on the statement.
    /// To undo this, use `set_if_exists(false)`.
    pub fn if_exists(&mut self) -> &mut Self {
        self.if_exists.replace(true);
        self
    }
}

impl Parse for DropUserStatement {
    type Output = Self;
    fn parse(s: &mut StatementStream<'_>) -> anyhow::Result<Self::Output> {
        s.parse::<(DROP, USER)>()?;
        let mut res = DropUserStatementBuilder::default();
        res.set_if_exists(s.parse::<Option<(IF, EXISTS)>>()?.is_some())
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

impl Display for DropUserStatement {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "DROP USER{} {}",
            if self.if_exists { " IF EXISTS" } else { "" },
            self.name
        )
    }
}

#[derive(ParseFromStr, Copy, Clone, Debug, ToTokens, PartialEq, Eq)]
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

impl Display for ListUsersStatement {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "LIST USERS")
    }
}

#[derive(ParseFromStr, Clone, Debug, TryInto, From, ToTokens, PartialEq, Eq)]
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

#[derive(ParseFromStr, Builder, Clone, Debug, ToTokens, PartialEq, Eq)]
#[builder(build_fn(validate = "Self::validate"))]
pub struct CreateUserDefinedTypeStatement {
    #[builder(setter(name = "set_if_not_exists"), default)]
    pub if_not_exists: bool,
    #[builder(setter(into))]
    pub name: KeyspaceQualifiedName,
    pub fields: Vec<FieldDefinition>,
}

impl CreateUserDefinedTypeStatementBuilder {
    /// Set IF NOT EXISTS on the statement.
    /// To undo this, use `set_if_not_exists(false)`.
    pub fn if_not_exists(&mut self) -> &mut Self {
        self.if_not_exists.replace(true);
        self
    }

    fn validate(&self) -> Result<(), String> {
        if self.fields.as_ref().map(|s| s.is_empty()).unwrap_or(false) {
            return Err("Field definitions cannot be empty".to_string());
        }
        Ok(())
    }
}

impl Parse for CreateUserDefinedTypeStatement {
    type Output = Self;
    fn parse(s: &mut StatementStream<'_>) -> anyhow::Result<Self::Output> {
        s.parse::<(CREATE, TYPE)>()?;
        let mut res = CreateUserDefinedTypeStatementBuilder::default();
        res.set_if_not_exists(s.parse::<Option<(IF, NOT, EXISTS)>>()?.is_some())
            .name(s.parse::<KeyspaceQualifiedName>()?)
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

impl Display for CreateUserDefinedTypeStatement {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "CREATE TYPE{} {} ({})",
            if self.if_not_exists { " IF NOT EXISTS" } else { "" },
            self.name,
            self.fields.iter().map(|f| f.to_string()).collect::<Vec<_>>().join(", ")
        )
    }
}

#[derive(ParseFromStr, Builder, Clone, Debug, ToTokens, PartialEq, Eq)]
#[builder(build_fn(validate = "Self::validate"))]
pub struct AlterUserDefinedTypeStatement {
    #[builder(setter(into))]
    pub name: KeyspaceQualifiedName,
    pub instruction: AlterTypeInstruction,
}

impl AlterUserDefinedTypeStatementBuilder {
    fn validate(&self) -> Result<(), String> {
        if self
            .instruction
            .as_ref()
            .map(|s| match s {
                AlterTypeInstruction::Rename(s) => s.is_empty(),
                _ => false,
            })
            .unwrap_or(false)
        {
            return Err("RENAME clause cannot be empty".to_string());
        }
        Ok(())
    }
}

impl Parse for AlterUserDefinedTypeStatement {
    type Output = Self;
    fn parse(s: &mut StatementStream<'_>) -> anyhow::Result<Self::Output> {
        s.parse::<(ALTER, TYPE)>()?;
        let mut res = AlterUserDefinedTypeStatementBuilder::default();
        res.name(s.parse::<KeyspaceQualifiedName>()?).instruction(s.parse()?);
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

impl Display for AlterUserDefinedTypeStatement {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "ALTER TYPE {} {}", self.name, self.instruction)
    }
}

#[derive(ParseFromStr, Clone, Debug, ToTokens, PartialEq, Eq)]
pub enum AlterTypeInstruction {
    Add(FieldDefinition),
    Rename(Vec<(Name, Name)>),
}

impl AlterTypeInstruction {
    pub fn add<T: Into<FieldDefinition>>(field: T) -> Self {
        Self::Add(field.into())
    }

    pub fn rename(renames: Vec<(impl Into<Name>, impl Into<Name>)>) -> Self {
        Self::Rename(renames.into_iter().map(|(from, to)| (from.into(), to.into())).collect())
    }
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

impl Display for AlterTypeInstruction {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Add(d) => write!(f, "ADD {}", d),
            Self::Rename(renames) => {
                let renames = renames
                    .iter()
                    .map(|(a, b)| format!(" {} TO {}", a, b))
                    .collect::<String>();
                write!(f, "RENAME{}", renames)
            }
        }
    }
}

#[derive(ParseFromStr, Builder, Clone, Debug, ToTokens, PartialEq, Eq)]
pub struct DropUserDefinedTypeStatement {
    #[builder(setter(name = "set_if_exists"), default)]
    pub if_exists: bool,
    #[builder(setter(into))]
    pub name: KeyspaceQualifiedName,
}

impl DropUserDefinedTypeStatementBuilder {
    /// Set IF EXISTS on the statement.
    /// To undo this, use `set_if_exists(false)`.
    pub fn if_exists(&mut self) -> &mut Self {
        self.if_exists.replace(true);
        self
    }
}

impl Parse for DropUserDefinedTypeStatement {
    type Output = Self;
    fn parse(s: &mut StatementStream<'_>) -> anyhow::Result<Self::Output> {
        s.parse::<(DROP, TYPE)>()?;
        let mut res = DropUserDefinedTypeStatementBuilder::default();
        res.set_if_exists(s.parse::<Option<(IF, EXISTS)>>()?.is_some())
            .name(s.parse::<KeyspaceQualifiedName>()?);
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

impl Display for DropUserDefinedTypeStatement {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "DROP TYPE{} {}",
            if self.if_exists { " IF EXISTS" } else { "" },
            self.name
        )
    }
}

#[cfg(test)]
mod test {
    use crate::{
        CollectionType,
        Constant,
        KeyspaceQualifyExt,
        NativeType,
    };

    use super::*;

    #[test]
    fn test_parse_create_role() {
        let mut builder = CreateRoleStatementBuilder::default();
        builder.name("test_role");
        let statement = builder.build().unwrap().to_string();
        assert_eq!(builder.build().unwrap(), statement.parse().unwrap());
        builder.options(maplit::btreeset! {
            RoleOpt::password("test_password"),
        });
        let statement = builder.build().unwrap().to_string();
        assert_eq!(builder.build().unwrap(), statement.parse().unwrap());
        builder.if_not_exists();
        let statement = builder.build().unwrap().to_string();
        assert_eq!(builder.build().unwrap(), statement.parse().unwrap());
        builder.options(maplit::btreeset! {
            RoleOpt::password("test_password"),
            RoleOpt::login(true),
            RoleOpt::superuser(false),
            RoleOpt::access_to_datacenters(maplit::btreeset!{
                Constant::string("dc1"),
                Constant::string("dc2"),
            }),
            RoleOpt::access_to_all_datacenters(),
            RoleOpt::options(maplit::btreemap! {
                Constant::string("custom_option1") => Constant::string("custom value1"),
                Constant::string("custom_option2") => 99_i32.into(),
            })
        });
        let statement = builder.build().unwrap().to_string();
        assert_eq!(builder.build().unwrap(), statement.parse().unwrap());
    }

    #[test]
    fn test_parse_alter_role() {
        let mut builder = AlterRoleStatementBuilder::default();
        builder.name("test_role");
        assert!(builder.build().is_err());
        builder.options(BTreeSet::new());
        assert!(builder.build().is_err());
        builder.options(maplit::btreeset! {
            RoleOpt::password("test_password"),
        });
        let statement = builder.build().unwrap().to_string();
        assert_eq!(builder.build().unwrap(), statement.parse().unwrap());
        builder.options(maplit::btreeset! {
            RoleOpt::password("test_password"),
            RoleOpt::login(true),
            RoleOpt::superuser(false),
            RoleOpt::access_to_datacenters(maplit::btreeset!{
                Constant::string("dc1"),
                Constant::string("dc2"),
            }),
            RoleOpt::access_to_all_datacenters(),
            RoleOpt::options(maplit::btreemap! {
                Constant::string("custom_option1") => Constant::string("custom value1"),
                Constant::string("custom_option2") => 99_i32.into(),
            })
        });
    }

    #[test]
    fn test_parse_drop_role() {
        let mut builder = DropRoleStatementBuilder::default();
        builder.name("test_role");
        let statement = builder.build().unwrap().to_string();
        assert_eq!(builder.build().unwrap(), statement.parse().unwrap());
        builder.if_exists();
        let statement = builder.build().unwrap().to_string();
        assert_eq!(builder.build().unwrap(), statement.parse().unwrap());
    }

    #[test]
    fn test_parse_grant_role() {
        let mut builder = GrantRoleStatementBuilder::default();
        builder.name("test_role");
        assert!(builder.build().is_err());
        builder.to("test_person");
        let statement = builder.build().unwrap().to_string();
        assert_eq!(builder.build().unwrap(), statement.parse().unwrap());
    }

    #[test]
    fn test_parse_revoke_role() {
        let mut builder = RevokeRoleStatementBuilder::default();
        builder.name("test_role");
        assert!(builder.build().is_err());
        builder.from("test_person");
        let statement = builder.build().unwrap().to_string();
        assert_eq!(builder.build().unwrap(), statement.parse().unwrap());
    }

    #[test]
    fn test_parse_list_roles() {
        let mut builder = ListRolesStatementBuilder::default();
        let statement = builder.build().unwrap().to_string();
        assert_eq!(builder.build().unwrap(), statement.parse().unwrap());
        builder.of("test_person");
        let statement = builder.build().unwrap().to_string();
        assert_eq!(builder.build().unwrap(), statement.parse().unwrap());
        builder.no_recursive();
        let statement = builder.build().unwrap().to_string();
        assert_eq!(builder.build().unwrap(), statement.parse().unwrap());
    }

    #[test]
    fn test_parse_grant_permission() {
        let mut builder = GrantPermissionStatementBuilder::default();
        builder.permission(Permission::Create);
        assert!(builder.build().is_err());
        builder.resource(Resource::keyspace("test_keyspace"));
        assert!(builder.build().is_err());
        builder.to("test_person");
        let statement = builder.build().unwrap().to_string();
        assert_eq!(builder.build().unwrap(), statement.parse().unwrap());
        builder.permission(Permission::Alter);
        let statement = builder.build().unwrap().to_string();
        assert_eq!(builder.build().unwrap(), statement.parse().unwrap());
        builder.permission(Permission::Drop);
        let statement = builder.build().unwrap().to_string();
        assert_eq!(builder.build().unwrap(), statement.parse().unwrap());
        builder.permission(Permission::Select);
        let statement = builder.build().unwrap().to_string();
        assert_eq!(builder.build().unwrap(), statement.parse().unwrap());
        builder.permission(Permission::Modify);
        let statement = builder.build().unwrap().to_string();
        assert_eq!(builder.build().unwrap(), statement.parse().unwrap());
        builder.permission(Permission::Authorize);
        let statement = builder.build().unwrap().to_string();
        assert_eq!(builder.build().unwrap(), statement.parse().unwrap());
        builder.permission(Permission::Describe);
        let statement = builder.build().unwrap().to_string();
        assert_eq!(builder.build().unwrap(), statement.parse().unwrap());
        builder.permission(Permission::Execute);
        let statement = builder.build().unwrap().to_string();
        assert_eq!(builder.build().unwrap(), statement.parse().unwrap());
        builder.permission(PermissionKind::All);
        let statement = builder.build().unwrap().to_string();
        assert_eq!(builder.build().unwrap(), statement.parse().unwrap());
        builder.resource(Resource::all_keyspaces());
        let statement = builder.build().unwrap().to_string();
        assert_eq!(builder.build().unwrap(), statement.parse().unwrap());
        builder.resource(Resource::table("test_table"));
        let statement = builder.build().unwrap().to_string();
        assert_eq!(builder.build().unwrap(), statement.parse().unwrap());
        builder.resource(Resource::all_roles());
        let statement = builder.build().unwrap().to_string();
        assert_eq!(builder.build().unwrap(), statement.parse().unwrap());
        builder.resource(Resource::role("test_role"));
        let statement = builder.build().unwrap().to_string();
        assert_eq!(builder.build().unwrap(), statement.parse().unwrap());
        builder.resource(Resource::all_functions_in_keyspace("test_keyspace"));
        let statement = builder.build().unwrap().to_string();
        assert_eq!(builder.build().unwrap(), statement.parse().unwrap());
        builder.resource(Resource::all_functions());
        let statement = builder.build().unwrap().to_string();
        assert_eq!(builder.build().unwrap(), statement.parse().unwrap());
        builder.resource(Resource::function("test_keyspace".dot("func")));
        let statement = builder.build().unwrap().to_string();
        assert_eq!(builder.build().unwrap(), statement.parse().unwrap());
        builder.resource(Resource::all_mbeans());
        let statement = builder.build().unwrap().to_string();
        assert_eq!(builder.build().unwrap(), statement.parse().unwrap());
        builder.resource(Resource::mbean("test_mbean"));
        let statement = builder.build().unwrap().to_string();
        assert_eq!(builder.build().unwrap(), statement.parse().unwrap());
    }

    #[test]
    fn test_parse_revoke_permission() {
        let mut builder = RevokePermissionStatementBuilder::default();
        builder.permission(PermissionKind::All);
        assert!(builder.build().is_err());
        builder.resource(Resource::keyspace("test_keyspace"));
        assert!(builder.build().is_err());
        builder.from("test_person");
        let statement = builder.build().unwrap().to_string();
        assert_eq!(builder.build().unwrap(), statement.parse().unwrap());
        builder.permission(Permission::Alter);
        let statement = builder.build().unwrap().to_string();
        assert_eq!(builder.build().unwrap(), statement.parse().unwrap());
        builder.permission(Permission::Drop);
        let statement = builder.build().unwrap().to_string();
        assert_eq!(builder.build().unwrap(), statement.parse().unwrap());
        builder.permission(Permission::Select);
        let statement = builder.build().unwrap().to_string();
        assert_eq!(builder.build().unwrap(), statement.parse().unwrap());
        builder.permission(Permission::Modify);
        let statement = builder.build().unwrap().to_string();
        assert_eq!(builder.build().unwrap(), statement.parse().unwrap());
        builder.permission(Permission::Authorize);
        let statement = builder.build().unwrap().to_string();
        assert_eq!(builder.build().unwrap(), statement.parse().unwrap());
        builder.permission(Permission::Describe);
        let statement = builder.build().unwrap().to_string();
        assert_eq!(builder.build().unwrap(), statement.parse().unwrap());
        builder.permission(Permission::Execute);
        let statement = builder.build().unwrap().to_string();
        assert_eq!(builder.build().unwrap(), statement.parse().unwrap());
        builder.permission(Permission::Create);
        let statement = builder.build().unwrap().to_string();
        assert_eq!(builder.build().unwrap(), statement.parse().unwrap());
        builder.resource(Resource::all_keyspaces());
        let statement = builder.build().unwrap().to_string();
        assert_eq!(builder.build().unwrap(), statement.parse().unwrap());
        builder.resource(Resource::table("test_table"));
        let statement = builder.build().unwrap().to_string();
        assert_eq!(builder.build().unwrap(), statement.parse().unwrap());
        builder.resource(Resource::all_roles());
        let statement = builder.build().unwrap().to_string();
        assert_eq!(builder.build().unwrap(), statement.parse().unwrap());
        builder.resource(Resource::role("test_role"));
        let statement = builder.build().unwrap().to_string();
        assert_eq!(builder.build().unwrap(), statement.parse().unwrap());
        builder.resource(Resource::all_functions_in_keyspace("test_keyspace"));
        let statement = builder.build().unwrap().to_string();
        assert_eq!(builder.build().unwrap(), statement.parse().unwrap());
        builder.resource(Resource::all_functions());
        let statement = builder.build().unwrap().to_string();
        assert_eq!(builder.build().unwrap(), statement.parse().unwrap());
        builder.resource(Resource::function("test_keyspace".dot("func")));
        let statement = builder.build().unwrap().to_string();
        assert_eq!(builder.build().unwrap(), statement.parse().unwrap());
        builder.resource(Resource::all_mbeans());
        let statement = builder.build().unwrap().to_string();
        assert_eq!(builder.build().unwrap(), statement.parse().unwrap());
        builder.resource(Resource::mbean("test_mbean"));
        let statement = builder.build().unwrap().to_string();
        assert_eq!(builder.build().unwrap(), statement.parse().unwrap());
    }

    #[test]
    fn test_parse_list_permissions() {
        let mut builder = ListPermissionsStatementBuilder::default();
        builder.permission(PermissionKind::All);
        let statement = builder.build().unwrap().to_string();
        assert_eq!(builder.build().unwrap(), statement.parse().unwrap());
        builder.resource(Resource::keyspace("test_keyspace"));
        let statement = builder.build().unwrap().to_string();
        assert_eq!(builder.build().unwrap(), statement.parse().unwrap());
        builder.of("test_person");
        let statement = builder.build().unwrap().to_string();
        assert_eq!(builder.build().unwrap(), statement.parse().unwrap());
        builder.no_recursive();
        let statement = builder.build().unwrap().to_string();
        assert_eq!(builder.build().unwrap(), statement.parse().unwrap());
        builder.permission(Permission::Alter);
        let statement = builder.build().unwrap().to_string();
        assert_eq!(builder.build().unwrap(), statement.parse().unwrap());
        builder.permission(Permission::Drop);
        let statement = builder.build().unwrap().to_string();
        assert_eq!(builder.build().unwrap(), statement.parse().unwrap());
        builder.permission(Permission::Select);
        let statement = builder.build().unwrap().to_string();
        assert_eq!(builder.build().unwrap(), statement.parse().unwrap());
        builder.permission(Permission::Modify);
        let statement = builder.build().unwrap().to_string();
        assert_eq!(builder.build().unwrap(), statement.parse().unwrap());
        builder.permission(Permission::Authorize);
        let statement = builder.build().unwrap().to_string();
        assert_eq!(builder.build().unwrap(), statement.parse().unwrap());
        builder.permission(Permission::Describe);
        let statement = builder.build().unwrap().to_string();
        assert_eq!(builder.build().unwrap(), statement.parse().unwrap());
        builder.permission(Permission::Execute);
        let statement = builder.build().unwrap().to_string();
        assert_eq!(builder.build().unwrap(), statement.parse().unwrap());
        builder.permission(Permission::Create);
        let statement = builder.build().unwrap().to_string();
        assert_eq!(builder.build().unwrap(), statement.parse().unwrap());
        builder.resource(Resource::all_keyspaces());
        let statement = builder.build().unwrap().to_string();
        assert_eq!(builder.build().unwrap(), statement.parse().unwrap());
        builder.resource(Resource::table("test_table"));
        let statement = builder.build().unwrap().to_string();
        assert_eq!(builder.build().unwrap(), statement.parse().unwrap());
        builder.resource(Resource::all_roles());
        let statement = builder.build().unwrap().to_string();
        assert_eq!(builder.build().unwrap(), statement.parse().unwrap());
        builder.resource(Resource::role("test_role"));
        let statement = builder.build().unwrap().to_string();
        assert_eq!(builder.build().unwrap(), statement.parse().unwrap());
        builder.resource(Resource::all_functions_in_keyspace("test_keyspace"));
        let statement = builder.build().unwrap().to_string();
        assert_eq!(builder.build().unwrap(), statement.parse().unwrap());
        builder.resource(Resource::all_functions());
        let statement = builder.build().unwrap().to_string();
        assert_eq!(builder.build().unwrap(), statement.parse().unwrap());
        builder.resource(Resource::function("test_keyspace".dot("func")));
        let statement = builder.build().unwrap().to_string();
        assert_eq!(builder.build().unwrap(), statement.parse().unwrap());
        builder.resource(Resource::all_mbeans());
        let statement = builder.build().unwrap().to_string();
        assert_eq!(builder.build().unwrap(), statement.parse().unwrap());
        builder.resource(Resource::mbean("test_mbean"));
        let statement = builder.build().unwrap().to_string();
        assert_eq!(builder.build().unwrap(), statement.parse().unwrap());
    }

    #[test]
    fn test_parse_create_user() {
        let mut builder = CreateUserStatementBuilder::default();
        builder.name("test_person");
        let statement = builder.build().unwrap().to_string();
        assert_eq!(builder.build().unwrap(), statement.parse().unwrap());
        builder.with_password("test_password");
        let statement = builder.build().unwrap().to_string();
        assert_eq!(builder.build().unwrap(), statement.parse().unwrap());
        builder.superuser(true);
        let statement = builder.build().unwrap().to_string();
        assert_eq!(builder.build().unwrap(), statement.parse().unwrap());
        builder.superuser(false);
        let statement = builder.build().unwrap().to_string();
        assert_eq!(builder.build().unwrap(), statement.parse().unwrap());
        builder.if_not_exists();
        let statement = builder.build().unwrap().to_string();
        assert_eq!(builder.build().unwrap(), statement.parse().unwrap());
    }

    #[test]
    fn test_parse_alter_user() {
        let mut builder = AlterUserStatementBuilder::default();
        builder.name("test_person");
        let statement = builder.build().unwrap().to_string();
        assert_eq!(builder.build().unwrap(), statement.parse().unwrap());
        builder.with_password("test_password");
        let statement = builder.build().unwrap().to_string();
        assert_eq!(builder.build().unwrap(), statement.parse().unwrap());
        builder.superuser(true);
        let statement = builder.build().unwrap().to_string();
        assert_eq!(builder.build().unwrap(), statement.parse().unwrap());
        builder.superuser(false);
        let statement = builder.build().unwrap().to_string();
        assert_eq!(builder.build().unwrap(), statement.parse().unwrap());
    }

    #[test]
    fn test_parse_drop_user() {
        let mut builder = DropUserStatementBuilder::default();
        builder.name("test person");
        let statement = builder.build().unwrap().to_string();
        assert_eq!(builder.build().unwrap(), statement.parse().unwrap());
        builder.if_exists();
        let statement = builder.build().unwrap().to_string();
        assert_eq!(builder.build().unwrap(), statement.parse().unwrap());
    }

    #[test]
    fn test_parse_list_users() {
        assert_eq!(ListUsersStatement, ListUsersStatement.to_string().parse().unwrap());
    }

    #[test]
    fn test_parse_create_udt() {
        let mut builder = CreateUserDefinedTypeStatementBuilder::default();
        builder.name("test_udt");
        assert!(builder.build().is_err());
        builder.fields(vec![]);
        assert!(builder.build().is_err());
        builder.fields(vec![
            ("test_field1", NativeType::Int).into(),
            ("test field2", CollectionType::list(NativeType::Text)).into(),
        ]);
        let statement = builder.build().unwrap().to_string();
        assert_eq!(builder.build().unwrap(), statement.parse().unwrap());
        builder.if_not_exists();
        let statement = builder.build().unwrap().to_string();
        assert_eq!(builder.build().unwrap(), statement.parse().unwrap());
    }

    #[test]
    fn test_parse_alter_udt() {
        let mut builder = AlterUserDefinedTypeStatementBuilder::default();
        builder.name("test_udt");
        assert!(builder.build().is_err());
        builder.instruction(AlterTypeInstruction::add((
            "test_field1",
            vec![NativeType::Float.into(), NativeType::Date.into()],
        )));
        let statement = builder.build().unwrap().to_string();
        assert_eq!(builder.build().unwrap(), statement.parse().unwrap());
        builder.instruction(AlterTypeInstruction::rename(vec![
            ("test_field1", "tuple field"),
            ("test_field2", "list_field"),
        ]));
        let statement = builder.build().unwrap().to_string();
        assert_eq!(builder.build().unwrap(), statement.parse().unwrap());
    }

    #[test]
    fn test_parse_drop_udt() {
        let mut builder = DropUserDefinedTypeStatementBuilder::default();
        builder.name("test_udt");
        let statement = builder.build().unwrap().to_string();
        assert_eq!(builder.build().unwrap(), statement.parse().unwrap());
        builder.if_exists();
        let statement = builder.build().unwrap().to_string();
        assert_eq!(builder.build().unwrap(), statement.parse().unwrap());
    }
}

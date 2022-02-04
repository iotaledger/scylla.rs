// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use super::*;
use std::collections::BTreeSet;

#[derive(ParseFromStr, Clone, Debug, TryInto, From, ToTokens, PartialEq, Eq)]
#[parse_via(TaggedRoleStatement)]
pub enum RoleStatement {
    Create(CreateRoleStatement),
    Alter(AlterRoleStatement),
    Drop(DropRoleStatement),
    Grant(GrantRoleStatement),
    Revoke(RevokeRoleStatement),
    List(ListRolesStatement),
}

impl TryFrom<TaggedRoleStatement> for RoleStatement {
    type Error = anyhow::Error;

    fn try_from(value: TaggedRoleStatement) -> Result<Self, Self::Error> {
        Ok(match value {
            TaggedRoleStatement::Create(v) => RoleStatement::Create(v.try_into()?),
            TaggedRoleStatement::Alter(v) => RoleStatement::Alter(v.try_into()?),
            TaggedRoleStatement::Drop(v) => RoleStatement::Drop(v.try_into()?),
            TaggedRoleStatement::Grant(v) => RoleStatement::Grant(v.try_into()?),
            TaggedRoleStatement::Revoke(v) => RoleStatement::Revoke(v.try_into()?),
            TaggedRoleStatement::List(v) => RoleStatement::List(v.try_into()?),
        })
    }
}

#[derive(ParseFromStr, Clone, Debug, TryInto, From, ToTokens, PartialEq, Eq)]
#[tokenize_as(RoleStatement)]
pub enum TaggedRoleStatement {
    Create(TaggedCreateRoleStatement),
    Alter(TaggedAlterRoleStatement),
    Drop(TaggedDropRoleStatement),
    Grant(TaggedGrantRoleStatement),
    Revoke(TaggedRevokeRoleStatement),
    List(TaggedListRolesStatement),
}

impl Parse for TaggedRoleStatement {
    type Output = Self;
    fn parse(s: &mut StatementStream<'_>) -> anyhow::Result<Self::Output> {
        Ok(if s.check::<CREATE>() {
            Self::Create(s.parse()?)
        } else if s.check::<ALTER>() {
            Self::Alter(s.parse()?)
        } else if s.check::<DROP>() {
            Self::Drop(s.parse()?)
        } else if s.check::<GRANT>() {
            Self::Grant(s.parse()?)
        } else if s.check::<REVOKE>() {
            Self::Revoke(s.parse()?)
        } else if s.check::<LIST>() {
            Self::List(s.parse()?)
        } else {
            anyhow::bail!("Expected a role statement, found {}", s.info())
        })
    }
}

impl Display for RoleStatement {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Create(stmt) => stmt.fmt(f),
            Self::Alter(stmt) => stmt.fmt(f),
            Self::Drop(stmt) => stmt.fmt(f),
            Self::Grant(stmt) => stmt.fmt(f),
            Self::Revoke(stmt) => stmt.fmt(f),
            Self::List(stmt) => stmt.fmt(f),
        }
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
#[parse_via(TaggedCreateRoleStatement)]
pub struct CreateRoleStatement {
    #[builder(setter(name = "set_if_not_exists"), default)]
    pub if_not_exists: bool,
    #[builder(setter(into))]
    pub name: Name,
    #[builder(default)]
    pub options: Option<BTreeSet<RoleOpt>>,
}

impl TryFrom<TaggedCreateRoleStatement> for CreateRoleStatement {
    type Error = anyhow::Error;
    fn try_from(value: TaggedCreateRoleStatement) -> Result<Self, Self::Error> {
        Ok(Self {
            if_not_exists: value.if_not_exists,
            name: value.name.into_value()?,
            options: value.options.map(|v| v.into_value()).transpose()?,
        })
    }
}

#[derive(ParseFromStr, Builder, Clone, Debug, ToTokens, PartialEq, Eq)]
#[builder(setter(strip_option))]
#[tokenize_as(CreateRoleStatement)]
pub struct TaggedCreateRoleStatement {
    #[builder(setter(name = "set_if_not_exists"), default)]
    pub if_not_exists: bool,
    pub name: Tag<Name>,
    #[builder(default)]
    pub options: Option<Tag<BTreeSet<RoleOpt>>>,
}

impl CreateRoleStatementBuilder {
    /// Set IF NOT EXISTS on the statement.
    /// To undo this, use `set_if_not_exists(false)`.
    pub fn if_not_exists(&mut self) -> &mut Self {
        self.if_not_exists.replace(true);
        self
    }
}

impl Parse for TaggedCreateRoleStatement {
    type Output = Self;
    fn parse(s: &mut StatementStream<'_>) -> anyhow::Result<Self::Output> {
        s.parse::<(CREATE, ROLE)>()?;
        let mut res = TaggedCreateRoleStatementBuilder::default();
        res.set_if_not_exists(s.parse::<Option<(IF, NOT, EXISTS)>>()?.is_some())
            .name(s.parse()?);
        match s.parse_from::<If<WITH, Tag<List<RoleOpt, AND>>>>()? {
            Some(Tag::Value(o)) => {
                let mut opts = BTreeSet::new();
                for opt in o {
                    if opts.contains(&opt) {
                        anyhow::bail!("Duplicate option: {}", opt);
                    } else {
                        opts.insert(opt);
                    }
                }
                res.options(Tag::Value(opts));
            }
            Some(Tag::Tag(t)) => {
                res.options(Tag::Tag(t));
            }
            _ => (),
        }
        s.parse::<Option<Semicolon>>()?;
        Ok(res
            .build()
            .map_err(|e| anyhow::anyhow!("Invalid CREATE ROLE statement: {}", e))?)
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
#[parse_via(TaggedAlterRoleStatement)]
pub struct AlterRoleStatement {
    #[builder(setter(into))]
    pub name: Name,
    pub options: BTreeSet<RoleOpt>,
}

impl TryFrom<TaggedAlterRoleStatement> for AlterRoleStatement {
    type Error = anyhow::Error;
    fn try_from(value: TaggedAlterRoleStatement) -> Result<Self, Self::Error> {
        Ok(Self {
            name: value.name.into_value()?,
            options: value.options.into_value()?,
        })
    }
}

#[derive(ParseFromStr, Builder, Clone, Debug, ToTokens, PartialEq, Eq)]
#[builder(build_fn(validate = "Self::validate"))]
#[tokenize_as(AlterRoleStatement)]
pub struct TaggedAlterRoleStatement {
    pub name: Tag<Name>,
    pub options: Tag<BTreeSet<RoleOpt>>,
}

impl AlterRoleStatementBuilder {
    fn validate(&self) -> Result<(), String> {
        if self.options.as_ref().map(|s| s.is_empty()).unwrap_or(false) {
            return Err("Role options cannot be empty".to_string());
        }
        Ok(())
    }
}

impl TaggedAlterRoleStatementBuilder {
    fn validate(&self) -> Result<(), String> {
        if self
            .options
            .as_ref()
            .map(|s| match s {
                Tag::Value(v) => v.is_empty(),
                _ => false,
            })
            .unwrap_or(false)
        {
            return Err("Role options cannot be empty".to_string());
        }
        Ok(())
    }
}

impl Parse for TaggedAlterRoleStatement {
    type Output = Self;
    fn parse(s: &mut StatementStream<'_>) -> anyhow::Result<Self::Output> {
        s.parse::<(ALTER, ROLE)>()?;
        let mut res = TaggedAlterRoleStatementBuilder::default();
        res.name(s.parse()?);
        let o = s.parse_from::<(WITH, Tag<List<RoleOpt, AND>>)>()?.1;
        match o {
            Tag::Value(o) => {
                let mut opts = BTreeSet::new();
                for opt in o {
                    if opts.contains(&opt) {
                        anyhow::bail!("Duplicate option: {}", opt);
                    } else {
                        opts.insert(opt);
                    }
                }
                res.options(Tag::Value(opts));
            }
            Tag::Tag(t) => {
                res.options(Tag::Tag(t));
            }
        }

        s.parse::<Option<Semicolon>>()?;
        Ok(res
            .build()
            .map_err(|e| anyhow::anyhow!("Invalid ALTER ROLE statement: {}", e))?)
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
#[parse_via(TaggedDropRoleStatement)]
pub struct DropRoleStatement {
    #[builder(setter(name = "set_if_exists"), default)]
    pub if_exists: bool,
    #[builder(setter(into))]
    pub name: Name,
}

impl TryFrom<TaggedDropRoleStatement> for DropRoleStatement {
    type Error = anyhow::Error;
    fn try_from(value: TaggedDropRoleStatement) -> Result<Self, Self::Error> {
        Ok(Self {
            if_exists: value.if_exists,
            name: value.name.into_value()?,
        })
    }
}

#[derive(ParseFromStr, Builder, Clone, Debug, ToTokens, PartialEq, Eq)]
#[tokenize_as(DropRoleStatement)]
pub struct TaggedDropRoleStatement {
    #[builder(setter(name = "set_if_exists"), default)]
    pub if_exists: bool,
    pub name: Tag<Name>,
}

impl DropRoleStatementBuilder {
    /// Set IF EXISTS on the statement.
    /// To undo this, use `set_if_exists(false)`.
    pub fn if_exists(&mut self) -> &mut Self {
        self.if_exists.replace(true);
        self
    }
}

impl Parse for TaggedDropRoleStatement {
    type Output = Self;
    fn parse(s: &mut StatementStream<'_>) -> anyhow::Result<Self::Output> {
        s.parse::<(DROP, ROLE)>()?;
        let mut res = TaggedDropRoleStatementBuilder::default();
        loop {
            if s.remaining() == 0 || s.parse::<Option<Semicolon>>()?.is_some() {
                break;
            }
            if s.parse::<Option<(IF, EXISTS)>>()?.is_some() {
                res.set_if_exists(true);
            } else if let Some(n) = s.parse()? {
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
#[parse_via(TaggedGrantRoleStatement)]
pub struct GrantRoleStatement {
    pub name: Name,
    pub to: Name,
}

impl TryFrom<TaggedGrantRoleStatement> for GrantRoleStatement {
    type Error = anyhow::Error;
    fn try_from(value: TaggedGrantRoleStatement) -> Result<Self, Self::Error> {
        Ok(Self {
            name: value.name.into_value()?,
            to: value.to.into_value()?,
        })
    }
}

#[derive(ParseFromStr, Builder, Clone, Debug, ToTokens, PartialEq, Eq)]
#[tokenize_as(GrantRoleStatement)]
pub struct TaggedGrantRoleStatement {
    pub name: Tag<Name>,
    pub to: Tag<Name>,
}

impl Parse for TaggedGrantRoleStatement {
    type Output = Self;
    fn parse(s: &mut StatementStream<'_>) -> anyhow::Result<Self::Output> {
        s.parse::<GRANT>()?;
        let mut res = TaggedGrantRoleStatementBuilder::default();
        res.name(s.parse()?);
        s.parse::<TO>()?;
        res.to(s.parse()?);
        s.parse::<Option<Semicolon>>()?;
        Ok(res
            .build()
            .map_err(|e| anyhow::anyhow!("Invalid GRANT ROLE statement: {}", e))?)
    }
}

impl Display for GrantRoleStatement {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "GRANT {} TO {}", self.name, self.to)
    }
}

#[derive(ParseFromStr, Builder, Clone, Debug, ToTokens, PartialEq, Eq)]
#[builder(setter(into))]
#[parse_via(TaggedRevokeRoleStatement)]
pub struct RevokeRoleStatement {
    pub name: Name,
    pub from: Name,
}

impl TryFrom<TaggedRevokeRoleStatement> for RevokeRoleStatement {
    type Error = anyhow::Error;
    fn try_from(value: TaggedRevokeRoleStatement) -> Result<Self, Self::Error> {
        Ok(Self {
            name: value.name.into_value()?,
            from: value.from.into_value()?,
        })
    }
}

#[derive(ParseFromStr, Builder, Clone, Debug, ToTokens, PartialEq, Eq)]
#[tokenize_as(RevokeRoleStatement)]
pub struct TaggedRevokeRoleStatement {
    pub name: Tag<Name>,
    pub from: Tag<Name>,
}

impl Parse for TaggedRevokeRoleStatement {
    type Output = Self;
    fn parse(s: &mut StatementStream<'_>) -> anyhow::Result<Self::Output> {
        s.parse::<REVOKE>()?;
        let mut res = TaggedRevokeRoleStatementBuilder::default();
        res.name(s.parse()?);
        s.parse::<FROM>()?;
        res.from(s.parse()?);
        s.parse::<Option<Semicolon>>()?;
        Ok(res
            .build()
            .map_err(|e| anyhow::anyhow!("Invalid REVOKE ROLE statement: {}", e))?)
    }
}

impl Display for RevokeRoleStatement {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "REVOKE {} FROM {}", self.name, self.from)
    }
}

#[derive(ParseFromStr, Builder, Clone, Debug, ToTokens, PartialEq, Eq)]
#[builder(setter(strip_option))]
#[parse_via(TaggedListRolesStatement)]
pub struct ListRolesStatement {
    #[builder(setter(into), default)]
    pub of: Option<Name>,
    #[builder(setter(name = "set_no_recursive"), default)]
    pub no_recursive: bool,
}

impl TryFrom<TaggedListRolesStatement> for ListRolesStatement {
    type Error = anyhow::Error;
    fn try_from(value: TaggedListRolesStatement) -> Result<Self, Self::Error> {
        Ok(Self {
            of: value.of.map(|v| v.into_value()).transpose()?,
            no_recursive: value.no_recursive,
        })
    }
}

#[derive(ParseFromStr, Builder, Clone, Debug, ToTokens, PartialEq, Eq)]
#[builder(setter(strip_option))]
#[tokenize_as(ListRolesStatement)]
pub struct TaggedListRolesStatement {
    #[builder(default)]
    pub of: Option<Tag<Name>>,
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

impl Parse for TaggedListRolesStatement {
    type Output = Self;
    fn parse(s: &mut StatementStream<'_>) -> anyhow::Result<Self::Output> {
        s.parse::<(LIST, ROLES)>()?;
        let mut res = TaggedListRolesStatementBuilder::default();
        if let Some(n) = s.parse_from::<If<OF, Tag<Name>>>()? {
            res.of(n);
        }
        res.set_no_recursive(s.parse::<Option<NORECURSIVE>>()?.is_some());
        s.parse::<Option<Semicolon>>()?;
        Ok(res
            .build()
            .map_err(|e| anyhow::anyhow!("Invalid LIST ROLES statement: {}", e))?)
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
            p @ _ => anyhow::bail!("Expected permission, found {}", p),
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
            anyhow::bail!("Expected resource, found {}", s.info())
        })
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
#[parse_via(TaggedPermissionStatement)]
pub enum PermissionStatement {
    Grant(GrantPermissionStatement),
    Revoke(RevokePermissionStatement),
    List(ListPermissionsStatement),
}

impl TryFrom<TaggedPermissionStatement> for PermissionStatement {
    type Error = anyhow::Error;
    fn try_from(value: TaggedPermissionStatement) -> Result<Self, Self::Error> {
        Ok(match value {
            TaggedPermissionStatement::Grant(s) => PermissionStatement::Grant(s.try_into()?),
            TaggedPermissionStatement::Revoke(s) => PermissionStatement::Revoke(s.try_into()?),
            TaggedPermissionStatement::List(s) => PermissionStatement::List(s.try_into()?),
        })
    }
}

#[derive(ParseFromStr, Clone, Debug, TryInto, From, ToTokens, PartialEq, Eq)]
#[tokenize_as(PermissionStatement)]
pub enum TaggedPermissionStatement {
    Grant(TaggedGrantPermissionStatement),
    Revoke(TaggedRevokePermissionStatement),
    List(TaggedListPermissionsStatement),
}

impl Parse for TaggedPermissionStatement {
    type Output = Self;
    fn parse(s: &mut StatementStream<'_>) -> anyhow::Result<Self::Output> {
        Ok(if s.check::<GRANT>() {
            Self::Grant(s.parse()?)
        } else if s.check::<REVOKE>() {
            Self::Revoke(s.parse()?)
        } else if s.check::<LIST>() {
            Self::List(s.parse()?)
        } else {
            anyhow::bail!("Expected a permission statement, found {}", s.info())
        })
    }
}

impl Display for PermissionStatement {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Grant(stmt) => stmt.fmt(f),
            Self::Revoke(stmt) => stmt.fmt(f),
            Self::List(stmt) => stmt.fmt(f),
        }
    }
}

#[derive(ParseFromStr, Builder, Clone, Debug, ToTokens, PartialEq, Eq)]
#[builder(setter(into))]
#[parse_via(TaggedGrantPermissionStatement)]
pub struct GrantPermissionStatement {
    pub permission: PermissionKind,
    pub resource: Resource,
    pub to: Name,
}

impl TryFrom<TaggedGrantPermissionStatement> for GrantPermissionStatement {
    type Error = anyhow::Error;
    fn try_from(value: TaggedGrantPermissionStatement) -> Result<Self, Self::Error> {
        Ok(Self {
            permission: value.permission.into_value()?,
            resource: value.resource.into_value()?,
            to: value.to.into_value()?,
        })
    }
}

#[derive(ParseFromStr, Builder, Clone, Debug, ToTokens, PartialEq, Eq)]
#[tokenize_as(GrantPermissionStatement)]
pub struct TaggedGrantPermissionStatement {
    pub permission: Tag<PermissionKind>,
    pub resource: Tag<Resource>,
    pub to: Tag<Name>,
}

impl Parse for TaggedGrantPermissionStatement {
    type Output = Self;
    fn parse(s: &mut StatementStream<'_>) -> anyhow::Result<Self::Output> {
        s.parse::<GRANT>()?;
        let mut res = TaggedGrantPermissionStatementBuilder::default();
        res.permission(s.parse()?);
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

impl Display for GrantPermissionStatement {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "GRANT {} ON {} TO {}", self.permission, self.resource, self.to)
    }
}

#[derive(ParseFromStr, Builder, Clone, Debug, ToTokens, PartialEq, Eq)]
#[builder(setter(into))]
#[parse_via(TaggedRevokePermissionStatement)]
pub struct RevokePermissionStatement {
    pub permission: PermissionKind,
    pub resource: Resource,
    pub from: Name,
}

impl TryFrom<TaggedRevokePermissionStatement> for RevokePermissionStatement {
    type Error = anyhow::Error;
    fn try_from(value: TaggedRevokePermissionStatement) -> Result<Self, Self::Error> {
        Ok(Self {
            permission: value.permission.into_value()?,
            resource: value.resource.into_value()?,
            from: value.from.into_value()?,
        })
    }
}

#[derive(ParseFromStr, Builder, Clone, Debug, ToTokens, PartialEq, Eq)]
#[tokenize_as(RevokePermissionStatement)]
pub struct TaggedRevokePermissionStatement {
    pub permission: Tag<PermissionKind>,
    pub resource: Tag<Resource>,
    pub from: Tag<Name>,
}

impl Parse for TaggedRevokePermissionStatement {
    type Output = Self;
    fn parse(s: &mut StatementStream<'_>) -> anyhow::Result<Self::Output> {
        s.parse::<REVOKE>()?;
        let mut res = TaggedRevokePermissionStatementBuilder::default();
        res.permission(s.parse()?);
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

impl Display for RevokePermissionStatement {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "REVOKE {} ON {} FROM {}", self.permission, self.resource, self.from)
    }
}

#[derive(ParseFromStr, Builder, Clone, Debug, ToTokens, PartialEq, Eq)]
#[builder(setter(strip_option))]
#[parse_via(TaggedListPermissionsStatement)]
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

impl TryFrom<TaggedListPermissionsStatement> for ListPermissionsStatement {
    type Error = anyhow::Error;
    fn try_from(value: TaggedListPermissionsStatement) -> Result<Self, Self::Error> {
        Ok(Self {
            permission: value.permission.into_value()?,
            resource: value.resource.map(|v| v.into_value()).transpose()?,
            of: value.of.map(|v| v.into_value()).transpose()?,
            no_recursive: value.no_recursive,
        })
    }
}

#[derive(ParseFromStr, Builder, Clone, Debug, ToTokens, PartialEq, Eq)]
#[builder(setter(strip_option))]
#[tokenize_as(ListPermissionsStatement)]
pub struct TaggedListPermissionsStatement {
    pub permission: Tag<PermissionKind>,
    #[builder(default)]
    pub resource: Option<Tag<Resource>>,
    #[builder(default)]
    pub of: Option<Tag<Name>>,
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

impl Parse for TaggedListPermissionsStatement {
    type Output = Self;
    fn parse(s: &mut StatementStream<'_>) -> anyhow::Result<Self::Output> {
        s.parse::<LIST>()?;
        let mut res = TaggedListPermissionsStatementBuilder::default();
        res.permission(s.parse()?);
        loop {
            if s.remaining() == 0 || s.parse::<Option<Semicolon>>()?.is_some() {
                break;
            }
            if let Some(resource) = s.parse_from::<If<ON, Tag<Resource>>>()? {
                if res.resource.is_some() {
                    anyhow::bail!("Duplicate ON RESOURCE clause!");
                }
                res.resource(resource);
            } else if let Some(role) = s.parse_from::<If<OF, Tag<Name>>>()? {
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
#[parse_via(TaggedUserStatement)]
pub enum UserStatement {
    Create(CreateUserStatement),
    Alter(AlterUserStatement),
    Drop(DropUserStatement),
    List(ListUsersStatement),
}

impl TryFrom<TaggedUserStatement> for UserStatement {
    type Error = anyhow::Error;
    fn try_from(value: TaggedUserStatement) -> Result<Self, Self::Error> {
        Ok(match value {
            TaggedUserStatement::Create(v) => Self::Create(v.try_into()?),
            TaggedUserStatement::Alter(v) => Self::Alter(v.try_into()?),
            TaggedUserStatement::Drop(v) => Self::Drop(v.try_into()?),
            TaggedUserStatement::List(v) => Self::List(v.try_into()?),
        })
    }
}

#[derive(ParseFromStr, Clone, Debug, TryInto, From, ToTokens, PartialEq, Eq)]
#[tokenize_as(UserStatement)]
pub enum TaggedUserStatement {
    Create(TaggedCreateUserStatement),
    Alter(TaggedAlterUserStatement),
    Drop(TaggedDropUserStatement),
    List(ListUsersStatement),
}

impl Parse for TaggedUserStatement {
    type Output = Self;
    fn parse(s: &mut StatementStream<'_>) -> anyhow::Result<Self::Output> {
        Ok(if s.check::<CREATE>() {
            Self::Create(s.parse()?)
        } else if s.check::<ALTER>() {
            Self::Alter(s.parse()?)
        } else if s.check::<DROP>() {
            Self::Drop(s.parse()?)
        } else if s.check::<LIST>() {
            Self::List(s.parse()?)
        } else {
            anyhow::bail!("Expected a user statement, found {}", s.info())
        })
    }
}

impl Display for UserStatement {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Create(stmt) => stmt.fmt(f),
            Self::Alter(stmt) => stmt.fmt(f),
            Self::Drop(stmt) => stmt.fmt(f),
            Self::List(stmt) => stmt.fmt(f),
        }
    }
}

#[derive(ParseFromStr, Builder, Clone, Debug, ToTokens, PartialEq, Eq)]
#[builder(setter(strip_option))]
#[parse_via(TaggedCreateUserStatement)]
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

impl TryFrom<TaggedCreateUserStatement> for CreateUserStatement {
    type Error = anyhow::Error;
    fn try_from(value: TaggedCreateUserStatement) -> Result<Self, Self::Error> {
        Ok(Self {
            if_not_exists: value.if_not_exists,
            name: value.name.into_value()?,
            with_password: value.with_password.map(|v| v.into_value()).transpose()?,
            superuser: value.superuser,
        })
    }
}

#[derive(ParseFromStr, Builder, Clone, Debug, ToTokens, PartialEq, Eq)]
#[builder(setter(strip_option))]
#[tokenize_as(CreateUserStatement)]
pub struct TaggedCreateUserStatement {
    #[builder(setter(name = "set_if_not_exists"), default)]
    pub if_not_exists: bool,
    pub name: Tag<Name>,
    #[builder(default)]
    pub with_password: Option<Tag<LitStr>>,
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

impl Parse for TaggedCreateUserStatement {
    type Output = Self;
    fn parse(s: &mut StatementStream<'_>) -> anyhow::Result<Self::Output> {
        s.parse::<(CREATE, USER)>()?;
        let mut res = TaggedCreateUserStatementBuilder::default();
        res.set_if_not_exists(s.parse::<Option<(IF, NOT, EXISTS)>>()?.is_some())
            .name(s.parse()?);
        loop {
            if s.remaining() == 0 || s.parse::<Option<Semicolon>>()?.is_some() {
                break;
            }
            if let Some(password) = s.parse_from::<If<(WITH, PASSWORD), Tag<LitStr>>>()? {
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
#[parse_via(TaggedAlterUserStatement)]
pub struct AlterUserStatement {
    #[builder(setter(into))]
    pub name: Name,
    #[builder(setter(into), default)]
    pub with_password: Option<LitStr>,
    #[builder(default)]
    pub superuser: Option<bool>,
}

impl TryFrom<TaggedAlterUserStatement> for AlterUserStatement {
    type Error = anyhow::Error;
    fn try_from(value: TaggedAlterUserStatement) -> Result<Self, Self::Error> {
        Ok(Self {
            name: value.name.into_value()?,
            with_password: value.with_password.map(|v| v.into_value()).transpose()?,
            superuser: value.superuser,
        })
    }
}

#[derive(ParseFromStr, Builder, Clone, Debug, ToTokens, PartialEq, Eq)]
#[builder(setter(strip_option))]
#[tokenize_as(AlterUserStatement)]
pub struct TaggedAlterUserStatement {
    pub name: Tag<Name>,
    #[builder(default)]
    pub with_password: Option<Tag<LitStr>>,
    #[builder(default)]
    pub superuser: Option<bool>,
}

impl Parse for TaggedAlterUserStatement {
    type Output = Self;
    fn parse(s: &mut StatementStream<'_>) -> anyhow::Result<Self::Output> {
        s.parse::<(ALTER, USER)>()?;
        let mut res = TaggedAlterUserStatementBuilder::default();
        res.name(s.parse()?);
        loop {
            if s.remaining() == 0 || s.parse::<Option<Semicolon>>()?.is_some() {
                break;
            }
            if let Some(password) = s.parse_from::<If<(WITH, PASSWORD), Tag<LitStr>>>()? {
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
#[parse_via(TaggedDropUserStatement)]
pub struct DropUserStatement {
    #[builder(setter(name = "set_if_exists"), default)]
    pub if_exists: bool,
    #[builder(setter(into))]
    pub name: Name,
}

impl TryFrom<TaggedDropUserStatement> for DropUserStatement {
    type Error = anyhow::Error;
    fn try_from(value: TaggedDropUserStatement) -> Result<Self, Self::Error> {
        Ok(Self {
            if_exists: value.if_exists,
            name: value.name.into_value()?,
        })
    }
}

#[derive(ParseFromStr, Builder, Clone, Debug, ToTokens, PartialEq, Eq)]
#[tokenize_as(DropUserStatement)]
pub struct TaggedDropUserStatement {
    #[builder(setter(name = "set_if_exists"), default)]
    pub if_exists: bool,
    pub name: Tag<Name>,
}

impl DropUserStatementBuilder {
    /// Set IF EXISTS on the statement.
    /// To undo this, use `set_if_exists(false)`.
    pub fn if_exists(&mut self) -> &mut Self {
        self.if_exists.replace(true);
        self
    }
}

impl Parse for TaggedDropUserStatement {
    type Output = Self;
    fn parse(s: &mut StatementStream<'_>) -> anyhow::Result<Self::Output> {
        s.parse::<(DROP, USER)>()?;
        let mut res = TaggedDropUserStatementBuilder::default();
        res.set_if_exists(s.parse::<Option<(IF, EXISTS)>>()?.is_some())
            .name(s.parse()?);
        s.parse::<Option<Semicolon>>()?;
        Ok(res
            .build()
            .map_err(|e| anyhow::anyhow!("Invalid DROP USER statement: {}", e))?)
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

impl Display for ListUsersStatement {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "LIST USERS")
    }
}

#[derive(ParseFromStr, Clone, Debug, TryInto, From, ToTokens, PartialEq, Eq)]
#[parse_via(TaggedUserDefinedTypeStatement)]
pub enum UserDefinedTypeStatement {
    Create(CreateUserDefinedTypeStatement),
    Alter(AlterUserDefinedTypeStatement),
    Drop(DropUserDefinedTypeStatement),
}

impl TryFrom<TaggedUserDefinedTypeStatement> for UserDefinedTypeStatement {
    type Error = anyhow::Error;
    fn try_from(value: TaggedUserDefinedTypeStatement) -> Result<Self, Self::Error> {
        Ok(match value {
            TaggedUserDefinedTypeStatement::Create(s) => UserDefinedTypeStatement::Create(s.try_into()?),
            TaggedUserDefinedTypeStatement::Alter(s) => UserDefinedTypeStatement::Alter(s.try_into()?),
            TaggedUserDefinedTypeStatement::Drop(s) => UserDefinedTypeStatement::Drop(s.try_into()?),
        })
    }
}

#[derive(ParseFromStr, Clone, Debug, TryInto, From, ToTokens, PartialEq, Eq)]
#[tokenize_as(UserDefinedTypeStatement)]
pub enum TaggedUserDefinedTypeStatement {
    Create(TaggedCreateUserDefinedTypeStatement),
    Alter(TaggedAlterUserDefinedTypeStatement),
    Drop(TaggedDropUserDefinedTypeStatement),
}

impl Parse for TaggedUserDefinedTypeStatement {
    type Output = Self;
    fn parse(s: &mut StatementStream<'_>) -> anyhow::Result<Self::Output> {
        Ok(
            if let Some(stmt) = s.parse::<Option<TaggedCreateUserDefinedTypeStatement>>()? {
                Self::Create(stmt)
            } else if let Some(stmt) = s.parse::<Option<TaggedAlterUserDefinedTypeStatement>>()? {
                Self::Alter(stmt)
            } else if let Some(stmt) = s.parse::<Option<TaggedDropUserDefinedTypeStatement>>()? {
                Self::Drop(stmt)
            } else {
                anyhow::bail!("Expected user defined type statement, found {}", s.info())
            },
        )
    }
}

impl Display for UserDefinedTypeStatement {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Create(stmt) => stmt.fmt(f),
            Self::Alter(stmt) => stmt.fmt(f),
            Self::Drop(stmt) => stmt.fmt(f),
        }
    }
}

#[derive(ParseFromStr, Builder, Clone, Debug, ToTokens, PartialEq, Eq)]
#[builder(build_fn(validate = "Self::validate"))]
#[parse_via(TaggedCreateUserDefinedTypeStatement)]
pub struct CreateUserDefinedTypeStatement {
    #[builder(setter(name = "set_if_not_exists"), default)]
    pub if_not_exists: bool,
    #[builder(setter(into))]
    pub name: KeyspaceQualifiedName,
    pub fields: Vec<FieldDefinition>,
}

impl TryFrom<TaggedCreateUserDefinedTypeStatement> for CreateUserDefinedTypeStatement {
    type Error = anyhow::Error;
    fn try_from(value: TaggedCreateUserDefinedTypeStatement) -> Result<Self, Self::Error> {
        Ok(Self {
            if_not_exists: value.if_not_exists,
            name: value.name.try_into()?,
            fields: value.fields,
        })
    }
}

#[derive(ParseFromStr, Builder, Clone, Debug, ToTokens, PartialEq, Eq)]
#[builder(build_fn(validate = "Self::validate"))]
#[tokenize_as(CreateUserDefinedTypeStatement)]
pub struct TaggedCreateUserDefinedTypeStatement {
    #[builder(setter(name = "set_if_not_exists"), default)]
    pub if_not_exists: bool,
    pub name: TaggedKeyspaceQualifiedName,
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

impl TaggedCreateUserDefinedTypeStatementBuilder {
    fn validate(&self) -> Result<(), String> {
        if self.fields.as_ref().map(|s| s.is_empty()).unwrap_or(false) {
            return Err("Field definitions cannot be empty".to_string());
        }
        Ok(())
    }
}

impl Parse for TaggedCreateUserDefinedTypeStatement {
    type Output = Self;
    fn parse(s: &mut StatementStream<'_>) -> anyhow::Result<Self::Output> {
        s.parse::<(CREATE, TYPE)>()?;
        let mut res = TaggedCreateUserDefinedTypeStatementBuilder::default();
        res.set_if_not_exists(s.parse::<Option<(IF, NOT, EXISTS)>>()?.is_some())
            .name(s.parse()?)
            .fields(s.parse_from::<Parens<List<FieldDefinition, Comma>>>()?);
        s.parse::<Option<Semicolon>>()?;
        Ok(res
            .build()
            .map_err(|e| anyhow::anyhow!("Invalid CREATE TYPE statement: {}", e))?)
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
#[parse_via(TaggedAlterUserDefinedTypeStatement)]
pub struct AlterUserDefinedTypeStatement {
    #[builder(setter(into))]
    pub name: KeyspaceQualifiedName,
    pub instruction: AlterTypeInstruction,
}

impl TryFrom<TaggedAlterUserDefinedTypeStatement> for AlterUserDefinedTypeStatement {
    type Error = anyhow::Error;
    fn try_from(value: TaggedAlterUserDefinedTypeStatement) -> Result<Self, Self::Error> {
        Ok(Self {
            name: value.name.try_into()?,
            instruction: value.instruction,
        })
    }
}

#[derive(ParseFromStr, Builder, Clone, Debug, ToTokens, PartialEq, Eq)]
#[builder(build_fn(validate = "Self::validate"))]
#[tokenize_as(AlterUserDefinedTypeStatement)]
pub struct TaggedAlterUserDefinedTypeStatement {
    pub name: TaggedKeyspaceQualifiedName,
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

impl TaggedAlterUserDefinedTypeStatementBuilder {
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

impl Parse for TaggedAlterUserDefinedTypeStatement {
    type Output = Self;
    fn parse(s: &mut StatementStream<'_>) -> anyhow::Result<Self::Output> {
        s.parse::<(ALTER, TYPE)>()?;
        let mut res = TaggedAlterUserDefinedTypeStatementBuilder::default();
        res.name(s.parse()?).instruction(s.parse()?);
        s.parse::<Option<Semicolon>>()?;
        Ok(res
            .build()
            .map_err(|e| anyhow::anyhow!("Invalid ALTER TYPE statement: {}", e))?)
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
#[parse_via(TaggedDropUserDefinedTypeStatement)]
pub struct DropUserDefinedTypeStatement {
    #[builder(setter(name = "set_if_exists"), default)]
    pub if_exists: bool,
    #[builder(setter(into))]
    pub name: KeyspaceQualifiedName,
}

impl TryFrom<TaggedDropUserDefinedTypeStatement> for DropUserDefinedTypeStatement {
    type Error = anyhow::Error;
    fn try_from(value: TaggedDropUserDefinedTypeStatement) -> Result<Self, Self::Error> {
        Ok(Self {
            if_exists: value.if_exists,
            name: value.name.try_into()?,
        })
    }
}

#[derive(ParseFromStr, Builder, Clone, Debug, ToTokens, PartialEq, Eq)]
#[tokenize_as(DropUserDefinedTypeStatement)]
pub struct TaggedDropUserDefinedTypeStatement {
    #[builder(setter(name = "set_if_exists"), default)]
    pub if_exists: bool,
    pub name: TaggedKeyspaceQualifiedName,
}

impl DropUserDefinedTypeStatementBuilder {
    /// Set IF EXISTS on the statement.
    /// To undo this, use `set_if_exists(false)`.
    pub fn if_exists(&mut self) -> &mut Self {
        self.if_exists.replace(true);
        self
    }
}

impl Parse for TaggedDropUserDefinedTypeStatement {
    type Output = Self;
    fn parse(s: &mut StatementStream<'_>) -> anyhow::Result<Self::Output> {
        s.parse::<(DROP, TYPE)>()?;
        let mut res = TaggedDropUserDefinedTypeStatementBuilder::default();
        res.set_if_exists(s.parse::<Option<(IF, EXISTS)>>()?.is_some())
            .name(s.parse()?);
        s.parse::<Option<Semicolon>>()?;
        Ok(res
            .build()
            .map_err(|e| anyhow::anyhow!("Invalid DROP TYPE statement: {}", e))?)
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

// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use super::*;

pub type FunctionName = KeyspaceQualifiedName;

#[derive(ParseFromStr, Builder, Clone, Debug, ToTokens, PartialEq, Eq)]
pub struct FunctionDeclaration {
    #[builder(setter(into))]
    pub name: FunctionName,
    #[builder(setter(into))]
    pub args: Vec<ArgumentDeclaration>,
}

impl FunctionDeclaration {
    pub fn new<N: Into<FunctionName>, T: Into<ArgumentDeclaration>>(name: N, args: Vec<T>) -> Self {
        Self {
            name: name.into(),
            args: args.into_iter().map(T::into).collect(),
        }
    }
}

impl Parse for FunctionDeclaration {
    type Output = Self;
    fn parse(s: &mut StatementStream<'_>) -> anyhow::Result<Self::Output> {
        let name = s.parse()?;
        let args = s.parse_from::<Parens<List<ArgumentDeclaration, Comma>>>()?;
        Ok(Self { name, args })
    }
}

impl Display for FunctionDeclaration {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{} ({})",
            self.name,
            self.args.iter().map(|a| a.to_string()).collect::<Vec<_>>().join(", ")
        )
    }
}

#[derive(ParseFromStr, Clone, Debug, ToTokens, PartialEq, Eq)]
pub struct FunctionReference {
    pub name: FunctionName,
    pub args: Option<Vec<CqlType>>,
}

impl FunctionReference {
    pub fn args<T: Into<CqlType>>(self, args: Vec<T>) -> Self {
        Self {
            name: self.name,
            args: Some(args.into_iter().map(T::into).collect()),
        }
    }
}

impl Parse for FunctionReference {
    type Output = Self;
    fn parse(s: &mut StatementStream<'_>) -> anyhow::Result<Self::Output> {
        let name = s.parse()?;
        let args = s.parse_from::<Option<Parens<List<CqlType, Comma>>>>()?;
        Ok(Self { name, args })
    }
}

impl Display for FunctionReference {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}{}",
            self.name,
            if let Some(a) = self.args.as_ref() {
                format!("({})", a.iter().map(|a| a.to_string()).collect::<Vec<_>>().join(", "))
            } else {
                String::new()
            }
        )
    }
}

impl From<FunctionName> for FunctionReference {
    fn from(name: FunctionName) -> Self {
        Self { name, args: None }
    }
}

impl From<String> for FunctionReference {
    fn from(name: String) -> Self {
        Self {
            name: name.into(),
            args: None,
        }
    }
}

impl From<&str> for FunctionReference {
    fn from(name: &str) -> Self {
        Self {
            name: name.into(),
            args: None,
        }
    }
}

#[derive(ParseFromStr, Clone, Debug, PartialEq, Eq, Hash, Ord, PartialOrd, ToTokens)]
pub struct FunctionCall {
    pub name: FunctionName,
    pub args: Vec<Term>,
}

impl FunctionCall {
    pub fn new<F: Into<FunctionName>, T: Into<Term>>(name: F, args: Vec<T>) -> Self {
        Self {
            name: name.into(),
            args: args.into_iter().map(|a| a.into()).collect(),
        }
    }
}

impl Parse for FunctionCall {
    type Output = Self;
    fn parse(s: &mut StatementStream<'_>) -> anyhow::Result<Self::Output> {
        let (name, args) = s.parse_from::<(FunctionName, Parens<List<Term, Comma>>)>()?;
        Ok(Self { name, args })
    }
}
impl Peek for FunctionCall {
    fn peek(mut s: StatementStream<'_>) -> bool {
        s.parse::<Self>().is_ok()
    }
}

impl Display for FunctionCall {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}({})",
            self.name,
            self.args.iter().map(|t| t.to_string()).collect::<Vec<_>>().join(", ")
        )
    }
}

#[derive(ParseFromStr, Clone, Debug, TryInto, From, ToTokens, PartialEq, Eq)]
pub enum UserDefinedFunctionStatement {
    Create(CreateFunctionStatement),
    Drop(DropFunctionStatement),
    CreateAggregate(CreateAggregateFunctionStatement),
    DropAggregate(DropAggregateFunctionStatement),
}

impl Parse for UserDefinedFunctionStatement {
    type Output = Self;
    fn parse(s: &mut StatementStream<'_>) -> anyhow::Result<Self::Output> {
        Ok(if let Some(stmt) = s.parse::<Option<CreateFunctionStatement>>()? {
            Self::Create(stmt)
        } else if let Some(stmt) = s.parse::<Option<DropFunctionStatement>>()? {
            Self::Drop(stmt)
        } else if let Some(stmt) = s.parse::<Option<CreateAggregateFunctionStatement>>()? {
            Self::CreateAggregate(stmt)
        } else if let Some(stmt) = s.parse::<Option<DropAggregateFunctionStatement>>()? {
            Self::DropAggregate(stmt)
        } else {
            anyhow::bail!("Expected a data manipulation statement, found {}", s.info())
        })
    }
}

impl Peek for UserDefinedFunctionStatement {
    fn peek(s: StatementStream<'_>) -> bool {
        s.check::<CreateFunctionStatement>()
            || s.check::<DropFunctionStatement>()
            || s.check::<CreateAggregateFunctionStatement>()
            || s.check::<DropAggregateFunctionStatement>()
    }
}

impl Display for UserDefinedFunctionStatement {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Create(stmt) => stmt.fmt(f),
            Self::Drop(stmt) => stmt.fmt(f),
            Self::CreateAggregate(stmt) => stmt.fmt(f),
            Self::DropAggregate(stmt) => stmt.fmt(f),
        }
    }
}

#[derive(ParseFromStr, Builder, Clone, Debug, ToTokens, PartialEq, Eq)]
pub struct CreateFunctionStatement {
    #[builder(setter(name = "set_or_replace"), default)]
    pub or_replace: bool,
    #[builder(setter(name = "set_if_not_exists"), default)]
    pub if_not_exists: bool,
    pub func: FunctionDeclaration,
    pub on_null_input: OnNullInput,
    #[builder(setter(into))]
    pub return_type: CqlType,
    #[builder(setter(into))]
    pub language: Name,
    #[builder(setter(into))]
    pub body: LitStr,
}

impl CreateFunctionStatementBuilder {
    /// Set IF NOT EXISTS on the statement.
    /// To undo this, use `set_if_not_exists(false)`.
    pub fn if_not_exists(&mut self) -> &mut Self {
        self.if_not_exists.replace(true);
        self
    }

    /// Set OR REPLACE on the statement.
    /// To undo this, use `set_or_replace(false)`.
    pub fn or_replace(&mut self) -> &mut Self {
        self.or_replace.replace(true);
        self
    }
}

impl Parse for CreateFunctionStatement {
    type Output = Self;
    fn parse(s: &mut StatementStream<'_>) -> anyhow::Result<Self::Output> {
        s.parse::<CREATE>()?;
        let mut res = CreateFunctionStatementBuilder::default();
        res.set_or_replace(s.parse::<Option<(OR, REPLACE)>>()?.is_some());
        s.parse::<FUNCTION>()?;
        res.set_if_not_exists(s.parse::<Option<(IF, NOT, EXISTS)>>()?.is_some())
            .func(s.parse()?)
            .on_null_input(s.parse()?)
            .return_type(s.parse::<(RETURNS, CqlType)>()?.1)
            .language(s.parse::<(LANGUAGE, Name)>()?.1);
        res.body(s.parse::<(AS, LitStr)>()?.1);
        s.parse::<Option<Semicolon>>()?;
        Ok(res
            .build()
            .map_err(|e| anyhow::anyhow!("Invalid CREATE FUNCTION statement: {}", e))?)
    }
}

impl Peek for CreateFunctionStatement {
    fn peek(s: StatementStream<'_>) -> bool {
        s.check::<(CREATE, Option<(OR, REPLACE)>, FUNCTION)>()
    }
}

impl Display for CreateFunctionStatement {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "CREATE{} FUNCTION{} {} {} RETURNS {} LANGUAGE {} AS {}",
            if self.or_replace { " OR REPLACE" } else { "" },
            if self.if_not_exists { " IF NOT EXISTS" } else { "" },
            self.func,
            self.on_null_input,
            self.return_type,
            self.language,
            self.body
        )
    }
}

#[derive(ParseFromStr, Clone, Debug, ToTokens, PartialEq, Eq)]
pub enum OnNullInput {
    Called,
    ReturnsNull,
}

impl Parse for OnNullInput {
    type Output = Self;
    fn parse(s: &mut StatementStream<'_>) -> anyhow::Result<Self::Output> {
        Ok(if s.parse::<(CALLED, ON, NULL, INPUT)>().is_ok() {
            Self::Called
        } else if s.parse::<(RETURNS, NULL, ON, NULL, INPUT)>().is_ok() {
            Self::ReturnsNull
        } else {
            anyhow::bail!("Invalid ON NULL INPUT declaration: {}", s.info())
        })
    }
}

impl Display for OnNullInput {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Called => write!(f, "CALLED ON NULL INPUT"),
            Self::ReturnsNull => write!(f, "RETURNS NULL ON NULL INPUT"),
        }
    }
}

pub type ArgumentDeclaration = FieldDefinition;

#[derive(ParseFromStr, Builder, Clone, Debug, ToTokens, PartialEq, Eq)]
pub struct DropFunctionStatement {
    #[builder(setter(name = "set_if_exists"), default)]
    pub if_exists: bool,
    #[builder(setter(into))]
    pub func: FunctionReference,
}

impl DropFunctionStatementBuilder {
    /// Set IF EXISTS on the statement.
    /// To undo this, use `set_if_exists(false)`.
    pub fn if_exists(&mut self) -> &mut Self {
        self.if_exists.replace(true);
        self
    }
}

impl Parse for DropFunctionStatement {
    type Output = Self;
    fn parse(s: &mut StatementStream<'_>) -> anyhow::Result<Self::Output> {
        s.parse::<(DROP, FUNCTION)>()?;
        let mut res = DropFunctionStatementBuilder::default();
        res.set_if_exists(s.parse::<Option<(IF, EXISTS)>>()?.is_some())
            .func(s.parse::<FunctionReference>()?);
        s.parse::<Option<Semicolon>>()?;
        Ok(res
            .build()
            .map_err(|e| anyhow::anyhow!("Invalid DROP FUNCTION statement: {}", e))?)
    }
}

impl Peek for DropFunctionStatement {
    fn peek(s: StatementStream<'_>) -> bool {
        s.check::<(DROP, FUNCTION)>()
    }
}

impl Display for DropFunctionStatement {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "DROP FUNCTION{} {}",
            if self.if_exists { " IF EXISTS" } else { "" },
            self.func
        )
    }
}
#[derive(ParseFromStr, Builder, Clone, Debug, ToTokens, PartialEq, Eq)]
#[builder(setter(strip_option))]
pub struct CreateAggregateFunctionStatement {
    #[builder(setter(name = "set_or_replace"), default)]
    pub or_replace: bool,
    #[builder(setter(name = "set_if_not_exists"), default)]
    pub if_not_exists: bool,
    pub func: FunctionDeclaration,
    pub state_modifying_fn: FunctionName,
    #[builder(setter(into))]
    pub state_value_type: CqlType,
    #[builder(default)]
    pub final_fn: Option<FunctionName>,
    #[builder(setter(into), default)]
    pub init_condition: Option<Term>,
}

impl CreateAggregateFunctionStatementBuilder {
    /// Set IF NOT EXISTS on the statement.
    /// To undo this, use `set_if_not_exists(false)`.
    pub fn if_not_exists(&mut self) -> &mut Self {
        self.if_not_exists.replace(true);
        self
    }

    /// Set OR REPLACE on the statement.
    /// To undo this, use `set_or_replace(false)`.
    pub fn or_replace(&mut self) -> &mut Self {
        self.or_replace.replace(true);
        self
    }
}

impl Parse for CreateAggregateFunctionStatement {
    type Output = Self;
    fn parse(s: &mut StatementStream<'_>) -> anyhow::Result<Self::Output> {
        s.parse::<CREATE>()?;
        let mut res = CreateAggregateFunctionStatementBuilder::default();
        res.set_or_replace(s.parse::<Option<(OR, REPLACE)>>()?.is_some());
        s.parse::<AGGREGATE>()?;
        res.set_if_not_exists(s.parse::<Option<(IF, NOT, EXISTS)>>()?.is_some())
            .func(s.parse()?)
            .state_modifying_fn(s.parse::<(SFUNC, _)>()?.1)
            .state_value_type(s.parse::<(STYPE, CqlType)>()?.1);
        loop {
            if s.remaining() == 0 || s.parse::<Option<Semicolon>>()?.is_some() {
                break;
            }
            if let Some(f) = s.parse_from::<If<FINALFUNC, FunctionName>>()? {
                if res.final_fn.is_some() {
                    anyhow::bail!("Duplicate FINALFUNC declaration");
                }
                res.final_fn(f);
            } else if let Some(i) = s.parse_from::<If<INITCOND, Term>>()? {
                if res.init_condition.is_some() {
                    anyhow::bail!("Duplicate INITCOND declaration");
                }
                res.init_condition(i);
            } else {
                return Ok(res.build().map_err(|_| {
                    anyhow::anyhow!("Invalid tokens in CREATE AGGREGATE FUNCTION statement: {}", s.info())
                })?);
            }
        }
        s.parse::<Option<Semicolon>>()?;
        Ok(res
            .build()
            .map_err(|e| anyhow::anyhow!("Invalid CREATE AGGREGATE FUNCTION statement: {}", e))?)
    }
}

impl Peek for CreateAggregateFunctionStatement {
    fn peek(s: StatementStream<'_>) -> bool {
        s.check::<(CREATE, Option<(OR, REPLACE)>, AGGREGATE)>()
    }
}

impl Display for CreateAggregateFunctionStatement {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "CREATE{} AGGREGATE{} {} SFUNC {} STYPE {}",
            if self.or_replace { " OR REPLACE" } else { "" },
            if self.if_not_exists { " IF NOT EXISTS" } else { "" },
            self.func,
            self.state_modifying_fn,
            self.state_value_type
        )?;
        if let Some(n) = &self.final_fn {
            write!(f, " FINALFUNC {}", n)?;
        }
        if let Some(i) = &self.init_condition {
            write!(f, " INITCOND {}", i)?;
        }
        Ok(())
    }
}

#[derive(ParseFromStr, Builder, Clone, Debug, ToTokens, PartialEq, Eq)]
pub struct DropAggregateFunctionStatement {
    #[builder(setter(name = "set_if_exists"), default)]
    pub if_exists: bool,
    #[builder(setter(into))]
    pub func: FunctionReference,
}

impl DropAggregateFunctionStatementBuilder {
    /// Set IF EXISTS on the statement.
    /// To undo this, use `set_if_exists(false)`.
    pub fn if_exists(&mut self) -> &mut Self {
        self.if_exists.replace(true);
        self
    }
}

impl Parse for DropAggregateFunctionStatement {
    type Output = Self;
    fn parse(s: &mut StatementStream<'_>) -> anyhow::Result<Self::Output> {
        s.parse::<(DROP, AGGREGATE)>()?;
        let mut res = DropAggregateFunctionStatementBuilder::default();
        res.set_if_exists(s.parse::<Option<(IF, EXISTS)>>()?.is_some())
            .func(s.parse::<FunctionReference>()?);
        s.parse::<Option<Semicolon>>()?;
        Ok(res
            .build()
            .map_err(|e| anyhow::anyhow!("Invalid DROP AGGREGATE statement: {}", e))?)
    }
}

impl Peek for DropAggregateFunctionStatement {
    fn peek(s: StatementStream<'_>) -> bool {
        s.check::<(DROP, AGGREGATE)>()
    }
}

impl Display for DropAggregateFunctionStatement {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "DROP AGGREGATE{} {}",
            if self.if_exists { " IF EXISTS" } else { "" },
            self.func
        )
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::{
        ArithmeticOp,
        CollectionType,
        Constant,
        KeyspaceQualifyExt,
        NativeType,
    };

    #[test]
    fn test_parse_create_function() {
        let mut builder = CreateFunctionStatementBuilder::default();
        builder.on_null_input(OnNullInput::Called);
        assert!(builder.build().is_err());
        builder.func(FunctionDeclaration::new(
            "test".dot("func"),
            vec![("a", NativeType::Int), ("b whitespace", NativeType::Float)],
        ));
        assert!(builder.build().is_err());
        builder.return_type(NativeType::Int);
        assert!(builder.build().is_err());
        builder.language("java");
        assert!(builder.build().is_err());
        builder.body("java.lang.Math.add(a, b)");
        let statement = builder.build().unwrap().to_string();
        assert_eq!(builder.build().unwrap(), statement.parse().unwrap());
        builder.or_replace();
        let statement = builder.build().unwrap().to_string();
        assert_eq!(builder.build().unwrap(), statement.parse().unwrap());
        builder.if_not_exists();
        let statement = builder.build().unwrap().to_string();
        assert_eq!(builder.build().unwrap(), statement.parse().unwrap());
    }

    #[test]
    fn test_parse_drop_function() {
        let mut builder = DropFunctionStatementBuilder::default();
        builder.func("test".dot("func"));
        let statement = builder.build().unwrap().to_string();
        assert_eq!(builder.build().unwrap(), statement.parse().unwrap());
        builder.func(FunctionReference::from("test".dot("func")).args::<CqlType>(vec![
            NativeType::Int.into(),
            CollectionType::list(NativeType::Float).into(),
            "my_keyspace".dot("my_func").into(),
        ]));
        builder.if_exists();
        let statement = builder.build().unwrap().to_string();
        assert_eq!(builder.build().unwrap(), statement.parse().unwrap());
    }

    #[test]
    fn test_parse_create_aggregate_function() {
        let mut builder = CreateAggregateFunctionStatementBuilder::default();
        builder.func(
            FunctionDeclarationBuilder::default()
                .name("test".dot("func"))
                .args(vec![("a", NativeType::Int).into(), ("b", NativeType::Float).into()])
                .build()
                .unwrap(),
        );
        assert!(builder.build().is_err());
        builder.state_modifying_fn("test".dot("func_state_modifying_fn"));
        assert!(builder.build().is_err());
        builder.state_value_type(NativeType::Int);
        let statement = builder.build().unwrap().to_string();
        assert_eq!(builder.build().unwrap(), statement.parse().unwrap());
        builder.init_condition(0_i32);
        let statement = builder.build().unwrap().to_string();
        assert_eq!(builder.build().unwrap(), statement.parse().unwrap());
        builder.if_not_exists();
        let statement = builder.build().unwrap().to_string();
        assert_eq!(builder.build().unwrap(), statement.parse().unwrap());
        builder.or_replace();
        let statement = builder.build().unwrap().to_string();
        assert_eq!(builder.build().unwrap(), statement.parse().unwrap());
        builder.state_value_type(CollectionType::list(NativeType::Uuid));
        let statement = builder.build().unwrap().to_string();
        assert_eq!(builder.build().unwrap(), statement.parse().unwrap());
        builder.final_fn("test".dot("final_fn"));
        let statement = builder.build().unwrap().to_string();
        assert_eq!(builder.build().unwrap(), statement.parse().unwrap());
        builder.init_condition(Term::negative(Constant::string("test")));
        let statement = builder.build().unwrap().to_string();
        assert_eq!(builder.build().unwrap(), statement.parse().unwrap());
        builder.init_condition(Term::arithmetic_op(
            10_f32,
            ArithmeticOp::Mul,
            Constant::float("2.06E3").unwrap(),
        ));
        let statement = builder.build().unwrap().to_string();
        assert_eq!(builder.build().unwrap(), statement.parse().unwrap());
        builder.init_condition(Term::type_hint(NativeType::Bigint, "test"));
        let statement = builder.build().unwrap().to_string();
        assert_eq!(builder.build().unwrap(), statement.parse().unwrap());
        builder.init_condition(Term::bind_marker("marker whitespace"));
        let statement = builder.build().unwrap().to_string();
        assert_eq!(builder.build().unwrap(), statement.parse().unwrap());
    }

    #[test]
    fn test_parse_drop_aggregate_function() {
        let mut builder = DropAggregateFunctionStatementBuilder::default();
        builder.func("func");
        let statement = builder.build().unwrap().to_string();
        assert_eq!(builder.build().unwrap(), statement.parse().unwrap());
        builder.if_exists();
        let statement = builder.build().unwrap().to_string();
        assert_eq!(builder.build().unwrap(), statement.parse().unwrap());
    }
}

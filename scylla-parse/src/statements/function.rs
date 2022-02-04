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

#[derive(ParseFromStr, Clone, Debug, ToTokens, PartialEq, Eq)]
pub struct FunctionSignature {
    pub name: FunctionName,
    pub args: Vec<CqlType>,
}

impl FunctionSignature {
    pub fn new<F: Into<FunctionName>, T: Into<CqlType>>(name: F, args: Vec<T>) -> Self {
        Self {
            name: name.into(),
            args: args.into_iter().map(T::into).collect(),
        }
    }
}

impl Parse for FunctionSignature {
    type Output = Self;
    fn parse(s: &mut StatementStream<'_>) -> anyhow::Result<Self::Output> {
        Ok(Self {
            name: s.parse()?,
            args: s.parse_from::<Parens<List<CqlType, Comma>>>()?,
        })
    }
}

impl Display for FunctionSignature {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}({})",
            self.name,
            self.args.iter().map(|a| a.to_string()).collect::<Vec<_>>().join(", ")
        )
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
#[parse_via(TaggedUserDefinedFunctionStatement)]
pub enum UserDefinedFunctionStatement {
    Create(CreateFunctionStatement),
    Drop(DropFunctionStatement),
    CreateAggregate(CreateAggregateFunctionStatement),
    DropAggregate(DropAggregateFunctionStatement),
}

impl TryFrom<TaggedUserDefinedFunctionStatement> for UserDefinedFunctionStatement {
    type Error = anyhow::Error;
    fn try_from(t: TaggedUserDefinedFunctionStatement) -> anyhow::Result<Self> {
        Ok(match t {
            TaggedUserDefinedFunctionStatement::Create(s) => Self::Create(s.try_into()?),
            TaggedUserDefinedFunctionStatement::Drop(s) => Self::Drop(s.try_into()?),
            TaggedUserDefinedFunctionStatement::CreateAggregate(s) => Self::CreateAggregate(s.try_into()?),
            TaggedUserDefinedFunctionStatement::DropAggregate(s) => Self::DropAggregate(s.try_into()?),
        })
    }
}

#[derive(ParseFromStr, Clone, Debug, TryInto, From, ToTokens, PartialEq, Eq)]
#[tokenize_as(UserDefinedFunctionStatement)]
pub enum TaggedUserDefinedFunctionStatement {
    Create(TaggedCreateFunctionStatement),
    Drop(TaggedDropFunctionStatement),
    CreateAggregate(TaggedCreateAggregateFunctionStatement),
    DropAggregate(TaggedDropAggregateFunctionStatement),
}

impl Parse for TaggedUserDefinedFunctionStatement {
    type Output = Self;
    fn parse(s: &mut StatementStream<'_>) -> anyhow::Result<Self::Output> {
        let mut lookahead = s.clone();
        let keyword1 = lookahead.parse::<ReservedKeyword>()?;
        Ok(match keyword1 {
            ReservedKeyword::CREATE | ReservedKeyword::DROP => {
                let keyword2 = lookahead.parse_from::<Alpha>()?;
                match (keyword1, keyword2.to_uppercase().as_str()) {
                    (ReservedKeyword::CREATE, "OR") => {
                        if lookahead.check::<(REPLACE, FUNCTION)>() {
                            Self::Create(s.parse()?)
                        } else if lookahead.check::<(REPLACE, AGGREGATE)>() {
                            Self::CreateAggregate(s.parse()?)
                        } else {
                            anyhow::bail!("Unexpected token following OR: {}", keyword2);
                        }
                    }
                    (ReservedKeyword::CREATE, "FUNCTION") => Self::Create(s.parse()?),
                    (ReservedKeyword::CREATE, "AGGREGATE") => Self::CreateAggregate(s.parse()?),
                    (ReservedKeyword::DROP, "FUNCTION") => Self::Drop(s.parse()?),
                    (ReservedKeyword::DROP, "AGGREGATE") => Self::DropAggregate(s.parse()?),
                    _ => anyhow::bail!("Unexpected token following {}: {}", keyword1, keyword2),
                }
            }
            _ => anyhow::bail!("Expected a user defined function statement, found {}", s.info()),
        })
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
#[parse_via(TaggedCreateFunctionStatement)]
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

impl TryFrom<TaggedCreateFunctionStatement> for CreateFunctionStatement {
    type Error = anyhow::Error;
    fn try_from(value: TaggedCreateFunctionStatement) -> Result<Self, Self::Error> {
        Ok(Self {
            or_replace: value.or_replace,
            if_not_exists: value.if_not_exists,
            func: value.func.into_value()?,
            on_null_input: value.on_null_input.into_value()?,
            return_type: value.return_type.into_value()?,
            language: value.language.into_value()?,
            body: value.body.into_value()?,
        })
    }
}

#[derive(ParseFromStr, Builder, Clone, Debug, ToTokens, PartialEq, Eq)]
#[tokenize_as(CreateFunctionStatement)]
pub struct TaggedCreateFunctionStatement {
    #[builder(setter(name = "set_or_replace"), default)]
    pub or_replace: bool,
    #[builder(setter(name = "set_if_not_exists"), default)]
    pub if_not_exists: bool,
    pub func: Tag<FunctionDeclaration>,
    pub on_null_input: Tag<OnNullInput>,
    pub return_type: Tag<CqlType>,
    pub language: Tag<Name>,
    pub body: Tag<LitStr>,
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

impl Parse for TaggedCreateFunctionStatement {
    type Output = Self;
    fn parse(s: &mut StatementStream<'_>) -> anyhow::Result<Self::Output> {
        s.parse::<CREATE>()?;
        let mut res = TaggedCreateFunctionStatementBuilder::default();
        res.set_or_replace(s.parse::<Option<(OR, REPLACE)>>()?.is_some());
        s.parse::<FUNCTION>()?;
        res.set_if_not_exists(s.parse::<Option<(IF, NOT, EXISTS)>>()?.is_some())
            .func(s.parse()?)
            .on_null_input(s.parse()?)
            .return_type(s.parse::<(RETURNS, _)>()?.1)
            .language(s.parse::<(LANGUAGE, _)>()?.1);
        res.body(s.parse::<(AS, _)>()?.1);
        s.parse::<Option<Semicolon>>()?;
        Ok(res
            .build()
            .map_err(|e| anyhow::anyhow!("Invalid CREATE FUNCTION statement: {}", e))?)
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
            anyhow::bail!("Expected ON NULL INPUT declaration, found {}", s.info())
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
#[parse_via(TaggedDropFunctionStatement)]
pub struct DropFunctionStatement {
    #[builder(setter(name = "set_if_exists"), default)]
    pub if_exists: bool,
    #[builder(setter(into))]
    pub func: FunctionReference,
}

impl TryFrom<TaggedDropFunctionStatement> for DropFunctionStatement {
    type Error = anyhow::Error;
    fn try_from(value: TaggedDropFunctionStatement) -> Result<Self, Self::Error> {
        Ok(Self {
            if_exists: value.if_exists,
            func: value.func.into_value()?,
        })
    }
}

#[derive(ParseFromStr, Builder, Clone, Debug, ToTokens, PartialEq, Eq)]
#[tokenize_as(DropFunctionStatement)]
pub struct TaggedDropFunctionStatement {
    #[builder(setter(name = "set_if_exists"), default)]
    pub if_exists: bool,
    pub func: Tag<FunctionReference>,
}

impl DropFunctionStatementBuilder {
    /// Set IF EXISTS on the statement.
    /// To undo this, use `set_if_exists(false)`.
    pub fn if_exists(&mut self) -> &mut Self {
        self.if_exists.replace(true);
        self
    }
}

impl Parse for TaggedDropFunctionStatement {
    type Output = Self;
    fn parse(s: &mut StatementStream<'_>) -> anyhow::Result<Self::Output> {
        s.parse::<(DROP, FUNCTION)>()?;
        let mut res = TaggedDropFunctionStatementBuilder::default();
        res.set_if_exists(s.parse::<Option<(IF, EXISTS)>>()?.is_some())
            .func(s.parse()?);
        s.parse::<Option<Semicolon>>()?;
        Ok(res
            .build()
            .map_err(|e| anyhow::anyhow!("Invalid DROP FUNCTION statement: {}", e))?)
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
#[parse_via(TaggedCreateAggregateFunctionStatement)]
pub struct CreateAggregateFunctionStatement {
    #[builder(setter(name = "set_or_replace"), default)]
    pub or_replace: bool,
    #[builder(setter(name = "set_if_not_exists"), default)]
    pub if_not_exists: bool,
    pub func: FunctionSignature,
    pub state_modifying_fn: FunctionName,
    #[builder(setter(into))]
    pub state_value_type: CqlType,
    #[builder(default)]
    pub final_fn: Option<FunctionName>,
    #[builder(setter(into), default)]
    pub init_condition: Option<Term>,
}

impl TryFrom<TaggedCreateAggregateFunctionStatement> for CreateAggregateFunctionStatement {
    type Error = anyhow::Error;
    fn try_from(value: TaggedCreateAggregateFunctionStatement) -> Result<Self, Self::Error> {
        Ok(Self {
            or_replace: value.or_replace,
            if_not_exists: value.if_not_exists,
            func: value.func.into_value()?,
            state_modifying_fn: value.state_modifying_fn.into_value()?,
            state_value_type: value.state_value_type.into_value()?,
            final_fn: value.final_fn.map(|v| v.into_value()).transpose()?,
            init_condition: value.init_condition.map(|v| v.into_value()).transpose()?,
        })
    }
}

#[derive(ParseFromStr, Builder, Clone, Debug, ToTokens, PartialEq, Eq)]
#[builder(setter(strip_option))]
#[tokenize_as(CreateAggregateFunctionStatement)]
pub struct TaggedCreateAggregateFunctionStatement {
    #[builder(setter(name = "set_or_replace"), default)]
    pub or_replace: bool,
    #[builder(setter(name = "set_if_not_exists"), default)]
    pub if_not_exists: bool,
    pub func: Tag<FunctionSignature>,
    pub state_modifying_fn: Tag<FunctionName>,
    pub state_value_type: Tag<CqlType>,
    #[builder(default)]
    pub final_fn: Option<Tag<FunctionName>>,
    #[builder(default)]
    pub init_condition: Option<Tag<Term>>,
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

impl Parse for TaggedCreateAggregateFunctionStatement {
    type Output = Self;
    fn parse(s: &mut StatementStream<'_>) -> anyhow::Result<Self::Output> {
        s.parse::<CREATE>()?;
        let mut res = TaggedCreateAggregateFunctionStatementBuilder::default();
        res.set_or_replace(s.parse::<Option<(OR, REPLACE)>>()?.is_some());
        s.parse::<AGGREGATE>()?;
        res.set_if_not_exists(s.parse::<Option<(IF, NOT, EXISTS)>>()?.is_some())
            .func(s.parse()?)
            .state_modifying_fn(s.parse::<(SFUNC, _)>()?.1)
            .state_value_type(s.parse::<(STYPE, _)>()?.1);
        loop {
            if s.remaining() == 0 || s.parse::<Option<Semicolon>>()?.is_some() {
                break;
            }
            if let Some(f) = s.parse_from::<If<FINALFUNC, Tag<FunctionName>>>()? {
                if res.final_fn.is_some() {
                    anyhow::bail!("Duplicate FINALFUNC declaration");
                }
                res.final_fn(f);
            } else if let Some(i) = s.parse_from::<If<INITCOND, Tag<Term>>>()? {
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
#[parse_via(TaggedDropAggregateFunctionStatement)]
pub struct DropAggregateFunctionStatement {
    #[builder(setter(name = "set_if_exists"), default)]
    pub if_exists: bool,
    #[builder(setter(into))]
    pub func: FunctionReference,
}

impl TryFrom<TaggedDropAggregateFunctionStatement> for DropAggregateFunctionStatement {
    type Error = anyhow::Error;
    fn try_from(value: TaggedDropAggregateFunctionStatement) -> Result<Self, Self::Error> {
        Ok(Self {
            if_exists: value.if_exists,
            func: value.func.into_value()?,
        })
    }
}

#[derive(ParseFromStr, Builder, Clone, Debug, ToTokens, PartialEq, Eq)]
#[tokenize_as(DropAggregateFunctionStatement)]
pub struct TaggedDropAggregateFunctionStatement {
    #[builder(setter(name = "set_if_exists"), default)]
    pub if_exists: bool,
    pub func: Tag<FunctionReference>,
}

impl DropAggregateFunctionStatementBuilder {
    /// Set IF EXISTS on the statement.
    /// To undo this, use `set_if_exists(false)`.
    pub fn if_exists(&mut self) -> &mut Self {
        self.if_exists.replace(true);
        self
    }
}

impl Parse for TaggedDropAggregateFunctionStatement {
    type Output = Self;
    fn parse(s: &mut StatementStream<'_>) -> anyhow::Result<Self::Output> {
        s.parse::<(DROP, AGGREGATE)>()?;
        let mut res = TaggedDropAggregateFunctionStatementBuilder::default();
        res.set_if_exists(s.parse::<Option<(IF, EXISTS)>>()?.is_some())
            .func(s.parse()?);
        s.parse::<Option<Semicolon>>()?;
        Ok(res
            .build()
            .map_err(|e| anyhow::anyhow!("Invalid DROP AGGREGATE statement: {}", e))?)
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
        builder.func(FunctionSignature::new(
            "test".dot("func"),
            vec![NativeType::Int, NativeType::Float],
        ));
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

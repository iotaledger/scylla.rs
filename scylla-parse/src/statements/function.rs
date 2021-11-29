// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use super::*;

pub type FunctionName = KeyspaceQualifiedName;

#[derive(ParseFromStr, Builder, Clone, Debug, ToTokens)]
pub struct FunctionDeclaration {
    #[builder(setter(into))]
    pub name: FunctionName,
    #[builder(setter(into))]
    pub args: Vec<ArgumentDeclaration>,
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

#[derive(ParseFromStr, Clone, Debug, ToTokens)]
pub struct FunctionReference {
    pub name: FunctionName,
    pub args: Option<Vec<CqlType>>,
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

#[derive(ParseFromStr, Clone, Debug, TryInto, From, ToTokens)]
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

#[derive(ParseFromStr, Builder, Clone, Debug, ToTokens)]
pub struct CreateFunctionStatement {
    #[builder(default)]
    pub or_replace: bool,
    #[builder(default)]
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

impl Parse for CreateFunctionStatement {
    type Output = Self;
    fn parse(s: &mut StatementStream<'_>) -> anyhow::Result<Self::Output> {
        s.parse::<CREATE>()?;
        let mut res = CreateFunctionStatementBuilder::default();
        res.or_replace(s.parse::<Option<(OR, REPLACE)>>()?.is_some());
        s.parse::<FUNCTION>()?;
        res.if_not_exists(s.parse::<Option<(IF, NOT, EXISTS)>>()?.is_some())
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

#[derive(ParseFromStr, Clone, Debug, ToTokens)]
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

#[derive(ParseFromStr, Builder, Clone, Debug, ToTokens)]
pub struct DropFunctionStatement {
    #[builder(default)]
    pub if_exists: bool,
    #[builder(setter(into))]
    pub func: FunctionReference,
}

impl Parse for DropFunctionStatement {
    type Output = Self;
    fn parse(s: &mut StatementStream<'_>) -> anyhow::Result<Self::Output> {
        s.parse::<(DROP, FUNCTION)>()?;
        let mut res = DropFunctionStatementBuilder::default();
        res.if_exists(s.parse::<Option<(IF, EXISTS)>>()?.is_some())
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
#[derive(ParseFromStr, Builder, Clone, Debug, ToTokens)]
#[builder(setter(strip_option))]
pub struct CreateAggregateFunctionStatement {
    #[builder(default)]
    pub or_replace: bool,
    #[builder(default)]
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

impl Parse for CreateAggregateFunctionStatement {
    type Output = Self;
    fn parse(s: &mut StatementStream<'_>) -> anyhow::Result<Self::Output> {
        s.parse::<CREATE>()?;
        let mut res = CreateAggregateFunctionStatementBuilder::default();
        res.or_replace(s.parse::<Option<(OR, REPLACE)>>()?.is_some());
        s.parse::<AGGREGATE>()?;
        res.if_not_exists(s.parse::<Option<(IF, NOT, EXISTS)>>()?.is_some())
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

#[derive(ParseFromStr, Builder, Clone, Debug, ToTokens)]
pub struct DropAggregateFunctionStatement {
    #[builder(default)]
    pub if_exists: bool,
    #[builder(setter(into))]
    pub func: FunctionReference,
}

impl Parse for DropAggregateFunctionStatement {
    type Output = Self;
    fn parse(s: &mut StatementStream<'_>) -> anyhow::Result<Self::Output> {
        s.parse::<(DROP, AGGREGATE)>()?;
        let mut res = DropAggregateFunctionStatementBuilder::default();
        res.if_exists(s.parse::<Option<(IF, EXISTS)>>()?.is_some())
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
        KeyspaceQualifyExt,
        NativeType,
    };

    #[test]
    fn test_parse_create_function() {
        let mut s = StatementStream::new(
            "CREATE FUNCTION test.func(a int, b float) CALLED ON NULL INPUT RETURNS int LANGUAGE java AS 'java.lang.Math.add(a, b)'",
        );
        let res = s.parse::<CreateFunctionStatement>().unwrap();
        let test = CreateFunctionStatementBuilder::default()
            .if_not_exists(false)
            .func(
                FunctionDeclarationBuilder::default()
                    .name("test".dot("func"))
                    .args(vec![("a", NativeType::Int).into(), ("b", NativeType::Float).into()])
                    .build()
                    .unwrap(),
            )
            .on_null_input(OnNullInput::Called)
            .return_type(NativeType::Int)
            .language("java")
            .body("java.lang.Math.add(a, b)")
            .build()
            .unwrap();
        assert_eq!(res.to_string(), test.to_string());
    }

    #[test]
    fn test_parse_drop_function() {
        let mut s = StatementStream::new("DROP FUNCTION test.func");
        let res = s.parse::<DropFunctionStatement>().unwrap();
        let test = DropFunctionStatementBuilder::default()
            .if_exists(false)
            .func("test".dot("func"))
            .build()
            .unwrap();
        assert_eq!(res.to_string(), test.to_string());
    }

    #[test]
    fn test_parse_create_aggregate_function() {
        let mut s = StatementStream::new(
            "CREATE AGGREGATE test.func(a int, b float) SFUNC test.func_state_modifying_fn STYPE int INITCOND 0",
        );
        let res = s.parse::<CreateAggregateFunctionStatement>().unwrap();
        let test = CreateAggregateFunctionStatementBuilder::default()
            .if_not_exists(false)
            .func(
                FunctionDeclarationBuilder::default()
                    .name("test".dot("func"))
                    .args(vec![("a", NativeType::Int).into(), ("b", NativeType::Float).into()])
                    .build()
                    .unwrap(),
            )
            .state_modifying_fn("test".dot("func_state_modifying_fn"))
            .state_value_type(NativeType::Int)
            .init_condition(0_i32)
            .build()
            .unwrap();
        assert_eq!(res.to_string(), test.to_string());
    }

    #[test]
    fn test_parse_drop_aggregate_function() {
        let mut s = StatementStream::new("DROP AGGREGATE test.func");
        let res = s.parse::<DropAggregateFunctionStatement>().unwrap();
        let test = DropAggregateFunctionStatementBuilder::default()
            .if_exists(false)
            .func("test".dot("func"))
            .build()
            .unwrap();
        assert_eq!(res.to_string(), test.to_string());
    }
}

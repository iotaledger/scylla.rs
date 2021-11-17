use super::*;

pub type FunctionName = KeyspaceQualifiedName;

#[derive(ParseFromStr, Clone, Debug)]
pub struct FunctionDeclaration {
    pub name: FunctionName,
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

#[derive(ParseFromStr, Clone, Debug)]
pub struct FunctionReference {
    pub name: FunctionName,
    pub args: Vec<CqlType>,
}

impl Parse for FunctionReference {
    type Output = Self;
    fn parse(s: &mut StatementStream<'_>) -> anyhow::Result<Self::Output> {
        let name = s.parse()?;
        let args = s.parse_from::<Parens<List<CqlType, Comma>>>()?;
        Ok(Self { name, args })
    }
}

#[derive(ParseFromStr, Clone, Debug)]
pub struct FunctionCall {
    pub name: FunctionName,
    pub args: Vec<Term>,
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

#[derive(ParseFromStr, Clone, Debug, TryInto, From)]
pub enum UserDefinedFunctionStatement {
    Create(CreateFunctionStatement),
    Drop(DropFunctionStatement),
    CreateAggregate(CreateAggregateFunctionStatement),
    DropAggregate(DropAggregateFunctionStatement),
}

impl Parse for UserDefinedFunctionStatement {
    type Output = Self;
    fn parse(s: &mut StatementStream<'_>) -> anyhow::Result<Self::Output> {
        Ok(if let Some(stmt) = s.parse_if::<CreateFunctionStatement>() {
            Self::Create(stmt?)
        } else if let Some(stmt) = s.parse_if::<DropFunctionStatement>() {
            Self::Drop(stmt?)
        } else if let Some(stmt) = s.parse_if::<CreateAggregateFunctionStatement>() {
            Self::CreateAggregate(stmt?)
        } else if let Some(stmt) = s.parse_if::<DropAggregateFunctionStatement>() {
            Self::DropAggregate(stmt?)
        } else {
            anyhow::bail!("Expected a data manipulation statement!")
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

#[derive(ParseFromStr, Builder, Clone, Debug)]
pub struct CreateFunctionStatement {
    #[builder(default)]
    pub or_replace: bool,
    #[builder(default)]
    pub if_not_exists: bool,
    pub func: FunctionDeclaration,
    pub on_null_input: OnNullInput,
    pub return_type: CqlType,
    pub language: Name,
    pub body: String,
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
            .return_type(s.parse()?)
            .language(s.parse()?)
            .body(s.parse()?);
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

#[derive(ParseFromStr, Clone, Debug)]
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
            anyhow::bail!("Invalid ON NULL INPUT declaration: {}", s.parse_from::<Token>()?)
        })
    }
}

#[derive(ParseFromStr, Clone, Debug)]
pub struct ArgumentDeclaration {
    pub ident: Name,
    pub cql_type: CqlType,
}

impl Parse for ArgumentDeclaration {
    type Output = Self;
    fn parse(s: &mut StatementStream<'_>) -> anyhow::Result<Self::Output> {
        let (ident, cql_type) = s.parse::<(Name, CqlType)>()?;
        Ok(Self { ident, cql_type })
    }
}

#[derive(ParseFromStr, Builder, Clone, Debug)]
pub struct DropFunctionStatement {
    #[builder(default)]
    pub if_exists: bool,
    pub func: FunctionReference,
}

impl Parse for DropFunctionStatement {
    type Output = Self;
    fn parse(s: &mut StatementStream<'_>) -> anyhow::Result<Self::Output> {
        s.parse::<(DROP, FUNCTION)>()?;
        let mut res = DropFunctionStatementBuilder::default();
        res.if_exists(s.parse::<Option<(IF, EXISTS)>>()?.is_some())
            .func(s.parse()?);
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

#[derive(ParseFromStr, Builder, Clone, Debug)]
pub struct CreateAggregateFunctionStatement {
    #[builder(default)]
    pub or_replace: bool,
    #[builder(default)]
    pub if_not_exists: bool,
    pub func: FunctionDeclaration,
    pub state_modifying_fn: FunctionName,
    pub state_value_type: CqlType,
    #[builder(default)]
    pub final_fn: Option<FunctionName>,
    #[builder(default)]
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
            .state_value_type(s.parse::<(STYPE, _)>()?.1)
            .final_fn(s.parse::<Option<(FINALFUNC, _)>>()?.map(|i| i.1))
            .init_condition(s.parse::<Option<(INITCOND, _)>>()?.map(|i| i.1));
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

#[derive(ParseFromStr, Builder, Clone, Debug)]
pub struct DropAggregateFunctionStatement {
    #[builder(default)]
    pub if_exists: bool,
    pub func: FunctionReference,
}

impl Parse for DropAggregateFunctionStatement {
    type Output = Self;
    fn parse(s: &mut StatementStream<'_>) -> anyhow::Result<Self::Output> {
        s.parse::<(DROP, AGGREGATE)>()?;
        let mut res = DropAggregateFunctionStatementBuilder::default();
        res.if_exists(s.parse::<Option<(IF, EXISTS)>>()?.is_some())
            .func(s.parse()?);
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

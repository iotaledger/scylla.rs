use crate::parser::{
    CqlType,
    Identifier,
    Name,
    Term,
};

#[derive(Clone, Debug)]
pub struct FunctionName {
    pub keyspace: Option<Name>,
    pub name: Name,
}

#[derive(Clone, Debug)]
pub struct FunctionDeclaration {
    pub name: FunctionName,
    pub args: Vec<ArgumentDeclaration>,
}

#[derive(Clone, Debug)]
pub struct FunctionReference {
    pub name: FunctionName,
    pub args: Vec<CqlType>,
}

#[derive(Clone, Debug)]
pub struct FunctionCall {
    pub name: FunctionName,
    pub args: Vec<Term>,
}

#[derive(Clone, Debug)]
pub enum UserDefinedFunctionStatement {
    Create(CreateFunctionStatement),
    Drop(DropFunctionStatement),
    CreateAggregate(CreateAggregateFunctionStatement),
    DropAggregate(DropAggregateFunctionStatement),
}

#[derive(Clone, Debug)]
pub struct CreateFunctionStatement {
    pub or_replace: bool,
    pub if_not_exists: bool,
    pub func: FunctionDeclaration,
    pub on_null_input: OnNullInput,
    pub return_type: CqlType,
    pub language: Identifier,
    pub body: String,
}

#[derive(Clone, Debug)]
pub enum OnNullInput {
    Called,
    ReturnsNull,
}

#[derive(Clone, Debug)]
pub struct ArgumentDeclaration {
    pub ident: Identifier,
    pub cql_type: CqlType,
}

#[derive(Clone, Debug)]
pub struct DropFunctionStatement {
    pub if_exists: bool,
    pub func: FunctionReference,
}

#[derive(Clone, Debug)]
pub struct CreateAggregateFunctionStatement {
    pub or_replace: bool,
    pub if_not_exists: bool,
    pub func: FunctionDeclaration,
    pub state_modifying_fn: FunctionName,
    pub state_value_type: CqlType,
    pub final_fn: Option<FunctionName>,
    pub init_condition: Option<Term>,
}

#[derive(Clone, Debug)]
pub struct DropAggregateFunctionStatement {
    pub if_exists: bool,
    pub func: FunctionReference,
}
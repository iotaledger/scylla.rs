// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use super::{
    format_cql_f32,
    format_cql_f64,
    keywords::*,
    Alpha,
    Angles,
    BindMarker,
    Braces,
    Brackets,
    CustomToTokens,
    Float,
    FunctionCall,
    Hex,
    KeyspaceQualifiedName,
    List,
    LitStr,
    Name,
    Parens,
    Parse,
    SignedNumber,
    StatementStream,
    Tag,
    TokenWrapper,
};
use chrono::{
    Datelike,
    NaiveDate,
    NaiveDateTime,
    NaiveTime,
    Timelike,
};
use derive_builder::Builder;
use derive_more::{
    From,
    TryInto,
};
use scylla_parse_macros::{
    ParseFromStr,
    ToTokens,
};
use std::{
    collections::{
        BTreeMap,
        BTreeSet,
        HashMap,
        HashSet,
    },
    convert::{
        TryFrom,
        TryInto,
    },
    fmt::{
        Display,
        Formatter,
    },
    str::FromStr,
};
use uuid::Uuid;

#[derive(ParseFromStr, Copy, Clone, Debug, PartialEq, Eq, Hash, Ord, PartialOrd, ToTokens)]
pub enum ArithmeticOp {
    Add,
    Sub,
    Mul,
    Div,
    Mod,
}

impl Display for ArithmeticOp {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}",
            match self {
                ArithmeticOp::Add => "+",
                ArithmeticOp::Sub => "-",
                ArithmeticOp::Mul => "*",
                ArithmeticOp::Div => "/",
                ArithmeticOp::Mod => "%",
            }
        )
    }
}

impl TryFrom<char> for ArithmeticOp {
    type Error = anyhow::Error;

    fn try_from(value: char) -> Result<Self, Self::Error> {
        match value {
            '+' => Ok(ArithmeticOp::Add),
            '-' => Ok(ArithmeticOp::Sub),
            '*' => Ok(ArithmeticOp::Mul),
            '/' => Ok(ArithmeticOp::Div),
            '%' => Ok(ArithmeticOp::Mod),
            _ => anyhow::bail!("Invalid arithmetic operator: {}", value),
        }
    }
}

impl Parse for ArithmeticOp {
    type Output = Self;
    fn parse(s: &mut StatementStream<'_>) -> anyhow::Result<Self::Output> {
        s.parse::<char>()?.try_into()
    }
}

#[derive(ParseFromStr, Copy, Clone, Debug, ToTokens, PartialEq, Eq)]
pub enum Operator {
    Equal,
    NotEqual,
    GreaterThan,
    GreaterThanOrEqual,
    LessThan,
    LessThanOrEqual,
    In,
    Contains,
    ContainsKey,
    Like,
}

impl Display for Operator {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}",
            match self {
                Operator::Equal => "=",
                Operator::NotEqual => "!=",
                Operator::GreaterThan => ">",
                Operator::GreaterThanOrEqual => ">=",
                Operator::LessThan => "<",
                Operator::LessThanOrEqual => "<=",
                Operator::In => "IN",
                Operator::Contains => "CONTAINS",
                Operator::ContainsKey => "CONTAINS KEY",
                Operator::Like => "LIKE",
            }
        )
    }
}

impl Parse for Operator {
    type Output = Self;
    fn parse(s: &mut StatementStream<'_>) -> anyhow::Result<Self> {
        if s.parse::<Option<(CONTAINS, KEY)>>()?.is_some() {
            Ok(Operator::ContainsKey)
        } else if s.parse::<Option<CONTAINS>>()?.is_some() {
            Ok(Operator::Contains)
        } else if s.parse::<Option<IN>>()?.is_some() {
            Ok(Operator::In)
        } else if s.parse::<Option<LIKE>>()?.is_some() {
            Ok(Operator::Like)
        } else if let (Some(first), second) = (s.next(), s.peek()) {
            Ok(match (first, second) {
                ('=', _) => Operator::Equal,
                ('!', Some('=')) => {
                    s.next();
                    Operator::NotEqual
                }
                ('>', Some('=')) => {
                    s.next();
                    Operator::GreaterThanOrEqual
                }
                ('<', Some('=')) => {
                    s.next();
                    Operator::LessThanOrEqual
                }
                ('>', _) => Operator::GreaterThan,
                ('<', _) => Operator::LessThan,
                _ => anyhow::bail!(
                    "Invalid operator: {}",
                    if let Some(second) = second {
                        format!("{}{}", first, second)
                    } else {
                        first.to_string()
                    }
                ),
            })
        } else {
            anyhow::bail!("Expected operator, found {}", s.info())
        }
    }
}

#[derive(Copy, Clone, Debug)]
pub enum TimeUnit {
    Nanos,
    Micros,
    Millis,
    Seconds,
    Minutes,
    Hours,
    Days,
    Weeks,
    Months,
    Years,
}

impl FromStr for TimeUnit {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "ns" => Ok(TimeUnit::Nanos),
            "us" | "µs" => Ok(TimeUnit::Micros),
            "ms" => Ok(TimeUnit::Millis),
            "s" => Ok(TimeUnit::Seconds),
            "m" => Ok(TimeUnit::Minutes),
            "h" => Ok(TimeUnit::Hours),
            "d" => Ok(TimeUnit::Days),
            "w" => Ok(TimeUnit::Weeks),
            "mo" => Ok(TimeUnit::Months),
            "y" => Ok(TimeUnit::Years),
            _ => anyhow::bail!("Invalid time unit: {}", s),
        }
    }
}

impl Display for TimeUnit {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}",
            match self {
                TimeUnit::Nanos => "ns",
                TimeUnit::Micros => "us",
                TimeUnit::Millis => "ms",
                TimeUnit::Seconds => "s",
                TimeUnit::Minutes => "m",
                TimeUnit::Hours => "h",
                TimeUnit::Days => "d",
                TimeUnit::Weeks => "w",
                TimeUnit::Months => "mo",
                TimeUnit::Years => "y",
            }
        )
    }
}

impl Parse for TimeUnit {
    type Output = Self;
    fn parse(s: &mut StatementStream<'_>) -> anyhow::Result<Self::Output> {
        if let (Some(first), second) = (s.next(), s.peek()) {
            Ok(match (first, second) {
                ('n', Some('s')) => {
                    s.next();
                    TimeUnit::Nanos
                }
                ('u', Some('s')) | ('µ', Some('s')) => {
                    s.next();
                    TimeUnit::Micros
                }
                ('m', Some('s')) => {
                    s.next();
                    TimeUnit::Millis
                }
                ('m', Some('o')) => {
                    s.next();
                    TimeUnit::Months
                }
                ('s', _) => TimeUnit::Seconds,
                ('m', _) => TimeUnit::Minutes,
                ('h', _) => TimeUnit::Hours,
                ('d', _) => TimeUnit::Days,
                ('w', _) => TimeUnit::Weeks,
                ('y', _) => TimeUnit::Years,
                _ => anyhow::bail!(
                    "Invalid time unit: {}",
                    if let Some(second) = second {
                        format!("{}{}", first, second)
                    } else {
                        first.to_string()
                    }
                ),
            })
        } else {
            anyhow::bail!("Expected time unit, found {}", s.info())
        }
    }
}

#[derive(ParseFromStr, Clone, Debug, PartialEq, Eq, Hash, Ord, PartialOrd, ToTokens, From)]
pub enum Term {
    Constant(Constant),
    Literal(Literal),
    FunctionCall(FunctionCall),
    #[from(ignore)]
    ArithmeticOp {
        lhs: Option<Box<Term>>,
        op: ArithmeticOp,
        rhs: Box<Term>,
    },
    #[from(ignore)]
    TypeHint {
        hint: CqlType,
        ident: Name,
    },
    BindMarker(BindMarker),
}

impl Term {
    pub fn constant<T: Into<Constant>>(value: T) -> Self {
        Term::Constant(value.into())
    }

    pub fn literal<T: Into<Literal>>(value: T) -> Self {
        Term::Literal(value.into())
    }

    pub fn function_call<T: Into<FunctionCall>>(value: T) -> Self {
        Term::FunctionCall(value.into())
    }

    pub fn negative<T: Into<Term>>(t: T) -> Self {
        Term::ArithmeticOp {
            lhs: None,
            op: ArithmeticOp::Sub,
            rhs: Box::new(t.into()),
        }
    }

    pub fn arithmetic_op<LT: Into<Term>, RT: Into<Term>>(lhs: LT, op: ArithmeticOp, rhs: RT) -> Self {
        Term::ArithmeticOp {
            lhs: Some(Box::new(lhs.into())),
            op,
            rhs: Box::new(rhs.into()),
        }
    }

    pub fn type_hint<T: Into<CqlType>, N: Into<Name>>(hint: T, ident: N) -> Self {
        Term::TypeHint {
            hint: hint.into(),
            ident: ident.into(),
        }
    }

    pub fn bind_marker<T: Into<BindMarker>>(marker: T) -> Self {
        Term::BindMarker(marker.into())
    }
}

impl Parse for Term {
    type Output = Self;
    fn parse(s: &mut StatementStream<'_>) -> anyhow::Result<Self> {
        Ok(if let Some(c) = s.parse()? {
            if let Some((op, rhs)) = s.parse::<Option<(ArithmeticOp, Term)>>()? {
                Self::ArithmeticOp {
                    lhs: Some(Box::new(Self::Constant(c))),
                    op,
                    rhs: Box::new(rhs),
                }
            } else {
                Self::Constant(c)
            }
        } else if let Some(lit) = s.parse()? {
            if let Some((op, rhs)) = s.parse::<Option<(ArithmeticOp, Term)>>()? {
                Self::ArithmeticOp {
                    lhs: Some(Box::new(Self::Literal(lit))),
                    op,
                    rhs: Box::new(rhs),
                }
            } else {
                Self::Literal(lit)
            }
        } else if let Some(f) = s.parse()? {
            if let Some((op, rhs)) = s.parse::<Option<(ArithmeticOp, Term)>>()? {
                Self::ArithmeticOp {
                    lhs: Some(Box::new(Self::FunctionCall(f))),
                    op,
                    rhs: Box::new(rhs),
                }
            } else {
                Self::FunctionCall(f)
            }
        } else if let Some((hint, ident)) = s.parse()? {
            if let Some((op, rhs)) = s.parse::<Option<(ArithmeticOp, Term)>>()? {
                Self::ArithmeticOp {
                    lhs: Some(Box::new(Self::TypeHint { hint, ident })),
                    op,
                    rhs: Box::new(rhs),
                }
            } else {
                Self::TypeHint { hint, ident }
            }
        } else if let Some(b) = s.parse()? {
            if let Some((op, rhs)) = s.parse::<Option<(ArithmeticOp, Term)>>()? {
                Self::ArithmeticOp {
                    lhs: Some(Box::new(Self::BindMarker(b))),
                    op,
                    rhs: Box::new(rhs),
                }
            } else {
                Self::BindMarker(b)
            }
        } else if let Some((op, rhs)) = s.parse::<Option<(ArithmeticOp, Term)>>()? {
            Self::ArithmeticOp {
                lhs: None,
                op,
                rhs: Box::new(rhs),
            }
        } else {
            anyhow::bail!("Expected term, found {}", s.info())
        })
    }
}

impl Display for Term {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Constant(c) => c.fmt(f),
            Self::Literal(l) => l.fmt(f),
            Self::FunctionCall(fc) => fc.fmt(f),
            Self::ArithmeticOp { lhs, op, rhs } => match lhs {
                Some(lhs) => write!(f, "{}{}{}", lhs, op, rhs),
                None => write!(f, "{}{}", op, rhs),
            },
            Self::TypeHint { hint, ident } => write!(f, "{} {}", hint, ident),
            Self::BindMarker(b) => b.fmt(f),
        }
    }
}

impl TryInto<LitStr> for Term {
    type Error = anyhow::Error;

    fn try_into(self) -> anyhow::Result<LitStr> {
        if let Self::Constant(c) = self {
            c.try_into()
        } else {
            Err(anyhow::anyhow!("Expected constant, found {}", self))
        }
    }
}

impl TryInto<i32> for Term {
    type Error = anyhow::Error;

    fn try_into(self) -> anyhow::Result<i32> {
        if let Self::Constant(c) = self {
            c.try_into()
        } else {
            Err(anyhow::anyhow!("Expected constant, found {}", self))
        }
    }
}

impl TryInto<i64> for Term {
    type Error = anyhow::Error;

    fn try_into(self) -> anyhow::Result<i64> {
        if let Self::Constant(c) = self {
            c.try_into()
        } else {
            Err(anyhow::anyhow!("Expected constant, found {}", self))
        }
    }
}

impl TryInto<f32> for Term {
    type Error = anyhow::Error;

    fn try_into(self) -> anyhow::Result<f32> {
        if let Self::Constant(c) = self {
            c.try_into()
        } else {
            Err(anyhow::anyhow!("Expected constant, found {}", self))
        }
    }
}

impl TryInto<f64> for Term {
    type Error = anyhow::Error;

    fn try_into(self) -> anyhow::Result<f64> {
        if let Self::Constant(c) = self {
            c.try_into()
        } else {
            Err(anyhow::anyhow!("Expected constant, found {}", self))
        }
    }
}

impl TryInto<bool> for Term {
    type Error = anyhow::Error;

    fn try_into(self) -> anyhow::Result<bool> {
        if let Self::Constant(c) = self {
            c.try_into()
        } else {
            Err(anyhow::anyhow!("Expected constant, found {}", self))
        }
    }
}

impl TryInto<Uuid> for Term {
    type Error = anyhow::Error;

    fn try_into(self) -> anyhow::Result<Uuid> {
        if let Self::Constant(c) = self {
            c.try_into()
        } else {
            Err(anyhow::anyhow!("Expected constant, found {}", self))
        }
    }
}

macro_rules! impl_from_constant_to_term {
    ($t:ty) => {
        impl From<$t> for Term {
            fn from(t: $t) -> Self {
                Self::Constant(t.into())
            }
        }
    };
}

impl_from_constant_to_term!(LitStr);
impl_from_constant_to_term!(i32);
impl_from_constant_to_term!(i64);
impl_from_constant_to_term!(f32);
impl_from_constant_to_term!(f64);
impl_from_constant_to_term!(bool);
impl_from_constant_to_term!(Uuid);

#[derive(ParseFromStr, Clone, Debug, PartialEq, Eq, Hash, Ord, PartialOrd, ToTokens)]
pub enum Constant {
    Null,
    String(LitStr),
    Integer(String),
    Float(String),
    Boolean(bool),
    Uuid(#[wrap] Uuid),
    Hex(Vec<u8>),
    Blob(Vec<u8>),
}

impl Constant {
    pub fn string(s: &str) -> Self {
        Self::String(s.into())
    }

    pub fn integer(s: &str) -> anyhow::Result<Self> {
        Ok(Self::Integer(StatementStream::new(s).parse_from::<SignedNumber>()?))
    }

    pub fn float(s: &str) -> anyhow::Result<Self> {
        Ok(Self::Float(StatementStream::new(s).parse_from::<Float>()?))
    }

    pub fn bool(b: bool) -> Self {
        Self::Boolean(b)
    }

    pub fn uuid(u: Uuid) -> Self {
        Self::Uuid(u)
    }

    pub fn hex(s: &str) -> anyhow::Result<Self> {
        Ok(Self::Hex(StatementStream::new(s).parse_from::<Hex>()?))
    }

    pub fn blob(b: Vec<u8>) -> Self {
        Self::Blob(b)
    }
}

impl Parse for Constant {
    type Output = Self;
    fn parse(s: &mut StatementStream<'_>) -> anyhow::Result<Self::Output> {
        Ok(if s.parse::<Option<NULL>>()?.is_some() {
            Constant::Null
        } else if let Some(ss) = s.parse()? {
            Constant::String(ss)
        } else if let Some(f) = s.parse_from::<Option<Float>>()? {
            Constant::Float(f)
        } else if let Some(i) = s.parse_from::<Option<SignedNumber>>()? {
            Constant::Integer(i)
        } else if let Some(b) = s.parse()? {
            Constant::Boolean(b)
        } else if let Some(u) = s.parse()? {
            Constant::Uuid(u)
        } else if s.peekn(2).map(|s| s.to_lowercase().as_str() == "0x").unwrap_or(false) {
            s.nextn(2);
            Constant::Blob(s.parse_from::<Hex>()?)
        } else if let Some(h) = s.parse_from::<Option<Hex>>()? {
            Constant::Hex(h)
        } else {
            anyhow::bail!("Expected constant, found {}", s.info())
        })
    }
}

impl Display for Constant {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Null => write!(f, "NULL"),
            Self::String(s) => s.fmt(f),
            Self::Integer(s) => s.fmt(f),
            Self::Float(s) => s.fmt(f),
            Self::Boolean(b) => b.to_string().to_uppercase().fmt(f),
            Self::Uuid(u) => u.fmt(f),
            Self::Hex(h) => hex::encode(h).fmt(f),
            Self::Blob(b) => write!(f, "0x{}", hex::encode(b)),
        }
    }
}

impl TryInto<LitStr> for Constant {
    type Error = anyhow::Error;

    fn try_into(self) -> anyhow::Result<LitStr> {
        if let Self::String(s) = self {
            Ok(s)
        } else {
            Err(anyhow::anyhow!("Expected string constant, found {}", self))
        }
    }
}

impl TryInto<i32> for Constant {
    type Error = anyhow::Error;

    fn try_into(self) -> anyhow::Result<i32> {
        if let Self::Integer(i) = self {
            Ok(i.parse()?)
        } else {
            Err(anyhow::anyhow!("Expected integer constant, found {}", self))
        }
    }
}

impl TryInto<i64> for Constant {
    type Error = anyhow::Error;

    fn try_into(self) -> anyhow::Result<i64> {
        if let Self::Integer(i) = self {
            Ok(i.parse()?)
        } else {
            Err(anyhow::anyhow!("Expected integer constant, found {}", self))
        }
    }
}

impl TryInto<f32> for Constant {
    type Error = anyhow::Error;

    fn try_into(self) -> anyhow::Result<f32> {
        if let Self::Float(f) = self {
            Ok(f.parse()?)
        } else {
            Err(anyhow::anyhow!("Expected float constant, found {}", self))
        }
    }
}

impl TryInto<f64> for Constant {
    type Error = anyhow::Error;

    fn try_into(self) -> anyhow::Result<f64> {
        if let Self::Float(f) = self {
            Ok(f.parse()?)
        } else {
            Err(anyhow::anyhow!("Expected float constant, found {}", self))
        }
    }
}

impl TryInto<bool> for Constant {
    type Error = anyhow::Error;

    fn try_into(self) -> anyhow::Result<bool> {
        if let Self::Boolean(b) = self {
            Ok(b)
        } else {
            Err(anyhow::anyhow!("Expected boolean constant, found {}", self))
        }
    }
}

impl TryInto<Uuid> for Constant {
    type Error = anyhow::Error;

    fn try_into(self) -> anyhow::Result<Uuid> {
        if let Self::Uuid(u) = self {
            Ok(u)
        } else {
            Err(anyhow::anyhow!("Expected UUID constant, found {}", self))
        }
    }
}

impl From<LitStr> for Constant {
    fn from(s: LitStr) -> Self {
        Self::String(s)
    }
}

impl From<i32> for Constant {
    fn from(i: i32) -> Self {
        Self::Integer(i.to_string())
    }
}

impl From<i64> for Constant {
    fn from(i: i64) -> Self {
        Self::Integer(i.to_string())
    }
}

impl From<f32> for Constant {
    fn from(f: f32) -> Self {
        Self::Float(format_cql_f32(f))
    }
}

impl From<f64> for Constant {
    fn from(f: f64) -> Self {
        Self::Float(format_cql_f64(f))
    }
}

impl From<bool> for Constant {
    fn from(b: bool) -> Self {
        Self::Boolean(b)
    }
}

impl From<Uuid> for Constant {
    fn from(u: Uuid) -> Self {
        Self::Uuid(u)
    }
}

#[derive(ParseFromStr, Clone, Debug, TryInto, From, PartialEq, Eq, Hash, Ord, PartialOrd, ToTokens)]
pub enum Literal {
    Collection(CollectionTypeLiteral),
    UserDefined(UserDefinedTypeLiteral),
    Tuple(TupleLiteral),
}

impl Parse for Literal {
    type Output = Self;
    fn parse(s: &mut StatementStream<'_>) -> anyhow::Result<Self::Output> {
        Ok(if let Some(c) = s.parse()? {
            Self::Collection(c)
        } else if let Some(u) = s.parse()? {
            Self::UserDefined(u)
        } else if let Some(t) = s.parse()? {
            Self::Tuple(t)
        } else {
            anyhow::bail!("Expected CQL literal, found {}", s.info())
        })
    }
}

impl Display for Literal {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Collection(c) => c.fmt(f),
            Self::UserDefined(u) => u.fmt(f),
            Self::Tuple(t) => t.fmt(f),
        }
    }
}

#[derive(ParseFromStr, Clone, Debug, PartialEq, Eq, Hash, Ord, PartialOrd, From, ToTokens)]
pub enum CqlType {
    Native(NativeType),
    #[from(ignore)]
    Collection(Box<CollectionType>),
    UserDefined(UserDefinedType),
    Tuple(Vec<CqlType>),
    Custom(LitStr),
}

impl Parse for CqlType {
    type Output = Self;
    fn parse(s: &mut StatementStream<'_>) -> anyhow::Result<Self::Output>
    where
        Self: Sized,
    {
        Ok(if let Some(c) = s.parse()? {
            Self::Collection(Box::new(c))
        } else if s.parse::<Option<TUPLE>>()?.is_some() {
            Self::Tuple(s.parse_from::<Angles<List<CqlType, Comma>>>()?)
        } else if let Some(n) = s.parse()? {
            Self::Native(n)
        } else if let Some(udt) = s.parse()? {
            Self::UserDefined(udt)
        } else if let Some(c) = s.parse()? {
            Self::Custom(c)
        } else {
            anyhow::bail!("Expected CQL Type, found {}", s.info())
        })
    }
}

impl Display for CqlType {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Native(n) => n.fmt(f),
            Self::Collection(c) => c.fmt(f),
            Self::UserDefined(u) => u.fmt(f),
            Self::Tuple(t) => write!(
                f,
                "TUPLE<{}>",
                t.iter().map(|t| t.to_string()).collect::<Vec<_>>().join(", ")
            ),
            Self::Custom(c) => c.fmt(f),
        }
    }
}

impl From<CollectionType> for CqlType {
    fn from(c: CollectionType) -> Self {
        Self::Collection(Box::new(c))
    }
}

#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash, Ord, PartialOrd, ToTokens)]
pub enum NativeType {
    Ascii,
    Bigint,
    Blob,
    Boolean,
    Counter,
    Date,
    Decimal,
    Double,
    Duration,
    Float,
    Inet,
    Int,
    Smallint,
    Text,
    Time,
    Timestamp,
    Timeuuid,
    Tinyint,
    Uuid,
    Varchar,
    Varint,
}

impl Display for NativeType {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}",
            match self {
                NativeType::Ascii => "ASCII",
                NativeType::Bigint => "BIGINT",
                NativeType::Blob => "BLOB",
                NativeType::Boolean => "BOOLEAN",
                NativeType::Counter => "COUNTER",
                NativeType::Date => "DATE",
                NativeType::Decimal => "DECIMAL",
                NativeType::Double => "DOUBLE",
                NativeType::Duration => "DURATION",
                NativeType::Float => "FLOAT",
                NativeType::Inet => "INET",
                NativeType::Int => "INT",
                NativeType::Smallint => "SMALLINT",
                NativeType::Text => "TEXT",
                NativeType::Time => "TIME",
                NativeType::Timestamp => "TIMESTAMP",
                NativeType::Timeuuid => "TIMEUUID",
                NativeType::Tinyint => "TINYINT",
                NativeType::Uuid => "UUID",
                NativeType::Varchar => "VARCHAR",
                NativeType::Varint => "VARINT",
            }
        )
    }
}

impl FromStr for NativeType {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(match s.to_uppercase().as_str() {
            "ASCII" => NativeType::Ascii,
            "BIGINT" => NativeType::Bigint,
            "BLOB" => NativeType::Blob,
            "BOOLEAN" => NativeType::Boolean,
            "COUNTER" => NativeType::Counter,
            "DATE" => NativeType::Date,
            "DECIMAL" => NativeType::Decimal,
            "DOUBLE" => NativeType::Double,
            "DURATION" => NativeType::Duration,
            "FLOAT" => NativeType::Float,
            "INET" => NativeType::Inet,
            "INT" => NativeType::Int,
            "SMALLINT" => NativeType::Smallint,
            "TEXT" => NativeType::Text,
            "TIME" => NativeType::Time,
            "TIMESTAMP" => NativeType::Timestamp,
            "TIMEUUID" => NativeType::Timeuuid,
            "TINYINT" => NativeType::Tinyint,
            "UUID" => NativeType::Uuid,
            "VARCHAR" => NativeType::Varchar,
            "VARINT" => NativeType::Varint,
            _ => anyhow::bail!("Invalid native type: {}", s),
        })
    }
}

impl Parse for NativeType {
    type Output = Self;
    fn parse(s: &mut StatementStream<'_>) -> anyhow::Result<Self::Output>
    where
        Self: Sized,
    {
        let token = s.parse_from::<Alpha>()?;
        NativeType::from_str(&token)
    }
}

#[derive(ParseFromStr, Clone, Debug, PartialEq, Eq, Hash, Ord, PartialOrd, ToTokens)]
pub enum CollectionTypeLiteral {
    List(ListLiteral),
    Set(SetLiteral),
    Map(MapLiteral),
}

impl Parse for CollectionTypeLiteral {
    type Output = Self;
    fn parse(s: &mut StatementStream<'_>) -> anyhow::Result<Self::Output> {
        Ok(if let Some(l) = s.parse()? {
            Self::List(l)
        } else if let Some(s) = s.parse()? {
            Self::Set(s)
        } else if let Some(m) = s.parse()? {
            Self::Map(m)
        } else {
            anyhow::bail!("Expected collection literal, found {}", s.info())
        })
    }
}

impl Display for CollectionTypeLiteral {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::List(l) => l.fmt(f),
            Self::Set(s) => s.fmt(f),
            Self::Map(m) => m.fmt(f),
        }
    }
}

#[derive(ParseFromStr, Clone, Debug, PartialEq, Eq, Hash, Ord, PartialOrd, ToTokens)]
pub enum CollectionType {
    List(CqlType),
    Set(CqlType),
    Map(CqlType, CqlType),
}

impl CollectionType {
    pub fn list<T: Into<CqlType>>(t: T) -> Self {
        Self::List(t.into())
    }

    pub fn set<T: Into<CqlType>>(t: T) -> Self {
        Self::Set(t.into())
    }

    pub fn map<K: Into<CqlType>, V: Into<CqlType>>(k: K, v: V) -> Self {
        Self::Map(k.into(), v.into())
    }
}

impl Parse for CollectionType {
    type Output = Self;
    fn parse(s: &mut StatementStream<'_>) -> anyhow::Result<Self::Output>
    where
        Self: Sized,
    {
        Ok(if s.parse::<Option<MAP>>()?.is_some() {
            let (t1, _, t2) = s.parse_from::<Angles<(CqlType, Comma, CqlType)>>()?;
            Self::Map(t1, t2)
        } else if s.parse::<Option<SET>>()?.is_some() {
            Self::Set(s.parse_from::<Angles<CqlType>>()?)
        } else if s.parse::<Option<LIST>>()?.is_some() {
            Self::List(s.parse_from::<Angles<CqlType>>()?)
        } else {
            anyhow::bail!("Expected collection type, found {}", s.info())
        })
    }
}

impl Display for CollectionType {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::List(e) => write!(f, "LIST<{}>", e),
            Self::Set(e) => write!(f, "SET<{}>", e),
            Self::Map(k, v) => write!(f, "MAP<{}, {}>", k, v),
        }
    }
}

#[derive(ParseFromStr, Clone, Debug, PartialEq, Eq, Hash, Ord, PartialOrd, ToTokens)]
#[parse_via(TaggedMapLiteral)]
pub struct MapLiteral {
    pub elements: BTreeMap<Term, Term>,
}

impl TryFrom<TaggedMapLiteral> for MapLiteral {
    type Error = anyhow::Error;

    fn try_from(value: TaggedMapLiteral) -> Result<Self, Self::Error> {
        let mut elements = BTreeMap::new();
        for (k, v) in value.elements {
            if elements.insert(k.into_value()?, v.into_value()?).is_some() {
                anyhow::bail!("Duplicate key in map literal");
            }
        }
        Ok(Self { elements })
    }
}

#[derive(ParseFromStr, Clone, Debug, PartialEq, Eq, Hash, Ord, PartialOrd, ToTokens)]
pub struct TaggedMapLiteral {
    pub elements: BTreeMap<Tag<Term>, Tag<Term>>,
}

impl Parse for TaggedMapLiteral {
    type Output = Self;
    fn parse(s: &mut StatementStream<'_>) -> anyhow::Result<Self::Output>
    where
        Self: Sized,
    {
        Ok(Self {
            elements: s
                .parse_from::<Braces<List<(Tag<Term>, Colon, Tag<Term>), Comma>>>()?
                .into_iter()
                .map(|(k, _, v)| (k, v))
                .collect(),
        })
    }
}

impl Display for MapLiteral {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{{{}}}",
            self.elements
                .iter()
                .map(|(k, v)| format!("{}: {}", k, v))
                .collect::<Vec<_>>()
                .join(", ")
        )
    }
}

impl Display for TaggedMapLiteral {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{{{}}}",
            self.elements
                .iter()
                .map(|(k, v)| format!("{}: {}", k, v))
                .collect::<Vec<_>>()
                .join(", ")
        )
    }
}

impl<T: Into<Term>> From<HashMap<T, T>> for MapLiteral {
    fn from(m: HashMap<T, T>) -> Self {
        Self {
            elements: m.into_iter().map(|(k, v)| (k.into(), v.into())).collect(),
        }
    }
}

impl<T: Into<Term>> From<BTreeMap<T, T>> for MapLiteral {
    fn from(m: BTreeMap<T, T>) -> Self {
        Self {
            elements: m.into_iter().map(|(k, v)| (k.into(), v.into())).collect(),
        }
    }
}

#[derive(ParseFromStr, Clone, Debug, PartialEq, Eq, Hash, Ord, PartialOrd, ToTokens)]
pub struct TupleLiteral {
    pub elements: Vec<Term>,
}

impl Parse for TupleLiteral {
    type Output = Self;
    fn parse(s: &mut StatementStream<'_>) -> anyhow::Result<Self::Output>
    where
        Self: Sized,
    {
        Ok(Self {
            elements: s.parse_from::<Parens<List<Term, Comma>>>()?,
        })
    }
}

impl Display for TupleLiteral {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "({})",
            self.elements
                .iter()
                .map(|t| t.to_string())
                .collect::<Vec<_>>()
                .join(", ")
        )
    }
}

impl<T: Into<Term>> From<Vec<T>> for TupleLiteral {
    fn from(elements: Vec<T>) -> Self {
        Self {
            elements: elements.into_iter().map(|t| t.into()).collect(),
        }
    }
}

#[derive(ParseFromStr, Clone, Debug, PartialEq, Eq, Hash, Ord, PartialOrd, ToTokens)]
pub struct SetLiteral {
    pub elements: BTreeSet<Term>,
}

impl Parse for SetLiteral {
    type Output = Self;
    fn parse(s: &mut StatementStream<'_>) -> anyhow::Result<Self::Output> {
        let v = s.parse_from::<Braces<List<Term, Comma>>>()?;
        let mut elements = BTreeSet::new();
        for e in v {
            if elements.contains(&e) {
                anyhow::bail!("Duplicate element in set: {}", e);
            }
            elements.insert(e);
        }
        Ok(Self { elements })
    }
}

impl Display for SetLiteral {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{{{}}}",
            self.elements
                .iter()
                .map(|t| t.to_string())
                .collect::<Vec<_>>()
                .join(", ")
        )
    }
}

impl<T: Into<Term>> From<HashSet<T>> for SetLiteral {
    fn from(elements: HashSet<T>) -> Self {
        Self {
            elements: elements.into_iter().map(|t| t.into()).collect(),
        }
    }
}

impl<T: Into<Term>> From<BTreeSet<T>> for SetLiteral {
    fn from(elements: BTreeSet<T>) -> Self {
        Self {
            elements: elements.into_iter().map(|t| t.into()).collect(),
        }
    }
}

#[derive(ParseFromStr, Clone, Debug, PartialEq, Eq, Hash, Ord, PartialOrd, ToTokens)]
pub struct ListLiteral {
    pub elements: Vec<Term>,
}

impl Parse for ListLiteral {
    type Output = Self;
    fn parse(s: &mut StatementStream<'_>) -> anyhow::Result<Self::Output> {
        Ok(Self {
            elements: s.parse_from::<Brackets<List<Term, Comma>>>()?,
        })
    }
}

impl Display for ListLiteral {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "[{}]",
            self.elements
                .iter()
                .map(|t| t.to_string())
                .collect::<Vec<_>>()
                .join(", ")
        )
    }
}

impl<T: Into<Term>> From<Vec<T>> for ListLiteral {
    fn from(elements: Vec<T>) -> Self {
        Self {
            elements: elements.into_iter().map(|t| t.into()).collect(),
        }
    }
}

#[derive(Clone, Debug, Default)]
pub struct TimestampLiteral(i64);
impl Parse for TimestampLiteral {
    type Output = Self;
    fn parse(s: &mut StatementStream<'_>) -> anyhow::Result<Self::Output> {
        if let Some(ts) = s.parse::<Option<LitStr>>()? {
            Ok(Self(
                ts.value
                    .parse::<NaiveDateTime>()
                    .map_err(|e| anyhow::anyhow!(e))?
                    .timestamp_millis(),
            ))
        } else {
            Ok(Self(s.parse::<u64>()? as i64))
        }
    }
}

#[derive(Clone, Debug, Default)]
pub struct DateLiteral(u32);
impl Parse for DateLiteral {
    type Output = Self;
    fn parse(s: &mut StatementStream<'_>) -> anyhow::Result<Self::Output> {
        if let Some(d) = s.parse::<Option<LitStr>>()? {
            let dur = d.value.parse::<NaiveDate>().map_err(|e| anyhow::anyhow!(e))? - NaiveDate::from_ymd(1970, 1, 1);
            Ok(Self(dur.num_days() as u32 + (1u32 << 31)))
        } else {
            Ok(Self(s.parse::<u32>()?))
        }
    }
}

#[derive(Clone, Debug, Default)]
pub struct TimeLiteral(i64);
impl Parse for TimeLiteral {
    type Output = Self;
    fn parse(s: &mut StatementStream<'_>) -> anyhow::Result<Self::Output> {
        if let Some(t) = s.parse::<Option<LitStr>>()? {
            let t = t.value.parse::<NaiveTime>().map_err(|e| anyhow::anyhow!(e))? - NaiveTime::from_hms(0, 0, 0);
            Ok(Self(
                t.num_nanoseconds()
                    .ok_or_else(|| anyhow::anyhow!("Invalid time literal!"))?,
            ))
        } else {
            Ok(Self(s.parse::<u64>()? as i64))
        }
    }
}

enum DurationLiteralKind {
    QuantityUnit,
    ISO8601,
}

#[derive(Builder, Clone, Debug, Default)]
#[builder(default, build_fn(validate = "Self::validate"))]
struct ISO8601 {
    years: i64,
    months: i64,
    days: i64,
    hours: i64,
    minutes: i64,
    seconds: i64,
    weeks: i64,
}

impl ISO8601Builder {
    fn validate(&self) -> Result<(), String> {
        if self.weeks.is_some()
            && (self.years.is_some()
                || self.months.is_some()
                || self.days.is_some()
                || self.hours.is_some()
                || self.minutes.is_some()
                || self.seconds.is_some())
        {
            return Err("ISO8601 duration cannot have weeks and other units".to_string());
        }
        Ok(())
    }
}

#[derive(ParseFromStr, Clone, Debug, Default, ToTokens, PartialEq, Eq)]
pub struct DurationLiteral {
    pub months: i32,
    pub days: i32,
    pub nanos: i64,
}

impl DurationLiteral {
    pub fn ns(mut self, ns: i64) -> Self {
        self.nanos += ns;
        self
    }

    pub fn us(self, us: i64) -> Self {
        self.ns(us * 1000)
    }

    pub fn ms(self, ms: i64) -> Self {
        self.us(ms * 1000)
    }

    pub fn s(self, s: i64) -> Self {
        self.ms(s * 1000)
    }

    pub fn m(self, m: i32) -> Self {
        self.s(m as i64 * 60)
    }

    pub fn h(self, h: i32) -> Self {
        self.m(h * 60)
    }

    pub fn d(mut self, d: i32) -> Self {
        self.days += d;
        self
    }

    pub fn w(self, w: i32) -> Self {
        self.d(w * 7)
    }

    pub fn mo(mut self, mo: i32) -> Self {
        self.months += mo;
        self
    }

    pub fn y(self, y: i32) -> Self {
        self.mo(y * 12)
    }
}

impl Parse for DurationLiteral {
    type Output = Self;
    fn parse(s: &mut StatementStream<'_>) -> anyhow::Result<Self::Output> {
        let kind = match s.peek() {
            Some(c) => match c {
                'P' => {
                    s.next();
                    DurationLiteralKind::ISO8601
                }
                _ => DurationLiteralKind::QuantityUnit,
            },
            None => anyhow::bail!("End of statement!"),
        };
        match kind {
            DurationLiteralKind::ISO8601 => {
                let mut iso = ISO8601Builder::default();
                let mut ty = 'Y';
                let mut num = None;
                let mut time = false;
                let mut res = None;
                let mut alternative = None;
                while let Some(c) = s.peek() {
                    if c == 'P' {
                        anyhow::bail!("Invalid ISO8601 duration literal: Too many date specifiers");
                    } else if c == 'T' {
                        if time {
                            anyhow::bail!("Invalid ISO8601 duration literal: Too many time specifiers");
                        }
                        match ty {
                            'Y' => {
                                ty = 'h';
                            }
                            'D' => {
                                let num = num
                                    .take()
                                    .ok_or_else(|| anyhow::anyhow!("Invalid ISO8601 duration: Missing days"))?;
                                if num < 1 || num > 31 {
                                    anyhow::bail!("Invalid ISO8601 duration: Day out of range");
                                }
                                iso.days(num);
                                ty = 'h';
                            }
                            _ => {
                                panic!("Duration `ty` variable got set improperly to {}. This is a bug!", ty);
                            }
                        }
                        s.next();
                        time = true;
                    } else if c == '-' {
                        match alternative {
                            Some(true) => (),
                            Some(false) => anyhow::bail!("Invalid ISO8601 duration literal: Invalid '-' character"),
                            None => alternative = Some(true),
                        }
                        if time {
                            anyhow::bail!("Invalid ISO8601 duration literal: Date separator outside of date");
                        }
                        match ty {
                            'Y' => {
                                iso.years(
                                    num.take()
                                        .ok_or_else(|| anyhow::anyhow!("Invalid ISO8601 duration: Missing years"))?,
                                );
                                ty = 'M';
                            }
                            'M' => {
                                let num = num
                                    .take()
                                    .ok_or_else(|| anyhow::anyhow!("Invalid ISO8601 duration: Missing months"))?;
                                if num < 1 || num > 12 {
                                    anyhow::bail!("Invalid ISO8601 duration: Month out of range");
                                }
                                iso.months(num);
                                ty = 'D';
                            }
                            _ => {
                                panic!("Duration `ty` variable got set improperly to {}. This is a bug!", ty);
                            }
                        }
                        s.next();
                    } else if c == ':' {
                        match alternative {
                            Some(true) => (),
                            Some(false) => anyhow::bail!("Invalid ISO8601 duration literal: Invalid '-' character"),
                            None => alternative = Some(true),
                        }
                        if !time {
                            anyhow::bail!("Invalid ISO8601 duration: Time separator outside of time");
                        }
                        match ty {
                            'h' => {
                                let num = num
                                    .take()
                                    .ok_or_else(|| anyhow::anyhow!("Invalid ISO8601 duration: Missing hours"))?;
                                if num > 24 {
                                    anyhow::bail!("Invalid ISO8601 duration: Hour out of range");
                                }
                                iso.hours(num);
                                ty = 'm';
                            }
                            'm' => {
                                let num = num
                                    .take()
                                    .ok_or_else(|| anyhow::anyhow!("Invalid ISO8601 duration: Missing minutes"))?;
                                if num > 59 {
                                    anyhow::bail!("Invalid ISO8601 duration: Minutes out of range");
                                }
                                iso.minutes(num);
                                ty = 's';
                            }
                            _ => {
                                panic!("Duration `ty` variable got set improperly to {}. This is a bug!", ty);
                            }
                        }
                        s.next();
                    } else if c.is_alphabetic() {
                        match alternative {
                            Some(false) => (),
                            Some(true) => anyhow::bail!("Invalid ISO8601 duration literal: Invalid unit specifier character in alternative format"),
                            None => alternative = Some(false),
                        }
                        match c {
                            'Y' => {
                                if iso.years.is_some() {
                                    anyhow::bail!("Invalid ISO8601 duration: Duplicate year specifiers");
                                }
                                iso.years(num.take().ok_or_else(|| {
                                    anyhow::anyhow!("Invalid ISO8601 duration: Missing number preceeding years unit")
                                })?);
                            }
                            'M' => {
                                if !time {
                                    if iso.months.is_some() {
                                        anyhow::bail!("Invalid ISO8601 duration: Duplicate month specifiers");
                                    }
                                    iso.months(num.take().ok_or_else(|| {
                                        anyhow::anyhow!(
                                            "Invalid ISO8601 duration: Missing number preceeding months unit"
                                        )
                                    })?);
                                } else {
                                    if iso.minutes.is_some() {
                                        anyhow::bail!("Invalid ISO8601 duration: Duplicate minute specifiers");
                                    }
                                    iso.minutes(num.take().ok_or_else(|| {
                                        anyhow::anyhow!(
                                            "Invalid ISO8601 duration: Missing number preceeding minutes unit"
                                        )
                                    })?);
                                }
                            }
                            'D' => {
                                if iso.days.is_some() {
                                    anyhow::bail!("Invalid ISO8601 duration: Duplicate day specifiers");
                                }
                                iso.days(num.take().ok_or_else(|| {
                                    anyhow::anyhow!("Invalid ISO8601 duration: Missing number preceeding days unit")
                                })?);
                            }
                            'H' => {
                                if iso.hours.is_some() {
                                    anyhow::bail!("Invalid ISO8601 duration: Duplicate hour specifiers");
                                }
                                iso.hours(num.take().ok_or_else(|| {
                                    anyhow::anyhow!("Invalid ISO8601 duration: Missing number preceeding hours unit")
                                })?);
                            }
                            'S' => {
                                if iso.seconds.is_some() {
                                    anyhow::bail!("Invalid ISO8601 duration: Duplicate second specifiers");
                                }
                                iso.seconds(num.take().ok_or_else(|| {
                                    anyhow::anyhow!("Invalid ISO8601 duration: Missing number preceeding seconds unit")
                                })?);
                            }
                            'W' => {
                                if iso.weeks.is_some() {
                                    anyhow::bail!("Invalid ISO8601 duration: Duplicate week specifiers");
                                }
                                iso.weeks(num.take().ok_or_else(|| {
                                    anyhow::anyhow!("Invalid ISO8601 duration: Missing number preceeding weeks unit")
                                })?);
                            }
                            _ => {
                                anyhow::bail!(
                                    "Invalid ISO8601 duration: Expected P, Y, M, W, D, T, H, M, or S, found {}",
                                    c
                                );
                            }
                        }
                        s.next();
                    } else if c.is_numeric() {
                        num = Some(s.parse::<u64>()? as i64);
                    } else {
                        break;
                    }
                }
                let alternative = alternative
                    .ok_or_else(|| anyhow::anyhow!("Invalid ISO8601 duration literal: Unable to determine format"))?;
                if let Some(num) = num {
                    if !alternative {
                        anyhow::bail!("Invalid ISO8601 duration: Trailing number");
                    }
                    if !time {
                        if ty != 'D' {
                            anyhow::bail!("Invalid ISO8601 duration: Trailing number");
                        }
                        if num < 1 || num > 31 {
                            anyhow::bail!("Invalid ISO8601 duration: Day out of range");
                        }
                        iso.days(num);
                    } else {
                        if ty != 's' {
                            anyhow::bail!("Invalid ISO8601 duration: Trailing number");
                        }
                        if num > 59 {
                            anyhow::bail!("Invalid ISO8601 duration: Seconds out of range");
                        }
                        iso.seconds(num);
                    }
                    if iso.years.is_none() && iso.months.is_none() && iso.days.is_none()
                        || iso.hours.is_none() && iso.minutes.is_none() && iso.seconds.is_none()
                    {
                        anyhow::bail!("Invalid ISO8601 duration: Missing required unit for alternative format");
                    }
                    res = Some(iso);
                } else if !alternative {
                    res = Some(iso);
                }
                if let Some(iso) = res {
                    Ok(iso.build().map_err(|e| anyhow::anyhow!(e))?.into())
                } else {
                    anyhow::bail!("End of statement!");
                }
            }
            DurationLiteralKind::QuantityUnit => {
                let mut res = DurationLiteral::default();
                let mut num = None;
                while let Some(c) = s.peek() {
                    if c.is_numeric() {
                        num = Some(s.parse_from::<u64>()? as i64);
                    } else if c.is_alphabetic() {
                        if let Some(num) = num.take() {
                            match s.parse::<TimeUnit>()? {
                                TimeUnit::Nanos => res.nanos += num,
                                TimeUnit::Micros => res.nanos += num * 1000,
                                TimeUnit::Millis => res.nanos += num * 1_000_000,
                                TimeUnit::Seconds => res.nanos += num * 1_000_000_000,
                                TimeUnit::Minutes => res.nanos += num * 60_000_000_000,
                                TimeUnit::Hours => res.nanos += num * 3_600_000_000_000,
                                TimeUnit::Days => res.days += num as i32,
                                TimeUnit::Weeks => res.days += num as i32 * 7,
                                TimeUnit::Months => res.months += num as i32,
                                TimeUnit::Years => res.months += num as i32 * 12,
                            }
                        } else {
                            anyhow::bail!("Invalid ISO8601 duration: Missing number preceeding unit specifier");
                        }
                    } else {
                        break;
                    }
                }
                Ok(res)
            }
        }
    }
}

impl Display for DurationLiteral {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        if self.months == 0 && self.days == 0 && self.nanos == 0 {
            write!(f, "0ns")
        } else {
            if self.months > 0 {
                write!(f, "{}mo", self.months)?;
            }
            if self.days > 0 {
                write!(f, "{}d", self.days)?;
            }
            if self.nanos > 0 {
                write!(f, "{}ns", self.nanos)?;
            }
            Ok(())
        }
    }
}

impl From<NaiveDateTime> for DurationLiteral {
    fn from(dt: NaiveDateTime) -> Self {
        let mut res = DurationLiteral::default();
        res.months = dt.year() * 12 + dt.month() as i32;
        res.days = dt.day() as i32;
        res.nanos = dt.hour() as i64 * 3_600_000_000_000
            + dt.minute() as i64 * 60_000_000_000
            + dt.second() as i64 * 1_000_000_000;
        res
    }
}

impl From<std::time::Duration> for DurationLiteral {
    fn from(d: std::time::Duration) -> Self {
        let mut res = DurationLiteral::default();
        let mut s = d.as_secs();
        res.months = (s / (60 * 60 * 24 * 30)) as i32;
        s %= 60 * 60 * 24 * 30;
        res.days = (s / (60 * 60 * 24)) as i32;
        s %= 60 * 60 * 24;
        res.nanos = (s * 1_000_000_000 + d.subsec_nanos() as u64) as i64;
        res
    }
}

impl From<ISO8601> for DurationLiteral {
    fn from(iso: ISO8601) -> Self {
        let mut res = DurationLiteral::default();
        res.months = (iso.years * 12 + iso.months) as i32;
        res.days = (iso.weeks * 7 + iso.days) as i32;
        res.nanos = iso.hours * 3_600_000_000_000 + iso.minutes * 60_000_000_000 + iso.seconds * 1_000_000_000;
        res
    }
}

#[derive(ParseFromStr, Clone, Debug, PartialEq, Eq, Hash, Ord, PartialOrd, ToTokens)]
pub struct UserDefinedTypeLiteral {
    pub fields: BTreeMap<Name, Term>,
}

impl Parse for UserDefinedTypeLiteral {
    type Output = Self;
    fn parse(s: &mut StatementStream<'_>) -> anyhow::Result<Self::Output>
    where
        Self: Sized,
    {
        Ok(Self {
            fields: s
                .parse_from::<Braces<List<(Name, Colon, Term), Comma>>>()?
                .into_iter()
                .fold(BTreeMap::new(), |mut acc, (k, _, v)| {
                    acc.insert(k, v);
                    acc
                }),
        })
    }
}

impl Display for UserDefinedTypeLiteral {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{{{}}}",
            self.fields
                .iter()
                .map(|(k, v)| format!("{}: {}", k, v))
                .collect::<Vec<_>>()
                .join(", ")
        )
    }
}

pub type UserDefinedType = KeyspaceQualifiedName;

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_duration_literals() {
        assert_eq!(
            "P3Y6M4DT12H30M5S".parse::<DurationLiteral>().unwrap(),
            DurationLiteral::default().y(3).mo(6).d(4).h(12).m(30).s(5),
        );
        assert_eq!(
            "P23DT23H".parse::<DurationLiteral>().unwrap(),
            DurationLiteral::default().d(23).h(23),
        );
        assert_eq!(
            "P4Y".parse::<DurationLiteral>().unwrap(),
            DurationLiteral::default().y(4)
        );
        assert_eq!("PT0S".parse::<DurationLiteral>().unwrap(), DurationLiteral::default());
        assert_eq!(
            "P1M".parse::<DurationLiteral>().unwrap(),
            DurationLiteral::default().mo(1)
        );
        assert_eq!(
            "PT1M".parse::<DurationLiteral>().unwrap(),
            DurationLiteral::default().m(1)
        );
        assert_eq!(
            "PT36H".parse::<DurationLiteral>().unwrap(),
            DurationLiteral::default().h(36)
        );
        assert_eq!(
            "P1DT12H".parse::<DurationLiteral>().unwrap(),
            DurationLiteral::default().d(1).h(12)
        );
        assert_eq!(
            "P0003-06-04T12:30:05".parse::<DurationLiteral>().unwrap(),
            DurationLiteral::default().y(3).mo(6).d(4).h(12).m(30).s(5)
        );
        assert_eq!(
            "89h4m48s".parse::<DurationLiteral>().unwrap(),
            DurationLiteral::default().h(89).m(4).s(48)
        );
        assert_eq!(
            "89d4w48ns2us15ms".parse::<DurationLiteral>().unwrap(),
            DurationLiteral::default().d(89).w(4).ns(48).us(2).ms(15)
        );

        assert!("P".parse::<DurationLiteral>().is_err());
        assert!("T".parse::<DurationLiteral>().is_err());
        assert!("PT".parse::<DurationLiteral>().is_err());
        assert!("P1".parse::<DurationLiteral>().is_err());
        assert!("P10Y3".parse::<DurationLiteral>().is_err());
        assert!("P0003-06-04".parse::<DurationLiteral>().is_err());
        assert!("T11:30:05".parse::<DurationLiteral>().is_err());
        assert!("PT11:30:05".parse::<DurationLiteral>().is_err());
        assert!("P0003-06-04T25:30:05".parse::<DurationLiteral>().is_err());
        assert!("P0003-06-04T12:70:05".parse::<DurationLiteral>().is_err());
        assert!("P0003-06-04T12:30:70".parse::<DurationLiteral>().is_err());
        assert!("P0003-06-80T12:30:05".parse::<DurationLiteral>().is_err());
        assert!("P0003-13-04T12:30:05".parse::<DurationLiteral>().is_err());
        assert!("2w6y8mo96ns4u".parse::<DurationLiteral>().is_err());
        assert!("2w6b8mo96ns4us".parse::<DurationLiteral>().is_err());
    }
}

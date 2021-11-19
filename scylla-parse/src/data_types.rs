use super::{
    keywords::*,
    Angles,
    BindMarker,
    Braces,
    Brackets,
    CustomToTokens,
    Float,
    FunctionCall,
    Hex,
    List,
    Name,
    Nothing,
    Number,
    Parens,
    Parse,
    Peek,
    SignedNumber,
    StatementStream,
};
use crate::{
    Alpha,
    KeyspaceQualifiedName,
    LitStr,
    TokenWrapper,
};
use chrono::{
    DateTime,
    NaiveDate,
    NaiveTime,
    Utc,
};
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
        HashMap,
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

impl Peek for ArithmeticOp {
    fn peek(mut s: StatementStream<'_>) -> bool {
        s.parse::<Self>().is_ok()
    }
}

#[derive(ParseFromStr, Copy, Clone, Debug, ToTokens)]
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
            anyhow::bail!("Invalid token for operator: {}", s.info())
        }
    }
}

impl Peek for Operator {
    fn peek(mut s: StatementStream<'_>) -> bool {
        s.parse::<Self>().is_ok()
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
            anyhow::bail!("Invalid token for time unit: {}", s.info())
        }
    }
}

impl Peek for TimeUnit {
    fn peek(mut s: StatementStream<'_>) -> bool {
        s.parse::<Self>().is_ok()
    }
}

#[derive(ParseFromStr, Clone, Debug, PartialEq, Eq, Hash, Ord, PartialOrd, ToTokens)]
pub enum Term {
    Constant(Constant),
    Literal(Literal),
    FunctionCall(FunctionCall),
    ArithmeticOp {
        lhs: Option<Box<Term>>,
        op: ArithmeticOp,
        rhs: Box<Term>,
    },
    TypeHint {
        hint: CqlType,
        ident: Name,
    },
    BindMarker(BindMarker),
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
            anyhow::bail!("Invalid term: {}", s.info())
        })
    }
}

impl Peek for Term {
    fn peek(s: StatementStream<'_>) -> bool {
        s.check::<Constant>()
            || s.check::<Literal>()
            || s.check::<FunctionCall>()
            || s.check::<(Option<Term>, ArithmeticOp, Term)>()
            || s.check::<(CqlType, Name)>()
            || s.check::<BindMarker>()
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
            anyhow::bail!("Invalid constant: {}", s.info())
        })
    }
}
impl Peek for Constant {
    fn peek(mut s: StatementStream<'_>) -> bool {
        s.check::<NULL>()
            || s.check::<LitStr>()
            || s.check::<SignedNumber>()
            || s.check::<Float>()
            || s.check::<bool>()
            || s.check::<Uuid>()
            || s.check::<Hex>()
            || s.nextn(2).map(|s| s.to_lowercase().as_str() == "0x").unwrap_or(false)
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
        Self::Float(f.to_string())
    }
}

impl From<f64> for Constant {
    fn from(f: f64) -> Self {
        Self::Float(f.to_string())
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
            anyhow::bail!("Invalid CQL literal type: {}", s.info())
        })
    }
}
impl Peek for Literal {
    fn peek(s: StatementStream<'_>) -> bool {
        s.check::<CollectionTypeLiteral>() || s.check::<UserDefinedTypeLiteral>() || s.check::<TupleLiteral>()
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
    Collection(CollectionType),
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
            Self::Collection(c)
        } else if s.parse::<Option<TUPLE>>()?.is_some() {
            Self::Tuple(s.parse_from::<Angles<List<CqlType, Comma>>>()?)
        } else if let Some(n) = s.parse()? {
            Self::Native(n)
        } else if let Some(udt) = s.parse()? {
            Self::UserDefined(udt)
        } else if let Some(c) = s.parse()? {
            Self::Custom(c)
        } else {
            anyhow::bail!("Invalid CQL Type: {}", s.info())
        })
    }
}
impl Peek for CqlType {
    fn peek(s: StatementStream<'_>) -> bool {
        s.check::<CollectionType>()
            || s.check::<TUPLE>()
            || s.check::<NativeType>()
            || s.check::<UserDefinedType>()
            || s.check::<LitStr>()
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

impl Peek for NativeType {
    fn peek(mut s: StatementStream<'_>) -> bool {
        if let Ok(token) = s.parse_from::<Alpha>() {
            NativeType::from_str(&token).is_ok()
        } else {
            false
        }
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
            anyhow::bail!("Invalid collection literal type: {}", s.info())
        })
    }
}
impl Peek for CollectionTypeLiteral {
    fn peek(s: StatementStream<'_>) -> bool {
        s.check::<ListLiteral>() || s.check::<SetLiteral>() || s.check::<MapLiteral>()
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
    List(Box<CqlType>),
    Set(Box<CqlType>),
    Map(Box<CqlType>, Box<CqlType>),
}

impl Parse for CollectionType {
    type Output = Self;
    fn parse(s: &mut StatementStream<'_>) -> anyhow::Result<Self::Output>
    where
        Self: Sized,
    {
        Ok(if s.parse::<Option<MAP>>()?.is_some() {
            let (t1, _, t2) = s.parse_from::<Angles<(CqlType, Comma, CqlType)>>()?;
            Self::Map(Box::new(t1), Box::new(t2))
        } else if s.parse::<Option<SET>>()?.is_some() {
            Self::Set(Box::new(s.parse_from::<Angles<CqlType>>()?))
        } else if s.parse::<Option<LIST>>()?.is_some() {
            Self::List(Box::new(s.parse_from::<Angles<CqlType>>()?))
        } else {
            anyhow::bail!("Invalid collection type: {}", s.info())
        })
    }
}

impl Peek for CollectionType {
    fn peek(s: StatementStream<'_>) -> bool {
        s.check::<MAP>() || s.check::<SET>() || s.check::<LIST>()
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
pub struct MapLiteral {
    pub elements: BTreeMap<Term, Term>,
}

impl Parse for MapLiteral {
    type Output = Self;
    fn parse(s: &mut StatementStream<'_>) -> anyhow::Result<Self::Output>
    where
        Self: Sized,
    {
        Ok(Self {
            elements: s
                .parse_from::<Braces<List<(Term, Colon, Term), Comma>>>()?
                .into_iter()
                .map(|(k, _, v)| (k, v))
                .collect(),
        })
    }
}
impl Peek for MapLiteral {
    fn peek(mut s: StatementStream<'_>) -> bool {
        s.parse::<Self>().is_ok()
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

impl<T: Into<Term>> From<HashMap<T, T>> for MapLiteral {
    fn from(m: HashMap<T, T>) -> Self {
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
impl Peek for TupleLiteral {
    fn peek(mut s: StatementStream<'_>) -> bool {
        s.parse::<Self>().is_ok()
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
    pub elements: Vec<Term>,
}

impl Parse for SetLiteral {
    type Output = Self;
    fn parse(s: &mut StatementStream<'_>) -> anyhow::Result<Self::Output> {
        Ok(Self {
            elements: s.parse_from::<Braces<List<Term, Comma>>>()?,
        })
    }
}
impl Peek for SetLiteral {
    fn peek(mut s: StatementStream<'_>) -> bool {
        s.parse::<Self>().is_ok()
    }
}

impl Display for SetLiteral {
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

impl<T: Into<Term>> From<Vec<T>> for SetLiteral {
    fn from(elements: Vec<T>) -> Self {
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
impl Peek for ListLiteral {
    fn peek(mut s: StatementStream<'_>) -> bool {
        s.parse::<Self>().is_ok()
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
                    .parse::<DateTime<Utc>>()
                    .map_err(|e| anyhow::anyhow!(e))?
                    .timestamp(),
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
            Ok(Self(dur.num_days() as u32))
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

#[derive(Clone, Debug, Default, ToTokens)]
pub struct DurationLiteral {
    pub months: i32,
    pub days: i32,
    pub nanos: i64,
}

impl Parse for DurationLiteral {
    type Output = Self;
    fn parse(s: &mut StatementStream<'_>) -> anyhow::Result<Self::Output> {
        Ok(
            if let Some(v) = s.parse_from::<Option<List<(Number, TimeUnit), Nothing>>>()? {
                let mut res = DurationLiteral::default();
                for (n, u) in v {
                    match u {
                        TimeUnit::Nanos => res.nanos += n.parse::<i64>()?,
                        TimeUnit::Micros => res.nanos += n.parse::<i64>()? * 1000,
                        TimeUnit::Millis => res.nanos += n.parse::<i64>()? * 1_000_000,
                        TimeUnit::Seconds => res.nanos += n.parse::<i64>()? * 1_000_000_000,
                        TimeUnit::Minutes => res.nanos += n.parse::<i64>()? * 60_000_000_000,
                        TimeUnit::Hours => res.nanos += n.parse::<i64>()? * 3_600_000_000_000,
                        TimeUnit::Days => res.days += n.parse::<i32>()?,
                        TimeUnit::Weeks => res.days += n.parse::<i32>()? * 7,
                        TimeUnit::Months => res.months += n.parse::<i32>()?,
                        TimeUnit::Years => res.months += n.parse::<i32>()? * 12,
                    }
                }
                res
            } else {
                anyhow::bail!("ISO 8601 not currently supported for durations! Use `(quantity unit)+` instead!");
                // let token = s.parse::<String>()?;
                // let dt = DateTime::parse_from_rfc3339(&token).map_err(|e| anyhow::anyhow!(e))?;
                // DurationLiteral {
                //    months: dt.year() * 12 + dt.month() as i32,
                //    days: dt.day() as i32,
                //    nanos: dt.hour() as i64 * 3_600_000_000_000
                //        + dt.minute() as i64 * 60_000_000_000
                //        + dt.second() as i64 * 1_000_000_000,
                //}
            },
        )
    }
}
impl Peek for DurationLiteral {
    fn peek(mut s: StatementStream<'_>) -> bool {
        s.parse::<Self>().is_ok()
    }
}

impl Display for DurationLiteral {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}mo{}d{}ns", self.months, self.days, self.nanos)
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
impl Peek for UserDefinedTypeLiteral {
    fn peek(mut s: StatementStream<'_>) -> bool {
        s.parse::<Self>().is_ok()
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

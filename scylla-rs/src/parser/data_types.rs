use super::{
    keywords::*, Angles, BindMarker, Braces, Brackets, Float, FunctionCall, Hex, Identifier, List, Name, Nothing,
    Number, Parens, Parse, Peek, SignedNumber, StatementStream, Token,
};
use chrono::{DateTime, NaiveDate, NaiveTime, Utc};
use std::{
    collections::HashMap,
    convert::{TryFrom, TryInto},
    fmt::Display,
    str::FromStr,
};
use uuid::Uuid;

#[derive(Copy, Clone, Debug)]
pub enum ArithmeticOp {
    Add,
    Sub,
    Mul,
    Div,
    Mod,
}

impl Display for ArithmeticOp {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
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
    type Output = ArithmeticOp;
    fn parse(s: &mut StatementStream<'_>) -> anyhow::Result<Self::Output> {
        s.parse::<char>()?.try_into()
    }
}

impl Peek for ArithmeticOp {
    fn peek(mut s: StatementStream<'_>) -> bool {
        s.parse::<Self>().is_ok()
    }
}

#[derive(Copy, Clone, Debug)]
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
}

impl Display for Operator {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
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
            }
        )
    }
}

impl Parse for Operator {
    type Output = Operator;
    fn parse(s: &mut StatementStream<'_>) -> anyhow::Result<Self> {
        if let Some(res) = s.parse_if::<(Keyword, Option<Keyword>)>() {
            let (first, second) = res?;
            Ok(match (first, second) {
                (Keyword::IN, _) => Operator::In,
                (Keyword::CONTAINS, Some(Keyword::KEY)) => Operator::ContainsKey,
                (Keyword::CONTAINS, _) => Operator::Contains,
                _ => anyhow::bail!("Invalid keyword operator!"),
            })
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
                _ => anyhow::bail!("Invalid operator"),
            })
        } else {
            anyhow::bail!("Invalid token for operator!")
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
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
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
    type Output = TimeUnit;
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
                _ => anyhow::bail!("Invalid time unit!"),
            })
        } else {
            anyhow::bail!("Invalid token for time unit!")
        }
    }
}

impl Peek for TimeUnit {
    fn peek(mut s: StatementStream<'_>) -> bool {
        s.parse::<Self>().is_ok()
    }
}

#[derive(Clone, Debug)]
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
        ident: Identifier,
    },
    BindMarker(BindMarker),
}

impl Parse for Term {
    type Output = Term;
    fn parse(s: &mut StatementStream<'_>) -> anyhow::Result<Self> {
        Ok(if let Some(c) = s.parse_if() {
            Self::Constant(c?)
        } else if let Some(lit) = s.parse_if() {
            Self::Literal(lit?)
        } else if let Some(f) = s.parse_if() {
            Self::FunctionCall(f?)
        } else if let Some(res) = s.parse_if() {
            let (hint, ident) = res?;
            Self::TypeHint { hint, ident }
        } else if let Some(b) = s.parse_if() {
            Self::BindMarker(b?)
        } else if let Some(res) = s.parse_if::<(Option<Term>, ArithmeticOp, Term)>() {
            let (lhs, op, rhs) = res?;
            Self::ArithmeticOp {
                lhs: lhs.map(Box::new),
                op,
                rhs: Box::new(rhs),
            }
        } else {
            anyhow::bail!("Invalid term!")
        })
    }
}

impl Peek for Term {
    fn peek(s: StatementStream<'_>) -> bool {
        s.check::<Constant>()
            || s.check::<Literal>()
            || s.check::<FunctionCall>()
            || s.check::<(Option<Term>, ArithmeticOp, Term)>()
            || s.check::<(CqlType, Identifier)>()
            || s.check::<BindMarker>()
    }
}

#[derive(Clone, Debug)]
pub enum Constant {
    Null,
    String(String),
    Integer(String),
    Float(String),
    Boolean(bool),
    Uuid(Uuid),
    Hex(Vec<u8>),
    Blob(Vec<u8>),
}

impl Parse for Constant {
    type Output = Constant;
    fn parse(s: &mut StatementStream<'_>) -> anyhow::Result<Self::Output> {
        Ok(if s.parse_if::<NULL>().is_some() {
            Constant::Null
        } else if let Some(ss) = s.parse_if() {
            Constant::String(ss?)
        } else if let Some(i) = s.parse_from_if::<SignedNumber>() {
            Constant::Integer(i?)
        } else if let Some(f) = s.parse_from_if::<Float>() {
            Constant::Float(f?)
        } else if s.parse_from_if::<TRUE>().is_some() {
            Constant::Boolean(true)
        } else if s.parse_from_if::<FALSE>().is_some() {
            Constant::Boolean(false)
        } else if let Some(u) = s.parse_if() {
            Constant::Uuid(u?)
        } else if let Some(h) = s.parse_from_if::<Hex>() {
            Constant::Hex(h?)
        } else if s.peekn(2).map(|s| s.to_lowercase().as_str() == "0x").unwrap_or(false) {
            s.nextn(2);
            Constant::Blob(s.parse_from::<Hex>()?)
        } else {
            anyhow::bail!("Invalid constant!")
        })
    }
}
impl Peek for Constant {
    fn peek(mut s: StatementStream<'_>) -> bool {
        s.check::<NULL>()
            || s.check::<String>()
            || s.check::<SignedNumber>()
            || s.check::<Float>()
            || s.check::<TRUE>()
            || s.check::<FALSE>()
            || s.check::<Uuid>()
            || s.check::<Hex>()
            || s.nextn(2).map(|s| s.to_lowercase().as_str() == "0x").unwrap_or(false)
    }
}

#[derive(Clone, Debug)]
pub enum Literal {
    Collection(CollectionTypeLiteral),
    UserDefined(UserDefinedTypeLiteral),
    Tuple(TupleLiteral),
}

impl Parse for Literal {
    type Output = Literal;
    fn parse(s: &mut StatementStream<'_>) -> anyhow::Result<Self::Output> {
        Ok(if let Some(c) = s.parse_if() {
            Self::Collection(c?)
        } else if let Some(u) = s.parse_if() {
            Self::UserDefined(u?)
        } else if let Some(t) = s.parse_if() {
            Self::Tuple(t?)
        } else {
            anyhow::bail!("Invalid CQL literal type")
        })
    }
}
impl Peek for Literal {
    fn peek(s: StatementStream<'_>) -> bool {
        s.check::<CollectionTypeLiteral>() || s.check::<UserDefinedTypeLiteral>() || s.check::<TupleLiteral>()
    }
}

#[derive(Clone, Debug)]
pub enum CqlType {
    Native(NativeType),
    Collection(CollectionType),
    UserDefined(UserDefinedType),
    Tuple(Vec<CqlType>),
    Custom(String),
}

impl Parse for CqlType {
    type Output = CqlType;
    fn parse(s: &mut StatementStream<'_>) -> anyhow::Result<Self::Output>
    where
        Self: Sized,
    {
        Ok(if let Some(c) = s.parse_if() {
            Self::Collection(c?)
        } else if s.parse_if::<TUPLE>().is_some() {
            Self::Tuple(s.parse_from::<Angles<List<CqlType, Comma>>>()?)
        } else if let Some(n) = s.parse_if() {
            Self::Native(n?)
        } else if let Some(udt) = s.parse_if() {
            Self::UserDefined(udt?)
        } else if let Some(c) = s.parse_if() {
            Self::Custom(c?)
        } else {
            anyhow::bail!("Invalid CQL Type!")
        })
    }
}
impl Peek for CqlType {
    fn peek(s: StatementStream<'_>) -> bool {
        s.check::<CollectionType>()
            || s.check::<TUPLE>()
            || s.check::<NativeType>()
            || s.check::<UserDefinedType>()
            || s.check::<String>()
    }
}

#[derive(Copy, Clone, Debug)]
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
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
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
            _ => anyhow::bail!("Invalid native type!"),
        })
    }
}

impl Parse for NativeType {
    type Output = NativeType;
    fn parse(s: &mut StatementStream<'_>) -> anyhow::Result<Self::Output>
    where
        Self: Sized,
    {
        let token = s.parse_from::<Token>()?;
        NativeType::from_str(&token)
    }
}

impl Peek for NativeType {
    fn peek(mut s: StatementStream<'_>) -> bool {
        if let Ok(token) = s.parse_from::<Token>() {
            NativeType::from_str(&token).is_ok()
        } else {
            false
        }
    }
}

#[derive(Clone, Debug)]
pub enum CollectionTypeLiteral {
    List(ListLiteral),
    Set(SetLiteral),
    Map(MapLiteral),
}

impl Parse for CollectionTypeLiteral {
    type Output = CollectionTypeLiteral;
    fn parse(s: &mut StatementStream<'_>) -> anyhow::Result<Self::Output> {
        Ok(if let Some(l) = s.parse_if() {
            Self::List(l?)
        } else if let Some(s) = s.parse_if() {
            Self::Set(s?)
        } else if let Some(m) = s.parse_if() {
            Self::Map(m?)
        } else {
            anyhow::bail!("Invalid collection literal type!")
        })
    }
}
impl Peek for CollectionTypeLiteral {
    fn peek(s: StatementStream<'_>) -> bool {
        s.check::<ListLiteral>() || s.check::<SetLiteral>() || s.check::<MapLiteral>()
    }
}

#[derive(Clone, Debug)]
pub enum CollectionType {
    List(Box<CqlType>),
    Set(Box<CqlType>),
    Map(Box<CqlType>, Box<CqlType>),
}

impl Parse for CollectionType {
    type Output = CollectionType;
    fn parse(s: &mut StatementStream<'_>) -> anyhow::Result<Self::Output>
    where
        Self: Sized,
    {
        Ok(if s.parse_if::<MAP>().is_some() {
            let (t1, _, t2) = s.parse_from::<Angles<(CqlType, Comma, CqlType)>>()?;
            Self::Map(Box::new(t1), Box::new(t2))
        } else if s.parse_if::<SET>().is_some() {
            Self::Set(Box::new(s.parse_from::<Angles<CqlType>>()?))
        } else if s.parse_if::<LIST>().is_some() {
            Self::List(Box::new(s.parse_from::<Angles<CqlType>>()?))
        } else {
            anyhow::bail!("Invalid collection type!")
        })
    }
}

impl Peek for CollectionType {
    fn peek(s: StatementStream<'_>) -> bool {
        s.check::<MAP>() || s.check::<SET>() || s.check::<LIST>()
    }
}

#[derive(Clone, Debug)]
pub struct MapLiteral {
    pub elements: Vec<(Term, Term)>,
}

impl Parse for MapLiteral {
    type Output = MapLiteral;
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

#[derive(Clone, Debug)]
pub struct TupleLiteral {
    pub elements: Vec<Term>,
}

impl Parse for TupleLiteral {
    type Output = TupleLiteral;
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

#[derive(Clone, Debug)]
pub struct SetLiteral {
    pub elements: Vec<Term>,
}

impl Parse for SetLiteral {
    type Output = SetLiteral;
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

#[derive(Clone, Debug)]
pub struct ListLiteral {
    pub elements: Vec<Term>,
}

impl Parse for ListLiteral {
    type Output = ListLiteral;
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

#[derive(Clone, Debug, Default)]
pub struct TimestampLiteral(i64);
impl Parse for TimestampLiteral {
    type Output = TimestampLiteral;
    fn parse(s: &mut StatementStream<'_>) -> anyhow::Result<Self::Output> {
        if let Some(res) = s.parse_if::<String>() {
            let ts = res?;
            Ok(Self(ts.parse::<DateTime<Utc>>()?.timestamp()))
        } else {
            Ok(Self(s.parse::<u64>()? as i64))
        }
    }
}

#[derive(Clone, Debug, Default)]
pub struct DateLiteral(u32);
impl Parse for DateLiteral {
    type Output = DateLiteral;
    fn parse(s: &mut StatementStream<'_>) -> anyhow::Result<Self::Output> {
        if let Some(res) = s.parse_if::<String>() {
            let d = res?;
            let dur = d.parse::<NaiveDate>()? - NaiveDate::from_ymd(1970, 1, 1);
            Ok(Self(dur.num_days() as u32))
        } else {
            Ok(Self(s.parse::<u32>()?))
        }
    }
}

#[derive(Clone, Debug, Default)]
pub struct TimeLiteral(i64);
impl Parse for TimeLiteral {
    type Output = TimeLiteral;
    fn parse(s: &mut StatementStream<'_>) -> anyhow::Result<Self::Output> {
        if let Some(res) = s.parse_from_if::<String>() {
            let t = res?;
            let t = t.parse::<NaiveTime>()? - NaiveTime::from_hms(0, 0, 0);
            Ok(Self(
                t.num_nanoseconds()
                    .ok_or_else(|| anyhow::anyhow!("Invalid time literal!"))?,
            ))
        } else {
            Ok(Self(s.parse::<u64>()? as i64))
        }
    }
}

#[derive(Clone, Debug, Default)]
pub struct DurationLiteral {
    pub months: i32,
    pub days: i32,
    pub nanos: i64,
}

impl Parse for DurationLiteral {
    type Output = DurationLiteral;
    fn parse(s: &mut StatementStream<'_>) -> anyhow::Result<Self::Output> {
        Ok(
            if let Some(v) = s.parse_from_if::<List<(Number, TimeUnit), Nothing>>() {
                let mut res = DurationLiteral::default();
                for (n, u) in v? {
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

#[derive(Clone, Debug)]
pub struct UserDefinedTypeLiteral {
    pub fields: HashMap<Identifier, Term>,
}

impl Parse for UserDefinedTypeLiteral {
    type Output = UserDefinedTypeLiteral;
    fn parse(s: &mut StatementStream<'_>) -> anyhow::Result<Self::Output>
    where
        Self: Sized,
    {
        Ok(Self {
            fields: s
                .parse_from::<Braces<List<(Identifier, Colon, Term), Comma>>>()?
                .into_iter()
                .fold(HashMap::new(), |mut acc, (k, _, v)| {
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

#[derive(Clone, Debug)]
pub struct UserDefinedType {
    pub keyspace: Option<Name>,
    pub ident: Identifier,
}

impl Parse for UserDefinedType {
    type Output = UserDefinedType;
    fn parse(s: &mut StatementStream<'_>) -> anyhow::Result<Self::Output>
    where
        Self: Sized,
    {
        let (keyspace, ident) = s.parse::<(Option<(Name, Dot)>, Identifier)>()?;
        Ok(Self {
            keyspace: keyspace.map(|(i, _)| i),
            ident,
        })
    }
}

impl Peek for UserDefinedType {
    fn peek(s: StatementStream<'_>) -> bool {
        s.check::<(Option<(Name, Dot)>, Identifier)>()
    }
}

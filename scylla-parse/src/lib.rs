use derive_builder::Builder;
use scylla_parse_macros::ParseFromStr;
use std::{
    collections::HashMap,
    fmt::{
        Display,
        Formatter,
    },
    marker::PhantomData,
    str::FromStr,
};
use uuid::Uuid;

mod statements;
pub use statements::*;

mod keywords;
pub use keywords::*;

mod data_types;
pub use data_types::*;

mod regex;
pub use self::regex::*;

#[derive(Clone)]
pub struct StatementStream<'a> {
    cursor: std::iter::Peekable<std::str::Chars<'a>>,
}

impl<'a> StatementStream<'a> {
    pub fn new(statement: &'a str) -> Self {
        Self {
            cursor: statement.chars().peekable(),
        }
    }

    pub fn remaining(&self) -> usize {
        self.cursor.clone().count()
    }

    pub fn nremaining(&self, n: usize) -> bool {
        let mut cursor = self.cursor.clone();
        for _ in 0..n {
            if cursor.next().is_none() {
                return false;
            }
        }
        true
    }

    pub fn peek(&mut self) -> Option<char> {
        self.cursor.peek().map(|c| *c)
    }

    pub fn peekn(&mut self, n: usize) -> Option<String> {
        let mut cursor = self.cursor.clone();
        let mut res = String::new();
        for _ in 0..n {
            if let Some(next) = cursor.next() {
                res.push(next);
            } else {
                return None;
            }
        }
        Some(res)
    }

    pub fn next(&mut self) -> Option<char> {
        self.cursor.next()
    }

    pub fn nextn(&mut self, n: usize) -> Option<String> {
        if self.nremaining(n) {
            let mut res = String::new();
            for _ in 0..n {
                res.push(self.next().unwrap());
            }
            Some(res)
        } else {
            None
        }
    }

    fn skip_whitespace(&mut self) {
        while let Some(c) = self.peek() {
            if c.is_whitespace() {
                self.next();
                continue;
            } else {
                break;
            };
        }
    }

    pub fn check<P: Peek>(&self) -> bool {
        let mut this = self.clone();
        this.skip_whitespace();
        P::peek(this)
    }

    pub fn find<P: Parse<Output = P>>(&self) -> Option<P> {
        let mut this = self.clone();
        this.skip_whitespace();
        P::parse(&mut this).ok()
    }

    pub fn find_from<P: Parse>(&self) -> Option<P::Output> {
        let mut this = self.clone();
        this.skip_whitespace();
        P::parse(&mut this).ok()
    }

    pub fn parse_if<P: Peek + Parse<Output = P>>(&mut self) -> Option<anyhow::Result<P>> {
        self.parse::<Option<P>>().transpose()
    }

    pub fn parse_from_if<P: Peek + Parse>(&mut self) -> Option<anyhow::Result<P::Output>> {
        self.parse_from::<Option<P>>().transpose()
    }

    pub fn parse<P: Parse<Output = P>>(&mut self) -> anyhow::Result<P> {
        self.skip_whitespace();
        P::parse(self)
    }

    pub fn parse_from<P: Parse>(&mut self) -> anyhow::Result<P::Output> {
        self.skip_whitespace();
        P::parse(self)
    }
}

pub trait Peek {
    fn peek(s: StatementStream<'_>) -> bool;
}

pub trait Parse {
    type Output;
    fn parse(s: &mut StatementStream<'_>) -> anyhow::Result<Self::Output>;
}

macro_rules! peek_parse_tuple {
    ($($t:ident),+) => {
        impl<$($t: Peek + Parse),+> Peek for ($($t),+,) {
            fn peek(mut s: StatementStream<'_>) -> bool {
                $(
                    if s.parse_from_if::<$t>().is_none() {
                        return false;
                    }
                )+
                true
            }
        }

        impl<$($t: Parse),+> Parse for ($($t),+,) {
            type Output = ($($t::Output),+,);
            fn parse(s: &mut StatementStream<'_>) -> anyhow::Result<Self::Output> {
                Ok(($(
                    s.parse_from::<$t>()?,
                )+))
            }
        }
    };
}

peek_parse_tuple!(T0);
peek_parse_tuple!(T0, T1);
peek_parse_tuple!(T0, T1, T2);
peek_parse_tuple!(T0, T1, T2, T3);
peek_parse_tuple!(T0, T1, T2, T3, T4);
peek_parse_tuple!(T0, T1, T2, T3, T4, T5);
peek_parse_tuple!(T0, T1, T2, T3, T4, T5, T6);
peek_parse_tuple!(T0, T1, T2, T3, T4, T5, T6, T7);
peek_parse_tuple!(T0, T1, T2, T3, T4, T5, T6, T7, T8);
peek_parse_tuple!(T0, T1, T2, T3, T4, T5, T6, T7, T8, T9);

impl Parse for char {
    type Output = Self;
    fn parse(s: &mut StatementStream<'_>) -> anyhow::Result<Self::Output> {
        match s.next() {
            Some(c) => Ok(c),
            None => Err(anyhow::anyhow!("End of statement!")),
        }
    }
}

impl Peek for char {
    fn peek(mut s: StatementStream<'_>) -> bool {
        s.next().is_some()
    }
}

impl Parse for bool {
    type Output = Self;
    fn parse(s: &mut StatementStream<'_>) -> anyhow::Result<Self::Output> {
        Ok(if s.parse_if::<TRUE>().is_some() {
            true
        } else if s.parse_if::<FALSE>().is_some() {
            false
        } else {
            anyhow::bail!("Expected boolean!")
        })
    }
}

impl Peek for bool {
    fn peek(s: StatementStream<'_>) -> bool {
        s.check::<TRUE>() || s.check::<FALSE>()
    }
}

macro_rules! peek_parse_number {
    ($n:ident, $t:ident) => {
        impl Parse for $n {
            type Output = Self;
            fn parse(s: &mut StatementStream<'_>) -> anyhow::Result<Self::Output> {
                s.parse_from::<$t>()?
                    .parse()
                    .map_err(|_| anyhow::anyhow!("Invalid {}!", std::any::type_name::<$n>()))
            }
        }

        impl Peek for $n {
            fn peek(mut s: StatementStream<'_>) -> bool {
                s.parse::<Self>().is_ok()
            }
        }
    };
}

peek_parse_number!(i8, SignedNumber);
peek_parse_number!(i16, SignedNumber);
peek_parse_number!(i32, SignedNumber);
peek_parse_number!(i64, SignedNumber);
peek_parse_number!(u8, Number);
peek_parse_number!(u16, Number);
peek_parse_number!(u32, Number);
peek_parse_number!(u64, Number);
peek_parse_number!(f32, Float);
peek_parse_number!(f64, Float);

pub struct If<Cond, Res>(PhantomData<fn(Cond, Res) -> (Cond, Res)>);
impl<Cond: Peek + Parse, Res: Parse> Parse for If<Cond, Res> {
    type Output = Option<Res::Output>;
    fn parse(s: &mut StatementStream<'_>) -> anyhow::Result<Self::Output> {
        match s.parse_from::<Option<Cond>>()? {
            Some(_) => Ok(Some(s.parse_from::<Res>()?)),
            None => Ok(None),
        }
    }
}

impl<T: Peek> Peek for Option<T> {
    fn peek(s: StatementStream<'_>) -> bool {
        s.check::<T>()
    }
}

impl<T: Parse + Peek> Parse for Option<T> {
    type Output = Option<T::Output>;
    fn parse(s: &mut StatementStream<'_>) -> anyhow::Result<Self::Output> {
        Ok(if s.check::<T>() {
            Some(s.parse_from::<T>()?)
        } else {
            None
        })
    }
}

pub struct List<T, Delim>(PhantomData<fn(T, Delim) -> (T, Delim)>);
impl<T: Parse, Delim: Parse + Peek> Parse for List<T, Delim> {
    type Output = Vec<T::Output>;
    fn parse(s: &mut StatementStream<'_>) -> anyhow::Result<Self::Output> {
        let mut res = vec![s.parse_from::<T>()?];
        while s.parse_from_if::<Delim>().is_some() {
            res.push(s.parse_from::<T>()?);
        }
        Ok(res)
    }
}
impl<T: Parse, Delim: Parse + Peek> Peek for List<T, Delim> {
    fn peek(mut s: StatementStream<'_>) -> bool {
        s.parse_from::<Self>().is_ok()
    }
}

pub struct Nothing;
impl Parse for Nothing {
    type Output = Self;
    fn parse(_: &mut StatementStream<'_>) -> anyhow::Result<Self::Output> {
        Ok(Nothing)
    }
}
impl Peek for Nothing {
    fn peek(_: StatementStream<'_>) -> bool {
        true
    }
}

pub struct Whitespace;
impl Parse for Whitespace {
    type Output = Self;
    fn parse(s: &mut StatementStream<'_>) -> anyhow::Result<Self::Output> {
        while let Some(c) = s.peek() {
            if c.is_whitespace() {
                s.next();
            } else {
                break;
            }
        }
        Ok(Whitespace)
    }
}
impl Peek for Whitespace {
    fn peek(mut s: StatementStream<'_>) -> bool {
        s.peek().map(|c| c.is_whitespace()).unwrap_or(false)
    }
}

impl Parse for String {
    type Output = Self;
    fn parse(s: &mut StatementStream<'_>) -> anyhow::Result<Self::Output> {
        let mut res = String::new();
        let mut dollars = false;
        if s.peek() == Some('\'') {
            s.next();
        } else if s.peekn(2).map(|s| s.as_str() == "$$").unwrap_or(false) {
            dollars = true;
            s.nextn(2);
        } else {
            return Err(anyhow::anyhow!("Expected opening quote!"));
        }
        while let Some(c) = s.next() {
            if dollars && c == '$' && s.peek().map(|c| c == '$').unwrap_or(false) {
                s.next();
                return Ok(res);
            } else if !dollars && c == '\'' {
                return Ok(res);
            } else {
                res.push(c);
            }
        }
        if res.is_empty() {
            anyhow::bail!("End of statement!")
        }
        Ok(res)
    }
}
impl Peek for String {
    fn peek(mut s: StatementStream<'_>) -> bool {
        s.parse::<Self>().is_ok()
    }
}

pub struct Token;
impl Parse for Token {
    type Output = String;
    fn parse(s: &mut StatementStream<'_>) -> anyhow::Result<Self::Output> {
        let mut res = String::new();
        while let Some(c) = s.next() {
            if c.is_whitespace() {
                break;
            } else {
                res.push(c);
            }
        }
        if res.is_empty() {
            anyhow::bail!("End of statement!")
        }
        Ok(res)
    }
}
impl Peek for Token {
    fn peek(mut s: StatementStream<'_>) -> bool {
        s.peek().is_some()
    }
}

pub struct Alpha;
impl Parse for Alpha {
    type Output = String;
    fn parse(s: &mut StatementStream<'_>) -> anyhow::Result<Self::Output> {
        let mut res = String::new();
        while let Some(c) = s.peek() {
            if c.is_alphabetic() {
                res.push(c);
                s.next();
            } else {
                break;
            }
        }
        if res.is_empty() {
            anyhow::bail!("End of statement!")
        }
        Ok(res)
    }
}
impl Peek for Alpha {
    fn peek(mut s: StatementStream<'_>) -> bool {
        s.parse_from::<Alpha>().is_ok()
    }
}

pub struct Hex;
impl Parse for Hex {
    type Output = Vec<u8>;
    fn parse(s: &mut StatementStream<'_>) -> anyhow::Result<Self::Output> {
        let mut res = String::new();
        while let Some(c) = s.peek() {
            if c.is_alphanumeric() {
                res.push(c);
                s.next();
            } else {
                break;
            }
        }
        if res.is_empty() {
            anyhow::bail!("End of statement!")
        }
        Ok(hex::decode(res)?)
    }
}
impl Peek for Hex {
    fn peek(mut s: StatementStream<'_>) -> bool {
        s.parse_from::<Hex>().is_ok()
    }
}

pub struct Alphanumeric;
impl Parse for Alphanumeric {
    type Output = String;
    fn parse(s: &mut StatementStream<'_>) -> anyhow::Result<Self::Output> {
        let mut res = String::new();
        while let Some(c) = s.peek() {
            if c.is_alphanumeric() {
                res.push(c);
                s.next();
            } else {
                break;
            }
        }
        if res.is_empty() {
            anyhow::bail!("End of statement!")
        }
        Ok(res)
    }
}
impl Peek for Alphanumeric {
    fn peek(mut s: StatementStream<'_>) -> bool {
        s.parse_from::<Alphanumeric>().is_ok()
    }
}

pub struct Number;
impl Parse for Number {
    type Output = String;
    fn parse(s: &mut StatementStream<'_>) -> anyhow::Result<Self::Output> {
        let mut res = String::new();
        while let Some(c) = s.peek() {
            if c.is_numeric() {
                res.push(c);
                s.next();
            } else {
                break;
            }
        }
        if res.is_empty() {
            anyhow::bail!("End of statement!")
        }
        Ok(res)
    }
}
impl Peek for Number {
    fn peek(mut s: StatementStream<'_>) -> bool {
        s.parse_from::<Number>().is_ok()
    }
}

pub struct SignedNumber;
impl Parse for SignedNumber {
    type Output = String;
    fn parse(s: &mut StatementStream<'_>) -> anyhow::Result<Self::Output> {
        let mut res = String::new();
        let mut has_negative = false;
        while let Some(c) = s.peek() {
            if c.is_numeric() {
                res.push(c);
                s.next();
            } else if c == '-' {
                if has_negative || !res.is_empty() {
                    anyhow::bail!("Invalid number: Improper negative sign")
                } else {
                    has_negative = true;
                    res.push(c);
                    s.next();
                }
            } else {
                break;
            }
        }
        if res.is_empty() {
            anyhow::bail!("End of statement!")
        }
        Ok(res)
    }
}
impl Peek for SignedNumber {
    fn peek(mut s: StatementStream<'_>) -> bool {
        s.parse_from::<SignedNumber>().is_ok()
    }
}

pub struct Float;
impl Parse for Float {
    type Output = String;
    fn parse(s: &mut StatementStream<'_>) -> anyhow::Result<Self::Output> {
        let mut res = String::new();
        let mut has_dot = false;
        let mut has_negative = false;
        let mut has_e = false;
        while let Some(c) = s.peek() {
            if c.is_numeric() {
                res.push(c);
                s.next();
            } else if c == '-' {
                if has_negative || !res.is_empty() {
                    anyhow::bail!("Invalid float: Improper negative sign")
                } else {
                    has_negative = true;
                    res.push(c);
                    s.next();
                }
            } else if c == '.' {
                if has_dot {
                    anyhow::bail!("Invalid float: Too many decimal points")
                } else {
                    has_dot = true;
                    res.push(c);
                    s.next();
                }
            } else if c == 'e' || c == 'E' {
                if has_e {
                    anyhow::bail!("Invalid float: Too many scientific notations")
                } else {
                    if res.is_empty() {
                        anyhow::bail!("Invalid float: Missing number before scientific notation")
                    }
                    res.push(c);
                    s.next();
                    has_e = true;
                    if let Some(next) = s.next() {
                        if next == '-' || next == '+' || next.is_numeric() {
                            res.push(next);
                        } else {
                            anyhow::bail!("Invalid float: Invalid scientific notation")
                        }
                    } else {
                        anyhow::bail!("Invalid float: Missing scientific notation value")
                    }
                }
            } else {
                break;
            }
        }
        if res.is_empty() {
            anyhow::bail!("End of statement!")
        }
        Ok(res)
    }
}
impl Peek for Float {
    fn peek(mut s: StatementStream<'_>) -> bool {
        s.parse_from::<Float>().is_ok()
    }
}

macro_rules! parse_peek_group {
    ($g:ident, $l:ident, $r:ident) => {
        pub struct $g<T>(T);
        impl<T: Parse> Parse for $g<T> {
            type Output = T::Output;
            fn parse(s: &mut StatementStream<'_>) -> anyhow::Result<Self::Output> {
                s.parse_from::<$l>()?;
                let res = s.parse_from::<T>()?;
                s.parse_from::<$r>()?;
                Ok(res)
            }
        }
        impl<T> Peek for $g<T> {
            fn peek(s: StatementStream<'_>) -> bool {
                s.check::<$l>()
            }
        }
    };
}

parse_peek_group!(Parens, LeftParen, RightParen);
parse_peek_group!(Brackets, LeftBracket, RightBracket);
parse_peek_group!(Braces, LeftBrace, RightBrace);
parse_peek_group!(Angles, LeftAngle, RightAngle);
parse_peek_group!(SingleQuoted, SingleQuote, SingleQuote);
parse_peek_group!(DoubleQuoted, DoubleQuote, DoubleQuote);

#[derive(ParseFromStr, Clone, Debug)]
pub enum BindMarker {
    Anonymous,
    Named(Name),
}

impl Parse for BindMarker {
    type Output = Self;
    fn parse(s: &mut StatementStream<'_>) -> anyhow::Result<Self::Output> {
        Ok(if s.parse_if::<Question>().is_some() {
            BindMarker::Anonymous
        } else {
            let (_, id) = s.parse::<(Colon, Name)>()?;
            BindMarker::Named(id)
        })
    }
}

impl Peek for BindMarker {
    fn peek(s: StatementStream<'_>) -> bool {
        s.check::<Question>() || s.check::<(Colon, Name)>()
    }
}

impl Display for BindMarker {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            BindMarker::Anonymous => write!(f, "?"),
            BindMarker::Named(id) => write!(f, ":{}", id),
        }
    }
}

impl Parse for Uuid {
    type Output = Self;
    fn parse(s: &mut StatementStream<'_>) -> anyhow::Result<Self::Output> {
        if let Some(u) = s.nextn(36) {
            Ok(Uuid::parse_str(&u)?)
        } else {
            anyhow::bail!("Invalid UUID: {}", s.parse_from::<Token>()?)
        }
    }
}
impl Peek for Uuid {
    fn peek(mut s: StatementStream<'_>) -> bool {
        s.parse::<Self>().is_ok()
    }
}

#[derive(ParseFromStr, Clone, Debug, Hash, Eq, PartialEq)]
pub enum Identifier {
    Name(Name),
    Keyword(ReservedKeyword),
}

impl Parse for Identifier {
    type Output = Self;
    fn parse(s: &mut StatementStream<'_>) -> anyhow::Result<Self> {
        if let Some(keyword) = s.parse_if::<ReservedKeyword>() {
            Ok(Identifier::Keyword(keyword?))
        } else {
            Ok(Identifier::Name(s.parse::<Name>()?))
        }
    }
}

impl Peek for Identifier {
    fn peek(s: StatementStream<'_>) -> bool {
        s.check::<ReservedKeyword>() || s.check::<Name>()
    }
}

#[derive(ParseFromStr, Clone, Debug, Hash, Eq, PartialEq)]
pub enum Name {
    Quoted(String),
    Unquoted(String),
}

impl Parse for Name {
    type Output = Self;
    fn parse(s: &mut StatementStream<'_>) -> anyhow::Result<Self> {
        let mut res = String::new();
        if s.peek().map(|c| c == '"').unwrap_or(false) {
            while let Some(c) = s.next() {
                if c == '"' {
                    return Ok(Self::Quoted(res));
                } else {
                    res.push(c);
                }
            }
            anyhow::bail!("End of statement!")
        } else {
            while let Some(c) = s.peek() {
                if c.is_alphanumeric() || c == '_' {
                    s.next();
                    res.push(c);
                } else {
                    break;
                }
            }
            if res.is_empty() {
                anyhow::bail!("End of statement!")
            } else if ReservedKeyword::from_str(&res).is_ok() {
                anyhow::bail!("Invalid name: {} is a reserved keyword", res)
            }
            return Ok(Self::Unquoted(res));
        }
    }
}

impl Peek for Name {
    fn peek(mut s: StatementStream<'_>) -> bool {
        s.parse::<Self>().is_ok()
    }
}

impl Display for Name {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Quoted(s) => write!(f, "\"{}\"", s),
            Self::Unquoted(s) => s.fmt(f),
        }
    }
}

#[derive(ParseFromStr, Clone, Debug)]
pub struct KeyspaceQualifiedName {
    pub keyspace: Option<Name>,
    pub name: Name,
}

impl Parse for KeyspaceQualifiedName {
    type Output = Self;
    fn parse(s: &mut StatementStream<'_>) -> anyhow::Result<Self> {
        let (keyspace, name) = s.parse::<(Option<(Name, Dot)>, Name)>()?;
        Ok(KeyspaceQualifiedName {
            keyspace: keyspace.map(|(i, _)| i),
            name,
        })
    }
}

impl Peek for KeyspaceQualifiedName {
    fn peek(s: StatementStream<'_>) -> bool {
        s.check::<(Option<(Name, Dot)>, Name)>()
    }
}

impl Display for KeyspaceQualifiedName {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        if let Some(keyspace) = &self.keyspace {
            write!(f, "{}.{}", keyspace, self.name)?;
        } else {
            self.name.fmt(f)?;
        }
        Ok(())
    }
}

#[derive(ParseFromStr, Clone, Debug)]
pub struct StatementOpt {
    pub name: Name,
    pub value: StatementOptValue,
}

impl Parse for StatementOpt {
    type Output = Self;
    fn parse(s: &mut StatementStream<'_>) -> anyhow::Result<Self> {
        let (name, _, value) = s.parse::<(Name, Equals, StatementOptValue)>()?;
        Ok(StatementOpt { name, value })
    }
}

impl Display for StatementOpt {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} = {}", self.name, self.value)
    }
}

#[derive(ParseFromStr, Clone, Debug)]
pub enum StatementOptValue {
    Identifier(Name),
    Constant(Constant),
    Map(MapLiteral),
}

impl Parse for StatementOptValue {
    type Output = Self;
    fn parse(s: &mut StatementStream<'_>) -> anyhow::Result<Self> {
        if let Some(map) = s.parse_if::<MapLiteral>() {
            Ok(StatementOptValue::Map(map?))
        } else if let Some(constant) = s.parse_if::<Constant>() {
            Ok(StatementOptValue::Constant(constant?))
        } else if let Some(identifier) = s.parse_if::<Name>() {
            Ok(StatementOptValue::Identifier(identifier?))
        } else {
            anyhow::bail!("Invalid statement option value: {}", s.parse_from::<Token>()?)
        }
    }
}

impl Display for StatementOptValue {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Identifier(identifier) => identifier.fmt(f),
            Self::Constant(constant) => constant.fmt(f),
            Self::Map(map) => map.fmt(f),
        }
    }
}

#[derive(Builder, Clone, Debug)]
pub struct ColumnDefinition {
    pub name: Name,
    pub data_type: CqlType,
    #[builder(default)]
    pub static_column: bool,
    #[builder(default)]
    pub primary_key: bool,
}

impl ColumnDefinition {
    pub fn build() -> ColumnDefinitionBuilder {
        ColumnDefinitionBuilder::default()
    }
}

impl Parse for ColumnDefinition {
    type Output = Self;
    fn parse(s: &mut StatementStream<'_>) -> anyhow::Result<Self> {
        Ok(Self {
            name: s.parse()?,
            data_type: s.parse()?,
            static_column: s.parse::<Option<STATIC>>()?.is_some(),
            primary_key: s.parse::<Option<(PRIMARY, KEY)>>()?.is_some(),
        })
    }
}

impl Peek for ColumnDefinition {
    fn peek(s: StatementStream<'_>) -> bool {
        s.check::<(Name, CqlType)>()
    }
}

impl Display for ColumnDefinition {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} {}", self.name, self.data_type)?;
        if self.static_column {
            write!(f, " STATIC")?;
        }
        if self.primary_key {
            write!(f, " PRIMARY KEY")?;
        }
        Ok(())
    }
}

#[derive(ParseFromStr, Clone, Debug)]
pub struct PrimaryKey {
    pub partition_key: PartitionKey,
    pub clustering_columns: Option<Vec<Name>>,
}

impl Parse for PrimaryKey {
    type Output = Self;
    fn parse(s: &mut StatementStream<'_>) -> anyhow::Result<Self> {
        let (partition_key, clustering_columns) =
            s.parse_from::<(PartitionKey, Option<(Comma, List<Name, Comma>)>)>()?;
        Ok(PrimaryKey {
            partition_key,
            clustering_columns: clustering_columns.map(|i| i.1),
        })
    }
}

impl Display for PrimaryKey {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        self.partition_key.fmt(f)?;
        if let Some(clustering_columns) = &self.clustering_columns {
            write!(
                f,
                ", {}",
                clustering_columns
                    .iter()
                    .map(|i| i.to_string())
                    .collect::<Vec<_>>()
                    .join(", ")
            )?;
        }
        Ok(())
    }
}

#[derive(ParseFromStr, Clone, Debug)]
pub struct PartitionKey {
    pub columns: Vec<Name>,
}

impl Parse for PartitionKey {
    type Output = Self;
    fn parse(s: &mut StatementStream<'_>) -> anyhow::Result<Self> {
        Ok(if let Some(columns) = s.parse_from_if::<Parens<List<Name, Comma>>>() {
            Self { columns: columns? }
        } else {
            Self {
                columns: vec![s.parse::<Name>()?],
            }
        })
    }
}

impl Display for PartitionKey {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self.columns.len() {
            0 => {
                panic!("No partition key columns specified!");
            }
            1 => self.columns[0].fmt(f),
            _ => write!(
                f,
                "({})",
                self.columns
                    .iter()
                    .map(|i| i.to_string())
                    .collect::<Vec<_>>()
                    .join(", ")
            ),
        }
    }
}

// TODO: Scylla encryption opts and caching?
#[derive(Builder, Clone, Debug, Default)]
#[builder(default)]
pub struct TableOpts {
    pub compact_storage: bool,
    pub clustering_order: Option<Vec<ColumnOrder>>,
    pub options: Option<HashMap<Name, StatementOptValue>>,
}

impl Parse for TableOpts {
    type Output = Self;
    fn parse(s: &mut StatementStream<'_>) -> anyhow::Result<Self> {
        let mut res = TableOptsBuilder::default();
        loop {
            if s.parse_if::<(COMPACT, STORAGE)>().is_some() {
                if res.compact_storage.is_some() {
                    anyhow::bail!("Duplicate compact storage option");
                }
                res.compact_storage(true);
                if s.parse::<Option<AND>>()?.is_none() {
                    break;
                }
            } else if s.parse_if::<(CLUSTERING, ORDER, BY)>().is_some() {
                if res.clustering_order.is_some() {
                    anyhow::bail!("Duplicate clustering order option");
                }
                res.clustering_order(Some(s.parse_from::<Parens<List<ColumnOrder, Comma>>>()?));
                if s.parse::<Option<AND>>()?.is_none() {
                    break;
                }
            } else {
                res.options(s.parse_from::<Option<List<StatementOpt, AND>>>()?.map(|i| {
                    i.into_iter().fold(HashMap::new(), |mut acc, opt| {
                        acc.insert(opt.name, opt.value);
                        acc
                    })
                }));
                break;
            }
        }
        Ok(res
            .build()
            .map_err(|e| anyhow::anyhow!("Invalid Table Options: {}", e))?)
    }
}

impl Display for TableOpts {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match (
            self.compact_storage,
            self.clustering_order.as_ref(),
            self.options.as_ref(),
        ) {
            (true, None, None) => write!(f, "COMPACT STORAGE"),
            (true, None, Some(opts)) => write!(
                f,
                "COMPACT STORAGE AND {}",
                opts.iter()
                    .map(|(name, value)| format!("{} = {}", name, value))
                    .collect::<Vec<_>>()
                    .join(" AND ")
            ),
            (true, Some(c), None) => write!(
                f,
                "COMPACT STORAGE AND {}",
                c.iter().map(|i| i.to_string()).collect::<Vec<_>>().join(", ")
            ),
            (true, Some(c), Some(opts)) => write!(
                f,
                "COMPACT STORAGE AND {} AND {}",
                c.iter().map(|i| i.to_string()).collect::<Vec<_>>().join(", "),
                opts.iter()
                    .map(|(name, value)| format!("{} = {}", name, value))
                    .collect::<Vec<_>>()
                    .join(" AND ")
            ),
            (false, None, Some(opts)) => write!(
                f,
                "{}",
                opts.iter()
                    .map(|(name, value)| format!("{} = {}", name, value))
                    .collect::<Vec<_>>()
                    .join(" AND ")
            ),
            (false, Some(c), None) => write!(f, "{}", c.iter().map(|i| i.to_string()).collect::<Vec<_>>().join(", ")),
            (false, Some(c), Some(opts)) => write!(
                f,
                "{} AND {}",
                c.iter().map(|i| i.to_string()).collect::<Vec<_>>().join(", "),
                opts.iter()
                    .map(|(name, value)| format!("{} = {}", name, value))
                    .collect::<Vec<_>>()
                    .join(" AND ")
            ),
            (false, None, None) => panic!("Invalid table options!"),
        }
    }
}

#[derive(ParseFromStr, Clone, Debug)]
pub struct ColumnOrder {
    pub column: Name,
    pub order: Order,
}

impl Parse for ColumnOrder {
    type Output = Self;
    fn parse(s: &mut StatementStream<'_>) -> anyhow::Result<Self::Output> {
        let (column, order) = s.parse::<(Name, Order)>()?;
        Ok(ColumnOrder { column, order })
    }
}

impl Display for ColumnOrder {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} {}", self.column, self.order)
    }
}

#[derive(Copy, Clone, Debug)]
pub enum Order {
    Ascending,
    Descending,
}

impl Parse for Order {
    type Output = Self;
    fn parse(s: &mut StatementStream<'_>) -> anyhow::Result<Self::Output> {
        if s.parse_if::<ASC>().is_some() {
            Ok(Order::Ascending)
        } else if s.parse_if::<DESC>().is_some() {
            Ok(Order::Descending)
        } else {
            anyhow::bail!("Invalid sort order: {}", s.parse_from::<Token>()?)
        }
    }
}

impl Default for Order {
    fn default() -> Self {
        Self::Ascending
    }
}

impl Display for Order {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Ascending => write!(f, "ASC"),
            Self::Descending => write!(f, "DESC"),
        }
    }
}

#[derive(ParseFromStr, Clone, Debug)]
pub enum Relation {
    Normal {
        column: Name,
        operator: Operator,
        term: Term,
    },
    Tuple {
        columns: Vec<Name>,
        operator: Operator,
        tuple_literal: TupleLiteral,
    },
    Token {
        columns: Vec<Name>,
        operator: Operator,
        term: Term,
    },
}

impl Parse for Relation {
    type Output = Self;
    fn parse(s: &mut StatementStream<'_>) -> anyhow::Result<Self> {
        Ok(if s.parse_if::<TOKEN>().is_some() {
            let (columns, operator, term) = s.parse_from::<(Parens<List<Name, Comma>>, Operator, Term)>()?;
            Relation::Token {
                columns,
                operator,
                term,
            }
        } else if s.check::<LeftParen>() {
            let (columns, operator, tuple_literal) =
                s.parse_from::<(Parens<List<Name, Comma>>, Operator, TupleLiteral)>()?;
            Relation::Tuple {
                columns,
                operator,
                tuple_literal,
            }
        } else {
            let (column, operator, term) = s.parse()?;
            Relation::Normal { column, operator, term }
        })
    }
}

impl Display for Relation {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Relation::Normal { column, operator, term } => write!(f, "{} {} {}", column, operator, term),
            Relation::Tuple {
                columns,
                operator,
                tuple_literal,
            } => write!(
                f,
                "({}) {} {}",
                columns.iter().map(|c| c.to_string()).collect::<Vec<_>>().join(", "),
                operator,
                tuple_literal
            ),
            Relation::Token {
                columns,
                operator,
                term,
            } => write!(
                f,
                "TOKEN ({}) {} {}",
                columns.iter().map(|c| c.to_string()).collect::<Vec<_>>().join(", "),
                operator,
                term
            ),
        }
    }
}

#[derive(Clone, Debug)]
pub enum Replication {
    SimpleStrategy(i32),
    NetworkTopologyStrategy(HashMap<String, i32>),
}

impl Replication {
    pub fn simple(replication_factor: i32) -> Self {
        Replication::SimpleStrategy(replication_factor)
    }

    pub fn network_topology(replication_map: HashMap<String, i32>) -> Self {
        Replication::NetworkTopologyStrategy(replication_map)
    }
}

impl Display for Replication {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Replication::SimpleStrategy(i) => write!(f, "{{'class': 'SimpleStrategy', 'replication_factor': {}}}", i),
            Replication::NetworkTopologyStrategy(i) => write!(
                f,
                "{{'class': 'NetworkTopologyStrategy', {}}}",
                i.iter()
                    .map(|(k, v)| format!("'{}': {}", k, v))
                    .collect::<Vec<_>>()
                    .join(", ")
            ),
        }
    }
}

#[derive(Clone, Debug)]
pub enum SpeculativeRetry {
    None,
    Always,
    Percentile(f32),
    Custom(String),
}

impl Default for SpeculativeRetry {
    fn default() -> Self {
        SpeculativeRetry::Percentile(99.0)
    }
}

impl Display for SpeculativeRetry {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            SpeculativeRetry::None => write!(f, "'NONE'"),
            SpeculativeRetry::Always => write!(f, "'ALWAYS'"),
            SpeculativeRetry::Percentile(p) => write!(f, "'{:.1}PERCENTILE'", p),
            SpeculativeRetry::Custom(s) => write!(f, "'{}'", s),
        }
    }
}

#[derive(Builder, Copy, Clone, Debug)]
#[builder(default)]
pub struct SizeTieredCompactionStrategy {
    enabled: bool,
    tombstone_threshhold: f32,
    tombsone_compaction_interval: i32,
    log_all: bool,
    unchecked_tombstone_compaction: bool,
    only_purge_repaired_tombstone: bool,
    min_threshold: i32,
    max_threshold: i32,
    min_sstable_size: i32,
    bucket_low: f32,
    bucket_high: f32,
}

impl CompactionType for SizeTieredCompactionStrategy {}

impl Default for SizeTieredCompactionStrategy {
    fn default() -> Self {
        Self {
            enabled: true,
            tombstone_threshhold: 0.2,
            tombsone_compaction_interval: 86400,
            log_all: false,
            unchecked_tombstone_compaction: false,
            only_purge_repaired_tombstone: false,
            min_threshold: 4,
            max_threshold: 32,
            min_sstable_size: 50,
            bucket_low: 0.5,
            bucket_high: 1.5,
        }
    }
}

impl Display for SizeTieredCompactionStrategy {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{{'class': 'SizeTieredCompactionStrategy', 'enabled': {}, 'tombstone_threshold': {}, 'tombstone_compaction_interval': {}, \
            'log_all': {}, 'unchecked_tombstone_compaction': {}, 'only_purge_repaired_tombstone': {}, 'min_threshold': {}, \
            'max_threshold': {}, 'min_sstable_size': {}, 'bucket_low': {:.1}, 'bucket_high': {:.1}}}",
            self.enabled,
            self.tombstone_threshhold,
            self.tombsone_compaction_interval,
            self.log_all,
            self.unchecked_tombstone_compaction,
            self.only_purge_repaired_tombstone,
            self.min_threshold,
            self.max_threshold,
            self.min_sstable_size,
            self.bucket_low,
            self.bucket_high
        )
    }
}

#[derive(Builder, Copy, Clone, Debug)]
#[builder(default)]
pub struct LeveledCompactionStrategy {
    enabled: bool,
    tombstone_threshhold: f32,
    tombsone_compaction_interval: i32,
    log_all: bool,
    unchecked_tombstone_compaction: bool,
    only_purge_repaired_tombstone: bool,
    min_threshold: i32,
    max_threshold: i32,
    sstable_size_in_mb: i32,
    fanout_size: i32,
}

impl CompactionType for LeveledCompactionStrategy {}

impl Default for LeveledCompactionStrategy {
    fn default() -> Self {
        Self {
            enabled: true,
            tombstone_threshhold: 0.2,
            tombsone_compaction_interval: 86400,
            log_all: false,
            unchecked_tombstone_compaction: false,
            only_purge_repaired_tombstone: false,
            min_threshold: 4,
            max_threshold: 32,
            sstable_size_in_mb: 160,
            fanout_size: 10,
        }
    }
}

impl Display for LeveledCompactionStrategy {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{{'class': 'LeveledCompactionStrategy', 'enabled': {}, 'tombstone_threshold': {:.1}, 'tombstone_compaction_interval': {}, \
            'log_all': {}, 'unchecked_tombstone_compaction': {}, 'only_purge_repaired_tombstone': {}, 'min_threshold': {}, \
            'max_threshold': {}, 'sstable_size_in_mb': {}, 'fanout_size': {}}}",
            self.enabled,
            self.tombstone_threshhold,
            self.tombsone_compaction_interval,
            self.log_all,
            self.unchecked_tombstone_compaction,
            self.only_purge_repaired_tombstone,
            self.min_threshold,
            self.max_threshold,
            self.sstable_size_in_mb,
            self.fanout_size
        )
    }
}

#[derive(Builder, Copy, Clone, Debug)]
#[builder(default)]
pub struct TimeWindowCompactionStrategy {
    enabled: bool,
    tombstone_threshhold: f32,
    tombsone_compaction_interval: i32,
    log_all: bool,
    unchecked_tombstone_compaction: bool,
    only_purge_repaired_tombstone: bool,
    min_threshold: i32,
    max_threshold: i32,
    compaction_window_unit: JavaTimeUnit,
    compaction_window_size: i32,
    unsafe_aggressive_sstable_expiration: bool,
}

impl CompactionType for TimeWindowCompactionStrategy {}

impl Default for TimeWindowCompactionStrategy {
    fn default() -> Self {
        Self {
            enabled: true,
            tombstone_threshhold: 0.2,
            tombsone_compaction_interval: 86400,
            log_all: false,
            unchecked_tombstone_compaction: false,
            only_purge_repaired_tombstone: false,
            min_threshold: 4,
            max_threshold: 32,
            compaction_window_unit: JavaTimeUnit::Days,
            compaction_window_size: 1,
            unsafe_aggressive_sstable_expiration: false,
        }
    }
}

impl Display for TimeWindowCompactionStrategy {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{{'class': 'TimeWindowCompactionStrategy', 'enabled': {}, 'tombstone_threshold': {:.1}, 'tombstone_compaction_interval': {}, \
            'log_all': {}, 'unchecked_tombstone_compaction': {}, 'only_purge_repaired_tombstone': {}, 'min_threshold': {}, \
            'max_threshold': {}, 'compaction_window_unit': '{}', 'compaction_window_size': {}, 'unsafe_aggressive_sstable_expiration': {}}}",
            self.enabled,
            self.tombstone_threshhold,
            self.tombsone_compaction_interval,
            self.log_all,
            self.unchecked_tombstone_compaction,
            self.only_purge_repaired_tombstone,
            self.min_threshold,
            self.max_threshold,
            self.compaction_window_unit,
            self.compaction_window_size,
            self.unsafe_aggressive_sstable_expiration,
        )
    }
}

pub trait CompactionType: Display {}

pub enum Compaction {
    SizeTiered(SizeTieredCompactionStrategy),
    Leveled(LeveledCompactionStrategy),
    TimeWindow(TimeWindowCompactionStrategy),
}

impl Compaction {
    pub fn size_tiered() -> SizeTieredCompactionStrategyBuilder
    where
        Self: Sized,
    {
        SizeTieredCompactionStrategyBuilder::default()
    }

    pub fn leveled() -> LeveledCompactionStrategyBuilder
    where
        Self: Sized,
    {
        LeveledCompactionStrategyBuilder::default()
    }

    pub fn time_window() -> TimeWindowCompactionStrategyBuilder
    where
        Self: Sized,
    {
        TimeWindowCompactionStrategyBuilder::default()
    }
}

impl Display for Compaction {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Compaction::SizeTiered(s) => s.fmt(f),
            Compaction::Leveled(s) => s.fmt(f),
            Compaction::TimeWindow(s) => s.fmt(f),
        }
    }
}

#[derive(Copy, Clone, Debug)]
pub enum JavaTimeUnit {
    Minutes,
    Hours,
    Days,
}

impl Display for JavaTimeUnit {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            JavaTimeUnit::Minutes => write!(f, "MINUTES"),
            JavaTimeUnit::Hours => write!(f, "HOURS"),
            JavaTimeUnit::Days => write!(f, "DAYS"),
        }
    }
}

#[derive(Builder, Clone, Debug)]
#[builder(default)]
pub struct Compression {
    #[builder(default = "default_compressor()")]
    class: String,
    enabled: bool,
    chunk_length_in_kb: i32,
    crc_check_chance: f32,
    compression_level: i32,
}

impl Compression {
    pub fn build() -> CompressionBuilder {
        CompressionBuilder::default()
    }
}

impl Default for Compression {
    fn default() -> Self {
        Self {
            class: default_compressor(),
            enabled: true,
            chunk_length_in_kb: 64,
            crc_check_chance: 1.0,
            compression_level: 3,
        }
    }
}

impl Display for Compression {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{{'class': '{}', 'enabled': {}, 'chunk_length_in_kb': {}, 'crc_check_chance': {:.1}, 'compression_level': {}}}",
            self.class, self.enabled, self.chunk_length_in_kb, self.crc_check_chance, self.compression_level
        )
    }
}

fn default_compressor() -> String {
    "LZ4Compressor".to_string()
}

#[derive(Builder, Clone, Debug)]
#[builder(default)]
pub struct Caching {
    keys: Keys,
    rows_per_partition: RowsPerPartition,
}

impl Caching {
    pub fn build() -> CachingBuilder {
        CachingBuilder::default()
    }
}

impl Default for Caching {
    fn default() -> Self {
        Self {
            keys: Keys::All,
            rows_per_partition: RowsPerPartition::None,
        }
    }
}

impl Display for Caching {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{{'keys': {}, 'rows_per_partition': {}}}",
            self.keys, self.rows_per_partition
        )
    }
}

#[derive(Copy, Clone, Debug)]
pub enum Keys {
    All,
    None,
}

impl Display for Keys {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Keys::All => write!(f, "'ALL'"),
            Keys::None => write!(f, "'NONE'"),
        }
    }
}

#[derive(Copy, Clone, Debug)]
pub enum RowsPerPartition {
    All,
    None,
    Count(i32),
}

impl Display for RowsPerPartition {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            RowsPerPartition::All => write!(f, "'ALL'"),
            RowsPerPartition::None => write!(f, "'NONE'"),
            RowsPerPartition::Count(count) => count.fmt(f),
        }
    }
}

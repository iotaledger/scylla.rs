use std::{marker::PhantomData, str::FromStr};
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
    pub(crate) fn new(statement: &'a str) -> Self {
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
        while let Some(c) = self.cursor.peek() {
            if c.is_whitespace() {
                self.cursor.next();
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

pub trait Parse {
    type Output;
    fn parse(s: &mut StatementStream<'_>) -> anyhow::Result<Self::Output>;
}

impl Parse for char {
    type Output = char;
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

macro_rules! peek_parse_number {
    ($n:ident, $t:ident) => {
        impl Parse for $n {
            type Output = $n;
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
    type Output = Nothing;
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
    type Output = Whitespace;
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
    type Output = String;
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

#[derive(Clone, Debug)]
pub enum BindMarker {
    Anonymous,
    Named(Name),
}

impl Parse for BindMarker {
    type Output = BindMarker;
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

impl Parse for Uuid {
    type Output = Uuid;
    fn parse(s: &mut StatementStream<'_>) -> anyhow::Result<Self::Output> {
        if let Some(u) = s.nextn(36) {
            Ok(Uuid::parse_str(&u)?)
        } else {
            anyhow::bail!("Invalid UUID!")
        }
    }
}
impl Peek for Uuid {
    fn peek(mut s: StatementStream<'_>) -> bool {
        s.parse::<Self>().is_ok()
    }
}

#[derive(Clone, Debug, Hash, Eq, PartialEq)]
pub enum Identifier {
    Name(Name),
    Keyword(ReservedKeyword),
}

impl Parse for Identifier {
    type Output = Identifier;
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

#[derive(Clone, Debug, Hash, Eq, PartialEq)]
pub enum Name {
    Quoted(String),
    Unquoted(String),
}

impl Parse for Name {
    type Output = Name;
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

#[derive(Clone, Debug)]
pub struct TableName {
    pub keyspace: Option<Name>,
    pub name: Name,
}

impl Parse for TableName {
    type Output = TableName;
    fn parse(s: &mut StatementStream<'_>) -> anyhow::Result<Self> {
        let (keyspace, name) = s.parse::<(Option<(Name, Dot)>, Name)>()?;
        Ok(TableName {
            keyspace: keyspace.map(|(i, _)| i),
            name,
        })
    }
}

impl Peek for TableName {
    fn peek(s: StatementStream<'_>) -> bool {
        s.check::<(Option<(Name, Dot)>, Name)>()
    }
}

pub struct StatementOpt {
    pub name: Name,
    pub value: StatementOptValue,
}

pub enum StatementOptValue {
    Identifier(Name),
    Constant(Constant),
    Map(MapLiteral),
}

pub struct ColumnDefinition {
    pub name: Name,
    pub data_type: CqlType,
    pub static_column: bool,
    pub primary_key: bool,
}

pub struct PrimaryKey {
    pub partition_key: PartitionKey,
    pub clustering_columns: Vec<Name>,
}

pub struct PartitionKey {
    pub columns: Vec<Name>,
}

pub enum TableOpt {
    CompactStorage,
    ClusteringOrder(Vec<ColumnOrder>),
    Options(Vec<StatementOpt>),
}

#[derive(Clone, Debug)]
pub struct ColumnOrder {
    pub column: Name,
    pub order: Order,
}

impl Parse for ColumnOrder {
    type Output = ColumnOrder;
    fn parse(s: &mut StatementStream<'_>) -> anyhow::Result<Self::Output> {
        let (column, order) = s.parse::<(Name, Order)>()?;
        Ok(ColumnOrder { column, order })
    }
}

#[derive(Copy, Clone, Debug)]
pub enum Order {
    Ascending,
    Descending,
}

impl Parse for Order {
    type Output = Order;
    fn parse(s: &mut StatementStream<'_>) -> anyhow::Result<Self::Output> {
        if s.parse_if::<ASC>().is_some() {
            Ok(Order::Ascending)
        } else if s.parse_if::<DESC>().is_some() {
            Ok(Order::Descending)
        } else {
            anyhow::bail!("Invalid sort order!")
        }
    }
}

impl Default for Order {
    fn default() -> Self {
        Self::Ascending
    }
}

#[derive(Clone, Debug)]
pub struct FromClause {
    pub table: TableName,
}

impl Parse for FromClause {
    type Output = FromClause;
    fn parse(s: &mut StatementStream<'_>) -> anyhow::Result<Self> {
        let (_, table) = s.parse::<(FROM, TableName)>()?;
        Ok(FromClause { table })
    }
}

impl Peek for FromClause {
    fn peek(s: StatementStream<'_>) -> bool {
        s.check::<FROM>()
    }
}

#[derive(Clone, Debug)]
pub struct WhereClause {
    pub relations: Vec<Relation>,
}

impl Parse for WhereClause {
    type Output = WhereClause;
    fn parse(s: &mut StatementStream<'_>) -> anyhow::Result<Self> {
        let (_, relations) = s.parse_from::<(WHERE, List<Relation, AND>)>()?;
        Ok(WhereClause { relations })
    }
}

impl Peek for WhereClause {
    fn peek(s: StatementStream<'_>) -> bool {
        s.check::<WHERE>()
    }
}

#[derive(Clone, Debug)]
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
    type Output = Relation;
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

#[derive(Clone, Debug)]
pub struct GroupByClause {
    pub columns: Vec<Name>,
}

impl Parse for GroupByClause {
    type Output = GroupByClause;
    fn parse(s: &mut StatementStream<'_>) -> anyhow::Result<Self> {
        let (_, _, columns) = s.parse_from::<(GROUP, BY, List<Name, Comma>)>()?;
        Ok(GroupByClause { columns })
    }
}

impl Peek for GroupByClause {
    fn peek(s: StatementStream<'_>) -> bool {
        s.check::<(GROUP, BY)>()
    }
}

#[derive(Clone, Debug)]
pub struct OrderingClause {
    pub columns: Vec<ColumnOrder>,
}

impl Parse for OrderingClause {
    type Output = OrderingClause;
    fn parse(s: &mut StatementStream<'_>) -> anyhow::Result<Self> {
        let (_, _, columns) = s.parse_from::<(GROUP, BY, List<ColumnOrder, Comma>)>()?;
        Ok(OrderingClause { columns })
    }
}

impl Peek for OrderingClause {
    fn peek(s: StatementStream<'_>) -> bool {
        s.check::<(ORDER, BY)>()
    }
}

#[derive(Clone, Debug)]
pub enum Limit {
    Literal(i32),
    BindMarker(BindMarker),
}

impl Parse for Limit {
    type Output = Limit;
    fn parse(s: &mut StatementStream<'_>) -> anyhow::Result<Self::Output> {
        if let Some(bind) = s.parse_if::<BindMarker>() {
            Ok(Limit::BindMarker(bind?))
        } else {
            Ok(Limit::Literal(s.parse::<i32>()?))
        }
    }
}

impl Peek for Limit {
    fn peek(s: StatementStream<'_>) -> bool {
        s.check::<i32>() || s.check::<BindMarker>()
    }
}

#[derive(Copy, Clone, Debug)]
pub enum ColumnDefault {
    Null,
    Unset,
}

impl Parse for ColumnDefault {
    type Output = ColumnDefault;
    fn parse(s: &mut StatementStream<'_>) -> anyhow::Result<Self::Output> {
        if s.parse_if::<NULL>().is_some() {
            Ok(ColumnDefault::Null)
        } else if s.parse_if::<UNSET>().is_some() {
            Ok(ColumnDefault::Unset)
        } else {
            anyhow::bail!("Invalid column default!")
        }
    }
}

impl Peek for ColumnDefault {
    fn peek(s: StatementStream<'_>) -> bool {
        s.check::<NULL>() || s.check::<UNSET>()
    }
}

mod test {
    #[test]
    fn test_parse_select() {
        let mut stream = super::StatementStream::new(
            "SELECT time, value
            FROM my_keyspace.events
            WHERE event_type = 'myEvent'
            AND time > '2011-02-03'
            AND time <= '2012-01-01'",
        );
        let statement = stream.parse::<super::SelectStatement>().unwrap();
        println!("{:#?}", statement);
    }

    #[test]
    fn test_parse_insert() {
        let mut stream = super::StatementStream::new(
            "INSERT INTO NerdMovies (movie, director, main_actor, year)
            VALUES ('Serenity', 'Joss Whedon', 'Nathan Fillion', 2005)
            USING TTL 86400 IF NOT EXISTS;",
        );
        let statement = stream.parse::<super::InsertStatement>().unwrap();
        println!("{:#?}", statement);
    }

    #[test]
    fn test_parse_update() {
        let mut stream = super::StatementStream::new(
            "UPDATE NerdMovies
            SET director = 'Joss Whedon', main_actor = 'Nathan Fillion'
            WHERE movie = 'Serenity'
            IF EXISTS;",
        );
        let statement = stream.parse::<super::UpdateStatement>().unwrap();
        println!("{:#?}", statement);
    }

    #[test]
    fn test_parse_delete() {
        let mut stream = super::StatementStream::new(
            "DELETE FROM NerdMovies
            WHERE movie = 'Serenity'
            IF EXISTS;",
        );
        let statement = stream.parse::<super::DeleteStatement>().unwrap();
        println!("{:#?}", statement);
    }

    #[test]
    fn test_parse_batch() {
        let mut stream = super::StatementStream::new(
            "BEGIN BATCH
            INSERT INTO NerdMovies (movie, director, main_actor, year)
            VALUES ('Serenity', 'Joss Whedon', 'Nathan Fillion', 2005)
            USING TTL 86400 IF NOT EXISTS;
            UPDATE NerdMovies
            SET director = 'Joss Whedon', main_actor = 'Nathan Fillion'
            WHERE movie = 'Serenity'
            IF EXISTS;
            DELETE FROM NerdMovies
            WHERE movie = 'Serenity'
            IF EXISTS;
            APPLY BATCH;",
        );
        let statement = stream.parse::<super::BatchStatement>().unwrap();
        println!("{:#?}", statement);
    }
}

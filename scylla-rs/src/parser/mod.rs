use std::str::FromStr;

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

    pub fn peek(&mut self) -> Option<char> {
        self.cursor.peek().map(|c| *c)
    }

    pub fn peekn(&mut self, n: usize) -> Option<char> {
        let mut cursor = self.cursor.clone();
        for _ in 0..n - 1 {
            cursor.next();
        }
        cursor.next()
    }

    pub fn next(&mut self) -> Option<char> {
        self.cursor.next()
    }

    fn ignore_empty(&mut self) {
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
        this.ignore_empty();
        P::peek(this)
    }

    pub fn parse_if<P: Peek + Parse<Output = P>>(&mut self) -> Option<anyhow::Result<P>> {
        let mut this = self.clone();
        this.ignore_empty();
        self.parse::<Option<P>>().transpose()
    }

    pub fn parse_from_if<P: Peek + Parse>(&mut self) -> Option<anyhow::Result<P::Output>> {
        let mut this = self.clone();
        this.ignore_empty();
        self.parse_from::<Option<P>>().transpose()
    }

    pub fn parse<P: Parse<Output = P>>(&mut self) -> anyhow::Result<P> {
        self.ignore_empty();
        P::parse(self)
    }

    pub fn parse_from<P: Parse>(&mut self) -> anyhow::Result<P::Output> {
        self.ignore_empty();
        P::parse(self)
    }
}

mod test {
    #[test]
    fn test_parse_select() {
        let mut stream = super::StatementStream::new(
            "SELECT time, value
            FROM events
            WHERE event_type = 'myEvent'
              AND time > '2011-02-03'
              AND time <= '2012-01-01'",
        );
        while let Ok(token) = stream.parse::<super::SelectStatement>() {
            println!("{:?}", token);
        }
    }
}

pub trait Peek {
    fn peek(s: StatementStream<'_>) -> bool;
}

macro_rules! peek_parse_tuple {
    ($($t:ident),+) => {
        impl<$($t: Peek + Parse),+> Peek for ($($t),+,) {
            fn peek(s: StatementStream<'_>) -> bool {
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
        s.cursor.next().ok_or_else(|| anyhow::anyhow!("End of statement!"))
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

impl<T: Parse> Parse for Vec<T> {
    type Output = Vec<T::Output>;
    fn parse(s: &mut StatementStream<'_>) -> anyhow::Result<Self::Output> {
        let res = vec![s.parse_from::<T>()?];
        while s.parse_from_if::<Comma>().is_some() {
            res.push(s.parse_from::<T>()?);
        }
        Ok(res)
    }
}

pub struct Token(String);
impl Parse for Token {
    type Output = String;
    fn parse(s: &mut StatementStream<'_>) -> anyhow::Result<Self::Output> {
        let res = String::new();
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
    fn peek(s: StatementStream<'_>) -> bool {
        s.peek().is_some()
    }
}

pub struct Alphanumeric(String);
impl Parse for Alphanumeric {
    type Output = String;
    fn parse(s: &mut StatementStream<'_>) -> anyhow::Result<Self::Output> {
        let res = String::new();
        while let Some(c) = s.next() {
            if c.is_alphanumeric() {
                res.push(c);
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
    fn peek(s: StatementStream<'_>) -> bool {
        s.peek().map(|c| c.is_alphanumeric()).unwrap_or(false)
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

#[derive(Copy, Clone, Debug)]
pub enum ArithmeticOp {
    Add,
    Sub,
    Mul,
    Div,
    Mod,
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

impl Parse for Operator {
    type Output = Operator;
    fn parse(s: &mut StatementStream<'_>) -> anyhow::Result<Self> {
        if let Some(res) = s.parse_if::<(Keyword, Option<Keyword>)>() {
            let (first, second) = res?;
            Ok(match (first, second) {
                (Keyword::IN, _) => Operator::In,
                (Keyword::CONTAINS, _) => Operator::Contains,
                (Keyword::CONTAINS, Some(Keyword::KEY)) => Operator::ContainsKey,
                _ => anyhow::bail!("Invalid keyword operator!"),
            })
        } else if let (Some(first), second) = (s.next(), s.peek()) {
            match (first, second) {
                ('=', _) => Ok(Operator::Equal),
                ('!', Some('=')) => Ok(Operator::NotEqual),
                ('>', Some('=')) => Ok(Operator::GreaterThanOrEqual),
                ('<', Some('=')) => Ok(Operator::LessThanOrEqual),
                ('>', _) => Ok(Operator::GreaterThan),
                ('<', _) => Ok(Operator::LessThan),
                _ => anyhow::bail!("Invalid operator"),
            }
        } else {
            anyhow::bail!("Invalid token for operator!")
        }
    }
}

#[derive(Clone, Debug)]
pub enum BindMarker {
    Anonymous,
    Named(Identifier),
}

#[derive(Clone, Debug)]
pub enum Term {
    Constant(Constant),
    Literal(CqlTypeLiteral),
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
        todo!()
    }
}

impl Peek for Term {
    fn peek(s: StatementStream<'_>) -> bool {
        todo!()
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

// impl Parse for Constant {
//    fn parse(input: ParseStream) -> syn::Result<Self> {
//        let lookahead = input.lookahead1();
//        Ok(if lookahead.peek(NULL) {
//            Constant::Null
//        } else if lookahead.peek(NAN) {
//            Constant::Float(Keyword::NAN.to_string())
//        } else if lookahead.peek(INFINITY) {
//            Constant::Float(Keyword::INFINITY.to_string())
//        } else if lookahead.peek(syn::Ident::peek_any) {
//            let id = syn::Ident::parse_any(input)?.to_string().to_lowercase();
//            if let Some(captures) = STRING_REGEX.captures(&id) {
//                Constant::String(
//                    captures
//                        .get(1)
//                        .unwrap_or_else(|| captures.get(2).unwrap())
//                        .as_str()
//                        .to_string(),
//                )
//            } else if regex::Regex::new(INTEGER_REGEX).unwrap().is_match(&id) {
//                Constant::Integer(id)
//            } else if regex::Regex::new(FLOAT_REGEX).unwrap().is_match(&id) {
//                Constant::Float(id)
//            } else if regex::Regex::new(BOOLEAN_REGEX).unwrap().is_match(&id) {
//                Constant::Boolean(id == "true")
//            } else if regex::Regex::new(UUID_REGEX).unwrap().is_match(&id) {
//                Constant::Uuid(Uuid::parse_str(&id).unwrap())
//            } else if regex::Regex::new(HEX_REGEX).unwrap().is_match(&id) {
//                Constant::Hex(hex::decode(id).unwrap())
//            } else if regex::Regex::new(BLOB_REGEX).unwrap().is_match(&id) {
//                Constant::Blob(hex::decode(id).unwrap())
//            } else {
//                return Err(lookahead.error());
//            }
//        } else {
//            return Err(lookahead.error());
//        })
//    }
//}

#[derive(Clone, Debug)]
pub enum Identifier {
    Name(Name),
    Keyword(Keyword),
}

impl Parse for Identifier {
    type Output = Identifier;
    fn parse(s: &mut StatementStream<'_>) -> anyhow::Result<Self> {
        if let Some(keyword) = s.parse_if::<Keyword>() {
            Ok(Identifier::Keyword(keyword?))
        } else {
            Ok(Identifier::Name(s.parse::<Name>()?))
        }
    }
}

impl Peek for Identifier {
    fn peek(s: StatementStream<'_>) -> bool {
        match s.parse_from::<Token>() {
            Ok(t) => Keyword::from_str(&t).is_ok() || UNQUOTED_REGEX.is_match(&t) || QUOTED_REGEX.is_match(&t),
            Err(e) => false,
        }
    }
}

#[derive(Clone, Debug)]
pub enum Name {
    Quoted(String),
    Unquoted(String),
}

impl Parse for Name {
    type Output = Name;
    fn parse(s: &mut StatementStream<'_>) -> anyhow::Result<Self> {
        if s.parse_if::<Quote>().is_some() {
            let res = s.parse_from::<Alphanumeric>()?;
            s.parse::<Quote>()?;
            Ok(Name::Quoted(res))
        } else {
            Ok(Name::Unquoted(s.parse_from::<Alphanumeric>()?))
        }
    }
}

impl Peek for Name {
    fn peek(s: StatementStream<'_>) -> bool {
        s.parse_if::<Quote>();
        s.check::<Alphanumeric>()
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
    pub name: Identifier,
    pub value: StatementOptValue,
}

pub enum StatementOptValue {
    Identifier(Identifier),
    Constant(Constant),
    Map(MapLiteral),
}

pub struct ColumnDefinition {
    pub name: Identifier,
    pub data_type: CqlType,
    pub static_column: bool,
    pub primary_key: bool,
}

pub struct PrimaryKey {
    pub partition_key: PartitionKey,
    pub clustering_columns: Vec<Identifier>,
}

pub struct PartitionKey {
    pub columns: Vec<Identifier>,
}

pub enum TableOpt {
    CompactStorage,
    ClusteringOrder(Vec<ColumnOrder>),
    Options(Vec<StatementOpt>),
}

#[derive(Clone, Debug)]
pub struct ColumnOrder {
    pub column: Identifier,
    pub order: Order,
}

// impl Parse for ColumnOrder {
//    fn parse(input: ParseStream) -> syn::Result<Self> {
//        let column = input.parse::<Identifier>()?;
//        let lookahead = input.lookahead1();
//        let order = if lookahead.peek(ASC) {
//            input.parse::<ASC>()?;
//            Order::Ascending
//        } else if lookahead.peek(DESC) {
//            input.parse::<DESC>()?;
//            Order::Descending
//        } else {
//            Order::default()
//        };
//        Ok(ColumnOrder { column, order })
//    }
//}

#[derive(Copy, Clone, Debug)]
pub enum Order {
    Ascending,
    Descending,
}

impl Default for Order {
    fn default() -> Self {
        Self::Ascending
    }
}

#[derive(Clone, Debug)]
pub struct WhereClause {
    pub relations: Vec<Relation>,
}

impl Parse for WhereClause {
    type Output = WhereClause;
    fn parse(s: &mut StatementStream<'_>) -> anyhow::Result<Self> {
        let (_, relations) = s.parse::<(WHERE, Vec<Relation>)>()?;
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
        column: Identifier,
        operator: Operator,
        term: Term,
    },
    Tuple {
        columns: Vec<Identifier>,
        operator: Operator,
        tuple_literal: TupleLiteral,
    },
    Token {
        columns: Vec<Identifier>,
        operator: Operator,
        term: Term,
    },
}

impl Parse for Relation {
    type Output = Relation;
    fn parse(s: &mut StatementStream<'_>) -> anyhow::Result<Self> {
        Ok(if s.parse_if::<TOKEN>().is_some() {
            let (columns, operator, term) = s.parse_from::<(Parens<Vec<Identifier>>, Operator, Term)>()?;
            Relation::Token {
                columns,
                operator,
                term,
            }
        } else if s.check::<LeftParen>() {
            let (columns, operator, tuple_literal) =
                s.parse_from::<(Parens<Vec<Identifier>>, Operator, TupleLiteral)>()?;
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
    pub columns: Vec<Identifier>,
}

// impl Parse for GroupByClause {
//    fn parse(input: ParseStream) -> syn::Result<Self> {
//        input.parse::<GROUP>()?;
//        input.parse::<BY>()?;
//        let mut columns = vec![input.parse()?];
//        while input.peek(syn::Token![,]) {
//            input.parse::<syn::Token![,]>()?;
//            columns.push(input.parse()?);
//        }
//        Ok(Self { columns })
//    }
//}
// impl CustomToken for GroupByClause {
//    fn peek(cursor: syn::buffer::Cursor) -> bool {
//        if let Some((group, rest)) = cursor.ident() {
//            group == "GROUP" && rest.ident().unwrap().0 == "BY"
//        } else {
//            false
//        }
//    }
//
//    fn display() -> &'static str {
//        "GROUP BY clause"
//    }
//}

#[derive(Clone, Debug)]
pub struct OrderingClause {
    pub columns: Vec<ColumnOrder>,
}

// impl Parse for OrderingClause {
//    fn parse(input: ParseStream) -> syn::Result<Self> {
//        input.parse::<ORDER>()?;
//        input.parse::<BY>()?;
//        let mut columns = vec![input.parse()?];
//        while input.peek(syn::Token![,]) {
//            input.parse::<syn::Token![,]>()?;
//            columns.push(input.parse()?);
//        }
//        Ok(Self { columns })
//    }
//}
// impl CustomToken for OrderingClause {
//    fn peek(cursor: syn::buffer::Cursor) -> bool {
//        if let Some((order, rest)) = cursor.ident() {
//            order == "ORDER" && rest.ident().unwrap().0 == "BY"
//        } else {
//            false
//        }
//    }
//
//    fn display() -> &'static str {
//        "ORDER BY clause"
//    }
//}

#[derive(Clone, Debug)]
pub enum MaybeBound {
    Literal(i32),
    BindMarker(BindMarker),
}

// impl Parse for MaybeBound {
//    fn parse(input: ParseStream) -> syn::Result<Self> {
//        todo!()
//    }
//}
// impl CustomToken for MaybeBound {
//    fn peek(cursor: syn::buffer::Cursor) -> bool {
//        if let Some((lit, _rest)) = cursor.literal() {
//            return regex::Regex::new(INTEGER_REGEX).unwrap().is_match(&lit.to_string());
//        }
//        if let Some((id, rest)) = cursor.ident() {
//            if id == "?" {
//                return true;
//            } else {
//                todo!();
//            }
//        } else {
//            false
//        }
//    }
//
//    fn display() -> &'static str {
//        "bind marker or literal"
//    }
//}

pub enum ColumnDefault {
    Null,
    Unset,
}

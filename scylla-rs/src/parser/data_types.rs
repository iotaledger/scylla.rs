use super::{
    keywords::*,
    Angles,
    Identifier,
    Name,
    Parse,
    Peek,
    Term,
    Token,
};
use std::{
    collections::HashMap,
    net::IpAddr,
    str::FromStr,
};
use uuid::Uuid;

#[derive(Clone, Debug)]
pub enum CqlTypeLiteral {
    Native(NativeTypeLiteral),
    Collection(CollectionTypeLiteral),
    UserDefined(UserDefinedTypeLiteral),
    Tuple(TupleLiteral),
    Custom(String),
}

// impl Parse for CqlTypeLiteral {
//     fn parse(input: syn::parse::ParseStream) -> syn::Result<Self> {
//         todo!()
//     }
// }

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
    fn parse(s: &mut super::StatementStream<'_>) -> anyhow::Result<Self::Output>
    where
        Self: Sized,
    {
        Ok(if let Some(c) = s.parse_if() {
            Self::Collection(c?)
        } else if s.parse_if::<TUPLE>().is_some() {
            Self::Tuple(s.parse_from::<Angles<Vec<CqlType>>>()?)
        } else if let Some(n) = s.parse_if() {
            Self::Native(n?)
        } else if let Some(udt) = s.parse_if() {
            Self::UserDefined(udt?)
        } else if let Some(c) = s.parse_from_if::<Token>() {
            Self::Custom(c?)
        } else {
            anyhow::bail!("Invalid CQL Type!")
        })
    }
}

#[derive(Clone, Debug)]
pub enum NativeTypeLiteral {
    Ascii(Vec<char>),
    Bigint(i64),
    Blob(Vec<u8>),
    Boolean(bool),
    Counter(i64),
    Date(u32),
    Decimal(String),
    Double(f64),
    Duration { months: i32, days: i32, nanos: i64 },
    Float(f32),
    Inet(IpAddr),
    Int(i32),
    Smallint(i16),
    Text(String),
    Time(i64),
    Timestamp(i64),
    Timeuuid(Uuid),
    Tinyint(i8),
    Uuid(Uuid),
    Varchar(String),
    Varint(String),
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

impl FromStr for NativeType {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(match s {
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
    fn parse(s: &mut super::StatementStream<'_>) -> anyhow::Result<Self::Output>
    where
        Self: Sized,
    {
        let token = s.parse_from::<Token>()?;
        NativeType::from_str(&token)
    }
}

impl Peek for NativeType {
    fn peek(s: super::StatementStream<'_>) -> bool {
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

#[derive(Clone, Debug)]
pub enum CollectionType {
    List(Box<CqlType>),
    Set(Box<CqlType>),
    Map(Box<CqlType>, Box<CqlType>),
}

impl Parse for CollectionType {
    type Output = CollectionType;
    fn parse(s: &mut super::StatementStream<'_>) -> anyhow::Result<Self::Output>
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
    fn peek(s: super::StatementStream<'_>) -> bool {
        s.check::<MAP>() || s.check::<SET>() || s.check::<LIST>()
    }
}

#[derive(Clone, Debug)]
pub struct MapLiteral {
    pub key: Box<CqlTypeLiteral>,
    pub value: Box<CqlTypeLiteral>,
}

#[derive(Clone, Debug)]
pub struct TupleLiteral {
    pub elements: Vec<CqlTypeLiteral>,
}

impl Parse for TupleLiteral {
    type Output = TupleLiteral;
    fn parse(s: &mut super::StatementStream<'_>) -> anyhow::Result<Self::Output>
    where
        Self: Sized,
    {
        todo!()
    }
}

// impl Parse for TupleLiteral {
//     fn parse(input: syn::parse::ParseStream) -> syn::Result<Self> {
//         input.parse::<TUPLE>()?;
//         input.parse::<syn::Token![<]>()?;
//         let mut elements = vec![input.parse::<CqlTypeLiteral>()?];
//         while !input.peek(syn::Token![>]) {
//             input.parse::<syn::Token![,]>()?;
//             elements.push(input.parse::<CqlTypeLiteral>()?);
//         }
//         input.parse::<syn::Token![>]>()?;
//         Ok(TupleLiteral { elements })
//     }
// }

#[derive(Clone, Debug)]
pub struct SetLiteral {
    pub elements: Box<CqlTypeLiteral>,
}

#[derive(Clone, Debug)]
pub struct ListLiteral {
    pub elements: Box<CqlTypeLiteral>,
}

#[derive(Clone, Debug)]
pub struct UserDefinedTypeLiteral {
    pub fields: HashMap<Identifier, Term>,
}

#[derive(Clone, Debug)]
pub struct UserDefinedType {
    pub keyspace: Option<Name>,
    pub ident: Identifier,
}

impl Parse for UserDefinedType {
    type Output = UserDefinedType;
    fn parse(s: &mut super::StatementStream<'_>) -> anyhow::Result<Self::Output>
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
    fn peek(s: super::StatementStream<'_>) -> bool {
        s.check::<(Option<(Name, Dot)>, Identifier)>()
    }
}

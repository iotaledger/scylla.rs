// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use anymap::{
    AnyMap,
    Map,
};
use derive_builder::Builder;
use derive_more::{
    From,
    TryInto,
};
use quote::{
    quote,
    ToTokens,
    __private::TokenStream,
};
use scylla_parse_macros::{
    ParseFromStr,
    ToTokens,
};
use std::{
    cell::RefCell,
    collections::{
        BTreeMap,
        BTreeSet,
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
    marker::PhantomData,
    rc::Rc,
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

pub struct StreamInfo {
    pub next_token: String,
    pub pos: usize,
    pub rem: usize,
    pub ordered_tags: Vec<String>,
    pub keyed_tags: HashMap<String, String>,
}

impl Display for StreamInfo {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{{ next token: '{}', pos: {}, remaining: {}, ordered args: {:?}, keyed args: {:?} }}",
            self.next_token, self.pos, self.rem, self.ordered_tags, self.keyed_tags
        )
    }
}

#[derive(Debug)]
pub struct Cached<T: Parse> {
    pub value: T::Output,
    pub len: usize,
}

impl<T: Parse> Clone for Cached<T>
where
    T::Output: Clone,
{
    fn clone(&self) -> Self {
        Self {
            value: self.value.clone(),
            len: self.len,
        }
    }
}

impl<T: Parse> Cached<T> {
    pub fn new(value: T::Output, len: usize) -> Self {
        Self { value, len }
    }
}

#[derive(Clone)]
pub struct StatementStream<'a> {
    cursor: std::iter::Peekable<std::str::Chars<'a>>,
    pos: usize,
    rem: usize,
    cache: Rc<RefCell<HashMap<usize, AnyMap>>>,
    ordered_tags: Rc<RefCell<Vec<TokenStream>>>,
    curr_ordered_tag: usize,
    keyed_tags: Rc<RefCell<HashMap<String, TokenStream>>>,
}

impl<'a> StatementStream<'a> {
    pub fn new(statement: &'a str) -> Self {
        Self {
            cursor: statement.chars().peekable(),
            pos: 0,
            rem: statement.chars().count(),
            cache: Default::default(),
            ordered_tags: Default::default(),
            curr_ordered_tag: Default::default(),
            keyed_tags: Default::default(),
        }
    }

    pub fn push_ordered_tag(&mut self, tag: TokenStream) {
        self.ordered_tags.borrow_mut().push(tag);
    }

    pub fn insert_keyed_tag(&mut self, key: String, tag: TokenStream) {
        self.keyed_tags.borrow_mut().insert(key, tag);
    }

    pub fn info(&self) -> StreamInfo {
        self.info_with_token(self.clone().parse_from::<Token>().unwrap_or_default())
    }

    fn info_with_token(&self, next_token: String) -> StreamInfo {
        StreamInfo {
            next_token,
            pos: self.pos,
            rem: self.rem,
            ordered_tags: self.ordered_tags.borrow().iter().map(|t| t.to_string()).collect(),
            keyed_tags: self
                .keyed_tags
                .borrow()
                .iter()
                .map(|(k, t)| (k.clone(), t.to_string()))
                .collect(),
        }
    }

    pub fn current_pos(&self) -> usize {
        self.pos
    }

    pub fn remaining(&self) -> usize {
        self.rem
    }

    pub fn nremaining(&self, n: usize) -> bool {
        self.rem >= n
    }

    pub fn peek(&mut self) -> Option<char> {
        if self.rem == 0 {
            return None;
        }
        self.cursor.peek().map(|c| *c)
    }

    pub fn peekn(&mut self, n: usize) -> Option<String> {
        if self.rem < n {
            return None;
        }
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

    pub(crate) fn next(&mut self) -> Option<char> {
        if self.rem == 0 {
            return None;
        }
        let res = self.cursor.next();
        if res.is_some() {
            self.pos += 1;
            self.rem -= 1;
        }
        res
    }

    pub(crate) fn nextn(&mut self, n: usize) -> Option<String> {
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

    fn check_cache<P: 'static + Parse>(&self) -> bool {
        self.cache
            .borrow()
            .get(&self.pos)
            .and_then(|m| m.get::<Cached<P>>())
            .is_some()
    }

    fn retrieve_cache<P: 'static + Parse>(&self) -> Option<Cached<P>>
    where
        P::Output: Clone,
    {
        self.cache
            .borrow()
            .get(&self.pos)
            .and_then(|m| m.get::<Cached<P>>().cloned())
    }

    fn set_cache<P: 'static + Parse>(&self, value: P::Output, prev_pos: usize) {
        let mut cache = self.cache.borrow_mut();
        let map = cache.entry(prev_pos).or_insert_with(Map::new);
        map.insert(Cached::<P>::new(value, self.pos - prev_pos));
    }

    fn set_and_retrieve_cache<P: 'static + Parse>(&self, value: P::Output, prev_pos: usize) -> P::Output
    where
        P::Output: Clone,
    {
        let mut cache = self.cache.borrow_mut();
        let map = cache.entry(prev_pos).or_insert_with(Map::new);
        map.entry::<Cached<P>>()
            .or_insert(Cached::new(value, self.pos - prev_pos))
            .value
            .clone()
    }

    fn next_ordered_tag(&mut self) -> Option<TokenStream> {
        let c = self.curr_ordered_tag;
        self.curr_ordered_tag += 1;
        self.ordered_tags.borrow().get(c).cloned()
    }

    fn ordered_tag(&self, n: usize) -> Option<TokenStream> {
        self.ordered_tags.borrow().get(n).cloned()
    }

    fn mapped_tag(&self, key: &String) -> Option<TokenStream> {
        self.keyed_tags.borrow_mut().get(key).cloned()
    }

    pub fn check<P: 'static + Parse>(&self) -> bool
    where
        P::Output: 'static + Clone,
    {
        if self.check_cache::<P>() {
            return true;
        }
        let mut this = self.clone();
        this.skip_whitespace();
        P::parse(&mut this).map(|p| this.set_cache::<P>(p, self.pos)).is_ok()
    }

    pub fn find<P: 'static + Parse<Output = P> + Clone>(&self) -> Option<P> {
        if let Some(cached) = self.retrieve_cache::<P>() {
            return Some(cached.value);
        }
        let mut this = self.clone();
        this.skip_whitespace();
        P::parse(&mut this)
            .ok()
            .map(|p| this.set_and_retrieve_cache::<P>(p, self.pos))
    }

    pub fn find_from<P: 'static + Parse>(&self) -> Option<P::Output>
    where
        P::Output: 'static + Clone,
    {
        if let Some(cached) = self.retrieve_cache::<P>() {
            return Some(cached.value);
        }
        let mut this = self.clone();
        this.skip_whitespace();
        P::parse(&mut this)
            .ok()
            .map(|p| this.set_and_retrieve_cache::<P>(p, self.pos))
    }

    pub fn parse<P: 'static + Parse<Output = P> + Clone>(&mut self) -> anyhow::Result<P> {
        let pos = self.pos;
        if let Some(cached) = self.retrieve_cache::<P>() {
            self.nextn(cached.len);
            return Ok(cached.value);
        }
        self.skip_whitespace();
        P::parse(self).map(|p| self.set_and_retrieve_cache::<P>(p, pos))
    }

    pub fn parse_from<P: 'static + Parse>(&mut self) -> anyhow::Result<P::Output>
    where
        P::Output: 'static + Clone,
    {
        let pos = self.pos;
        if let Some(cached) = self.retrieve_cache::<P>() {
            self.nextn(cached.len);
            return Ok(cached.value);
        }
        self.skip_whitespace();
        P::parse(self).map(|p| self.set_and_retrieve_cache::<P>(p, pos))
    }
}

pub trait Parse {
    type Output;
    fn parse(s: &mut StatementStream<'_>) -> anyhow::Result<Self::Output>;
}

macro_rules! parse_tuple {
    ($($t:ident),+) => {
        impl<$($t: Parse),+> Parse for ($($t),+,)
        where
            $($t: 'static  + Clone),+,
            $($t::Output: 'static + Clone),+
        {
            type Output = ($($t::Output),+,);
            fn parse(s: &mut StatementStream<'_>) -> anyhow::Result<Self::Output> {
                Ok(($(
                    s.parse_from::<$t>()?,
                )+))
            }
        }
    };
}

parse_tuple!(T0);
parse_tuple!(T0, T1);
parse_tuple!(T0, T1, T2);
parse_tuple!(T0, T1, T2, T3);
parse_tuple!(T0, T1, T2, T3, T4);
parse_tuple!(T0, T1, T2, T3, T4, T5);
parse_tuple!(T0, T1, T2, T3, T4, T5, T6);
parse_tuple!(T0, T1, T2, T3, T4, T5, T6, T7);
parse_tuple!(T0, T1, T2, T3, T4, T5, T6, T7, T8);
parse_tuple!(T0, T1, T2, T3, T4, T5, T6, T7, T8, T9);

impl Parse for char {
    type Output = Self;
    fn parse(s: &mut StatementStream<'_>) -> anyhow::Result<Self::Output> {
        match s.next() {
            Some(c) => Ok(c),
            None => Err(anyhow::anyhow!("End of statement!")),
        }
    }
}

impl Parse for bool {
    type Output = Self;
    fn parse(s: &mut StatementStream<'_>) -> anyhow::Result<Self::Output> {
        Ok(if s.parse::<Option<TRUE>>()?.is_some() {
            true
        } else if s.parse::<Option<FALSE>>()?.is_some() {
            false
        } else {
            anyhow::bail!("Expected boolean, found {}", s.info())
        })
    }
}

macro_rules! parse_number {
    ($n:ident, $t:ident) => {
        impl Parse for $n {
            type Output = Self;
            fn parse(s: &mut StatementStream<'_>) -> anyhow::Result<Self::Output> {
                s.parse_from::<$t>()?
                    .parse()
                    .map_err(|_| anyhow::anyhow!("Invalid {}!", std::any::type_name::<$n>()))
            }
        }
    };
}

parse_number!(i8, SignedNumber);
parse_number!(i16, SignedNumber);
parse_number!(i32, SignedNumber);
parse_number!(i64, SignedNumber);
parse_number!(u8, Number);
parse_number!(u16, Number);
parse_number!(u32, Number);
parse_number!(u64, Number);
parse_number!(f32, Float);
parse_number!(f64, Float);

#[derive(Debug)]
pub struct If<Cond, Res>(PhantomData<fn(Cond, Res) -> (Cond, Res)>);
impl<Cond: Parse, Res: 'static + Parse> Parse for If<Cond, Res>
where
    Cond: 'static + Clone,
    Cond::Output: 'static + Clone,
    Res::Output: 'static + Clone,
{
    type Output = Option<Res::Output>;
    fn parse(s: &mut StatementStream<'_>) -> anyhow::Result<Self::Output> {
        match s.parse_from::<Option<Cond>>()? {
            Some(_) => Ok(Some(s.parse_from::<Res>()?)),
            None => Ok(None),
        }
    }
}

impl<T: 'static + Parse + Clone> Parse for Option<T>
where
    T::Output: 'static + Clone,
{
    type Output = Option<T::Output>;
    fn parse(s: &mut StatementStream<'_>) -> anyhow::Result<Self::Output> {
        Ok(if s.check::<T>() {
            Some(s.parse_from::<T>()?)
        } else {
            None
        })
    }
}

pub struct Not<T>(PhantomData<fn(T) -> T>);
impl<T: 'static + Parse> Parse for Not<T>
where
    T::Output: 'static + Clone,
{
    type Output = bool;
    fn parse(s: &mut StatementStream<'_>) -> anyhow::Result<Self::Output> {
        Ok(!s.check::<T>())
    }
}

pub type List<T, Delim> = TerminatingList<T, Delim, EmptyPeek>;

#[derive(Debug)]
pub struct TerminatingList<T, Delim, End>(PhantomData<fn(T, Delim, End) -> (T, Delim, End)>);
impl<T: 'static + Parse, Delim: 'static + Parse + Clone, End: 'static + Parse> Parse for TerminatingList<T, Delim, End>
where
    Delim::Output: 'static + Clone,
    T::Output: 'static + Clone,
    End::Output: 'static + Clone,
{
    type Output = Vec<T::Output>;
    fn parse(s: &mut StatementStream<'_>) -> anyhow::Result<Self::Output> {
        let mut res = vec![s.parse_from::<T>()?];
        if s.remaining() == 0 || s.check::<End>() {
            return Ok(res);
        }
        while s.parse_from::<Option<Delim>>()?.is_some() {
            if s.check::<End>() {
                return Ok(res);
            }
            res.push(s.parse_from::<T>()?);
            if s.remaining() == 0 {
                return Ok(res);
            }
        }
        Ok(res)
    }
}

impl<T, Delim, End> Clone for TerminatingList<T, Delim, End> {
    fn clone(&self) -> Self {
        Self(PhantomData)
    }
}

#[derive(Copy, Clone, Debug)]
pub struct Nothing;
impl Parse for Nothing {
    type Output = Self;
    fn parse(_: &mut StatementStream<'_>) -> anyhow::Result<Self::Output> {
        Ok(Self)
    }
}

#[derive(Copy, Clone, Debug)]
pub struct EmptyPeek;
impl Parse for EmptyPeek {
    type Output = Self;
    fn parse(_: &mut StatementStream<'_>) -> anyhow::Result<Self::Output> {
        anyhow::bail!("Empty peek!")
    }
}

#[derive(Copy, Clone, Debug)]
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

#[derive(Copy, Clone, Debug)]
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

#[derive(Clone, Debug, PartialEq, Eq, Hash, Ord, PartialOrd)]
pub enum Tag<T> {
    Tag(String),
    Value(T),
}
impl<T> Tag<T> {
    pub(crate) fn into_value(self) -> anyhow::Result<T> {
        match self {
            Tag::Value(v) => Ok(v),
            _ => anyhow::bail!("Expected value!"),
        }
    }
}
impl<T: 'static + Parse + Clone> Parse for Tag<T>
where
    T::Output: 'static + Clone,
{
    type Output = Tag<T::Output>;
    fn parse(s: &mut StatementStream<'_>) -> anyhow::Result<Self::Output> {
        Ok(if s.check::<HashTag>() {
            let tag = s.parse_from::<HashTag>()?;
            let tag = match tag {
                HashTag::Keyed(key) => s
                    .mapped_tag(&key)
                    .ok_or_else(|| anyhow::anyhow!("No argument found for key {}: {}", key, s.info()))?,
                HashTag::Ordered(n) => s
                    .ordered_tag(n)
                    .ok_or_else(|| anyhow::anyhow!("No argument found for index {}: {}", n, s.info()))?,
                HashTag::Next => s
                    .next_ordered_tag()
                    .ok_or_else(|| anyhow::anyhow!("Insufficient unkeyed arguments: {}", s.info()))?,
            };
            Tag::Tag(tag.to_string())
        } else {
            Tag::Value(s.parse_from::<T>()?)
        })
    }
}
impl<T> From<T> for Tag<T> {
    fn from(t: T) -> Self {
        Tag::Value(t)
    }
}
impl<T: Display> Display for Tag<T> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Tag::Tag(t) => t.fmt(f),
            Tag::Value(v) => v.fmt(f),
        }
    }
}
impl<T: Default> Default for Tag<T> {
    fn default() -> Self {
        Tag::Value(T::default())
    }
}

impl<'a, T: 'a> CustomToTokens<'a> for Tag<T>
where
    TokenWrapper<'a, T>: ToTokens,
{
    fn to_tokens(&'a self, tokens: &mut TokenStream) {
        tokens.extend(match self {
            Tag::Tag(t) => {
                let t = TokenStream::from_str(t).unwrap();
                quote!(#t.into())
            }
            Tag::Value(v) => {
                let v = TokenWrapper(v);
                quote! {#v}
            }
        });
    }
}

impl<T> ToTokens for Tag<T>
where
    for<'a> TokenWrapper<'a, T>: ToTokens,
{
    fn to_tokens(&self, tokens: &mut TokenStream) {
        CustomToTokens::to_tokens(self, tokens);
    }
}
#[derive(Clone, Debug)]
pub enum HashTag {
    Next,
    Ordered(usize),
    Keyed(String),
}
impl Parse for HashTag {
    type Output = Self;
    fn parse(s: &mut StatementStream<'_>) -> anyhow::Result<Self::Output> {
        if let Some(c) = s.next() {
            let mut res = String::new();
            if c == '#' {
                while let Some(c) = s.peek() {
                    if c.is_alphanumeric() || c == '_' {
                        s.next();
                        res.push(c);
                    } else {
                        break;
                    }
                }
                Ok(if res.is_empty() {
                    Self::Next
                } else if res.chars().all(|c| c.is_numeric()) {
                    Self::Ordered(res.parse().unwrap())
                } else {
                    Self::Keyed(res)
                })
            } else {
                anyhow::bail!("Expected #")
            }
        } else {
            anyhow::bail!("End of statement!")
        }
    }
}

#[derive(Copy, Clone, Debug)]
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

#[derive(Copy, Clone, Debug)]
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

#[derive(Copy, Clone, Debug)]
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

#[derive(Copy, Clone, Debug)]
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

#[derive(Copy, Clone, Debug)]
pub struct SignedNumber;
impl Parse for SignedNumber {
    type Output = String;
    fn parse(s: &mut StatementStream<'_>) -> anyhow::Result<Self::Output> {
        let mut res = String::new();
        let mut has_numerals = false;
        let mut has_negative = false;
        while let Some(c) = s.peek() {
            if c.is_numeric() {
                has_numerals = true;
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
        if has_negative && !has_numerals {
            anyhow::bail!("Invalid number: Negative sign without number")
        }
        if res.is_empty() {
            anyhow::bail!("End of statement!")
        }
        Ok(res)
    }
}

#[derive(Copy, Clone, Debug)]
pub struct Float;
impl Parse for Float {
    type Output = String;
    fn parse(s: &mut StatementStream<'_>) -> anyhow::Result<Self::Output> {
        let mut res = String::new();
        let mut has_numerals = false;
        let mut has_dot = false;
        let mut has_negative = false;
        let mut has_e = false;
        while let Some(c) = s.peek() {
            if c.is_numeric() {
                has_numerals = true;
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
        if has_negative && !has_numerals {
            anyhow::bail!("Invalid float: Negative sign without number")
        }
        if has_dot && !has_numerals {
            anyhow::bail!("Invalid float: Decimal point without number")
        }
        if !has_dot {
            anyhow::bail!("Invalid float: Missing decimal point")
        }
        if res.is_empty() {
            anyhow::bail!("End of statement!")
        }
        Ok(res)
    }
}

macro_rules! parse_peek_group {
    ($g:ident, $l:ident, $r:ident) => {
        #[derive(Clone, Debug)]
        pub struct $g<T>(T);
        impl<T: 'static + Parse> Parse for $g<T>
        where
            T::Output: 'static + Clone,
        {
            type Output = T::Output;
            fn parse(s: &mut StatementStream<'_>) -> anyhow::Result<Self::Output> {
                s.parse_from::<$l>()?;
                let res = s.parse_from::<T>()?;
                s.parse_from::<$r>()?;
                Ok(res)
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

#[derive(ParseFromStr, Clone, Debug, TryInto, PartialEq, Eq, Hash, Ord, PartialOrd, ToTokens)]
pub enum BindMarker {
    #[try_into(ignore)]
    Anonymous,
    Named(Name),
}

impl Parse for BindMarker {
    type Output = Self;
    fn parse(s: &mut StatementStream<'_>) -> anyhow::Result<Self::Output> {
        Ok(if s.parse::<Option<Question>>()?.is_some() {
            BindMarker::Anonymous
        } else {
            let (_, id) = s.parse::<(Colon, Name)>()?;
            BindMarker::Named(id)
        })
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

impl<T: Into<Name>> From<T> for BindMarker {
    fn from(id: T) -> Self {
        BindMarker::Named(id.into())
    }
}

impl Parse for Uuid {
    type Output = Self;
    fn parse(s: &mut StatementStream<'_>) -> anyhow::Result<Self::Output> {
        if let Some(u) = s.nextn(36) {
            Ok(uuid::Uuid::parse_str(&u)?)
        } else {
            anyhow::bail!("Invalid UUID: {}", s.info())
        }
    }
}

impl<'a> CustomToTokens<'a> for Uuid {
    fn to_tokens(&'a self, tokens: &mut TokenStream) {
        let u = self.to_string();
        tokens.extend(quote!(Uuid::parse_str(#u).unwrap()));
    }
}

#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash, Ord, PartialOrd, ToTokens)]
pub enum LitStrKind {
    Quoted,
    Escaped,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash, Ord, PartialOrd, ToTokens)]
pub struct LitStr {
    pub kind: LitStrKind,
    pub value: String,
}

impl LitStr {
    pub fn quoted(value: &str) -> Self {
        Self {
            kind: LitStrKind::Quoted,
            value: value.to_string(),
        }
    }

    pub fn escaped(value: &str) -> Self {
        Self {
            kind: LitStrKind::Escaped,
            value: value.to_string(),
        }
    }
}

impl Parse for LitStr {
    type Output = Self;
    fn parse(s: &mut StatementStream<'_>) -> anyhow::Result<Self::Output> {
        let mut res = String::new();
        let mut kind = LitStrKind::Quoted;
        if s.peek() == Some('\'') {
            s.next();
        } else if s.peekn(2).map(|s| s.as_str() == "$$").unwrap_or(false) {
            kind = LitStrKind::Escaped;
            s.nextn(2);
        } else {
            return Err(anyhow::anyhow!(
                "Expected opening quote for LitStr, found: {}",
                s.info()
            ));
        }
        while let Some(c) = s.next() {
            if kind == LitStrKind::Escaped && c == '$' && s.peek().map(|c| c == '$').unwrap_or(false) {
                s.next();
                return Ok(LitStr { kind, value: res });
            } else if kind == LitStrKind::Quoted && c == '\'' {
                return Ok(LitStr { kind, value: res });
            } else {
                res.push(c);
            }
        }
        anyhow::bail!("End of statement!")
    }
}

impl Display for LitStr {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self.kind {
            LitStrKind::Quoted => write!(f, "'{}'", self.value),
            LitStrKind::Escaped => write!(f, "$${}$$", self.value),
        }
    }
}

impl From<String> for LitStr {
    fn from(s: String) -> Self {
        if s.contains('\'') {
            LitStr {
                kind: LitStrKind::Escaped,
                value: s,
            }
        } else {
            LitStr {
                kind: LitStrKind::Quoted,
                value: s,
            }
        }
    }
}

impl From<&str> for LitStr {
    fn from(s: &str) -> Self {
        s.to_string().into()
    }
}

#[derive(ParseFromStr, Clone, Debug, Hash, Eq, PartialEq, Ord, PartialOrd, ToTokens)]
pub enum Name {
    Quoted(String),
    Unquoted(String),
}

impl Name {
    pub fn quoted(s: &str) -> Self {
        Name::Quoted(s.to_string())
    }

    pub fn unquoted(s: &str) -> Self {
        Name::Unquoted(s.to_string())
    }

    pub fn term(self, term: impl Into<Term>) -> SimpleSelection {
        SimpleSelection::Term(self, term.into())
    }

    pub fn field(self, field: impl Into<Name>) -> SimpleSelection {
        SimpleSelection::Field(self, field.into())
    }
}

impl Parse for Name {
    type Output = Self;
    fn parse(s: &mut StatementStream<'_>) -> anyhow::Result<Self> {
        let mut res = String::new();
        if s.peek().map(|c| c == '\"').unwrap_or(false) {
            s.next();
            while let Some(c) = s.next() {
                if c == '\"' {
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

impl Display for Name {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Quoted(s) => write!(f, "\"{}\"", s),
            Self::Unquoted(s) => s.fmt(f),
        }
    }
}

impl From<String> for Name {
    fn from(s: String) -> Self {
        if s.contains(char::is_whitespace) {
            Self::Quoted(s)
        } else {
            Self::Unquoted(s)
        }
    }
}

impl From<&str> for Name {
    fn from(s: &str) -> Self {
        s.to_string().into()
    }
}

pub trait KeyspaceQualifyExt {
    fn dot(self, other: impl Into<Name>) -> KeyspaceQualifiedName;

    fn with_keyspace(self, keyspace: impl Into<Name>) -> KeyspaceQualifiedName;
}

impl<N: Into<Name>> KeyspaceQualifyExt for N {
    fn dot(self, other: impl Into<Name>) -> KeyspaceQualifiedName {
        KeyspaceQualifiedName {
            keyspace: Some(self.into()),
            name: other.into(),
        }
    }

    fn with_keyspace(self, keyspace: impl Into<Name>) -> KeyspaceQualifiedName {
        KeyspaceQualifiedName {
            keyspace: Some(keyspace.into()),
            name: self.into(),
        }
    }
}

#[derive(ParseFromStr, Clone, Debug, PartialEq, Eq, Hash, Ord, PartialOrd, ToTokens)]
#[parse_via(TaggedKeyspaceQualifiedName)]
pub struct KeyspaceQualifiedName {
    pub keyspace: Option<Name>,
    pub name: Name,
}

impl TryFrom<TaggedKeyspaceQualifiedName> for KeyspaceQualifiedName {
    type Error = anyhow::Error;
    fn try_from(t: TaggedKeyspaceQualifiedName) -> anyhow::Result<Self> {
        Ok(KeyspaceQualifiedName {
            keyspace: t.keyspace.map(|s| s.into_value()).transpose()?,
            name: t.name.into_value()?,
        })
    }
}

#[derive(ParseFromStr, Clone, Debug, PartialEq, Eq, Hash, Ord, PartialOrd, ToTokens)]
#[tokenize_as(KeyspaceQualifiedName)]
pub struct TaggedKeyspaceQualifiedName {
    pub keyspace: Option<Tag<Name>>,
    pub name: Tag<Name>,
}

impl Parse for TaggedKeyspaceQualifiedName {
    type Output = Self;
    fn parse(s: &mut StatementStream<'_>) -> anyhow::Result<Self> {
        let (keyspace, name) = s.parse::<(Option<(Tag<Name>, Dot)>, Tag<Name>)>()?;
        Ok(Self {
            keyspace: keyspace.map(|(i, _)| i),
            name,
        })
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

impl From<String> for KeyspaceQualifiedName {
    fn from(s: String) -> Self {
        Self {
            keyspace: None,
            name: s.into(),
        }
    }
}

impl From<&str> for KeyspaceQualifiedName {
    fn from(s: &str) -> Self {
        Self {
            keyspace: None,
            name: s.into(),
        }
    }
}

#[derive(ParseFromStr, Clone, Debug, ToTokens, PartialEq, Eq)]
#[parse_via(TaggedStatementOpt)]
pub struct StatementOpt {
    pub name: Name,
    pub value: StatementOptValue,
}

impl StatementOpt {
    pub fn new<N: Into<Name>>(name: N, value: StatementOptValue) -> Self {
        Self {
            name: name.into(),
            value,
        }
    }
}

impl TryFrom<TaggedStatementOpt> for StatementOpt {
    type Error = anyhow::Error;
    fn try_from(t: TaggedStatementOpt) -> anyhow::Result<Self> {
        Ok(Self {
            name: t.name,
            value: t.value.try_into()?,
        })
    }
}

#[derive(ParseFromStr, Clone, Debug, ToTokens, PartialEq, Eq)]
#[tokenize_as(StatementOpt)]
pub struct TaggedStatementOpt {
    pub name: Name,
    pub value: TaggedStatementOptValue,
}

impl Parse for TaggedStatementOpt {
    type Output = Self;
    fn parse(s: &mut StatementStream<'_>) -> anyhow::Result<Self> {
        let (name, _, value) = s.parse::<(Name, Equals, TaggedStatementOptValue)>()?;
        Ok(Self { name, value })
    }
}

impl Display for StatementOpt {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} = {}", self.name, self.value)
    }
}

#[derive(ParseFromStr, Clone, Debug, ToTokens, PartialEq, Eq, From)]
#[parse_via(TaggedStatementOptValue)]
pub enum StatementOptValue {
    Identifier(Name),
    Constant(Constant),
    Map(MapLiteral),
}

impl TryFrom<TaggedStatementOptValue> for StatementOptValue {
    type Error = anyhow::Error;
    fn try_from(t: TaggedStatementOptValue) -> anyhow::Result<Self> {
        Ok(match t {
            TaggedStatementOptValue::Identifier(i) => Self::Identifier(i.into_value()?),
            TaggedStatementOptValue::Constant(c) => Self::Constant(c.into_value()?),
            TaggedStatementOptValue::Map(m) => Self::Map(m.into_value()?.try_into()?),
        })
    }
}

#[derive(ParseFromStr, Clone, Debug, ToTokens, PartialEq, Eq, From)]
#[tokenize_as(StatementOptValue)]
pub enum TaggedStatementOptValue {
    Identifier(Tag<Name>),
    Constant(Tag<Constant>),
    Map(Tag<TaggedMapLiteral>),
}

impl Parse for TaggedStatementOptValue {
    type Output = Self;
    fn parse(s: &mut StatementStream<'_>) -> anyhow::Result<Self> {
        if let Some(map) = s.parse::<Option<Tag<TaggedMapLiteral>>>()? {
            Ok(Self::Map(map))
        } else if let Some(constant) = s.parse::<Option<Tag<Constant>>>()? {
            Ok(Self::Constant(constant))
        } else if let Some(identifier) = s.parse::<Option<Tag<Name>>>()? {
            Ok(Self::Identifier(identifier))
        } else {
            anyhow::bail!("Invalid statement option value: {}", s.info())
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

impl Display for TaggedStatementOptValue {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Identifier(identifier) => identifier.fmt(f),
            Self::Constant(constant) => constant.fmt(f),
            Self::Map(map) => map.fmt(f),
        }
    }
}

#[derive(Builder, Clone, Debug, ToTokens, PartialEq, Eq)]
pub struct ColumnDefinition {
    #[builder(setter(into))]
    pub name: Name,
    #[builder(setter(into))]
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

impl<T: Into<Name>> From<(T, NativeType)> for ColumnDefinition {
    fn from((name, data_type): (T, NativeType)) -> Self {
        Self {
            name: name.into(),
            data_type: CqlType::Native(data_type),
            static_column: false,
            primary_key: false,
        }
    }
}

#[derive(ParseFromStr, Clone, Debug, ToTokens, PartialEq, Eq)]
pub struct PrimaryKey {
    pub partition_key: PartitionKey,
    pub clustering_columns: Option<Vec<Name>>,
}

impl PrimaryKey {
    pub fn partition_key(partition_key: impl Into<PartitionKey>) -> Self {
        PrimaryKey {
            partition_key: partition_key.into(),
            clustering_columns: None,
        }
    }

    pub fn clustering_columns(self, clustering_columns: Vec<impl Into<Name>>) -> Self {
        PrimaryKey {
            partition_key: self.partition_key,
            clustering_columns: if clustering_columns.is_empty() {
                self.clustering_columns
            } else {
                Some(clustering_columns.into_iter().map(Into::into).collect())
            },
        }
    }
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
            if !clustering_columns.is_empty() {
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
        }
        Ok(())
    }
}

impl<N: Into<Name>> From<Vec<N>> for PrimaryKey {
    fn from(names: Vec<N>) -> Self {
        let mut names = names.into_iter().map(Into::into);
        let partition_key = names.next().unwrap().into();
        PrimaryKey {
            partition_key,
            clustering_columns: Some(names.collect()),
        }
    }
}

#[derive(ParseFromStr, Clone, Debug, ToTokens, PartialEq, Eq)]
pub struct PartitionKey {
    pub columns: Vec<Name>,
}

impl Parse for PartitionKey {
    type Output = Self;
    fn parse(s: &mut StatementStream<'_>) -> anyhow::Result<Self> {
        Ok(
            if let Some(columns) = s.parse_from::<Option<Parens<List<Name, Comma>>>>()? {
                Self { columns }
            } else {
                Self {
                    columns: vec![s.parse::<Name>()?],
                }
            },
        )
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

impl<N: Into<Name>> From<Vec<N>> for PartitionKey {
    fn from(columns: Vec<N>) -> Self {
        Self {
            columns: columns.into_iter().map(Into::into).collect(),
        }
    }
}

impl<N: Into<Name>> From<N> for PartitionKey {
    fn from(column: N) -> Self {
        Self {
            columns: vec![column.into()],
        }
    }
}

// TODO: Scylla encryption opts and caching?
#[derive(Builder, Clone, Debug, Default, ToTokens, PartialEq)]
#[builder(setter(strip_option), default, build_fn(validate = "Self::validate"))]
pub struct TableOpts {
    pub compact_storage: bool,
    pub clustering_order: Option<Vec<ColumnOrder>>,
    #[builder(setter(into))]
    pub comment: Option<LitStr>,
    pub speculative_retry: Option<SpeculativeRetry>,
    pub change_data_capture: Option<bool>,
    pub gc_grace_seconds: Option<i32>,
    pub bloom_filter_fp_chance: Option<f32>,
    pub default_time_to_live: Option<i32>,
    pub compaction: Option<Compaction>,
    pub compression: Option<Compression>,
    pub caching: Option<Caching>,
    pub memtable_flush_period_in_ms: Option<i32>,
    pub read_repair: Option<bool>,
}

impl Parse for TableOpts {
    type Output = Self;
    fn parse(s: &mut StatementStream<'_>) -> anyhow::Result<Self> {
        let mut res = TableOptsBuilder::default();
        loop {
            if s.parse::<Option<(COMPACT, STORAGE)>>()?.is_some() {
                if res.compact_storage.is_some() {
                    anyhow::bail!("Duplicate compact storage option");
                }
                res.compact_storage(true);
                if s.parse::<Option<AND>>()?.is_none() {
                    break;
                }
            } else if s.parse::<Option<(CLUSTERING, ORDER, BY)>>()?.is_some() {
                if res.clustering_order.is_some() {
                    anyhow::bail!("Duplicate clustering order option");
                }
                res.clustering_order(s.parse_from::<Parens<List<ColumnOrder, Comma>>>()?);
                if s.parse::<Option<AND>>()?.is_none() {
                    break;
                }
            } else {
                if let Some(v) = s.parse_from::<Option<List<StatementOpt, AND>>>()? {
                    for StatementOpt { name, value } in v {
                        let (Name::Quoted(n) | Name::Unquoted(n)) = &name;
                        match n.as_str() {
                            "comment" => {
                                if res.comment.is_some() {
                                    anyhow::bail!("Duplicate comment option");
                                } else if let StatementOptValue::Constant(Constant::String(s)) = value {
                                    res.comment(s);
                                } else {
                                    anyhow::bail!("Invalid comment value: {}", value);
                                }
                            }
                            "speculative_retry" => {
                                if res.speculative_retry.is_some() {
                                    anyhow::bail!("Duplicate speculative retry option");
                                } else if let StatementOptValue::Constant(Constant::String(s)) = value {
                                    res.speculative_retry(s.to_string().parse()?);
                                } else {
                                    anyhow::bail!("Invalid speculative retry value: {}", value);
                                }
                            }
                            "cdc" => {
                                if res.change_data_capture.is_some() {
                                    anyhow::bail!("Duplicate change data capture option");
                                } else if let StatementOptValue::Constant(Constant::Boolean(b)) = value {
                                    res.change_data_capture(b);
                                } else {
                                    anyhow::bail!("Invalid change data capture value: {}", value);
                                }
                            }
                            "gc_grace_seconds" => {
                                if res.gc_grace_seconds.is_some() {
                                    anyhow::bail!("Duplicate gc_grace_seconds option");
                                } else if let StatementOptValue::Constant(Constant::Integer(i)) = value {
                                    res.gc_grace_seconds(i.parse()?);
                                } else {
                                    anyhow::bail!("Invalid gc_grace_seconds value: {}", value);
                                }
                            }
                            "bloom_filter_fp_chance" => {
                                if res.bloom_filter_fp_chance.is_some() {
                                    anyhow::bail!("Duplicate bloom_filter_fp_chance option");
                                } else if let StatementOptValue::Constant(Constant::Float(f)) = value {
                                    res.bloom_filter_fp_chance(f.parse()?);
                                } else {
                                    anyhow::bail!("Invalid bloom_filter_fp_chance value: {}", value);
                                }
                            }
                            "default_time_to_live" => {
                                if res.default_time_to_live.is_some() {
                                    anyhow::bail!("Duplicate default_time_to_live option");
                                } else if let StatementOptValue::Constant(Constant::Integer(i)) = value {
                                    res.default_time_to_live(i.parse()?);
                                } else {
                                    anyhow::bail!("Invalid default_time_to_live value: {}", value);
                                }
                            }
                            "compaction" => {
                                if res.compaction.is_some() {
                                    anyhow::bail!("Duplicate compaction option");
                                } else if let StatementOptValue::Map(m) = value {
                                    res.compaction(m.try_into()?);
                                } else {
                                    anyhow::bail!("Invalid compaction value: {}", value);
                                }
                            }
                            "compression" => {
                                if res.compression.is_some() {
                                    anyhow::bail!("Duplicate compression option");
                                } else if let StatementOptValue::Map(m) = value {
                                    res.compression(m.try_into()?);
                                } else {
                                    anyhow::bail!("Invalid compression value: {}", value);
                                }
                            }
                            "caching" => {
                                if res.caching.is_some() {
                                    anyhow::bail!("Duplicate caching option");
                                } else if let StatementOptValue::Map(m) = value {
                                    res.caching(m.try_into()?);
                                } else {
                                    anyhow::bail!("Invalid caching value: {}", value);
                                }
                            }
                            "memtable_flush_period_in_ms" => {
                                if res.memtable_flush_period_in_ms.is_some() {
                                    anyhow::bail!("Duplicate memtable_flush_period_in_ms option");
                                } else if let StatementOptValue::Constant(Constant::Integer(i)) = value {
                                    res.memtable_flush_period_in_ms(i.parse()?);
                                } else {
                                    anyhow::bail!("Invalid memtable_flush_period_in_ms value: {}", value);
                                }
                            }
                            "read_repair" => {
                                if res.read_repair.is_some() {
                                    anyhow::bail!("Duplicate read_repair option");
                                } else if let StatementOptValue::Constant(Constant::String(s)) = value {
                                    res.read_repair(match s.value.to_uppercase().as_str() {
                                        "BLOCKING" => true,
                                        "NONE" => false,
                                        _ => anyhow::bail!("Invalid read_repair value: {}", s),
                                    });
                                } else {
                                    anyhow::bail!("Invalid read_repair value: {}", value);
                                }
                            }
                            _ => anyhow::bail!("Invalid table option: {}", name),
                        }
                    }
                }
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
        let mut res = Vec::new();
        if self.compact_storage {
            res.push("COMPACT STORAGE".to_string());
        }
        if let Some(ref c) = self.clustering_order {
            res.push(format!(
                "COMPACT STORAGE AND {}",
                c.iter().map(|i| i.to_string()).collect::<Vec<_>>().join(", ")
            ));
        }
        if let Some(ref c) = self.comment {
            res.push(format!("comment = {}", c));
        }
        if let Some(ref c) = self.speculative_retry {
            res.push(format!("speculative_retry = {}", c));
        }
        if let Some(c) = self.change_data_capture {
            res.push(format!("cdc = {}", c));
        }
        if let Some(c) = self.gc_grace_seconds {
            res.push(format!("gc_grace_seconds = {}", c));
        }
        if let Some(c) = self.bloom_filter_fp_chance {
            res.push(format!("bloom_filter_fp_chance = {}", format_cql_f32(c)));
        }
        if let Some(c) = self.default_time_to_live {
            res.push(format!("default_time_to_live = {}", c));
        }
        if let Some(ref c) = self.compaction {
            res.push(format!("compaction = {}", c));
        }
        if let Some(ref c) = self.compression {
            res.push(format!("compression = {}", c));
        }
        if let Some(ref c) = self.caching {
            res.push(format!("caching = {}", c));
        }
        if let Some(c) = self.memtable_flush_period_in_ms {
            res.push(format!("memtable_flush_period_in_ms = {}", c));
        }
        if let Some(c) = self.read_repair {
            res.push(
                match c {
                    true => "read_repair = 'BLOCKING'",
                    false => "read_repair = 'NONE'",
                }
                .to_string(),
            );
        }
        write!(f, "{}", res.join(" AND "))
    }
}

impl TableOptsBuilder {
    fn validate(&self) -> Result<(), String> {
        if self.compact_storage.is_some()
            || self.clustering_order.as_ref().map(|v| v.is_some()).unwrap_or_default()
            || self.comment.as_ref().map(|v| v.is_some()).unwrap_or_default()
            || self.speculative_retry.as_ref().map(|v| v.is_some()).unwrap_or_default()
            || self
                .change_data_capture
                .as_ref()
                .map(|v| v.is_some())
                .unwrap_or_default()
            || self.gc_grace_seconds.as_ref().map(|v| v.is_some()).unwrap_or_default()
            || self
                .bloom_filter_fp_chance
                .as_ref()
                .map(|v| v.is_some())
                .unwrap_or_default()
            || self
                .default_time_to_live
                .as_ref()
                .map(|v| v.is_some())
                .unwrap_or_default()
            || self.compaction.as_ref().map(|v| v.is_some()).unwrap_or_default()
            || self.compression.as_ref().map(|v| v.is_some()).unwrap_or_default()
            || self.caching.as_ref().map(|v| v.is_some()).unwrap_or_default()
            || self
                .memtable_flush_period_in_ms
                .as_ref()
                .map(|v| v.is_some())
                .unwrap_or_default()
            || self.read_repair.as_ref().map(|v| v.is_some()).unwrap_or_default()
        {
            Ok(())
        } else {
            Err("No table options specified".to_string())
        }
    }
}

#[derive(ParseFromStr, Clone, Debug, ToTokens, PartialEq, Eq)]
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

impl<T: Into<Name>> From<(T, Order)> for ColumnOrder {
    fn from((t, order): (T, Order)) -> Self {
        ColumnOrder {
            column: t.into(),
            order,
        }
    }
}

#[derive(Copy, Clone, Debug, ToTokens, PartialEq, Eq)]
pub enum Order {
    Ascending,
    Descending,
}

impl Parse for Order {
    type Output = Self;
    fn parse(s: &mut StatementStream<'_>) -> anyhow::Result<Self::Output> {
        if s.parse::<Option<ASC>>()?.is_some() {
            Ok(Order::Ascending)
        } else if s.parse::<Option<DESC>>()?.is_some() {
            Ok(Order::Descending)
        } else {
            anyhow::bail!("Invalid sort order: {}", s.info())
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

#[derive(ParseFromStr, Clone, Debug, ToTokens, PartialEq, Eq)]
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
    MVExclusion {
        column: Name,
    },
}

impl Relation {
    pub fn normal(column: impl Into<Name>, operator: Operator, term: impl Into<Term>) -> Self {
        Self::Normal {
            column: column.into(),
            operator,
            term: term.into(),
        }
    }

    pub fn tuple(columns: Vec<impl Into<Name>>, operator: Operator, tuple_literal: Vec<impl Into<Term>>) -> Self {
        Self::Tuple {
            columns: columns.into_iter().map(Into::into).collect(),
            operator,
            tuple_literal: tuple_literal.into_iter().map(Into::into).collect::<Vec<_>>().into(),
        }
    }

    pub fn token(columns: Vec<impl Into<Name>>, operator: Operator, term: impl Into<Term>) -> Self {
        Self::Token {
            columns: columns.into_iter().map(Into::into).collect(),
            operator,
            term: term.into(),
        }
    }

    pub fn is_not_null(column: impl Into<Name>) -> Self {
        Self::MVExclusion { column: column.into() }
    }
}

impl Parse for Relation {
    type Output = Self;
    fn parse(s: &mut StatementStream<'_>) -> anyhow::Result<Self> {
        Ok(
            if let Some((column, _, _, _)) = s.parse::<Option<(Name, IS, NOT, NULL)>>()? {
                Relation::MVExclusion { column }
            } else if s.parse::<Option<TOKEN>>()?.is_some() {
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
            },
        )
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
            Relation::MVExclusion { column } => write!(f, "{} IS NOT NULL", column),
        }
    }
}

#[derive(ParseFromStr, Clone, Debug, ToTokens, PartialEq, Eq)]
#[parse_via(TaggedReplication)]
pub struct Replication {
    pub class: LitStr,
    pub replication_factor: Option<i32>,
    pub datacenters: BTreeMap<LitStr, i32>,
}

impl Replication {
    pub fn simple(replication_factor: i32) -> Self {
        Self {
            class: "SimpleStrategy".into(),
            replication_factor: Some(replication_factor),
            datacenters: BTreeMap::new(),
        }
    }

    pub fn network_topology<S: Into<String>>(replication_map: BTreeMap<S, i32>) -> Self {
        let mut map: BTreeMap<String, _> = replication_map.into_iter().map(|(k, v)| (k.into(), v)).collect();
        Self {
            class: "NetworkTopologyStrategy".into(),
            replication_factor: map.remove("replication_factor"),
            datacenters: map.into_iter().map(|(dc, rf)| (dc.into(), rf)).collect(),
        }
    }
}

impl From<i32> for Replication {
    fn from(replication_factor: i32) -> Self {
        Self::simple(replication_factor)
    }
}

impl Default for Replication {
    fn default() -> Self {
        Self::simple(1)
    }
}

impl Display for Replication {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut opts = vec![("'class'".to_string(), self.class.to_string())];
        if let Some(replication_factor) = self.replication_factor {
            opts.push(("'replication_factor'".to_string(), format!("{}", replication_factor)));
        }
        for (dc, rf) in self.datacenters.iter() {
            opts.push((dc.to_string(), format!("{}", rf)));
        }
        write!(
            f,
            "{{{}}}",
            opts.iter()
                .map(|(k, v)| format!("{}: {}", k, v))
                .collect::<Vec<_>>()
                .join(", ")
        )
    }
}

impl TryFrom<TaggedReplication> for Replication {
    type Error = anyhow::Error;
    fn try_from(value: TaggedReplication) -> Result<Self, Self::Error> {
        let mut datacenters = BTreeMap::new();
        for (k, v) in value.datacenters {
            if datacenters.insert(k.into_value()?, v.into_value()?).is_some() {
                anyhow::bail!("Duplicate key in replication map");
            }
        }
        Ok(Self {
            class: value.class.into_value()?,
            replication_factor: value.replication_factor.map(|v| v.into_value()).transpose()?,
            datacenters,
        })
    }
}

#[derive(ParseFromStr, Clone, Debug, ToTokens, PartialEq, Eq)]
#[tokenize_as(Replication)]
pub struct TaggedReplication {
    pub class: Tag<LitStr>,
    pub replication_factor: Option<Tag<i32>>,
    pub datacenters: BTreeMap<Tag<LitStr>, Tag<i32>>,
}

impl Default for TaggedReplication {
    fn default() -> Self {
        Self {
            class: Tag::Value("SimpleStrategy".into()),
            replication_factor: Some(Tag::Value(1)),
            datacenters: BTreeMap::new(),
        }
    }
}

impl TryFrom<TaggedMapLiteral> for TaggedReplication {
    type Error = anyhow::Error;
    fn try_from(value: TaggedMapLiteral) -> Result<Self, Self::Error> {
        let mut class = None;
        let v = value
            .elements
            .into_iter()
            .filter(|(k, v)| {
                if let Tag::Value(Term::Constant(Constant::String(s))) = k {
                    if s.value.to_lowercase().as_str() == "class" {
                        class = Some(v.clone());
                        return false;
                    }
                }
                true
            })
            .collect::<Vec<_>>();
        let class = match class.ok_or_else(|| anyhow::anyhow!("No class in replication map literal!"))? {
            Tag::Value(Term::Constant(Constant::String(s))) => Tag::Value(s),
            Tag::Tag(t) => Tag::Tag(t),
            c @ _ => anyhow::bail!("Invalid class: {:?}", c),
        };
        let mut replication_factor = None;
        let mut datacenters = BTreeMap::new();
        match class {
            Tag::Tag(_) => (),
            Tag::Value(ref class) => {
                if class.value.ends_with("SimpleStrategy") {
                    if v.len() > 1 {
                        anyhow::bail!(
                            "SimpleStrategy map literal should only contain a single 'replication_factor' key!"
                        )
                    } else if v.is_empty() {
                        anyhow::bail!("SimpleStrategy map literal should contain a 'replication_factor' key!")
                    }
                    let (k, v) = v.into_iter().next().unwrap();
                    if let Tag::Value(Term::Constant(Constant::String(s))) = k {
                        if s.value.to_lowercase().as_str() == "replication_factor" {
                            match v {
                                Tag::Value(Term::Constant(Constant::Integer(i))) => {
                                    replication_factor = Some(Tag::Value(i.parse()?));
                                }
                                Tag::Tag(t) => replication_factor = Some(Tag::Tag(t)),
                                _ => anyhow::bail!("Invalid replication factor value: {}", v),
                            }
                        } else {
                            anyhow::bail!("SimpleStrategy map literal should only contain a 'class' and 'replication_factor' key!")
                        }
                    } else {
                        anyhow::bail!("Invalid key: {}", k)
                    }
                } else {
                    for (k, v) in v {
                        if let Tag::Value(Term::Constant(Constant::String(ref s))) = k {
                            if s.value.to_lowercase().as_str() == "replication_factor" {
                                match v {
                                    Tag::Value(Term::Constant(Constant::Integer(i))) => {
                                        replication_factor = Some(Tag::Value(i.parse()?));
                                    }
                                    Tag::Tag(t) => replication_factor = Some(Tag::Tag(t)),
                                    _ => anyhow::bail!("Invalid replication factor value: {}", v),
                                }
                                continue;
                            }
                        }
                        datacenters.insert(
                            match k {
                                Tag::Tag(t) => Tag::Tag(t),
                                Tag::Value(Term::Constant(Constant::String(s))) => Tag::Value(s),
                                _ => anyhow::bail!("Invalid key in replication map literal!"),
                            },
                            match v {
                                Tag::Tag(t) => Tag::Tag(t),
                                Tag::Value(Term::Constant(Constant::Integer(i))) => Tag::Value(i.parse()?),
                                _ => anyhow::bail!("Invalid replication factor value: {}", v),
                            },
                        );
                    }
                }
            }
        }
        Ok(Self {
            class,
            replication_factor,
            datacenters,
        })
    }
}

impl Parse for TaggedReplication {
    type Output = Self;
    fn parse(s: &mut StatementStream<'_>) -> anyhow::Result<Self::Output> {
        s.parse::<TaggedMapLiteral>()?.try_into()
    }
}

#[derive(ParseFromStr, Clone, Debug, ToTokens, PartialEq)]
pub enum SpeculativeRetry {
    None,
    Always,
    Percentile(f32),
    Custom(LitStr),
}

impl SpeculativeRetry {
    pub fn custom<S: Into<LitStr>>(s: S) -> Self {
        SpeculativeRetry::Custom(s.into())
    }
}

impl Default for SpeculativeRetry {
    fn default() -> Self {
        SpeculativeRetry::Percentile(99.0)
    }
}

impl Parse for SpeculativeRetry {
    type Output = Self;
    fn parse(s: &mut StatementStream<'_>) -> anyhow::Result<Self::Output> {
        let token = s.parse::<LitStr>()?;
        Ok(match token.value.to_uppercase().as_str() {
            "NONE" => SpeculativeRetry::None,
            "ALWAYS" => SpeculativeRetry::Always,
            _ => {
                if let Ok(res) = StatementStream::new(&token.value).parse_from::<(Float, PERCENTILE)>() {
                    SpeculativeRetry::Percentile(res.0.parse()?)
                } else if let Ok(res) = StatementStream::new(&token.value).parse_from::<(Number, PERCENTILE)>() {
                    SpeculativeRetry::Percentile(res.0.parse()?)
                } else {
                    SpeculativeRetry::Custom(token)
                }
            }
        })
    }
}

impl Display for SpeculativeRetry {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            SpeculativeRetry::None => write!(f, "'NONE'"),
            SpeculativeRetry::Always => write!(f, "'ALWAYS'"),
            SpeculativeRetry::Percentile(p) => write!(f, "'{}PERCENTILE'", format_cql_f32(*p)),
            SpeculativeRetry::Custom(s) => s.fmt(f),
        }
    }
}

#[derive(Builder, Copy, Clone, Debug, Default, ToTokens, PartialEq)]
#[builder(setter(strip_option), default, build_fn(validate = "Self::validate"))]
pub struct SizeTieredCompactionStrategy {
    enabled: Option<bool>,
    tombstone_threshold: Option<f32>,
    tombstone_compaction_interval: Option<i32>,
    log_all: Option<bool>,
    unchecked_tombstone_compaction: Option<bool>,
    only_purge_repaired_tombstone: Option<bool>,
    min_threshold: Option<i32>,
    max_threshold: Option<i32>,
    min_sstable_size: Option<i32>,
    bucket_low: Option<f32>,
    bucket_high: Option<f32>,
}

impl SizeTieredCompactionStrategyBuilder {
    fn validate(&self) -> Result<(), String> {
        if let Some(v) = self.tombstone_threshold.flatten() {
            if v < 0.0 || v > 1.0 {
                return Err(format!("tombstone_threshold must be between 0.0 and 1.0, found {}", v));
            }
        }
        if let Some(v) = self.tombstone_compaction_interval.flatten() {
            if v < 0 {
                return Err(format!(
                    "tombstone_compaction_interval must be a positive integer, found {}",
                    v
                ));
            }
        }
        if let Some(v) = self.min_sstable_size.flatten() {
            if v < 0 {
                return Err(format!("min_sstable_size must be a positive integer, found {}", v));
            }
        }
        if let Some(v) = self.bucket_high.flatten() {
            if v < 0.0 {
                return Err(format!("bucket_high must be a positive float, found {}", v));
            }
        }
        if let Some(v) = self.bucket_low.flatten() {
            if v < 0.0 {
                return Err(format!("bucket_low must be a positive float, found {}", v));
            }
        }
        if let Some(v) = self.min_threshold.flatten() {
            if v < 0 {
                return Err(format!("min_threshold must be a positive integer, found {}", v));
            }
        }
        if let Some(v) = self.max_threshold.flatten() {
            if v < 0 {
                return Err(format!("max_threshold must be a positive integer, found {}", v));
            }
        }
        Ok(())
    }
}

impl CompactionType for SizeTieredCompactionStrategy {}

impl Display for SizeTieredCompactionStrategy {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut res = vec![format!("'class': 'SizeTieredCompactionStrategy'")];
        if let Some(enabled) = self.enabled {
            res.push(format!("'enabled': {}", enabled));
        }
        if let Some(tombstone_threshold) = self.tombstone_threshold {
            res.push(format!(
                "'tombstone_threshold': {}",
                format_cql_f32(tombstone_threshold)
            ));
        }
        if let Some(tombstone_compaction_interval) = self.tombstone_compaction_interval {
            res.push(format!(
                "'tombstone_compaction_interval': {}",
                tombstone_compaction_interval
            ));
        }
        if let Some(log_all) = self.log_all {
            res.push(format!("'log_all': {}", log_all));
        }
        if let Some(unchecked_tombstone_compaction) = self.unchecked_tombstone_compaction {
            res.push(format!(
                "'unchecked_tombstone_compaction': {}",
                unchecked_tombstone_compaction
            ));
        }
        if let Some(only_purge_repaired_tombstone) = self.only_purge_repaired_tombstone {
            res.push(format!(
                "'only_purge_repaired_tombstone': {}",
                only_purge_repaired_tombstone
            ));
        }
        if let Some(min_threshold) = self.min_threshold {
            res.push(format!("'min_threshold': {}", min_threshold));
        }
        if let Some(max_threshold) = self.max_threshold {
            res.push(format!("'max_threshold': {}", max_threshold));
        }
        if let Some(min_sstable_size) = self.min_sstable_size {
            res.push(format!("'min_sstable_size': {}", min_sstable_size));
        }
        if let Some(bucket_low) = self.bucket_low {
            res.push(format!("'bucket_low': {}", format_cql_f32(bucket_low)));
        }
        if let Some(bucket_high) = self.bucket_high {
            res.push(format!("'bucket_high': {}", format_cql_f32(bucket_high)));
        }
        write!(f, "{{{}}}", res.join(", "))
    }
}

#[derive(Builder, Copy, Clone, Debug, Default, ToTokens, PartialEq)]
#[builder(setter(strip_option), default, build_fn(validate = "Self::validate"))]
pub struct LeveledCompactionStrategy {
    enabled: Option<bool>,
    tombstone_threshold: Option<f32>,
    tombstone_compaction_interval: Option<i32>,
    log_all: Option<bool>,
    unchecked_tombstone_compaction: Option<bool>,
    only_purge_repaired_tombstone: Option<bool>,
    min_threshold: Option<i32>,
    max_threshold: Option<i32>,
    sstable_size_in_mb: Option<i32>,
    fanout_size: Option<i32>,
}

impl LeveledCompactionStrategyBuilder {
    fn validate(&self) -> Result<(), String> {
        if let Some(v) = self.tombstone_threshold.flatten() {
            if v < 0.0 || v > 1.0 {
                return Err(format!("tombstone_threshold must be between 0.0 and 1.0, found {}", v));
            }
        }
        if let Some(v) = self.tombstone_compaction_interval.flatten() {
            if v < 0 {
                return Err(format!(
                    "tombstone_compaction_interval must be a positive integer, found {}",
                    v
                ));
            }
        }
        if let Some(v) = self.sstable_size_in_mb.flatten() {
            if v < 0 {
                return Err(format!("sstable_size_in_mb must be a positive integer, found {}", v));
            }
        }
        if let Some(v) = self.fanout_size.flatten() {
            if v < 0 {
                return Err(format!("fanout_size must be a positive integer, found {}", v));
            }
        }
        if let Some(v) = self.min_threshold.flatten() {
            if v < 0 {
                return Err(format!("min_threshold must be a positive integer, found {}", v));
            }
        }
        if let Some(v) = self.max_threshold.flatten() {
            if v < 0 {
                return Err(format!("max_threshold must be a positive integer, found {}", v));
            }
        }
        Ok(())
    }
}

impl CompactionType for LeveledCompactionStrategy {}

impl Display for LeveledCompactionStrategy {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut res = vec![format!("'class': 'LeveledCompactionStrategy'")];
        if let Some(enabled) = self.enabled {
            res.push(format!("'enabled': {}", enabled));
        }
        if let Some(tombstone_threshold) = self.tombstone_threshold {
            res.push(format!(
                "'tombstone_threshold': {}",
                format_cql_f32(tombstone_threshold)
            ));
        }
        if let Some(tombstone_compaction_interval) = self.tombstone_compaction_interval {
            res.push(format!(
                "'tombstone_compaction_interval': {}",
                tombstone_compaction_interval
            ));
        }
        if let Some(log_all) = self.log_all {
            res.push(format!("'log_all': {}", log_all));
        }
        if let Some(unchecked_tombstone_compaction) = self.unchecked_tombstone_compaction {
            res.push(format!(
                "'unchecked_tombstone_compaction': {}",
                unchecked_tombstone_compaction
            ));
        }
        if let Some(only_purge_repaired_tombstone) = self.only_purge_repaired_tombstone {
            res.push(format!(
                "'only_purge_repaired_tombstone': {}",
                only_purge_repaired_tombstone
            ));
        }
        if let Some(min_threshold) = self.min_threshold {
            res.push(format!("'min_threshold': {}", min_threshold));
        }
        if let Some(max_threshold) = self.max_threshold {
            res.push(format!("'max_threshold': {}", max_threshold));
        }
        if let Some(sstable_size_in_mb) = self.sstable_size_in_mb {
            res.push(format!("'sstable_size_in_mb': {}", sstable_size_in_mb));
        }
        if let Some(fanout_size) = self.fanout_size {
            res.push(format!("'fanout_size': {}", fanout_size));
        }
        write!(f, "{{{}}}", res.join(", "))
    }
}

#[derive(Builder, Copy, Clone, Debug, Default, ToTokens, PartialEq)]
#[builder(setter(strip_option), default, build_fn(validate = "Self::validate"))]
pub struct TimeWindowCompactionStrategy {
    enabled: Option<bool>,
    tombstone_threshold: Option<f32>,
    tombstone_compaction_interval: Option<i32>,
    log_all: Option<bool>,
    unchecked_tombstone_compaction: Option<bool>,
    only_purge_repaired_tombstone: Option<bool>,
    min_threshold: Option<i32>,
    max_threshold: Option<i32>,
    compaction_window_unit: Option<JavaTimeUnit>,
    compaction_window_size: Option<i32>,
    unsafe_aggressive_sstable_expiration: Option<bool>,
}

impl TimeWindowCompactionStrategyBuilder {
    fn validate(&self) -> Result<(), String> {
        if let Some(v) = self.tombstone_threshold.flatten() {
            if v < 0.0 || v > 1.0 {
                return Err(format!("tombstone_threshold must be between 0.0 and 1.0, found {}", v));
            }
        }
        if let Some(v) = self.tombstone_compaction_interval.flatten() {
            if v < 0 {
                return Err(format!(
                    "tombstone_compaction_interval must be a positive integer, found {}",
                    v
                ));
            }
        }
        if let Some(v) = self.compaction_window_size.flatten() {
            if v < 0 {
                return Err(format!(
                    "compaction_window_size must be a positive integer, found {}",
                    v
                ));
            }
        }
        if let Some(v) = self.min_threshold.flatten() {
            if v < 0 {
                return Err(format!("min_threshold must be a positive integer, found {}", v));
            }
        }
        if let Some(v) = self.max_threshold.flatten() {
            if v < 0 {
                return Err(format!("max_threshold must be a positive integer, found {}", v));
            }
        }
        Ok(())
    }
}

impl CompactionType for TimeWindowCompactionStrategy {}

impl Display for TimeWindowCompactionStrategy {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut res = vec![format!("'class': 'TimeWindowCompactionStrategy'")];
        if let Some(enabled) = self.enabled {
            res.push(format!("'enabled': {}", enabled));
        }
        if let Some(tombstone_threshold) = self.tombstone_threshold {
            res.push(format!(
                "'tombstone_threshold': {}",
                format_cql_f32(tombstone_threshold)
            ));
        }
        if let Some(tombstone_compaction_interval) = self.tombstone_compaction_interval {
            res.push(format!(
                "'tombstone_compaction_interval': {}",
                tombstone_compaction_interval
            ));
        }
        if let Some(log_all) = self.log_all {
            res.push(format!("'log_all': {}", log_all));
        }
        if let Some(unchecked_tombstone_compaction) = self.unchecked_tombstone_compaction {
            res.push(format!(
                "'unchecked_tombstone_compaction': {}",
                unchecked_tombstone_compaction
            ));
        }
        if let Some(only_purge_repaired_tombstone) = self.only_purge_repaired_tombstone {
            res.push(format!(
                "'only_purge_repaired_tombstone': {}",
                only_purge_repaired_tombstone
            ));
        }
        if let Some(min_threshold) = self.min_threshold {
            res.push(format!("'min_threshold': {}", min_threshold));
        }
        if let Some(max_threshold) = self.max_threshold {
            res.push(format!("'max_threshold': {}", max_threshold));
        }
        if let Some(compaction_window_unit) = self.compaction_window_unit {
            res.push(format!("'compaction_window_unit': {}", compaction_window_unit));
        }
        if let Some(compaction_window_size) = self.compaction_window_size {
            res.push(format!("'compaction_window_size': {}", compaction_window_size));
        }
        if let Some(unsafe_aggressive_sstable_expiration) = self.unsafe_aggressive_sstable_expiration {
            res.push(format!(
                "'unsafe_aggressive_sstable_expiration': {}",
                unsafe_aggressive_sstable_expiration
            ));
        }
        write!(f, "{{{}}}", res.join(", "))
    }
}

pub trait CompactionType: Display + Into<Compaction> {}

#[derive(Clone, Debug, From, TryInto, ToTokens, PartialEq)]
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

impl TryFrom<MapLiteral> for Compaction {
    type Error = anyhow::Error;

    fn try_from(value: MapLiteral) -> Result<Self, Self::Error> {
        let mut class = None;
        let v = value
            .elements
            .into_iter()
            .filter(|(k, v)| {
                if let Term::Constant(Constant::String(s)) = k {
                    if s.value.to_lowercase().as_str() == "class" {
                        class = Some(v.clone());
                        return false;
                    }
                }
                true
            })
            .collect::<Vec<_>>();
        let class = class.ok_or_else(|| anyhow::anyhow!("No class in compaction map literal!"))?;
        Ok(match class {
            Term::Constant(Constant::String(s)) => {
                let mut map = HashMap::new();
                for (k, v) in v {
                    if let Term::Constant(Constant::String(s)) = k {
                        map.insert(s.value.to_lowercase(), v);
                    } else {
                        anyhow::bail!("Invalid key in compaction map literal!");
                    }
                }
                if s.value.ends_with("SizeTieredCompactionStrategy") {
                    let mut builder = Self::size_tiered();
                    if let Some(t) = map.remove("enabled") {
                        builder.enabled(t.try_into()?);
                    }
                    if let Some(t) = map.remove("tombstone_threshold") {
                        builder.tombstone_threshold(t.try_into()?);
                    }
                    if let Some(t) = map.remove("tombstone_compaction_interval") {
                        builder.tombstone_compaction_interval(t.try_into()?);
                    }
                    if let Some(t) = map.remove("log_all") {
                        builder.log_all(t.try_into()?);
                    }
                    if let Some(t) = map.remove("unchecked_tombstone_compaction") {
                        builder.unchecked_tombstone_compaction(t.try_into()?);
                    }
                    if let Some(t) = map.remove("only_purge_repaired_tombstone") {
                        builder.only_purge_repaired_tombstone(t.try_into()?);
                    }
                    if let Some(t) = map.remove("min_threshold") {
                        builder.min_threshold(t.try_into()?);
                    }
                    if let Some(t) = map.remove("max_threshold") {
                        builder.max_threshold(t.try_into()?);
                    }
                    if let Some(t) = map.remove("min_sstable_size") {
                        builder.min_sstable_size(t.try_into()?);
                    }
                    if let Some(t) = map.remove("bucket_low") {
                        builder.bucket_low(t.try_into()?);
                    }
                    if let Some(t) = map.remove("bucket_high") {
                        builder.bucket_high(t.try_into()?);
                    }
                    Compaction::SizeTiered(builder.build()?)
                } else if s.value.ends_with("LeveledCompactionStrategy") {
                    let mut builder = Self::leveled();
                    if let Some(t) = map.remove("enabled") {
                        builder.enabled(t.try_into()?);
                    }
                    if let Some(t) = map.remove("tombstone_threshold") {
                        builder.tombstone_threshold(t.try_into()?);
                    }
                    if let Some(t) = map.remove("tombstone_compaction_interval") {
                        builder.tombstone_compaction_interval(t.try_into()?);
                    }
                    if let Some(t) = map.remove("log_all") {
                        builder.log_all(t.try_into()?);
                    }
                    if let Some(t) = map.remove("unchecked_tombstone_compaction") {
                        builder.unchecked_tombstone_compaction(t.try_into()?);
                    }
                    if let Some(t) = map.remove("only_purge_repaired_tombstone") {
                        builder.only_purge_repaired_tombstone(t.try_into()?);
                    }
                    if let Some(t) = map.remove("min_threshold") {
                        builder.min_threshold(t.try_into()?);
                    }
                    if let Some(t) = map.remove("max_threshold") {
                        builder.max_threshold(t.try_into()?);
                    }
                    if let Some(t) = map.remove("sstable_size_in_mb") {
                        builder.sstable_size_in_mb(t.try_into()?);
                    }
                    if let Some(t) = map.remove("fanout_size") {
                        builder.fanout_size(t.try_into()?);
                    }
                    Compaction::Leveled(builder.build()?)
                } else if s.value.ends_with("TimeWindowCompactionStrategy") {
                    let mut builder = Self::time_window();
                    if let Some(t) = map.remove("enabled") {
                        builder.enabled(t.try_into()?);
                    }
                    if let Some(t) = map.remove("tombstone_threshold") {
                        builder.tombstone_threshold(t.try_into()?);
                    }
                    if let Some(t) = map.remove("tombstone_compaction_interval") {
                        builder.tombstone_compaction_interval(t.try_into()?);
                    }
                    if let Some(t) = map.remove("log_all") {
                        builder.log_all(t.try_into()?);
                    }
                    if let Some(t) = map.remove("unchecked_tombstone_compaction") {
                        builder.unchecked_tombstone_compaction(t.try_into()?);
                    }
                    if let Some(t) = map.remove("only_purge_repaired_tombstone") {
                        builder.only_purge_repaired_tombstone(t.try_into()?);
                    }
                    if let Some(t) = map.remove("min_threshold") {
                        builder.min_threshold(t.try_into()?);
                    }
                    if let Some(t) = map.remove("max_threshold") {
                        builder.max_threshold(t.try_into()?);
                    }
                    if let Some(t) = map.remove("compaction_window_unit") {
                        builder.compaction_window_unit(TryInto::<LitStr>::try_into(t)?.value.parse()?);
                    }
                    if let Some(t) = map.remove("compaction_window_size") {
                        builder.compaction_window_size(t.try_into()?);
                    }
                    if let Some(t) = map.remove("unsafe_aggressive_sstable_expiration") {
                        builder.unsafe_aggressive_sstable_expiration(t.try_into()?);
                    }
                    Compaction::TimeWindow(builder.build()?)
                } else {
                    return Err(anyhow::anyhow!("Unknown compaction class: {}", s));
                }
            }
            _ => anyhow::bail!("Invalid class: {}", class),
        })
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

#[derive(Copy, Clone, Debug, ToTokens, PartialEq, Eq)]
pub enum JavaTimeUnit {
    Minutes,
    Hours,
    Days,
}

impl FromStr for JavaTimeUnit {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "MINUTES" => Ok(JavaTimeUnit::Minutes),
            "HOURS" => Ok(JavaTimeUnit::Hours),
            "DAYS" => Ok(JavaTimeUnit::Days),
            _ => Err(anyhow::anyhow!("Invalid time unit: {}", s)),
        }
    }
}

impl Parse for JavaTimeUnit {
    type Output = Self;

    fn parse(s: &mut StatementStream<'_>) -> anyhow::Result<Self::Output> {
        s.parse::<LitStr>()?.value.parse()
    }
}

impl Display for JavaTimeUnit {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            JavaTimeUnit::Minutes => write!(f, "'MINUTES'"),
            JavaTimeUnit::Hours => write!(f, "'HOURS'"),
            JavaTimeUnit::Days => write!(f, "'DAYS'"),
        }
    }
}

#[derive(Builder, Clone, Debug, Default, ToTokens, PartialEq)]
#[builder(setter(strip_option), default)]
pub struct Compression {
    #[builder(setter(into))]
    class: Option<LitStr>,
    enabled: Option<bool>,
    chunk_length_in_kb: Option<i32>,
    crc_check_chance: Option<f32>,
    compression_level: Option<i32>,
}

impl Compression {
    pub fn build() -> CompressionBuilder {
        CompressionBuilder::default()
    }
}

impl Display for Compression {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut res = Vec::new();
        if let Some(ref class) = self.class {
            res.push(format!("'class': {}", class));
        }
        if let Some(enabled) = self.enabled {
            res.push(format!("'enabled': {}", enabled));
        }
        if let Some(chunk_length_in_kb) = self.chunk_length_in_kb {
            res.push(format!("'chunk_length_in_kb': {}", chunk_length_in_kb));
        }
        if let Some(crc_check_chance) = self.crc_check_chance {
            res.push(format!("'crc_check_chance': {}", format_cql_f32(crc_check_chance)));
        }
        if let Some(compression_level) = self.compression_level {
            res.push(format!("'compression_level': {}", compression_level));
        }
        write!(f, "{{{}}}", res.join(", "))
    }
}

impl TryFrom<MapLiteral> for Compression {
    type Error = anyhow::Error;

    fn try_from(value: MapLiteral) -> Result<Self, Self::Error> {
        let mut map = HashMap::new();
        for (k, v) in value.elements {
            if let Term::Constant(Constant::String(s)) = k {
                map.insert(s.value.to_lowercase(), v);
            } else {
                anyhow::bail!("Invalid key in compaction map literal!");
            }
        }
        let mut builder = Self::build();
        if let Some(t) = map.remove("class") {
            builder.class(TryInto::<LitStr>::try_into(t)?);
        }
        if let Some(t) = map.remove("enabled") {
            builder.enabled(t.try_into()?);
        }
        if let Some(t) = map.remove("chunk_length_in_kb") {
            builder.chunk_length_in_kb(t.try_into()?);
        }
        if let Some(t) = map.remove("crc_check_chance") {
            builder.crc_check_chance(t.try_into()?);
        }
        if let Some(t) = map.remove("compression_level") {
            builder.compression_level(t.try_into()?);
        }
        Ok(builder.build()?)
    }
}

#[derive(Builder, Clone, Debug, Default, ToTokens, PartialEq, Eq)]
#[builder(setter(strip_option), default)]
pub struct Caching {
    keys: Option<Keys>,
    rows_per_partition: Option<RowsPerPartition>,
}

impl Caching {
    pub fn build() -> CachingBuilder {
        CachingBuilder::default()
    }
}

impl Display for Caching {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut res = Vec::new();
        if let Some(keys) = &self.keys {
            res.push(format!("'keys': {}", keys));
        }
        if let Some(rows_per_partition) = &self.rows_per_partition {
            res.push(format!("'rows_per_partition': {}", rows_per_partition));
        }
        write!(f, "{{{}}}", res.join(", "))
    }
}

impl TryFrom<MapLiteral> for Caching {
    type Error = anyhow::Error;

    fn try_from(value: MapLiteral) -> Result<Self, Self::Error> {
        let mut map = HashMap::new();
        for (k, v) in value.elements {
            if let Term::Constant(Constant::String(s)) = k {
                map.insert(s.value.to_lowercase(), v);
            } else {
                anyhow::bail!("Invalid key in compaction map literal!");
            }
        }
        let mut builder = Self::build();
        if let Some(t) = map.remove("keys") {
            builder.keys(TryInto::<LitStr>::try_into(t)?.value.parse()?);
        }
        if let Some(t) = map.remove("rows_per_partition") {
            builder.rows_per_partition(t.to_string().parse()?);
        }
        Ok(builder.build()?)
    }
}

#[derive(Copy, Clone, Debug, ToTokens, PartialEq, Eq)]
pub enum Keys {
    All,
    None,
}

impl FromStr for Keys {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_uppercase().as_str() {
            "ALL" => Ok(Keys::All),
            "NONE" => Ok(Keys::None),
            _ => Err(anyhow::anyhow!("Invalid keys: {}", s)),
        }
    }
}

impl Parse for Keys {
    type Output = Self;
    fn parse(s: &mut StatementStream<'_>) -> anyhow::Result<Self::Output> {
        s.parse::<LitStr>()?.value.parse()
    }
}

impl Display for Keys {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Keys::All => write!(f, "'ALL'"),
            Keys::None => write!(f, "'NONE'"),
        }
    }
}

#[derive(ParseFromStr, Copy, Clone, Debug, ToTokens, PartialEq, Eq)]
pub enum RowsPerPartition {
    All,
    None,
    Count(i32),
}

impl Parse for RowsPerPartition {
    type Output = Self;
    fn parse(s: &mut StatementStream<'_>) -> anyhow::Result<Self::Output> {
        Ok(if let Some(ss) = s.parse::<Option<LitStr>>()? {
            match ss.value.to_uppercase().as_str() {
                "ALL" => RowsPerPartition::All,
                "NONE" => RowsPerPartition::None,
                _ => anyhow::bail!("Invalid rows_per_partition: {}", ss),
            }
        } else {
            Self::Count(s.parse()?)
        })
    }
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

pub fn format_cql_f32(f: f32) -> String {
    let s = f.to_string();
    if let Ok(res) = StatementStream::new(&s).parse_from::<Float>() {
        res
    } else {
        format!("{}.0", s)
    }
}

pub fn format_cql_f64(f: f64) -> String {
    let s = f.to_string();
    if let Ok(res) = StatementStream::new(&s).parse_from::<Float>() {
        res
    } else {
        format!("{}.0", s)
    }
}

pub trait CustomToTokens<'a> {
    fn to_tokens(&'a self, tokens: &mut TokenStream);
}

pub struct TokenWrapper<'a, T>(pub &'a T);

impl<'a, T> ToTokens for TokenWrapper<'a, T>
where
    T: CustomToTokens<'a>,
{
    fn to_tokens(&self, tokens: &mut TokenStream) {
        self.0.to_tokens(tokens);
    }
}

impl<'a, T: 'a> CustomToTokens<'a> for Option<T>
where
    TokenWrapper<'a, T>: ToTokens,
{
    fn to_tokens(&'a self, tokens: &mut TokenStream) {
        tokens.extend(match self {
            Some(t) => {
                let t = TokenWrapper(t);
                quote! {Some(#t)}
            }
            None => quote! {None},
        });
    }
}

impl<'a, T: 'a> CustomToTokens<'a> for Box<T>
where
    T: ToTokens,
{
    fn to_tokens(&'a self, tokens: &mut TokenStream) {
        let i = self;
        tokens.extend(quote! {Box::new(#i)});
    }
}

impl<'a, T: 'a> CustomToTokens<'a> for Vec<T>
where
    TokenWrapper<'a, T>: ToTokens,
{
    fn to_tokens(&'a self, tokens: &mut TokenStream) {
        let t = self.iter().map(|t| TokenWrapper(t));
        tokens.extend(quote! { vec![#(#t),*]});
    }
}

impl<'a, K: 'static, V: 'static> CustomToTokens<'a> for HashMap<K, V>
where
    TokenWrapper<'a, K>: ToTokens,
    TokenWrapper<'a, V>: ToTokens,
{
    fn to_tokens(&'a self, tokens: &mut TokenStream) {
        let t = self.iter().map(|(k, v)| {
            let (k, v) = (TokenWrapper(k), TokenWrapper(v));
            quote! {#k => #v}
        });
        tokens.extend(quote! { maplit::hashmap![#(#t),*]});
    }
}

impl<'a, K: 'static, V: 'static> CustomToTokens<'a> for BTreeMap<K, V>
where
    TokenWrapper<'a, K>: ToTokens,
    TokenWrapper<'a, V>: ToTokens,
{
    fn to_tokens(&'a self, tokens: &mut TokenStream) {
        let t = self.iter().map(|(k, v)| {
            let (k, v) = (TokenWrapper(k), TokenWrapper(v));
            quote! {#k => #v}
        });
        tokens.extend(quote! { maplit::btreemap![#(#t),*]});
    }
}

impl<'a, K: 'static> CustomToTokens<'a> for BTreeSet<K>
where
    TokenWrapper<'a, K>: ToTokens,
{
    fn to_tokens(&'a self, tokens: &mut TokenStream) {
        let t = self.iter().map(|k| TokenWrapper(k));
        tokens.extend(quote! { maplit::btreeset![#(#t),*]});
    }
}
impl<'a> CustomToTokens<'a> for &str {
    fn to_tokens(&'a self, tokens: &mut TokenStream) {
        let s = self;
        tokens.extend(quote! { #s });
    }
}

impl<'a> CustomToTokens<'a> for String {
    fn to_tokens(&'a self, tokens: &mut TokenStream) {
        let s = self;
        tokens.extend(quote! { #s.to_string() });
    }
}

macro_rules! impl_custom_to_tokens {
    ($($t:ty),*) => {
        $(
            impl<'a> CustomToTokens<'a> for $t {
                fn to_tokens(&'a self, tokens: &mut quote::__private::TokenStream) {
                    ToTokens::to_tokens(self, tokens);
                }
            }
        )*
    };
}

impl_custom_to_tokens!(i8, i32, i64, u8, u32, u64, f32, f64, bool, char);

macro_rules! impl_custom_to_tokens_tuple {
    ($(($t:ident, $v:ident)),+) => {
        impl<'a, $($t: ToTokens),+> CustomToTokens<'a> for ($($t),+,) {
            fn to_tokens(&'a self, tokens: &mut quote::__private::TokenStream) {
                let ( $($v),*, ) = self;
                tokens.extend(quote! { ($(#$v),*) });
            }
        }
    };
}

impl_custom_to_tokens_tuple!((T0, t0));
impl_custom_to_tokens_tuple!((T0, t0), (T1, t1));
impl_custom_to_tokens_tuple!((T0, t0), (T1, t1), (T2, t2));
impl_custom_to_tokens_tuple!((T0, t0), (T1, t1), (T2, t2), (T3, t3));
impl_custom_to_tokens_tuple!((T0, t0), (T1, t1), (T2, t2), (T3, t3), (T4, t4));
impl_custom_to_tokens_tuple!((T0, t0), (T1, t1), (T2, t2), (T3, t3), (T4, t4), (T5, t5));
impl_custom_to_tokens_tuple!((T0, t0), (T1, t1), (T2, t2), (T3, t3), (T4, t4), (T5, t5), (T6, t6));
impl_custom_to_tokens_tuple!(
    (T0, t0),
    (T1, t1),
    (T2, t2),
    (T3, t3),
    (T4, t4),
    (T5, t5),
    (T6, t6),
    (T7, t7)
);
impl_custom_to_tokens_tuple!(
    (T0, t0),
    (T1, t1),
    (T2, t2),
    (T3, t3),
    (T4, t4),
    (T5, t5),
    (T6, t6),
    (T7, t7),
    (T8, t8)
);
impl_custom_to_tokens_tuple!(
    (T0, t0),
    (T1, t1),
    (T2, t2),
    (T3, t3),
    (T4, t4),
    (T5, t5),
    (T6, t6),
    (T7, t7),
    (T8, t8),
    (T9, t9)
);

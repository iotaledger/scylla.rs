// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

//! This module implements the frame encoder.

use super::Blob;
use chrono::{
    Datelike,
    NaiveDate,
    NaiveDateTime,
    NaiveTime,
    Timelike,
};
use core::fmt::{
    Debug,
    Formatter,
};
use std::{
    collections::HashMap,
    convert::Infallible,
    net::{
        IpAddr,
        Ipv4Addr,
        Ipv6Addr,
    },
};
use thiserror::Error;

/// The 16-byte body length.
pub const BE_16_BYTES_LEN: [u8; 4] = [0, 0, 0, 16];
/// The 8-byte body length.
pub const BE_8_BYTES_LEN: [u8; 4] = [0, 0, 0, 8];
/// The 4-byte body length.
pub const BE_4_BYTES_LEN: [u8; 4] = [0, 0, 0, 4];
/// The 2-byte body length.
pub const BE_2_BYTES_LEN: [u8; 4] = [0, 0, 0, 2];
/// The 1-byte body length.
pub const BE_1_BYTES_LEN: [u8; 4] = [0, 0, 0, 1];
/// The 0-byte body length.
pub const BE_0_BYTES_LEN: [u8; 4] = [0, 0, 0, 0];
/// The NULL body length.
pub const BE_NULL_BYTES_LEN: [u8; 4] = [255, 255, 255, 255]; // -1 length
/// The UNSET body length.
pub const BE_UNSET_BYTES_LEN: [u8; 4] = [255, 255, 255, 254]; // -2 length
/// The NULL value used to indicate the body length.
#[allow(unused)]
pub const NULL_VALUE: Null = Null;
/// The unset value used to indicate the body length.
pub const UNSET_VALUE: Unset = Unset;
/// The Null unit stucture.
pub struct Null;
/// The Unset unit stucture.
pub struct Unset;

/// The frame column encoder.
pub trait ColumnEncoder {
    type Error: Debug;

    /// Encoder the column buffer.
    fn encode(&self, buffer: &mut Vec<u8>) -> Result<(), Self::Error>;

    /// Encode this value to a new buffer
    fn encode_new(&self) -> Result<Vec<u8>, Self::Error> {
        let mut buf = Vec::new();
        self.encode(&mut buf)?;
        Ok(buf)
    }

    /// Encode this value to a new buffer with a given capacity
    fn encode_with_capacity(&self, capacity: usize) -> Result<Vec<u8>, Self::Error> {
        let mut buf = Vec::with_capacity(capacity);
        self.encode(&mut buf)?;
        Ok(buf)
    }
}

impl<E: ColumnEncoder + ?Sized> ColumnEncoder for &E {
    type Error = E::Error;
    fn encode(&self, buffer: &mut Vec<u8>) -> Result<(), Self::Error> {
        E::encode(*self, buffer)
    }
}

impl<E: ColumnEncoder + ?Sized> ColumnEncoder for Box<E> {
    type Error = E::Error;
    fn encode(&self, buffer: &mut Vec<u8>) -> Result<(), Self::Error> {
        E::encode(&*self, buffer)
    }
}

impl<E: ColumnEncoder> ColumnEncoder for Option<E> {
    type Error = E::Error;
    fn encode(&self, buffer: &mut Vec<u8>) -> Result<(), Self::Error> {
        match self {
            Some(value) => {
                value.encode(buffer)?;
            }
            None => {
                let _ = ColumnEncoder::encode(&UNSET_VALUE, buffer);
            }
        }
        Ok(())
    }
}

#[derive(Error)]
pub enum ListEncodeError<E: ColumnEncoder> {
    #[error("List Value encode error: {0:?}")]
    ValueEncodeError(E::Error),
    #[error("List is too large! Max byte length is {}", i32::MAX)]
    TooLarge,
}

impl<E: ColumnEncoder> Debug for ListEncodeError<E> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ValueEncodeError(arg0) => f.debug_tuple("ValueEncodeError").field(arg0).finish(),
            Self::TooLarge => write!(f, "TooLarge"),
        }
    }
}

impl<E: ColumnEncoder> ColumnEncoder for Vec<E> {
    type Error = ListEncodeError<E>;
    fn encode(&self, buffer: &mut Vec<u8>) -> Result<(), Self::Error> {
        let mut buf = Vec::new();
        for e in self {
            e.encode(&mut buf).map_err(ListEncodeError::ValueEncodeError)?;
        }
        buffer.extend(&i32::to_be_bytes(buf.len() as i32));
        buffer.extend(buf);
        Ok(())
    }
}

#[derive(Error)]
pub enum MapEncodeError<K: ColumnEncoder, V: ColumnEncoder> {
    #[error("Map Key encode error: {0:?}")]
    KeyEncodeError(K::Error),
    #[error("Map Value encode error: {0:?}")]
    ValueEncodeError(V::Error),
    #[error("Map is too large! Max byte length is {}", i32::MAX)]
    TooLarge,
}

impl<K: ColumnEncoder, V: ColumnEncoder> Debug for MapEncodeError<K, V> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::KeyEncodeError(arg0) => f.debug_tuple("KeyEncodeError").field(arg0).finish(),
            Self::ValueEncodeError(arg0) => f.debug_tuple("ValueEncodeError").field(arg0).finish(),
            Self::TooLarge => write!(f, "TooLarge"),
        }
    }
}

impl<K: ColumnEncoder, V: ColumnEncoder, S: ::std::hash::BuildHasher> ColumnEncoder for HashMap<K, V, S> {
    type Error = MapEncodeError<K, V>;
    fn encode(&self, buffer: &mut Vec<u8>) -> Result<(), Self::Error> {
        let mut buf = Vec::new();
        for (k, v) in self {
            k.encode(&mut buf).map_err(MapEncodeError::KeyEncodeError)?;
            v.encode(&mut buf).map_err(MapEncodeError::ValueEncodeError)?;
        }
        if buf.len() > i32::MAX as usize {
            return Err(MapEncodeError::TooLarge);
        };
        buffer.extend(&i32::to_be_bytes(buf.len() as i32));
        buffer.extend(buf);
        Ok(())
    }
}

macro_rules! impl_tuple_encoder {
    ($(($v:ident, $e:tt)),*) => {
        impl<$($e: ColumnEncoder),*> ColumnEncoder for ($($e,)*) {
            type Error = anyhow::Error;
            fn encode(&self, buffer: &mut Vec<u8>) -> Result<(), Self::Error> {
                let ($($v,)*) = self;
                $(
                    $v.encode(buffer).map_err(|e| anyhow::anyhow!("{:?}", e))?;
                )*
                Ok(())
            }
        }
    };
}

impl_tuple_encoder!((e1, E1));
impl_tuple_encoder!((e1, E1), (e2, E2));
impl_tuple_encoder!((e1, E1), (e2, E2), (e3, E3));
impl_tuple_encoder!((e1, E1), (e2, E2), (e3, E3), (e4, E4));
impl_tuple_encoder!((e1, E1), (e2, E2), (e3, E3), (e4, E4), (e5, E5));
impl_tuple_encoder!((e1, E1), (e2, E2), (e3, E3), (e4, E4), (e5, E5), (e6, E6));
impl_tuple_encoder!((e1, E1), (e2, E2), (e3, E3), (e4, E4), (e5, E5), (e6, E6), (e7, E7));
impl_tuple_encoder!(
    (e1, E1),
    (e2, E2),
    (e3, E3),
    (e4, E4),
    (e5, E5),
    (e6, E6),
    (e7, E7),
    (e8, E8)
);
impl_tuple_encoder!(
    (e1, E1),
    (e2, E2),
    (e3, E3),
    (e4, E4),
    (e5, E5),
    (e6, E6),
    (e7, E7),
    (e8, E8),
    (e9, E9)
);

macro_rules! impl_simple_encoder {
    ($t:ty, $len:ident) => {
        impl ColumnEncoder for $t {
            type Error = Infallible;
            fn encode(&self, buffer: &mut Vec<u8>) -> Result<(), Self::Error> {
                buffer.extend(&$len);
                buffer.extend(&<$t>::to_be_bytes(*self));
                Ok(())
            }
        }
    };
}

impl_simple_encoder!(i128, BE_16_BYTES_LEN);
impl_simple_encoder!(i64, BE_8_BYTES_LEN);
impl_simple_encoder!(i32, BE_4_BYTES_LEN);
impl_simple_encoder!(i16, BE_2_BYTES_LEN);
impl_simple_encoder!(i8, BE_1_BYTES_LEN);
impl_simple_encoder!(u128, BE_16_BYTES_LEN);
impl_simple_encoder!(u64, BE_8_BYTES_LEN);
impl_simple_encoder!(u32, BE_4_BYTES_LEN);
impl_simple_encoder!(u16, BE_2_BYTES_LEN);
impl_simple_encoder!(u8, BE_1_BYTES_LEN);
impl_simple_encoder!(f64, BE_8_BYTES_LEN);
impl_simple_encoder!(f32, BE_4_BYTES_LEN);

impl ColumnEncoder for bool {
    type Error = Infallible;
    fn encode(&self, buffer: &mut Vec<u8>) -> Result<(), Self::Error> {
        buffer.extend(&BE_1_BYTES_LEN);
        buffer.push(*self as u8);
        Ok(())
    }
}

macro_rules! impl_str_encoder {
    ($t:ty) => {
        impl ColumnEncoder for $t {
            type Error = ColumnEncodeError;
            fn encode(&self, buffer: &mut Vec<u8>) -> Result<(), Self::Error> {
                if self.len() > i32::MAX as usize {
                    return Err(ColumnEncodeError::ValueTooLarge);
                };
                buffer.extend(&i32::to_be_bytes(self.len() as i32));
                buffer.extend(self.bytes());
                Ok(())
            }
        }
    };
}

impl_str_encoder!(String);
impl_str_encoder!(str);

#[derive(Debug, Error)]
pub enum ColumnEncodeError {
    #[error("Value is too large! Max value length is {}", i32::MAX)]
    ValueTooLarge,
}

impl ColumnEncoder for Blob {
    type Error = ColumnEncodeError;
    fn encode(&self, buffer: &mut Vec<u8>) -> Result<(), Self::Error> {
        if self.len() > i32::MAX as usize {
            return Err(ColumnEncodeError::ValueTooLarge);
        };
        buffer.extend(&i32::to_be_bytes(self.len() as i32));
        buffer.extend(self.as_slice());
        Ok(())
    }
}

impl ColumnEncoder for IpAddr {
    type Error = Infallible;
    fn encode(&self, buffer: &mut Vec<u8>) -> Result<(), Self::Error> {
        match *self {
            IpAddr::V4(ip) => {
                buffer.extend(&BE_4_BYTES_LEN);
                buffer.extend(&ip.octets());
            }
            IpAddr::V6(ip) => {
                buffer.extend(&BE_16_BYTES_LEN);
                buffer.extend(&ip.octets());
            }
        }
        Ok(())
    }
}

impl ColumnEncoder for Ipv4Addr {
    type Error = Infallible;
    fn encode(&self, buffer: &mut Vec<u8>) -> Result<(), Self::Error> {
        buffer.extend(&BE_4_BYTES_LEN);
        buffer.extend(&self.octets());
        Ok(())
    }
}

impl ColumnEncoder for Ipv6Addr {
    type Error = Infallible;
    fn encode(&self, buffer: &mut Vec<u8>) -> Result<(), Self::Error> {
        buffer.extend(&BE_16_BYTES_LEN);
        buffer.extend(&self.octets());
        Ok(())
    }
}

impl ColumnEncoder for Unset {
    type Error = Infallible;
    fn encode(&self, buffer: &mut Vec<u8>) -> Result<(), Self::Error> {
        buffer.extend(&BE_UNSET_BYTES_LEN);
        Ok(())
    }
}

impl ColumnEncoder for Null {
    type Error = Infallible;
    fn encode(&self, buffer: &mut Vec<u8>) -> Result<(), Self::Error> {
        buffer.extend(&BE_NULL_BYTES_LEN);
        Ok(())
    }
}

impl ColumnEncoder for NaiveDate {
    type Error = Infallible;
    fn encode(&self, buffer: &mut Vec<u8>) -> Result<(), Self::Error> {
        let days = self.num_days_from_ce() as u32 - 719_163 + (1u32 << 31);
        buffer.extend(&BE_4_BYTES_LEN);
        buffer.extend(&u32::to_be_bytes(days));
        Ok(())
    }
}

impl ColumnEncoder for NaiveTime {
    type Error = Infallible;
    fn encode(&self, buffer: &mut Vec<u8>) -> Result<(), Self::Error> {
        let nanos = self.hour() as u64 * 3_600_000_000_000
            + self.minute() as u64 * 60_000_000_000
            + self.second() as u64 * 1_000_000_000
            + self.nanosecond() as u64;
        buffer.extend(&BE_8_BYTES_LEN);
        buffer.extend(&u64::to_be_bytes(nanos as u64));
        Ok(())
    }
}

impl ColumnEncoder for NaiveDateTime {
    type Error = Infallible;
    fn encode(&self, buffer: &mut Vec<u8>) -> Result<(), Self::Error> {
        let cql_timestamp = self.timestamp_millis();
        buffer.extend(&BE_8_BYTES_LEN);
        buffer.extend(&u64::to_be_bytes(cql_timestamp as u64));
        Ok(())
    }
}

/// An encode chain. Allows sequential encodes stored back-to-back in a buffer.
#[derive(Default, Debug, Clone)]
pub struct TokenEncodeChain {
    len: usize,
    buffer: Option<Vec<u8>>,
}

impl TokenEncodeChain {
    fn try_from<E: ColumnEncoder + ?Sized>(e: &E) -> Result<Self, E::Error> {
        Ok(TokenEncodeChain {
            len: 1,
            buffer: Some(e.encode_new()?[2..].into()),
        })
    }
    /// Chain a new value
    pub fn chain<E: TokenEncoder + ?Sized>(mut self, other: &E) -> Result<Self, E::Error>
    where
        Self: Sized,
    {
        self.append(other)?;
        Ok(self)
    }

    /// Chain a new value
    pub fn append<E: TokenEncoder + ?Sized>(&mut self, other: &E) -> Result<(), E::Error> {
        let other = other.encode_token()?;
        if other.len > 0 {
            if let Some(other_buffer) = other.buffer {
                match self.buffer.as_mut() {
                    Some(buffer) => {
                        buffer.push(0);
                        buffer.extend_from_slice(&other_buffer[..]);
                        self.len += other.len;
                    }
                    None => {
                        self.buffer = Some(other_buffer);
                        self.len = other.len;
                    }
                }
            }
        }
        Ok(())
    }

    /// Complete the chain and return the token
    pub fn finish(self) -> i64 {
        match self.len {
            0 => rand::random(),
            1 => crate::cql::murmur3_cassandra_x64_128(&self.buffer.unwrap()[2..], 0).0,
            _ => crate::cql::murmur3_cassandra_x64_128(&self.buffer.unwrap(), 0).0,
        }
    }
}

#[derive(Error)]
pub enum TokenEncodeChainError<E1: TokenEncoder, E2: TokenEncoder> {
    #[error("Encode error: {0:?}")]
    EncodeError(E1::Error),
    #[error("Chained encode error: {0:?}")]
    EncodeChainError(E2::Error),
}

impl<E1: TokenEncoder, E2: TokenEncoder> Debug for TokenEncodeChainError<E1, E2> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::EncodeError(arg0) => f.debug_tuple("EncodeError").field(arg0).finish(),
            Self::EncodeChainError(arg0) => f.debug_tuple("EncodeChainError").field(arg0).finish(),
        }
    }
}

/// Encoding functionality for tokens
pub trait TokenEncoder {
    type Error: Debug;
    /// Start an encode chain
    fn chain<E: TokenEncoder>(&self, other: &E) -> Result<TokenEncodeChain, TokenEncodeChainError<Self, E>>
    where
        Self: Sized,
    {
        self.encode_token()
            .map_err(TokenEncodeChainError::EncodeError)?
            .chain(other)
            .map_err(TokenEncodeChainError::EncodeChainError)
    }

    /// Create a token encoding chain for this value
    fn encode_token(&self) -> Result<TokenEncodeChain, Self::Error>;
    /// Encode a single token
    fn token(&self) -> Result<i64, Self::Error> {
        Ok(self.encode_token()?.finish())
    }
}

impl TokenEncoder for TokenEncodeChain {
    type Error = Infallible;

    fn encode_token(&self) -> Result<TokenEncodeChain, Self::Error> {
        Ok(self.clone())
    }
}

impl<E: ColumnEncoder> TokenEncoder for E {
    type Error = E::Error;
    fn encode_token(&self) -> Result<TokenEncodeChain, Self::Error> {
        TokenEncodeChain::try_from(self)
    }
}

impl<E: TokenEncoder> TokenEncoder for [E] {
    type Error = E::Error;
    fn encode_token(&self) -> Result<TokenEncodeChain, Self::Error> {
        let mut token_chain = TokenEncodeChain::default();
        for v in self.iter() {
            token_chain.append(v)?;
        }
        Ok(token_chain)
    }
}

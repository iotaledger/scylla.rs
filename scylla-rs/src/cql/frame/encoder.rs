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
use core::fmt::Debug;
use std::{
    collections::HashMap,
    net::{
        IpAddr,
        Ipv4Addr,
        Ipv6Addr,
        SocketAddr,
        SocketAddrV4,
        SocketAddrV6,
    },
};

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
pub const NULL_VALUE: Null = Null;
/// The unset value used to indicate the body length.
pub const UNSET_VALUE: Unset = Unset;
/// The Null unit stucture.
pub struct Null;
/// The Unset unit stucture.
pub struct Unset;

/// The frame column encoder.
pub trait ColumnEncoder {
    /// Encoder the column buffer.
    fn encode(&self, buffer: &mut Vec<u8>);

    /// Encode this value to a new buffer
    fn encode_new(&self) -> Vec<u8> {
        let mut buf = Vec::new();
        self.encode(&mut buf);
        buf
    }

    /// Encode this value to a new buffer with a given capacity
    fn encode_with_capacity(&self, capacity: usize) -> Vec<u8> {
        let mut buf = Vec::with_capacity(capacity);
        self.encode(&mut buf);
        buf
    }
}

impl<E: ColumnEncoder + ?Sized> ColumnEncoder for &E {
    fn encode(&self, buffer: &mut Vec<u8>) {
        E::encode(*self, buffer)
    }
}

impl<E: ColumnEncoder + ?Sized> ColumnEncoder for Box<E> {
    fn encode(&self, buffer: &mut Vec<u8>) {
        E::encode(&*self, buffer)
    }
}

impl<E: ColumnEncoder> ColumnEncoder for Option<E> {
    fn encode(&self, buffer: &mut Vec<u8>) {
        match self {
            Some(value) => {
                value.encode(buffer);
            }
            None => {
                ColumnEncoder::encode(&UNSET_VALUE, buffer);
            }
        }
    }
}

impl<E: ColumnEncoder> ColumnEncoder for Vec<E> {
    fn encode(&self, buffer: &mut Vec<u8>) {
        for e in self {
            e.encode(buffer);
        }
    }
}

impl<K: ColumnEncoder, V: ColumnEncoder, S: ::std::hash::BuildHasher> ColumnEncoder for HashMap<K, V, S> {
    fn encode(&self, buffer: &mut Vec<u8>) {
        for (k, v) in self {
            k.encode(buffer);
            v.encode(buffer);
        }
    }
}

macro_rules! impl_tuple_encoder {
    ($(($v:ident, $e:tt)),*) => {
        impl<$($e: ColumnEncoder),*> ColumnEncoder for ($($e,)*) {
            fn encode(&self, buffer: &mut Vec<u8>) {
                let ($($v,)*) = self;
                $(
                    $v.encode(buffer);
                )*
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
            fn encode(&self, buffer: &mut Vec<u8>) {
                buffer.extend(&<$t>::to_be_bytes(*self));
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
    fn encode(&self, buffer: &mut Vec<u8>) {
        buffer.push(*self as u8);
    }
}

macro_rules! impl_str_encoder {
    ($t:ty) => {
        impl ColumnEncoder for $t {
            fn encode(&self, buffer: &mut Vec<u8>) {
                buffer.extend(self.bytes());
            }
        }
    };
}

impl_str_encoder!(String);
impl_str_encoder!(str);

impl ColumnEncoder for Blob {
    fn encode(&self, buffer: &mut Vec<u8>) {
        buffer.extend(self.as_slice());
    }
}

impl ColumnEncoder for IpAddr {
    fn encode(&self, buffer: &mut Vec<u8>) {
        match self {
            IpAddr::V4(addr) => {
                addr.encode(buffer);
            }
            IpAddr::V6(addr) => {
                addr.encode(buffer);
            }
        }
    }
}

impl ColumnEncoder for Ipv4Addr {
    fn encode(&self, buffer: &mut Vec<u8>) {
        buffer.extend(&self.octets());
    }
}

impl ColumnEncoder for Ipv6Addr {
    fn encode(&self, buffer: &mut Vec<u8>) {
        buffer.extend(&self.octets());
    }
}

impl ColumnEncoder for SocketAddr {
    fn encode(&self, buffer: &mut Vec<u8>) {
        match self {
            SocketAddr::V4(addr) => {
                addr.encode(buffer);
            }
            SocketAddr::V6(addr) => {
                addr.encode(buffer);
            }
        }
    }
}

impl ColumnEncoder for SocketAddrV4 {
    fn encode(&self, buffer: &mut Vec<u8>) {
        buffer.push(0);
        buffer.extend(&self.ip().octets());
        buffer.extend(&(self.port() as i32).to_be_bytes());
    }
}

impl ColumnEncoder for SocketAddrV6 {
    fn encode(&self, buffer: &mut Vec<u8>) {
        buffer.push(1);
        buffer.extend(&self.ip().octets());
        buffer.extend(&(self.port() as i32).to_be_bytes());
    }
}

impl ColumnEncoder for Unset {
    fn encode(&self, buffer: &mut Vec<u8>) {}
}

impl ColumnEncoder for Null {
    fn encode(&self, buffer: &mut Vec<u8>) {}
}

impl ColumnEncoder for NaiveDate {
    fn encode(&self, buffer: &mut Vec<u8>) {
        let days = self.num_days_from_ce() as u32 - 719_163 + (1u32 << 31);
        buffer.extend(&u32::to_be_bytes(days));
    }
}

impl ColumnEncoder for NaiveTime {
    fn encode(&self, buffer: &mut Vec<u8>) {
        let nanos = self.hour() as u64 * 3_600_000_000_000
            + self.minute() as u64 * 60_000_000_000
            + self.second() as u64 * 1_000_000_000
            + self.nanosecond() as u64;
        buffer.extend(&u64::to_be_bytes(nanos as u64));
    }
}

impl ColumnEncoder for NaiveDateTime {
    fn encode(&self, buffer: &mut Vec<u8>) {
        let cql_timestamp = self.timestamp_millis();
        buffer.extend(&u64::to_be_bytes(cql_timestamp as u64));
    }
}

/// An encode chain. Allows sequential encodes stored back-to-back in a buffer.
#[derive(Default, Debug, Clone)]
pub struct TokenEncodeChain {
    len: usize,
    buffer: Option<Vec<u8>>,
}

impl<T: ColumnEncoder + ?Sized> From<&T> for TokenEncodeChain {
    fn from(t: &T) -> Self {
        TokenEncodeChain {
            len: 1,
            buffer: Some(t.encode_new()[2..].into()),
        }
    }
}

impl TokenEncodeChain {
    fn try_from<E: ColumnEncoder + ?Sized>(e: &E) -> Self {
        TokenEncodeChain {
            len: 1,
            buffer: Some(e.encode_new()[2..].into()),
        }
    }
    /// Chain a new value
    pub fn chain<E: TokenEncoder + ?Sized>(mut self, other: &E) -> Self
    where
        Self: Sized,
    {
        self.append(other);
        self
    }

    /// Chain a new value
    pub fn append<E: TokenEncoder + ?Sized>(&mut self, other: &E) -> () {
        let other = other.encode_token();
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
/// Encoding functionality for tokens
pub trait TokenEncoder {
    /// Start an encode chain
    fn chain<E: TokenEncoder>(&self, other: &E) -> TokenEncodeChain
    where
        Self: Sized,
    {
        self.encode_token().chain(other)
    }

    /// Create a token encoding chain for this value
    fn encode_token(&self) -> TokenEncodeChain;
    /// Encode a single token
    fn token(&self) -> i64 {
        self.encode_token().finish()
    }
}

impl TokenEncoder for TokenEncodeChain {
    fn encode_token(&self) -> TokenEncodeChain {
        self.clone()
    }
}

impl<E: ColumnEncoder> TokenEncoder for E {
    fn encode_token(&self) -> TokenEncodeChain {
        TokenEncodeChain::from(self)
    }
}

impl<E: TokenEncoder> TokenEncoder for [E] {
    fn encode_token(&self) -> TokenEncodeChain {
        let mut token_chain = TokenEncodeChain::default();
        for v in self.iter() {
            token_chain.append(v);
        }
        token_chain
    }
}

// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

//! This module implements the version 4 scylla frame protocol.
//! See `https://github.com/apache/cassandra/blob/trunk/doc/native_protocol_v4.spec` for more details.

pub mod bind;
pub mod consistency;
pub mod decoder;
pub mod encoder;
pub mod header;
pub mod opcode;
pub mod requests;
pub mod responses;
pub mod rows;

pub use self::{
    requests::{
        auth_response::*,
        batch::*,
        batch_flags::*,
        execute::*,
        options::*,
        prepare::*,
        query::*,
        query_flags::*,
        register::*,
        startup::*,
        *,
    },
    responses::{
        auth_challenge::*,
        auth_success::*,
        authenticate::*,
        error::*,
        event::*,
        ready::*,
        result::*,
        supported::*,
        *,
    },
};
use crate::prelude::CompressionError;
pub use bind::*;
pub use consistency::Consistency;
use core::fmt::Debug;
pub use decoder::{
    ColumnDecoder,
    RowsDecoder,
};
use derive_more::{
    From,
    TryInto,
};
pub use encoder::{
    ColumnEncoder,
    TokenEncodeChain,
    TokenEncoder,
};
pub use header::Header;
pub use opcode::*;
pub use rows::*;
pub use std::convert::TryInto;
use std::{
    collections::HashMap,
    convert::TryFrom,
    net::{
        IpAddr,
        SocketAddr,
    },
    ops::{
        Deref,
        DerefMut,
    },
};
use thiserror::Error;

#[allow(missing_docs)]
#[derive(Error, Debug)]
pub enum FrameError {
    #[error("Invalid frame opcode. Expected {0:x}, got {1:x}")]
    WrongHeaderOpcode(u8, u8),
    #[error("Invalid frame header: {0}")]
    InvalidHeader(anyhow::Error),
    #[error("Invalid frame body: {0}")]
    InvalidBody(anyhow::Error),
    #[error("Invalid frame: {0}")]
    InvalidFrame(anyhow::Error),
    #[error("Payload is too small")]
    TooSmall,
    #[error(transparent)]
    CompressionError(#[from] CompressionError),
}

/// A wrapper for a `Vec<u8>` that can be used to encode and decode values as the `blob` scylla type.
#[derive(Debug, Clone)]
pub struct Blob(pub Vec<u8>);

#[allow(missing_docs)]
impl Blob {
    pub fn new(data: Vec<u8>) -> Self {
        Blob(data)
    }

    pub fn into_inner(self) -> Vec<u8> {
        self.0
    }
}

impl Deref for Blob {
    type Target = Vec<u8>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for Blob {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl From<Vec<u8>> for Blob {
    fn from(v: Vec<u8>) -> Self {
        Blob(v)
    }
}

/// Read a scylla `[string]` from a payload into an owned String.
///
/// `[string]`: A `[short]` n, followed by n bytes representing a UTF-8 string.
pub fn read_string(start: &mut usize, payload: &[u8]) -> anyhow::Result<String> {
    anyhow::ensure!(payload.len() >= *start + 2, "Not enough bytes for string length");
    let length = read_short(start, payload)? as usize;
    anyhow::ensure!(payload.len() >= *start + length, "Not enough bytes for string");
    let res = String::try_decode_column(&payload[*start..][..length])?;
    *start += length;
    Ok(res)
}

/// Read a scylla `[long string]` from a payload into an owned String.
///
/// `[long string]`: An `[int]` n, followed by n bytes representing a UTF-8 string.
pub fn read_long_string(start: &mut usize, payload: &[u8]) -> anyhow::Result<String> {
    anyhow::ensure!(payload.len() >= *start + 4, "Not enough bytes for string length");
    let length = read_int(start, payload)? as usize;
    anyhow::ensure!(payload.len() >= *start + length, "Not enough bytes for string");
    let res = String::try_decode_column(&payload[*start..][..length])?;
    *start += length;
    Ok(res)
}

/// Read a scylla `[string]` from a payload into a borrowed str.
///
/// `[string]`: A `[short]` n, followed by n bytes representing a UTF-8 string.
pub fn read_str<'a>(start: &mut usize, payload: &'a [u8]) -> anyhow::Result<&'a str> {
    anyhow::ensure!(payload.len() >= *start + 2, "Not enough bytes for string length");
    let length = read_short(start, payload)? as usize;
    anyhow::ensure!(payload.len() >= *start + length, "Not enough bytes for string");
    let res = std::str::from_utf8(&payload[*start..][..length])?;
    *start += length;
    Ok(res)
}

/// Read a scylla `[long string]` from a payload into a borrowed str.
///
/// `[long string]`: An `[int]` n, followed by n bytes representing a UTF-8 string.
pub fn read_long_str<'a>(start: &mut usize, payload: &'a [u8]) -> anyhow::Result<&'a str> {
    anyhow::ensure!(payload.len() >= *start + 4, "Not enough bytes for string length");
    let length = read_int(start, payload)? as usize;
    anyhow::ensure!(payload.len() >= *start + length, "Not enough bytes for string");
    let res = std::str::from_utf8(&payload[*start..][..length])?;
    *start += length;
    Ok(res)
}

/// Write a scylla `[string]` to a payload.
///
/// `[string]`: A `[short]` n, followed by n bytes representing a UTF-8 string.
pub fn write_string(s: &str, payload: &mut Vec<u8>) {
    payload.extend((s.len() as u16).to_be_bytes());
    payload.extend(s.as_bytes());
}

/// Write a scylla `[long string]` to a payload.
///
/// `[long string]`: An `[int]` n, followed by n bytes representing a UTF-8 string.
pub fn write_long_string(s: &str, payload: &mut Vec<u8>) {
    payload.extend((s.len() as i32).to_be_bytes());
    payload.extend(s.as_bytes());
}

/// Read a scylla `[short]` from a payload into a u16.
///
/// `[short]`: A 2 bytes unsigned integer
pub fn read_short(start: &mut usize, payload: &[u8]) -> anyhow::Result<u16> {
    anyhow::ensure!(payload.len() >= *start + 2, "Not enough bytes for short");
    let res = u16::from_be_bytes(payload[*start..][..2].try_into()?);
    *start += 2;
    Ok(res)
}

/// Write a scylla `[short]` to a payload.
///
/// `[short]`: A 2 bytes unsigned integer
pub fn write_short(v: u16, payload: &mut Vec<u8>) {
    payload.extend(v.to_be_bytes());
}

/// Read a scylla `[int]` from a payload into an i32.
///
/// `[int]`: A 4 bytes integer
pub fn read_int(start: &mut usize, payload: &[u8]) -> anyhow::Result<i32> {
    anyhow::ensure!(payload.len() >= *start + 4, "Not enough bytes for int");
    let res = i32::from_be_bytes(payload[*start..][..4].try_into()?);
    *start += 4;
    Ok(res)
}

/// Write a scylla `[int]` to a payload.
///
/// `[int]`: A 4 bytes integer
pub fn write_int(v: i32, payload: &mut Vec<u8>) {
    payload.extend(v.to_be_bytes());
}

/// Read a scylla `[long]` from a payload into an i64.
///
/// `[long]`: An 8 bytes integer
pub fn read_long(start: &mut usize, payload: &[u8]) -> anyhow::Result<i64> {
    anyhow::ensure!(payload.len() >= *start + 8, "Not enough bytes for long");
    let res = i64::from_be_bytes(payload[*start..][..8].try_into()?);
    *start += 8;
    Ok(res)
}

/// Write a scylla `[long]` to a payload.
///
/// `[long]`: An 8 bytes integer
pub fn write_long(v: i64, payload: &mut Vec<u8>) {
    payload.extend(v.to_be_bytes());
}

/// Read a scylla `[bytes]` from a payload into a borrowed slice.
///
/// `[bytes]`: An `[int]` n, followed by n bytes if `n >= 0`. If `n < 0`, no byte should follow and the value
/// represented is `null`.
pub fn read_bytes<'a>(start: &mut usize, payload: &'a [u8]) -> anyhow::Result<&'a [u8]> {
    anyhow::ensure!(payload.len() >= *start + 4, "Not enough bytes for length");
    let length = read_int(start, payload)?;
    if length > 0 {
        anyhow::ensure!(payload.len() >= *start + length as usize, "Not enough bytes");
        let res = &payload[*start..][..length as usize];
        *start += length as usize;
        Ok(res)
    } else {
        Ok(&[])
    }
}

/// Write a scylla `[bytes]` to a payload.
///
/// `[bytes]`: An `[int]` n, followed by n bytes if `n >= 0`. If `n < 0`, no byte should follow and the value
/// represented is `null`.
pub fn write_bytes(b: &[u8], payload: &mut Vec<u8>) {
    payload.extend((b.len() as i32).to_be_bytes());
    payload.extend(b);
}

/// Read a scylla `[short bytes]` from a payload into a borrowed slice.
///
/// `[short bytes]`: A `[short]` n, followed by n bytes if `n >= 0`.
pub fn read_short_bytes<'a>(start: &mut usize, payload: &'a [u8]) -> anyhow::Result<&'a [u8]> {
    anyhow::ensure!(payload.len() >= *start + 2, "Not enough bytes");
    let length = read_short(start, payload)?;
    if length > 0 {
        anyhow::ensure!(payload.len() >= *start + length as usize, "Not enough bytes");
        let res = &payload[*start..][..length as usize];
        *start += length as usize;
        Ok(res)
    } else {
        Ok(&[])
    }
}

/// Write a scylla `[short bytes]` to a payload.
///
/// `[short bytes]`: A `[short]` n, followed by n bytes if `n >= 0`.
pub fn write_short_bytes(b: &[u8], payload: &mut Vec<u8>) {
    payload.extend((b.len() as u16).to_be_bytes());
    payload.extend(b);
}

/// Read scylla values from a payload.
///
/// `[value]`: An `[int]` n, followed by n bytes if `n >= 0`.
///     - If `n == -1` no byte should follow and the value represented is `null`.
///     - If `n == -2` no byte should follow and the value represented is
///     `not set` not resulting in any change to the existing value.
///     - `n < -2` is an invalid value and results in an error.
pub fn read_values(start: &mut usize, payload: &[u8]) -> anyhow::Result<Values> {
    let values_count = read_short(start, payload)? as usize;
    let mut values = Values::default();
    for _ in 0..values_count {
        values.push(None, read_bytes(start, payload)?);
    }
    Ok(values)
}

/// Read scylla named values from a payload.
///
/// `[value]`: An `[int]` n, followed by n bytes if `n >= 0`.
///     - If `n == -1` no byte should follow and the value represented is `null`.
///     - If `n == -2` no byte should follow and the value represented is
///     `not set` not resulting in any change to the existing value.
///     - `n < -2` is an invalid value and results in an error.
pub fn read_named_values(start: &mut usize, payload: &[u8]) -> anyhow::Result<Values> {
    let values_count = read_short(start, payload)? as usize;
    let mut values = Values::default();
    for _ in 0..values_count {
        values.push(Some(read_str(start, payload)?), read_bytes(start, payload)?);
    }
    Ok(values)
}

/// Read a prepared id from a payload into a `[u8; 16]`.
pub fn read_prepared_id(start: &mut usize, payload: &[u8]) -> anyhow::Result<[u8; 16]> {
    anyhow::ensure!(payload.len() >= *start + 18, "Not enough bytes for prepared id");
    Ok(read_short_bytes(start, payload)?.try_into()?)
}

/// Write a prepared id to a payload.
pub fn write_prepared_id(id: [u8; 16], payload: &mut Vec<u8>) {
    payload.extend((id.len() as u16).to_be_bytes());
    payload.extend(id);
}

/// Read a scylla `[string list]` from a payload into a `Vec<String>`.
///
/// `[string list]`: A `[short]` n, followed by n `[string]`.
pub fn read_string_list(start: &mut usize, payload: &[u8]) -> anyhow::Result<Vec<String>> {
    let list_len = read_short(start, payload)? as usize;
    let mut list = Vec::with_capacity(list_len);
    for _ in 0..list_len {
        list.push(read_string(start, payload)?);
    }
    Ok(list)
}

/// Write a scylla `[string list]` to a payload.
///
/// `[string list]`: A `[short]` n, followed by n `[string]`.
pub fn write_string_list(l: &[String], payload: &mut Vec<u8>) {
    payload.extend((l.len() as u16).to_be_bytes());
    for s in l {
        write_string(s, payload);
    }
}

/// Read a list of any type that can be read from a payload into a `Vec<T>`.
/// Uses `[short]` for the length of the list.
pub fn read_list<T: FromPayload>(start: &mut usize, payload: &[u8]) -> anyhow::Result<Vec<T>> {
    let list_len = read_short(start, payload)? as usize;
    let mut list = Vec::with_capacity(list_len);
    for _ in 0..list_len {
        list.push(T::from_payload(start, payload)?);
    }
    Ok(list)
}

/// Write a list of any type that can be written to a payload.
/// Uses `[short]` for the length of the list.
pub fn write_list<T: ToPayload>(l: Vec<T>, payload: &mut Vec<u8>) {
    payload.extend((l.len() as u16).to_be_bytes());
    for v in l {
        T::to_payload(v, payload);
    }
}

/// Read a scylla `[string map]` from a payload into a `HashMap<String, String>`.
///
/// `[string map]`: A `[short]` n, followed by n pair `<k><v>` where `<k>` and `<v>` are `[string]`.
pub fn read_string_map(start: &mut usize, payload: &[u8]) -> anyhow::Result<HashMap<String, String>> {
    let length = read_short(start, payload)? as usize;
    let mut multimap = HashMap::with_capacity(length);
    for _ in 0..length {
        multimap.insert(read_string(start, payload)?, read_string(start, payload)?);
    }
    Ok(multimap)
}

/// Write a scylla `[string map]` to a payload.
///
/// `[string map]`: A `[short]` n, followed by n pair `<k><v>` where `<k>` and `<v>` are `[string]`.
pub fn write_string_map(m: &HashMap<String, String>, payload: &mut Vec<u8>) {
    payload.extend((m.len() as u16).to_be_bytes());
    for (k, v) in m {
        write_string(k, payload);
        write_string(v, payload);
    }
}

/// Read a scylla `[string multimap]` from a payload into a `HashMap<String, Vec<String>>`.
///
/// `[string multimap]`: A `[short]` n, followed by n pair `<k><v>` where `<k>` is a `[string]` and `<v>` is a `[string
/// list]`.
pub fn read_string_multimap(start: &mut usize, payload: &[u8]) -> anyhow::Result<HashMap<String, Vec<String>>> {
    let length = read_short(start, payload)? as usize;
    let mut multimap = HashMap::with_capacity(length);
    for _ in 0..length {
        multimap.insert(read_string(start, payload)?, read_string_list(start, payload)?);
    }
    Ok(multimap)
}

/// Write a scylla `[string multimap]` to a payload.
///
/// `[string multimap]`: A `[short]` n, followed by n pair `<k><v>` where `<k>` is a `[string]` and `<v>` is a `[string
/// list]`.
pub fn write_string_multimap(m: &HashMap<String, Vec<String>>, payload: &mut Vec<u8>) {
    payload.extend((m.len() as u16).to_be_bytes());
    for (k, v) in m {
        write_string(k, payload);
        write_string_list(v, payload);
    }
}

/// Read a scylla `[inet]` from a payload into a `SocketAddr`.
///
/// `[inet]`: An address (ip and port) to a node. It consists of one
///     `[byte]` n, that represents the address size, followed by n
///     `[byte]` representing the IP address (in practice n can only be
///     either 4 (IPv4) or 16 (IPv6)), following by one `[int]`
///     representing the port.
pub fn read_inet(start: &mut usize, payload: &[u8]) -> anyhow::Result<SocketAddr> {
    anyhow::ensure!(payload.len() > *start, "Not enough bytes for inet");
    let address_len = payload[0] as usize;
    *start += address_len + 5;
    Ok(if address_len == 4 {
        anyhow::ensure!(payload.len() >= *start + 8, "Not enough bytes for ipv4 inet");
        SocketAddr::new(
            IpAddr::V4(u32::from_be_bytes(payload[1..][..4].try_into()?).into()),
            i32::from_be_bytes(payload[5..][..4].try_into()?) as u16,
        )
    } else {
        anyhow::ensure!(payload.len() >= *start + 24, "Not enough bytes for ipv6 inet");
        SocketAddr::new(
            IpAddr::V6(u128::from_be_bytes(payload[1..][..16].try_into()?).into()),
            i32::from_be_bytes(payload[16..][..4].try_into()?) as u16,
        )
    })
}

/// Write a scylla `[inet]` to a payload.
///
/// `[inet]`: An address (ip and port) to a node. It consists of one
///     `[byte]` n, that represents the address size, followed by n
///     `[byte]` representing the IP address (in practice n can only be
///     either 4 (IPv4) or 16 (IPv6)), following by one `[int]`
///     representing the port.
pub fn write_inet(a: SocketAddr, payload: &mut Vec<u8>) {
    match a {
        SocketAddr::V4(addr) => {
            payload.push(4u8);
            payload.extend(addr.ip().octets());
            payload.extend((addr.port() as i32).to_be_bytes());
        }
        SocketAddr::V6(addr) => {
            payload.push(16u8);
            payload.extend(addr.ip().octets());
            payload.extend((addr.port() as i32).to_be_bytes());
        }
    }
}

/// Read a scylla `[byte`] from a payload into a `u8`.
///
/// `[byte]`: A 1 byte unsigned integer
pub fn read_byte(start: &mut usize, payload: &[u8]) -> anyhow::Result<u8> {
    anyhow::ensure!(payload.len() > *start, "Not enough bytes");
    let res = payload[*start];
    *start += 1;
    Ok(res)
}

/// Write a scylla `[byte]` to a payload.
///
/// `[byte]`: A 1 byte unsigned integer
pub fn write_byte(b: u8, payload: &mut Vec<u8>) {
    payload.push(b);
}

/// Defines a type that can be read from a frame payload.
pub trait FromPayload: Sized {
    /// Read this value from a frame payload. This method should read the payload beginning with the given `start`
    /// index, and update it by adding the number of bytes read.
    fn from_payload(start: &mut usize, payload: &[u8]) -> anyhow::Result<Self>;
}

impl FromPayload for String {
    fn from_payload(start: &mut usize, payload: &[u8]) -> anyhow::Result<Self> {
        read_string(start, payload)
    }
}

impl FromPayload for SocketAddr {
    fn from_payload(start: &mut usize, payload: &[u8]) -> anyhow::Result<Self> {
        read_inet(start, payload)
    }
}

/// Defines a type that can be written to a frame payload.
pub trait ToPayload {
    /// Write this value to a frame payload.
    fn to_payload(self, payload: &mut Vec<u8>);
}

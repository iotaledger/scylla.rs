// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

//! This module implements the frame encoder.

use std::{
    collections::HashMap,
    net::{IpAddr, Ipv4Addr, Ipv6Addr},
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
    /// Encoder the column buffer.
    fn encode(&self, buffer: &mut Vec<u8>);
}

impl<T> ColumnEncoder for Option<T>
where
    T: ColumnEncoder,
{
    fn encode(&self, buffer: &mut Vec<u8>) {
        match self {
            Some(value) => value.encode(buffer),
            None => UNSET_VALUE.encode(buffer),
        }
    }
}

impl ColumnEncoder for i64 {
    fn encode(&self, buffer: &mut Vec<u8>) {
        buffer.extend(&BE_8_BYTES_LEN);
        buffer.extend(&i64::to_be_bytes(*self));
    }
}

impl ColumnEncoder for u64 {
    fn encode(&self, buffer: &mut Vec<u8>) {
        buffer.extend(&BE_8_BYTES_LEN);
        buffer.extend(&u64::to_be_bytes(*self));
    }
}

impl ColumnEncoder for f64 {
    fn encode(&self, buffer: &mut Vec<u8>) {
        buffer.extend(&BE_8_BYTES_LEN);
        buffer.extend(&f64::to_be_bytes(*self));
    }
}

impl ColumnEncoder for i32 {
    fn encode(&self, buffer: &mut Vec<u8>) {
        buffer.extend(&BE_4_BYTES_LEN);
        buffer.extend(&i32::to_be_bytes(*self));
    }
}

impl ColumnEncoder for u32 {
    fn encode(&self, buffer: &mut Vec<u8>) {
        buffer.extend(&BE_4_BYTES_LEN);
        buffer.extend(&u32::to_be_bytes(*self));
    }
}

impl ColumnEncoder for f32 {
    fn encode(&self, buffer: &mut Vec<u8>) {
        buffer.extend(&BE_4_BYTES_LEN);
        buffer.extend(&f32::to_be_bytes(*self));
    }
}

impl ColumnEncoder for i16 {
    fn encode(&self, buffer: &mut Vec<u8>) {
        buffer.extend(&BE_2_BYTES_LEN);
        buffer.extend(&i16::to_be_bytes(*self));
    }
}

impl ColumnEncoder for u16 {
    fn encode(&self, buffer: &mut Vec<u8>) {
        buffer.extend(&BE_2_BYTES_LEN);
        buffer.extend(&u16::to_be_bytes(*self));
    }
}

impl ColumnEncoder for i8 {
    fn encode(&self, buffer: &mut Vec<u8>) {
        buffer.extend(&BE_1_BYTES_LEN);
        buffer.extend(&i8::to_be_bytes(*self));
    }
}

impl ColumnEncoder for u8 {
    fn encode(&self, buffer: &mut Vec<u8>) {
        buffer.extend(&BE_1_BYTES_LEN);
        buffer.push(*self);
    }
}

impl ColumnEncoder for bool {
    fn encode(&self, buffer: &mut Vec<u8>) {
        buffer.extend(&BE_1_BYTES_LEN);
        buffer.push(*self as u8);
    }
}

impl ColumnEncoder for String {
    fn encode(&self, buffer: &mut Vec<u8>) {
        buffer.extend(&i32::to_be_bytes(self.len() as i32));
        buffer.extend(self.bytes());
    }
}
impl ColumnEncoder for &str {
    fn encode(&self, buffer: &mut Vec<u8>) {
        buffer.extend(&i32::to_be_bytes(self.len() as i32));
        buffer.extend(self.bytes());
    }
}
impl ColumnEncoder for &[u8] {
    fn encode(&self, buffer: &mut Vec<u8>) {
        buffer.extend(&i32::to_be_bytes(self.len() as i32));
        buffer.extend(*self);
    }
}

impl ColumnEncoder for IpAddr {
    fn encode(&self, buffer: &mut Vec<u8>) {
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
    }
}

impl ColumnEncoder for Ipv4Addr {
    fn encode(&self, buffer: &mut Vec<u8>) {
        buffer.extend(&BE_4_BYTES_LEN);
        buffer.extend(&self.octets());
    }
}

impl ColumnEncoder for Ipv6Addr {
    fn encode(&self, buffer: &mut Vec<u8>) {
        buffer.extend(&BE_16_BYTES_LEN);
        buffer.extend(&self.octets());
    }
}

impl<E> ColumnEncoder for Vec<E>
where
    E: ColumnEncoder,
{
    fn encode(&self, buffer: &mut Vec<u8>) {
        // total byte_size of the list is unknown,
        // therefore we pad zero length for now.
        buffer.extend(&BE_0_BYTES_LEN);
        // in order to compute the byte_size we snapshot
        // the current buffer length in advance
        let current_length = buffer.len();
        buffer.extend(&i32::to_be_bytes(self.len() as i32));
        for e in self {
            e.encode(buffer);
        }
        let list_byte_size = buffer.len() - current_length;
        buffer[(current_length - 4)..current_length].copy_from_slice(&i32::to_be_bytes(list_byte_size as i32));
    }
}

impl<K, V, S: ::std::hash::BuildHasher> ColumnEncoder for HashMap<K, V, S>
where
    K: ColumnEncoder,
    V: ColumnEncoder,
{
    fn encode(&self, buffer: &mut Vec<u8>) {
        buffer.extend(&BE_0_BYTES_LEN);
        let current_length = buffer.len();
        buffer.extend(&i32::to_be_bytes(self.len() as i32));
        for (k, v) in self {
            k.encode(buffer);
            v.encode(buffer);
        }
        let map_byte_size = buffer.len() - current_length;
        buffer[(current_length - 4)..current_length].copy_from_slice(&i32::to_be_bytes(map_byte_size as i32));
    }
}

impl ColumnEncoder for Unset {
    fn encode(&self, buffer: &mut Vec<u8>) {
        buffer.extend(&BE_UNSET_BYTES_LEN);
    }
}

impl ColumnEncoder for Null {
    fn encode(&self, buffer: &mut Vec<u8>) {
        buffer.extend(&BE_NULL_BYTES_LEN);
    }
}

// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use super::ComputeToken;
use crate::cql::{
    murmur3_cassandra_x64_128,
    Decoder,
    RowsDecoder,
    VoidDecoder,
};
/// Represents a Scylla Keyspace which holds a set of tables and
/// queries on those tables.
/// A keyspace can have predefined queries and functionality to
/// decode the results they return. To make use of this, implement
/// the following traits on a `Keyspace`:
///
/// - `RowsDecoder`
/// - `VoidDecoder`
/// - `Select`
/// - `Update`
/// - `Insert`
/// - `Delete`
pub trait Keyspace: Send + Sized + Sync + Clone {
    /// Get the name of the keyspace as represented in the database
    fn name(&self) -> String;

    /// Decode void result
    fn decode_void(decoder: Decoder) -> anyhow::Result<()> {
        VoidDecoder::try_decode_void(decoder)
    }
    /// Decode rows result
    fn decode_rows<V>(decoder: Decoder) -> anyhow::Result<Option<V>>
    where
        V: RowsDecoder,
    {
        V::try_decode_rows(decoder)
    }
    // TODO replication_refactor, strategy, options,etc.
}

impl<T> Keyspace for T
where
    T: ToString + Clone + Send + Sync,
{
    fn name(&self) -> String {
        self.to_string()
    }
}

impl<T> ComputeToken for T
where
    T: AsBytes,
{
    fn token(&self) -> i64 {
        murmur3_cassandra_x64_128(&self.as_bytes(), 0).0
    }
}

pub trait AsBytes {
    fn as_bytes(&self) -> Vec<u8>;
}

impl AsBytes for u8 {
    fn as_bytes(&self) -> Vec<u8> {
        self.to_be_bytes().to_vec()
    }
}

impl AsBytes for u16 {
    fn as_bytes(&self) -> Vec<u8> {
        self.to_be_bytes().to_vec()
    }
}

impl AsBytes for u32 {
    fn as_bytes(&self) -> Vec<u8> {
        self.to_be_bytes().to_vec()
    }
}

impl AsBytes for u64 {
    fn as_bytes(&self) -> Vec<u8> {
        self.to_be_bytes().to_vec()
    }
}

impl AsBytes for u128 {
    fn as_bytes(&self) -> Vec<u8> {
        self.to_be_bytes().to_vec()
    }
}

impl AsBytes for usize {
    fn as_bytes(&self) -> Vec<u8> {
        self.to_be_bytes().to_vec()
    }
}

impl AsBytes for i8 {
    fn as_bytes(&self) -> Vec<u8> {
        self.to_be_bytes().to_vec()
    }
}

impl AsBytes for i16 {
    fn as_bytes(&self) -> Vec<u8> {
        self.to_be_bytes().to_vec()
    }
}

impl AsBytes for i32 {
    fn as_bytes(&self) -> Vec<u8> {
        self.to_be_bytes().to_vec()
    }
}

impl AsBytes for i64 {
    fn as_bytes(&self) -> Vec<u8> {
        self.to_be_bytes().to_vec()
    }
}

impl AsBytes for i128 {
    fn as_bytes(&self) -> Vec<u8> {
        self.to_be_bytes().to_vec()
    }
}

impl AsBytes for isize {
    fn as_bytes(&self) -> Vec<u8> {
        self.to_be_bytes().to_vec()
    }
}

impl AsBytes for f32 {
    fn as_bytes(&self) -> Vec<u8> {
        self.to_be_bytes().to_vec()
    }
}

impl AsBytes for f64 {
    fn as_bytes(&self) -> Vec<u8> {
        self.to_be_bytes().to_vec()
    }
}

impl AsBytes for String {
    fn as_bytes(&self) -> Vec<u8> {
        self.as_bytes().to_vec()
    }
}

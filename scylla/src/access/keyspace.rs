// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use std::borrow::Cow;

/// Represents a Scylla Keyspace which holds a set of tables and
/// queries on those tables.
///
/// ## Usage
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
pub trait Keyspace: Send + Sized + Sync {
    /// Get the name of the keyspace as represented in the database
    fn name(&self) -> &Cow<'static, str>;
    /// Decode void result
    fn decode_void(decoder: scylla_cql::Decoder) -> Result<(), scylla_cql::CqlError>
    where
        Self: scylla_cql::VoidDecoder,
    {
        Self::try_decode(decoder)
    }
    /// Decode rows result
    fn decode_rows<K, V>(decoder: scylla_cql::Decoder) -> Result<Option<V>, scylla_cql::CqlError>
    where
        Self: scylla_cql::RowsDecoder<K, V>,
    {
        Self::try_decode(decoder)
    }
    // TODO replication_refactor, strategy, options,etc.
}

pub trait StatementsStore {
    fn init();
}

pub trait GetStatementsStore {}

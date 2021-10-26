// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use crate::cql::{
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
pub trait Keyspace: Send + Sized + Sync + Clone + std::fmt::Debug {
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

    /// Helper function to replace the {{keyspace}} token in dynamic statements
    fn replace_keyspace_token(&self, statement: &str) -> String {
        statement.replace("{{keyspace}}", &self.name())
    }
}

impl<T> Keyspace for T
where
    T: ToString + Clone + Send + Sync + std::fmt::Debug,
{
    fn name(&self) -> String {
        self.to_string()
    }
}

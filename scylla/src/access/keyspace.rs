// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use scylla_cql::{Frame, RowsDecoder};
/// Keyspace trait, almost marker
pub trait Keyspace: Send + Sized + Sync {
    /// name of the keyspace
    const NAME: &'static str;
    /// Decode void result
    fn decode_void(decoder: scylla_cql::Decoder) -> Result<(), scylla_cql::CqlError> {
        decoder.get_void()
    }
    /// Decode rows result
    fn decode_rows<K, V>(decoder: scylla_cql::Decoder) -> Result<Option<V>, scylla_cql::CqlError>
    where
        Self: RowsDecoder<K, V>,
    {
        Self::try_decode(decoder)
    }
    /// Get the name of the keyspace as represented in the database
    fn name() -> &'static str {
        Self::NAME
    }
    // TODO replication_refactor, strategy, options,etc.
}

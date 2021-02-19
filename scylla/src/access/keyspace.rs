// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use crate::worker::Worker;
use crate::ring::Ring;
use crate::stage::ReporterEvent;
use scylla_cql::Frame;

/// Keyspace trait, almost marker
pub trait Keyspace: Send + Sized + Sync {
    type Error;
    const NAME: &'static str;
    fn name() -> &'static str {
        Self::NAME
    }
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
    /// Send query to a random replica in the local datacenter;
    fn send_local(token: i64, payload: Vec<u8>, worker: Box<dyn Worker>);
    /// Send query to a random replica in any global datacenter;
    fn send_global(token: i64, payload: Vec<u8>, worker: Box<dyn Worker>);
    // TODO replication_refactor, strategy, options,etc.
}

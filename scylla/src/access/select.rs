// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use super::*;
use scylla_cql::SelectDecoder;

#[async_trait::async_trait]
/// `Select<K, V>` trait extends the `keyspace` with `select` operation for the (key: K, value: V);
/// therefore, it should be explicitly implemented for the corresponding `Keyspace` with the correct SELECT CQL query.
pub trait Select<K, V>: Keyspace {
    /// Selects the value associated with the key from the storage keyspace,
    /// and responde back using async callback to the worker.
    async fn select<T>(&self, worker: Box<T>, key: &K)
    where
        T: SelectDecoder<K, V> + Worker;
}

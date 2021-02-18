// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use super::*;
use scylla_cql::VoidDecoder;

#[async_trait::async_trait]
/// `Update<K, V>` trait extends the `keyspace` with `Update` operation for the (key: K, value: V);
/// therefore, it should be explicitly implemented for the corresponding `Keyspace` with the correct UPDATE CQL query.
pub trait Update<K, V>: Keyspace + VoidDecoder {
    /// Update K, V pair in the keyspace
    async fn update<T: Worker>(&self, worker: Box<T>, key: &K, value: &V);
}

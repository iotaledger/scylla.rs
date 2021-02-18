// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use super::*;
use scylla_cql::VoidDecoder;

#[async_trait::async_trait]
/// `Delete<K, V>` trait extends the `keyspace` with `delete` operation for the (key: K, value: V);
/// therefore, it should be explicitly implemented for the corresponding `Keyspace` with the correct DELETE CQL query.
pub trait Delete<K, V>: Keyspace + VoidDecoder {
    /// Delete K, V record from the keyspace
    async fn delete<T: Worker>(&self, worker: T, key: &K);
}

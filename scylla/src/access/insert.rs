// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use super::*;

#[async_trait::async_trait]
/// `Insert<K, V>` trait extends the `keyspace` with `insert` operation for the (key: K, value: V);
/// therefore, it should be explicitly implemented for the corresponding `Keyspace` with the correct INSERT CQL query.
pub trait Insert<K, V>: Keyspace {
    /// Insert K, V into the Keyspace in our Cluster
    async fn insert<T: Worker>(&self, worker: Box<T>, key: &K, value: &V);
}

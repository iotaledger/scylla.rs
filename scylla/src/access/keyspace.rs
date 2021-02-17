// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use super::{delete::DeleteQuery, insert::InsertQuery, select::SelectQuery, update::UpdateQuery, Worker};

/// Keyspace trait, almost marker
#[async_trait::async_trait]
pub trait Keyspace: Send + Sized + Sync {
    type Error;
    const NAME: &'static str;
    fn name() -> &'static str {
        Self::NAME
    }

    async fn send_select<W: Worker, K, V>(&mut self, worker: W, query: SelectQuery<K, V>);
    async fn send_update<W: Worker, K, V>(&mut self, worker: W, query: UpdateQuery<K, V>);
    async fn send_delete<W: Worker, K>(&mut self, worker: W, query: DeleteQuery<K>);
    async fn send_insert<W: Worker, V>(&mut self, worker: W, query: InsertQuery<V>);

    // TODO replication_refactor, strategy, options,etc.
}

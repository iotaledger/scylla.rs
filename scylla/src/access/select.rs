// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use super::*;

pub struct SelectQuery<K, V> {
    inner: Query,
    key: PhantomData<K>,
    val: PhantomData<V>,
}

impl<K, V> Deref for SelectQuery<K, V> {
    type Target = Query;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl<K, V> DerefMut for SelectQuery<K, V> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.inner
    }
}

impl<K, V> SelectQuery<K, V> {
    pub fn into_bytes(self) -> Vec<u8> {
        self.inner.0
    }

    pub fn into_inner(self) -> Query {
        self.inner
    }
}

pub trait Select<K, V>: Keyspace {
    fn select(&self, key: &K) -> SelectQuery<K, V>;
}

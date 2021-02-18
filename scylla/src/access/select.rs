// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use super::*;

#[derive(Clone)]
pub struct SelectQuery<S, K, V> {
    inner: Query,
    keyspace: PhantomData<S>,
    key: PhantomData<K>,
    val: PhantomData<V>,
}

impl<S, K, V> Default for SelectQuery<S, K, V> {
    fn default() -> Self {
        Self {
            inner: Query::default(),
            keyspace: PhantomData::default(),
            key: PhantomData::default(),
            val: PhantomData::default(),
        }
    }
}

impl<S, K, V> Deref for SelectQuery<S, K, V> {
    type Target = Query;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl<S, K, V> DerefMut for SelectQuery<S, K, V> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.inner
    }
}

impl<S: Select<K, V>, K, V> SelectQuery<S, K, V> {
    pub fn new(query: Query) -> Self {
        Self {
            inner: query,
            ..Default::default()
        }
    }

    pub fn into_bytes(&self) -> Vec<u8> {
        self.inner.0.clone()
    }

    pub fn take(&mut self) -> Vec<u8> {
        std::mem::take(&mut self.inner).0
    }

    pub fn decode(&self, bytes: Vec<u8>) -> Result<Option<V>, CqlError> {
        S::decode(bytes.into())
    }
}

pub trait Select<K, V>: Keyspace {
    fn select(&self, key: &K) -> SelectQuery<Self, K, V>;

    fn decode(decoder: Decoder) -> Result<Option<V>, CqlError>;
}

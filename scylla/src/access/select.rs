// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use scylla_cql::{CqlError, Decoder};

use super::*;

pub struct SelectQuery<S, K, V> {
    inner: Query,
    keyspace: PhantomData<S>,
    key: PhantomData<K>,
    val: PhantomData<V>,
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
    pub fn into_bytes(&self) -> Vec<u8> {
        self.inner.0.clone()
    }

    pub fn take(&mut self) -> Query {
        std::mem::take(&mut self.inner)
    }

    pub fn try_decode(bytes: Vec<u8>) -> Result<Option<V>, CqlError> {
        S::try_decode(bytes.into())
    }

    fn decode(bytes: Vec<u8>) -> Option<V> {
        Self::try_decode(bytes).unwrap()
    }
}

pub trait Select<K, V>: Keyspace {
    fn select(&self, key: &K) -> SelectQuery<Self, K, V>;

    fn try_decode(decoder: Decoder) -> Result<Option<V>, CqlError>;

    fn decode(decoder: Decoder) -> Option<V> {
        Self::try_decode(decoder).unwrap()
    }
}

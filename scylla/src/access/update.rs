// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use super::*;

#[derive(Clone)]
pub struct UpdateQuery<S, K, V> {
    inner: Query,
    keyspace: PhantomData<S>,
    key: PhantomData<K>,
    val: PhantomData<V>,
}

impl<S, K, V> Default for UpdateQuery<S, K, V> {
    fn default() -> Self {
        Self {
            inner: Query::default(),
            keyspace: PhantomData::default(),
            key: PhantomData::default(),
            val: PhantomData::default(),
        }
    }
}

impl<S, K, V> Deref for UpdateQuery<S, K, V> {
    type Target = Query;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl<S, K, V> DerefMut for UpdateQuery<S, K, V> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.inner
    }
}

impl<S: Update<K, V>, K, V> UpdateQuery<S, K, V> {
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

    pub fn decode(&self, bytes: Vec<u8>) -> Result<(), CqlError> {
        S::decode(bytes.into())
    }
}

pub trait Update<K, V>: Keyspace {
    fn update(&self, key: &K, value: &V) -> UpdateQuery<Self, K, V>;

    fn decode(decoder: Decoder) -> Result<(), CqlError> {
        if decoder.is_error() {
            Err(decoder.body().into())
        } else {
            Ok(())
        }
    }
}

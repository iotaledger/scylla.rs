// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use super::*;

pub struct InsertQuery<S, V> {
    inner: Query,
    keyspace: PhantomData<S>,
    val: PhantomData<V>,
}

impl<S, V> Default for InsertQuery<S, V> {
    fn default() -> Self {
        Self {
            inner: Query::default(),
            keyspace: PhantomData::default(),
            val: PhantomData::default(),
        }
    }
}

impl<S, V> Deref for InsertQuery<S, V> {
    type Target = Query;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl<S, V> DerefMut for InsertQuery<S, V> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.inner
    }
}

impl<S: Insert<V>, V> InsertQuery<S, V> {
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

pub trait Insert<V>: Keyspace {
    fn insert(&self, value: &V) -> InsertQuery<Self, V>;

    fn decode(decoder: Decoder) -> Result<(), CqlError> {
        if decoder.is_error() {
            Err(decoder.body().into())
        } else {
            Ok(())
        }
    }
}

// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use super::*;

pub struct SelectValRequest<'a, S, K, V> {
    keyspace: &'a S,
    _marker: PhantomData<(K, V)>,
}

pub trait GetSelectValRequest<S, K> {
    fn to_get<V>(&self) -> SelectValRequest<S, K, V>;
}

impl<S: Keyspace, K> GetSelectValRequest<S, K> for S {
    fn to_get<V>(&self) -> SelectValRequest<S, K, V> {
        SelectValRequest {
            keyspace: self,
            _marker: PhantomData,
        }
    }
}

pub struct SelectRequest<'a, S, K, V> {
    token: i64,
    inner: Query,
    keyspace: &'a S,
    _marker: PhantomData<(K, V)>,
}

impl<'a, S: Select<'a, K, V>, K, V> SelectValRequest<'a, S, K, V> {
    pub fn select(&self, key: &'a K) -> SelectRequest<S, K, V> {
        S::select(self.keyspace, key)
    }
}

impl<'a, S: Select<'a, K, V>, K, V> SelectRequest<'a, S, K, V> {
    pub fn new(query: Query, token: i64, keyspace: &'a S) -> Self {
        Self {
            token,
            inner: query,
            keyspace,
            _marker: PhantomData,
        }
    }

    pub fn send_local(self, worker: Box<dyn Worker>) -> DecodeResult<DecodeRows<S, K, V>> {
        S::send_local(self.token, self.inner.0, worker);
        DecodeResult {
            inner: DecodeRows { _marker: PhantomData },
            request_type: RequestType::Select,
        }
    }

    pub fn send_global(self, worker: Box<dyn Worker>) -> DecodeResult<DecodeRows<S, K, V>> {
        S::send_global(self.token, self.inner.0, worker);
        DecodeResult {
            inner: DecodeRows { _marker: PhantomData },
            request_type: RequestType::Select,
        }
    }
}

impl<'a, S: Select<'a, K, V>, K, V> DecodeRows<S, K, V> {
    pub fn decode(&self, bytes: Vec<u8>) -> Result<Option<V>, CqlError> {
        S::try_decode(bytes.into())
    }
}

pub trait Select<'a, K, V>: Keyspace + RowsDecoder<K, V> {
    fn select(&'a self, key: &K) -> SelectRequest<'a, Self, K, V>;
}

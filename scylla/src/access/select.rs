// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use super::*;

pub struct SelectRequest<S, K, V> {
    token: i64,
    inner: Query,
    _marker: PhantomData<(S, K, V)>,
}

impl<S: Select<K, V>, K, V> SelectRequest<S, K, V> {
    pub fn new(query: Query, token: i64) -> Self {
        Self {
            token,
            inner: query,
            _marker: PhantomData,
        }
    }
    pub fn send_local(self, worker: Box<dyn Worker>) -> DecodeResult<DecodeRows<S, K, V>> {
        S::send_local(self.token, self.inner.0, worker);
        DecodeResult {
            inner: DecodeRows::<S, K, V> {_marker: PhantomData},
            request_type: RequestType::Select,
        }
    }
    pub fn send_global(self, worker: Box<dyn Worker>) -> DecodeResult<DecodeRows<S, K, V>> {
        S::send_global(self.token, self.inner.0, worker);
        DecodeResult {
            inner: DecodeRows::<S, K, V> {_marker: PhantomData},
            request_type: RequestType::Select,
        }
    }
}

impl<S: Select<K, V>, K, V> DecodeRows<S, K, V> {
    pub fn decode(&self, bytes: Vec<u8>) -> Result<Option<V>, CqlError> {
        S::try_decode(bytes.into())
    }
}

pub trait Select<K, V>: Keyspace + RowsDecoder<K, V> {
    fn select(&self, key: &K) -> SelectRequest<Self, K, V>;
}

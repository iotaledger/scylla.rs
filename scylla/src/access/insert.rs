// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use super::*;

pub struct VoidRequest<S, K, V> {
    token: i64,
    inner: Query,
    _marker: PhantomData<(S, K, V)>,
}

impl<S: Insert<K, V> + Default, K, V> VoidRequest<S, K, V> {
    pub fn new(query: Query, token: i64) -> Self {
        Self {
            token,
            inner: query,
            _marker: PhantomData,
        }
    }
    pub fn send_local(self, worker: Box<dyn Worker>) -> DecodeResult<DecodeVoid<S>> {
        S::send_local(self.token, self.inner.0, worker);
        DecodeResult {
            inner: DecodeVoid::default(),
            request_type: RequestType::Insert,
        }

    }
    pub fn send_global(self, worker: Box<dyn Worker>) -> DecodeResult<DecodeVoid<S>> {
        S::send_global(self.token, self.inner.0, worker);
        DecodeResult {
            inner: DecodeVoid::default(),
            request_type: RequestType::Insert,
        }
    }
}

impl<S: VoidDecoder> DecodeVoid<S> {
    pub fn decode(&self, bytes: Vec<u8>) -> Result<(), CqlError> {
        S::try_decode(bytes.into())
    }
}

pub trait Insert<K, V>: Keyspace + VoidDecoder {
    fn insert(&self, key: &K, value: &V) -> VoidRequest<Self, K, V>;
}

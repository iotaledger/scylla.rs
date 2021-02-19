// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use super::*;

pub struct UpdateRequest<'a, S, K, V> {
    token: i64,
    inner: Query,
    keyspace: &'a S,
    _marker: PhantomData<(K, V)>,
}

impl<'a, S: Update<'a, K, V>, K, V> UpdateRequest<'a, S, K, V> {
    pub fn new(query: Query, token: i64, keyspace: &'a S) -> Self {
        Self {
            token,
            inner: query,
            keyspace,
            _marker: PhantomData,
        }
    }

    pub fn send_local(self, worker: Box<dyn Worker>) -> DecodeResult<DecodeVoid<S>> {
        S::send_local(self.token, self.inner.0, worker);
        DecodeResult {
            inner: DecodeVoid { _marker: PhantomData },
            request_type: RequestType::Update,
        }
    }

    pub fn send_global(self, worker: Box<dyn Worker>) -> DecodeResult<DecodeVoid<S>> {
        S::send_global(self.token, self.inner.0, worker);
        DecodeResult {
            inner: DecodeVoid { _marker: PhantomData },
            request_type: RequestType::Update,
        }
    }
}

pub trait Update<'a, K, V>: Keyspace {
    fn update(&'a self, key: &K, value: &V) -> UpdateRequest<'a, Self, K, V>;
}

// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use super::*;

pub struct DeleteValRequest<'a, S, K, V> {
    keyspace: &'a S,
    _marker: PhantomData<(K, V)>,
}

pub trait GetDeleteValRequest<S, K> {
    fn for_value_type<V>(&self) -> DeleteValRequest<S, K, V>;
}

impl<S: Keyspace, K> GetDeleteValRequest<S, K> for S {
    fn for_value_type<V>(&self) -> DeleteValRequest<S, K, V> {
        DeleteValRequest {
            keyspace: self,
            _marker: PhantomData,
        }
    }
}

pub struct DeleteRequest<'a, S, K, V> {
    token: i64,
    inner: Query,
    keyspace: &'a S,
    _marker: PhantomData<(K, V)>,
}

impl<'a, S: Delete<'a, K, V>, K, V> DeleteValRequest<'a, S, K, V> {
    pub fn delete(&self, key: &'a K) -> DeleteRequest<S, K, V> {
        S::delete(self.keyspace, key)
    }
}

impl<'a, S: Delete<'a, K, V>, K, V> DeleteRequest<'a, S, K, V> {
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
            request_type: RequestType::Delete,
        }
    }

    pub fn send_global(self, worker: Box<dyn Worker>) -> DecodeResult<DecodeVoid<S>> {
        S::send_global(self.token, self.inner.0, worker);
        DecodeResult {
            inner: DecodeVoid { _marker: PhantomData },
            request_type: RequestType::Delete,
        }
    }
}

pub trait Delete<'a, K, V>: Keyspace {
    fn delete(&'a self, key: &K) -> DeleteRequest<'a, Self, K, V>;
}

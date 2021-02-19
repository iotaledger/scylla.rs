// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use super::*;

pub struct DeleteRequest<'a, S, K> {
    token: i64,
    inner: Query,
    keyspace: &'a S,
    _marker: PhantomData<K>,
}

impl<'a, S: Delete<'a, K>, K> DeleteRequest<'a, S, K> {
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

pub trait Delete<'a, K>: Keyspace {
    fn delete(&'a self, key: &K) -> DeleteRequest<'a, Self, K>;
}

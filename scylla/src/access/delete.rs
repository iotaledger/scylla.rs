// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use super::*;

pub struct DeleteQuery<S, K> {
    inner: Query,
    keyspace: PhantomData<S>,
    key: PhantomData<K>,
}

impl<S, K> Deref for DeleteQuery<S, K> {
    type Target = Query;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl<S, K> DerefMut for DeleteQuery<S, K> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.inner
    }
}

impl<S: Delete<K>, K> DeleteQuery<S, K> {
    pub fn into_bytes(&self) -> Vec<u8> {
        self.inner.0.clone()
    }

    pub fn take(&mut self) -> Query {
        std::mem::take(&mut self.inner)
    }

    pub fn decode(&self, bytes: Vec<u8>) -> Result<(), CqlError> {
        S::decode(bytes.into())
    }
}

pub trait Delete<K>: Keyspace {
    fn delete(&self, key: &K) -> DeleteQuery<Self, K>;

    fn decode(decoder: Decoder) -> Result<(), CqlError> {
        if decoder.is_error() {
            Err(decoder.body().into())
        } else {
            Ok(())
        }
    }
}

// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use super::*;

pub struct DeleteQuery<K> {
    inner: Query,
    key: PhantomData<K>,
}

impl<K> Deref for DeleteQuery<K> {
    type Target = Query;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl<K> DerefMut for DeleteQuery<K> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.inner
    }
}

impl<K> DeleteQuery<K> {
    pub fn into_bytes(self) -> Vec<u8> {
        self.inner.0
    }

    pub fn into_inner(self) -> Query {
        self.inner
    }
}

/// `Delete<K, V>` trait extends the `keyspace` with `delete` operation for the (key: K, value: V);
/// therefore, it should be explicitly implemented for the corresponding `Keyspace` with the correct DELETE CQL query.
pub trait Delete<K>: Keyspace {
    /// Delete
    fn delete(&self, key: &K) -> DeleteQuery<K>;
}

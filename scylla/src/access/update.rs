// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use super::*;

pub struct UpdateQuery<K, V> {
    inner: Query,
    key: PhantomData<K>,
    val: PhantomData<V>,
}

impl<K, V> Deref for UpdateQuery<K, V> {
    type Target = Query;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl<K, V> DerefMut for UpdateQuery<K, V> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.inner
    }
}

pub trait Update<K, V>: Keyspace {
    fn update(&self, key: &K, value: &V) -> UpdateQuery<K, V>;
}

// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use super::*;

pub struct InsertQuery<V> {
    inner: Query,
    val: PhantomData<V>,
}

impl<V> Deref for InsertQuery<V> {
    type Target = Query;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl<V> DerefMut for InsertQuery<V> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.inner
    }
}

pub trait Insert<V>: Keyspace {
    fn insert(&self, value: &V) -> InsertQuery<V>;
}

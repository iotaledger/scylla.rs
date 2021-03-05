// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use super::*;
use scylla_cql::{
    BatchBuild, BatchBuilder, BatchFlags, BatchStatementOrId, BatchTimestamp, BatchType, BatchTypeCounter,
    BatchTypeLogged, BatchTypeUnlogged, BatchTypeUnset, BatchValues, ColumnEncoder, Consistency,
};
use std::collections::HashMap;

pub trait InsertBatch<K, V, T: Copy + Into<u8>>: Insert<K, V> {
    fn recommended<B: QueryOrPrepared>(&self, builder: B) -> BatchBuilder<B::BatchType, BatchValues>;
    fn push_insert(builder: BatchBuilder<T, BatchValues>, key: &K, value: &V) -> BatchBuilder<T, BatchValues>;
}

pub trait UpdateBatch<K, V, T: Copy + Into<u8>>: Update<K, V> {
    fn recommended<B: QueryOrPrepared>(&self, builder: B) -> BatchBuilder<B::BatchType, BatchValues>;
    fn push_update(builder: BatchBuilder<T, BatchValues>, key: &K, value: &V) -> BatchBuilder<T, BatchValues>;
}

pub trait DeleteBatch<K, V, T: Copy + Into<u8>>: Delete<K, V> {
    fn recommended<B: QueryOrPrepared>(&self, builder: B) -> BatchBuilder<B::BatchType, BatchValues>;
    fn push_delete(builder: BatchBuilder<T, BatchValues>, key: &K) -> BatchBuilder<T, BatchValues>;
}

#[derive(Clone, Debug)]
pub struct BatchRequest<S> {
    token: i64,
    inner: Vec<u8>,
    map: HashMap<[u8; 16], std::borrow::Cow<'static, str>>,
    keyspace: S,
    _data: PhantomData<S>,
}

impl<S: Keyspace> BatchRequest<S> {
    /// Compute the murmur3 token from the provided K
    pub fn compute_token<K>(mut self, key: &K) -> Self
    where
        S: ComputeToken<K>,
    {
        self.token = S::token(key);
        self
    }

    /// Send a local request using the keyspace impl and return a type marker
    pub fn send_local(self, worker: Box<dyn Worker>) -> DecodeResult<DecodeVoid<S>> {
        send_local(
            self.token,
            self.inner,
            worker,
            self.keyspace.name().clone().into_owned(),
        );
        DecodeResult::batch()
    }

    /// Send a global request using the keyspace impl and return a type marker
    pub fn send_global(self, worker: Box<dyn Worker>) -> DecodeResult<DecodeVoid<S>> {
        send_global(
            self.token,
            self.inner,
            worker,
            self.keyspace.name().clone().into_owned(),
        );
        DecodeResult::batch()
    }

    pub fn get_cql(&self, id: &[u8; 16]) -> Option<&std::borrow::Cow<'static, str>> {
        self.map.get(id)
    }

    pub fn payload(&self) -> &Vec<u8> {
        &self.inner
    }
}
pub struct BatchCollector<S, Type: Copy + Into<u8>, Stage> {
    builder: BatchBuilder<Type, Stage>,
    map: HashMap<[u8; 16], std::borrow::Cow<'static, str>>,
    keyspace: S,
}

impl<S: Keyspace + Clone> BatchCollector<S, BatchTypeUnset, BatchType> {
    pub fn new(keyspace: &S) -> BatchCollector<S, BatchTypeUnset, BatchType> {
        BatchCollector {
            builder: scylla_cql::Batch::new(),
            map: HashMap::new(),
            keyspace: keyspace.clone(),
        }
    }

    pub fn with_capacity(keyspace: &S, capacity: usize) -> BatchCollector<S, BatchTypeUnset, BatchType> {
        BatchCollector {
            builder: scylla_cql::Batch::with_capacity(capacity),
            map: HashMap::new(),
            keyspace: keyspace.clone(),
        }
    }

    pub fn batch_type<Type: Copy + Into<u8>>(self, batch_type: Type) -> BatchCollector<S, Type, BatchStatementOrId> {
        Self::step(self.builder.batch_type(batch_type), self.map, self.keyspace)
    }
    pub fn logged(self) -> BatchCollector<S, BatchTypeLogged, BatchStatementOrId> {
        Self::step(self.builder.logged(), self.map, self.keyspace)
    }
    pub fn unlogged(self) -> BatchCollector<S, BatchTypeUnlogged, BatchStatementOrId> {
        Self::step(self.builder.unlogged(), self.map, self.keyspace)
    }
    pub fn counter(self) -> BatchCollector<S, BatchTypeCounter, BatchStatementOrId> {
        Self::step(self.builder.counter(), self.map, self.keyspace)
    }
}

impl<S: Keyspace, Type: Copy + Into<u8>> BatchCollector<S, Type, BatchStatementOrId> {
    pub fn insert_recommended<K, V>(self, key: &K, value: &V) -> BatchCollector<S, Type, BatchValues>
    where
        S: InsertBatch<K, V, Type>,
    {
        Self::step(S::recommended(&self.keyspace, self.builder), self.map, self.keyspace)
    }
    pub fn insert_query<K, V>(self, key: &K, value: &V) -> BatchCollector<S, Type, BatchValues>
    where
        S: InsertBatch<K, V, Type>,
    {
        let res = Self::step(
            self.builder.statement(self.keyspace.insert_statement().as_ref()),
            self.map,
            self.keyspace,
        );
        Self::step(S::push_insert(res.builder, key, value), res.map, res.keyspace)
    }
    pub fn insert_prepared<K, V>(mut self, key: &K, value: &V) -> BatchCollector<S, Type, BatchValues>
    where
        S: InsertBatch<K, V, Type>,
    {
        let id = self.keyspace.insert_id();
        self.map.insert(id, self.keyspace.insert_statement());
        let res = Self::step(self.builder.id(&id), self.map, self.keyspace);
        Self::step(S::push_insert(res.builder, key, value), res.map, res.keyspace)
    }

    pub fn update_recommended<K, V>(self, key: &K, value: &V) -> BatchCollector<S, Type, BatchValues>
    where
        S: UpdateBatch<K, V, Type>,
    {
        Self::step(S::recommended(&self.keyspace, self.builder), self.map, self.keyspace)
    }

    pub fn update_query<K, V>(self, key: &K, value: &V) -> BatchCollector<S, Type, BatchValues>
    where
        S: UpdateBatch<K, V, Type>,
    {
        let res = Self::step(
            self.builder.statement(self.keyspace.update_statement().as_ref()),
            self.map,
            self.keyspace,
        );
        Self::step(S::push_update(res.builder, key, value), res.map, res.keyspace)
    }

    pub fn update_prepared<K, V>(mut self, key: &K, value: &V) -> BatchCollector<S, Type, BatchValues>
    where
        S: UpdateBatch<K, V, Type>,
    {
        let id = self.keyspace.update_id();
        self.map.insert(id, self.keyspace.update_statement());
        let res = Self::step(self.builder.id(&id), self.map, self.keyspace);
        Self::step(S::push_update(res.builder, key, value), res.map, res.keyspace)
    }

    pub fn delete_recommended<K, V>(self, key: &K) -> BatchCollector<S, Type, BatchValues>
    where
        S: DeleteBatch<K, V, Type>,
    {
        Self::step(S::recommended(&self.keyspace, self.builder), self.map, self.keyspace)
    }

    pub fn delete_query<K, V>(self, key: &K) -> BatchCollector<S, Type, BatchValues>
    where
        S: DeleteBatch<K, V, Type>,
    {
        let res = Self::step(
            self.builder.statement(self.keyspace.delete_statement().as_ref()),
            self.map,
            self.keyspace,
        );
        Self::step(S::push_delete(res.builder, key), res.map, res.keyspace)
    }

    pub fn delete_prepared<K, V>(mut self, key: &K) -> BatchCollector<S, Type, BatchValues>
    where
        S: DeleteBatch<K, V, Type>,
    {
        let id = self.keyspace.delete_id();
        self.map.insert(id, self.keyspace.delete_statement());
        let res = Self::step(self.builder.id(&id), self.map, self.keyspace);
        Self::step(S::push_delete(res.builder, key), res.map, res.keyspace)
    }
}

impl<S: Keyspace, Type: Copy + Into<u8>> BatchCollector<S, Type, BatchValues> {
    pub fn insert_recommended<K, V>(self, key: &K, value: &V) -> BatchCollector<S, Type, BatchValues>
    where
        S: InsertBatch<K, V, Type>,
    {
        Self::step(S::recommended(&self.keyspace, self.builder), self.map, self.keyspace)
    }

    pub fn insert_query<K, V>(self, key: &K, value: &V) -> BatchCollector<S, Type, BatchValues>
    where
        S: InsertBatch<K, V, Type>,
    {
        let res = Self::step(
            self.builder.statement(self.keyspace.insert_statement().as_ref()),
            self.map,
            self.keyspace,
        );
        Self::step(S::push_insert(res.builder, key, value), res.map, res.keyspace)
    }

    pub fn insert_prepared<K, V>(mut self, key: &K, value: &V) -> BatchCollector<S, Type, BatchValues>
    where
        S: InsertBatch<K, V, Type>,
    {
        let id = self.keyspace.insert_id();
        self.map.insert(id, self.keyspace.insert_statement());
        let res = Self::step(self.builder.id(&id), self.map, self.keyspace);
        Self::step(S::push_insert(res.builder, key, value), res.map, res.keyspace)
    }

    pub fn update_recommended<K, V>(self, key: &K, value: &V) -> BatchCollector<S, Type, BatchValues>
    where
        S: UpdateBatch<K, V, Type>,
    {
        Self::step(S::recommended(&self.keyspace, self.builder), self.map, self.keyspace)
    }

    pub fn update_query<K, V>(self, key: &K, value: &V) -> BatchCollector<S, Type, BatchValues>
    where
        S: UpdateBatch<K, V, Type>,
    {
        let res = Self::step(
            self.builder.statement(self.keyspace.update_statement().as_ref()),
            self.map,
            self.keyspace,
        );
        Self::step(S::push_update(res.builder, key, value), res.map, res.keyspace)
    }

    pub fn update_prepared<K, V>(mut self, key: &K, value: &V) -> BatchCollector<S, Type, BatchValues>
    where
        S: UpdateBatch<K, V, Type>,
    {
        let id = self.keyspace.update_id();
        self.map.insert(id, self.keyspace.update_statement());
        let res = Self::step(self.builder.id(&id), self.map, self.keyspace);
        Self::step(S::push_update(res.builder, key, value), res.map, res.keyspace)
    }

    pub fn delete_recommended<K, V>(self, key: &K) -> BatchCollector<S, Type, BatchValues>
    where
        S: DeleteBatch<K, V, Type>,
    {
        Self::step(S::recommended(&self.keyspace, self.builder), self.map, self.keyspace)
    }

    pub fn delete_query<K, V>(self, key: &K) -> BatchCollector<S, Type, BatchValues>
    where
        S: DeleteBatch<K, V, Type>,
    {
        let res = Self::step(
            self.builder.statement(self.keyspace.delete_statement().as_ref()),
            self.map,
            self.keyspace,
        );
        Self::step(S::push_delete(res.builder, key), res.map, res.keyspace)
    }

    pub fn delete_prepared<K, V>(mut self, key: &K) -> BatchCollector<S, Type, BatchValues>
    where
        S: DeleteBatch<K, V, Type>,
    {
        let id = self.keyspace.delete_id();
        self.map.insert(id, self.keyspace.delete_statement());
        let res = Self::step(self.builder.id(&id), self.map, self.keyspace);
        Self::step(S::push_delete(res.builder, key), res.map, res.keyspace)
    }

    pub fn value<V: ColumnEncoder>(self, value: &V) -> Self {
        Self::step(self.builder.value(value), self.map, self.keyspace)
    }
    pub fn unset_value(self) -> Self {
        Self::step(self.builder.unset_value(), self.map, self.keyspace)
    }
    pub fn null_value(self) -> Self {
        Self::step(self.builder.null_value(), self.map, self.keyspace)
    }
    pub fn consistency(self, consistency: Consistency) -> BatchCollector<S, Type, BatchFlags> {
        Self::step(self.builder.consistency(consistency), self.map, self.keyspace)
    }
}

impl<S: Keyspace, Type: Copy + Into<u8>> BatchCollector<S, Type, BatchFlags> {
    pub fn serial_consistency(self, consistency: Consistency) -> BatchCollector<S, Type, BatchTimestamp> {
        Self::step(self.builder.serial_consistency(consistency), self.map, self.keyspace)
    }
    pub fn timestamp(self, timestamp: i64) -> BatchCollector<S, Type, BatchBuild> {
        Self::step(self.builder.timestamp(timestamp), self.map, self.keyspace)
    }
    pub fn build(self) -> BatchRequest<S> {
        BatchRequest {
            token: rand::random::<i64>(),
            map: self.map,
            inner: self.builder.build().0.into(),
            keyspace: self.keyspace,
            _data: PhantomData,
        }
    }
}

impl<S: Keyspace, Type: Copy + Into<u8>> BatchCollector<S, Type, BatchTimestamp> {
    pub fn timestamp(self, timestamp: i64) -> BatchCollector<S, Type, BatchBuild> {
        Self::step(self.builder.timestamp(timestamp), self.map, self.keyspace)
    }
    pub fn build(self) -> BatchRequest<S> {
        BatchRequest {
            token: rand::random::<i64>(),
            map: self.map,
            inner: self.builder.build().0.into(),
            keyspace: self.keyspace,
            _data: PhantomData,
        }
    }
}

impl<S: Keyspace, Type: Copy + Into<u8>> BatchCollector<S, Type, BatchBuild> {
    pub fn build(self) -> BatchRequest<S> {
        BatchRequest {
            token: rand::random::<i64>(),
            map: self.map,
            inner: self.builder.build().0.into(),
            keyspace: self.keyspace,
            _data: PhantomData,
        }
    }
}

impl<S: Keyspace, Type: Copy + Into<u8>, Stage> BatchCollector<S, Type, Stage> {
    fn step<NextType: Copy + Into<u8>, NextStage>(
        builder: BatchBuilder<NextType, NextStage>,
        map: HashMap<[u8; 16], std::borrow::Cow<'static, str>>,
        keyspace: S,
    ) -> BatchCollector<S, NextType, NextStage> {
        BatchCollector { builder, map, keyspace }
    }
}

/// Defines a helper method to allow keyspaces to begin constructing a batch
pub trait Batch {
    /// Start building a batch.
    /// This function will borrow the keyspace until the batch is fully built in order
    /// to access its trait definitions.
    fn batch(&self) -> BatchCollector<Self, BatchTypeUnset, BatchType>
    where
        Self: Keyspace + Clone,
    {
        BatchCollector::new(self)
    }
}

impl<S: Keyspace + Clone> Batch for S {}

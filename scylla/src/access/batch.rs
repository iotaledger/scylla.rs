// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use super::*;
use scylla_cql::{
    BatchBuild, BatchBuilder, BatchFlags, BatchStatementOrId, BatchTimestamp, BatchType, BatchTypeCounter,
    BatchTypeLogged, BatchTypeUnlogged, BatchTypeUnset, BatchValues, ColumnEncoder, Consistency,
};
use std::{collections::HashMap, marker::PhantomData};

pub trait InsertBatch<K, V, T: Copy + Into<u8>>: Insert<K, V> {
    fn recommended() -> BatchQueryType;
    fn query() -> BatchQueryType {
        BatchQueryType::Query
    }
    fn prepared() -> BatchQueryType {
        BatchQueryType::Prepared
    }
    fn push_insert(builder: BatchBuilder<T, BatchValues>, key: &K, value: &V) -> BatchBuilder<T, BatchValues>;
}

pub trait UpdateBatch<K, V, T: Copy + Into<u8>>: Update<K, V> {
    fn recommended() -> BatchQueryType;
    fn query() -> BatchQueryType {
        BatchQueryType::Query
    }
    fn prepared() -> BatchQueryType {
        BatchQueryType::Prepared
    }
    fn push_update(builder: BatchBuilder<T, BatchValues>, key: &K, value: &V) -> BatchBuilder<T, BatchValues>;
}

pub trait DeleteBatch<K, V, T: Copy + Into<u8>>: Delete<K, V> {
    fn recommended() -> BatchQueryType;
    fn push_delete(builder: BatchBuilder<T, BatchValues>, key: &K) -> BatchBuilder<T, BatchValues>;
}

#[derive(Clone, Debug)]
pub struct BatchRequest<S, C: IterCqls<S>> {
    token: i64,
    inner: Vec<u8>,
    cqls: C,
    keyspace: S,
    _data: PhantomData<S>,
}

impl<C: IterCqls<S>, S: Keyspace> BatchRequest<S, C> {
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

    pub fn get_cql(&self, id: &[u8; 16]) -> Option<String> {
        self.cqls.cql(&self.keyspace, id)
    }

    pub fn payload(&self) -> &Vec<u8> {
        &self.inner
    }
}
pub struct BatchCollector<C: IterCqls<S>, S, Type: Copy + Into<u8>, Stage> {
    builder: BatchBuilder<Type, Stage>,
    cqls: C,
    keyspace: S,
}

#[derive(Clone)]
pub struct CqlsTypeUnset;
impl<S> IterCqls<S> for CqlsTypeUnset {
    fn cql(&self, keyspace: &S, id: &[u8; 16]) -> Option<String> {
        None
    }
}

impl<C: IterCqls<S>, S: Keyspace + Clone> BatchCollector<C, S, BatchTypeUnset, BatchType> {
    pub fn new(keyspace: &S) -> BatchCollector<CqlsTypeUnset, S, BatchTypeUnset, BatchType> {
        BatchCollector {
            builder: scylla_cql::Batch::new(),
            cqls: CqlsTypeUnset,
            keyspace: keyspace.clone(),
        }
    }

    pub fn with_capacity(keyspace: &S, capacity: usize) -> BatchCollector<CqlsTypeUnset, S, BatchTypeUnset, BatchType> {
        BatchCollector {
            builder: scylla_cql::Batch::with_capacity(capacity),
            cqls: CqlsTypeUnset,
            keyspace: keyspace.clone(),
        }
    }

    pub fn batch_type<Type: Copy + Into<u8>>(
        self,
        batch_type: Type,
    ) -> BatchCollector<CqlsTypeUnset, S, Type, BatchStatementOrId> {
        Self::step(self.builder.batch_type(batch_type), CqlsTypeUnset, self.keyspace)
    }
    pub fn logged(self) -> BatchCollector<CqlsTypeUnset, S, BatchTypeLogged, BatchStatementOrId> {
        Self::step(self.builder.logged(), CqlsTypeUnset, self.keyspace)
    }
    pub fn unlogged(self) -> BatchCollector<CqlsTypeUnset, S, BatchTypeUnlogged, BatchStatementOrId> {
        Self::step(self.builder.unlogged(), CqlsTypeUnset, self.keyspace)
    }
    pub fn counter(self) -> BatchCollector<CqlsTypeUnset, S, BatchTypeCounter, BatchStatementOrId> {
        Self::step(self.builder.counter(), CqlsTypeUnset, self.keyspace)
    }
}
#[derive(Clone)]
pub struct InsertCqls<S, C: IterCqls<S>, K, V> {
    _marker: PhantomData<(S, K, V)>,
    prev: C,
}
#[derive(Clone)]
pub struct UpdateCqls<S, C: IterCqls<S>, K, V> {
    _marker: PhantomData<(S, K, V)>,
    prev: C,
}
#[derive(Clone)]
pub struct DeleteCqls<S, C: IterCqls<S>, K, V> {
    _marker: PhantomData<(S, K, V)>,
    prev: C,
}
#[derive(Clone)]
pub struct SelectCqls<S, C: IterCqls<S>, K, V> {
    _marker: PhantomData<(S, K, V)>,
    prev: C,
}

/// UnknownCqls type
#[derive(Clone)]
pub enum UnknownCqls<Old, New> {
    Old(Old),
    New(New),
}

impl<S, Old: IterCqls<S>, New: IterCqls<S>> IterCqls<S> for UnknownCqls<Old, New> {
    fn cql(&self, keyspace: &S, id: &[u8; 16]) -> Option<String> {
        match self {
            UnknownCqls::Old(old) => old.cql(keyspace, id),
            UnknownCqls::New(new) => new.cql(keyspace, id),
        }
    }
}

impl<S: Insert<K, V>, C: IterCqls<S>, K, V> IterCqls<S> for InsertCqls<S, C, K, V> {
    fn cql(&self, keyspace: &S, id: &[u8; 16]) -> Option<String> {
        if &keyspace.insert_id::<K, V>() == id {
            // found it
            Some(keyspace.insert_statement::<K, V>().to_string())
        } else {
            // move to prev operation type
            self.prev.cql(keyspace, id)
        }
    }
}
impl<S: Update<K, V>, C: IterCqls<S>, K, V> IterCqls<S> for UpdateCqls<S, C, K, V> {
    fn cql(&self, keyspace: &S, id: &[u8; 16]) -> Option<String> {
        if &keyspace.update_id::<K, V>() == id {
            // found it
            Some(keyspace.update_statement::<K, V>().to_string())
        } else {
            // move to prev operation type
            self.prev.cql(keyspace, id)
        }
    }
}
impl<S: Delete<K, V>, C: IterCqls<S>, K, V> IterCqls<S> for DeleteCqls<S, C, K, V> {
    fn cql(&self, keyspace: &S, id: &[u8; 16]) -> Option<String> {
        if &keyspace.delete_id::<K, V>() == id {
            // found it
            Some(keyspace.delete_statement::<K, V>().to_string())
        } else {
            // move to next operation type
            self.prev.cql(keyspace, id)
        }
    }
}
// this not needed for batch, likely to be removed later
impl<S: Select<K, V>, C: IterCqls<S>, K, V> IterCqls<S> for SelectCqls<S, C, K, V> {
    fn cql(&self, keyspace: &S, id: &[u8; 16]) -> Option<String> {
        if &keyspace.select_id::<K, V>() == id {
            // found it
            Some(keyspace.select_statement::<K, V>().to_string())
        } else {
            // move to next operation type
            self.prev.cql(keyspace, id)
        }
    }
}
impl<C: IterCqls<S>, S: Keyspace, Type: Copy + Into<u8>> BatchCollector<C, S, Type, BatchStatementOrId> {
    pub fn insert_recommended<K, V>(
        self,
        key: &K,
        value: &V,
    ) -> BatchCollector<UnknownCqls<C, InsertCqls<S, C, K, V>>, S, Type, BatchValues>
    where
        S: InsertBatch<K, V, Type>,
    {
        match S::recommended() {
            BatchQueryType::Query => {
                let unknown = UnknownCqls::Old(self.cqls);
                Self::step(
                    S::push_insert(
                        self.builder.statement(self.keyspace.insert_statement().as_ref()),
                        key,
                        value,
                    ),
                    unknown,
                    self.keyspace,
                )
            }
            BatchQueryType::Prepared => {
                let new_cqls = InsertCqls {
                    _marker: PhantomData,
                    prev: self.cqls,
                };
                let unknown = UnknownCqls::New(new_cqls);
                Self::step(
                    S::push_insert(self.builder.id(&self.keyspace.insert_id()), key, value),
                    unknown,
                    self.keyspace,
                )
            }
        }
    }
    pub fn insert_query<K, V>(self, key: &K, value: &V) -> BatchCollector<C, S, Type, BatchValues>
    where
        S: InsertBatch<K, V, Type>,
    {
        // no need to update cqls type for query
        let res = Self::step(
            self.builder.statement(self.keyspace.insert_statement().as_ref()),
            self.cqls,
            self.keyspace,
        );
        Self::step(S::push_insert(res.builder, key, value), res.cqls, res.keyspace)
    }
    pub fn insert_prepared<K, V>(
        mut self,
        key: &K,
        value: &V,
    ) -> BatchCollector<InsertCqls<S, C, K, V>, S, Type, BatchValues>
    where
        S: InsertBatch<K, V, Type>,
    {
        let new_cqls = InsertCqls {
            _marker: PhantomData,
            prev: self.cqls,
        };

        let id = self.keyspace.insert_id();
        let res = Self::step(self.builder.id(&id), new_cqls, self.keyspace);
        Self::step(S::push_insert(res.builder, key, value), res.cqls, res.keyspace)
    }

    pub fn update_recommended<K, V>(
        self,
        key: &K,
        value: &V,
    ) -> BatchCollector<UnknownCqls<C, UpdateCqls<S, C, K, V>>, S, Type, BatchValues>
    where
        S: UpdateBatch<K, V, Type>,
    {
        match S::recommended() {
            BatchQueryType::Query => {
                let unknown = UnknownCqls::Old(self.cqls);
                Self::step(
                    S::push_update(
                        self.builder.statement(self.keyspace.update_statement().as_ref()),
                        key,
                        value,
                    ),
                    unknown,
                    self.keyspace,
                )
            }
            BatchQueryType::Prepared => {
                let new_cqls = UpdateCqls {
                    _marker: PhantomData,
                    prev: self.cqls,
                };
                let unknown = UnknownCqls::New(new_cqls);
                Self::step(
                    S::push_update(self.builder.id(&self.keyspace.update_id()), key, value),
                    unknown,
                    self.keyspace,
                )
            }
        }
    }

    pub fn update_query<K, V>(self, key: &K, value: &V) -> BatchCollector<C, S, Type, BatchValues>
    where
        S: UpdateBatch<K, V, Type>,
    {
        let res = Self::step(
            self.builder.statement(self.keyspace.update_statement().as_ref()),
            self.cqls,
            self.keyspace,
        );
        Self::step(S::push_update(res.builder, key, value), res.cqls, res.keyspace)
    }

    pub fn update_prepared<K, V>(
        mut self,
        key: &K,
        value: &V,
    ) -> BatchCollector<UpdateCqls<S, C, K, V>, S, Type, BatchValues>
    where
        S: UpdateBatch<K, V, Type>,
    {
        let new_cqls = UpdateCqls {
            _marker: PhantomData,
            prev: self.cqls,
        };
        let id = self.keyspace.update_id();
        let res = Self::step(self.builder.id(&id), new_cqls, self.keyspace);
        Self::step(S::push_update(res.builder, key, value), res.cqls, res.keyspace)
    }

    pub fn delete_recommended<K, V>(
        self,
        key: &K,
    ) -> BatchCollector<UnknownCqls<C, DeleteCqls<S, C, K, V>>, S, Type, BatchValues>
    where
        S: DeleteBatch<K, V, Type>,
    {
        match S::recommended() {
            BatchQueryType::Query => {
                let unknown = UnknownCqls::Old(self.cqls);
                Self::step(
                    S::push_delete(self.builder.statement(self.keyspace.delete_statement().as_ref()), key),
                    unknown,
                    self.keyspace,
                )
            }
            BatchQueryType::Prepared => {
                let new_cqls = DeleteCqls {
                    _marker: PhantomData,
                    prev: self.cqls,
                };
                let unknown = UnknownCqls::New(new_cqls);
                Self::step(
                    S::push_delete(self.builder.id(&self.keyspace.delete_id()), key),
                    unknown,
                    self.keyspace,
                )
            }
        }
    }

    pub fn delete_query<K, V>(self, key: &K) -> BatchCollector<C, S, Type, BatchValues>
    where
        S: DeleteBatch<K, V, Type>,
    {
        let res = Self::step(
            self.builder.statement(self.keyspace.delete_statement().as_ref()),
            self.cqls,
            self.keyspace,
        );
        Self::step(S::push_delete(res.builder, key), res.cqls, res.keyspace)
    }

    pub fn delete_prepared<K, V>(mut self, key: &K) -> BatchCollector<DeleteCqls<S, C, K, V>, S, Type, BatchValues>
    where
        S: DeleteBatch<K, V, Type>,
    {
        let new_cqls = DeleteCqls {
            _marker: PhantomData,
            prev: self.cqls,
        };
        let id = self.keyspace.delete_id();
        let res = Self::step(self.builder.id(&id), new_cqls, self.keyspace);
        Self::step(S::push_delete(res.builder, key), res.cqls, res.keyspace)
    }
}

impl<C: IterCqls<S>, S: Keyspace, Type: Copy + Into<u8>> BatchCollector<C, S, Type, BatchValues> {
    pub fn insert_recommended<K, V>(
        self,
        key: &K,
        value: &V,
    ) -> BatchCollector<UnknownCqls<C, InsertCqls<S, C, K, V>>, S, Type, BatchValues>
    where
        S: InsertBatch<K, V, Type>,
    {
        match S::recommended() {
            BatchQueryType::Query => {
                let unknown = UnknownCqls::Old(self.cqls);
                Self::step(
                    S::push_insert(
                        self.builder.statement(self.keyspace.insert_statement().as_ref()),
                        key,
                        value,
                    ),
                    unknown,
                    self.keyspace,
                )
            }
            BatchQueryType::Prepared => {
                let new_cqls = InsertCqls {
                    _marker: PhantomData,
                    prev: self.cqls,
                };
                let unknown = UnknownCqls::New(new_cqls);
                Self::step(
                    S::push_insert(self.builder.id(&self.keyspace.insert_id()), key, value),
                    unknown,
                    self.keyspace,
                )
            }
        }
    }

    pub fn insert_query<K, V>(self, key: &K, value: &V) -> BatchCollector<C, S, Type, BatchValues>
    where
        S: InsertBatch<K, V, Type>,
    {
        // no need to update cqls type for query
        let res = Self::step(
            self.builder.statement(self.keyspace.insert_statement().as_ref()),
            self.cqls,
            self.keyspace,
        );
        Self::step(S::push_insert(res.builder, key, value), res.cqls, res.keyspace)
    }

    pub fn insert_prepared<K, V>(
        mut self,
        key: &K,
        value: &V,
    ) -> BatchCollector<InsertCqls<S, C, K, V>, S, Type, BatchValues>
    where
        S: InsertBatch<K, V, Type>,
    {
        let new_cqls = InsertCqls {
            _marker: PhantomData,
            prev: self.cqls,
        };

        let id = self.keyspace.insert_id();
        let res = Self::step(self.builder.id(&id), new_cqls, self.keyspace);
        Self::step(S::push_insert(res.builder, key, value), res.cqls, res.keyspace)
    }

    pub fn update_recommended<K, V>(
        self,
        key: &K,
        value: &V,
    ) -> BatchCollector<UnknownCqls<C, UpdateCqls<S, C, K, V>>, S, Type, BatchValues>
    where
        S: UpdateBatch<K, V, Type>,
    {
        match S::recommended() {
            BatchQueryType::Query => {
                let unknown = UnknownCqls::Old(self.cqls);
                Self::step(
                    S::push_update(
                        self.builder.statement(self.keyspace.update_statement().as_ref()),
                        key,
                        value,
                    ),
                    unknown,
                    self.keyspace,
                )
            }
            BatchQueryType::Prepared => {
                let new_cqls = UpdateCqls {
                    _marker: PhantomData,
                    prev: self.cqls,
                };
                let unknown = UnknownCqls::New(new_cqls);
                Self::step(
                    S::push_update(self.builder.id(&self.keyspace.update_id()), key, value),
                    unknown,
                    self.keyspace,
                )
            }
        }
    }

    pub fn update_query<K, V>(self, key: &K, value: &V) -> BatchCollector<C, S, Type, BatchValues>
    where
        S: UpdateBatch<K, V, Type>,
    {
        let res = Self::step(
            self.builder.statement(self.keyspace.update_statement().as_ref()),
            self.cqls,
            self.keyspace,
        );
        Self::step(S::push_update(res.builder, key, value), res.cqls, res.keyspace)
    }

    pub fn update_prepared<K, V>(
        mut self,
        key: &K,
        value: &V,
    ) -> BatchCollector<UpdateCqls<S, C, K, V>, S, Type, BatchValues>
    where
        S: UpdateBatch<K, V, Type>,
    {
        let new_cqls = UpdateCqls {
            _marker: PhantomData,
            prev: self.cqls,
        };
        let id = self.keyspace.update_id();
        let res = Self::step(self.builder.id(&id), new_cqls, self.keyspace);
        Self::step(S::push_update(res.builder, key, value), res.cqls, res.keyspace)
    }

    pub fn delete_recommended<K, V>(
        self,
        key: &K,
    ) -> BatchCollector<UnknownCqls<C, DeleteCqls<S, C, K, V>>, S, Type, BatchValues>
    where
        S: DeleteBatch<K, V, Type>,
    {
        match S::recommended() {
            BatchQueryType::Query => {
                let unknown = UnknownCqls::Old(self.cqls);
                Self::step(
                    S::push_delete(self.builder.statement(self.keyspace.delete_statement().as_ref()), key),
                    unknown,
                    self.keyspace,
                )
            }
            BatchQueryType::Prepared => {
                let new_cqls = DeleteCqls {
                    _marker: PhantomData,
                    prev: self.cqls,
                };
                let unknown = UnknownCqls::New(new_cqls);
                Self::step(
                    S::push_delete(self.builder.id(&self.keyspace.delete_id()), key),
                    unknown,
                    self.keyspace,
                )
            }
        }
    }

    pub fn delete_query<K, V>(self, key: &K) -> BatchCollector<C, S, Type, BatchValues>
    where
        S: DeleteBatch<K, V, Type>,
    {
        let res = Self::step(
            self.builder.statement(self.keyspace.delete_statement().as_ref()),
            self.cqls,
            self.keyspace,
        );
        Self::step(S::push_delete(res.builder, key), res.cqls, res.keyspace)
    }

    pub fn delete_prepared<K, V>(mut self, key: &K) -> BatchCollector<DeleteCqls<S, C, K, V>, S, Type, BatchValues>
    where
        S: DeleteBatch<K, V, Type>,
    {
        let new_cqls = DeleteCqls {
            _marker: PhantomData,
            prev: self.cqls,
        };
        let id = self.keyspace.delete_id();
        let res = Self::step(self.builder.id(&id), new_cqls, self.keyspace);
        Self::step(S::push_delete(res.builder, key), res.cqls, res.keyspace)
    }

    pub fn value<V: ColumnEncoder>(self, value: &V) -> Self {
        Self::step(self.builder.value(value), self.cqls, self.keyspace)
    }
    pub fn unset_value(self) -> Self {
        Self::step(self.builder.unset_value(), self.cqls, self.keyspace)
    }
    pub fn null_value(self) -> Self {
        Self::step(self.builder.null_value(), self.cqls, self.keyspace)
    }
    pub fn consistency(self, consistency: Consistency) -> BatchCollector<C, S, Type, BatchFlags> {
        Self::step(self.builder.consistency(consistency), self.cqls, self.keyspace)
    }
}

impl<C: IterCqls<S>, S: Keyspace, Type: Copy + Into<u8>> BatchCollector<C, S, Type, BatchFlags> {
    pub fn serial_consistency(self, consistency: Consistency) -> BatchCollector<C, S, Type, BatchTimestamp> {
        Self::step(self.builder.serial_consistency(consistency), self.cqls, self.keyspace)
    }
    pub fn timestamp(self, timestamp: i64) -> BatchCollector<C, S, Type, BatchBuild> {
        Self::step(self.builder.timestamp(timestamp), self.cqls, self.keyspace)
    }
    pub fn build(self) -> BatchRequest<S, C> {
        BatchRequest {
            token: rand::random::<i64>(),
            cqls: self.cqls,
            inner: self.builder.build().0.into(),
            keyspace: self.keyspace,
            _data: PhantomData,
        }
    }
}

impl<C: IterCqls<S>, S: Keyspace, Type: Copy + Into<u8>> BatchCollector<C, S, Type, BatchTimestamp> {
    pub fn timestamp(self, timestamp: i64) -> BatchCollector<C, S, Type, BatchBuild> {
        Self::step(self.builder.timestamp(timestamp), self.cqls, self.keyspace)
    }
    pub fn build(self) -> BatchRequest<S, C> {
        BatchRequest {
            token: rand::random::<i64>(),
            cqls: self.cqls,
            inner: self.builder.build().0.into(),
            keyspace: self.keyspace,
            _data: PhantomData,
        }
    }
}

impl<C: IterCqls<S>, S: Keyspace, Type: Copy + Into<u8>> BatchCollector<C, S, Type, BatchBuild> {
    pub fn build(self) -> BatchRequest<S, C> {
        BatchRequest {
            token: rand::random::<i64>(),
            cqls: self.cqls,
            inner: self.builder.build().0.into(),
            keyspace: self.keyspace,
            _data: PhantomData,
        }
    }
}

impl<C: IterCqls<S>, S: Keyspace, Type: Copy + Into<u8>, Stage> BatchCollector<C, S, Type, Stage> {
    fn step<NextCqls: IterCqls<S>, NextType: Copy + Into<u8>, NextStage>(
        builder: BatchBuilder<NextType, NextStage>,
        cqls: NextCqls,
        keyspace: S,
    ) -> BatchCollector<NextCqls, S, NextType, NextStage> {
        BatchCollector {
            builder,
            cqls,
            keyspace,
        }
    }
}

/// Defines a helper method to allow keyspaces to begin constructing a batch
pub trait Batch {
    /// Start building a batch.
    /// This function will borrow the keyspace until the batch is fully built in order
    /// to access its trait definitions.
    fn batch(&self) -> BatchCollector<CqlsTypeUnset, Self, BatchTypeUnset, BatchType>
    where
        Self: Keyspace + Clone,
    {
        BatchCollector::<CqlsTypeUnset, Self, BatchTypeUnset, BatchType>::new(self)
    }
}

impl<S: Keyspace + Clone> Batch for S {}

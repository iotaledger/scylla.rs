use std::collections::HashMap;

use scylla_cql::{
    compression::Compression, BatchBuild, BatchBuilder, BatchFlags, BatchStatementOrId, BatchTimestamp, BatchType,
    BatchTypes, BatchValues, ColumnEncoder, Consistency,
};

use crate::Worker;

use super::{DecodeResult, DecodeVoid, Delete, Insert, Keyspace, Update};

pub struct BatchRequest<'a, S> {
    token: i64,
    inner: Vec<u8>,
    keyspace: &'a S,
}

impl<'a, S: 'a + Keyspace> BatchRequest<'a, S> {
    /// Send a local request using the keyspace impl and return a type marker
    pub fn send_local(self, worker: Box<dyn Worker>) -> DecodeResult<DecodeVoid<S>> {
        self.keyspace.send_local(self.token, self.inner, worker);
        DecodeResult::batch()
    }

    /// Send a global request using the keyspace impl and return a type marker
    pub fn send_global(self, worker: Box<dyn Worker>) -> DecodeResult<DecodeVoid<S>> {
        self.keyspace.send_global(self.token, self.inner, worker);
        DecodeResult::batch()
    }
}
pub struct BatchCollector<'a, S, T> {
    builder: BatchBuilder<T>,
    map: HashMap<[u8; 16], std::borrow::Cow<'static, str>>,
    keyspace: &'a S,
}

impl<'a, S: 'a + Keyspace> BatchCollector<'a, S, BatchType> {
    pub fn new(keyspace: &'a S) -> Self {
        Self {
            builder: scylla_cql::Batch::new(),
            map: HashMap::new(),
            keyspace,
        }
    }

    pub fn with_capacity(keyspace: &'a S, capacity: usize) -> Self {
        Self {
            builder: scylla_cql::Batch::with_capacity(capacity),
            map: HashMap::new(),
            keyspace,
        }
    }

    pub fn batch_type(mut self, batch_type: BatchTypes) -> BatchCollector<'a, S, BatchStatementOrId> {
        Self::step(self.builder.batch_type(batch_type), self.map, self.keyspace)
    }
    pub fn logged(mut self) -> BatchCollector<'a, S, BatchStatementOrId> {
        Self::step(self.builder.logged(), self.map, self.keyspace)
    }
    pub fn unlogged(mut self) -> BatchCollector<'a, S, BatchStatementOrId> {
        Self::step(self.builder.unlogged(), self.map, self.keyspace)
    }
    pub fn counter(mut self) -> BatchCollector<'a, S, BatchStatementOrId> {
        Self::step(self.builder.counter(), self.map, self.keyspace)
    }
}

impl<'a, S: 'a + Keyspace> BatchCollector<'a, S, BatchStatementOrId> {
    pub fn insert_query<K, V>(mut self) -> BatchCollector<'a, S, BatchValues>
    where
        S: Insert<'a, K, V>,
    {
        Self::step(
            self.builder.statement(S::insert_statement().as_ref()),
            self.map,
            self.keyspace,
        )
    }

    pub fn insert_prepared<K, V>(mut self) -> BatchCollector<'a, S, BatchValues>
    where
        S: Insert<'a, K, V>,
    {
        let id = S::insert_id();
        self.map.insert(id, S::insert_statement());
        Self::step(self.builder.id(&id), self.map, self.keyspace)
    }

    pub fn update_query<K, V>(mut self) -> BatchCollector<'a, S, BatchValues>
    where
        S: Update<'a, K, V>,
    {
        Self::step(
            self.builder.statement(S::update_statement().as_ref()),
            self.map,
            self.keyspace,
        )
    }

    pub fn update_prepared<K, V>(mut self) -> BatchCollector<'a, S, BatchValues>
    where
        S: Update<'a, K, V>,
    {
        let id = S::update_id();
        self.map.insert(id, S::update_statement());
        Self::step(self.builder.id(&id), self.map, self.keyspace)
    }

    pub fn delete_query<K, V>(mut self) -> BatchCollector<'a, S, BatchValues>
    where
        S: Delete<'a, K, V>,
    {
        Self::step(
            self.builder.statement(S::delete_statement().as_ref()),
            self.map,
            self.keyspace,
        )
    }

    pub fn delete_prepared<K, V>(mut self) -> BatchCollector<'a, S, BatchValues>
    where
        S: Delete<'a, K, V>,
    {
        let id = S::delete_id();
        self.map.insert(id, S::delete_statement());
        Self::step(self.builder.id(&id), self.map, self.keyspace)
    }
}

impl<'a, S: 'a + Keyspace> BatchCollector<'a, S, BatchValues> {
    pub fn insert_query<K, V>(mut self) -> Self
    where
        S: Insert<'a, K, V>,
    {
        Self::step(
            self.builder.statement(S::insert_statement().as_ref()),
            self.map,
            self.keyspace,
        )
    }

    pub fn insert_prepared<K, V>(mut self) -> Self
    where
        S: Insert<'a, K, V>,
    {
        let id = S::insert_id();
        self.map.insert(id, S::insert_statement());
        Self::step(self.builder.id(&id), self.map, self.keyspace)
    }

    pub fn update_query<K, V>(mut self) -> Self
    where
        S: Update<'a, K, V>,
    {
        Self::step(
            self.builder.statement(S::update_statement().as_ref()),
            self.map,
            self.keyspace,
        )
    }

    pub fn update_prepared<K, V>(mut self) -> Self
    where
        S: Update<'a, K, V>,
    {
        let id = S::update_id();
        self.map.insert(id, S::update_statement());
        Self::step(self.builder.id(&id), self.map, self.keyspace)
    }

    pub fn delete_query<K, V>(mut self) -> Self
    where
        S: Delete<'a, K, V>,
    {
        Self::step(
            self.builder.statement(S::delete_statement().as_ref()),
            self.map,
            self.keyspace,
        )
    }

    pub fn delete_prepared<K, V>(mut self) -> Self
    where
        S: Delete<'a, K, V>,
    {
        let id = S::delete_id();
        self.map.insert(id, S::delete_statement());
        Self::step(self.builder.id(&id), self.map, self.keyspace)
    }

    pub fn value(mut self, value: impl ColumnEncoder) -> Self {
        Self::step(self.builder.value(value), self.map, self.keyspace)
    }
    pub fn unset_value(mut self) -> Self {
        Self::step(self.builder.unset_value(), self.map, self.keyspace)
    }
    pub fn null_value(mut self) -> Self {
        Self::step(self.builder.null_value(), self.map, self.keyspace)
    }
    pub fn consistency(mut self, consistency: Consistency) -> BatchCollector<'a, S, BatchFlags> {
        Self::step(self.builder.consistency(consistency), self.map, self.keyspace)
    }
}

impl<'a, S: 'a + Keyspace> BatchCollector<'a, S, BatchFlags> {
    pub fn serial_consistency(mut self, consistency: Consistency) -> BatchCollector<'a, S, BatchTimestamp> {
        Self::step(self.builder.serial_consistency(consistency), self.map, self.keyspace)
    }
    pub fn timestamp(mut self, timestamp: i64) -> BatchCollector<'a, S, BatchBuild> {
        Self::step(self.builder.timestamp(timestamp), self.map, self.keyspace)
    }
    pub fn build(mut self, token: i64, compression: impl Compression) -> BatchRequest<'a, S> {
        BatchRequest {
            token,
            inner: self.builder.build(compression).0,
            keyspace: self.keyspace,
        }
    }
}

impl<'a, S: 'a + Keyspace> BatchCollector<'a, S, BatchTimestamp> {
    pub fn timestamp(mut self, timestamp: i64) -> BatchCollector<'a, S, BatchBuild> {
        Self::step(self.builder.timestamp(timestamp), self.map, self.keyspace)
    }
    pub fn build(mut self, token: i64, compression: impl Compression) -> BatchRequest<'a, S> {
        BatchRequest {
            token,
            inner: self.builder.build(compression).0,
            keyspace: self.keyspace,
        }
    }
}

impl<'a, S: 'a + Keyspace> BatchCollector<'a, S, BatchBuild> {
    pub fn build(mut self, token: i64, compression: impl Compression) -> BatchRequest<'a, S> {
        BatchRequest {
            token,
            inner: self.builder.build(compression).0,
            keyspace: self.keyspace,
        }
    }
}

impl<'a, T, S: 'a + Keyspace> BatchCollector<'a, S, T> {
    pub fn get_cql(&self, id: &[u8; 16]) -> Option<&std::borrow::Cow<'static, str>> {
        self.map.get(id)
    }

    fn step<N>(
        builder: BatchBuilder<N>,
        map: HashMap<[u8; 16], std::borrow::Cow<'static, str>>,
        keyspace: &'a S,
    ) -> BatchCollector<'a, S, N> {
        BatchCollector { builder, map, keyspace }
    }
}

pub trait Batch<'a> {
    fn batch(&'a self) -> BatchCollector<'a, Self, BatchType>
    where
        Self: Sized + Keyspace,
    {
        BatchCollector::new(self)
    }
}

impl<'a, S: 'a + Keyspace> Batch<'a> for S {}

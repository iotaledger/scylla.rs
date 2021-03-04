use super::*;
use scylla_cql::{
    compression::Compression, BatchBuild, BatchBuilder, BatchFlags, BatchStatementOrId, BatchTimestamp, BatchType,
    BatchTypes, BatchValues, ColumnEncoder, Consistency,
};
use std::collections::HashMap;

#[derive(Clone)]
pub struct BatchRequest<'a, S> {
    token: i64,
    inner: Vec<u8>,
    map: HashMap<[u8; 16], std::borrow::Cow<'static, str>>,
    keyspace: &'a S,
    _data: PhantomData<S>,
}

impl<'a, S: Keyspace> BatchRequest<'a, S> {
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

    pub fn batch_type(self, batch_type: BatchTypes) -> BatchCollector<'a, S, BatchStatementOrId> {
        Self::step(self.builder.batch_type(batch_type), self.map, self.keyspace)
    }
    pub fn logged(self) -> BatchCollector<'a, S, BatchStatementOrId> {
        Self::step(self.builder.logged(), self.map, self.keyspace)
    }
    pub fn unlogged(self) -> BatchCollector<'a, S, BatchStatementOrId> {
        Self::step(self.builder.unlogged(), self.map, self.keyspace)
    }
    pub fn counter(self) -> BatchCollector<'a, S, BatchStatementOrId> {
        Self::step(self.builder.counter(), self.map, self.keyspace)
    }
}

impl<'a, S: 'a + Keyspace> BatchCollector<'a, S, BatchStatementOrId> {
    pub fn insert_query<K, V>(self) -> BatchCollector<'a, S, BatchValues>
    where
        S: Insert<K, V>,
    {
        Self::step(
            self.builder.statement(self.keyspace.insert_statement().as_ref()),
            self.map,
            self.keyspace,
        )
    }

    pub fn insert_prepared<K, V>(mut self) -> BatchCollector<'a, S, BatchValues>
    where
        S: Insert<K, V>,
    {
        let id = self.keyspace.insert_id();
        self.map.insert(id, self.keyspace.insert_statement());
        Self::step(self.builder.id(&id), self.map, self.keyspace)
    }

    pub fn update_query<K, V>(self) -> BatchCollector<'a, S, BatchValues>
    where
        S: Update<K, V>,
    {
        Self::step(
            self.builder.statement(self.keyspace.update_statement().as_ref()),
            self.map,
            self.keyspace,
        )
    }

    pub fn update_prepared<K, V>(mut self) -> BatchCollector<'a, S, BatchValues>
    where
        S: Update<K, V>,
    {
        let id = self.keyspace.update_id();
        self.map.insert(id, self.keyspace.update_statement());
        Self::step(self.builder.id(&id), self.map, self.keyspace)
    }

    pub fn delete_query<K, V>(self) -> BatchCollector<'a, S, BatchValues>
    where
        S: Delete<K, V>,
    {
        Self::step(
            self.builder.statement(self.keyspace.delete_statement().as_ref()),
            self.map,
            self.keyspace,
        )
    }

    pub fn delete_prepared<K, V>(mut self) -> BatchCollector<'a, S, BatchValues>
    where
        S: Delete<K, V>,
    {
        let id = self.keyspace.delete_id();
        self.map.insert(id, self.keyspace.delete_statement());
        Self::step(self.builder.id(&id), self.map, self.keyspace)
    }
}

impl<'a, S: 'a + Keyspace> BatchCollector<'a, S, BatchValues> {
    pub fn insert_query<K, V>(self) -> Self
    where
        S: Insert<K, V>,
    {
        Self::step(
            self.builder.statement(self.keyspace.insert_statement().as_ref()),
            self.map,
            self.keyspace,
        )
    }

    pub fn insert_prepared<K, V>(mut self) -> Self
    where
        S: Insert<K, V>,
    {
        let id = self.keyspace.insert_id();
        self.map.insert(id, self.keyspace.insert_statement());
        Self::step(self.builder.id(&id), self.map, self.keyspace)
    }

    pub fn update_query<K, V>(self) -> Self
    where
        S: Update<K, V>,
    {
        Self::step(
            self.builder.statement(self.keyspace.update_statement().as_ref()),
            self.map,
            self.keyspace,
        )
    }

    pub fn update_prepared<K, V>(mut self) -> Self
    where
        S: Update<K, V>,
    {
        let id = self.keyspace.update_id();
        self.map.insert(id, self.keyspace.update_statement());
        Self::step(self.builder.id(&id), self.map, self.keyspace)
    }

    pub fn delete_query<K, V>(self) -> Self
    where
        S: Delete<K, V>,
    {
        Self::step(
            self.builder.statement(self.keyspace.delete_statement().as_ref()),
            self.map,
            self.keyspace,
        )
    }

    pub fn delete_prepared<K, V>(mut self) -> Self
    where
        S: Delete<K, V>,
    {
        let id = self.keyspace.delete_id();
        self.map.insert(id, self.keyspace.delete_statement());
        Self::step(self.builder.id(&id), self.map, self.keyspace)
    }

    pub fn value(self, value: impl ColumnEncoder) -> Self {
        Self::step(self.builder.value(value), self.map, self.keyspace)
    }
    pub fn unset_value(self) -> Self {
        Self::step(self.builder.unset_value(), self.map, self.keyspace)
    }
    pub fn null_value(self) -> Self {
        Self::step(self.builder.null_value(), self.map, self.keyspace)
    }
    pub fn consistency(self, consistency: Consistency) -> BatchCollector<'a, S, BatchFlags> {
        Self::step(self.builder.consistency(consistency), self.map, self.keyspace)
    }
}

impl<'a, S: 'a + Keyspace> BatchCollector<'a, S, BatchFlags> {
    pub fn serial_consistency(self, consistency: Consistency) -> BatchCollector<'a, S, BatchTimestamp> {
        Self::step(self.builder.serial_consistency(consistency), self.map, self.keyspace)
    }
    pub fn timestamp(self, timestamp: i64) -> BatchCollector<'a, S, BatchBuild> {
        Self::step(self.builder.timestamp(timestamp), self.map, self.keyspace)
    }
    pub fn build(self, token: i64, compression: impl Compression) -> BatchRequest<'a, S> {
        BatchRequest {
            token,
            map: self.map,
            inner: self.builder.build(compression).0.into(),
            keyspace: self.keyspace,
            _data: PhantomData,
        }
    }
}

impl<'a, S: 'a + Keyspace> BatchCollector<'a, S, BatchTimestamp> {
    pub fn timestamp(self, timestamp: i64) -> BatchCollector<'a, S, BatchBuild> {
        Self::step(self.builder.timestamp(timestamp), self.map, self.keyspace)
    }
    pub fn build(self, token: i64, compression: impl Compression) -> BatchRequest<'a, S> {
        BatchRequest {
            token,
            map: self.map,
            inner: self.builder.build(compression).0.into(),
            keyspace: self.keyspace,
            _data: PhantomData,
        }
    }
}

impl<'a, S: 'a + Keyspace> BatchCollector<'a, S, BatchBuild> {
    pub fn build(self, token: i64, compression: impl Compression) -> BatchRequest<'a, S> {
        BatchRequest {
            token,
            map: self.map,
            inner: self.builder.build(compression).0.into(),
            keyspace: self.keyspace,
            _data: PhantomData,
        }
    }
}

impl<'a, T, S: 'a + Keyspace> BatchCollector<'a, S, T> {
    fn step<N>(
        builder: BatchBuilder<N>,
        map: HashMap<[u8; 16], std::borrow::Cow<'static, str>>,
        keyspace: &'a S,
    ) -> BatchCollector<'a, S, N> {
        BatchCollector { builder, map, keyspace }
    }
}

/// Defines a helper method to allow keyspaces to begin constructing a batch
pub trait Batch<'a> {
    /// Start building a batch.
    /// This function will borrow the keyspace until the batch is fully built in order
    /// to access its trait definitions.
    fn batch(&'a self) -> BatchCollector<'a, Self, BatchType>
    where
        Self: Sized + Keyspace,
    {
        BatchCollector::new(self)
    }
}

impl<'a, S: 'a + Keyspace> Batch<'a> for S {}

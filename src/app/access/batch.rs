// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use super::{delete::DeleteRecommended, insert::InsertRecommended, update::UpdateRecommended, *};
use crate::cql::{
    BatchBuild, BatchBuilder, BatchFlags, BatchStatementOrId, BatchTimestamp, BatchType, BatchTypeCounter,
    BatchTypeLogged, BatchTypeUnlogged, BatchTypeUnset, BatchValues, Consistency,
};
use dyn_clone::DynClone;
use std::{any::Any, collections::HashMap, marker::PhantomData};

/// An aggregation trait which defines a statement marker of any type
pub trait AnyStatement<S>: Any + Statement<S> + Send + Sync + DynClone {}

dyn_clone::clone_trait_object!(<S> AnyStatement<S>);

/// A Batch request, which can be used to send queries to the Ring.
/// Stores a map of prepared statement IDs that were added to the
/// batch so that the associated statements can be re-prepared if necessary.
#[derive(Clone)]
pub struct BatchRequest<S> {
    token: i64,
    inner: Vec<u8>,
    map: HashMap<[u8; 16], Box<dyn AnyStatement<S>>>,
    keyspace: S,
}

/// A marker trait which holds dynamic types for a statement
/// to be retrieved from a keyspace
pub trait Statement<S> {
    /// Get the statement defined by this keyspace
    fn statement(&self, keyspace: &S) -> Cow<'static, str>;
}

/// A marker specifically for Insert statements
#[derive(Clone)]
pub struct InsertStatement<S, K, V> {
    _data: PhantomData<(S, K, V)>,
}

impl<S: Insert<K, V>, K, V> Statement<S> for InsertStatement<S, K, V> {
    fn statement(&self, keyspace: &S) -> Cow<'static, str> {
        keyspace.insert_statement::<K, V>()
    }
}

impl<S, K, V> AnyStatement<S> for InsertStatement<S, K, V>
where
    S: 'static + Insert<K, V> + Clone,
    K: 'static + Clone + Send + Sync,
    V: 'static + Clone + Send + Sync,
{
}

/// A marker specifically for Update statements
#[derive(Clone)]
pub struct UpdateStatement<S, K, V> {
    _data: PhantomData<(S, K, V)>,
}

impl<S: Update<K, V>, K, V> Statement<S> for UpdateStatement<S, K, V> {
    fn statement(&self, keyspace: &S) -> Cow<'static, str> {
        keyspace.update_statement::<K, V>()
    }
}

impl<S, K, V> AnyStatement<S> for UpdateStatement<S, K, V>
where
    S: 'static + Update<K, V> + Clone,
    K: 'static + Clone + Send + Sync,
    V: 'static + Clone + Send + Sync,
{
}

/// A marker specifically for Delete statements
#[derive(Clone)]
pub struct DeleteStatement<S, K, V> {
    _data: PhantomData<(S, K, V)>,
}

impl<S: Delete<K, V>, K, V> Statement<S> for DeleteStatement<S, K, V> {
    fn statement(&self, keyspace: &S) -> Cow<'static, str> {
        keyspace.delete_statement::<K, V>()
    }
}

impl<S, K, V> AnyStatement<S> for DeleteStatement<S, K, V>
where
    S: 'static + Delete<K, V> + Clone,
    K: 'static + Clone + Send + Sync,
    V: 'static + Clone + Send + Sync,
{
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

    /// Clone the cql map
    pub fn clone_map(&self) -> HashMap<[u8; 16], Box<dyn AnyStatement<S>>> {
        self.map.clone()
    }

    /// Take the cql map, leaving an empty map in the request
    pub fn take_map(&mut self) -> HashMap<[u8; 16], Box<dyn AnyStatement<S>>> {
        std::mem::take(&mut self.map)
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

    /// Get a statement given an id from the request's map
    pub fn get_statement(&self, id: &[u8; 16]) -> Option<Cow<'static, str>> {
        self.map.get(id).and_then(|res| Some(res.statement(&self.keyspace)))
    }

    /// Get the request payload
    pub fn payload(&self) -> &Vec<u8> {
        &self.inner
    }
}

/// A batch collector, used to collect statements and build a `BatchRequest`.
/// Access queries are defined by access traits ([`Insert`], [`Delete`], [`Update`])
/// and qualified for use in a Batch via batch traits ([`InsertBatch`], [`DeleteBatch`], [`UpdateBatch`])
/// ## Example
/// ```
/// # use scylla_rs::app::access::tests::MyKeyspace;
/// use scylla_rs::{
///     app::access::Batchable,
///     cql::{Batch, Consistency},
/// };
///
/// # let keyspace = MyKeyspace::new();
/// # let (my_key, my_val, token_key) = (1, 1.0, 1);
/// let req = keyspace
///     // Creates the `BatchCollector`
///     .batch()
///     .logged()
///     // Add a few pre-defined access queries
///     .delete::<_, f32>(&my_key)
///     .insert_query(&my_key, &my_val)
///     .update_prepared(&my_key, &my_val)
///     .consistency(Consistency::One)
///     .build()?
///     .compute_token(&token_key);
/// # Ok::<(), anyhow::Error>(())
/// ```
pub struct BatchCollector<S, Type: Copy + Into<u8>, Stage> {
    builder: BatchBuilder<Type, Stage>,
    map: HashMap<[u8; 16], Box<dyn AnyStatement<S>>>,
    keyspace: S,
}

impl<S: Keyspace + Clone> BatchCollector<S, BatchTypeUnset, BatchType> {
    /// Construct a new batch collector with a keyspace definition
    /// which should implement access and batch traits that will be used
    /// to build this batch. The keyspace will be cloned here and held by
    /// the collector.
    pub fn new(keyspace: &S) -> BatchCollector<S, BatchTypeUnset, BatchType> {
        BatchCollector {
            builder: crate::cql::Batch::new(),
            map: HashMap::new(),
            keyspace: keyspace.clone(),
        }
    }

    /// Construct a new batch collector with a provided capacity and a keyspace definition
    /// which should implement access and batch traits that will be used
    /// to build this batch. The keyspace will be cloned here and held by
    /// the collector.
    pub fn with_capacity(keyspace: &S, capacity: usize) -> BatchCollector<S, BatchTypeUnset, BatchType> {
        BatchCollector {
            builder: crate::cql::Batch::with_capacity(capacity),
            map: HashMap::new(),
            keyspace: keyspace.clone(),
        }
    }

    /// Specify the batch type using an enum
    pub fn batch_type<Type: Copy + Into<u8>>(self, batch_type: Type) -> BatchCollector<S, Type, BatchStatementOrId> {
        Self::step(self.builder.batch_type(batch_type), self.map, self.keyspace)
    }

    /// Specify the batch type as Logged
    pub fn logged(self) -> BatchCollector<S, BatchTypeLogged, BatchStatementOrId> {
        Self::step(self.builder.logged(), self.map, self.keyspace)
    }

    /// Specify the batch type as Unlogged
    pub fn unlogged(self) -> BatchCollector<S, BatchTypeUnlogged, BatchStatementOrId> {
        Self::step(self.builder.unlogged(), self.map, self.keyspace)
    }

    /// Specify the batch type as Counter
    pub fn counter(self) -> BatchCollector<S, BatchTypeCounter, BatchStatementOrId> {
        Self::step(self.builder.counter(), self.map, self.keyspace)
    }
}

impl<S: Keyspace, Type: Copy + Into<u8>> BatchCollector<S, Type, BatchStatementOrId> {
    /// Append an insert query using the default query type defined in the `InsertBatch` impl
    /// and the statement defined in the `Insert` impl.
    pub fn insert<K, V>(mut self, key: &K, value: &V) -> BatchCollector<S, Type, BatchValues>
    where
        S: 'static + Insert<K, V>,
        K: 'static + Clone + Send + Sync,
        V: 'static + Clone + Send + Sync,
    {
        // Add PreparedId to map if is_prepared
        if S::QueryOrPrepared::is_prepared() {
            let id = self.keyspace.insert_id();
            self.map.insert(
                id,
                Box::new(InsertStatement {
                    _data: PhantomData::<(S, K, V)>,
                }),
            );
        };

        // this will advnace the builder as defined in the Insert<K, V>
        let builder = S::QueryOrPrepared::make(self.builder, &self.keyspace);
        // bind_values of Insert<K, V>
        let builder = S::bind_values(builder, key, value);

        Self::step(builder, self.map, self.keyspace)
    }

    /// Append an unprepared insert query using the statement defined in the `Insert` impl.
    pub fn insert_query<K, V>(self, key: &K, value: &V) -> BatchCollector<S, Type, BatchValues>
    where
        S: Insert<K, V>,
    {
        // this will advnace the builder with QueryStatement
        let builder = <QueryStatement as InsertRecommended<S, K, V>>::make(self.builder, &self.keyspace);
        // bind_values of Insert<K, V>
        let builder = S::bind_values(builder, key, value);

        Self::step(builder, self.map, self.keyspace)
    }

    /// Append a prepared insert query using the statement defined in the `Insert` impl.
    pub fn insert_prepared<K, V>(mut self, key: &K, value: &V) -> BatchCollector<S, Type, BatchValues>
    where
        S: 'static + Insert<K, V>,
        K: 'static + Clone + Send + Sync,
        V: 'static + Clone + Send + Sync,
    {
        // Add PreparedId to map
        let id = self.keyspace.insert_id();
        self.map.insert(
            id,
            Box::new(InsertStatement {
                _data: PhantomData::<(S, K, V)>,
            }),
        );

        // this will advnace the builder with PreparedStatement
        let builder = <PreparedStatement as InsertRecommended<S, K, V>>::make(self.builder, &self.keyspace);
        // bind_values of Insert<K, V>
        let builder = S::bind_values(builder, key, value);

        Self::step(builder, self.map, self.keyspace)
    }

    /// Append an update query using the default query type defined in the `UpdateBatch` impl
    /// and the statement defined in the `Update` impl.
    pub fn update<K, V>(mut self, key: &K, value: &V) -> BatchCollector<S, Type, BatchValues>
    where
        S: 'static + Update<K, V>,
        K: 'static + Clone + Send + Sync,
        V: 'static + Clone + Send + Sync,
    {
        // Add PreparedId to map if is_prepared
        if S::QueryOrPrepared::is_prepared() {
            let id = self.keyspace.update_id();
            self.map.insert(
                id,
                Box::new(UpdateStatement {
                    _data: PhantomData::<(S, K, V)>,
                }),
            );
        };

        // this will advnace the builder as defined in the Update<K, V>
        let builder = S::QueryOrPrepared::make(self.builder, &self.keyspace);
        // bind_values of Update<K, V>
        let builder = S::bind_values(builder, key, value);

        Self::step(builder, self.map, self.keyspace)
    }

    /// Append an unprepared update query using the statement defined in the `Update` impl.
    pub fn update_query<K, V>(self, key: &K, value: &V) -> BatchCollector<S, Type, BatchValues>
    where
        S: Update<K, V>,
    {
        // this will advnace the builder with QueryStatement
        let builder = <QueryStatement as UpdateRecommended<S, K, V>>::make(self.builder, &self.keyspace);
        // bind_values of Update<K, V>
        let builder = S::bind_values(builder, key, value);

        Self::step(builder, self.map, self.keyspace)
    }

    /// Append a prepared update query using the statement defined in the `Update` impl.
    pub fn update_prepared<K, V>(mut self, key: &K, value: &V) -> BatchCollector<S, Type, BatchValues>
    where
        S: 'static + Update<K, V>,
        K: 'static + Clone + Send + Sync,
        V: 'static + Clone + Send + Sync,
    {
        // Add PreparedId to map
        let id = self.keyspace.update_id();
        self.map.insert(
            id,
            Box::new(UpdateStatement {
                _data: PhantomData::<(S, K, V)>,
            }),
        );

        // this will advnace the builder with PreparedStatement
        let builder = <PreparedStatement as UpdateRecommended<S, K, V>>::make(self.builder, &self.keyspace);
        // bind_values of Update<K, V>
        let builder = S::bind_values(builder, key, value);

        Self::step(builder, self.map, self.keyspace)
    }

    /// Append a delete query using the default query type defined in the `DeleteBatch` impl
    /// and the statement defined in the `Delete` impl.
    pub fn delete<K, V>(mut self, key: &K) -> BatchCollector<S, Type, BatchValues>
    where
        S: 'static + Delete<K, V>,
        K: 'static + Clone + Send + Sync,
        V: 'static + Clone + Send + Sync,
    {
        // Add PreparedId to map if is_prepared
        if S::QueryOrPrepared::is_prepared() {
            let id = self.keyspace.delete_id();
            self.map.insert(
                id,
                Box::new(DeleteStatement {
                    _data: PhantomData::<(S, K, V)>,
                }),
            );
        };

        // this will advnace the builder as defined in the Delete<K, V>
        let builder = S::QueryOrPrepared::make(self.builder, &self.keyspace);
        // bind_values of Delete<K, V>
        let builder = S::bind_values(builder, key);

        Self::step(builder, self.map, self.keyspace)
    }

    /// Append an unprepared delete query using the statement defined in the `Delete` impl.
    pub fn delete_query<K, V>(self, key: &K) -> BatchCollector<S, Type, BatchValues>
    where
        S: Delete<K, V>,
    {
        // this will advnace the builder with QueryStatement
        let builder = <QueryStatement as DeleteRecommended<S, K, V>>::make(self.builder, &self.keyspace);
        // bind_values of Delete<K, V>
        let builder = S::bind_values(builder, key);

        Self::step(builder, self.map, self.keyspace)
    }

    /// Append a prepared delete query using the statement defined in the `Delete` impl.
    pub fn delete_prepared<K, V>(mut self, key: &K) -> BatchCollector<S, Type, BatchValues>
    where
        S: 'static + Delete<K, V>,
        K: 'static + Clone + Send + Sync,
        V: 'static + Clone + Send + Sync,
    {
        // Add PreparedId to map
        let id = self.keyspace.delete_id();
        self.map.insert(
            id,
            Box::new(DeleteStatement {
                _data: PhantomData::<(S, K, V)>,
            }),
        );

        // this will advnace the builder with PreparedStatement
        let builder = <PreparedStatement as DeleteRecommended<S, K, V>>::make(self.builder, &self.keyspace);
        // bind_values of Delete<K, V>
        let builder = S::bind_values(builder, key);

        Self::step(builder, self.map, self.keyspace)
    }
}

impl<S: Keyspace, Type: Copy + Into<u8>> BatchCollector<S, Type, BatchValues> {
    /// Append an insert query using the default query type defined in the `InsertBatch` impl
    /// and the statement defined in the `Insert` impl.
    pub fn insert<K, V>(mut self, key: &K, value: &V) -> BatchCollector<S, Type, BatchValues>
    where
        S: 'static + Insert<K, V>,
        K: 'static + Clone + Send + Sync,
        V: 'static + Clone + Send + Sync,
    {
        // Add PreparedId to map if is_prepared
        if S::QueryOrPrepared::is_prepared() {
            let id = self.keyspace.insert_id();
            self.map.insert(
                id,
                Box::new(InsertStatement {
                    _data: PhantomData::<(S, K, V)>,
                }),
            );
        };

        // this will advnace the builder as defined in the Insert<K, V>
        let builder = S::QueryOrPrepared::make(self.builder, &self.keyspace);
        // bind_values of Insert<K, V>
        let builder = S::bind_values(builder, key, value);

        Self::step(builder, self.map, self.keyspace)
    }

    /// Append an unprepared insert query using the statement defined in the `Insert` impl.
    pub fn insert_query<K, V>(self, key: &K, value: &V) -> BatchCollector<S, Type, BatchValues>
    where
        S: Insert<K, V>,
    {
        // this will advnace the builder with QueryStatement
        let builder = <QueryStatement as InsertRecommended<S, K, V>>::make(self.builder, &self.keyspace);
        // bind_values of Insert<K, V>
        let builder = S::bind_values(builder, key, value);

        Self::step(builder, self.map, self.keyspace)
    }

    /// Append a prepared insert query using the statement defined in the `Insert` impl.
    pub fn insert_prepared<K, V>(mut self, key: &K, value: &V) -> BatchCollector<S, Type, BatchValues>
    where
        S: 'static + Insert<K, V>,
        K: 'static + Clone + Send + Sync,
        V: 'static + Clone + Send + Sync,
    {
        // Add PreparedId to map
        let id = self.keyspace.insert_id();
        self.map.insert(
            id,
            Box::new(InsertStatement {
                _data: PhantomData::<(S, K, V)>,
            }),
        );

        // this will advnace the builder with PreparedStatement
        let builder = <PreparedStatement as InsertRecommended<S, K, V>>::make(self.builder, &self.keyspace);
        // bind_values of Insert<K, V>
        let builder = S::bind_values(builder, key, value);

        Self::step(builder, self.map, self.keyspace)
    }

    /// Append an update query using the default query type defined in the `UpdateBatch` impl
    /// and the statement defined in the `Update` impl.
    pub fn update<K, V>(mut self, key: &K, value: &V) -> BatchCollector<S, Type, BatchValues>
    where
        S: 'static + Update<K, V>,
        K: 'static + Clone + Send + Sync,
        V: 'static + Clone + Send + Sync,
    {
        // Add PreparedId to map if is_prepared
        if S::QueryOrPrepared::is_prepared() {
            let id = self.keyspace.update_id();
            self.map.insert(
                id,
                Box::new(UpdateStatement {
                    _data: PhantomData::<(S, K, V)>,
                }),
            );
        };

        // this will advnace the builder as defined in the Update<K, V>
        let builder = S::QueryOrPrepared::make(self.builder, &self.keyspace);
        // bind_values of Update<K, V>
        let builder = S::bind_values(builder, key, value);

        Self::step(builder, self.map, self.keyspace)
    }

    /// Append an unprepared update query using the statement defined in the `Update` impl.
    pub fn update_query<K, V>(self, key: &K, value: &V) -> BatchCollector<S, Type, BatchValues>
    where
        S: Update<K, V>,
    {
        // this will advnace the builder with QueryStatement
        let builder = <QueryStatement as UpdateRecommended<S, K, V>>::make(self.builder, &self.keyspace);
        // bind_values of Update<K, V>
        let builder = S::bind_values(builder, key, value);

        Self::step(builder, self.map, self.keyspace)
    }

    /// Append a prepared update query using the statement defined in the `Update` impl.
    pub fn update_prepared<K, V>(mut self, key: &K, value: &V) -> BatchCollector<S, Type, BatchValues>
    where
        S: 'static + Update<K, V>,
        K: 'static + Clone + Send + Sync,
        V: 'static + Clone + Send + Sync,
    {
        // Add PreparedId to map
        let id = self.keyspace.update_id();
        self.map.insert(
            id,
            Box::new(UpdateStatement {
                _data: PhantomData::<(S, K, V)>,
            }),
        );

        // this will advnace the builder with PreparedStatement
        let builder = <PreparedStatement as UpdateRecommended<S, K, V>>::make(self.builder, &self.keyspace);
        // bind_values of Update<K, V>
        let builder = S::bind_values(builder, key, value);

        Self::step(builder, self.map, self.keyspace)
    }

    /// Append a delete query using the default query type defined in the `DeleteBatch` impl
    /// and the statement defined in the `Delete` impl.
    pub fn delete<K, V>(mut self, key: &K) -> BatchCollector<S, Type, BatchValues>
    where
        S: 'static + Delete<K, V>,
        K: 'static + Clone + Send + Sync,
        V: 'static + Clone + Send + Sync,
    {
        // Add PreparedId to map if is_prepared
        if S::QueryOrPrepared::is_prepared() {
            let id = self.keyspace.delete_id();
            self.map.insert(
                id,
                Box::new(DeleteStatement {
                    _data: PhantomData::<(S, K, V)>,
                }),
            );
        };

        // this will advnace the builder as defined in the Delete<K, V>
        let builder = S::QueryOrPrepared::make(self.builder, &self.keyspace);
        // bind_values of Delete<K, V>
        let builder = S::bind_values(builder, key);

        Self::step(builder, self.map, self.keyspace)
    }

    /// Append an unprepared delete query using the statement defined in the `Delete` impl.
    pub fn delete_query<K, V>(self, key: &K) -> BatchCollector<S, Type, BatchValues>
    where
        S: Delete<K, V>,
    {
        // this will advnace the builder with QueryStatement
        let builder = <QueryStatement as DeleteRecommended<S, K, V>>::make(self.builder, &self.keyspace);
        // bind_values of Delete<K, V>
        let builder = S::bind_values(builder, key);

        Self::step(builder, self.map, self.keyspace)
    }

    /// Append a prepared delete query using the statement defined in the `Delete` impl.
    pub fn delete_prepared<K, V>(mut self, key: &K) -> BatchCollector<S, Type, BatchValues>
    where
        S: 'static + Delete<K, V>,
        K: 'static + Clone + Send + Sync,
        V: 'static + Clone + Send + Sync,
    {
        // Add PreparedId to map
        let id = self.keyspace.delete_id();
        self.map.insert(
            id,
            Box::new(DeleteStatement {
                _data: PhantomData::<(S, K, V)>,
            }),
        );

        // this will advnace the builder with PreparedStatement
        let builder = <PreparedStatement as DeleteRecommended<S, K, V>>::make(self.builder, &self.keyspace);
        // bind_values of Delete<K, V>
        let builder = S::bind_values(builder, key);

        Self::step(builder, self.map, self.keyspace)
    }

    /// Set the consistency for this batch
    pub fn consistency(self, consistency: Consistency) -> BatchCollector<S, Type, BatchFlags> {
        Self::step(self.builder.consistency(consistency), self.map, self.keyspace)
    }
}

impl<S: Keyspace, Type: Copy + Into<u8>> BatchCollector<S, Type, BatchFlags> {
    /// Set the serial consistency for the batch
    pub fn serial_consistency(self, consistency: Consistency) -> BatchCollector<S, Type, BatchTimestamp> {
        Self::step(self.builder.serial_consistency(consistency), self.map, self.keyspace)
    }
    /// Set the timestamp for the batch
    pub fn timestamp(self, timestamp: i64) -> BatchCollector<S, Type, BatchBuild> {
        Self::step(self.builder.timestamp(timestamp), self.map, self.keyspace)
    }
    /// Build the batch request using the current collector
    pub fn build(self) -> anyhow::Result<BatchRequest<S>> {
        Ok(BatchRequest {
            token: rand::random::<i64>(),
            map: self.map,
            inner: self.builder.build()?.0.into(),
            keyspace: self.keyspace,
        })
    }
}

impl<S: Keyspace, Type: Copy + Into<u8>> BatchCollector<S, Type, BatchTimestamp> {
    /// Set the timestamp for the batch
    pub fn timestamp(self, timestamp: i64) -> BatchCollector<S, Type, BatchBuild> {
        Self::step(self.builder.timestamp(timestamp), self.map, self.keyspace)
    }
    /// Build the batch request using the current collector
    pub fn build(self) -> anyhow::Result<BatchRequest<S>> {
        Ok(BatchRequest {
            token: rand::random::<i64>(),
            map: self.map,
            inner: self.builder.build()?.0.into(),
            keyspace: self.keyspace,
        })
    }
}

impl<S: Keyspace, Type: Copy + Into<u8>> BatchCollector<S, Type, BatchBuild> {
    /// Build the batch request using the current collector
    pub fn build(self) -> anyhow::Result<BatchRequest<S>> {
        Ok(BatchRequest {
            token: rand::random::<i64>(),
            map: self.map,
            inner: self.builder.build()?.0.into(),
            keyspace: self.keyspace,
        })
    }
}

impl<S: Keyspace, Type: Copy + Into<u8>, Stage> BatchCollector<S, Type, Stage> {
    fn step<NextType: Copy + Into<u8>, NextStage>(
        builder: BatchBuilder<NextType, NextStage>,
        map: HashMap<[u8; 16], Box<dyn AnyStatement<S>>>,
        keyspace: S,
    ) -> BatchCollector<S, NextType, NextStage> {
        BatchCollector { builder, map, keyspace }
    }
}

/// Defines a helper method to allow keyspaces to begin constructing a batch
pub trait Batchable {
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

impl<S: Keyspace + Clone> Batchable for S {}

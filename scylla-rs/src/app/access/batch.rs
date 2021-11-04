// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use super::*;
use crate::cql::{
    BatchBuild,
    BatchBuilder,
    BatchFlags,
    BatchStatementOrId,
    BatchTimestamp,
    BatchType,
    BatchTypeCounter,
    BatchTypeLogged,
    BatchTypeUnlogged,
    BatchTypeUnset,
    BatchValues,
    Consistency,
};
use std::collections::HashMap;

/// A batch collector, used to collect statements and build a `BatchRequest`.
/// Access queries are defined by access traits ([`Insert`], [`Delete`], [`Update`])
/// and qualified for use in a Batch via batch traits ([`InsertBatch`], [`DeleteBatch`], [`UpdateBatch`])
/// ## Example
/// ```no_run
/// # use scylla_rs::app::access::tests::MyKeyspace;
/// use scylla_rs::{
///     app::access::Batchable,
///     cql::{
///         Batch,
///         Consistency,
///     },
/// };
///
/// # let keyspace = MyKeyspace::new();
/// # let (my_key, my_val, token_key) = (1, 1.0, 1);
/// let req = keyspace
///     // Creates the `BatchCollector`
///     .batch()
///     .logged()
///     // Add a few pre-defined access queries
///     .delete::<_, _, f32>(&my_key, &())
///     .insert_query(&my_key, &my_val)
///     .update_prepared(&my_key, &(), &my_val)
///     .consistency(Consistency::One)
///     .build()?
///     .compute_token(&token_key);
/// # Ok::<(), anyhow::Error>(())
/// ```
pub struct BatchCollector<'a, S, Type: Copy + Into<u8>, Stage: Copy> {
    builder: BatchBuilder<Type, Stage>,
    map: HashMap<[u8; 16], Box<dyn ToStatement>>,
    keyspace: &'a S,
}

impl<'a, S: Keyspace + Clone> BatchCollector<'a, S, BatchTypeUnset, BatchType> {
    /// Construct a new batch collector with a keyspace definition
    /// which should implement access and batch traits that will be used
    /// to build this batch. The keyspace will be cloned here and held by
    /// the collector.
    pub fn new(keyspace: &S) -> BatchCollector<S, BatchTypeUnset, BatchType> {
        BatchCollector {
            builder: crate::cql::Batch::new(),
            map: HashMap::new(),
            keyspace,
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
            keyspace,
        }
    }

    /// Specify the batch type using an enum
    pub fn batch_type<Type: Copy + Into<u8>>(
        self,
        batch_type: Type,
    ) -> BatchCollector<'a, S, Type, BatchStatementOrId> {
        Self::step(self.builder.batch_type(batch_type), self.map, self.keyspace)
    }

    /// Specify the batch type as Logged
    pub fn logged(self) -> BatchCollector<'a, S, BatchTypeLogged, BatchStatementOrId> {
        Self::step(self.builder.logged(), self.map, self.keyspace)
    }

    /// Specify the batch type as Unlogged
    pub fn unlogged(self) -> BatchCollector<'a, S, BatchTypeUnlogged, BatchStatementOrId> {
        Self::step(self.builder.unlogged(), self.map, self.keyspace)
    }

    /// Specify the batch type as Counter
    pub fn counter(self) -> BatchCollector<'a, S, BatchTypeCounter, BatchStatementOrId> {
        Self::step(self.builder.counter(), self.map, self.keyspace)
    }
}

impl<'a, S: Keyspace, Type: Copy + Into<u8>> BatchCollector<'a, S, Type, BatchStatementOrId> {
    /// Add a dynamic query to the batch
    pub fn execute_query<T: ToStatement>(
        self,
        statement: &T,
        variables: &[&dyn BindableValue<BatchBuilder<Type, BatchValues>>],
    ) -> BatchCollector<'a, S, Type, BatchValues>
    where
        S: Keyspace,
    {
        let statement = statement.to_statement();
        let mut builder =
            QueryStatement::encode_statement(self.builder, &self.keyspace.replace_keyspace_token(&statement));
        builder = if variables.len() > 0 {
            builder.bind(variables)
        } else {
            builder
        };
        Self::step(builder, self.map, self.keyspace)
    }

    /// Add a dynamic prepared statement to the batch
    pub fn execute_prepared<T: ToStatement>(
        mut self,
        statement: &T,
        variables: &[&dyn BindableValue<BatchBuilder<Type, BatchValues>>],
    ) -> BatchCollector<'a, S, Type, BatchValues>
    where
        S: Keyspace,
    {
        let statement = self.keyspace.replace_keyspace_token(&statement.to_statement());
        let mut builder = PreparedStatement::encode_statement(self.builder, &statement);
        builder = if variables.len() > 0 {
            builder.bind(variables)
        } else {
            builder
        };
        self.map.insert(md5::compute(&statement).into(), Box::new(statement));
        Self::step(builder, self.map, self.keyspace)
    }
    /// Append an insert query using the default query type defined in the `InsertBatch` impl
    /// and the statement defined in the `Insert` impl.
    pub fn insert<K, V>(mut self, key: &K, value: &V) -> BatchCollector<'a, S, Type, BatchValues>
    where
        S: 'static + Insert<K, V> + std::fmt::Debug,
        K: 'static + Clone + Send + std::fmt::Debug,
        V: 'static + Clone + Send + std::fmt::Debug,
    {
        // Add PreparedId to map if is_prepared
        if S::QueryOrPrepared::is_prepared() {
            let id = self.keyspace.insert_id();
            self.map.insert(id, Box::new(InsertStatement::new(self.keyspace)));
        };

        // this will advnace the builder as defined in the Insert<K, V>
        let builder = S::QueryOrPrepared::encode_statement(self.builder, &self.keyspace.statement());
        // bind_values of Insert<K, V>
        let builder = S::bind_values(builder, key, value);

        Self::step(builder, self.map, self.keyspace)
    }

    /// Append an unprepared insert query using the statement defined in the `Insert` impl.
    pub fn insert_query<K, V>(self, key: &K, value: &V) -> BatchCollector<'a, S, Type, BatchValues>
    where
        S: Insert<K, V>,
    {
        // this will advnace the builder with QueryStatement
        let builder = QueryStatement::encode_statement(self.builder, &self.keyspace.statement());
        // bind_values of Insert<K, V>
        let builder = S::bind_values(builder, key, value);

        Self::step(builder, self.map, self.keyspace)
    }

    /// Append a prepared insert query using the statement defined in the `Insert` impl.
    pub fn insert_prepared<K, V>(mut self, key: &K, value: &V) -> BatchCollector<'a, S, Type, BatchValues>
    where
        S: 'static + Insert<K, V> + std::fmt::Debug,
        K: 'static + Clone + Send + std::fmt::Debug,
        V: 'static + Clone + Send + std::fmt::Debug,
    {
        // Add PreparedId to map
        let id = self.keyspace.insert_id();
        self.map.insert(id, Box::new(InsertStatement::new(self.keyspace)));

        // this will advnace the builder with PreparedStatement
        let builder = PreparedStatement::encode_statement(self.builder, &self.keyspace.statement());
        // bind_values of Insert<K, V>
        let builder = S::bind_values(builder, key, value);

        Self::step(builder, self.map, self.keyspace)
    }

    /// Append an update query using the default query type defined in the `UpdateBatch` impl
    /// and the statement defined in the `Update` impl.
    pub fn update<K, V, I>(mut self, key: &K, variables: &V, values: &I) -> BatchCollector<'a, S, Type, BatchValues>
    where
        S: 'static + Update<K, V, I> + std::fmt::Debug,
        K: 'static + Clone + Send + std::fmt::Debug,
        V: 'static + Clone + Send + std::fmt::Debug,
        I: 'static,
    {
        // Add PreparedId to map if is_prepared
        if S::QueryOrPrepared::is_prepared() {
            let id = self.keyspace.update_id();
            self.map.insert(id, Box::new(UpdateStatement::new(self.keyspace)));
        };

        // this will advnace the builder as defined in the Update<K, V>
        let builder = S::QueryOrPrepared::encode_statement(self.builder, &self.keyspace.statement());
        // bind_values of Update<K, V>
        let builder = S::bind_values(builder, key, variables, values);

        Self::step(builder, self.map, self.keyspace)
    }

    /// Append an unprepared update query using the statement defined in the `Update` impl.
    pub fn update_query<K, V, I>(self, key: &K, variables: &V, values: &I) -> BatchCollector<'a, S, Type, BatchValues>
    where
        S: Update<K, V, I>,
    {
        // this will advnace the builder with QueryStatement
        let builder = QueryStatement::encode_statement(self.builder, &self.keyspace.statement());
        // bind_values of Update<K, V>
        let builder = S::bind_values(builder, key, variables, values);

        Self::step(builder, self.map, self.keyspace)
    }

    /// Append a prepared update query using the statement defined in the `Update` impl.
    pub fn update_prepared<K, V, I>(
        mut self,
        key: &K,
        variables: &V,
        values: &I,
    ) -> BatchCollector<'a, S, Type, BatchValues>
    where
        S: 'static + Update<K, V, I> + std::fmt::Debug,
        K: 'static + Clone + Send + std::fmt::Debug,
        V: 'static + Clone + Send + std::fmt::Debug,
        I: 'static,
    {
        // Add PreparedId to map
        let id = self.keyspace.update_id();
        self.map.insert(id, Box::new(UpdateStatement::new(self.keyspace)));

        // this will advnace the builder with PreparedStatement
        let builder = PreparedStatement::encode_statement(self.builder, &self.keyspace.statement());
        // bind_values of Update<K, V>
        let builder = S::bind_values(builder, key, variables, values);

        Self::step(builder, self.map, self.keyspace)
    }

    /// Append a delete query using the default query type defined in the `DeleteBatch` impl
    /// and the statement defined in the `Delete` impl.
    pub fn delete<K, V, D>(mut self, key: &K, variables: &V) -> BatchCollector<'a, S, Type, BatchValues>
    where
        S: 'static + Delete<K, V, D> + std::fmt::Debug,
        K: 'static + Clone + Send + std::fmt::Debug,
        V: 'static + Clone + Send + std::fmt::Debug,
        D: 'static,
    {
        // Add PreparedId to map if is_prepared
        if S::QueryOrPrepared::is_prepared() {
            let id = self.keyspace.delete_id();
            self.map.insert(id, Box::new(DeleteStatement::new(self.keyspace)));
        };

        // this will advnace the builder as defined in the Delete<K, V>
        let builder = S::QueryOrPrepared::encode_statement(self.builder, &self.keyspace.statement());
        // bind_values of Delete<K, V>
        let builder = S::bind_values(builder, key, variables);

        Self::step(builder, self.map, self.keyspace)
    }

    /// Append an unprepared delete query using the statement defined in the `Delete` impl.
    pub fn delete_query<K, V, D>(self, key: &K, variables: &V) -> BatchCollector<'a, S, Type, BatchValues>
    where
        S: Delete<K, V, D>,
    {
        // this will advnace the builder with QueryStatement
        let builder = QueryStatement::encode_statement(self.builder, &self.keyspace.statement());
        // bind_values of Delete<K, V>
        let builder = S::bind_values(builder, key, variables);

        Self::step(builder, self.map, self.keyspace)
    }

    /// Append a prepared delete query using the statement defined in the `Delete` impl.
    pub fn delete_prepared<K, V, D>(mut self, key: &K, variables: &V) -> BatchCollector<'a, S, Type, BatchValues>
    where
        S: 'static + Delete<K, V, D> + std::fmt::Debug,
        K: 'static + Clone + Send + std::fmt::Debug,
        V: 'static + Clone + Send + std::fmt::Debug,
        D: 'static,
    {
        // Add PreparedId to map
        let id = self.keyspace.delete_id();
        self.map.insert(id, Box::new(DeleteStatement::new(self.keyspace)));

        // this will advnace the builder with PreparedStatement
        let builder = PreparedStatement::encode_statement(self.builder, &self.keyspace.statement());
        // bind_values of Delete<K, V>
        let builder = S::bind_values(builder, key, variables);

        Self::step(builder, self.map, self.keyspace)
    }
}

impl<'a, S: Keyspace, Type: Copy + Into<u8>> BatchCollector<'a, S, Type, BatchValues> {
    /// Append an insert query using the default query type defined in the `InsertBatch` impl
    /// and the statement defined in the `Insert` impl.
    pub fn insert<K, V>(mut self, key: &K, value: &V) -> BatchCollector<'a, S, Type, BatchValues>
    where
        S: 'static + Insert<K, V> + std::fmt::Debug,
        K: 'static + Clone + Send + std::fmt::Debug,
        V: 'static + Clone + Send + std::fmt::Debug,
    {
        // Add PreparedId to map if is_prepared
        if S::QueryOrPrepared::is_prepared() {
            let id = self.keyspace.insert_id();
            self.map.insert(id, Box::new(InsertStatement::new(self.keyspace)));
        };

        // this will advnace the builder as defined in the Insert<K, V>
        let builder = S::QueryOrPrepared::encode_statement(self.builder, &self.keyspace.statement());
        // bind_values of Insert<K, V>
        let builder = S::bind_values(builder, key, value);

        Self::step(builder, self.map, self.keyspace)
    }

    /// Append an unprepared insert query using the statement defined in the `Insert` impl.
    pub fn insert_query<K, V>(self, key: &K, value: &V) -> BatchCollector<'a, S, Type, BatchValues>
    where
        S: Insert<K, V>,
    {
        // this will advnace the builder with QueryStatement
        let builder = QueryStatement::encode_statement(self.builder, &self.keyspace.statement());
        // bind_values of Insert<K, V>
        let builder = S::bind_values(builder, key, value);

        Self::step(builder, self.map, self.keyspace)
    }

    /// Append a prepared insert query using the statement defined in the `Insert` impl.
    pub fn insert_prepared<K, V>(mut self, key: &K, value: &V) -> BatchCollector<'a, S, Type, BatchValues>
    where
        S: 'static + Insert<K, V> + std::fmt::Debug,
        K: 'static + Clone + Send + std::fmt::Debug,
        V: 'static + Clone + Send + std::fmt::Debug,
    {
        // Add PreparedId to map
        let id = self.keyspace.insert_id();
        self.map.insert(id, Box::new(InsertStatement::new(self.keyspace)));

        // this will advnace the builder with PreparedStatement
        let builder = PreparedStatement::encode_statement(self.builder, &self.keyspace.statement());
        // bind_values of Insert<K, V>
        let builder = S::bind_values(builder, key, value);

        Self::step(builder, self.map, self.keyspace)
    }

    /// Append an update query using the default query type defined in the `UpdateBatch` impl
    /// and the statement defined in the `Update` impl.
    pub fn update<K, V, I>(mut self, key: &K, variables: &V, values: &I) -> BatchCollector<'a, S, Type, BatchValues>
    where
        S: 'static + Update<K, V, I> + std::fmt::Debug,
        K: 'static + Clone + Send + std::fmt::Debug,
        V: 'static + Clone + Send + std::fmt::Debug,
        I: 'static,
    {
        // Add PreparedId to map if is_prepared
        if S::QueryOrPrepared::is_prepared() {
            let id = self.keyspace.update_id();
            self.map.insert(id, Box::new(UpdateStatement::new(self.keyspace)));
        };

        // this will advnace the builder as defined in the Update<K, V>
        let builder = S::QueryOrPrepared::encode_statement(self.builder, &self.keyspace.statement());
        // bind_values of Update<K, V>
        let builder = S::bind_values(builder, key, variables, values);

        Self::step(builder, self.map, self.keyspace)
    }

    /// Append an unprepared update query using the statement defined in the `Update` impl.
    pub fn update_query<K, V, I>(self, key: &K, variables: &V, values: &I) -> BatchCollector<'a, S, Type, BatchValues>
    where
        S: Update<K, V, I>,
    {
        // this will advnace the builder with QueryStatement
        let builder = QueryStatement::encode_statement(self.builder, &self.keyspace.statement());
        // bind_values of Update<K, V>
        let builder = S::bind_values(builder, key, variables, values);

        Self::step(builder, self.map, self.keyspace)
    }

    /// Append a prepared update query using the statement defined in the `Update` impl.
    pub fn update_prepared<K, V, I>(
        mut self,
        key: &K,
        variables: &V,
        values: &I,
    ) -> BatchCollector<'a, S, Type, BatchValues>
    where
        S: 'static + Update<K, V, I> + std::fmt::Debug,
        K: 'static + Clone + Send + std::fmt::Debug,
        V: 'static + Clone + Send + std::fmt::Debug,
        I: 'static,
    {
        // Add PreparedId to map
        let id = self.keyspace.update_id();
        self.map.insert(id, Box::new(UpdateStatement::new(self.keyspace)));

        // this will advnace the builder with PreparedStatement
        let builder = PreparedStatement::encode_statement(self.builder, &self.keyspace.statement());
        // bind_values of Update<K, V>
        let builder = S::bind_values(builder, key, variables, values);

        Self::step(builder, self.map, self.keyspace)
    }

    /// Append a delete query using the default query type defined in the `DeleteBatch` impl
    /// and the statement defined in the `Delete` impl.
    pub fn delete<K, V, D>(mut self, key: &K, variables: &V) -> BatchCollector<'a, S, Type, BatchValues>
    where
        S: 'static + Delete<K, V, D> + std::fmt::Debug,
        K: 'static + Clone + Send + std::fmt::Debug,
        V: 'static + Clone + Send + std::fmt::Debug,
        D: 'static,
    {
        // Add PreparedId to map if is_prepared
        if S::QueryOrPrepared::is_prepared() {
            let id = self.keyspace.delete_id();
            self.map.insert(id, Box::new(DeleteStatement::new(self.keyspace)));
        };

        // this will advnace the builder as defined in the Delete<K, V>
        let builder = S::QueryOrPrepared::encode_statement(self.builder, &self.keyspace.statement());
        // bind_values of Delete<K, V>
        let builder = S::bind_values(builder, key, variables);

        Self::step(builder, self.map, self.keyspace)
    }

    /// Append an unprepared delete query using the statement defined in the `Delete` impl.
    pub fn delete_query<K, V, D>(self, key: &K, variables: &V) -> BatchCollector<'a, S, Type, BatchValues>
    where
        S: Delete<K, V, D>,
    {
        // this will advnace the builder with QueryStatement
        let builder = QueryStatement::encode_statement(self.builder, &self.keyspace.statement());
        // bind_values of Delete<K, V>
        let builder = S::bind_values(builder, key, variables);

        Self::step(builder, self.map, self.keyspace)
    }

    /// Append a prepared delete query using the statement defined in the `Delete` impl.
    pub fn delete_prepared<K, V, D>(mut self, key: &K, variables: &V) -> BatchCollector<'a, S, Type, BatchValues>
    where
        S: 'static + Delete<K, V, D> + std::fmt::Debug,
        K: 'static + Clone + Send + std::fmt::Debug,
        V: 'static + Clone + Send + std::fmt::Debug,
        D: 'static,
    {
        // Add PreparedId to map
        let id = self.keyspace.delete_id();
        self.map.insert(id, Box::new(DeleteStatement::new(self.keyspace)));

        // this will advnace the builder with PreparedStatement
        let builder = PreparedStatement::encode_statement(self.builder, &self.keyspace.statement());
        // bind_values of Delete<K, V>
        let builder = S::bind_values(builder, key, variables);

        Self::step(builder, self.map, self.keyspace)
    }

    /// Set the consistency for this batch
    pub fn consistency(self, consistency: Consistency) -> BatchCollector<'a, S, Type, BatchFlags> {
        Self::step(self.builder.consistency(consistency), self.map, self.keyspace)
    }

    /// Set the serial consistency for the batch
    pub fn serial_consistency(self, consistency: Consistency) -> BatchCollector<'a, S, Type, BatchTimestamp> {
        Self::step(
            self.builder
                .consistency(Consistency::Quorum)
                .serial_consistency(consistency),
            self.map,
            self.keyspace,
        )
    }
    /// Set the timestamp for the batch
    pub fn timestamp(self, timestamp: i64) -> BatchCollector<'a, S, Type, BatchBuild> {
        Self::step(
            self.builder.consistency(Consistency::Quorum).timestamp(timestamp),
            self.map,
            self.keyspace,
        )
    }
    /// Build the batch request using the current collector
    pub fn build(self) -> anyhow::Result<BatchRequest> {
        Ok(BatchRequest {
            token: rand::random(),
            map: self.map,
            payload: self.builder.consistency(Consistency::Quorum).build()?.0.into(),
        })
    }
}

impl<'a, S: Keyspace, Type: Copy + Into<u8>> BatchCollector<'a, S, Type, BatchFlags> {
    /// Set the serial consistency for the batch
    pub fn serial_consistency(self, consistency: Consistency) -> BatchCollector<'a, S, Type, BatchTimestamp> {
        Self::step(self.builder.serial_consistency(consistency), self.map, self.keyspace)
    }
    /// Set the timestamp for the batch
    pub fn timestamp(self, timestamp: i64) -> BatchCollector<'a, S, Type, BatchBuild> {
        Self::step(self.builder.timestamp(timestamp), self.map, self.keyspace)
    }
    /// Build the batch request using the current collector
    pub fn build(self) -> anyhow::Result<BatchRequest> {
        Ok(BatchRequest {
            token: rand::random(),
            map: self.map,
            payload: self.builder.build()?.0.into(),
        })
    }
}

impl<'a, S: Keyspace, Type: Copy + Into<u8>> BatchCollector<'a, S, Type, BatchTimestamp> {
    /// Set the timestamp for the batch
    pub fn timestamp(self, timestamp: i64) -> BatchCollector<'a, S, Type, BatchBuild> {
        Self::step(self.builder.timestamp(timestamp), self.map, self.keyspace)
    }
    /// Build the batch request using the current collector
    pub fn build(self) -> anyhow::Result<BatchRequest> {
        Ok(BatchRequest {
            token: rand::random(),
            map: self.map,
            payload: self.builder.build()?.0.into(),
        })
    }
}

impl<'a, S: Keyspace, Type: Copy + Into<u8>> BatchCollector<'a, S, Type, BatchBuild> {
    /// Build the batch request using the current collector
    pub fn build(self) -> anyhow::Result<BatchRequest> {
        Ok(BatchRequest {
            token: rand::random(),
            map: self.map,
            payload: self.builder.build()?.0.into(),
        })
    }
}

impl<'a, S: Keyspace, Type: Copy + Into<u8>, Stage: Copy> BatchCollector<'a, S, Type, Stage> {
    fn step<NextType: Copy + Into<u8>, NextStage: Copy>(
        builder: BatchBuilder<NextType, NextStage>,
        map: HashMap<[u8; 16], Box<dyn ToStatement>>,
        keyspace: &'a S,
    ) -> BatchCollector<'a, S, NextType, NextStage> {
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

/// A Batch request, which can be used to send queries to the Ring.
/// Stores a map of prepared statement IDs that were added to the
/// batch so that the associated statements can be re-prepared if necessary.
#[derive(Clone, Debug)]
pub struct BatchRequest {
    token: i64,
    payload: Vec<u8>,
    map: HashMap<[u8; 16], Box<dyn ToStatement>>,
}

impl Request for BatchRequest {
    fn token(&self) -> i64 {
        self.token
    }

    fn statement(&self) -> &Cow<'static, str> {
        panic!("Must use `get_statement` on batch requests!")
    }

    fn payload(&self) -> Vec<u8> {
        self.payload.clone()
    }
}

impl SendRequestExt for BatchRequest {
    type Marker = DecodeVoid;
    type Worker = BasicRetryWorker<Self>;
    const TYPE: RequestType = RequestType::Batch;

    fn worker(self) -> Box<Self::Worker> {
        BasicRetryWorker::new(self)
    }
}

impl BatchRequest {
    /// Compute the murmur3 token from the provided K
    pub fn compute_token<K>(mut self, key: &K) -> Self
    where
        K: TokenEncoder,
    {
        self.token = key.token();
        self
    }

    /// Clone the cql map
    pub fn clone_map(&self) -> HashMap<[u8; 16], Box<dyn ToStatement>> {
        self.map.clone()
    }

    /// Take the cql map, leaving an empty map in the request
    pub fn take_map(&mut self) -> HashMap<[u8; 16], Box<dyn ToStatement>> {
        std::mem::take(&mut self.map)
    }

    /// Get a statement given an id from the request's map
    pub fn get_statement(&self, id: &[u8; 16]) -> Option<Cow<str>> {
        self.map.get(id).map(|res| res.to_statement())
    }

    /// Get a basic worker for this request
    pub fn worker(self) -> Box<BasicRetryWorker<Self>> {
        BasicRetryWorker::new(self)
    }
}

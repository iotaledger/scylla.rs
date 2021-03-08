// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

pub(crate) mod batch;
/// Provides the `Delete` trait which can be implemented to
/// define delete queries for Key / Value pairs and how
/// they are decoded
pub(crate) mod delete;
/// Provides the `Insert` trait which can be implemented to
/// define insert queries for Key / Value pairs and how
/// they are decoded
pub(crate) mod insert;
/// Provides the `Keyspace` trait which defines a scylla
/// keyspace. Structs that impl this trait should also impl
/// required query and decoder traits.
pub(crate) mod keyspace;
/// Provides the `Select` trait which can be implemented to
/// define select queries for Key / Value pairs and how
/// they are decoded
pub(crate) mod select;
/// Provides the `Update` trait which can be implemented to
/// define update queries for Key / Value pairs and how
/// they are decoded
pub(crate) mod update;

pub use super::{Worker, WorkerError};
pub use batch::*;
pub use delete::{Delete, DeleteRequest, GetDeleteRequest, GetDeleteStatement};
pub use insert::{GetInsertRequest, GetInsertStatement, Insert, InsertRequest};
pub use keyspace::Keyspace;
pub use select::{GetSelectRequest, GetSelectStatement, Select, SelectRequest};
pub use update::{GetUpdateRequest, GetUpdateStatement, Update, UpdateRequest};

/// alias the ring (in case it's needed)
pub use crate::ring::Ring;
/// alias the reporter event (in case it's needed)
pub use crate::stage::{ReporterEvent, ReporterHandle};
/// alias to cql traits and types
pub use scylla_cql::*;

use std::{borrow::Cow, marker::PhantomData, ops::Deref};

#[repr(u8)]
#[derive(Copy, Clone)]
enum RequestType {
    Insert = 0,
    Update = 1,
    Delete = 2,
    Select = 3,
    Batch = 4,
}

/// Defines a computed token for a key type
pub trait ComputeToken<K>: Keyspace {
    /// Compute the token from the provided partition_key by using murmur3 hash function
    fn token(key: &K) -> i64;
}

/// Create request from cql frame
pub trait CreateRequest<T>: Keyspace {
    /// Create request of Type T
    fn create_request<Q: Into<Vec<u8>>>(&self, query: Q, token: i64) -> T;
}

/// Unifying trait for requests which defines shared functionality
pub trait Request: Send + std::fmt::Debug {
    /// Get the statement that was used to create this request
    fn statement(&self) -> Cow<'static, str>;

    /// Get the request payload
    fn payload(&self) -> &Vec<u8>;
}

/// A marker struct which holds types used for a query
/// so that it may be decoded via `RowsDecoder` later
#[derive(Clone, Copy, Default)]
pub struct DecodeRows<S, K, V> {
    _marker: PhantomData<(S, K, V)>,
}

impl<S, K, V> DecodeRows<S, K, V> {
    fn new() -> Self {
        Self { _marker: PhantomData }
    }
}

impl<'a, S: RowsDecoder<K, V>, K, V> DecodeRows<S, K, V> {
    /// Decode a result payload using the `RowsDecoder` impl
    pub fn decode(&self, bytes: Vec<u8>) -> Result<Option<V>, CqlError> {
        S::try_decode(bytes.into())
    }
}

/// A marker struct which holds the keyspace type
/// so that it may be decoded (checked for errors)
/// via `VoidDecoder` later
#[derive(Copy, Clone)]
pub struct DecodeVoid<S> {
    _marker: PhantomData<S>,
}

impl<S> DecodeVoid<S> {
    fn new() -> Self {
        Self { _marker: PhantomData }
    }
}

impl<S: VoidDecoder> DecodeVoid<S> {
    /// Decode a result payload using the `VoidDecoder` impl
    pub fn decode(&self, bytes: Vec<u8>) -> Result<(), CqlError> {
        S::try_decode(bytes.into())
    }
}

/// A synchronous marker type returned when sending
/// a query to the `Ring`. Provides the request's type
/// as well as an appropriate decoder which can be used
/// once the response is received.
#[derive(Clone)]
pub struct DecodeResult<T> {
    inner: T,
    request_type: RequestType,
}
impl<S, K, V> DecodeResult<DecodeRows<S, K, V>> {
    fn select() -> Self {
        Self {
            inner: DecodeRows::<S, K, V>::new(),
            request_type: RequestType::Select,
        }
    }
}

impl<S> DecodeResult<DecodeVoid<S>> {
    fn insert() -> Self {
        Self {
            inner: DecodeVoid::<S>::new(),
            request_type: RequestType::Insert,
        }
    }
    fn update() -> Self {
        Self {
            inner: DecodeVoid::<S>::new(),
            request_type: RequestType::Update,
        }
    }
    fn delete() -> Self {
        Self {
            inner: DecodeVoid::<S>::new(),
            request_type: RequestType::Delete,
        }
    }
    fn batch() -> Self {
        Self {
            inner: DecodeVoid::<S>::new(),
            request_type: RequestType::Batch,
        }
    }
}

/// Send a local request to the Ring
pub fn send_local(token: i64, payload: Vec<u8>, worker: Box<dyn Worker>, _keyspace: String) {
    let request = ReporterEvent::Request { worker, payload };

    Ring::send_local_random_replica(token, request);
}

/// Send a global request to the Ring
pub fn send_global(token: i64, payload: Vec<u8>, worker: Box<dyn Worker>, _keyspace: String) {
    let request = ReporterEvent::Request { worker, payload };

    Ring::send_global_random_replica(token, request);
}

impl<T> Deref for DecodeResult<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

mod tests {

    use super::*;

    #[derive(Default, Clone, Debug)]
    struct Mainnet {
        pub name: Cow<'static, str>,
    }

    impl Mainnet {
        pub fn new() -> Self {
            Self { name: "mainnet".into() }
        }
    }

    impl Keyspace for Mainnet {
        fn name(&self) -> &Cow<'static, str> {
            &self.name
        }
    }

    impl Select<u32, f32> for Mainnet {
        fn statement(&self) -> Cow<'static, str> {
            "SELECT col1 FROM keyspace.table WHERE key = ?".into()
        }

        fn get_request(&self, key: &u32) -> SelectRequest<Self, u32, f32> {
            let query = Query::new()
                .statement(&self.select_statement::<u32, f32>())
                .consistency(scylla_cql::Consistency::One)
                .value(key)
                .build();
            let token = rand::random::<i64>();

            self.create_request(query, token)
        }
    }

    impl Select<u32, i32> for Mainnet {
        fn statement(&self) -> Cow<'static, str> {
            format!("SELECT * FROM {}.table WHERE key = ?", self.name()).into()
        }

        fn get_request(&self, key: &u32) -> SelectRequest<Self, u32, i32> {
            let prepared_cql = Execute::new()
                .id(&self.select_id::<u32, f32>())
                .consistency(scylla_cql::Consistency::One)
                .value(key)
                .build();
            let token = rand::random::<i64>();
            self.create_request(prepared_cql, token)
        }
    }

    impl ComputeToken<u32> for Mainnet {
        fn token(_key: &u32) -> i64 {
            rand::random()
        }
    }
    impl Insert<u32, f32> for Mainnet {
        fn statement(&self) -> Cow<'static, str> {
            format!("INSERT INTO {}.table (key, val1, val2) VALUES (?,?,?)", self.name()).into()
        }

        fn get_request(&self, key: &u32, value: &f32) -> InsertRequest<Self, u32, f32> {
            let query = Query::new()
                .statement(&self.insert_statement::<u32, f32>())
                .consistency(scylla_cql::Consistency::One)
                .value(key)
                .value(value)
                .value(value)
                .build();
            let token = Self::token(key);
            self.create_request(query, token)
        }
    }

    impl InsertBatch<u32, f32, BatchTypeLogged> for Mainnet {
        fn default_type() -> BatchQueryType {
            BatchQueryType::Prepared
        }
        fn push_insert(
            builder: scylla_cql::BatchBuilder<BatchTypeLogged, scylla_cql::BatchValues>,
            key: &u32,
            value: &f32,
        ) -> scylla_cql::BatchBuilder<BatchTypeLogged, scylla_cql::BatchValues> {
            builder.value(key).value(value).value(value)
        }
    }

    impl Update<u32, f32> for Mainnet {
        fn statement(&self) -> Cow<'static, str> {
            format!("UPDATE {}.table SET val1 = ?, val2 = ? WHERE key = ?", self.name()).into()
        }

        fn get_request(&self, key: &u32, value: &f32) -> UpdateRequest<Self, u32, f32> {
            let query = Query::new()
                .statement(&self.update_statement::<u32, f32>())
                .consistency(scylla_cql::Consistency::One)
                .value(value)
                .value(value)
                .value(key)
                .build();
            let token = rand::random::<i64>();
            self.create_request(query, token)
        }
    }

    impl UpdateBatch<u32, f32, BatchTypeLogged> for Mainnet {
        fn default_type() -> BatchQueryType {
            BatchQueryType::Query
        }
        fn push_update(
            builder: scylla_cql::BatchBuilder<BatchTypeLogged, scylla_cql::BatchValues>,
            key: &u32,
            value: &f32,
        ) -> scylla_cql::BatchBuilder<BatchTypeLogged, scylla_cql::BatchValues> {
            builder.value(value).value(value).value(key)
        }
    }

    impl Delete<u32, f32> for Mainnet {
        fn statement(&self) -> Cow<'static, str> {
            "DELETE FROM keyspace.table WHERE key = ?".into()
        }

        fn get_request(&self, key: &u32) -> DeleteRequest<Self, u32, f32> {
            let query = Query::new()
                .statement(&self.delete_statement::<u32, f32>())
                .consistency(scylla_cql::Consistency::One)
                .value(key)
                .build();
            let token = rand::random::<i64>();
            self.create_request(query, token)
        }
    }

    impl Delete<u32, i32> for Mainnet {
        fn statement(&self) -> Cow<'static, str> {
            format!("DELETE FROM {}.table WHERE key = ?", self.name()).into()
        }

        fn get_request(&self, key: &u32) -> DeleteRequest<Self, u32, i32> {
            let prepared_cql = Execute::new()
                .id(&self.delete_id::<u32, i32>())
                .consistency(scylla_cql::Consistency::One)
                .value(key)
                .build();
            let token = rand::random::<i64>();
            self.create_request(prepared_cql, token)
        }
    }

    impl DeleteBatch<u32, f32, BatchTypeLogged> for Mainnet {
        fn default_type() -> BatchQueryType {
            BatchQueryType::Prepared
        }
        fn push_delete(
            builder: scylla_cql::BatchBuilder<BatchTypeLogged, scylla_cql::BatchValues>,
            key: &u32,
        ) -> scylla_cql::BatchBuilder<BatchTypeLogged, scylla_cql::BatchValues> {
            builder.value(key)
        }
    }

    impl DeleteBatch<u32, i32, BatchTypeLogged> for Mainnet {
        fn default_type() -> BatchQueryType {
            BatchQueryType::Query
        }
        fn push_delete(
            builder: scylla_cql::BatchBuilder<BatchTypeLogged, scylla_cql::BatchValues>,
            key: &u32,
        ) -> scylla_cql::BatchBuilder<BatchTypeLogged, scylla_cql::BatchValues> {
            builder.value(key)
        }
    }
    impl RowsDecoder<u32, f32> for Mainnet {
        type Row = f32;
        fn try_decode(decoder: Decoder) -> Result<Option<f32>, CqlError> {
            todo!()
        }
    }

    impl RowsDecoder<u32, i32> for Mainnet {
        type Row = i32;
        fn try_decode(decoder: Decoder) -> Result<Option<i32>, CqlError> {
            todo!()
        }
    }

    impl VoidDecoder for Mainnet {}

    #[derive(Debug)]
    struct TestWorker {
        request: Box<dyn Request>,
    }

    impl Worker for TestWorker {
        fn handle_response(self: Box<Self>, giveload: Vec<u8>) {
            // Do nothing
        }

        fn handle_error(
            self: Box<Self>,
            error: crate::worker::WorkerError,
            reporter: &Option<crate::stage::ReporterHandle>,
        ) {
            if let WorkerError::Cql(mut cql_error) = error {
                if let (Some(_), Some(reporter)) = (cql_error.take_unprepared_id(), reporter) {
                    let prepare = Prepare::new().statement(&self.request.statement()).build();
                    let prepare_worker = PrepareWorker {
                        retries: 3,
                        payload: prepare.0.clone(),
                    };
                    let prepare_request = ReporterEvent::Request {
                        worker: Box::new(prepare_worker),
                        payload: prepare.0,
                    };
                    reporter.send(prepare_request).ok();
                    let payload = self.request.payload().clone();
                    let retry_request = ReporterEvent::Request { worker: self, payload };
                    reporter.send(retry_request).ok();
                }
            }
        }
    }

    #[derive(Debug)]
    struct BatchWorker<S> {
        request: BatchRequest<S>,
    }

    impl<S: 'static + Keyspace + std::fmt::Debug> Worker for BatchWorker<S> {
        fn handle_response(self: Box<Self>, giveload: Vec<u8>) {
            // Do nothing
        }

        fn handle_error(
            self: Box<Self>,
            error: crate::worker::WorkerError,
            reporter: &Option<crate::stage::ReporterHandle>,
        ) {
            if let WorkerError::Cql(mut cql_error) = error {
                if let (Some(id), Some(reporter)) = (cql_error.take_unprepared_id(), reporter) {
                    if let Some(statement) = self.request.get_statement(&id) {
                        let prepare = Prepare::new().statement(&statement).build();
                        let prepare_worker = PrepareWorker {
                            retries: 3,
                            payload: prepare.0.clone(),
                        };
                        let prepare_request = ReporterEvent::Request {
                            worker: Box::new(prepare_worker),
                            payload: prepare.0,
                        };
                        reporter.send(prepare_request).ok();
                        let payload = self.request.payload().clone();
                        let retry_request = ReporterEvent::Request { worker: self, payload };
                        reporter.send(retry_request).ok();
                    }
                }
            }
        }
    }

    #[derive(Debug)]
    pub struct PrepareWorker {
        pub retries: usize,
        pub payload: Vec<u8>,
    }

    impl Worker for PrepareWorker {
        fn handle_response(self: Box<Self>, giveload: Vec<u8>) {
            // Do nothing
        }

        fn handle_error(self: Box<Self>, error: WorkerError, reporter: &Option<ReporterHandle>) {
            if self.retries > 0 {
                let prepare_worker = PrepareWorker {
                    retries: self.retries - 1,
                    payload: self.payload.clone(),
                };
                let request = ReporterEvent::Request {
                    worker: Box::new(prepare_worker),
                    payload: self.payload.clone(),
                };
            }
        }
    }

    #[allow(dead_code)]
    fn test_select() {
        let keyspace = Mainnet::new();
        let req1 = keyspace.select::<f32>(&3);
        let worker1 = TestWorker {
            request: Box::new(req1.clone()),
        };
        let req2 = keyspace.select::<i32>(&3);
        let worker2 = TestWorker {
            request: Box::new(req1.clone()),
        };
        let worker3 = TestWorker {
            request: Box::new(req2.clone()),
        };
        let res = req1.clone().send_local(Box::new(worker1));
        let res = req1.send_local(Box::new(worker2));
        let res = req2.send_local(Box::new(worker3));
    }

    #[allow(dead_code)]
    fn test_insert() {
        let keyspace = Mainnet { name: "mainnet".into() };
        let req = keyspace.insert(&3, &8.0);
        let worker = TestWorker {
            request: Box::new(req.clone()),
        };

        let res = req.send_local(Box::new(worker));
    }

    #[allow(dead_code)]
    fn test_update() {
        let keyspace = Mainnet { name: "mainnet".into() };
        let req = keyspace.update(&3, &8.0);
        let worker = TestWorker {
            request: Box::new(req.clone()),
        };

        let res = req.send_local(Box::new(worker));
    }

    #[allow(dead_code)]
    fn test_delete() {
        let keyspace = Mainnet { name: "mainnet".into() };
        let req = keyspace.delete::<f32>(&3);
        let worker = TestWorker {
            request: Box::new(req.clone()),
        };

        let res = req.send_local(Box::new(worker));
    }

    #[test]
    #[allow(dead_code)]
    fn test_batch() {
        let keyspace = Mainnet::new();
        let req = keyspace
            .batch()
            .logged() // or .batch_type(BatchTypeLogged)
            .insert(&3, &9.0)
            .update_query(&3, &8.0)
            .insert_prepared(&3, &8.0)
            .delete_prepared::<_, f32>(&3)
            .consistency(Consistency::One)
            .build()
            .compute_token(&3);
        let id = keyspace.insert_id::<u32, f32>();
        let statement = req.get_statement(&id).unwrap();
        assert_eq!(statement, keyspace.insert_statement::<u32, f32>());
        let worker = BatchWorker { request: req.clone() };
        let res = req.clone().send_local(Box::new(worker));
    }
}

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
pub use scylla_cql::{
    BatchTypeCounter, BatchTypeLogged, BatchTypeUnlogged, Consistency, CqlError, Decoder, Execute, Query, RowsDecoder,
    VoidDecoder,
};

use scylla_cql::*;
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

pub trait ComputeToken<K>: Keyspace {
    /// Compute the token from the provided partition_key by using murmur3 hash function
    fn token(key: &K) -> i64;
}

/// Create request from cql frame
pub trait CreateRequest<'a, T>: Keyspace {
    /// Create request of Type T
    fn create_request<Q: Into<Vec<u8>>>(&'a self, query: Q, token: i64) -> T;
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

    #[derive(Default, Clone)]
    struct Mainnet {
        pub name: Cow<'static, str>,
    }

    impl Keyspace for Mainnet {
        fn name(&self) -> &Cow<'static, str> {
            &self.name
        }
    }

    impl Select<u32, f32> for Mainnet {
        fn statement(&self) -> Cow<'static, str> {
            "SELECT * FROM keyspace.table WHERE key = ?".into()
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
        fn recommended<B: QueryOrPrepared>(&self, builder: B) -> BatchBuilder<B::BatchType, BatchValues> {
            builder.prepared(&self.insert_id::<u32, f32>())
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
        fn recommended<B: QueryOrPrepared>(&self, builder: B) -> BatchBuilder<B::BatchType, BatchValues> {
            builder.prepared(&self.insert_id::<u32, f32>())
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

        fn get_request(&self, key: &u32) -> DeleteRequest<Self, u32, i32>
        where
            Self: Delete<u32, i32>,
        {
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
        fn recommended<B: QueryOrPrepared>(&self, builder: B) -> BatchBuilder<B::BatchType, BatchValues> {
            builder.prepared(&self.insert_id())
        }
        fn push_delete(
            builder: scylla_cql::BatchBuilder<BatchTypeLogged, scylla_cql::BatchValues>,
            key: &u32,
        ) -> scylla_cql::BatchBuilder<BatchTypeLogged, scylla_cql::BatchValues> {
            builder.value(key)
        }
    }

    impl DeleteBatch<u32, i32, BatchTypeLogged> for Mainnet {
        fn recommended<B: QueryOrPrepared>(&self, builder: B) -> BatchBuilder<B::BatchType, BatchValues> {
            builder.prepared(&self.insert_id())
        }
        fn push_delete(
            builder: scylla_cql::BatchBuilder<BatchTypeLogged, scylla_cql::BatchValues>,
            key: &u32,
        ) -> scylla_cql::BatchBuilder<BatchTypeLogged, scylla_cql::BatchValues> {
            builder.value(key)
        }
    }

    impl RowsDecoder<u32, f32> for Mainnet {
        fn try_decode(decoder: Decoder) -> Result<Option<f32>, CqlError> {
            todo!()
        }
    }

    impl RowsDecoder<u32, i32> for Mainnet {
        fn try_decode(decoder: Decoder) -> Result<Option<i32>, CqlError> {
            todo!()
        }
    }

    impl VoidDecoder for Mainnet {}

    #[derive(Debug, Clone)]
    struct TestWorker;

    impl Worker for TestWorker {
        fn handle_response(self: Box<Self>, giveload: Vec<u8>) {
            todo!()
        }

        fn handle_error(
            self: Box<Self>,
            error: crate::worker::WorkerError,
            reporter: &Option<crate::stage::ReporterHandle>,
        ) {
            todo!()
        }
    }

    #[allow(dead_code)]
    fn test_select() {
        let worker = TestWorker;
        let mut keyspace = Mainnet { name: "mainnet".into() };
        let req1 = keyspace.select::<f32>(&3);
        // Can't do this here
        // keyspace.name = "testnet".into();
        let req2 = keyspace.select::<i32>(&3);
        let res = req1.clone().send_local(Box::new(worker.clone()));
        let res = req1.send_local(Box::new(worker.clone()));
        // Or here (or anywhere in between)
        // keyspace.name = "testnet".into();
        let res = req2.send_local(Box::new(worker));
        // But now that we've consumed both requests that reference the keyspace, we can do this again
        keyspace.name = "testnet".into();
    }

    #[allow(dead_code)]
    fn test_insert() {
        let worker = TestWorker;
        let keyspace = Mainnet { name: "mainnet".into() };
        let res = keyspace.insert(&3, &8.0).send_local(Box::new(worker));
    }

    #[allow(dead_code)]
    fn test_update() {
        let worker = TestWorker;
        let keyspace = Mainnet { name: "mainnet".into() };
        let res = keyspace.update(&3, &8.0).send_local(Box::new(worker));
    }

    #[allow(dead_code)]
    fn test_delete() {
        let worker = TestWorker;
        let keyspace = Mainnet { name: "mainnet".into() };
        let res = keyspace.delete::<f32>(&3).send_local(Box::new(worker));
    }

    #[allow(dead_code)]
    fn test_batch() {
        let worker = TestWorker;
        let keyspace = Mainnet { name: "mainnet".into() };
        let req = keyspace
            .batch()
            .logged() // or .batch_type(BatchTypeLogged)
            .insert_recommended(&3, &9.0)
            .update_query(&3, &8.0)
            .insert_prepared(&3, &8.0)
            .delete_prepared::<_, f32>(&3)
            .consistency(Consistency::One)
            .build()
            .compute_token(&3);
        let res = req.clone().send_local(Box::new(worker));

        // Later, after getting an unprepared error:
        let unprepared_id = keyspace.update_id();
        let unprepared_statement = req.get_cql(&unprepared_id).expect("How could this happen to me?");
        // Do something to prepare the statement...
        //   try_prepare(unprepared_statement)
        // Then, resend the request
        let worker = TestWorker;
        let res = req.send_local(Box::new(worker));
    }
}

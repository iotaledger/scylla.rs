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
pub use batch::Batch;
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
pub use scylla_cql::{Consistency, CqlError, Decoder, Execute, Query, RowsDecoder, VoidDecoder};

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

/// A query type which indicates whether the statement
/// should be used dynamically or via its MD5 hash
#[repr(u8)]
#[derive(Copy, Clone)]
pub enum QueryType {
    /// A dynamic statement
    Dynamic = 0,
    /// A prepared statement
    Prepared = 1,
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
    use scylla_cql::{compression::UNCOMPRESSED, BatchTypes};

    use super::*;

    #[derive(Default, Clone)]
    struct Mainnet {
        name: Cow<'static, str>,
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
                .value(key.to_string())
                .build();
            let token = rand::random::<i64>();

            SelectRequest::from_query(query, token, self)
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
                .value(key.to_string())
                .build();
            let token = rand::random::<i64>();
            SelectRequest::from_prepared(prepared_cql, token, self)
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
                .value(key.to_string())
                .value(value.to_string())
                .value(value.to_string())
                .build();
            let token = rand::random::<i64>();
            InsertRequest::from_query(query, token, self)
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
                .value(value.to_string())
                .value(value.to_string())
                .value(key.to_string())
                .build();
            let token = rand::random::<i64>();
            UpdateRequest::from_query(query, token, self)
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
                .value(key.to_string())
                .build();
            let token = rand::random::<i64>();
            DeleteRequest::from_query(query, token, self)
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
                .value(key.to_string())
                .build();
            let token = rand::random::<i64>();
            DeleteRequest::from_prepared(prepared_cql, token, self)
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
        let keyspace = Mainnet { name: "mainnet".into() };
        let req1 = keyspace.select::<f32>(&3);
        let req2 = keyspace.select::<i32>(&3);
        let res = req1.clone().send_local(Box::new(worker.clone()));
        let res = req1.send_local(Box::new(worker.clone()));
        let res = req2.send_local(Box::new(worker));
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
            .batch_type(BatchTypes::Unlogged)
            .update_query::<u32, f32>()
            .insert_prepared::<u32, f32>()
            .delete_prepared::<u32, i32>()
            .consistency(Consistency::One)
            .build(0, UNCOMPRESSED);
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

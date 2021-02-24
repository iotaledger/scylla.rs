// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

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

pub use super::Worker;
pub use delete::{Delete, DeleteRequest, GetDeleteRequest};
pub use insert::{GetInsertRequest, Insert, InsertRequest};
pub use keyspace::Keyspace;
pub use select::{GetSelectRequest, Select, SelectRequest};
pub use update::{GetUpdateRequest, Update, UpdateRequest};

/// alias to cql traits and types
pub use scylla_cql::{CqlError, Decoder, Execute, Query, RowsDecoder, VoidDecoder};

use std::{borrow::Cow, marker::PhantomData, ops::Deref};

#[repr(u8)]
#[derive(Copy, Clone)]
enum RequestType {
    Insert = 0,
    Update = 1,
    Delete = 2,
    Select = 3,
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

impl<'a, S: RowsDecoder<K, V>, K, V> DecodeRows<S, K, V> {
    /// Decode a result payload using the `RowsDecoder` impl
    pub fn decode(&self, bytes: Vec<u8>) -> Result<Option<V>, CqlError> {
        S::try_decode(bytes.into())
    }
}

/// A marker struct which holds the keyspace type
/// so that it may be decoded (checked for errors)
/// via `VoidDecoder` later
#[derive(Copy, Clone, Default)]
pub struct DecodeVoid<S> {
    _marker: PhantomData<S>,
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

impl<T> Deref for DecodeResult<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

mod tests {
    use super::*;

    #[derive(Default)]
    struct Mainnet;

    impl Keyspace for Mainnet {
        const NAME: &'static str = "Mainnet";
        fn new() -> Self {
            Mainnet
        }
        fn send_local(&self, token: i64, payload: Vec<u8>, worker: Box<dyn Worker>) {
            todo!()
        }

        fn send_global(&self, token: i64, payload: Vec<u8>, worker: Box<dyn Worker>) {
            todo!()
        }
    }

    impl<'a> Select<'a, u32, f32> for Mainnet {
        fn select_statement() -> Cow<'static, str> {
            "SELECT * FROM keyspace.table WHERE key = ?".into()
        }

        fn get_request(&'a self, key: &u32) -> SelectRequest<'a, Self, u32, f32>
        where
            Self: Select<'a, u32, f32>,
        {
            let query = Query::new()
                .statement(&Self::select_statement())
                .consistency(scylla_cql::Consistency::One)
                .value(key.to_string())
                .build();
            let token = rand::random::<i64>();

            SelectRequest::from_query(query, token, self)
        }
    }

    impl<'a> Select<'a, u32, i32> for Mainnet {
        fn select_statement() -> Cow<'static, str> {
            format!("SELECT * FROM {}.table WHERE key = ?", Self::name()).into()
        }

        fn get_request(&'a self, key: &u32) -> SelectRequest<'a, Self, u32, i32>
        where
            Self: Select<'a, u32, i32>,
        {
            let prepared_cql = Execute::new()
                .id(&Self::select_id())
                .consistency(scylla_cql::Consistency::One)
                .value(key.to_string())
                .build();
            let token = rand::random::<i64>();
            SelectRequest::from_prepared(prepared_cql, token, self)
        }
    }

    impl<'a> Insert<'a, u32, f32> for Mainnet {
        fn insert_statement() -> Cow<'static, str> {
            format!("INSERT INTO {}.table (key, val1, val2) VALUES (?,?,?)", Self::name()).into()
        }

        fn get_request(&'a self, key: &u32, value: &f32) -> InsertRequest<'a, Self, u32, f32>
        where
            Self: Insert<'a, u32, f32>,
        {
            let query = Query::new()
                .statement(&Self::insert_statement())
                .consistency(scylla_cql::Consistency::One)
                .value(key.to_string())
                .value(value.to_string())
                .value(value.to_string())
                .build();
            let token = rand::random::<i64>();
            InsertRequest::from_query(query, token, self)
        }
    }

    impl<'a> Update<'a, u32, f32> for Mainnet {
        fn update_statement() -> Cow<'static, str> {
            format!("UPDATE {}.table SET val1 = ?, val2 = ? WHERE key = ?", Self::name()).into()
        }

        fn get_request(&'a self, key: &u32, value: &f32) -> UpdateRequest<'a, Self, u32, f32>
        where
            Self: Update<'a, u32, f32>,
        {
            let query = Query::new()
                .statement(&Self::update_statement())
                .consistency(scylla_cql::Consistency::One)
                .value(value.to_string())
                .value(value.to_string())
                .value(key.to_string())
                .build();
            let token = rand::random::<i64>();
            UpdateRequest::from_query(query, token, self)
        }
    }

    impl<'a> Delete<'a, u32, f32> for Mainnet {
        fn delete_statement() -> Cow<'static, str> {
            "DELETE FROM keyspace.table WHERE key = ?".into()
        }

        fn get_request(&'a self, key: &u32) -> DeleteRequest<'a, Self, u32, f32>
        where
            Self: Delete<'a, u32, f32>,
        {
            let query = Query::new()
                .statement(&Self::delete_statement())
                .consistency(scylla_cql::Consistency::One)
                .value(key.to_string())
                .build();
            let token = rand::random::<i64>();
            DeleteRequest::from_query(query, token, self)
        }
    }

    impl<'a> Delete<'a, u32, i32> for Mainnet {
        fn delete_statement() -> Cow<'static, str> {
            format!("DELETE FROM {}.table WHERE key = ?", Self::name()).into()
        }

        fn get_request(&'a self, key: &u32) -> DeleteRequest<'a, Self, u32, i32>
        where
            Self: Delete<'a, u32, i32>,
        {
            let prepared_cql = Execute::new()
                .id(&Self::delete_id())
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

    #[derive(Debug)]
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
        let res = Mainnet.select::<f32>(&3).send_local(Box::new(worker));
    }

    #[allow(dead_code)]
    fn test_insert() {
        let worker = TestWorker;
        let res = Mainnet.insert(&3, &8.0).send_local(Box::new(worker));
    }

    #[allow(dead_code)]
    fn test_update() {
        let worker = TestWorker;
        let res = Mainnet.update(&3, &8.0).send_local(Box::new(worker));
    }

    #[allow(dead_code)]
    fn test_delete() {
        let worker = TestWorker;
        let res = Mainnet.delete::<f32>(&3).send_local(Box::new(worker));
    }
}

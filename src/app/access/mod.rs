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

use super::{
    Worker,
    WorkerError,
};
use crate::{
    app::{
        ring::{
            Ring,
            RingSendError,
        },
        stage::reporter::{
            ReporterEvent,
            ReporterHandle,
        },
    },
    cql::{
        Consistency,
        Decoder,
        Prepare,
        PreparedStatement,
        Query,
        QueryBuild,
        QueryBuilder,
        QueryConsistency,
        QueryOrPrepared,
        QueryStatement,
        QueryValues,
        RowsDecoder,
        Values,
        VoidDecoder,
    },
};
pub use batch::*;
pub use delete::{
    Delete,
    DeleteRequest,
    GetDeleteRequest,
};
pub use insert::{
    GetInsertRequest,
    Insert,
    UpsertRequest,
};
pub use keyspace::Keyspace;
pub use select::{
    GetStaticSelectRequest,
    Select,
    SelectRequest,
};
use std::{
    borrow::Cow,
    convert::TryInto,
    marker::PhantomData,
    ops::Deref,
};
use thiserror::Error;
pub use update::{
    GetUpdateRequest,
    Update,
};

#[repr(u8)]
#[derive(Copy, Clone)]
pub enum RequestType {
    Insert = 0,
    Update = 1,
    Delete = 2,
    Select = 3,
    Batch = 4,
}

pub struct DynamicRequest;
pub struct StaticRequest;

#[derive(Error, Debug)]
pub enum RequestError {
    #[error("Error sending to the Ring: {0}")]
    Send(#[from] RingSendError),
    #[error("{0}")]
    Worker(#[from] WorkerError),
    #[error("Error receiving response from Scylla: {0}")]
    Receive(#[from] anyhow::Error),
}

/// Defines a computed token for a key type
pub trait ComputeToken<K: ?Sized> {
    /// Compute the token from the provided partition_key by using murmur3 hash function
    fn token(key: &K) -> i64;
}

pub trait Request {
    /// Get the statement that was used to create this request
    fn statement(&self) -> &Cow<'static, str>;

    /// Get the request payload
    fn payload(&self) -> &Vec<u8>;

    fn into_payload(self) -> Vec<u8>;
}

pub trait SendRequestExt: Request {
    type Marker: Send;
    const TYPE: RequestType;

    fn token(&self) -> i64;

    fn marker() -> Self::Marker;

    /// Send a local request using the keyspace impl and return a type marker
    fn send_local(self, worker: Box<dyn Worker>) -> Result<DecodeResult<Self::Marker>, RingSendError>
    where
        Self: Sized,
    {
        send_local(self.token(), self.into_payload(), worker)?;
        Ok(DecodeResult::new(Self::marker(), Self::TYPE))
    }

    /// Send a global request using the keyspace impl and return a type marker
    fn send_global(self, worker: Box<dyn Worker>) -> Result<DecodeResult<Self::Marker>, RingSendError>
    where
        Self: Sized,
    {
        send_global(self.token(), self.into_payload(), worker)?;
        Ok(DecodeResult::new(Self::marker(), Self::TYPE))
    }
}

/// Unifying trait for requests which defines shared functionality
#[async_trait::async_trait]
pub trait GetRequestExt<O: Send>: SendRequestExt {
    fn worker(handle: tokio::sync::mpsc::UnboundedSender<Result<O, WorkerError>>) -> Box<dyn Worker>;

    async fn get_local(self) -> Result<O, RequestError>
    where
        Self: Sized,
    {
        let (sender, mut inbox) = tokio::sync::mpsc::unbounded_channel();
        let worker = Self::worker(sender);
        self.send_local(worker)?;
        Ok(inbox
            .recv()
            .await
            .ok_or_else(|| anyhow::anyhow!("No response from worker!"))??)
    }

    fn get_local_blocking(self) -> Result<O, RequestError>
    where
        Self: Sized,
    {
        let (sender, mut inbox) = tokio::sync::mpsc::unbounded_channel();
        let worker = Self::worker(sender);
        self.send_local(worker)?;
        Ok(inbox
            .blocking_recv()
            .ok_or_else(|| anyhow::anyhow!("No response from worker!"))??)
    }

    async fn get_global(self) -> Result<O, RequestError>
    where
        Self: Sized,
    {
        let (sender, mut inbox) = tokio::sync::mpsc::unbounded_channel();
        let worker = Self::worker(sender);
        self.send_global(worker)?;
        Ok(inbox
            .recv()
            .await
            .ok_or_else(|| anyhow::anyhow!("No response from worker!"))??)
    }

    fn get_global_blocking(self) -> Result<O, RequestError>
    where
        Self: Sized,
    {
        let (sender, mut inbox) = tokio::sync::mpsc::unbounded_channel();
        let worker = Self::worker(sender);
        self.send_global(worker)?;
        Ok(inbox
            .blocking_recv()
            .ok_or_else(|| anyhow::anyhow!("No response from worker!"))??)
    }
}

/// Defines two helper methods to specify statement / id
pub trait GetStatementIdExt {
    fn select_statement<K, V>(&self) -> Cow<'static, str>
    where
        Self: Select<K, V>,
    {
        self.statement()
    }

    fn select_id<K, V>(&self) -> [u8; 16]
    where
        Self: Select<K, V>,
    {
        self.id()
    }

    fn insert_statement<K, V>(&self) -> Cow<'static, str>
    where
        Self: Insert<K, V>,
    {
        self.statement()
    }

    fn insert_id<K, V>(&self) -> [u8; 16]
    where
        Self: Insert<K, V>,
    {
        self.id()
    }

    fn update_statement<K, V>(&self) -> Cow<'static, str>
    where
        Self: Update<K, V>,
    {
        self.statement()
    }

    fn update_id<K, V>(&self) -> [u8; 16]
    where
        Self: Update<K, V>,
    {
        self.id()
    }

    fn delete_statement<K, V>(&self) -> Cow<'static, str>
    where
        Self: Delete<K, V>,
    {
        self.statement()
    }

    fn delete_id<K, V>(&self) -> [u8; 16]
    where
        Self: Delete<K, V>,
    {
        self.id()
    }
}

impl<S: Keyspace> GetStatementIdExt for S {}

/// A marker struct which holds types used for a query
/// so that it may be decoded via `RowsDecoder` later
#[derive(Clone, Copy, Default)]
pub struct DecodeRows<S, V> {
    _marker: PhantomData<fn(S, V) -> (S, V)>,
}

impl<S, V> DecodeRows<S, V> {
    fn new() -> Self {
        Self { _marker: PhantomData }
    }
}

impl<'a, S: RowsDecoder<V>, V> DecodeRows<S, V> {
    /// Decode a result payload using the `RowsDecoder` impl
    pub fn decode(&self, bytes: Vec<u8>) -> anyhow::Result<Option<V>> {
        S::try_decode_rows(bytes.try_into()?)
    }
}

/// A marker struct which holds the keyspace type
/// so that it may be decoded (checked for errors)
/// via `VoidDecoder` later
#[derive(Copy, Clone)]
pub struct DecodeVoid<S> {
    _marker: PhantomData<fn(S) -> S>,
}

impl<S> DecodeVoid<S> {
    fn new() -> Self {
        Self { _marker: PhantomData }
    }
}

impl<S: VoidDecoder> DecodeVoid<S> {
    /// Decode a result payload using the `VoidDecoder` impl
    pub fn decode(&self, bytes: Vec<u8>) -> anyhow::Result<()> {
        S::try_decode_void(bytes.try_into()?)
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
impl<T> DecodeResult<T> {
    fn new(inner: T, request_type: RequestType) -> Self {
        Self { inner, request_type }
    }
}
impl<S, V> DecodeResult<DecodeRows<S, V>> {
    fn select() -> Self {
        Self {
            inner: DecodeRows::<S, V>::new(),
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
pub fn send_local(token: i64, payload: Vec<u8>, worker: Box<dyn Worker>) -> Result<(), RingSendError> {
    let request = ReporterEvent::Request { worker, payload };

    Ring::send_local_random_replica(token, request)
}

/// Send a global request to the Ring
pub fn send_global(token: i64, payload: Vec<u8>, worker: Box<dyn Worker>) -> Result<(), RingSendError> {
    let request = ReporterEvent::Request { worker, payload };

    Ring::send_global_random_replica(token, request)
}

impl<T> Deref for DecodeResult<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

#[doc(hidden)]
pub mod tests {

    use crate::{
        cql::query::StatementType,
        prelude::{
            select::GetDynamicSelectRequest,
            BasicWorker,
        },
    };

    use super::*;

    #[derive(Default, Clone, Debug)]
    pub struct MyKeyspace {
        pub name: Cow<'static, str>,
    }

    impl MyKeyspace {
        pub fn new() -> Self {
            Self {
                name: "my_keyspace".into(),
            }
        }
    }

    impl ToString for MyKeyspace {
        fn to_string(&self) -> String {
            self.name.to_string()
        }
    }

    impl Select<u32, f32> for MyKeyspace {
        type QueryOrPrepared = PreparedStatement;
        fn statement(&self) -> Cow<'static, str> {
            "SELECT col1 FROM keyspace.table WHERE key = ?".into()
        }
        fn bind_values<T: Values>(builder: T, key: &u32) -> T::Return {
            builder.value(key)
        }
    }

    impl Select<u32, i32> for MyKeyspace {
        type QueryOrPrepared = QueryStatement;
        fn statement(&self) -> Cow<'static, str> {
            format!("SELECT col2 FROM {}.table WHERE key = ?", self.name()).into()
        }

        fn bind_values<T: Values>(builder: T, key: &u32) -> T::Return {
            builder.value(key)
        }
    }

    impl Insert<u32, f32> for MyKeyspace {
        type QueryOrPrepared = PreparedStatement;
        fn statement(&self) -> Cow<'static, str> {
            format!("INSERT INTO {}.table (key, val1, val2) VALUES (?,?,?)", self.name()).into()
        }

        fn bind_values<T: Values>(builder: T, key: &u32, value: &f32) -> T::Return {
            builder.value(key).value(value).value(value)
        }
    }

    impl Update<u32, f32> for MyKeyspace {
        type QueryOrPrepared = PreparedStatement;
        fn statement(&self) -> Cow<'static, str> {
            format!("UPDATE {}.table SET val1 = ?, val2 = ? WHERE key = ?", self.name()).into()
        }
        fn bind_values<T: Values>(builder: T, key: &u32, value: &f32) -> T::Return {
            builder.value(value).value(value).value(key)
        }
    }

    impl Delete<u32, f32> for MyKeyspace {
        type QueryOrPrepared = PreparedStatement;
        fn statement(&self) -> Cow<'static, str> {
            "DELETE FROM keyspace.table WHERE key = ?".into()
        }

        fn bind_values<T: Values>(builder: T, key: &u32) -> T::Return {
            builder.value(key).value(key)
        }
    }

    impl Delete<u32, i32> for MyKeyspace {
        type QueryOrPrepared = PreparedStatement;
        fn statement(&self) -> Cow<'static, str> {
            format!("DELETE FROM {}.table WHERE key = ?", self.name()).into()
        }

        fn bind_values<T: Values>(builder: T, key: &u32) -> T::Return {
            builder.value(key)
        }
    }

    impl RowsDecoder<f32> for MyKeyspace {
        type Row = f32;
        fn try_decode_rows(_decoder: Decoder) -> anyhow::Result<Option<f32>> {
            todo!()
        }
    }

    impl RowsDecoder<i32> for MyKeyspace {
        type Row = i32;
        fn try_decode_rows(_decoder: Decoder) -> anyhow::Result<Option<i32>> {
            todo!()
        }
    }

    struct TestWorker {
        request: Box<dyn Request + Send>,
    }

    impl std::fmt::Debug for TestWorker {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.debug_struct("TestWorker").finish()
        }
    }

    impl Worker for TestWorker {
        fn handle_response(self: Box<Self>, _giveload: Vec<u8>) -> anyhow::Result<()> {
            // Do nothing
            Ok(())
        }

        fn handle_error(
            self: Box<Self>,
            error: crate::app::worker::WorkerError,
            reporter: &ReporterHandle,
        ) -> anyhow::Result<()> {
            if let WorkerError::Cql(mut cql_error) = error {
                if let Some(_) = cql_error.take_unprepared_id() {
                    if let Ok(prepare) = Prepare::new().statement(&self.request.statement()).build() {
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
            Ok(())
        }
    }

    #[derive(Debug)]
    struct BatchWorker<S> {
        request: BatchRequest<S>,
    }

    impl<S: 'static + Keyspace + std::fmt::Debug> Worker for BatchWorker<S> {
        fn handle_response(self: Box<Self>, _giveload: Vec<u8>) -> anyhow::Result<()> {
            // Do nothing
            Ok(())
        }

        fn handle_error(
            self: Box<Self>,
            error: crate::app::worker::WorkerError,
            reporter: &crate::app::stage::reporter::ReporterHandle,
        ) -> anyhow::Result<()> {
            if let WorkerError::Cql(mut cql_error) = error {
                if let Some(id) = cql_error.take_unprepared_id() {
                    if let Some(statement) = self.request.get_statement(&id) {
                        if let Ok(prepare) = Prepare::new().statement(&statement).build() {
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
            Ok(())
        }
    }

    #[derive(Debug)]
    pub struct PrepareWorker {
        pub retries: usize,
        pub payload: Vec<u8>,
    }

    impl Worker for PrepareWorker {
        fn handle_response(self: Box<Self>, _giveload: Vec<u8>) -> anyhow::Result<()> {
            // Do nothing
            Ok(())
        }

        fn handle_error(self: Box<Self>, _error: WorkerError, _reporter: &ReporterHandle) -> anyhow::Result<()> {
            if self.retries > 0 {
                let prepare_worker = PrepareWorker {
                    retries: self.retries - 1,
                    payload: self.payload.clone(),
                };
                let _request = ReporterEvent::Request {
                    worker: Box::new(prepare_worker),
                    payload: self.payload.clone(),
                };
            }
            Ok(())
        }
    }

    #[allow(dead_code)]
    fn test_select() {
        let keyspace = MyKeyspace::new();
        let res = keyspace
            .select_with::<f32>(
                "SELECT col1 FROM keyspace.table WHERE key = ?",
                &[&3],
                StatementType::Query,
            )
            .consistency(Consistency::One)
            .build()
            .unwrap()
            .get_local_blocking();
        assert!(res.is_err());
        let req2 = keyspace
            .select::<i32>(&3)
            .consistency(Consistency::One)
            .page_size(500)
            .build()
            .unwrap();
        let worker3 = TestWorker {
            request: Box::new(req2.clone()),
        };
        let _res = req2.clone().send_local(Box::new(worker3));
    }

    #[allow(dead_code)]
    fn test_insert() {
        let keyspace = MyKeyspace { name: "mainnet".into() };
        let req = keyspace.insert(&3, &8.0).consistency(Consistency::One).build().unwrap();
        let worker = BasicWorker::new();

        let _res = req.send_local(worker);
    }

    #[allow(dead_code)]
    fn test_update() {
        let keyspace = MyKeyspace { name: "mainnet".into() };
        let req = keyspace.update(&3, &8.0).consistency(Consistency::One).build().unwrap();
        let worker = TestWorker {
            request: Box::new(req.clone()),
        };

        let _res = req.send_local(Box::new(worker));
    }

    #[allow(dead_code)]
    fn test_delete() {
        let keyspace = MyKeyspace { name: "mainnet".into() };
        let req = keyspace
            .delete::<f32>(&3)
            .consistency(Consistency::One)
            .build()
            .unwrap();
        let worker = TestWorker {
            request: Box::new(req.clone()),
        };

        let _res = req.send_local(Box::new(worker));
    }

    #[test]
    #[allow(dead_code)]
    fn test_batch() {
        let keyspace = MyKeyspace::new();
        let req = keyspace
            .batch()
            .logged() // or .batch_type(BatchTypeLogged)
            .insert(&3, &9.0)
            .update_query(&3, &8.0)
            .insert_prepared(&3, &8.0)
            .delete_prepared::<_, f32>(&3)
            .consistency(Consistency::One)
            .build()
            .unwrap()
            .compute_token(&3);
        let id = keyspace.insert_id::<u32, f32>();
        let statement = req.get_statement(&id).unwrap();
        assert_eq!(statement, keyspace.insert_statement::<u32, f32>());
        let worker = BatchWorker { request: req.clone() };
        let _res = req.clone().send_local(Box::new(worker));
    }
}

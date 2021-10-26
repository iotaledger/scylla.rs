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

pub(crate) mod execute;

pub(crate) mod prepare;

use super::{
    worker::BasicRetryWorker,
    Worker,
    WorkerError,
};
pub use crate::cql::{
    query::StatementType,
    Bindable,
    Consistency,
    PreparedStatement,
    QueryStatement,
    Values,
};
use crate::{
    app::{
        ring::{
            Ring,
            RingSendError,
        },
        stage::reporter::ReporterEvent,
    },
    cql::{
        Decoder,
        DynValues,
        Query,
        QueryBuild,
        QueryBuilder,
        QueryConsistency,
        QueryOrPrepared,
        QueryPagingState,
        QuerySerialConsistency,
        QueryValues,
        RowsDecoder,
        VoidDecoder,
    },
    prelude::{
        ColumnEncoder,
        IntoRespondingWorker,
        ReporterHandle,
        RetryableWorker,
        TokenEncoder,
    },
};
pub use batch::*;
pub use delete::{
    AsDynamicDeleteRequest,
    Delete,
    DeleteRequest,
    GetDynamicDeleteRequest,
    GetStaticDeleteRequest,
};
use dyn_clone::DynClone;
pub use execute::{
    AsDynamicExecuteRequest,
    ExecuteRequest,
    GetDynamicExecuteRequest,
};
pub use insert::{
    AsDynamicInsertRequest,
    GetDynamicInsertRequest,
    GetStaticInsertRequest,
    Insert,
    InsertRequest,
};
pub use keyspace::Keyspace;
pub use prepare::{
    AsDynamicPrepareRequest,
    GetDynamicPrepareRequest,
    GetStaticPrepareRequest,
    PrepareRequest,
};
pub use select::{
    AsDynamicSelectRequest,
    GetDynamicSelectRequest,
    GetStaticSelectRequest,
    Select,
    SelectRequest,
};
pub use std::borrow::Cow;
use std::{
    convert::TryInto,
    fmt::Debug,
    marker::PhantomData,
    ops::{
        Deref,
        DerefMut,
    },
};
use thiserror::Error;
pub use update::{
    AsDynamicUpdateRequest,
    GetDynamicUpdateRequest,
    GetStaticUpdateRequest,
    Update,
    UpdateRequest,
};

/// A token that is bindable as a column
pub trait BindableToken: TokenEncoder + ColumnEncoder {}
impl<T: TokenEncoder + ColumnEncoder> BindableToken for T {}

/// The possible request types
#[allow(missing_docs)]
#[repr(u8)]
#[derive(Copy, Clone)]
pub enum RequestType {
    Insert = 0,
    Update = 1,
    Delete = 2,
    Select = 3,
    Batch = 4,
    Execute = 5,
}

/// Represents anything that can be used to generate a statement.
/// For instance, a select query string or a keyspace with a `Select<K, V>` impl.
pub trait ToStatement: DynClone + Debug + Send + Sync {
    /// Get the statement from this type
    fn to_statement(&self) -> Cow<'static, str>;
}
dyn_clone::clone_trait_object!(ToStatement);

struct UpdateStatement<S: Update<K, V>, K, V>(S, PhantomData<fn(K, V) -> (K, V)>);
impl<S: Update<K, V>, K, V> UpdateStatement<S, K, V> {
    fn new(keyspace: &S) -> Self {
        Self(keyspace.clone(), PhantomData)
    }
}
impl<S: Update<K, V>, K, V> Clone for UpdateStatement<S, K, V> {
    fn clone(&self) -> Self {
        Self(self.0.clone(), PhantomData)
    }
}
impl<S: Update<K, V> + Debug, K, V> Debug for UpdateStatement<S, K, V> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("UpdateStatement").field(&self.0).finish()
    }
}
struct InsertStatement<S: Insert<K, V>, K, V>(S, PhantomData<fn(K, V) -> (K, V)>);
impl<S: Insert<K, V>, K, V> InsertStatement<S, K, V> {
    fn new(keyspace: &S) -> Self {
        Self(keyspace.clone(), PhantomData)
    }
}
impl<S: Insert<K, V>, K, V> Clone for InsertStatement<S, K, V> {
    fn clone(&self) -> Self {
        Self(self.0.clone(), PhantomData)
    }
}
impl<S: Insert<K, V> + Debug, K, V> Debug for InsertStatement<S, K, V> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("InsertStatement").field(&self.0).finish()
    }
}
struct DeleteStatement<S: Delete<K, V>, K, V>(S, PhantomData<fn(K, V) -> (K, V)>);
impl<S: Delete<K, V>, K, V> DeleteStatement<S, K, V> {
    fn new(keyspace: &S) -> Self {
        Self(keyspace.clone(), PhantomData)
    }
}
impl<S: Delete<K, V>, K, V> Clone for DeleteStatement<S, K, V> {
    fn clone(&self) -> Self {
        Self(self.0.clone(), PhantomData)
    }
}
impl<S: Delete<K, V> + Debug, K, V> Debug for DeleteStatement<S, K, V> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("DeleteStatement").field(&self.0).finish()
    }
}

struct SelectStatement<S: Select<K, V>, K, V>(S, PhantomData<fn(K, V) -> (K, V)>);
impl<S: Select<K, V>, K, V> SelectStatement<S, K, V> {
    #[allow(unused)]
    fn new(keyspace: &S) -> Self {
        Self(keyspace.clone(), PhantomData)
    }
}
impl<S: Select<K, V>, K, V> Clone for SelectStatement<S, K, V> {
    fn clone(&self) -> Self {
        Self(self.0.clone(), PhantomData)
    }
}
impl<S: Select<K, V> + Debug, K, V> Debug for SelectStatement<S, K, V> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("SelectStatement").field(&self.0).finish()
    }
}

impl<S: Update<K, V> + Debug, K, V> ToStatement for UpdateStatement<S, K, V> {
    fn to_statement(&self) -> Cow<'static, str> {
        self.0.statement()
    }
}

impl<S: Insert<K, V> + Debug, K, V> ToStatement for InsertStatement<S, K, V> {
    fn to_statement(&self) -> Cow<'static, str> {
        self.0.statement()
    }
}

impl<S: Delete<K, V> + Debug, K, V> ToStatement for DeleteStatement<S, K, V> {
    fn to_statement(&self) -> Cow<'static, str> {
        self.0.statement()
    }
}

impl<S: Select<K, V> + Debug, K, V> ToStatement for SelectStatement<S, K, V> {
    fn to_statement(&self) -> Cow<'static, str> {
        self.0.statement()
    }
}

impl ToStatement for String {
    fn to_statement(&self) -> Cow<'static, str> {
        self.clone().into()
    }
}

impl ToStatement for &str {
    fn to_statement(&self) -> Cow<'static, str> {
        self.to_string().into()
    }
}

/// Marker for dynamic requests
pub struct DynamicRequest;
/// Marker for static requests
pub struct StaticRequest;
/// Marker for requests that need to use a manually defined bind fn
pub struct ManualBoundRequest<'a> {
    pub(crate) bind_fn: Box<
        dyn Fn(
            Box<dyn DynValues<Return = QueryBuilder<QueryValues>>>,
            &'a [&(dyn BindableToken + Sync)],
            &'a [&(dyn ColumnEncoder + Sync)],
        ) -> QueryBuilder<QueryValues>,
    >,
}

/// Errors which can be returned from a sent request
#[allow(missing_docs)]
#[derive(Error, Debug)]
pub enum RequestError {
    #[error("Error sending to the Ring: {0}")]
    Ring(#[from] RingSendError),
    #[error("{0}")]
    Worker(#[from] WorkerError),
    #[error(transparent)]
    Other(#[from] anyhow::Error),
}

/// A request which has a token, statement, and payload
pub trait Request {
    /// Get the token for this request
    fn token(&self) -> i64;

    /// Get the statement that was used to create this request
    fn statement(&self) -> &Cow<'static, str>;

    /// Get the request payload
    fn payload(&self) -> Vec<u8>;
}

/// Extension trait which provides helper functions for sending requests and retrieving their responses
#[async_trait::async_trait]
pub trait SendRequestExt: 'static + Request + Debug + Send + Sync + Sized {
    /// The marker type which will be returned when sending a request
    type Marker: 'static + Marker;
    /// The default worker type
    type Worker: RetryableWorker<Self>;
    /// The request type
    const TYPE: RequestType;

    /// Create a worker containing this request
    fn worker(self) -> Box<Self::Worker>;

    /// Send this request to a specific reporter, without waiting for a response
    fn send_to_reporter(self, reporter: &ReporterHandle) -> Result<DecodeResult<Self::Marker>, RequestError> {
        self.worker().send_to_reporter(reporter)?;
        Ok(DecodeResult::new(Self::Marker::new(), Self::TYPE))
    }

    /// Send this request to the local datacenter, without waiting for a response
    fn send_local(self) -> Result<DecodeResult<Self::Marker>, RequestError> {
        send_local(self.token(), self.payload(), self.worker())?;
        Ok(DecodeResult::new(Self::Marker::new(), Self::TYPE))
    }

    /// Send this request to a global datacenter, without waiting for a response
    fn send_global(self) -> Result<DecodeResult<Self::Marker>, RequestError> {
        send_global(self.token(), self.payload(), self.worker())?;
        Ok(DecodeResult::new(Self::Marker::new(), Self::TYPE))
    }

    /// Send this request to the local datacenter and await the response asynchronously
    async fn get_local(self) -> Result<<Self::Marker as Marker>::Output, RequestError>
    where
        Self::Marker: Send + Sync,
        Self::Worker: IntoRespondingWorker<Self, tokio::sync::oneshot::Sender<Result<Decoder, WorkerError>>, Decoder>,
    {
        self.worker().get_local().await
    }

    /// Send this request to the local datacenter and await the response synchronously
    fn get_local_blocking(self) -> Result<<Self::Marker as Marker>::Output, RequestError>
    where
        Self::Worker: IntoRespondingWorker<Self, tokio::sync::oneshot::Sender<Result<Decoder, WorkerError>>, Decoder>,
    {
        self.worker().get_local_blocking()
    }

    /// Send this request to a global datacenter and await the response asynchronously
    async fn get_global(self) -> Result<<Self::Marker as Marker>::Output, RequestError>
    where
        Self::Marker: Send + Sync,
        Self::Worker: IntoRespondingWorker<Self, tokio::sync::oneshot::Sender<Result<Decoder, WorkerError>>, Decoder>,
    {
        self.worker().get_global().await
    }

    /// Send this request to a global datacenter and await the response synchronously
    fn get_global_blocking(self) -> Result<<Self::Marker as Marker>::Output, RequestError>
    where
        Self::Worker: IntoRespondingWorker<Self, tokio::sync::oneshot::Sender<Result<Decoder, WorkerError>>, Decoder>,
    {
        self.worker().get_global_blocking()
    }
}

/// A common request type which contains only the bare minimum information needed
#[derive(Debug, Clone)]
pub struct CommonRequest {
    pub(crate) token: i64,
    pub(crate) payload: Vec<u8>,
    pub(crate) statement: Cow<'static, str>,
}

impl CommonRequest {
    #[allow(missing_docs)]
    pub fn new(statement: &str, payload: Vec<u8>) -> Self {
        Self {
            token: 0,
            payload,
            statement: statement.to_string().into(),
        }
    }
}

impl Request for CommonRequest {
    fn token(&self) -> i64 {
        self.token
    }

    fn statement(&self) -> &Cow<'static, str> {
        &self.statement
    }

    fn payload(&self) -> Vec<u8> {
        self.payload.clone()
    }
}

/// Defines two helper methods to specify statement / id
#[allow(missing_docs)]
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
pub struct DecodeRows<V> {
    _marker: PhantomData<fn(V) -> V>,
}

impl<V> DecodeRows<V> {
    fn new() -> Self {
        Self { _marker: PhantomData }
    }
}

impl<V: RowsDecoder> DecodeRows<V> {
    /// Decode a result payload using the `RowsDecoder` impl
    pub fn decode(&self, bytes: Vec<u8>) -> anyhow::Result<Option<V>> {
        V::try_decode_rows(bytes.try_into()?)
    }
}

/// A marker struct which holds the keyspace type
/// so that it may be decoded (checked for errors)
/// via `VoidDecoder` later
#[derive(Copy, Clone)]
pub struct DecodeVoid;

impl DecodeVoid {
    /// Decode a result payload using the `VoidDecoder` impl
    pub fn decode(&self, bytes: Vec<u8>) -> anyhow::Result<()> {
        VoidDecoder::try_decode_void(bytes.try_into()?)
    }
}

/// A marker returned by a request to allow for later decoding of the response
pub trait Marker {
    /// The marker's output
    type Output: Send;

    #[allow(missing_docs)]
    fn new() -> Self;

    /// Try to decode the response payload using this marker
    fn try_decode(&self, d: Decoder) -> anyhow::Result<Self::Output> {
        Self::internal_try_decode(d)
    }

    #[allow(missing_docs)]
    fn internal_try_decode(d: Decoder) -> anyhow::Result<Self::Output>;
}

impl<T: RowsDecoder + Send> Marker for DecodeRows<T> {
    type Output = Option<T>;

    fn new() -> Self {
        DecodeRows::new()
    }

    fn internal_try_decode(d: Decoder) -> anyhow::Result<Self::Output> {
        T::try_decode_rows(d)
    }
}

impl Marker for DecodeVoid {
    type Output = ();

    fn new() -> Self {
        Self
    }

    fn internal_try_decode(d: Decoder) -> anyhow::Result<Self::Output> {
        VoidDecoder::try_decode_void(d)
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
    pub(crate) fn new(inner: T, request_type: RequestType) -> Self {
        Self { inner, request_type }
    }
}
impl<V> DecodeResult<DecodeRows<V>> {
    fn select() -> Self {
        Self {
            inner: DecodeRows::<V>::new(),
            request_type: RequestType::Select,
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
    use super::*;
    use crate::{
        cql::query::StatementType,
        prelude::select::{
            AsDynamicSelectRequest,
            GetDynamicSelectRequest,
        },
    };

    #[derive(Default, Clone, Debug)]
    pub struct MyKeyspace {
        pub name: String,
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

    #[allow(dead_code)]
    fn test_select() {
        let keyspace = MyKeyspace::new();
        let res: Result<DecodeResult<DecodeRows<f32>>, RequestError> = keyspace
            .select_with::<f32>(
                "SELECT col1 FROM keyspace.table WHERE key = ?",
                &[&3],
                StatementType::Query,
            )
            .build()
            .unwrap()
            .worker()
            .with_retries(3)
            .send_local();
        assert!(res.is_err());
        let res = "SELECT col1 FROM keyspace.table WHERE key = ?"
            .as_select_prepared::<f32>(&[&3])
            .build()
            .unwrap()
            .get_local_blocking();
        assert!(res.is_err());
        let res = keyspace
            .select_prepared::<f32>(&3u32)
            .build()
            .unwrap()
            .get_local_blocking();
        assert!(res.is_err());
        let req2 = keyspace.select::<i32>(&3).page_size(500).build().unwrap();
        let _res = req2.clone().send_local();
    }

    #[allow(dead_code)]
    fn test_insert() {
        let keyspace = MyKeyspace { name: "mainnet".into() };
        let req = keyspace.insert(&3, &8.0).build().unwrap();
        let _res = req.send_local();

        "my_keyspace"
            .insert_with(
                "INSERT INTO {{keyspace}}.table (key, val1, val2) VALUES (?,?,?)",
                &[&3],
                &[&8.0, &"hello"],
                StatementType::Query,
            )
            .bind_values(|builder, keys, values| builder.bind(keys).bind(values))
            .build()
            .unwrap()
            .get_local_blocking()
            .unwrap();

        "INSERT INTO my_keyspace.table (key, val1, val2) VALUES (?,?,?)"
            .as_insert_query(&[&3], &[&8.0, &"hello"])
            .bind_values(|builder, keys, values| builder.bind(keys).bind(values))
            .build()
            .unwrap()
            .send_local()
            .unwrap();
    }

    #[allow(dead_code)]
    fn test_update() {
        let keyspace = MyKeyspace { name: "mainnet".into() };
        let req = keyspace.update(&3, &8.0).build().unwrap();

        let _res = req.send_local();
    }

    #[allow(dead_code)]
    fn test_delete() {
        let keyspace = MyKeyspace { name: "mainnet".into() };
        let req = keyspace
            .delete::<f32>(&3)
            .consistency(Consistency::All)
            .build()
            .unwrap();

        let _res = req.send_local();
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
            .build()
            .unwrap()
            .compute_token(&3);
        let id = keyspace.insert_id::<u32, f32>();
        let statement = req.get_statement(&id).unwrap();
        assert_eq!(statement, keyspace.insert_statement::<u32, f32>());
        let _res = req.clone().send_local().unwrap();
    }

    #[tokio::test]
    async fn test_insert2() {
        use crate::prelude::*;
        use std::net::SocketAddr;
        std::env::set_var("RUST_LOG", "info");
        env_logger::init();
        let node: SocketAddr = std::env::var("SCYLLA_NODE").map_or_else(
            |_| ([127, 0, 0, 1], 9042).into(),
            |n| {
                n.parse()
                    .expect("Invalid SCYLLA_NODE env, use this format '127.0.0.1:19042' ")
            },
        );
        let runtime = Runtime::new(None, Scylla::new("datacenter1", num_cpus::get(), 2, Default::default()))
            .await
            .expect("Runtime failed to start!");
        let cluster_handle = runtime
            .handle()
            .cluster_handle()
            .await
            .expect("Failed to acquire cluster handle!");
        cluster_handle.add_node(node).await.expect("Failed to add node!");
        cluster_handle.build_ring(1).await.expect("Failed to build ring!");
        backstage::spawn_task("adding node task", async move {
            "scylla_example"
                .insert_query_with(
                    "INSERT INTO {{keyspace}}.test (key, data) VALUES (?, ?)",
                    &[&"Test 1"],
                    &[&1],
                )
                .build()?
                .send_local()?;
            Result::<_, RequestError>::Ok(())
        });
        runtime
            .block_on()
            .await
            .expect("Runtime failed to shutdown gracefully!")
    }
}

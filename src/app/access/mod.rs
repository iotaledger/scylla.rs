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
    worker::BasicRetryWorker,
    Worker,
    WorkerError,
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
        query::StatementType,
        Consistency,
        Decoder,
        DynValues,
        PreparedStatement,
        Query,
        QueryBuild,
        QueryBuilder,
        QueryConsistency,
        QueryOrPrepared,
        QueryPagingState,
        QuerySerialConsistency,
        QueryStatement,
        QueryValues,
        RowsDecoder,
        TokenEncodeChain,
        Values,
        VoidDecoder,
    },
    prelude::{
        ColumnEncoder,
        RetryableWorker,
        TokenEncoder,
    },
};
pub use batch::*;
pub use delete::{
    Delete,
    DeleteRequest,
    GetDynamicDeleteRequest,
    GetStaticDeleteRequest,
};
pub use insert::{
    AsDynamicInsertRequest,
    GetDynamicInsertRequest,
    GetStaticInsertRequest,
    Insert,
    InsertRequest,
};
pub use keyspace::Keyspace;
pub use select::{
    AsDynamicSelectRequest,
    GetDynamicSelectRequest,
    GetStaticSelectRequest,
    Select,
    SelectRequest,
};
use std::{
    borrow::Cow,
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

#[repr(u8)]
#[derive(Copy, Clone)]
pub enum RequestType {
    Insert = 0,
    Update = 1,
    Delete = 2,
    Select = 3,
    Batch = 4,
}

pub trait Statement: ToString {}
impl<T> Statement for T where T: ToString {}

pub struct DynamicRequest;
pub struct StaticRequest;
pub struct ManualBoundRequest {
    pub(crate) bind_fn: Box<
        dyn Fn(
            Box<dyn DynValues<Return = QueryBuilder<QueryValues>>>,
            &[&dyn TokenEncoder],
            &[&dyn ColumnEncoder],
        ) -> QueryBuilder<QueryValues>,
    >,
}

#[derive(Error, Debug)]
pub enum RequestError {
    #[error("Error sending to the Ring: {0}")]
    Ring(#[from] RingSendError),
    #[error("{0}")]
    Worker(#[from] WorkerError),
    #[error(transparent)]
    Other(#[from] anyhow::Error),
}

/// Defines a computed token for a key type
pub trait ComputeToken {
    /// Compute the token from the provided partition_key by using murmur3 hash function
    fn token(&self) -> i64;
}

pub trait Request {
    fn token(&self) -> i64;

    /// Get the statement that was used to create this request
    fn statement(&self) -> &Cow<'static, str>;

    /// Get the request payload
    fn payload(&self) -> &Vec<u8>;

    fn payload_mut(&mut self) -> &mut Vec<u8>;

    fn into_payload(self) -> Vec<u8>
    where
        Self: Sized;
}

#[async_trait::async_trait]
pub trait SendRequestExt: 'static + Request + Debug + Send + Sync + Sized {
    type Marker: 'static + Marker;
    const TYPE: RequestType;

    fn send_local(self) -> Result<DecodeResult<Self::Marker>, RequestError> {
        send_local(self.token(), self.payload().clone(), BasicRetryWorker::new(self))?;
        Ok(DecodeResult::new(Self::Marker::new(), Self::TYPE))
    }

    fn send_global(self) -> Result<DecodeResult<Self::Marker>, RequestError> {
        send_global(self.token(), self.payload().clone(), BasicRetryWorker::new(self))?;
        Ok(DecodeResult::new(Self::Marker::new(), Self::TYPE))
    }

    async fn get_local(self) -> Result<<Self::Marker as Marker>::Output, RequestError>
    where
        Self::Marker: Send + Sync,
    {
        BasicRetryWorker::new(self).get_local().await
    }

    fn get_local_blocking(self) -> Result<<Self::Marker as Marker>::Output, RequestError> {
        BasicRetryWorker::new(self).get_local_blocking()
    }

    async fn get_global(self) -> Result<<Self::Marker as Marker>::Output, RequestError>
    where
        Self::Marker: Send + Sync,
    {
        BasicRetryWorker::new(self).get_global().await
    }

    fn get_global_blocking(self) -> Result<<Self::Marker as Marker>::Output, RequestError> {
        BasicRetryWorker::new(self).get_global_blocking()
    }
}

#[derive(Debug, Clone)]
pub struct CommonRequest {
    pub(crate) token: i64,
    pub(crate) payload: Vec<u8>,
    pub(crate) statement: Cow<'static, str>,
}

impl CommonRequest {
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
        &self.statement()
    }

    fn payload(&self) -> &Vec<u8> {
        &self.payload
    }

    fn payload_mut(&mut self) -> &mut Vec<u8> {
        &mut self.payload
    }

    fn into_payload(self) -> Vec<u8> {
        self.payload
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
pub struct DecodeRows<V> {
    _marker: PhantomData<fn(V) -> V>,
}

impl<V> DecodeRows<V> {
    fn new() -> Self {
        Self { _marker: PhantomData }
    }
}

impl<'a, V: RowsDecoder> DecodeRows<V> {
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

pub trait Marker {
    type Output: Send;

    fn new() -> Self;

    fn try_decode(&self, d: Decoder) -> anyhow::Result<Self::Output> {
        Self::internal_try_decode(d)
    }

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

impl DecodeResult<DecodeVoid> {
    fn insert() -> Self {
        Self {
            inner: DecodeVoid,
            request_type: RequestType::Insert,
        }
    }
    fn update() -> Self {
        Self {
            inner: DecodeVoid,
            request_type: RequestType::Update,
        }
    }
    fn delete() -> Self {
        Self {
            inner: DecodeVoid,
            request_type: RequestType::Delete,
        }
    }
    fn batch() -> Self {
        Self {
            inner: DecodeVoid,
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
    use std::net::SocketAddr;

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

    #[allow(dead_code)]
    fn test_select() {
        let keyspace = MyKeyspace::new();
        let res: Result<DecodeResult<DecodeRows<f32>>, RequestError> = keyspace
            .select_with::<f32>(
                "SELECT col1 FROM keyspace.table WHERE key = ?",
                &[&3],
                StatementType::Query,
            )
            .consistency(Consistency::One)
            .build()
            .unwrap()
            .worker()
            .with_retries(3)
            .send_local();
        assert!(res.is_err());
        let res = "SELECT col1 FROM keyspace.table WHERE key = ?"
            .as_select_prepared::<f32>(&[&3])
            .consistency(Consistency::One)
            .build()
            .unwrap()
            .get_local_blocking();
        assert!(res.is_err());
        let res = keyspace
            .select_prepared::<f32>(&3u32)
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
        let _res = req2.clone().send_local();
    }

    #[allow(dead_code)]
    fn test_insert() {
        let keyspace = MyKeyspace { name: "mainnet".into() };
        let req = keyspace.insert(&3, &8.0).consistency(Consistency::One).build().unwrap();
        let _res = req.send_local();

        "my_keyspace"
            .insert_with(
                "INSERT INTO {keyspace}.table (key, val1, val2) VALUES (?,?,?)",
                &[&3],
                &[&8.0, &"hello"],
                StatementType::Query,
            )
            .bind_values(|builder, keys, values| builder.values(keys).values(values))
            .consistency(Consistency::One)
            .build()
            .unwrap()
            .get_local_blocking()
            .unwrap();

        "INSERT INTO my_keyspace.table (key, val1, val2) VALUES (?,?,?)"
            .as_insert_query(&[&3], &[&8.0, &"hello"])
            .bind_values(|builder, keys, values| builder.values(keys).values(values))
            .consistency(Consistency::One)
            .build()
            .unwrap()
            .send_local()
            .unwrap();
    }

    #[allow(dead_code)]
    fn test_update() {
        let keyspace = MyKeyspace { name: "mainnet".into() };
        let req = keyspace.update(&3, &8.0).consistency(Consistency::One).build().unwrap();

        let _res = req.send_local();
    }

    #[allow(dead_code)]
    fn test_delete() {
        let keyspace = MyKeyspace { name: "mainnet".into() };
        let req = keyspace
            .delete::<f32>(&3)
            .consistency(Consistency::One)
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
            .consistency(Consistency::One)
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
            .expect("runtime to run");
        let cluster_handle = runtime
            .handle()
            .cluster_handle()
            .await
            .expect("running scylla application");
        cluster_handle.add_node(node).await.expect("to add node");
        cluster_handle.build_ring(1).await.expect("to build ring");
        backstage::spawn_task("adding node task", async move {
            "INSERT INTO scylla_example.test (key, data) VALUES (?, ?)"
                .as_insert_query(&[&"Test 1"], &[&1])
                .consistency(Consistency::One)
                .build()?
                .send_local()?;
            Result::<_, RequestError>::Ok(())
        });
        runtime.block_on().await.expect("runtime to gracefully shutdown")
    }
}

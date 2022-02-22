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
pub use crate::{
    app::{
        ring::{
            shared::SharedRing,
            RingSendError,
        },
        stage::reporter::ReporterEvent,
    },
    cql::{
        compression::{
            Compression,
            Uncompressed,
        },
        Bindable,
        Binder,
        Consistency,
        Decoder,
        Query,
        QueryBuilder,
        RowsDecoder,
        VoidDecoder,
    },
    prelude::{
        IntoRespondingWorker,
        ReporterHandle,
        RetryableWorker,
        TokenEncoder,
    },
};
pub use batch::{
    BatchCollector,
    BatchRequest,
};
pub use delete::{
    AsDynamicDeleteRequest,
    Delete,
    DeleteBuilder,
    DeleteRequest,
    GetStaticDeleteRequest,
};
pub use execute::{
    AsDynamicExecuteRequest,
    ExecuteBuilder,
    ExecuteRequest,
};
pub use insert::{
    AsDynamicInsertRequest,
    GetStaticInsertRequest,
    Insert,
    InsertBuilder,
    InsertRequest,
};
pub use keyspace::Keyspace;
pub use prepare::{
    AsDynamicPrepareRequest,
    GetStaticPrepareRequest,
    PrepareRequest,
};
pub use scylla_parse::*;
pub use scylla_rs_macros::parse_statement;
pub use select::{
    AsDynamicSelectRequest,
    GetStaticSelectRequest,
    Select,
    SelectBuilder,
    SelectRequest,
};
pub use std::{
    borrow::Cow,
    convert::{
        TryFrom,
        TryInto,
    },
};
use std::{
    collections::BTreeMap,
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
    GetStaticUpdateRequest,
    Update,
    UpdateBuilder,
    UpdateRequest,
};

pub trait TableMetadata {
    const NAME: &'static str;
    const COLS: &'static [(&'static str, NativeType)];
    const PARTITION_KEY: &'static [&'static str];
    const CLUSTERING_COLS: &'static [(&'static str, Order)];
    type PartitionKey: TokenEncoder + Bindable;
    type PrimaryKey: TokenEncoder + Bindable;

    fn partition_key(&self) -> &Self::PartitionKey;

    fn primary_key(&self) -> &Self::PrimaryKey;
}

pub trait Table: TableMetadata {
    /// Options defined for this keyspace
    fn opts() -> Option<TableOpts> {
        if Self::CLUSTERING_COLS.len() > 0 {
            Some(
                TableOptsBuilder::default()
                    .clustering_order(Self::CLUSTERING_COLS.iter().map(|&(c, o)| (c, o).into()).collect())
                    .build()
                    .unwrap(),
            )
        } else {
            None
        }
    }

    /// Retrieve a CREATE KEYSPACE statement builder for this keyspace name
    fn create(keyspace: &dyn Keyspace) -> CreateTableStatement {
        let mut builder = scylla_parse::CreateTableStatementBuilder::default();
        builder
            .if_not_exists()
            .table(keyspace.name().dot(Self::NAME))
            .columns(Self::COLS.iter().map(|&c| c.into()).collect())
            .primary_key(
                PrimaryKey::partition_key(Self::PARTITION_KEY.to_vec())
                    .clustering_columns(Self::CLUSTERING_COLS.iter().map(|&(c, _)| c).collect()),
            );
        if let Some(opts) = Self::opts() {
            builder.options(opts);
        }
        builder.build().unwrap()
    }

    /// Retrieve a DROP KEYSPACE statement builder for this keyspace name
    fn drop(keyspace: &dyn Keyspace) -> DropTableStatement {
        scylla_parse::DropTableStatementBuilder::default()
            .table(keyspace.name().dot(Self::NAME))
            .if_exists()
            .build()
            .unwrap()
    }
}

pub trait IdExt {
    fn id(&self) -> [u8; 16];
}

impl<T: ToString> IdExt for T {
    fn id(&self) -> [u8; 16] {
        md5::compute(self.to_string().as_bytes()).into()
    }
}

pub trait TokenIndices: Table {
    fn select_indices(stmt: &SelectStatement) -> Vec<usize>;

    fn insert_indices(stmt: &InsertStatement) -> Vec<usize>;

    fn update_indices(stmt: &UpdateStatement) -> Vec<usize>;

    fn delete_indices(stmt: &DeleteStatement) -> Vec<usize>;

    fn check_term(term: &Term, col: Option<&String>, idx: &mut usize, map: &mut BTreeMap<usize, usize>) {
        match term {
            Term::FunctionCall(f) => {
                for t in f.args.iter() {
                    Self::check_term(t, col, idx, map)
                }
            }
            Term::ArithmeticOp { lhs, op: _, rhs } => {
                match lhs {
                    Some(t) => Self::check_term(&**t, col, idx, map),
                    _ => (),
                }
                Self::check_term(&**rhs, col, idx, map)
            }
            Term::BindMarker(_) => {
                if let Some(col) = col {
                    if let Some(key_idx) = Self::PARTITION_KEY.iter().position(|&c| c == col.as_str()) {
                        map.insert(key_idx, *idx);
                    }
                }
                *idx += 1;
            }
            _ => (),
        }
    }

    fn check_selector(selector: &Selector, col: Option<&String>, idx: &mut usize, map: &mut BTreeMap<usize, usize>) {
        match &selector.kind {
            SelectorKind::Term(t) => Self::check_term(t, col, idx, map),
            SelectorKind::Cast(s, _) => Self::check_selector(&**s, col, idx, map),
            SelectorKind::Function(f) => {
                for s in f.args.iter() {
                    Self::check_selector(s, col, idx, map)
                }
            }
            _ => (),
        }
    }

    fn check_where(where_clause: &WhereClause, idx: &mut usize, map: &mut BTreeMap<usize, usize>) {
        for r in where_clause.relations.iter() {
            match r {
                Relation::Normal {
                    column,
                    operator: _,
                    term,
                } => {
                    Self::check_term(
                        term,
                        Some(match column {
                            Name::Quoted(s) | Name::Unquoted(s) => s,
                        }),
                        idx,
                        map,
                    );
                }
                Relation::Token {
                    columns: _,
                    operator: _,
                    term,
                } => Self::check_term(term, None, idx, map),
                Relation::Tuple {
                    columns: _,
                    operator: _,
                    tuple_literal,
                } => {
                    for t in tuple_literal.elements.iter() {
                        Self::check_term(t, None, idx, map);
                    }
                }
                _ => (),
            }
        }
    }
}

impl<T: Table> TokenIndices for T {
    fn select_indices(stmt: &SelectStatement) -> Vec<usize> {
        let mut idx = 0;
        let mut map = BTreeMap::new();
        // Check select clause for bind markers
        match &stmt.select_clause {
            SelectClause::Selectors(selectors) => {
                for s in selectors.iter() {
                    Self::check_selector(s, None, &mut idx, &mut map);
                }
            }
            SelectClause::All => (),
        }
        // Check where clause for bind markers
        if let Some(where_clause) = &stmt.where_clause {
            Self::check_where(where_clause, &mut idx, &mut map);
        }
        map.into_values().collect()
    }

    fn insert_indices(stmt: &InsertStatement) -> Vec<usize> {
        let mut idx = 0;
        let mut map = BTreeMap::new();
        match &stmt.kind {
            InsertKind::NameValue { names, values } => {
                for (name, value) in names.iter().zip(values.elements.iter()) {
                    Self::check_term(
                        value,
                        Some(match name {
                            Name::Quoted(s) | Name::Unquoted(s) => s,
                        }),
                        &mut idx,
                        &mut map,
                    );
                }
            }
            _ => (),
        }
        map.into_values().collect()
    }

    fn update_indices(stmt: &UpdateStatement) -> Vec<usize> {
        let mut idx = 0;
        let mut map = BTreeMap::new();
        // Check set clause for bind markers
        for a in stmt.set_clause.iter() {
            match a {
                Assignment::Simple { selection, term } => {
                    match selection {
                        SimpleSelection::Term(_, t) => Self::check_term(t, None, &mut idx, &mut map),
                        _ => (),
                    }
                    Self::check_term(term, None, &mut idx, &mut map);
                }
                Assignment::Arithmetic {
                    assignee: _,
                    lhs: _,
                    op: _,
                    rhs,
                } => {
                    Self::check_term(rhs, None, &mut idx, &mut map);
                }
                Assignment::Append {
                    assignee: _,
                    list,
                    item: _,
                } => {
                    for t in list.elements.iter() {
                        Self::check_term(t, None, &mut idx, &mut map);
                    }
                }
            }
        }
        // Check where clause for bind markers
        Self::check_where(&stmt.where_clause, &mut idx, &mut map);
        map.into_values().collect()
    }

    fn delete_indices(stmt: &DeleteStatement) -> Vec<usize> {
        let mut idx = 0;
        let mut map = BTreeMap::new();
        // Check select clause for bind markers
        if let Some(selections) = &stmt.selections {
            for selection in selections.iter() {
                match selection {
                    SimpleSelection::Term(_, t) => Self::check_term(t, None, &mut idx, &mut map),
                    _ => (),
                }
            }
        }
        // Check where clause for bind markers
        Self::check_where(&stmt.where_clause, &mut idx, &mut map);
        map.into_values().collect()
    }
}

pub struct StaticRequest;
pub struct DynamicRequest;

/// The possible request types
#[allow(missing_docs)]
#[repr(u8)]
#[derive(Copy, Clone, Debug)]
pub enum RequestType {
    Insert = 0,
    Update = 1,
    Delete = 2,
    Select = 3,
    Batch = 4,
    Execute = 5,
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
    fn statement(&self) -> &String;

    /// Get the request payload
    fn payload(&self) -> Vec<u8>;

    /// get the keyspace of the request
    fn keyspace(&self) -> &Option<String>;
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

    /// Send this request and worker to a specific reporter, without waiting for a response
    fn send_to_reporter_with_worker<W: 'static + RetryableWorker<Self>>(
        self,
        reporter: &ReporterHandle,
        worker: Box<W>,
    ) -> Result<DecodeResult<Self::Marker>, RequestError> {
        worker.send_to_reporter(reporter)?;
        Ok(DecodeResult::new(Self::Marker::new(), Self::TYPE))
    }

    /// Send this request to the local datacenter, without waiting for a response
    fn send_local(self) -> Result<DecodeResult<Self::Marker>, RequestError> {
        send_local(
            self.keyspace().clone().as_ref().map(|s| s.as_str()),
            self.token(),
            self.payload(),
            self.worker(),
        )?;
        Ok(DecodeResult::new(Self::Marker::new(), Self::TYPE))
    }

    /// Send this request and worker to the local datacenter, without waiting for a response
    fn send_local_with_worker<W: 'static + Worker>(
        self,
        worker: Box<W>,
    ) -> Result<DecodeResult<Self::Marker>, RequestError> {
        send_local(
            self.keyspace().clone().as_ref().map(|s| s.as_str()),
            self.token(),
            self.payload(),
            worker,
        )?;
        Ok(DecodeResult::new(Self::Marker::new(), Self::TYPE))
    }
    /// Send this request to a global datacenter, without waiting for a response
    fn send_global(self) -> Result<DecodeResult<Self::Marker>, RequestError> {
        send_global(
            self.keyspace().clone().as_ref().map(|s| s.as_str()),
            self.token(),
            self.payload(),
            self.worker(),
        )?;
        Ok(DecodeResult::new(Self::Marker::new(), Self::TYPE))
    }

    /// Send this request and worker to a global datacenter, without waiting for a response
    fn send_global_with_worker<W: 'static + Worker>(
        self,
        worker: Box<W>,
    ) -> Result<DecodeResult<Self::Marker>, RequestError> {
        send_global(
            self.keyspace().as_ref().map(|s| s.as_str()),
            self.token(),
            self.payload(),
            worker,
        )?;
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

    /// Send this request to the local datacenter and await the response asynchronously
    async fn get_local_with_worker<W: 'static + RetryableWorker<Self>>(
        self,
        worker: Box<W>,
    ) -> Result<<Self::Marker as Marker>::Output, RequestError>
    where
        Self::Marker: Send + Sync,
        W: IntoRespondingWorker<Self, tokio::sync::oneshot::Sender<Result<Decoder, WorkerError>>, Decoder>,
    {
        worker.get_local().await
    }

    /// Send this request to the local datacenter and await the response synchronously
    fn get_local_blocking(self) -> Result<<Self::Marker as Marker>::Output, RequestError>
    where
        Self::Worker: IntoRespondingWorker<Self, tokio::sync::oneshot::Sender<Result<Decoder, WorkerError>>, Decoder>,
    {
        self.worker().get_local_blocking()
    }

    /// Send this request to the local datacenter and await the response synchronously
    fn get_local_blocking_with_worker<W: 'static + RetryableWorker<Self>>(
        self,
        worker: Box<W>,
    ) -> Result<<Self::Marker as Marker>::Output, RequestError>
    where
        W: IntoRespondingWorker<Self, tokio::sync::oneshot::Sender<Result<Decoder, WorkerError>>, Decoder>,
    {
        worker.get_local_blocking()
    }

    /// Send this request to a global datacenter and await the response asynchronously
    async fn get_global(self) -> Result<<Self::Marker as Marker>::Output, RequestError>
    where
        Self::Marker: Send + Sync,
        Self::Worker: IntoRespondingWorker<Self, tokio::sync::oneshot::Sender<Result<Decoder, WorkerError>>, Decoder>,
    {
        self.worker().get_global().await
    }

    /// Send this request to a global datacenter and await the response asynchronously
    async fn get_global_with_worker<W: 'static + RetryableWorker<Self>>(
        self,
        worker: Box<W>,
    ) -> Result<<Self::Marker as Marker>::Output, RequestError>
    where
        Self::Marker: Send + Sync,
        W: IntoRespondingWorker<Self, tokio::sync::oneshot::Sender<Result<Decoder, WorkerError>>, Decoder>,
    {
        worker.get_global().await
    }

    /// Send this request to a global datacenter and await the response synchronously
    fn get_global_blocking(self) -> Result<<Self::Marker as Marker>::Output, RequestError>
    where
        Self::Worker: IntoRespondingWorker<Self, tokio::sync::oneshot::Sender<Result<Decoder, WorkerError>>, Decoder>,
    {
        self.worker().get_global_blocking()
    }

    /// Send this request to a global datacenter and await the response synchronously
    fn get_global_blocking_with_worker<W: 'static + RetryableWorker<Self>>(
        self,
        worker: Box<W>,
    ) -> Result<<Self::Marker as Marker>::Output, RequestError>
    where
        W: IntoRespondingWorker<Self, tokio::sync::oneshot::Sender<Result<Decoder, WorkerError>>, Decoder>,
    {
        worker.get_global_blocking()
    }
}

/// A common request type which contains only the bare minimum information needed
#[derive(Debug, Clone)]
pub struct CommonRequest {
    pub(crate) token: i64,
    pub(crate) payload: Vec<u8>,
    pub(crate) keyspace: Option<String>,
    pub(crate) statement: String,
}

impl CommonRequest {
    #[allow(missing_docs)]
    pub fn new<T: Into<String>>(keyspace: Option<String>, statement: String, payload: Vec<u8>) -> Self {
        Self {
            token: 0,
            payload,
            keyspace,
            statement,
        }
    }
}

impl Request for CommonRequest {
    fn token(&self) -> i64 {
        self.token
    }

    fn statement(&self) -> &String {
        &self.statement
    }

    fn payload(&self) -> Vec<u8> {
        self.payload.clone()
    }

    fn keyspace(&self) -> &Option<String> {
        &self.keyspace
    }
}

/// Defines two helper methods to specify statement / id
#[allow(missing_docs)]
pub trait GetStatementIdExt: Keyspace + Sized {
    fn select_statement<T, K, O>(&self) -> SelectStatement
    where
        T: Select<K, O>,
        K: Bindable + TokenEncoder,
        O: RowsDecoder,
    {
        T::statement(self)
    }

    fn select_id<T, K, O>(&self) -> [u8; 16]
    where
        T: Select<K, O>,
        K: Bindable + TokenEncoder,
        O: RowsDecoder,
    {
        T::statement(self).id()
    }

    fn insert_statement<T, K>(&self) -> InsertStatement
    where
        T: Insert<K>,
        K: Bindable + TokenEncoder,
        T: Table,
    {
        T::statement(self)
    }

    fn insert_id<T, K>(&self) -> [u8; 16]
    where
        T: Insert<K>,
        K: Bindable + TokenEncoder,
        T: Table,
    {
        T::statement(self).id()
    }

    fn update_statement<T, K, V>(&self) -> UpdateStatement
    where
        T: Update<K, V>,
        K: Bindable + TokenEncoder,
        T: Table,
    {
        T::statement(self)
    }

    fn update_id<T, K, V>(&self) -> [u8; 16]
    where
        T: Update<K, V>,
        K: Bindable + TokenEncoder,
        T: Table,
    {
        T::statement(self).id()
    }

    fn delete_statement<T, K>(&self) -> DeleteStatement
    where
        T: Delete<K>,
        K: Bindable + TokenEncoder,
        T: Table,
    {
        T::statement(self)
    }

    fn delete_id<T, K>(&self) -> [u8; 16]
    where
        T: Delete<K>,
        K: Bindable + TokenEncoder,
    {
        T::statement(self).id()
    }
}

impl<S: Keyspace> GetStatementIdExt for S {}

#[derive(Debug, Error)]
pub enum TokenBindError<T: TokenEncoder> {
    #[error("Error binding values {0}")]
    BindError(#[from] <QueryBuilder as Binder>::Error),
    #[error("Error encoding token {0:?}")]
    TokenEncodeError(T::Error),
}

/// A marker struct which holds types used for a query
/// so that it may be decoded via `RowsDecoder` later
#[derive(Clone, Copy, Default, Debug)]
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
    pub fn decode<C: Compression>(&self, bytes: Vec<u8>) -> anyhow::Result<Option<V>> {
        V::try_decode_rows(Decoder::new::<C>(bytes)?)
    }
}

/// A marker struct which holds the keyspace type
/// so that it may be decoded (checked for errors)
/// via `VoidDecoder` later
#[derive(Copy, Clone, Debug)]
pub struct DecodeVoid;

impl DecodeVoid {
    /// Decode a result payload using the `VoidDecoder` impl
    #[inline]
    pub fn decode<C: Compression>(&self, bytes: Vec<u8>) -> anyhow::Result<()> {
        VoidDecoder::try_decode_void(Decoder::new::<C>(bytes)?)
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
#[derive(Debug, Clone)]
pub struct DecodeResult<T> {
    inner: T,
    /// Identify the type of request
    pub request_type: RequestType,
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
#[inline]
pub fn send_local(
    keyspace: Option<&str>,
    token: i64,
    payload: Vec<u8>,
    worker: Box<dyn Worker>,
) -> Result<(), RingSendError> {
    let request = ReporterEvent::Request { worker, payload };

    SharedRing::send_local_random_replica(keyspace, token, request)
}

/// Send a global request to the Ring
#[inline]
pub fn send_global(
    keyspace: Option<&str>,
    token: i64,
    payload: Vec<u8>,
    worker: Box<dyn Worker>,
) -> Result<(), RingSendError> {
    let request = ReporterEvent::Request { worker, payload };

    SharedRing::send_global_random_replica(keyspace, token, request)
}

impl<T> Deref for DecodeResult<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

#[cfg(test)]
mod tests {
    use scylla_rs_macros::parse_statement;

    use super::*;
    use crate::{
        cql::TokenEncodeChain,
        prelude::select::AsDynamicSelectRequest,
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

    impl Keyspace for MyKeyspace {
        fn opts(&self) -> KeyspaceOpts {
            KeyspaceOpts::default()
        }

        fn name(&self) -> &str {
            self.name.as_ref()
        }
    }

    pub struct MyTable {
        pub key: f32,
        pub val1: i32,
        pub val2: String,
    }

    impl TableMetadata for MyTable {
        const NAME: &'static str = "my_table";
        const COLS: &'static [(&'static str, NativeType)] = &[
            ("key", NativeType::Text),
            ("val1", NativeType::Int),
            ("val2", NativeType::Float),
        ];
        const PARTITION_KEY: &'static [&'static str] = &["key"];
        const CLUSTERING_COLS: &'static [(&'static str, Order)] = &[];

        type PartitionKey = f32;
        type PrimaryKey = f32;

        fn partition_key(&self) -> &Self::PartitionKey {
            &self.key
        }

        fn primary_key(&self) -> &Self::PrimaryKey {
            &self.key
        }
    }
    impl Table for MyTable {}

    impl TokenEncoder for MyTable {
        type Error = <<Self as TableMetadata>::PartitionKey as TokenEncoder>::Error;

        fn encode_token(&self) -> Result<TokenEncodeChain, Self::Error> {
            self.key.encode_token()
        }
    }

    impl Select<u32, f32> for MyTable {
        fn statement(keyspace: &dyn Keyspace) -> SelectStatement {
            parse_statement!("SELECT col1 FROM #.my_table WHERE key = ?", keyspace.name())
        }
    }

    impl Select<u32, i32> for MyTable {
        fn statement(keyspace: &dyn Keyspace) -> SelectStatement {
            parse_statement!("SELECT col2 FROM #.my_table WHERE key = ?", keyspace.name())
        }
    }

    impl Insert<(u32, f32, f32)> for MyTable {
        fn statement(keyspace: &dyn Keyspace) -> InsertStatement {
            parse_statement!(
                "INSERT INTO #.my_table (key, val1, val2) VALUES (?,?,?)",
                keyspace.name()
            )
        }
    }

    impl Update<u32, (f32, f32)> for MyTable {
        fn statement(keyspace: &dyn Keyspace) -> UpdateStatement {
            parse_statement!(
                "UPDATE #.my_table SET val1 = ?, val2 = ? WHERE key = ?",
                keyspace.name()
            )
        }

        fn bind_values<B: Binder>(binder: B, key: &u32, values: &(f32, f32)) -> Result<B, B::Error> {
            binder.bind(values)?.bind(key)
        }
    }

    impl Delete<u32> for MyTable {
        fn statement(keyspace: &dyn Keyspace) -> DeleteStatement {
            parse_statement!("DELETE FROM #.my_table WHERE key = ?", keyspace.name())
        }
    }

    #[allow(dead_code)]
    fn test_select() {
        let keyspace = MyKeyspace::new();
        let res = parse_statement!("SELECT col1 FROM #.my_table WHERE key = ?", keyspace.name())
            .query::<f32>()
            .bind(&3)
            .unwrap()
            .build()
            .unwrap()
            .worker()
            .with_retries(3)
            .send_local();
        assert!(res.is_err());
        let res = parse_statement!(r#"SELECT col1 FROM #."table" WHERE key = ?"#, keyspace.name())
            .query_prepared::<f32>()
            .bind(&3)
            .unwrap()
            .build()
            .unwrap()
            .get_local_blocking();
        assert!(res.is_err());
        let res = MyTable::select::<f32>(&keyspace, &3)
            .unwrap()
            .build()
            .unwrap()
            .get_local_blocking();
        assert!(res.is_err());
        let req2 = MyTable::select::<i32>(&keyspace, &3)
            .unwrap()
            .page_size(500)
            .build()
            .unwrap();
        let _res = req2.clone().send_local();
    }

    #[allow(dead_code)]
    fn test_insert() {
        let keyspace = MyKeyspace { name: "mainnet".into() };
        let req = MyTable::insert(&keyspace, &(3, 8.0, 7.5)).unwrap().build().unwrap();
        let _res = req.send_local();

        parse_statement!(
            "INSERT INTO #.my_table (key, val1, val2) VALUES (?,?,?)",
            keyspace.name()
        )
        .query()
        .bind(&(3, 8.0, 7.5))
        .unwrap()
        .build()
        .unwrap()
        .get_local_blocking()
        .unwrap();

        parse_statement!(
            "INSERT INTO #.my_table (key, val1, val2) VALUES (?,?,?)",
            keyspace.name()
        )
        .query_prepared()
        .bind(&(3, 8.0, 7.5))
        .unwrap()
        .build()
        .unwrap()
        .send_local()
        .unwrap();
    }

    #[allow(dead_code)]
    fn test_update() {
        let keyspace = MyKeyspace { name: "mainnet".into() };
        let req = MyTable::update(&keyspace, &3, &(8.0, 5.5)).unwrap().build().unwrap();

        let _res = req.send_local();
    }

    #[allow(dead_code)]
    fn test_delete() {
        let keyspace = MyKeyspace { name: "mainnet".into() };
        let req = MyTable::delete(&keyspace, &3)
            .unwrap()
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
            .insert::<MyTable, _>(&(3, 9.0, 3.0))
            .unwrap()
            .update::<MyTable, _, _>(&3, &(8.0, 1.0))
            .unwrap()
            .insert_prepared::<MyTable, _>(&(3, 8.0, 5.0))
            .unwrap()
            .delete_prepared::<MyTable, _>(&3)
            .unwrap()
            .build()
            .unwrap();
        let id = keyspace.insert_id::<MyTable, (u32, f32, f32)>();
        let statement = req.get_statement(&id).unwrap().clone();
        assert_eq!(
            statement,
            keyspace.insert_statement::<MyTable, (u32, f32, f32)>().to_string()
        );
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
        let runtime = Runtime::new(None, Scylla::default())
            .await
            .expect("Runtime failed to start!");
        let cluster_handle = runtime
            .handle()
            .cluster_handle()
            .await
            .expect("Failed to acquire cluster handle!");
        cluster_handle.add_node(node).await.expect("Failed to add node!");
        cluster_handle.build_ring().await.expect("Failed to build ring!");
        backstage::spawn_task("adding node task", async move {
            parse_statement!("INSERT INTO test (key, data) VALUES (?, ?)")
                .query()
                .bind(&(1, "test"))?
                .build()?
                .send_local()?;
            Result::<_, anyhow::Error>::Ok(())
        });
        runtime
            .block_on()
            .await
            .expect("Runtime failed to shutdown gracefully!")
    }
}

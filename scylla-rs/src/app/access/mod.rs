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

use self::{
    delete::DeleteTable,
    insert::InsertTable,
    select::SelectTable,
    update::UpdateTable,
};

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
        query::StatementType,
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
    DeleteRequest,
    GetStaticDeleteRequest,
};
pub use execute::{
    AsDynamicExecuteRequest,
    ExecuteRequest,
};
pub use insert::{
    AsDynamicInsertRequest,
    GetStaticInsertRequest,
    Insert,
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
    UpdateRequest,
};

pub trait Table: TokenEncoder {
    const NAME: &'static str;
    const COLS: &'static [&'static str];
    type PartitionKey;
    type PrimaryKey;
}

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
    fn statement(&self) -> Statement;

    /// Get the request payload
    fn payload(&self) -> Vec<u8>;

    /// get the keyspace of the request
    fn keyspace(&self) -> Option<String>;
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
            self.keyspace().as_ref().map(|s| s.as_str()),
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
            self.keyspace().as_ref().map(|s| s.as_str()),
            self.token(),
            self.payload(),
            worker,
        )?;
        Ok(DecodeResult::new(Self::Marker::new(), Self::TYPE))
    }
    /// Send this request to a global datacenter, without waiting for a response
    fn send_global(self) -> Result<DecodeResult<Self::Marker>, RequestError> {
        send_global(
            self.keyspace().as_ref().map(|s| s.as_str()),
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
    pub(crate) statement: DataManipulationStatement,
}

impl CommonRequest {
    #[allow(missing_docs)]
    pub fn new<T: Into<String>>(statement: DataManipulationStatement, payload: Vec<u8>) -> Self {
        Self {
            token: 0,
            payload,
            statement,
        }
    }
}

impl Request for CommonRequest {
    fn token(&self) -> i64 {
        self.token
    }

    fn statement(&self) -> Statement {
        self.statement.clone().into()
    }

    fn payload(&self) -> Vec<u8> {
        self.payload.clone()
    }

    fn keyspace(&self) -> Option<String> {
        self.statement.get_keyspace()
    }
}

/// Defines two helper methods to specify statement / id
#[allow(missing_docs)]
pub trait GetStatementIdExt {
    fn select_statement<T, K, O>(&self) -> SelectStatement
    where
        Self: SelectTable<T, K, O>,
        T: Select<Self, K, O>,
        K: Bindable + TokenEncoder,
        O: RowsDecoder,
    {
        T::statement(self)
    }

    fn select_id<T, K, O>(&self) -> [u8; 16]
    where
        Self: SelectTable<T, K, O>,
        T: Select<Self, K, O>,
        K: Bindable + TokenEncoder,
        O: RowsDecoder,
    {
        T::id(self)
    }

    fn insert_statement<T, K>(&self) -> InsertStatement
    where
        Self: InsertTable<T, K>,
        T: Insert<Self, K>,
        K: Bindable + TokenEncoder,
    {
        T::statement(self)
    }

    fn insert_id<T, K>(&self) -> [u8; 16]
    where
        Self: InsertTable<T, K>,
        T: Insert<Self, K>,
        K: Bindable + TokenEncoder,
    {
        T::id(self)
    }

    fn update_statement<T, K, V>(&self) -> UpdateStatement
    where
        Self: UpdateTable<T, K, V>,
        T: Update<Self, K, V>,
        K: Bindable + TokenEncoder,
    {
        T::statement(self)
    }

    fn update_id<T, K, V>(&self) -> [u8; 16]
    where
        Self: UpdateTable<T, K, V>,
        T: Update<Self, K, V>,
        K: Bindable + TokenEncoder,
    {
        T::id(self)
    }

    fn delete_statement<T, K>(&self) -> DeleteStatement
    where
        Self: DeleteTable<T, K>,
        T: Delete<Self, K>,
        K: Bindable + TokenEncoder,
    {
        T::statement(self)
    }

    fn delete_id<T, K>(&self) -> [u8; 16]
    where
        Self: DeleteTable<T, K>,
        T: Delete<Self, K>,
        K: Bindable + TokenEncoder,
    {
        T::id(self)
    }
}

impl<S: Keyspace> GetStatementIdExt for S {}

#[derive(Debug, Error)]
pub enum StaticQueryError<T: TokenEncoder> {
    #[error("Error binding values {0}")]
    BindError(#[from] <QueryBuilder as Binder>::Error),
    #[error("Error encoding token {0:?}")]
    TokenEncodeError(T::Error),
}

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
    #[inline]
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

#[cfg(feature = "testy")]
mod testy {
    use crate::prelude::{
        ColumnValue,
        Row,
        Rows,
    };

    use super::{
        BindableValue,
        Binder,
        Consistency,
        QueryBuilder,
        QueryValues,
    };
    use scylla_parse::*;
    use scylla_rs_macros::parse_statement;

    pub struct TransactionsTable {
        transaction_id: String,
        idx: u16,
        variant: String,
        message_id: String,
        version: u8,
        data: Vec<u8>,
        inclusion_state: Option<u8>,
        milestone_index: Option<u32>,
    }

    // Derived
    pub struct TransactionsTableStmtBuilder<B> {
        builder: B,
    }

    // Derived
    impl TransactionsTableStmtBuilder<SelectStatementBuilder> {
        fn transaction_id(self) -> Self {}
    }

    pub trait Table {
        const NAME: &'static str;
        const COLS: &'static [&'static str];
        type PartitionKey;
        type PrimaryKey;

        fn bind_values<B: Binder>(&self, binder: B) -> B;

        fn insert<K: AsRef<str>>(keyspace: K) -> InsertStatementBuilder {
            let mut stmt = InsertStatementBuilder::default();
            stmt.table(keyspace.as_ref().dot(Self::NAME));
            stmt.kind(
                InsertKind::name_value(
                    Self::COLS.iter().map(|&c| c.into()).collect(),
                    Self::COLS.iter().map(|_| BindMarker::Anonymous.into()).collect(),
                )
                .unwrap(),
            );
            stmt
        }

        fn select<K: AsRef<str>>(keyspace: K) -> SelectStatementBuilder {
            let mut stmt = SelectStatementBuilder::default();
            stmt.from(keyspace.as_ref().dot(Self::NAME));
            stmt
        }

        fn id<S: ToString + Into<Statement>>(stmt: S) -> [u8; 16] {
            md5::compute(stmt.to_string().as_bytes()).into()
        }
    }

    // Derived
    impl Table for TransactionsTable {
        const NAME: &'static str = "transactions";
        const COLS: &'static [&'static str] = &[
            "transaction_id",
            "idx",
            "variant",
            "message_id",
            "version",
            "data",
            "inclusion_state",
            "milestone_index",
        ];
        type PartitionKey = String;
        type PrimaryKey = (String, u16, String, String);

        fn bind_values<B: Binder>(&self, binder: B) -> B {
            binder
                .value(&self.transaction_id)
                .value(&self.idx)
                .value(&self.variant)
                .value(&self.message_id)
                .value(&self.version)
                .value(&self.data)
                .value(&self.inclusion_state)
                .value(&self.milestone_index)
        }
    }

    // Derived
    impl Row for TransactionsTable {
        fn try_decode_row<R: Rows + ColumnValue>(rows: &mut R) -> anyhow::Result<Self>
        where
            Self: Sized,
        {
            Ok(Self {
                transaction_id: todo!(),
                idx: todo!(),
                variant: todo!(),
                message_id: todo!(),
                version: todo!(),
                data: todo!(),
                inclusion_state: todo!(),
                milestone_index: todo!(),
            })
        }
    }

    async fn potential_select_syntax() {
        TransactionsTable::select_all("my_keyspace") // select * from my_keyspace.transactions
            .transaction_id(Operator::Equal, "129386sdgh481hsd262395asfs876") // where transaction_id = '129386sdgh481hsd262395asfs876'
            .idx(Operator::GreaterThan, 0) // and idx > 0
            .consistency(Consistency::One)
            .page_size(100)
            .build()
            .get_local()
            .await?;

        TransactionsTable::select("my_keyspace", &["message_id", "inclusion_state", "milestone_index"])
            .transaction_id(Operator::Equal, "129386sdgh481hsd262395asfs876")
            .idx(Operator::GreaterThan, 0)
            .group_by(&["version", "variant"])
            .order_by(&["version", "variant"])
            .consistency(Consistency::One)
            .page_size(100)
            .build()
            .get_local()
            .await?;

        TransactionsTable::in_keyspace("my_keyspace")
            .select_all() // select * from my_keyspace.transactions
            .transaction_id_bind(Operator::Equal) // where transaction_id = ?
            .idx_bind(Operator::GreaterThan) // and idx > ?
            .consistency(Consistency::One)
            .page_size(100)
            .build()
            .get_local()
            .await?;

        TransactionsTable::in_keyspace("my_keyspace")
            .select(&[
                // consts generated by Table derive?
                TransactionsTable::MESSAGE_ID,
                TransactionsTable::INCLUSION_STATE,
                TransactionsTable::MILESTONE_INDEX,
            ]) // select message_id, inclusion_state, milestone_index from my_keyspace.transactions
            .transaction_id(Operator::Equal, BindMarker::Anonymous) // where transaction_id = ?
            .idx(Operator::GreaterThan, 0) // and idx > 0
            .group_by(&[TransactionsTable::VERSION, TransactionsTable::VARIANT]) // group by version, variant
            .order_by(&[TransactionsTable::VERSION, TransactionsTable::VARIANT]) // order by version, variant
            .consistency(Consistency::One)
            .page_size(100)
            .build()
            .get_local()
            .await?;

        TransactionsTable::in_keyspace("my_keyspace")
            .distinct()
            .select_message_id()
            .select_inclusion_state()
            .select_milestone_index() // select distinct message_id, inclusion_state, milestone_index from my_keyspace.transactions
            .where_transaction_id(Operator::Equal, "129386sdgh481hsd262395asfs876") // where transaction_id = '129386sdgh481hsd262395asfs876'
            .where_idx(Operator::GreaterThan, 0) // and idx > 0
            .group_by_version()
            .group_by_variant() // group by version, variant
            .order_by_version()
            .order_by_variant() // order by version, variant
            .consistency(Consistency::One)
            .page_size(100)
            .build()
            .get_local()
            .await?;

        TransactionsTable::in_keyspace("my_keyspace")
            .select(
                // Type generated by the Table derive
                TransactionsTableColumns
                    .message_id()
                    .inclusion_state()
                    .milestone_index(),
            ) // select message_id, inclusion_state, milestone_index from my_keyspace.transactions
            .where_clause(
                TransactionsTableColumns::transaction_id(Operator::Equal, "129386sdgh481hsd262395asfs876")
                    .and(TransactionsTableColumns::idx(Operator::GreaterThan, 0)),
            ) // where transaction_id = '129386sdgh481hsd262395asfs876' and idx > 0
            .group_by(TransactionsTableColumns.version().variant())
            .order_by(TransactionsTableColumns.version().variant())
            .consistency(Consistency::One)
            .page_size(100)
            .build()
            .get_local()
            .await?;

        // Since TransactionsTable is user defined, fns can be defined on it to simply
        // return a SelectStatement(/Builder) which can be manually created or parsed from
        // a string using parse_statement!()
        TransactionsTable::my_pre_defined_select() // Returns built SelectStatement
            .consistency(Consistency::One)
            .page_size(100)
            .build()
            .get_local()
            .await?;
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

    impl Table for MyTable {
        const NAME: &'static str = "my_table";
        const COLS: &'static [&'static str] = &["key", "val1", "val2"];

        type PartitionKey = f32;
        type PrimaryKey = f32;
    }

    impl TokenEncoder for MyTable {
        type Error = <<Self as Table>::PartitionKey as TokenEncoder>::Error;

        fn encode_token(&self) -> Result<TokenEncodeChain, Self::Error> {
            self.key.encode_token()
        }
    }

    impl<S: Keyspace> Select<S, u32, f32> for MyTable {
        fn statement(keyspace: &S) -> SelectStatement {
            parse_statement!("SELECT col1 FROM #.my_table WHERE key = ?", keyspace.name())
        }
    }

    impl<S: Keyspace> Select<S, u32, i32> for MyTable {
        fn statement(keyspace: &S) -> SelectStatement {
            parse_statement!("SELECT col2 FROM #.my_table WHERE key = ?", keyspace.name())
        }
    }

    impl<S: Keyspace> Insert<S, (u32, f32, f32)> for MyTable {
        fn statement(keyspace: &S) -> InsertStatement {
            parse_statement!(
                "INSERT INTO #.my_table (key, val1, val2) VALUES (?,?,?)",
                keyspace.name()
            )
        }
    }

    impl<S: Keyspace> Update<S, u32, (f32, f32)> for MyTable {
        fn statement(keyspace: &S) -> UpdateStatement {
            parse_statement!(
                "UPDATE #.my_table SET val1 = ?, val2 = ? WHERE key = ?",
                keyspace.name()
            )
        }

        fn bind_values<B: Binder>(binder: &mut B, key: &u32, values: &(f32, f32)) -> Result<(), B::Error> {
            binder.bind(values)?.bind(key)?;
            Ok(())
        }
    }

    impl<S: Keyspace> Delete<S, u32> for MyTable {
        fn statement(keyspace: &S) -> DeleteStatement {
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
            .prepared::<f32>()
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
        .prepared()
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
            keyspace.insert_statement::<MyTable, (u32, f32, f32)>().into()
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

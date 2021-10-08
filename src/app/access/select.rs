// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use std::fmt::Debug;

use super::*;
use crate::{
    cql::{
        query::StatementType,
        QueryPagingState,
        QuerySerialConsistency,
        TokenEncodeChain,
    },
    prelude::TokenEncoder,
};

/// Select query trait which creates a `SelectRequest`
/// that can be sent to the `Ring`.
///
/// ## Examples
/// ```
/// use scylla_rs::{
///     app::{
///         access::{
///             ComputeToken,
///             GetSelectRequest,
///             Keyspace,
///             Select,
///         },
///         worker::{
///             ValueWorker,
///             WorkerError,
///         },
///     },
///     cql::{
///         Batch,
///         Consistency,
///         PreparedStatement,
///         RowsDecoder,
///         Values,
///     },
/// };
/// use std::borrow::Cow;
/// # use scylla_rs::cql::Decoder;
/// # #[derive(Default, Clone, Debug)]
/// # struct MyKeyspace {
/// #     pub name: Cow<'static, str>,
/// # }
/// #
/// # impl MyKeyspace {
/// #     pub fn new() -> Self {
/// #         Self {
/// #             name: "my_keyspace".into(),
/// #         }
/// #     }
/// # }
///
/// # impl Keyspace for MyKeyspace {
/// #     fn name(&self) -> &Cow<'static, str> {
/// #         &self.name
/// #     }
/// # }
/// # impl ComputeToken<i32> for MyKeyspace {
/// #     fn token(_key: &i32) -> i64 {
/// #         rand::random()
/// #     }
/// # }
/// # type MyKeyType = i32;
/// # type MyValueType = f32;
/// # impl RowsDecoder<MyKeyType, MyValueType> for MyKeyspace {
/// #     type Row = f32;
/// #     fn try_decode(decoder: Decoder) -> anyhow::Result<Option<f32>> {
/// #         todo!()
/// #     }
/// # }
/// impl Select<MyKeyType, MyValueType> for MyKeyspace {
///     type QueryOrPrepared = PreparedStatement;
///     fn statement(&self) -> Cow<'static, str> {
///         format!("SELECT * FROM {}.table where key = ?", self.name()).into()
///     }
///
///     fn bind_values<T: Values>(builder: T, key: &MyKeyType) -> T::Return {
///         builder.value(key)
///     }
/// }
///
/// # let keyspace = MyKeyspace::new();
/// # let my_key = 1;
/// let (sender, receiver) = tokio::sync::mpsc::unbounded_channel::<Result<Option<MyValueType>, WorkerError>>();
/// # use std::marker::PhantomData;
/// let worker = ValueWorker::boxed(sender, keyspace.clone(), my_key, 3, PhantomData::<MyValueType>);
///
/// let request = keyspace // A Scylla keyspace
///     .select::<MyValueType>(&my_key) // Get the Select Request by specifying the value type
///     .consistency(Consistency::One)
///     .build()?;
/// # Ok::<(), anyhow::Error>(())
/// ```
pub trait Select<K, V>: Keyspace {
    /// Set the query type; `QueryStatement` or `PreparedStatement`
    type QueryOrPrepared: QueryOrPrepared;

    /// Create your select statement here.
    fn statement(&self) -> Cow<'static, str>;

    /// Get the MD5 hash of this implementation's statement
    /// for use when generating queries that should use
    /// the prepared statement.
    fn id(&self) -> [u8; 16] {
        md5::compute(self.select_statement().as_bytes()).into()
    }
    /// Bind the cql values to the builder
    fn bind_values<T: Values>(builder: T, key: &K) -> Box<T::Return>;
}

/// Defines a helper method to specify the Value type
/// expected by the `Select` trait.
pub trait GetStaticSelectRequest<K>: Keyspace {
    /// Specifies the returned Value type for an upcoming select request
    fn select<'a, V>(&'a self, key: &'a K) -> SelectBuilder<'a, Self, K, V, QueryConsistency, StaticRequest>
    where
        Self: Select<K, V>,
    {
        SelectBuilder {
            _marker: PhantomData,
            keyspace: self,
            statement: self.statement(),
            key,
            builder: Self::QueryOrPrepared::encode_statement(Query::new(), &self.statement()),
        }
    }
    /// Specifies the returned Value type for an upcoming select request using a query statement
    fn select_query<'a, V>(&'a self, key: &'a K) -> SelectBuilder<'a, Self, K, V, QueryConsistency, StaticRequest>
    where
        Self: Select<K, V>,
    {
        SelectBuilder {
            _marker: PhantomData,
            keyspace: self,
            statement: self.statement(),
            key,
            builder: QueryStatement::encode_statement(Query::new(), &self.statement()),
        }
    }
    /// Specifies the returned Value type for an upcoming select request using a prepared statement id
    fn select_prepared<'a, V>(&'a self, key: &'a K) -> SelectBuilder<'a, Self, K, V, QueryConsistency, StaticRequest>
    where
        Self: Select<K, V>,
    {
        SelectBuilder {
            _marker: PhantomData,
            keyspace: self,
            statement: self.statement(),
            key,
            builder: PreparedStatement::encode_statement(Query::new(), &self.statement()),
        }
    }
}

pub trait GetDynamicSelectRequest: Keyspace {
    /// Specifies the returned Value type for an upcoming select request
    fn select_with<'a, V>(
        &'a self,
        statement: &str,
        variables: &'a [&dyn TokenEncoder],
        statement_type: StatementType,
    ) -> SelectBuilder<'a, Self, [&'a dyn TokenEncoder], V, QueryConsistency, DynamicRequest> {
        match statement_type {
            StatementType::Query => self.select_query_with(statement, variables),
            StatementType::Prepared => self.select_prepared_with(statement, variables),
        }
    }
    /// Specifies the returned Value type for an upcoming select request using a query statement
    fn select_query_with<'a, V>(
        &'a self,
        statement: &str,
        variables: &'a [&dyn TokenEncoder],
    ) -> SelectBuilder<'a, Self, [&dyn TokenEncoder], V, QueryConsistency, DynamicRequest> {
        SelectBuilder {
            _marker: PhantomData,
            keyspace: self,
            statement: statement.to_owned().into(),
            key: variables,
            builder: QueryStatement::encode_statement(Query::new(), statement),
        }
    }
    /// Specifies the returned Value type for an upcoming select request using a prepared statement id
    fn select_prepared_with<'a, V>(
        &'a self,
        statement: &str,
        variables: &'a [&dyn TokenEncoder],
    ) -> SelectBuilder<'a, Self, [&dyn TokenEncoder], V, QueryConsistency, DynamicRequest> {
        SelectBuilder {
            _marker: PhantomData,
            keyspace: self,
            statement: statement.to_owned().into(),
            key: variables,
            builder: PreparedStatement::encode_statement(Query::new(), statement),
        }
    }
}

pub trait AsDynamicSelectRequest: Statement
where
    Self: Sized,
{
    /// Specifies the returned Value type for an upcoming select request
    fn as_select<'a, V>(
        &'a self,
        variables: &'a [&dyn TokenEncoder],
        statement_type: StatementType,
    ) -> SelectBuilder<'a, Self, [&'a dyn TokenEncoder], V, QueryConsistency, DynamicRequest> {
        match statement_type {
            StatementType::Query => self.as_select_query(variables),
            StatementType::Prepared => self.as_select_prepared(variables),
        }
    }
    /// Specifies the returned Value type for an upcoming select request using a query statement
    fn as_select_query<'a, V>(
        &'a self,
        variables: &'a [&dyn TokenEncoder],
    ) -> SelectBuilder<'a, Self, [&dyn TokenEncoder], V, QueryConsistency, DynamicRequest> {
        SelectBuilder {
            _marker: PhantomData,
            keyspace: self,
            statement: self.to_string().to_owned().into(),
            key: variables,
            builder: QueryStatement::encode_statement(Query::new(), &self.to_string()),
        }
    }
    /// Specifies the returned Value type for an upcoming select request using a prepared statement id
    fn as_select_prepared<'a, V>(
        &'a self,
        variables: &'a [&dyn TokenEncoder],
    ) -> SelectBuilder<'a, Self, [&dyn TokenEncoder], V, QueryConsistency, DynamicRequest> {
        SelectBuilder {
            _marker: PhantomData,
            keyspace: self,
            statement: self.to_string().to_owned().into(),
            key: variables,
            builder: PreparedStatement::encode_statement(Query::new(), &self.to_string()),
        }
    }
}

impl<S: Keyspace, K> GetStaticSelectRequest<K> for S {}
impl<S: Keyspace> GetDynamicSelectRequest for S {}
impl<S: Statement> AsDynamicSelectRequest for S {}

pub struct SelectBuilder<'a, S, K: ?Sized, V, Stage, T> {
    keyspace: &'a S,
    statement: Cow<'static, str>,
    key: &'a K,
    builder: QueryBuilder<Stage>,
    _marker: PhantomData<fn(V, T) -> (V, T)>,
}

impl<'a, S: Select<K, V>, K, V> SelectBuilder<'a, S, K, V, QueryConsistency, StaticRequest> {
    pub fn consistency(self, consistency: Consistency) -> SelectBuilder<'a, S, K, V, QueryValues, StaticRequest> {
        SelectBuilder {
            _marker: self._marker,
            keyspace: self.keyspace,
            statement: self.statement,
            key: self.key,
            builder: *S::bind_values(self.builder.consistency(consistency), &self.key),
        }
    }
}

impl<'a, S: Keyspace, V> SelectBuilder<'a, S, [&dyn TokenEncoder], V, QueryConsistency, DynamicRequest> {
    pub fn consistency(
        self,
        consistency: Consistency,
    ) -> SelectBuilder<'a, S, [&'a dyn TokenEncoder], V, QueryValues, DynamicRequest> {
        let builder = self.builder.consistency(consistency);
        let builder = *match self.key.len() {
            0 => builder.null_value(),
            _ => {
                let mut iter = self.key.iter();
                let mut builder = builder.value(iter.next().unwrap());
                for v in iter {
                    builder = builder.value(v);
                }
                builder
            }
        };
        SelectBuilder {
            _marker: self._marker,
            keyspace: self.keyspace,
            statement: self.statement,
            key: self.key,
            builder,
        }
    }
}

impl<'a, S, K, V, T> SelectBuilder<'a, S, K, V, QueryValues, T> {
    pub fn page_size(self, page_size: i32) -> SelectBuilder<'a, S, K, V, QueryPagingState, T> {
        SelectBuilder {
            _marker: self._marker,
            keyspace: self.keyspace,
            statement: self.statement,
            key: self.key,
            builder: self.builder.page_size(page_size),
        }
    }
    /// Set the paging state.
    pub fn paging_state(self, paging_state: &Option<Vec<u8>>) -> SelectBuilder<'a, S, K, V, QuerySerialConsistency, T> {
        SelectBuilder {
            _marker: self._marker,
            keyspace: self.keyspace,
            statement: self.statement,
            key: self.key,
            builder: self.builder.paging_state(paging_state),
        }
    }
    pub fn timestamp(self, timestamp: i64) -> SelectBuilder<'a, S, K, V, QueryBuild, T> {
        SelectBuilder {
            _marker: self._marker,
            keyspace: self.keyspace,
            statement: self.statement,
            key: self.key,
            builder: self.builder.timestamp(timestamp),
        }
    }
}
impl<'a, S: Keyspace, V: RowsDecoder> SelectBuilder<'a, S, [&dyn TokenEncoder], V, QueryValues, DynamicRequest> {
    /// Build the SelectRequest
    pub fn build(self) -> anyhow::Result<SelectRequest<V>> {
        let mut token_chain = TokenEncodeChain::default();
        for v in self.key.iter() {
            token_chain.dyn_chain(*v);
        }
        let query = self.builder.build()?;
        // create the request
        Ok(SelectRequest {
            token: token_chain.finish(),
            inner: query.into(),
            statement: self.statement,
            _marker: PhantomData,
        })
    }
}

impl<'a, S: Keyspace, K: ComputeToken, V: RowsDecoder> SelectBuilder<'a, S, K, V, QueryValues, StaticRequest> {
    /// Build the SelectRequest
    pub fn build(self) -> anyhow::Result<SelectRequest<V>> {
        let query = self.builder.build()?;
        // create the request
        Ok(SelectRequest {
            token: self.key.token(),
            inner: query.into(),
            statement: self.statement,
            _marker: PhantomData,
        })
    }
}

impl<'a, S: Keyspace, V: RowsDecoder> SelectBuilder<'a, S, [&dyn TokenEncoder], V, QueryBuild, DynamicRequest> {
    /// Build the SelectRequest
    pub fn build_encoded(self) -> anyhow::Result<SelectRequest<V>> {
        let mut token_chain = TokenEncodeChain::default();
        for v in self.key.iter() {
            token_chain.dyn_chain(*v);
        }
        let query = self.builder.build()?;
        // create the request
        Ok(SelectRequest {
            token: token_chain.finish(),
            inner: query.into(),
            statement: self.statement,
            _marker: PhantomData,
        })
    }
}

impl<'a, S: Keyspace, K: ComputeToken, V: RowsDecoder> SelectBuilder<'a, S, K, V, QueryBuild, StaticRequest> {
    /// Build the SelectRequest
    pub fn build(self) -> anyhow::Result<SelectRequest<V>> {
        let query = self.builder.build()?;
        // create the request
        Ok(SelectRequest {
            token: self.key.token(),
            inner: query.into(),
            statement: self.statement,
            _marker: PhantomData,
        })
    }
}

impl<'a, S, K, V, T> SelectBuilder<'a, S, K, V, QueryPagingState, T> {
    /// Set the paging state in the query frame.
    pub fn paging_state(self, paging_state: &Option<Vec<u8>>) -> SelectBuilder<'a, S, K, V, QuerySerialConsistency, T> {
        SelectBuilder {
            _marker: self._marker,
            keyspace: self.keyspace,
            statement: self.statement,
            key: self.key,
            builder: self.builder.paging_state(paging_state),
        }
    }

    /// Set the timestamp of the query frame.
    pub fn timestamp(self, timestamp: i64) -> SelectBuilder<'a, S, K, V, QueryBuild, T> {
        SelectBuilder {
            _marker: self._marker,
            keyspace: self.keyspace,
            statement: self.statement,
            key: self.key,
            builder: self.builder.timestamp(timestamp),
        }
    }
}
impl<'a, S: Keyspace, K: ComputeToken, V: RowsDecoder, T> SelectBuilder<'a, S, K, V, QueryPagingState, T> {
    pub fn build(self) -> anyhow::Result<SelectRequest<V>> {
        let query = self.builder.build()?;
        // create the request
        Ok(SelectRequest {
            token: self.key.token(),
            inner: query.into(),
            statement: self.statement,
            _marker: PhantomData,
        })
    }
}
impl<'a, S, K, V, T> SelectBuilder<'a, S, K, V, QuerySerialConsistency, T> {
    /// Set the timestamp of the query frame.
    pub fn timestamp(self, timestamp: i64) -> SelectBuilder<'a, S, K, V, QueryBuild, T> {
        SelectBuilder {
            _marker: self._marker,
            keyspace: self.keyspace,
            statement: self.statement,
            key: self.key,
            builder: self.builder.timestamp(timestamp),
        }
    }
}

impl<'a, S: Keyspace, K: ComputeToken, V: RowsDecoder, T> SelectBuilder<'a, S, K, V, QuerySerialConsistency, T> {
    pub fn build(self) -> anyhow::Result<SelectRequest<V>> {
        let query = self.builder.build()?;
        // create the request
        Ok(SelectRequest {
            token: self.key.token(),
            inner: query.into(),
            statement: self.statement,
            _marker: PhantomData,
        })
    }
}

/// A request to select a record which can be sent to the ring
pub struct SelectRequest<V> {
    token: i64,
    inner: Vec<u8>,
    statement: Cow<'static, str>,
    _marker: PhantomData<fn(V) -> V>,
}

impl<V> Debug for SelectRequest<V> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SelectRequest")
            .field("token", &self.token)
            .field("inner", &self.inner)
            .field("statement", &self.statement)
            .finish()
    }
}

impl<V> Clone for SelectRequest<V> {
    fn clone(&self) -> Self {
        Self {
            token: self.token,
            inner: self.inner.clone(),
            statement: self.statement.clone(),
            _marker: PhantomData,
        }
    }
}

impl<V: 'static> Request for SelectRequest<V> {
    type Marker = DecodeRows<V>;
    const TYPE: RequestType = RequestType::Select;

    fn token(&self) -> i64 {
        self.token
    }

    fn marker() -> Self::Marker {
        DecodeRows::new()
    }

    fn statement(&self) -> &Cow<'static, str> {
        &self.statement
    }

    fn payload(&self) -> &Vec<u8> {
        &self.inner
    }

    fn into_payload(self) -> Vec<u8> {
        self.inner
    }
}

impl<V> SelectRequest<V> {
    /// Return DecodeResult marker type, useful in case the worker struct wants to hold the
    /// decoder in order to decode the response inside handle_response method.
    pub fn result_decoder(&self) -> DecodeResult<DecodeRows<V>> {
        DecodeResult::select()
    }

    pub fn send_local(self) -> Result<DecodeResult<<Self as Request>::Marker>, RequestError>
    where
        Self: 'static + Sized,
    {
        send_local(self.token(), self.into_payload(), BasicWorker::new())?;
        Ok(DecodeResult::new(<Self as Request>::marker(), <Self as Request>::TYPE))
    }

    pub fn send_global(self) -> Result<DecodeResult<<Self as Request>::Marker>, RequestError>
    where
        Self: 'static + Sized,
    {
        send_global(self.token(), self.into_payload(), BasicWorker::new())?;
        Ok(DecodeResult::new(<Self as Request>::marker(), <Self as Request>::TYPE))
    }

    pub fn worker(self) -> Box<BasicRetryWorker<Self>> {
        BasicRetryWorker::new(self)
    }
}
impl<V> SelectRequest<V>
where
    V: 'static + Send + RowsDecoder,
{
    pub async fn get_local(self) -> Result<Option<V>, RequestError> {
        self.worker().get_local().await
    }

    pub fn get_local_blocking(self) -> Result<Option<V>, RequestError> {
        self.worker().get_local_blocking()
    }

    pub async fn get_global(self) -> Result<Option<V>, RequestError> {
        self.worker().get_global().await
    }

    pub fn get_global_blocking(self) -> Result<Option<V>, RequestError> {
        self.worker().get_global_blocking()
    }
}

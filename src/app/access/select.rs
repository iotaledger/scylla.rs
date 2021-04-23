// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use super::*;
use crate::cql::{QueryPagingState, QuerySerialConsistency};

/// Select query trait which creates a `SelectRequest`
/// that can be sent to the `Ring`.
///
/// ## Examples
/// ```
/// use scylla_rs::{
///     app::{
///         access::{ComputeToken, GetSelectRequest, Keyspace, Select},
///         worker::{ValueWorker, WorkerError},
///     },
///     cql::{Batch, Consistency, PreparedStatement, RowsDecoder, Values},
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
pub trait Select<K, V>: Keyspace + RowsDecoder<K, V> + ComputeToken<K> {
    /// Set the query type; `QueryStatement` or `PreparedStatement`
    type QueryOrPrepared: SelectRecommended<Self, K, V>;

    /// Create your select statement here.
    fn statement(&self) -> Cow<'static, str>;

    /// Get the MD5 hash of this implementation's statement
    /// for use when generating queries that should use
    /// the prepared statement.
    fn id(&self) -> [u8; 16] {
        md5::compute(self.select_statement().as_bytes()).into()
    }
    /// Bind the cql values to the builder
    fn bind_values<T: Values>(builder: T, key: &K) -> T::Return;
}

pub trait SelectRecommended<S: Select<K, V>, K, V>: QueryOrPrepared {
    fn make<T: Statements>(query_or_batch: T, keyspace: &S) -> T::Return {
        Self::encode_statement(query_or_batch, &keyspace.statement())
    }
}

impl<S: Select<K, V>, K, V> SelectRecommended<S, K, V> for QueryStatement {}

impl<S: Select<K, V>, K, V> SelectRecommended<S, K, V> for PreparedStatement {}

/// Defines a helper method to specify the Value type
/// expected by the `Select` trait.
pub trait GetSelectRequest<S, K> {
    /// Specifies the returned Value type for an upcoming select request
    fn select<'a, V>(&'a self, key: &'a K) -> SelectBuilder<'a, S, K, V, QueryConsistency>
    where
        S: Select<K, V>;
    /// Specifies the returned Value type for an upcoming select request using a query statement
    fn select_query<'a, V>(&'a self, key: &'a K) -> SelectBuilder<'a, S, K, V, QueryConsistency>
    where
        S: Select<K, V>;
    /// Specifies the returned Value type for an upcoming select request using a prepared statement id
    fn select_prepared<'a, V>(&'a self, key: &'a K) -> SelectBuilder<'a, S, K, V, QueryConsistency>
    where
        S: Select<K, V>;
}

impl<S: Keyspace, K> GetSelectRequest<S, K> for S {
    fn select<'a, V>(&'a self, key: &'a K) -> SelectBuilder<'a, S, K, V, QueryConsistency>
    where
        S: Select<K, V>,
    {
        SelectBuilder {
            _marker: PhantomData,
            keyspace: self,
            key,
            builder: S::QueryOrPrepared::make(Query::new(), self),
        }
    }
    fn select_query<'a, V>(&'a self, key: &'a K) -> SelectBuilder<'a, S, K, V, QueryConsistency>
    where
        S: Select<K, V>,
    {
        SelectBuilder {
            _marker: PhantomData,
            keyspace: self,
            key,
            builder: <QueryStatement as SelectRecommended<S, K, V>>::make(Query::new(), self),
        }
    }
    fn select_prepared<'a, V>(&'a self, key: &'a K) -> SelectBuilder<'a, S, K, V, QueryConsistency>
    where
        S: Select<K, V>,
    {
        SelectBuilder {
            _marker: PhantomData,
            keyspace: self,
            key,
            builder: <PreparedStatement as SelectRecommended<S, K, V>>::make(Query::new(), self),
        }
    }
}

pub struct SelectBuilder<'a, S, K, V, Stage> {
    _marker: PhantomData<(&'a S, &'a K, &'a V)>,
    keyspace: &'a S,
    key: &'a K,
    builder: QueryBuilder<Stage>,
}

impl<'a, S: Select<K, V>, K, V> SelectBuilder<'a, S, K, V, QueryConsistency> {
    pub fn consistency(self, consistency: Consistency) -> SelectBuilder<'a, S, K, V, QueryValues> {
        SelectBuilder {
            _marker: self._marker,
            keyspace: self.keyspace,
            key: self.key,
            builder: S::bind_values(self.builder.consistency(consistency), self.key),
        }
    }
}
impl<'a, S: Select<K, V>, K, V> SelectBuilder<'a, S, K, V, QueryValues> {
    pub fn page_size(self, page_size: i32) -> SelectBuilder<'a, S, K, V, QueryPagingState> {
        SelectBuilder {
            _marker: self._marker,
            keyspace: self.keyspace,
            key: self.key,
            builder: self.builder.page_size(page_size),
        }
    }
    /// Set the paging state.
    pub fn paging_state(self, paging_state: &Option<Vec<u8>>) -> SelectBuilder<'a, S, K, V, QuerySerialConsistency> {
        SelectBuilder {
            _marker: self._marker,
            keyspace: self.keyspace,
            key: self.key,
            builder: self.builder.paging_state(paging_state),
        }
    }
    pub fn timestamp(self, timestamp: i64) -> SelectBuilder<'a, S, K, V, QueryBuild> {
        SelectBuilder {
            _marker: self._marker,
            keyspace: self.keyspace,
            key: self.key,
            builder: self.builder.timestamp(timestamp),
        }
    }
    /// Build the SelectRequest
    pub fn build(self) -> anyhow::Result<SelectRequest<S, K, V>> {
        let query = self.builder.build()?;
        // create the request
        Ok(self.keyspace.create_request(query, S::token(self.key)))
    }
}

impl<'a, S: Select<K, V>, K, V> SelectBuilder<'a, S, K, V, QueryBuild> {
    /// Build the InsertRequest
    pub fn build(self) -> anyhow::Result<SelectRequest<S, K, V>> {
        let query = self.builder.build()?;
        // create the request
        Ok(self.keyspace.create_request(query, S::token(self.key)))
    }
}

impl<'a, S: Select<K, V>, K, V> SelectBuilder<'a, S, K, V, QueryPagingState> {
    /// Set the paging state in the query frame.
    pub fn paging_state(self, paging_state: &Option<Vec<u8>>) -> SelectBuilder<'a, S, K, V, QuerySerialConsistency> {
        SelectBuilder {
            _marker: self._marker,
            keyspace: self.keyspace,
            key: self.key,
            builder: self.builder.paging_state(paging_state),
        }
    }

    /// Set the timestamp of the query frame.
    pub fn timestamp(self, timestamp: i64) -> SelectBuilder<'a, S, K, V, QueryBuild> {
        SelectBuilder {
            _marker: self._marker,
            keyspace: self.keyspace,
            key: self.key,
            builder: self.builder.timestamp(timestamp),
        }
    }

    pub fn build(self) -> anyhow::Result<SelectRequest<S, K, V>> {
        let query = self.builder.build()?;
        // create the request
        Ok(self.keyspace.create_request(query, S::token(self.key)))
    }
}
impl<'a, S: Select<K, V>, K, V> SelectBuilder<'a, S, K, V, QuerySerialConsistency> {
    /// Set the timestamp of the query frame.
    pub fn timestamp(self, timestamp: i64) -> SelectBuilder<'a, S, K, V, QueryBuild> {
        SelectBuilder {
            _marker: self._marker,
            keyspace: self.keyspace,
            key: self.key,
            builder: self.builder.timestamp(timestamp),
        }
    }

    pub fn build(self) -> anyhow::Result<SelectRequest<S, K, V>> {
        let query = self.builder.build()?;
        // create the request
        Ok(self.keyspace.create_request(query, S::token(self.key)))
    }
}

/// Defines two helper methods to specify statement / id
pub trait GetSelectStatement<S> {
    /// Specifies the Key and Value type for a select statement
    fn select_statement<K, V>(&self) -> Cow<'static, str>
    where
        S: Select<K, V>;

    /// Specifies the Key and Value type for a prepared select statement id
    fn select_id<K, V>(&self) -> [u8; 16]
    where
        S: Select<K, V>;
}

impl<S: Keyspace> GetSelectStatement<S> for S {
    fn select_statement<K, V>(&self) -> Cow<'static, str>
    where
        S: Select<K, V>,
    {
        S::statement(self)
    }

    fn select_id<K, V>(&self) -> [u8; 16]
    where
        S: Select<K, V>,
    {
        S::id(self)
    }
}

/// A request to select a record which can be sent to the ring
#[derive(Clone, Debug)]
pub struct SelectRequest<S, K, V> {
    token: i64,
    inner: Vec<u8>,
    keyspace: S,
    _marker: PhantomData<(S, K, V)>,
}

impl<K, V, S: Select<K, V> + Clone> CreateRequest<SelectRequest<S, K, V>> for S {
    /// Create a new Select Request from a Query/Execute, token, and the keyspace.
    fn create_request<Q: Into<Vec<u8>>>(&self, query: Q, token: i64) -> SelectRequest<S, K, V> {
        SelectRequest::<S, K, V> {
            token,
            inner: query.into(),
            keyspace: self.clone(),
            _marker: PhantomData,
        }
    }
}

impl<S, K, V> Request for SelectRequest<S, K, V>
where
    S: Select<K, V>,
    K: Send,
    V: Send,
{
    fn statement(&self) -> Cow<'static, str> {
        self.keyspace.select_statement::<K, V>()
    }

    fn payload(&self) -> &Vec<u8> {
        &self.inner
    }
}

impl<S: Select<K, V>, K, V> SelectRequest<S, K, V> {
    /// Return DecodeResult marker type, useful in case the worker struct wants to hold the
    /// decoder in order to decode the response inside handle_response method.
    pub fn result_decoder(&self) -> DecodeResult<DecodeRows<S, K, V>> {
        DecodeResult::select()
    }
    /// Send a local request using the keyspace impl and return a type marker
    pub fn send_local(self, worker: Box<dyn Worker>) -> DecodeResult<DecodeRows<S, K, V>> {
        send_local(
            self.token,
            self.inner,
            worker,
            self.keyspace.name().clone().into_owned(),
        );
        DecodeResult::select()
    }

    /// Send a global request using the keyspace impl and return a type marker
    pub fn send_global(self, worker: Box<dyn Worker>) -> DecodeResult<DecodeRows<S, K, V>> {
        send_global(
            self.token,
            self.inner,
            worker,
            self.keyspace.name().clone().into_owned(),
        );
        DecodeResult::select()
    }

    /// Consume the request to retrieve the payload
    pub fn into_payload(self) -> Vec<u8> {
        self.inner
    }
}

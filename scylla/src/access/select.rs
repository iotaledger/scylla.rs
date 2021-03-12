// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use super::*;

/// Select query trait which creates an `SelectRequest`
/// that can be sent to the `Ring`.
///
/// ## Examples
/// ### Dynamic query
/// ```no_run
/// impl Select<MyKeyType, MyValueType> for MyKeyspace {
///     fn statement(&self) -> Cow<'static, str> {
///         "SELECT * FROM keyspace.table WHERE key = ?".into()
///     }
///
///     fn get_request(&self, key: &MyKeyType) -> SelectRequest<Self, MyKeyType, MyValueType> {
///         let query = Query::new()
///             .statement(&self.select_statement::<MyKeyType, MyValueType>())
///             .consistency(scylla_cql::Consistency::One)
///             .value(&key.to_string())
///             .build();
///
///         let token = self.token(&key);
///
///         self.create_request(query, token)
///     }
/// }
/// ```
/// ### Prepared statement
/// ```no_run
/// impl Select<MyKeyType, MyValueType> for MyKeyspace {
///     fn statement(&self) -> Cow<'static, str> {
///         format!("SELECT * FROM {}.table WHERE key = ?", self.name()).into()
///     }
///
///     fn get_request(&self, key: &MyKeyType) -> SelectRequest<Self, MyKeyType, MyValueType> {
///         let prepared_cql = Execute::new()
///             .id(&self.select_id::<MyKeyType, MyValueType>())
///             .consistency(scylla_cql::Consistency::One)
///             .value(&key.to_string())
///             .build();
///
///         let token = self.token(&key);
///
///         self.create_request(prepared_cql, token)
///     }
/// }
/// ```
/// ### Usage
/// ```
/// let res = keyspace // A Scylla keyspace
///     .select::<MyValueType>(&my_key) // Get the Select Request by specifying the Value type
///     .send_global(worker); // Send the request to the Ring
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
    fn make<T: Statements>(query_or_batch: T, keyspace: &S) -> T::Return;
}

impl<S: Select<K, V>, K, V> SelectRecommended<S, K, V> for QueryStatement {
    fn make<T: Statements>(query_or_batch: T, keyspace: &S) -> T::Return {
        Self::encode_statement(query_or_batch, keyspace.statement().as_bytes())
    }
}

impl<S: Select<K, V>, K, V> SelectRecommended<S, K, V> for PreparedStatement {
    fn make<T: Statements>(query_or_batch: T, keyspace: &S) -> T::Return {
        Self::encode_statement(query_or_batch, &keyspace.id())
    }
}

/// Defines a helper method to specify the Value type
/// expected by the `Select` trait.
pub trait GetSelectRequest<S, K> {
    /// Specifies the returned Value type for an upcoming select request
    fn select<'a, V>(&'a self, key: &'a K) -> SelectBuilder<'a, S, K, V, QueryConsistency>
    where
        S: Select<K, V>;
    fn select_query<'a, V>(&'a self, key: &'a K) -> SelectBuilder<'a, S, K, V, QueryConsistency>
    where
        S: Select<K, V>;
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
    pub fn page_size(mut self, page_size: i32) -> SelectBuilder<'a, S, K, V, QueryPagingState> {
        SelectBuilder {
            _marker: self._marker,
            keyspace: self.keyspace,
            key: self.key,
            builder: self.builder.page_size(page_size),
        }
    }
    /// Set the paging state.
    pub fn paging_state(
        mut self,
        paging_state: &Option<Vec<u8>>,
    ) -> SelectBuilder<'a, S, K, V, QuerySerialConsistency> {
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
    pub fn build(self) -> SelectRequest<S, K, V> {
        let query = self.builder.build();
        // create the request
        self.keyspace.create_request(query, S::token(self.key))
    }
}

impl<'a, S: Select<K, V>, K, V> SelectBuilder<'a, S, K, V, QueryBuild> {
    /// Build the InsertRequest
    pub fn build(self) -> SelectRequest<S, K, V> {
        let query = self.builder.build();
        // create the request
        self.keyspace.create_request(query, S::token(self.key))
    }
}

impl<'a, S: Select<K, V>, K, V> SelectBuilder<'a, S, K, V, QueryPagingState> {
    /// Set the paging state in the query frame.
    pub fn paging_state(
        mut self,
        paging_state: &Option<Vec<u8>>,
    ) -> SelectBuilder<'a, S, K, V, QuerySerialConsistency> {
        SelectBuilder {
            _marker: self._marker,
            keyspace: self.keyspace,
            key: self.key,
            builder: self.builder.paging_state(paging_state),
        }
    }

    /// Set the timestamp of the query frame.
    pub fn timestamp(mut self, timestamp: i64) -> SelectBuilder<'a, S, K, V, QueryBuild> {
        SelectBuilder {
            _marker: self._marker,
            keyspace: self.keyspace,
            key: self.key,
            builder: self.builder.timestamp(timestamp),
        }
    }

    pub fn build(mut self) -> SelectRequest<S, K, V> {
        let query = self.builder.build();
        // create the request
        self.keyspace.create_request(query, S::token(self.key))
    }
}
impl<'a, S: Select<K, V>, K, V> SelectBuilder<'a, S, K, V, QuerySerialConsistency> {
    /// Set the timestamp of the query frame.
    pub fn timestamp(mut self, timestamp: i64) -> SelectBuilder<'a, S, K, V, QueryBuild> {
        SelectBuilder {
            _marker: self._marker,
            keyspace: self.keyspace,
            key: self.key,
            builder: self.builder.timestamp(timestamp),
        }
    }

    pub fn build(mut self) -> SelectRequest<S, K, V> {
        let query = self.builder.build();
        // create the request
        self.keyspace.create_request(query, S::token(self.key))
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
    S: Select<K, V> + std::fmt::Debug + Clone,
    K: Send + std::fmt::Debug + Clone,
    V: Send + std::fmt::Debug + Clone,
{
    fn statement(&self) -> Cow<'static, str> {
        self.keyspace.select_statement::<K, V>()
    }

    fn payload(&self) -> &Vec<u8> {
        &self.inner
    }
    fn into_payload(self) -> Vec<u8> {
        self.inner
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
}

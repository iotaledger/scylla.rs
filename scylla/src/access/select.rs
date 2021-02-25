// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use super::*;

/// Select query trait which creates a Select Request
/// that can be sent to the `Ring`.
///
/// ## Example
/// ```no_compile
/// let res = keyspace // A Scylla keyspace
///     .select::<MyValueType>(key) // Get the Select Request by specifying the return Value type
///     .send_local(worker); // Send the request to the Ring
/// ```
pub trait Select<'a, K, V>: Keyspace + RowsDecoder<K, V> {
    /// Create your select statement here.
    ///
    /// ## Examples
    /// ```no_compile
    /// fn select_statement() -> Cow<'static, str> {
    ///     "SELECT * FROM keyspace.table WHERE key = ?".into()
    /// }
    /// ```
    /// ```no_compile
    /// fn select_statement() -> Cow<'static, str> {
    ///     format!("SELECT * FROM {}.table WHERE key = ?", Self::name()).into()
    /// }
    /// ```
    fn select_statement() -> Cow<'static, str>;

    /// Get the MD5 hash of this implementation's statement
    /// for use when generating queries that should use
    /// the prepared statement.
    fn select_id() -> [u8; 16] {
        md5::compute(Self::select_statement().as_bytes()).into()
    }
    /// Construct your select query here and use it to create a
    /// `SelectRequest`.
    ///
    /// ## Examples
    /// ### Dynamic query
    /// ```no_compile
    /// fn get_request(&'a self, key: &MyKeyType) -> SelectRequest<'a, Self, MyKeyType, MyValueType>
    /// where
    ///     Self: Select<'a, MyKeyType, MyValueType>,
    /// {
    ///     let query = Query::new()
    ///         .statement(&Self::select_statement())
    ///         .consistency(scylla_cql::Consistency::One)
    ///         .value(key.to_string())
    ///         .build();
    ///
    ///     let token = rand::random::<i64>();
    ///
    ///     SelectRequest::from_query(query, token, self)
    /// }
    /// ```
    /// ### Prepared statement
    /// ```no_compile
    /// fn get_request(&'a self, key: &MyKeyType) -> SelectRequest<'a, Self, MyKeyType, MyValueType>
    /// where
    ///     Self: Select<'a, MyKeyType, MyValueType>,
    /// {
    ///     let prepared_cql = Execute::new()
    ///         .id(&Self::select_id())
    ///         .consistency(scylla_cql::Consistency::One)
    ///         .value(key.to_string())
    ///         .build();
    ///
    ///     let token = rand::random::<i64>();
    ///
    ///     SelectRequest::from_prepared(prepared_cql, token, self)
    /// }
    /// ```
    fn get_request(&'a self, key: &K) -> SelectRequest<'a, Self, K, V>
    where
        Self: Select<'a, K, V>;
}

/// Defines a helper method to specify the Value type
/// expected by the `Select` trait.
pub trait GetSelectRequest<S, K> {
    /// Specifies the returned Value type for an upcoming select request
    fn select<'a, V>(&'a self, key: &K) -> SelectRequest<S, K, V>
    where
        S: Select<'a, K, V>;
}

impl<S: Keyspace, K> GetSelectRequest<S, K> for S {
    fn select<'a, V>(&'a self, key: &K) -> SelectRequest<S, K, V>
    where
        S: Select<'a, K, V>,
    {
        S::get_request(self, key)
    }
}

/// A request to select a record which can be sent to the ring
pub struct SelectRequest<'a, S, K, V> {
    token: i64,
    inner: Vec<u8>,
    /// The type of query this request contains
    pub query_type: QueryType,
    keyspace: &'a S,
    _marker: PhantomData<(K, V)>,
}

impl<'a, S: Select<'a, K, V>, K, V> SelectRequest<'a, S, K, V> {
    /// Create a new Select Request from a Query, token, and the keyspace.
    pub fn from_query(query: Query, token: i64, keyspace: &'a S) -> Self {
        Self {
            token,
            inner: query.0,
            query_type: QueryType::Dynamic,
            keyspace,
            _marker: PhantomData,
        }
    }

    /// Create a new Select Request from a Query, token, and the keyspace.
    pub fn from_prepared(pcql: Execute, token: i64, keyspace: &'a S) -> Self {
        Self {
            token,
            inner: pcql.0,
            query_type: QueryType::Prepared,
            keyspace,
            _marker: PhantomData,
        }
    }

    /// Send a local request using the keyspace impl and return a type marker
    pub fn send_local(self, worker: Box<dyn Worker>) -> DecodeResult<DecodeRows<S, K, V>> {
        self.keyspace.send_local(self.token, self.inner, worker);
        DecodeResult::select()
    }

    /// Send a global request using the keyspace impl and return a type marker
    pub fn send_global(self, worker: Box<dyn Worker>) -> DecodeResult<DecodeRows<S, K, V>> {
        self.keyspace.send_global(self.token, self.inner, worker);
        DecodeResult::select()
    }
}

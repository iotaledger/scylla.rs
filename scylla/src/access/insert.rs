// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use super::*;

/// Insert query trait which creates an Insert Request
/// that can be sent to the `Ring`.
///
/// ## Example
/// ```no_compile
/// let res = keyspace // A Scylla keyspace
///     .insert(key, value) // Get the Insert Request
///     .send_local(worker); // Send the request to the Ring
/// ```
pub trait Insert<'a, K, V>: Keyspace + VoidDecoder {
    /// Create your insert statement here.
    ///
    /// ## Examples
    /// ```no_compile
    /// fn insert_statement() -> Cow<'static, str> {
    ///     "INSERT INTO keyspace.table (key, val1, val2) VALUES (?,?,?)".into()
    /// }
    /// ```
    /// ```no_compile
    /// fn insert_statement() -> Cow<'static, str> {
    ///     format!("INSERT INTO {}.table (key, val1, val2) VALUES (?,?,?)", Self::name()).into()
    /// }
    /// ```
    fn insert_statement() -> Cow<'static, str>;

    /// Get the MD5 hash of this implementation's statement
    /// for use when generating queries that should use
    /// the prepared statement.
    fn insert_id() -> [u8; 16] {
        md5::compute(Self::insert_statement().as_bytes()).into()
    }
    /// Construct your insert query here and use it to create an
    /// `InsertRequest`.
    ///
    /// ## Examples
    /// ### Dynamic query
    /// ```no_compile
    /// fn get_request(&'a self, key: &MyKeyType, value: &MyValueType) -> InsertRequest<'a, Self, MyKeyType, MyValueType>
    /// where
    ///     Self: Insert<'a, MyKeyType, MyValueType>,
    /// {
    ///     let query = Query::new()
    ///         .statement(&Self::insert_statement())
    ///         .consistency(scylla_cql::Consistency::One)
    ///         .value(key.to_string())
    ///         .value(value.val1.to_string())
    ///         .value(value.val2.to_string())
    ///         .build();
    ///
    ///     let token = rand::random::<i64>();
    ///
    ///     InsertRequest::from_query(query, token, self)
    /// }
    /// ```
    /// ### Prepared statement
    /// ```no_compile
    /// fn get_request(&'a self, key: &MyKeyType, value: &MyValueType) -> InsertRequest<'a, Self, MyKeyType, MyValueType>
    /// where
    ///     Self: Insert<'a, MyKeyType, MyValueType>,
    /// {
    ///     use scylla::access::*;
    ///     let prepared_cql = Execute::new()
    ///         .id(&Self::select_id())
    ///         .consistency(scylla_cql::Consistency::One)
    ///         .value(key.to_string())
    ///         .value(value.val1.to_string())
    ///         .value(value.val2.to_string())
    ///         .build();
    ///
    ///     let token = rand::random::<i64>();
    ///
    ///     InsertRequest::from_prepared(prepared_cql, token, self)
    /// }
    /// ```
    fn get_request(&'a self, key: &K, value: &V) -> InsertRequest<'a, Self, K, V>
    where
        Self: Insert<'a, K, V>;
}

/// Wrapper for the `Insert` trait which provides the `insert` function
pub trait GetInsertRequest<S, K, V> {
    /// Calls the appropriate `Insert` implementation for this Key/Value pair
    fn insert<'a>(&'a self, key: &K, value: &V) -> InsertRequest<S, K, V>
    where
        S: Insert<'a, K, V>;
}

impl<S: Keyspace, K, V> GetInsertRequest<S, K, V> for S {
    fn insert<'a>(&'a self, key: &K, value: &V) -> InsertRequest<S, K, V>
    where
        S: Insert<'a, K, V>,
    {
        S::get_request(self, key, value)
    }
}

/// A request to insert a record which can be sent to the ring
pub struct InsertRequest<'a, S, K, V> {
    token: i64,
    inner: Vec<u8>,
    /// The type of query this request contains
    pub query_type: QueryType,
    keyspace: &'a S,
    _marker: PhantomData<(K, V)>,
}

impl<'a, S: Insert<'a, K, V>, K, V> InsertRequest<'a, S, K, V> {
    /// Create a new Insert Request from a Query, token, and the keyspace.
    pub fn from_query(query: Query, token: i64, keyspace: &'a S) -> Self {
        Self {
            token,
            inner: query.0,
            query_type: QueryType::Dynamic,
            keyspace,
            _marker: PhantomData,
        }
    }

    /// Create a new Insert Request from a Query, token, and the keyspace.
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
    pub fn send_local(self, worker: Box<dyn Worker>) -> DecodeResult<DecodeVoid<S>> {
        self.keyspace.send_local(self.token, self.inner, worker);
        DecodeResult::insert()
    }

    /// Send a global request using the keyspace impl and return a type marker
    pub fn send_global(self, worker: Box<dyn Worker>) -> DecodeResult<DecodeVoid<S>> {
        self.keyspace.send_global(self.token, self.inner, worker);
        DecodeResult::insert()
    }
}

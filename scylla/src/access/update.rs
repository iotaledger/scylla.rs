// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use super::*;

/// Update query trait which creates an Update Request
/// that can be sent to the `Ring`.
///
/// ## Example
/// ```no_compile
/// let res = keyspace // A Scylla keyspace
///     .update(key, value) // Get the Update Request
///     .send_local(worker); // Send the request to the Ring
/// ```
pub trait Update<K, V>: Keyspace + VoidDecoder {
    /// Create your update statement here.
    ///
    /// ## Examples
    /// ```no_run
    /// fn update_statement() -> Cow<'static, str> {
    ///     "UPDATE keyspace.table SET val1 = ?, val2 = ? WHERE key = ?".into()
    /// }
    /// ```
    /// ```no_run
    /// fn update_statement() -> Cow<'static, str> {
    ///     format!("UPDATE {}.table SET val1 = ?, val2 = ? WHERE key = ?", Self::name()).into()
    /// }
    /// ```
    fn statement(&self) -> Cow<'static, str>;
    /// Get the MD5 hash of this implementation's statement
    /// for use when generating queries that should use
    /// the prepared statement.
    fn id(&self) -> [u8; 16] {
        md5::compute(self.update_statement().as_bytes()).into()
    }
    /// Construct your update query here and use it to create an
    /// `UpdateRequest`.
    ///
    /// ## Examples
    /// ### Dynamic query
    /// ```no_run
    /// fn get_request(&self, key: &MyKeyType, value: &MyValueType) -> UpdateRequest<Self, MyKeyType, MyValueType>
    /// where
    ///     Self: Update<MyKeyType, MyValueType>,
    /// {
    ///     let query = Query::new()
    ///         .statement(&self.update_statement::<MyKeyType, MyValueType>())
    ///         .consistency(scylla_cql::Consistency::One)
    ///         .value(value.val1.to_string())
    ///         .value(value.val2.to_string())
    ///         .value(key.to_string())
    ///         .build();
    ///
    ///     let token = rand::random::<i64>();
    ///
    ///     UpdateRequest::from_query(query, token, self.name())
    /// }
    /// ```
    /// ### Prepared statement
    /// ```no_run
    /// fn get_request(&self, key: &MyKeyType, value: &MyValueType) -> UpdateRequest<Self, MyKeyType, MyValueType>
    /// where
    ///     Self: Update<MyKeyType, MyValueType>,
    /// {
    ///     let prepared_cql = Execute::new()
    ///         .id(&self.update_id::<MyKeyType, MyValueType>())
    ///         .consistency(scylla_cql::Consistency::One)
    ///         .value(value.val1.to_string())
    ///         .value(value.val2.to_string())
    ///         .value(key.to_string())
    ///         .build();
    ///
    ///     let token = rand::random::<i64>();
    ///
    ///     UpdateRequest::from_prepared(prepared_cql, token, self.name())
    /// }
    /// ```
    fn get_request(&self, key: &K, value: &V) -> UpdateRequest<Self, K, V>
    where
        Self: Update<K, V>;
}

/// Wrapper for the `Update` trait which provides the `update` function
pub trait GetUpdateRequest<S, K, V> {
    /// Calls the appropriate `Update` implementation for this Key/Value pair
    fn update(&self, key: &K, value: &V) -> UpdateRequest<S, K, V>
    where
        S: Update<K, V>;
}

impl<S: Keyspace, K, V> GetUpdateRequest<S, K, V> for S {
    fn update(&self, key: &K, value: &V) -> UpdateRequest<S, K, V>
    where
        S: Update<K, V>,
    {
        S::get_request(self, key, value)
    }
}

/// Defines two helper methods to specify statement / id
pub trait GetUpdateStatement<S> {
    /// Specifies the Key and Value type for an update statement
    fn update_statement<K, V>(&self) -> Cow<'static, str>
    where
        S: Update<K, V>;

    /// Specifies the Key and Value type for a prepared update statement id
    fn update_id<K, V>(&self) -> [u8; 16]
    where
        S: Update<K, V>;
}

impl<S: Keyspace> GetUpdateStatement<S> for S {
    fn update_statement<K, V>(&self) -> Cow<'static, str>
    where
        S: Update<K, V>,
    {
        S::statement(self)
    }

    fn update_id<K, V>(&self) -> [u8; 16]
    where
        S: Update<K, V>,
    {
        S::id(self)
    }
}

/// A request to update a record which can be sent to the ring
#[derive(Clone)]
pub struct UpdateRequest<'a, S, K, V> {
    token: i64,
    inner: Vec<u8>,
    /// The type of query this request contains
    pub query_type: QueryType,
    keyspace: &'a S,
    _marker: PhantomData<(S, K, V)>,
}

impl<'a, S: Update<K, V>, K, V> UpdateRequest<'a, S, K, V> {
    /// Create a new Update Request from a Query, token, and the keyspace.
    pub fn from_query(query: Query, token: i64, keyspace: &'a S) -> Self {
        Self {
            token,
            inner: query.0,
            query_type: QueryType::Dynamic,
            keyspace,
            _marker: PhantomData,
        }
    }

    /// Create a new Update Request from a Query, token, and the keyspace.
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
        send_local(
            self.token,
            self.inner,
            worker,
            self.keyspace.name().clone().into_owned(),
        );
        DecodeResult::update()
    }

    /// Send a global request using the keyspace impl and return a type marker
    pub fn send_global(self, worker: Box<dyn Worker>) -> DecodeResult<DecodeVoid<S>> {
        send_global(
            self.token,
            self.inner,
            worker,
            self.keyspace.name().clone().into_owned(),
        );
        DecodeResult::update()
    }
}

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
pub trait Insert<K, V>: Keyspace + VoidDecoder + ComputeToken<K> {
    /// Create your insert statement here.
    ///
    /// ## Examples
    /// ```no_run
    /// fn insert_statement() -> Cow<'static, str> {
    ///     "INSERT INTO keyspace.table (key, val1, val2) VALUES (?,?,?)".into()
    /// }
    /// ```
    /// ```no_run
    /// fn insert_statement() -> Cow<'static, str> {
    ///     format!("INSERT INTO {}.table (key, val1, val2) VALUES (?,?,?)", self.name()).into()
    /// }
    /// ```
    fn statement(&self) -> Cow<'static, str>;

    /// Get the MD5 hash of this implementation's statement
    /// for use when generating queries that should use
    /// the prepared statement.
    fn id(&self) -> [u8; 16] {
        md5::compute(self.insert_statement().as_bytes()).into()
    }
    /// Construct your insert query here and use it to create an
    /// `InsertRequest`.
    ///
    /// ## Examples
    /// ### Dynamic query
    /// ```no_run
    /// fn get_request(&self, key: &MyKeyType, value: &MyValueType) -> InsertRequest<Self, MyKeyType, MyValueType>
    /// where
    ///     Self: Insert<MyKeyType, MyValueType>,
    /// {
    ///     let query = Query::new()
    ///         .statement(&self.insert_statement::<MyKeyType, MyValueType>())
    ///         .consistency(scylla_cql::Consistency::One)
    ///         .value(key.to_string())
    ///         .value(value.val1.to_string())
    ///         .value(value.val2.to_string())
    ///         .build();
    ///
    ///     let token = rand::random::<i64>();
    ///
    ///     self.create_request(query, token)
    /// }
    /// ```
    /// ### Prepared statement
    /// ```no_run
    /// fn get_request(&self, key: &MyKeyType, value: &MyValueType) -> InsertRequest<Self, MyKeyType, MyValueType>
    /// where
    ///     Self: Insert<MyKeyType, MyValueType>,
    /// {
    ///     use scylla::access::*;
    ///     let prepared_cql = Execute::new()
    ///         .id(&self.select_id::<MyKeyType, MyValueType>())
    ///         .consistency(scylla_cql::Consistency::One)
    ///         .value(key.to_string())
    ///         .value(value.val1.to_string())
    ///         .value(value.val2.to_string())
    ///         .build();
    ///
    ///     let token = rand::random::<i64>();
    ///
    ///     self.create_request(prepared_cql, token)
    /// }
    /// ```
    fn get_request(&self, key: &K, value: &V) -> InsertRequest<Self, K, V>;
}

/// Wrapper for the `Insert` trait which provides the `insert` function
pub trait GetInsertRequest<S, K, V> {
    /// Calls the appropriate `Insert` implementation for this Key/Value pair
    fn insert(&self, key: &K, value: &V) -> InsertRequest<S, K, V>
    where
        S: Insert<K, V>;
}

impl<S: Keyspace, K, V> GetInsertRequest<S, K, V> for S {
    fn insert(&self, key: &K, value: &V) -> InsertRequest<S, K, V>
    where
        S: Insert<K, V>,
    {
        S::get_request(self, key, value)
    }
}

/// Defines two helper methods to specify statement / id
pub trait GetInsertStatement<S> {
    /// Specifies the Key and Value type for an insert statement
    fn insert_statement<K, V>(&self) -> Cow<'static, str>
    where
        S: Insert<K, V>;

    /// Specifies the Key and Value type for a prepared insert statement id
    fn insert_id<K, V>(&self) -> [u8; 16]
    where
        S: Insert<K, V>;
}

impl<S: Keyspace> GetInsertStatement<S> for S {
    fn insert_statement<K, V>(&self) -> Cow<'static, str>
    where
        S: Insert<K, V>,
    {
        S::statement(self)
    }

    fn insert_id<K, V>(&self) -> [u8; 16]
    where
        S: Insert<K, V>,
    {
        S::id(self)
    }
}

/// A request to insert a record which can be sent to the ring
#[derive(Clone, Debug)]
pub struct InsertRequest<S, K, V> {
    token: i64,
    inner: Vec<u8>,
    keyspace: S,
    _marker: PhantomData<(S, K, V)>,
}

impl<K, V, S: Insert<K, V> + Clone> CreateRequest<InsertRequest<S, K, V>> for S {
    /// Create a new Insert Request from a Query/Execute, token, and the keyspace.
    fn create_request<Q: Into<Vec<u8>>>(&self, query: Q, token: i64) -> InsertRequest<S, K, V> {
        InsertRequest {
            token,
            inner: query.into(),
            keyspace: self.clone(),
            _marker: PhantomData,
        }
    }
}

impl<S, K, V> Request for InsertRequest<S, K, V>
where
    S: Insert<K, V> + std::fmt::Debug + Clone,
    K: Send + std::fmt::Debug + Clone,
    V: Send + std::fmt::Debug + Clone,
{
    fn statement(&self) -> Cow<'static, str> {
        self.keyspace.insert_statement::<K, V>()
    }

    fn payload(&self) -> &Vec<u8> {
        &self.inner
    }
}

impl<S: Insert<K, V>, K, V> InsertRequest<S, K, V> {
    /// Send a local request using the keyspace impl and return a type marker
    pub fn send_local(self, worker: Box<dyn Worker>) -> DecodeResult<DecodeVoid<S>> {
        send_local(
            self.token,
            self.inner,
            worker,
            self.keyspace.name().clone().into_owned(),
        );
        DecodeResult::insert()
    }

    /// Send a global request using the keyspace impl and return a type marker
    pub fn send_global(self, worker: Box<dyn Worker>) -> DecodeResult<DecodeVoid<S>> {
        send_global(
            self.token,
            self.inner,
            worker,
            self.keyspace.name().clone().into_owned(),
        );
        DecodeResult::insert()
    }
}

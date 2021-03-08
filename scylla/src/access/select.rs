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
pub trait Select<K, V>: Keyspace + RowsDecoder<K, V> + ComputeToken<K> {
    /// Create your select statement here.
    ///
    /// ## Examples
    /// ```no_run
    /// fn select_statement() -> Cow<'static, str> {
    ///     "SELECT * FROM keyspace.table WHERE key = ?".into()
    /// }
    /// ```
    /// ```no_run
    /// fn select_statement() -> Cow<'static, str> {
    ///     format!("SELECT * FROM {}.table WHERE key = ?", self.name()).into()
    /// }
    /// ```
    fn statement(&self) -> Cow<'static, str>;

    /// Get the MD5 hash of this implementation's statement
    /// for use when generating queries that should use
    /// the prepared statement.
    fn id(&self) -> [u8; 16] {
        md5::compute(self.select_statement().as_bytes()).into()
    }
    /// Construct your select query here and use it to create a
    /// `SelectRequest`.
    ///
    /// ## Examples
    /// ### Dynamic query
    /// ```no_run
    /// fn get_request(&self, key: &MyKeyType) -> SelectRequest<Self, MyKeyType, MyValueType>
    /// where
    ///     Self: Select<MyKeyType, MyValueType>,
    /// {
    ///     let query = Query::new()
    ///         .statement(&self.select_statement::<MyKeyType, MyValueType>())
    ///         .consistency(scylla_cql::Consistency::One)
    ///         .value(key.to_string())
    ///         .build();
    ///
    ///     let token = rand::random::<i64>();
    ///
    ///     self.create_request(query, token)
    /// }
    /// ```
    /// ### Prepared statement
    /// ```no_run
    /// fn get_request(&self, key: &MyKeyType) -> SelectRequest<Self, MyKeyType, MyValueType>
    /// where
    ///     Self: Select<MyKeyType, MyValueType>,
    /// {
    ///     let prepared_cql = Execute::new()
    ///         .id(&self.select_id::<MyKeyType, MyValueType>())
    ///         .consistency(scylla_cql::Consistency::One)
    ///         .value(key.to_string())
    ///         .build();
    ///
    ///     let token = rand::random::<i64>();
    ///
    ///     self.create_request(prepared_cql, token)
    /// }
    /// ```
    fn get_request(&self, key: &K) -> SelectRequest<Self, K, V>;
}

/// Defines a helper method to specify the Value type
/// expected by the `Select` trait.
pub trait GetSelectRequest<S, K> {
    /// Specifies the returned Value type for an upcoming select request
    fn select<V>(&self, key: &K) -> SelectRequest<S, K, V>
    where
        S: Select<K, V>;
}

impl<S: Keyspace, K> GetSelectRequest<S, K> for S {
    fn select<V>(&self, key: &K) -> SelectRequest<S, K, V>
    where
        S: Select<K, V>,
    {
        S::get_request(self, key)
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

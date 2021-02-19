// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use super::*;

/// Select query trait which creates a Select Request
/// that can be sent to the `Ring`.
///
/// ## Example
/// ```
/// let res = keyspace // A Scylla keyspace
///     .select::<MyValueType>(key) // Get the Select Request by specifying the return Value type
///     .send_local(worker) // Send the request to the Ring
/// ```
pub trait Select<'a, K, V>: Keyspace + RowsDecoder<K, V> {
    /// Construct your delete query here and use it to create a
    /// `DeleteRequest`.
    ///
    /// ## Example
    /// ```
    /// fn get_request(&'a self, key: &MyKeyType) -> SelectRequest<'a, Self, MyKeyType, MyValueType> {
    ///     let query = Query::new()
    ///         .statement(&format!("SELECT * FROM {}.table WHERE key = ?", MyKeyspace::name()))
    ///         .consistency(scylla_cql::Consistency::One)
    ///         .value(key.to_string())
    ///         .build();
    ///
    ///     let token = rand::random::<i64>();
    ///
    ///     SelectRequest::new(query, token, self)
    /// }
    /// ```
    fn get_request(&'a self, key: &K) -> SelectRequest<'a, Self, K, V>;
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
    inner: Query,
    keyspace: &'a S,
    _marker: PhantomData<(K, V)>,
}

impl<'a, S: Select<'a, K, V>, K, V> SelectRequest<'a, S, K, V> {
    /// Create a new Select Request from a Query, token, and the keyspace.
    pub fn new(query: Query, token: i64, keyspace: &'a S) -> Self {
        Self {
            token,
            inner: query,
            keyspace,
            _marker: PhantomData,
        }
    }

    pub fn send_local(self, worker: Box<dyn Worker>) -> DecodeResult<DecodeRows<S, K, V>> {
        self.keyspace.send_local(self.token, self.inner.0, worker);
        DecodeResult {
            inner: DecodeRows { _marker: PhantomData },
            request_type: RequestType::Select,
        }
    }

    pub fn send_global(self, worker: Box<dyn Worker>) -> DecodeResult<DecodeRows<S, K, V>> {
        self.keyspace.send_global(self.token, self.inner.0, worker);
        DecodeResult {
            inner: DecodeRows { _marker: PhantomData },
            request_type: RequestType::Select,
        }
    }
}

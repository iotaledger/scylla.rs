// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use super::*;

/// Insert query trait which creates an Insert Request
/// that can be sent to the `Ring`.
///
/// ## Example
/// ```
/// let res = keyspace // A Scylla keyspace
///     .insert(key, value) // Get the Insert Request
///     .send_local(worker) // Send the request to the Ring
/// ```
pub trait Insert<'a, K, V>: Keyspace + VoidDecoder {
    /// Construct your insert query here and use it to create an
    /// `InsertRequest`.
    ///
    /// ## Example
    /// ```
    /// fn get_request(&'a self, key: &MyKeyType, value: &MyValueType) -> InsertRequest<'a, Self, MyKeyType, MyValueType> {
    ///     let query = Query::new()
    ///         .statement(&format!(
    ///             "INSERT INTO {}.table (key, val1, val2) VALUES (?,?,?)",
    ///             MyKeyspace::name()
    ///         ))
    ///         .consistency(scylla_cql::Consistency::One)
    ///         .value(value.val1.to_string())
    ///         .value(value.val2.to_string())
    ///         .build();
    ///
    ///     let token = rand::random::<i64>();
    ///
    ///     InsertRequest::new(query, token, self)
    /// }
    /// ```
    fn get_request(&'a self, key: &K, value: &V) -> InsertRequest<'a, Self, K, V>;
}

/// Wrapper for the `Insert` trait which provides the `insert` function
pub trait GetInsertRequest<S, K, V> {
    /// Calls the appropriate `Insert` implementation for this Key/Value pair
    fn insert<'a>(&'a self, key: &K, value: &V) -> InsertRequest<S, K, V>
    where
        S: Insert<'a, K, V>;
}

impl<S, K, V> GetInsertRequest<S, K, V> for S {
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
    inner: Query,
    keyspace: &'a S,
    _marker: PhantomData<(K, V)>,
}

impl<'a, S: Insert<'a, K, V> + Default, K, V> InsertRequest<'a, S, K, V> {
    /// Create a new Insert Request from a Query, token, and the keyspace.
    pub fn new(query: Query, token: i64, keyspace: &'a S) -> Self {
        Self {
            token,
            inner: query,
            keyspace,
            _marker: PhantomData,
        }
    }

    pub fn send_local(self, worker: Box<dyn Worker>) -> DecodeResult<DecodeVoid<S>> {
        self.keyspace.send_local(self.token, self.inner.0, worker);
        DecodeResult {
            inner: DecodeVoid::default(),
            request_type: RequestType::Insert,
        }
    }

    pub fn send_global(self, worker: Box<dyn Worker>) -> DecodeResult<DecodeVoid<S>> {
        self.keyspace.send_global(self.token, self.inner.0, worker);
        DecodeResult {
            inner: DecodeVoid::default(),
            request_type: RequestType::Insert,
        }
    }
}

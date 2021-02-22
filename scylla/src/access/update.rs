// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use super::*;

/// Update query trait which creates an Update Request
/// that can be sent to the `Ring`.
///
/// ## Example
/// ```
/// let res = keyspace // A Scylla keyspace
///     .update(key, value) // Get the Update Request
///     .send_local(worker) // Send the request to the Ring
/// ```
pub trait Update<'a, K, V>: Keyspace {
    /// Construct your update query here and use it to create an
    /// `UpdateRequest`.
    ///
    /// ## Example
    /// ```
    /// fn get_request(&'a self, key: &MyKeyType, value: &MyValueType) -> UpdateRequest<'a, Self, MyKeyType, MyValueType> {
    ///     let query = Query::new()
    ///         .statement(&format!(
    ///             "UPDATE {}.table SET val1 = ?, val2 = ? WHERE key = ?",
    ///             MyKeyspace::name()
    ///         ))
    ///         .consistency(scylla_cql::Consistency::One)
    ///         .value(value.val1.to_string())
    ///         .value(value.val2.to_string())
    ///         .value(key.to_string())
    ///         .build();
    ///
    ///     let token = rand::random::<i64>();
    ///
    ///     UpdateRequest::new(query, token, self)
    /// }
    /// ```
    fn get_request(&'a self, key: &K, value: &V) -> UpdateRequest<'a, Self, K, V>;
}

/// Wrapper for the `Update` trait which provides the `update` function
pub trait GetUpdateRequest<S, K, V> {
    /// Calls the appropriate `Update` implementation for this Key/Value pair
    fn update<'a>(&'a self, key: &K, value: &V) -> UpdateRequest<S, K, V>
    where
        S: Update<'a, K, V>;
}

impl<S, K, V> GetUpdateRequest<S, K, V> for S {
    fn update<'a>(&'a self, key: &K, value: &V) -> UpdateRequest<S, K, V>
    where
        S: Update<'a, K, V>,
    {
        S::get_request(self, key, value)
    }
}

/// A request to update a record which can be sent to the ring
pub struct UpdateRequest<'a, S, K, V> {
    token: i64,
    inner: Query,
    keyspace: &'a S,
    _marker: PhantomData<(K, V)>,
}

impl<'a, S: Update<'a, K, V>, K, V> UpdateRequest<'a, S, K, V> {
    /// Create a new Update Request from a Query, token, and the keyspace.
    pub fn new(query: Query, token: i64, keyspace: &'a S) -> Self {
        Self {
            token,
            inner: query,
            keyspace,
            _marker: PhantomData,
        }
    }

    /// Send a local request using the keyspace impl and return a type marker
    pub fn send_local(self, worker: Box<dyn Worker>) -> DecodeResult<DecodeVoid<S>> {
        self.keyspace.send_local(self.token, self.inner.0, worker);
        DecodeResult {
            inner: DecodeVoid { _marker: PhantomData },
            request_type: RequestType::Update,
        }
    }

    /// Send a global request using the keyspace impl and return a type marker
    pub fn send_global(self, worker: Box<dyn Worker>) -> DecodeResult<DecodeVoid<S>> {
        self.keyspace.send_global(self.token, self.inner.0, worker);
        DecodeResult {
            inner: DecodeVoid { _marker: PhantomData },
            request_type: RequestType::Update,
        }
    }
}

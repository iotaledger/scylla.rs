// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use super::*;

/// Delete query trait which creates a Delete Request
/// that can be sent to the `Ring`.
///
/// ## Example
/// ```
/// let res = keyspace // A Scylla keyspace
///     .delete::<MyValueType>(key) // Get the Delete Request by specifying the Value type
///     .send_local(worker) // Send the request to the Ring
/// ```
pub trait Delete<'a, K, V>: Keyspace {
    /// Construct your delete query here and use it to create a
    /// `DeleteRequest`.
    ///
    /// ## Example
    /// ```
    /// fn get_request(&'a self, key: &MyKeyType) -> DeleteRequest<'a, Self, MyKeyType, MyValueType> {
    ///     let query = Query::new()
    ///         .statement(&format!("DELETE FROM {}.table WHERE key = ?", MyKeyspace::name()))
    ///         .consistency(scylla_cql::Consistency::One)
    ///         .value(key.to_string())
    ///         .build();
    ///
    ///     let token = rand::random::<i64>();
    ///
    ///     DeleteRequest::new(query, token, self)
    /// }
    /// ```
    fn get_request(&'a self, key: &K) -> DeleteRequest<'a, Self, K, V>;
}

/// Defines a helper method to specify the Value type
/// expected by the `Delete` trait.
pub trait GetDeleteRequest<S, K> {
    /// Specifies the Value type for an upcoming delete request
    fn delete<'a, V>(&'a self, key: &K) -> DeleteRequest<S, K, V>
    where
        S: Delete<'a, K, V>;
}

impl<S: Keyspace, K> GetDeleteRequest<S, K> for S {
    fn delete<'a, V>(&'a self, key: &K) -> DeleteRequest<S, K, V>
    where
        S: Delete<'a, K, V>,
    {
        S::get_request(self, key)
    }
}

/// A request to delete a record which can be sent to the ring
pub struct DeleteRequest<'a, S, K, V> {
    token: i64,
    inner: Query,
    keyspace: &'a S,
    _marker: PhantomData<(K, V)>,
}

impl<'a, S: Delete<'a, K, V>, K, V> DeleteRequest<'a, S, K, V> {
    /// Create a new Delete Request from a Query, token, and the keyspace.
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
            inner: DecodeVoid { _marker: PhantomData },
            request_type: RequestType::Delete,
        }
    }

    pub fn send_global(self, worker: Box<dyn Worker>) -> DecodeResult<DecodeVoid<S>> {
        self.keyspace.send_global(self.token, self.inner.0, worker);
        DecodeResult {
            inner: DecodeVoid { _marker: PhantomData },
            request_type: RequestType::Delete,
        }
    }
}

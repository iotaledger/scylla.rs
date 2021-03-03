// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use super::*;

/// Delete query trait which creates a Delete Request
/// that can be sent to the `Ring`.
///
/// ## Example
/// ```no_compile
/// let res = keyspace // A Scylla keyspace
///     .delete::<MyValueType>(key) // Get the Delete Request by specifying the Value type
///     .send_local(worker); // Send the request to the Ring
/// ```
pub trait Delete<'a, K, V>: Keyspace + VoidDecoder {
    /// Create your delete statement here.
    ///
    /// ## Examples
    /// ```no_compile
    /// fn delete_statement() -> Cow<'static, str> {
    ///     "DELETE FROM keyspace.table WHERE key = ?".into()
    /// }
    /// ```
    /// ```no_compile
    /// fn delete_statement() -> Cow<'static, str> {
    ///     format!("DELETE FROM {}.table WHERE key = ?", Self::name()).into()
    /// }
    /// ```
    fn delete_statement() -> Cow<'static, str>;

    /// Get the MD5 hash of this implementation's statement
    /// for use when generating queries that should use
    /// the prepared statement.
    fn delete_id() -> [u8; 16] {
        md5::compute(Self::delete_statement().as_bytes()).into()
    }

    /// Construct your delete query here and use it to create a
    /// `DeleteRequest`.
    /// ## Examples
    /// ### Dynamic query
    /// ```no_compile
    /// fn get_request(&'a self, key: &MyKeyType) -> DeleteRequest<'a, Self, MyKeyType, MyValueType>
    /// where
    ///     Self: Delete<'a, MyKeyType, MyValueType>,
    /// {
    ///     let query = Query::new()
    ///         .statement(&Self::delete_statement())
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
    /// ```no_compile
    /// fn get_request(&'a self, key: &MyKeyType) -> DeleteRequest<'a, Self, MyKeyType, MyValueType>
    /// where
    ///     Self: Delete<'a, MyKeyType, MyValueType>,
    /// {
    ///     let prepared_cql = Execute::new()
    ///         .id(&Self::delete_id())
    ///         .consistency(scylla_cql::Consistency::One)
    ///         .value(key.to_string())
    ///         .build();
    ///
    ///     let token = rand::random::<i64>();
    ///
    ///     self.create_request(prepared_cql, token)
    /// }
    /// ```
    fn get_request(&'a self, key: &K) -> DeleteRequest<'a, Self, K, V>
    where
        Self: Delete<'a, K, V>;
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
    inner: Vec<u8>,
    keyspace: &'a S,
    _marker: PhantomData<(K, V)>,
}

impl<'a, K, V, S: Delete<'a, K, V>> CreateRequest<'a, DeleteRequest<'a, S, K, V>> for S {
    /// Create a new Delete Request from a Query/Execute, token, and the keyspace.
    fn create_request<Q: Into<Vec<u8>>>(&'a self, query: Q, token: i64) -> DeleteRequest<'a, S, K, V> {
        DeleteRequest::<'a, S, K, V> {
            token,
            inner: query.into(),
            keyspace: self,
            _marker: PhantomData,
        }
    }
}

impl<'a, S: Delete<'a, K, V>, K, V> DeleteRequest<'a, S, K, V> {
    /// Send a local request using the keyspace impl and return a type marker
    pub fn send_local(self, worker: Box<dyn Worker>) -> DecodeResult<DecodeVoid<S>> {
        self.keyspace.send_local(self.token, self.inner, worker);
        DecodeResult::delete()
    }

    /// Send a global request using the keyspace impl and return a type marker
    pub fn send_global(self, worker: Box<dyn Worker>) -> DecodeResult<DecodeVoid<S>> {
        self.keyspace.send_global(self.token, self.inner, worker);
        DecodeResult::delete()
    }
}

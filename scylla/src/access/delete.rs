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
///     .send_local(worker); // Send the request to the Ring
/// ```
pub trait Delete<'a, K, V>: Keyspace + VoidDecoder {
    /// Hardcode your delete/prepare statement here.
    const DELETE_STATEMENT: &'static str;
    /// Get the MD5 hash of this implementation's statement
    /// for use when generating queries that should use
    /// the prepared statement.
    const DELETE_ID: [u8; 16] = md5::compute_hash(Self::DELETE_STATEMENT.as_bytes());
    /// Get the cql statement
    fn statement(&'a self) -> &'static str {
        Self::DELETE_STATEMENT
    }
    /// Get the prepared md5 id
    fn id(&'a self) -> [u8; 16] {
        Self::DELETE_ID
    }
    /// Construct your delete query here and use it to create a
    /// `DeleteRequest`.
    /// ## Examples
    /// ### Dynamic query
    /// ```
    /// fn get_request(&'a self, key: &MyKeyType) -> DeleteRequest<'a, Self, MyKeyType, MyValueType>
    /// where
    ///     Self: Delete<'a, MyKeyType, MyValueType>,
    /// {
    ///     let query = Query::new()
    ///         .statement(&Delete::statement(self))
    ///         .consistency(scylla_cql::Consistency::One)
    ///         .value(key.to_string())
    ///         .build();
    ///
    ///     let token = rand::random::<i64>();
    ///
    ///     DeleteRequest::from_query(query, token, self)
    /// }
    /// ```
    /// ### Prepared statement
    /// ```
    /// fn get_request(&'a self, key: &MyKeyType) -> DeleteRequest<'a, Self, MyKeyType, MyValueType>
    /// where
    ///     Self: Delete<'a, MyKeyType, MyValueType>,
    /// {
    ///     let prepared_cql = Execute::new()
    ///         .id(&Delete::get_prepared_hash(self))
    ///         .consistency(scylla_cql::Consistency::One)
    ///         .value(key.to_string())
    ///         .build();
    ///
    ///     let token = rand::random::<i64>();
    ///
    ///     DeleteRequest::from_prepared(prepared_cql, token, self)
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
    /// The type of query this request contains
    pub query_type: QueryType,
    keyspace: &'a S,
    _marker: PhantomData<(K, V)>,
}

impl<'a, S: Delete<'a, K, V>, K, V> DeleteRequest<'a, S, K, V> {
    /// Create a new Delete Request from a Query, token, and the keyspace.
    pub fn from_query(query: Query, token: i64, keyspace: &'a S) -> Self {
        Self {
            token,
            inner: query.0,
            query_type: QueryType::Dynamic,
            keyspace,
            _marker: PhantomData,
        }
    }

    /// Create a new Delete Request from a Query, token, and the keyspace.
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
        DecodeResult {
            inner: DecodeVoid { _marker: PhantomData },
            request_type: RequestType::Delete,
            cql: S::DELETE_STATEMENT,
        }
    }

    /// Send a global request using the keyspace impl and return a type marker
    pub fn send_global(self, worker: Box<dyn Worker>) -> DecodeResult<DecodeVoid<S>> {
        self.keyspace.send_global(self.token, self.inner, worker);
        DecodeResult {
            inner: DecodeVoid { _marker: PhantomData },
            request_type: RequestType::Delete,
            cql: S::DELETE_STATEMENT,
        }
    }
}

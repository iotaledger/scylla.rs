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
pub trait Delete<K, V>: Keyspace + VoidDecoder {
    /// Create your delete statement here.
    ///
    /// ## Examples
    /// ```no_run
    /// fn statement() -> Cow<'static, str> {
    ///     "DELETE FROM keyspace.table WHERE key = ?".into()
    /// }
    /// ```
    /// ```no_run
    /// fn statement() -> Cow<'static, str> {
    ///     format!("DELETE FROM {}.table WHERE key = ?", self.name()).into()
    /// }
    /// ```
    fn statement(&self) -> Cow<'static, str>;

    /// Get the MD5 hash of this implementation's statement
    /// for use when generating queries that should use
    /// the prepared statement.
    fn id(&self) -> [u8; 16] {
        md5::compute(self.delete_statement().as_bytes()).into()
    }

    /// Construct your delete query here and use it to create a
    /// `DeleteRequest`.
    /// ## Examples
    /// ### Dynamic query
    /// ```no_run
    /// fn get_request(&self, key: &MyKeyType) -> DeleteRequest<Self, MyKeyType, MyValueType>
    /// where
    ///     Self: Delete<MyKeyType, MyValueType>,
    /// {
    ///     let query = Query::new()
    ///         .statement(&self.delete_statement::<MyKeyType, MyValueType>())
    ///         .consistency(scylla_cql::Consistency::One)
    ///         .value(key.to_string())
    ///         .build();
    ///
    ///     let token = rand::random::<i64>();
    ///
    ///     DeleteRequest::from_query(query, token, self.name())
    /// }
    /// ```
    /// ### Prepared statement
    /// ```no_run
    /// fn get_request(&self, key: &MyKeyType) -> DeleteRequest<Self, MyKeyType, MyValueType>
    /// where
    ///     Self: Delete<MyKeyType, MyValueType>,
    /// {
    ///     let prepared_cql = Execute::new()
    ///         .id(&self.delete_id::<MyKeyType, MyValueType>())
    ///         .consistency(scylla_cql::Consistency::One)
    ///         .value(key.to_string())
    ///         .build();
    ///
    ///     let token = rand::random::<i64>();
    ///
    ///     DeleteRequest::from_prepared(prepared_cql, token, self.name())
    /// }
    /// ```
    fn get_request(&self, key: &K) -> DeleteRequest<Self, K, V>
    where
        Self: Delete<K, V>;
}

/// Defines a helper method to specify the Value type
/// expected by the `Delete` trait.
pub trait GetDeleteRequest<S, K> {
    /// Specifies the Value type for an upcoming delete request
    fn delete<V>(&self, key: &K) -> DeleteRequest<S, K, V>
    where
        S: Delete<K, V>;
}

impl<S: Keyspace, K> GetDeleteRequest<S, K> for S {
    fn delete<V>(&self, key: &K) -> DeleteRequest<S, K, V>
    where
        S: Delete<K, V>,
    {
        S::get_request(self, key)
    }
}

/// Defines two helper methods to specify statement / id
pub trait GetDeleteStatement<S> {
    /// Specifies the Key and Value type for a delete statement
    fn delete_statement<K, V>(&self) -> Cow<'static, str>
    where
        S: Delete<K, V>;

    /// Specifies the Key and Value type for a prepared delete statement id
    fn delete_id<K, V>(&self) -> [u8; 16]
    where
        S: Delete<K, V>;
}

impl<S: Keyspace> GetDeleteStatement<S> for S {
    fn delete_statement<K, V>(&self) -> Cow<'static, str>
    where
        S: Delete<K, V>,
    {
        S::statement(self)
    }

    fn delete_id<K, V>(&self) -> [u8; 16]
    where
        S: Delete<K, V>,
    {
        S::id(self)
    }
}

/// A request to delete a record which can be sent to the ring
#[derive(Clone)]
pub struct DeleteRequest<'a, S, K, V> {
    token: i64,
    inner: Vec<u8>,
    /// The type of query this request contains
    pub query_type: QueryType,
    keyspace: &'a S,
    _marker: PhantomData<(S, K, V)>,
}

impl<'a, S: Delete<K, V>, K, V> DeleteRequest<'a, S, K, V> {
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
        send_local(
            self.token,
            self.inner,
            worker,
            self.keyspace.name().clone().into_owned(),
        );
        DecodeResult::delete()
    }

    /// Send a global request using the keyspace impl and return a type marker
    pub fn send_global(self, worker: Box<dyn Worker>) -> DecodeResult<DecodeVoid<S>> {
        send_global(
            self.token,
            self.inner,
            worker,
            self.keyspace.name().clone().into_owned(),
        );
        DecodeResult::delete()
    }
}

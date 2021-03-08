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
pub trait Delete<K, V>: Keyspace + VoidDecoder + ComputeToken<K> {
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
    ///     self.create_request(query, token)
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
    ///     self.create_request(prepared_cql, token)
    /// }
    /// ```
    fn get_request(&self, key: &K) -> DeleteRequest<Self, K, V>;
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
#[derive(Clone, Debug)]
pub struct DeleteRequest<S, K, V> {
    token: i64,
    inner: Vec<u8>,
    keyspace: S,
    _marker: PhantomData<(S, K, V)>,
}

impl<K, V, S: Delete<K, V> + Clone> CreateRequest<DeleteRequest<S, K, V>> for S {
    /// Create a new Delete Request from a Query/Execute, token, and the keyspace.
    fn create_request<Q: Into<Vec<u8>>>(&self, query: Q, token: i64) -> DeleteRequest<S, K, V> {
        DeleteRequest::<S, K, V> {
            token,
            inner: query.into(),
            keyspace: self.clone(),
            _marker: PhantomData,
        }
    }
}

impl<S, K, V> Request for DeleteRequest<S, K, V>
where
    S: Delete<K, V> + std::fmt::Debug + Clone,
    K: Send + std::fmt::Debug + Clone,
    V: Send + std::fmt::Debug + Clone,
{
    fn statement(&self) -> Cow<'static, str> {
        self.keyspace.delete_statement::<K, V>()
    }

    fn payload(&self) -> &Vec<u8> {
        &self.inner
    }
}

impl<S: Delete<K, V>, K, V> DeleteRequest<S, K, V> {
    /// Return cqls marker type
    pub fn cqls(&self) -> DeleteCql<S, K, V>
    where
        DeleteCql<S, K, V>: IterCqls<S>,
    {
        DeleteCql::<S, K, V> { _marker: PhantomData }
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

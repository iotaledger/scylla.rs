// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use super::*;

/// Delete query trait which creates a `DeleteRequest`
/// that can be sent to the `Ring`.
///
/// ## Examples
/// ### Dynamic query
/// ```no_run
/// impl Delete<MyKeyType, MyValueType> for MyKeyspace {
///     fn statement(&self) -> Cow<'static, str> {
///         "DELETE FROM keyspace.table WHERE key = ?".into()
///     }
///
///     fn get_request(&self, key: &MyKeyType) -> DeleteRequest<Self, MyKeyType, MyValueType> {
///         let query = Query::new()
///             .statement(&self.delete_statement::<MyKeyType, MyValueType>())
///             .consistency(scylla_cql::Consistency::One)
///             .value(&key.to_string())
///             .build();
///
///         let token = self.token(&key);
///
///         self.create_request(query, token)
///     }
/// }
/// ```
/// ### Prepared statement
/// ```no_run
/// impl Delete<MyKeyType, MyValueType> for MyKeyspace {
///     fn statement(&self) -> Cow<'static, str> {
///         format!("DELETE FROM {}.table WHERE key = ?", self.name()).into()
///     }
///
///     fn get_request(&self, key: &MyKeyType) -> DeleteRequest<Self, MyKeyType, MyValueType> {
///         let prepared_cql = Execute::new()
///             .id(&self.delete_id::<MyKeyType, MyValueType>())
///             .consistency(scylla_cql::Consistency::One)
///             .value(&key.to_string())
///             .build();
///
///         let token = self.token(&key);
///
///         self.create_request(prepared_cql, token)
///     }
/// }
/// ```
/// ### Usage
/// ```
/// let res = keyspace // A Scylla keyspace
///     .delete::<MyValueType>(&my_key)? // Get the Delete Request by specifying the Value type
///     .send_local(worker); // Send the request to the Ring
/// ```
pub trait Delete<K, V>: Keyspace + VoidDecoder + ComputeToken<K> {
    /// Set the query type; `QueryStatement` or `PreparedStatement`
    type QueryOrPrepared: DeleteRecommended<Self, K, V>;

    /// Create your delete statement here.
    fn statement(&self) -> Cow<'static, str>;

    /// Get the MD5 hash of this implementation's statement
    /// for use when generating queries that should use
    /// the prepared statement.
    fn id(&self) -> [u8; 16] {
        md5::compute(self.delete_statement().as_bytes()).into()
    }

    /// Bind the cql values to the builder
    fn bind_values<T: Values>(builder: T, key: &K) -> T::Return;
}

pub trait DeleteRecommended<S: Delete<K, V>, K, V>: QueryOrPrepared {
    fn make<T: Statements>(query_or_batch: T, keyspace: &S) -> anyhow::Result<T::Return>;
}

impl<S: Delete<K, V>, K, V> DeleteRecommended<S, K, V> for QueryStatement {
    fn make<T: Statements>(query_or_batch: T, keyspace: &S) -> anyhow::Result<T::Return> {
        Self::encode_statement(query_or_batch, keyspace.statement().as_bytes())
    }
}

impl<S: Delete<K, V>, K, V> DeleteRecommended<S, K, V> for PreparedStatement {
    fn make<T: Statements>(query_or_batch: T, keyspace: &S) -> anyhow::Result<T::Return> {
        Self::encode_statement(query_or_batch, &keyspace.id())
    }
}

/// Defines a helper method to specify the Value type
/// expected by the `Delete` trait.
pub trait GetDeleteRequest<S, K> {
    /// Specifies the Value type for an upcoming delete request
    fn delete<'a, V>(&'a self, key: &'a K) -> anyhow::Result<DeleteBuilder<'a, S, K, V, QueryConsistency>>
    where
        S: Delete<K, V>;
    fn delete_query<'a, V>(&'a self, key: &'a K) -> anyhow::Result<DeleteBuilder<'a, S, K, V, QueryConsistency>>
    where
        S: Delete<K, V>;
    fn delete_prepared<'a, V>(&'a self, key: &'a K) -> anyhow::Result<DeleteBuilder<'a, S, K, V, QueryConsistency>>
    where
        S: Delete<K, V>;
}

impl<S: Keyspace, K> GetDeleteRequest<S, K> for S {
    fn delete<'a, V>(&'a self, key: &'a K) -> anyhow::Result<DeleteBuilder<'a, S, K, V, QueryConsistency>>
    where
        S: Delete<K, V>,
    {
        Ok(DeleteBuilder {
            _marker: PhantomData,
            keyspace: self,
            key,
            builder: S::QueryOrPrepared::make(Query::new(), self)?,
        })
    }
    fn delete_query<'a, V>(&'a self, key: &'a K) -> anyhow::Result<DeleteBuilder<'a, S, K, V, QueryConsistency>>
    where
        S: Delete<K, V>,
    {
        Ok(DeleteBuilder {
            _marker: PhantomData,
            keyspace: self,
            key,
            builder: <QueryStatement as DeleteRecommended<S, K, V>>::make(Query::new(), self)?,
        })
    }
    fn delete_prepared<'a, V>(&'a self, key: &'a K) -> anyhow::Result<DeleteBuilder<'a, S, K, V, QueryConsistency>>
    where
        S: Delete<K, V>,
    {
        Ok(DeleteBuilder {
            _marker: PhantomData,
            keyspace: self,
            key,
            builder: <PreparedStatement as DeleteRecommended<S, K, V>>::make(Query::new(), self)?,
        })
    }
}
pub struct DeleteBuilder<'a, S, K, V, Stage> {
    _marker: PhantomData<(&'a S, &'a K, &'a V)>,
    keyspace: &'a S,
    key: &'a K,
    builder: QueryBuilder<Stage>,
}

impl<'a, S: Delete<K, V>, K, V> DeleteBuilder<'a, S, K, V, QueryConsistency> {
    pub fn consistency(self, consistency: Consistency) -> DeleteBuilder<'a, S, K, V, QueryValues> {
        DeleteBuilder {
            _marker: self._marker,
            keyspace: self.keyspace,
            key: self.key,
            builder: S::bind_values(self.builder.consistency(consistency), self.key),
        }
    }
}

impl<'a, S: Delete<K, V>, K, V> DeleteBuilder<'a, S, K, V, QueryValues> {
    pub fn timestamp(self, timestamp: i64) -> DeleteBuilder<'a, S, K, V, QueryBuild> {
        DeleteBuilder {
            _marker: self._marker,
            keyspace: self.keyspace,
            key: self.key,
            builder: self.builder.timestamp(timestamp),
        }
    }
    /// Build the DeleteRequest
    pub fn build(self) -> anyhow::Result<DeleteRequest<S, K, V>> {
        let query = self.builder.build()?;
        // create the request
        Ok(self.keyspace.create_request(query, S::token(self.key)))
    }
}

impl<'a, S: Delete<K, V>, K, V> DeleteBuilder<'a, S, K, V, QueryBuild> {
    /// Build the DeleteRequest
    pub fn build(self) -> anyhow::Result<DeleteRequest<S, K, V>> {
        let query = self.builder.build()?;
        // create the request
        Ok(self.keyspace.create_request(query, S::token(self.key)))
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
    K: Send,
    V: Send,
{
    fn statement(&self) -> Cow<'static, str> {
        self.keyspace.delete_statement::<K, V>()
    }

    fn payload(&self) -> &Vec<u8> {
        &self.inner
    }
}

impl<S: Delete<K, V>, K, V> DeleteRequest<S, K, V> {
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

    pub fn into_payload(self) -> Vec<u8> {
        self.inner
    }
}

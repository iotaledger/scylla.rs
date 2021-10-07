// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use crate::{
    cql::{
        query::StatementType,
        QueryFlags,
    },
    prelude::BasicWorker,
};

use super::*;

/// Delete query trait which creates a `DeleteRequest`
/// that can be sent to the `Ring`.
///
/// ## Examples
/// ```
/// use scylla_rs::{
///     app::{
///         access::{
///             ComputeToken,
///             Delete,
///             GetDeleteRequest,
///             Keyspace,
///         },
///         worker::DeleteWorker,
///     },
///     cql::{
///         Batch,
///         Consistency,
///         PreparedStatement,
///         Values,
///         VoidDecoder,
///     },
/// };
/// use std::borrow::Cow;
/// # #[derive(Default, Clone, Debug)]
/// # struct MyKeyspace {
/// #     pub name: Cow<'static, str>,
/// # }
/// #
/// # impl MyKeyspace {
/// #     pub fn new() -> Self {
/// #         Self {
/// #             name: "my_keyspace".into(),
/// #         }
/// #     }
/// # }
///
/// # impl Keyspace for MyKeyspace {
/// #     fn name(&self) -> &Cow<'static, str> {
/// #         &self.name
/// #     }
/// # }
/// # impl ComputeToken<i32> for MyKeyspace {
/// #     fn token(_key: &i32) -> i64 {
/// #         rand::random()
/// #     }
/// # }
/// # type MyKeyType = i32;
/// # type MyValueType = f32;
/// impl Delete<MyKeyType, MyValueType> for MyKeyspace {
///     type QueryOrPrepared = PreparedStatement;
///     fn statement(&self) -> Cow<'static, str> {
///         format!("DELETE FROM {}.table WHERE key = ?", self.name()).into()
///     }
///
///     fn bind_values<T: Values>(builder: T, key: &MyKeyType) -> T::Return {
///         builder.value(key).value(key)
///     }
/// }
///
/// # let keyspace = MyKeyspace::new();
/// # let my_key = 1;
/// let worker = DeleteWorker::boxed(keyspace.clone(), my_key, 3);
///
/// let request = keyspace // A Scylla keyspace
///     .delete::<MyValueType>(&my_key) // Get the Delete Request by specifying the Value type
///     .consistency(Consistency::One)
///     .build()?;
/// # Ok::<(), anyhow::Error>(())
/// ```
pub trait Delete<K, V>: Keyspace + VoidDecoder + ComputeToken<K> {
    /// Set the query type; `QueryStatement` or `PreparedStatement`
    type QueryOrPrepared: QueryOrPrepared;

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

/// Defines a helper method to specify the Value type
/// expected by the `Delete` trait.
pub trait GetDeleteRequest<K>: Keyspace {
    /// Specifies the returned Value type for an upcoming delete request
    fn delete<'a, V>(&'a self, key: &'a K) -> DeleteBuilder<'a, Self, K, V, QueryConsistency, StaticRequest>
    where
        Self: Delete<K, V>,
    {
        DeleteBuilder {
            _marker: PhantomData,
            keyspace: self,
            statement: self.statement(),
            key,
            builder: Self::QueryOrPrepared::encode_statement(Query::new(), &self.statement()),
        }
    }
    /// Specifies the returned Value type for an upcoming delete request using a query statement
    fn delete_query<'a, V>(&'a self, key: &'a K) -> DeleteBuilder<'a, Self, K, V, QueryConsistency, StaticRequest>
    where
        Self: Delete<K, V>,
    {
        DeleteBuilder {
            _marker: PhantomData,
            keyspace: self,
            statement: self.statement(),
            key,
            builder: QueryStatement::encode_statement(Query::new(), &self.statement()),
        }
    }
    /// Specifies the returned Value type for an upcoming delete request using a prepared statement id
    fn delete_prepared<'a, V>(&'a self, key: &'a K) -> DeleteBuilder<'a, Self, K, V, QueryConsistency, StaticRequest>
    where
        Self: Delete<K, V>,
    {
        DeleteBuilder {
            _marker: PhantomData,
            keyspace: self,
            statement: self.statement(),
            key,
            builder: PreparedStatement::encode_statement(Query::new(), &self.statement()),
        }
    }

    /// Specifies the returned Value type for an upcoming delete request
    fn delete_with<'a, V>(
        &'a self,
        statement: &str,
        key: &'a K,
        statement_type: StatementType,
    ) -> DeleteBuilder<'a, Self, K, V, QueryConsistency, DynamicRequest> {
        match statement_type {
            StatementType::Query => self.delete_query_with(statement, key),
            StatementType::Prepared => self.delete_prepared_with(statement, key),
        }
    }
    /// Specifies the returned Value type for an upcoming delete request using a query statement
    fn delete_query_with<'a, V>(
        &'a self,
        statement: &str,
        key: &'a K,
    ) -> DeleteBuilder<'a, Self, K, V, QueryConsistency, DynamicRequest> {
        DeleteBuilder {
            _marker: PhantomData,
            keyspace: self,
            statement: statement.to_owned().into(),
            key,
            builder: QueryStatement::encode_statement(Query::new(), statement),
        }
    }
    /// Specifies the returned Value type for an upcoming delete request using a prepared statement id
    fn delete_prepared_with<'a, V>(
        &'a self,
        statement: &str,
        key: &'a K,
    ) -> DeleteBuilder<'a, Self, K, V, QueryConsistency, DynamicRequest> {
        DeleteBuilder {
            _marker: PhantomData,
            keyspace: self,
            statement: statement.to_owned().into(),
            key,
            builder: PreparedStatement::encode_statement(Query::new(), statement),
        }
    }
}

impl<S: Keyspace, K> GetDeleteRequest<K> for S {}

pub struct DeleteBuilder<'a, S, K, V, Stage, T> {
    keyspace: &'a S,
    statement: Cow<'static, str>,
    key: &'a K,
    builder: QueryBuilder<Stage>,
    _marker: PhantomData<fn(V, T) -> (V, T)>,
}

impl<'a, S: Keyspace, K, V, T> DeleteBuilder<'a, S, K, V, QueryConsistency, T> {
    pub fn consistency(self, consistency: Consistency) -> DeleteBuilder<'a, S, K, V, QueryFlags, T> {
        DeleteBuilder {
            _marker: self._marker,
            keyspace: self.keyspace,
            statement: self.statement,
            key: self.key,
            builder: self.builder.consistency(consistency),
        }
    }
}

impl<'a, S: Keyspace, K, V> DeleteBuilder<'a, S, K, V, QueryFlags, DynamicRequest> {
    pub fn bind_values<F: Fn(QueryBuilder<QueryFlags>, &K) -> QueryBuilder<QueryValues>>(
        self,
        bind_fn: F,
    ) -> DeleteBuilder<'a, S, K, V, QueryValues, DynamicRequest> {
        DeleteBuilder {
            _marker: self._marker,
            keyspace: self.keyspace,
            statement: self.statement,
            key: self.key,
            builder: bind_fn(self.builder, &self.key),
        }
    }
}

impl<'a, S: Delete<K, V>, K, V, T> DeleteBuilder<'a, S, K, V, QueryFlags, T> {
    pub fn timestamp(self, timestamp: i64) -> DeleteBuilder<'a, S, K, V, QueryBuild, T> {
        DeleteBuilder {
            _marker: self._marker,
            keyspace: self.keyspace,
            statement: self.statement,
            key: self.key,
            builder: S::bind_values(self.builder, &self.key).timestamp(timestamp),
        }
    }
}
impl<'a, S: Keyspace + ComputeToken<K>, K, V> DeleteBuilder<'a, S, K, V, QueryFlags, DynamicRequest> {
    /// Build the DeleteRequest
    pub fn build(self) -> anyhow::Result<DeleteRequest<S, K, V, DynamicRequest>> {
        let query = self.builder.build()?;
        // create the request
        Ok(DeleteRequest {
            token: S::token(self.key),
            inner: query.into(),
            statement: self.statement,
            _marker: PhantomData,
        })
    }
}
impl<'a, S: Delete<K, V>, K, V> DeleteBuilder<'a, S, K, V, QueryFlags, StaticRequest> {
    /// Build the DeleteRequest
    pub fn build(self) -> anyhow::Result<DeleteRequest<S, K, V, StaticRequest>> {
        let query = S::bind_values(self.builder, &self.key).build()?;
        // create the request
        Ok(DeleteRequest {
            token: S::token(self.key),
            inner: query.into(),
            statement: self.statement,
            _marker: PhantomData,
        })
    }
}

impl<'a, S, K, V, T> DeleteBuilder<'a, S, K, V, QueryValues, T> {
    pub fn timestamp(self, timestamp: i64) -> DeleteBuilder<'a, S, K, V, QueryBuild, T> {
        DeleteBuilder {
            _marker: self._marker,
            keyspace: self.keyspace,
            statement: self.statement,
            key: self.key,
            builder: self.builder.timestamp(timestamp),
        }
    }
}
impl<'a, S: Keyspace + ComputeToken<K>, K, V, T> DeleteBuilder<'a, S, K, V, QueryValues, T> {
    /// Build the DeleteRequest
    pub fn build(self) -> anyhow::Result<DeleteRequest<S, K, V, T>> {
        let query = self.builder.build()?;
        // create the request
        Ok(DeleteRequest {
            token: S::token(self.key),
            inner: query.into(),
            statement: self.statement,
            _marker: PhantomData,
        })
    }
}

impl<'a, S: Keyspace + ComputeToken<K>, K, V, T> DeleteBuilder<'a, S, K, V, QueryBuild, T> {
    /// Build the InsertRequest
    pub fn build(self) -> anyhow::Result<DeleteRequest<S, K, V, T>> {
        let query = self.builder.build()?;
        // create the request
        Ok(DeleteRequest {
            token: S::token(self.key),
            inner: query.into(),
            statement: self.statement,
            _marker: PhantomData,
        })
    }
}

/// A request to delete a record which can be sent to the ring
pub struct DeleteRequest<S, K, V, T> {
    token: i64,
    inner: Vec<u8>,
    statement: Cow<'static, str>,
    _marker: PhantomData<fn(S, K, V, T) -> (S, K, V, T)>,
}

impl<S, K, V, T> std::fmt::Debug for DeleteRequest<S, K, V, T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DeleteRequest")
            .field("token", &self.token)
            .field("inner", &self.inner)
            .field("statement", &self.statement)
            .finish()
    }
}

impl<S, K, V, T> Clone for DeleteRequest<S, K, V, T> {
    fn clone(&self) -> Self {
        Self {
            token: self.token,
            inner: self.inner.clone(),
            statement: self.statement.clone(),
            _marker: PhantomData,
        }
    }
}

impl<S, K, V, T> Request for DeleteRequest<S, K, V, T> {
    fn statement(&self) -> &Cow<'static, str> {
        &self.statement
    }

    fn payload(&self) -> &Vec<u8> {
        &self.inner
    }

    fn into_payload(self) -> Vec<u8> {
        self.inner
    }
}

impl<S, K, V, T> SendRequestExt for DeleteRequest<S, K, V, T> {
    type Marker = DecodeVoid<S>;
    const TYPE: RequestType = RequestType::Delete;

    fn token(&self) -> i64 {
        self.token
    }

    fn marker() -> Self::Marker {
        DecodeVoid::<S>::new()
    }
}

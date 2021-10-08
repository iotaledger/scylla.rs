// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use super::*;
use crate::{
    cql::{
        query::StatementType,
        TokenEncodeChain,
    },
    prelude::TokenEncoder,
};

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
pub trait Delete<K, V>: Keyspace {
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
    fn bind_values<T: Values>(builder: T, key: &K) -> Box<T::Return>;
}

/// Wrapper for the `Delete` trait which provides the `delete` function
pub trait GetStaticDeleteRequest<K>: Keyspace {
    /// Calls the appropriate `Delete` implementation for this Key/Value pair
    fn delete<'a, V>(&'a self, key: &'a K) -> DeleteBuilder<'a, Self, K, V, QueryConsistency, StaticRequest>
    where
        Self: Delete<K, V>,
    {
        DeleteBuilder {
            keyspace: self,
            statement: self.statement(),
            key,
            builder: Self::QueryOrPrepared::encode_statement(Query::new(), &self.statement()),
            _marker: PhantomData,
        }
    }
    /// Calls the `Delete` implementation for this Key/Value pair using a query statement
    fn delete_query<'a, V>(&'a self, key: &'a K) -> DeleteBuilder<'a, Self, K, V, QueryConsistency, StaticRequest>
    where
        Self: Delete<K, V>,
    {
        DeleteBuilder {
            keyspace: self,
            statement: self.statement(),
            key,
            builder: QueryStatement::encode_statement(Query::new(), &self.statement()),
            _marker: PhantomData,
        }
    }
    /// Calls the `Delete` implementation for this Key/Value pair using a prepared statement id
    fn delete_prepared<'a, V>(&'a self, key: &'a K) -> DeleteBuilder<'a, Self, K, V, QueryConsistency, StaticRequest>
    where
        Self: Delete<K, V>,
    {
        DeleteBuilder {
            keyspace: self,
            statement: self.statement(),
            key,
            builder: PreparedStatement::encode_statement(Query::new(), &self.statement()),
            _marker: PhantomData,
        }
    }
}
pub trait GetDynamicDeleteRequest: Keyspace {
    /// Specifies the returned Value type for an upcoming select request
    fn delete_with<'a, V>(
        &'a self,
        statement: &str,
        variables: &'a [&dyn TokenEncoder],
        statement_type: StatementType,
    ) -> DeleteBuilder<'a, Self, [&dyn TokenEncoder], V, QueryConsistency, DynamicRequest> {
        match statement_type {
            StatementType::Query => self.delete_query_with(statement, variables),
            StatementType::Prepared => self.delete_prepared_with(statement, variables),
        }
    }
    /// Specifies the returned Value type for an upcoming select request using a query statement
    fn delete_query_with<'a, V>(
        &'a self,
        statement: &str,
        variables: &'a [&dyn TokenEncoder],
    ) -> DeleteBuilder<'a, Self, [&dyn TokenEncoder], V, QueryConsistency, DynamicRequest> {
        DeleteBuilder {
            keyspace: self,
            statement: statement.to_owned().into(),
            key: variables,
            builder: QueryStatement::encode_statement(Query::new(), statement),
            _marker: PhantomData,
        }
    }
    /// Specifies the returned Value type for an upcoming select request using a prepared statement id
    fn delete_prepared_with<'a, V>(
        &'a self,
        statement: &str,
        variables: &'a [&dyn TokenEncoder],
    ) -> DeleteBuilder<'a, Self, [&dyn TokenEncoder], V, QueryConsistency, DynamicRequest> {
        DeleteBuilder {
            keyspace: self,
            statement: statement.to_owned().into(),
            key: variables,
            builder: PreparedStatement::encode_statement(Query::new(), statement),
            _marker: PhantomData,
        }
    }
}

impl<S: Keyspace, K> GetStaticDeleteRequest<K> for S {}
impl<S: Keyspace> GetDynamicDeleteRequest for S {}
pub struct DeleteBuilder<'a, S, K: ?Sized, V, Stage, T> {
    pub(crate) keyspace: &'a S,
    pub(crate) statement: Cow<'static, str>,
    pub(crate) key: &'a K,
    pub(crate) builder: QueryBuilder<Stage>,
    pub(crate) _marker: PhantomData<fn(V, T) -> (V, T)>,
}

impl<'a, S: Delete<K, V>, K, V> DeleteBuilder<'a, S, K, V, QueryConsistency, StaticRequest> {
    pub fn consistency(self, consistency: Consistency) -> DeleteBuilder<'a, S, K, V, QueryValues, StaticRequest> {
        DeleteBuilder {
            _marker: self._marker,
            keyspace: self.keyspace,
            statement: self.statement,
            key: self.key,
            builder: *S::bind_values(self.builder.consistency(consistency), &self.key),
        }
    }
}

impl<'a, S: Keyspace, V> DeleteBuilder<'a, S, [&dyn TokenEncoder], V, QueryConsistency, DynamicRequest> {
    pub fn consistency(
        self,
        consistency: Consistency,
    ) -> DeleteBuilder<'a, S, [&'a dyn TokenEncoder], V, QueryValues, DynamicRequest> {
        let builder = self.builder.consistency(consistency);
        let builder = *match self.key.len() {
            0 => builder.null_value(),
            _ => {
                let mut iter = self.key.iter();
                let mut builder = builder.value(iter.next().unwrap());
                for v in iter {
                    builder = builder.value(v);
                }
                builder
            }
        };
        DeleteBuilder {
            _marker: self._marker,
            keyspace: self.keyspace,
            statement: self.statement,
            key: self.key,
            builder,
        }
    }
}

impl<'a, S: Delete<K, V>, K: ComputeToken, V> DeleteBuilder<'a, S, K, V, QueryValues, StaticRequest> {
    pub fn timestamp(self, timestamp: i64) -> DeleteBuilder<'a, S, K, V, QueryBuild, StaticRequest> {
        DeleteBuilder {
            keyspace: self.keyspace,
            statement: self.statement,
            key: self.key,
            builder: self.builder.timestamp(timestamp),
            _marker: self._marker,
        }
    }
    /// Build the DeleteRequest
    pub fn build(self) -> anyhow::Result<DeleteRequest> {
        let query = self.builder.build()?;
        // create the request
        Ok(DeleteRequest {
            token: self.key.token(),
            inner: query.into(),
            statement: self.statement,
        })
    }
}

impl<'a, S: Keyspace, V> DeleteBuilder<'a, S, [&dyn TokenEncoder], V, QueryValues, DynamicRequest> {
    /// Build the DeleteRequest
    pub fn build(self) -> anyhow::Result<DeleteRequest> {
        let mut token_chain = TokenEncodeChain::default();
        for v in self.key.iter() {
            token_chain.dyn_chain(*v);
        }
        let query = self.builder.build()?;
        // create the request
        Ok(DeleteRequest {
            token: token_chain.finish(),
            inner: query.into(),
            statement: self.statement,
        })
    }
}

impl<'a, S: Delete<K, V>, K: ComputeToken, V, T> DeleteBuilder<'a, S, K, V, QueryBuild, T> {
    /// Build the DeleteRequest
    pub fn build(self) -> anyhow::Result<DeleteRequest> {
        let query = self.builder.build()?;
        // create the request
        Ok(DeleteRequest {
            token: self.key.token(),
            inner: query.into(),
            statement: self.statement,
        })
    }
}

impl<'a, S: Keyspace, V> DeleteBuilder<'a, S, [&dyn TokenEncoder], V, QueryBuild, DynamicRequest> {
    /// Build the DeleteRequest
    pub fn build(self) -> anyhow::Result<DeleteRequest> {
        let mut token_chain = TokenEncodeChain::default();
        for v in self.key.iter() {
            token_chain.dyn_chain(*v);
        }
        let query = self.builder.build()?;
        // create the request
        Ok(DeleteRequest {
            token: token_chain.finish(),
            inner: query.into(),
            statement: self.statement,
        })
    }
}

/// A request to delete a record which can be sent to the ring
pub struct DeleteRequest {
    token: i64,
    inner: Vec<u8>,
    statement: Cow<'static, str>,
}

impl std::fmt::Debug for DeleteRequest {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DeleteRequest")
            .field("token", &self.token)
            .field("inner", &self.inner)
            .field("statement", &self.statement)
            .finish()
    }
}

impl Clone for DeleteRequest {
    fn clone(&self) -> Self {
        Self {
            token: self.token,
            inner: self.inner.clone(),
            statement: self.statement.clone(),
        }
    }
}

impl Request for DeleteRequest {
    type Marker = DecodeVoid;
    const TYPE: RequestType = RequestType::Delete;

    fn token(&self) -> i64 {
        self.token
    }

    fn marker() -> Self::Marker {
        DecodeVoid
    }

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

impl DeleteRequest {
    pub fn send_local(self) -> Result<DecodeResult<<Self as Request>::Marker>, RequestError>
    where
        Self: 'static + Sized,
    {
        send_local(self.token(), self.into_payload(), BasicWorker::new())?;
        Ok(DecodeResult::new(<Self as Request>::marker(), <Self as Request>::TYPE))
    }

    pub fn send_global(self) -> Result<DecodeResult<<Self as Request>::Marker>, RequestError>
    where
        Self: 'static + Sized,
    {
        send_global(self.token(), self.into_payload(), BasicWorker::new())?;
        Ok(DecodeResult::new(<Self as Request>::marker(), <Self as Request>::TYPE))
    }

    pub fn worker(self) -> Box<BasicRetryWorker<Self>> {
        BasicRetryWorker::new(self)
    }
}

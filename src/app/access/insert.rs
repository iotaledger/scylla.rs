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

/// Insert query trait which creates an `InsertRequest`
/// that can be sent to the `Ring`.
///
/// ## Examples
/// ```
/// use scylla_rs::{
///     app::{
///         access::{
///             ComputeToken,
///             GetInsertRequest,
///             Insert,
///             Keyspace,
///         },
///         worker::InsertWorker,
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
/// impl Insert<MyKeyType, MyValueType> for MyKeyspace {
///     type QueryOrPrepared = PreparedStatement;
///     fn statement(&self) -> Cow<'static, str> {
///         format!("INSERT INTO {}.table (key, val1, val2) VALUES (?,?,?)", self.name()).into()
///     }
///
///     fn bind_values<T: Values>(builder: T, key: &MyKeyType, value: &MyValueType) -> T::Return {
///         builder.value(key).value(value).value(value)
///     }
/// }
///
/// # let keyspace = MyKeyspace::new();
/// # let (my_key, my_val) = (1, 1.0);
/// let worker = InsertWorker::boxed(keyspace.clone(), my_key, my_val, 3);
///
/// let request = keyspace // A Scylla keyspace
///     .insert(&my_key, &my_val) // Get the Insert Request
///     .consistency(Consistency::One)
///     .build()?;
/// # Ok::<(), anyhow::Error>(())
/// ```
pub trait Insert<K, V>: Keyspace + VoidDecoder + ComputeToken<K> {
    /// Set the query type; `QueryStatement` or `PreparedStatement`
    type QueryOrPrepared: QueryOrPrepared;
    /// Create your insert statement here.
    fn statement(&self) -> Cow<'static, str>;
    /// Get the MD5 hash of this implementation's statement
    /// for use when generating queries that should use
    /// the prepared statement.
    fn id(&self) -> [u8; 16] {
        md5::compute(self.insert_statement().as_bytes()).into()
    }
    /// Bind the cql values to the builder
    fn bind_values<T: Values>(builder: T, key: &K, value: &V) -> T::Return;
}

/// Wrapper for the `Insert` trait which provides the `insert` function
pub trait GetInsertRequest<K, V>: Keyspace {
    /// Calls the appropriate `Insert` implementation for this Key/Value pair
    fn insert<'a>(&'a self, key: &'a K, value: &'a V) -> UpsertBuilder<'a, Self, K, V, QueryConsistency, StaticRequest>
    where
        Self: Insert<K, V>,
    {
        UpsertBuilder {
            keyspace: self,
            statement: self.statement(),
            key,
            value,
            builder: Self::QueryOrPrepared::encode_statement(Query::new(), &self.statement()),
            _marker: PhantomData,
        }
    }
    /// Calls the `Insert` implementation for this Key/Value pair using a query statement
    fn insert_query<'a>(
        &'a self,
        key: &'a K,
        value: &'a V,
    ) -> UpsertBuilder<'a, Self, K, V, QueryConsistency, StaticRequest>
    where
        Self: Insert<K, V>,
    {
        UpsertBuilder {
            keyspace: self,
            statement: self.statement(),
            key,
            value,
            builder: QueryStatement::encode_statement(Query::new(), &self.statement()),
            _marker: PhantomData,
        }
    }
    /// Calls the `Insert` implementation for this Key/Value pair using a prepared statement id
    fn insert_prepared<'a>(
        &'a self,
        key: &'a K,
        value: &'a V,
    ) -> UpsertBuilder<'a, Self, K, V, QueryConsistency, StaticRequest>
    where
        Self: Insert<K, V>,
    {
        UpsertBuilder {
            keyspace: self,
            statement: self.statement(),
            key,
            value,
            builder: PreparedStatement::encode_statement(Query::new(), &self.statement()),
            _marker: PhantomData,
        }
    }

    /// Specifies the returned Value type for an upcoming select request
    fn insert_with<'a>(
        &'a self,
        statement: &str,
        key: &'a K,
        value: &'a V,
        statement_type: StatementType,
    ) -> UpsertBuilder<'a, Self, K, V, QueryConsistency, DynamicRequest> {
        match statement_type {
            StatementType::Query => self.insert_query_with(statement, key, value),
            StatementType::Prepared => self.insert_prepared_with(statement, key, value),
        }
    }
    /// Specifies the returned Value type for an upcoming select request using a query statement
    fn insert_query_with<'a>(
        &'a self,
        statement: &str,
        key: &'a K,
        value: &'a V,
    ) -> UpsertBuilder<'a, Self, K, V, QueryConsistency, DynamicRequest> {
        UpsertBuilder {
            keyspace: self,
            statement: statement.to_owned().into(),
            key,
            value,
            builder: QueryStatement::encode_statement(Query::new(), statement),
            _marker: PhantomData,
        }
    }
    /// Specifies the returned Value type for an upcoming select request using a prepared statement id
    fn insert_prepared_with<'a>(
        &'a self,
        statement: &str,
        key: &'a K,
        value: &'a V,
    ) -> UpsertBuilder<'a, Self, K, V, QueryConsistency, DynamicRequest> {
        UpsertBuilder {
            keyspace: self,
            statement: statement.to_owned().into(),
            key,
            value,
            builder: PreparedStatement::encode_statement(Query::new(), statement),
            _marker: PhantomData,
        }
    }
}

impl<S: Keyspace, K, V> GetInsertRequest<K, V> for S {}
pub struct UpsertBuilder<'a, S, K, V, Stage, T> {
    pub(crate) keyspace: &'a S,
    pub(crate) statement: Cow<'static, str>,
    pub(crate) key: &'a K,
    pub(crate) value: &'a V,
    pub(crate) builder: QueryBuilder<Stage>,
    pub(crate) _marker: PhantomData<fn(T) -> T>,
}
impl<'a, S: Insert<K, V>, K, V, T> UpsertBuilder<'a, S, K, V, QueryConsistency, T> {
    pub fn consistency(self, consistency: Consistency) -> UpsertBuilder<'a, S, K, V, QueryFlags, T> {
        UpsertBuilder {
            keyspace: self.keyspace,
            statement: self.statement,
            key: self.key,
            value: self.value,
            builder: self.builder.consistency(consistency),
            _marker: self._marker,
        }
    }
}

impl<'a, S: Keyspace, K, V> UpsertBuilder<'a, S, K, V, QueryFlags, DynamicRequest> {
    pub fn bind_values<F: Fn(QueryBuilder<QueryFlags>, &K, &V) -> QueryBuilder<QueryValues>>(
        self,
        bind_fn: F,
    ) -> UpsertBuilder<'a, S, K, V, QueryValues, DynamicRequest> {
        UpsertBuilder {
            keyspace: self.keyspace,
            statement: self.statement,
            key: self.key,
            value: self.value,
            builder: bind_fn(self.builder, &self.key, &self.value),
            _marker: self._marker,
        }
    }
}

impl<'a, S: Insert<K, V>, K, V, T> UpsertBuilder<'a, S, K, V, QueryFlags, T> {
    pub fn timestamp(self, timestamp: i64) -> UpsertBuilder<'a, S, K, V, QueryBuild, T> {
        UpsertBuilder {
            _marker: self._marker,
            keyspace: self.keyspace,
            statement: self.statement,
            key: self.key,
            value: self.value,
            builder: S::bind_values(self.builder, &self.key, &self.value).timestamp(timestamp),
        }
    }
}
impl<'a, S: Keyspace + ComputeToken<K>, K, V> UpsertBuilder<'a, S, K, V, QueryFlags, DynamicRequest> {
    /// Build the InsertRequest
    pub fn build(self) -> anyhow::Result<UpsertRequest<S, K, V, DynamicRequest>> {
        let query = self.builder.build()?;
        // create the request
        Ok(UpsertRequest {
            token: S::token(self.key),
            inner: query.into(),
            statement: self.statement,
            _marker: PhantomData,
        })
    }
}
impl<'a, S: Insert<K, V>, K, V> UpsertBuilder<'a, S, K, V, QueryFlags, StaticRequest> {
    /// Build the InsertRequest
    pub fn build(self) -> anyhow::Result<UpsertRequest<S, K, V, StaticRequest>> {
        let query = S::bind_values(self.builder, &self.key, &self.value).build()?;
        // create the request
        Ok(UpsertRequest {
            token: S::token(self.key),
            inner: query.into(),
            statement: self.statement,
            _marker: PhantomData,
        })
    }
}

impl<'a, S: Insert<K, V>, K, V, T> UpsertBuilder<'a, S, K, V, QueryValues, T> {
    pub fn timestamp(self, timestamp: i64) -> UpsertBuilder<'a, S, K, V, QueryBuild, T> {
        UpsertBuilder {
            keyspace: self.keyspace,
            statement: self.statement,
            key: self.key,
            value: self.value,
            builder: self.builder.timestamp(timestamp),
            _marker: self._marker,
        }
    }
    /// Build the InsertRequest
    pub fn build(self) -> anyhow::Result<UpsertRequest<S, K, V, T>> {
        let query = self.builder.build()?;
        // create the request
        Ok(UpsertRequest {
            token: S::token(self.key),
            inner: query.into(),
            statement: self.statement,
            _marker: PhantomData,
        })
    }
}

impl<'a, S: Insert<K, V>, K, V, T> UpsertBuilder<'a, S, K, V, QueryBuild, T> {
    /// Build the InsertRequest
    pub fn build(self) -> anyhow::Result<UpsertRequest<S, K, V, T>> {
        let query = self.builder.build()?;
        // create the request
        Ok(UpsertRequest {
            token: S::token(self.key),
            inner: query.into(),
            statement: self.statement,
            _marker: PhantomData,
        })
    }
}

/// A request to insert a record which can be sent to the ring
pub struct UpsertRequest<S, K, V, T> {
    token: i64,
    inner: Vec<u8>,
    statement: Cow<'static, str>,
    _marker: PhantomData<fn(S, K, V, T) -> (S, K, V, T)>,
}

impl<S, K, V, T> std::fmt::Debug for UpsertRequest<S, K, V, T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("UpsertRequest")
            .field("token", &self.token)
            .field("inner", &self.inner)
            .field("statement", &self.statement)
            .finish()
    }
}

impl<S, K, V, T> Clone for UpsertRequest<S, K, V, T> {
    fn clone(&self) -> Self {
        Self {
            token: self.token,
            inner: self.inner.clone(),
            statement: self.statement.clone(),
            _marker: PhantomData,
        }
    }
}

impl<S, K, V, T> Request for UpsertRequest<S, K, V, T> {
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

impl<S, K, V, T> SendRequestExt for UpsertRequest<S, K, V, T> {
    type Marker = DecodeVoid<S>;
    // TODO: Since this request is used for both Inserts and Updates, maybe split this again?
    const TYPE: RequestType = RequestType::Insert;

    fn token(&self) -> i64 {
        self.token
    }

    fn marker() -> Self::Marker {
        DecodeVoid::<S>::new()
    }
}

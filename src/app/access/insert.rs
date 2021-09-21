// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

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
/// # impl VoidDecoder for MyKeyspace {}
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
    type QueryOrPrepared: InsertRecommended<Self, K, V>;
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

pub trait InsertRecommended<S: Insert<K, V>, K, V>: QueryOrPrepared {
    fn make<T: Statements>(query_or_batch: T, keyspace: &S) -> T::Return {
        Self::encode_statement(query_or_batch, &keyspace.statement())
    }
}

impl<S: Insert<K, V>, K, V> InsertRecommended<S, K, V> for QueryStatement {}

impl<S: Insert<K, V>, K, V> InsertRecommended<S, K, V> for PreparedStatement {}

/// Wrapper for the `Insert` trait which provides the `insert` function
pub trait GetInsertRequest<S, K, V> {
    /// Calls the appropriate `Insert` implementation for this Key/Value pair
    fn insert<'a>(&'a self, key: &'a K, value: &'a V) -> InsertBuilder<'a, S, K, V, QueryConsistency>
    where
        S: Insert<K, V>;
    /// Calls `Insert` implementation for this Key/Value pair using a query statement
    fn insert_query<'a>(&'a self, key: &'a K, value: &'a V) -> InsertBuilder<'a, S, K, V, QueryConsistency>
    where
        S: Insert<K, V>;
    /// Calls `Insert` implementation for this Key/Value pair using a prepared statement id
    fn insert_prepared<'a>(&'a self, key: &'a K, value: &'a V) -> InsertBuilder<'a, S, K, V, QueryConsistency>
    where
        S: Insert<K, V>;
}

impl<S: Insert<K, V>, K, V> GetInsertRequest<S, K, V> for S {
    fn insert<'a>(&'a self, key: &'a K, value: &'a V) -> InsertBuilder<'a, S, K, V, QueryConsistency>
    where
        S: Insert<K, V>,
    {
        InsertBuilder {
            _marker: PhantomData,
            keyspace: self,
            key,
            value,
            builder: S::QueryOrPrepared::make(Query::new(), self),
        }
    }
    fn insert_query<'a>(&'a self, key: &'a K, value: &'a V) -> InsertBuilder<'a, S, K, V, QueryConsistency>
    where
        S: Insert<K, V>,
    {
        InsertBuilder {
            _marker: PhantomData,
            keyspace: self,
            key,
            value,
            builder: <QueryStatement as InsertRecommended<S, K, V>>::make(Query::new(), self),
        }
    }
    fn insert_prepared<'a>(&'a self, key: &'a K, value: &'a V) -> InsertBuilder<'a, S, K, V, QueryConsistency>
    where
        S: Insert<K, V>,
    {
        InsertBuilder {
            _marker: PhantomData,
            keyspace: self,
            key,
            value,
            builder: <PreparedStatement as InsertRecommended<S, K, V>>::make(Query::new(), self),
        }
    }
}
pub struct InsertBuilder<'a, S, K, V, Stage> {
    _marker: PhantomData<(&'a S, &'a K, &'a V)>,
    keyspace: &'a S,
    key: &'a K,
    value: &'a V,
    builder: QueryBuilder<Stage>,
}
impl<'a, S: Insert<K, V>, K, V> InsertBuilder<'a, S, K, V, QueryConsistency> {
    pub fn consistency(self, consistency: Consistency) -> InsertBuilder<'a, S, K, V, QueryValues> {
        InsertBuilder {
            _marker: self._marker,
            keyspace: self.keyspace,
            key: self.key,
            value: self.value,
            builder: S::bind_values(self.builder.consistency(consistency), self.key, self.value),
        }
    }
}

impl<'a, S: Insert<K, V>, K, V> InsertBuilder<'a, S, K, V, QueryValues> {
    pub fn timestamp(self, timestamp: i64) -> InsertBuilder<'a, S, K, V, QueryBuild> {
        InsertBuilder {
            _marker: self._marker,
            keyspace: self.keyspace,
            key: self.key,
            value: self.value,
            builder: self.builder.timestamp(timestamp),
        }
    }
    /// Build the InsertRequest
    pub fn build(self) -> anyhow::Result<InsertRequest<S, K, V>> {
        let query = self.builder.build()?;
        // create the request
        Ok(self.keyspace.create_request(query, S::token(self.key)))
    }
}

impl<'a, S: Insert<K, V>, K, V> InsertBuilder<'a, S, K, V, QueryBuild> {
    /// Build the InsertRequest
    pub fn build(self) -> anyhow::Result<InsertRequest<S, K, V>> {
        let query = self.builder.build()?;
        // create the request
        Ok(self.keyspace.create_request(query, S::token(self.key)))
    }
}

/// Defines two helper methods to specify statement / id
pub trait GetInsertStatement<S> {
    /// Specifies the Key and Value type for an insert statement
    fn insert_statement<K, V>(&self) -> Cow<'static, str>
    where
        S: Insert<K, V>;

    /// Specifies the Key and Value type for a prepared insert statement id
    fn insert_id<K, V>(&self) -> [u8; 16]
    where
        S: Insert<K, V>;
}

impl<S: Keyspace> GetInsertStatement<S> for S {
    fn insert_statement<K, V>(&self) -> Cow<'static, str>
    where
        S: Insert<K, V>,
    {
        S::statement(self)
    }

    fn insert_id<K, V>(&self) -> [u8; 16]
    where
        S: Insert<K, V>,
    {
        S::id(self)
    }
}

/// A request to insert a record which can be sent to the ring
#[derive(Clone, Debug)]
pub struct InsertRequest<S, K, V> {
    token: i64,
    inner: Vec<u8>,
    keyspace: S,
    _marker: PhantomData<(S, K, V)>,
}

impl<K, V, S: Insert<K, V> + Clone> CreateRequest<InsertRequest<S, K, V>> for S {
    /// Create a new Insert Request from a Query/Execute, token, and the keyspace.
    fn create_request<Q: Into<Vec<u8>>>(&self, query: Q, token: i64) -> InsertRequest<S, K, V> {
        InsertRequest {
            token,
            inner: query.into(),
            keyspace: self.clone(),
            _marker: PhantomData,
        }
    }
}

impl<S, K, V> Request for InsertRequest<S, K, V>
where
    S: Insert<K, V> + std::fmt::Debug,
    K: Send + std::fmt::Debug,
    V: Send + std::fmt::Debug,
{
    fn statement(&self) -> Cow<'static, str> {
        self.keyspace.insert_statement::<K, V>()
    }

    fn payload(&self) -> &Vec<u8> {
        &self.inner
    }
}

impl<S: Insert<K, V>, K, V> InsertRequest<S, K, V> {
    /// Send a local request using the keyspace impl and return a type marker
    pub fn send_local(self, worker: Box<dyn Worker>) -> DecodeResult<DecodeVoid<S>> {
        send_local(
            self.token,
            self.inner,
            worker,
            self.keyspace.name().clone().into_owned(),
        );
        DecodeResult::insert()
    }

    /// Send a global request using the keyspace impl and return a type marker
    pub fn send_global(self, worker: Box<dyn Worker>) -> DecodeResult<DecodeVoid<S>> {
        send_global(
            self.token,
            self.inner,
            worker,
            self.keyspace.name().clone().into_owned(),
        );
        DecodeResult::insert()
    }

    /// Consume the request to retrieve the payload
    pub fn into_payload(self) -> Vec<u8> {
        self.inner
    }
}

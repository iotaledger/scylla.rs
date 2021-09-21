// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use super::*;

/// Update query trait which creates an `UpdateRequest`
/// that can be sent to the `Ring`.
///
/// ## Examples
/// ```
/// use scylla_rs::{
///     app::access::{
///         ComputeToken,
///         GetUpdateRequest,
///         Keyspace,
///         Update,
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
/// impl Update<MyKeyType, MyValueType> for MyKeyspace {
///     type QueryOrPrepared = PreparedStatement;
///     fn statement(&self) -> Cow<'static, str> {
///         format!("UPDATE {}.table SET val1 = ?, val2 = ? WHERE key = ?", self.name()).into()
///     }
///
///     fn bind_values<T: Values>(builder: T, key: &MyKeyType, value: &MyValueType) -> T::Return {
///         builder.value(key).value(value).value(value)
///     }
/// }
///
/// # let keyspace = MyKeyspace::new();
/// # let (my_key, my_val) = (1, 1.0);
/// let request = keyspace // A Scylla keyspace
///     .update(&my_key, &my_val) // Get the Update Request
///     .consistency(Consistency::One)
///     .build()?;
/// # Ok::<(), anyhow::Error>(())
/// ```
pub trait Update<K, V>: Keyspace + VoidDecoder + ComputeToken<K> {
    /// Set the query type; `QueryStatement` or `PreparedStatement`
    type QueryOrPrepared: UpdateRecommended<Self, K, V>;

    /// Create your update statement here.
    fn statement(&self) -> Cow<'static, str>;
    /// Get the MD5 hash of this implementation's statement
    /// for use when generating queries that should use
    /// the prepared statement.
    fn id(&self) -> [u8; 16] {
        md5::compute(self.update_statement().as_bytes()).into()
    }
    /// Bind the cql values to the builder
    fn bind_values<T: Values>(builder: T, key: &K, value: &V) -> T::Return;
}

pub trait UpdateRecommended<S: Update<K, V>, K, V>: QueryOrPrepared {
    fn make<T: Statements>(query_or_batch: T, keyspace: &S) -> T::Return {
        Self::encode_statement(query_or_batch, &keyspace.statement())
    }
}

impl<S: Update<K, V>, K, V> UpdateRecommended<S, K, V> for QueryStatement {}

impl<S: Update<K, V>, K, V> UpdateRecommended<S, K, V> for PreparedStatement {}

/// Wrapper for the `Update` trait which provides the `update` function
pub trait GetUpdateRequest<S, K, V> {
    /// Calls the appropriate `Update` implementation for this Key/Value pair
    fn update<'a>(&'a self, key: &'a K, value: &'a V) -> UpdateBuilder<'a, S, K, V, QueryConsistency>
    where
        S: Update<K, V>;
    /// Calls the `Update` implementation for this Key/Value pair using a query statement
    fn update_query<'a>(&'a self, key: &'a K, value: &'a V) -> UpdateBuilder<'a, S, K, V, QueryConsistency>
    where
        S: Update<K, V>;
    /// Calls the `Update` implementation for this Key/Value pair using a prepared statement id
    fn update_prepared<'a>(&'a self, key: &'a K, value: &'a V) -> UpdateBuilder<'a, S, K, V, QueryConsistency>
    where
        S: Update<K, V>;
}

impl<S: Update<K, V>, K, V> GetUpdateRequest<S, K, V> for S {
    fn update<'a>(&'a self, key: &'a K, value: &'a V) -> UpdateBuilder<'a, S, K, V, QueryConsistency>
    where
        S: Update<K, V>,
    {
        UpdateBuilder {
            _marker: PhantomData,
            keyspace: self,
            key,
            value,
            builder: S::QueryOrPrepared::make(Query::new(), self),
        }
    }
    fn update_query<'a>(&'a self, key: &'a K, value: &'a V) -> UpdateBuilder<'a, S, K, V, QueryConsistency>
    where
        S: Update<K, V>,
    {
        UpdateBuilder {
            _marker: PhantomData,
            keyspace: self,
            key,
            value,
            builder: <QueryStatement as UpdateRecommended<S, K, V>>::make(Query::new(), self),
        }
    }
    fn update_prepared<'a>(&'a self, key: &'a K, value: &'a V) -> UpdateBuilder<'a, S, K, V, QueryConsistency>
    where
        S: Update<K, V>,
    {
        UpdateBuilder {
            _marker: PhantomData,
            keyspace: self,
            key,
            value,
            builder: <PreparedStatement as UpdateRecommended<S, K, V>>::make(Query::new(), self),
        }
    }
}
pub struct UpdateBuilder<'a, S, K, V, Stage> {
    _marker: PhantomData<(&'a S, &'a K, &'a V)>,
    keyspace: &'a S,
    key: &'a K,
    value: &'a V,
    builder: QueryBuilder<Stage>,
}
impl<'a, S: Update<K, V>, K, V> UpdateBuilder<'a, S, K, V, QueryConsistency> {
    pub fn consistency(self, consistency: Consistency) -> UpdateBuilder<'a, S, K, V, QueryValues> {
        UpdateBuilder {
            _marker: self._marker,
            keyspace: self.keyspace,
            key: self.key,
            value: self.value,
            builder: S::bind_values(self.builder.consistency(consistency), self.key, self.value),
        }
    }
}

impl<'a, S: Update<K, V>, K, V> UpdateBuilder<'a, S, K, V, QueryValues> {
    pub fn timestamp(self, timestamp: i64) -> UpdateBuilder<'a, S, K, V, QueryBuild> {
        UpdateBuilder {
            _marker: self._marker,
            keyspace: self.keyspace,
            key: self.key,
            value: self.value,
            builder: self.builder.timestamp(timestamp),
        }
    }
    /// Build the UpdateRequest
    pub fn build(self) -> anyhow::Result<UpdateRequest<S, K, V>> {
        let query = self.builder.build()?;
        // create the request
        Ok(self.keyspace.create_request(query, S::token(self.key)))
    }
}

impl<'a, S: Update<K, V>, K, V> UpdateBuilder<'a, S, K, V, QueryBuild> {
    /// Build the UpdateRequest
    pub fn build(self) -> anyhow::Result<UpdateRequest<S, K, V>> {
        let query = self.builder.build()?;
        // create the request
        Ok(self.keyspace.create_request(query, S::token(self.key)))
    }
}

/// Defines two helper methods to specify statement / id
pub trait GetUpdateStatement<S> {
    /// Specifies the Key and Value type for an update statement
    fn update_statement<K, V>(&self) -> Cow<'static, str>
    where
        S: Update<K, V>;

    /// Specifies the Key and Value type for a prepared update statement id
    fn update_id<K, V>(&self) -> [u8; 16]
    where
        S: Update<K, V>;
}

impl<S: Keyspace> GetUpdateStatement<S> for S {
    fn update_statement<K, V>(&self) -> Cow<'static, str>
    where
        S: Update<K, V>,
    {
        S::statement(self)
    }

    fn update_id<K, V>(&self) -> [u8; 16]
    where
        S: Update<K, V>,
    {
        S::id(self)
    }
}

/// A request to update a record which can be sent to the ring
#[derive(Clone, Debug)]
pub struct UpdateRequest<S, K, V> {
    token: i64,
    inner: Vec<u8>,
    keyspace: S,
    _marker: PhantomData<(S, K, V)>,
}

impl<K, V, S: Update<K, V> + Clone> CreateRequest<UpdateRequest<S, K, V>> for S {
    /// Create a new Update Request from a Query/Execute, token, and the keyspace.
    fn create_request<Q: Into<Vec<u8>>>(&self, query: Q, token: i64) -> UpdateRequest<S, K, V> {
        UpdateRequest::<S, K, V> {
            token,
            inner: query.into(),
            keyspace: self.clone(),
            _marker: PhantomData,
        }
    }
}

impl<S, K, V> Request for UpdateRequest<S, K, V>
where
    S: Update<K, V> + std::fmt::Debug + Clone,
    K: Send + std::fmt::Debug,
    V: Send + std::fmt::Debug,
{
    fn statement(&self) -> Cow<'static, str> {
        self.keyspace.update_statement::<K, V>()
    }

    fn payload(&self) -> &Vec<u8> {
        &self.inner
    }
}

impl<S: Update<K, V>, K, V> UpdateRequest<S, K, V> {
    /// Send a local request using the keyspace impl and return a type marker
    pub fn send_local(self, worker: Box<dyn Worker>) -> DecodeResult<DecodeVoid<S>> {
        send_local(
            self.token,
            self.inner,
            worker,
            self.keyspace.name().clone().into_owned(),
        );
        DecodeResult::update()
    }

    /// Send a global request using the keyspace impl and return a type marker
    pub fn send_global(self, worker: Box<dyn Worker>) -> DecodeResult<DecodeVoid<S>> {
        send_global(
            self.token,
            self.inner,
            worker,
            self.keyspace.name().clone().into_owned(),
        );
        DecodeResult::update()
    }

    /// Consume the request to retrieve the payload
    pub fn into_payload(self) -> Vec<u8> {
        self.inner
    }
}

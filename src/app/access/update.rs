// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use super::*;
use crate::{
    cql::{
        query::StatementType,
        TokenEncodeChain,
    },
    prelude::{
        ColumnEncoder,
        TokenEncoder,
    },
};

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
pub trait Update<K, V>: Keyspace {
    /// Set the query type; `QueryStatement` or `PreparedStatement`
    type QueryOrPrepared: QueryOrPrepared;

    /// Create your update statement here.
    fn statement(&self) -> Cow<'static, str>;
    /// Get the MD5 hash of this implementation's statement
    /// for use when generating queries that should use
    /// the prepared statement.
    fn id(&self) -> [u8; 16] {
        md5::compute(self.update_statement().as_bytes()).into()
    }
    /// Bind the cql values to the builder
    fn bind_values<T: Values>(builder: T, key: &K, value: &V) -> Box<T::Return>;
}

/// Wrapper for the `Update` trait which provides the `update` function
pub trait GetStaticUpdateRequest<K, V>: Keyspace {
    /// Calls the appropriate `Update` implementation for this Key/Value pair
    fn update<'a>(&'a self, key: &'a K, value: &'a V) -> UpdateBuilder<'a, Self, K, V, QueryConsistency, StaticRequest>
    where
        Self: Update<K, V>,
    {
        UpdateBuilder {
            keyspace: self,
            statement: self.statement(),
            key,
            value,
            builder: Self::QueryOrPrepared::encode_statement(Query::new(), &self.statement()),
            _marker: StaticRequest,
        }
    }
    /// Calls the `Update` implementation for this Key/Value pair using a query statement
    fn update_query<'a>(
        &'a self,
        key: &'a K,
        value: &'a V,
    ) -> UpdateBuilder<'a, Self, K, V, QueryConsistency, StaticRequest>
    where
        Self: Update<K, V>,
    {
        UpdateBuilder {
            keyspace: self,
            statement: self.statement(),
            key,
            value,
            builder: QueryStatement::encode_statement(Query::new(), &self.statement()),
            _marker: StaticRequest,
        }
    }
    /// Calls the `Update` implementation for this Key/Value pair using a prepared statement id
    fn update_prepared<'a>(
        &'a self,
        key: &'a K,
        value: &'a V,
    ) -> UpdateBuilder<'a, Self, K, V, QueryConsistency, StaticRequest>
    where
        Self: Update<K, V>,
    {
        UpdateBuilder {
            keyspace: self,
            statement: self.statement(),
            key,
            value,
            builder: PreparedStatement::encode_statement(Query::new(), &self.statement()),
            _marker: StaticRequest,
        }
    }
}
pub trait GetDynamicUpdateRequest: Keyspace {
    /// Specifies the returned Value type for an upcoming update request
    fn update_with<'a>(
        &'a self,
        statement: &str,
        key: &'a [&dyn TokenEncoder],
        variables: &'a [&dyn ColumnEncoder],
        statement_type: StatementType,
    ) -> UpdateBuilder<'a, Self, [&dyn TokenEncoder], [&dyn ColumnEncoder], QueryConsistency, DynamicRequest> {
        match statement_type {
            StatementType::Query => self.update_query_with(statement, key, variables),
            StatementType::Prepared => self.update_prepared_with(statement, key, variables),
        }
    }
    /// Specifies the returned Value type for an upcoming update request using a query statement
    fn update_query_with<'a>(
        &'a self,
        statement: &str,
        key: &'a [&dyn TokenEncoder],
        variables: &'a [&dyn ColumnEncoder],
    ) -> UpdateBuilder<'a, Self, [&dyn TokenEncoder], [&dyn ColumnEncoder], QueryConsistency, DynamicRequest> {
        UpdateBuilder {
            keyspace: self,
            statement: statement.to_owned().into(),
            key,
            value: variables,
            builder: QueryStatement::encode_statement(Query::new(), statement),
            _marker: DynamicRequest,
        }
    }
    /// Specifies the returned Value type for an upcoming update request using a prepared statement id
    fn update_prepared_with<'a>(
        &'a self,
        statement: &str,
        key: &'a [&dyn TokenEncoder],
        variables: &'a [&dyn ColumnEncoder],
    ) -> UpdateBuilder<'a, Self, [&dyn TokenEncoder], [&dyn ColumnEncoder], QueryConsistency, DynamicRequest> {
        UpdateBuilder {
            keyspace: self,
            statement: statement.to_owned().into(),
            key,
            value: variables,
            builder: PreparedStatement::encode_statement(Query::new(), statement),
            _marker: DynamicRequest,
        }
    }
}

pub trait AsDynamicUpdateRequest: Statement
where
    Self: Sized,
{
    /// Specifies the returned Value type for an upcoming update request
    fn as_update<'a>(
        &'a self,
        key: &'a [&dyn TokenEncoder],
        variables: &'a [&dyn ColumnEncoder],
        statement_type: StatementType,
    ) -> UpdateBuilder<'a, Self, [&'a dyn TokenEncoder], [&dyn ColumnEncoder], QueryConsistency, DynamicRequest> {
        match statement_type {
            StatementType::Query => self.as_update_query(key, variables),
            StatementType::Prepared => self.as_update_prepared(key, variables),
        }
    }
    /// Specifies the returned Value type for an upcoming update request using a query statement
    fn as_update_query<'a>(
        &'a self,
        key: &'a [&dyn TokenEncoder],
        variables: &'a [&dyn ColumnEncoder],
    ) -> UpdateBuilder<'a, Self, [&dyn TokenEncoder], [&dyn ColumnEncoder], QueryConsistency, DynamicRequest> {
        UpdateBuilder {
            _marker: DynamicRequest,
            keyspace: self,
            statement: self.to_string().to_owned().into(),
            key,
            value: variables,
            builder: QueryStatement::encode_statement(Query::new(), &self.to_string()),
        }
    }
    /// Specifies the returned Value type for an upcoming update request using a prepared statement id
    fn as_update_prepared<'a>(
        &'a self,
        key: &'a [&dyn TokenEncoder],
        variables: &'a [&dyn ColumnEncoder],
    ) -> UpdateBuilder<'a, Self, [&dyn TokenEncoder], [&dyn ColumnEncoder], QueryConsistency, DynamicRequest> {
        UpdateBuilder {
            _marker: DynamicRequest,
            keyspace: self,
            statement: self.to_string().to_owned().into(),
            key,
            value: variables,
            builder: PreparedStatement::encode_statement(Query::new(), &self.to_string()),
        }
    }
}

impl<S: Keyspace, K, V> GetStaticUpdateRequest<K, V> for S {}
impl<S: Keyspace> GetDynamicUpdateRequest for S {}
impl<S: Statement> AsDynamicUpdateRequest for S {}
pub struct UpdateBuilder<'a, S, K: ?Sized, V: ?Sized, Stage, T> {
    pub(crate) keyspace: &'a S,
    pub(crate) statement: Cow<'static, str>,
    pub(crate) key: &'a K,
    pub(crate) value: &'a V,
    pub(crate) builder: QueryBuilder<Stage>,
    pub(crate) _marker: T,
}

impl<'a, S: Update<K, V>, K, V> UpdateBuilder<'a, S, K, V, QueryConsistency, StaticRequest> {
    pub fn consistency(self, consistency: Consistency) -> UpdateBuilder<'a, S, K, V, QueryValues, StaticRequest> {
        UpdateBuilder {
            _marker: self._marker,
            keyspace: self.keyspace,
            statement: self.statement,
            key: self.key,
            value: self.value,
            builder: *S::bind_values(self.builder.consistency(consistency), &self.key, &self.value),
        }
    }
}

impl<'a, S: Keyspace>
    UpdateBuilder<'a, S, [&dyn TokenEncoder], [&'a dyn ColumnEncoder], QueryConsistency, DynamicRequest>
{
    pub fn bind_values<
        F: 'static
            + Fn(
                Box<dyn Values<Return = QueryBuilder<QueryValues>>>,
                &[&dyn TokenEncoder],
                &[&dyn ColumnEncoder],
            ) -> Box<QueryBuilder<QueryValues>>,
    >(
        self,
        bind_fn: F,
    ) -> UpdateBuilder<'a, S, [&'a dyn TokenEncoder], [&'a dyn ColumnEncoder], QueryConsistency, ManualBoundRequest>
    {
        UpdateBuilder {
            _marker: ManualBoundRequest {
                bind_fn: Box::new(bind_fn),
            },
            keyspace: self.keyspace,
            statement: self.statement,
            key: self.key,
            value: self.value,
            builder: self.builder,
        }
    }

    pub fn consistency(
        self,
        consistency: Consistency,
    ) -> UpdateBuilder<'a, S, [&'a dyn TokenEncoder], [&'a dyn ColumnEncoder], QueryValues, DynamicRequest> {
        let builder = self.builder.consistency(consistency);
        let builder = *match self.value.len() + self.key.len() {
            0 => builder.null_value(),
            _ => {
                let mut iter = self.value.iter();
                let mut builder = builder.value(iter.next().unwrap());
                for v in iter {
                    builder = builder.value(v);
                }
                for v in self.key.iter() {
                    builder = builder.value(v);
                }
                builder
            }
        };
        UpdateBuilder {
            _marker: self._marker,
            keyspace: self.keyspace,
            statement: self.statement,
            key: self.key,
            value: self.value,
            builder,
        }
    }
}

impl<'a, S: Keyspace>
    UpdateBuilder<'a, S, [&dyn TokenEncoder], [&dyn ColumnEncoder], QueryConsistency, ManualBoundRequest>
{
    pub fn consistency(
        self,
        consistency: Consistency,
    ) -> UpdateBuilder<'a, S, [&'a dyn TokenEncoder], [&'a dyn ColumnEncoder], QueryValues, DynamicRequest> {
        UpdateBuilder {
            _marker: DynamicRequest,
            keyspace: self.keyspace,
            statement: self.statement,
            key: self.key,
            value: self.value,
            builder: *(self._marker.bind_fn)(Box::new(self.builder.consistency(consistency)), &self.key, &self.value),
        }
    }
}

impl<'a, S: Update<K, V>, K: ComputeToken, V> UpdateBuilder<'a, S, K, V, QueryValues, StaticRequest> {
    pub fn timestamp(self, timestamp: i64) -> UpdateBuilder<'a, S, K, V, QueryBuild, StaticRequest> {
        UpdateBuilder {
            keyspace: self.keyspace,
            statement: self.statement,
            key: self.key,
            value: self.value,
            builder: self.builder.timestamp(timestamp),
            _marker: self._marker,
        }
    }
    /// Build the UpdateRequest
    pub fn build(self) -> anyhow::Result<UpdateRequest> {
        let query = self.builder.build()?;
        // create the request
        Ok(UpdateRequest {
            token: self.key.token(),
            inner: query.into(),
            statement: self.statement,
        })
    }
}

impl<'a, S: Keyspace, V> UpdateBuilder<'a, S, [&dyn TokenEncoder], V, QueryValues, DynamicRequest> {
    /// Build the UpdateRequest
    pub fn build(self) -> anyhow::Result<UpdateRequest> {
        let mut token_chain = TokenEncodeChain::default();
        for v in self.key.iter() {
            token_chain.dyn_chain(*v);
        }
        let query = self.builder.build()?;
        // create the request
        Ok(UpdateRequest {
            token: token_chain.finish(),
            inner: query.into(),
            statement: self.statement,
        })
    }
}

impl<'a, S: Update<K, V>, K: ComputeToken, V, T> UpdateBuilder<'a, S, K, V, QueryBuild, T> {
    /// Build the UpdateRequest
    pub fn build(self) -> anyhow::Result<UpdateRequest> {
        let query = self.builder.build()?;
        // create the request
        Ok(UpdateRequest {
            token: self.key.token(),
            inner: query.into(),
            statement: self.statement,
        })
    }
}

impl<'a, S: Keyspace, V> UpdateBuilder<'a, S, [&dyn TokenEncoder], V, QueryBuild, DynamicRequest> {
    /// Build the UpdateRequest
    pub fn build(self) -> anyhow::Result<UpdateRequest> {
        let mut token_chain = TokenEncodeChain::default();
        for v in self.key.iter() {
            token_chain.dyn_chain(*v);
        }
        let query = self.builder.build()?;
        // create the request
        Ok(UpdateRequest {
            token: token_chain.finish(),
            inner: query.into(),
            statement: self.statement,
        })
    }
}

/// A request to update a record which can be sent to the ring
pub struct UpdateRequest {
    token: i64,
    inner: Vec<u8>,
    statement: Cow<'static, str>,
}

impl std::fmt::Debug for UpdateRequest {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("UpdateRequest")
            .field("token", &self.token)
            .field("inner", &self.inner)
            .field("statement", &self.statement)
            .finish()
    }
}

impl Clone for UpdateRequest {
    fn clone(&self) -> Self {
        Self {
            token: self.token,
            inner: self.inner.clone(),
            statement: self.statement.clone(),
        }
    }
}

impl Request for UpdateRequest {
    type Marker = DecodeVoid;
    const TYPE: RequestType = RequestType::Update;

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

impl UpdateRequest {
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

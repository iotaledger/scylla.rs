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
pub trait Insert<K, V>: Keyspace {
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
    fn bind_values<T: Values>(builder: T, key: &K, value: &V) -> Box<T::Return>;
}

/// Wrapper for the `Insert` trait which provides the `insert` function
pub trait GetStaticInsertRequest<K, V>: Keyspace {
    /// Calls the appropriate `Insert` implementation for this Key/Value pair
    fn insert<'a>(&'a self, key: &'a K, value: &'a V) -> InsertBuilder<'a, Self, K, V, QueryConsistency, StaticRequest>
    where
        Self: Insert<K, V>,
    {
        InsertBuilder {
            keyspace: self,
            statement: self.statement(),
            key,
            value,
            builder: Self::QueryOrPrepared::encode_statement(Query::new(), &self.statement()),
            _marker: StaticRequest,
        }
    }
    /// Calls the `Insert` implementation for this Key/Value pair using a query statement
    fn insert_query<'a>(
        &'a self,
        key: &'a K,
        value: &'a V,
    ) -> InsertBuilder<'a, Self, K, V, QueryConsistency, StaticRequest>
    where
        Self: Insert<K, V>,
    {
        InsertBuilder {
            keyspace: self,
            statement: self.statement(),
            key,
            value,
            builder: QueryStatement::encode_statement(Query::new(), &self.statement()),
            _marker: StaticRequest,
        }
    }
    /// Calls the `Insert` implementation for this Key/Value pair using a prepared statement id
    fn insert_prepared<'a>(
        &'a self,
        key: &'a K,
        value: &'a V,
    ) -> InsertBuilder<'a, Self, K, V, QueryConsistency, StaticRequest>
    where
        Self: Insert<K, V>,
    {
        InsertBuilder {
            keyspace: self,
            statement: self.statement(),
            key,
            value,
            builder: PreparedStatement::encode_statement(Query::new(), &self.statement()),
            _marker: StaticRequest,
        }
    }
}
pub trait GetDynamicInsertRequest: Keyspace {
    /// Specifies the returned Value type for an upcoming select request
    fn insert_with<'a>(
        &'a self,
        statement: &str,
        key: &'a [&dyn TokenEncoder],
        variables: &'a [&dyn ColumnEncoder],
        statement_type: StatementType,
    ) -> InsertBuilder<'a, Self, [&dyn TokenEncoder], [&dyn ColumnEncoder], QueryConsistency, DynamicRequest> {
        match statement_type {
            StatementType::Query => self.insert_query_with(statement, key, variables),
            StatementType::Prepared => self.insert_prepared_with(statement, key, variables),
        }
    }
    /// Specifies the returned Value type for an upcoming select request using a query statement
    fn insert_query_with<'a>(
        &'a self,
        statement: &str,
        key: &'a [&dyn TokenEncoder],
        variables: &'a [&dyn ColumnEncoder],
    ) -> InsertBuilder<'a, Self, [&dyn TokenEncoder], [&dyn ColumnEncoder], QueryConsistency, DynamicRequest> {
        InsertBuilder {
            keyspace: self,
            statement: statement.to_owned().into(),
            key,
            value: variables,
            builder: QueryStatement::encode_statement(Query::new(), statement),
            _marker: DynamicRequest,
        }
    }
    /// Specifies the returned Value type for an upcoming select request using a prepared statement id
    fn insert_prepared_with<'a>(
        &'a self,
        statement: &str,
        key: &'a [&dyn TokenEncoder],
        variables: &'a [&dyn ColumnEncoder],
    ) -> InsertBuilder<'a, Self, [&dyn TokenEncoder], [&dyn ColumnEncoder], QueryConsistency, DynamicRequest> {
        InsertBuilder {
            keyspace: self,
            statement: statement.to_owned().into(),
            key,
            value: variables,
            builder: PreparedStatement::encode_statement(Query::new(), statement),
            _marker: DynamicRequest,
        }
    }
}

pub trait AsDynamicInsertRequest: Statement
where
    Self: Sized,
{
    /// Specifies the returned Value type for an upcoming insert request
    fn as_insert<'a>(
        &'a self,
        key: &'a [&dyn TokenEncoder],
        variables: &'a [&dyn ColumnEncoder],
        statement_type: StatementType,
    ) -> InsertBuilder<'a, Self, [&'a dyn TokenEncoder], [&dyn ColumnEncoder], QueryConsistency, DynamicRequest> {
        match statement_type {
            StatementType::Query => self.as_insert_query(key, variables),
            StatementType::Prepared => self.as_insert_prepared(key, variables),
        }
    }
    /// Specifies the returned Value type for an upcoming insert request using a query statement
    fn as_insert_query<'a>(
        &'a self,
        key: &'a [&dyn TokenEncoder],
        variables: &'a [&dyn ColumnEncoder],
    ) -> InsertBuilder<'a, Self, [&dyn TokenEncoder], [&dyn ColumnEncoder], QueryConsistency, DynamicRequest> {
        InsertBuilder {
            _marker: DynamicRequest,
            keyspace: self,
            statement: self.to_string().to_owned().into(),
            key,
            value: variables,
            builder: QueryStatement::encode_statement(Query::new(), &self.to_string()),
        }
    }
    /// Specifies the returned Value type for an upcoming insert request using a prepared statement id
    fn as_insert_prepared<'a>(
        &'a self,
        key: &'a [&dyn TokenEncoder],
        variables: &'a [&dyn ColumnEncoder],
    ) -> InsertBuilder<'a, Self, [&dyn TokenEncoder], [&dyn ColumnEncoder], QueryConsistency, DynamicRequest> {
        InsertBuilder {
            _marker: DynamicRequest,
            keyspace: self,
            statement: self.to_string().to_owned().into(),
            key,
            value: variables,
            builder: PreparedStatement::encode_statement(Query::new(), &self.to_string()),
        }
    }
}

impl<S: Keyspace, K, V> GetStaticInsertRequest<K, V> for S {}
impl<S: Keyspace> GetDynamicInsertRequest for S {}
impl<S: Statement> AsDynamicInsertRequest for S {}
pub struct InsertBuilder<'a, S, K: ?Sized, V: ?Sized, Stage, T> {
    pub(crate) keyspace: &'a S,
    pub(crate) statement: Cow<'static, str>,
    pub(crate) key: &'a K,
    pub(crate) value: &'a V,
    pub(crate) builder: QueryBuilder<Stage>,
    pub(crate) _marker: T,
}

impl<'a, S: Insert<K, V>, K, V> InsertBuilder<'a, S, K, V, QueryConsistency, StaticRequest> {
    pub fn consistency(self, consistency: Consistency) -> InsertBuilder<'a, S, K, V, QueryValues, StaticRequest> {
        InsertBuilder {
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
    InsertBuilder<'a, S, [&dyn TokenEncoder], [&dyn ColumnEncoder], QueryConsistency, DynamicRequest>
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
    ) -> InsertBuilder<'a, S, [&'a dyn TokenEncoder], [&'a dyn ColumnEncoder], QueryConsistency, ManualBoundRequest>
    {
        InsertBuilder {
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
    ) -> InsertBuilder<'a, S, [&'a dyn TokenEncoder], [&'a dyn ColumnEncoder], QueryValues, DynamicRequest> {
        let builder = self.builder.consistency(consistency);
        let builder = *match self.value.len() + self.key.len() {
            0 => builder.null_value(),
            _ => {
                let mut iter = self.key.iter();
                let mut builder = builder.value(iter.next().unwrap());
                for v in iter {
                    builder = builder.value(v);
                }
                for v in self.value.iter() {
                    builder = builder.value(v);
                }
                builder
            }
        };
        InsertBuilder {
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
    InsertBuilder<'a, S, [&dyn TokenEncoder], [&dyn ColumnEncoder], QueryConsistency, ManualBoundRequest>
{
    pub fn consistency(
        self,
        consistency: Consistency,
    ) -> InsertBuilder<'a, S, [&'a dyn TokenEncoder], [&'a dyn ColumnEncoder], QueryValues, DynamicRequest> {
        InsertBuilder {
            _marker: DynamicRequest,
            keyspace: self.keyspace,
            statement: self.statement,
            key: self.key,
            value: self.value,
            builder: *(self._marker.bind_fn)(Box::new(self.builder.consistency(consistency)), &self.key, &self.value),
        }
    }
}

impl<'a, S: Insert<K, V>, K: ComputeToken, V> InsertBuilder<'a, S, K, V, QueryValues, StaticRequest> {
    pub fn timestamp(self, timestamp: i64) -> InsertBuilder<'a, S, K, V, QueryBuild, StaticRequest> {
        InsertBuilder {
            keyspace: self.keyspace,
            statement: self.statement,
            key: self.key,
            value: self.value,
            builder: self.builder.timestamp(timestamp),
            _marker: self._marker,
        }
    }
    /// Build the InsertRequest
    pub fn build(self) -> anyhow::Result<InsertRequest> {
        let query = self.builder.build()?;
        // create the request
        Ok(InsertRequest {
            token: self.key.token(),
            inner: query.into(),
            statement: self.statement,
        })
    }
}

impl<'a, S: Keyspace, V: ?Sized> InsertBuilder<'a, S, [&dyn TokenEncoder], V, QueryValues, DynamicRequest> {
    /// Build the SelectRequest
    pub fn build(self) -> anyhow::Result<InsertRequest> {
        let mut token_chain = TokenEncodeChain::default();
        for v in self.key.iter() {
            token_chain.dyn_chain(*v);
        }
        let query = self.builder.build()?;
        // create the request
        Ok(InsertRequest {
            token: token_chain.finish(),
            inner: query.into(),
            statement: self.statement,
        })
    }
}

impl<'a, S: Insert<K, V>, K: ComputeToken, V, T> InsertBuilder<'a, S, K, V, QueryBuild, T> {
    /// Build the InsertRequest
    pub fn build(self) -> anyhow::Result<InsertRequest> {
        let query = self.builder.build()?;
        // create the request
        Ok(InsertRequest {
            token: self.key.token(),
            inner: query.into(),
            statement: self.statement,
        })
    }
}

impl<'a, S: Keyspace, V: ?Sized> InsertBuilder<'a, S, [&dyn TokenEncoder], V, QueryBuild, DynamicRequest> {
    /// Build the SelectRequest
    pub fn build(self) -> anyhow::Result<InsertRequest> {
        let mut token_chain = TokenEncodeChain::default();
        for v in self.key.iter() {
            token_chain.dyn_chain(*v);
        }
        let query = self.builder.build()?;
        // create the request
        Ok(InsertRequest {
            token: token_chain.finish(),
            inner: query.into(),
            statement: self.statement,
        })
    }
}

/// A request to insert a record which can be sent to the ring
pub struct InsertRequest {
    token: i64,
    inner: Vec<u8>,
    statement: Cow<'static, str>,
}

impl std::fmt::Debug for InsertRequest {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("InsertRequest")
            .field("token", &self.token)
            .field("inner", &self.inner)
            .field("statement", &self.statement)
            .finish()
    }
}

impl Clone for InsertRequest {
    fn clone(&self) -> Self {
        Self {
            token: self.token,
            inner: self.inner.clone(),
            statement: self.statement.clone(),
        }
    }
}

impl Request for InsertRequest {
    type Marker = DecodeVoid;
    const TYPE: RequestType = RequestType::Insert;

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

impl InsertRequest {
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

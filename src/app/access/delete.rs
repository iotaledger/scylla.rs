// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use super::*;

/// Delete query trait which creates a `DeleteRequest`
/// that can be sent to the `Ring`.
///
/// ## Example
/// ```
/// use scylla_rs::app::access::*;
/// #[derive(Clone, Debug)]
/// struct MyKeyspace {
///     pub name: String,
/// }
/// # impl MyKeyspace {
/// #     pub fn new(name: &str) -> Self {
/// #         Self {
/// #             name: name.to_string().into(),
/// #         }
/// #     }
/// # }
/// impl Keyspace for MyKeyspace {
///     fn name(&self) -> String {
///         self.name.clone()
///     }
/// }
/// # type MyKeyType = i32;
/// # type MyValueType = f32;
/// impl Delete<MyKeyType, MyValueType> for MyKeyspace {
///     type QueryOrPrepared = PreparedStatement;
///     fn statement(&self) -> Cow<'static, str> {
///         format!("DELETE FROM {}.table WHERE key = ?", self.name()).into()
///     }
///     fn bind_values<T: Values>(builder: T, key: &MyKeyType) -> T::Return {
///         builder.bind(key)
///     }
/// }
/// # let my_key = 1;
/// let request = MyKeyspace::new("my_keyspace")
///     .delete::<MyValueType>(&my_key)
///     .consistency(Consistency::One)
///     .build()?;
/// let worker = request.worker();
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
    fn bind_values<T: Values>(builder: T, key: &K) -> T::Return;
}

/// Specifies helper functions for creating static delete requests from a keyspace with a `Delete<K, V>` definition
pub trait GetStaticDeleteRequest<K>: Keyspace {
    /// Create a static delete request from a keyspace with a `Delete<K, V>` definition. Will use the default `type
    /// QueryOrPrepared` from the trait definition.
    ///
    /// ## Example
    /// ```no_run
    /// use scylla_rs::app::access::*;
    /// #[derive(Clone, Debug)]
    /// struct MyKeyspace {
    ///     pub name: String,
    /// }
    /// # impl MyKeyspace {
    /// #     pub fn new(name: &str) -> Self {
    /// #         Self {
    /// #             name: name.to_string().into(),
    /// #         }
    /// #     }
    /// # }
    /// impl Keyspace for MyKeyspace {
    ///     fn name(&self) -> String {
    ///         self.name.clone()
    ///     }
    /// }
    /// # type MyKeyType = i32;
    /// # type MyValueType = f32;
    /// impl Delete<MyKeyType, MyValueType> for MyKeyspace {
    ///     type QueryOrPrepared = PreparedStatement;
    ///     fn statement(&self) -> Cow<'static, str> {
    ///         format!("DELETE FROM {}.table WHERE key = ?", self.name()).into()
    ///     }
    ///     fn bind_values<T: Values>(builder: T, key: &MyKeyType) -> T::Return {
    ///         builder.bind(key)
    ///     }
    /// }
    /// # let my_key = 1;
    /// MyKeyspace::new("my_keyspace")
    ///     .delete::<MyValueType>(&my_key)
    ///     .consistency(Consistency::One)
    ///     .build()?
    ///     .get_local_blocking()?;
    /// # Ok::<(), anyhow::Error>(())
    /// ```
    fn delete<'a, V>(&'a self, key: &'a K) -> DeleteBuilder<'a, Self, K, V, QueryConsistency, StaticRequest>
    where
        Self: Delete<K, V>,
    {
        DeleteBuilder {
            keyspace: PhantomData,
            statement: self.statement(),
            key,
            builder: Self::QueryOrPrepared::encode_statement(Query::new(), &self.statement()),
            _marker: PhantomData,
        }
    }

    /// Create a static delete query request from a keyspace with a `Delete<K, V>` definition.
    ///
    /// ## Example
    /// ```no_run
    /// use scylla_rs::app::access::*;
    /// #[derive(Clone, Debug)]
    /// struct MyKeyspace {
    ///     pub name: String,
    /// }
    /// # impl MyKeyspace {
    /// #     pub fn new(name: &str) -> Self {
    /// #         Self {
    /// #             name: name.to_string().into(),
    /// #         }
    /// #     }
    /// # }
    /// impl Keyspace for MyKeyspace {
    ///     fn name(&self) -> String {
    ///         self.name.clone()
    ///     }
    /// }
    /// # type MyKeyType = i32;
    /// # type MyValueType = f32;
    /// impl Delete<MyKeyType, MyValueType> for MyKeyspace {
    ///     type QueryOrPrepared = PreparedStatement;
    ///     fn statement(&self) -> Cow<'static, str> {
    ///         format!("DELETE FROM {}.table WHERE key = ?", self.name()).into()
    ///     }
    ///     fn bind_values<T: Values>(builder: T, key: &MyKeyType) -> T::Return {
    ///         builder.bind(key)
    ///     }
    /// }
    /// # let my_key = 1;
    /// MyKeyspace::new("my_keyspace")
    ///     .delete_query::<MyValueType>(&my_key)
    ///     .consistency(Consistency::One)
    ///     .build()?
    ///     .get_local_blocking()?;
    /// # Ok::<(), anyhow::Error>(())
    /// ```
    fn delete_query<'a, V>(&'a self, key: &'a K) -> DeleteBuilder<'a, Self, K, V, QueryConsistency, StaticRequest>
    where
        Self: Delete<K, V>,
    {
        DeleteBuilder {
            keyspace: PhantomData,
            statement: self.statement(),
            key,
            builder: QueryStatement::encode_statement(Query::new(), &self.statement()),
            _marker: PhantomData,
        }
    }

    /// Create a static delete prepared request from a keyspace with a `Delete<K, V>` definition.
    ///
    /// ## Example
    /// ```no_run
    /// use scylla_rs::app::access::*;
    /// #[derive(Clone, Debug)]
    /// struct MyKeyspace {
    ///     pub name: String,
    /// }
    /// # impl MyKeyspace {
    /// #     pub fn new(name: &str) -> Self {
    /// #         Self {
    /// #             name: name.to_string().into(),
    /// #         }
    /// #     }
    /// # }
    /// impl Keyspace for MyKeyspace {
    ///     fn name(&self) -> String {
    ///         self.name.clone()
    ///     }
    /// }
    /// # type MyKeyType = i32;
    /// # type MyValueType = f32;
    /// impl Delete<MyKeyType, MyValueType> for MyKeyspace {
    ///     type QueryOrPrepared = PreparedStatement;
    ///     fn statement(&self) -> Cow<'static, str> {
    ///         format!("DELETE FROM {}.table WHERE key = ?", self.name()).into()
    ///     }
    ///     fn bind_values<T: Values>(builder: T, key: &MyKeyType) -> T::Return {
    ///         builder.bind(key)
    ///     }
    /// }
    /// # let my_key = 1;
    /// MyKeyspace::new("my_keyspace")
    ///     .delete_prepared::<MyValueType>(&my_key)
    ///     .consistency(Consistency::One)
    ///     .build()?
    ///     .get_local_blocking()?;
    /// # Ok::<(), anyhow::Error>(())
    /// ```
    fn delete_prepared<'a, V>(&'a self, key: &'a K) -> DeleteBuilder<'a, Self, K, V, QueryConsistency, StaticRequest>
    where
        Self: Delete<K, V>,
    {
        DeleteBuilder {
            keyspace: PhantomData,
            statement: self.statement(),
            key,
            builder: PreparedStatement::encode_statement(Query::new(), &self.statement()),
            _marker: PhantomData,
        }
    }
}

/// Specifies helper functions for creating dynamic delete requests from anything that can be interpreted as a keyspace
pub trait GetDynamicDeleteRequest: Keyspace {
    /// Create a dynamic delete request from a statement and variables. Can be specified as either
    /// a query or prepared statement. The token `{{keyspace}}` will be replaced with the keyspace name.
    ///
    /// ## Example
    /// ```no_run
    /// use scylla_rs::app::access::*;
    /// "my_keyspace"
    ///     .delete_with(
    ///         "DELETE FROM {{keyspace}}.table WHERE key = ?",
    ///         &[&3],
    ///         StatementType::Query,
    ///     )
    ///     .consistency(Consistency::One)
    ///     .build()?
    ///     .get_local_blocking()?;
    /// # Ok::<(), anyhow::Error>(())
    /// ```
    fn delete_with<'a>(
        &'a self,
        statement: &str,
        variables: &'a [&(dyn TokenChainer + Sync)],
        statement_type: StatementType,
    ) -> DeleteBuilder<'a, Self, [&(dyn TokenChainer + Sync)], (), QueryConsistency, DynamicRequest> {
        match statement_type {
            StatementType::Query => self.delete_query_with(statement, variables),
            StatementType::Prepared => self.delete_prepared_with(statement, variables),
        }
    }

    /// Create a dynamic query delete request from a statement and variables.
    /// The token `{{keyspace}}` will be replaced with the keyspace name.
    ///
    /// ## Example
    /// ```no_run
    /// use scylla_rs::app::access::*;
    /// "my_keyspace"
    ///     .delete_query_with("DELETE FROM {{keyspace}}.table WHERE key = ?", &[&3])
    ///     .consistency(Consistency::One)
    ///     .build()?
    ///     .get_local_blocking()?;
    /// # Ok::<(), anyhow::Error>(())
    /// ```
    fn delete_query_with<'a>(
        &'a self,
        statement: &str,
        variables: &'a [&(dyn TokenChainer + Sync)],
    ) -> DeleteBuilder<'a, Self, [&(dyn TokenChainer + Sync)], (), QueryConsistency, DynamicRequest> {
        DeleteBuilder {
            keyspace: PhantomData,
            statement: statement.to_owned().into(),
            key: variables,
            builder: QueryStatement::encode_statement(Query::new(), &self.replace_keyspace_token(statement)),
            _marker: PhantomData,
        }
    }

    /// Create a dynamic prepared delete request from a statement and variables.
    /// The token `{{keyspace}}` will be replaced with the keyspace name.
    ///
    /// ## Example
    /// ```no_run
    /// use scylla_rs::app::access::*;
    /// "my_keyspace"
    ///     .delete_prepared_with("DELETE FROM {{keyspace}}.table WHERE key = ?", &[&3])
    ///     .consistency(Consistency::One)
    ///     .build()?
    ///     .get_local_blocking()?;
    /// # Ok::<(), anyhow::Error>(())
    /// ```
    fn delete_prepared_with<'a>(
        &'a self,
        statement: &str,
        variables: &'a [&(dyn TokenChainer + Sync)],
    ) -> DeleteBuilder<'a, Self, [&(dyn TokenChainer + Sync)], (), QueryConsistency, DynamicRequest> {
        DeleteBuilder {
            keyspace: PhantomData,
            statement: statement.to_owned().into(),
            key: variables,
            builder: PreparedStatement::encode_statement(Query::new(), &self.replace_keyspace_token(statement)),
            _marker: PhantomData,
        }
    }
}

/// Specifies helper functions for creating dynamic delete requests from anything that can be interpreted as a statement
pub trait AsDynamicDeleteRequest: ToStatement
where
    Self: Sized,
{
    /// Create a dynamic delete request from a statement and variables. Can be specified as either
    /// a query or prepared statement.
    ///
    /// ## Example
    /// ```no_run
    /// use scylla_rs::app::access::*;
    /// "DELETE FROM my_keyspace.table WHERE key = ?"
    ///     .as_delete(&[&3], StatementType::Prepared)
    ///     .consistency(Consistency::One)
    ///     .build()?
    ///     .get_local_blocking()?;
    /// # Ok::<(), anyhow::Error>(())
    /// ```
    fn as_delete<'a>(
        &self,
        key: &'a [&'a (dyn TokenChainer + Sync)],
        statement_type: StatementType,
    ) -> DeleteBuilder<'a, Self, [&'a (dyn TokenChainer + Sync)], (), QueryConsistency, DynamicRequest> {
        match statement_type {
            StatementType::Query => self.as_delete_query(key),
            StatementType::Prepared => self.as_delete_prepared(key),
        }
    }

    /// Create a dynamic query delete request from a statement and variables.
    ///
    /// ## Example
    /// ```no_run
    /// use scylla_rs::app::access::*;
    /// "DELETE FROM my_keyspace.table WHERE key = ?"
    ///     .as_delete_query(&[&3])
    ///     .consistency(Consistency::One)
    ///     .build()?
    ///     .get_local_blocking()?;
    /// # Ok::<(), anyhow::Error>(())
    /// ```
    fn as_delete_query<'a>(
        &self,
        key: &'a [&(dyn TokenChainer + Sync)],
    ) -> DeleteBuilder<'a, Self, [&'a (dyn TokenChainer + Sync)], (), QueryConsistency, DynamicRequest> {
        let statement = self.to_statement();
        DeleteBuilder {
            _marker: PhantomData,
            keyspace: PhantomData,
            builder: QueryStatement::encode_statement(Query::new(), &statement),
            statement,
            key,
        }
    }

    /// Create a dynamic prepared delete request from a statement and variables.
    ///
    /// ## Example
    /// ```no_run
    /// use scylla_rs::app::access::*;
    /// "DELETE FROM my_keyspace.table WHERE key = ?"
    ///     .as_delete_prepared(&[&3])
    ///     .consistency(Consistency::One)
    ///     .build()?
    ///     .get_local_blocking()?;
    /// # Ok::<(), anyhow::Error>(())
    /// ```
    fn as_delete_prepared<'a>(
        &self,
        key: &'a [&(dyn TokenChainer + Sync)],
    ) -> DeleteBuilder<'a, Self, [&'a (dyn TokenChainer + Sync)], (), QueryConsistency, DynamicRequest> {
        let statement = self.to_statement();
        DeleteBuilder {
            _marker: PhantomData,
            keyspace: PhantomData,
            builder: PreparedStatement::encode_statement(Query::new(), &statement),
            statement,
            key,
        }
    }
}

impl<S: Keyspace, K> GetStaticDeleteRequest<K> for S {}
impl<S: Keyspace> GetDynamicDeleteRequest for S {}
impl<S: ToStatement> AsDynamicDeleteRequest for S {}

pub struct DeleteBuilder<'a, S, K: ?Sized, V, Stage, T> {
    pub(crate) keyspace: PhantomData<fn(S) -> S>,
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
            builder: S::bind_values(self.builder.consistency(consistency), &self.key),
        }
    }
}

impl<'a, S: Keyspace, V> DeleteBuilder<'a, S, [&(dyn TokenChainer + Sync)], V, QueryConsistency, DynamicRequest> {
    pub fn consistency(
        self,
        consistency: Consistency,
    ) -> DeleteBuilder<'a, S, [&'a (dyn TokenChainer + Sync)], V, QueryValues, DynamicRequest> {
        let builder = self.builder.consistency(consistency).bind(self.key);
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
        Ok(CommonRequest {
            token: self.key.token(),
            payload: query.into(),
            statement: self.statement,
        }
        .into())
    }
}

impl<'a, S: Keyspace, V> DeleteBuilder<'a, S, [&(dyn TokenChainer + Sync)], V, QueryValues, DynamicRequest> {
    pub fn timestamp(
        self,
        timestamp: i64,
    ) -> DeleteBuilder<'a, S, [&'a (dyn TokenChainer + Sync)], V, QueryBuild, DynamicRequest> {
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
        Ok(CommonRequest {
            token: self.key.get_token(),
            payload: query.into(),
            statement: self.statement,
        }
        .into())
    }
}

impl<'a, S: Delete<K, V>, K: ComputeToken, V, T> DeleteBuilder<'a, S, K, V, QueryBuild, T> {
    /// Build the DeleteRequest
    pub fn build(self) -> anyhow::Result<DeleteRequest> {
        let query = self.builder.build()?;
        // create the request
        Ok(CommonRequest {
            token: self.key.token(),
            payload: query.into(),
            statement: self.statement,
        }
        .into())
    }
}

impl<'a, S: Keyspace, V> DeleteBuilder<'a, S, [&(dyn TokenChainer + Sync)], V, QueryBuild, DynamicRequest> {
    /// Build the DeleteRequest
    pub fn build(self) -> anyhow::Result<DeleteRequest> {
        let query = self.builder.build()?;
        // create the request
        Ok(CommonRequest {
            token: self.key.get_token(),
            payload: query.into(),
            statement: self.statement,
        }
        .into())
    }
}

/// A request to delete a record which can be sent to the ring
#[derive(Debug, Clone)]
pub struct DeleteRequest(CommonRequest);

impl From<CommonRequest> for DeleteRequest {
    fn from(req: CommonRequest) -> Self {
        DeleteRequest(req)
    }
}

impl Deref for DeleteRequest {
    type Target = CommonRequest;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for DeleteRequest {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl Request for DeleteRequest {
    fn token(&self) -> i64 {
        self.0.token()
    }

    fn statement(&self) -> &Cow<'static, str> {
        self.0.statement()
    }

    fn payload(&self) -> Vec<u8> {
        self.0.payload()
    }
}

impl SendRequestExt for DeleteRequest {
    type Marker = DecodeVoid;
    type Worker = BasicRetryWorker<Self>;
    const TYPE: RequestType = RequestType::Delete;

    fn worker(self) -> Box<Self::Worker> {
        BasicRetryWorker::new(self)
    }
}

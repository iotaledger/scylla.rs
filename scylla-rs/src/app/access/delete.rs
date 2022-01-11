// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use scylla_parse::DeleteStatement;

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
///
///     fn opts(&self) -> KeyspaceOpts {
///         KeyspaceOptsBuilder::default()
///             .replication(Replication::network_topology(maplit::btreemap! {
///                 "datacenter1" => 1,
///             }))
///             .durable_writes(true)
///             .build()
///             .unwrap()
///     }
/// }
/// # type MyKeyType = i32;
/// # type MyVarType = String;
/// # type MyValueType = f32;
/// impl Delete<MyKeyType, MyVarType, MyValueType> for MyKeyspace {
///     type QueryOrPrepared = PreparedStatement;
///     fn statement(&self) -> DeleteStatement {
///         parse_statement!("DELETE FROM my_table WHERE key = ? AND var = ?")
///     }
///     fn bind_values<B: Binder>(builder: B, key: &MyKeyType, variables: &MyVarType) -> B {
///         builder.bind(key).bind(variables)
///     }
/// }
/// # let (my_key, my_var) = (1, MyVarType::default());
/// let request = MyKeyspace::new("my_keyspace")
///     .delete::<MyValueType>(&my_key, &my_var)
///     .consistency(Consistency::One)
///     .build()?;
/// let worker = request.worker();
/// # Ok::<(), anyhow::Error>(())
/// ```
pub trait Delete<K, V, D>: Keyspace {
    /// Set the query type; `QueryStatement` or `PreparedStatement`
    type QueryOrPrepared: QueryOrPrepared;

    /// Create your delete statement here.
    fn statement(&self) -> DeleteStatement;

    /// Get the MD5 hash of this implementation's statement
    /// for use when generating queries that should use
    /// the prepared statement.
    fn id(&self) -> [u8; 16] {
        md5::compute(self.delete_statement().to_string().as_bytes()).into()
    }

    /// Bind the cql values to the builder
    fn bind_values<B: Binder>(binder: B, key: &K, variables: &V) -> B;
}

/// Specifies helper functions for creating static delete requests from a keyspace with a `Delete<K, V>` definition
pub trait GetStaticDeleteRequest<K, V>: Keyspace {
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
    ///
    ///     fn opts(&self) -> KeyspaceOpts {
    ///         KeyspaceOptsBuilder::default()
    ///             .replication(Replication::network_topology(maplit::btreemap! {
    ///                 "datacenter1" => 1,
    ///             }))
    ///             .durable_writes(true)
    ///             .build()
    ///             .unwrap()
    ///     }
    /// }
    /// # type MyKeyType = i32;
    /// # type MyVarType = String;
    /// # type MyValueType = f32;
    /// impl Delete<MyKeyType, MyVarType, MyValueType> for MyKeyspace {
    ///     type QueryOrPrepared = PreparedStatement;
    ///     fn statement(&self) -> DeleteStatement {
    ///         parse_statement!("DELETE FROM my_table WHERE key = ? AND var = ?")
    ///     }
    ///     fn bind_values<B: Binder>(builder: B, key: &MyKeyType, variables: &MyVarType) -> B {
    ///         builder.bind(key).bind(variables)
    ///     }
    /// }
    /// # let (my_key, my_var) = (1, MyVarType::default());
    /// MyKeyspace::new("my_keyspace")
    ///     .delete::<MyValueType>(&my_key, &my_var)
    ///     .consistency(Consistency::One)
    ///     .build()?
    ///     .get_local_blocking()?;
    /// # Ok::<(), anyhow::Error>(())
    /// ```
    fn delete<'a, D>(
        &'a self,
        key: &'a K,
        variables: &'a V,
    ) -> DeleteBuilder<'a, Self, K, V, D, QueryConsistency, StaticRequest>
    where
        Self: Delete<K, V, D>,
    {
        let statement = self.statement();
        DeleteBuilder {
            keyspace: PhantomData,
            key,
            variables,
            builder: Self::QueryOrPrepared::encode_statement(Query::new(), &statement.to_string()),
            statement,
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
    ///
    ///     fn opts(&self) -> KeyspaceOpts {
    ///         KeyspaceOptsBuilder::default()
    ///             .replication(Replication::network_topology(maplit::btreemap! {
    ///                 "datacenter1" => 1,
    ///             }))
    ///             .durable_writes(true)
    ///             .build()
    ///             .unwrap()
    ///     }
    /// }
    /// # type MyKeyType = i32;
    /// # type MyVarType = String;
    /// # type MyValueType = f32;
    /// impl Delete<MyKeyType, MyVarType, MyValueType> for MyKeyspace {
    ///     type QueryOrPrepared = PreparedStatement;
    ///     fn statement(&self) -> DeleteStatement {
    ///         parse_statement!("DELETE FROM my_table WHERE key = ? AND var = ?")
    ///     }
    ///     fn bind_values<B: Binder>(builder: B, key: &MyKeyType, variables: &MyVarType) -> B {
    ///         builder.bind(key).bind(variables)
    ///     }
    /// }
    /// # let (my_key, my_var) = (1, MyVarType::default());
    /// MyKeyspace::new("my_keyspace")
    ///     .delete_query::<MyValueType>(&my_key, &my_var)
    ///     .consistency(Consistency::One)
    ///     .build()?
    ///     .get_local_blocking()?;
    /// # Ok::<(), anyhow::Error>(())
    /// ```
    fn delete_query<'a, D>(
        &'a self,
        key: &'a K,
        variables: &'a V,
    ) -> DeleteBuilder<'a, Self, K, V, D, QueryConsistency, StaticRequest>
    where
        Self: Delete<K, V, D>,
    {
        let statement = self.statement();
        DeleteBuilder {
            keyspace: PhantomData,
            key,
            variables,
            builder: QueryStatement::encode_statement(Query::new(), &statement.to_string()),
            statement,
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
    ///
    ///     fn opts(&self) -> KeyspaceOpts {
    ///         KeyspaceOptsBuilder::default()
    ///             .replication(Replication::network_topology(maplit::btreemap! {
    ///                 "datacenter1" => 1,
    ///             }))
    ///             .durable_writes(true)
    ///             .build()
    ///             .unwrap()
    ///     }
    /// }
    /// # type MyKeyType = i32;
    /// # type MyVarType = String;
    /// # type MyValueType = f32;
    /// impl Delete<MyKeyType, MyVarType, MyValueType> for MyKeyspace {
    ///     type QueryOrPrepared = PreparedStatement;
    ///     fn statement(&self) -> DeleteStatement {
    ///         parse_statement!("DELETE FROM my_table WHERE key = ? AND var = ?")
    ///     }
    ///     fn bind_values<B: Binder>(builder: B, key: &MyKeyType, variables: &MyVarType) -> B {
    ///         builder.bind(key).bind(variables)
    ///     }
    /// }
    /// # let (my_key, my_var) = (1, MyVarType::default());
    /// MyKeyspace::new("my_keyspace")
    ///     .delete_prepared::<MyValueType>(&my_key, &my_var)
    ///     .consistency(Consistency::One)
    ///     .build()?
    ///     .get_local_blocking()?;
    /// # Ok::<(), anyhow::Error>(())
    /// ```
    fn delete_prepared<'a, D>(
        &'a self,
        key: &'a K,
        variables: &'a V,
    ) -> DeleteBuilder<'a, Self, K, V, D, QueryConsistency, StaticRequest>
    where
        Self: Delete<K, V, D>,
    {
        let statement = self.statement();
        DeleteBuilder {
            keyspace: PhantomData,
            key,
            variables,
            builder: PreparedStatement::encode_statement(Query::new(), &statement.to_string()),
            statement,
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
    ///         parse_statement!("DELETE FROM my_table WHERE key = ? AND var = ?"),
    ///         &[&3],
    ///         &[&"hello"],
    ///         StatementType::Query,
    ///     )
    ///     .consistency(Consistency::One)
    ///     .build()?
    ///     .get_local_blocking()?;
    /// # Ok::<(), anyhow::Error>(())
    /// ```
    fn delete_with<'a>(
        &'a self,
        statement: DeleteStatement,
        key: &'a [&dyn BindableToken<QueryBuilder<QueryValues>>],
        variables: &'a [&dyn BindableValue<QueryBuilder<QueryValues>>],
        statement_type: StatementType,
    ) -> DeleteBuilder<
        'a,
        Self,
        [&'a dyn BindableToken<QueryBuilder<QueryValues>>],
        [&'a dyn BindableValue<QueryBuilder<QueryValues>>],
        (),
        QueryConsistency,
        DynamicRequest,
    > {
        match statement_type {
            StatementType::Query => self.delete_query_with(statement, key, variables),
            StatementType::Prepared => self.delete_prepared_with(statement, key, variables),
        }
    }

    /// Create a dynamic query delete request from a statement and variables.
    /// The token `{{keyspace}}` will be replaced with the keyspace name.
    ///
    /// ## Example
    /// ```no_run
    /// use scylla_rs::app::access::*;
    /// "my_keyspace"
    ///     .delete_query_with(
    ///         parse_statement!("DELETE FROM my_table WHERE key = ? AND var = ?"),
    ///         &[&3],
    ///         &[&"hello"],
    ///     )
    ///     .consistency(Consistency::One)
    ///     .build()?
    ///     .get_local_blocking()?;
    /// # Ok::<(), anyhow::Error>(())
    /// ```
    fn delete_query_with<'a>(
        &'a self,
        statement: DeleteStatement,
        key: &'a [&dyn BindableToken<QueryBuilder<QueryValues>>],
        variables: &'a [&dyn BindableValue<QueryBuilder<QueryValues>>],
    ) -> DeleteBuilder<
        'a,
        Self,
        [&'a dyn BindableToken<QueryBuilder<QueryValues>>],
        [&'a dyn BindableValue<QueryBuilder<QueryValues>>],
        (),
        QueryConsistency,
        DynamicRequest,
    > {
        let statement = statement.with_keyspace(self.name());
        DeleteBuilder {
            keyspace: PhantomData,
            builder: QueryStatement::encode_statement(Query::new(), &statement.to_string()),
            statement,
            key,
            variables,
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
    ///     .delete_prepared_with(
    ///         parse_statement!("DELETE FROM my_table WHERE key = ? AND var = ?"),
    ///         &[&3],
    ///         &[&"hello"],
    ///     )
    ///     .consistency(Consistency::One)
    ///     .build()?
    ///     .get_local_blocking()?;
    /// # Ok::<(), anyhow::Error>(())
    /// ```
    fn delete_prepared_with<'a>(
        &'a self,
        statement: DeleteStatement,
        key: &'a [&dyn BindableToken<QueryBuilder<QueryValues>>],
        variables: &'a [&dyn BindableValue<QueryBuilder<QueryValues>>],
    ) -> DeleteBuilder<
        'a,
        Self,
        [&'a dyn BindableToken<QueryBuilder<QueryValues>>],
        [&'a dyn BindableValue<QueryBuilder<QueryValues>>],
        (),
        QueryConsistency,
        DynamicRequest,
    > {
        let statement = statement.with_keyspace(self.name());
        DeleteBuilder {
            keyspace: PhantomData,
            builder: PreparedStatement::encode_statement(Query::new(), &statement.to_string()),
            statement,
            key,
            variables,
            _marker: PhantomData,
        }
    }
}

/// Specifies helper functions for creating dynamic delete requests from anything that can be interpreted as a statement
pub trait AsDynamicDeleteRequest
where
    Self: Sized,
{
    /// Create a dynamic delete request from a statement and variables. Can be specified as either
    /// a query or prepared statement.
    ///
    /// ## Example
    /// ```no_run
    /// use scylla_rs::app::access::*;
    /// parse_statement!("DELETE FROM my_keyspace.my_table WHERE key = ? AND var = ?")
    ///     .as_delete(&[&3], &[&"hello"], StatementType::Prepared)
    ///     .consistency(Consistency::One)
    ///     .build()?
    ///     .get_local_blocking()?;
    /// # Ok::<(), anyhow::Error>(())
    /// ```
    fn as_delete<'a>(
        self,
        key: &'a [&dyn BindableToken<QueryBuilder<QueryValues>>],
        variables: &'a [&dyn BindableValue<QueryBuilder<QueryValues>>],
        statement_type: StatementType,
    ) -> DeleteBuilder<
        'a,
        Self,
        [&'a dyn BindableToken<QueryBuilder<QueryValues>>],
        [&'a dyn BindableValue<QueryBuilder<QueryValues>>],
        (),
        QueryConsistency,
        DynamicRequest,
    > {
        match statement_type {
            StatementType::Query => self.as_delete_query(key, variables),
            StatementType::Prepared => self.as_delete_prepared(key, variables),
        }
    }

    /// Create a dynamic query delete request from a statement and variables.
    ///
    /// ## Example
    /// ```no_run
    /// use scylla_rs::app::access::*;
    /// parse_statement!("DELETE FROM my_keyspace.my_table WHERE key = ? AND var = ?")
    ///     .as_delete_query(&[&3], &[&"hello"])
    ///     .consistency(Consistency::One)
    ///     .build()?
    ///     .get_local_blocking()?;
    /// # Ok::<(), anyhow::Error>(())
    /// ```
    fn as_delete_query<'a>(
        self,
        key: &'a [&dyn BindableToken<QueryBuilder<QueryValues>>],
        variables: &'a [&dyn BindableValue<QueryBuilder<QueryValues>>],
    ) -> DeleteBuilder<
        'a,
        Self,
        [&'a dyn BindableToken<QueryBuilder<QueryValues>>],
        [&'a dyn BindableValue<QueryBuilder<QueryValues>>],
        (),
        QueryConsistency,
        DynamicRequest,
    >;

    /// Create a dynamic prepared delete request from a statement and variables.
    ///
    /// ## Example
    /// ```no_run
    /// use scylla_rs::app::access::*;
    /// parse_statement!("DELETE FROM my_keyspace.my_table WHERE key = ? AND var = ?")
    ///     .as_delete_prepared(&[&3], &[&"hello"])
    ///     .consistency(Consistency::One)
    ///     .build()?
    ///     .get_local_blocking()?;
    /// # Ok::<(), anyhow::Error>(())
    /// ```
    fn as_delete_prepared<'a>(
        self,
        key: &'a [&dyn BindableToken<QueryBuilder<QueryValues>>],
        variables: &'a [&dyn BindableValue<QueryBuilder<QueryValues>>],
    ) -> DeleteBuilder<
        'a,
        Self,
        [&'a dyn BindableToken<QueryBuilder<QueryValues>>],
        [&'a dyn BindableValue<QueryBuilder<QueryValues>>],
        (),
        QueryConsistency,
        DynamicRequest,
    >;
}

impl<S: Keyspace, K, V> GetStaticDeleteRequest<K, V> for S {}
impl<S: Keyspace> GetDynamicDeleteRequest for S {}
impl AsDynamicDeleteRequest for DeleteStatement {
    fn as_delete_query<'a>(
        self,
        key: &'a [&dyn BindableToken<QueryBuilder<QueryValues>>],
        variables: &'a [&dyn BindableValue<QueryBuilder<QueryValues>>],
    ) -> DeleteBuilder<
        'a,
        Self,
        [&'a dyn BindableToken<QueryBuilder<QueryValues>>],
        [&'a dyn BindableValue<QueryBuilder<QueryValues>>],
        (),
        QueryConsistency,
        DynamicRequest,
    > {
        DeleteBuilder {
            _marker: PhantomData,
            keyspace: PhantomData,
            builder: QueryStatement::encode_statement(Query::new(), &self.to_string()),
            statement: self,
            key,
            variables,
        }
    }

    fn as_delete_prepared<'a>(
        self,
        key: &'a [&dyn BindableToken<QueryBuilder<QueryValues>>],
        variables: &'a [&dyn BindableValue<QueryBuilder<QueryValues>>],
    ) -> DeleteBuilder<
        'a,
        Self,
        [&'a dyn BindableToken<QueryBuilder<QueryValues>>],
        [&'a dyn BindableValue<QueryBuilder<QueryValues>>],
        (),
        QueryConsistency,
        DynamicRequest,
    > {
        DeleteBuilder {
            _marker: PhantomData,
            keyspace: PhantomData,
            builder: PreparedStatement::encode_statement(Query::new(), &self.to_string()),
            statement: self,
            key,
            variables,
        }
    }
}

pub struct DeleteBuilder<'a, S, K: ?Sized, V: ?Sized, D, Stage, T> {
    pub(crate) keyspace: PhantomData<fn(S) -> S>,
    pub(crate) statement: DeleteStatement,
    pub(crate) key: &'a K,
    pub(crate) variables: &'a V,
    pub(crate) builder: QueryBuilder<Stage>,
    pub(crate) _marker: PhantomData<fn(D, T) -> (D, T)>,
}

impl<'a, S: Delete<K, V, D>, K: TokenEncoder, V, D> DeleteBuilder<'a, S, K, V, D, QueryConsistency, StaticRequest> {
    pub fn consistency(self, consistency: Consistency) -> DeleteBuilder<'a, S, K, V, D, QueryValues, StaticRequest> {
        DeleteBuilder {
            _marker: self._marker,
            keyspace: self.keyspace,
            statement: self.statement,
            key: self.key,
            variables: self.variables,
            builder: S::bind_values(
                self.builder.consistency(consistency).bind_values(),
                &self.key,
                &self.variables,
            ),
        }
    }

    pub fn timestamp(self, timestamp: i64) -> DeleteBuilder<'a, S, K, V, D, QueryBuild, StaticRequest> {
        DeleteBuilder {
            keyspace: self.keyspace,
            statement: self.statement,
            key: self.key,
            variables: self.variables,
            builder: S::bind_values(
                self.builder.consistency(Consistency::Quorum).bind_values(),
                &self.key,
                &self.variables,
            )
            .timestamp(timestamp),
            _marker: self._marker,
        }
    }

    pub fn build(self) -> anyhow::Result<DeleteRequest> {
        let query = S::bind_values(
            self.builder.consistency(Consistency::Quorum).bind_values(),
            &self.key,
            &self.variables,
        )
        .build()?;
        // create the request
        Ok(CommonRequest {
            token: self.key.token(),
            payload: query.into(),
            statement: self.statement.into(),
        }
        .into())
    }
}

impl<'a, S: Keyspace, D>
    DeleteBuilder<
        'a,
        S,
        [&'a dyn BindableToken<QueryBuilder<QueryValues>>],
        [&'a dyn BindableValue<QueryBuilder<QueryValues>>],
        D,
        QueryConsistency,
        DynamicRequest,
    >
{
    pub fn consistency(
        self,
        consistency: Consistency,
    ) -> DeleteBuilder<
        'a,
        S,
        [&'a dyn BindableToken<QueryBuilder<QueryValues>>],
        [&'a dyn BindableValue<QueryBuilder<QueryValues>>],
        D,
        QueryValues,
        DynamicRequest,
    > {
        DeleteBuilder {
            _marker: self._marker,
            keyspace: self.keyspace,
            statement: self.statement,
            key: self.key,
            variables: self.variables,
            builder: self.builder.consistency(consistency).bind_values().bind(self.key),
        }
    }

    pub fn timestamp(
        self,
        timestamp: i64,
    ) -> DeleteBuilder<
        'a,
        S,
        [&'a dyn BindableToken<QueryBuilder<QueryValues>>],
        [&'a dyn BindableValue<QueryBuilder<QueryValues>>],
        D,
        QueryBuild,
        DynamicRequest,
    > {
        DeleteBuilder {
            keyspace: self.keyspace,
            statement: self.statement,
            key: self.key,
            variables: self.variables,
            builder: self
                .builder
                .consistency(Consistency::Quorum)
                .bind_values()
                .bind(self.key)
                .timestamp(timestamp),
            _marker: self._marker,
        }
    }

    pub fn build(self) -> anyhow::Result<DeleteRequest> {
        let query = self
            .builder
            .consistency(Consistency::Quorum)
            .bind_values()
            .bind(self.key)
            .build()?;
        // create the request
        Ok(CommonRequest {
            token: self.key.token(),
            payload: query.into(),
            statement: self.statement.into(),
        }
        .into())
    }
}

impl<'a, S, K: ?Sized, V: ?Sized, D, T> DeleteBuilder<'a, S, K, V, D, QueryValues, T> {
    pub fn timestamp(self, timestamp: i64) -> DeleteBuilder<'a, S, K, V, D, QueryBuild, T> {
        DeleteBuilder {
            keyspace: self.keyspace,
            statement: self.statement,
            key: self.key,
            variables: self.variables,
            builder: self.builder.timestamp(timestamp),
            _marker: self._marker,
        }
    }
}

impl<'a, S, K: TokenEncoder + ?Sized, V: ?Sized, D, T> DeleteBuilder<'a, S, K, V, D, QueryValues, T> {
    pub fn build(self) -> anyhow::Result<DeleteRequest> {
        let query = self.builder.build()?;
        // create the request
        Ok(CommonRequest {
            token: self.key.token(),
            payload: query.into(),
            statement: self.statement.into(),
        }
        .into())
    }
}

impl<'a, S, K: TokenEncoder + ?Sized, V: ?Sized, D, T> DeleteBuilder<'a, S, K, V, D, QueryBuild, T> {
    pub fn build(self) -> anyhow::Result<DeleteRequest> {
        let query = self.builder.build()?;
        // create the request
        Ok(CommonRequest {
            token: self.key.token(),
            payload: query.into(),
            statement: self.statement.into(),
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

    fn statement(&self) -> Statement {
        self.0.statement()
    }

    fn payload(&self) -> Vec<u8> {
        self.0.payload()
    }
    fn keyspace(&self) -> Option<String> {
        self.0.keyspace()
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

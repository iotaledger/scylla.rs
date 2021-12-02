// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use scylla_parse::UpdateStatement;

use super::*;

/// Update query trait which creates an `UpdateRequest`
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
/// # type MyVarType = String;
/// # #[derive(Default)]
/// struct MyValueType {
///     value1: f32,
///     value2: f32,
/// }
/// impl<B: Binder> Bindable<B> for MyValueType {
///     fn bind(&self, binder: B) -> B {
///         binder.bind(&self.value1).bind(&self.value2)
///     }
/// }
/// impl Update<MyKeyType, MyVarType, MyValueType> for MyKeyspace {
///     type QueryOrPrepared = PreparedStatement;
///     fn statement(&self) -> String {
///         format!(
///             "UPDATE {}.table SET val1 = ?, val2 = ? WHERE key = ? AND var = ?",
///             self.name()
///         )
///         .into()
///     }
///
///     fn bind_values<B: Binder>(builder: B, key: &MyKeyType, variables: &MyVarType, value: &MyValueType) -> B {
///         builder.bind(value).value(key).value(variables)
///     }
/// }
///
/// # let (my_key, my_var, my_val) = (1, MyVarType::default(), MyValueType::default());
/// let request = MyKeyspace::new("my_keyspace")
///     .update_query(&my_key, &my_var, &my_val)
///     .consistency(Consistency::One)
///     .build()?;
/// let worker = request.worker();
/// # Ok::<(), anyhow::Error>(())
/// ```
pub trait Update<K, V, U>: Keyspace {
    /// Set the query type; `QueryStatement` or `PreparedStatement`
    type QueryOrPrepared: QueryOrPrepared;

    /// Create your update statement here.
    fn statement(&self) -> UpdateStatement;
    /// Get the MD5 hash of this implementation's statement
    /// for use when generating queries that should use
    /// the prepared statement.
    fn id(&self) -> [u8; 16] {
        md5::compute(self.update_statement().to_string().as_bytes()).into()
    }
    /// Bind the cql values to the builder
    fn bind_values<B: Binder>(binder: B, key: &K, variables: &V, values: &U) -> B;
}

/// Specifies helper functions for creating static update requests from a keyspace with a `Delete<K, V>` definition
pub trait GetStaticUpdateRequest<K, V, U>: Keyspace {
    /// Create a static update request from a keyspace with a `Update<K, V>` definition. Will use the default `type
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
    /// # type MyVarType = String;
    /// # #[derive(Default)]
    /// struct MyValueType {
    ///     value1: f32,
    ///     value2: f32,
    /// }
    /// impl Update<MyKeyType, MyVarType, MyValueType> for MyKeyspace {
    ///     type QueryOrPrepared = PreparedStatement;
    ///     fn statement(&self) -> String {
    ///         format!(
    ///             "UPDATE {}.table SET val1 = ?, val2 = ? WHERE key = ? AND var = ?",
    ///             self.name()
    ///         )
    ///         .into()
    ///     }
    ///
    ///     fn bind_values<B: Binder>(builder: B, key: &MyKeyType, variables: &MyVarType, value: &MyValueType) -> B {
    ///         builder
    ///             .value(&value.value1)
    ///             .value(&value.value2)
    ///             .value(key)
    ///             .value(variables)
    ///     }
    /// }
    /// # let (my_key, my_var, my_val) = (1, MyVarType::default(), MyValueType::default());
    /// MyKeyspace::new("my_keyspace")
    ///     .update(&my_key, &my_var, &my_val)
    ///     .consistency(Consistency::One)
    ///     .build()?
    ///     .get_local_blocking()?;
    /// # Ok::<(), anyhow::Error>(())
    /// ```
    fn update<'a>(
        &'a self,
        key: &'a K,
        variables: &'a V,
        values: &'a U,
    ) -> UpdateBuilder<'a, Self, K, V, U, QueryConsistency, StaticRequest>
    where
        Self: Update<K, V, U>,
    {
        let statement = self.statement();
        UpdateBuilder {
            keyspace: PhantomData,
            key,
            variables,
            values,
            builder: Self::QueryOrPrepared::encode_statement(Query::new(), &statement.to_string()),
            statement,
            _marker: StaticRequest,
        }
    }

    /// Create a static update query request from a keyspace with a `Update<K, V>` definition.
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
    /// # type MyVarType = String;
    /// # #[derive(Default)]
    /// struct MyValueType {
    ///     value1: f32,
    ///     value2: f32,
    /// }
    /// impl Update<MyKeyType, MyVarType, MyValueType> for MyKeyspace {
    ///     type QueryOrPrepared = PreparedStatement;
    ///     fn statement(&self) -> String {
    ///         format!(
    ///             "UPDATE {}.table SET val1 = ?, val2 = ? WHERE key = ? AND var = ?",
    ///             self.name()
    ///         )
    ///         .into()
    ///     }
    ///
    ///     fn bind_values<B: Binder>(builder: B, key: &MyKeyType, variables: &MyVarType, value: &MyValueType) -> B {
    ///         builder
    ///             .value(&value.value1)
    ///             .value(&value.value2)
    ///             .value(key)
    ///             .value(variables)
    ///     }
    /// }
    /// # let (my_key, my_var, my_val) = (1, MyVarType::default(), MyValueType::default());
    /// MyKeyspace::new("my_keyspace")
    ///     .update_query(&my_key, &my_var, &my_val)
    ///     .consistency(Consistency::One)
    ///     .build()?
    ///     .get_local_blocking()?;
    /// # Ok::<(), anyhow::Error>(())
    /// ```
    fn update_query<'a>(
        &'a self,
        key: &'a K,
        variables: &'a V,
        values: &'a U,
    ) -> UpdateBuilder<'a, Self, K, V, U, QueryConsistency, StaticRequest>
    where
        Self: Update<K, V, U>,
    {
        let statement = self.statement();
        UpdateBuilder {
            keyspace: PhantomData,
            key,
            variables,
            values,
            builder: QueryStatement::encode_statement(Query::new(), &statement.to_string()),
            statement,
            _marker: StaticRequest,
        }
    }

    /// Create a static update prepared request from a keyspace with a `Update<K, V>` definition.
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
    /// # type MyVarType = String;
    /// # #[derive(Default)]
    /// struct MyValueType {
    ///     value1: f32,
    ///     value2: f32,
    /// }
    /// impl Update<MyKeyType, MyVarType, MyValueType> for MyKeyspace {
    ///     type QueryOrPrepared = PreparedStatement;
    ///     fn statement(&self) -> String {
    ///         format!(
    ///             "UPDATE {}.table SET val1 = ?, val2 = ? WHERE key = ? AND var = ?",
    ///             self.name()
    ///         )
    ///         .into()
    ///     }
    ///
    ///     fn bind_values<B: Binder>(builder: B, key: &MyKeyType, variables: &MyVarType, value: &MyValueType) -> B {
    ///         builder
    ///             .value(&value.value1)
    ///             .value(&value.value2)
    ///             .value(key)
    ///             .value(variables)
    ///     }
    /// }
    /// # let (my_key, my_var, my_val) = (1, MyVarType::default(), MyValueType::default());
    /// MyKeyspace::new("my_keyspace")
    ///     .update_prepared(&my_key, &my_var, &my_val)
    ///     .consistency(Consistency::One)
    ///     .build()?
    ///     .get_local_blocking()?;
    /// # Ok::<(), anyhow::Error>(())
    /// ```
    fn update_prepared<'a>(
        &'a self,
        key: &'a K,
        variables: &'a V,
        values: &'a U,
    ) -> UpdateBuilder<'a, Self, K, V, U, QueryConsistency, StaticRequest>
    where
        Self: Update<K, V, U>,
    {
        let statement = self.statement();
        UpdateBuilder {
            keyspace: PhantomData,
            key,
            variables,
            values,
            builder: PreparedStatement::encode_statement(Query::new(), &statement.to_string()),
            statement,
            _marker: StaticRequest,
        }
    }
}

/// Specifies helper functions for creating dynamic update requests from anything that can be interpreted as a keyspace

pub trait GetDynamicUpdateRequest: Keyspace {
    /// Create a dynamic update request from a statement and variables. Can be specified as either
    /// a query or prepared statement. The token `{{keyspace}}` will be replaced with the keyspace name.
    ///
    /// ## Example
    /// ```no_run
    /// use scylla_rs::app::access::*;
    /// "my_keyspace"
    ///     .update_with(
    ///         "UPDATE {{keyspace}}.table SET val1 = ?, val2 = ? WHERE key = ?",
    ///         &[&3],
    ///         &[&4.0, &5.0],
    ///         StatementType::Query,
    ///     )
    ///     .consistency(Consistency::One)
    ///     .build()?
    ///     .get_local_blocking()?;
    /// # Ok::<(), anyhow::Error>(())
    /// ```
    fn update_with<'a>(
        &'a self,
        statement: UpdateStatement,
        key: &'a [&dyn BindableToken<QueryBuilder<QueryValues>>],
        variables: &'a [&dyn BindableValue<QueryBuilder<QueryValues>>],
        statement_type: StatementType,
    ) -> UpdateBuilder<
        'a,
        Self,
        [&'a dyn BindableToken<QueryBuilder<QueryValues>>],
        [&'a dyn BindableValue<QueryBuilder<QueryValues>>],
        (),
        QueryConsistency,
        DynamicRequest,
    > {
        match statement_type {
            StatementType::Query => self.update_query_with(statement, key, variables),
            StatementType::Prepared => self.update_prepared_with(statement, key, variables),
        }
    }

    /// Create a dynamic update query request from a statement and variables. The token `{{keyspace}}` will be replaced
    /// with the keyspace name.
    ///
    /// ## Example
    /// ```no_run
    /// use scylla_rs::app::access::*;
    /// "my_keyspace"
    ///     .update_query_with(
    ///         "UPDATE {{keyspace}}.table SET val1 = ?, val2 = ? WHERE key = ?",
    ///         &[&3],
    ///         &[&4.0, &5.0],
    ///     )
    ///     .consistency(Consistency::One)
    ///     .build()?
    ///     .get_local_blocking()?;
    /// # Ok::<(), anyhow::Error>(())
    /// ```
    fn update_query_with<'a>(
        &'a self,
        statement: UpdateStatement,
        key: &'a [&dyn BindableToken<QueryBuilder<QueryValues>>],
        variables: &'a [&dyn BindableValue<QueryBuilder<QueryValues>>],
    ) -> UpdateBuilder<
        'a,
        Self,
        [&'a dyn BindableToken<QueryBuilder<QueryValues>>],
        [&'a dyn BindableValue<QueryBuilder<QueryValues>>],
        (),
        QueryConsistency,
        DynamicRequest,
    > {
        let statement = statement.with_keyspace(self.name());
        UpdateBuilder {
            keyspace: PhantomData,
            builder: QueryStatement::encode_statement(Query::new(), &statement.to_string()),
            statement,
            key,
            variables,
            values: &(),
            _marker: DynamicRequest,
        }
    }

    /// Create a dynamic update prepared request from a statement and variables. The token `{{keyspace}}` will be
    /// replaced with the keyspace name.
    ///
    /// ## Example
    /// ```no_run
    /// use scylla_rs::app::access::*;
    /// "my_keyspace"
    ///     .update_prepared_with(
    ///         "UPDATE {{keyspace}}.table SET val1 = ?, val2 = ? WHERE key = ?",
    ///         &[&3],
    ///         &[&4.0, &5.0],
    ///     )
    ///     .consistency(Consistency::One)
    ///     .build()?
    ///     .get_local_blocking()?;
    /// # Ok::<(), anyhow::Error>(())
    /// ```
    fn update_prepared_with<'a>(
        &'a self,
        statement: UpdateStatement,
        key: &'a [&dyn BindableToken<QueryBuilder<QueryValues>>],
        variables: &'a [&dyn BindableValue<QueryBuilder<QueryValues>>],
    ) -> UpdateBuilder<
        'a,
        Self,
        [&'a dyn BindableToken<QueryBuilder<QueryValues>>],
        [&'a dyn BindableValue<QueryBuilder<QueryValues>>],
        (),
        QueryConsistency,
        DynamicRequest,
    > {
        let statement = statement.with_keyspace(self.name());
        UpdateBuilder {
            keyspace: PhantomData,
            builder: PreparedStatement::encode_statement(Query::new(), &statement.to_string()),
            statement,
            key,
            variables,
            values: &(),
            _marker: DynamicRequest,
        }
    }
}

/// Specifies helper functions for creating dynamic insert requests from anything that can be interpreted as a statement

pub trait AsDynamicUpdateRequest
where
    Self: Sized,
{
    /// Create a dynamic update request from a statement and variables. Can be specified as either
    /// a query or prepared statement.
    ///
    /// ## Example
    /// ```no_run
    /// use scylla_rs::app::access::*;
    /// "UPDATE my_keyspace.table SET val1 = ?, val2 = ? WHERE key = ?"
    ///     .as_update(&[&3], &[&4.0, &5.0], StatementType::Query)
    ///     .consistency(Consistency::One)
    ///     .build()?
    ///     .get_local_blocking()?;
    /// # Ok::<(), anyhow::Error>(())
    /// ```
    fn as_update<'a>(
        self,
        key: &'a [&dyn BindableToken<QueryBuilder<QueryValues>>],
        variables: &'a [&dyn BindableValue<QueryBuilder<QueryValues>>],
        statement_type: StatementType,
    ) -> UpdateBuilder<
        'a,
        Self,
        [&'a dyn BindableToken<QueryBuilder<QueryValues>>],
        [&'a dyn BindableValue<QueryBuilder<QueryValues>>],
        (),
        QueryConsistency,
        DynamicRequest,
    > {
        match statement_type {
            StatementType::Query => self.as_update_query(key, variables),
            StatementType::Prepared => self.as_update_prepared(key, variables),
        }
    }

    /// Create a dynamic update query request from a statement and variables.
    ///
    /// ## Example
    /// ```no_run
    /// use scylla_rs::app::access::*;
    /// "UPDATE my_keyspace.table SET val1 = ?, val2 = ? WHERE key = ?"
    ///     .as_update_query(&[&3], &[&4.0, &5.0])
    ///     .consistency(Consistency::One)
    ///     .build()?
    ///     .get_local_blocking()?;
    /// # Ok::<(), anyhow::Error>(())
    /// ```
    fn as_update_query<'a>(
        self,
        key: &'a [&dyn BindableToken<QueryBuilder<QueryValues>>],
        variables: &'a [&dyn BindableValue<QueryBuilder<QueryValues>>],
    ) -> UpdateBuilder<
        'a,
        Self,
        [&'a dyn BindableToken<QueryBuilder<QueryValues>>],
        [&'a dyn BindableValue<QueryBuilder<QueryValues>>],
        (),
        QueryConsistency,
        DynamicRequest,
    >;

    /// Create a dynamic update prepared request from a statement and variables.
    ///
    /// ## Example
    /// ```no_run
    /// use scylla_rs::app::access::*;
    /// "UPDATE my_keyspace.table SET val1 = ?, val2 = ? WHERE key = ?"
    ///     .as_update_prepared(&[&3], &[&4.0, &5.0])
    ///     .consistency(Consistency::One)
    ///     .build()?
    ///     .get_local_blocking()?;
    /// # Ok::<(), anyhow::Error>(())
    /// ```
    fn as_update_prepared<'a>(
        self,
        key: &'a [&dyn BindableToken<QueryBuilder<QueryValues>>],
        variables: &'a [&dyn BindableValue<QueryBuilder<QueryValues>>],
    ) -> UpdateBuilder<
        'a,
        Self,
        [&'a dyn BindableToken<QueryBuilder<QueryValues>>],
        [&'a dyn BindableValue<QueryBuilder<QueryValues>>],
        (),
        QueryConsistency,
        DynamicRequest,
    >;
}

impl<S: Keyspace, K, V, U> GetStaticUpdateRequest<K, V, U> for S {}
impl<S: Keyspace> GetDynamicUpdateRequest for S {}
impl AsDynamicUpdateRequest for UpdateStatement {
    fn as_update_query<'a>(
        self,
        key: &'a [&dyn BindableToken<QueryBuilder<QueryValues>>],
        variables: &'a [&dyn BindableValue<QueryBuilder<QueryValues>>],
    ) -> UpdateBuilder<
        'a,
        Self,
        [&'a dyn BindableToken<QueryBuilder<QueryValues>>],
        [&'a dyn BindableValue<QueryBuilder<QueryValues>>],
        (),
        QueryConsistency,
        DynamicRequest,
    > {
        UpdateBuilder {
            _marker: DynamicRequest,
            keyspace: PhantomData,
            builder: QueryStatement::encode_statement(Query::new(), &self.to_string()),
            statement: self,
            key,
            values: &(),
            variables,
        }
    }

    fn as_update_prepared<'a>(
        self,
        key: &'a [&dyn BindableToken<QueryBuilder<QueryValues>>],
        variables: &'a [&dyn BindableValue<QueryBuilder<QueryValues>>],
    ) -> UpdateBuilder<
        'a,
        Self,
        [&'a dyn BindableToken<QueryBuilder<QueryValues>>],
        [&'a dyn BindableValue<QueryBuilder<QueryValues>>],
        (),
        QueryConsistency,
        DynamicRequest,
    > {
        UpdateBuilder {
            _marker: DynamicRequest,
            keyspace: PhantomData,
            builder: PreparedStatement::encode_statement(Query::new(), &self.to_string()),
            statement: self,
            key,
            values: &(),
            variables,
        }
    }
}

pub struct UpdateBuilder<'a, S, K: ?Sized, V: ?Sized, U: ?Sized, Stage, T> {
    pub(crate) keyspace: PhantomData<fn(S) -> S>,
    pub(crate) statement: UpdateStatement,
    pub(crate) key: &'a K,
    pub(crate) variables: &'a V,
    pub(crate) values: &'a U,
    pub(crate) builder: QueryBuilder<Stage>,
    pub(crate) _marker: T,
}

impl<'a, S: Update<K, V, U>, K: TokenEncoder, V, U> UpdateBuilder<'a, S, K, V, U, QueryConsistency, StaticRequest> {
    pub fn consistency(self, consistency: Consistency) -> UpdateBuilder<'a, S, K, V, U, QueryValues, StaticRequest> {
        UpdateBuilder {
            _marker: self._marker,
            keyspace: self.keyspace,
            statement: self.statement,
            key: self.key,
            variables: self.variables,
            values: self.values,
            builder: S::bind_values(
                self.builder.consistency(consistency).bind_values(),
                &self.key,
                &self.variables,
                &self.values,
            ),
        }
    }

    pub fn timestamp(self, timestamp: i64) -> UpdateBuilder<'a, S, K, V, U, QueryBuild, StaticRequest> {
        UpdateBuilder {
            keyspace: self.keyspace,
            statement: self.statement,
            key: self.key,
            variables: self.variables,
            values: self.values,
            builder: S::bind_values(
                self.builder.consistency(Consistency::Quorum).bind_values(),
                &self.key,
                &self.variables,
                &self.values,
            )
            .timestamp(timestamp),
            _marker: self._marker,
        }
    }

    pub fn build(self) -> anyhow::Result<UpdateRequest> {
        let query = S::bind_values(
            self.builder.consistency(Consistency::Quorum).bind_values(),
            &self.key,
            &self.variables,
            &self.values,
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

impl<'a, S: Keyspace>
    UpdateBuilder<
        'a,
        S,
        [&'a dyn BindableToken<QueryBuilder<QueryValues>>],
        [&'a dyn BindableValue<QueryBuilder<QueryValues>>],
        (),
        QueryConsistency,
        DynamicRequest,
    >
{
    pub fn bind_values<
        F: 'static
            + Fn(
                QueryBuilder<QueryValues>,
                &'a [&dyn BindableToken<QueryBuilder<QueryValues>>],
                &'a [&dyn BindableValue<QueryBuilder<QueryValues>>],
            ) -> QueryBuilder<QueryValues>,
    >(
        self,
        bind_fn: F,
    ) -> UpdateBuilder<
        'a,
        S,
        [&'a dyn BindableToken<QueryBuilder<QueryValues>>],
        [&'a dyn BindableValue<QueryBuilder<QueryValues>>],
        (),
        QueryConsistency,
        ManualBoundRequest<'a, QueryBuilder<QueryValues>>,
    > {
        UpdateBuilder {
            _marker: ManualBoundRequest {
                bind_fn: Box::new(bind_fn),
            },
            keyspace: self.keyspace,
            statement: self.statement,
            key: self.key,
            variables: self.variables,
            values: self.values,
            builder: self.builder,
        }
    }

    pub fn consistency(
        self,
        consistency: Consistency,
    ) -> UpdateBuilder<
        'a,
        S,
        [&'a dyn BindableToken<QueryBuilder<QueryValues>>],
        [&'a dyn BindableValue<QueryBuilder<QueryValues>>],
        (),
        QueryValues,
        DynamicRequest,
    > {
        let builder = self
            .builder
            .consistency(consistency)
            .bind_values()
            .bind(self.variables)
            .bind(self.key);
        UpdateBuilder {
            _marker: self._marker,
            keyspace: self.keyspace,
            statement: self.statement,
            key: self.key,
            variables: self.variables,
            values: self.values,
            builder,
        }
    }

    pub fn timestamp(
        self,
        timestamp: i64,
    ) -> UpdateBuilder<
        'a,
        S,
        [&'a dyn BindableToken<QueryBuilder<QueryValues>>],
        [&'a dyn BindableValue<QueryBuilder<QueryValues>>],
        (),
        QueryBuild,
        DynamicRequest,
    > {
        UpdateBuilder {
            keyspace: self.keyspace,
            statement: self.statement,
            key: self.key,
            variables: self.variables,
            values: self.values,
            builder: self
                .builder
                .consistency(Consistency::Quorum)
                .bind_values()
                .bind(self.variables)
                .bind(self.key)
                .timestamp(timestamp),
            _marker: self._marker,
        }
    }

    pub fn build(self) -> anyhow::Result<UpdateRequest> {
        let query = self
            .builder
            .consistency(Consistency::Quorum)
            .bind_values()
            .bind(self.variables)
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

impl<'a, S: Keyspace>
    UpdateBuilder<
        'a,
        S,
        [&'a dyn BindableToken<QueryBuilder<QueryValues>>],
        [&'a dyn BindableValue<QueryBuilder<QueryValues>>],
        (),
        QueryConsistency,
        ManualBoundRequest<'a, QueryBuilder<QueryValues>>,
    >
{
    pub fn consistency(
        self,
        consistency: Consistency,
    ) -> UpdateBuilder<
        'a,
        S,
        [&'a dyn BindableToken<QueryBuilder<QueryValues>>],
        [&'a dyn BindableValue<QueryBuilder<QueryValues>>],
        (),
        QueryValues,
        DynamicRequest,
    > {
        UpdateBuilder {
            _marker: DynamicRequest,
            keyspace: self.keyspace,
            statement: self.statement,
            key: self.key,
            variables: self.variables,
            values: self.values,
            builder: (self._marker.bind_fn)(
                self.builder.consistency(consistency).bind_values(),
                self.key,
                self.variables,
            ),
        }
    }

    pub fn timestamp(
        self,
        timestamp: i64,
    ) -> UpdateBuilder<
        'a,
        S,
        [&'a dyn BindableToken<QueryBuilder<QueryValues>>],
        [&'a dyn BindableValue<QueryBuilder<QueryValues>>],
        (),
        QueryBuild,
        DynamicRequest,
    > {
        UpdateBuilder {
            keyspace: self.keyspace,
            statement: self.statement,
            key: self.key,
            variables: self.variables,
            values: self.values,
            builder: (self._marker.bind_fn)(
                self.builder.consistency(Consistency::Quorum).bind_values(),
                self.key,
                self.variables,
            )
            .timestamp(timestamp),
            _marker: DynamicRequest,
        }
    }

    pub fn build(self) -> anyhow::Result<UpdateRequest> {
        let query = (self._marker.bind_fn)(
            self.builder.consistency(Consistency::Quorum).bind_values(),
            self.key,
            self.variables,
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

impl<'a, S, K: TokenEncoder + ?Sized, V: ?Sized, U: ?Sized, T> UpdateBuilder<'a, S, K, V, U, QueryValues, T> {
    pub fn timestamp(self, timestamp: i64) -> UpdateBuilder<'a, S, K, V, U, QueryBuild, T> {
        UpdateBuilder {
            keyspace: self.keyspace,
            statement: self.statement,
            key: self.key,
            variables: self.variables,
            values: self.values,
            builder: self.builder.timestamp(timestamp),
            _marker: self._marker,
        }
    }

    pub fn build(self) -> anyhow::Result<UpdateRequest> {
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

impl<'a, S, K: TokenEncoder + ?Sized, V: ?Sized, U: ?Sized, T> UpdateBuilder<'a, S, K, V, U, QueryBuild, T> {
    pub fn build(self) -> anyhow::Result<UpdateRequest> {
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

/// A request to update a record which can be sent to the ring
#[derive(Debug, Clone)]
pub struct UpdateRequest(CommonRequest);

impl From<CommonRequest> for UpdateRequest {
    fn from(req: CommonRequest) -> Self {
        UpdateRequest(req)
    }
}

impl Deref for UpdateRequest {
    type Target = CommonRequest;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for UpdateRequest {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl Request for UpdateRequest {
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

impl SendRequestExt for UpdateRequest {
    type Marker = DecodeVoid;
    type Worker = BasicRetryWorker<Self>;
    const TYPE: RequestType = RequestType::Update;

    fn worker(self) -> Box<Self::Worker> {
        BasicRetryWorker::new(self)
    }
}

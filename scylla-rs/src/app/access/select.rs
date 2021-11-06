// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use super::*;

/// Select query trait which creates a `SelectRequest`
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
/// # type MyValueType = f32;
/// impl Select<MyKeyType, MyVarType, MyValueType> for MyKeyspace {
///     type QueryOrPrepared = PreparedStatement;
///     fn statement(&self) -> Cow<'static, str> {
///         format!("SELECT val FROM {}.table where key = ? AND var = ?", self.name()).into()
///     }
///     fn bind_values<B: Binder>(builder: B, key: &MyKeyType, variables: &MyVarType) -> B {
///         builder.bind(key).bind(variables)
///     }
/// }
/// # let (my_key, my_var) = (1, MyVarType::default());
/// let request = MyKeyspace::new("my_keyspace")
///     .select::<MyValueType>(&my_key, &my_var)
///     .consistency(Consistency::One)
///     .build()?;
/// let worker = request.worker();
/// # Ok::<(), anyhow::Error>(())
/// ```
pub trait Select<K, V, O>: Keyspace {
    /// Set the query type; `QueryStatement` or `PreparedStatement`
    type QueryOrPrepared: QueryOrPrepared;

    /// Create your select statement here.
    fn statement(&self) -> Cow<'static, str>;

    /// Get the MD5 hash of this implementation's statement
    /// for use when generating queries that should use
    /// the prepared statement.
    fn id(&self) -> [u8; 16] {
        md5::compute(self.select_statement().as_bytes()).into()
    }
    /// Bind the cql values to the builder
    fn bind_values<B: Binder>(binder: B, key: &K, variables: &V) -> B;
}

/// Specifies helper functions for creating static delete requests from a keyspace with a `Delete<K, V>` definition

pub trait GetStaticSelectRequest<K, V>: Keyspace {
    /// Create a static select request from a keyspace with a `Select<K, V>` definition. Will use the default `type
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
    /// # type MyValueType = f32;
    /// impl Select<MyKeyType, MyVarType, MyValueType> for MyKeyspace {
    ///     type QueryOrPrepared = PreparedStatement;
    ///     fn statement(&self) -> Cow<'static, str> {
    ///         format!("SELECT val FROM {}.table where key = ? AND var = ?", self.name()).into()
    ///     }
    ///     fn bind_values<B: Binder>(builder: B, key: &MyKeyType, variables: &MyVarType) -> B {
    ///         builder.bind(key).bind(variables)
    ///     }
    /// }
    /// # let (my_key, my_var) = (1, MyVarType::default());
    /// let res: Option<MyValueType> = MyKeyspace::new("my_keyspace")
    ///     .select::<MyValueType>(&my_key, &my_var)
    ///     .consistency(Consistency::One)
    ///     .build()?
    ///     .get_local_blocking()?;
    /// # Ok::<(), anyhow::Error>(())
    /// ```
    fn select<'a, O>(
        &'a self,
        key: &'a K,
        variables: &'a V,
    ) -> SelectBuilder<'a, Self, K, V, O, QueryConsistency, StaticRequest>
    where
        Self: Select<K, V, O>,
    {
        SelectBuilder {
            _marker: StaticRequest,
            keyspace_name: self.name(),
            keyspace: PhantomData,
            statement: self.statement(),
            key,
            variables,
            builder: Self::QueryOrPrepared::encode_statement(Query::new(), &self.statement()),
        }
    }

    /// Create a static select query request from a keyspace with a `Select<K, V>` definition.
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
    /// # type MyValueType = f32;
    /// impl Select<MyKeyType, MyVarType, MyValueType> for MyKeyspace {
    ///     type QueryOrPrepared = PreparedStatement;
    ///     fn statement(&self) -> Cow<'static, str> {
    ///         format!("SELECT val FROM {}.table where key = ? AND var = ?", self.name()).into()
    ///     }
    ///     fn bind_values<B: Binder>(builder: B, key: &MyKeyType, variables: &MyVarType) -> B {
    ///         builder.bind(key).bind(variables)
    ///     }
    /// }
    /// # let (my_key, my_var) = (1, MyVarType::default());
    /// let res: Option<MyValueType> = MyKeyspace::new("my_keyspace")
    ///     .select_query::<MyValueType>(&my_key, &my_var)
    ///     .consistency(Consistency::One)
    ///     .build()?
    ///     .get_local_blocking()?;
    /// # Ok::<(), anyhow::Error>(())
    /// ```
    fn select_query<'a, O>(
        &'a self,
        key: &'a K,
        variables: &'a V,
    ) -> SelectBuilder<'a, Self, K, V, O, QueryConsistency, StaticRequest>
    where
        Self: Select<K, V, O>,
    {
        SelectBuilder {
            _marker: StaticRequest,
            keyspace_name: self.name(),
            keyspace: PhantomData,
            statement: self.statement(),
            key,
            variables,
            builder: QueryStatement::encode_statement(Query::new(), &self.statement()),
        }
    }

    /// Create a static select prepared request from a keyspace with a `Select<K, V>` definition.
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
    /// # type MyValueType = f32;
    /// impl Select<MyKeyType, MyVarType, MyValueType> for MyKeyspace {
    ///     type QueryOrPrepared = PreparedStatement;
    ///     fn statement(&self) -> Cow<'static, str> {
    ///         format!("SELECT val FROM {}.table where key = ? AND var = ?", self.name()).into()
    ///     }
    ///     fn bind_values<B: Binder>(builder: B, key: &MyKeyType, variables: &MyVarType) -> B {
    ///         builder.bind(key).bind(variables)
    ///     }
    /// }
    /// # let (my_key, my_var) = (1, MyVarType::default());
    /// let res: Option<MyValueType> = MyKeyspace::new("my_keyspace")
    ///     .select_prepared::<MyValueType>(&my_key, &my_var)
    ///     .consistency(Consistency::One)
    ///     .build()?
    ///     .get_local_blocking()?;
    /// # Ok::<(), anyhow::Error>(())
    /// ```
    fn select_prepared<'a, O>(
        &'a self,
        key: &'a K,
        variables: &'a V,
    ) -> SelectBuilder<'a, Self, K, V, O, QueryConsistency, StaticRequest>
    where
        Self: Select<K, V, O>,
    {
        SelectBuilder {
            _marker: StaticRequest,
            keyspace_name: self.name(),
            keyspace: PhantomData,
            statement: self.statement(),
            key,
            variables,
            builder: PreparedStatement::encode_statement(Query::new(), &self.statement()),
        }
    }
}

/// Specifies helper functions for creating dynamic select requests from anything that can be interpreted as a keyspace

pub trait GetDynamicSelectRequest: Keyspace {
    /// Create a dynamic select request from a statement and variables. Can be specified as either
    /// a query or prepared statement. The token `{{keyspace}}` will be replaced with the keyspace name.
    ///
    /// ## Example
    /// ```no_run
    /// use scylla_rs::app::access::*;
    /// let res: Option<f32> = "my_keyspace"
    ///     .select_with::<f32>(
    ///         "SELECT val FROM {{keyspace}}.table where key = ? AND var = ?",
    ///         &[&3],
    ///         &[&"hello"],
    ///         StatementType::Query,
    ///     )
    ///     .consistency(Consistency::One)
    ///     .build()?
    ///     .get_local_blocking()?;
    /// # Ok::<(), anyhow::Error>(())
    /// ```
    fn select_with<'a, O>(
        &'a self,
        statement: &str,
        key: &'a [&dyn BindableToken<QueryBuilder<QueryValues>>],
        variables: &'a [&dyn BindableValue<QueryBuilder<QueryValues>>],
        statement_type: StatementType,
    ) -> SelectBuilder<
        'a,
        Self,
        [&'a dyn BindableToken<QueryBuilder<QueryValues>>],
        [&'a dyn BindableValue<QueryBuilder<QueryValues>>],
        O,
        QueryConsistency,
        DynamicRequest,
    > {
        match statement_type {
            StatementType::Query => self.select_query_with(statement, key, variables),
            StatementType::Prepared => self.select_prepared_with(statement, key, variables),
        }
    }

    /// Create a dynamic select query request from a statement and variables. The token `{{keyspace}}` will be replaced
    /// with the keyspace name.
    ///
    /// ## Example
    /// ```no_run
    /// use scylla_rs::app::access::*;
    /// let res: Option<f32> = "my_keyspace"
    ///     .select_query_with::<f32>(
    ///         "SELECT val FROM {{keyspace}}.table where key = ? AND var = ?",
    ///         &[&3],
    ///         &[&"hello"],
    ///     )
    ///     .consistency(Consistency::One)
    ///     .build()?
    ///     .get_local_blocking()?;
    /// # Ok::<(), anyhow::Error>(())
    /// ```
    fn select_query_with<'a, O>(
        &'a self,
        statement: &str,
        key: &'a [&dyn BindableToken<QueryBuilder<QueryValues>>],
        variables: &'a [&dyn BindableValue<QueryBuilder<QueryValues>>],
    ) -> SelectBuilder<
        'a,
        Self,
        [&'a dyn BindableToken<QueryBuilder<QueryValues>>],
        [&'a dyn BindableValue<QueryBuilder<QueryValues>>],
        O,
        QueryConsistency,
        DynamicRequest,
    > {
        SelectBuilder {
            _marker: DynamicRequest,
            keyspace_name: self.name(),
            keyspace: PhantomData,
            statement: statement.to_owned().into(),
            key,
            variables,
            builder: QueryStatement::encode_statement(Query::new(), &self.replace_keyspace_token(statement)),
        }
    }

    /// Create a dynamic select prepared request from a statement and variables. The token `{{keyspace}}` will be
    /// replaced with the keyspace name.
    ///
    /// ## Example
    /// ```no_run
    /// use scylla_rs::app::access::*;
    /// let res: Option<f32> = "my_keyspace"
    ///     .select_prepared_with::<f32>(
    ///         "SELECT val FROM {{keyspace}}.table where key = ? AND var = ?",
    ///         &[&3],
    ///         &[&"hello"],
    ///     )
    ///     .consistency(Consistency::One)
    ///     .build()?
    ///     .get_local_blocking()?;
    /// # Ok::<(), anyhow::Error>(())
    /// ```
    fn select_prepared_with<'a, O>(
        &'a self,
        statement: &str,
        key: &'a [&dyn BindableToken<QueryBuilder<QueryValues>>],
        variables: &'a [&dyn BindableValue<QueryBuilder<QueryValues>>],
    ) -> SelectBuilder<
        'a,
        Self,
        [&'a dyn BindableToken<QueryBuilder<QueryValues>>],
        [&'a dyn BindableValue<QueryBuilder<QueryValues>>],
        O,
        QueryConsistency,
        DynamicRequest,
    > {
        SelectBuilder {
            _marker: DynamicRequest,
            keyspace_name: self.name(),
            keyspace: PhantomData,
            statement: statement.to_owned().into(),
            key,
            variables,
            builder: PreparedStatement::encode_statement(Query::new(), &self.replace_keyspace_token(statement)),
        }
    }
}

/// Specifies helper functions for creating dynamic select requests from anything that can be interpreted as a statement

pub trait AsDynamicSelectRequest: ToStatement
where
    Self: Sized,
{
    /// Create a dynamic select request from a statement and variables. Can be specified as either
    /// a query or prepared statement.
    ///
    /// ## Example
    /// ```no_run
    /// use scylla_rs::app::access::*;
    /// let res: Option<f32> = "SELECT val FROM my_keyspace.table where key = ? AND var = ?"
    ///     .as_select::<f32>(&[&3], &[&"hello"], StatementType::Query)
    ///     .consistency(Consistency::One)
    ///     .build()?
    ///     .get_local_blocking()?;
    /// # Ok::<(), anyhow::Error>(())
    /// ```
    fn as_select<'a, O>(
        &self,
        key: &'a [&dyn BindableToken<QueryBuilder<QueryValues>>],
        variables: &'a [&dyn BindableValue<QueryBuilder<QueryValues>>],
        statement_type: StatementType,
    ) -> SelectBuilder<
        'a,
        Self,
        [&'a dyn BindableToken<QueryBuilder<QueryValues>>],
        [&'a dyn BindableValue<QueryBuilder<QueryValues>>],
        O,
        QueryConsistency,
        DynamicRequest,
    > {
        match statement_type {
            StatementType::Query => self.as_select_query(key, variables),
            StatementType::Prepared => self.as_select_prepared(key, variables),
        }
    }

    /// Create a dynamic select query request from a statement and variables.
    ///
    /// ## Example
    /// ```no_run
    /// use scylla_rs::app::access::*;
    /// let res: Option<f32> = "SELECT val FROM my_keyspace.table where key = ? AND var = ?"
    ///     .as_select_query::<f32>(&[&3], &[&"hello"])
    ///     .consistency(Consistency::One)
    ///     .build()?
    ///     .get_local_blocking()?;
    /// # Ok::<(), anyhow::Error>(())
    /// ```
    fn as_select_query<'a, O>(
        &self,
        key: &'a [&dyn BindableToken<QueryBuilder<QueryValues>>],
        variables: &'a [&dyn BindableValue<QueryBuilder<QueryValues>>],
    ) -> SelectBuilder<
        'a,
        Self,
        [&'a dyn BindableToken<QueryBuilder<QueryValues>>],
        [&'a dyn BindableValue<QueryBuilder<QueryValues>>],
        O,
        QueryConsistency,
        DynamicRequest,
    > {
        let statement = self.to_statement();
        SelectBuilder {
            _marker: DynamicRequest,
            keyspace_name: self.keyspace().clone().into(),
            keyspace: PhantomData,
            builder: QueryStatement::encode_statement(Query::new(), &statement),
            statement,
            key,
            variables,
        }
    }

    /// Create a dynamic select prepared request from a statement and variables.
    ///
    /// ## Example
    /// ```no_run
    /// use scylla_rs::app::access::*;
    /// let res: Option<f32> = "SELECT val FROM my_keyspace.table where key = ? AND var = ?"
    ///     .as_select_prepared::<f32>(&[&3], &[&"hello"])
    ///     .consistency(Consistency::One)
    ///     .build()?
    ///     .get_local_blocking()?;
    /// # Ok::<(), anyhow::Error>(())
    /// ```
    fn as_select_prepared<'a, O>(
        &self,
        key: &'a [&dyn BindableToken<QueryBuilder<QueryValues>>],
        variables: &'a [&dyn BindableValue<QueryBuilder<QueryValues>>],
    ) -> SelectBuilder<
        'a,
        Self,
        [&'a dyn BindableToken<QueryBuilder<QueryValues>>],
        [&'a dyn BindableValue<QueryBuilder<QueryValues>>],
        O,
        QueryConsistency,
        DynamicRequest,
    > {
        let statement = self.to_statement();
        SelectBuilder {
            _marker: DynamicRequest,
            keyspace_name: self.keyspace().clone().into(),
            keyspace: PhantomData,
            builder: PreparedStatement::encode_statement(Query::new(), &statement),
            statement,
            key,
            variables,
        }
    }
}

impl<S: Keyspace, K, V> GetStaticSelectRequest<K, V> for S {}
impl<S: Keyspace> GetDynamicSelectRequest for S {}
impl<S: ToStatement> AsDynamicSelectRequest for S {}

pub struct SelectBuilder<'a, S, K: ?Sized, V: ?Sized, O, Stage, T> {
    pub(crate) keyspace_name: String,
    pub(crate) keyspace: PhantomData<fn(S, O) -> (S, O)>,
    pub(crate) statement: Cow<'static, str>,
    pub(crate) key: &'a K,
    pub(crate) variables: &'a V,
    pub(crate) builder: QueryBuilder<Stage>,
    pub(crate) _marker: T,
}

impl<'a, S: Select<K, V, O>, K: TokenEncoder, V, O> SelectBuilder<'a, S, K, V, O, QueryConsistency, StaticRequest> {
    pub fn consistency(self, consistency: Consistency) -> SelectBuilder<'a, S, K, V, O, QueryValues, StaticRequest> {
        SelectBuilder {
            _marker: self._marker,
            keyspace_name: self.keyspace_name,
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

    pub fn page_size(self, page_size: i32) -> SelectBuilder<'a, S, K, V, O, QueryPagingState, StaticRequest> {
        SelectBuilder {
            _marker: self._marker,
            keyspace_name: self.keyspace_name,
            keyspace: self.keyspace,
            statement: self.statement,
            key: self.key,
            variables: self.variables,
            builder: S::bind_values(
                self.builder.consistency(Consistency::One).bind_values(),
                &self.key,
                &self.variables,
            )
            .page_size(page_size),
        }
    }
    /// Set the paging state.
    pub fn paging_state(
        self,
        paging_state: &Option<Vec<u8>>,
    ) -> SelectBuilder<'a, S, K, V, O, QuerySerialConsistency, StaticRequest> {
        SelectBuilder {
            _marker: self._marker,
            keyspace_name: self.keyspace_name,
            keyspace: self.keyspace,
            statement: self.statement,
            key: self.key,
            variables: self.variables,
            builder: S::bind_values(
                self.builder.consistency(Consistency::One).bind_values(),
                &self.key,
                &self.variables,
            )
            .paging_state(paging_state),
        }
    }
    pub fn timestamp(self, timestamp: i64) -> SelectBuilder<'a, S, K, V, O, QueryBuild, StaticRequest> {
        SelectBuilder {
            _marker: self._marker,
            keyspace_name: self.keyspace_name,
            keyspace: self.keyspace,
            statement: self.statement,
            key: self.key,
            variables: self.variables,
            builder: S::bind_values(
                self.builder.consistency(Consistency::One).bind_values(),
                &self.key,
                &self.variables,
            )
            .timestamp(timestamp),
        }
    }

    pub fn build(self) -> anyhow::Result<SelectRequest<O>> {
        let query = S::bind_values(
            self.builder.consistency(Consistency::One).bind_values(),
            &self.key,
            &self.variables,
        )
        .build()?;
        // create the request
        Ok(CommonRequest {
            keyspace_name: self.keyspace_name,
            token: self.key.token(),
            payload: query.into(),
            statement: self.statement,
        }
        .into())
    }
}

impl<'a, S: Keyspace, O>
    SelectBuilder<
        'a,
        S,
        [&'a dyn BindableToken<QueryBuilder<QueryValues>>],
        [&'a dyn BindableValue<QueryBuilder<QueryValues>>],
        O,
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
    ) -> SelectBuilder<
        'a,
        S,
        [&'a dyn BindableToken<QueryBuilder<QueryValues>>],
        [&'a dyn BindableValue<QueryBuilder<QueryValues>>],
        O,
        QueryConsistency,
        ManualBoundRequest<'a, QueryBuilder<QueryValues>>,
    > {
        SelectBuilder {
            _marker: ManualBoundRequest {
                bind_fn: Box::new(bind_fn),
            },
            keyspace_name: self.keyspace_name,
            keyspace: self.keyspace,
            statement: self.statement,
            key: self.key,
            variables: self.variables,
            builder: self.builder,
        }
    }

    pub fn consistency(
        self,
        consistency: Consistency,
    ) -> SelectBuilder<
        'a,
        S,
        [&'a dyn BindableToken<QueryBuilder<QueryValues>>],
        [&'a dyn BindableValue<QueryBuilder<QueryValues>>],
        O,
        QueryValues,
        DynamicRequest,
    > {
        let builder = self
            .builder
            .consistency(consistency)
            .bind_values()
            .bind(self.key)
            .bind(self.variables);
        SelectBuilder {
            keyspace_name: self.keyspace_name,
            _marker: self._marker,
            keyspace: self.keyspace,
            statement: self.statement,
            key: self.key,
            variables: self.variables,
            builder,
        }
    }

    pub fn page_size(
        self,
        page_size: i32,
    ) -> SelectBuilder<
        'a,
        S,
        [&'a dyn BindableToken<QueryBuilder<QueryValues>>],
        [&'a dyn BindableValue<QueryBuilder<QueryValues>>],
        O,
        QueryPagingState,
        DynamicRequest,
    > {
        SelectBuilder {
            _marker: self._marker,
            keyspace_name: self.keyspace_name,
            keyspace: self.keyspace,
            statement: self.statement,
            key: self.key,
            variables: self.variables,
            builder: self
                .builder
                .consistency(Consistency::One)
                .bind_values()
                .bind(self.key)
                .bind(self.variables)
                .page_size(page_size),
        }
    }
    /// Set the paging state.
    pub fn paging_state(
        self,
        paging_state: &Option<Vec<u8>>,
    ) -> SelectBuilder<
        'a,
        S,
        [&'a dyn BindableToken<QueryBuilder<QueryValues>>],
        [&'a dyn BindableValue<QueryBuilder<QueryValues>>],
        O,
        QuerySerialConsistency,
        DynamicRequest,
    > {
        SelectBuilder {
            _marker: self._marker,
            keyspace_name: self.keyspace_name,
            keyspace: self.keyspace,
            statement: self.statement,
            key: self.key,
            variables: self.variables,
            builder: self
                .builder
                .consistency(Consistency::One)
                .bind_values()
                .bind(self.key)
                .bind(self.variables)
                .paging_state(paging_state),
        }
    }
    pub fn timestamp(
        self,
        timestamp: i64,
    ) -> SelectBuilder<
        'a,
        S,
        [&'a dyn BindableToken<QueryBuilder<QueryValues>>],
        [&'a dyn BindableValue<QueryBuilder<QueryValues>>],
        O,
        QueryBuild,
        DynamicRequest,
    > {
        SelectBuilder {
            _marker: self._marker,
            keyspace_name: self.keyspace_name,
            keyspace: self.keyspace,
            statement: self.statement,
            key: self.key,
            variables: self.variables,
            builder: self
                .builder
                .consistency(Consistency::One)
                .bind_values()
                .bind(self.key)
                .bind(self.variables)
                .timestamp(timestamp),
        }
    }

    pub fn build(self) -> anyhow::Result<SelectRequest<O>> {
        let query = self
            .builder
            .consistency(Consistency::One)
            .bind_values()
            .bind(self.key)
            .bind(self.variables)
            .build()?;
        // create the request
        Ok(CommonRequest {
            keyspace_name: self.keyspace_name,
            token: self.key.token(),
            payload: query.into(),
            statement: self.statement,
        }
        .into())
    }
}

impl<'a, S: Keyspace, O>
    SelectBuilder<
        'a,
        S,
        [&'a dyn BindableToken<QueryBuilder<QueryValues>>],
        [&'a dyn BindableValue<QueryBuilder<QueryValues>>],
        O,
        QueryConsistency,
        ManualBoundRequest<'a, QueryBuilder<QueryValues>>,
    >
{
    pub fn consistency(
        self,
        consistency: Consistency,
    ) -> SelectBuilder<
        'a,
        S,
        [&'a dyn BindableToken<QueryBuilder<QueryValues>>],
        [&'a dyn BindableValue<QueryBuilder<QueryValues>>],
        O,
        QueryValues,
        DynamicRequest,
    > {
        SelectBuilder {
            _marker: DynamicRequest,
            keyspace_name: self.keyspace_name,
            keyspace: self.keyspace,
            statement: self.statement,
            key: self.key,
            variables: self.variables,
            builder: (self._marker.bind_fn)(
                self.builder.consistency(consistency).bind_values(),
                self.key,
                self.variables,
            ),
        }
    }

    pub fn page_size(
        self,
        page_size: i32,
    ) -> SelectBuilder<
        'a,
        S,
        [&'a dyn BindableToken<QueryBuilder<QueryValues>>],
        [&'a dyn BindableValue<QueryBuilder<QueryValues>>],
        O,
        QueryPagingState,
        DynamicRequest,
    > {
        SelectBuilder {
            _marker: DynamicRequest,
            keyspace_name: self.keyspace_name,
            keyspace: self.keyspace,
            statement: self.statement,
            key: self.key,
            variables: self.variables,
            builder: (self._marker.bind_fn)(
                self.builder.consistency(Consistency::One).bind_values(),
                self.key,
                self.variables,
            )
            .page_size(page_size),
        }
    }
    /// Set the paging state.
    pub fn paging_state(
        self,
        paging_state: &Option<Vec<u8>>,
    ) -> SelectBuilder<
        'a,
        S,
        [&'a dyn BindableToken<QueryBuilder<QueryValues>>],
        [&'a dyn BindableValue<QueryBuilder<QueryValues>>],
        O,
        QuerySerialConsistency,
        DynamicRequest,
    > {
        SelectBuilder {
            _marker: DynamicRequest,
            keyspace_name: self.keyspace_name,
            keyspace: self.keyspace,
            statement: self.statement,
            key: self.key,
            variables: self.variables,
            builder: (self._marker.bind_fn)(
                self.builder.consistency(Consistency::One).bind_values(),
                self.key,
                self.variables,
            )
            .paging_state(paging_state),
        }
    }
    pub fn timestamp(
        self,
        timestamp: i64,
    ) -> SelectBuilder<
        'a,
        S,
        [&'a dyn BindableToken<QueryBuilder<QueryValues>>],
        [&'a dyn BindableValue<QueryBuilder<QueryValues>>],
        O,
        QueryBuild,
        DynamicRequest,
    > {
        SelectBuilder {
            _marker: DynamicRequest,
            keyspace_name: self.keyspace_name,
            keyspace: self.keyspace,
            statement: self.statement,
            key: self.key,
            variables: self.variables,
            builder: (self._marker.bind_fn)(
                self.builder.consistency(Consistency::One).bind_values(),
                self.key,
                self.variables,
            )
            .timestamp(timestamp),
        }
    }

    pub fn build(self) -> anyhow::Result<SelectRequest<O>> {
        let query = self
            .builder
            .consistency(Consistency::One)
            .bind_values()
            .bind(self.key)
            .bind(self.variables)
            .build()?;
        // create the request
        Ok(CommonRequest {
            keyspace_name: self.keyspace_name,
            token: self.key.token(),
            payload: query.into(),
            statement: self.statement,
        }
        .into())
    }
}

impl<'a, S, K: ?Sized, V: ?Sized, O, T> SelectBuilder<'a, S, K, V, O, QueryValues, T> {
    pub fn page_size(self, page_size: i32) -> SelectBuilder<'a, S, K, V, O, QueryPagingState, T> {
        SelectBuilder {
            _marker: self._marker,
            keyspace_name: self.keyspace_name,
            keyspace: self.keyspace,
            statement: self.statement,
            key: self.key,
            variables: self.variables,
            builder: self.builder.page_size(page_size),
        }
    }
    pub fn paging_state(
        self,
        paging_state: &Option<Vec<u8>>,
    ) -> SelectBuilder<'a, S, K, V, O, QuerySerialConsistency, T> {
        SelectBuilder {
            _marker: self._marker,
            keyspace_name: self.keyspace_name,
            keyspace: self.keyspace,
            statement: self.statement,
            key: self.key,
            variables: self.variables,
            builder: self.builder.paging_state(paging_state),
        }
    }
    pub fn timestamp(self, timestamp: i64) -> SelectBuilder<'a, S, K, V, O, QueryBuild, T> {
        SelectBuilder {
            _marker: self._marker,
            keyspace_name: self.keyspace_name,
            keyspace: self.keyspace,
            statement: self.statement,
            key: self.key,
            variables: self.variables,
            builder: self.builder.timestamp(timestamp),
        }
    }
}

impl<'a, S, K: TokenEncoder + ?Sized, V: ?Sized, O: RowsDecoder, T> SelectBuilder<'a, S, K, V, O, QueryValues, T> {
    pub fn build(self) -> anyhow::Result<SelectRequest<O>> {
        let query = self.builder.build()?;
        // create the request
        Ok(CommonRequest {
            keyspace_name: self.keyspace_name,
            token: self.key.token(),
            payload: query.into(),
            statement: self.statement,
        }
        .into())
    }
}

impl<'a, S, K: TokenEncoder + ?Sized, V: ?Sized, O: RowsDecoder, T> SelectBuilder<'a, S, K, V, O, QueryBuild, T> {
    pub fn build(self) -> anyhow::Result<SelectRequest<O>> {
        let query = self.builder.build()?;
        // create the request
        Ok(CommonRequest {
            keyspace_name: self.keyspace_name,
            token: self.key.token(),
            payload: query.into(),
            statement: self.statement,
        }
        .into())
    }
}

impl<'a, S, K: TokenEncoder + ?Sized, V: ?Sized, O, T> SelectBuilder<'a, S, K, V, O, QueryPagingState, T> {
    pub fn paging_state(
        self,
        paging_state: &Option<Vec<u8>>,
    ) -> SelectBuilder<'a, S, K, V, O, QuerySerialConsistency, T> {
        SelectBuilder {
            _marker: self._marker,
            keyspace_name: self.keyspace_name,
            keyspace: self.keyspace,
            statement: self.statement,
            key: self.key,
            variables: self.variables,
            builder: self.builder.paging_state(paging_state),
        }
    }

    pub fn timestamp(self, timestamp: i64) -> SelectBuilder<'a, S, K, V, O, QueryBuild, T> {
        SelectBuilder {
            _marker: self._marker,
            keyspace_name: self.keyspace_name,
            keyspace: self.keyspace,
            statement: self.statement,
            key: self.key,
            variables: self.variables,
            builder: self.builder.timestamp(timestamp),
        }
    }

    pub fn build(self) -> anyhow::Result<SelectRequest<O>> {
        let query = self.builder.build()?;
        // create the request
        Ok(CommonRequest {
            keyspace_name: self.keyspace_name,
            token: self.key.token(),
            payload: query.into(),
            statement: self.statement,
        }
        .into())
    }
}

impl<'a, S, K: TokenEncoder + ?Sized, V: ?Sized, O, T> SelectBuilder<'a, S, K, V, O, QuerySerialConsistency, T> {
    pub fn timestamp(self, timestamp: i64) -> SelectBuilder<'a, S, K, V, O, QueryBuild, T> {
        SelectBuilder {
            keyspace_name: self.keyspace_name,
            _marker: self._marker,
            keyspace: self.keyspace,
            statement: self.statement,
            key: self.key,
            variables: self.variables,
            builder: self.builder.timestamp(timestamp),
        }
    }

    pub fn build(self) -> anyhow::Result<SelectRequest<O>> {
        let query = self.builder.build()?;
        // create the request
        Ok(CommonRequest {
            keyspace_name: self.keyspace_name,
            token: self.key.token(),
            payload: query.into(),
            statement: self.statement,
        }
        .into())
    }
}

/// A request to select a record which can be sent to the ring
pub struct SelectRequest<O> {
    inner: CommonRequest,
    _marker: PhantomData<fn(O) -> O>,
}

impl<O> From<CommonRequest> for SelectRequest<O> {
    fn from(inner: CommonRequest) -> Self {
        SelectRequest {
            inner,
            _marker: PhantomData,
        }
    }
}

impl<O> Deref for SelectRequest<O> {
    type Target = CommonRequest;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl<O> DerefMut for SelectRequest<O> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.inner
    }
}

impl<O> Debug for SelectRequest<O> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SelectRequest").field("inner", &self.inner).finish()
    }
}

impl<O> Clone for SelectRequest<O> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            _marker: PhantomData,
        }
    }
}

impl<O: 'static> Request for SelectRequest<O> {
    fn token(&self) -> i64 {
        self.inner.token()
    }

    fn statement(&self) -> &Cow<'static, str> {
        self.inner.statement()
    }

    fn payload(&self) -> Vec<u8> {
        self.inner.payload()
    }
    fn keyspace(&self) -> String {
        self.inner.keyspace()
    }
}

impl<O> SelectRequest<O> {
    /// Return DecodeResult marker type, useful in case the worker struct wants to hold the
    /// decoder in order to decode the response inside handle_response method.
    pub fn result_decoder(&self) -> DecodeResult<DecodeRows<O>> {
        DecodeResult::select()
    }
}

impl<O> SendRequestExt for SelectRequest<O>
where
    O: 'static + Send + RowsDecoder + Debug,
{
    type Marker = DecodeRows<O>;
    type Worker = BasicRetryWorker<Self>;
    const TYPE: RequestType = RequestType::Select;

    fn worker(self) -> Box<Self::Worker> {
        BasicRetryWorker::new(self)
    }
}

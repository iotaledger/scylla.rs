// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use super::*;

/// Insert query trait which creates an `InsertRequest`
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
/// # #[derive(Default)]
/// struct MyValueType {
///     value1: f32,
///     value2: f32,
/// }
/// impl Bindable for MyValueType {
///     fn bind<V: Values>(&self, binder: V) -> V::Return {
///         binder.bind(&self.value1).bind(&self.value2)
///     }
/// }
/// impl Insert<MyKeyType, MyValueType> for MyKeyspace {
///     type QueryOrPrepared = PreparedStatement;
///     fn statement(&self) -> Cow<'static, str> {
///         format!("INSERT INTO {}.table (key, val1, val2) VALUES (?,?,?)", self.name()).into()
///     }
///
///     fn bind_values<T: Values>(builder: T, key: &MyKeyType, values: &MyValueType) -> T::Return {
///         builder.value(key).bind(value)
///     }
/// }
///
/// # let (my_key, my_val) = (1, MyValueType::default());
/// let request = MyKeyspace::new("my_keyspace")
///     .insert_prepared(&my_key, &my_val)
///     .consistency(Consistency::One)
///     .build()?;
/// let worker = request.worker();
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
    fn bind_values<B: Binder>(binder: B, key: &K, values: &V) -> B;
}

/// Specifies helper functions for creating static insert requests from a keyspace with a `Delete<K, V>` definition
pub trait GetStaticInsertRequest<K, V>: Keyspace {
    /// Create a static insert request from a keyspace with a `Insert<K, V>` definition. Will use the default `type
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
    /// # #[derive(Default)]
    /// struct MyValueType {
    ///     value1: f32,
    ///     value2: f32,
    /// }
    /// impl Insert<MyKeyType, MyValueType> for MyKeyspace {
    ///     type QueryOrPrepared = PreparedStatement;
    ///     fn statement(&self) -> Cow<'static, str> {
    ///         format!("INSERT INTO {}.table (key, val1, val2) VALUES (?,?,?)", self.name()).into()
    ///     }
    ///
    ///     fn bind_values<T: Values>(builder: T, key: &MyKeyType, values: &MyValueType) -> T::Return {
    ///         builder.value(key).value(&value.value1).value(&value.value2)
    ///     }
    /// }
    /// # let (my_key, my_val) = (1, MyValueType::default());
    /// MyKeyspace::new("my_keyspace")
    ///     .insert(&my_key, &my_val)
    ///     .consistency(Consistency::One)
    ///     .build()?
    ///     .get_local_blocking()?;
    /// # Ok::<(), anyhow::Error>(())
    /// ```
    fn insert<'a>(&'a self, key: &'a K, values: &'a V) -> InsertBuilder<'a, Self, K, V, QueryConsistency, StaticRequest>
    where
        Self: Insert<K, V>,
    {
        InsertBuilder {
            keyspace: PhantomData,
            statement: self.statement(),
            key,
            values,
            builder: Self::QueryOrPrepared::encode_statement(Query::new(), &self.statement()),
            _marker: StaticRequest,
        }
    }

    /// Create a static insert query request from a keyspace with a `Insert<K, V>` definition.
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
    /// # #[derive(Default)]
    /// struct MyValueType {
    ///     value1: f32,
    ///     value2: f32,
    /// }
    /// impl Insert<MyKeyType, MyValueType> for MyKeyspace {
    ///     type QueryOrPrepared = PreparedStatement;
    ///     fn statement(&self) -> Cow<'static, str> {
    ///         format!("INSERT INTO {}.table (key, val1, val2) VALUES (?,?,?)", self.name()).into()
    ///     }
    ///
    ///     fn bind_values<T: Values>(builder: T, key: &MyKeyType, values: &MyValueType) -> T::Return {
    ///         builder.value(key).value(&value.value1).value(&value.value2)
    ///     }
    /// }
    /// # let (my_key, my_val) = (1, MyValueType::default());
    /// MyKeyspace::new("my_keyspace")
    ///     .insert_query(&my_key, &my_val)
    ///     .consistency(Consistency::One)
    ///     .build()?
    ///     .get_local_blocking()?;
    /// # Ok::<(), anyhow::Error>(())
    /// ```
    fn insert_query<'a>(
        &'a self,
        key: &'a K,
        values: &'a V,
    ) -> InsertBuilder<'a, Self, K, V, QueryConsistency, StaticRequest>
    where
        Self: Insert<K, V>,
    {
        InsertBuilder {
            keyspace: PhantomData,
            statement: self.statement(),
            key,
            values: values,
            builder: QueryStatement::encode_statement(Query::new(), &self.statement()),
            _marker: StaticRequest,
        }
    }

    /// Create a static insert prepared request from a keyspace with a `Insert<K, V>` definition.
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
    /// # #[derive(Default)]
    /// struct MyValueType {
    ///     value1: f32,
    ///     value2: f32,
    /// }
    /// impl Insert<MyKeyType, MyValueType> for MyKeyspace {
    ///     type QueryOrPrepared = PreparedStatement;
    ///     fn statement(&self) -> Cow<'static, str> {
    ///         format!("INSERT INTO {}.table (key, val1, val2) VALUES (?,?,?)", self.name()).into()
    ///     }
    ///
    ///     fn bind_values<T: Values>(builder: T, key: &MyKeyType, values: &MyValueType) -> T::Return {
    ///         builder.value(key).value(&value.value1).value(&value.value2)
    ///     }
    /// }
    /// # let (my_key, my_val) = (1, MyValueType::default());
    /// MyKeyspace::new("my_keyspace")
    ///     .insert_prepared(&my_key, &my_val)
    ///     .consistency(Consistency::One)
    ///     .build()?
    ///     .get_local_blocking()?;
    /// # Ok::<(), anyhow::Error>(())
    /// ```
    fn insert_prepared<'a>(
        &'a self,
        key: &'a K,
        values: &'a V,
    ) -> InsertBuilder<'a, Self, K, V, QueryConsistency, StaticRequest>
    where
        Self: Insert<K, V>,
    {
        InsertBuilder {
            keyspace: PhantomData,
            statement: self.statement(),
            key,
            values,
            builder: PreparedStatement::encode_statement(Query::new(), &self.statement()),
            _marker: StaticRequest,
        }
    }
}

/// Specifies helper functions for creating dynamic insert requests from anything that can be interpreted as a keyspace

pub trait GetDynamicInsertRequest: Keyspace {
    /// Create a dynamic insert request from a statement and variables. Can be specified as either
    /// a query or prepared statement. The token `{{keyspace}}` will be replaced with the keyspace name.
    ///
    /// ## Example
    /// ```no_run
    /// use scylla_rs::app::access::*;
    /// "my_keyspace"
    ///     .insert_with(
    ///         "INSERT INTO {{keyspace}}.table (key, val1, val2) VALUES (?,?,?)",
    ///         &[&3],
    ///         &[&4.0, &5.0],
    ///         StatementType::Query,
    ///     )
    ///     .consistency(Consistency::One)
    ///     .build()?
    ///     .get_local_blocking()?;
    /// # Ok::<(), anyhow::Error>(())
    /// ```
    fn insert_with<'a>(
        &'a self,
        statement: &str,
        key: &'a [&dyn BindableToken<QueryBuilder<QueryValues>>],
        values: &'a [&dyn BindableValue<QueryBuilder<QueryValues>>],
        statement_type: StatementType,
    ) -> InsertBuilder<
        'a,
        Self,
        [&'a dyn BindableToken<QueryBuilder<QueryValues>>],
        [&'a dyn BindableValue<QueryBuilder<QueryValues>>],
        QueryConsistency,
        DynamicRequest,
    > {
        match statement_type {
            StatementType::Query => self.insert_query_with(statement, key, values),
            StatementType::Prepared => self.insert_prepared_with(statement, key, values),
        }
    }

    /// Create a dynamic insert query request from a statement and variables. The token `{{keyspace}}` will be replaced
    /// with the keyspace name.
    ///
    /// ## Example
    /// ```no_run
    /// use scylla_rs::app::access::*;
    /// "my_keyspace"
    ///     .insert_query_with(
    ///         "INSERT INTO {{keyspace}}.table (key, val1, val2) VALUES (?,?,?)",
    ///         &[&3],
    ///         &[&4.0, &5.0],
    ///     )
    ///     .consistency(Consistency::One)
    ///     .build()?
    ///     .get_local_blocking()?;
    /// # Ok::<(), anyhow::Error>(())
    /// ```
    fn insert_query_with<'a>(
        &'a self,
        statement: &str,
        key: &'a [&dyn BindableToken<QueryBuilder<QueryValues>>],
        values: &'a [&dyn BindableValue<QueryBuilder<QueryValues>>],
    ) -> InsertBuilder<
        'a,
        Self,
        [&'a dyn BindableToken<QueryBuilder<QueryValues>>],
        [&'a dyn BindableValue<QueryBuilder<QueryValues>>],
        QueryConsistency,
        DynamicRequest,
    > {
        InsertBuilder {
            keyspace: PhantomData,
            statement: statement.to_owned().into(),
            key,
            values,
            builder: QueryStatement::encode_statement(Query::new(), &self.replace_keyspace_token(statement)),
            _marker: DynamicRequest,
        }
    }

    /// Create a dynamic insert prepared request from a statement and variables. The token `{{keyspace}}` will be
    /// replaced with the keyspace name.
    ///
    /// ## Example
    /// ```no_run
    /// use scylla_rs::app::access::*;
    /// "my_keyspace"
    ///     .insert_prepared_with(
    ///         "INSERT INTO {{keyspace}}.table (key, val1, val2) VALUES (?,?,?)",
    ///         &[&3],
    ///         &[&4.0, &5.0],
    ///     )
    ///     .consistency(Consistency::One)
    ///     .build()?
    ///     .get_local_blocking()?;
    /// # Ok::<(), anyhow::Error>(())
    /// ```
    fn insert_prepared_with<'a>(
        &'a self,
        statement: &str,
        key: &'a [&dyn BindableToken<QueryBuilder<QueryValues>>],
        values: &'a [&dyn BindableValue<QueryBuilder<QueryValues>>],
    ) -> InsertBuilder<
        'a,
        Self,
        [&'a dyn BindableToken<QueryBuilder<QueryValues>>],
        [&'a dyn BindableValue<QueryBuilder<QueryValues>>],
        QueryConsistency,
        DynamicRequest,
    > {
        InsertBuilder {
            keyspace: PhantomData,
            statement: statement.to_owned().into(),
            key,
            values,
            builder: PreparedStatement::encode_statement(Query::new(), &self.replace_keyspace_token(statement)),
            _marker: DynamicRequest,
        }
    }
}

/// Specifies helper functions for creating dynamic insert requests from anything that can be interpreted as a statement

pub trait AsDynamicInsertRequest: ToStatement
where
    Self: Sized,
{
    /// Create a dynamic insert request from a statement and variables. Can be specified as either
    /// a query or prepared statement.
    ///
    /// ## Example
    /// ```no_run
    /// use scylla_rs::app::access::*;
    /// "INSERT INTO my_keyspace.table (key, val1, val2) VALUES (?,?,?)"
    ///     .as_insert(&[&3], &[&4.0, &5.0], StatementType::Prepared)
    ///     .consistency(Consistency::One)
    ///     .build()?
    ///     .get_local_blocking()?;
    /// # Ok::<(), anyhow::Error>(())
    /// ```
    fn as_insert<'a>(
        &self,
        key: &'a [&dyn BindableToken<QueryBuilder<QueryValues>>],
        values: &'a [&dyn BindableValue<QueryBuilder<QueryValues>>],
        statement_type: StatementType,
    ) -> InsertBuilder<
        'a,
        Self,
        [&'a dyn BindableToken<QueryBuilder<QueryValues>>],
        [&'a dyn BindableValue<QueryBuilder<QueryValues>>],
        QueryConsistency,
        DynamicRequest,
    > {
        match statement_type {
            StatementType::Query => self.as_insert_query(key, values),
            StatementType::Prepared => self.as_insert_prepared(key, values),
        }
    }

    /// Create a dynamic insert query request from a statement and variables.
    ///
    /// ## Example
    /// ```no_run
    /// use scylla_rs::app::access::*;
    /// "INSERT INTO my_keyspace.table (key, val1, val2) VALUES (?,?,?)"
    ///     .as_insert_query(&[&3], &[&4.0, &5.0])
    ///     .consistency(Consistency::One)
    ///     .build()?
    ///     .get_local_blocking()?;
    /// # Ok::<(), anyhow::Error>(())
    /// ```
    fn as_insert_query<'a>(
        &self,
        key: &'a [&dyn BindableToken<QueryBuilder<QueryValues>>],
        values: &'a [&dyn BindableValue<QueryBuilder<QueryValues>>],
    ) -> InsertBuilder<
        'a,
        Self,
        [&'a dyn BindableToken<QueryBuilder<QueryValues>>],
        [&'a dyn BindableValue<QueryBuilder<QueryValues>>],
        QueryConsistency,
        DynamicRequest,
    > {
        let statement = self.to_statement();
        InsertBuilder {
            _marker: DynamicRequest,
            keyspace: PhantomData,
            builder: QueryStatement::encode_statement(Query::new(), &statement),
            statement,
            key,
            values,
        }
    }

    /// Create a dynamic insert prepared request from a statement and variables.
    ///
    /// ## Example
    /// ```no_run
    /// use scylla_rs::app::access::*;
    /// "INSERT INTO my_keyspace.table (key, val1, val2) VALUES (?,?,?)"
    ///     .as_insert_prepared(&[&3], &[&4.0, &5.0])
    ///     .consistency(Consistency::One)
    ///     .build()?
    ///     .get_local_blocking()?;
    /// # Ok::<(), anyhow::Error>(())
    /// ```
    fn as_insert_prepared<'a>(
        &self,
        key: &'a [&dyn BindableToken<QueryBuilder<QueryValues>>],
        values: &'a [&dyn BindableValue<QueryBuilder<QueryValues>>],
    ) -> InsertBuilder<
        'a,
        Self,
        [&'a dyn BindableToken<QueryBuilder<QueryValues>>],
        [&'a dyn BindableValue<QueryBuilder<QueryValues>>],
        QueryConsistency,
        DynamicRequest,
    > {
        let statement = self.to_statement();
        InsertBuilder {
            _marker: DynamicRequest,
            keyspace: PhantomData,
            builder: PreparedStatement::encode_statement(Query::new(), &statement),
            statement,
            key,
            values,
        }
    }
}

impl<S: Keyspace, K, V> GetStaticInsertRequest<K, V> for S {}
impl<S: Keyspace> GetDynamicInsertRequest for S {}
impl<S: ToStatement> AsDynamicInsertRequest for S {}

pub struct InsertBuilder<'a, S, K: ?Sized, V: ?Sized, Stage, T> {
    pub(crate) keyspace: PhantomData<fn(S) -> S>,
    pub(crate) statement: Cow<'static, str>,
    pub(crate) key: &'a K,
    pub(crate) values: &'a V,
    pub(crate) builder: QueryBuilder<Stage>,
    pub(crate) _marker: T,
}

impl<'a, S: Insert<K, V>, K: TokenEncoder, V> InsertBuilder<'a, S, K, V, QueryConsistency, StaticRequest> {
    pub fn consistency(self, consistency: Consistency) -> InsertBuilder<'a, S, K, V, QueryValues, StaticRequest> {
        InsertBuilder {
            _marker: self._marker,
            keyspace: self.keyspace,
            statement: self.statement,
            key: self.key,
            values: self.values,
            builder: S::bind_values(
                self.builder.consistency(consistency).bind_values(),
                &self.key,
                &self.values,
            ),
        }
    }

    pub fn timestamp(self, timestamp: i64) -> InsertBuilder<'a, S, K, V, QueryBuild, StaticRequest> {
        InsertBuilder {
            keyspace: self.keyspace,
            statement: self.statement,
            key: self.key,
            values: self.values,
            builder: S::bind_values(
                self.builder.consistency(Consistency::Quorum).bind_values(),
                &self.key,
                &self.values,
            )
            .timestamp(timestamp),
            _marker: self._marker,
        }
    }

    pub fn build(self) -> anyhow::Result<InsertRequest> {
        let query = S::bind_values(
            self.builder.consistency(Consistency::Quorum).bind_values(),
            &self.key,
            &self.values,
        )
        .build()?;
        // create the request
        Ok(CommonRequest {
            token: self.key.token(),
            payload: query.into(),
            statement: self.statement,
        }
        .into())
    }
}

impl<'a, S: Keyspace>
    InsertBuilder<
        'a,
        S,
        [&'a dyn BindableToken<QueryBuilder<QueryValues>>],
        [&'a dyn BindableValue<QueryBuilder<QueryValues>>],
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
    ) -> InsertBuilder<
        'a,
        S,
        [&'a dyn BindableToken<QueryBuilder<QueryValues>>],
        [&'a dyn BindableValue<QueryBuilder<QueryValues>>],
        QueryConsistency,
        ManualBoundRequest<'a, QueryBuilder<QueryValues>>,
    > {
        InsertBuilder {
            _marker: ManualBoundRequest {
                bind_fn: Box::new(bind_fn),
            },
            keyspace: self.keyspace,
            statement: self.statement,
            key: self.key,
            values: self.values,
            builder: self.builder,
        }
    }

    pub fn consistency(
        self,
        consistency: Consistency,
    ) -> InsertBuilder<
        'a,
        S,
        [&'a dyn BindableToken<QueryBuilder<QueryValues>>],
        [&'a dyn BindableValue<QueryBuilder<QueryValues>>],
        QueryValues,
        DynamicRequest,
    > {
        let builder = self
            .builder
            .consistency(consistency)
            .bind_values()
            .bind(self.key)
            .bind(self.values);
        InsertBuilder {
            _marker: self._marker,
            keyspace: self.keyspace,
            statement: self.statement,
            key: self.key,
            values: self.values,
            builder,
        }
    }

    pub fn timestamp(
        self,
        timestamp: i64,
    ) -> InsertBuilder<
        'a,
        S,
        [&'a dyn BindableToken<QueryBuilder<QueryValues>>],
        [&'a dyn BindableValue<QueryBuilder<QueryValues>>],
        QueryBuild,
        DynamicRequest,
    > {
        InsertBuilder {
            keyspace: self.keyspace,
            statement: self.statement,
            key: self.key,
            values: self.values,
            builder: self
                .builder
                .consistency(Consistency::Quorum)
                .bind_values()
                .bind(self.key)
                .bind(self.values)
                .timestamp(timestamp),
            _marker: self._marker,
        }
    }

    pub fn build(self) -> anyhow::Result<InsertRequest> {
        let query = self
            .builder
            .consistency(Consistency::Quorum)
            .bind_values()
            .bind(self.key)
            .bind(self.values)
            .build()?;
        // create the request
        Ok(CommonRequest {
            token: self.key.token(),
            payload: query.into(),
            statement: self.statement,
        }
        .into())
    }
}

impl<'a, S: Keyspace>
    InsertBuilder<
        'a,
        S,
        [&'a dyn BindableToken<QueryBuilder<QueryValues>>],
        [&'a dyn BindableValue<QueryBuilder<QueryValues>>],
        QueryConsistency,
        ManualBoundRequest<'a, QueryBuilder<QueryValues>>,
    >
{
    pub fn consistency(
        self,
        consistency: Consistency,
    ) -> InsertBuilder<
        'a,
        S,
        [&'a dyn BindableToken<QueryBuilder<QueryValues>>],
        [&'a dyn BindableValue<QueryBuilder<QueryValues>>],
        QueryValues,
        DynamicRequest,
    > {
        InsertBuilder {
            _marker: DynamicRequest,
            keyspace: self.keyspace,
            statement: self.statement,
            key: self.key,
            values: self.values,
            builder: (self._marker.bind_fn)(
                self.builder.consistency(consistency).bind_values(),
                self.key,
                self.values,
            ),
        }
    }

    pub fn timestamp(
        self,
        timestamp: i64,
    ) -> InsertBuilder<
        'a,
        S,
        [&'a dyn BindableToken<QueryBuilder<QueryValues>>],
        [&'a dyn BindableValue<QueryBuilder<QueryValues>>],
        QueryBuild,
        DynamicRequest,
    > {
        InsertBuilder {
            keyspace: self.keyspace,
            statement: self.statement,
            key: self.key,
            values: self.values,
            builder: (self._marker.bind_fn)(
                self.builder.consistency(Consistency::Quorum).bind_values(),
                self.key,
                self.values,
            )
            .timestamp(timestamp),
            _marker: DynamicRequest,
        }
    }

    pub fn build(self) -> anyhow::Result<InsertRequest> {
        let query = (self._marker.bind_fn)(
            self.builder.consistency(Consistency::Quorum).bind_values(),
            self.key,
            self.values,
        )
        .build()?;
        // create the request
        Ok(CommonRequest {
            token: self.key.token(),
            payload: query.into(),
            statement: self.statement,
        }
        .into())
    }
}

impl<'a, S, K: ?Sized, V: ?Sized, T> InsertBuilder<'a, S, K, V, QueryValues, T> {
    pub fn timestamp(self, timestamp: i64) -> InsertBuilder<'a, S, K, V, QueryBuild, T> {
        InsertBuilder {
            keyspace: self.keyspace,
            statement: self.statement,
            key: self.key,
            values: self.values,
            builder: self.builder.timestamp(timestamp),
            _marker: self._marker,
        }
    }
}

impl<'a, S, K: TokenEncoder + ?Sized, V: ?Sized, T> InsertBuilder<'a, S, K, V, QueryValues, T> {
    pub fn build(self) -> anyhow::Result<InsertRequest> {
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

impl<'a, S, K: TokenEncoder + ?Sized, V: ?Sized, T> InsertBuilder<'a, S, K, V, QueryBuild, T> {
    pub fn build(self) -> anyhow::Result<InsertRequest> {
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

/// A request to insert a record which can be sent to the ring
#[derive(Debug, Clone)]
pub struct InsertRequest(CommonRequest);

impl From<CommonRequest> for InsertRequest {
    fn from(req: CommonRequest) -> Self {
        InsertRequest(req)
    }
}

impl Deref for InsertRequest {
    type Target = CommonRequest;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for InsertRequest {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl Request for InsertRequest {
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

impl SendRequestExt for InsertRequest {
    type Marker = DecodeVoid;
    type Worker = BasicRetryWorker<Self>;
    const TYPE: RequestType = RequestType::Insert;

    fn worker(self) -> Box<Self::Worker> {
        BasicRetryWorker::new(self)
    }
}

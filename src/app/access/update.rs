// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use super::*;

/// Update query trait which creates an `UpdateRequest`
/// that can be sent to the `Ring`.
///
/// ## Example
/// ```
/// use crate::app::access::*;
/// #[derive(Clone, Debug)]
/// struct MyKeyspace {
///     pub name: Cow<'static, str>,
/// }
/// # impl MyKeyspace {
/// #     pub fn new(name: &str) -> Self {
/// #         Self {
/// #             name: name.into(),
/// #         }
/// #     }
/// # }
/// impl Keyspace for MyKeyspace {
///     fn name(&self) -> &Cow<'static, str> {
///         &self.name
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
/// impl Update<MyKeyType, MyValueType> for MyKeyspace {
///     type QueryOrPrepared = PreparedStatement;
///     fn statement(&self) -> Cow<'static, str> {
///         format!("UPDATE {}.table SET val1 = ?, val2 = ? WHERE key = ?", self.name()).into()
///     }
///
///     fn bind_values<T: Values>(builder: T, key: &MyKeyType, value: &MyValueType) -> T::Return {
///         builder.bind(value).value(key)
///     }
/// }
///
/// # let keyspace = MyKeyspace::new();
/// # let (my_key, my_val) = (1, MyValueType::default());
/// let request = Mykeyspace::new("my_keyspace")
///     .update_query(&my_key, &my_val)
///     .consistency(Consistency::One)
///     .build()?;
/// let worker = request.worker();
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
    fn bind_values<T: Values>(builder: T, key: &K, value: &V) -> T::Return;
}

/// Specifies helper functions for creating static update requests from a keyspace with a `Delete<K, V>` definition
pub trait GetStaticUpdateRequest<K, V>: Keyspace {
    /// Create a static update request from a keyspace with a `Update<K, V>` definition. Will use the default `type
    /// QueryOrPrepared` from the trait definition.
    ///
    /// ## Example
    /// ```no_run
    /// use crate::app::access::*;
    /// #[derive(Clone, Debug)]
    /// struct MyKeyspace {
    ///     pub name: Cow<'static, str>,
    /// }
    /// # impl MyKeyspace {
    /// #     pub fn new(name: &str) -> Self {
    /// #         Self {
    /// #             name: name.into(),
    /// #         }
    /// #     }
    /// # }
    /// impl Keyspace for MyKeyspace {
    ///     fn name(&self) -> &Cow<'static, str> {
    ///         &self.name
    ///     }
    /// }
    /// # type MyKeyType = i32;
    /// # #[derive(Default)]
    /// struct MyValueType {
    ///     value1: f32,
    ///     value2: f32,
    /// }
    /// impl Update<MyKeyType, MyValueType> for MyKeyspace {
    ///     type QueryOrPrepared = PreparedStatement;
    ///     fn statement(&self) -> Cow<'static, str> {
    ///         format!("UPDATE {}.table SET val1 = ?, val2 = ? WHERE key = ?", self.name()).into()
    ///     }
    ///
    ///     fn bind_values<T: Values>(builder: T, key: &MyKeyType, value: &MyValueType) -> T::Return {
    ///         builder.value(value.value1).value(value.value2).value(key)
    ///     }
    /// }
    ///
    /// # let keyspace = MyKeyspace::new();
    /// # let (my_key, my_val) = (1, MyValueType::default());
    /// let request = Mykeyspace::new("my_keyspace")
    ///     .update(&my_key, &my_val)
    ///     .consistency(Consistency::One)
    ///     .build()?
    ///     .get_local()
    ///     .await?;
    /// # Ok::<(), anyhow::Error>(())
    /// ```
    fn update<'a>(&'a self, key: &'a K, value: &'a V) -> UpdateBuilder<'a, Self, K, V, QueryConsistency, StaticRequest>
    where
        Self: Update<K, V>,
    {
        UpdateBuilder {
            keyspace: PhantomData,
            statement: self.statement(),
            key,
            value,
            builder: Self::QueryOrPrepared::encode_statement(Query::new(), &self.statement()),
            _marker: StaticRequest,
        }
    }

    /// Create a static update query request from a keyspace with a `Update<K, V>` definition.
    ///
    /// ## Example
    /// ```no_run
    /// use crate::app::access::*;
    /// #[derive(Clone, Debug)]
    /// struct MyKeyspace {
    ///     pub name: Cow<'static, str>,
    /// }
    /// # impl MyKeyspace {
    /// #     pub fn new(name: &str) -> Self {
    /// #         Self {
    /// #             name: name.into(),
    /// #         }
    /// #     }
    /// # }
    /// impl Keyspace for MyKeyspace {
    ///     fn name(&self) -> &Cow<'static, str> {
    ///         &self.name
    ///     }
    /// }
    /// # type MyKeyType = i32;
    /// # #[derive(Default)]
    /// struct MyValueType {
    ///     value1: f32,
    ///     value2: f32,
    /// }
    /// impl Update<MyKeyType, MyValueType> for MyKeyspace {
    ///     type QueryOrPrepared = PreparedStatement;
    ///     fn statement(&self) -> Cow<'static, str> {
    ///         format!("UPDATE {}.table SET val1 = ?, val2 = ? WHERE key = ?", self.name()).into()
    ///     }
    ///
    ///     fn bind_values<T: Values>(builder: T, key: &MyKeyType, value: &MyValueType) -> T::Return {
    ///         builder.value(value.value1).value(value.value2).value(key)
    ///     }
    /// }
    ///
    /// # let keyspace = MyKeyspace::new();
    /// # let (my_key, my_val) = (1, MyValueType::default());
    /// let request = Mykeyspace::new("my_keyspace")
    ///     .update_query(&my_key, &my_val)
    ///     .consistency(Consistency::One)
    ///     .build()?
    ///     .get_local()
    ///     .await?;
    /// # Ok::<(), anyhow::Error>(())
    /// ```
    fn update_query<'a>(
        &'a self,
        key: &'a K,
        value: &'a V,
    ) -> UpdateBuilder<'a, Self, K, V, QueryConsistency, StaticRequest>
    where
        Self: Update<K, V>,
    {
        UpdateBuilder {
            keyspace: PhantomData,
            statement: self.statement(),
            key,
            value,
            builder: QueryStatement::encode_statement(Query::new(), &self.statement()),
            _marker: StaticRequest,
        }
    }

    /// Create a static update prepared request from a keyspace with a `Update<K, V>` definition.
    ///
    /// ## Example
    /// ```no_run
    /// use crate::app::access::*;
    /// #[derive(Clone, Debug)]
    /// struct MyKeyspace {
    ///     pub name: Cow<'static, str>,
    /// }
    /// # impl MyKeyspace {
    /// #     pub fn new(name: &str) -> Self {
    /// #         Self {
    /// #             name: name.into(),
    /// #         }
    /// #     }
    /// # }
    /// impl Keyspace for MyKeyspace {
    ///     fn name(&self) -> &Cow<'static, str> {
    ///         &self.name
    ///     }
    /// }
    /// # type MyKeyType = i32;
    /// # #[derive(Default)]
    /// struct MyValueType {
    ///     value1: f32,
    ///     value2: f32,
    /// }
    /// impl Update<MyKeyType, MyValueType> for MyKeyspace {
    ///     type QueryOrPrepared = PreparedStatement;
    ///     fn statement(&self) -> Cow<'static, str> {
    ///         format!("UPDATE {}.table SET val1 = ?, val2 = ? WHERE key = ?", self.name()).into()
    ///     }
    ///
    ///     fn bind_values<T: Values>(builder: T, key: &MyKeyType, value: &MyValueType) -> T::Return {
    ///         builder.value(value.value1).value(value.value2).value(key)
    ///     }
    /// }
    ///
    /// # let keyspace = MyKeyspace::new();
    /// # let (my_key, my_val) = (1, MyValueType::default());
    /// let request = Mykeyspace::new("my_keyspace")
    ///     .update_prepared(&my_key, &my_val)
    ///     .consistency(Consistency::One)
    ///     .build()?
    ///     .get_local()
    ///     .await?;
    /// # Ok::<(), anyhow::Error>(())
    /// ```
    fn update_prepared<'a>(
        &'a self,
        key: &'a K,
        value: &'a V,
    ) -> UpdateBuilder<'a, Self, K, V, QueryConsistency, StaticRequest>
    where
        Self: Update<K, V>,
    {
        UpdateBuilder {
            keyspace: PhantomData,
            statement: self.statement(),
            key,
            value,
            builder: PreparedStatement::encode_statement(Query::new(), &self.statement()),
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
    /// use crate::app::access::*;
    /// "my_keyspace"
    ///     .update_with(
    ///         "UPDATE {{keyspace}}.table SET val1 = ?, val2 = ? WHERE key = ?",
    ///         &[&3],
    ///         &[&4.0, &5.0],
    ///         StatementType::Query,
    ///     )
    ///     .consistency(Consistency::One)
    ///     .build()?
    ///     .get_local()
    ///     .await?;
    /// # Ok::<(), anyhow::Error>(())
    /// ```
    fn update_with<'a>(
        &'a self,
        statement: &str,
        key: &'a [&(dyn TokenChainer + Sync)],
        variables: &'a [&(dyn ColumnEncoder + Sync)],
        statement_type: StatementType,
    ) -> UpdateBuilder<
        'a,
        Self,
        [&(dyn TokenChainer + Sync)],
        [&(dyn ColumnEncoder + Sync)],
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
    /// use crate::app::access::*;
    /// "my_keyspace"
    ///     .update_query_with(
    ///         "UPDATE {{keyspace}}.table SET val1 = ?, val2 = ? WHERE key = ?",
    ///         &[&3],
    ///         &[&4.0, &5.0],
    ///     )
    ///     .consistency(Consistency::One)
    ///     .build()?
    ///     .get_local()
    ///     .await?;
    /// # Ok::<(), anyhow::Error>(())
    /// ```
    fn update_query_with<'a>(
        &'a self,
        statement: &str,
        key: &'a [&(dyn TokenChainer + Sync)],
        variables: &'a [&(dyn ColumnEncoder + Sync)],
    ) -> UpdateBuilder<
        'a,
        Self,
        [&(dyn TokenChainer + Sync)],
        [&(dyn ColumnEncoder + Sync)],
        QueryConsistency,
        DynamicRequest,
    > {
        UpdateBuilder {
            keyspace: PhantomData,
            statement: statement.to_owned().into(),
            key,
            value: variables,
            builder: QueryStatement::encode_statement(Query::new(), &self.replace_keyspace_token(statement)),
            _marker: DynamicRequest,
        }
    }

    /// Create a dynamic update prepared request from a statement and variables. The token `{{keyspace}}` will be
    /// replaced with the keyspace name.
    ///
    /// ## Example
    /// ```no_run
    /// use crate::app::access::*;
    /// "my_keyspace"
    ///     .update_prepared_with(
    ///         "UPDATE {{keyspace}}.table SET val1 = ?, val2 = ? WHERE key = ?",
    ///         &[&3],
    ///         &[&4.0, &5.0],
    ///     )
    ///     .consistency(Consistency::One)
    ///     .build()?
    ///     .get_local()
    ///     .await?;
    /// # Ok::<(), anyhow::Error>(())
    /// ```
    fn update_prepared_with<'a>(
        &'a self,
        statement: &str,
        key: &'a [&(dyn TokenChainer + Sync)],
        variables: &'a [&(dyn ColumnEncoder + Sync)],
    ) -> UpdateBuilder<
        'a,
        Self,
        [&(dyn TokenChainer + Sync)],
        [&(dyn ColumnEncoder + Sync)],
        QueryConsistency,
        DynamicRequest,
    > {
        UpdateBuilder {
            keyspace: PhantomData,
            statement: statement.to_owned().into(),
            key,
            value: variables,
            builder: PreparedStatement::encode_statement(Query::new(), &self.replace_keyspace_token(statement)),
            _marker: DynamicRequest,
        }
    }
}

/// Specifies helper functions for creating dynamic insert requests from anything that can be interpreted as a statement

pub trait AsDynamicUpdateRequest: ToStatement
where
    Self: Sized,
{
    /// Create a dynamic update request from a statement and variables. Can be specified as either
    /// a query or prepared statement.
    ///
    /// ## Example
    /// ```no_run
    /// use crate::app::access::*;
    /// "UPDATE my_keyspace.table SET val1 = ?, val2 = ? WHERE key = ?"
    ///     .as_update(&[&3], &[&4.0, &5.0], StatementType::Query)
    ///     .consistency(Consistency::One)
    ///     .build()?
    ///     .get_local()
    ///     .await?;
    /// # Ok::<(), anyhow::Error>(())
    /// ```
    fn as_update<'a>(
        &self,
        key: &'a [&(dyn TokenChainer + Sync)],
        variables: &'a [&(dyn ColumnEncoder + Sync)],
        statement_type: StatementType,
    ) -> UpdateBuilder<
        'a,
        Self,
        [&'a (dyn TokenChainer + Sync)],
        [&'a (dyn ColumnEncoder + Sync)],
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
    /// use crate::app::access::*;
    /// "UPDATE my_keyspace.table SET val1 = ?, val2 = ? WHERE key = ?"
    ///     .as_update_query(&[&3], &[&4.0, &5.0])
    ///     .consistency(Consistency::One)
    ///     .build()?
    ///     .get_local()
    ///     .await?;
    /// # Ok::<(), anyhow::Error>(())
    /// ```
    fn as_update_query<'a>(
        &self,
        key: &'a [&(dyn TokenChainer + Sync)],
        variables: &'a [&(dyn ColumnEncoder + Sync)],
    ) -> UpdateBuilder<
        'a,
        Self,
        [&'a (dyn TokenChainer + Sync)],
        [&'a (dyn ColumnEncoder + Sync)],
        QueryConsistency,
        DynamicRequest,
    > {
        let statement = self.to_statement();
        UpdateBuilder {
            _marker: DynamicRequest,
            keyspace: PhantomData,
            builder: QueryStatement::encode_statement(Query::new(), &statement),
            statement,
            key,
            value: variables,
        }
    }

    /// Create a dynamic update prepared request from a statement and variables.
    ///
    /// ## Example
    /// ```no_run
    /// use crate::app::access::*;
    /// "UPDATE my_keyspace.table SET val1 = ?, val2 = ? WHERE key = ?"
    ///     .as_update_prepared(&[&3], &[&4.0, &5.0])
    ///     .consistency(Consistency::One)
    ///     .build()?
    ///     .get_local()
    ///     .await?;
    /// # Ok::<(), anyhow::Error>(())
    /// ```
    fn as_update_prepared<'a>(
        &self,
        key: &'a [&(dyn TokenChainer + Sync)],
        variables: &'a [&(dyn ColumnEncoder + Sync)],
    ) -> UpdateBuilder<
        'a,
        Self,
        [&'a (dyn TokenChainer + Sync)],
        [&'a (dyn ColumnEncoder + Sync)],
        QueryConsistency,
        DynamicRequest,
    > {
        let statement = self.to_statement();
        UpdateBuilder {
            _marker: DynamicRequest,
            keyspace: PhantomData,
            builder: PreparedStatement::encode_statement(Query::new(), &statement),
            statement,
            key,
            value: variables,
        }
    }
}

impl<S: Keyspace, K, V> GetStaticUpdateRequest<K, V> for S {}
impl<S: Keyspace> GetDynamicUpdateRequest for S {}
impl<S: ToStatement> AsDynamicUpdateRequest for S {}

pub struct UpdateBuilder<'a, S, K: ?Sized, V: ?Sized, Stage, T> {
    pub(crate) keyspace: PhantomData<fn(S) -> S>,
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
            builder: S::bind_values(self.builder.consistency(consistency), &self.key, &self.value),
        }
    }
}

impl<'a, S: Keyspace>
    UpdateBuilder<
        'a,
        S,
        [&(dyn TokenChainer + Sync)],
        [&'a (dyn ColumnEncoder + Sync)],
        QueryConsistency,
        DynamicRequest,
    >
{
    pub fn bind_values<
        F: 'static
            + Fn(
                Box<dyn DynValues<Return = QueryBuilder<QueryValues>>>,
                &[&(dyn TokenChainer + Sync)],
                &[&(dyn ColumnEncoder + Sync)],
            ) -> QueryBuilder<QueryValues>,
    >(
        self,
        bind_fn: F,
    ) -> UpdateBuilder<
        'a,
        S,
        [&'a (dyn TokenChainer + Sync)],
        [&'a (dyn ColumnEncoder + Sync)],
        QueryConsistency,
        ManualBoundRequest<'a>,
    > {
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
    ) -> UpdateBuilder<
        'a,
        S,
        [&'a (dyn TokenChainer + Sync)],
        [&'a (dyn ColumnEncoder + Sync)],
        QueryValues,
        DynamicRequest,
    > {
        let builder = self.builder.consistency(consistency).bind(self.value).bind(self.key);
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
    UpdateBuilder<
        'a,
        S,
        [&(dyn TokenChainer + Sync)],
        [&(dyn ColumnEncoder + Sync)],
        QueryConsistency,
        ManualBoundRequest<'a>,
    >
{
    pub fn consistency(
        self,
        consistency: Consistency,
    ) -> UpdateBuilder<
        'a,
        S,
        [&'a (dyn TokenChainer + Sync)],
        [&'a (dyn ColumnEncoder + Sync)],
        QueryValues,
        DynamicRequest,
    > {
        UpdateBuilder {
            _marker: DynamicRequest,
            keyspace: self.keyspace,
            statement: self.statement,
            key: self.key,
            value: self.value,
            builder: (self._marker.bind_fn)(Box::new(self.builder.consistency(consistency)), self.key, self.value),
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
        Ok(CommonRequest {
            token: self.key.token(),
            payload: query.into(),
            statement: self.statement,
        }
        .into())
    }
}

impl<'a, S: Keyspace, V: ?Sized> UpdateBuilder<'a, S, [&(dyn TokenChainer + Sync)], V, QueryValues, DynamicRequest> {
    pub fn timestamp(
        self,
        timestamp: i64,
    ) -> UpdateBuilder<'a, S, [&'a (dyn TokenChainer + Sync)], V, QueryBuild, DynamicRequest> {
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
        Ok(CommonRequest {
            token: self.key.get_token(),
            payload: query.into(),
            statement: self.statement,
        }
        .into())
    }
}

impl<'a, S: Update<K, V>, K: ComputeToken, V, T> UpdateBuilder<'a, S, K, V, QueryBuild, T> {
    /// Build the UpdateRequest
    pub fn build(self) -> anyhow::Result<UpdateRequest> {
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

impl<'a, S: Keyspace, V: ?Sized> UpdateBuilder<'a, S, [&(dyn TokenChainer + Sync)], V, QueryBuild, DynamicRequest> {
    /// Build the UpdateRequest
    pub fn build(self) -> anyhow::Result<UpdateRequest> {
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

impl UpdateRequest {
    /// Get a basic worker for this request
    pub fn worker(self) -> Box<BasicRetryWorker<Self>> {
        BasicRetryWorker::new(self)
    }
}

impl Request for UpdateRequest {
    fn token(&self) -> i64 {
        self.0.token()
    }

    fn statement(&self) -> &Cow<'static, str> {
        self.0.statement()
    }

    fn payload(&self) -> &Vec<u8> {
        self.0.payload()
    }

    fn payload_mut(&mut self) -> &mut Vec<u8> {
        self.0.payload_mut()
    }

    fn into_payload(self) -> Vec<u8> {
        self.0.into_payload()
    }
}

impl SendRequestExt for UpdateRequest {
    type Marker = DecodeVoid;
    const TYPE: RequestType = RequestType::Update;
}

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
/// # type MyValueType = f32;
/// impl Select<MyKeyType, MyValueType> for MyKeyspace {
///     type QueryOrPrepared = PreparedStatement;
///     fn statement(&self) -> Cow<'static, str> {
///         format!("SELECT val FROM {}.table where key = ?", self.name()).into()
///     }
///     fn bind_values<T: Values>(builder: T, key: &MyKeyType) -> T::Return {
///         builder.bind(key)
///     }
/// }
/// # let my_key = 1;
/// let request = MyKeyspace::new("my_keyspace")
///     .select::<MyValueType>(&my_key)
///     .consistency(Consistency::One)
///     .build()?;
/// let worker = request.worker();
/// # Ok::<(), anyhow::Error>(())
/// ```
pub trait Select<K, V>: Keyspace {
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
    fn bind_values<T: Values>(builder: T, key: &K) -> T::Return;
}

/// Specifies helper functions for creating static delete requests from a keyspace with a `Delete<K, V>` definition

pub trait GetStaticSelectRequest<K>: Keyspace {
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
    /// # type MyValueType = f32;
    /// impl Select<MyKeyType, MyValueType> for MyKeyspace {
    ///     type QueryOrPrepared = PreparedStatement;
    ///     fn statement(&self) -> Cow<'static, str> {
    ///         format!("SELECT val FROM {}.table where key = ?", self.name()).into()
    ///     }
    ///     fn bind_values<T: Values>(builder: T, key: &MyKeyType) -> T::Return {
    ///         builder.bind(key)
    ///     }
    /// }
    /// # let my_key = 1;
    /// let res: Option<MyValueType> = MyKeyspace::new("my_keyspace")
    ///     .select::<MyValueType>(&my_key)
    ///     .consistency(Consistency::One)
    ///     .build()?
    ///     .get_local_blocking()?;
    /// # Ok::<(), anyhow::Error>(())
    /// ```
    fn select<'a, V>(&'a self, key: &'a K) -> SelectBuilder<'a, Self, K, V, QueryConsistency, StaticRequest>
    where
        Self: Select<K, V>,
    {
        SelectBuilder {
            _marker: PhantomData,
            keyspace: PhantomData,
            statement: self.statement(),
            key,
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
    /// # type MyValueType = f32;
    /// impl Select<MyKeyType, MyValueType> for MyKeyspace {
    ///     type QueryOrPrepared = PreparedStatement;
    ///     fn statement(&self) -> Cow<'static, str> {
    ///         format!("SELECT val FROM {}.table where key = ?", self.name()).into()
    ///     }
    ///     fn bind_values<T: Values>(builder: T, key: &MyKeyType) -> T::Return {
    ///         builder.bind(key)
    ///     }
    /// }
    /// # let my_key = 1;
    /// let res: Option<MyValueType> = MyKeyspace::new("my_keyspace")
    ///     .select_query::<MyValueType>(&my_key)
    ///     .consistency(Consistency::One)
    ///     .build()?
    ///     .get_local_blocking()?;
    /// # Ok::<(), anyhow::Error>(())
    /// ```
    fn select_query<'a, V>(&'a self, key: &'a K) -> SelectBuilder<'a, Self, K, V, QueryConsistency, StaticRequest>
    where
        Self: Select<K, V>,
    {
        SelectBuilder {
            _marker: PhantomData,
            keyspace: PhantomData,
            statement: self.statement(),
            key,
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
    /// # type MyValueType = f32;
    /// impl Select<MyKeyType, MyValueType> for MyKeyspace {
    ///     type QueryOrPrepared = PreparedStatement;
    ///     fn statement(&self) -> Cow<'static, str> {
    ///         format!("SELECT val FROM {}.table where key = ?", self.name()).into()
    ///     }
    ///     fn bind_values<T: Values>(builder: T, key: &MyKeyType) -> T::Return {
    ///         builder.bind(key)
    ///     }
    /// }
    /// # let my_key = 1;
    /// let res: Option<MyValueType> = MyKeyspace::new("my_keyspace")
    ///     .select_prepared::<MyValueType>(&my_key)
    ///     .consistency(Consistency::One)
    ///     .build()?
    ///     .get_local_blocking()?;
    /// # Ok::<(), anyhow::Error>(())
    /// ```
    fn select_prepared<'a, V>(&'a self, key: &'a K) -> SelectBuilder<'a, Self, K, V, QueryConsistency, StaticRequest>
    where
        Self: Select<K, V>,
    {
        SelectBuilder {
            _marker: PhantomData,
            keyspace: PhantomData,
            statement: self.statement(),
            key,
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
    ///         "SELECT val FROM {{keyspace}}.table where key = ?",
    ///         &[&3],
    ///         StatementType::Query,
    ///     )
    ///     .consistency(Consistency::One)
    ///     .build()?
    ///     .get_local_blocking()?;
    /// # Ok::<(), anyhow::Error>(())
    /// ```
    fn select_with<'a, V>(
        &'a self,
        statement: &str,
        key: &'a [&(dyn TokenChainer + Sync)],
        statement_type: StatementType,
    ) -> SelectBuilder<'a, Self, [&'a (dyn TokenChainer + Sync)], V, QueryConsistency, DynamicRequest> {
        match statement_type {
            StatementType::Query => self.select_query_with(statement, key),
            StatementType::Prepared => self.select_prepared_with(statement, key),
        }
    }

    /// Create a dynamic select query request from a statement and variables. The token `{{keyspace}}` will be replaced
    /// with the keyspace name.
    ///
    /// ## Example
    /// ```no_run
    /// use scylla_rs::app::access::*;
    /// let res: Option<f32> = "my_keyspace"
    ///     .select_query_with::<f32>("SELECT val FROM {{keyspace}}.table where key = ?", &[&3])
    ///     .consistency(Consistency::One)
    ///     .build()?
    ///     .get_local_blocking()?;
    /// # Ok::<(), anyhow::Error>(())
    /// ```
    fn select_query_with<'a, V>(
        &'a self,
        statement: &str,
        key: &'a [&(dyn TokenChainer + Sync)],
    ) -> SelectBuilder<'a, Self, [&(dyn TokenChainer + Sync)], V, QueryConsistency, DynamicRequest> {
        SelectBuilder {
            _marker: PhantomData,
            keyspace: PhantomData,
            statement: statement.to_owned().into(),
            key,
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
    ///     .select_prepared_with::<f32>("SELECT val FROM {{keyspace}}.table where key = ?", &[&3])
    ///     .consistency(Consistency::One)
    ///     .build()?
    ///     .get_local_blocking()?;
    /// # Ok::<(), anyhow::Error>(())
    /// ```
    fn select_prepared_with<'a, V>(
        &'a self,
        statement: &str,
        key: &'a [&(dyn TokenChainer + Sync)],
    ) -> SelectBuilder<'a, Self, [&(dyn TokenChainer + Sync)], V, QueryConsistency, DynamicRequest> {
        SelectBuilder {
            _marker: PhantomData,
            keyspace: PhantomData,
            statement: statement.to_owned().into(),
            key,
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
    /// let res: Option<f32> = "SELECT val FROM my_keyspace.table where key = ?"
    ///     .as_select::<f32>(&[&3], StatementType::Query)
    ///     .consistency(Consistency::One)
    ///     .build()?
    ///     .get_local_blocking()?;
    /// # Ok::<(), anyhow::Error>(())
    /// ```
    fn as_select<'a, V>(
        &self,
        key: &'a [&(dyn TokenChainer + Sync)],
        statement_type: StatementType,
    ) -> SelectBuilder<'a, Self, [&'a (dyn TokenChainer + Sync)], V, QueryConsistency, DynamicRequest> {
        match statement_type {
            StatementType::Query => self.as_select_query(key),
            StatementType::Prepared => self.as_select_prepared(key),
        }
    }

    /// Create a dynamic select query request from a statement and variables.
    ///
    /// ## Example
    /// ```no_run
    /// use scylla_rs::app::access::*;
    /// let res: Option<f32> = "SELECT val FROM my_keyspace.table where key = ?"
    ///     .as_select_query::<f32>(&[&3])
    ///     .consistency(Consistency::One)
    ///     .build()?
    ///     .get_local_blocking()?;
    /// # Ok::<(), anyhow::Error>(())
    /// ```
    fn as_select_query<'a, V>(
        &self,
        key: &'a [&(dyn TokenChainer + Sync)],
    ) -> SelectBuilder<'a, Self, [&'a (dyn TokenChainer + Sync)], V, QueryConsistency, DynamicRequest> {
        let statement = self.to_statement();
        SelectBuilder {
            _marker: PhantomData,
            keyspace: PhantomData,
            builder: QueryStatement::encode_statement(Query::new(), &statement),
            statement,
            key,
        }
    }

    /// Create a dynamic select prepared request from a statement and variables.
    ///
    /// ## Example
    /// ```no_run
    /// use scylla_rs::app::access::*;
    /// let res: Option<f32> = "SELECT val FROM my_keyspace.table where key = ?"
    ///     .as_select_prepared::<f32>(&[&3])
    ///     .consistency(Consistency::One)
    ///     .build()?
    ///     .get_local_blocking()?;
    /// # Ok::<(), anyhow::Error>(())
    /// ```
    fn as_select_prepared<'a, V>(
        &self,
        key: &'a [&(dyn TokenChainer + Sync)],
    ) -> SelectBuilder<'a, Self, [&'a (dyn TokenChainer + Sync)], V, QueryConsistency, DynamicRequest> {
        let statement = self.to_statement();
        SelectBuilder {
            _marker: PhantomData,
            keyspace: PhantomData,
            builder: PreparedStatement::encode_statement(Query::new(), &statement),
            statement,
            key,
        }
    }
}

impl<S: Keyspace, K> GetStaticSelectRequest<K> for S {}
impl<S: Keyspace> GetDynamicSelectRequest for S {}
impl<S: ToStatement> AsDynamicSelectRequest for S {}

pub struct SelectBuilder<'a, S, K: ?Sized, V, Stage, T> {
    keyspace: PhantomData<fn(S) -> S>,
    statement: Cow<'static, str>,
    key: &'a K,
    builder: QueryBuilder<Stage>,
    _marker: PhantomData<fn(V, T) -> (V, T)>,
}

impl<'a, S: Select<K, V>, K, V> SelectBuilder<'a, S, K, V, QueryConsistency, StaticRequest> {
    pub fn consistency(self, consistency: Consistency) -> SelectBuilder<'a, S, K, V, QueryValues, StaticRequest> {
        SelectBuilder {
            _marker: self._marker,
            keyspace: self.keyspace,
            statement: self.statement,
            key: self.key,
            builder: S::bind_values(self.builder.consistency(consistency), &self.key),
        }
    }
}

impl<'a, S: Keyspace, V> SelectBuilder<'a, S, [&(dyn TokenChainer + Sync)], V, QueryConsistency, DynamicRequest> {
    pub fn consistency(
        self,
        consistency: Consistency,
    ) -> SelectBuilder<'a, S, [&'a (dyn TokenChainer + Sync)], V, QueryValues, DynamicRequest> {
        let builder = self.builder.consistency(consistency).bind(self.key);
        SelectBuilder {
            _marker: self._marker,
            keyspace: self.keyspace,
            statement: self.statement,
            key: self.key,
            builder,
        }
    }
}

impl<'a, S, K, V, T> SelectBuilder<'a, S, K, V, QueryValues, T> {
    pub fn page_size(self, page_size: i32) -> SelectBuilder<'a, S, K, V, QueryPagingState, T> {
        SelectBuilder {
            _marker: self._marker,
            keyspace: self.keyspace,
            statement: self.statement,
            key: self.key,
            builder: self.builder.page_size(page_size),
        }
    }
    /// Set the paging state.
    pub fn paging_state(self, paging_state: &Option<Vec<u8>>) -> SelectBuilder<'a, S, K, V, QuerySerialConsistency, T> {
        SelectBuilder {
            _marker: self._marker,
            keyspace: self.keyspace,
            statement: self.statement,
            key: self.key,
            builder: self.builder.paging_state(paging_state),
        }
    }
    pub fn timestamp(self, timestamp: i64) -> SelectBuilder<'a, S, K, V, QueryBuild, T> {
        SelectBuilder {
            _marker: self._marker,
            keyspace: self.keyspace,
            statement: self.statement,
            key: self.key,
            builder: self.builder.timestamp(timestamp),
        }
    }
}

impl<'a, S: Keyspace, V: RowsDecoder>
    SelectBuilder<'a, S, [&(dyn TokenChainer + Sync)], V, QueryValues, DynamicRequest>
{
    /// Build the SelectRequest
    pub fn build(self) -> anyhow::Result<SelectRequest<V>> {
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

impl<'a, S: Keyspace, K: ComputeToken, V: RowsDecoder> SelectBuilder<'a, S, K, V, QueryValues, StaticRequest> {
    /// Build the SelectRequest
    pub fn build(self) -> anyhow::Result<SelectRequest<V>> {
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

impl<'a, S: Keyspace, V: RowsDecoder>
    SelectBuilder<'a, S, [&(dyn TokenChainer + Sync)], V, QueryBuild, DynamicRequest>
{
    /// Build the SelectRequest
    pub fn build(self) -> anyhow::Result<SelectRequest<V>> {
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

impl<'a, S: Keyspace, K: ComputeToken, V: RowsDecoder> SelectBuilder<'a, S, K, V, QueryBuild, StaticRequest> {
    /// Build the SelectRequest
    pub fn build(self) -> anyhow::Result<SelectRequest<V>> {
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

impl<'a, S, K, V, T> SelectBuilder<'a, S, K, V, QueryPagingState, T> {
    /// Set the paging state in the query frame.
    pub fn paging_state(self, paging_state: &Option<Vec<u8>>) -> SelectBuilder<'a, S, K, V, QuerySerialConsistency, T> {
        SelectBuilder {
            _marker: self._marker,
            keyspace: self.keyspace,
            statement: self.statement,
            key: self.key,
            builder: self.builder.paging_state(paging_state),
        }
    }

    /// Set the timestamp of the query frame.
    pub fn timestamp(self, timestamp: i64) -> SelectBuilder<'a, S, K, V, QueryBuild, T> {
        SelectBuilder {
            _marker: self._marker,
            keyspace: self.keyspace,
            statement: self.statement,
            key: self.key,
            builder: self.builder.timestamp(timestamp),
        }
    }
}
impl<'a, S: Keyspace, K: ComputeToken, V: RowsDecoder> SelectBuilder<'a, S, K, V, QueryPagingState, StaticRequest> {
    pub fn build(self) -> anyhow::Result<SelectRequest<V>> {
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
impl<'a, S: Keyspace, V: RowsDecoder>
    SelectBuilder<'a, S, [&(dyn TokenChainer + Sync)], V, QueryPagingState, DynamicRequest>
{
    /// Build the SelectRequest
    pub fn build(self) -> anyhow::Result<SelectRequest<V>> {
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
impl<'a, S, K, V, T> SelectBuilder<'a, S, K, V, QuerySerialConsistency, T> {
    /// Set the timestamp of the query frame.
    pub fn timestamp(self, timestamp: i64) -> SelectBuilder<'a, S, K, V, QueryBuild, T> {
        SelectBuilder {
            _marker: self._marker,
            keyspace: self.keyspace,
            statement: self.statement,
            key: self.key,
            builder: self.builder.timestamp(timestamp),
        }
    }
}

impl<'a, S: Keyspace, K: ComputeToken, V: RowsDecoder>
    SelectBuilder<'a, S, K, V, QuerySerialConsistency, StaticRequest>
{
    pub fn build(self) -> anyhow::Result<SelectRequest<V>> {
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
impl<'a, S: Keyspace, V: RowsDecoder>
    SelectBuilder<'a, S, [&(dyn TokenChainer + Sync)], V, QuerySerialConsistency, DynamicRequest>
{
    /// Build the SelectRequest
    pub fn build(self) -> anyhow::Result<SelectRequest<V>> {
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

/// A request to select a record which can be sent to the ring
pub struct SelectRequest<V> {
    inner: CommonRequest,
    _marker: PhantomData<fn(V) -> V>,
}

impl<V> From<CommonRequest> for SelectRequest<V> {
    fn from(inner: CommonRequest) -> Self {
        SelectRequest {
            inner,
            _marker: PhantomData,
        }
    }
}

impl<V> Deref for SelectRequest<V> {
    type Target = CommonRequest;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl<V> DerefMut for SelectRequest<V> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.inner
    }
}

impl<V> Debug for SelectRequest<V> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SelectRequest").field("inner", &self.inner).finish()
    }
}

impl<V> Clone for SelectRequest<V> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            _marker: PhantomData,
        }
    }
}

impl<V: 'static> Request for SelectRequest<V> {
    fn token(&self) -> i64 {
        self.inner.token()
    }

    fn statement(&self) -> &Cow<'static, str> {
        self.inner.statement()
    }

    fn payload(&self) -> Vec<u8> {
        self.inner.payload()
    }
}

impl<V> SelectRequest<V> {
    /// Return DecodeResult marker type, useful in case the worker struct wants to hold the
    /// decoder in order to decode the response inside handle_response method.
    pub fn result_decoder(&self) -> DecodeResult<DecodeRows<V>> {
        DecodeResult::select()
    }
}

impl<V> SendRequestExt for SelectRequest<V>
where
    V: 'static + Send + RowsDecoder + Debug,
{
    type Marker = DecodeRows<V>;
    type Worker = BasicRetryWorker<Self>;
    const TYPE: RequestType = RequestType::Select;

    fn worker(self) -> Box<Self::Worker> {
        BasicRetryWorker::new(self)
    }
}

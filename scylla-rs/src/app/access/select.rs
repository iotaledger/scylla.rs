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
/// impl Select<MyKeyType, MyVarType, MyValueType> for MyKeyspace {
///     type QueryOrPrepared = PreparedStatement;
///     fn statement(&self) -> SelectStatement {
///         parse_statement!("SELECT val FROM my_table where key = ? AND var = ?")
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
pub trait Select<T: Table, K: Bindable + TokenEncoder, O: RowsDecoder>: Keyspace {
    /// Create your select statement here.
    fn statement(&self) -> SelectStatement;

    /// Bind the cql values to the builder
    fn bind_values<B: Binder>(binder: B, key: &K) -> Result<B, B::Error> {
        binder.bind(key)
    }
}

impl<T: Table + RowsDecoder, S: Keyspace> Select<T, T::PrimaryKey, T> for S
where
    T::PrimaryKey: Bindable + TokenEncoder,
{
    fn statement(&self) -> SelectStatement {
        let where_clause = T::PARTITION_KEY
            .iter()
            .chain(T::CLUSTERING_COLS.iter())
            .map(|&c| Relation::normal(c, Operator::Equal, BindMarker::Anonymous))
            .collect::<Vec<_>>();
        parse_statement!("SELECT * FROM #.# #", self.name(), T::NAME, where_clause)
    }
}

/// Specifies helper functions for creating static delete requests from a keyspace with a `Delete<K, V>` definition

pub trait GetStaticSelectRequest<T: Table, K: Bindable + TokenEncoder>: Keyspace {
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
    /// impl Select<MyKeyType, MyVarType, MyValueType> for MyKeyspace {
    ///     type QueryOrPrepared = PreparedStatement;
    ///     fn statement(&self) -> SelectStatement {
    ///         parse_statement!("SELECT val FROM my_table where key = ? AND var = ?")
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
    fn select<O>(&self, key: &K) -> Result<SelectBuilder<StaticRequest, O>, TokenBindError<K>>
    where
        Self: Select<T, K, O>,
        O: RowsDecoder,
    {
        let statement = self.statement();
        let keyspace = statement.get_keyspace();
        let statement = statement.to_string();
        let mut builder = QueryBuilder::default()
            .consistency(Consistency::One)
            .statement(&statement);
        builder = Self::bind_values(builder, key)?;
        Ok(SelectBuilder {
            token: Some(key.token().map_err(TokenBindError::TokenEncodeError)?),
            builder,
            statement,
            keyspace,
            _marker: PhantomData,
        })
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
    /// impl Select<MyKeyType, MyVarType, MyValueType> for MyKeyspace {
    ///     type QueryOrPrepared = PreparedStatement;
    ///     fn statement(&self) -> SelectStatement {
    ///         parse_statement!("SELECT val FROM my_table where key = ? AND var = ?")
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
    fn select_prepared<O>(&self, key: &K) -> Result<SelectBuilder<StaticRequest, O>, TokenBindError<K>>
    where
        Self: Select<T, K, O>,
        O: RowsDecoder,
    {
        let statement = self.statement();
        let keyspace = statement.get_keyspace();
        let statement = statement.to_string();
        let mut builder = QueryBuilder::default()
            .consistency(Consistency::One)
            .id(&statement.id());
        builder = Self::bind_values(builder, key)?;
        Ok(SelectBuilder {
            token: Some(key.token().map_err(TokenBindError::TokenEncodeError)?),
            builder,
            statement,
            keyspace,
            _marker: PhantomData,
        })
    }
}
impl<T: Table, S: Keyspace, K: Bindable + TokenEncoder> GetStaticSelectRequest<T, K> for S {}

/// Specifies helper functions for creating dynamic select requests from anything that can be interpreted as a statement

pub trait AsDynamicSelectRequest: Sized {
    /// Create a dynamic select query request from a statement and variables.
    ///
    /// ## Example
    /// ```no_run
    /// use scylla_rs::app::access::*;
    /// let res: Option<f32> = parse_statement!("SELECT val FROM my_keyspace.my_table where key = ? AND var = ?")
    ///     .as_select_query::<f32>(&[&3], &[&"hello"])
    ///     .consistency(Consistency::One)
    ///     .build()?
    ///     .get_local_blocking()?;
    /// # Ok::<(), anyhow::Error>(())
    /// ```
    fn query<O: RowsDecoder>(&self) -> SelectBuilder<DynamicRequest, O>;

    /// Create a dynamic select prepared request from a statement and variables.
    ///
    /// ## Example
    /// ```no_run
    /// use scylla_rs::app::access::*;
    /// let res: Option<f32> = parse_statement!("SELECT val FROM my_keyspace.my_table where key = ? AND var = ?")
    ///     .as_select_prepared::<f32>(&[&3], &[&"hello"])
    ///     .consistency(Consistency::One)
    ///     .build()?
    ///     .get_local_blocking()?;
    /// # Ok::<(), anyhow::Error>(())
    /// ```
    fn query_prepared<O: RowsDecoder>(&self) -> SelectBuilder<DynamicRequest, O>;
}
impl AsDynamicSelectRequest for SelectStatement {
    fn query<O: RowsDecoder>(&self) -> SelectBuilder<DynamicRequest, O> {
        let keyspace = self.get_keyspace();
        let statement = self.to_string();
        SelectBuilder {
            builder: QueryBuilder::default()
                .consistency(Consistency::One)
                .statement(&statement),
            statement,
            keyspace,
            token: None,
            _marker: PhantomData,
        }
    }

    fn query_prepared<O: RowsDecoder>(&self) -> SelectBuilder<DynamicRequest, O> {
        let keyspace = self.get_keyspace();
        let statement = self.to_string();
        SelectBuilder {
            builder: QueryBuilder::default()
                .consistency(Consistency::One)
                .id(&statement.id()),
            statement,
            keyspace,
            token: None,
            _marker: PhantomData,
        }
    }
}

pub struct SelectBuilder<R, O: RowsDecoder> {
    keyspace: Option<String>,
    statement: String,
    builder: QueryBuilder,
    token: Option<i64>,
    _marker: PhantomData<fn(R, O) -> (R, O)>,
}

impl<R, O: RowsDecoder> SelectBuilder<R, O> {
    pub fn consistency(mut self, consistency: Consistency) -> Self {
        self.builder = self.builder.consistency(consistency);
        self
    }

    pub fn page_size(mut self, page_size: i32) -> Self {
        self.builder = self.builder.page_size(page_size);
        self
    }
    /// Set the paging state.
    pub fn paging_state(mut self, paging_state: Vec<u8>) -> Self {
        self.builder = self.builder.paging_state(paging_state);
        self
    }
    pub fn timestamp(mut self, timestamp: i64) -> Self {
        self.builder = self.builder.timestamp(timestamp);
        self
    }

    pub fn build(self) -> anyhow::Result<SelectRequest<O>> {
        Ok(CommonRequest {
            token: self.token.unwrap_or_else(|| rand::random()),
            payload: self.builder.build()?.into(),
            keyspace: self.keyspace,
            statement: self.statement,
        }
        .into())
    }
}

impl<O: RowsDecoder> SelectBuilder<DynamicRequest, O> {
    pub fn token<V: TokenEncoder>(mut self, value: &V) -> Result<Self, V::Error> {
        self.token.replace(value.token()?);
        Ok(self)
    }

    pub fn bind<V: Bindable>(mut self, value: &V) -> Result<Self, <QueryBuilder as Binder>::Error> {
        self.builder = self.builder.bind(value)?;
        Ok(self)
    }

    pub fn bind_token<V: Bindable + TokenEncoder>(mut self, value: &V) -> Result<Self, TokenBindError<V>> {
        self.token
            .replace(value.token().map_err(TokenBindError::TokenEncodeError)?);
        self.builder = self.builder.bind(value)?;
        Ok(self)
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

    fn statement(&self) -> &String {
        self.inner.statement()
    }

    fn payload(&self) -> Vec<u8> {
        self.inner.payload()
    }
    fn keyspace(&self) -> &Option<String> {
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

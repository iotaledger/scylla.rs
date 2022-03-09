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
/// impl Insert<MyKeyType, MyValueType> for MyKeyspace {
///     type QueryOrPrepared = PreparedStatement;
///     fn statement(&self) -> InsertStatement {
///         parse_statement!("INSERT INTO my_table (key, val1, val2) VALUES (?,?,?)")
///     }
///
///     fn bind_values<B: Binder>(builder: B, key: &MyKeyType, values: &MyValueType) -> B {
///         builder.value(key).bind(values)
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
pub trait Insert<K: Bindable>: Table {
    /// Create your insert statement here.
    fn statement(keyspace: &dyn Keyspace) -> InsertStatement;

    /// Bind the cql values to the builder
    fn bind_values<B: Binder>(binder: B, key: &K) -> Result<B, B::Error> {
        binder.bind(key)
    }
}

impl<T: Table + Bindable> Insert<T> for T {
    fn statement(keyspace: &dyn Keyspace) -> InsertStatement {
        let names = T::COLS.iter().map(|&(c, _)| Name::from(c)).collect::<Vec<_>>();
        let values = T::COLS
            .iter()
            .map(|_| Term::from(BindMarker::Anonymous))
            .collect::<Vec<_>>();
        parse_statement!(
            "INSERT INTO #.# (#) VALUES (#)",
            keyspace.name(),
            T::NAME,
            names,
            values
        )
    }
}

/// Specifies helper functions for creating static insert requests from a keyspace with a `Insert<K, V>` definition
pub trait GetStaticInsertRequest<K: Bindable>: Table {
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
    /// # #[derive(Default)]
    /// struct MyValueType {
    ///     value1: f32,
    ///     value2: f32,
    /// }
    /// impl Insert<MyKeyType, MyVarType, MyValueType> for MyKeyspace {
    ///     type QueryOrPrepared = PreparedStatement;
    ///     fn statement(&self) -> InsertStatement {
    ///         parse_statement!("UPDATE my_table SET val1 = ?, val2 = ? WHERE key = ? AND var = ?")
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
    ///     .insert(&my_key, &my_var, &my_val)
    ///     .consistency(Consistency::One)
    ///     .build()?
    ///     .get_local_blocking()?;
    /// # Ok::<(), anyhow::Error>(())
    /// ```
    fn insert(
        keyspace: &dyn Keyspace,
        key: &K,
    ) -> Result<InsertBuilder<StaticRequest, QueryFrameBuilder>, <QueryFrameBuilder as Binder>::Error>
    where
        Self: Insert<K>,
        K: Bindable,
    {
        let statement = Self::statement(keyspace);
        let keyspace = statement.get_keyspace();
        let token_indexes = statement.token_indexes::<Self>();
        let statement = statement.to_string();
        let mut builder = QueryFrameBuilder::default()
            .consistency(Consistency::Quorum)
            .statement(statement.clone());
        builder = Self::bind_values(builder, key)?;
        Ok(InsertBuilder {
            token_indexes,
            builder,
            keyspace,
            statement,
            _marker: PhantomData,
        })
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
    /// # #[derive(Default)]
    /// struct MyValueType {
    ///     value1: f32,
    ///     value2: f32,
    /// }
    /// impl Insert<MyKeyType, MyVarType, MyValueType> for MyKeyspace {
    ///     type QueryOrPrepared = PreparedStatement;
    ///     fn statement(&self) -> InsertStatement {
    ///         parse_statement!("UPDATE my_table SET val1 = ?, val2 = ? WHERE key = ? AND var = ?")
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
    ///     .insert_prepared(&my_key, &my_var, &my_val)
    ///     .consistency(Consistency::One)
    ///     .build()?
    ///     .get_local_blocking()?;
    /// # Ok::<(), anyhow::Error>(())
    /// ```
    fn insert_prepared(
        keyspace: &dyn Keyspace,
        key: &K,
    ) -> Result<InsertBuilder<StaticRequest, ExecuteFrameBuilder>, <ExecuteFrameBuilder as Binder>::Error>
    where
        Self: Insert<K>,
        K: Bindable,
    {
        let statement = Self::statement(keyspace);
        let keyspace = statement.get_keyspace();
        let token_indexes = statement.token_indexes::<Self>();
        let statement = statement.to_string();
        let mut builder = ExecuteFrameBuilder::default()
            .consistency(Consistency::Quorum)
            .id(statement.id());
        builder = Self::bind_values(builder, key)?;
        Ok(InsertBuilder {
            token_indexes,
            builder,
            keyspace,
            statement,
            _marker: PhantomData,
        })
    }
}
impl<T: Table, K: Bindable> GetStaticInsertRequest<K> for T {}

/// Specifies helper functions for creating dynamic insert requests from anything that can be interpreted as a statement

pub trait AsDynamicInsertRequest: Sized {
    /// Create a dynamic insert request from a statement and variables. Can be specified as either
    /// a query or prepared statement.
    ///
    /// ## Example
    /// ```no_run
    /// use scylla_rs::app::access::*;
    /// parse_statement!("UPDATE my_keyspace.my_table SET val1 = ?, val2 = ? WHERE key = ? AND var = ?")
    ///     .as_insert(&[&3], &[&4.0, &5.0], StatementType::Query)
    ///     .consistency(Consistency::One)
    ///     .build()?
    ///     .get_local_blocking()?;
    /// # Ok::<(), anyhow::Error>(())
    /// ```
    fn query(&self) -> InsertBuilder<DynamicRequest, QueryFrameBuilder>;

    /// Create a dynamic insert prepared request from a statement and variables.
    ///
    /// ## Example
    /// ```no_run
    /// use scylla_rs::app::access::*;
    /// parse_statement!("UPDATE my_keyspace.my_table SET val1 = ?, val2 = ? WHERE key = ? AND var = ?")
    ///     .as_insert_prepared(&[&3], &[&4.0, &5.0])
    ///     .consistency(Consistency::One)
    ///     .build()?
    ///     .get_local_blocking()?;
    /// # Ok::<(), anyhow::Error>(())
    /// ```
    fn query_prepared(&self) -> InsertBuilder<DynamicRequest, ExecuteFrameBuilder>;
}
impl AsDynamicInsertRequest for InsertStatement {
    fn query(&self) -> InsertBuilder<DynamicRequest, QueryFrameBuilder> {
        let keyspace = self.get_keyspace();
        let statement = self.to_string();
        InsertBuilder {
            builder: QueryFrameBuilder::default()
                .consistency(Consistency::Quorum)
                .statement(statement.clone()),
            keyspace,
            statement,
            token_indexes: Default::default(),
            _marker: PhantomData,
        }
    }

    fn query_prepared(&self) -> InsertBuilder<DynamicRequest, ExecuteFrameBuilder> {
        let keyspace = self.get_keyspace();
        let statement = self.to_string();
        InsertBuilder {
            builder: ExecuteFrameBuilder::default()
                .consistency(Consistency::Quorum)
                .id(statement.id()),
            keyspace,
            statement,
            token_indexes: Default::default(),
            _marker: PhantomData,
        }
    }
}

pub struct InsertBuilder<R, B> {
    keyspace: Option<String>,
    statement: String,
    builder: B,
    token_indexes: Vec<usize>,
    _marker: PhantomData<fn(R) -> R>,
}

impl<R> InsertBuilder<R, QueryFrameBuilder> {
    pub fn consistency(mut self, consistency: Consistency) -> Self {
        self.builder = self.builder.consistency(consistency);
        self
    }

    pub fn timestamp(mut self, timestamp: i64) -> Self {
        self.builder = self.builder.timestamp(timestamp);
        self
    }

    pub fn build(self) -> anyhow::Result<QueryInsertRequest> {
        let frame = self.builder.build()?;
        let mut token = TokenEncodeChain::default();
        for idx in self.token_indexes {
            if frame.values.len() <= idx {
                anyhow::bail!("No value bound at index {}", idx);
            }
            token.append(&frame.values[idx]);
        }
        Ok(QueryInsertRequest::new(frame, token.finish(), self.keyspace))
    }
}

impl<R> InsertBuilder<R, ExecuteFrameBuilder> {
    pub fn consistency(mut self, consistency: Consistency) -> Self {
        self.builder = self.builder.consistency(consistency);
        self
    }

    pub fn timestamp(mut self, timestamp: i64) -> Self {
        self.builder = self.builder.timestamp(timestamp);
        self
    }

    pub fn build(self) -> anyhow::Result<ExecuteInsertRequest> {
        let frame = self.builder.build()?;
        let mut token = TokenEncodeChain::default();
        for idx in self.token_indexes {
            if frame.values.len() <= idx {
                anyhow::bail!("No value bound at index {}", idx);
            }
            token.append(&frame.values[idx]);
        }
        Ok(ExecuteInsertRequest::new(
            frame,
            token.finish(),
            self.keyspace,
            self.statement,
        ))
    }
}

impl<B: Binder> InsertBuilder<DynamicRequest, B> {
    pub fn bind<V: Bindable>(mut self, value: &V) -> Result<Self, B::Error> {
        self.builder = self.builder.bind(value)?;
        Ok(self)
    }
}

impl<R> From<PreparedQuery> for InsertBuilder<R, ExecuteFrameBuilder> {
    fn from(res: PreparedQuery) -> Self {
        Self {
            keyspace: res.keyspace,
            statement: res.statement,
            builder: ExecuteFrameBuilder::default()
                .id(res.result.id)
                .consistency(Consistency::Quorum),
            token_indexes: res.result.metadata().pk_indexes().iter().map(|v| *v as usize).collect(),
            _marker: PhantomData,
        }
    }
}

impl<R, B: std::fmt::Debug> std::fmt::Debug for InsertBuilder<R, B> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("InsertBuilder")
            .field("keyspace", &self.keyspace)
            .field("statement", &self.statement)
            .field("builder", &self.builder)
            .field("token_indexes", &self.token_indexes)
            .finish()
    }
}

impl<R, B: Clone> Clone for InsertBuilder<R, B> {
    fn clone(&self) -> Self {
        Self {
            keyspace: self.keyspace.clone(),
            statement: self.statement.clone(),
            builder: self.builder.clone(),
            token_indexes: self.token_indexes.clone(),
            _marker: PhantomData,
        }
    }
}

impl<R> TryInto<QueryInsertRequest> for InsertBuilder<R, QueryFrameBuilder> {
    type Error = anyhow::Error;

    fn try_into(self) -> Result<QueryInsertRequest, Self::Error> {
        self.build()
    }
}
impl<R> TryInto<ExecuteInsertRequest> for InsertBuilder<R, ExecuteFrameBuilder> {
    type Error = anyhow::Error;

    fn try_into(self) -> Result<ExecuteInsertRequest, Self::Error> {
        self.build()
    }
}
impl<R> SendAsRequestExt<QueryInsertRequest> for InsertBuilder<R, QueryFrameBuilder> {}
impl<R> SendAsRequestExt<ExecuteInsertRequest> for InsertBuilder<R, ExecuteFrameBuilder> {}

/// A request to insert a record which can be sent to the ring
#[derive(Debug, Clone)]
pub struct QueryInsertRequest {
    frame: QueryFrame,
    token: i64,
    keyspace: Option<String>,
}

impl QueryInsertRequest {
    pub fn new(frame: QueryFrame, token: i64, keyspace: Option<String>) -> Self {
        Self { frame, token, keyspace }
    }
}

impl RequestFrameExt for QueryInsertRequest {
    type Frame = QueryFrame;

    fn frame(&self) -> &Self::Frame {
        &self.frame
    }

    fn into_frame(self) -> RequestFrame {
        self.frame.into()
    }
}

impl ShardAwareExt for QueryInsertRequest {
    fn token(&self) -> i64 {
        self.token
    }

    fn keyspace(&self) -> Option<&String> {
        self.keyspace.as_ref()
    }
}

impl Deref for QueryInsertRequest {
    type Target = QueryFrame;

    fn deref(&self) -> &Self::Target {
        &self.frame
    }
}

impl DerefMut for QueryInsertRequest {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.frame
    }
}

impl SendRequestExt for QueryInsertRequest {
    type Worker = BasicRetryWorker<Self>;
    type Marker = DecodeVoid;
    const TYPE: RequestType = RequestType::Insert;

    fn marker(&self) -> Self::Marker {
        DecodeVoid
    }

    fn event(self) -> (Self::Worker, RequestFrame) {
        (BasicRetryWorker::new(self.clone()), self.into_frame())
    }

    fn worker(self) -> Self::Worker {
        BasicRetryWorker::new(self)
    }
}

/// A request to delete a record which can be sent to the ring
#[derive(Debug, Clone)]
pub struct ExecuteInsertRequest {
    frame: ExecuteFrame,
    token: i64,
    keyspace: Option<String>,
    statement: String,
}

impl ExecuteInsertRequest {
    pub fn new(frame: ExecuteFrame, token: i64, keyspace: Option<String>, statement: String) -> Self {
        Self {
            frame,
            token,
            keyspace,
            statement,
        }
    }
}

impl RequestFrameExt for ExecuteInsertRequest {
    type Frame = ExecuteFrame;

    fn frame(&self) -> &Self::Frame {
        &self.frame
    }

    fn into_frame(self) -> RequestFrame {
        self.frame.into()
    }
}

impl ShardAwareExt for ExecuteInsertRequest {
    fn token(&self) -> i64 {
        self.token
    }

    fn keyspace(&self) -> Option<&String> {
        self.keyspace.as_ref()
    }
}

impl ReprepareExt for ExecuteInsertRequest {
    type OutRequest = QueryInsertRequest;
    fn convert(self) -> Self::OutRequest {
        QueryInsertRequest {
            token: self.token,
            frame: QueryFrame::from_execute(self.frame, self.statement),
            keyspace: self.keyspace,
        }
    }

    fn statement(&self) -> &String {
        &self.statement
    }
}

impl Deref for ExecuteInsertRequest {
    type Target = ExecuteFrame;

    fn deref(&self) -> &Self::Target {
        &self.frame
    }
}

impl DerefMut for ExecuteInsertRequest {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.frame
    }
}

impl SendRequestExt for ExecuteInsertRequest {
    type Worker = BasicRetryWorker<Self>;
    type Marker = DecodeVoid;
    const TYPE: RequestType = RequestType::Insert;

    fn marker(&self) -> Self::Marker {
        DecodeVoid
    }

    fn event(self) -> (Self::Worker, RequestFrame) {
        (BasicRetryWorker::new(self.clone()), self.into_frame())
    }

    fn worker(self) -> Self::Worker {
        BasicRetryWorker::new(self)
    }
}

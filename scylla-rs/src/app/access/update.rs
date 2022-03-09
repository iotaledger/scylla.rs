// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

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
/// impl<B: Binder> Bindable<B> for MyValueType {
///     fn bind(&self, binder: B) -> B {
///         binder.bind(&self.value1).bind(&self.value2)
///     }
/// }
/// impl Update<MyKeyType, MyVarType, MyValueType> for MyKeyspace {
///     type QueryOrPrepared = PreparedStatement;
///     fn statement(&self) -> UpdateStatement {
///         parse_statement!("UPDATE my_table SET val1 = ?, val2 = ? WHERE key = ? AND var = ?")
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
pub trait Update<K: Bindable, V>: Table {
    /// Create your update statement here.
    fn statement(keyspace: &dyn Keyspace) -> UpdateStatement;

    /// Bind the cql values to the builder
    fn bind_values<B: Binder>(binder: B, key: &K, values: &V) -> Result<B, B::Error>;
}

/// Specifies helper functions for creating static update requests from a keyspace with a `Delete<K, V>` definition
pub trait GetStaticUpdateRequest<K: Bindable, V>: Table {
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
    /// impl Update<MyKeyType, MyVarType, MyValueType> for MyKeyspace {
    ///     type QueryOrPrepared = PreparedStatement;
    ///     fn statement(&self) -> UpdateStatement {
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
    ///     .update(&my_key, &my_var, &my_val)
    ///     .consistency(Consistency::One)
    ///     .build()?
    ///     .get_local_blocking()?;
    /// # Ok::<(), anyhow::Error>(())
    /// ```
    fn update(
        keyspace: &dyn Keyspace,
        key: &K,
        values: &V,
    ) -> Result<UpdateBuilder<StaticRequest, QueryFrameBuilder>, <QueryFrameBuilder as Binder>::Error>
    where
        Self: Update<K, V>,
    {
        let statement = Self::statement(keyspace);
        let keyspace = statement.get_keyspace();
        let token_indexes = statement.token_indexes::<Self>();
        let statement = statement.to_string();
        let mut builder = QueryFrameBuilder::default()
            .consistency(Consistency::Quorum)
            .statement(statement.clone());
        builder = Self::bind_values(builder, key, values)?;
        Ok(UpdateBuilder {
            token_indexes,
            builder,
            statement,
            keyspace,
            _marker: PhantomData,
        })
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
    /// impl Update<MyKeyType, MyVarType, MyValueType> for MyKeyspace {
    ///     type QueryOrPrepared = PreparedStatement;
    ///     fn statement(&self) -> UpdateStatement {
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
    ///     .update_prepared(&my_key, &my_var, &my_val)
    ///     .consistency(Consistency::One)
    ///     .build()?
    ///     .get_local_blocking()?;
    /// # Ok::<(), anyhow::Error>(())
    /// ```
    fn update_prepared(
        keyspace: &dyn Keyspace,
        key: &K,
        values: &V,
    ) -> Result<UpdateBuilder<StaticRequest, ExecuteFrameBuilder>, <ExecuteFrameBuilder as Binder>::Error>
    where
        Self: Update<K, V>,
    {
        let statement = Self::statement(keyspace);
        let keyspace = statement.get_keyspace();
        let token_indexes = statement.token_indexes::<Self>();
        let statement = statement.to_string();
        let mut builder = ExecuteFrameBuilder::default()
            .consistency(Consistency::Quorum)
            .id(statement.id());
        builder = Self::bind_values(builder, key, values)?;
        Ok(UpdateBuilder {
            token_indexes,
            builder,
            statement,
            keyspace,
            _marker: PhantomData,
        })
    }
}
impl<T: Table, K: Bindable, V> GetStaticUpdateRequest<K, V> for T {}

/// Specifies helper functions for creating dynamic insert requests from anything that can be interpreted as a statement

pub trait AsDynamicUpdateRequest: Sized {
    /// Create a dynamic update request from a statement and variables. Can be specified as either
    /// a query or prepared statement.
    ///
    /// ## Example
    /// ```no_run
    /// use scylla_rs::app::access::*;
    /// parse_statement!("UPDATE my_keyspace.my_table SET val1 = ?, val2 = ? WHERE key = ? AND var = ?")
    ///     .as_update(&[&3], &[&4.0, &5.0], StatementType::Query)
    ///     .consistency(Consistency::One)
    ///     .build()?
    ///     .get_local_blocking()?;
    /// # Ok::<(), anyhow::Error>(())
    /// ```
    fn query(&self) -> UpdateBuilder<DynamicRequest, QueryFrameBuilder>;

    /// Create a dynamic update prepared request from a statement and variables.
    ///
    /// ## Example
    /// ```no_run
    /// use scylla_rs::app::access::*;
    /// parse_statement!("UPDATE my_keyspace.my_table SET val1 = ?, val2 = ? WHERE key = ? AND var = ?")
    ///     .as_update_prepared(&[&3], &[&4.0, &5.0])
    ///     .consistency(Consistency::One)
    ///     .build()?
    ///     .get_local_blocking()?;
    /// # Ok::<(), anyhow::Error>(())
    /// ```
    fn query_prepared(&self) -> UpdateBuilder<DynamicRequest, ExecuteFrameBuilder>;
}
impl AsDynamicUpdateRequest for UpdateStatement {
    fn query(&self) -> UpdateBuilder<DynamicRequest, QueryFrameBuilder> {
        let keyspace = self.get_keyspace();
        let statement = self.to_string();
        UpdateBuilder {
            builder: QueryFrameBuilder::default()
                .consistency(Consistency::Quorum)
                .statement(statement.clone()),
            statement,
            keyspace,
            token_indexes: Default::default(),
            _marker: PhantomData,
        }
    }

    fn query_prepared(&self) -> UpdateBuilder<DynamicRequest, ExecuteFrameBuilder> {
        let keyspace = self.get_keyspace();
        let statement = self.to_string();
        UpdateBuilder {
            builder: ExecuteFrameBuilder::default()
                .consistency(Consistency::Quorum)
                .id(statement.id()),
            statement,
            keyspace,
            token_indexes: Default::default(),
            _marker: PhantomData,
        }
    }
}

pub struct UpdateBuilder<R, B> {
    keyspace: Option<String>,
    statement: String,
    builder: B,
    token_indexes: Vec<usize>,
    _marker: PhantomData<fn(R) -> R>,
}

impl<R> UpdateBuilder<R, QueryFrameBuilder> {
    pub fn consistency(mut self, consistency: Consistency) -> Self {
        self.builder = self.builder.consistency(consistency);
        self
    }

    pub fn timestamp(mut self, timestamp: i64) -> Self {
        self.builder = self.builder.timestamp(timestamp);
        self
    }

    pub fn build(self) -> anyhow::Result<QueryUpdateRequest> {
        let frame = self.builder.build()?;
        let mut token = TokenEncodeChain::default();
        for idx in self.token_indexes {
            if frame.values.len() <= idx {
                anyhow::bail!("No value bound at index {}", idx);
            }
            token.append(&frame.values[idx]);
        }
        Ok(QueryUpdateRequest::new(frame, token.finish(), self.keyspace))
    }
}

impl<R> UpdateBuilder<R, ExecuteFrameBuilder> {
    pub fn consistency(mut self, consistency: Consistency) -> Self {
        self.builder = self.builder.consistency(consistency);
        self
    }

    pub fn timestamp(mut self, timestamp: i64) -> Self {
        self.builder = self.builder.timestamp(timestamp);
        self
    }

    pub fn build(self) -> anyhow::Result<ExecuteUpdateRequest> {
        let frame = self.builder.build()?;
        let mut token = TokenEncodeChain::default();
        for idx in self.token_indexes {
            if frame.values.len() <= idx {
                anyhow::bail!("No value bound at index {}", idx);
            }
            token.append(&frame.values[idx]);
        }
        Ok(ExecuteUpdateRequest::new(
            frame,
            token.finish(),
            self.keyspace,
            self.statement,
        ))
    }
}

impl<B: Binder> UpdateBuilder<DynamicRequest, B> {
    pub fn bind<V: Bindable>(mut self, value: &V) -> Result<Self, B::Error> {
        self.builder = self.builder.bind(value)?;
        Ok(self)
    }
}

impl<R> From<PreparedQuery> for UpdateBuilder<R, ExecuteFrameBuilder> {
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

impl<R, B: std::fmt::Debug> std::fmt::Debug for UpdateBuilder<R, B> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("UpdateBuilder")
            .field("keyspace", &self.keyspace)
            .field("statement", &self.statement)
            .field("builder", &self.builder)
            .field("token_indexes", &self.token_indexes)
            .finish()
    }
}

impl<R, B: Clone> Clone for UpdateBuilder<R, B> {
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

impl<R> TryInto<QueryUpdateRequest> for UpdateBuilder<R, QueryFrameBuilder> {
    type Error = anyhow::Error;

    fn try_into(self) -> Result<QueryUpdateRequest, Self::Error> {
        self.build()
    }
}
impl<R> TryInto<ExecuteUpdateRequest> for UpdateBuilder<R, ExecuteFrameBuilder> {
    type Error = anyhow::Error;

    fn try_into(self) -> Result<ExecuteUpdateRequest, Self::Error> {
        self.build()
    }
}
impl<R> SendAsRequestExt<QueryUpdateRequest> for UpdateBuilder<R, QueryFrameBuilder> {}
impl<R> SendAsRequestExt<ExecuteUpdateRequest> for UpdateBuilder<R, ExecuteFrameBuilder> {}

/// A request to update a record which can be sent to the ring
#[derive(Debug, Clone)]
pub struct QueryUpdateRequest {
    frame: QueryFrame,
    token: i64,
    keyspace: Option<String>,
}

impl QueryUpdateRequest {
    pub fn new(frame: QueryFrame, token: i64, keyspace: Option<String>) -> Self {
        Self { frame, token, keyspace }
    }
}

impl RequestFrameExt for QueryUpdateRequest {
    type Frame = QueryFrame;

    fn frame(&self) -> &Self::Frame {
        &self.frame
    }

    fn into_frame(self) -> RequestFrame {
        self.frame.into()
    }
}

impl ShardAwareExt for QueryUpdateRequest {
    fn token(&self) -> i64 {
        self.token
    }

    fn keyspace(&self) -> Option<&String> {
        self.keyspace.as_ref()
    }
}

impl Deref for QueryUpdateRequest {
    type Target = QueryFrame;

    fn deref(&self) -> &Self::Target {
        &self.frame
    }
}

impl DerefMut for QueryUpdateRequest {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.frame
    }
}

impl SendRequestExt for QueryUpdateRequest {
    type Worker = BasicRetryWorker<Self>;
    type Marker = DecodeVoid;
    const TYPE: RequestType = RequestType::Update;

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
pub struct ExecuteUpdateRequest {
    frame: ExecuteFrame,
    token: i64,
    keyspace: Option<String>,
    statement: String,
}

impl ExecuteUpdateRequest {
    pub fn new(frame: ExecuteFrame, token: i64, keyspace: Option<String>, statement: String) -> Self {
        Self {
            frame,
            token,
            keyspace,
            statement,
        }
    }
}

impl RequestFrameExt for ExecuteUpdateRequest {
    type Frame = ExecuteFrame;

    fn frame(&self) -> &Self::Frame {
        &self.frame
    }

    fn into_frame(self) -> RequestFrame {
        self.frame.into()
    }
}

impl ShardAwareExt for ExecuteUpdateRequest {
    fn token(&self) -> i64 {
        self.token
    }

    fn keyspace(&self) -> Option<&String> {
        self.keyspace.as_ref()
    }
}

impl ReprepareExt for ExecuteUpdateRequest {
    type OutRequest = QueryUpdateRequest;
    fn convert(self) -> Self::OutRequest {
        QueryUpdateRequest {
            token: self.token,
            frame: QueryFrame::from_execute(self.frame, self.statement),
            keyspace: self.keyspace,
        }
    }

    fn statement(&self) -> &String {
        &self.statement
    }
}

impl Deref for ExecuteUpdateRequest {
    type Target = ExecuteFrame;

    fn deref(&self) -> &Self::Target {
        &self.frame
    }
}

impl DerefMut for ExecuteUpdateRequest {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.frame
    }
}

impl SendRequestExt for ExecuteUpdateRequest {
    type Worker = BasicRetryWorker<Self>;
    type Marker = DecodeVoid;
    const TYPE: RequestType = RequestType::Update;

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

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
pub trait Delete<K: Bindable>: Table {
    /// Create your delete statement here.
    fn statement(keyspace: &dyn Keyspace) -> DeleteStatement;

    /// Bind the cql values to the builder
    fn bind_values<B: Binder>(binder: B, key: &K) -> Result<B, B::Error> {
        binder.bind(key)
    }
}

/// Specifies helper functions for creating static delete requests from a keyspace with a `Delete<K, V>` definition
pub trait GetStaticDeleteRequest<K: Bindable>: Table {
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
    fn delete(
        keyspace: &dyn Keyspace,
        key: &K,
    ) -> Result<DeleteBuilder<StaticRequest, QueryFrameBuilder>, <QueryFrameBuilder as Binder>::Error>
    where
        Self: Delete<K>,
    {
        let statement = Self::statement(keyspace);
        let keyspace = statement.get_keyspace();
        let token_indexes = statement.token_indexes::<Self>();
        let statement = statement.to_string();
        let mut builder = QueryFrameBuilder::default()
            .consistency(Consistency::Quorum)
            .statement(statement.clone());
        builder = Self::bind_values(builder, key)?;
        Ok(DeleteBuilder {
            token_indexes,
            statement,
            builder,
            keyspace,
            _marker: PhantomData,
        })
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
    fn delete_prepared(
        keyspace: &dyn Keyspace,
        key: &K,
    ) -> Result<DeleteBuilder<StaticRequest, ExecuteFrameBuilder>, <ExecuteFrameBuilder as Binder>::Error>
    where
        Self: Delete<K>,
    {
        let statement = Self::statement(keyspace);
        let keyspace = statement.get_keyspace();
        let token_indexes = statement.token_indexes::<Self>();
        let statement = statement.to_string();
        let mut builder = ExecuteFrameBuilder::default()
            .consistency(Consistency::Quorum)
            .id(statement.id());
        builder = Self::bind_values(builder, key)?;
        Ok(DeleteBuilder {
            token_indexes,
            builder,
            statement,
            keyspace,
            _marker: PhantomData,
        })
    }
}
impl<T: Table, K: Bindable> GetStaticDeleteRequest<K> for T {}

/// Specifies helper functions for creating dynamic delete requests from anything that can be interpreted as a statement
pub trait AsDynamicDeleteRequest: Sized {
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
    fn query(&self) -> DeleteBuilder<DynamicRequest, QueryFrameBuilder>;

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
    fn query_prepared(&self) -> DeleteBuilder<DynamicRequest, ExecuteFrameBuilder>;
}
impl AsDynamicDeleteRequest for DeleteStatement {
    fn query(&self) -> DeleteBuilder<DynamicRequest, QueryFrameBuilder> {
        let keyspace = self.get_keyspace();
        let statement = self.to_string();
        DeleteBuilder {
            builder: QueryFrameBuilder::default()
                .consistency(Consistency::Quorum)
                .statement(statement.clone()),
            statement,
            keyspace,
            token_indexes: Default::default(),
            _marker: PhantomData,
        }
    }

    fn query_prepared(&self) -> DeleteBuilder<DynamicRequest, ExecuteFrameBuilder> {
        let keyspace = self.get_keyspace();
        let statement = self.to_string();
        DeleteBuilder {
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

pub struct DeleteBuilder<R, B> {
    keyspace: Option<String>,
    statement: String,
    builder: B,
    token_indexes: Vec<usize>,
    _marker: PhantomData<fn(R) -> R>,
}

impl<R> DeleteBuilder<R, QueryFrameBuilder> {
    pub fn consistency(mut self, consistency: Consistency) -> Self {
        self.builder = self.builder.consistency(consistency);
        self
    }

    pub fn timestamp(mut self, timestamp: i64) -> Self {
        self.builder = self.builder.timestamp(timestamp);
        self
    }

    pub fn build(self) -> anyhow::Result<DeleteRequest> {
        let frame = self.builder.build()?;
        let mut token = TokenEncodeChain::default();
        for idx in self.token_indexes {
            if frame.values.len() <= idx {
                anyhow::bail!("No value bound at index {}", idx);
            }
            token.append(&frame.values[idx]);
        }
        Ok(CommonRequest {
            token: token.finish(),
            statement: frame.statement().clone(),
            payload: RequestFrame::from(frame).build_payload(),
            keyspace: self.keyspace,
        }
        .into())
    }
}

impl<R> DeleteBuilder<R, ExecuteFrameBuilder> {
    pub fn consistency(mut self, consistency: Consistency) -> Self {
        self.builder = self.builder.consistency(consistency);
        self
    }

    pub fn timestamp(mut self, timestamp: i64) -> Self {
        self.builder = self.builder.timestamp(timestamp);
        self
    }

    pub fn build(self) -> anyhow::Result<DeleteRequest> {
        let frame = self.builder.build()?;
        let mut token = TokenEncodeChain::default();
        for idx in self.token_indexes {
            if frame.values.len() <= idx {
                anyhow::bail!("No value bound at index {}", idx);
            }
            token.append(&frame.values[idx]);
        }
        Ok(CommonRequest {
            token: token.finish(),
            statement: self.statement,
            payload: RequestFrame::from(frame).build_payload(),
            keyspace: self.keyspace,
        }
        .into())
    }
}

impl<B: Binder> DeleteBuilder<DynamicRequest, B> {
    pub fn bind<V: Bindable>(mut self, value: &V) -> Result<Self, B::Error> {
        self.builder = self.builder.bind(value)?;
        Ok(self)
    }
}

impl<R> From<PreparedQuery> for DeleteBuilder<R, ExecuteFrameBuilder> {
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

impl<R, B: std::fmt::Debug> std::fmt::Debug for DeleteBuilder<R, B> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DeleteBuilder")
            .field("keyspace", &self.keyspace)
            .field("statement", &self.statement)
            .field("builder", &self.builder)
            .field("token_indexes", &self.token_indexes)
            .finish()
    }
}

impl<R, B: Clone> Clone for DeleteBuilder<R, B> {
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

impl<R> TryInto<DeleteRequest> for DeleteBuilder<R, QueryFrameBuilder> {
    type Error = anyhow::Error;

    fn try_into(self) -> Result<DeleteRequest, Self::Error> {
        self.build()
    }
}
impl<R> TryInto<DeleteRequest> for DeleteBuilder<R, ExecuteFrameBuilder> {
    type Error = anyhow::Error;

    fn try_into(self) -> Result<DeleteRequest, Self::Error> {
        self.build()
    }
}
impl<R> SendAsRequestExt<DeleteRequest> for DeleteBuilder<R, QueryFrameBuilder> {}
impl<R> SendAsRequestExt<DeleteRequest> for DeleteBuilder<R, ExecuteFrameBuilder> {}

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

    fn statement(&self) -> &String {
        self.0.statement()
    }

    fn payload(&self) -> Vec<u8> {
        self.0.payload()
    }
    fn keyspace(&self) -> Option<&String> {
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

    fn marker(&self) -> Self::Marker {
        DecodeVoid
    }
}

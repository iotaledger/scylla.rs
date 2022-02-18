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
pub trait Update<T: Table, K: TokenEncoder, V>: Keyspace {
    /// Create your update statement here.
    fn statement(&self) -> UpdateStatement;

    /// Bind the cql values to the builder
    fn bind_values<B: Binder>(binder: B, key: &K, values: &V) -> Result<B, B::Error>;
}

/// Specifies helper functions for creating static update requests from a keyspace with a `Delete<K, V>` definition
pub trait GetStaticUpdateRequest<T: Table, K: Bindable + TokenEncoder, V>: Keyspace {
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
    fn update(&self, key: &K, values: &V) -> Result<UpdateBuilder<StaticRequest>, TokenBindError<K>>
    where
        Self: Update<T, K, V>,
    {
        let statement = self.statement();
        let keyspace = statement.get_keyspace();
        let statement = statement.to_string();
        let mut builder = QueryBuilder::default()
            .consistency(Consistency::Quorum)
            .statement(&statement);
        builder = Self::bind_values(builder, key, values)?;
        Ok(UpdateBuilder {
            token: Some(key.token().map_err(TokenBindError::TokenEncodeError)?),
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
    fn update_prepared(&self, key: &K, values: &V) -> Result<UpdateBuilder<StaticRequest>, TokenBindError<K>>
    where
        Self: Update<T, K, V>,
    {
        let statement = self.statement();
        let keyspace = statement.get_keyspace();
        let statement = statement.to_string();
        let mut builder = QueryBuilder::default()
            .consistency(Consistency::Quorum)
            .id(&statement.id());
        builder = Self::bind_values(builder, key, values)?;
        Ok(UpdateBuilder {
            token: Some(key.token().map_err(TokenBindError::TokenEncodeError)?),
            builder,
            statement,
            keyspace,
            _marker: PhantomData,
        })
    }
}
impl<T: Table, S: Keyspace, K: Bindable + TokenEncoder, V> GetStaticUpdateRequest<T, K, V> for S {}

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
    fn query(&self) -> UpdateBuilder<DynamicRequest>;

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
    fn query_prepared(&self) -> UpdateBuilder<DynamicRequest>;
}
impl AsDynamicUpdateRequest for UpdateStatement {
    fn query(&self) -> UpdateBuilder<DynamicRequest> {
        let keyspace = self.get_keyspace();
        let statement = self.to_string();
        UpdateBuilder {
            builder: QueryBuilder::default()
                .consistency(Consistency::Quorum)
                .statement(&statement),
            statement,
            keyspace,
            token: None,
            _marker: PhantomData,
        }
    }

    fn query_prepared(&self) -> UpdateBuilder<DynamicRequest> {
        let keyspace = self.get_keyspace();
        let statement = self.to_string();
        UpdateBuilder {
            builder: QueryBuilder::default()
                .consistency(Consistency::Quorum)
                .id(&statement.id()),
            statement,
            keyspace,
            token: None,
            _marker: PhantomData,
        }
    }
}

#[derive(Debug)]
pub struct UpdateBuilder<R> {
    keyspace: Option<String>,
    statement: String,
    builder: QueryBuilder,
    token: Option<i64>,
    _marker: PhantomData<fn(R) -> R>,
}

impl<R> UpdateBuilder<R> {
    pub fn consistency(mut self, consistency: Consistency) -> Self {
        self.builder = self.builder.consistency(consistency);
        self
    }

    pub fn timestamp(mut self, timestamp: i64) -> Self {
        self.builder = self.builder.timestamp(timestamp);
        self
    }

    pub fn build(self) -> anyhow::Result<UpdateRequest> {
        Ok(CommonRequest {
            token: self.token.unwrap_or_else(|| rand::random()),
            payload: self.builder.build()?.into(),
            keyspace: self.keyspace,
            statement: self.statement,
        }
        .into())
    }
}

impl UpdateBuilder<DynamicRequest> {
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

    fn statement(&self) -> &String {
        self.0.statement()
    }

    fn payload(&self) -> Vec<u8> {
        self.0.payload()
    }
    fn keyspace(&self) -> &Option<String> {
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

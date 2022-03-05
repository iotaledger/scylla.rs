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
pub trait Insert<K: Bindable + TokenEncoder>: Table {
    /// Create your insert statement here.
    fn statement(keyspace: &dyn Keyspace) -> InsertStatement;

    /// Bind the cql values to the builder
    fn bind_values<B: Binder>(binder: B, key: &K) -> Result<B, B::Error> {
        binder.bind(key)
    }
}

impl<T: Table + Bindable + TokenEncoder> Insert<T> for T {
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

/// Specifies helper functions for creating static insert requests from a keyspace with a `Delete<K, V>` definition
pub trait GetStaticInsertRequest<K: Bindable + TokenEncoder>: Table {
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
        K: Bindable + TokenEncoder,
    {
        let statement = Self::statement(keyspace);
        let keyspace = statement.get_keyspace();
        let statement = statement.to_string();
        let mut builder = QueryFrameBuilder::default()
            .consistency(Consistency::Quorum)
            .statement(statement.clone());
        builder = Self::bind_values(builder, key)?;
        Ok(InsertBuilder {
            token: Some(key.token()),
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
        K: Bindable + TokenEncoder,
    {
        let statement = Self::statement(keyspace);
        let keyspace = statement.get_keyspace();
        let statement = statement.to_string();
        let mut builder = ExecuteFrameBuilder::default()
            .consistency(Consistency::Quorum)
            .id(statement.id());
        builder = Self::bind_values(builder, key)?;
        Ok(InsertBuilder {
            token: Some(key.token()),
            builder,
            keyspace,
            statement,
            _marker: PhantomData,
        })
    }
}
impl<T: Table, K: Bindable + TokenEncoder> GetStaticInsertRequest<K> for T {}

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
            token: None,
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
            token: None,
            _marker: PhantomData,
        }
    }
}
#[derive(Debug)]
pub struct InsertBuilder<R, B> {
    keyspace: Option<String>,
    statement: String,
    builder: B,
    token: Option<i64>,
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

    pub fn build(self) -> anyhow::Result<InsertRequest> {
        Ok(CommonRequest {
            token: self.token.unwrap_or_else(|| rand::random()),
            payload: RequestFrame::from(self.builder.build()?).build_payload(),
            keyspace: self.keyspace,
            statement: self.statement,
        }
        .into())
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

    pub fn build(self) -> anyhow::Result<InsertRequest> {
        Ok(CommonRequest {
            token: self.token.unwrap_or_else(|| rand::random()),
            payload: RequestFrame::from(self.builder.build()?).build_payload(),
            keyspace: self.keyspace,
            statement: self.statement,
        }
        .into())
    }
}

impl<B: Binder> InsertBuilder<DynamicRequest, B> {
    pub fn token<V: TokenEncoder>(mut self, value: &V) -> Self {
        self.token.replace(value.token());
        self
    }

    pub fn bind<V: Bindable>(mut self, value: &V) -> Result<Self, B::Error> {
        self.builder = self.builder.bind(value)?;
        Ok(self)
    }

    pub fn bind_token<V: Bindable + TokenEncoder>(mut self, value: &V) -> Result<Self, B::Error> {
        self.token.replace(value.token());
        self.builder = self.builder.bind(value)?;
        Ok(self)
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

impl SendRequestExt for InsertRequest {
    type Marker = DecodeVoid;
    type Worker = BasicRetryWorker<Self>;
    const TYPE: RequestType = RequestType::Insert;

    fn worker(self) -> Box<Self::Worker> {
        BasicRetryWorker::new(self)
    }
}

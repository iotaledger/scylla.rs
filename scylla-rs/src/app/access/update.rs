// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use super::*;
use scylla_parse::UpdateStatement;

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
pub trait Update<S: Keyspace, K: TokenEncoder, V>: Table {
    /// Create your update statement here.
    fn statement(keyspace: &S) -> UpdateStatement;
    /// Get the MD5 hash of this implementation's statement
    /// for use when generating queries that should use
    /// the prepared statement.
    fn id(keyspace: &S) -> [u8; 16] {
        md5::compute(Self::statement(keyspace).to_string().as_bytes()).into()
    }
    /// Bind the cql values to the builder
    fn bind_values<B: Binder>(binder: &mut B, key: &K, values: &V) -> Result<(), B::Error>;
}

pub trait UpdateTable<T: Update<Self, K, V>, K: TokenEncoder, V>: Keyspace {}
impl<S: Keyspace, T: Update<Self, K, V>, K: TokenEncoder, V> UpdateTable<T, K, V> for S {}

/// Specifies helper functions for creating static update requests from a keyspace with a `Delete<K, V>` definition
pub trait GetStaticUpdateRequest<S: Keyspace, K, V>: Table {
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
    fn update(keyspace: &S, key: &K, values: &V) -> Result<UpdateBuilder, StaticQueryError<K>>
    where
        Self: Update<S, K, V>,
        K: TokenEncoder,
    {
        let statement = Self::statement(keyspace);
        let mut builder = QueryBuilder::default();
        builder
            .consistency(Consistency::Quorum)
            .statement(&statement.to_string());
        Self::bind_values(&mut builder, key, values)?;
        Ok(UpdateBuilder {
            token: Some(key.token().map_err(StaticQueryError::TokenEncodeError)?),
            builder,
            statement,
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
    fn update_prepared(keyspace: &S, key: &K, values: &V) -> Result<UpdateBuilder, StaticQueryError<K>>
    where
        Self: Update<S, K, V>,
        K: TokenEncoder,
    {
        let statement = Self::statement(keyspace);
        let mut builder = QueryBuilder::default();
        builder.consistency(Consistency::Quorum).id(&Self::id(keyspace));
        Self::bind_values(&mut builder, key, values)?;
        Ok(UpdateBuilder {
            token: Some(key.token().map_err(StaticQueryError::TokenEncodeError)?),
            builder,
            statement,
        })
    }
}

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
    fn query(self) -> UpdateBuilder;

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
    fn prepared(self) -> UpdateBuilder;
}

impl<T: Table, S: Keyspace, K, V> GetStaticUpdateRequest<S, K, V> for T {}
impl AsDynamicUpdateRequest for UpdateStatement {
    fn query(self) -> UpdateBuilder {
        let mut builder = QueryBuilder::default();
        builder.consistency(Consistency::Quorum).statement(&self.to_string());
        UpdateBuilder {
            builder,
            statement: self,
            token: None,
        }
    }

    fn prepared(self) -> UpdateBuilder {
        let mut builder = QueryBuilder::default();
        builder
            .consistency(Consistency::Quorum)
            .id(&md5::compute(self.to_string().as_bytes()).into());
        UpdateBuilder {
            builder,
            statement: self,
            token: None,
        }
    }
}

#[derive(Debug)]
pub struct UpdateBuilder {
    statement: UpdateStatement,
    builder: QueryBuilder,
    token: Option<i64>,
}

impl UpdateBuilder {
    pub fn consistency(&mut self, consistency: Consistency) -> &mut Self {
        self.builder.consistency(consistency);
        self
    }

    pub fn timestamp(&mut self, timestamp: i64) -> &mut Self {
        self.builder.timestamp(timestamp);
        self
    }

    pub fn token<V: TokenEncoder>(&mut self, value: &V) -> Result<&mut Self, V::Error> {
        self.token.replace(value.token()?);
        Ok(self)
    }

    pub fn bind<V: Bindable>(&mut self, value: &V) -> Result<&mut Self, <QueryBuilder as Binder>::Error> {
        self.builder.bind(value)?;
        Ok(self)
    }

    pub fn build(&self) -> anyhow::Result<UpdateRequest> {
        Ok(CommonRequest {
            token: self.token.unwrap_or_else(|| rand::random()),
            payload: self.builder.build()?.into(),
            statement: self.statement.clone().into(),
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

impl Request for UpdateRequest {
    fn token(&self) -> i64 {
        self.0.token()
    }

    fn statement(&self) -> Statement {
        self.0.statement()
    }

    fn payload(&self) -> Vec<u8> {
        self.0.payload()
    }
    fn keyspace(&self) -> Option<String> {
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

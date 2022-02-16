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
pub trait Delete<S: Keyspace, K: Bindable + TokenEncoder>: Table {
    /// Create your delete statement here.
    fn statement(keyspace: &S) -> DeleteStatement;

    /// Get the MD5 hash of this implementation's statement
    /// for use when generating queries that should use
    /// the prepared statement.
    fn id(keyspace: &S) -> [u8; 16] {
        md5::compute(Self::statement(keyspace).to_string().as_bytes()).into()
    }

    /// Bind the cql values to the builder
    fn bind_values<B: Binder>(binder: &mut B, key: &K) -> Result<(), B::Error> {
        binder.bind(key)?;
        Ok(())
    }
}

pub trait DeleteTable<T: Delete<Self, K>, K: Bindable + TokenEncoder>: Keyspace {}
impl<S: Keyspace, T: Delete<Self, K>, K: Bindable + TokenEncoder> DeleteTable<T, K> for S {}

/// Specifies helper functions for creating static delete requests from a keyspace with a `Delete<K, V>` definition
pub trait GetStaticDeleteRequest<S: Keyspace, K>: Table {
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
    fn delete(keyspace: &S, key: &K) -> Result<DeleteBuilder, StaticQueryError<K>>
    where
        Self: Delete<S, K>,
        K: Bindable + TokenEncoder,
    {
        let statement = Self::statement(keyspace);
        let mut builder = QueryBuilder::default();
        builder
            .consistency(Consistency::Quorum)
            .statement(&statement.to_string());
        Self::bind_values(&mut builder, key)?;
        Ok(DeleteBuilder {
            token: Some(key.token().map_err(StaticQueryError::TokenEncodeError)?),
            builder,
            statement,
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
    fn delete_prepared(keyspace: &S, key: &K) -> Result<DeleteBuilder, StaticQueryError<K>>
    where
        Self: Delete<S, K>,
        K: Bindable + TokenEncoder,
    {
        let statement = Self::statement(keyspace);
        let mut builder = QueryBuilder::default();
        builder.consistency(Consistency::Quorum).id(&Self::id(keyspace));
        Self::bind_values(&mut builder, key)?;
        Ok(DeleteBuilder {
            token: Some(key.token().map_err(StaticQueryError::TokenEncodeError)?),
            builder,
            statement,
        })
    }
}

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
    fn query(self) -> DeleteBuilder;

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
    fn prepared(self) -> DeleteBuilder;
}

impl<T: Table, S: Keyspace, K> GetStaticDeleteRequest<S, K> for T {}
impl AsDynamicDeleteRequest for DeleteStatement {
    fn query(self) -> DeleteBuilder {
        let mut builder = QueryBuilder::default();
        builder.consistency(Consistency::Quorum).statement(&self.to_string());
        DeleteBuilder {
            builder,
            statement: self,
            token: None,
        }
    }

    fn prepared(self) -> DeleteBuilder {
        let mut builder = QueryBuilder::default();
        builder
            .consistency(Consistency::Quorum)
            .id(&md5::compute(self.to_string().as_bytes()).into());
        DeleteBuilder {
            builder,
            statement: self,
            token: None,
        }
    }
}

pub struct DeleteBuilder {
    statement: DeleteStatement,
    builder: QueryBuilder,
    token: Option<i64>,
}

impl DeleteBuilder {
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

    pub fn build(&self) -> anyhow::Result<DeleteRequest> {
        Ok(CommonRequest {
            token: self.token.unwrap_or_else(|| rand::random()),
            payload: self.builder.build()?.into(),
            statement: self.statement.clone().into(),
        }
        .into())
    }
}

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

impl SendRequestExt for DeleteRequest {
    type Marker = DecodeVoid;
    type Worker = BasicRetryWorker<Self>;
    const TYPE: RequestType = RequestType::Delete;

    fn worker(self) -> Box<Self::Worker> {
        BasicRetryWorker::new(self)
    }
}

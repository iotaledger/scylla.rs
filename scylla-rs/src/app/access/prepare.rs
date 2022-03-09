// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use super::*;
use crate::prelude::{
    PrepareFrame,
    PrepareWorker,
};

/// Specifies helper functions for creating static prepare requests from a keyspace with any access trait definition

pub trait GetStaticPrepareRequest: Keyspace + Sized {
    /// Create a static prepare request from a keyspace with a `Select<K, V>` definition.
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
    /// MyKeyspace::new("my_keyspace")
    ///     .prepare_select::<MyKeyType, MyVarType, MyValueType>()
    ///     .get_local_blocking()?;
    /// # Ok::<(), anyhow::Error>(())
    /// ```
    fn prepare_select<T, K, O>(&self) -> PrepareRequest<SelectBuilder<DynamicRequest, O, ExecuteFrameBuilder>>
    where
        T: Select<K, O>,
        K: Bindable,
        O: RowsDecoder,
    {
        PrepareRequest::new(
            PrepareFrame::new(T::statement(self).to_string()),
            rand::random(),
            Some(self.name().to_string()),
        )
    }

    /// Create a static prepare request from a keyspace with a `Insert<K, V>` definition.
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
    /// # #[derive(Default)]
    /// struct MyValueType {
    ///     value1: f32,
    ///     value2: f32,
    /// }
    /// impl Insert<MyKeyType, MyValueType> for MyKeyspace {
    ///     type QueryOrPrepared = PreparedStatement;
    ///     fn statement(&self) -> InsertStatement {
    ///         parse_statement!("INSERT INTO my_table (key, val1, val2) VALUES (?,?,?)")
    ///     }
    ///
    ///     fn bind_values<B: Binder>(builder: B, key: &MyKeyType, value: &MyValueType) -> B {
    ///         builder.value(key).value(&value.value1).value(&value.value2)
    ///     }
    /// }
    /// MyKeyspace::new("my_keyspace")
    ///     .prepare_insert::<MyKeyType, MyValueType>()
    ///     .get_local_blocking()?;
    /// # Ok::<(), anyhow::Error>(())
    /// ```
    fn prepare_insert<T, K>(&self) -> PrepareRequest<InsertBuilder<DynamicRequest, ExecuteFrameBuilder>>
    where
        T: Insert<K>,
        K: Bindable,
    {
        PrepareRequest::new(
            PrepareFrame::new(T::statement(self).to_string()),
            rand::random(),
            Some(self.name().to_string()),
        )
    }

    /// Create a static prepare request from a keyspace with a `Update<K, V>` definition.
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
    ///             .bind(&value.value1)
    ///             .value(&value.value2)
    ///             .value(key)
    ///             .bind(variables)
    ///     }
    /// }
    /// MyKeyspace::new("my_keyspace")
    ///     .prepare_update::<MyKeyType, MyVarType, MyValueType>()
    ///     .get_local_blocking()?;
    /// # Ok::<(), anyhow::Error>(())
    /// ```
    fn prepare_update<T, K, V>(&self) -> PrepareRequest<UpdateBuilder<DynamicRequest, ExecuteFrameBuilder>>
    where
        T: Update<K, V>,
        K: Bindable,
    {
        PrepareRequest::new(
            PrepareFrame::new(T::statement(self).to_string()),
            rand::random(),
            Some(self.name().to_string()),
        )
    }

    /// Create a static prepare request from a keyspace with a `Delete<K, V>` definition.
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
    /// MyKeyspace::new("my_keyspace")
    ///     .prepare_delete::<MyKeyType, MyVarType, MyValueType>()
    ///     .get_local_blocking()?;
    /// # Ok::<(), anyhow::Error>(())
    /// ```
    fn prepare_delete<T, K>(&self) -> PrepareRequest<DeleteBuilder<DynamicRequest, ExecuteFrameBuilder>>
    where
        T: Delete<K>,
        K: Bindable,
    {
        PrepareRequest::new(
            PrepareFrame::new(T::statement(self).to_string()),
            rand::random(),
            Some(self.name().to_string()),
        )
    }
}

/// Specifies helper functions for creating dynamic prepare requests from anything that can be interpreted as a
/// statement

pub trait AsDynamicPrepareRequest<P>: KeyspaceExt + ToString {
    /// Create a dynamic prepare request from a statement.
    /// name.
    ///
    /// ## Example
    /// ```no_run
    /// use scylla_rs::app::access::*;
    /// parse_statement!("DELETE FROM my_keyspace.my_table WHERE key = ?")
    ///     .prepare()
    ///     .get_local_blocking()?;
    /// # Ok::<(), anyhow::Error>(())
    /// ```
    fn prepare(&self) -> PrepareRequest<P> {
        PrepareRequest::new(PrepareFrame::new(self.to_string()), rand::random(), self.get_keyspace())
    }
}

pub trait AsDynamicPrepareSelectRequest: KeyspaceExt + ToString {
    /// Create a dynamic prepare request from a statement.
    /// name.
    ///
    /// ## Example
    /// ```no_run
    /// use scylla_rs::app::access::*;
    /// parse_statement!("DELETE FROM my_keyspace.my_table WHERE key = ?")
    ///     .prepare()
    ///     .get_local_blocking()?;
    /// # Ok::<(), anyhow::Error>(())
    /// ```
    fn prepare<O: RowsDecoder>(&self) -> PrepareRequest<SelectBuilder<DynamicRequest, O, ExecuteFrameBuilder>> {
        PrepareRequest::new(PrepareFrame::new(self.to_string()), rand::random(), self.get_keyspace())
    }
}

impl<S: Keyspace> GetStaticPrepareRequest for S {}
impl AsDynamicPrepareSelectRequest for SelectStatement {}
impl AsDynamicPrepareRequest<InsertBuilder<DynamicRequest, ExecuteFrameBuilder>> for InsertStatement {}
impl AsDynamicPrepareRequest<UpdateBuilder<DynamicRequest, ExecuteFrameBuilder>> for UpdateStatement {}
impl AsDynamicPrepareRequest<DeleteBuilder<DynamicRequest, ExecuteFrameBuilder>> for DeleteStatement {}

/// A request to prepare a record which can be sent to the ring
pub struct PrepareRequest<P> {
    frame: PrepareFrame,
    keyspace: Option<String>,
    _marker: PhantomData<fn(P) -> P>,
}

impl<P> Debug for PrepareRequest<P> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PrepareRequest")
            .field("frame", &self.frame)
            .field("keyspace", &self.keyspace)
            .finish()
    }
}

impl<P> Clone for PrepareRequest<P> {
    fn clone(&self) -> Self {
        Self {
            frame: self.frame.clone(),
            keyspace: self.keyspace.clone(),
            _marker: PhantomData,
        }
    }
}

impl<P> PrepareRequest<P> {
    pub fn new(frame: PrepareFrame, token: i64, keyspace: Option<String>) -> Self {
        Self {
            frame,
            keyspace,
            _marker: PhantomData,
        }
    }
}

impl<P> RequestFrameExt for PrepareRequest<P> {
    type Frame = PrepareFrame;

    fn frame(&self) -> &Self::Frame {
        &self.frame
    }

    fn into_frame(self) -> RequestFrame {
        self.frame.into()
    }
}

impl<P> ShardAwareExt for PrepareRequest<P> {
    fn token(&self) -> i64 {
        rand::random()
    }

    fn keyspace(&self) -> Option<&String> {
        self.keyspace.as_ref()
    }
}

impl<P> Deref for PrepareRequest<P> {
    type Target = PrepareFrame;

    fn deref(&self) -> &Self::Target {
        &self.frame
    }
}

impl<P> DerefMut for PrepareRequest<P> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.frame
    }
}

impl<P> SendRequestExt for PrepareRequest<P>
where
    P: 'static + From<PreparedQuery> + Debug + Send + Sync,
{
    type Worker = PrepareWorker<P>;
    type Marker = DecodePrepared<P>;
    const TYPE: RequestType = RequestType::Prepare;

    fn marker(&self) -> Self::Marker {
        DecodePrepared::new(self.keyspace.clone(), self.frame.statement.clone())
    }

    fn event(self) -> (Self::Worker, RequestFrame) {
        (PrepareWorker::new(self.clone()), self.into_frame())
    }

    fn worker(self) -> Self::Worker {
        PrepareWorker::new(self)
    }
}

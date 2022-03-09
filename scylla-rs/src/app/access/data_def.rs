// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use super::*;

/// Specifies helper functions for creating dynamic requests from anything that can be interpreted as a statement
pub trait AsDynamicDataDefRequest: Into<DataDefinitionStatement>
where
    Self: Sized,
{
    /// Create a dynamic request from a statement and variables. Can be specified as either
    /// a query or prepared statement.
    ///
    /// ## Example
    /// ```no_run
    /// use scylla_rs::app::access::*;
    /// parse_statement!(
    ///     "CREATE KEYSPACE IF NOT EXISTS my_keyspace
    ///     WITH replication = {'class': 'NetworkTopologyStrategy', 'datacenter1': 1}
    ///     AND durable_writes = true"
    /// )
    /// .execute()
    /// .consistency(Consistency::All)
    /// .build()?
    /// .get_local_blocking()?;
    /// # Ok::<(), anyhow::Error>(())
    /// ```
    fn define(self) -> DataDefBuilder {
        let statement = self.into();
        let keyspace = statement.get_keyspace();
        let statement = statement.to_string();
        DataDefBuilder {
            builder: QueryFrameBuilder::default()
                .statement(statement)
                .consistency(Consistency::Quorum),
            keyspace,
        }
    }
}
impl<T: Into<DataDefinitionStatement>> AsDynamicDataDefRequest for T {}

/// A builder for creating data definition (DDL) requests
pub struct DataDefBuilder {
    keyspace: Option<String>,
    builder: QueryFrameBuilder,
}

impl DataDefBuilder {
    pub fn consistency(mut self, consistency: Consistency) -> Self {
        self.builder = self.builder.consistency(consistency);
        self
    }

    pub fn timestamp(mut self, timestamp: i64) -> Self {
        self.builder = self.builder.timestamp(timestamp);
        self
    }

    pub fn bind<V: Bindable>(mut self, value: &V) -> Result<Self, <QueryFrameBuilder as Binder>::Error> {
        self.builder = self.builder.bind(value)?;
        Ok(self)
    }

    pub fn build(self) -> anyhow::Result<DataDefRequest> {
        let frame = self.builder.build()?;
        Ok(DataDefRequest::new(frame.into(), self.keyspace))
    }
}

/// A request to execute a record which can be sent to the ring
#[derive(Debug, Clone)]
pub struct DataDefRequest {
    frame: QueryFrame,
    keyspace: Option<String>,
}

impl DataDefRequest {
    pub fn new(frame: QueryFrame, keyspace: Option<String>) -> Self {
        Self { frame, keyspace }
    }
}

impl RequestFrameExt for DataDefRequest {
    type Frame = QueryFrame;

    fn frame(&self) -> &Self::Frame {
        &self.frame
    }

    fn into_frame(self) -> RequestFrame {
        self.frame.into()
    }
}

impl ShardAwareExt for DataDefRequest {
    fn token(&self) -> i64 {
        rand::random()
    }

    fn keyspace(&self) -> Option<&String> {
        self.keyspace.as_ref()
    }
}

impl SendRequestExt for DataDefRequest {
    type Worker = BasicRetryWorker<Self>;
    type Marker = DecodeVoid;
    const TYPE: RequestType = RequestType::DataDef;

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

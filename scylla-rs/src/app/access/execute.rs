// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use super::*;

pub trait KeyspaceExecuteExt: KeyspaceExt + Into<Statement> {}
impl<T> KeyspaceExecuteExt for T where T: KeyspaceExt + Into<Statement> {}

/// Specifies helper functions for creating dynamic requests from anything that can be interpreted as a statement
pub trait AsDynamicExecuteRequest: Into<Statement>
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
    fn execute(self) -> ExecuteBuilder {
        let statement = self.into();
        let keyspace = statement.get_keyspace();
        let statement = statement.to_string();
        ExecuteBuilder {
            builder: QueryFrameBuilder::default()
                .statement(statement)
                .consistency(Consistency::Quorum),
            keyspace,
        }
    }
}
impl<T: Into<Statement>> AsDynamicExecuteRequest for T {}

pub struct ExecuteBuilder {
    keyspace: Option<String>,
    builder: QueryFrameBuilder,
}

impl ExecuteBuilder {
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

    pub fn build(self) -> anyhow::Result<ExecuteRequest> {
        let frame = self.builder.build()?;
        Ok(CommonRequest {
            token: rand::random(),
            statement: frame.statement().clone(),
            payload: RequestFrame::from(frame).build_payload(),
            keyspace: self.keyspace,
        }
        .into())
    }
}

/// A request to execute a record which can be sent to the ring
#[derive(Debug, Clone)]
pub struct ExecuteRequest(CommonRequest);

impl From<CommonRequest> for ExecuteRequest {
    fn from(req: CommonRequest) -> Self {
        ExecuteRequest(req)
    }
}

impl Request for ExecuteRequest {
    fn token(&self) -> i64 {
        self.0.token
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

impl SendRequestExt for ExecuteRequest {
    type Marker = DecodeVoid;
    type Worker = BasicRetryWorker<Self>;
    const TYPE: RequestType = RequestType::Execute;

    fn worker(self) -> Box<Self::Worker> {
        BasicRetryWorker::new(self)
    }

    fn marker(&self) -> Self::Marker {
        DecodeVoid
    }
}

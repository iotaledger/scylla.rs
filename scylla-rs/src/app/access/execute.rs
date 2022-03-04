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
    fn execute<'a>(self) -> ExecuteBuilder<'a, [&'a dyn BindableValue<QueryBuilder<QueryValues>>], QueryConsistency> {
        let statement = self.into();
        ExecuteBuilder {
            builder: QueryStatement::encode_statement(Query::new(), &statement.to_string()),
            statement,
            variables: &[],
        }
    }

    /// Create a dynamic request from a statement and variables. Can be specified as either
    /// a query or prepared statement.
    ///
    /// ## Example
    /// ```no_run
    /// use scylla_rs::app::access::*;
    /// parse_statement!(
    ///     "CREATE OR REPLACE AGGREGATE test.average(int)
    ///     SFUNC averageState
    ///     STYPE tuple<int,bigint>
    ///     FINALFUNC averageFinal
    ///     INITCOND (?, ?);"
    /// )
    /// .execute_with_vars(&[&0, &0])
    /// .consistency(Consistency::All)
    /// .build()?
    /// .get_local_blocking()?;
    /// # Ok::<(), anyhow::Error>(())
    /// ```
    fn execute_with_vars<'a>(
        self,
        variables: &'a [&'a dyn BindableValue<QueryBuilder<QueryValues>>],
    ) -> ExecuteBuilder<'a, [&'a dyn BindableValue<QueryBuilder<QueryValues>>], QueryConsistency> {
        let statement = self.into();
        ExecuteBuilder {
            builder: QueryStatement::encode_statement(Query::new(), &statement.to_string()),
            statement,
            variables,
        }
    }
}
impl<T: Into<Statement>> AsDynamicExecuteRequest for T {}

pub struct ExecuteBuilder<'a, V: ?Sized, Stage> {
    pub(crate) statement: Statement,
    pub(crate) variables: &'a V,
    pub(crate) builder: QueryBuilder<Stage>,
}

impl<'a> ExecuteBuilder<'a, [&'a dyn BindableValue<QueryBuilder<QueryValues>>], QueryConsistency> {
    pub fn consistency(
        self,
        consistency: Consistency,
    ) -> ExecuteBuilder<'a, [&'a dyn BindableValue<QueryBuilder<QueryValues>>], QueryValues> {
        let builder = self.builder.consistency(consistency).bind_values().bind(self.variables);
        ExecuteBuilder {
            statement: self.statement,
            variables: self.variables,
            builder,
        }
    }

    pub fn timestamp(
        self,
        timestamp: i64,
    ) -> ExecuteBuilder<'a, [&'a dyn BindableValue<QueryBuilder<QueryValues>>], QueryBuild> {
        ExecuteBuilder {
            statement: self.statement,
            variables: self.variables,
            builder: self
                .builder
                .consistency(Consistency::Quorum)
                .bind_values()
                .bind(self.variables)
                .timestamp(timestamp),
        }
    }

    pub fn build(self) -> anyhow::Result<ExecuteRequest> {
        let query = self
            .builder
            .consistency(Consistency::Quorum)
            .bind_values()
            .bind(self.variables)
            .build()?;
        // create the request
        Ok(ExecuteRequest {
            token: rand::random(),
            payload: query.into(),
            statement: self.statement,
        })
    }
}

impl<'a> ExecuteBuilder<'a, [&'a dyn BindableValue<QueryBuilder<QueryValues>>], QueryValues> {
    pub fn timestamp(
        self,
        timestamp: i64,
    ) -> ExecuteBuilder<'a, [&'a dyn BindableValue<QueryBuilder<QueryValues>>], QueryBuild> {
        ExecuteBuilder {
            statement: self.statement,
            variables: self.variables,
            builder: self.builder.timestamp(timestamp),
        }
    }

    pub fn build(self) -> anyhow::Result<ExecuteRequest> {
        let query = self.builder.build()?;
        // create the request
        Ok(ExecuteRequest {
            token: rand::random(),
            payload: query.into(),
            statement: self.statement,
        })
    }
}

impl<'a, V: ?Sized> ExecuteBuilder<'a, V, QueryBuild> {
    pub fn build(self) -> anyhow::Result<ExecuteRequest> {
        let query = self.builder.build()?;
        // create the request
        Ok(ExecuteRequest {
            token: rand::random(),
            payload: query.into(),
            statement: self.statement,
        })
    }
}

/// A request to execute a record which can be sent to the ring
#[derive(Debug, Clone)]
pub struct ExecuteRequest {
    pub(crate) token: i64,
    pub(crate) payload: Vec<u8>,
    pub(crate) statement: Statement,
}

impl From<CommonRequest> for ExecuteRequest {
    fn from(req: CommonRequest) -> Self {
        ExecuteRequest {
            token: req.token,
            payload: req.payload,
            statement: req.statement.into(),
        }
    }
}

impl TryFrom<ExecuteRequest> for CommonRequest {
    type Error = <scylla_parse::Statement as std::convert::TryInto<scylla_parse::DataManipulationStatement>>::Error;
    fn try_from(req: ExecuteRequest) -> Result<Self, Self::Error> {
        Ok(CommonRequest {
            token: req.token,
            payload: req.payload,
            statement: req.statement.try_into()?,
        })
    }
}

impl Request for ExecuteRequest {
    fn token(&self) -> i64 {
        self.token
    }

    fn statement(&self) -> Statement {
        self.statement.clone().into()
    }

    fn payload(&self) -> Vec<u8> {
        self.payload.clone()
    }

    fn keyspace(&self) -> Option<String> {
        self.statement.get_keyspace()
    }
}

impl SendRequestExt for ExecuteRequest {
    type Marker = DecodeVoid;
    type Worker = BasicRetryWorker<Self>;
    const TYPE: RequestType = RequestType::Execute;

    fn worker(self) -> Box<Self::Worker> {
        BasicRetryWorker::new(self)
    }
}

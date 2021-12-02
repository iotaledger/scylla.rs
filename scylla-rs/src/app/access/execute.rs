// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use super::*;

pub trait KeyspaceExecuteExt: KeyspaceExt + Into<Statement> {}
impl<T> KeyspaceExecuteExt for T where T: KeyspaceExt + Into<Statement> {}

/// Specifies helper functions for creating dynamic requests
pub trait GetDynamicExecuteRequest: Keyspace {
    /// Create a dynamic request from a statement and variables. Can be specified as either
    /// a query or prepared statement. The token `{{keyspace}}` will be replaced with the keyspace name.
    ///
    /// ## Example
    /// ```no_run
    /// use scylla_rs::app::access::*;
    /// "my_keyspace"
    ///     .execute(
    ///         "CREATE TABLE IF NOT EXISTS {{keyspace}}.test (
    ///             key text PRIMARY KEY,
    ///             data blob,
    ///         )",
    ///         &[],
    ///         StatementType::Query,
    ///     )
    ///     .consistency(Consistency::All)
    ///     .build()?
    ///     .get_local_blocking()?;
    /// # Ok::<(), anyhow::Error>(())
    /// ```
    fn execute<'a, S: KeyspaceExecuteExt>(
        &self,
        statement: S,
        variables: &'a [&'a dyn BindableValue<QueryBuilder<QueryValues>>],
        statement_type: StatementType,
    ) -> ExecuteBuilder<'a, [&'a dyn BindableValue<QueryBuilder<QueryValues>>], QueryConsistency> {
        match statement_type {
            StatementType::Query => self.execute_query(statement, variables),
            StatementType::Prepared => self.execute_prepared(statement, variables),
        }
    }

    /// Create a dynamic query request from a statement and variables.
    /// The token `{{keyspace}}` will be replaced with the keyspace name.
    ///
    /// ## Example
    /// ```no_run
    /// use scylla_rs::app::access::*;
    /// "my_keyspace"
    ///     .execute_query(
    ///         "CREATE TABLE IF NOT EXISTS {{keyspace}}.test (
    ///             key text PRIMARY KEY,
    ///             data blob,
    ///         )",
    ///         &[],
    ///     )
    ///     .consistency(Consistency::All)
    ///     .build()?
    ///     .get_local_blocking()?;
    /// # Ok::<(), anyhow::Error>(())
    /// ```
    fn execute_query<'a, S: KeyspaceExecuteExt>(
        &self,
        statement: S,
        variables: &'a [&'a dyn BindableValue<QueryBuilder<QueryValues>>],
    ) -> ExecuteBuilder<'a, [&'a dyn BindableValue<QueryBuilder<QueryValues>>], QueryConsistency> {
        let statement = statement.with_keyspace(self.name()).into();
        ExecuteBuilder {
            builder: QueryStatement::encode_statement(Query::new(), &statement.to_string()),
            statement,
            variables,
        }
    }

    /// Create a dynamic prepared request from a statement and variables.
    /// The token `{{keyspace}}` will be replaced with the keyspace name.
    ///
    /// ## Example
    /// ```no_run
    /// use scylla_rs::app::access::*;
    /// "my_keyspace"
    ///     .execute_prepared(
    ///         "CREATE TABLE IF NOT EXISTS {{keyspace}}.test (
    ///             key text PRIMARY KEY,
    ///             data blob,
    ///         )",
    ///         &[],
    ///     )
    ///     .consistency(Consistency::All)
    ///     .build()?
    ///     .get_local_blocking()?;
    /// # Ok::<(), anyhow::Error>(())
    /// ```
    fn execute_prepared<'a, S: KeyspaceExecuteExt>(
        &self,
        statement: S,
        variables: &'a [&'a dyn BindableValue<QueryBuilder<QueryValues>>],
    ) -> ExecuteBuilder<'a, [&'a dyn BindableValue<QueryBuilder<QueryValues>>], QueryConsistency> {
        let statement = statement.with_keyspace(self.name()).into();
        ExecuteBuilder {
            builder: PreparedStatement::encode_statement(Query::new(), &statement.to_string()),
            statement,
            variables,
        }
    }
}

/// Specifies helper functions for creating dynamic requests from anything that can be interpreted as a statement
pub trait AsDynamicExecuteRequest
where
    Self: Sized,
{
    /// Create a dynamic request from a statement and variables. Can be specified as either
    /// a query or prepared statement.
    ///
    /// ## Example
    /// ```no_run
    /// use scylla_rs::app::access::*;
    /// "CREATE KEYSPACE IF NOT EXISTS my_keyspace
    /// WITH replication = {'class': 'NetworkTopologyStrategy', 'datacenter1': 1}
    /// AND durable_writes = true"
    ///     .as_execute(&[], StatementType::Query)
    ///     .consistency(Consistency::All)
    ///     .build()?
    ///     .get_local_blocking()?;
    /// # Ok::<(), anyhow::Error>(())
    /// ```
    fn as_execute<'a>(
        self,
        variables: &'a [&'a dyn BindableValue<QueryBuilder<QueryValues>>],
        statement_type: StatementType,
    ) -> ExecuteBuilder<'a, [&'a dyn BindableValue<QueryBuilder<QueryValues>>], QueryConsistency> {
        match statement_type {
            StatementType::Query => self.as_execute_query(variables),
            StatementType::Prepared => self.as_execute_prepared(variables),
        }
    }

    /// Create a dynamic query request from a statement and variables.
    ///
    /// ## Example
    /// ```no_run
    /// use scylla_rs::app::access::*;
    /// "CREATE KEYSPACE IF NOT EXISTS my_keyspace
    /// WITH replication = {'class': 'NetworkTopologyStrategy', 'datacenter1': 1}
    /// AND durable_writes = true"
    ///     .as_execute_query(&[])
    ///     .consistency(Consistency::All)
    ///     .build()?
    ///     .get_local_blocking()?;
    /// # Ok::<(), anyhow::Error>(())
    /// ```
    fn as_execute_query<'a>(
        self,
        variables: &'a [&'a dyn BindableValue<QueryBuilder<QueryValues>>],
    ) -> ExecuteBuilder<'a, [&'a dyn BindableValue<QueryBuilder<QueryValues>>], QueryConsistency>;

    /// Create a dynamic query request from a statement and variables.
    ///
    /// ## Example
    /// ```no_run
    /// use scylla_rs::app::access::*;
    /// "CREATE KEYSPACE IF NOT EXISTS my_keyspace
    /// WITH replication = {'class': 'NetworkTopologyStrategy', 'datacenter1': 1}
    /// AND durable_writes = true"
    ///     .as_execute_prepared(&[])
    ///     .consistency(Consistency::All)
    ///     .build()?
    ///     .get_local_blocking()?;
    /// # Ok::<(), anyhow::Error>(())
    /// ```
    fn as_execute_prepared<'a>(
        self,
        variables: &'a [&'a dyn BindableValue<QueryBuilder<QueryValues>>],
    ) -> ExecuteBuilder<'a, [&'a dyn BindableValue<QueryBuilder<QueryValues>>], QueryConsistency>;
}

impl<S: Keyspace> GetDynamicExecuteRequest for S {}
impl<T: Into<Statement>> AsDynamicExecuteRequest for T {
    fn as_execute_query<'a>(
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

    fn as_execute_prepared<'a>(
        self,
        variables: &'a [&'a dyn BindableValue<QueryBuilder<QueryValues>>],
    ) -> ExecuteBuilder<'a, [&'a dyn BindableValue<QueryBuilder<QueryValues>>], QueryConsistency> {
        let statement = self.into();
        ExecuteBuilder {
            builder: PreparedStatement::encode_statement(Query::new(), &statement.to_string()),
            statement,
            variables,
        }
    }
}

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

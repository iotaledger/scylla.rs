// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use super::*;

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
    fn execute<'a>(
        &self,
        statement: &str,
        variables: &'a [&'a (dyn ColumnEncoder + Sync)],
        statement_type: StatementType,
    ) -> ExecuteBuilder<'a, [&'a (dyn ColumnEncoder + Sync)], QueryConsistency> {
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
    fn execute_query<'a>(
        &self,
        statement: &str,
        variables: &'a [&'a (dyn ColumnEncoder + Sync)],
    ) -> ExecuteBuilder<'a, [&'a (dyn ColumnEncoder + Sync)], QueryConsistency> {
        ExecuteBuilder {
            statement: statement.to_owned().into(),
            variables,
            builder: QueryStatement::encode_statement(Query::new(), &self.replace_keyspace_token(statement)),
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
    fn execute_prepared<'a>(
        &self,
        statement: &str,
        variables: &'a [&'a (dyn ColumnEncoder + Sync)],
    ) -> ExecuteBuilder<'a, [&'a (dyn ColumnEncoder + Sync)], QueryConsistency> {
        ExecuteBuilder {
            statement: statement.to_owned().into(),
            variables,
            builder: PreparedStatement::encode_statement(Query::new(), &self.replace_keyspace_token(statement)),
        }
    }
}

/// Specifies helper functions for creating dynamic requests from anything that can be interpreted as a statement
pub trait AsDynamicExecuteRequest: ToStatement
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
        &self,
        variables: &'a [&'a (dyn ColumnEncoder + Sync)],
        statement_type: StatementType,
    ) -> ExecuteBuilder<'a, [&'a (dyn ColumnEncoder + Sync)], QueryConsistency> {
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
        &self,
        variables: &'a [&'a (dyn ColumnEncoder + Sync)],
    ) -> ExecuteBuilder<'a, [&'a (dyn ColumnEncoder + Sync)], QueryConsistency> {
        let statement = self.to_statement();
        ExecuteBuilder {
            builder: QueryStatement::encode_statement(Query::new(), &statement),
            statement,
            variables,
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
    ///     .as_execute_prepared(&[])
    ///     .consistency(Consistency::All)
    ///     .build()?
    ///     .get_local_blocking()?;
    /// # Ok::<(), anyhow::Error>(())
    /// ```
    fn as_execute_prepared<'a>(
        &self,
        variables: &'a [&'a (dyn ColumnEncoder + Sync)],
    ) -> ExecuteBuilder<'a, [&'a (dyn ColumnEncoder + Sync)], QueryConsistency> {
        let statement = self.to_statement();
        ExecuteBuilder {
            builder: PreparedStatement::encode_statement(Query::new(), &statement),
            statement,
            variables,
        }
    }
}

impl<S: Keyspace> GetDynamicExecuteRequest for S {}
impl<S: ToStatement> AsDynamicExecuteRequest for S {}

pub struct ExecuteBuilder<'a, V: ?Sized, Stage> {
    pub(crate) statement: Cow<'static, str>,
    pub(crate) variables: &'a V,
    pub(crate) builder: QueryBuilder<Stage>,
}

impl<'a> ExecuteBuilder<'a, [&'a (dyn ColumnEncoder + Sync)], QueryConsistency> {
    pub fn consistency(
        self,
        consistency: Consistency,
    ) -> ExecuteBuilder<'a, [&'a (dyn ColumnEncoder + Sync)], QueryValues> {
        let builder = self.builder.consistency(consistency).bind(self.variables);
        ExecuteBuilder {
            statement: self.statement,
            variables: self.variables,
            builder,
        }
    }
}

impl<'a> ExecuteBuilder<'a, [&'a (dyn ColumnEncoder + Sync)], QueryValues> {
    pub fn timestamp(self, timestamp: i64) -> ExecuteBuilder<'a, [&'a (dyn ColumnEncoder + Sync)], QueryBuild> {
        ExecuteBuilder {
            statement: self.statement,
            variables: self.variables,
            builder: self.builder.timestamp(timestamp),
        }
    }
    /// Build the ExecuteRequest
    pub fn build(self) -> anyhow::Result<ExecuteRequest> {
        let query = self.builder.build()?;
        // create the request
        Ok(CommonRequest {
            token: rand::random(),
            payload: query.into(),
            statement: self.statement,
        }
        .into())
    }
}

impl<'a, V: ?Sized> ExecuteBuilder<'a, V, QueryBuild> {
    /// Build the ExecuteRequest
    pub fn build(self) -> anyhow::Result<ExecuteRequest> {
        let query = self.builder.build()?;
        // create the request
        Ok(CommonRequest {
            token: rand::random(),
            payload: query.into(),
            statement: self.statement,
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

impl Deref for ExecuteRequest {
    type Target = CommonRequest;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for ExecuteRequest {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl Request for ExecuteRequest {
    fn token(&self) -> i64 {
        self.0.token()
    }

    fn statement(&self) -> &Cow<'static, str> {
        self.0.statement()
    }

    fn payload(&self) -> Vec<u8> {
        self.0.payload()
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

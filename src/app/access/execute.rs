// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use super::*;

pub trait GetDynamicExecuteRequest: Keyspace {
    /// Specifies the returned Value type for an upcoming execute request
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
    /// Specifies the returned Value type for an upcoming execute request using a query statement
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
    /// Specifies the returned Value type for an upcoming execute request using a prepared statement id
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

pub trait AsDynamicExecuteRequest: Statement
where
    Self: Sized,
{
    /// Specifies the returned Value type for an upcoming execute request
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
    /// Specifies the returned Value type for an upcoming execute request using a query statement
    fn as_execute_query<'a>(
        &self,
        variables: &'a [&'a (dyn ColumnEncoder + Sync)],
    ) -> ExecuteBuilder<'a, [&'a (dyn ColumnEncoder + Sync)], QueryConsistency> {
        ExecuteBuilder {
            statement: self.to_string().to_owned().into(),
            variables,
            builder: QueryStatement::encode_statement(Query::new(), &self.to_string()),
        }
    }
    /// Specifies the returned Value type for an upcoming execute request using a prepared statement id
    fn as_execute_prepared<'a>(
        &self,
        variables: &'a [&'a (dyn ColumnEncoder + Sync)],
    ) -> ExecuteBuilder<'a, [&'a (dyn ColumnEncoder + Sync)], QueryConsistency> {
        ExecuteBuilder {
            statement: self.to_string().to_owned().into(),
            variables,
            builder: PreparedStatement::encode_statement(Query::new(), &self.to_string()),
        }
    }
}

impl<S: Keyspace> GetDynamicExecuteRequest for S {}
impl<S: Statement> AsDynamicExecuteRequest for S {}
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
        let builder = self.builder.consistency(consistency).values(self.variables);
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

impl ExecuteRequest {
    pub fn worker(self) -> Box<BasicRetryWorker<Self>> {
        BasicRetryWorker::new(self)
    }
}

impl Request for ExecuteRequest {
    fn token(&self) -> i64 {
        self.0.token()
    }

    fn statement(&self) -> &Cow<'static, str> {
        self.0.statement()
    }

    fn payload(&self) -> &Vec<u8> {
        self.0.payload()
    }

    fn payload_mut(&mut self) -> &mut Vec<u8> {
        self.0.payload_mut()
    }

    fn into_payload(self) -> Vec<u8> {
        self.0.into_payload()
    }
}

impl SendRequestExt for ExecuteRequest {
    type Marker = DecodeVoid;
    const TYPE: RequestType = RequestType::Execute;
}

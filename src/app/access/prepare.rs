// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use super::*;
use crate::{
    cql::prepare::PrepareBuild,
    prelude::Prepare,
};

pub trait GetStaticPrepareRequest: Keyspace {
    fn prepare_select<K, V>(&self) -> PrepareBuilder
    where
        Self: Select<K, V>,
    {
        let statement = self.select_statement();
        PrepareBuilder {
            builder: Prepare::new().statement(&statement),
            statement,
        }
    }

    fn prepare_insert<K, V>(&self) -> PrepareBuilder
    where
        Self: Insert<K, V>,
    {
        let statement = self.insert_statement();
        PrepareBuilder {
            builder: Prepare::new().statement(&statement),
            statement,
        }
    }

    fn prepare_update<K, V>(&self) -> PrepareBuilder
    where
        Self: Update<K, V>,
    {
        let statement = self.update_statement();
        PrepareBuilder {
            builder: Prepare::new().statement(&statement),
            statement,
        }
    }

    fn prepare_delete<K, V>(&self) -> PrepareBuilder
    where
        Self: Delete<K, V>,
    {
        let statement = self.delete_statement();
        PrepareBuilder {
            builder: Prepare::new().statement(&statement),
            statement,
        }
    }
}

pub trait GetDynamicPrepareRequest: Keyspace {
    /// Specifies the returned Value type for an upcoming prepare request
    fn prepare_with(&self, statement: &str) -> PrepareBuilder {
        PrepareBuilder {
            builder: Prepare::new().statement(statement),
            statement: statement.to_string().into(),
        }
    }
}

impl<S: Keyspace> GetStaticPrepareRequest for S {}
impl<S: Keyspace> GetDynamicPrepareRequest for S {}

pub struct PrepareBuilder {
    builder: crate::cql::prepare::PrepareBuilder<PrepareBuild>,
    statement: Cow<'static, str>,
}

impl PrepareBuilder {
    pub fn new(statement: &str) -> Self {
        PrepareBuilder {
            builder: Prepare::new().statement(statement),
            statement: statement.to_string().into(),
        }
    }

    pub fn build(self) -> anyhow::Result<PrepareRequest> {
        Ok(PrepareRequest {
            inner: self.builder.build()?,
            statement: self.statement,
            token: rand::random(),
        })
    }
}

/// A request to prepare a record which can be sent to the ring
#[derive(Debug, Clone)]
pub struct PrepareRequest {
    inner: Prepare,
    statement: Cow<'static, str>,
    token: i64,
}

impl Deref for PrepareRequest {
    type Target = Prepare;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl DerefMut for PrepareRequest {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.inner
    }
}

impl PrepareRequest {
    pub fn worker(self) -> Box<BasicRetryWorker<Self>> {
        BasicRetryWorker::new(self)
    }
}

impl Request for PrepareRequest {
    fn token(&self) -> i64 {
        self.token
    }

    fn statement(&self) -> &Cow<'static, str> {
        &self.statement
    }

    fn payload(&self) -> &Vec<u8> {
        &self.inner.0
    }

    fn payload_mut(&mut self) -> &mut Vec<u8> {
        &mut self.inner.0
    }

    fn into_payload(self) -> Vec<u8> {
        self.inner.0
    }
}

impl SendRequestExt for PrepareRequest {
    type Marker = DecodeVoid;
    const TYPE: RequestType = RequestType::Execute;
}

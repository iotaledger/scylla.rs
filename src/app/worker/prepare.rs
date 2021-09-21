// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use super::*;
use std::fmt::Debug;

/// A statement prepare worker
#[derive(Debug)]
pub struct PrepareWorker {
    /// The expected id for this statement
    pub id: [u8; 16],
    /// The statement to prepare
    pub statement: String,
}
impl PrepareWorker {
    /// Create a new prepare worker
    pub fn new<T: ToString>(id: [u8; 16], statement: T) -> Self {
        Self {
            id,
            statement: statement.to_string(),
        }
    }
    /// Create a new boxed prepare worker
    pub fn boxed<T: ToString>(id: [u8; 16], statement: T) -> Box<Self> {
        Box::new(Self::new(id, statement))
    }
    /// Create a prepare worker for an insert statement given a keyspace with the
    /// appropriate trait definition
    pub fn insert<S, K, V>(keyspace: &S) -> Self
    where
        S: Insert<K, V>,
    {
        Self {
            id: keyspace.id(),
            statement: keyspace.statement().to_string(),
        }
    }
    /// Create a prepare worker for a select statement given a keyspace with the
    /// appropriate trait definition
    pub fn select<S, K, V>(keyspace: &S) -> Self
    where
        S: Select<K, V>,
    {
        Self {
            id: keyspace.id(),
            statement: keyspace.statement().to_string(),
        }
    }
    /// Create a prepare worker for an update statement given a keyspace with the
    /// appropriate trait definition
    pub fn update<S, K, V>(keyspace: &S) -> Self
    where
        S: Update<K, V>,
    {
        Self {
            id: keyspace.id(),
            statement: keyspace.statement().to_string(),
        }
    }
    /// Create a prepare worker for a delete statement given a keyspace with the
    /// appropriate trait definition
    pub fn delete<S, K, V>(keyspace: &S) -> Self
    where
        S: Delete<K, V>,
    {
        Self {
            id: keyspace.id(),
            statement: keyspace.statement().to_string(),
        }
    }
}
impl Worker for PrepareWorker {
    fn handle_response(self: Box<Self>, _giveload: Vec<u8>) -> anyhow::Result<()> {
        info!("Successfully prepared statement: '{}'", self.statement);
        Ok(())
    }
    fn handle_error(self: Box<Self>, error: WorkerError, _reporter: &ReporterHandle) -> anyhow::Result<()> {
        error!("Failed to prepare statement: {}, error: {}", self.statement, error);
        Ok(())
    }
}

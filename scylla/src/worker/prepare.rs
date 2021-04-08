// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use super::*;

pub struct PrepareWorker {
    pub id: [u8; 16],
    pub statement: String,
}
impl PrepareWorker {
    pub fn new<T: ToString>(id: [u8; 16], statement: T) -> Self {
        Self {
            id,
            statement: statement.to_string(),
        }
    }
    pub fn boxed<T: ToString>(id: [u8; 16], statement: T) -> Box<Self> {
        Box::new(Self::new(id, statement))
    }
    pub fn insert<S, K, V>(keyspace: &S) -> Self
    where
        S: Insert<K, V>,
    {
        Self {
            id: keyspace.id(),
            statement: keyspace.statement().to_string(),
        }
    }
    pub fn select<S, K, V>(keyspace: &S) -> Self
    where
        S: Select<K, V>,
    {
        Self {
            id: keyspace.id(),
            statement: keyspace.statement().to_string(),
        }
    }
    pub fn update<S, K, V>(keyspace: &S) -> Self
    where
        S: Update<K, V>,
    {
        Self {
            id: keyspace.id(),
            statement: keyspace.statement().to_string(),
        }
    }
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
    fn handle_response(self: Box<Self>, _giveload: Vec<u8>) {
        info!("Successfully prepared statement: '{}'", self.statement);
    }
    fn handle_error(self: Box<Self>, error: WorkerError, _reporter: &Option<ReporterHandle>) {
        error!("Failed to prepare statement: {}, error: {}", self.statement, error);
    }
}

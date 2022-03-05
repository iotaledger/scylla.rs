// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use super::BatchCollector;
use scylla_parse::{
    CreateKeyspaceStatement,
    DropKeyspaceStatement,
    KeyspaceOpts,
};

/// Represents a Scylla Keyspace which holds a set of tables and
/// queries on those tables.
/// A keyspace can have predefined queries and functionality to
/// decode the results they return. To make use of this, implement
/// the following traits on a `Keyspace`:
///
/// - `RowsDecoder`
/// - `VoidDecoder`
/// - `Select`
/// - `Update`
/// - `Insert`
/// - `Delete`
pub trait Keyspace: Send + Sync {
    /// Options defined for this keyspace
    fn opts(&self) -> KeyspaceOpts;

    /// Get the name of the keyspace as represented in the database
    fn name(&self) -> &str;

    fn batch<'a>(&'a self) -> BatchCollector<'a>
    where
        Self: Sized,
    {
        BatchCollector::new(self)
    }

    /// Retrieve a CREATE KEYSPACE statement builder for this keyspace name
    fn create(&self) -> CreateKeyspaceStatement {
        scylla_parse::CreateKeyspaceStatementBuilder::default()
            .keyspace(self.name())
            .options(self.opts())
            .if_not_exists()
            .build()
            .unwrap()
    }

    /// Retrieve a DROP KEYSPACE statement builder for this keyspace name
    fn drop(&self) -> DropKeyspaceStatement {
        scylla_parse::DropKeyspaceStatementBuilder::default()
            .keyspace(self.name())
            .if_exists()
            .build()
            .unwrap()
    }

    // TODO replication_refactor, strategy, options,etc.
}

impl<T> Keyspace for T
where
    T: AsRef<str> + Clone + Send + Sync,
{
    fn name(&self) -> &str {
        self.as_ref()
    }

    fn opts(&self) -> KeyspaceOpts {
        Default::default()
    }
}

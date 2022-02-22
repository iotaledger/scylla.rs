// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use crate::cql::{
    Decoder,
    RowsDecoder,
    VoidDecoder,
};
use scylla_parse::{
    CreateKeyspaceStatement,
    DropKeyspaceStatement,
    KeyspaceOpts,
};

use super::BatchCollector;

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

    /// Decode void result
    fn decode_void(decoder: Decoder) -> anyhow::Result<()>
    where
        Self: Sized,
    {
        VoidDecoder::try_decode_void(decoder)
    }
    /// Decode rows result
    fn decode_rows<V>(decoder: Decoder) -> anyhow::Result<Option<V>>
    where
        Self: Sized,
        V: RowsDecoder,
    {
        V::try_decode_rows(decoder)
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

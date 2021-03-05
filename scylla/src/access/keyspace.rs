// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use std::borrow::Cow;

/// Represents a Scylla Keyspace which holds a set of tables and
/// queries on those tables.
///
/// ## Usage
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
pub trait Keyspace: Send + Sized + Sync {
    /// Get the name of the keyspace as represented in the database
    fn name(&self) -> &Cow<'static, str>;

    /// Decode void result
    fn decode_void(decoder: scylla_cql::Decoder) -> Result<(), scylla_cql::CqlError>
    where
        Self: scylla_cql::VoidDecoder,
    {
        Self::try_decode(decoder)
    }
    /// Decode rows result
    fn decode_rows<K, V>(decoder: scylla_cql::Decoder) -> Result<Option<V>, scylla_cql::CqlError>
    where
        Self: scylla_cql::RowsDecoder<K, V>,
    {
        Self::try_decode(decoder)
    }
    // TODO replication_refactor, strategy, options,etc.
}

/// Statements store builder
pub struct StatementsStoreBuilder<T: Keyspace> {
    _keyspace: std::marker::PhantomData<T>,
    cqls: Vec<String>,
}

impl<T: Keyspace> StatementsStoreBuilder<T> {
    /// Create empty statements store builder for T: keyspace.
    pub fn new() -> Self {
        StatementsStoreBuilder::<T> {
            _keyspace: std::marker::PhantomData::<T>,
            cqls: Vec::new(),
        }
    }
    /// Add cql to the statement store builder.
    /// NOTE: Have keyspace as "{}"
    pub fn add_cql(mut self, cql: String) -> Self {
        assert!(cql.contains("{}"));
        self.cqls.push(cql);
        self
    }
    /// Build the StatementsStore for T: Keyspace.
    pub fn build(self, keyspace: &T) -> StatementsStore<T> {
        let mut store = std::collections::HashMap::new();
        for cql in self.cqls {
            let cql_with_keyspace_name = cql.replace("{}", keyspace.name());
            let id = md5::compute(&cql_with_keyspace_name).0;
            store.insert(id, cql_with_keyspace_name);
        }
        StatementsStore::<T> {
            _keyspace: std::marker::PhantomData::<T>,
            store,
        }
    }
}

pub struct StatementsStore<T: Keyspace> {
    _keyspace: std::marker::PhantomData<T>,
    store: std::collections::HashMap<[u8; 16], String>,
}

impl<T: Keyspace> StatementsStore<T> {
    pub fn get_statement(&self, id: &[u8; 16]) -> Option<&String> {
        self.store.get(id)
    }
}

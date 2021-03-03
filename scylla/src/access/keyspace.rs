// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use crate::worker::Worker;

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
    /// Name of the keyspace
    const NAME: &'static str;
    /// Create new keyspace instance
    fn new() -> Self;
    /// Get the name of the keyspace as represented in the database
    fn name() -> &'static str {
        Self::NAME
    }
    fn get_statement(id: &[u8; 16]) -> Option<&String>;
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
    /// Send query to a random replica in the local datacenter;
    fn send_local(&self, token: i64, payload: Vec<u8>, worker: Box<dyn Worker>);
    /// Send query to a random replica in any global datacenter;
    fn send_global(&self, token: i64, payload: Vec<u8>, worker: Box<dyn Worker>);
    // TODO replication_refactor, strategy, options,etc.
}

pub struct StatementsStoreBuilder<T: Keyspace> {
    _keyspace: std::marker::PhantomData<T>,
    cqls: Vec<String>,
}

impl<T: Keyspace> StatementsStoreBuilder<T> {
    pub fn new() -> Self {
        StatementsStoreBuilder::<T> {
            _keyspace: std::marker::PhantomData::<T>,
            cqls: Vec::new(),
        }
    }
    pub fn add_cql(mut self, cql: String) -> Self {
        self.cqls.push(cql);
        self
    }
    pub fn build(self) -> StatementsStore<T> {
        let mut store = std::collections::HashMap::new();
        for cql in self.cqls {
            let cql_with_keyspace_name = cql.replace("{}", T::name());
            let id = md5::compute(&cql_with_keyspace_name).0;
            store.insert(id, cql_with_keyspace_name);
        }
        StatementsStore::<T> {
            _keyspace: std::marker::PhantomData::<T>,
            store,
        }
    }
}

static mut MAINNET_STORE: Option<std::collections::HashMap<[u8; 16], String>> = None;

pub struct StatementsStore<T: Keyspace> {
    _keyspace: std::marker::PhantomData<T>,
    store: std::collections::HashMap<[u8; 16], String>,
}

impl<T: Keyspace> StatementsStore<T> {
    pub fn get_statement(&self, id: &[u8; 16]) -> Option<&String> {
        self.store.get(id)
    }
}

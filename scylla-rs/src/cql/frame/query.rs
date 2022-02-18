// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

//! This module defines the query frame.

use super::{
    consistency::Consistency,
    encoder::{
        ColumnEncoder,
        BE_NULL_BYTES_LEN,
        BE_UNSET_BYTES_LEN,
    },
    opcode::{
        EXECUTE,
        QUERY,
    },
    queryflags::*,
    Binder,
    FrameBuilder,
};
use std::ops::{
    Deref,
    DerefMut,
};
use thiserror::Error;

/// Blanket cql frame header for query frame.
const QUERY_HEADER: [u8; 5] = [4, 0, 0, 0, QUERY];
const EXECUTE_HEADER: [u8; 5] = [4, 0, 0, 0, EXECUTE];

/// Query request builder. Maintains a type-gated stage so that operations
/// are applied in a valid order.
///
/// ## Example
/// ```
/// use scylla_rs::cql::{
///     Binder,
///     Consistency,
///     Query,
///     Statements,
/// };
///
/// let builder = Query::new();
/// let query = builder
///     .statement("statement")
///     .consistency(Consistency::One)
///     .bind_values()
///     .value(&0)
///     .value(&"val2".to_string())
///     .build()?;
/// let payload = query.0;
/// # Ok::<(), anyhow::Error>(())
/// ```
#[derive(Clone, Debug)]
pub struct QueryBuilder {
    statement: Option<QueryStatementKind>,
    consistency: Consistency,
    value_count: u16,
    bind_type: Option<BindType>,
    values: Vec<u8>,
    page_size: Option<i32>,
    paging_state: Option<Vec<u8>>,
    serial_consistency: Option<Consistency>,
    timestamp: Option<i64>,
}

impl Default for QueryBuilder {
    fn default() -> Self {
        Self {
            statement: Default::default(),
            consistency: Consistency::One,
            value_count: Default::default(),
            bind_type: Default::default(),
            values: Default::default(),
            page_size: Default::default(),
            paging_state: Default::default(),
            serial_consistency: Default::default(),
            timestamp: Default::default(),
        }
    }
}

#[derive(Clone, Debug)]
pub enum QueryStatementKind {
    Query(String),
    Execute([u8; 16]),
}

impl QueryStatementKind {
    pub fn encode(self, buffer: &mut Vec<u8>) {
        match self {
            QueryStatementKind::Query(stmt) => {
                buffer.reserve(stmt.len() + 4);
                buffer.extend(&i32::to_be_bytes(stmt.len() as i32));
                buffer.extend(stmt.as_bytes());
            }
            QueryStatementKind::Execute(id) => {
                buffer.reserve(18);
                buffer.extend(&super::MD5_BE_LENGTH);
                buffer.extend(id);
            }
        }
    }

    pub fn header(&self) -> [u8; 5] {
        match self {
            QueryStatementKind::Query(_) => QUERY_HEADER,
            QueryStatementKind::Execute(_) => EXECUTE_HEADER,
        }
    }

    pub fn len(&self) -> usize {
        match self {
            QueryStatementKind::Query(stmt) => stmt.len() + 4,
            QueryStatementKind::Execute(_) => 18,
        }
    }
}

#[derive(Copy, Clone, Debug)]
pub enum BindType {
    Named,
    Unnamed,
}

#[derive(Debug, Error)]
pub enum QueryBuildError {
    #[error("No query statement provided")]
    NoStatement,
    #[error("Failed to encode the frame: {0}")]
    BadEncode(anyhow::Error),
}

impl QueryBuilder {
    /// Set the statement in the query frame.
    pub fn statement(mut self, statement: &str) -> Self {
        self.statement.replace(QueryStatementKind::Query(statement.to_owned()));
        self
    }
    /// Set the id in the query frame.
    /// Note: this will make the Query frame identical to Execute frame.
    pub fn id(mut self, id: &[u8; 16]) -> Self {
        self.statement.replace(QueryStatementKind::Execute(id.to_owned()));
        self
    }
    /// Set the consistency in the query frame.
    pub fn consistency(mut self, consistency: Consistency) -> Self {
        self.consistency = consistency;
        self
    }

    /// Set the page size in the query frame, without any value.
    pub fn page_size(mut self, page_size: i32) -> Self {
        self.page_size.replace(page_size);
        self
    }
    /// Set the paging state in the query frame. without any value.
    pub fn paging_state(mut self, paging_state: Vec<u8>) -> Self {
        self.paging_state.replace(paging_state);
        self
    }
    /// Set serial consistency for the query frame.
    pub fn serial_consistency(mut self, serial_consistency: Consistency) -> Self {
        self.serial_consistency.replace(serial_consistency);
        self
    }
    /// Set the timestamp of the query frame, without any value.
    pub fn timestamp(mut self, timestamp: i64) -> Self {
        self.timestamp.replace(timestamp);
        self
    }
    /// Build a query frame with an assigned compression type, without any value.
    pub fn build(mut self) -> Result<Query, QueryBuildError> {
        let statement = self.statement.take().ok_or_else(|| QueryBuildError::NoStatement)?;
        let mut body_buf = Vec::with_capacity(statement.len() + 3);
        let header = statement.header();
        statement.encode(&mut body_buf);
        body_buf.extend(&u16::to_be_bytes(self.consistency as u16));
        let flags_idx = body_buf.len();
        // push SKIP_METADATA query_flag to the buffer
        // TODO: Maybe allow configuring this?
        body_buf.push(SKIP_METADATA);
        if self.value_count > 0 {
            body_buf[flags_idx] |= VALUES;
            body_buf.reserve(self.values.len() + 2);
            body_buf.extend(&u16::to_be_bytes(self.value_count));
            body_buf.extend(self.values);
            if matches!(self.bind_type, Some(BindType::Named)) {
                body_buf[flags_idx] |= NAMED_VALUES;
            }
        }
        if let Some(page_size) = self.page_size {
            body_buf[flags_idx] |= PAGE_SIZE;
            body_buf.extend(&i32::to_be_bytes(page_size));
        }
        if let Some(paging_state) = self.paging_state {
            body_buf[flags_idx] |= PAGING_STATE;
            body_buf.reserve(paging_state.len() + 2);
            body_buf.extend(&u32::to_be_bytes(paging_state.len() as u32));
            body_buf.extend(paging_state);
        }
        if let Some(serial_consistency) = self.serial_consistency {
            body_buf[flags_idx] |= SERIAL_CONSISTENCY;
            body_buf.extend(&u16::to_be_bytes(serial_consistency as u16));
        }
        if let Some(timestamp) = self.timestamp {
            body_buf[flags_idx] |= TIMESTAMP;
            body_buf.extend(&i64::to_be_bytes(timestamp));
        }
        Ok(Query(FrameBuilder::build(header, body_buf)))
    }
}

#[derive(Debug, Error)]
pub enum QueryBindError {
    #[error("No statement to bind values for")]
    NoStatement,
    #[error("Batch encode error: {0}")]
    EncodeError(#[from] anyhow::Error),
    #[error("Cannot mix named and unnamed values")]
    MixedBindTypes,
}

impl Binder for QueryBuilder {
    type Error = QueryBindError;
    /// Set the next value in the query frame.
    fn value<V: ColumnEncoder>(mut self, value: &V) -> Result<Self, Self::Error> {
        if matches!(self.bind_type, Some(BindType::Named)) {
            return Err(QueryBindError::MixedBindTypes);
        }
        self.bind_type.replace(BindType::Unnamed);
        // increase the value_count
        self.value_count += 1;
        // apply value
        value.encode(&mut self.values).map_err(|e| anyhow::anyhow!("{:?}", e))?;
        Ok(self)
    }
    fn named_value<V: ColumnEncoder>(mut self, name: &str, value: &V) -> Result<Self, Self::Error>
    where
        Self: Sized,
    {
        if matches!(self.bind_type, Some(BindType::Unnamed)) {
            return Err(QueryBindError::MixedBindTypes);
        }
        self.bind_type.replace(BindType::Named);
        // increase the value_count
        self.value_count += 1;
        name.encode(&mut self.values).map_err(|e| anyhow::anyhow!(e))?;
        // apply value
        value.encode(&mut self.values).map_err(|e| anyhow::anyhow!("{:?}", e))?;
        Ok(self)
    }
    /// Set the value to be unset in the query frame.
    fn unset_value(mut self) -> Result<Self, Self::Error> {
        // increase the value_count
        self.value_count += 1;
        // apply value
        self.values.extend(&BE_UNSET_BYTES_LEN);
        Ok(self)
    }

    /// Set the value to be null in the query frame.
    fn null_value(mut self) -> Result<Self, Self::Error> {
        // increase the value_count
        self.value_count += 1;
        // apply value
        self.values.extend(&BE_NULL_BYTES_LEN);
        Ok(self)
    }
}

#[derive(Default, Clone)]
/// The query frame structure.
pub struct Query(pub Vec<u8>);

impl Query {
    pub fn into_inner(self) -> Vec<u8> {
        self.0
    }
}

impl Deref for Query {
    type Target = Vec<u8>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for Query {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl Into<Vec<u8>> for Query {
    fn into(self) -> Vec<u8> {
        self.0
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{
        SystemTime,
        UNIX_EPOCH,
    };
    #[test]
    // note: junk data
    fn simple_query_builder_test() {
        let Query(_payload) = QueryBuilder::default()
            .statement("INSERT_TX_QUERY")
            .consistency(Consistency::One)
            .value(&"HASH_VALUE")
            .unwrap()
            .value(&"PAYLOAD_VALUE")
            .unwrap()
            .value(&"ADDRESS_VALUE")
            .unwrap()
            .value(&0_i64)
            .unwrap() // tx-value as i64
            .value(&"OBSOLETE_TAG_VALUE")
            .unwrap()
            .value(&SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs())
            .unwrap() // junk timestamp
            .value(&0)
            .unwrap() // current-index
            .unset_value()
            .unwrap() // not-set value for milestone
            .build()
            .unwrap();
    }
}

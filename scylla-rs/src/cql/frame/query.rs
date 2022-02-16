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
};
use crate::cql::compression::{
    Compression,
    MyCompression,
};
use std::ops::{
    Deref,
    DerefMut,
};
use thiserror::Error;

/// Blanket cql frame header for query frame.
const QUERY_HEADER: &'static [u8] = &[4, 0, 0, 0, QUERY, 0, 0, 0, 0];
const EXECUTE_HEADER: &'static [u8] = &[4, 0, 0, 0, EXECUTE, 0, 0, 0, 0];

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
    header: Option<Vec<u8>>,
    statement: Option<Vec<u8>>,
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
            header: Default::default(),
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

#[derive(Copy, Clone, Debug)]
pub enum BindType {
    Named,
    Unnamed,
}

pub enum StatementType {
    Query,
    Prepared,
}

#[derive(Debug, Error)]
pub enum QueryBuildError {
    #[error("No query statement provided")]
    NoStatement,
    #[error("Failed to compress the frame: {0}")]
    BadCompression(#[from] anyhow::Error),
    #[error("Failed to encode the frame: {0}")]
    BadEncode(anyhow::Error),
}

impl QueryBuilder {
    /// Set the statement in the query frame.
    pub fn statement(&mut self, statement: &str) -> &mut Self {
        self.header.replace(QUERY_HEADER.to_vec());
        let mut buffer = Vec::new();
        buffer.extend(&i32::to_be_bytes(statement.len() as i32));
        buffer.extend(statement.as_bytes());
        self.statement.replace(buffer);
        self
    }
    /// Set the id in the query frame.
    /// Note: this will make the Query frame identical to Execute frame.
    pub fn id(&mut self, id: &[u8; 16]) -> &mut Self {
        self.header.replace(EXECUTE_HEADER.to_vec());
        let mut buffer = Vec::new();
        buffer.extend(&super::MD5_BE_LENGTH);
        buffer.extend(id);
        self.statement.replace(buffer);
        self
    }
    /// Set the consistency in the query frame.
    pub fn consistency(&mut self, consistency: Consistency) -> &mut Self {
        self.consistency = consistency;
        self
    }

    /// Set the page size in the query frame, without any value.
    pub fn page_size(&mut self, page_size: i32) -> &mut Self {
        self.page_size.replace(page_size);
        self
    }
    /// Set the paging state in the query frame. without any value.
    pub fn paging_state(&mut self, paging_state: Vec<u8>) -> &mut Self {
        self.paging_state.replace(paging_state);
        self
    }
    /// Set serial consistency for the query frame.
    pub fn serial_consistency(&mut self, serial_consistency: Consistency) -> &mut Self {
        self.serial_consistency.replace(serial_consistency);
        self
    }
    /// Set the timestamp of the query frame, without any value.
    pub fn timestamp(&mut self, timestamp: i64) -> &mut Self {
        self.timestamp.replace(timestamp);
        self
    }
    /// Build a query frame with an assigned compression type, without any value.
    pub fn build(&self) -> Result<Query, QueryBuildError> {
        let mut buf = self
            .header
            .as_ref()
            .ok_or_else(|| QueryBuildError::NoStatement)?
            .clone();
        // apply compression flag(if any to the header)
        buf[1] |= MyCompression::flag();
        let mut body_buf = self
            .statement
            .as_ref()
            .ok_or_else(|| QueryBuildError::NoStatement)?
            .clone();
        body_buf.extend(&u16::to_be_bytes(self.consistency as u16));
        let flags_idx = body_buf.len();
        // push SKIP_METADATA query_flag to the buffer
        // TODO: Maybe allow configuring this?
        body_buf.push(SKIP_METADATA);
        if self.value_count > 0 {
            body_buf[flags_idx] |= VALUES;
            body_buf.extend(&u16::to_be_bytes(self.value_count));
            body_buf.extend(self.values.iter());
            if matches!(self.bind_type, Some(BindType::Named)) {
                body_buf[flags_idx] |= NAMED_VALUES;
            }
        }
        if let Some(page_size) = self.page_size {
            body_buf[flags_idx] |= PAGE_SIZE;
            body_buf.extend(&i32::to_be_bytes(page_size));
        }
        if let Some(paging_state) = self.paging_state.as_ref() {
            body_buf[flags_idx] |= PAGING_STATE;
            body_buf.extend(&u32::to_be_bytes(paging_state.len() as u32));
            body_buf.extend(paging_state.iter());
        }
        if let Some(serial_consistency) = self.serial_consistency {
            body_buf[flags_idx] |= SERIAL_CONSISTENCY;
            body_buf.extend(&u16::to_be_bytes(serial_consistency as u16));
        }
        if let Some(timestamp) = self.timestamp {
            body_buf[flags_idx] |= TIMESTAMP;
            body_buf.extend(&i64::to_be_bytes(timestamp));
        }
        // apply compression to query frame
        body_buf = MyCompression::get().compress(body_buf)?;
        buf.extend(&u32::to_be_bytes(body_buf.len() as u32));
        buf.extend(body_buf);
        // create query
        Ok(Query(buf))
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
    fn value<V: ColumnEncoder>(&mut self, value: &V) -> Result<&mut Self, Self::Error> {
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
    fn named_value<V: ColumnEncoder>(&mut self, name: &str, value: &V) -> Result<&mut Self, Self::Error>
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
    fn unset_value(&mut self) -> Result<&mut Self, Self::Error> {
        // increase the value_count
        self.value_count += 1;
        // apply value
        self.values.extend(&BE_UNSET_BYTES_LEN);
        Ok(self)
    }

    /// Set the value to be null in the query frame.
    fn null_value(&mut self) -> Result<&mut Self, Self::Error> {
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

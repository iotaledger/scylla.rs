// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

//! This module defines the query frame.

use super::{
    consistency::Consistency,
    encoder::{
        ColumnEncoder,
        BE_8_BYTES_LEN,
        BE_NULL_BYTES_LEN,
        BE_UNSET_BYTES_LEN,
    },
    opcode::{
        EXECUTE,
        QUERY,
    },
    queryflags::*,
    Binder,
    QueryOrPrepared,
    Statements,
};
use crate::cql::compression::{
    Compression,
    MyCompression,
};
use std::convert::TryInto;

/// Blanket cql frame header for query frame.
const QUERY_HEADER: &'static [u8] = &[4, 0, 0, 0, QUERY, 0, 0, 0, 0];

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
pub struct QueryBuilder<Stage> {
    buffer: Vec<u8>,
    stage: Stage,
}

/// Gating type for query headers
pub struct QueryHeader;
/// Gating type for query statement
pub struct QueryStatement;

/// Gating type for prepared statement id
pub struct PreparedStatement;

/// Gating type for query consistency
pub struct QueryConsistency;

pub enum StatementType {
    Query,
    Prepared,
}

/// Gating type for query flags
pub struct QueryFlags {
    index: usize,
}

/// Gating type for query values
pub struct QueryValues {
    query_flags: QueryFlags,
    value_count: u16,
}

/// Gating type for query paging state
pub struct QueryPagingState {
    query_flags: QueryFlags,
}

/// Gating type for query serial consistency
pub struct QuerySerialConsistency {
    query_flags: QueryFlags,
}

/// Gating type for query timestamps
pub struct QueryTimestamp {
    query_flags: QueryFlags,
}

/// Gating type for completed query
pub struct QueryBuild;

impl QueryBuilder<QueryHeader> {
    /// Create a new query builder
    pub fn new() -> QueryBuilder<QueryStatement> {
        let mut buffer: Vec<u8> = Vec::new();
        buffer.extend_from_slice(&QUERY_HEADER);
        QueryBuilder::<QueryStatement> {
            buffer,
            stage: QueryStatement,
        }
    }
    /// Create a new query builder with a given buffer capacity
    pub fn with_capacity(capacity: usize) -> QueryBuilder<QueryStatement> {
        let mut buffer: Vec<u8> = Vec::with_capacity(capacity);
        buffer.extend_from_slice(&QUERY_HEADER);
        QueryBuilder::<QueryStatement> {
            buffer,
            stage: QueryStatement,
        }
    }
}

impl QueryOrPrepared for QueryStatement {
    fn encode_statement<T: Statements>(query_or_batch: T, statement: &str) -> T::Return {
        query_or_batch.statement(statement)
    }
    fn is_prepared() -> bool {
        false
    }
}
impl QueryOrPrepared for PreparedStatement {
    fn encode_statement<T: Statements>(query_or_batch: T, statement: &str) -> T::Return {
        query_or_batch.id(&md5::compute(statement.as_bytes()).into())
    }
    fn is_prepared() -> bool {
        true
    }
}
impl<T: QueryOrPrepared> Statements for QueryBuilder<T> {
    type Return = QueryBuilder<QueryConsistency>;
    /// Set the statement in the query frame.
    fn statement(mut self, statement: &str) -> Self::Return {
        self.buffer.extend(&i32::to_be_bytes(statement.len() as i32));
        self.buffer.extend(statement.as_bytes());
        QueryBuilder::<QueryConsistency> {
            buffer: self.buffer,
            stage: QueryConsistency,
        }
    }
    /// Set the id in the query frame.
    /// Note: this will make the Query frame identical to Execute frame.
    fn id(mut self, id: &[u8; 16]) -> Self::Return {
        // Overwrite opcode
        self.buffer[4] = EXECUTE;
        self.buffer.extend(&super::MD5_BE_LENGTH);
        self.buffer.extend(id);
        QueryBuilder::<QueryConsistency> {
            buffer: self.buffer,
            stage: QueryConsistency,
        }
    }
}

impl QueryBuilder<QueryConsistency> {
    /// Set the consistency in the query frame.
    pub fn consistency(mut self, consistency: Consistency) -> QueryBuilder<QueryFlags> {
        self.buffer.extend(&u16::to_be_bytes(consistency as u16));
        let query_flag_index = self.buffer.len();
        QueryBuilder::<QueryFlags> {
            buffer: self.buffer,
            stage: QueryFlags {
                index: query_flag_index,
            },
        }
    }
}

impl QueryBuilder<QueryFlags> {
    /// Prepare to bind values for this query
    pub fn bind_values(mut self) -> QueryBuilder<QueryValues> {
        self.buffer.push(SKIP_METADATA);
        QueryBuilder {
            buffer: self.buffer,
            stage: QueryValues {
                query_flags: self.stage,
                value_count: 0,
            },
        }
    }
    /// Set the page size in the query frame, without any value.
    pub fn page_size(mut self, page_size: i32) -> QueryBuilder<QueryPagingState> {
        // push SKIP_METADATA and page_size query_flag to the buffer
        self.buffer.push(SKIP_METADATA | PAGE_SIZE);
        // apply page_size to query frame
        self.buffer.extend(&i32::to_be_bytes(page_size));
        // create query_paging_state
        let query_paging_state = QueryPagingState {
            query_flags: self.stage,
        };
        QueryBuilder::<QueryPagingState> {
            buffer: self.buffer,
            stage: query_paging_state,
        }
    }
    /// Set the paging state in the query frame. without any value.
    pub fn paging_state(mut self, paging_state: &Option<Vec<u8>>) -> QueryBuilder<QuerySerialConsistency> {
        if let Some(paging_state) = paging_state {
            // push SKIP_METADATA and PAGING_STATE query_flag to the buffer
            self.buffer.push(SKIP_METADATA | PAGING_STATE);
            // apply paging_state to query frame
            self.buffer.extend(&i32::to_be_bytes(paging_state.len() as i32));
            self.buffer.extend(paging_state);
        } else {
            // push only SKIP_METADATA
            self.buffer.push(SKIP_METADATA);
        }
        // create query_serial_consistency
        let query_serial_consistency = QuerySerialConsistency {
            query_flags: self.stage,
        };
        QueryBuilder::<QuerySerialConsistency> {
            buffer: self.buffer,
            stage: query_serial_consistency,
        }
    }
    /// Set serial consistency for the query frame.
    pub fn serial_consistency(mut self, consistency: Consistency) -> QueryBuilder<QueryTimestamp> {
        // push SKIP_METADATA and SERIAL_CONSISTENCY query_flag to the buffer
        self.buffer.push(SKIP_METADATA | SERIAL_CONSISTENCY);
        // apply serial_consistency to query frame
        self.buffer.extend(&u16::to_be_bytes(consistency as u16));
        // create query_timestamp
        let query_timestamp = QueryTimestamp {
            query_flags: self.stage,
        };
        QueryBuilder::<QueryTimestamp> {
            buffer: self.buffer,
            stage: query_timestamp,
        }
    }
    /// Set the timestamp of the query frame, without any value.
    pub fn timestamp(mut self, timestamp: i64) -> QueryBuilder<QueryBuild> {
        // push SKIP_METADATA and TIMESTAMP query_flag to the buffer
        self.buffer.push(SKIP_METADATA | TIMESTAMP);
        // apply timestamp to query frame
        self.buffer.extend(&BE_8_BYTES_LEN);
        self.buffer.extend(&i64::to_be_bytes(timestamp));
        // create query_build
        let query_build = QueryBuild;
        QueryBuilder::<QueryBuild> {
            buffer: self.buffer,
            stage: query_build,
        }
    }
    /// Build a query frame with an assigned compression type, without any value.
    pub fn build(mut self) -> anyhow::Result<Query> {
        // apply compression flag(if any to the header)
        self.buffer[1] |= MyCompression::flag();
        // push SKIP_METADATA query_flag to the buffer
        self.buffer.push(SKIP_METADATA);
        // apply compression to query frame
        self.buffer = MyCompression::get().compress(self.buffer)?;
        // create query
        Ok(Query(self.buffer))
    }
}
impl Binder for QueryBuilder<QueryValues> {
    /// Set the next value in the query frame.
    fn value<V: ColumnEncoder + Sync>(mut self, value: V) -> Self {
        if self.stage.value_count == 0 {
            self.buffer[self.stage.query_flags.index] |= VALUES;
            self.buffer.extend(&u16::to_be_bytes(1));
        }
        // increase the value_count
        self.stage.value_count += 1;
        // apply value
        value.encode(&mut self.buffer);
        self
    }
    /// Set the value to be unset in the query frame.
    fn unset_value(mut self) -> Self {
        if self.stage.value_count == 0 {
            self.buffer[self.stage.query_flags.index] |= VALUES;
            self.buffer.extend(&u16::to_be_bytes(1));
        }
        // increase the value_count
        self.stage.value_count += 1;
        // apply value
        self.buffer.extend(&BE_UNSET_BYTES_LEN);
        self
    }

    /// Set the value to be null in the query frame.
    fn null_value(mut self) -> Self {
        if self.stage.value_count == 0 {
            self.buffer[self.stage.query_flags.index] |= VALUES;
            self.buffer.extend(&u16::to_be_bytes(1));
        }
        // increase the value_count
        self.stage.value_count += 1;
        // apply value
        self.buffer.extend(&BE_NULL_BYTES_LEN);
        self
    }
}
impl QueryBuilder<QueryValues> {
    /// Set the page size in the query frame, with values.
    pub fn page_size(mut self, page_size: i32) -> QueryBuilder<QueryPagingState> {
        // add page_size query_flag to the buffer
        self.buffer[self.stage.query_flags.index] |= PAGE_SIZE;
        // apply page_size to query frame
        self.buffer.extend(&i32::to_be_bytes(page_size));
        // modiy the buffer total value_count
        let start = self.stage.query_flags.index + 1;
        let end = start + 2;
        self.buffer[start..end].copy_from_slice(&self.stage.value_count.to_be_bytes());
        // create query_page_size
        let query_page_size = QueryPagingState {
            query_flags: self.stage.query_flags,
        };
        QueryBuilder::<QueryPagingState> {
            buffer: self.buffer,
            stage: query_page_size,
        }
    }
    /// Set the paging state in the query frame, with values.
    pub fn paging_state(mut self, paging_state: &Option<Vec<u8>>) -> QueryBuilder<QuerySerialConsistency> {
        if let Some(paging_state) = paging_state {
            // add PAGING_STATE query_flag to the buffer
            self.buffer[self.stage.query_flags.index] |= PAGING_STATE;
            // apply paging_state to query frame
            self.buffer.extend(&i32::to_be_bytes(paging_state.len() as i32));
            self.buffer.extend(paging_state);
        }
        // modiy the buffer total value_count
        let start = self.stage.query_flags.index + 1;
        let end = start + 2;
        self.buffer[start..end].copy_from_slice(&self.stage.value_count.to_be_bytes());
        // create query_serial_consistency
        let query_serial_consistency = QuerySerialConsistency {
            query_flags: self.stage.query_flags,
        };
        QueryBuilder::<QuerySerialConsistency> {
            buffer: self.buffer,
            stage: query_serial_consistency,
        }
    }
    /// Set serial consistency for the query frame, with values.
    pub fn serial_consistency(mut self, consistency: Consistency) -> QueryBuilder<QueryTimestamp> {
        // add SERIAL_CONSISTENCY query_flag to the buffer
        self.buffer[self.stage.query_flags.index] |= SERIAL_CONSISTENCY;
        // apply serial_consistency to query frame
        self.buffer.extend(&u16::to_be_bytes(consistency as u16));
        // modiy the buffer total value_count
        let start = self.stage.query_flags.index + 1;
        let end = start + 2;
        self.buffer[start..end].copy_from_slice(&self.stage.value_count.to_be_bytes());
        // create query_timestamp
        let query_timestamp = QueryTimestamp {
            query_flags: self.stage.query_flags,
        };
        QueryBuilder::<QueryTimestamp> {
            buffer: self.buffer,
            stage: query_timestamp,
        }
    }
    /// Set the timestamp of the query frame, with values.
    pub fn timestamp(mut self, timestamp: i64) -> QueryBuilder<QueryBuild> {
        // add TIMESTAMP query_flag to the buffer
        self.buffer[self.stage.query_flags.index] |= TIMESTAMP;
        // apply timestamp to query frame
        self.buffer.extend(&BE_8_BYTES_LEN);
        self.buffer.extend(&i64::to_be_bytes(timestamp));
        // modiy the buffer total value_count
        let start = self.stage.query_flags.index + 1;
        let end = start + 2;
        self.buffer[start..end].copy_from_slice(&self.stage.value_count.to_be_bytes());
        // create query_build
        let query_build = QueryBuild;
        QueryBuilder::<QueryBuild> {
            buffer: self.buffer,
            stage: query_build,
        }
    }

    /// Build a query frame with an assigned compression type, with values.
    pub fn build(mut self) -> anyhow::Result<Query> {
        // apply compression flag(if any to the header)
        self.buffer[1] |= MyCompression::flag();
        // modiy the buffer total value_count
        if self.stage.value_count > 0 {
            let start = self.stage.query_flags.index + 1;
            let end = start + 2;
            self.buffer[start..end].copy_from_slice(&self.stage.value_count.to_be_bytes());
        }
        // apply compression to query frame
        self.buffer = MyCompression::get().compress(self.buffer)?;
        // create query
        Ok(Query(self.buffer))
    }
}

impl QueryBuilder<QueryPagingState> {
    /// Set the paging state in the query frame.
    pub fn paging_state(mut self, paging_state: &Option<Vec<u8>>) -> QueryBuilder<QuerySerialConsistency> {
        // apply paging_state to query frame
        if let Some(paging_state) = paging_state {
            // add PAGING_STATE query_flag to the buffer
            self.buffer[self.stage.query_flags.index] |= PAGING_STATE;
            self.buffer.extend(&i32::to_be_bytes(paging_state.len() as i32));
            self.buffer.extend(paging_state);
        }
        // create query_serial_consistency
        let query_serial_consistency = QuerySerialConsistency {
            query_flags: self.stage.query_flags,
        };
        QueryBuilder::<QuerySerialConsistency> {
            buffer: self.buffer,
            stage: query_serial_consistency,
        }
    }
    /// Set serial consistency for the query frame.
    pub fn serial_consistency(mut self, consistency: Consistency) -> QueryBuilder<QueryTimestamp> {
        // add SERIAL_CONSISTENCY query_flag to the buffer
        self.buffer[self.stage.query_flags.index] |= SERIAL_CONSISTENCY;
        // apply serial_consistency to query frame
        self.buffer.extend(&u16::to_be_bytes(consistency as u16));
        // create query_timestamp
        let query_timestamp = QueryTimestamp {
            query_flags: self.stage.query_flags,
        };
        QueryBuilder::<QueryTimestamp> {
            buffer: self.buffer,
            stage: query_timestamp,
        }
    }
    /// Set the timestamp of the query frame.
    pub fn timestamp(mut self, timestamp: i64) -> QueryBuilder<QueryBuild> {
        // add TIMESTAMP query_flag to the buffer
        self.buffer[self.stage.query_flags.index] |= TIMESTAMP;
        // apply timestamp to query frame
        self.buffer.extend(&BE_8_BYTES_LEN);
        self.buffer.extend(&i64::to_be_bytes(timestamp));
        // create query_build
        let query_build = QueryBuild;
        QueryBuilder::<QueryBuild> {
            buffer: self.buffer,
            stage: query_build,
        }
    }

    /// Build a query frame with an assigned compression type.
    pub fn build(mut self) -> anyhow::Result<Query> {
        // apply compression flag(if any to the header)
        self.buffer[1] |= MyCompression::flag();
        // apply compression to query frame
        self.buffer = MyCompression::get().compress(self.buffer)?;
        // create query
        Ok(Query(self.buffer))
    }
}

impl QueryBuilder<QuerySerialConsistency> {
    /// Set serial consistency for the query frame.
    pub fn serial_consistency(mut self, consistency: Consistency) -> QueryBuilder<QueryTimestamp> {
        // add SERIAL_CONSISTENCY query_flag to the buffer
        self.buffer[self.stage.query_flags.index] |= SERIAL_CONSISTENCY;
        // apply serial_consistency to query frame
        self.buffer.extend(&u16::to_be_bytes(consistency as u16));
        // create query_timestamp
        let query_timestamp = QueryTimestamp {
            query_flags: self.stage.query_flags,
        };
        QueryBuilder::<QueryTimestamp> {
            buffer: self.buffer,
            stage: query_timestamp,
        }
    }
    /// Set the timestamp of the query frame.
    pub fn timestamp(mut self, timestamp: i64) -> QueryBuilder<QueryBuild> {
        // add TIMESTAMP query_flag to the buffer
        self.buffer[self.stage.query_flags.index] |= TIMESTAMP;
        // apply timestamp to query frame
        self.buffer.extend(&BE_8_BYTES_LEN);
        self.buffer.extend(&i64::to_be_bytes(timestamp));
        // create query_build
        let query_build = QueryBuild;
        QueryBuilder::<QueryBuild> {
            buffer: self.buffer,
            stage: query_build,
        }
    }

    /// Build a query frame with an assigned compression type.
    pub fn build(mut self) -> anyhow::Result<Query> {
        // apply compression flag(if any to the header)
        self.buffer[1] |= MyCompression::flag();
        // apply compression to query frame
        self.buffer = MyCompression::get().compress(self.buffer)?;
        // create query
        Ok(Query(self.buffer))
    }
}

impl QueryBuilder<QueryTimestamp> {
    /// Set the timestamp of the query frame.
    pub fn timestamp(mut self, timestamp: i64) -> QueryBuilder<QueryBuild> {
        // add TIMESTAMP query_flag to the buffer
        self.buffer[self.stage.query_flags.index] |= TIMESTAMP;
        // apply timestamp to query frame
        self.buffer.extend(&BE_8_BYTES_LEN);
        self.buffer.extend(&i64::to_be_bytes(timestamp));
        // create query_build
        let query_build = QueryBuild;
        QueryBuilder::<QueryBuild> {
            buffer: self.buffer,
            stage: query_build,
        }
    }
    /// Build a query frame with an assigned compression type.
    pub fn build(mut self) -> anyhow::Result<Query> {
        // apply compression flag(if any to the header)
        self.buffer[1] |= MyCompression::flag();
        // apply compression to query frame
        self.buffer = MyCompression::get().compress(self.buffer)?;
        // create query
        Ok(Query(self.buffer))
    }
}

impl QueryBuilder<QueryBuild> {
    /// Build a query frame with an assigned compression type.
    pub fn build(mut self) -> anyhow::Result<Query> {
        // apply compression flag(if any to the header)
        self.buffer[1] |= MyCompression::flag();
        // apply compression to query frame
        self.buffer = MyCompression::get().compress(self.buffer)?;
        // create query
        Ok(Query(self.buffer))
    }
}

#[derive(Default, Clone)]
/// The query frame structure.
pub struct Query(pub Vec<u8>);

impl Query {
    /// Create CQL query by following the cql binary v4 specs
    pub fn new() -> QueryBuilder<QueryStatement> {
        QueryBuilder::<QueryHeader>::new()
    }
    /// Create CQL query with_capacity by following the cql binary v4 specs
    pub fn with_capacity(capacity: usize) -> QueryBuilder<QueryStatement> {
        QueryBuilder::<QueryHeader>::with_capacity(capacity)
    }
}

impl Into<Vec<u8>> for Query {
    fn into(self) -> Vec<u8> {
        self.0
    }
}

impl Query {
    /// Create a Query from a payload without validating it.
    /// This assumes that the user knows for certain that the payload
    /// is a valid Query payload. Otherwise, using it as one
    /// will likely cause panics.
    pub fn from_payload_unchecked(payload: Vec<u8>) -> Query {
        Query(payload)
    }

    /// Convert a Query frame into an Execute frame.
    /// Will return an error if the frame is not a Query frame.
    pub fn convert_to_execute(&mut self) -> anyhow::Result<&mut Self> {
        anyhow::ensure!(self.0[4] == QUERY, "Not a query frame");
        let body_len = i32::from_be_bytes(self.0[5..9].try_into()?);
        let stmt_len = i32::from_be_bytes(self.0[9..13].try_into()?);
        let total_stmt_len = stmt_len + 4;
        let id: [u8; 16] = md5::compute(&self.0[13..(13 + stmt_len as usize)]).into();
        self.0[4] = EXECUTE;
        match total_stmt_len.cmp(&16) {
            std::cmp::Ordering::Less => {
                let dif = (16 - total_stmt_len) as usize;
                self.0.reserve(dif);
                self.0[(13 + stmt_len as usize)..].rotate_right(dif);
                self.0[5..9].copy_from_slice(&(body_len + dif as i32).to_be_bytes());
            }
            std::cmp::Ordering::Greater => {
                let dif = (total_stmt_len - 16) as usize;
                self.0[(13 + stmt_len as usize)..].rotate_left(dif);
                self.0.truncate(self.0.len() - dif);
                self.0[5..9].copy_from_slice(&(body_len - dif as i32).to_be_bytes());
            }
            std::cmp::Ordering::Equal => (),
        }
        self.0[9..25].copy_from_slice(&id);
        Ok(self)
    }

    /// Convert an Execute frame into a Query frame.
    /// Will return an error if the frame is not an Execute frame.
    pub fn convert_to_query(&mut self, stmt: &str) -> anyhow::Result<&mut Query> {
        anyhow::ensure!(self.0[4] == EXECUTE, "Not an execute frame");
        let body_len = i32::from_be_bytes(self.0[5..9].try_into()?);
        let stmt_len = stmt.len();
        let total_stmt_len = stmt_len + 4;
        let total_id_len = 18;
        let rem_start = 9 + total_id_len;
        self.0[4] = QUERY;
        match total_id_len.cmp(&total_stmt_len) {
            std::cmp::Ordering::Greater => {
                let dif = total_id_len - total_stmt_len;
                self.0[rem_start..].rotate_left(dif);
                self.0.truncate(self.0.len() - dif);
                self.0[5..9].copy_from_slice(&(body_len - dif as i32).to_be_bytes());
            }
            std::cmp::Ordering::Less => {
                let dif = total_stmt_len - total_id_len;
                self.0.reserve(dif);
                self.0[rem_start..].rotate_right(dif);
                self.0[5..9].copy_from_slice(&(body_len + dif as i32).to_be_bytes());
            }
            std::cmp::Ordering::Equal => (),
        }
        self.0[9..13].copy_from_slice(&(stmt_len as i32).to_be_bytes());
        self.0[13..][..stmt_len].copy_from_slice(stmt.as_bytes());
        Ok(self)
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
        let Query(_payload) = Query::new()
            .statement("INSERT_TX_QUERY")
            .consistency(Consistency::One)
            .bind_values()
            .value(&"HASH_VALUE")
            .value(&"PAYLOAD_VALUE")
            .value(&"ADDRESS_VALUE")
            .value(&0_i64) // tx-value as i64
            .value(&"OBSOLETE_TAG_VALUE")
            .value(&SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs()) // junk timestamp
            .value(&0) // current-index
            .unset_value() // not-set value for milestone
            .build()
            .unwrap();
    }
}

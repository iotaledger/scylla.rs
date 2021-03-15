// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

//! This module defines the query frame.

use super::{
    consistency::Consistency,
    encoder::{ColumnEncoder, BE_8_BYTES_LEN, BE_NULL_BYTES_LEN, BE_UNSET_BYTES_LEN},
    opcode::{EXECUTE, QUERY},
    queryflags::*,
    QueryOrPrepared, Statements, Values,
};
use crate::compression::{Compression, MyCompression};
use std::convert::TryInto;

/// Blanket cql frame header for query frame.
const QUERY_HEADER: &'static [u8] = &[4, 0, 0, 0, QUERY, 0, 0, 0, 0];

pub struct QueryBuilder<Stage> {
    buffer: Vec<u8>,
    stage: Stage,
}

pub struct QueryHeader;
pub struct QueryStatement;
pub struct PreparedStatement;
pub struct QueryConsistency;
pub struct QueryFlags {
    index: usize,
}
pub struct QueryValues {
    query_flags: QueryFlags,
    value_count: u16,
}
pub struct QueryPagingState {
    query_flags: QueryFlags,
}
pub struct QuerySerialConsistency {
    query_flags: QueryFlags,
}
pub struct QueryTimestamp {
    query_flags: QueryFlags,
}
pub struct QueryBuild;

impl QueryBuilder<QueryHeader> {
    fn new() -> QueryBuilder<QueryStatement> {
        let mut buffer: Vec<u8> = Vec::new();
        buffer.extend_from_slice(&QUERY_HEADER);
        QueryBuilder::<QueryStatement> {
            buffer,
            stage: QueryStatement,
        }
    }
    fn with_capacity(capacity: usize) -> QueryBuilder<QueryStatement> {
        let mut buffer: Vec<u8> = Vec::with_capacity(capacity);
        buffer.extend_from_slice(&QUERY_HEADER);
        QueryBuilder::<QueryStatement> {
            buffer,
            stage: QueryStatement,
        }
    }
}

impl QueryOrPrepared for QueryStatement {
    fn encode_statement<T: Statements>(query_or_batch: T, statement: &[u8]) -> T::Return {
        let statement = unsafe { std::str::from_utf8_unchecked(&statement) };
        query_or_batch.statement(statement)
    }
    fn is_prepared() -> bool {
        false
    }
}
impl QueryOrPrepared for PreparedStatement {
    fn encode_statement<T: Statements>(query_or_batch: T, statement: &[u8]) -> T::Return {
        query_or_batch.id(statement.try_into().unwrap())
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

impl Values for QueryBuilder<QueryFlags> {
    type Return = QueryBuilder<QueryValues>;
    /// Set the first value to be null in the query frame.
    fn null_value(mut self) -> QueryBuilder<QueryValues> {
        // push SKIP_METADATA and VALUES query_flag to the buffer
        self.buffer.push(SKIP_METADATA | VALUES);
        let value_count = 1;
        // push value_count
        self.buffer.extend(&u16::to_be_bytes(value_count));
        // apply null value
        self.buffer.extend(&BE_NULL_BYTES_LEN);
        // create query_values
        let query_values = QueryValues {
            query_flags: self.stage,
            value_count,
        };
        QueryBuilder::<QueryValues> {
            buffer: self.buffer,
            stage: query_values,
        }
    }
    /// Set the value to be unset in the query frame.
    fn unset_value(mut self) -> QueryBuilder<QueryValues> {
        // push SKIP_METADATA and VALUES query_flag to the buffer
        self.buffer.push(SKIP_METADATA | VALUES);
        let value_count = 1;
        // push value_count
        self.buffer.extend(&u16::to_be_bytes(value_count));
        // apply null value
        self.buffer.extend(&BE_UNSET_BYTES_LEN);
        // create query_values
        let query_values = QueryValues {
            query_flags: self.stage,
            value_count,
        };
        QueryBuilder::<QueryValues> {
            buffer: self.buffer,
            stage: query_values,
        }
    }
    /// Set the first value in the query frame.
    fn value<V: ColumnEncoder>(mut self, value: &V) -> QueryBuilder<QueryValues> {
        // push SKIP_METADATA and VALUES query_flag to the buffer
        self.buffer.push(SKIP_METADATA | VALUES);
        let value_count = 1;
        // push value_count
        self.buffer.extend(&u16::to_be_bytes(value_count));
        // create query_values
        let query_values = QueryValues {
            query_flags: self.stage,
            value_count,
        };
        // push value
        value.encode(&mut self.buffer);
        QueryBuilder::<QueryValues> {
            buffer: self.buffer,
            stage: query_values,
        }
    }
}
impl QueryBuilder<QueryFlags> {
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
    pub fn build(mut self) -> Query {
        // apply compression flag(if any to the header)
        self.buffer[1] |= MyCompression::flag();
        // push SKIP_METADATA query_flag to the buffer
        self.buffer.push(SKIP_METADATA);
        // apply compression to query frame
        self.buffer = MyCompression::get().compress(self.buffer);
        // create query
        Query(self.buffer)
    }
}
impl Values for QueryBuilder<QueryValues> {
    type Return = QueryBuilder<QueryValues>;
    /// Set the next value in the query frame.
    fn value<V: ColumnEncoder>(mut self, value: &V) -> Self {
        // increase the value_count
        self.stage.value_count += 1;
        // apply value
        value.encode(&mut self.buffer);
        self
    }
    /// Set the value to be unset in the query frame.
    fn unset_value(mut self) -> Self {
        // increase the value_count
        self.stage.value_count += 1;
        // apply value
        self.buffer.extend(&BE_UNSET_BYTES_LEN);
        self
    }
    /// Set the value to be null in the query frame.
    fn null_value(mut self) -> Self {
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
    pub fn build(mut self) -> Query {
        // apply compression flag(if any to the header)
        self.buffer[1] |= MyCompression::flag();
        // modiy the buffer total value_count
        let start = self.stage.query_flags.index + 1;
        let end = start + 2;
        self.buffer[start..end].copy_from_slice(&self.stage.value_count.to_be_bytes());
        // apply compression to query frame
        self.buffer = MyCompression::get().compress(self.buffer);
        // create query
        Query(self.buffer)
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
    pub fn build(mut self) -> Query {
        // apply compression flag(if any to the header)
        self.buffer[1] |= MyCompression::flag();
        // apply compression to query frame
        self.buffer = MyCompression::get().compress(self.buffer);
        // create query
        Query(self.buffer)
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
    pub fn build(mut self) -> Query {
        // apply compression flag(if any to the header)
        self.buffer[1] |= MyCompression::flag();
        // apply compression to query frame
        self.buffer = MyCompression::get().compress(self.buffer);
        // create query
        Query(self.buffer)
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
    pub fn build(mut self) -> Query {
        // apply compression flag(if any to the header)
        self.buffer[1] |= MyCompression::flag();
        // apply compression to query frame
        self.buffer = MyCompression::get().compress(self.buffer);
        // create query
        Query(self.buffer)
    }
}

impl QueryBuilder<QueryBuild> {
    /// Build a query frame with an assigned compression type.
    pub fn build(mut self) -> Query {
        // apply compression flag(if any to the header)
        self.buffer[1] |= MyCompression::flag();
        // apply compression to query frame
        self.buffer = MyCompression::get().compress(self.buffer);
        // create query
        Query(self.buffer)
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
#[cfg(test)]
mod tests {
    use super::*;

    use std::time::{SystemTime, UNIX_EPOCH};
    #[test]
    // note: junk data
    fn simple_query_builder_test() {
        let Query(_payload) = Query::new()
            .statement("INSERT_TX_QUERY")
            .consistency(Consistency::One)
            .value(&"HASH_VALUE")
            .value(&"PAYLOAD_VALUE")
            .value(&"ADDRESS_VALUE")
            .value::<i64>(&0) // tx-value as i64
            .value(&"OBSOLETE_TAG_VALUE")
            .value(&SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs()) // junk timestamp
            .value(&0) // current-index
            .unset_value() // not-set value for milestone
            .build();
    }
}

// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

//! This module defines the execute frame.

use super::{
    consistency::Consistency,
    encoder::{ColumnEncoder, BE_8_BYTES_LEN, BE_NULL_BYTES_LEN, BE_UNSET_BYTES_LEN},
    opcode::EXECUTE,
    queryflags::*,
    MD5_BE_LENGTH,
};
use crate::compression::{Compression, MyCompression};

/// Blanket cql frame header for execute frame.
const EXECUTE_HEADER: &'static [u8] = &[4, 0, 0, 0, EXECUTE, 0, 0, 0, 0];

pub struct ExecuteBuilder<Stage> {
    buffer: Vec<u8>,
    stage: Stage,
}

pub struct ExecuteHeader;
pub struct ExecuteId;
pub struct ExecuteConsistency;
pub struct ExecuteFlags {
    index: usize,
}
pub struct ExecuteValues {
    query_flags: ExecuteFlags,
    value_count: u16,
}
pub struct ExecutePagingState {
    query_flags: ExecuteFlags,
}
pub struct ExecuteSerialConsistency {
    query_flags: ExecuteFlags,
}
pub struct ExecuteTimestamp {
    query_flags: ExecuteFlags,
}
pub struct ExecuteBuild;

impl ExecuteBuilder<ExecuteHeader> {
    fn new() -> ExecuteBuilder<ExecuteId> {
        let mut buffer: Vec<u8> = Vec::new();
        buffer.extend_from_slice(&EXECUTE_HEADER);
        ExecuteBuilder::<ExecuteId> {
            buffer,
            stage: ExecuteId,
        }
    }
    fn with_capacity(capacity: usize) -> ExecuteBuilder<ExecuteId> {
        let mut buffer: Vec<u8> = Vec::with_capacity(capacity);
        buffer.extend_from_slice(&EXECUTE_HEADER);
        ExecuteBuilder::<ExecuteId> {
            buffer,
            stage: ExecuteId,
        }
    }
}
impl ExecuteBuilder<ExecuteId> {
    /// Set the id in the execute frame.
    pub fn id(mut self, id: &[u8; 16]) -> ExecuteBuilder<ExecuteConsistency> {
        self.buffer.extend(&MD5_BE_LENGTH);
        self.buffer.extend(id);
        ExecuteBuilder::<ExecuteConsistency> {
            buffer: self.buffer,
            stage: ExecuteConsistency,
        }
    }
}

impl ExecuteBuilder<ExecuteConsistency> {
    /// Set the consistency in the query frame.
    pub fn consistency(mut self, consistency: Consistency) -> ExecuteBuilder<ExecuteFlags> {
        self.buffer.extend(&u16::to_be_bytes(consistency as u16));
        let query_flag_index = self.buffer.len();
        ExecuteBuilder::<ExecuteFlags> {
            buffer: self.buffer,
            stage: ExecuteFlags {
                index: query_flag_index,
            },
        }
    }
}

impl ExecuteBuilder<ExecuteFlags> {
    /// Set the first value to be null in the execute frame.
    pub fn null_value(mut self) -> ExecuteBuilder<ExecuteValues> {
        // push SKIP_METADATA and VALUES query_flag to the buffer
        self.buffer.push(SKIP_METADATA | VALUES);
        let value_count = 1;
        // push value_count
        self.buffer.extend(&u16::to_be_bytes(value_count));
        // apply null value
        self.buffer.extend(&BE_NULL_BYTES_LEN);
        // create execute_values
        let execute_values = ExecuteValues {
            query_flags: self.stage,
            value_count,
        };
        ExecuteBuilder::<ExecuteValues> {
            buffer: self.buffer,
            stage: execute_values,
        }
    }
    /// Set the first value in the query frame.
    pub fn value(mut self, value: impl ColumnEncoder) -> ExecuteBuilder<ExecuteValues> {
        // push SKIP_METADATA and VALUES query_flag to the buffer
        self.buffer.push(SKIP_METADATA | VALUES);
        let value_count = 1;
        // push value_count
        self.buffer.extend(&u16::to_be_bytes(value_count));
        // push value
        value.encode(&mut self.buffer);
        // create execute_values
        let execute_values = ExecuteValues {
            query_flags: self.stage,
            value_count,
        };
        ExecuteBuilder::<ExecuteValues> {
            buffer: self.buffer,
            stage: execute_values,
        }
    }
    /// Set the page size in the execute frame, without any value.
    pub fn page_size(mut self, page_size: i32) -> ExecuteBuilder<ExecutePagingState> {
        // push SKIP_METADATA and page_size query_flag to the buffer
        self.buffer.push(SKIP_METADATA | PAGE_SIZE);
        // apply page_size to query frame
        self.buffer.extend(&i32::to_be_bytes(page_size));
        // create execute_paging_state
        let execute_paging_state = ExecutePagingState {
            query_flags: self.stage,
        };
        ExecuteBuilder::<ExecutePagingState> {
            buffer: self.buffer,
            stage: execute_paging_state,
        }
    }
    /// Set the paging state in the execute frame. without any value.
    pub fn paging_state(mut self, paging_state: &Option<Vec<u8>>) -> ExecuteBuilder<ExecuteSerialConsistency> {
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
        // create execute_serial_consistency
        let execute_serial_consistency = ExecuteSerialConsistency {
            query_flags: self.stage,
        };
        ExecuteBuilder::<ExecuteSerialConsistency> {
            buffer: self.buffer,
            stage: execute_serial_consistency,
        }
    }
    /// Set serial consistency for the execute frame.
    pub fn serial_consistency(mut self, consistency: Consistency) -> ExecuteBuilder<ExecuteTimestamp> {
        // push SKIP_METADATA and SERIAL_CONSISTENCY query_flag to the buffer
        self.buffer.push(SKIP_METADATA | SERIAL_CONSISTENCY);
        // apply serial_consistency to query frame
        self.buffer.extend(&u16::to_be_bytes(consistency as u16));
        // create Execute_timestamp
        let execute_timestamp = ExecuteTimestamp {
            query_flags: self.stage,
        };
        ExecuteBuilder::<ExecuteTimestamp> {
            buffer: self.buffer,
            stage: execute_timestamp,
        }
    }
    /// Set the timestamp of the execute frame, without any value.
    pub fn timestamp(mut self, timestamp: i64) -> ExecuteBuilder<ExecuteBuild> {
        // push SKIP_METADATA and TIMESTAMP query_flag to the buffer
        self.buffer.push(SKIP_METADATA | TIMESTAMP);
        // apply timestamp to query frame
        self.buffer.extend(&BE_8_BYTES_LEN);
        self.buffer.extend(&i64::to_be_bytes(timestamp));
        // create execute_build
        let execute_build = ExecuteBuild;
        ExecuteBuilder::<ExecuteBuild> {
            buffer: self.buffer,
            stage: execute_build,
        }
    }
    /// Build a execute frame with an assigned compression type, without any value.
    pub fn build(mut self) -> Execute {
        // apply compression flag(if any to the header)
        self.buffer[1] |= MyCompression::flag();
        // push SKIP_METADATA query_flag to the buffer
        self.buffer.push(SKIP_METADATA);
        // apply compression to query frame
        self.buffer = MyCompression::get().compress(self.buffer);
        // create Execute
        Execute(self.buffer)
    }
}

impl ExecuteBuilder<ExecuteValues> {
    /// Set the next value in the query frame.
    pub fn value(mut self, value: impl ColumnEncoder) -> Self {
        // increase the value_count
        self.stage.value_count += 1;
        // apply value
        value.encode(&mut self.buffer);
        self
    }
    /// Set the value to be unset in the query frame.
    pub fn unset_value(mut self) -> Self {
        // increase the value_count
        self.stage.value_count += 1;
        // apply value
        self.buffer.extend(&BE_UNSET_BYTES_LEN);
        self
    }
    /// Set the value to be null in the query frame.
    pub fn null_value(mut self) -> Self {
        // increase the value_count
        self.stage.value_count += 1;
        // apply value
        self.buffer.extend(&BE_NULL_BYTES_LEN);
        self
    }

    /// Set the page size in the query frame, with values.
    pub fn page_size(mut self, page_size: i32) -> ExecuteBuilder<ExecutePagingState> {
        // add page_size query_flag to the buffer
        self.buffer[self.stage.query_flags.index] |= PAGE_SIZE;
        // apply page_size to query frame
        self.buffer.extend(&i32::to_be_bytes(page_size));
        // modiy the buffer total value_count
        let start = self.stage.query_flags.index + 1;
        let end = start + 2;
        self.buffer[start..end].copy_from_slice(&self.stage.value_count.to_be_bytes());
        // create execute_page_size
        let execute_page_size = ExecutePagingState {
            query_flags: self.stage.query_flags,
        };
        ExecuteBuilder::<ExecutePagingState> {
            buffer: self.buffer,
            stage: execute_page_size,
        }
    }
    /// Set the paging state in the query frame, with values.
    pub fn paging_state(mut self, paging_state: &Option<Vec<u8>>) -> ExecuteBuilder<ExecuteSerialConsistency> {
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
        let query_serial_consistency = ExecuteSerialConsistency {
            query_flags: self.stage.query_flags,
        };
        ExecuteBuilder::<ExecuteSerialConsistency> {
            buffer: self.buffer,
            stage: query_serial_consistency,
        }
    }
    /// Set serial consistency for the query frame, with values.
    pub fn serial_consistency(mut self, consistency: Consistency) -> ExecuteBuilder<ExecuteTimestamp> {
        // add SERIAL_CONSISTENCY query_flag to the buffer
        self.buffer[self.stage.query_flags.index] |= SERIAL_CONSISTENCY;
        // apply serial_consistency to query frame
        self.buffer.extend(&u16::to_be_bytes(consistency as u16));
        // modiy the buffer total value_count
        let start = self.stage.query_flags.index + 1;
        let end = start + 2;
        self.buffer[start..end].copy_from_slice(&self.stage.value_count.to_be_bytes());
        // create query_timestamp
        let query_timestamp = ExecuteTimestamp {
            query_flags: self.stage.query_flags,
        };
        ExecuteBuilder::<ExecuteTimestamp> {
            buffer: self.buffer,
            stage: query_timestamp,
        }
    }
    /// Set the timestamp of the query frame, with values.
    pub fn timestamp(mut self, timestamp: i64) -> ExecuteBuilder<ExecuteBuild> {
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
        let query_build = ExecuteBuild;
        ExecuteBuilder::<ExecuteBuild> {
            buffer: self.buffer,
            stage: query_build,
        }
    }

    /// Build a query frame with an assigned compression type, with values.
    pub fn build(mut self) -> Execute {
        // apply compression flag(if any to the header)
        self.buffer[1] |= MyCompression::flag();
        // modiy the buffer total value_count
        let start = self.stage.query_flags.index + 1;
        let end = start + 2;
        self.buffer[start..end].copy_from_slice(&self.stage.value_count.to_be_bytes());
        // apply compression to query frame
        self.buffer = MyCompression::get().compress(self.buffer);
        // create query
        Execute(self.buffer)
    }
}

impl ExecuteBuilder<ExecutePagingState> {
    /// Set the paging state in the query frame.
    pub fn paging_state(mut self, paging_state: &Option<Vec<u8>>) -> ExecuteBuilder<ExecuteSerialConsistency> {
        // apply paging_state to query frame
        if let Some(paging_state) = paging_state {
            // add PAGING_STATE query_flag to the buffer
            self.buffer[self.stage.query_flags.index] |= PAGING_STATE;
            self.buffer.extend(&i32::to_be_bytes(paging_state.len() as i32));
            self.buffer.extend(paging_state);
        }
        // create query_serial_consistency
        let query_serial_consistency = ExecuteSerialConsistency {
            query_flags: self.stage.query_flags,
        };
        ExecuteBuilder::<ExecuteSerialConsistency> {
            buffer: self.buffer,
            stage: query_serial_consistency,
        }
    }
    /// Set serial consistency for the query frame.
    pub fn serial_consistency(mut self, consistency: Consistency) -> ExecuteBuilder<ExecuteTimestamp> {
        // add SERIAL_CONSISTENCY query_flag to the buffer
        self.buffer[self.stage.query_flags.index] |= SERIAL_CONSISTENCY;
        // apply serial_consistency to query frame
        self.buffer.extend(&u16::to_be_bytes(consistency as u16));
        // create execute_timestamp
        let execute_timestamp = ExecuteTimestamp {
            query_flags: self.stage.query_flags,
        };
        ExecuteBuilder::<ExecuteTimestamp> {
            buffer: self.buffer,
            stage: execute_timestamp,
        }
    }
    /// Set the timestamp of the query frame.
    pub fn timestamp(mut self, timestamp: i64) -> ExecuteBuilder<ExecuteBuild> {
        // add TIMESTAMP query_flag to the buffer
        self.buffer[self.stage.query_flags.index] |= TIMESTAMP;
        // apply timestamp to query frame
        self.buffer.extend(&BE_8_BYTES_LEN);
        self.buffer.extend(&i64::to_be_bytes(timestamp));
        // create query_build
        let execute_build = ExecuteBuild;
        ExecuteBuilder::<ExecuteBuild> {
            buffer: self.buffer,
            stage: execute_build,
        }
    }

    /// Build a query frame with an assigned compression type.
    pub fn build(mut self) -> Execute {
        // apply compression flag(if any to the header)
        self.buffer[1] |= MyCompression::flag();
        // apply compression to query frame
        self.buffer = MyCompression::get().compress(self.buffer);
        // create query
        Execute(self.buffer)
    }
}

impl ExecuteBuilder<ExecuteSerialConsistency> {
    /// Set serial consistency for the query frame.
    pub fn serial_consistency(mut self, consistency: Consistency) -> ExecuteBuilder<ExecuteTimestamp> {
        // add SERIAL_CONSISTENCY query_flag to the buffer
        self.buffer[self.stage.query_flags.index] |= SERIAL_CONSISTENCY;
        // apply serial_consistency to query frame
        self.buffer.extend(&u16::to_be_bytes(consistency as u16));
        // create query_timestamp
        let query_timestamp = ExecuteTimestamp {
            query_flags: self.stage.query_flags,
        };
        ExecuteBuilder::<ExecuteTimestamp> {
            buffer: self.buffer,
            stage: query_timestamp,
        }
    }
    /// Set the timestamp of the query frame.
    pub fn timestamp(mut self, timestamp: i64) -> ExecuteBuilder<ExecuteBuild> {
        // add TIMESTAMP query_flag to the buffer
        self.buffer[self.stage.query_flags.index] |= TIMESTAMP;
        // apply timestamp to query frame
        self.buffer.extend(&BE_8_BYTES_LEN);
        self.buffer.extend(&i64::to_be_bytes(timestamp));
        // create query_build
        let query_build = ExecuteBuild;
        ExecuteBuilder::<ExecuteBuild> {
            buffer: self.buffer,
            stage: query_build,
        }
    }

    /// Build a query frame with an assigned compression type.
    pub fn build(mut self) -> Execute {
        // apply compression flag(if any to the header)
        self.buffer[1] |= MyCompression::flag();
        // apply compression to query frame
        self.buffer = MyCompression::get().compress(self.buffer);
        // create query
        Execute(self.buffer)
    }
}

impl ExecuteBuilder<ExecuteTimestamp> {
    /// Set the timestamp of the query frame.
    pub fn timestamp(mut self, timestamp: i64) -> ExecuteBuilder<ExecuteBuild> {
        // add TIMESTAMP query_flag to the buffer
        self.buffer[self.stage.query_flags.index] |= TIMESTAMP;
        // apply timestamp to query frame
        self.buffer.extend(&BE_8_BYTES_LEN);
        self.buffer.extend(&i64::to_be_bytes(timestamp));
        // create query_build
        let query_build = ExecuteBuild;
        ExecuteBuilder::<ExecuteBuild> {
            buffer: self.buffer,
            stage: query_build,
        }
    }
    /// Build a Execute frame with an assigned compression type.
    pub fn build(mut self) -> Execute {
        // apply compression flag(if any to the header)
        self.buffer[1] |= MyCompression::flag();
        // apply compression to query frame
        self.buffer = MyCompression::get().compress(self.buffer);
        // create query
        Execute(self.buffer)
    }
}

impl ExecuteBuilder<ExecuteBuild> {
    /// Build a Execute frame with an assigned compression type.
    pub fn build(mut self) -> Execute {
        // apply compression flag(if any to the header)
        self.buffer[1] |= MyCompression::flag();
        // apply compression to query frame
        self.buffer = MyCompression::get().compress(self.buffer);
        // create query
        Execute(self.buffer)
    }
}

/// The Execute frame structure.
pub struct Execute(pub Vec<u8>);

impl Execute {
    /// Create Execute Cql frame
    pub fn new() -> ExecuteBuilder<ExecuteId> {
        ExecuteBuilder::<ExecuteHeader>::new()
    }
    /// Create Execute Cql frame with capacity
    pub fn with_capacity(capacity: usize) -> ExecuteBuilder<ExecuteId> {
        ExecuteBuilder::<ExecuteHeader>::with_capacity(capacity)
    }
}

impl Into<Vec<u8>> for Execute {
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
        let Execute(_payload) = Execute::new()
            .id(&[0; 16])
            .consistency(Consistency::One)
            .value("HASH_VALUE")
            .value("PAYLOAD_VALUE")
            .value("ADDRESS_VALUE")
            .value(0 as i64) // tx-value as i64
            .value("OBSOLETE_TAG_VALUE")
            .value(SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs() as i64) // junk timestamp
            .value(0 as i64) // current-index
            .value(0 as i64) // last-index
            .value("BUNDLE_HASH_VALUE")
            .value("TRUNK_VALUE")
            .value("BRANCH_VALUE")
            .value("TAG_VALUE")
            .value(0 as i64) // attachment_timestamp
            .value(0 as i64) // attachment_timestamp_lower
            .value(0 as i64) // attachment_timestamp_upper
            .value("NONCE_VALUE") // nonce
            .unset_value() // not-set value for milestone
            .build();
    }
}

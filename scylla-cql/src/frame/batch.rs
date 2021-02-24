// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

//! This module implements the batch query frame.

use super::{
    batchflags::*,
    consistency::Consistency,
    encoder::{ColumnEncoder, BE_8_BYTES_LEN, BE_NULL_BYTES_LEN, BE_UNSET_BYTES_LEN},
    opcode::BATCH,
    MD5_BE_LENGTH,
};
use crate::compression::{Compression, MyCompression};

/// Blanket cql frame header for BATCH frame.
const BATCH_HEADER: &'static [u8] = &[4, 0, 0, 0, BATCH, 0, 0, 0, 0];

/// The batch frame.
pub struct Batch(pub Vec<u8>);

#[repr(u8)]
/// The batch type enum.
pub enum BatchTypes {
    /// The batch will be logged.
    Logged = 0,
    /// The batch will be unlogged.
    Unlogged = 1,
    /// The batch will be a "counter" batch.
    Counter = 2,
}
pub struct BatchBuilder<Stage> {
    buffer: Vec<u8>,
    query_count: u16,
    stage: Stage,
}
pub struct BatchHeader;
pub struct BatchType;
pub struct BatchStatementOrId;
pub struct BatchValues {
    value_count: u16,
    index: usize,
}
pub struct BatchFlags;
pub struct BatchTimestamp;
pub struct BatchBuild;

impl BatchBuilder<BatchHeader> {
    pub fn new() -> BatchBuilder<BatchType> {
        let mut buffer: Vec<u8> = Vec::new();
        buffer.extend_from_slice(&BATCH_HEADER);
        BatchBuilder::<BatchType> {
            buffer,
            query_count: 0,
            stage: BatchType,
        }
    }
    pub fn with_capacity(capacity: usize) -> BatchBuilder<BatchType> {
        let mut buffer: Vec<u8> = Vec::with_capacity(capacity);
        buffer.extend_from_slice(&BATCH_HEADER);
        BatchBuilder::<BatchType> {
            buffer,
            query_count: 0,
            stage: BatchType,
        }
    }
}

impl BatchBuilder<BatchType> {
    /// Set the batch type in the Batch frame.
    pub fn batch_type(mut self, batch_type: BatchTypes) -> BatchBuilder<BatchStatementOrId> {
        // push batch_type and pad zero querycount
        self.buffer.extend(&[batch_type as u8, 0, 0]);
        BatchBuilder::<BatchStatementOrId> {
            buffer: self.buffer,
            query_count: self.query_count,
            stage: BatchStatementOrId,
        }
    }
    pub fn logged(mut self) -> BatchBuilder<BatchStatementOrId> {
        // push logged batch_type and pad zero querycount
        self.buffer.extend(&[0, 0, 0]);
        BatchBuilder::<BatchStatementOrId> {
            buffer: self.buffer,
            query_count: self.query_count,
            stage: BatchStatementOrId,
        }
    }
    pub fn unlogged(mut self) -> BatchBuilder<BatchStatementOrId> {
        // push unlogged batch_type and pad zero querycount
        self.buffer.extend(&[1, 0, 0]);
        BatchBuilder::<BatchStatementOrId> {
            buffer: self.buffer,
            query_count: self.query_count,
            stage: BatchStatementOrId,
        }
    }
    pub fn counter(mut self) -> BatchBuilder<BatchStatementOrId> {
        // push counter batch_type and pad zero querycount
        self.buffer.extend(&[2, 0, 0]);
        BatchBuilder::<BatchStatementOrId> {
            buffer: self.buffer,
            query_count: self.query_count,
            stage: BatchStatementOrId,
        }
    }
}

impl BatchBuilder<BatchStatementOrId> {
    /// Set the statement in the Batch frame.
    pub fn statement(mut self, statement: &str) -> BatchBuilder<BatchValues> {
        // normal query
        self.buffer.push(0);
        self.buffer.extend(&i32::to_be_bytes(statement.len() as i32));
        self.buffer.extend(statement.bytes());
        self.query_count += 1; // update querycount
        let index = self.buffer.len();
        // pad zero value_count for the query
        self.buffer.extend(&[0, 0]);
        BatchBuilder::<BatchValues> {
            buffer: self.buffer,
            query_count: self.query_count,
            stage: BatchValues { value_count: 0, index },
        }
    }
    /// Set the id in the Batch frame.
    pub fn id(mut self, id: &[u8; 16]) -> BatchBuilder<BatchValues> {
        // prepared query
        self.buffer.push(1);
        self.buffer.extend(&MD5_BE_LENGTH);
        self.buffer.extend(id);
        self.query_count += 1;
        let index = self.buffer.len();
        // pad zero value_count for the query
        self.buffer.extend(&[0, 0]);
        BatchBuilder::<BatchValues> {
            buffer: self.buffer,
            query_count: self.query_count,
            stage: BatchValues { value_count: 0, index },
        }
    }
}

impl BatchBuilder<BatchValues> {
    /// Set the value in the Batch frame.
    pub fn value(mut self, value: impl ColumnEncoder) -> Self {
        value.encode(&mut self.buffer);
        self.stage.value_count += 1;
        self
    }
    /// Set the value to be unset in the Batch frame.
    pub fn unset_value(mut self) -> Self {
        self.buffer.extend(&BE_UNSET_BYTES_LEN);
        self.stage.value_count += 1;
        self
    }
    /// Set the value to be null in the Batch frame.
    pub fn null_value(mut self) -> Self {
        self.buffer.extend(&BE_NULL_BYTES_LEN);
        self.stage.value_count += 1;
        self
    }
    /// Set the statement in the Batch frame.
    pub fn statement(mut self, statement: &str) -> BatchBuilder<BatchValues> {
        // adjust value_count for prev query(if any)
        self.buffer[self.stage.index..(self.stage.index + 2)]
            .copy_from_slice(&u16::to_be_bytes(self.stage.value_count));
        // normal query
        self.buffer.push(0);
        self.buffer.extend(&i32::to_be_bytes(statement.len() as i32));
        self.buffer.extend(statement.bytes());
        self.query_count += 1; // update querycount
                               // pad zero value_count for the query
        self.buffer.extend(&[0, 0]);
        let index = self.buffer.len();
        BatchBuilder::<BatchValues> {
            buffer: self.buffer,
            query_count: self.query_count,
            stage: BatchValues { value_count: 0, index },
        }
    }
    /// Set the id in the Batch frame.
    pub fn id(mut self, id: &[u8; 16]) -> BatchBuilder<BatchValues> {
        // adjust value_count for prev query
        self.buffer[self.stage.index..(self.stage.index + 2)]
            .copy_from_slice(&u16::to_be_bytes(self.stage.value_count));
        // prepared query
        self.buffer.push(1);
        self.buffer.extend(&MD5_BE_LENGTH);
        self.buffer.extend(id);
        self.query_count += 1;
        // pad zero value_count for the query
        self.buffer.extend(&[0, 0]);
        let index = self.buffer.len();
        BatchBuilder::<BatchValues> {
            buffer: self.buffer,
            query_count: self.query_count,
            stage: BatchValues { value_count: 0, index },
        }
    }
    /// Set the consistency of the Batch frame.
    pub fn consistency(mut self, consistency: Consistency) -> BatchBuilder<BatchFlags> {
        // adjust value_count for prev query
        self.buffer[self.stage.index..(self.stage.index + 2)]
            .copy_from_slice(&u16::to_be_bytes(self.stage.value_count));
        self.buffer.extend(&u16::to_be_bytes(consistency as u16));
        BatchBuilder::<BatchFlags> {
            buffer: self.buffer,
            query_count: self.query_count,
            stage: BatchFlags,
        }
    }
}

impl BatchBuilder<BatchFlags> {
    /// Set the serial consistency in the Batch frame.
    pub fn serial_consistency(mut self, consistency: Consistency) -> BatchBuilder<BatchTimestamp> {
        // add serial_consistency byte for batch flags
        self.buffer.push(SERIAL_CONSISTENCY);
        self.buffer.extend(&u16::to_be_bytes(consistency as u16));
        BatchBuilder::<BatchTimestamp> {
            buffer: self.buffer,
            query_count: self.query_count,
            stage: BatchTimestamp,
        }
    }
    /// Set the timestamp of the Batch frame.
    pub fn timestamp(mut self, timestamp: i64) -> BatchBuilder<BatchBuild> {
        // add timestamp byte for batch flags
        self.buffer.push(TIMESTAMP);
        self.buffer.extend(&BE_8_BYTES_LEN);
        self.buffer.extend(&i64::to_be_bytes(timestamp));
        BatchBuilder::<BatchBuild> {
            buffer: self.buffer,
            query_count: self.query_count,
            stage: BatchBuild,
        }
    }
    /// Build a Batch frame.
    pub fn build(mut self, compression: impl Compression) -> Batch {
        // apply compression flag(if any to the header)
        self.buffer[1] |= MyCompression::flag();
        // add noflags byte for batch flags
        self.buffer.push(NOFLAGS);
        // adjust the querycount
        self.buffer[10..12].copy_from_slice(&u16::to_be_bytes(self.query_count));
        self.buffer = compression.compress(self.buffer);
        Batch(self.buffer)
    }
}

impl BatchBuilder<BatchTimestamp> {
    /// Set the timestamp of the Batch frame.
    pub fn timestamp(mut self, timestamp: i64) -> BatchBuilder<BatchBuild> {
        *self.buffer.last_mut().unwrap() |= TIMESTAMP;
        self.buffer.extend(&BE_8_BYTES_LEN);
        self.buffer.extend(&i64::to_be_bytes(timestamp));
        BatchBuilder::<BatchBuild> {
            buffer: self.buffer,
            query_count: self.query_count,
            stage: BatchBuild,
        }
    }
    /// Build a Batch frame.
    pub fn build(mut self, compression: impl Compression) -> Batch {
        // apply compression flag(if any to the header)
        self.buffer[1] |= MyCompression::flag();
        // adjust the querycount
        self.buffer[10..12].copy_from_slice(&u16::to_be_bytes(self.query_count));
        self.buffer = compression.compress(self.buffer);
        Batch(self.buffer)
    }
}

impl BatchBuilder<BatchBuild> {
    /// Build a Batch frame.
    pub fn build(mut self, compression: impl Compression) -> Batch {
        // apply compression flag(if any to the header)
        self.buffer[1] |= MyCompression::flag();
        // adjust the querycount
        self.buffer[10..12].copy_from_slice(&u16::to_be_bytes(self.query_count));
        self.buffer = compression.compress(self.buffer);
        Batch(self.buffer)
    }
}
impl Batch {
    /// Create Batch cql frame
    pub fn new() -> BatchBuilder<BatchType> {
        BatchBuilder::<BatchHeader>::new()
    }
    /// Create Batch cql frame with capacity
    pub fn with_capacity(capacity: usize) -> BatchBuilder<BatchType> {
        BatchBuilder::<BatchHeader>::with_capacity(capacity)
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    use crate::compression::UNCOMPRESSED;
    use std::time::{SystemTime, UNIX_EPOCH};
    #[test]
    // note: junk data
    fn simple_query_builder_test() {
        let Batch(_payload) = Batch::new()
            .logged()
            .statement("INSERT_TX_QUERY")
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
            .id(&[0; 16]) // add second query(prepared one) to the batch
            .value("JUNK_VALUE") // junk value
            .consistency(Consistency::One)
            .build(UNCOMPRESSED); // build uncompressed batch
    }
}

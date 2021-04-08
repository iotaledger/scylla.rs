// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

//! This module implements the batch query frame.

use super::{
    batchflags::*,
    consistency::Consistency,
    encoder::{ColumnEncoder, BE_8_BYTES_LEN, BE_NULL_BYTES_LEN, BE_UNSET_BYTES_LEN},
    opcode::BATCH,
    Statements, Values, MD5_BE_LENGTH,
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
pub struct BatchBuilder<Type: Copy + Into<u8>, Stage> {
    buffer: Vec<u8>,
    query_count: u16,
    batch_type: Type,
    stage: Stage,
}
pub struct BatchHeader;

pub struct BatchType;
#[derive(Copy, Clone)]
pub struct BatchTypeUnset;
impl Into<u8> for BatchTypeUnset {
    fn into(self) -> u8 {
        panic!("Batch type is not set!")
    }
}
#[derive(Copy, Clone)]
pub struct BatchTypeLogged;
impl Into<u8> for BatchTypeLogged {
    fn into(self) -> u8 {
        0
    }
}
#[derive(Copy, Clone)]
pub struct BatchTypeUnlogged;
impl Into<u8> for BatchTypeUnlogged {
    fn into(self) -> u8 {
        1
    }
}
#[derive(Copy, Clone)]
pub struct BatchTypeCounter;
impl Into<u8> for BatchTypeCounter {
    fn into(self) -> u8 {
        2
    }
}
pub struct BatchStatementOrId;
pub struct BatchValues {
    value_count: u16,
    index: usize,
}
pub struct BatchFlags;
pub struct BatchTimestamp;
pub struct BatchBuild;

impl BatchBuilder<BatchTypeUnset, BatchHeader> {
    pub fn new() -> BatchBuilder<BatchTypeUnset, BatchType> {
        let mut buffer: Vec<u8> = Vec::new();
        buffer.extend_from_slice(&BATCH_HEADER);
        BatchBuilder {
            buffer,
            query_count: 0,
            batch_type: BatchTypeUnset,
            stage: BatchType,
        }
    }
    pub fn with_capacity(capacity: usize) -> BatchBuilder<BatchTypeUnset, BatchType> {
        let mut buffer: Vec<u8> = Vec::with_capacity(capacity);
        buffer.extend_from_slice(&BATCH_HEADER);
        BatchBuilder {
            buffer,
            query_count: 0,
            batch_type: BatchTypeUnset,
            stage: BatchType,
        }
    }
}

impl BatchBuilder<BatchTypeUnset, BatchType> {
    /// Set the batch type in the Batch frame.
    pub fn batch_type<Type: Copy + Into<u8>>(mut self, batch_type: Type) -> BatchBuilder<Type, BatchStatementOrId> {
        // push batch_type and pad zero querycount
        self.buffer.extend(&[batch_type.into(), 0, 0]);
        BatchBuilder {
            buffer: self.buffer,
            query_count: self.query_count,
            batch_type,
            stage: BatchStatementOrId,
        }
    }
    pub fn logged(mut self) -> BatchBuilder<BatchTypeLogged, BatchStatementOrId> {
        // push logged batch_type and pad zero querycount
        self.buffer.extend(&[0, 0, 0]);
        BatchBuilder {
            buffer: self.buffer,
            query_count: self.query_count,
            batch_type: BatchTypeLogged,
            stage: BatchStatementOrId,
        }
    }
    pub fn unlogged(mut self) -> BatchBuilder<BatchTypeUnlogged, BatchStatementOrId> {
        // push unlogged batch_type and pad zero querycount
        self.buffer.extend(&[1, 0, 0]);
        BatchBuilder {
            buffer: self.buffer,
            query_count: self.query_count,
            batch_type: BatchTypeUnlogged,
            stage: BatchStatementOrId,
        }
    }
    pub fn counter(mut self) -> BatchBuilder<BatchTypeCounter, BatchStatementOrId> {
        // push counter batch_type and pad zero querycount
        self.buffer.extend(&[2, 0, 0]);
        BatchBuilder {
            buffer: self.buffer,
            query_count: self.query_count,
            batch_type: BatchTypeCounter,
            stage: BatchStatementOrId,
        }
    }
}

impl<Type: Copy + Into<u8>> Statements for BatchBuilder<Type, BatchStatementOrId> {
    type Return = BatchBuilder<Type, BatchValues>;
    /// Set the statement in the Batch frame.
    fn statement(mut self, statement: &str) -> Self::Return {
        // normal query
        self.buffer.push(0);
        self.buffer.extend(&i32::to_be_bytes(statement.len() as i32));
        self.buffer.extend(statement.bytes());
        self.query_count += 1; // update querycount
        let index = self.buffer.len();
        // pad zero value_count for the query
        self.buffer.extend(&[0, 0]);
        BatchBuilder {
            buffer: self.buffer,
            query_count: self.query_count,
            batch_type: self.batch_type,
            stage: BatchValues { value_count: 0, index },
        }
    }
    /// Set the id in the Batch frame.
    fn id(mut self, id: &[u8; 16]) -> Self::Return {
        // prepared query
        self.buffer.push(1);
        self.buffer.extend(&MD5_BE_LENGTH);
        self.buffer.extend(id);
        self.query_count += 1;
        let index = self.buffer.len();
        // pad zero value_count for the query
        self.buffer.extend(&[0, 0]);
        BatchBuilder {
            buffer: self.buffer,
            query_count: self.query_count,
            batch_type: self.batch_type,
            stage: BatchValues { value_count: 0, index },
        }
    }
}

impl<Type: Copy + Into<u8>> Values for BatchBuilder<Type, BatchValues> {
    type Return = BatchBuilder<Type, BatchValues>;
    /// Set the value in the Batch frame.
    fn value<V: ColumnEncoder>(mut self, value: &V) -> Self {
        value.encode(&mut self.buffer);
        self.stage.value_count += 1;
        self
    }
    /// Set the value to be unset in the Batch frame.
    fn unset_value(mut self) -> Self {
        self.buffer.extend(&BE_UNSET_BYTES_LEN);
        self.stage.value_count += 1;
        self
    }
    /// Set the value to be null in the Batch frame.
    fn null_value(mut self) -> Self {
        self.buffer.extend(&BE_NULL_BYTES_LEN);
        self.stage.value_count += 1;
        self
    }
}

impl<Type: Copy + Into<u8>> Statements for BatchBuilder<Type, BatchValues> {
    type Return = Self;
    /// Set the statement in the Batch frame.
    fn statement(mut self, statement: &str) -> BatchBuilder<Type, BatchValues> {
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
        BatchBuilder {
            buffer: self.buffer,
            query_count: self.query_count,
            batch_type: self.batch_type,
            stage: BatchValues { value_count: 0, index },
        }
    }
    /// Set the id in the Batch frame.
    fn id(mut self, id: &[u8; 16]) -> BatchBuilder<Type, BatchValues> {
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
        BatchBuilder {
            buffer: self.buffer,
            query_count: self.query_count,
            batch_type: self.batch_type,
            stage: BatchValues { value_count: 0, index },
        }
    }
}
impl<Type: Copy + Into<u8>> BatchBuilder<Type, BatchValues> {
    /// Set the consistency of the Batch frame.
    pub fn consistency(mut self, consistency: Consistency) -> BatchBuilder<Type, BatchFlags> {
        // adjust value_count for prev query
        self.buffer[self.stage.index..(self.stage.index + 2)]
            .copy_from_slice(&u16::to_be_bytes(self.stage.value_count));
        self.buffer.extend(&u16::to_be_bytes(consistency as u16));
        BatchBuilder {
            buffer: self.buffer,
            query_count: self.query_count,
            batch_type: self.batch_type,
            stage: BatchFlags,
        }
    }
}

impl<Type: Copy + Into<u8>> BatchBuilder<Type, BatchFlags> {
    /// Set the serial consistency in the Batch frame.
    pub fn serial_consistency(mut self, consistency: Consistency) -> BatchBuilder<Type, BatchTimestamp> {
        // add serial_consistency byte for batch flags
        self.buffer.push(SERIAL_CONSISTENCY);
        self.buffer.extend(&u16::to_be_bytes(consistency as u16));
        BatchBuilder {
            buffer: self.buffer,
            query_count: self.query_count,
            batch_type: self.batch_type,
            stage: BatchTimestamp,
        }
    }
    /// Set the timestamp of the Batch frame.
    pub fn timestamp(mut self, timestamp: i64) -> BatchBuilder<Type, BatchBuild> {
        // add timestamp byte for batch flags
        self.buffer.push(TIMESTAMP);
        self.buffer.extend(&BE_8_BYTES_LEN);
        self.buffer.extend(&i64::to_be_bytes(timestamp));
        BatchBuilder {
            buffer: self.buffer,
            query_count: self.query_count,
            batch_type: self.batch_type,
            stage: BatchBuild,
        }
    }
    /// Build a Batch frame.
    pub fn build(mut self) -> anyhow::Result<Batch> {
        // apply compression flag(if any to the header)
        self.buffer[1] |= MyCompression::flag();
        // add noflags byte for batch flags
        self.buffer.push(NOFLAGS);
        // adjust the querycount
        self.buffer[10..12].copy_from_slice(&u16::to_be_bytes(self.query_count));
        self.buffer = MyCompression::get().compress(self.buffer)?;
        Ok(Batch(self.buffer))
    }
}

impl<Type: Copy + Into<u8>> BatchBuilder<Type, BatchTimestamp> {
    /// Set the timestamp of the Batch frame.
    pub fn timestamp(mut self, timestamp: i64) -> BatchBuilder<Type, BatchBuild> {
        self.buffer.last_mut().map(|last_byte| *last_byte |= TIMESTAMP);
        self.buffer.extend(&BE_8_BYTES_LEN);
        self.buffer.extend(&i64::to_be_bytes(timestamp));
        BatchBuilder {
            buffer: self.buffer,
            query_count: self.query_count,
            batch_type: self.batch_type,
            stage: BatchBuild,
        }
    }
    /// Build a Batch frame.
    pub fn build(mut self) -> anyhow::Result<Batch> {
        // apply compression flag(if any to the header)
        self.buffer[1] |= MyCompression::flag();
        // adjust the querycount
        self.buffer[10..12].copy_from_slice(&u16::to_be_bytes(self.query_count));
        self.buffer = MyCompression::get().compress(self.buffer)?;
        Ok(Batch(self.buffer))
    }
}

impl<Type: Copy + Into<u8>> BatchBuilder<Type, BatchBuild> {
    /// Build a Batch frame.
    pub fn build(mut self) -> anyhow::Result<Batch> {
        // apply compression flag(if any to the header)
        self.buffer[1] |= MyCompression::flag();
        // adjust the querycount
        self.buffer[10..12].copy_from_slice(&u16::to_be_bytes(self.query_count));
        self.buffer = MyCompression::get().compress(self.buffer)?;
        Ok(Batch(self.buffer))
    }
}
impl Batch {
    /// Create Batch cql frame
    pub fn new() -> BatchBuilder<BatchTypeUnset, BatchType> {
        BatchBuilder::new()
    }
    /// Create Batch cql frame with capacity
    pub fn with_capacity(capacity: usize) -> BatchBuilder<BatchTypeUnset, BatchType> {
        BatchBuilder::with_capacity(capacity)
    }
}
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    // note: junk data
    fn simple_query_builder_test() {
        let Batch(_payload) = Batch::new()
            .logged()
            .statement("INSERT_TX_QUERY")
            .value(&"HASH_VALUE")
            .value(&"PAYLOAD_VALUE")
            .id(&[0; 16]) // add second query(prepared one) to the batch
            .value(&"JUNK_VALUE") // junk value
            .consistency(Consistency::One)
            .build()
            .unwrap();
    }
}

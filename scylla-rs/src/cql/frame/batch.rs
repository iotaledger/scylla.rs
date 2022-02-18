// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

//! This module implements the batch query frame.

use super::{
    batchflags::*,
    consistency::Consistency,
    encoder::{
        ColumnEncoder,
        BE_8_BYTES_LEN,
        BE_NULL_BYTES_LEN,
        BE_UNSET_BYTES_LEN,
    },
    opcode::BATCH,
    Binder,
    FrameBuilder,
    MD5_BE_LENGTH,
};
use thiserror::Error;

/// Blanket cql frame header for BATCH frame.
const BATCH_HEADER: [u8; 5] = [4, 0, 0, 0, BATCH];

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

/// Batch request builder. Maintains a type-gated stage so that operations
/// are applied in a valid order.
///
/// ## Example
/// ```
/// use scylla_rs::cql::{
///     Batch,
///     Consistency,
///     Statements,
/// };
///
/// let builder = Batch::new();
/// let batch = builder
///     .logged()
///     .statement("statement")
///     .consistency(Consistency::One)
///     .build()?;
/// let payload = batch.0;
/// # Ok::<(), anyhow::Error>(())
/// ```
#[derive(Clone, Debug)]
pub struct BatchBuilder {
    stmt_builders: Vec<BatchStatementBuilder>,
    batch_type: BatchType,
    consistency: Consistency,
    serial_consistency: Option<Consistency>,
    timestamp: Option<i64>,
}

impl Default for BatchBuilder {
    fn default() -> Self {
        Self {
            stmt_builders: Default::default(),
            batch_type: Default::default(),
            consistency: Consistency::Quorum,
            serial_consistency: Default::default(),
            timestamp: Default::default(),
        }
    }
}

#[derive(Clone, Debug, Default)]
pub struct BatchStatementBuilder {
    stmt_buf: Vec<u8>,
    val_buf: Option<Vec<u8>>,
    value_count: u16,
}

/// Gating type for batch headers
#[derive(Copy, Clone)]
pub struct BatchHeader;

/// Gating type for batch type
#[derive(Copy, Clone, Debug)]
#[repr(u8)]
pub enum BatchType {
    /// The batch will be logged.
    Logged = 0,
    /// The batch will be unlogged.
    Unlogged = 1,
    /// The batch will be a "counter" batch.
    Counter = 2,
}

impl Default for BatchType {
    fn default() -> Self {
        BatchType::Logged
    }
}

#[derive(Debug, Error)]
pub enum BatchBuildError {
    #[error("No batch statements provided")]
    NoStatements,
}

impl BatchBuilder {
    /// Set the batch type in the Batch frame. See https://cassandra.apache.org/doc/latest/cql/dml.html#batch
    pub fn batch_type(mut self, batch_type: BatchType) -> Self {
        // push batch_type and pad zero querycount
        // self.buffer.extend(&[batch_type.into(), 0, 0]);
        self.batch_type = batch_type;
        self
    }
    /// Set the batch type to logged. See https://cassandra.apache.org/doc/latest/cql/dml.html#batch
    pub fn logged(mut self) -> Self {
        self.batch_type = BatchType::Logged;
        self
    }
    /// Set the batch type to unlogged. See https://cassandra.apache.org/doc/latest/cql/dml.html#unlogged-batches
    pub fn unlogged(mut self) -> Self {
        self.batch_type = BatchType::Unlogged;
        self
    }
    /// Set the batch type to counter. See https://cassandra.apache.org/doc/latest/cql/dml.html#counter-batches
    pub fn counter(mut self) -> Self {
        self.batch_type = BatchType::Counter;
        self
    }

    /// Set the statement in the Batch frame.
    pub fn statement(mut self, statement: &str) -> Self {
        // normal query
        let mut buf = Vec::new();
        buf.push(0);
        buf.extend(&i32::to_be_bytes(statement.len() as i32));
        buf.extend(statement.bytes());
        self.stmt_builders.push(BatchStatementBuilder {
            stmt_buf: buf,
            ..Default::default()
        });
        self
    }
    /// Set the id in the Batch frame.
    pub fn id(mut self, id: &[u8; 16]) -> Self {
        // prepared query
        let mut buf = Vec::new();
        buf.push(1);
        buf.extend(&MD5_BE_LENGTH);
        buf.extend(id);
        self.stmt_builders.push(BatchStatementBuilder {
            stmt_buf: buf,
            ..Default::default()
        });
        self
    }

    /// Set the consistency of the Batch frame.
    pub fn consistency(mut self, consistency: Consistency) -> Self {
        self.consistency = consistency;
        self
    }

    /// Set the serial consistency in the Batch frame.
    pub fn serial_consistency(mut self, serial_consistency: Consistency) -> Self {
        self.serial_consistency.replace(serial_consistency);
        self
    }
    /// Set the timestamp of the Batch frame.
    pub fn timestamp(mut self, timestamp: i64) -> Self {
        self.timestamp.replace(timestamp);
        self
    }
    /// Build a Batch frame.
    pub fn build(self) -> Result<Batch, BatchBuildError> {
        if self.stmt_builders.is_empty() {
            return Err(BatchBuildError::NoStatements);
        }
        let mut body_buf = Vec::new();
        // add batch type
        body_buf.push(self.batch_type as u8);
        // add query count
        body_buf.extend_from_slice(&u16::to_be_bytes(self.stmt_builders.len() as u16));
        for stmt_builder in self.stmt_builders {
            body_buf.extend(stmt_builder.stmt_buf);
            body_buf.extend(u16::to_be_bytes(stmt_builder.value_count));
            if let Some(val_buf) = stmt_builder.val_buf.as_ref() {
                body_buf.extend(val_buf);
            }
        }
        // add consistency
        body_buf.extend(&u16::to_be_bytes(self.consistency as u16));
        let flags_idx = body_buf.len();
        body_buf.push(NOFLAGS);
        // add serial consistency
        if let Some(serial_consistency) = self.serial_consistency {
            body_buf[flags_idx] |= SERIAL_CONSISTENCY;
            body_buf.extend(&u16::to_be_bytes(serial_consistency as u16));
        }
        // add timestamp
        if let Some(timestamp) = self.timestamp {
            body_buf[flags_idx] |= TIMESTAMP;
            body_buf.extend(&BE_8_BYTES_LEN);
            body_buf.extend(&i64::to_be_bytes(timestamp));
        }
        Ok(Batch(FrameBuilder::build(BATCH_HEADER, body_buf)))
    }
}

#[derive(Debug, Error)]
pub enum BatchBindError {
    #[error("No statements to bind values for")]
    NoStatements,
    #[error("Batch encode error: {0}")]
    EncodeError(#[from] anyhow::Error),
}

impl Binder for BatchBuilder {
    type Error = BatchBindError;
    /// Set the value in the Batch frame.
    fn value<V: ColumnEncoder>(mut self, value: &V) -> Result<Self, Self::Error>
    where
        Self: Sized,
    {
        if let Some(stmt_builder) = self.stmt_builders.last_mut() {
            stmt_builder.value_count += 1;
            let buf = stmt_builder.val_buf.get_or_insert_with(|| Vec::new());
            value.encode(buf).map_err(|e| anyhow::anyhow!("{:?}", e))?;
            Ok(self)
        } else {
            Err(BatchBindError::NoStatements)
        }
    }

    fn named_value<V: ColumnEncoder>(mut self, name: &str, value: &V) -> Result<Self, Self::Error>
    where
        Self: Sized,
    {
        todo!()
    }

    /// Set the value to be unset in the Batch frame.
    fn unset_value(mut self) -> Result<Self, Self::Error>
    where
        Self: Sized,
    {
        if let Some(stmt_builder) = self.stmt_builders.last_mut() {
            stmt_builder.value_count += 1;
            let buf = stmt_builder.val_buf.get_or_insert_with(|| Vec::new());
            buf.extend(&BE_UNSET_BYTES_LEN);
            Ok(self)
        } else {
            Err(BatchBindError::NoStatements)
        }
    }

    /// Set the value to be null in the Batch frame.
    fn null_value(mut self) -> Result<Self, Self::Error>
    where
        Self: Sized,
    {
        if let Some(stmt_builder) = self.stmt_builders.last_mut() {
            stmt_builder.value_count += 1;
            let buf = stmt_builder.val_buf.get_or_insert_with(|| Vec::new());
            buf.extend(&BE_NULL_BYTES_LEN);
            Ok(self)
        } else {
            Err(BatchBindError::NoStatements)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    // note: junk data
    fn simple_query_builder_test() {
        let Batch(_payload) = BatchBuilder::default()
            .logged()
            .statement("INSERT_TX_QUERY")
            .value(&"HASH_VALUE")
            .unwrap()
            .value(&"PAYLOAD_VALUE")
            .unwrap()
            .id(&[0; 16]) // add second query(prepared one) to the batch
            .value(&"JUNK_VALUE")
            .unwrap() // junk value
            .consistency(Consistency::One)
            .build()
            .unwrap();
    }
}

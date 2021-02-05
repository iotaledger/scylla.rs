// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

//! This module implements the Prepare frame.

use super::opcode::PREPARE;

use crate::compression::{Compression, MyCompression};

/// Blanket cql frame header for prepare frame.
const PREPARE_HEADER: &'static [u8] = &[4, 0, 0, 0, PREPARE, 0, 0, 0, 0];

/// The prepare frame structure.
pub struct Prepare(Vec<u8>);

struct PrepareBuilder<Stage> {
    buffer: Vec<u8>,
    stage: Stage,
}
struct PrepareHeader;
struct PrepareStatement;
struct PrepareBuild;

impl PrepareBuilder<PrepareHeader> {
    pub fn new() -> PrepareBuilder<PrepareStatement> {
        let mut buffer: Vec<u8> = Vec::new();
        buffer.extend_from_slice(&PREPARE_HEADER);
        PrepareBuilder::<PrepareStatement> {
            buffer,
            stage: PrepareStatement,
        }
    }
    pub fn with_capacity(capacity: usize) -> PrepareBuilder<PrepareStatement> {
        let mut buffer: Vec<u8> = Vec::with_capacity(capacity);
        buffer.extend_from_slice(&PREPARE_HEADER);
        PrepareBuilder::<PrepareStatement> {
            buffer,
            stage: PrepareStatement,
        }
    }
}

impl PrepareBuilder<PrepareStatement> {
    /// The statement for preparation.
    pub fn statement(mut self, statement: &str) -> PrepareBuilder<PrepareBuild> {
        self.buffer.extend(&i32::to_be_bytes(statement.len() as i32));
        self.buffer.extend(statement.bytes());
        PrepareBuilder::<PrepareBuild> {
            buffer: self.buffer,
            stage: PrepareBuild,
        }
    }
}

impl PrepareBuilder<PrepareBuild> {
    /// Build the prepare frame with an assigned compression type.
    pub fn build(mut self, compression: impl Compression) -> Prepare {
        // apply compression flag(if any to the header)
        self.buffer[1] |= MyCompression::flag();
        self.buffer = compression.compress(self.buffer);
        Prepare(self.buffer)
    }
}

impl Prepare {
    fn new() -> PrepareBuilder<PrepareStatement> {
        PrepareBuilder::<PrepareHeader>::new()
    }
    fn with_capacity(capacity: usize) -> PrepareBuilder<PrepareStatement> {
        PrepareBuilder::<PrepareHeader>::with_capacity(capacity)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::compression::UNCOMPRESSED;
    #[test]
    // note: junk data
    fn simple_prepare_builder_test() {
        let Prepare(_payload) = Prepare::new().statement("INSERT_TX_QUERY").build(UNCOMPRESSED); // build uncompressed
    }
}

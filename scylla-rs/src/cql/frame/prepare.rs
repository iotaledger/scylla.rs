// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

//! This module implements the Prepare frame.

use super::{
    opcode::PREPARE,
    FrameBuilder,
};
use thiserror::Error;

/// Blanket cql frame header for prepare frame.
const PREPARE_HEADER: [u8; 5] = [4, 0, 0, 0, PREPARE];

/// The prepare frame structure.
#[derive(Debug, Clone)]
pub struct Prepare(pub Vec<u8>);

#[derive(Debug, Clone, Default)]
pub struct PrepareBuilder {
    stmt: Option<Vec<u8>>,
}

#[derive(Debug, Error)]
pub enum PrepareBuildError {
    #[error("No query statement provided")]
    NoStatement,
}

impl PrepareBuilder {
    /// The statement for preparation.
    pub fn statement(mut self, statement: &str) -> Self {
        let mut buf = Vec::new();
        buf.extend(&i32::to_be_bytes(statement.len() as i32));
        buf.extend(statement.bytes());
        self.stmt.replace(buf);
        self
    }

    /// Build the prepare frame with an assigned compression type.
    pub fn build(mut self) -> Result<Prepare, PrepareBuildError> {
        Ok(Prepare(FrameBuilder::build(
            PREPARE_HEADER,
            self.stmt.take().ok_or_else(|| PrepareBuildError::NoStatement)?,
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    // note: junk data
    fn simple_prepare_builder_test() {
        let Prepare(_payload) = PrepareBuilder::default().statement("INSERT_TX_QUERY").build().unwrap();
    }
}

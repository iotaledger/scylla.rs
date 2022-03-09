// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

//! This module implements the PREPARE frame.

use super::*;

/// Prepare a query for later execution (through EXECUTE). The body consists of
/// the CQL query to prepare as a `[long string]`.
///
/// The server will respond with a RESULT message with a `prepared` kind.
#[derive(Clone, Debug, Builder)]
#[builder(derive(Clone, Debug))]
#[builder(pattern = "owned")]
pub struct PrepareFrame {
    #[allow(missing_docs)]
    pub(crate) statement: String,
}

impl PrepareFrame {
    /// Create a prepare frame from a statement.
    pub fn new(statement: String) -> Self {
        Self { statement }
    }

    /// Get the statement to be prepared.
    pub fn statement(&self) -> &String {
        &self.statement
    }
}

impl FromPayload for PrepareFrame {
    fn from_payload(start: &mut usize, payload: &[u8]) -> anyhow::Result<Self> {
        Ok(Self {
            statement: read_long_string(start, payload)?,
        })
    }
}

impl ToPayload for PrepareFrame {
    fn to_payload(self, payload: &mut Vec<u8>) {
        write_long_string(&self.statement, payload);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::prelude::Uncompressed;

    #[test]
    fn simple_prepare_builder_test() {
        let _payload = PrepareFrameBuilder::default()
            .statement("INSERT_TX_QUERY".to_owned())
            .build()
            .unwrap()
            .encode::<Uncompressed>()
            .unwrap();
    }
}

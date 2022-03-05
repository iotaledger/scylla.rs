// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

//! This module implements the Prepare frame.

use super::*;

/// The prepare frame structure.
#[derive(Clone, Debug, Builder)]
#[builder(derive(Clone, Debug))]
#[builder(pattern = "owned")]
pub struct PrepareFrame {
    pub(crate) statement: String,
}

impl PrepareFrame {
    pub fn new(statement: String) -> Self {
        Self { statement }
    }

    pub fn statement(&self) -> &str {
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

    #[test]
    fn simple_prepare_builder_test() {
        let frame = PrepareFrameBuilder::default()
            .statement("INSERT_TX_QUERY".to_owned())
            .build()
            .unwrap();
    }
}

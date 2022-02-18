// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

//! This module implements the Options frame.

use super::opcode::OPTIONS;

/// Blanket cql frame header for OPTIONS frame.
const OPTIONS_HEADER: [u8; 9] = [4, 0, 0, 0, OPTIONS, 0, 0, 0, 0];

/// The Options frame structure.
pub(crate) struct Options(pub Vec<u8>);

#[derive(Default, Clone, Debug)]
pub(crate) struct OptionsBuilder {
    buffer: Vec<u8>,
}

impl OptionsBuilder {
    pub fn build(mut self) -> Options {
        self.buffer.extend(OPTIONS_HEADER);
        Options(self.buffer)
    }
}

impl Options {
    pub fn new() -> Options {
        OptionsBuilder::default().build()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    // note: junk data
    fn simple_options_builder_test() {
        let Options(_payload) = Options::new(); // build uncompressed
    }
}

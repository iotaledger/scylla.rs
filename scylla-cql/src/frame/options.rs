// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

//! This module implements the Options frame.

use super::opcode::OPTIONS;

/// Blanket cql frame header for OPTIONS frame.
const OPTIONS_HEADER: &'static [u8] = &[4, 0, 0, 0, OPTIONS, 0, 0, 0, 0];

/// The Options frame structure.
pub(crate) struct Options(pub Vec<u8>);

pub(crate) struct OptionsBuilder<Stage> {
    buffer: Vec<u8>,
    stage: Stage,
}

struct OptionsHeader;
pub(crate) struct OptionsBuild;

impl OptionsBuilder<OptionsHeader> {
    pub fn new() -> OptionsBuilder<OptionsBuild> {
        let mut buffer: Vec<u8> = Vec::new();
        buffer.extend_from_slice(&OPTIONS_HEADER);
        OptionsBuilder::<OptionsBuild> {
            buffer,
            stage: OptionsBuild,
        }
    }
    pub fn with_capacity(capacity: usize) -> OptionsBuilder<OptionsBuild> {
        let mut buffer: Vec<u8> = Vec::with_capacity(capacity);
        buffer.extend_from_slice(&OPTIONS_HEADER);
        OptionsBuilder::<OptionsBuild> {
            buffer,
            stage: OptionsBuild,
        }
    }
}

impl OptionsBuilder<OptionsBuild> {
    pub(crate) fn build(self) -> Options {
        Options(self.buffer)
    }
}

impl Options {
    pub(crate) fn new() -> OptionsBuilder<OptionsBuild> {
        OptionsBuilder::<OptionsHeader>::new()
    }
    fn with_capacity(capacity: usize) -> OptionsBuilder<OptionsBuild> {
        OptionsBuilder::<OptionsHeader>::with_capacity(capacity)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    // note: junk data
    fn simple_options_builder_test() {
        let Options(_payload) = Options::new().build(); // build uncompressed
    }
}

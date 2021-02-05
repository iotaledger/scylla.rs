// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

//! This module implements the Startup frame.

use super::opcode::STARTUP;
use std::collections::HashMap;

pub(crate) struct StartupBuilder<Stage> {
    buffer: Vec<u8>,
    stage: Stage,
}
struct StartupHeader;
pub(crate) struct StartupOptions;
pub(crate) struct StartupBuild;

/// The Startup frame.
pub(crate) struct Startup(pub Vec<u8>);

/// Blanket cql frame header for startup frame.
const STARTUP_HEADER: &'static [u8] = &[4, 0, 0, 0, STARTUP, 0, 0, 0, 0];

impl StartupBuilder<StartupHeader> {
    pub fn new() -> StartupBuilder<StartupOptions> {
        let mut buffer: Vec<u8> = Vec::new();
        buffer.extend_from_slice(&STARTUP_HEADER);
        StartupBuilder::<StartupOptions> {
            buffer,
            stage: StartupOptions,
        }
    }
    pub fn with_capacity(capacity: usize) -> StartupBuilder<StartupOptions> {
        let mut buffer: Vec<u8> = Vec::with_capacity(capacity);
        buffer.extend_from_slice(&STARTUP_HEADER);
        StartupBuilder::<StartupOptions> {
            buffer,
            stage: StartupOptions,
        }
    }
}

impl StartupBuilder<StartupOptions> {
    /// Update the options in Startup frame.
    pub fn options(mut self, map: &HashMap<String, String>) -> StartupBuilder<StartupBuild> {
        self.buffer.extend(&u16::to_be_bytes(map.keys().len() as u16));
        for (k, v) in map {
            self.buffer.extend(&u16::to_be_bytes(k.len() as u16));
            self.buffer.extend(k.bytes());
            self.buffer.extend(&u16::to_be_bytes(v.len() as u16));
            self.buffer.extend(v.bytes());
        }
        let body_length = i32::to_be_bytes((self.buffer.len() as i32) - 9);
        self.buffer[5..9].copy_from_slice(&body_length);
        StartupBuilder {
            buffer: self.buffer,
            stage: StartupBuild,
        }
    }
}

impl StartupBuilder<StartupBuild> {
    /// Build the Startup frame.
    pub fn build(self) -> Startup {
        Startup(self.buffer)
    }
}
impl Startup {
    pub(crate) fn new() -> StartupBuilder<StartupOptions> {
        StartupBuilder::<StartupHeader>::new()
    }
    fn with_capacity(capacity: usize) -> StartupBuilder<StartupOptions> {
        StartupBuilder::<StartupHeader>::with_capacity(capacity)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    // note: junk data
    fn simple_startup_builder_test() {
        let mut options = HashMap::new();
        options.insert("CQL_VERSION".to_string(), "3.0.0".to_string());

        let Startup(_payload) = Startup::new().options(&options).build(); // build uncompressed
    }
}

// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

//! This module implements the Startup frame.

use super::{
    opcode::STARTUP,
    FrameBuilder,
};
use std::collections::HashMap;
use thiserror::Error;

#[derive(Clone, Debug, Default)]
pub struct StartupBuilder {
    opts_buf: Option<Vec<u8>>,
}

/// The Startup frame.
pub struct Startup(pub Vec<u8>);

/// Blanket cql frame header for startup frame.
const STARTUP_HEADER: [u8; 5] = [4, 0, 0, 0, STARTUP];

#[derive(Debug, Error)]
pub enum StartupBuildError {
    #[error("No options provided")]
    NoOptions,
}

impl StartupBuilder {
    /// Update the options in Startup frame.
    pub fn options(&mut self, map: &HashMap<String, String>) -> &mut Self {
        let mut buf = Vec::new();
        buf.extend(&u16::to_be_bytes(map.keys().len() as u16));
        for (k, v) in map {
            buf.extend(&u16::to_be_bytes(k.len() as u16));
            buf.extend(k.bytes());
            buf.extend(&u16::to_be_bytes(v.len() as u16));
            buf.extend(v.bytes());
        }
        self.opts_buf.replace(buf);
        self
    }

    /// Build the Startup frame.
    pub fn build(&self) -> Result<Startup, StartupBuildError> {
        Ok(Startup(FrameBuilder::build(
            STARTUP_HEADER,
            self.opts_buf.as_ref().ok_or_else(|| StartupBuildError::NoOptions)?,
        )))
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

        let Startup(_payload) = StartupBuilder::default().options(&options).build().unwrap();
    }
}

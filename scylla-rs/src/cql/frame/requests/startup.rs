// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

//! This module implements the Startup frame.

use super::*;
use std::collections::HashMap;

/// The Startup frame.
#[derive(Clone, Debug, Builder)]
#[builder(derive(Clone, Debug))]
#[builder(pattern = "owned")]
pub struct StartupFrame {
    pub(crate) options: HashMap<String, String>,
}

impl FromPayload for StartupFrame {
    fn from_payload(start: &mut usize, payload: &[u8]) -> anyhow::Result<Self> {
        Ok(Self {
            options: read_string_map(start, payload)?,
        })
    }
}

impl ToPayload for StartupFrame {
    fn to_payload(self, payload: &mut Vec<u8>) {
        write_string_map(&self.options, payload);
    }
}

impl StartupFrame {
    pub fn new(options: HashMap<String, String>) -> Self {
        Self { options }
    }

    pub fn options(&self) -> &HashMap<String, String> {
        &self.options
    }
}

impl StartupFrameBuilder {
    pub fn with_option(mut self, key: String, value: String) -> Self {
        match self.options {
            Some(ref mut options) => {
                options.insert(key, value);
            }
            None => {
                let mut options = HashMap::new();
                options.insert(key, value);
                self.options = Some(options);
            }
        }
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn simple_startup_builder_test() {
        let _payload = StartupFrameBuilder::default()
            .with_option("CQL_VERSION".to_string(), "3.0.0".to_string())
            .build()
            .unwrap();
    }
}

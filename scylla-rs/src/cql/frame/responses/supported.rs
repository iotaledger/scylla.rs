// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

//! This module implements the Supported frame.

use super::*;
use std::collections::HashMap;

/// The supported frame with options field.
#[derive(Clone, Debug)]
pub struct SupportedFrame {
    pub options: HashMap<String, Vec<String>>,
}

impl SupportedFrame {
    /// Get the options in the Supported frame.
    pub fn options(&self) -> &HashMap<String, Vec<String>> {
        &self.options
    }
}

impl FromPayload for SupportedFrame {
    fn from_payload(start: &mut usize, payload: &[u8]) -> anyhow::Result<Self> {
        Ok(Self {
            options: read_string_multimap(start, payload)?,
        })
    }
}

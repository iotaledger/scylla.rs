// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

//! This module implements the SUPPORTED frame.

use super::*;
use std::collections::HashMap;

/// Indicates which startup options are supported by the server. This message
/// comes as a response to an [`OptionsFrame`] message.
///
/// The body of a SUPPORTED message is a `[string multimap]`. This multimap gives
/// for each of the supported [`StartupFrame`] options, the list of supported values.
#[derive(Clone, Debug)]
pub struct SupportedFrame {
    /// Supported options
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

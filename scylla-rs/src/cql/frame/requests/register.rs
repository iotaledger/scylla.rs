// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

//! This module implements the REGISTER frame.

use super::*;

#[derive(Clone, Debug, Builder)]
#[builder(derive(Clone, Debug))]
#[builder(pattern = "owned")]
pub struct RegisterFrame {
    pub(crate) event_types: Vec<String>,
}

impl FromPayload for RegisterFrame {
    fn from_payload(start: &mut usize, payload: &[u8]) -> anyhow::Result<Self> {
        Ok(Self {
            event_types: read_string_list(start, payload)?,
        })
    }
}

impl ToPayload for RegisterFrame {
    fn to_payload(self, payload: &mut Vec<u8>) {
        write_string_list(&self.event_types, payload);
    }
}

impl RegisterFrameBuilder {
    pub fn with_event_type(mut self, event_type: String) -> Self {
        match self.event_types {
            Some(ref mut event_types) => {
                event_types.push(event_type);
            }
            None => {
                let mut event_types = Vec::new();
                event_types.push(event_type);
                self.event_types = Some(event_types);
            }
        }
        self
    }
}

// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

//! This module implements the REGISTER frame.

use std::fmt::Display;

use super::*;

/**
   Register this connection to receive some types of events. The body of the
   message is a `[string list]` representing the event types to register for.

   The response to a REGISTER message will be a [`ReadyFrame`] message.

   Please note that if a client driver maintains multiple connections to a
   Cassandra node and/or connections to multiple nodes, it is advised to
   dedicate a handful of connections to receive events, but to *not* register
   for events on all connections, as this would only result in receiving
   multiple times the same event messages, wasting bandwidth.
*/
#[derive(Clone, Debug, Builder)]
#[builder(derive(Clone, Debug))]
#[builder(pattern = "owned")]
pub struct RegisterFrame {
    /// The event types to request
    pub(crate) event_types: Vec<RegisterEventType>,
}

impl FromPayload for RegisterFrame {
    fn from_payload(start: &mut usize, payload: &[u8]) -> anyhow::Result<Self> {
        Ok(Self {
            event_types: read_list(start, payload)?,
        })
    }
}

impl ToPayload for RegisterFrame {
    fn to_payload(self, payload: &mut Vec<u8>) {
        write_list(self.event_types, payload);
    }
}

impl RegisterFrameBuilder {
    /// Add an event type to the list of event types to request.
    pub fn with_event_type(mut self, event_type: RegisterEventType) -> Self {
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

/// Event types
#[derive(Clone, Debug)]
#[allow(missing_docs)]
pub enum RegisterEventType {
    TopologyChange,
    StatusChange,
    SchemaChange,
}

impl Display for RegisterEventType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RegisterEventType::TopologyChange => write!(f, "TOPOLOGY_CHANGE"),
            RegisterEventType::StatusChange => write!(f, "STATUS_CHANGE"),
            RegisterEventType::SchemaChange => write!(f, "SCHEMA_CHANGE"),
        }
    }
}

impl FromPayload for RegisterEventType {
    fn from_payload(start: &mut usize, payload: &[u8]) -> anyhow::Result<Self> {
        Ok(match read_str(start, payload)? {
            "TOPOLOGY_CHANGE" => RegisterEventType::TopologyChange,
            "STATUS_CHANGE" => RegisterEventType::StatusChange,
            "SCHEMA_CHANGE" => RegisterEventType::SchemaChange,
            e => anyhow::bail!("Unknown event type: {}", e),
        })
    }
}

impl ToPayload for RegisterEventType {
    fn to_payload(self, payload: &mut Vec<u8>) {
        write_string(&self.to_string(), payload);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::prelude::Uncompressed;

    #[test]
    fn simple_register_builder_test() {
        let _payload = RegisterFrameBuilder::default()
            .with_event_type(RegisterEventType::TopologyChange)
            .build()
            .unwrap()
            .encode::<Uncompressed>()
            .unwrap();
    }
}

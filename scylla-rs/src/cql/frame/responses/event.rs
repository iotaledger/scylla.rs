// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

//! This module implements the EVENT frame.

use super::*;
use std::{
    net::SocketAddr,
    str::FromStr,
};

/// An event pushed by the server. A client will only receive events for the types it has registered to using a
/// [`RegisterFrame`].
#[derive(Clone, Debug)]
pub struct EventFrame {
    /// The event type.
    pub event_type: EventType,
}

impl EventFrame {
    /// Get the event type.
    pub fn event_type(&self) -> &EventType {
        &self.event_type
    }
}

impl FromPayload for EventFrame {
    fn from_payload(start: &mut usize, payload: &[u8]) -> anyhow::Result<Self> {
        Ok(Self {
            event_type: EventType::from_payload(start, payload)?,
        })
    }
}

/// Event types
#[derive(Clone, Debug)]
#[allow(missing_docs)]
pub enum EventType {
    TopologyChange {
        change_type: TopologyChangeType,
        address: SocketAddr,
    },
    StatusChange {
        change_type: StatusChangeType,
        address: SocketAddr,
    },
    SchemaChange {
        change_type: SchemaChangeType,
        target: SchemaChangeTarget,
    },
}

impl FromPayload for EventType {
    fn from_payload(start: &mut usize, payload: &[u8]) -> anyhow::Result<Self> {
        Ok(match read_str(start, payload)? {
            "TOPOLOGY_CHANGE" => Self::TopologyChange {
                change_type: TopologyChangeType::from_payload(start, payload)?,
                address: read_inet(start, payload)?,
            },
            "STATUS_CHANGE" => Self::StatusChange {
                change_type: StatusChangeType::from_payload(start, payload)?,
                address: read_inet(start, payload)?,
            },
            "SCHEMA_CHANGE" => Self::SchemaChange {
                change_type: SchemaChangeType::from_payload(start, payload)?,
                target: SchemaChangeTarget::from_payload(start, payload)?,
            },
            e => anyhow::bail!("Unknown event type: {}", e),
        })
    }
}

/// Events related to change in the cluster topology. Currently, events are sent
/// when new nodes are added to the cluster, and when nodes are removed.
#[derive(Copy, Clone, Debug)]
#[allow(missing_docs)]
pub enum TopologyChangeType {
    NewNode,
    RemovedNode,
}

impl FromStr for TopologyChangeType {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(match s {
            "NEW_NODE" => TopologyChangeType::NewNode,
            "REMOVED_NODE" => TopologyChangeType::RemovedNode,
            _ => return Err(anyhow::anyhow!("Invalid topology change type: {}", s)),
        })
    }
}

impl FromPayload for TopologyChangeType {
    fn from_payload(start: &mut usize, payload: &[u8]) -> anyhow::Result<Self> {
        Ok(TopologyChangeType::from_str(read_str(start, payload)?)?)
    }
}

/// Events related to change of node status.
#[derive(Copy, Clone, Debug)]
#[allow(missing_docs)]
pub enum StatusChangeType {
    Up,
    Down,
}

impl FromStr for StatusChangeType {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(match s {
            "UP" => StatusChangeType::Up,
            "DOWN" => StatusChangeType::Down,
            _ => return Err(anyhow::anyhow!("Invalid status change type: {}", s)),
        })
    }
}

impl FromPayload for StatusChangeType {
    fn from_payload(start: &mut usize, payload: &[u8]) -> anyhow::Result<Self> {
        Ok(StatusChangeType::from_str(read_str(start, payload)?)?)
    }
}

#[derive(Copy, Clone, Debug)]
#[allow(missing_docs)]
pub enum SchemaChangeType {
    Created,
    Updated,
    Dropped,
}

impl FromStr for SchemaChangeType {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(match s {
            "CREATED" => SchemaChangeType::Created,
            "UPDATED" => SchemaChangeType::Updated,
            "DROPPED" => SchemaChangeType::Dropped,
            _ => return Err(anyhow::anyhow!("Invalid schema change type: {}", s)),
        })
    }
}

impl FromPayload for SchemaChangeType {
    fn from_payload(start: &mut usize, payload: &[u8]) -> anyhow::Result<Self> {
        Ok(SchemaChangeType::from_str(read_str(start, payload)?)?)
    }
}

#[derive(Clone, Debug)]
#[allow(missing_docs)]
pub enum SchemaChangeTarget {
    Keyspace(String),
    Table {
        keyspace: String,
        table: String,
    },
    Type {
        keyspace: String,
        table: String,
    },
    Function {
        keyspace: String,
        name: String,
        args: Vec<String>,
    },
    Aggregate {
        keyspace: String,
        name: String,
        args: Vec<String>,
    },
}
